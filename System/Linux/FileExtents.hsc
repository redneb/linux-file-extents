------------------------------------------------------------------------------
-- |
-- Module      : System.Linux.FileExtents
--
-- Stability   : provisional
-- Portability : non-portable (requires Linux)
--
-- This module can be used to retrieve information about how a
-- particular file is stored on disk (i.e. the file fragmentation).
-- It accomplishes that by directly calling the FIEMAP ioctl provided by
-- recent versions of the Linux kernel. This ioctl is specific to Linux
-- and therefore this module is not portable.
--
-- For more information about the FIEMAP ioctl see @filesystems/fiemap.txt@
-- in the kernel documentation.
--
------------------------------------------------------------------------------


module System.Linux.FileExtents
    ( -- * Extent flags
      -- |See @filesystems/fiemap.txt@ in the kernel documentation for a more
      -- detailed description of each of these flags.
      ExtentFlags
    , efLast
    , efUnknown
    , efDelalloc
    , efEncoded
    , efDataEncrypted
    , efNotAligned
    , efDataInline
    , efDataTail
    , efUnwritten
    , efMerged
    , efShared
    -- * Extents
    , Extent(..)
    -- * Request flags
    , Flags(..)
    , defaultFlags
    -- * Getting extent information
    , getExtentsFd
    , getExtents
    , getExtentCountFd
    , getExtentCount
    ) where

import Control.Monad
import Control.Exception
import Data.Maybe

import Foreign hiding (void)
import Foreign.C
import System.Posix.Types
import System.Posix.IO

#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

--------------------------------------------------------------------------------
-- extent flags

type ExtentFlags = Word32

-- |Last extent in file.
efLast          :: ExtentFlags
efLast          = #const FIEMAP_EXTENT_LAST

-- |Data location unknown.
efUnknown       :: ExtentFlags
efUnknown       = #const FIEMAP_EXTENT_UNKNOWN

-- |Location still pending.
efDelalloc      :: ExtentFlags
efDelalloc      = #const FIEMAP_EXTENT_DELALLOC

-- |Data cannot be read while fs is unmounted.
efEncoded       :: ExtentFlags
efEncoded       = #const FIEMAP_EXTENT_ENCODED

-- |Data is encrypted by fs.
efDataEncrypted :: ExtentFlags
efDataEncrypted = #const FIEMAP_EXTENT_DATA_ENCRYPTED

-- |Extent offsets may not be block aligned.
efNotAligned    :: ExtentFlags
efNotAligned    = #const FIEMAP_EXTENT_NOT_ALIGNED

-- |Data mixed with metadata.
efDataInline    :: ExtentFlags
efDataInline    = #const FIEMAP_EXTENT_DATA_INLINE

-- |Multiple files in block.
efDataTail      :: ExtentFlags
efDataTail      = #const FIEMAP_EXTENT_DATA_TAIL

-- |Space allocated, but no data (i.e. zero).
efUnwritten     :: ExtentFlags
efUnwritten     = #const FIEMAP_EXTENT_UNWRITTEN

-- |File does not natively support extents. Result merged for efficiency.
efMerged        :: ExtentFlags
efMerged        = #const FIEMAP_EXTENT_MERGED

-- |Space shared with other files.
efShared        :: ExtentFlags
efShared        = #const FIEMAP_EXTENT_SHARED

--------------------------------------------------------------------------------
-- extent type

-- |Description of a single extent. All offsets and lengths are in bytes.
data Extent = Extent
    { extLogical :: Word64    -- ^ Offset relative to the beginning of the file.
    , extPhysical :: Word64   -- ^ Offset relative to the beginning of the underlying block device.
    , extLength :: Word64     -- ^ The length of the extent.
    , extFlags :: ExtentFlags -- ^ Flags for this extent.
    }
  deriving (Show, Eq)

instance Storable Extent where
    sizeOf _ = #size struct fiemap_extent
    alignment _ = alignment (undefined :: Int)
    peek ptr = do
        extLogical_  <- (#peek struct fiemap_extent, fe_logical ) ptr
        extPhysical_ <- (#peek struct fiemap_extent, fe_physical) ptr
        extLength_   <- (#peek struct fiemap_extent, fe_length  ) ptr
        extFlags_    <- (#peek struct fiemap_extent, fe_flags   ) ptr
        return (Extent extLogical_ extPhysical_ extLength_ extFlags_)
    poke ptr ext = do
        memset (castPtr ptr) 0 (#size struct fiemap_extent)
        (#poke struct fiemap_extent, fe_logical ) ptr (extLogical ext)
        (#poke struct fiemap_extent, fe_physical) ptr (extPhysical ext)
        (#poke struct fiemap_extent, fe_length  ) ptr (extLength ext)
        (#poke struct fiemap_extent, fe_flags   ) ptr (extFlags ext)

--------------------------------------------------------------------------------
-- request flags

-- |Request flags.
data Flags = Flags
    { fSync :: Bool  -- ^ Sync the file before requesting its extents.
    , fXattr :: Bool -- ^ Retrieve the extents of the inode's extended attribute lookup tree, instead of its data tree.
    }
  deriving (Show, Eq)

-- |Default values for the request flags. Both 'fSync' and 'fXattr' are set
-- to False.
defaultFlags :: Flags
defaultFlags = Flags False False

encodeFlags :: Flags -> Word32
encodeFlags f =
    (if fSync f then (#const FIEMAP_FLAG_SYNC) else 0)
      .|.
    (if fXattr f then (#const FIEMAP_FLAG_XATTR) else 0)

--------------------------------------------------------------------------------
-- get extents

-- | Retrieve the list of all extents associated with the file
-- referenced by the file descriptor. Extents returned mirror those on disk
-- - that is, the logical offset of the first returned extent may start
-- before the requested range, and the last returned extent may end after
-- the end of the requested range.
--
-- Note: 'getExtentsFd' might call the FIEMAP ioctl multiple times in order to
-- retrieve all the extents of the file. This is necessary when the file
-- has too many fragments. If the file is modified in the meantime, the
-- returned list might be inconsistent.
getExtentsFd
    :: Flags
    -> Fd
    -> Maybe (Word64, Word64) -- ^ The range (offset and length) within the file to look extents for. Use 'Nothing' for the entire file.
    -> IO [Extent]
getExtentsFd flags (Fd fd) range =
    allocaBytes allocSize $ \fiemap -> do
        let (start, len) = fromMaybe (0, maxBound) range
        memset (castPtr fiemap) 0 (#size struct fiemap)
        l <- getExtentsFd' start len fiemap
        return (concat l)
  where
    getExtentsFd' start len fiemap = do
        (#poke struct fiemap, fm_start       ) fiemap start
        (#poke struct fiemap, fm_length      ) fiemap len
        (#poke struct fiemap, fm_flags       ) fiemap flags'
        (#poke struct fiemap, fm_extent_count) fiemap maxExtentCount
        throwErrnoIfMinus1_ "getExtentsFd" $ ioctl fd (#const FS_IOC_FIEMAP) fiemap
        mappedExtents <- (#peek struct fiemap, fm_mapped_extents) fiemap :: IO Word32
        let extentsPtr = fiemap `plusPtr` (#offset struct fiemap, fm_extents)
        extents <- peekArray (fromIntegral mappedExtents) extentsPtr
        case extents of
            (_ : _) | mappedExtents == maxExtentCount
                    , lExt <- last extents
                    , lExtEnd <- extLogical lExt + extLength lExt
                    , bytesLeft <- start + len - lExtEnd
                    , bytesLeft > 0 -> do
                more <- getExtentsFd' lExtEnd bytesLeft fiemap
                return (extents : more)
            _ -> return [extents]
    flags' = encodeFlags flags
    maxExtentCount :: Word32
    maxExtentCount = (fromIntegral allocSize - (#size struct fiemap)) `quot` (#size struct fiemap_extent);
    allocSize = 16 * 1024

-- |Like 'getExtentsFd' except that it operates on file paths instead of
-- file descriptors.
getExtents :: Flags -> FilePath -> Maybe (Word64, Word64) -> IO [Extent]
getExtents flags path range = do
    bracket (openFd path ReadOnly Nothing defaultFileFlags) closeFd $ \fd ->
        getExtentsFd flags fd range

--------------------------------------------------------------------------------
-- get extent count

-- |Like 'getExtentsFd' except that it returns the number of extents
-- instead of a list.
getExtentCountFd :: Flags -> Fd -> Maybe (Word64, Word64) -> IO Word32
getExtentCountFd flags (Fd fd) range = do
    let (start, len) = fromMaybe (0, maxBound) range
    allocaBytes (#size struct fiemap) $ \fiemap -> do
        memset (castPtr fiemap) 0 (#size struct fiemap)
        (#poke struct fiemap, fm_start       ) fiemap start
        (#poke struct fiemap, fm_length      ) fiemap len
        (#poke struct fiemap, fm_flags       ) fiemap flags'
        (#poke struct fiemap, fm_extent_count) fiemap (0 :: Word32)
        throwErrnoIfMinus1_ "getExtentCountFd" $ ioctl fd (#const FS_IOC_FIEMAP) fiemap
        #{peek struct fiemap, fm_mapped_extents} fiemap
  where
    flags' = encodeFlags flags

-- |Like 'getExtents' except that it returns the number of extents
-- instead of a list.
getExtentCount :: Flags -> FilePath -> Maybe (Word64, Word64) -> IO Word32
getExtentCount flags path range = do
    bracket (openFd path ReadOnly Nothing defaultFileFlags) closeFd $ \fd ->
        getExtentCountFd flags fd range

--------------------------------------------------------------------------------
-- auxiliary stuff

foreign import ccall unsafe ioctl :: CInt -> CULong -> Ptr a -> IO CInt

foreign import ccall unsafe "string.h memset"
    c_memset :: Ptr a -> CInt -> CSize -> IO (Ptr a)

memset :: Ptr a -> Word8 -> CSize -> IO ()
memset p b l = void $ c_memset p (fromIntegral b) l
