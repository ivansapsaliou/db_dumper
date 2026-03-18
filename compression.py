"""
CompressionManager — unified interface for dump file compression.

Supported formats:
  gzip  (.gz)  — standard, widely supported, default
  bzip2 (.bz2) — better compression, slower
  zstd  (.zst) — fast + high compression ratio (requires zstandard package)
"""

import gzip
import bz2
import os
import shutil
import logging
from pathlib import Path
from typing import BinaryIO

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = ('gzip', 'bzip2', 'zstd')
EXTENSIONS = {'gzip': '.gz', 'bzip2': '.bz2', 'zstd': '.zst'}


class CompressionManager:

    def compress_file(self, src_path: str, fmt: str = 'gzip', level: int | None = None,
                      remove_src: bool = False) -> str:
        """
        Compress *src_path* using the requested format.
        Returns the path of the compressed output file.
        Optionally removes the original source file.
        """
        fmt = fmt.lower()
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(f'Unsupported compression format: {fmt}')

        ext = EXTENSIONS[fmt]
        dst_path = src_path + ext

        try:
            if fmt == 'gzip':
                lvl = level if level is not None else 6
                with open(src_path, 'rb') as f_in, gzip.open(dst_path, 'wb', compresslevel=lvl) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif fmt == 'bzip2':
                lvl = level if level is not None else 9
                with open(src_path, 'rb') as f_in, bz2.open(dst_path, 'wb', compresslevel=lvl) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif fmt == 'zstd':
                try:
                    import zstandard as zstd
                except ImportError:
                    logger.warning('zstandard not installed — falling back to gzip')
                    return self.compress_file(src_path, 'gzip', level, remove_src)
                lvl = level if level is not None else 3
                cctx = zstd.ZstdCompressor(level=lvl)
                with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
                    cctx.copy_stream(f_in, f_out)

            orig_size = os.path.getsize(src_path)
            comp_size = os.path.getsize(dst_path)
            ratio = (1 - comp_size / orig_size) * 100 if orig_size > 0 else 0
            logger.info(
                f'Compressed {os.path.basename(src_path)} [{fmt}]: '
                f'{_fmt_bytes(orig_size)} → {_fmt_bytes(comp_size)} ({ratio:.1f}% reduction)'
            )

            if remove_src:
                os.remove(src_path)

            return dst_path

        except Exception as e:
            # Clean up incomplete output
            if os.path.exists(dst_path):
                try:
                    os.remove(dst_path)
                except OSError:
                    pass
            raise RuntimeError(f'Compression failed ({fmt}): {e}') from e

    def decompress_file(self, src_path: str, dst_path: str | None = None,
                        remove_src: bool = False) -> str:
        """
        Auto-detect format from extension and decompress.
        Returns path to the decompressed file.
        """
        p = Path(src_path)
        ext = p.suffix.lower()
        if dst_path is None:
            dst_path = str(p.with_suffix(''))  # strip compression extension

        try:
            if ext == '.gz':
                with gzip.open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif ext == '.bz2':
                with bz2.open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif ext == '.zst':
                try:
                    import zstandard as zstd
                except ImportError:
                    raise RuntimeError('zstandard package required to decompress .zst files')
                dctx = zstd.ZstdDecompressor()
                with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
                    dctx.copy_stream(f_in, f_out)

            else:
                raise ValueError(f'Unknown compression extension: {ext}')

            if remove_src:
                os.remove(src_path)

            return dst_path

        except Exception as e:
            if os.path.exists(dst_path):
                try:
                    os.remove(dst_path)
                except OSError:
                    pass
            raise

    def get_output_path(self, base_path: str, fmt: str) -> str:
        """Return what the output filename would be for a given format."""
        return base_path + EXTENSIONS.get(fmt.lower(), '')

    def detect_format(self, path: str) -> str | None:
        """Return compression format string from file extension, or None."""
        ext = Path(path).suffix.lower()
        reverse = {v: k for k, v in EXTENSIONS.items()}
        return reverse.get(ext)

    def compress_stream(self, f_in: BinaryIO, f_out: BinaryIO, fmt: str = 'gzip',
                        level: int | None = None) -> None:
        """Compress from one open binary stream to another (streaming support)."""
        fmt = fmt.lower()
        if fmt == 'gzip':
            lvl = level if level is not None else 6
            with gzip.GzipFile(fileobj=f_out, mode='wb', compresslevel=lvl) as gz:
                shutil.copyfileobj(f_in, gz)
        elif fmt == 'bzip2':
            lvl = level if level is not None else 9
            with bz2.BZ2File(f_out, mode='w', compresslevel=lvl) as bz:
                shutil.copyfileobj(f_in, bz)
        elif fmt == 'zstd':
            try:
                import zstandard as zstd
                lvl = level if level is not None else 3
                cctx = zstd.ZstdCompressor(level=lvl)
                cctx.copy_stream(f_in, f_out)
            except ImportError:
                logger.warning('zstandard not installed — falling back to gzip streaming')
                self.compress_stream(f_in, f_out, 'gzip', level)
        else:
            raise ValueError(f'Unsupported format: {fmt}')


def _fmt_bytes(b: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if b < 1024:
            return f'{b:.1f} {unit}'
        b /= 1024
    return f'{b:.2f} TB'
