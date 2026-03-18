"""Unit tests for CompressionManager."""

import sys
import os
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from compression import CompressionManager, SUPPORTED_FORMATS, EXTENSIONS


@pytest.fixture
def compressor():
    return CompressionManager()


@pytest.fixture
def sample_file(tmp_path):
    p = tmp_path / 'sample.sql'
    content = b'CREATE TABLE test (id INT);\n' * 1000
    p.write_bytes(content)
    return str(p)


def test_compress_gzip(compressor, sample_file):
    out = compressor.compress_file(sample_file, fmt='gzip')
    assert out.endswith('.gz')
    assert os.path.exists(out)
    assert os.path.getsize(out) < os.path.getsize(sample_file)


def test_compress_bzip2(compressor, sample_file):
    out = compressor.compress_file(sample_file, fmt='bzip2')
    assert out.endswith('.bz2')
    assert os.path.exists(out)


def test_compress_zstd(compressor, sample_file):
    try:
        import zstandard
        out = compressor.compress_file(sample_file, fmt='zstd')
        assert out.endswith('.zst')
        assert os.path.exists(out)
    except ImportError:
        pytest.skip('zstandard not installed')


def test_decompress_gzip(compressor, sample_file):
    original_content = open(sample_file, 'rb').read()
    compressed = compressor.compress_file(sample_file, fmt='gzip')
    decompressed = compressor.decompress_file(compressed)
    assert open(decompressed, 'rb').read() == original_content


def test_decompress_bzip2(compressor, sample_file):
    original_content = open(sample_file, 'rb').read()
    compressed = compressor.compress_file(sample_file, fmt='bzip2')
    decompressed = compressor.decompress_file(compressed)
    assert open(decompressed, 'rb').read() == original_content


def test_compress_remove_src(compressor, sample_file):
    compressor.compress_file(sample_file, fmt='gzip', remove_src=True)
    assert not os.path.exists(sample_file)


def test_unsupported_format(compressor, sample_file):
    with pytest.raises(ValueError):
        compressor.compress_file(sample_file, fmt='lzma_xyz')


def test_detect_format(compressor):
    assert compressor.detect_format('dump.sql.gz') == 'gzip'
    assert compressor.detect_format('dump.sql.bz2') == 'bzip2'
    assert compressor.detect_format('dump.sql.zst') == 'zstd'
    assert compressor.detect_format('dump.sql') is None


def test_get_output_path(compressor):
    assert compressor.get_output_path('/tmp/dump.sql', 'gzip') == '/tmp/dump.sql.gz'
    assert compressor.get_output_path('/tmp/dump.sql', 'bzip2') == '/tmp/dump.sql.bz2'


def test_compress_stream_gzip(compressor, tmp_path):
    import gzip
    src = tmp_path / 'src.sql'
    src.write_bytes(b'INSERT INTO foo VALUES (1);\n' * 500)
    dst = tmp_path / 'dst.sql.gz'
    with open(str(src), 'rb') as fin, open(str(dst), 'wb') as fout:
        compressor.compress_stream(fin, fout, fmt='gzip')
    assert dst.exists()
    with gzip.open(str(dst), 'rb') as f:
        assert f.read() == src.read_bytes()
