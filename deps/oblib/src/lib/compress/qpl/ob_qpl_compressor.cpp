#include "lib/compress/qpl/ob_qpl_compressor.h"
#include "lib/compress/qpl/qpl_src/qpl_low_level_api.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {
const char* ObQPLCompressor::compressor_name = "qpl";

ObQPLCompressor::ObQPLCompressor()
{
   if (QPL_job_pool_init()) {
    QPL_ERROR("qpl job pool init faild.\n");
  }
}

ObQPLCompressor::~ObQPLCompressor()
{
  QPL_job_pool_destroy();
}


int ObQPLCompressor::compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "invalid compress argument, ",
        K(ret),
        KP(src_buffer),
        K(src_data_size),
        KP(dst_buffer),
        K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ", K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else if (0 >= (dst_data_size = QPL_compress(
                       src_buffer, dst_buffer, static_cast<int>(src_data_size), static_cast<int>(dst_buffer_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
        "fail to compress data by QPL_compress, ",
        K(ret),
        K(src_buffer),
        K(src_data_size),
        K(dst_buffer_size),
        K(dst_data_size));
  }

  return ret;
}

int ObQPLCompressor::decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "invalid decompress argument, ",
        K(ret),
        KP(src_buffer),
        K(src_data_size),
        K(dst_buffer),
        K(dst_buffer_size));
  } else if (0 >= (dst_data_size = QPL_decompress(
                       src_buffer, dst_buffer, static_cast<int>(src_data_size), static_cast<int>(dst_buffer_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
        "fail to decompress by QPL_uncompress, ",
        K(ret),
        KP(src_buffer),
        K(src_data_size),
        K(dst_buffer_size),
        K(dst_data_size));
  }

  return ret;
}

int ObQPLCompressor::get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = src_data_size / 255 + 16;
  }
  return ret;
}

const char* ObQPLCompressor::get_compressor_name() const
{
  return compressor_name;
}

ObCompressorType ObQPLCompressor::get_compressor_type() const
{
  return ObCompressorType::QPL_COMPRESSOR;
}

}  

}  

