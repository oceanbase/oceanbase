#ifndef OCEANBASE_COMMON_COMPRESS_ZLIB_LITE_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_ZLIB_LITE_COMPRESSOR_H_

#include "lib/compress/ob_compressor.h"
#include "zlib_lite_src/zconf.h"
namespace oceanbase {
namespace common {
namespace ZLIB_LITE
{
#define OB_PUBLIC_API __attribute__((visibility("default")))

class OB_PUBLIC_API ObZlibLiteCompressor : public ObCompressor {
public:
  int compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);

  int decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);

  const char* get_compressor_name() const;

  int get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const;
  virtual ObCompressorType get_compressor_type() const;
  explicit ObZlibLiteCompressor();
  virtual ~ObZlibLiteCompressor();
private:
  static const char* compressor_name;
  int zlib_lite_compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size);
  int zlib_lite_decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size);
  //has the same function as the compress and uncompress functions in the zlib source code.
  int zlib_compress(Bytef *dest, uLongf *destLen, const Bytef *source, uLong sourceLen);
  int zlib_decompress(Bytef *dest, uLongf *destLen, const Bytef *source, uLong sourceLen);
  //zlib compress level,default is 1.
  static constexpr auto compress_level = 1;
  //zlib window bits,in order to compress and decompress each other with the qpl algorithm, this parameter can only be -12.
  static constexpr auto window_bits = -12;
};
}
#undef OB_PUBLIC_API

} 
} 
#endif