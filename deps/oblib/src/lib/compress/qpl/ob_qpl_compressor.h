
#ifndef OCEANBASE_COMMON_COMPRESS_QPL_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_QPL_COMPRESSOR_H_

#include "lib/compress/ob_compressor.h"

namespace oceanbase {
namespace common {
#define OB_PUBLIC_API __attribute__((visibility("default")))

class OB_PUBLIC_API ObQPLCompressor : public ObCompressor {
public:
  int compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);

  int decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);

  const char* get_compressor_name() const;

  int get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const;
  virtual ObCompressorType get_compressor_type() const;
  ObQPLCompressor();
  virtual ~ObQPLCompressor();

private:
  static const char* compressor_name;
};

#undef OB_PUBLIC_API

} 
} 
#endif
