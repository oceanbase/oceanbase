/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_UNIVERSAL_COMPRESSION_H_
#define OB_UNIVERSAL_COMPRESSION_H_

#include "ob_codecs.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace common
{

class ObUniversalCompression : public ObCodec {
public:
  enum
  {
    BlockSize = 1,
  };
  ObUniversalCompression() : compressor_type_(ObCompressorType::INVALID_COMPRESSOR)
  {}

  virtual uint32_t get_block_size() override { return BlockSize; }

  virtual const char *name() const override { return "ObUniversalCompression"; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    return _encode_array(in, in_len, out, out_len, out_pos);
  }

  virtual int do_decode(const char *in,
                        const uint64_t in_len,
                        uint64_t &in_pos,
                        const uint64_t uint_count,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    return _decode_array(in, in_len, in_pos, uint_count, out, out_len, out_pos);
  }


  OB_INLINE int _encode_array(const char *in, const uint64_t in_len,
                              char *out, uint64_t out_len, uint64_t &out_pos)
  {
    int ret = OB_SUCCESS;
    ObCompressor *compressor = nullptr;
    int64_t dest_buf_size = out_len - out_pos;
    char *dest_buf = out + out_pos;
    int64_t dst_data_size = 0;
    int64_t max_overflow_size = 0;

    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type_, compressor))) {
      LIB_LOG(WARN, "fail to get compressor", K(ret), K(compressor_type_));
    } else if (OB_FAIL(compressor->get_max_overflow_size(in_len, max_overflow_size))) {
      LIB_LOG(WARN, "fail to get_max_overflow_size", K(ret), K(in_len), K(compressor_type_));
    } else {
      char *compress_buf = dest_buf;
      const int64_t compress_buf_size = in_len + max_overflow_size;
      if (dest_buf_size < compress_buf_size &&
          OB_ISNULL(compress_buf = (char*)allocator_->alloc(compress_buf_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "fail to malloc", K(ret), K(max_overflow_size), K(in_len), K(dest_buf_size));
      } else if (OB_FAIL(compressor->compress(in, in_len, compress_buf, compress_buf_size, dst_data_size))) {
        LIB_LOG(WARN, "fail to compress", K(ret), K(compressor_type_), K(in_len), K(compress_buf_size), K(dest_buf_size));
      } else if (dst_data_size > dest_buf_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "fail to compress", K(ret), K(compressor_type_), K(in_len), K(dest_buf_size), K(compress_buf_size), K(dst_data_size));
      } else if (dest_buf != compress_buf) {
        memcpy(dest_buf, compress_buf, dst_data_size);
      }
    }

    if (OB_SUCC(ret)) {
      out_pos += dst_data_size;
    }
    return ret;
  }

  OB_INLINE int _decode_array(const char *in, const uint64_t in_len, uint64_t &in_pos,
                              const uint64_t uint_count, char *out, const uint64_t out_len,
                              uint64_t &out_pos)
  {
    int ret = OB_SUCCESS;

    const char *src_buf = in + in_pos;
    const int64_t src_data_size = in_len - in_pos;
    char *dest_buf = out;
    const int64_t dest_buf_size = out_len - out_pos;
    int64_t dst_data_size = 0;
    ObCompressor *compressor = nullptr;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type_, compressor))) {
      LIB_LOG(WARN, "fail to get compressor, ", K(ret), K(compressor_type_));
    } else if (OB_FAIL(compressor->decompress(src_buf, src_data_size, dest_buf, dest_buf_size, dst_data_size))) {
      LIB_LOG(WARN, "fail to decompress, ", K(ret), K(compressor_type_), K(src_data_size), K(dest_buf_size));
    } else {
      in_pos = in_len;
      out_pos += dst_data_size;
    }

    return ret;
  }

  void set_compressor_type(const ObCompressorType compressor_type) { compressor_type_ = compressor_type; }
  INHERIT_TO_STRING_KV("ObCodec", ObCodec, "compressor_type", all_compressor_name[compressor_type_]);

protected:
  ObCompressorType compressor_type_;
};

} // namespace common
} // namespace oceanbase

#endif /* OB_UNIVERSAL_COMPRESSION_H_ */
