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

#ifndef OB_COMPOSITE_CODEC_H_
#define OB_COMPOSITE_CODEC_H_

#include "ob_codecs.h"
#include "ob_bp_util.h"

namespace oceanbase
{
namespace common
{

template <class Codec1, class Codec2>
class ObCompositeCodec : public ObCodec
{
 public:
  ObCompositeCodec() : codec1(), codec2() {}

  enum { BlockSize = 1 };
  Codec1 codec1;
  Codec2 codec2;

  virtual uint32_t get_block_size() override { return BlockSize; }

  virtual uint64_t get_max_encoding_size(const char *in, const uint64_t length) override
  {
    uint64_t len = 0;
    const uint64_t rounded_length = length / get_uint_bytes() / Codec1::BlockSize * Codec1::BlockSize;
    const uint64_t rounded_byte_length = rounded_length * get_uint_bytes();
    len += codec1.get_max_encoding_size(in, rounded_byte_length);
    len += codec2.get_max_encoding_size(in + rounded_byte_length, length - rounded_byte_length);
    return len;
  }

  virtual void set_uint_bytes(const uint8_t uint_bytes) override
  {
    ObCodec::set_uint_bytes(uint_bytes);
    codec1.set_uint_bytes(uint_bytes);
    codec2.set_uint_bytes(uint_bytes);
  }

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

  const char *name() const override { return "ObCompositeCodec"; }

 private:
  OB_INLINE int _encode_array(
      const char *in,
      const uint64_t in_len,
      char *out,
      uint64_t out_len,
      uint64_t &out_pos) {
    int ret = OB_SUCCESS;
    uint64_t uint_cnt = in_len / get_uint_bytes();
    const size_t rounded_length = uint_cnt / Codec1::BlockSize * Codec1::BlockSize;
    const uint64_t rounded_byte_length = rounded_length * get_uint_bytes();
    if (rounded_length > 0) {
      if (OB_FAIL(codec1.encode(in, rounded_byte_length, out, out_len, out_pos))) {
        LIB_LOG(WARN, "fail to encode array", K(rounded_byte_length), KPC(this),
                K(out_len), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (rounded_byte_length < in_len) {
        if (OB_UNLIKELY(out_pos > out_len)) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "invalid out pos", K(ret), K(out_pos), K(out_len));
        } else if (OB_FAIL(codec2.encode(in + rounded_byte_length,
                in_len - rounded_byte_length, out, out_len, out_pos))) {
          LIB_LOG(WARN, "fail to encode array", KP(in), K(rounded_byte_length),
                  K(out_len), K(in_len), K(out_pos), K(ret));
        }
      }
    }
    return ret;
  }

  OB_INLINE int _decode_array(
      const char *in,
      const uint64_t in_len,
      uint64_t &in_pos,
      const uint64_t uint_count,
      char *out,
      const uint64_t out_len,
      uint64_t &out_pos)
  {
    int ret = OB_SUCCESS;
    const size_t rounded_length = uint_count / Codec1::BlockSize * Codec1::BlockSize;
    uint64_t uint_count1 = rounded_length;
    if (uint_count1 > 0) {
      if (OB_FAIL(codec1.decode(in, in_len, in_pos, uint_count1, out, out_len, out_pos))) {
        LIB_LOG(WARN, "fail to decode array", K(in_len), K(in_pos), K(uint_count1), K(out_len),
                K(out_pos), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (uint_count > rounded_length) {
        uint64_t uint_count2 = uint_count - rounded_length;
        if (OB_FAIL(codec2.decode(in, in_len, in_pos, uint_count2, out, out_len, out_pos))) {
          LIB_LOG(WARN, "fail to decode array", K(in_len), K(in_pos), K(uint_count2), K(out_pos), K(ret));
        }
      } else if (uint_count == rounded_length) {
        //nothing
      } else {
        // impossible
      }
    }
    return ret;
  }
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_COMPOSITE_CODEC_H_ */
