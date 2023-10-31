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

#ifndef OB_TIRED_CODEC_H_
#define OB_TIRED_CODEC_H_

#include "ob_codecs.h"
#include "ob_bp_util.h"

namespace oceanbase
{
namespace common
{

template <class Codec1, class Codec2>
class ObTiredCodec : public ObCodec
{
 public:
  ObTiredCodec() : codec1_(), codec2_() {}

  enum { BlockSize = 1 };
  Codec1 codec1_;
  Codec2 codec2_;

  virtual uint32_t get_block_size() override { return BlockSize; }

  virtual uint64_t get_max_encoding_size(const char *in, const uint64_t length) override
  {
    uint64_t len1 = codec1_.get_max_encoding_size(in, length);
    uint64_t len2 = codec2_.get_max_encoding_size(in, length);
    uint64_t len = ((len1 > len2) ? len1 : len2);
    return len;
  }

  virtual void set_uint_bytes(const uint8_t uint_bytes) override
  {
    ObCodec::set_uint_bytes(uint_bytes);
    codec1_.set_uint_bytes(uint_bytes);
    codec2_.set_uint_bytes(uint_bytes);
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

  const char *name() const override { return "ObTiredCodec"; }

 private:
  OB_INLINE int _encode_array(
      const char *in,
      const uint64_t in_len,
      char *out,
      uint64_t out_len,
      uint64_t &out_pos) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY((get_block_size() != codec1_.get_block_size()) || (get_block_size() != codec2_.get_block_size()))) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid argument", K(get_block_size()), K(codec1_.get_block_size()), K(codec2_.get_block_size()));
    } else {
      int64_t alloc_len = codec1_.get_max_encoding_size(in, in_len);
      char *t_buf = nullptr;
      uint64_t t_out_pos = 0;

      if (OB_ISNULL(t_buf = (char *)allocator_->alloc(alloc_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "fail to alloc", K(ret));
      } else if (OB_FAIL(codec1_.encode(in, in_len, t_buf, alloc_len, t_out_pos))) {
        LIB_LOG(WARN, "fail to codec1 encode", K(ret), K(codec1_), KPC(this), K(alloc_len), K(in_len));
      } else {
        int64_t tmp_out_pos = out_pos;
        // store codec2 uncompressed len, use vbyte
        if (OB_FAIL(serialization::encode_vi64(out, out_len, tmp_out_pos, t_out_pos))) {
          LIB_LOG(WARN, "fail to encode vi64", K(ret), KP(out), K(out_pos), K(out_len),
                  K(t_out_pos), K(tmp_out_pos));
        } else {
          out_pos = tmp_out_pos;
        }
      }
      if (FAILEDx(codec2_.encode(t_buf, t_out_pos, out, out_len, out_pos))) {
        LIB_LOG(WARN, "fail to codec2 encode", K(ret), K(codec2_), KPC(this), K(alloc_len), K(t_out_pos));
      }

      if (nullptr != t_buf) {
        allocator_->free(t_buf);
        t_buf = nullptr;
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
    if (OB_UNLIKELY((get_block_size() != codec1_.get_block_size()) || (get_block_size() != codec2_.get_block_size()))) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid argument", K(get_block_size()), K(codec1_.get_block_size()), K(codec2_.get_block_size()));
    } else {
      int64_t alloc_len = 0;
      char *t_buf = nullptr;
      uint64_t t_out_pos = 0;
      uint64_t in_pos2 = 0;
      codec2_.disable_check_out_buf();

      int64_t tmp_in_pos = in_pos;
      if (OB_FAIL(serialization::decode_vi64(in, in_len, tmp_in_pos, &alloc_len))) {
        LIB_LOG(WARN, "fail to decode vi64", K(ret), KP(in), K(in_len), K(in_pos), K(tmp_in_pos));
      } else if (FALSE_IT(in_pos = tmp_in_pos)) {
        // impossible
      } else if (OB_ISNULL(t_buf = (char *)allocator_->alloc(alloc_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "fail to alloc", K(ret), K(alloc_len));
      } else if (OB_FAIL(codec2_.decode(in, in_len, in_pos, uint_count, t_buf, alloc_len, t_out_pos))) {
        LIB_LOG(WARN, "fail to codec2 decode", K(ret), K(codec1_), KPC(this), K(alloc_len), K(in_len));
      } else if (OB_FAIL(codec1_.decode(t_buf, t_out_pos, in_pos2, uint_count, out, out_len, out_pos))) {
        LIB_LOG(WARN, "fail to codec1 decode", K(ret), K(codec2_), KPC(this), K(alloc_len), K(t_out_pos));
      }

      if (nullptr != t_buf) {
        allocator_->free(t_buf);
        t_buf = nullptr;
      }
    }

    return ret;
  }
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_TIRED_CODEC_H_ */
