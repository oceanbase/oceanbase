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

#ifndef OB_CODECS_H_
#define OB_CODECS_H_

#include "ob_bp_util.h"
#include "ob_bp_helpers.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{
namespace common
{


#define CHECK_ENCODE_PARAM\
  int ret = OB_SUCCESS; \
  if (OB_UNLIKELY(OB_ISNULL(in) || OB_ISNULL(out) || (0 == in_len) || (0 == out_len) \
        || !is_valid_uint_bytes())) { \
    ret = OB_INVALID_ARGUMENT; \
    LIB_LOG(WARN, "invalid argument", K(ret), KP(in), KP(out), K(out_len), K(in_len), \
            K(out_pos), K(uint_bytes_), KPC(this)); \
  }

#define CHECK_ENCODE_END \
  if (OB_SUCC(ret)) { \
    if (OB_UNLIKELY(out_pos > out_len)) { \
      ret = OB_ERR_UNEXPECTED; \
      LIB_LOG(ERROR, "memory maybe overrun", K(ret), KPC(this), KP(in), K(in_len), KP(out), K(out_len), K(out_pos)); \
    } \
  }

#define CHECK_DECODE_PARAM \
  int ret = OB_SUCCESS; \
  const uint64_t orig_out_pos = out_pos; \
  if (OB_UNLIKELY(OB_ISNULL(in) || OB_ISNULL(out) || (0 == in_len) || (0 == out_len) || (0 == uint_count) \
        || (in_pos > in_len) || (out_pos > out_len) || ((check_out_buf_ && ((out_len - out_pos) < uint_count * get_uint_bytes())) \
        || !is_valid_uint_bytes()) || !divisibleby(uint_count, get_block_size()))) { \
    ret = OB_INVALID_ARGUMENT; \
    LIB_LOG(WARN, "invalid argument", K(ret), KP(in), KP(out), K(out_len), \
            K(in_len), K(uint_count), K(out_pos), K(in_pos), K(get_block_size()), KPC(this)); \
  }

#define CHECK_DECODE_END \
  if (OB_SUCC(ret)) { \
    if (OB_UNLIKELY((in_pos > in_len) || (out_pos > out_len) || (check_out_buf_ && (uint_count * get_uint_bytes() != (out_pos - orig_out_pos))))) { \
      ret = OB_ERR_UNEXPECTED; \
      LIB_LOG(ERROR, "unexpected", K(ret), KP(in), K(in_pos), K(uint_count), K(in_len), \
              KP(out), K(out_len), K(out_pos), K(orig_out_pos), KPC(this)); \
    } \
  }

#define TYPE_CONVERT(IN_UTYPE, OUT_UTYPE) \
  const IN_UTYPE *t_in = reinterpret_cast<const IN_UTYPE *>(in + in_pos); \
  const uint64_t t_in_len = (in_len - in_pos)/sizeof(IN_UTYPE); \
  uint64_t t_in_pos = 0; \
  OUT_UTYPE *t_out = reinterpret_cast<OUT_UTYPE *>(out + out_pos); \
  const uint64_t t_out_len = (out_len - out_pos)/sizeof(OUT_UTYPE); \
  uint64_t t_out_pos = 0;

#define ADVANCE_POS(IN_UTYPE, OUT_UTYPE) \
  if (OB_SUCC(ret)) { \
    in_pos += t_in_pos * sizeof(IN_UTYPE); \
    out_pos += t_out_pos * sizeof(OUT_UTYPE); \
  }


class ObCodec
{
public:
  // get buf size which will be used to hold output data of current codec,
  // in order to make enough buf, this value is estimated to be very large.
  static OB_INLINE int64_t get_default_max_encoding_size(const int64_t orig_size)
  {
    return 1024/*fixed meta cost*/ + 2 * orig_size;
  }
  // get buf size which will be used to hold output data of current codec,
  // in order not to consume a large amount of memory, this value is moderately estimated
  // and may lead buf not enough for some codecs, but it must be enough for raw encoding for which
  // the encoding size is equal to original size
  static OB_INLINE int64_t get_moderate_encoding_size(const int64_t orig_size)
  {
    return 1024/*fixed meta cost*/ + orig_size;
  }

  ObCodec()
    : uint_bytes_(0), check_out_buf_(true), allocator_(nullptr)
  {}

  virtual ~ObCodec() {}

  OB_INLINE int encode(const char *in,
                       const uint64_t in_len,
                       char *out,
                       const uint64_t out_len,
                       uint64_t &out_pos)
  {
    CHECK_ENCODE_PARAM;
    if (FAILEDx(do_encode(in, in_len, out, out_len, out_pos))) {
      LIB_LOG(WARN, "fail to do encode", K(ret), KP(in), K(in_len), KP(out),
              K(out_len), K(out_pos), KPC(this));
    }
    CHECK_ENCODE_END;
    return ret;
  }

  virtual OB_INLINE int do_encode(const char *in,
                                  const uint64_t in_len,
                                  char *out,
                                  const uint64_t out_len,
                                  uint64_t &out_pos)
  {
    return OB_NOT_SUPPORTED;
  }


  OB_INLINE int decode(const char *in,
                       const uint64_t in_len,
                       uint64_t &in_pos,
                       const uint64_t uint_count,
                       char *out,
                       const uint64_t out_len,
                       uint64_t &out_pos)
  {
    CHECK_DECODE_PARAM;
    if (FAILEDx(do_decode(in, in_len, in_pos, uint_count, out, out_len, out_pos))) {
      LIB_LOG(WARN, "fail to do decode", K(ret), KP(in), K(in_len), K(in_pos),
              K(uint_count), K(out_len), KP(out), K(out_pos), KPC(this));

    }
    CHECK_DECODE_END;
    return ret;
  }

  virtual int do_decode(const char *in,
                        const uint64_t in_len,
                        uint64_t &in_pos,
                        const uint64_t uint_count,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos)
  {
    return OB_NOT_SUPPORTED;
  }


  // number of bytes, that will be used to compress uncompressed_size bytes with current codec.
  // no need to be too precise.
  virtual uint64_t get_max_encoding_size(const char *in, const uint64_t length) {
    UNUSED(in);
    return get_default_max_encoding_size(length); // ensure plenty of memory
  }

  virtual uint32_t get_block_size() = 0;

  OB_INLINE bool is_valid_uint_bytes() const
  {
    return (1 == uint_bytes_) || (2 == uint_bytes_) || (4 == uint_bytes_) || (8 == uint_bytes_);
  }

  virtual const char *name() const = 0;

  virtual void set_uint_bytes(const uint8_t uint_bytes) { uint_bytes_ = uint_bytes; }
  OB_INLINE uint32_t get_uint_bytes() const { return uint_bytes_; };

  virtual void set_allocator(common::ObIAllocator &allocator) { allocator_ = &allocator; }
  OB_INLINE void disable_check_out_buf() { check_out_buf_ = false; }

  VIRTUAL_TO_STRING_KV(K(uint_bytes_), KP(allocator_), K(check_out_buf_), K(name()));

protected:
  uint8_t uint_bytes_; // orig uint bytes width, maybe 1, 2, 4, 8 bytes
  bool check_out_buf_;
  common::ObIAllocator *allocator_;
};

class ObJustCopy : public ObCodec
{
public:
  enum
  {
    BlockSize = 1,
  };
  virtual uint32_t get_block_size() override { return BlockSize; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(out_len - out_pos < in_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf not enough", K(ret), K(in_len), K(out_len), K(out_pos));
    } else {
      memcpy(out + out_pos, in, in_len);
      out_pos += in_len;
    }
    return ret;
  }

  virtual int do_decode(const char *in,
                        const uint64_t in_len,
                        uint64_t &in_pos,
                        const uint64_t uint_count,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    memcpy(out + out_pos, in + in_pos, uint_count * get_uint_bytes());
    out_pos += uint_count * get_uint_bytes();
    in_pos += uint_count * get_uint_bytes();
    return OB_SUCCESS;
  }

  virtual const char *name() const override { return "ObJustCopy"; }
};

class ObSimpleBitPacking : public ObCodec
{
public:
  ObSimpleBitPacking() : uint_packing_bits_(0)
  {}

  virtual const char *name() const override { return "ObSimpleBitPacking"; }
  enum
  {
    BlockSize = 1,
  };
  virtual uint32_t get_block_size() override { return BlockSize; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes())  {
      case 1 : {
        ret = _encode_array<uint8_t>(in, in_len, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 2 : {
        ret = _encode_array<uint16_t>(in, in_len, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 4 : {
        ret = _encode_array<uint32_t>(in, in_len, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 8 : {
        ret = _encode_array<uint64_t>(in, in_len, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "fail to do_encode", K(ret), K(get_uint_bytes()), KPC(this));
    }
    return ret;
  }

  template<typename T>
  static OB_INLINE int _encode_array(
      const char *in,
      const uint64_t in_len,
      char *out,
      uint64_t out_len,
      uint64_t &out_pos,
      const uint8_t uint_packing_bits)
  {
    int ret = OB_SUCCESS;
    const char *orig_in = in;
    char *orig_out = out;
    out += out_pos;
    const T *t_in_begin = reinterpret_cast<const T *>(in);

    const int64_t max_encoded_size = 1/*b*/ + in_len + 8/*safe reserve*/;
    if (OB_UNLIKELY(out_len - out_pos < max_encoded_size)) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf not enough", K(ret), K(in_len), K(out_len), K(out_pos), K(max_encoded_size));
    } else {
      uint32_t b = 0;
      if (0 != uint_packing_bits) {
        // byte packing no need record b
        b = uint_packing_bits;
      } else {
        const T *t_in_end = reinterpret_cast<const T *>(in + in_len);
        b = maxbits2<T>(t_in_begin, t_in_end);
        out[0] = b;
        out++;
        out_pos++;
      }
      scalar_bit_packing<T>(t_in_begin, in_len / sizeof(T), b, orig_out, out_len, out_pos);
    }
    return ret;
  }

  virtual int do_decode(const char *in,
                        const uint64_t in_len,
                        uint64_t &in_pos,
                        const uint64_t uint_count,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes())  {
      case 1 : {
        _decode_array<uint8_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 2 : {
        _decode_array<uint16_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 4 : {
        _decode_array<uint32_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      case 8 : {
        _decode_array<uint64_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, uint_packing_bits_);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        LIB_LOG(WARN, "not support", K(ret), K(get_uint_bytes()), KPC(this));
        break;
      }
    }
    return ret;
  }

  template<typename T>
  static OB_INLINE void _decode_array(
      const char *in,
      const uint64_t in_len,
      uint64_t &in_pos,
      const uint64_t uint_count,
      char *out,
      const uint64_t out_len,
      uint64_t &out_pos,
      const uint8_t uint_packing_bits)
  {
    const char *orig_in = in;
    in += in_pos;
    out += out_pos;

    uint32_t b = 0;
    if (0 != uint_packing_bits) {
      b = uint_packing_bits;
    } else {
      b = *in;
      in++;
      in_pos++;
    }

    T *t_uint_out = reinterpret_cast<T *>(out);
    uint64_t t_out_len = (out_len - out_pos) / sizeof(T);
    uint64_t t_out_pos = 0;
    scalar_bit_unpacking<T>(orig_in, in_len, in_pos, (uint32_t)uint_count, b, t_uint_out, t_out_len, t_out_pos);
    out_pos += t_out_pos * sizeof(T);
  }

  OB_INLINE void set_uint_packing_bits(const uint8_t uint_packing_bits) { uint_packing_bits_ = uint_packing_bits; }

  INHERIT_TO_STRING_KV("ObCodec", ObCodec, K(uint_packing_bits_));
protected:
  uint8_t uint_packing_bits_;
};

} // namespace common
} // namespace oceanbase

#endif /* OB_CODECS_H_ */
