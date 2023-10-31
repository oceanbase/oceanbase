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

#ifndef OB_DOUBLE_DELTA_ZIGZAG_RLE_H_
#define OB_DOUBLE_DELTA_ZIGZAG_RLE_H_

#include "ob_codecs.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

class ObDoubleDeltaZigzagRleInner
{
public:
static const uint64_t BASE_REPEAT_CNT = 18;

template<typename UIntT>
OB_INLINE static int encode_delta_and_repeat_cnt(
    uint64_t &bw, uint32_t &br,
    UIntT *&pp, UIntT *&ip,
    char *out, uint64_t out_len, uint64_t &out_pos,
    UIntT &delta)
{
  int ret = OB_SUCCESS;
  const int64_t max_encoded_size = sizeof(uint64_t)/*max repeat cnt*/ + 2/*3+3+4 for repeat cnt*/ +
      sizeof(delta) + 1/*4+3 for delta*/ + 8/*safe reserve*/;
  uint64_t r = ip - pp; // repeat count, maybe 0
  char *op = out + out_pos;
  // 1. save repeat cnt
  if (OB_UNLIKELY(out_len - out_pos < max_encoded_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "buf not enough", K(ret), K(out_len), K(out_pos), K(delta), K(r), K(max_encoded_size));
  } else if (r > BASE_REPEAT_CNT) {
    r -= BASE_REPEAT_CNT;
    uint32_t b = ((gccbits(r) + 7) >> 3); // byte count(round up)
    ObBitUtils::put32(bw, br, 4+3+3, (b-1)<<(4+3)); // 3bit: byte count, 3 bit zero, 4bit: zero, flag
    ObBitUtils::put64(bw, br, b<<3, r, op); // save repeat count(byte align)
    ObBitUtils::e_slide(bw, br, op);
  } else {
    while (r--) {
      // r > 0 && <= BASE_REPEAT_CNT
      ObBitUtils::put32(bw, br, 1, 1);
      ObBitUtils::e_slide(bw, br, op);
    }
  }

  // 2. save delta
  if (OB_SUCC(ret)) {
    delta = ObZigZag::encode<UIntT>(delta);
    const uint32_t N2 = get_N2<UIntT>();
    const uint32_t N3 = get_N3<UIntT>();
    const uint32_t N4 = get_N4<UIntT>();
    if (0 == delta) {
      // ObBitUtils::put32(bw, br, 1, 1);
    } else if (delta < (1 << (N2 - 1))) {
      ObBitUtils::put32(bw, br, N2+2, (delta<<2)|2);
    } else if (delta < (1 << (N3 - 1))) {
      ObBitUtils::put32(bw, br, N3+3, (delta<<3)|4);
    } else if (delta < (1 << (N4 - 1))) {
      ObBitUtils::put32(bw, br, N4+4, (delta<<4)|8);
    } else {
      uint32_t b = ((gccbits(delta)+7)>>3); // byte count(round up)
      ObBitUtils::put32(bw, br, 4+3, (b-1)<<4);
      ObBitUtils::put<UIntT>(bw, br, b<<3, delta, op);
    }
    ObBitUtils::e_slide(bw, br, op);

    out_pos = op - out;
  }
  return ret;
}

template<typename UIntT>
/*OB_INLINE*/ static int encode(
    const char *in,
    uint64_t in_len,
    char *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos,
    UIntT start)
{
  // performance critical, do not check param(outer has already checked)
  int ret = OB_SUCCESS;
  UIntT *ip = (UIntT *)(in);
  UIntT pd = 0;
  UIntT *pp = (UIntT *)(in);
  UIntT *in_start = (UIntT *)(in);
  uint64_t cnt = (in_len / sizeof(UIntT));
  UIntT delta = 0;
  char *op = out + out_pos;
  char *out_end = out + out_buf_len;

  uint64_t bw = 0;
  uint32_t br = 0;

#define DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT \
  do { \
    if (OB_FAIL(encode_delta_and_repeat_cnt<UIntT>(bw, br, pp, ip, out, out_buf_len, out_pos, delta))) { \
      LIB_LOG(WARN, "fail to encode_delta_and_repeat_cnt", K(ret), K(out_buf_len), K(out_pos)); \
    } else { \
      pp = ++ip; /*next unencoding num*/ \
    } \
  } while (false); \
  continue;

  if (cnt > 4) {
    for (; OB_SUCC(ret) && ip < (in_start + (cnt-1-4)); ) {
      start = ip[0] - start;
      delta = start - pd;
      pd = start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT; }
      ip++;

      start = ip[0] - start;
      delta = start - pd;
      pd = start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT; }
      ip++;

      start = ip[0] - start;
      delta = start - pd;
      pd = start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT; }
      ip++;

      start = ip[0] - start;
      delta = start - pd;
      pd = start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT; }
      ip++;

      DO_PREFETCH(ip + 256);
      // check next batch for rle repeat cnt
    }
  }

  for (; OB_SUCC(ret) && ip < (in_start + cnt); ) {
    start = ip[0] - start;
    delta = start - pd;
    pd = start;
    start = ip[0];
    if (0 != delta) {
      DO_ENCODE_DOUBLE_DELTA_AND_REPEAT_CNT;
    };
    ip++;
  }

  if (OB_SUCC(ret)) {
    if (ip > pp) {
      if (OB_FAIL(encode_delta_and_repeat_cnt<UIntT>(bw, br, pp, ip, out, out_buf_len, out_pos, delta))) {
        LIB_LOG(WARN, "fail to encode_delta_and_repeat_cnt", K(ret), K(out_buf_len), K(out_pos));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObBitUtils::flush(bw, br, out, out_buf_len, out_pos))) {
      LIB_LOG(WARN, "fail to flush", K(ret), K(out_buf_len), K(out_pos));
    }
  }

  return ret;
}

template<typename UIntT>
OB_INLINE static int decode(
    const char *in,
    const uint64_t in_len,
    uint64_t &pos,
    const uint64_t uint_count,
    char *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos,
    UIntT start)
{
  typedef typename ObDeltaIntType<UIntT>::Type DeltaIntType;

  int ret = OB_SUCCESS;
  UIntT *op = (UIntT *)(out + out_pos);
  UIntT *orig_op = (UIntT *)(out + out_pos);
  UIntT pd = 0;
  const char *ip = in + pos;
  const char *ip_end = in + in_len;

  uint64_t bw = 0;
  uint32_t br = 0;
  const uint32_t N2 = get_N2<UIntT>();
  const uint32_t N3 = get_N3<UIntT>();
  const uint32_t N4 = get_N4<UIntT>();

  ObBitUtils::d_slide(bw, br, ip, ip_end);
  for(; (op < orig_op + uint_count) && OB_SUCC(ret); ) {
    DO_PREFETCH(ip+384);
    // just read, not slide
    DeltaIntType delta = (DeltaIntType)ObBitUtils::rs_bw(bw, br);

    if ((delta & 1) > 0) {
      ObBitUtils::r_mv(br, 0+1);
      delta = 0;
    } else if ((delta & 2) > 0) {
      ObBitUtils::r_mv(br, N2+2);
      delta = (DeltaIntType)ObBitUtils::bzhi32((uint32_t)(delta >> 2), N2);
    } else if ((delta & 4) > 0) {
      ObBitUtils::r_mv(br, N3+3);
      delta = (DeltaIntType)ObBitUtils::bzhi32((uint32_t)(delta >> 3), N3);
    } else if ((delta & 8) > 0) {
      ObBitUtils::r_mv(br, N4+4);
      delta = (DeltaIntType)ObBitUtils::bzhi32((uint32_t)(delta >> 4), N4);
    } else {
      uint32_t b = 0;
      UIntT *lop = nullptr;  // local op
      uint64_t r = 0; // repeat cnt

      // fetch 7 bit and slide
      ObBitUtils::get32(bw, br, 4 + 3, b);

      b >>= 4; // low 4bit has judge above, indicate short delta
               // check high 3bit
      if (b <= 1) {
        // b == 1, overflow
        // b == 0, repeat cnt
        if (b == 1) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "can not be overflow", K(ret), K(b), K(bw), K(br), K(out_pos));
        } else {
          ObBitUtils::get32(bw, br, 3, b);
          ObBitUtils::get<uint32_t>(bw, br, ((b+1)<<3), r, ip, ip_end); // rle repeat count
          ObBitUtils::d_slide(bw, br, ip, ip_end);
          bool can_vectorize = false;

#if (defined(__SSE2__)/* || defined(__ARM_NEON)*/)
          if (4 == sizeof(UIntT)) {
            can_vectorize = true;
            uint32_t tmp_start = (uint32_t)start;
            uint32_t tmp_pd = (uint32_t)pd;
            __m128i sv = _mm_set1_epi32(tmp_start);
            __m128i cv = _mm_set_epi32(4*tmp_pd, 3*tmp_pd, 2*tmp_pd, 1*tmp_pd);
            for (r += BASE_REPEAT_CNT, lop = op; op != lop+(r&~7);) {
              sv = _mm_add_epi32(sv, cv);
              _mm_storeu_si128((__m128i_u *)op, sv);
              sv = _mm_shuffle_epi32(sv, _MM_SHUFFLE(3, 3, 3, 3));
              op += 4;
              sv = _mm_add_epi32(sv, cv);
              _mm_storeu_si128((__m128i_u *)op, sv);
              sv = _mm_shuffle_epi32(sv, _MM_SHUFFLE(3, 3, 3, 3));
              op += 4;
            }
            start = (uint32_t)_mm_cvtsi128_si32(_mm_srli_si128(sv, 12));
          }
#endif
          if (false == can_vectorize)  {
            for (r+=BASE_REPEAT_CNT, lop = op; op != (lop + (r&~7)); op += 8) {
              op[0]=(start+=pd),
              op[1]=(start+=pd),
              op[2]=(start+=pd),
              op[3]=(start+=pd),
              op[4]=(start+=pd),
              op[5]=(start+=pd),
              op[6]=(start+=pd),
              op[7]=(start+=pd);
            }
          }
          for(; op != (lop + r); op++) {
            *op = (start += pd);
          }
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t tmp_delta = 0;
        // get delta
        ObBitUtils::get<UIntT>(bw, br, (b+1)<<3, tmp_delta, ip, ip_end);
        delta = (DeltaIntType)tmp_delta;
      }
    }
    if (OB_SUCC(ret)) {
      // get orig delta value
      pd += ObZigZag::decode<UIntT>(delta);
      *op++ = (start += pd); // set orig value
      ObBitUtils::d_slide(bw, br, ip, ip_end);
    }
  }

  if (OB_SUCC(ret)) {
    ObBitUtils::align(bw,br,ip);

    pos = ip - in;
    out_pos = ((char *)op) - out;
  }

  return ret;
}
};

// double delta + zigzag + rle
class ObDoubleDeltaZigzagRle: public ObCodec {
public:
  virtual uint32_t get_block_size() { return BlockSize; }
  enum
  {
    BlockSize = 1,
  };

  virtual const char *name() const override { return "ObDeltaZigzagRle"; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes())  {
      case 1 : {
        ret = ObDoubleDeltaZigzagRleInner::encode<uint8_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ret = ObDoubleDeltaZigzagRleInner::encode<uint16_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ret = ObDoubleDeltaZigzagRleInner::encode<uint32_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ret = ObDoubleDeltaZigzagRleInner::encode<uint64_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "fail to do_encode", K(ret), K(get_uint_bytes()), KPC(this), K(in_len), K(out_len), K(out_pos));
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
        ret = ObDoubleDeltaZigzagRleInner::decode<uint8_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ret = ObDoubleDeltaZigzagRleInner::decode<uint16_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ret = ObDoubleDeltaZigzagRleInner::decode<uint32_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ret = ObDoubleDeltaZigzagRleInner::decode<uint64_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }

    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "fail to do_decode", K(ret), K(get_uint_bytes()), KPC(this), K(in_len), K(in_pos), K(out_len), K(out_pos));
    }
    return ret;
  }
};

} // namespace common
} // namespace oceanbase

#endif /*  OB_DOUBLE_DELTA_ZIGZAG_RLE_H_ */
