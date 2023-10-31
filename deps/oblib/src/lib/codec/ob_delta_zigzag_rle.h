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

#ifndef OB_DELTA_ZIGZAG_RLE_H_
#define OB_DELTA_ZIGZAG_RLE_H_

#include "ob_codecs.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_sse_to_neon.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

class ObDeltaZigzagRleInner
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
    // byte count(round up), how many bytes needed to store repeat cnt(r)
    // max_r is UINT64_AMX, so gccbit(max_r) = 64, so b max is 8.
    uint32_t b = ((gccbits(r) + 7) >> 3);
    // 3bit: byte count, 3 bit zero, 4bit: zero, flag
    // b-1 max is 7, need 3 bit to store
    // |----r(repeat_cnt)----|---3bit(byte count)---|---3 bit zero---|----4bit flag(all zero)----|
    //  3bit(byte count) store repeat value's bytes
    //  3bit zero to differentiate delta value below
    //  4bit flag(all zero) also to differentiate delta value below
    // save to bw
    ObBitUtils::put32(bw, br, 4+3+3, (b-1)<<(4+3));
    // save repeat count(byte align) to op
    // b<<3 convert to bit count, which can store r
    ObBitUtils::put64(bw, br, b<<3, r, op);
    // slide forward
    ObBitUtils::e_slide(bw, br, op);
  } else {
    // if 0 == r, no repeat cnt, only delta
    while (r--) {
      // r > 0 && <= BASE_REPEAT_CNT
      // |----all 1 (1~18)-----|
      // one bit represent one ele
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
      // delta == 0 must be means encoding completed, so no need next delta.
      // because when decode whill not read last delta(0) any more.
      // ObBitUtils::put32(bw, br, 1, 1); // 0b1
    } else if (delta < (1 << (N2 - 1))) {
      ObBitUtils::put32(bw, br, N2+2, (delta<<2)|2); // |----delta---|0b10|
    } else if (delta < (1 << (N3 - 1))) {
      ObBitUtils::put32(bw, br, N3+3, (delta<<3)|4); // |----delta---|0b100|
    } else if (delta < (1 << (N4 - 1))) {
      ObBitUtils::put32(bw, br, N4+4, (delta<<4)|8); // |----delta---|0b1000|
    } else {
      // byte count(round up)
      // min_N4 is 17,
      // min_b = (17 + 7) >> 3 = 3
      // min_b - 1 = 2,
      // b=0 or b=1, is no use by delta
      // b=0 means next is repeat cnt
      // b=1 means next is overflow(not used current)
      uint32_t b = ((gccbits(delta)+7)>>3);

      // |----delta---|---3bit(delta byte count)---|----0b0000---|
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
  int ret = OB_SUCCESS;
  // performance critical, do not check param(outer has already checked)
  UIntT *ip = (UIntT *)(in);
  UIntT *pp = (UIntT *)(in);
  UIntT *in_start = (UIntT *)(in);
  uint64_t cnt = (in_len / sizeof(UIntT));
  UIntT delta = 0;

  uint64_t bw = 0; // current uint64 to store bits
  uint32_t br = 0; // bit count already used, should shift left

#define DO_ENCODE_DELTA_AND_REPEAT_CNT \
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
      delta = ip[0] - start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DELTA_AND_REPEAT_CNT; }
      ip++;

      delta = ip[0] - start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DELTA_AND_REPEAT_CNT; }
      ip++;

      delta = ip[0] - start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DELTA_AND_REPEAT_CNT; }
      ip++;

      delta = ip[0] - start;
      start = ip[0];
      if (0 != delta) { DO_ENCODE_DELTA_AND_REPEAT_CNT; }
      ip++;

      DO_PREFETCH(ip + 256);
      // check next batch for rle repeat cnt
    }
  }

  for (; OB_SUCC(ret) && ip < (in_start + cnt); ) {
    delta = ip[0] - start;
    start = ip[0];
    if (0 != delta) {
      DO_ENCODE_DELTA_AND_REPEAT_CNT;
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
      // Two meanings:
      // 1. one repeat ele, delta must be 0
      // 2. end of en/decoding, also delta must be 0
      ObBitUtils::r_mv(br, 0+1); // just move forward
      delta = 0;
    } else if ((delta & 2) > 0) {
      ObBitUtils::r_mv(br, N2+2);
      delta = ObBitUtils::bzhi32((uint32_t)(delta >> 2), N2);
    } else if ((delta & 4) > 0) {
      ObBitUtils::r_mv(br, N3+3);
      delta = ObBitUtils::bzhi32((uint32_t)(delta >> 3), N3);
    } else if ((delta & 8) > 0) {
      ObBitUtils::r_mv(br, N4+4);
      delta = ObBitUtils::bzhi32((uint32_t)(delta >> 4), N4);
    } else {
      uint32_t b = 0;
      UIntT *lop = nullptr;  // local op
      uint64_t r = 0; // repeat cnt

      // fetch 7 bit and slide
      ObBitUtils::get32(bw, br, 4 + 3, b);

      b >>= 4; // low 4bit is all 0(has judge above), indicate short delta.
               // check high 3bit
      if (b <= 1) {
        // b == 1, overflow
        // b == 0, repeat cnt
        if (b == 1) {
          // can not be overflow
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(WARN, "can not be overflow", K(ret), K(b), K(bw), K(br), K(out_pos));
        } else {
          ObBitUtils::get32(bw, br, 3, b);
          ObBitUtils::get<uint32_t>(bw, br, ((b+1)<<3), r, ip, ip_end); // rle repeat count
          ObBitUtils::d_slide(bw, br, ip, ip_end);
          bool can_vectorize = false;
#if (defined(__SSE2__) || defined(__ARM_NEON))
          if (4 == sizeof(UIntT)) {
            can_vectorize = true;
            uint32_t tmp_start = start;
            __m128i sv = _mm_set1_epi32(tmp_start);
            for (r += BASE_REPEAT_CNT, lop = op; op != lop+(r&~7);) {
              _mm_storeu_si128((__m128i *)op, sv); op += 4;
              _mm_storeu_si128((__m128i *)op, sv); op += 4;
            }
          } else if (2 == sizeof(UIntT)) {
            can_vectorize = true;
            uint16_t tmp_start = start;
            __m128i sv = _mm_set1_epi16(tmp_start);
            for (r += BASE_REPEAT_CNT, lop = op; op != lop+(r&~7);) {
              _mm_storeu_si128((__m128i *)op, sv); op += 8;
            }
          }
#endif
          if (false == can_vectorize)  {
            for (r+=BASE_REPEAT_CNT, lop = op; op != (lop + (r&~7)); op += 8) {
              op[0]=op[1]=op[2]=op[3]=op[4]=op[5]=op[6]=op[7]=start;
            }
          }
          for(; op != (lop + r); op++) {
            *op = start;
          }
          // current round rle decode complete,
          // continue to decode delta for next round rle
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t tmp_delta = 0;
        // get delta, delta bits > N4
        ObBitUtils::get<UIntT>(bw, br, (b+1)<<3, tmp_delta, ip, ip_end);
        delta = (DeltaIntType)tmp_delta;
      }
    }
    if (OB_SUCC(ret)) {
      // get orig delta value
      delta = ObZigZag::decode<UIntT>(delta);
      *op++ = (start += delta); // set orig value
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

// delta + zigzag + rle
class ObDeltaZigzagRle: public ObCodec {
public:
  enum
  {
    BlockSize = 1,
  };
  virtual uint32_t get_block_size() override { return BlockSize; }

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
        ret = ObDeltaZigzagRleInner::encode<uint8_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ret = ObDeltaZigzagRleInner::encode<uint16_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ret = ObDeltaZigzagRleInner::encode<uint32_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ret = ObDeltaZigzagRleInner::encode<uint64_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "fail to do encode", K(ret), K(get_uint_bytes()), KPC(this), K(in_len), K(out_len), K(out_pos));
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
        ret = ObDeltaZigzagRleInner::decode<uint8_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ret = ObDeltaZigzagRleInner::decode<uint16_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ret = ObDeltaZigzagRleInner::decode<uint32_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ret = ObDeltaZigzagRleInner::decode<uint64_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "fail to decode", K(ret), K(get_uint_bytes()), KPC(this), K(in_len), K(in_pos), K(out_len), K(out_pos));
    }
    return ret;
  }
};

} // namespace common
} // namespace oceanbase

#endif /* OB_DELTA_ZIGZAG_RLE_H_ */
