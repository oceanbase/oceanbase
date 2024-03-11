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

#ifndef OB_SIMD_FIXED_FAST_PFOR_H_
#define OB_SIMD_FIXED_FAST_PFOR_H_

#include "ob_codecs.h"
#include "ob_bp_util.h"
#include "ob_generated_unalign_simd_bp_func.h"
#include "common/ob_target_specific.h"

namespace oceanbase
{
namespace common
{

// fixed means exceptions are fixed stored in each block
class ObSIMDFixedPFor : public ObCodec {
public:
  ObSIMDFixedPFor() {}

  enum {
    BlockSize = 128,
  };
  virtual uint32_t get_block_size() { return BlockSize; }

  template<typename UIntT>
  static OB_INLINE void find_most_fit_bx(
      const UIntT *__restrict in,
      uint32_t n,
      uint32_t &_b,
      uint32_t &_bx)
  {
    // 0 ~ sizeof(UIntT) * 8 bit, total (sizeof(UIntT) * 8 + 1);
    const uint32_t cnt_size = sizeof(UIntT) * 8 + 1;
    uint32_t cnt[cnt_size] = {0};
    uint32_t x = 0;
    uint32_t bx = 0;
    uint32_t bmp8 = pad8(n); // bitmap padding to byte align
    const UIntT *ip = nullptr;
    UIntT u = 0;
    UIntT first = in[0];
    int32_t b = 0;
    int32_t ml = 0;
    int32_t eq_cnt = 0; // equal count compared to first value, used for VByte

    // calculate the ele count of each bit width
    for(ip = in; ip != in+(n&~3); ip+=4) {
      ++cnt[gccbits(ip[0])]; u |= ip[0]; eq_cnt += (ip[0] == first);
      ++cnt[gccbits(ip[1])]; u |= ip[1]; eq_cnt += (ip[1] == first);
      ++cnt[gccbits(ip[2])]; u |= ip[2]; eq_cnt += (ip[2] == first);
      ++cnt[gccbits(ip[3])]; u |= ip[3]; eq_cnt += (ip[3] == first);
    }
    for(; ip != in+n; ip++) {
      ++cnt[gccbits(ip[0])]; u |= ip[0]; eq_cnt += (ip[0] == first);
    }

    b = gccbits(u); // max bit width
    bx = b;
    ml = pad8(n*b) + 1; // max_len when no exceptions, 1 is one uint8_t to store bit width

    // first assume exception bitwidth is max bit width,
    // in this case, just no exception and no bitmap, direct bitpacking,
    // so it's len is equal to ml above
    x = cnt[b];

    int32_t l = 0;
    int32_t i = (b - 1);
    for (; i >= 0; --i) {
      // 2: one byte to store b, one byte to store bx;
      // bmp8: exception bitmap;
      // pad8(x*(bx-i)): exception bitpacking len
      // pad8(n*i): data bitpacking len
      l = (2 + bmp8 + pad8(x * (bx - i)) + pad8(n * i));
      x += cnt[i]; // x is total cnt, which bitwith > i
      bool fi = (l < ml);
      ml = (fi ? l : ml); // choose min size
      b = (fi ? i : b);
    }
    _bx = bx - b; // exception bit width
    _b = b; // bit width

    //LIB_LOG(INFO, "pick b and bx", K(_bx), K(_b), K(sizeof(UIntT))); // TODO, oushen, remove later
  }

  template<typename UIntT>
  static OB_INLINE void do_miss(
      uint64_t *miss,
      uint64_t &i,
      const UIntT *__restrict in,
      UIntT msk,
      UIntT *_in,
      uint64_t &xn)
  {
    miss[xn] = i;
    xn += (in[i] > msk);
    _in[i] = in[i] & msk;
    i++;
  }

  template<typename UIntT>
  static OB_INLINE void inner_do_encode(
      const UIntT *__restrict in,
      uint32_t n,
      char *out,
      const uint32_t b,
      const uint32_t bx,
      const uint64_t out_buf_len,
      uint32_t &len) {
    // do not check param
    UIntT msk = (UIntT)((1ull << b) - 1);
    UIntT _in[BlockSize] = {0}; // value list(after mask)
    UIntT inx[BlockSize] = {0}; // exception list
    uint64_t xmap[BlockSize / 64] = {0}; // exception bitmap
    uint64_t miss[BlockSize] = {0}; // excepiont idx
    char *orig_out = out;

    if (bx == 0) { // no exception
      inner_bit_packing<UIntT>(in, out, b, out_buf_len);
    } else {
      uint64_t i = 0;
      uint64_t xn = 0;// the count of exceptions
      for(; i != (n&~3); ) { // loop expansion
        do_miss<UIntT>(miss, i, in, msk, _in, xn);
        do_miss<UIntT>(miss, i, in, msk, _in, xn);
        do_miss<UIntT>(miss, i, in, msk, _in, xn);
        do_miss<UIntT>(miss, i, in, msk, _in, xn);
      }
      while(i != n) { // loop remain
        do_miss<UIntT>(miss, i, in, msk, _in, xn);
      }

      uint32_t c = 0;
      // build up xbitmap
      for (i = 0; i != xn; ++i) {
        c = (uint32_t)miss[i];
        // c>>6 is move to next uint64(64 bit)
        xmap[c>>6] |= (1ull << (c&0x3f)); // set bitmap
        inx[i] = (in[c] >> b); // store exception
      }

      // store xbitmap
      for(i = 0; i < (n+63)/64; i++) {
        *reinterpret_cast<uint64_t *>(out+i*8) = xmap[i];
      }
      out += pad8(n); // align to byte

      // packing excpetion
      {
        uint64_t out_pos = 0;
        scalar_bit_packing<UIntT>(inx, xn, bx, out, out_buf_len, out_pos);
        out += out_pos;
      }

      // packing data
      inner_bit_packing<UIntT>(_in, out, b, out_buf_len);
    }
    len = (uint32_t)(out - orig_out);
  }

  template<typename UIntT>
  static OB_INLINE int store_bx_and_b(char *&out, uint32_t &b, uint32_t &bx)
  {
    int ret = OB_SUCCESS;
    uint32_t usize = sizeof(UIntT) * 8;
    if (bx == 0) { // no exception
      *out++ = b;
    } else if (bx <= usize) {
      // bit packing exception
      *out++ = 0x80|b;
      *out++ = bx;
    } else {
      // VByte exception
      //*out++ = (((bx == (usize + 1)) ? 0x40 : 0xc0) | b);
      // current can not run here
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "unexpected bx", K(ret), K(bx), K(b), KP(out));
    }
    return ret;
  }

  template<typename UIntT>
  static OB_INLINE int __encode_array(const UIntT *in, const uint32_t length,
                                      char *out, uint64_t out_buf_len, uint64_t &out_pos)
  {
    int ret = OB_SUCCESS;
    const int64_t max_encoded_size = 2/*store_bx_and_b*/ + length * sizeof(UIntT) + 8/*safe reserve*/;
    if (OB_UNLIKELY(out_buf_len - out_pos < max_encoded_size)) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf not enough", K(ret), K(length), K(out_buf_len), K(out_pos), K(max_encoded_size));
    } else {
      uint32_t b = 0;
      uint32_t bx = 0;
      char *orig_out = out;
      out += out_pos;
      find_most_fit_bx<UIntT>(in, length, b, bx);
      if (OB_FAIL(store_bx_and_b<UIntT>(out, b, bx))) {
        LIB_LOG(WARN, "fail to store bx", K(ret), K(b), K(bx), K(length), K(out_buf_len), K(out_pos));
      } else {
        uint32_t len = 0;
        uint64_t remain_out_buf_len = out_buf_len - (out - orig_out);
        inner_do_encode(in, length, out, b, bx, remain_out_buf_len, len);
        out += len;
        out_pos = out - orig_out;
      }
    }
    return ret;
  }

  template<typename UIntT>
  /*OB_INLINE*/ int inner_encode_array(const char *in, const uint64_t in_len,
                          char *out, uint64_t out_len, uint64_t &out_pos)
  {
    int ret = OB_SUCCESS;
    uint64_t t_in_len = in_len / sizeof(UIntT);
    const UIntT *t_in = reinterpret_cast<const UIntT *>(in);
    for (int64_t i = 0; OB_SUCC(ret) && i < t_in_len; i += BlockSize) {
      const UIntT *next_in = t_in + i;
      if (OB_FAIL(__encode_array<UIntT>(next_in, BlockSize, out, out_len, out_pos))) {
        LIB_LOG(WARN, "fail to encode array", K(ret), K(BlockSize), K(out_len), K(out_pos));
      }
    }

    return ret;
  }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes())  {
      case 1 : {
        ret =inner_encode_array<uint8_t>(in, in_len, out, out_len, out_pos);
        break;
      }
      case 2 : {
        ret = inner_encode_array<uint16_t>(in, in_len, out, out_len, out_pos);
        break;
      }
      case 4 : {
        ret = inner_encode_array<uint32_t>(in, in_len, out, out_len, out_pos);
        break;
      }
      case 8 : {
        ret = inner_encode_array<uint64_t>(in, in_len, out, out_len, out_pos);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      if (OB_FAIL(ret)) {
        LIB_LOG(WARN, "fail to do_encode", K(ret), KPC(this), K(get_uint_bytes()));
      }
    }
    return ret;
  }

  template<typename UIntT>
  static OB_INLINE void inner_do_decode(
      const char *_in,
      const uint64_t length,
      uint64_t &pos,
      char *_out,
      const uint64_t out_buf_len,
      uint64_t &out_pos)
  {
    const char *in = _in + pos;
    char *out = _out + out_pos;
    uint32_t b = 0;
    uint32_t bx = 0;

    b = *in++; // bit width
    if (0 == (b & 0x80)) {  // no exception value, direct unpack
      inner_bit_unpacking<UIntT>(in, _in, length, out, b);
    } else {
      b &= (0x80 - 1); // get normal bit width
      bx = *in++; // get exception bit width

      UIntT ex[BlockSize] = {0};
      uint64_t xbitmap1 = *(uint64_t *)(in);
      uint64_t xbitmap2 = *(uint64_t *)(in + 8);
      uint32_t x_cnt = popcnt64(xbitmap1) + popcnt64(xbitmap2); // exception count
      //LIB_LOG(INFO, "xbitmap", K(x_cnt), K(xbitmap1), K(xbitmap2), K(b), K(bx)); // TODO, oushen, remove later
      in += 2 * sizeof(uint64_t); // skip xbitmap

      // unpacking exception
      {
        uint64_t in_pos = in - _in;
        uint64_t in_len = length;
        uint64_t tmp_out_pos = 0;
        uint64_t tmp_out_buf_len = BlockSize;
        scalar_bit_unpacking<UIntT>(_in, in_len, in_pos, x_cnt, bx, ex, tmp_out_buf_len, tmp_out_pos);
        in = _in + in_pos;
      }

      // unpacking data
      inner_bit_unpacking<UIntT>(in, _in, length, out, b);
      UIntT *out_arr = reinterpret_cast<UIntT *>(_out + out_pos);

      // patch exception, TODO, oushen, optimize later
      int64_t ex_idx = 0;
      for (int64_t i = 0; i < 64; i++) {
        if ((xbitmap1 & (1LU << i)) > 0) {
          out_arr[i] |= (ex[ex_idx] << b);
          ex_idx++;
        }
      }
      for (int64_t i = 0; i < 64; i++) {
        if ((xbitmap2 & (1LU << i)) > 0) {
          out_arr[i + 64] |= (ex[ex_idx] << b);
          ex_idx++;
        }
      }
    }

    pos = in - _in;
    out_pos = out - _out;
  }

  template<typename UIntT>
  static OB_INLINE void __decode_array(
      const char *in,
      const uint64_t length,
      uint64_t &pos,
      const uint64_t uint_count,
      char *out,
      const uint64_t out_buf_len,
      uint64_t &out_pos)
  {
    for (int64_t i = 0; i < uint_count; i += BlockSize) {
      inner_do_decode<UIntT>(in, length, pos, out, out_buf_len, out_pos);
    }
  }

   virtual int do_decode(
       const char *in,
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
        __decode_array<uint8_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos);
        break;
      }
      case 2 : {
        __decode_array<uint16_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos);
        break;
      }
      case 4 : {
        __decode_array<uint32_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos);
        break;
      }
      case 8 : {
        __decode_array<uint64_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos);
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        LIB_LOG(WARN, "not support", K(ret), KPC(this), K(get_uint_bytes()));
        break;
      }
    }
    return ret;
  }

  virtual const char * name() const override { return "ObSIMDFixedPFor128"; }
private:
  template<typename UIntT>
  static OB_INLINE void inner_bit_packing(
      const UIntT *__restrict in,
      char *&out,
      const uint32_t bit,
      const uint64_t out_buf_len)
  {
    if (4 == sizeof(UIntT) && common::is_arch_supported(ObTargetArch::AVX2)) {
      // uint32_t simd packing
      uSIMD_fastpackwithoutmask_128_32((uint32_t *)in, reinterpret_cast<__m128i *>(out), bit);
      out += BlockSize * bit / 8;
    } else if (2 == sizeof(UIntT) && common::is_arch_supported(ObTargetArch::AVX2)) {
      // uint16_t simd packing
      uSIMD_fastpackwithoutmask_128_16((const uint16_t *)in, reinterpret_cast<__m128i *>(out), bit);
      out += BlockSize * bit / 8;
    } else {
      // uint64_t, uint8_t use scalar packing
      uint64_t out_pos = 0;
      scalar_bit_packing<UIntT>(in, BlockSize, bit, out, out_buf_len, out_pos);
      out += out_pos;
    }
  }

  template<typename UIntT>
  static OB_INLINE void inner_bit_unpacking(
      const char *&in,
      const char *_in,
      const uint64_t length,
      char *&out,
      const uint32_t bit)
  {
    if (4 == sizeof(UIntT) && common::is_arch_supported(ObTargetArch::AVX2)) {
      uSIMD_fastunpack_128_32(reinterpret_cast<const __m128i *>(in), reinterpret_cast<uint32_t *>(out), bit);
      in += BlockSize * bit / 8; // convert to byte;
    } else if (2 == sizeof(UIntT) && common::is_arch_supported(ObTargetArch::AVX2)) {
      uSIMD_fastunpack_128_16(reinterpret_cast<const __m128i *>(in), reinterpret_cast<uint16_t *>(out), bit);
      in += BlockSize * bit / 8;
    } else {
      // uint8 & uint64 does not support simd packing
      uint64_t in_pos = in - _in;
      uint64_t in_len = length;
      uint64_t tmp_out_pos = 0;
      uint64_t tmp_out_buf_len = BlockSize;
      UIntT *tmp_out = (UIntT *)out;
      scalar_bit_unpacking<UIntT>(_in, in_len, in_pos, BlockSize, bit,
          tmp_out, tmp_out_buf_len, tmp_out_pos);
      in = _in + in_pos;
    }
    out += BlockSize * sizeof(UIntT);
  }
};

} // namespace common
} // namespace oceanbase

#endif /* OB_SIMD_FIXED_FAST_PFOR_H_ */
