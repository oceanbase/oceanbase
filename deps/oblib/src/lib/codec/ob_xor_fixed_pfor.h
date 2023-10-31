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

#ifndef OB_XOF_FIEXED_PFOR_H_
#define OB_XOF_FIEXED_PFOR_H_

#include "ob_codecs.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/codec/ob_simd_fixed_pfor.h"

namespace oceanbase
{
namespace common
{

class ObXorFixedPforInner
{
public:
static const uint64_t BSIZE = 128;

template<typename UIntT>
OB_INLINE static void do_encode_one_batch(const uint32_t idx, UIntT &start, const UIntT *ip, UIntT *v, UIntT &b)
{
  UIntT cur = ip[idx];
  v[idx] = ObXor::encode<UIntT>(cur, start);
  b |= v[idx];
  start = cur;
}

template<typename UIntT>
/*OB_INLINE*/ static int encode(
    const char *in, uint64_t in_len,
    char *out, const uint64_t out_buf_len, uint64_t &out_pos,
    UIntT start)
{
  // performance critical, do not check param(outer has already checked)
  int ret = OB_SUCCESS;
  const UIntT *ip = reinterpret_cast<const UIntT *>(in);
  UIntT v[BSIZE];
  UIntT *tv = nullptr;
  const UIntT *in_start = reinterpret_cast<const UIntT *>(in);
  uint64_t cnt = (in_len / sizeof(UIntT));

  for (; OB_SUCC(ret) && (ip != in_start + (cnt&~(BSIZE-1))); ) {
    UIntT b = 0;
    for (tv = v; tv != &v[BSIZE]; tv+=4,ip+=4) {
      do_encode_one_batch(0, start, ip, tv, b);
      do_encode_one_batch(1, start, ip, tv, b);
      do_encode_one_batch(2, start, ip, tv, b);
      do_encode_one_batch(3, start, ip, tv, b);
    }

    b = sizeof(UIntT) * 8 - gccbits(b);

    for(tv = v; tv != &v[BSIZE]; tv += 4) {
      tv[0] = bit_reverse<UIntT>(tv[0] << b);
      tv[1] = bit_reverse<UIntT>(tv[1] << b);
      tv[2] = bit_reverse<UIntT>(tv[2] << b);
      tv[3] = bit_reverse<UIntT>(tv[3] << b);
    }

    if (OB_UNLIKELY(out_buf_len - out_pos) < 1) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf not enough", K(ret), K(out_buf_len), K(out_pos));
    } else {
      *(out + out_pos) = b;
      out_pos++;
      if (OB_FAIL(ObSIMDFixedPFor::__encode_array<UIntT>(v, BSIZE, out, out_buf_len, out_pos))) {
        LIB_LOG(WARN, "fail to encode array", K(ret), K(out_buf_len), K(out_pos));
      } else {
        DO_PREFETCH(ip + 512);
      }
    }
  }

  if (OB_SUCC(ret)) {
    uint32_t remain_cnt = cnt % BSIZE;
    if (remain_cnt > 0) {
      UIntT b = 0;
      for (tv = v; tv != &v[remain_cnt]; tv++,ip++) {
        do_encode_one_batch(0, start, ip, tv, b);
      }
      b = sizeof(UIntT) * 8 - gccbits(b);

      for (tv = v; tv != &v[remain_cnt]; tv++) {
        tv[0] = bit_reverse<UIntT>(tv[0] << b);
      }

      if (OB_UNLIKELY(out_buf_len - out_pos) < 1) {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "buf not enough", K(ret), K(out_buf_len), K(out_pos));
      } else {
        *(out + out_pos) = b;
        out_pos++;
        // just use simple bit packing
        const char *tmp_in = reinterpret_cast<const char *>(v);
        uint64_t tmp_in_len = remain_cnt * sizeof(UIntT);
        if (OB_FAIL(ObSimpleBitPacking::_encode_array<UIntT>(tmp_in, tmp_in_len, out, out_buf_len, out_pos, 0))) {
          LIB_LOG(WARN, "fail to encode array", K(ret), K(tmp_in_len), K(out_buf_len), K(out_pos));
        }
      }
    }
  }

  return ret;
}

template<typename UIntT>
OB_INLINE static void do_decode_one_batch(const uint32_t idx, UIntT &start, UIntT *op, UIntT *v, const UIntT b)
{
  UIntT cur = v[idx];
  cur = bit_reverse<UIntT>(cur);
  cur >>= b;
  cur = ObXor::decode<UIntT>(cur, start);
  op[idx] = cur;
  start = cur;
}

template<typename UIntT>
/*OB_INLINE*/ static void decode(
    const char *in,
    const uint64_t in_len,
    uint64_t &pos,
    const uint64_t uint_count,
    char *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos,
    UIntT start)
{
  UIntT *op = reinterpret_cast<UIntT *>(out + out_pos);
  UIntT *t_out = reinterpret_cast<UIntT *>(out + out_pos);
  char *orig_out = out;
  UIntT v[BSIZE];
  UIntT *tv = nullptr;
  char *tmp_out = reinterpret_cast<char *>(v);
  uint64_t tmp_out_len = BSIZE * sizeof(UIntT);
  uint64_t tmp_out_pos = 0;

  for (tv = v; op != t_out+(uint_count & ~(BSIZE - 1)); ) {
    DO_PREFETCH(in + pos + 512);
    uint8_t b = *(in + pos);
    pos++;
    tmp_out_pos = 0;
    ObSIMDFixedPFor::__decode_array<UIntT>(in, in_len, pos, BSIZE, tmp_out, tmp_out_len, tmp_out_pos);

    for(tv = v; tv != &v[BSIZE]; tv+=4,op+=4) {
      do_decode_one_batch<UIntT>(0, start, op, tv, b);
      do_decode_one_batch<UIntT>(1, start, op, tv, b);
      do_decode_one_batch<UIntT>(2, start, op, tv, b);
      do_decode_one_batch<UIntT>(3, start, op, tv, b);
    }
  }

  int64_t remain_cnt = uint_count % BSIZE;
  if (remain_cnt > 0) {
    UIntT b = *(in + pos);
    pos++;

    tmp_out_pos = 0;
    ObSimpleBitPacking::_decode_array<UIntT>(in, in_len, pos, remain_cnt, tmp_out, tmp_out_len, tmp_out_pos, 0);

    for (tv = v; tv < &v[remain_cnt]; tv++,op++) {
      do_decode_one_batch<UIntT>(0, start, op, tv, b);
    }
  }
  out_pos = ((char *)(op) - orig_out);
}
};

// xor + left shift + bit_reserve + fixedpfor
class ObXorFixedPfor: public ObCodec
{
public:
  enum
  {
    BlockSize = 1,
  };
  virtual uint32_t get_block_size() { return BlockSize; }

  virtual const char *name() const override { return "ObXorFixedPfor"; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes())  {
      case 1 : {
        ret = ObXorFixedPforInner::encode<uint8_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ret = ObXorFixedPforInner::encode<uint16_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ret = ObXorFixedPforInner::encode<uint32_t>(in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ret = ObXorFixedPforInner::encode<uint64_t>(in, in_len, out, out_len, out_pos, 0);
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
        ObXorFixedPforInner::decode<uint8_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        ObXorFixedPforInner::decode<uint16_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        ObXorFixedPforInner::decode<uint32_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        ObXorFixedPforInner::decode<uint64_t>(in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
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
};

} // namespace common
} // namespace oceanbase

#endif /*  OB_XOF_FIEXED_PFOR_H_ */
