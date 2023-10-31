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

#ifndef OB_DELTA_ZIGZAG_PFOR_H_
#define OB_DELTA_ZIGZAG_PFOR_H_

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

class ObDeltaZigzagFixedPfor
{
public:
const static uint32_t BSIZE = 128; // one block size

template<typename UIntT>
OB_INLINE static void encode_one(const UIntT *ip, const uint32_t idx, UIntT &start, UIntT *p)
{
  UIntT u = ip[idx];
  UIntT delta = u - start;
  p[idx] = ObZigZag::encode(delta);
  start = u;
}

template<typename UIntT>
/*OB_INLINE*/ static int encode(
    UIntT *&lp,
    const char *in,
    uint64_t in_len,
    char *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos,
    UIntT start)
{
  int ret = OB_SUCCESS;
  const UIntT *ip = reinterpret_cast<const UIntT *>(in);
  const UIntT *t_in = reinterpret_cast<const UIntT *>(in);
  UIntT *p = nullptr;
  uint64_t cnt = in_len / sizeof(UIntT);

  for (; OB_SUCC(ret) && ip != t_in + (cnt & ~(BSIZE - 1)); ) {
    for (p = lp; p != (lp + BSIZE); p+=4,ip+=4) {
      encode_one(ip, 0, start, p);
      encode_one(ip, 1, start, p);
      encode_one(ip, 2, start, p);
      encode_one(ip, 3, start, p);
    }

    if (OB_FAIL(ObSIMDFixedPFor::__encode_array<UIntT>(lp, BSIZE, out, out_buf_len, out_pos))) {
      LIB_LOG(WARN, "fail to encode array", K(ret), K(out_buf_len), K(out_pos));
    } else {
      DO_PREFETCH(ip + 512);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t remain_cnt = cnt % BSIZE;
    if (remain_cnt > 0) {
      for (p = lp; p != &lp[remain_cnt]; p++,ip++) {
        encode_one(ip, 0, start, p);
      }
      // just use simple bit packing
      const char *tmp_in = reinterpret_cast<const char *>(lp);
      uint64_t tmp_in_len = remain_cnt * sizeof(UIntT);
      if (OB_FAIL(ObSimpleBitPacking::_encode_array<UIntT>(tmp_in, tmp_in_len, out, out_buf_len, out_pos, 0))) {
        LIB_LOG(WARN, "fail to encode array", K(ret), K(remain_cnt), K(tmp_in_len), K(out_buf_len), K(out_pos));
      }
    }
  }

  return ret;
}

template<typename UIntT>
OB_INLINE static void deocde_one(UIntT *op, const uint32_t idx, UIntT &start, UIntT *p)
{
  UIntT u = ObZigZag::decode(p[idx]) + start;
  op[idx] = u;
  start = u;
}

template<typename UIntT>
OB_INLINE static void decode(
    UIntT *&lp,
    const char *in,
    const uint64_t in_len,
    uint64_t &pos,
    const uint64_t uint_count,
    char *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos,
    UIntT start)
{
  UIntT *p = nullptr;
  UIntT *op = reinterpret_cast<UIntT *>(out + out_pos);
  UIntT *t_out = reinterpret_cast<UIntT *>(out + out_pos);
  char *orig_out = out;

  char *tmp_out = reinterpret_cast<char *>(lp);
  uint64_t tmp_out_len = BSIZE * sizeof(UIntT);
  uint64_t tmp_out_pos = 0;

  for (; op != t_out + (uint_count & (~(BSIZE - 1))); ) {
    DO_PREFETCH(in + pos + 512);

    tmp_out_pos = 0;
    ObSIMDFixedPFor::__decode_array<UIntT>(in, in_len, pos, BSIZE, tmp_out, tmp_out_len, tmp_out_pos);
    p = lp;

    for(; p != (lp + BSIZE); p+=4,op+=4) {
      deocde_one(op, 0, start, p);
      deocde_one(op, 1, start, p);
      deocde_one(op, 2, start, p);
      deocde_one(op, 3, start, p);
    }
  }

  int64_t remain_cnt = uint_count % BSIZE;
  if (remain_cnt > 0) {
    tmp_out_pos = 0;
    ObSimpleBitPacking::_decode_array<UIntT>(in, in_len, pos, remain_cnt, tmp_out, tmp_out_len, tmp_out_pos, 0);
    p = lp;
    for(; p != &lp[remain_cnt]; p++,op++) {
      deocde_one(op, 0, start, p);
    }
  }
  out_pos = ((char *)(op) - orig_out);
}
};

// delta + zigzag + fixedpfor
class ObDeltaZigzagPFor : public ObCodec {
public:
  enum
  {
    BlockSize = 1,
  };
  virtual uint32_t get_block_size() { return BlockSize; }

  virtual const char *name() const override { return "ObDeltaZigzagPFor"; }

  virtual int do_encode(const char *in,
                        const uint64_t in_len,
                        char *out,
                        const uint64_t out_len,
                        uint64_t &out_pos) override
  {
    int ret = OB_SUCCESS;
    switch (get_uint_bytes()) {
      case 1 : {
        uint8_t *t_lp = reinterpret_cast<uint8_t *>(lp);
        ret = ObDeltaZigzagFixedPfor::encode<uint8_t>(t_lp, in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        uint16_t *t_lp = reinterpret_cast<uint16_t *>(lp);
        ret = ObDeltaZigzagFixedPfor::encode<uint16_t>(t_lp, in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        uint32_t *t_lp = reinterpret_cast<uint32_t *>(lp);
        ret = ObDeltaZigzagFixedPfor::encode<uint32_t>(t_lp, in, in_len, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        uint64_t *t_lp = reinterpret_cast<uint64_t *>(lp);
        ret = ObDeltaZigzagFixedPfor::encode<uint64_t>(t_lp, in, in_len, out, out_len, out_pos, 0);
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
        uint8_t *t_lp = reinterpret_cast<uint8_t *>(lp);
        ObDeltaZigzagFixedPfor::decode<uint8_t>(t_lp, in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 2 : {
        uint16_t *t_lp = reinterpret_cast<uint16_t *>(lp);
        ObDeltaZigzagFixedPfor::decode<uint16_t>(t_lp, in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 4 : {
        uint32_t *t_lp = reinterpret_cast<uint32_t *>(lp);
        ObDeltaZigzagFixedPfor::decode<uint32_t>(t_lp, in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
        break;
      }
      case 8 : {
        uint64_t *t_lp = reinterpret_cast<uint64_t *>(lp);
        ObDeltaZigzagFixedPfor::decode<uint64_t>(t_lp, in, in_len, in_pos, uint_count, out, out_len, out_pos, 0);
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

  // to reslove stack usage exceed 8192B
  uint64_t lp[ObDeltaZigzagFixedPfor::BSIZE + 32];
};

} // namespace common
} // namespace oceanbase

#endif /* OB_DELTA_ZIGZAG_PFOR_H_ */
