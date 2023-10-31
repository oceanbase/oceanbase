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

#ifndef OB_BP_HELPERS_
#define OB_BP_HELPERS_

#include "ob_fast_bp_func.h"
#include "ob_generated_scalar_bp_func.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
inline void fastunpack(const uint32_t *__restrict__ _in,
                       uint8_t *__restrict__ out, const uint32_t bit) {
  const uint8_t *in = reinterpret_cast<const uint8_t *>(_in);

  switch (bit) {
    case 0: {
      __fastunpack0(in + bit*0, out + 8*0);
      __fastunpack0(in + bit*1, out + 8*1);
      __fastunpack0(in + bit*2, out + 8*2);
      __fastunpack0(in + bit*3, out + 8*3);
      break;
    }
    case 1: {
      __fastunpack1(in + bit*0, out + 8*0);
      __fastunpack1(in + bit*1, out + 8*1);
      __fastunpack1(in + bit*2, out + 8*2);
      __fastunpack1(in + bit*3, out + 8*3);
      break;
    }
    case 2: {
      __fastunpack2(in + bit*0, out + 8*0);
      __fastunpack2(in + bit*1, out + 8*1);
      __fastunpack2(in + bit*2, out + 8*2);
      __fastunpack2(in + bit*3, out + 8*3);
      break;
    }
    case 3: {
      __fastunpack3(in + bit*0, out + 8*0);
      __fastunpack3(in + bit*1, out + 8*1);
      __fastunpack3(in + bit*2, out + 8*2);
      __fastunpack3(in + bit*3, out + 8*3);
      break;
    }
    case 4: {
      __fastunpack4(in + bit*0, out + 8*0);
      __fastunpack4(in + bit*1, out + 8*1);
      __fastunpack4(in + bit*2, out + 8*2);
      __fastunpack4(in + bit*3, out + 8*3);
      break;
    }
    case 5: {
      __fastunpack5(in + bit*0, out + 8*0);
      __fastunpack5(in + bit*1, out + 8*1);
      __fastunpack5(in + bit*2, out + 8*2);
      __fastunpack5(in + bit*3, out + 8*3);
      break;
    }
    case 6:{
      __fastunpack6(in + bit*0, out + 8*0);
      __fastunpack6(in + bit*1, out + 8*1);
      __fastunpack6(in + bit*2, out + 8*2);
      __fastunpack6(in + bit*3, out + 8*3);
      break;
    }
    case 7: {
      __fastunpack7(in + bit*0, out + 8*0);
      __fastunpack7(in + bit*1, out + 8*1);
      __fastunpack7(in + bit*2, out + 8*2);
      __fastunpack7(in + bit*3, out + 8*3);
      break;
    }
    case 8: {
      __fastunpack8(in + bit*0, out + 8*0);
      __fastunpack8(in + bit*1, out + 8*1);
      __fastunpack8(in + bit*2, out + 8*2);
      __fastunpack8(in + bit*3, out + 8*3);
      break;
    }
    default: {
      int ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
      break;
    }
  }
}

inline void fastunpack(const uint32_t *__restrict__ _in,
                       uint16_t *__restrict__ out, const uint32_t bit) {

  const uint16_t *in = reinterpret_cast<const uint16_t *>(_in);

  switch (bit) {
    case 0: {
      __fastunpack0(in + bit*0, out + 16*0);
      __fastunpack0(in + bit*1, out + 16*1);
      break;
    }
    case 1: {
      __fastunpack1(in + bit*0, out + 16*0);
      __fastunpack1(in + bit*1, out + 16*1);
      break;
    }
    case 2: {
      __fastunpack2(in + bit*0, out + 16*0);
      __fastunpack2(in + bit*1, out + 16*1);
      break;
    }
    case 3: {
      __fastunpack3(in + bit*0, out + 16*0);
      __fastunpack3(in + bit*1, out + 16*1);
      break;
    }
    case 4: {
      __fastunpack4(in + bit*0, out + 16*0);
      __fastunpack4(in + bit*1, out + 16*1);
      break;
    }
    case 5: {
      __fastunpack5(in + bit*0, out + 16*0);
      __fastunpack5(in + bit*1, out + 16*1);
      break;
    }
    case 6: {
      __fastunpack6(in + bit*0, out + 16*0);
      __fastunpack6(in + bit*1, out + 16*1);
      break;
    }
    case 7: {
      __fastunpack7(in + bit*0, out + 16*0);
      __fastunpack7(in + bit*1, out + 16*1);
      break;
    }
    case 8: {
      __fastunpack8(in + bit*0, out + 16*0);
      __fastunpack8(in + bit*1, out + 16*1);
      break;
    }
    case 9: {
      __fastunpack9(in + bit*0, out + 16*0);
      __fastunpack9(in + bit*1, out + 16*1);
      break;
    }
    case 10: {
      __fastunpack10(in + bit*0, out + 16*0);
      __fastunpack10(in + bit*1, out + 16*1);
      break;
    }
    case 11: {
      __fastunpack11(in + bit*0, out + 16*0);
      __fastunpack11(in + bit*1, out + 16*1);
      break;
    }
    case 12: {
      __fastunpack12(in + bit*0, out + 16*0);
      __fastunpack12(in + bit*1, out + 16*1);
      break;
    }
    case 13: {
      __fastunpack13(in + bit*0, out + 16*0);
      __fastunpack13(in + bit*1, out + 16*1);
      break;
    }
    case 14: {
      __fastunpack14(in + bit*0, out + 16*0);
      __fastunpack14(in + bit*1, out + 16*1);
      break;
    }
    case 15: {
      __fastunpack15(in + bit*0, out + 16*0);
      __fastunpack15(in + bit*1, out + 16*1);
      break;
    }
    case 16: {
      __fastunpack16(in + bit*0, out + 16*0);
      __fastunpack16(in + bit*1, out + 16*1);
      break;
    }
    default: {
      int ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
      break;
    }
  }
}


inline void fastunpack(const uint32_t *__restrict__ in,
                       uint32_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastunpack0(in, out);
    break;
  case 1:
    __fastunpack1(in, out);
    break;
  case 2:
    __fastunpack2(in, out);
    break;
  case 3:
    __fastunpack3(in, out);
    break;
  case 4:
    __fastunpack4(in, out);
    break;
  case 5:
    __fastunpack5(in, out);
    break;
  case 6:
    __fastunpack6(in, out);
    break;
  case 7:
    __fastunpack7(in, out);
    break;
  case 8:
    __fastunpack8(in, out);
    break;
  case 9:
    __fastunpack9(in, out);
    break;
  case 10:
    __fastunpack10(in, out);
    break;
  case 11:
    __fastunpack11(in, out);
    break;
  case 12:
    __fastunpack12(in, out);
    break;
  case 13:
    __fastunpack13(in, out);
    break;
  case 14:
    __fastunpack14(in, out);
    break;
  case 15:
    __fastunpack15(in, out);
    break;
  case 16:
    __fastunpack16(in, out);
    break;
  case 17:
    __fastunpack17(in, out);
    break;
  case 18:
    __fastunpack18(in, out);
    break;
  case 19:
    __fastunpack19(in, out);
    break;
  case 20:
    __fastunpack20(in, out);
    break;
  case 21:
    __fastunpack21(in, out);
    break;
  case 22:
    __fastunpack22(in, out);
    break;
  case 23:
    __fastunpack23(in, out);
    break;
  case 24:
    __fastunpack24(in, out);
    break;
  case 25:
    __fastunpack25(in, out);
    break;
  case 26:
    __fastunpack26(in, out);
    break;
  case 27:
    __fastunpack27(in, out);
    break;
  case 28:
    __fastunpack28(in, out);
    break;
  case 29:
    __fastunpack29(in, out);
    break;
  case 30:
    __fastunpack30(in, out);
    break;
  case 31:
    __fastunpack31(in, out);
    break;
  case 32:
    __fastunpack32(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

inline void fastunpack(const uint32_t *__restrict__ in,
                       uint64_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastunpack0(in, out);
    break;
  case 1:
    __fastunpack1(in, out);
    break;
  case 2:
    __fastunpack2(in, out);
    break;
  case 3:
    __fastunpack3(in, out);
    break;
  case 4:
    __fastunpack4(in, out);
    break;
  case 5:
    __fastunpack5(in, out);
    break;
  case 6:
    __fastunpack6(in, out);
    break;
  case 7:
    __fastunpack7(in, out);
    break;
  case 8:
    __fastunpack8(in, out);
    break;
  case 9:
    __fastunpack9(in, out);
    break;
  case 10:
    __fastunpack10(in, out);
    break;
  case 11:
    __fastunpack11(in, out);
    break;
  case 12:
    __fastunpack12(in, out);
    break;
  case 13:
    __fastunpack13(in, out);
    break;
  case 14:
    __fastunpack14(in, out);
    break;
  case 15:
    __fastunpack15(in, out);
    break;
  case 16:
    __fastunpack16(in, out);
    break;
  case 17:
    __fastunpack17(in, out);
    break;
  case 18:
    __fastunpack18(in, out);
    break;
  case 19:
    __fastunpack19(in, out);
    break;
  case 20:
    __fastunpack20(in, out);
    break;
  case 21:
    __fastunpack21(in, out);
    break;
  case 22:
    __fastunpack22(in, out);
    break;
  case 23:
    __fastunpack23(in, out);
    break;
  case 24:
    __fastunpack24(in, out);
    break;
  case 25:
    __fastunpack25(in, out);
    break;
  case 26:
    __fastunpack26(in, out);
    break;
  case 27:
    __fastunpack27(in, out);
    break;
  case 28:
    __fastunpack28(in, out);
    break;
  case 29:
    __fastunpack29(in, out);
    break;
  case 30:
    __fastunpack30(in, out);
    break;
  case 31:
    __fastunpack31(in, out);
    break;
  case 32:
    __fastunpack32(in, out);
    break;
  case 33:
    __fastunpack33(in, out);
    break;
  case 34:
    __fastunpack34(in, out);
    break;
  case 35:
    __fastunpack35(in, out);
    break;
  case 36:
    __fastunpack36(in, out);
    break;
  case 37:
    __fastunpack37(in, out);
    break;
  case 38:
    __fastunpack38(in, out);
    break;
  case 39:
    __fastunpack39(in, out);
    break;
  case 40:
    __fastunpack40(in, out);
    break;
  case 41:
    __fastunpack41(in, out);
    break;
  case 42:
    __fastunpack42(in, out);
    break;
  case 43:
    __fastunpack43(in, out);
    break;
  case 44:
    __fastunpack44(in, out);
    break;
  case 45:
    __fastunpack45(in, out);
    break;
  case 46:
    __fastunpack46(in, out);
    break;
  case 47:
    __fastunpack47(in, out);
    break;
  case 48:
    __fastunpack48(in, out);
    break;
  case 49:
    __fastunpack49(in, out);
    break;
  case 50:
    __fastunpack50(in, out);
    break;
  case 51:
    __fastunpack51(in, out);
    break;
  case 52:
    __fastunpack52(in, out);
    break;
  case 53:
    __fastunpack53(in, out);
    break;
  case 54:
    __fastunpack54(in, out);
    break;
  case 55:
    __fastunpack55(in, out);
    break;
  case 56:
    __fastunpack56(in, out);
    break;
  case 57:
    __fastunpack57(in, out);
    break;
  case 58:
    __fastunpack58(in, out);
    break;
  case 59:
    __fastunpack59(in, out);
    break;
  case 60:
    __fastunpack60(in, out);
    break;
  case 61:
    __fastunpack61(in, out);
    break;
  case 62:
    __fastunpack62(in, out);
    break;
  case 63:
    __fastunpack63(in, out);
    break;
  case 64:
    __fastunpack64(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

inline void fastpack(const uint8_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit) {
  uint8_t *out = reinterpret_cast<uint8_t *>(_out);
  switch (bit) {
    case 0: {
      __fastpack0(in + 8*0, out + bit*0);
      __fastpack0(in + 8*1, out + bit*1);
      __fastpack0(in + 8*2, out + bit*2);
      __fastpack0(in + 8*3, out + bit*3);
      break;
    }
    case 1: {
      __fastpack1(in + 8*0, out + bit*0);
      __fastpack1(in + 8*1, out + bit*1);
      __fastpack1(in + 8*2, out + bit*2);
      __fastpack1(in + 8*3, out + bit*3);
      break;
    }
    case 2: {
      __fastpack2(in + 8*0, out + bit*0);
      __fastpack2(in + 8*1, out + bit*1);
      __fastpack2(in + 8*2, out + bit*2);
      __fastpack2(in + 8*3, out + bit*3);
      break;
    }
    case 3: {
      __fastpack3(in + 8*0, out + bit*0);
      __fastpack3(in + 8*1, out + bit*1);
      __fastpack3(in + 8*2, out + bit*2);
      __fastpack3(in + 8*3, out + bit*3);
      break;
    }
    case 4: {
      __fastpack4(in + 8*0, out + bit*0);
      __fastpack4(in + 8*1, out + bit*1);
      __fastpack4(in + 8*2, out + bit*2);
      __fastpack4(in + 8*3, out + bit*3);
      break;
    }
    case 5: {
      __fastpack5(in + 8*0, out + bit*0);
      __fastpack5(in + 8*1, out + bit*1);
      __fastpack5(in + 8*2, out + bit*2);
      __fastpack5(in + 8*3, out + bit*3);
      break;
    }
    case 6: {
      __fastpack6(in + 8*0, out + bit*0);
      __fastpack6(in + 8*1, out + bit*1);
      __fastpack6(in + 8*2, out + bit*2);
      __fastpack6(in + 8*3, out + bit*3);
      break;
    }
    case 7: {
      __fastpack7(in + 8*0, out + bit*0);
      __fastpack7(in + 8*1, out + bit*1);
      __fastpack7(in + 8*2, out + bit*2);
      __fastpack7(in + 8*3, out + bit*3);
      break;
    }
    case 8: {
      __fastpack8(in + 8*0, out + bit*0);
      __fastpack8(in + 8*1, out + bit*1);
      __fastpack8(in + 8*2, out + bit*2);
      __fastpack8(in + 8*3, out + bit*3);
      break;
    }
    default: {
      int ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
      break;
    }
  }
}

inline void fastpack(const uint16_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit) {
  uint16_t *out = reinterpret_cast<uint16_t *>(_out);

  switch (bit) {
    case 0: {
      __fastpack0(in + 16*0, out + bit*0);
      __fastpack0(in + 16*1, out + bit*1);
      break;
    }
    case 1: {
      __fastpack1(in + 16*0, out + bit*0);
      __fastpack1(in + 16*1, out + bit*1);
      break;
    }
    case 2: {
      __fastpack2(in + 16*0, out + bit*0);
      __fastpack2(in + 16*1, out + bit*1);
      break;
    }
    case 3: {
      __fastpack3(in + 16*0, out + bit*0);
      __fastpack3(in + 16*1, out + bit*1);
      break;
    }
    case 4: {
      __fastpack4(in + 16*0, out + bit*0);
      __fastpack4(in + 16*1, out + bit*1);
      break;
    }
    case 5: {
      __fastpack5(in + 16*0, out + bit*0);
      __fastpack5(in + 16*1, out + bit*1);
      break;
    }
    case 6: {
      __fastpack6(in + 16*0, out + bit*0);
      __fastpack6(in + 16*1, out + bit*1);
      break;
    }
    case 7: {
      __fastpack7(in + 16*0, out + bit*0);
      __fastpack7(in + 16*1, out + bit*1);
      break;
    }
    case 8: {
      __fastpack8(in + 16*0, out + bit*0);
      __fastpack8(in + 16*1, out + bit*1);
      break;
    }
    case 9: {
      __fastpack9(in + 16*0, out + bit*0);
      __fastpack9(in + 16*1, out + bit*1);
      break;
    }
    case 10: {
      __fastpack10(in + 16*0, out + bit*0);
      __fastpack10(in + 16*1, out + bit*1);
      break;
    }
    case 11: {
      __fastpack11(in + 16*0, out + bit*0);
      __fastpack11(in + 16*1, out + bit*1);
      break;
    }
    case 12: {
      __fastpack12(in + 16*0, out + bit*0);
      __fastpack12(in + 16*1, out + bit*1);
      break;
    }
    case 13: {
      __fastpack13(in + 16*0, out + bit*0);
      __fastpack13(in + 16*1, out + bit*1);
      break;
    }
    case 14: {
      __fastpack14(in + 16*0, out + bit*0);
      __fastpack14(in + 16*1, out + bit*1);
      break;
    }
    case 15: {
      __fastpack15(in + 16*0, out + bit*0);
      __fastpack15(in + 16*1, out + bit*1);
      break;
    }
    case 16: {
      __fastpack16(in + 16*0, out + bit*0);
      __fastpack16(in + 16*1, out + bit*1);
      break;
    }
    default: {
      int ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
      break;
    }
  }
}

inline void fastpack(const uint32_t *__restrict__ in,
                     uint32_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastpack0(in, out);
    break;
  case 1:
    __fastpack1(in, out);
    break;
  case 2:
    __fastpack2(in, out);
    break;
  case 3:
    __fastpack3(in, out);
    break;
  case 4:
    __fastpack4(in, out);
    break;
  case 5:
    __fastpack5(in, out);
    break;
  case 6:
    __fastpack6(in, out);
    break;
  case 7:
    __fastpack7(in, out);
    break;
  case 8:
    __fastpack8(in, out);
    break;
  case 9:
    __fastpack9(in, out);
    break;
  case 10:
    __fastpack10(in, out);
    break;
  case 11:
    __fastpack11(in, out);
    break;
  case 12:
    __fastpack12(in, out);
    break;
  case 13:
    __fastpack13(in, out);
    break;
  case 14:
    __fastpack14(in, out);
    break;
  case 15:
    __fastpack15(in, out);
    break;
  case 16:
    __fastpack16(in, out);
    break;
  case 17:
    __fastpack17(in, out);
    break;
  case 18:
    __fastpack18(in, out);
    break;
  case 19:
    __fastpack19(in, out);
    break;
  case 20:
    __fastpack20(in, out);
    break;
  case 21:
    __fastpack21(in, out);
    break;
  case 22:
    __fastpack22(in, out);
    break;
  case 23:
    __fastpack23(in, out);
    break;
  case 24:
    __fastpack24(in, out);
    break;
  case 25:
    __fastpack25(in, out);
    break;
  case 26:
    __fastpack26(in, out);
    break;
  case 27:
    __fastpack27(in, out);
    break;
  case 28:
    __fastpack28(in, out);
    break;
  case 29:
    __fastpack29(in, out);
    break;
  case 30:
    __fastpack30(in, out);
    break;
  case 31:
    __fastpack31(in, out);
    break;
  case 32:
    __fastpack32(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

inline void fastpack(const uint64_t *__restrict__ in,
                     uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    __fastpack0(in, out);
    break;
  case 1:
    __fastpack1(in, out);
    break;
  case 2:
    __fastpack2(in, out);
    break;
  case 3:
    __fastpack3(in, out);
    break;
  case 4:
    __fastpack4(in, out);
    break;
  case 5:
    __fastpack5(in, out);
    break;
  case 6:
    __fastpack6(in, out);
    break;
  case 7:
    __fastpack7(in, out);
    break;
  case 8:
    __fastpack8(in, out);
    break;
  case 9:
    __fastpack9(in, out);
    break;
  case 10:
    __fastpack10(in, out);
    break;
  case 11:
    __fastpack11(in, out);
    break;
  case 12:
    __fastpack12(in, out);
    break;
  case 13:
    __fastpack13(in, out);
    break;
  case 14:
    __fastpack14(in, out);
    break;
  case 15:
    __fastpack15(in, out);
    break;
  case 16:
    __fastpack16(in, out);
    break;
  case 17:
    __fastpack17(in, out);
    break;
  case 18:
    __fastpack18(in, out);
    break;
  case 19:
    __fastpack19(in, out);
    break;
  case 20:
    __fastpack20(in, out);
    break;
  case 21:
    __fastpack21(in, out);
    break;
  case 22:
    __fastpack22(in, out);
    break;
  case 23:
    __fastpack23(in, out);
    break;
  case 24:
    __fastpack24(in, out);
    break;
  case 25:
    __fastpack25(in, out);
    break;
  case 26:
    __fastpack26(in, out);
    break;
  case 27:
    __fastpack27(in, out);
    break;
  case 28:
    __fastpack28(in, out);
    break;
  case 29:
    __fastpack29(in, out);
    break;
  case 30:
    __fastpack30(in, out);
    break;
  case 31:
    __fastpack31(in, out);
    break;
  case 32:
    __fastpack32(in, out);
    break;
  case 33:
    __fastpack33(in, out);
    break;
  case 34:
    __fastpack34(in, out);
    break;
  case 35:
    __fastpack35(in, out);
    break;
  case 36:
    __fastpack36(in, out);
    break;
  case 37:
    __fastpack37(in, out);
    break;
  case 38:
    __fastpack38(in, out);
    break;
  case 39:
    __fastpack39(in, out);
    break;
  case 40:
    __fastpack40(in, out);
    break;
  case 41:
    __fastpack41(in, out);
    break;
  case 42:
    __fastpack42(in, out);
    break;
  case 43:
    __fastpack43(in, out);
    break;
  case 44:
    __fastpack44(in, out);
    break;
  case 45:
    __fastpack45(in, out);
    break;
  case 46:
    __fastpack46(in, out);
    break;
  case 47:
    __fastpack47(in, out);
    break;
  case 48:
    __fastpack48(in, out);
    break;
  case 49:
    __fastpack49(in, out);
    break;
  case 50:
    __fastpack50(in, out);
    break;
  case 51:
    __fastpack51(in, out);
    break;
  case 52:
    __fastpack52(in, out);
    break;
  case 53:
    __fastpack53(in, out);
    break;
  case 54:
    __fastpack54(in, out);
    break;
  case 55:
    __fastpack55(in, out);
    break;
  case 56:
    __fastpack56(in, out);
    break;
  case 57:
    __fastpack57(in, out);
    break;
  case 58:
    __fastpack58(in, out);
    break;
  case 59:
    __fastpack59(in, out);
    break;
  case 60:
    __fastpack60(in, out);
    break;
  case 61:
    __fastpack61(in, out);
    break;
  case 62:
    __fastpack62(in, out);
    break;
  case 63:
    __fastpack63(in, out);
    break;
  case 64:
    __fastpack64(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

/*assumes that integers fit in the prescribed number of bits*/
inline void fastpackwithoutmask(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out,
                                const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastpackwithoutmask0(in, out);
    break;
  case 1:
    __fastpackwithoutmask1(in, out);
    break;
  case 2:
    __fastpackwithoutmask2(in, out);
    break;
  case 3:
    __fastpackwithoutmask3(in, out);
    break;
  case 4:
    __fastpackwithoutmask4(in, out);
    break;
  case 5:
    __fastpackwithoutmask5(in, out);
    break;
  case 6:
    __fastpackwithoutmask6(in, out);
    break;
  case 7:
    __fastpackwithoutmask7(in, out);
    break;
  case 8:
    __fastpackwithoutmask8(in, out);
    break;
  case 9:
    __fastpackwithoutmask9(in, out);
    break;
  case 10:
    __fastpackwithoutmask10(in, out);
    break;
  case 11:
    __fastpackwithoutmask11(in, out);
    break;
  case 12:
    __fastpackwithoutmask12(in, out);
    break;
  case 13:
    __fastpackwithoutmask13(in, out);
    break;
  case 14:
    __fastpackwithoutmask14(in, out);
    break;
  case 15:
    __fastpackwithoutmask15(in, out);
    break;
  case 16:
    __fastpackwithoutmask16(in, out);
    break;
  case 17:
    __fastpackwithoutmask17(in, out);
    break;
  case 18:
    __fastpackwithoutmask18(in, out);
    break;
  case 19:
    __fastpackwithoutmask19(in, out);
    break;
  case 20:
    __fastpackwithoutmask20(in, out);
    break;
  case 21:
    __fastpackwithoutmask21(in, out);
    break;
  case 22:
    __fastpackwithoutmask22(in, out);
    break;
  case 23:
    __fastpackwithoutmask23(in, out);
    break;
  case 24:
    __fastpackwithoutmask24(in, out);
    break;
  case 25:
    __fastpackwithoutmask25(in, out);
    break;
  case 26:
    __fastpackwithoutmask26(in, out);
    break;
  case 27:
    __fastpackwithoutmask27(in, out);
    break;
  case 28:
    __fastpackwithoutmask28(in, out);
    break;
  case 29:
    __fastpackwithoutmask29(in, out);
    break;
  case 30:
    __fastpackwithoutmask30(in, out);
    break;
  case 31:
    __fastpackwithoutmask31(in, out);
    break;
  case 32:
    __fastpackwithoutmask32(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

inline void fastpackwithoutmask(const uint64_t *__restrict__ in,
                                uint32_t *__restrict__ out,
                                const uint32_t bit) {
  switch (bit) {
  case 0:
    __fastpackwithoutmask0(in, out);
    break;
  case 1:
    __fastpackwithoutmask1(in, out);
    break;
  case 2:
    __fastpackwithoutmask2(in, out);
    break;
  case 3:
    __fastpackwithoutmask3(in, out);
    break;
  case 4:
    __fastpackwithoutmask4(in, out);
    break;
  case 5:
    __fastpackwithoutmask5(in, out);
    break;
  case 6:
    __fastpackwithoutmask6(in, out);
    break;
  case 7:
    __fastpackwithoutmask7(in, out);
    break;
  case 8:
    __fastpackwithoutmask8(in, out);
    break;
  case 9:
    __fastpackwithoutmask9(in, out);
    break;
  case 10:
    __fastpackwithoutmask10(in, out);
    break;
  case 11:
    __fastpackwithoutmask11(in, out);
    break;
  case 12:
    __fastpackwithoutmask12(in, out);
    break;
  case 13:
    __fastpackwithoutmask13(in, out);
    break;
  case 14:
    __fastpackwithoutmask14(in, out);
    break;
  case 15:
    __fastpackwithoutmask15(in, out);
    break;
  case 16:
    __fastpackwithoutmask16(in, out);
    break;
  case 17:
    __fastpackwithoutmask17(in, out);
    break;
  case 18:
    __fastpackwithoutmask18(in, out);
    break;
  case 19:
    __fastpackwithoutmask19(in, out);
    break;
  case 20:
    __fastpackwithoutmask20(in, out);
    break;
  case 21:
    __fastpackwithoutmask21(in, out);
    break;
  case 22:
    __fastpackwithoutmask22(in, out);
    break;
  case 23:
    __fastpackwithoutmask23(in, out);
    break;
  case 24:
    __fastpackwithoutmask24(in, out);
    break;
  case 25:
    __fastpackwithoutmask25(in, out);
    break;
  case 26:
    __fastpackwithoutmask26(in, out);
    break;
  case 27:
    __fastpackwithoutmask27(in, out);
    break;
  case 28:
    __fastpackwithoutmask28(in, out);
    break;
  case 29:
    __fastpackwithoutmask29(in, out);
    break;
  case 30:
    __fastpackwithoutmask30(in, out);
    break;
  case 31:
    __fastpackwithoutmask31(in, out);
    break;
  case 32:
    __fastpackwithoutmask32(in, out);
    break;
  case 33:
    __fastpackwithoutmask33(in, out);
    break;
  case 34:
    __fastpackwithoutmask34(in, out);
    break;
  case 35:
    __fastpackwithoutmask35(in, out);
    break;
  case 36:
    __fastpackwithoutmask36(in, out);
    break;
  case 37:
    __fastpackwithoutmask37(in, out);
    break;
  case 38:
    __fastpackwithoutmask38(in, out);
    break;
  case 39:
    __fastpackwithoutmask39(in, out);
    break;
  case 40:
    __fastpackwithoutmask40(in, out);
    break;
  case 41:
    __fastpackwithoutmask41(in, out);
    break;
  case 42:
    __fastpackwithoutmask42(in, out);
    break;
  case 43:
    __fastpackwithoutmask43(in, out);
    break;
  case 44:
    __fastpackwithoutmask44(in, out);
    break;
  case 45:
    __fastpackwithoutmask45(in, out);
    break;
  case 46:
    __fastpackwithoutmask46(in, out);
    break;
  case 47:
    __fastpackwithoutmask47(in, out);
    break;
  case 48:
    __fastpackwithoutmask48(in, out);
    break;
  case 49:
    __fastpackwithoutmask49(in, out);
    break;
  case 50:
    __fastpackwithoutmask50(in, out);
    break;
  case 51:
    __fastpackwithoutmask51(in, out);
    break;
  case 52:
    __fastpackwithoutmask52(in, out);
    break;
  case 53:
    __fastpackwithoutmask53(in, out);
    break;
  case 54:
    __fastpackwithoutmask54(in, out);
    break;
  case 55:
    __fastpackwithoutmask55(in, out);
    break;
  case 56:
    __fastpackwithoutmask56(in, out);
    break;
  case 57:
    __fastpackwithoutmask57(in, out);
    break;
  case 58:
    __fastpackwithoutmask58(in, out);
    break;
  case 59:
    __fastpackwithoutmask59(in, out);
    break;
  case 60:
    __fastpackwithoutmask60(in, out);
    break;
  case 61:
    __fastpackwithoutmask61(in, out);
    break;
  case 62:
    __fastpackwithoutmask62(in, out);
    break;
  case 63:
    __fastpackwithoutmask63(in, out);
    break;
  case 64:
    __fastpackwithoutmask64(in, out);
    break;
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}

inline void scalar_fastunpack(const uint32_t *__restrict__ _in,
                              uint8_t *__restrict__ out, const uint32_t bit) {
  scalar_fastunpack_8_32_count(_in, out, bit);
}

inline void scalar_fastunpack(const uint32_t *__restrict__ _in,
                              uint16_t *__restrict__ out, const uint32_t bit) {
  scalar_fastunpack_16_32_count(_in, out, bit);
}

inline void scalar_fastunpack(const uint32_t *__restrict__ _in,
                              uint32_t *__restrict__ out, const uint32_t bit) {
  scalar_fastunpack_32(_in, out, bit);
}

inline void scalar_fastunpack(const uint32_t *__restrict__ _in,
                              uint64_t *__restrict__ out, const uint32_t bit) {
  // uint64_t use fast fastunpack
  fastunpack(_in, out, bit);
}

inline void scalar_fastpackwithoutmask(const uint8_t *__restrict__ in,
                                       uint32_t *__restrict__ out,
                                       const uint32_t bit) {
  scalar_fastpackwithoutmask_8_32_count(in, out, bit);
}

inline void scalar_fastpackwithoutmask(const uint16_t *__restrict__ in,
                                       uint32_t *__restrict__ out,
                                       const uint32_t bit) {
  scalar_fastpackwithoutmask_16_32_count(in, out, bit);
}

inline void scalar_fastpackwithoutmask(const uint32_t *__restrict__ in,
                                       uint32_t *__restrict__ out,
                                       const uint32_t bit) {
  scalar_fastpackwithoutmask_32(in, out, bit);
}

inline void scalar_fastpackwithoutmask(const uint64_t *__restrict__ in,
                                       uint32_t *__restrict__ out,
                                       const uint32_t bit) {
  fastpackwithoutmask(in, out, bit);
}


// InIntType maybe uint8_t, uint16_t, uint32_t or uint64_t
template <typename InIntType>
inline void scalar_bit_packing(
    const InIntType *in,
    const uint64_t length,
    const uint32_t b,
    char *out,
    uint64_t out_buf_len,
    uint64_t &out_pos)
{
  char *orig_out = out;
  out += out_pos;
  for (uint32_t run = 0; run < length / 32; ++run) {
    // if use fastpackwithoutmask, high bits( > b) must be zero
    scalar_fastpackwithoutmask((const InIntType *)in, (uint32_t *)out, b);
    in += 32;
    out += (32 * b / CHAR_BIT);
  }

  uint32_t remain = length % 32;
  if (remain > 0) {
    InIntType tail[32] = {0};
    MEMCPY(tail, in, remain * sizeof(InIntType));
    InIntType tmp_out[32] = {0};
    scalar_fastpackwithoutmask((const InIntType *)tail, (uint32_t *)(tmp_out), b);

    uint64_t out_type_bits = sizeof(char) * CHAR_BIT;
    uint64_t copy_len = ((remain * b + out_type_bits - 1) / out_type_bits);
    memcpy(out, tmp_out, copy_len);
    out += copy_len;
    in += remain;
  }
  out_pos = out - orig_out;
  //LIB_LOG(INFO, "scalar pack", K(length), K(b), K(sizeof(InIntType)), K(out_buf_len), KP(orig_out), K(out_pos));
}

// OutIntType maybe uint8_t, uint16_t, uint32_t or uint64_t
template <typename OutIntType>
inline void scalar_bit_unpacking(
    const char *in,
    const uint64_t length,
    uint64_t &pos,
    const uint32_t uint_count,
    const uint32_t b,
    OutIntType *out,
    const uint64_t out_buf_len,
    uint64_t &out_pos)
{
  const char *orig_in = in;
  OutIntType *orig_out = out;
  in += pos;
  out += out_pos;

  for (uint32_t run = 0; run < uint_count / 32; ++run) {
    scalar_fastunpack((const uint32_t *)in, out, b);
    in += (32 * b / CHAR_BIT);
    out += 32;
  }

  uint32_t remain = uint_count % 32;
  if (remain > 0) {
    uint32_t tail[32 * sizeof(OutIntType) / sizeof(uint32_t)] = {0};
    uint32_t in_buf_type_bits = sizeof(char) * CHAR_BIT;
    uint32_t copy_len = ((b * remain + in_buf_type_bits - 1) / in_buf_type_bits);
    MEMCPY(tail, in, copy_len);
    OutIntType tmp_out[32] = {0};
    scalar_fastunpack(tail, tmp_out, b);
    MEMCPY(out, tmp_out, remain * sizeof(OutIntType));
    in += copy_len;
    out += remain;
  }
  out_pos += uint_count;
  pos = in - orig_in;

  //LIB_LOG(INFO, "scalar unpack", KP(orig_in), KP(orig_out), K(uint_count), K(b),
  //        K(sizeof(OutIntType)), K(out_buf_len), K(out_pos), K(length), K(pos));
}

} // namespace common
} // namespace oceanbase

#endif /* OB_BP_HELPERS_ */
