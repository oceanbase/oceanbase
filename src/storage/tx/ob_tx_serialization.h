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

#ifndef OCEANBASE_TRANSACTION_OB_TX_SERIALIZATION
#define OCEANBASE_TRANSACTION_OB_TX_SERIALIZATION

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"

#define MAX_COMPAT_BYTES_COUNT 14

namespace oceanbase
{
namespace transaction
{
typedef common::ObSEArray<uint8_t, 1> CompatByteArray;

class ObTxSerCompatByte
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;

  // object_index start from 1
  // byte_index start from 0
  // bit_index start from 0
public:
  ObTxSerCompatByte() { reset(); }
  void reset();
  int init(int64_t total_object_count);
  bool is_inited() { return total_byte_cnt_ > 0 && total_obj_cnt_ > 0; }
  int set_all_member_need_ser();
  int set_object_flag(int64_t object_index, bool is_valid);
  bool is_object_valid(int64_t object_index) const;

  const static uint8_t BASE_VALID_BIT;
  const static uint8_t NEXT_BYTE_VALID;
  const static uint8_t BASE_VALID_BYTE;

  const static uint8_t MAX_OBJECT_COUNT_PER_BYTE;
  const static uint8_t MAX_BIT_COUNT_PER_BYTE;

  TO_STRING_KV(K(total_obj_cnt_), K(total_byte_cnt_), KP(this));

private:
  void clear_compat_bytes_();
  int cal_object_location_(int64_t object_index, int64_t &byte_index, uint8_t &bit_index) const;
  void set_object_flag_(int64_t byte_index, uint8_t bit_index, bool is_valid);
  void is_object_valid_(int64_t byte_index, uint8_t bit_index, bool &is_valid) const;
  int init_all_bytes_valid_(const int64_t total_byte_cnt);

private:
  int64_t total_obj_cnt_;
  int64_t total_byte_cnt_;
  uint8_t compat_bytes_[MAX_COMPAT_BYTES_COUNT];
  // CompatByteArray compat_bytes_;
};

#define TX_NO_NEED_SER(NO_NEED_SER_COND, IDX, COMPAT_BYTES)                       \
  if (OB_SUCC(ret)) {                                                             \
    if (NO_NEED_SER_COND) {                                                       \
      if (OB_FAIL(COMPAT_BYTES.set_object_flag(IDX, false)))                      \
        TRANS_LOG(WARN, "set object invalid failed", K(NO_NEED_SER_COND), K(IDX), \
                  K(COMPAT_BYTES));                                               \
    }                                                                             \
  }

#define TX_IS_NEED_SER_OR_DE(COMPAT_BYTES, IDX) COMPAT_BYTES.is_object_valid(IDX)

#define TX_SER_COMPAT_BYTES(COMPAT_BYTES)                                             \
  if (OB_SUCC(ret)) {                                                                 \
    if (OB_FAIL(COMPAT_BYTES.serialize(buf, buf_len, pos))) {                        \
      TRANS_LOG(WARN, "serialize compat_bytes_ failed",K(ret), K(buf_len),K(pos),K(COMPAT_BYTES)); \
    }                                                                                 \
  }

#define TX_DSER_COMPAT_BYTES(COMPAT_BYTES)                                             \
  if (OB_SUCC(ret)) {                                                                 \
    if (OB_FAIL(COMPAT_BYTES.deserialize(buf, data_len, pos))) {                        \
      TRANS_LOG(WARN, "deserialize compat_bytes_ failed",K(ret), K(data_len),K(pos),K(COMPAT_BYTES)); \
    }                                                                                 \
  }

#define TX_SER_SIZE_COMPAT_BYTES(COMPAT_BYTES) len += COMPAT_BYTES.get_serialize_size();




#define TX_LST_DO_1(M, arg, s, P, ...) P(M, arg, 1, ##__VA_ARGS__)
#define TX_LST_DO_2(M, arg, s, P, ...) TX_LST_DO_1(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 2, ##__VA_ARGS__)
#define TX_LST_DO_3(M, arg, s, P, ...) TX_LST_DO_2(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 3, ##__VA_ARGS__)
#define TX_LST_DO_4(M, arg, s, P, ...) TX_LST_DO_3(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 4, ##__VA_ARGS__)
#define TX_LST_DO_5(M, arg, s, P, ...) TX_LST_DO_4(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 5, ##__VA_ARGS__)
#define TX_LST_DO_6(M, arg, s, P, ...) TX_LST_DO_5(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 6, ##__VA_ARGS__)
#define TX_LST_DO_7(M, arg, s, P, ...) TX_LST_DO_6(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 7, ##__VA_ARGS__)
#define TX_LST_DO_8(M, arg, s, P, ...) TX_LST_DO_7(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 8, ##__VA_ARGS__)
#define TX_LST_DO_9(M, arg, s, P, ...) TX_LST_DO_8(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 9, ##__VA_ARGS__)
#define TX_LST_DO_10(M, arg, s, P, ...) TX_LST_DO_9(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 10, ##__VA_ARGS__)
#define TX_LST_DO_11(M, arg, s, P, ...) TX_LST_DO_10(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 11, ##__VA_ARGS__)
#define TX_LST_DO_12(M, arg, s, P, ...) TX_LST_DO_11(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 12, ##__VA_ARGS__)
#define TX_LST_DO_13(M, arg, s, P, ...) TX_LST_DO_12(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 13, ##__VA_ARGS__)
#define TX_LST_DO_14(M, arg, s, P, ...) TX_LST_DO_13(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 14, ##__VA_ARGS__)
#define TX_LST_DO_15(M, arg, s, P, ...) TX_LST_DO_14(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 15, ##__VA_ARGS__)
#define TX_LST_DO_16(M, arg, s, P, ...) TX_LST_DO_15(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 16, ##__VA_ARGS__)
#define TX_LST_DO_17(M, arg, s, P, ...) TX_LST_DO_16(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 17, ##__VA_ARGS__)
#define TX_LST_DO_18(M, arg, s, P, ...) TX_LST_DO_17(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 18, ##__VA_ARGS__)
#define TX_LST_DO_19(M, arg, s, P, ...) TX_LST_DO_18(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 19, ##__VA_ARGS__)
#define TX_LST_DO_20(M, arg, s, P, ...) TX_LST_DO_19(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 20, ##__VA_ARGS__)
#define TX_LST_DO_21(M, arg, s, P, ...) TX_LST_DO_20(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 21, ##__VA_ARGS__)
#define TX_LST_DO_22(M, arg, s, P, ...) TX_LST_DO_21(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 22, ##__VA_ARGS__)
#define TX_LST_DO_23(M, arg, s, P, ...) TX_LST_DO_22(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 23, ##__VA_ARGS__)
#define TX_LST_DO_24(M, arg, s, P, ...) TX_LST_DO_23(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 24, ##__VA_ARGS__)
#define TX_LST_DO_25(M, arg, s, P, ...) TX_LST_DO_24(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 25, ##__VA_ARGS__)
#define TX_LST_DO_26(M, arg, s, P, ...) TX_LST_DO_25(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 26, ##__VA_ARGS__)
#define TX_LST_DO_27(M, arg, s, P, ...) TX_LST_DO_26(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 27, ##__VA_ARGS__)
#define TX_LST_DO_28(M, arg, s, P, ...) TX_LST_DO_27(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 28, ##__VA_ARGS__)
#define TX_LST_DO_29(M, arg, s, P, ...) TX_LST_DO_28(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 29, ##__VA_ARGS__)
#define TX_LST_DO_30(M, arg, s, P, ...) TX_LST_DO_29(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 30, ##__VA_ARGS__)
#define TX_LST_DO_31(M, arg, s, P, ...) TX_LST_DO_30(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 31, ##__VA_ARGS__)
#define TX_LST_DO_32(M, arg, s, P, ...) TX_LST_DO_31(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 32, ##__VA_ARGS__)
#define TX_LST_DO_33(M, arg, s, P, ...) TX_LST_DO_32(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 33, ##__VA_ARGS__)
#define TX_LST_DO_34(M, arg, s, P, ...) TX_LST_DO_33(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 34, ##__VA_ARGS__)
#define TX_LST_DO_35(M, arg, s, P, ...) TX_LST_DO_34(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 35, ##__VA_ARGS__)
#define TX_LST_DO_36(M, arg, s, P, ...) TX_LST_DO_35(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 36, ##__VA_ARGS__)
#define TX_LST_DO_37(M, arg, s, P, ...) TX_LST_DO_36(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 37, ##__VA_ARGS__)
#define TX_LST_DO_38(M, arg, s, P, ...) TX_LST_DO_37(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 38, ##__VA_ARGS__)
#define TX_LST_DO_39(M, arg, s, P, ...) TX_LST_DO_38(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 39, ##__VA_ARGS__)
#define TX_LST_DO_40(M, arg, s, P, ...) TX_LST_DO_39(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 40, ##__VA_ARGS__)
#define TX_LST_DO_41(M, arg, s, P, ...) TX_LST_DO_40(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 41, ##__VA_ARGS__)
#define TX_LST_DO_42(M, arg, s, P, ...) TX_LST_DO_41(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 42, ##__VA_ARGS__)
#define TX_LST_DO_43(M, arg, s, P, ...) TX_LST_DO_42(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 43, ##__VA_ARGS__)
#define TX_LST_DO_44(M, arg, s, P, ...) TX_LST_DO_43(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 44, ##__VA_ARGS__)
#define TX_LST_DO_45(M, arg, s, P, ...) TX_LST_DO_44(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 45, ##__VA_ARGS__)
#define TX_LST_DO_46(M, arg, s, P, ...) TX_LST_DO_45(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 46, ##__VA_ARGS__)
#define TX_LST_DO_47(M, arg, s, P, ...) TX_LST_DO_46(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 47, ##__VA_ARGS__)
#define TX_LST_DO_48(M, arg, s, P, ...) TX_LST_DO_47(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 48, ##__VA_ARGS__)
#define TX_LST_DO_49(M, arg, s, P, ...) TX_LST_DO_48(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 49, ##__VA_ARGS__)
#define TX_LST_DO_50(M, arg, s, P, ...) TX_LST_DO_49(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 50, ##__VA_ARGS__)
#define TX_LST_DO_51(M, arg, s, P, ...) TX_LST_DO_50(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 51, ##__VA_ARGS__)
#define TX_LST_DO_52(M, arg, s, P, ...) TX_LST_DO_51(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 52, ##__VA_ARGS__)
#define TX_LST_DO_53(M, arg, s, P, ...) TX_LST_DO_52(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 53, ##__VA_ARGS__)
#define TX_LST_DO_54(M, arg, s, P, ...) TX_LST_DO_53(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 54, ##__VA_ARGS__)
#define TX_LST_DO_55(M, arg, s, P, ...) TX_LST_DO_54(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 55, ##__VA_ARGS__)
#define TX_LST_DO_56(M, arg, s, P, ...) TX_LST_DO_55(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 56, ##__VA_ARGS__)
#define TX_LST_DO_57(M, arg, s, P, ...) TX_LST_DO_56(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 57, ##__VA_ARGS__)
#define TX_LST_DO_58(M, arg, s, P, ...) TX_LST_DO_57(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 58, ##__VA_ARGS__)
#define TX_LST_DO_59(M, arg, s, P, ...) TX_LST_DO_58(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 59, ##__VA_ARGS__)
#define TX_LST_DO_60(M, arg, s, P, ...) TX_LST_DO_59(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 60, ##__VA_ARGS__)
#define TX_LST_DO_61(M, arg, s, P, ...) TX_LST_DO_60(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 61, ##__VA_ARGS__)
#define TX_LST_DO_62(M, arg, s, P, ...) TX_LST_DO_61(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 62, ##__VA_ARGS__)
#define TX_LST_DO_63(M, arg, s, P, ...) TX_LST_DO_62(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 63, ##__VA_ARGS__)
#define TX_LST_DO_64(M, arg, s, P, ...) TX_LST_DO_63(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 64, ##__VA_ARGS__)
#define TX_LST_DO_65(M, arg, s, P, ...) TX_LST_DO_64(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 65, ##__VA_ARGS__)
#define TX_LST_DO_66(M, arg, s, P, ...) TX_LST_DO_65(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 66, ##__VA_ARGS__)
#define TX_LST_DO_67(M, arg, s, P, ...) TX_LST_DO_66(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 67, ##__VA_ARGS__)
#define TX_LST_DO_68(M, arg, s, P, ...) TX_LST_DO_67(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 68, ##__VA_ARGS__)
#define TX_LST_DO_69(M, arg, s, P, ...) TX_LST_DO_68(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 69, ##__VA_ARGS__)
#define TX_LST_DO_70(M, arg, s, P, ...) TX_LST_DO_69(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 70, ##__VA_ARGS__)
#define TX_LST_DO_71(M, arg, s, P, ...) TX_LST_DO_70(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 71, ##__VA_ARGS__)
#define TX_LST_DO_72(M, arg, s, P, ...) TX_LST_DO_71(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 72, ##__VA_ARGS__)
#define TX_LST_DO_73(M, arg, s, P, ...) TX_LST_DO_72(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 73, ##__VA_ARGS__)
#define TX_LST_DO_74(M, arg, s, P, ...) TX_LST_DO_73(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 74, ##__VA_ARGS__)
#define TX_LST_DO_75(M, arg, s, P, ...) TX_LST_DO_74(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 75, ##__VA_ARGS__)
#define TX_LST_DO_76(M, arg, s, P, ...) TX_LST_DO_75(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 76, ##__VA_ARGS__)
#define TX_LST_DO_77(M, arg, s, P, ...) TX_LST_DO_76(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 77, ##__VA_ARGS__)
#define TX_LST_DO_78(M, arg, s, P, ...) TX_LST_DO_77(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 78, ##__VA_ARGS__)
#define TX_LST_DO_79(M, arg, s, P, ...) TX_LST_DO_78(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 79, ##__VA_ARGS__)
#define TX_LST_DO_80(M, arg, s, P, ...) TX_LST_DO_79(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 80, ##__VA_ARGS__)
#define TX_LST_DO_81(M, arg, s, P, ...) TX_LST_DO_80(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 81, ##__VA_ARGS__)
#define TX_LST_DO_82(M, arg, s, P, ...) TX_LST_DO_81(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 82, ##__VA_ARGS__)
#define TX_LST_DO_83(M, arg, s, P, ...) TX_LST_DO_82(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 83, ##__VA_ARGS__)
#define TX_LST_DO_84(M, arg, s, P, ...) TX_LST_DO_83(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 84, ##__VA_ARGS__)
#define TX_LST_DO_85(M, arg, s, P, ...) TX_LST_DO_84(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 85, ##__VA_ARGS__)
#define TX_LST_DO_86(M, arg, s, P, ...) TX_LST_DO_85(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 86, ##__VA_ARGS__)
#define TX_LST_DO_87(M, arg, s, P, ...) TX_LST_DO_86(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 87, ##__VA_ARGS__)
#define TX_LST_DO_88(M, arg, s, P, ...) TX_LST_DO_87(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 88, ##__VA_ARGS__)
#define TX_LST_DO_89(M, arg, s, P, ...) TX_LST_DO_88(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 89, ##__VA_ARGS__)
#define TX_LST_DO_90(M, arg, s, P, ...) TX_LST_DO_89(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 90, ##__VA_ARGS__)
#define TX_LST_DO_91(M, arg, s, P, ...) TX_LST_DO_90(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 91, ##__VA_ARGS__)
#define TX_LST_DO_92(M, arg, s, P, ...) TX_LST_DO_91(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 92, ##__VA_ARGS__)
#define TX_LST_DO_93(M, arg, s, P, ...) TX_LST_DO_92(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 93, ##__VA_ARGS__)
#define TX_LST_DO_94(M, arg, s, P, ...) TX_LST_DO_93(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 94, ##__VA_ARGS__)
#define TX_LST_DO_95(M, arg, s, P, ...) TX_LST_DO_94(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 95, ##__VA_ARGS__)
#define TX_LST_DO_96(M, arg, s, P, ...) TX_LST_DO_95(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 96, ##__VA_ARGS__)
#define TX_LST_DO_97(M, arg, s, P, ...) TX_LST_DO_96(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 97, ##__VA_ARGS__)
#define TX_LST_DO_98(M, arg, s, P, ...) TX_LST_DO_97(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 98, ##__VA_ARGS__)
#define TX_LST_DO_99(M, arg, s, P, ...) TX_LST_DO_98(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 99, ##__VA_ARGS__)
#define TX_LST_DO_100(M, arg, s, P, ...) LST_DO_99(M, arg, s, P, ##__VA_ARGS__)SELF s P(M, arg, 100, ##__VA_ARGS__)

#define TX_LST_DO__(N, M, arg, s, P, ...) TX_LST_DO_##N(M, arg, s, P, ##__VA_ARGS__)
#define TX_LST_DO_(...) TX_LST_DO__(__VA_ARGS__)
#define TX_LST_DO(M, arg, s, ...) \
  TX_LST_DO_(ARGS_NUM(__VA_ARGS__), M, arg, s, TX_PROC_ONE, ##__VA_ARGS__)

#define TX_PROC_ONE(M, arg, IDX, ...) M(SELECT(IDX, __VA_ARGS__), IDX, arg)

#define TX_LST_DO_CODE(M, arg, ...) TX_LST_DO(M, arg, (;), ##__VA_ARGS__)

#define OB_TX_UNIS_ENCODE(obj, IDX, arg)                                             \
  if (OB_SUCC(ret) && TX_IS_NEED_SER_OR_DE(arg, IDX)) {                              \
    if (OB_FAIL(NS_::encode(buf, buf_len, pos, obj))) {                              \
      RPC_WARN("encode object fail", "name", MSTR(obj), K(buf_len), K(pos), K(ret)); \
    }                                                                                \
  }

#define OB_TX_UNIS_DECODEx(obj, IDX, arg)                                             \
  if (OB_SUCC(ret) && TX_IS_NEED_SER_OR_DE(arg, IDX)) {                               \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                              \
      RPC_WARN("decode object fail", "name", MSTR(obj), K(data_len), K(pos), K(ret)); \
    }                                                                                 \
  }

#define OB_TX_UNIS_DECODE(obj, IDX, arg)                                              \
  if (OB_SUCC(ret) && TX_IS_NEED_SER_OR_DE(arg, IDX) && pos < data_len) {             \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                              \
      RPC_WARN("decode object fail", "name", MSTR(obj), K(data_len), K(pos), K(ret)); \
    }                                                                                 \
  }

#define OB_TX_UNIS_ADD_LEN(obj, IDX, arg) \
  if (TX_IS_NEED_SER_OR_DE(arg, IDX)) {   \
    len += NS_::encoded_length(obj);      \
  }

#define OB_TX_SERIALIZE_MEMBER(CLS, COMPAT_ARG, ...) \
  OB_DEF_SERIALIZE(CLS)                                        \
  {                                                                                     \
    int ret = OK_;                                                                      \
    UNF_UNUSED_SER;                                                                     \
    TX_SER_COMPAT_BYTES(COMPAT_ARG); \
      TX_LST_DO_CODE(OB_TX_UNIS_ENCODE, COMPAT_ARG, ##__VA_ARGS__);                     \
    return ret;                                                                         \
  }                                                                                     \
  OB_DEF_DESERIALIZE(CLS)                                      \
  {                                                                                     \
    int ret = OK_;                                                                      \
    UNF_UNUSED_DES;                                                                     \
    TX_DSER_COMPAT_BYTES(COMPAT_ARG); \
    TX_LST_DO_CODE(OB_TX_UNIS_DECODE, COMPAT_ARG, ##__VA_ARGS__);                       \
    return ret;                                                                         \
  }                                                                                     \
  OB_DEF_SERIALIZE_SIZE(CLS)                                   \
  {                                                                                     \
    int64_t len = 0;                                                                    \
    TX_SER_SIZE_COMPAT_BYTES(COMPAT_ARG); \
      TX_LST_DO_CODE(OB_TX_UNIS_ADD_LEN, COMPAT_ARG, ##__VA_ARGS__);                    \
    return len;                                                                         \
  }


} // namespace transaction
} // namespace oceanbase

#endif

// serialization 
