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

#include "storage/tx/ob_tx_serialization.h"

namespace oceanbase
{
namespace transaction
{

  // const static uint8_t BASE_VALID_BIT = 1;
  // const static uint8_t NEXT_BYTE_VALID = BASE_VALID_BIT << 7;

const uint8_t ObTxSerCompatByte::BASE_VALID_BIT = 1;
const uint8_t ObTxSerCompatByte::NEXT_BYTE_VALID = BASE_VALID_BIT << 7;
const uint8_t ObTxSerCompatByte::BASE_VALID_BYTE = UINT8_MAX;

const uint8_t ObTxSerCompatByte::MAX_OBJECT_COUNT_PER_BYTE = 7;
const uint8_t ObTxSerCompatByte::MAX_BIT_COUNT_PER_BYTE = 8;


void ObTxSerCompatByte::clear_compat_bytes_()
{
  memset(compat_bytes_,0 , MAX_COMPAT_BYTES_COUNT);
}

void ObTxSerCompatByte::reset()
{
  total_obj_cnt_ = 0;
  total_byte_cnt_ = 0;
  clear_compat_bytes_();
}

int ObTxSerCompatByte::init(int64_t total_object_count)
{
  int ret = OB_SUCCESS;

  if (total_obj_cnt_ > 0) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init compat_bytes_ twice", K(ret), KPC(this));
  } else if (total_object_count < 0 || total_obj_cnt_ > 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(total_object_count), KPC(this));
  } else {
    const int64_t total_byte_cnt = total_object_count / MAX_OBJECT_COUNT_PER_BYTE
                                   + (total_object_count % MAX_OBJECT_COUNT_PER_BYTE == 0 ? 0 : 1);
    total_obj_cnt_ = total_object_count;
    init_all_bytes_valid_(total_byte_cnt);
  }
  return ret;
}

int ObTxSerCompatByte::set_all_member_need_ser()
{
  int ret = OB_SUCCESS;

  if (total_obj_cnt_ <= 0 || total_byte_cnt_ <= 0) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "compat bytes have not been inited", K(ret), KPC(this));
  } else {
    init_all_bytes_valid_(total_byte_cnt_);
  }

  return ret;
}

int ObTxSerCompatByte::set_object_flag(int64_t object_index, bool is_valid)
{
  int ret = OB_SUCCESS;

  int64_t byte_index = -1;
  uint8_t bit_index = 0;

  if (OB_FAIL(cal_object_location_(object_index, byte_index, bit_index))) {
    TRANS_LOG(WARN, "cal object location failed", K(ret));
  } else if (OB_FALSE_IT(set_object_flag_(byte_index, bit_index, is_valid))) {
  }

  // TRANS_LOG(INFO, "set object flag", K(object_index), K(is_valid), K(byte_index), K(bit_index),
  //           K(compat_bytes_[byte_index]));
  return ret;
}

bool ObTxSerCompatByte::is_object_valid(int64_t object_index) const
{
  int ret = OB_SUCCESS;
  bool is_valid = false;

  int64_t byte_index = -1;
  uint8_t bit_index = 0;

  if (OB_FAIL(cal_object_location_(object_index, byte_index, bit_index))) {
    TRANS_LOG(WARN, "cal object location failed", K(ret));
    if (OB_INVALID_ARGUMENT == ret) {
      // ret = OB_SUCCESS;
      is_valid = false;
    }
  } else if (OB_FALSE_IT(is_object_valid_(byte_index, bit_index, is_valid))) {
  }

  return is_valid;
}

int ObTxSerCompatByte::cal_object_location_(int64_t object_index,
                                            int64_t &byte_index,
                                            uint8_t &bit_index) const
{
  int ret = OB_SUCCESS;

  if (object_index < 1 || object_index > total_obj_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "object index may be invalid", K(ret), K(object_index), KPC(this));
  } else {
    byte_index = object_index / MAX_OBJECT_COUNT_PER_BYTE - 1;
    bit_index = object_index % MAX_OBJECT_COUNT_PER_BYTE;
    if (bit_index == 0) {
      bit_index = MAX_OBJECT_COUNT_PER_BYTE - 1;
    } else {
      bit_index -= 1;
      byte_index += 1;
    }
  }
  return ret;
}

void ObTxSerCompatByte::set_object_flag_(int64_t byte_index, uint8_t bit_index, bool is_valid)
{
  uint8_t bit_flag = BASE_VALID_BIT << bit_index;

  if (byte_index > total_byte_cnt_) {
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid byte index", K(byte_index), K(bit_index), K(is_valid), KPC(this));
  } else if (is_valid) {
    compat_bytes_[byte_index] |= bit_flag;
  } else {
    compat_bytes_[byte_index] &= ~bit_flag;
  }
}

void ObTxSerCompatByte::is_object_valid_(int64_t byte_index,
                                         uint8_t bit_index,
                                         bool &is_valid) const
{
  uint8_t bit_check_flag = BASE_VALID_BIT << bit_index;

  if (byte_index > total_byte_cnt_) {
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid byte index", K(byte_index), K(bit_index), K(is_valid), KPC(this));
  } else {
    is_valid = (compat_bytes_[byte_index] & bit_check_flag) != 0;
  }
}

int ObTxSerCompatByte::init_all_bytes_valid_(const int64_t total_byte_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t remain_bits = total_obj_cnt_ % MAX_OBJECT_COUNT_PER_BYTE;

  if (OB_FALSE_IT(clear_compat_bytes_())) {

  } else {

    total_byte_cnt_ = total_byte_cnt;
    
    for (int64_t i = 0; i < total_byte_cnt && OB_SUCC(ret); i++) {

      compat_bytes_[i] = BASE_VALID_BYTE;

      if (i < total_byte_cnt - 1) {
        // do nothing
      } else if (remain_bits == 0) {
        compat_bytes_[i] &= ~NEXT_BYTE_VALID;
      } else {
        for (uint8_t j = remain_bits; j < MAX_BIT_COUNT_PER_BYTE; j++) {
          set_object_flag_(i, j, false);
        }
      }
    }

    if (OB_FAIL(ret)) {
      total_byte_cnt_ = 0;
    }
  }
  return ret;
}

int ObTxSerCompatByte::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < total_byte_cnt_; i++) {
    if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, compat_bytes_[i]))) {
      TRANS_LOG(WARN, "encode compat byte", K(ret));
    }
  }

  return ret;
}

int ObTxSerCompatByte::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  uint8_t compat_byte = 0;

  int64_t loop_cnt = 0;

  reset();
  do {
    if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t *)(&compat_byte)))) {
      TRANS_LOG(WARN, "decode compat_byte failed", K(ret), K(data_len), K(pos), K(compat_byte),
                K(loop_cnt));
    } else if (OB_FALSE_IT(compat_bytes_[loop_cnt] = compat_byte)) {
      // do nothing
    }
    loop_cnt++;
  } while ((compat_byte & NEXT_BYTE_VALID) != 0 && OB_SUCC(ret));

  if (OB_SUCC(ret)) {
    total_byte_cnt_ = loop_cnt;
    total_obj_cnt_ = total_byte_cnt_ * MAX_OBJECT_COUNT_PER_BYTE;
  }

  return ret;
}

int64_t ObTxSerCompatByte::get_serialize_size(void) const { return total_byte_cnt_; }


} // namespace transacation
} // namespace oceanbase
