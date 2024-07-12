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

#ifndef OCEANBASE_TRANSACTION_OB_TX_SEQ_H
#define OCEANBASE_TRANSACTION_OB_TX_SEQ_H

#include "lib/ob_define.h"              // for int64_t
#include "lib/hash_func/murmur_hash.h"  // for murmurhash()
#include "lib/utility/ob_macro_utils.h" // for OB_ASSERT
#include "lib/utility/ob_print_utils.h" // for DECLARE_TO_STRING

namespace oceanbase
{
namespace transaction
{

// Transaction used Sequence number,
// since 4.3 the Sequence number split into two parts:
//   part 1: sequence number offset against transaction start
//   part 2: id of parallel write branch
class ObTxSEQ
{
public:
  ObTxSEQ() : raw_val_(0) {}
  explicit ObTxSEQ(int64_t seq, int16_t branch):
    branch_(branch), seq_(seq), n_format_(true), _sign_(0)
  {
    OB_ASSERT(seq > 0 && seq >> 62 == 0);
    OB_ASSERT(branch >= 0);
  }
private:
  explicit ObTxSEQ(int64_t raw_v): raw_val_(raw_v) {}
public:
  // old version builder
  static ObTxSEQ mk_v0(int64_t seq_v)
  {
    OB_ASSERT(seq_v > 0);
    return ObTxSEQ(seq_v);
  }
  static const ObTxSEQ &INVL() { static ObTxSEQ v; return v; }
  static const ObTxSEQ &MIN_VAL() { static ObTxSEQ v(1, 0); return v; }
  static const ObTxSEQ &MAX_VAL() { static ObTxSEQ v(INT64_MAX); return v; }
  void reset() { raw_val_ = 0; }
  bool is_valid() const { return raw_val_ > 0; }
  bool is_min() const { return *this == MIN_VAL(); }
  bool is_max() const { return *this == MAX_VAL(); }
  ObTxSEQ clone_with_seq(int64_t seq_abs, int64_t seq_base) const
  {
    ObTxSEQ n = *this;
    if (n_format_) {
      n.seq_ = seq_abs - seq_base;
    } else {
      n.seq_v0_ = seq_abs;
    }
    return n;
  }
  bool operator>(const ObTxSEQ &b) const
  {
    return n_format_ ? seq_ > b.seq_ : seq_v0_ > b.seq_v0_;
  }
  bool operator>=(const ObTxSEQ &b) const
  {
    return *this > b || (n_format_ ? (seq_ == b.seq_) : (seq_v0_ == b.seq_v0_));
  }
  bool operator<(const ObTxSEQ &b) const
  {
    return b > *this;
  }
  bool operator<=(const ObTxSEQ &b) const
  {
    return b >= *this;
  }
  bool operator==(const ObTxSEQ &b) const
  {
    return raw_val_ == b.raw_val_;
  }
  bool operator!=(const ObTxSEQ &b) const
  {
    return !(*this == b);
  }
  ObTxSEQ &operator++() {
    if (n_format_) { ++seq_; } else { ++seq_v0_; }
    return *this;
  }
  ObTxSEQ operator+(int n) const {
    int64_t s = n_format_ ? seq_ + n : seq_v0_ + n;
    return n_format_ ? ObTxSEQ(s, branch_) : ObTxSEQ::mk_v0(s);
  }
  uint64_t hash() const { return murmurhash(&raw_val_, sizeof(raw_val_), 0); }
  // atomic incremental update
  int64_t inc_update(const ObTxSEQ &b) { return common::inc_update(&raw_val_, b.raw_val_); }
  uint64_t cast_to_int() const { return raw_val_; }
  static ObTxSEQ cast_from_int(int64_t seq) { return ObTxSEQ(seq); }
  bool support_branch() const { return n_format_; }
  // return sequence number
  int64_t get_seq() const { return n_format_ ? seq_ : seq_v0_; }
  int16_t get_branch() const { return n_format_ ? branch_ : 0; }
  ObTxSEQ &set_branch(int16_t branch) { branch_ = branch; return *this; }
  // atomic Load/Store
  void atomic_reset() { ATOMIC_SET(&raw_val_, 0); }
  ObTxSEQ atomic_load() const { int64_t v = ATOMIC_LOAD(&raw_val_); ObTxSEQ s; s.raw_val_ = v; return s; }
  void atomic_store(ObTxSEQ seq) { ATOMIC_STORE(&raw_val_, seq.raw_val_); }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  union {
    int64_t raw_val_;
    union {
      struct { // v0, old_version
        uint64_t seq_v0_     :62;
      };
      struct { // new_version
        uint16_t branch_     :15;
        uint64_t seq_        :47;
        bool     n_format_   :1;
        int     _sign_       :1;
      };
    };
  };
};
static_assert(sizeof(ObTxSEQ) == sizeof(int64_t), "ObTxSEQ should sizeof(int64_t)");

inline int64_t ObTxSEQ::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (raw_val_ == INT64_MAX) {
    BUF_PRINTF("MAX");
  } else if (_sign_ == 0 && n_format_) {
    J_OBJ_START();
    J_KV(K_(branch), "seq", seq_);
    J_OBJ_END();
  } else {
    BUF_PRINTF("%lu", raw_val_);
  }
  return pos;
}

inline OB_DEF_SERIALIZE_SIMPLE(ObTxSEQ)
{
  return serialization::encode_vi64(buf, buf_len, pos, raw_val_);
}

inline OB_DEF_SERIALIZE_SIZE_SIMPLE(ObTxSEQ)
{
  return serialization::encoded_length_vi64(raw_val_);
}

inline OB_DEF_DESERIALIZE_SIMPLE(ObTxSEQ)
{
  return serialization::decode_vi64(buf, data_len, pos, &raw_val_);
}

}
}
#endif