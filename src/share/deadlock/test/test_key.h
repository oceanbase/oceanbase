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

#ifndef UNITTEST_DEADLOCK_TEST_KEY_H
#define UNITTEST_DEADLOCK_TEST_KEY_H
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_errno.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
namespace unittest {

class ObDeadLockTestIntKey {
  OB_UNIS_VERSION(1);
public:
  ObDeadLockTestIntKey() : v_(-1) {};
  ObDeadLockTestIntKey(int v) : v_(v) {};
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(v));
  int get_value() const { return v_; }
private:
  int v_;
};

class ObDeadLockTestDoubleKey {
  OB_UNIS_VERSION(1);
public:
  ObDeadLockTestDoubleKey() : v_(0.0) {};
  ObDeadLockTestDoubleKey(double v) : v_(v) {};
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(v));
private:
  double v_;
};

inline int ObDeadLockTestIntKey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
  return common::serialization::encode(buf, buf_len, pos, v_);
}
inline int ObDeadLockTestIntKey::deserialize(const char *buf, const int64_t data_len, int64_t &pos) {
  return common::serialization::decode(buf, data_len, pos, v_);
}
inline int64_t ObDeadLockTestIntKey::get_serialize_size(void) const {
  return common::serialization::encoded_length(v_);
}

inline int ObDeadLockTestDoubleKey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
  return common::serialization::encode(buf, buf_len, pos, v_);
}
inline int ObDeadLockTestDoubleKey::deserialize(const char *buf, const int64_t data_len, int64_t &pos) {
  return common::serialization::decode(buf, data_len, pos, v_);
}
inline int64_t ObDeadLockTestDoubleKey::get_serialize_size(void) const {
  return common::serialization::encoded_length(v_);
}

}
}
#endif