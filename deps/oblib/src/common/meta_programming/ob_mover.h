/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_MOVER_H
#define DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_MOVER_H

#include <type_traits>
#include "ob_type_traits.h"
namespace oceanbase
{
namespace common
{
namespace meta
{

// this structure is desgined for indicate that the wrappered object could be moved rather than copied.
// rvalue is not allowed in oceanbase.
template <typename  T>
struct ObMover {
  ObMover(T &obj) : obj_(obj) {}
  T &get_object() { return obj_; }
  // if T has to_string, ObMover support to_string also
  template <typename T2 = T, ENABLE_IF_HAS_TO_STRING(T2)>
  int64_t to_string(char *buf, const int64_t buf_len) const {
    return obj_.to_string(buf, buf_len);
  }
  // if T serializable, ObMover is serializable also
  template <typename T2 = T, ENABLE_IF_SERIALIZEABLE(T2)>
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    return obj_.serialize(buf, buf_len, pos);
  }
  template <typename T2 = T, ENABLE_IF_SERIALIZEABLE(T2)>
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) {
    return obj_.deserialize(buf, buf_len, pos);
  }
  template <typename T2 = T, ENABLE_IF_SERIALIZEABLE(T2)>
  int64_t get_serialize_size() const {
    return obj_.get_serialize_size();
  }
private:
  T &obj_;
};

}
}
}
#endif