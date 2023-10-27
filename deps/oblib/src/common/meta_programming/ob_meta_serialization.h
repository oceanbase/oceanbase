/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_SERIALIZATION_H
#define DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_SERIALIZATION_H

#include "ob_meta_define.h"
#include "lib/ob_errno.h"
#include "ob_type_traits.h"

namespace oceanbase
{
namespace common
{
namespace meta
{

template <typename T>
class MetaSerializer
{
  template <typename T2 = T,
            typename std::enable_if<!OB_TRAIT_SERIALIZEABLE(T2) &&
                                    !OB_TRAIT_DEEP_SERIALIZEABLE(T2), bool>::type = true>
  void requires() {
    static_assert(!(!OB_TRAIT_SERIALIZEABLE(T2) && !OB_TRAIT_DEEP_SERIALIZEABLE(T2)),
                  "your type is not serializable");
  }
public:
  MetaSerializer(ObIAllocator &alloc, const T &data)
  : alloc_(alloc),
  data_(const_cast<T &>(data)) {}
  MetaSerializer(const MetaSerializer &rhs)
  : alloc_(rhs.alloc_),
  data_(rhs.data_) {}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    return data_.serialize(buf, buf_len, pos);
  }
  template <typename T2 = T,
            typename std::enable_if<OB_TRAIT_SERIALIZEABLE(T2), bool>::type = true>
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
  {
    return data_.deserialize(buf, buf_len, pos);
  }
  template <typename T2 = T,
            typename std::enable_if<!OB_TRAIT_SERIALIZEABLE(T2) &&
                                    OB_TRAIT_DEEP_SERIALIZEABLE(T2), bool>::type = true>
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
  {
    return data_.deserialize(alloc_, buf, buf_len, pos);
  }
  int64_t get_serialize_size() const { return data_.get_serizalize_size(); }
private:
  ObIAllocator &alloc_;
  T &data_;
};

}
}
}
#endif