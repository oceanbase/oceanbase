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

#ifndef OCENABASE_SHARE_OB_RESTORE_TYPE_H
#define OCENABASE_SHARE_OB_RESTORE_TYPE_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

class ObRestoreType final
{
public:
  enum Type : uint8_t
  {
    // restore whole data, default retore type
    NORMAL_RESTORE = 0,
    // just restore minor and clog, major macro blocks are in remote reference state
    QUICK_RESTORE = 1,
    RESTORE_TYPE_MAX
  };

public:
  ObRestoreType() : type_(NORMAL_RESTORE) {}
  ~ObRestoreType() = default;
  explicit ObRestoreType(const Type &type);
  ObRestoreType &operator=(const ObRestoreType &restore_type);
  ObRestoreType &operator=(const Type &type);
  constexpr operator Type() const { return type_; }
  constexpr bool is_valid() const { return type_ >= Type::NORMAL_RESTORE && type_ < Type::RESTORE_TYPE_MAX;}
  bool is_quick_restore() const { return Type::QUICK_RESTORE == type_; }
  bool is_normal_restore() const { return Type::NORMAL_RESTORE == type_; }
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(type));

private:
  Type type_;
};


}
}

#endif