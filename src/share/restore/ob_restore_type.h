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
    FULL = 0,
    // just restore minor and clog, major macro blocks are in remote reference state
    QUICK = 1,
    RESTORE_TYPE_MAX
  };

public:
  ObRestoreType() : type_(FULL) {}
  ~ObRestoreType() = default;
  explicit ObRestoreType(const Type &type) : type_(type) {}
  explicit ObRestoreType(const ObString &str);
  ObRestoreType &operator=(const ObRestoreType &restore_type);
  ObRestoreType &operator=(const Type &type);
  constexpr operator Type() const { return type_; }
  constexpr bool is_valid() const { return type_ >= Type::FULL && type_ < Type::RESTORE_TYPE_MAX;}
  bool is_quick_restore() const { return Type::QUICK == type_; }
  bool is_full_restore() const { return Type::FULL == type_; }
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
  const char *to_str() const;
  TO_STRING_KV("restore_type", to_str());

private:
  Type type_;
};

static const ObRestoreType FULL_RESTORE_TYPE(ObRestoreType::Type::FULL);
static const ObRestoreType QUICK_RESTORE_TYPE(ObRestoreType::Type::QUICK);

}
}

#endif