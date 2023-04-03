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

#include "share/parameter/ob_parameter_attr.h"

namespace oceanbase {
namespace common {

#define _ATTR_STR(enum_str) #enum_str
#define _ATTR(enum_name)                                                       \
  [enum_name] = _ATTR_STR(enum_name)

#define DEF_ATTR_VALUES(ATTR_CLS, args...)                                     \
const char * ATTR_CLS::VALUES[] = {                                            \
  LST_DO(_ATTR, (,), args)                                                     \
}

DECL_ATTR_LIST(DEF_ATTR_VALUES);

bool ObParameterAttr::is_static() const
{
  return edit_level_ == EditLevel::STATIC_EFFECTIVE;
}

bool ObParameterAttr::is_readonly() const
{
  return edit_level_ == EditLevel::READONLY;
}

bool ObParameterAttr::is_invisible() const
{
  return visible_level_ == VisibleLevel::INVISIBLE;
}

} // common
} // oceanbase
