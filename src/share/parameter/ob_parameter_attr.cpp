/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
