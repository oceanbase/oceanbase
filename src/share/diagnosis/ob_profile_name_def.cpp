/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/diagnosis/ob_profile_name_def.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"
#include "src/sql/engine/ob_phy_operator_type.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::sql;
namespace oceanbase
{

namespace common
{

ObProfileNameSet::ObProfileNameSet()
{
#define OP_PROFILE_NAME_DEF(enum_type)                                                             \
  static_assert(sizeof(#enum_type) <= 38, "profile name length is bigger than 38");
#include "share/diagnosis/ob_profile_name_def.h"
#undef OP_PROFILE_NAME_DEF

#define OTHER_PROFILE_NAME_DEF(enum_type, name)                                                    \
  static_assert(sizeof(name) <= 32, "profile name length is bigger than 32");
#include "share/diagnosis/ob_profile_name_def.h"
#undef OTHER_PROFILE_NAME_DEF

#define OTHER_PROFILE_NAME_DEF(enum_type, name)                                                    \
  set_profile_type_name(ObProfileId::enum_type, name);
#include "share/diagnosis/ob_profile_name_def.h"
#undef OTHER_PROFILE_NAME_DEF

  static_assert((static_cast<int>(ObProfileId::PHY_END) == ObPhyOperatorType::PHY_END),
                "please keeping ObPhyOperatorType and ObProfileId in sync");
}

static ObProfileNameSet PROFILE_NAME_DESC_SET;

void ObProfileNameSet::set_profile_type_name(ObProfileId type, const char *name)
{
  if (OB_UNLIKELY(type < ObProfileId::OTHER_PROFILE_NAME_BEGIN)) {
  } else if (OB_LIKELY(type >= ObProfileId::OTHER_PROFILE_NAME_BEGIN
                       && type < ObProfileId::OTHER_PROFILE_NAME_END)) {
    int offset =
        static_cast<int>(type) - static_cast<int>(ObProfileId::OTHER_PROFILE_NAME_BEGIN);
    set_[offset].name_ = name;
  } else {
    COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid profile name", K(type));
  }
}

const char *ObProfileNameSet::get_profile_name(ObProfileId type, bool enable_rich_format)
{
  const char *profile_name = "UNKNOWN_PROFILE_NAME";
  if (type >= ObProfileId::PHY_INVALID && type < ObProfileId::PHY_END) {
    profile_name = sql::get_phy_op_name(static_cast<ObPhyOperatorType>(type), enable_rich_format);
  } else if (type >= ObProfileId::OTHER_PROFILE_NAME_BEGIN
             && type < ObProfileId::OTHER_PROFILE_NAME_END) {
    int offset = static_cast<int>(type) - static_cast<int>(ObProfileId::OTHER_PROFILE_NAME_BEGIN);
    profile_name = PROFILE_NAME_DESC_SET.set_[offset].name_;
  }
  return profile_name;
}

} // namespace common
} // namespace oceanbase
