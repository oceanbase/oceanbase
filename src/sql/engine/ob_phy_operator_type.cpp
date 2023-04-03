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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_phy_operator_type.h"
#include "lib/atomic/ob_atomic.h"
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

  const char *get_phy_op_name(ObPhyOperatorType type) {
    const char* ret_char = NULL;
    static const char *ObPhyOpName[PHY_END + 2] =
      {
#define PHY_OP_DEF(type) #type,
#include "ob_phy_operator_type.h"
#undef PHY_OP_DEF
#define END ""
        END
#undef END
      };

    if (type >= 0 && type < PHY_END + 2) {
      ret_char = ObPhyOpName[type];
    } else {
      ret_char = "INVALID_OP";
    }
    return ret_char;
  }

void ObPhyOperatorTypeDescSet::set_type_str(ObPhyOperatorType type, const char *type_str)
{
  if (OB_LIKELY(type >= PHY_INVALID && type < PHY_END)) {
    set_[type].name_ = type_str;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid phy operator", K(type));
  }
}

const char *ObPhyOperatorTypeDescSet::get_type_str(ObPhyOperatorType type) const
{
  const char *ret = "UNKNOWN_PHY_OP";
  if (OB_LIKELY(type >= PHY_INVALID && type < PHY_END)) {
    ret = set_[type].name_;
  }
  return ret;
}

static ObPhyOperatorTypeDescSet PHY_OP_TYPE_DESC_SET;
const char *ob_phy_operator_type_str(ObPhyOperatorType type)
{
  return PHY_OP_TYPE_DESC_SET.get_type_str(type);
}

}
}
