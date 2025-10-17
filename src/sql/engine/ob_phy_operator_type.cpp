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
#include "sql/engine/ob_operator_reg.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const char *get_phy_op_name(ObPhyOperatorType type, bool enable_rich_format /*false*/)
{
  const char *ret_char = NULL;
  static const char *ObPhyOpName[PHY_END + 2] = {
#define PHY_OP_DEF(type) #type,
#include "ob_phy_operator_type.h"
#undef PHY_OP_DEF
#define END ""
    END
#undef END
  };

  static const char *ObPhyVecOpName[PHY_END + 2] =
  {
#define PHY_OP_DEF(type) op_reg::ObOpTypeTraits<type>::vec_op_name_,
#include "ob_phy_operator_type.h"
#undef PHY_OP_DEF
#define END ""
    END
#undef END
  };

  if (type >= 0 && type < PHY_END + 2)
  {
    if (enable_rich_format && strlen(ObPhyVecOpName[type]) > 0) {
      ret_char = ObPhyVecOpName[type];
    } else {
      ret_char = ObPhyOpName[type];
    }
  } else {
    ret_char = "INVALID_OP";
  }
  return ret_char;
}

ObPhyOperatorTypeDescSet::ObPhyOperatorTypeDescSet()
{
#define PHY_OP_DEF(type) set_type_str(type, #type, op_reg::ObOpTypeTraits<type>::vec_op_name_);
#include "sql/engine/ob_phy_operator_type.h"
#undef PHY_OP_DEF
}

void ObPhyOperatorTypeDescSet::set_type_str(ObPhyOperatorType type, const char *type_str,
                                            const char *vec_name)
{
  if (OB_LIKELY(type >= PHY_INVALID && type < PHY_END)) {
    set_[type].name_ = type_str;
    set_[type].vec_name_ = vec_name;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid phy operator", K(type));
  }
}

const char *ObPhyOperatorTypeDescSet::get_type_str(ObPhyOperatorType type,
                                                   bool enable_rich_format /* false */) const
{
  const char *ret = "UNKNOWN_PHY_OP";
  if (OB_LIKELY(type >= PHY_INVALID && type < PHY_END)) {
    if (enable_rich_format && strlen(set_[type].vec_name_) > 0) {
      ret = set_[type].vec_name_;
    } else {
      ret = set_[type].name_;
    }
  }
  return ret;
}

static ObPhyOperatorTypeDescSet PHY_OP_TYPE_DESC_SET;
const char *ob_phy_operator_type_str(ObPhyOperatorType type, bool enable_rich_format /*false*/)
{
  return PHY_OP_TYPE_DESC_SET.get_type_str(type, enable_rich_format);
}

ObPhyOperatorType get_phy_type_from_name(const char *name, uint64_t length,
                                         bool &enable_rich_format)
{
  enable_rich_format = false;
  ObPhyOperatorType ret_type = PHY_INVALID;
  if (!name) {
    return PHY_INVALID;
  } else if (length < 7) {
    enable_rich_format = false;
  } else if (strncmp(name, "PHY_VEC_", 7) == 0) {
    enable_rich_format = true;
  }
  for (int type = PHY_INVALID; type < PHY_END; ++type) {
    const char *phy_name = ob_phy_operator_type_str((ObPhyOperatorType)type, enable_rich_format);
    uint64_t cmp_len = std::min(length, strlen(phy_name));
    if (strncmp(phy_name, name, cmp_len) == 0) {
      ret_type = (ObPhyOperatorType)type;
      break;
    }
  }
  return ret_type;
}

}
}
