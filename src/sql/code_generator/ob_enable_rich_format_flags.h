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

#ifndef CG_ENABLE_RICH_FORMAT_FLAGS
#define CG_ENABLE_RICH_FORMAT_FLAGS

#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql
{

struct EnableOpRichFormat
{
  EnableOpRichFormat(const int64_t flags, bool use_rich_format): use_rich_format_(use_rich_format)
  {
    MEMSET(data_, 0, DATA_BYTES);
    ObBitVector *disable_flags = to_bit_vector(data_);
    for (int i = 0; i < PHY_END; i++) {
      bool disabled = ((PHY_OPS_[i].disable_flag_ & flags) != 0);
      if (disabled) { disable_flags->set(i); }
      if (disabled && static_cast<ObPhyOperatorType>(i) == PHY_JOIN_FILTER) {
        disable_flags->set(PHY_HASH_JOIN);
      }
      if (disabled
          && ((static_cast<ObPhyOperatorType>(i) == PHY_TEMP_TABLE_ACCESS)
              || (static_cast<ObPhyOperatorType>(i) == PHY_TEMP_TABLE_INSERT)
              || (static_cast<ObPhyOperatorType>(i) == PHY_TEMP_TABLE_TRANSFORMATION))) {
        disable_flags->set(PHY_TEMP_TABLE_ACCESS);
        disable_flags->set(PHY_TEMP_TABLE_INSERT);
        disable_flags->set(PHY_TEMP_TABLE_TRANSFORMATION);
      }
    }
  }
  inline bool check(ObPhyOperatorType phy_op) const
  {
    return use_rich_format_
           && !to_bit_vector(data_)->at(static_cast<int64_t>(phy_op));
  }
  inline bool check_plan() const
  {
    return use_rich_format_;
  }

private:
  static const int64_t DATA_BYTES = (static_cast<int64_t>(PHY_END) + CHAR_BIT - 1) / CHAR_BIT;
private:
  struct PhyOpInfo
  {
    PhyOpInfo(): name_(nullptr), disable_flag_(0) {}
    PhyOpInfo(const char *name, int64_t disable_flag): name_(name), disable_flag_(disable_flag) {}
    const char *name_;
    int64_t disable_flag_;
  };
  static PhyOpInfo PHY_OPS_[PHY_END];
private:
  friend class InitPhyOpInfo;
  friend class DisableOpRichFormatHint;
private:
  char data_[DATA_BYTES];
  bool use_rich_format_;
};

}
}
#endif // CG_ENABLE_RICH_FORMAT_FLAGS