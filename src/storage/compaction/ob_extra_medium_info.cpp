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

#include "storage/compaction/ob_extra_medium_info.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace compaction
{
ObExtraMediumInfo::ObExtraMediumInfo()
  : compat_(MEDIUM_LIST_VERSION),
    last_compaction_type_(0),
    wait_check_flag_(0),
    reserved_(0),
    last_medium_scn_(0)
{
}

ObExtraMediumInfo::ObExtraMediumInfo(const ObExtraMediumInfo &other)
{
  if (this != &other) {
    compat_ = other.compat_;
    last_compaction_type_ = other.last_compaction_type_;
    wait_check_flag_ = other.wait_check_flag_;
    reserved_ = other.reserved_;
    last_medium_scn_ = other.last_medium_scn_;
  }
}

ObExtraMediumInfo &ObExtraMediumInfo::operator=(const ObExtraMediumInfo &other)
{
  if (this != &other) {
    compat_ = other.compat_;
    last_compaction_type_ = other.last_compaction_type_;
    wait_check_flag_ = other.wait_check_flag_;
    reserved_ = other.reserved_;
    last_medium_scn_ = other.last_medium_scn_;
  }
  return *this;
}

void ObExtraMediumInfo::reset()
{
  compat_ = MEDIUM_LIST_VERSION;
  last_compaction_type_ = 0;
  wait_check_flag_ = 0;
  reserved_ = 0;
  last_medium_scn_ = 0;
}

int ObExtraMediumInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      info_,
      last_medium_scn_);

  return ret;
}

int ObExtraMediumInfo::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      info_,
      last_medium_scn_);

  return ret;
}

int64_t ObExtraMediumInfo::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      info_,
      last_medium_scn_);

  return len;
}
} // namespace compaction
} // namespace oceanbase