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

#define USING_LOG_PREFIX STORAGE

#include "ob_major_mv_merge_info.h"
#include "share/ob_table_range.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObMajorMVMergeInfo::ObMajorMVMergeInfo()
  : major_mv_merge_scn_(share::ObScnRange::MIN_SCN),
    major_mv_merge_scn_safe_calc_(share::ObScnRange::MIN_SCN),
    major_mv_merge_scn_publish_(share::ObScnRange::MIN_SCN)
{
}

void ObMajorMVMergeInfo::reset()
{
  major_mv_merge_scn_ = share::ObScnRange::MIN_SCN;
  major_mv_merge_scn_safe_calc_ = share::ObScnRange::MIN_SCN;
  major_mv_merge_scn_publish_ = share::ObScnRange::MIN_SCN;
}

bool ObMajorMVMergeInfo::is_valid() const
{
  return major_mv_merge_scn_.is_valid()
      && major_mv_merge_scn_safe_calc_.is_valid()
      && major_mv_merge_scn_publish_.is_valid();
}

OB_SERIALIZE_MEMBER(ObMajorMVMergeInfo, major_mv_merge_scn_, major_mv_merge_scn_safe_calc_, major_mv_merge_scn_publish_);

}
}
