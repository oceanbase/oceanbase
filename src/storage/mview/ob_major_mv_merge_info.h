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

#ifndef OCEABASE_MAJOR_MV_MERGE_INFO_
#define OCEABASE_MAJOR_MV_MERGE_INFO_

#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

struct ObMajorMVMergeInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObMajorMVMergeInfo();
  ~ObMajorMVMergeInfo() = default;
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(major_mv_merge_scn), K_(major_mv_merge_scn_safe_calc), K_(major_mv_merge_scn_publish));

  share::SCN major_mv_merge_scn_;
  share::SCN major_mv_merge_scn_safe_calc_;
  share::SCN major_mv_merge_scn_publish_;
};


}
}

#endif
