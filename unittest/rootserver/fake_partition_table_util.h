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

#ifndef OCEANBASE_ROOTSERVER_FAKE_PARTITION_TABLE_UTIL_H_
#define OCEANBASE_ROOTSERVER_FAKE_PARTITION_TABLE_UTIL_H_

#include <utility>
#include "rootserver/ob_partition_table_util.h"
#include "rootserver/ob_zone_manager.h"

namespace oceanbase
{
namespace rootserver
{
class FakePartitionTableUtil : public ObPartitionTableUtil
{
public:
  FakePartitionTableUtil(ObZoneManager &cm) : cm_(cm) {}

  virtual int check_merge_progress(const volatile bool &stop, const int64_t version,
      ObZoneMergeProgress &all_progress, bool &all_merged)
  {
    UNUSED(version);
    int ret = common::OB_SUCCESS;
    all_progress.reset();
    int64_t zone_count = 0;
    if (OB_FAIL(cm_.get_zone_count(zone_count))) {
      RS_LOG(WARN, "get_zone_count failed", K(ret));
    }
    for (int64_t i = 0; !stop && common::OB_SUCCESS == ret && i < zone_count; ++i) {
      share::ObZoneInfo info;
      cm_.get_zone(i, info);
      MergeProgress progress;
      progress.zone_ = info.zone_;
      progress.merged_data_size_ = 100;
      progress.merged_partition_cnt_ = 100;
      all_progress.push_back(progress);
    }
    all_merged = true;
    return ret;
  }

  virtual int set_leader_backup_flag(const volatile bool &, const bool , ObPartitionTableUtil::ObLeaderInfoArray *)
  {
    return common::OB_SUCCESS;
  }

private:
  ObZoneManager &cm_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FAKE_PARTITION_TABLE_UTIL_H_
