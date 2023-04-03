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

#ifndef OCEANBASE_ROOTSERVER_FAKE_ZONE_MANAGER_H_
#define OCEANBASE_ROOTSERVER_FAKE_ZONE_MANAGER_H_

#include "rootserver/ob_zone_manager.h"

namespace oceanbase
{
namespace rootserver
{

class FakeZoneManager : public ObZoneManager
{
public:
  FakeZoneManager() : config_version_(1) {}
  ~FakeZoneManager() {}

  void init_zone_manager(const int64_t version, int64_t zone_cnt);

  virtual int generate_next_global_broadcast_version()
  {
    // global_info_.global_broadcast_version_.value_ ++;
    return common::OB_SUCCESS;
  }

  virtual int start_zone_merge(const common::ObZone &zone);
  virtual int finish_zone_merge(const common::ObZone &zone, const int64_t version,
      const int64_t all_merged_version);
  virtual int set_zone_merge_timeout(const common::ObZone &zone);

  virtual int set_frozen_info(const int64_t frozen_version, const int64_t frozen_time)
  {
    // global_info_.frozen_version_.value_ = frozen_version;
    // global_info_.try_frozen_version_.value_ = frozen_time;
    UNUSED(frozen_version);
    UNUSED(frozen_time);
    return common::OB_SUCCESS;
  }
  virtual int set_try_frozen_version(const int64_t try_frozen_version)
  {
    // global_info_.try_frozen_version_.value_ = try_frozen_version;
    UNUSED(try_frozen_version);
    return common::OB_SUCCESS;
  }

  virtual int set_zone_merging(const common::ObZone &);

  virtual int get_config_version(int64_t &config_version) const
  {
    config_version = config_version_;
    return common::OB_SUCCESS;
  }

  virtual int update_config_version(const int64_t ver)
  {
    config_version_ = ver;
    return common::OB_SUCCESS;
  }

  virtual int get_merge_list(common::ObIArray<common::ObZone> &) const
  {
    return common::OB_SUCCESS;
  }
  virtual int set_merge_list(const common::ObIArray<common::ObZone> &)
  {
    return common::OB_SUCCESS;
  }

  virtual int reset_global_merge_status()
  {
    return common::OB_SUCCESS;
  }
  virtual int try_update_global_last_merged_version()
  {
    // global_info_.last_merged_version_.value_ = global_info_.global_broadcast_version_.value_;
    return common::OB_SUCCESS;
  }

  virtual int set_warm_up_start_time(const int64_t time_ts)
  {
    UNUSED(time_ts);
    // global_info_.warm_up_start_time_.value_ = time_ts;
    return common::OB_SUCCESS;
  }

  share::ObZoneInfo *locate_zone(const common::ObZone &zone);

  int64_t config_version_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FAKE_ZONE_MANAGER_H_
