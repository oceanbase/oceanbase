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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_HELPER_H_
#define OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_HELPER_H_

#include "rootserver/freeze/window/ob_window_compaction_rpc_define.h"
#include "share/ob_zone_merge_info.h"

namespace oceanbase
{
namespace rootserver
{

struct ObWindowParameters final
{
public:
  using ObZoneMergeInfo = share::ObZoneMergeInfo;
  using ObGlobalMergeInfo = share::ObGlobalMergeInfo;
public:
  ObWindowParameters();
  ~ObWindowParameters() { reset(); }
  void reset();
  int init(const uint64_t tenant_id, const ObGlobalMergeInfo &global_info);
  int refresh_enable_window_compaction();
  int refresh_window_duration_us();
  int check_need_do_window_compaction(bool &need_do_window_compaction) const;
  OB_INLINE bool is_valid() const { return is_inited_
                                        && OB_INVALID_TENANT_ID != tenant_id_
                                        && merge_status_ >= ObZoneMergeInfo::MERGE_STATUS_IDLE
                                        && merge_status_ < ObZoneMergeInfo::MERGE_STATUS_MAX
                                        && merge_mode_ >= ObGlobalMergeInfo::MERGE_MODE_TENANT
                                        && merge_mode_ < ObGlobalMergeInfo::MERGE_MODE_MAX; }
  OB_INLINE bool is_global_during_window_compaction() const { return ObGlobalMergeInfo::MERGE_MODE_WINDOW == merge_mode_
                                                                  && ObZoneMergeInfo::MERGE_STATUS_MERGING == merge_status_; }
  OB_INLINE bool is_window_compaction_forever() const { return FULL_DAY_US == window_duration_us_; }
  OB_INLINE bool is_window_compaction_stopped() const { return 0 == window_duration_us_ || !enable_window_compaction_; }
  OB_INLINE bool is_window_merge_mode() const { return ObGlobalMergeInfo::MERGE_MODE_WINDOW == merge_mode_; }
  OB_INLINE bool is_tenant_merge_mode() const { return ObGlobalMergeInfo::MERGE_MODE_TENANT == merge_mode_; }
  OB_INLINE bool is_idle_merge_status() const { return ObZoneMergeInfo::MERGE_STATUS_IDLE == merge_status_; }
  OB_INLINE bool is_merging_merge_status() const { return ObZoneMergeInfo::MERGE_STATUS_MERGING == merge_status_; }
  OB_INLINE bool is_merge_verifying() const { return ObZoneMergeInfo::MERGE_STATUS_VERIFYING == merge_status_; }

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(enable_window_compaction), K_(window_duration_us), K_(window_start_time_us), K_(merge_status), K_(merge_mode));
public:
  static const int64_t FULL_DAY_US = 24 * 60 * 60 * 1000 * 1000L; // 24h
public:
  bool is_inited_;
  uint64_t tenant_id_;
  // tenant config parameter, need atomic store and load
  bool enable_window_compaction_;
  // only used and updated in sys leader leader
  int64_t window_duration_us_;
  // updated in ObTenantFreezeInfoMgr::ReloadTask
  int64_t window_start_time_us_;
  ObZoneMergeInfo::MergeStatus merge_status_;
  ObGlobalMergeInfo::MergeMode merge_mode_;
};

struct ObWindowResourceCache final
{
public:
  ObWindowResourceCache();
  ~ObWindowResourceCache() { reset(); };
  void reset();
  bool check_could_skip(const int64_t merge_start_time_us, const bool to_window) const;
  void update(const int64_t merge_start_time_us, const bool to_window);
  TO_STRING_KV(K_(merge_start_time_us), K_(finish_switch_to_window), K_(finish_switch_to_normal));
public:
  int64_t merge_start_time_us_; // use window merge start time to distinguish different merge rounds
  bool finish_switch_to_window_;
  bool finish_switch_to_normal_;
};

class ObWindowCompactionHelper
{
public:
  static int check_window_compaction_could_start(const uint64_t tenant_id,
                                                 const share::ObGlobalMergeInfo &global_info,
                                                 bool &could_start_window_compaction);
  static int check_and_alter_window_status_for_leader(const uint64_t tenant_id,
                                                      const int64_t current_epoch,
                                                      const share::ObGlobalMergeInfo &global_info,
                                                      ObWindowResourceCache &resource_cache);
  static int clean_before_major_merge(const uint64_t tenant_id,
                                      const int64_t merge_start_time_us,
                                      ObWindowResourceCache &resource_cache);
  static int trigger_window_compaction(const uint64_t tenant_id, const ObWindowCompactionParam &param);
  static int finish_window_compaction(const uint64_t tenant_id);
  static int check_window_compaction_global_active(const uint64_t tenant_id, bool &is_active);
private:
  static constexpr int64_t MAX_RPC_RETRY_COUNT = 5;
  static constexpr int64_t MAX_PROCESS_TIME_US = 10 * 1000 * 1000L;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_HELPER_H_