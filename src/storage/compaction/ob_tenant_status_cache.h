//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_TENANT_COMPACTION_STATUS_H_
#define OB_STORAGE_COMPACTION_TENANT_COMPACTION_STATUS_H_
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace compaction
{

struct ObTenantStatusCache final
{
  ObTenantStatusCache()
    : is_inited_(false),
      during_restore_(false),
      is_remote_tenant_(false),
      enable_adaptive_compaction_(false),
      enable_adaptive_merge_schedule_(false),
      min_data_version_(0)
  {}
  ~ObTenantStatusCache() {}
  void reset()
  {
    is_inited_ = false;
    during_restore_ = false;
    is_remote_tenant_ = false;
    enable_adaptive_compaction_ = false;
    enable_adaptive_merge_schedule_ = false;
    min_data_version_ = 0;
  }
  int during_restore(bool &during_restore) const;
  bool is_inited() const { return is_inited_; }
  bool enable_adaptive_compaction() const { return enable_adaptive_compaction_; }
  bool enable_adaptive_compaction_with_cpu_load() const;
  bool enable_adaptive_merge_schedule() const { return enable_adaptive_merge_schedule_; }
  int get_min_data_version(uint64_t &min_data_version);

  int init_or_refresh();
  int refresh_tenant_config(
    const bool enable_adaptive_compaction,
    const bool enable_adaptive_merge_schedule);

  TO_STRING_KV(K_(is_inited), K_(during_restore), K_(is_remote_tenant),
    K_(enable_adaptive_compaction), K_(enable_adaptive_merge_schedule), K_(min_data_version));
//private:
public:
  int inner_refresh_restore_status();
  int inner_refresh_remote_tenant();
  int refresh_data_version();
  static const int64_t REFRESH_TENANT_STATUS_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  bool is_inited_;
  bool during_restore_;
  bool is_remote_tenant_;
  // tenant config is valid even tenant_status is not inited
  bool enable_adaptive_compaction_;
  bool enable_adaptive_merge_schedule_;
  int64_t min_data_version_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TENANT_COMPACTION_STATUS_H_
