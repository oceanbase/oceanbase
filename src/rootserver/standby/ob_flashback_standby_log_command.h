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

#ifndef OCEANBASE_ROOTSERVER_OB_FLASHBACK_STANDBY_LOG_COMMAND_H
#define OCEANBASE_ROOTSERVER_OB_FLASHBACK_STANDBY_LOG_COMMAND_H

#include "share/ob_flashback_standby_log_struct.h"
#include "share/ob_tenant_info_proxy.h"
#include "rootserver/standby/ob_tenant_role_transition_service.h"//ObLSAccessModeModifier, ObTenantRoleTransAllLSInfo
#include <share/scn.h>

namespace oceanbase
{
namespace rootserver
{
class ObFlashbackStandbyLogTimeCostDetail {
public:
  enum CostType {
    INVALID_COST_TYPE,
    CLEAR_SERVICE_NAME,
    CLEAR_FETCHED_LOG_CACHE,
    FLASHBACK_LOG,
    CHANGE_ACCESS_MODE,
    OTHERS,
    MAX_COST_TYPE,
  };
  ObFlashbackStandbyLogTimeCostDetail() : cost_{}, start_timestamp_(0), end_timestamp_(0) {}
  ~ObFlashbackStandbyLogTimeCostDetail() {}
  int start();
  int end();
  int set_cost(const CostType cost_type, const int64_t time_cost);
  int get_start_timestamp() const { return start_timestamp_; }
  int get_end_timestamp() const { return end_timestamp_; }
  const char* type_to_str(const CostType type) const;
  int64_t to_string (char *buf, const int64_t buf_len) const;
private:
  int64_t cost_[MAX_COST_TYPE];
  int64_t start_timestamp_;
  int64_t end_timestamp_;
};
class ObFlashbackStandbyLogEvent {
public:
  ObFlashbackStandbyLogEvent() : tenant_id_(OB_INVALID_TENANT_ID), end_ret_(0), flashback_log_scn_{},
      op_start_tenant_info_(), op_end_tenant_info_(), all_flashback_ls_(), cost_detail_(){}
  ~ObFlashbackStandbyLogEvent() {}
  int start(const uint64_t tenant_id, const share::SCN &flashback_log_scn);
  int end(const share::ObAllTenantInfo &op_end_tenant_info, const int end_ret);
  int set_op_start_tenant_info(const share::ObAllTenantInfo &op_start_tenant_info);
  int add_all_flashback_ls(const share::ObLSStatusInfoArray &status_info_array);
  int set_clear_service_time_cost(int time_cost) {
    return cost_detail_.set_cost(ObFlashbackStandbyLogTimeCostDetail::CLEAR_SERVICE_NAME, time_cost);
  }
  int set_clear_fetched_log_cache_cost(int time_cost) {
    return cost_detail_.set_cost(ObFlashbackStandbyLogTimeCostDetail::CLEAR_FETCHED_LOG_CACHE, time_cost);
  }
  int set_flashback_log_time_cost(int time_cost) {
    return cost_detail_.set_cost(ObFlashbackStandbyLogTimeCostDetail::FLASHBACK_LOG, time_cost);
  }
  int set_change_access_mode_time_cost(int time_cost) {
    return cost_detail_.set_cost(ObFlashbackStandbyLogTimeCostDetail::CHANGE_ACCESS_MODE, time_cost);
  }
private:
  uint64_t tenant_id_;
  int end_ret_;
  share::SCN flashback_log_scn_;
  share::ObAllTenantInfo op_start_tenant_info_;
  share::ObAllTenantInfo op_end_tenant_info_;
  ObTenantRoleTransAllLSInfo all_flashback_ls_;
  ObFlashbackStandbyLogTimeCostDetail cost_detail_;
};
class ObFlashbackStandbyLogCommand
{
public:
  ObFlashbackStandbyLogCommand() : event_() {}
  ~ObFlashbackStandbyLogCommand() {}
  int execute(const share::ObFlashbackStandbyLogArg &arg);
  static int clear_local_fetched_log_cache(const share::ObClearFetchedLogCacheArg &arg, share::ObClearFetchedLogCacheRes &res);
private:
  int process_normal_status_(
      const uint64_t tenant_id,
      const share::SCN &flashback_log_scn,
      share::ObAllTenantInfo &tenant_info);
  int process_flashback_status_(
      const uint64_t tenant_id,
      const share::SCN &flashback_log_scn,
      share::ObAllTenantInfo &tenant_info);
  int check_requirements_for_normal_status_(
      const share::ObAllTenantInfo &tenant_info,
      const share::SCN &flashback_log_scn,
      ObISQLClient *proxy);
  int check_requirements_for_flashback_status_(
      const share::ObAllTenantInfo &tenant_info,
      const share::SCN &flashback_log_scn);
  int check_restore_source_empty_(const uint64_t tenant_id, ObISQLClient *proxy);
  int clear_fetched_log_cache_(const uint64_t tenant_id);
  int do_flashback_(const uint64_t tenant_id, const share::SCN &flashback_log_scn);
  int change_ls_access_mode_back_to_raw_rw_(const uint64_t tenant_id, const uint64_t switchover_epoch);
private:
  ObFlashbackStandbyLogEvent event_;
};
}
}

#endif /* !OCEANBASE_ROOTSERVER_OB_FLASHBACK_STANDBY_LOG_H */