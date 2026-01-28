//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_TTL_TENANT_COMPACTION_TTL_SCHEDULER_H_
#define OB_SHARE_COMPACTION_TTL_TENANT_COMPACTION_TTL_SCHEDULER_H_
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
namespace oceanbase
{
namespace share
{

struct ObCompactionTTLTaskInfo
{
public:
  ObCompactionTTLTaskInfo() : table_id_(OB_INVALID_ID), start_ts_(0), end_ts_(0) {}
  ObCompactionTTLTaskInfo(
    const uint64_t table_id,
    const int64_t start_ts,
    const int64_t end_ts
  ) : table_id_(table_id), start_ts_(start_ts), end_ts_(end_ts) {}
  ~ObCompactionTTLTaskInfo() = default;
  TO_STRING_KV(K_(table_id), K_(start_ts), K_(end_ts));
public:
  uint64_t table_id_;
  int64_t start_ts_;
  int64_t end_ts_;
};

class ObTenantCompactionTTLScheduler final : public table::ObTTLTaskScheduler
{
public:
  ObTenantCompactionTTLScheduler()
  : table::ObTTLTaskScheduler(),
    unsync_compaction_ttl_task_infos_()
  {}
  ~ObTenantCompactionTTLScheduler() = default;
public:
  virtual common::ObTTLType get_ttl_type() override { return common::ObTTLType::COMPACTION_TTL; }
  virtual bool check_tenant_config_enabled() override
  {
    return ObCompactionTTLUtil::is_enable_compaction_ttl(tenant_id_);
  }
  virtual int in_active_time(bool& is_active_time) override;
private:
  int get_tenant_tz_info_wrap(
    ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> &time_zone,
    ObTimeZoneInfoWrap &time_zone_info_wrap);
  virtual int check_all_table_finished(bool &all_finished) override;
  int deal_with_compaction_ttl_table(
    const ObTableSchema &table_schema,
    bool &all_finished);
  int replace_compaction_ttl_task(
    const uint64_t table_id,
    const int64_t err_code,
    const ObTTLTaskStatus task_status,
    const int64_t start_ts,
    const int64_t end_ts);
  int check_compaction_ttl_task_exists(const uint64_t table_id, bool &exists);
  int retry_to_sync_compaction_ttl_task_info();
  int create_compaction_ttl_task_record(
    const uint64_t table_id,
    const int64_t err_code,
    const ObTTLTaskStatus task_status,
    const int64_t start_ts,
    const int64_t end_ts,
    ObTTLStatus &ttl_task);
  int check_is_ttl_table(
    const ObTableSchema &table_schema,
    const uint64_t tenant_data_version,
    bool &is_ttl_table);
  static int get_tx_state(
      const share::ObLSID &ls_id,
      const transaction::ObTransID &tx_id,
      int64_t &state);
private:
  ObSEArray<ObCompactionTTLTaskInfo, 4> unsync_compaction_ttl_task_infos_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_TTL_TENANT_COMPACTION_TTL_SCHEDULER_H_
