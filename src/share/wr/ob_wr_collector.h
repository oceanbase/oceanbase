/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
#include "share/wr/ob_wr_snapshot_rpc_processor.h"
#include "share/wr/ob_wr_task.h"
namespace oceanbase
{
namespace share
{

struct ObWrSysstat
{
public:
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(stat_id), K_(value));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t stat_id_;
  int64_t value_;
};

struct ObWrAsh
{
public:
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(sample_id), K_(session_id), K_(sample_time),
      K_(user_id), K_(session_type), K_(sql_id), K_(trace_id), K_(event_no), K_(time_waited),
      K_(p1), K_(p2), K_(p3), K_(sql_plan_line_id), K_(time_model), K_(module), K_(action),
      K_(client_id), K_(plan_id));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t sample_id_;
  int64_t session_id_;
  int64_t sample_time_;
  int64_t user_id_;
  bool session_type_;
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  char trace_id_[OB_MAX_TRACE_ID_BUFFER_SIZE + 1];
  int64_t event_no_;
  int64_t time_waited_;
  int64_t p1_;
  int64_t p2_;
  int64_t p3_;
  int64_t sql_plan_line_id_;
  uint64_t time_model_;
  char module_[64 + 1];
  char action_[64 + 1];
  char client_id_[64 + 1];
  char backtrace_[512 + 1];
  int64_t plan_id_;
};

class ObWrCollector
{
public:
  explicit ObWrCollector(int64_t snap_id, int64_t snapshot_begin_time, int64_t snapshot_end_time,
      int64_t snapshot_timeout_ts);
  ~ObWrCollector() = default;
  DISABLE_COPY_ASSIGN(ObWrCollector);
  int collect();
  TO_STRING_KV(K_(snap_id), K_(snapshot_begin_time), K_(snapshot_end_time), K_(timeout_ts));

private:
  int collect_sysstat();
  int collect_ash();
  int collect_statname();
  int write_to_wr(ObDMLSqlSplicer &dml_splicer, const char *table_name, int64_t tenant_id);
  int64_t snap_id_;
  int64_t snapshot_begin_time_;
  int64_t snapshot_end_time_;
  int64_t timeout_ts_;
};

class ObWrDeleter
{
public:
  ObWrDeleter(const ObWrPurgeSnapshotArg &purge_arg) : purge_arg_(purge_arg)
  {}
  ~ObWrDeleter() = default;
  DISABLE_COPY_ASSIGN(ObWrDeleter);
  int do_delete();
  TO_STRING_KV(K_(purge_arg));

private:
  // delete expired data from wr tables
  //
  // @table_name [in] the name of the table whose data needs to be deleted
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @snap_id [in] the id of snapshot
  // @return the error code.
  int delete_expired_data_from_wr_table(const char *const table_name, const uint64_t tenant_id,
      const int64_t cluster_id, const int64_t snap_id, const int64_t query_timeout);
  int modify_snapshot_status(const uint64_t tenant_id, const int64_t cluster_id,
    const int64_t snap_id, const int64_t query_timeout, const ObWrSnapshotStatus status);
  const ObWrPurgeSnapshotArg &purge_arg_;
};

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
