/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/scn.h"
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace share
{
namespace schema
{
class ObDatabaseSchema;
}
}
namespace storage
{
struct ObMLogPurgeParam
{
public:
  ObMLogPurgeParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      master_table_id_(OB_INVALID_ID),
      purge_log_parallel_(0)
  {
  }
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_TENANT_ID && master_table_id_ != OB_INVALID_ID;
  }
  TO_STRING_KV(K_(tenant_id), K_(master_table_id), K_(purge_log_parallel));

public:
  uint64_t tenant_id_;
  uint64_t master_table_id_;
  int64_t purge_log_parallel_;
};

class ObMLogPurger
{
public:
  ObMLogPurger();
  ~ObMLogPurger();
  DISABLE_COPY_ASSIGN(ObMLogPurger);

  int init(sql::ObExecContext &exec_ctx,
           const ObMLogPurgeParam &purge_param,
           const bool called_in_refresh = false);
  int purge();

private:
  int prepare_for_purge();
  int do_purge();
  int update_last_purge_scn();
  bool need_purge() const
  {
    return ObMLogPurgeMethod::SQL == purge_method_ ||
           ObMLogPurgeMethod::COMPACTION == purge_method_;
  }
  int start_trans(ObMViewTransaction &trans, uint64_t database_id, const ObString &database_name);
  int register_mds(ObMViewTransaction &trans);
  int update_mlog_info(int64_t start_time, int64_t end_time, int64_t total_affected_rows);
  int get_database_info(ObIAllocator &allocator, uint64_t &database_id, ObString &database_name);

private:
  static const int64_t MLOG_PURGE_BATCH_ROW_LIMIT = 100000;
private:
  sql::ObExecContext *ctx_;
  ObMLogPurgeParam purge_param_;
  share::schema::ObMLogInfo mlog_info_;
  bool is_oracle_mode_;
  bool called_in_refresh_;
  bool need_real_purge_;
  common::ObTabletID tablet_id_;
  ObMLogPurgeMethod purge_method_;
  share::SCN purge_scn_;
  ObSqlString purge_sql_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
