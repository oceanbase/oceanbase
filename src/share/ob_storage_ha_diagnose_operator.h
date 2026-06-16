/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_HA_DIAGNOSE_OPERATOR_
#define OCEANBASE_SHARE_HA_DIAGNOSE_OPERATOR_

#include "share/storage/ob_ha_inflight_diag.h"
#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}

namespace share
{
class ObDMLSqlSplicer;

class ObStorageHADiagOperator final
{
public:
  ObStorageHADiagOperator();
  ~ObStorageHADiagOperator() {}

  int init();
  void reset();

  // Append one history row (columns + finish_row) to `dml`. Caller builds up
  // a batch across many items and then flushes with batch_write_inflight_rows,
  // yielding one multi-row INSERT per target table per drain tick.
  int append_inflight_row(
          const uint64_t report_tenant_id,
          const ObLSID &ls_id,
          const ObStorageHADiagModule &module,
          const ObStorageHADiagTaskType task_type,
          const ObHAInflightDiagState &state,
          const int32_t result_code,
          const int64_t retry_id,
          ObDMLSqlSplicer &dml);

  // Flush whatever rows have been appended into `dml` as a single multi-row
  // INSERT against `module`'s history table. No-op if dml is empty.
  int batch_write_inflight_rows(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const ObStorageHADiagModule &module,
          ObDMLSqlSplicer &dml);

  // Delete up to `batch_limit` history rows whose gmt_create is <=
  // delete_timestamp. Callers loop until `affected_rows < batch_limit`.
  int delete_expired_rows(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const ObStorageHADiagModule &module,
          const int64_t delete_timestamp,
          const int64_t batch_limit,
          int64_t &affected_rows) const;

private:
  int fill_inflight_dml_(
          const uint64_t report_tenant_id,
          const ObLSID &ls_id,
          const ObStorageHADiagModule &module,
          const ObStorageHADiagTaskType task_type,
          const ObHAInflightDiagState &state,
          const int32_t result_code,
          const int64_t retry_id,
          ObDMLSqlSplicer &dml);

  int get_table_name_(const ObStorageHADiagModule &module, const char *&table_name) const;
  int gen_event_ts_(int64_t &event_ts);

private:
  bool is_inited_;
  int64_t last_event_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagOperator);
};

} // end namespace share
} // end namespace oceanbase
#endif
