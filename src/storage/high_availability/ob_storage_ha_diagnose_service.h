/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_DIAG_SERVICE_
#define OCEABASE_STORAGE_HA_DIAG_SERVICE_

#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_pool.h"
#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObStorageHADiagOperator;
class ObDMLSqlSplicer;
}
namespace storage
{
class ObStorageHADiagMgr;
struct ObHADiagFlushItem;

// Global server-level worker that drains each tenant's inflight diag queue
// into the sys-tenant history tables and periodically recycles old history
// rows. One instance per observer — tenants register their flush items via
// the per-tenant ObStorageHADiagMgr, and this service walks every MTL tenant
// each tick to persist them.
class ObStorageHADiagService : public lib::ThreadPool
{
public:
  ObStorageHADiagService();
  virtual ~ObStorageHADiagService() {}

  int init(common::ObMySQLProxy *sql_proxy);
  void destroy();
  int start();
  void stop();
  void wait();
  void wakeup();
  int reload_config();

  static ObStorageHADiagService &instance();

  void run1() final;

public:
  static constexpr int64_t DEFAULT_IDLE_INTERVAL_MS = 5 * 60 * 1000L; // 5 minutes
  static constexpr int64_t DELETE_BATCH_LIMIT = 1000;
  // Upper bound on rows per multi-row INSERT. Bounds single-statement SQL
  // text, parse/plan cost, lock/redo footprint regardless of queue size.
  static constexpr int64_t DML_BATCH_ROWS = 128;

private:
  int drain_tenant_(const uint64_t tenant_id);
  void append_and_maybe_flush_(
      share::ObStorageHADiagOperator &op,
      const uint64_t report_tenant_id,
      const ObHADiagFlushItem &item,
      const share::ObStorageHADiagModule module,
      share::ObDMLSqlSplicer &dml,
      bool &poisoned,
      int64_t &written_rows);
  int do_clean_history_(const share::ObStorageHADiagModule module);
  bool should_run_gc_() const;

private:
  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t idle_interval_ms_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagService);
};

}
}
#endif
