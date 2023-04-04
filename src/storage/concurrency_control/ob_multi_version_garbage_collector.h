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

#ifndef OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_MULTI_VERSION_GARBAGE_COLLECTOR
#define OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_MULTI_VERSION_GARBAGE_COLLECTOR

#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include "lib/function/ob_function.h"
#include "share/ob_occam_timer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace concurrency_control
{

#define UPDATE_ONE_RESERVED_SNAPSHOT_SQL "    \
    update %s set                             \
    snapshot_version = %ld,                   \
    create_time = %ld                         \
    where tenant_id = %lu                     \
    and snapshot_type = %lu                   \
    and svr_ip = '%.*s'                       \
    and svr_port = %d                         \
    "

#define QUERY_ONE_RESERVED_SNAPSHOT_SQL_UDT " \
    select snapshot_version, snapshot_type,   \
    svr_ip, svr_port, create_time             \
    from %s                                   \
    where tenant_id = %lu                     \
    and snapshot_type = %lu                   \
    and svr_ip = '%.*s'                       \
    and svr_port = %d                         \
    for update                                \
    "

#define QUERY_ALL_RESERVED_SNAPSHOT_SQL "    \
    select snapshot_version, snapshot_type,  \
    svr_ip, svr_port, create_time, status    \
    from %s                                  \
    where tenant_id = %lu                    \
    "

#define INSERT_ON_UPDATE_ALL_RESERVED_SNAPSHOT_SQL " \
    insert into %s                                   \
    (tenant_id, snapshot_type, svr_ip, svr_port,     \
     create_time, status, snapshot_version)          \
    values (%lu, %lu, '%.*s', %d, '%ld', %ld, %ld),  \
    (%lu, %lu, '%.*s', %d, '%ld', %ld, %ld),         \
    (%lu, %lu, '%.*s', %d, '%ld', %ld, %ld),         \
    (%lu, %lu, '%.*s', %d, '%ld', %ld, %ld)          \
    on duplicate key update                          \
    create_time = VALUES(create_time),               \
    snapshot_version = VALUES(snapshot_version)      \
    "

#define UPDATE_RESERVED_SNAPSHOT_STATUS " \
    update %s set                         \
    status = %ld                          \
    where tenant_id = %lu                 \
    and svr_ip = '%.*s'                   \
    and svr_port = %d                     \
    "

#define QUERY_DISTINCT_SNAPSHOT_FOR_UPDATE " \
    select create_time, snapshot_type,       \
    svr_ip, svr_port, create_time, status    \
    from %s                                  \
    where tenant_id = %lu                    \
    group by snapshot_type, svr_ip, svr_port \
    order by create_time desc                \
    for update                               \
    "

#define DELETE_EXPIRED_RESERVED_SNAPSHOT "   \
    delete from %s                           \
    where tenant_id = %lu                    \
    and svr_ip = '%.*s'                      \
    and svr_port = %d                        \
    "

enum ObMultiVersionSnapshotType : uint64_t
{
  MIN_SNAPSHOT_TYPE         = 0,
  MIN_UNALLOCATED_GTS       = 1,
  MIN_UNALLOCATED_WRS       = 2,
  MAX_COMMITTED_TXN_VERSION = 3,
  ACTIVE_TXN_SNAPSHOT       = 4,
  MAX_SNAPSHOT_TYPE         = 5,
};

enum ObMultiVersionGCStatus : uint64_t
{
  NORMAL_GC_STATUS          = 0,
  DISABLED_GC_STATUS        = 1 << 0,
  INVALID_GC_STATUS         = UINT64_MAX,
};

inline ObMultiVersionGCStatus operator | (ObMultiVersionGCStatus a, ObMultiVersionGCStatus b)
{
  return static_cast<ObMultiVersionGCStatus>(static_cast<uint64_t>(a) | static_cast<uint64_t>(b));
}

inline ObMultiVersionGCStatus operator & (ObMultiVersionGCStatus a, ObMultiVersionGCStatus b)
{
  return static_cast<ObMultiVersionGCStatus>(static_cast<uint64_t>(a) & static_cast<uint64_t>(b));
}

class ObMultiVersionSnapshotInfo
{
public:
  ObMultiVersionSnapshotInfo();
  ObMultiVersionSnapshotInfo(const share::SCN snapshot_version,
                             const ObMultiVersionSnapshotType snapshot_type,
                             const ObMultiVersionGCStatus status,
                             const int64_t create_time,
                             const ObAddr addr);
  share::SCN snapshot_version_;
  ObMultiVersionSnapshotType snapshot_type_;
  ObMultiVersionGCStatus status_;
  int64_t create_time_;
  ObAddr addr_;

  TO_STRING_KV(K_(snapshot_version),
               K_(snapshot_type),
               K_(status),
               K_(create_time),
               K_(addr));
};

// Because of the value semantics of the ObFunction, we will also use the
// encapsulation of the class operator to implement the state machine. You can
// read the ObMultiVersionGCSnapshotOperator and its usage of lambda as an
// example.
using ObMultiVersionGCSnapshotFunction =
  ObFunction<int(const share::SCN snapshot_version,
                 const ObMultiVersionSnapshotType snapshot_type,
                 const ObMultiVersionGCStatus status,
                 const int64_t create_time,
                 const ObAddr addr)>;

// Because of the value semantics of the ObFunction, we use class and operator
// to implement the state transfer of the state machine under the collect function
// of the ObMultiVersionGCSnapshotCollector.
class ObMultiVersionGCSnapshotFunctor
{
public:
  virtual int operator()(const share::SCN snapshot_version,
                         const ObMultiVersionSnapshotType snapshot_type,
                         const ObMultiVersionGCStatus status,
                         const int64_t create_time,
                         const ObAddr addr) = 0;

  VIRTUAL_TO_STRING_KV("Operator", "MultiVersionGC");
};

// ObMultiVersionGCSnapshotCalculator is used for tenant freezer manager to
// calculate the min reserved multi-version snapshot and cache some information.
class ObMultiVersionGCSnapshotCalculator : public ObMultiVersionGCSnapshotFunctor
{
public:
  ObMultiVersionGCSnapshotCalculator();
  ~ObMultiVersionGCSnapshotCalculator();
  int operator()(const share::SCN snapshot_version,
                 const ObMultiVersionSnapshotType snapshot_type,
                 const ObMultiVersionGCStatus status,
                 const int64_t create_time,
                 const ObAddr addr);
  share::SCN get_reserved_snapshot_version() const;
  ObMultiVersionSnapshotType get_reserved_snapshot_type() const;
  ObMultiVersionGCStatus get_status() const;
  bool is_this_server_disabled() const
  { return is_this_server_disabled_; }

  VIRTUAL_TO_STRING_KV(K_(reserved_snapshot_version),
                       K_(reserved_snapshot_type),
                       K_(reserved_status),
                       K_(reserved_create_time),
                       K_(reserved_addr),
                       K_(is_this_server_disabled),
                       K_(status));
private:
  share::SCN reserved_snapshot_version_;
  ObMultiVersionSnapshotType reserved_snapshot_type_;
  ObMultiVersionGCStatus reserved_status_;
  int64_t reserved_create_time_;
  ObAddr reserved_addr_;
  // whether this server is disabled for gc status
  bool is_this_server_disabled_;
  // final status after transve all gc status
  ObMultiVersionGCStatus status_;
};

class ObMultiVersionGCSnapshotCollector : public ObMultiVersionGCSnapshotFunctor
{
public:
  ObMultiVersionGCSnapshotCollector(ObIArray<ObMultiVersionSnapshotInfo> &array);
  ~ObMultiVersionGCSnapshotCollector();
  int operator()(const share::SCN snapshot_version,
                 const ObMultiVersionSnapshotType snapshot_type,
                 const ObMultiVersionGCStatus status,
                 const int64_t create_time,
                 const ObAddr addr);

  VIRTUAL_TO_STRING_KV(K_(snapshots_info));
private:
  ObIArray<ObMultiVersionSnapshotInfo> &snapshots_info_;
};

class ObMultiVersionGCSnapshotOperator : public ObMultiVersionGCSnapshotFunctor
{
public:
  ObMultiVersionGCSnapshotOperator(const ObMultiVersionGCSnapshotFunction &func);
  int operator()(const share::SCN snapshot_version,
                 const ObMultiVersionSnapshotType snapshot_type,
                 const ObMultiVersionGCStatus status,
                 const int64_t create_time,
                 const ObAddr addr);
private:
  ObMultiVersionGCSnapshotFunction func_;
};

class GetMinActiveSnapshotVersionFunctor
{
public:
  GetMinActiveSnapshotVersionFunctor()
    : min_active_snapshot_version_(share::SCN::max_scn()) {}
  virtual ~GetMinActiveSnapshotVersionFunctor() {}
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
  share::SCN get_min_active_snapshot_version()
    { return min_active_snapshot_version_; }
private:
  share::SCN min_active_snapshot_version_;
};

// OceanBase 4.0 reclaims multi-version data through a globally incremented
// timestamp. It's the multi-version data that is less than the specified
// timestamp except for the latest version can be safely recycled. However, the
// setting of this timestamp is worth deliberating: If this timestamp is too
// small, the snapshot of long-running txns will be lost; if the timestamp is
// too large, a large amount of the multi-version data can not be recycled,
// affecting query efficiency and disk utilization.
//
// Therefore, in order to efficiently recover these multi-version data, the
// class ObMultiVersionGarbageCollector currenly detects the minimal active txn
// snapshot globally. And decides the recyclable timestamp using this minimum
// active txn snapshot. This method balances the difficulty of implementation
// and the benefits it brings
//
// In the future, we will expand to further implementations that bring more
// benefiets based on different competing products and papers, including but not
// limited to optimizing the reclaimation with only some range of the
// multi-version data; and optimizing based on table-level visibility, etc.
//
// Above all, we need to achieve the following 3 rules:
// 1. All data that will no longer be read needs to be recycled as soon as possible
// 2. All data that may be read must be preserved
// 3. Guarantee user-understandable recovery in abnormal scenarios
class ObMultiVersionGarbageCollector
{
public:
  ObMultiVersionGarbageCollector();
  ~ObMultiVersionGarbageCollector();
  static int mtl_init(ObMultiVersionGarbageCollector *&p_garbage_colloector);
public:
  int init();
  int start();
  int stop();
  void wait();
  void destroy();
  // cure means treat myself for the injurity. It resets all state just like
  // treat a patient
  void cure();
  // repeat_xxx will repeatably work for its task under task or meet the time
  // requirement. Currently we retry task every 1 minute and rework the task
  // every 10 minute according to unchangable configuration.
  //
  // study means learn for the different ObMultiVersionSnapshotType and report
  // to the inner table. refresh means refresh globally reserved snapshots.
  // reclaim means reclaim long-time unchangable value in the inner table.
  void repeat_study();
  void repeat_refresh();
  void repeat_reclaim();
  // collect will invoke the functor for every entries in the inner table.
  // It is flexible to use.
  // NB: it will stop if functor report error
  int collect(ObMultiVersionGCSnapshotFunctor& functor);
  // report will report the following four entries into the inner table.
  // NB: the 4 entries must be inserted atomically for the rule2.
  int report(const share::SCN min_unallocated_GTS,
             const share::SCN min_unallocated_WRS,
             const share::SCN max_committed_txn_version,
             const share::SCN min_active_txn_version);
  // update will update the four entries with the new status.
  // NB: the 4 entries must be updated atomically for the rule2.
  int update_status(const ObMultiVersionGCStatus status);
  // reclaim will remove the four entries for expired nodes.
  // NB: the 4 entries must be removed atomically for the rule2.
  int reclaim();

  // get_reserved_snapshot_for_active_txn fetch the cached globally reserved
  // snapshot if updated in time, otherwise max_scn() is used for available
  share::SCN get_reserved_snapshot_for_active_txn() const;
  // report_sstable_overflow marks the last sstable's overflow events and we
  // will use it to disable mvcc gc
  void report_sstable_overflow();
  // is_gc_disabled shows the global gc status of whether the gc is disabled
  bool is_gc_disabled() const;

  TO_STRING_KV(KP(this),
               K_(last_study_timestamp),
               K_(last_refresh_timestamp),
               K_(last_reclaim_timestamp),
               K_(last_sstable_overflow_timestamp),
               K_(has_error_when_study),
               K_(refresh_error_too_long),
               K_(has_error_when_reclaim),
               K_(gc_is_disabled),
               K_(global_reserved_snapshot),
               K_(is_inited));

public:
  static int64_t GARBAGE_COLLECT_RETRY_INTERVAL;
  static int64_t GARBAGE_COLLECT_EXEC_INTERVAL;
  static int64_t GARBAGE_COLLECT_PRECISION;
  static int64_t GARBAGE_COLLECT_RECLAIM_DURATION;
private:
  int study();
  int refresh_();
  int disk_monitor_(const bool is_this_server_disabled);
  int monitor_(const ObArray<ObAddr> &snapshot_servers);
  int reclaim_(const ObArray<ObAddr> &reclaimable_servers);
  int study_min_unallocated_GTS(share::SCN &min_unallocated_GTS);
  int study_min_unallocated_WRS(share::SCN &min_unallocated_WRS);
  int study_max_committed_txn_version(share::SCN &max_committed_txn_version);
  int study_min_active_txn_version(share::SCN &min_active_txn_version);
  int is_disk_almost_full_(bool &is_almost_full);
  bool is_sstable_overflow_();
  void decide_gc_status_(const ObMultiVersionGCStatus gc_status);
  void decide_reserved_snapshot_version_(const share::SCN reserved_snapshot,
                                         const ObMultiVersionSnapshotType reserved_type);

  // ============== for test ================
  OB_NOINLINE bool can_report();
  OB_NOINLINE bool is_refresh_fail();
private:
  common::ObOccamTimer timer_;
  common::ObOccamTimerTaskRAIIHandle timer_handle_;

  int64_t last_study_timestamp_;
  int64_t last_refresh_timestamp_;
  int64_t last_reclaim_timestamp_;
  // last timestamp sstable reports overflow during merge
  int64_t last_sstable_overflow_timestamp_;
  bool has_error_when_study_;
  // refresh too long without contacting inner table successfully.
  // It may be caused by inner table majority crash or network issues.
  bool refresh_error_too_long_;
  bool has_error_when_reclaim_;
  // gc is disabled in inner table
  // it may be disabled by disk mointor
  bool gc_is_disabled_;
  // globally reserved snapshot for active txn
  share::SCN global_reserved_snapshot_;
  bool is_inited_;
};

} // namespace concurrency_control
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_MULTI_VERSION_GARBAGE_COLLECTOR
