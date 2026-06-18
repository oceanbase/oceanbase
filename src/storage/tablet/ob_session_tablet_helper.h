/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_HELPER_H
#define OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_HELPER_H

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_latest_schema_guard.h"
#include "rootserver/ob_tablet_creator.h"
#include "storage/tablet/ob_session_tablet_info_map.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace storage
{

class ObSessionTabletCreateHelper final
{
public:
  ObSessionTabletCreateHelper(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t sequence,
    const uint64_t session_id,
    ObSessionTabletInfoMap &session_tablet_map)
    : tenant_id_(tenant_id),
      table_ids_(),
      sequence_(sequence),
      session_id_(session_id),
      ls_id_(),
      tablet_ids_(),
      trans_(),
      tablet_creator_(tenant_id, share::SCN::base_scn(), trans_),
      session_tablet_map_(session_tablet_map)
  {
    OB_ASSERT(OB_SUCCESS == table_ids_.push_back(table_id));
  }
  ObSessionTabletCreateHelper(
    const uint64_t tenant_id,
    const int64_t sequence,
    const uint64_t session_id,
    ObSessionTabletInfoMap &session_tablet_map)
    : tenant_id_(tenant_id),
      table_ids_(),
      sequence_(sequence),
      session_id_(session_id),
      ls_id_(),
      tablet_ids_(),
      trans_(),
      tablet_creator_(tenant_id, share::SCN::base_scn(), trans_),
      session_tablet_map_(session_tablet_map)
  {}
  int set_table_ids(const common::ObIArray<uint64_t> &table_ids);
  int do_work();
  static int is_ls_leader(const share::ObLSID &ls_id, bool &is_leader);
  ~ObSessionTabletCreateHelper() = default;
  const common::ObIArray<common::ObTabletID> &get_tablet_ids() const { return tablet_ids_; }
  const common::ObIArray<uint64_t> &get_table_ids() const { return table_ids_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  TO_STRING_KV(K_(tenant_id), K_(table_ids), K_(sequence), K_(session_id), K_(ls_id), K(tablet_ids_));

private:
  const static int64_t DEFAULT_TABLE_CREATE_COUNT = 4;
private:
  int fetch_tablet_id(
    const int64_t tablet_cnt,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObIArray<common::ObTabletID> &tablet_ids);
  int choose_log_stream(
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::schema::ObLatestSchemaGuard &schema_guard,
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTablegroupSchema *tablegroup_schema,
    share::ObLSID &ls_id);
  int generate_tablet_create_arg(
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::schema::ObLatestSchemaGuard &schema_guard,
    const uint64_t tenant_data_version,
    const share::schema::ObTableSchema *table_schema,
    const share::schema::ObTablegroupSchema *tablegroup_schema);

private:
  uint64_t tenant_id_;
  common::ObSEArray<uint64_t, DEFAULT_TABLE_CREATE_COUNT> table_ids_;
  int64_t sequence_;
  uint64_t session_id_;
  share::ObLSID ls_id_;
  common::ObSEArray<common::ObTabletID, DEFAULT_TABLE_CREATE_COUNT> tablet_ids_;
  common::ObMySQLTransaction trans_;
  rootserver::ObTabletCreator tablet_creator_;
  ObSessionTabletInfoMap &session_tablet_map_;
};

struct ObSessionTabletGCTaskSummary final
{
public:
  ObSessionTabletGCTaskSummary()
    : total_cnt_(0),
      schema_missing_tablets_cnt_(0),
      failed_cnt_(0)
  {
  }
  ~ObSessionTabletGCTaskSummary() = default;
  TO_STRING_KV(K_(total_cnt), K_(schema_missing_tablets_cnt), K_(failed_cnt));

public:
  int64_t total_cnt_;
  int64_t schema_missing_tablets_cnt_;
  int64_t failed_cnt_;
};

class ObSessionTabletDeleteHelper final
{
public:
  /// @param[in] schema_version: When not @c common::OB_INVALID_VERSION, @c do_work_for_gc obtains
  ///                            the tenant @c ObSchemaGetterGuard at this schema revision via
  ///                            @c ObMultiVersionSchemaService::get_tenant_schema_guard (GC path).
  ///                            @c common::OB_INVALID_VERSION keeps the default (unpinned) resolution
  ///                            for callers that do not need an explicit snapshot.
  ObSessionTabletDeleteHelper(
    const uint64_t tenant_id,
    ObIArray<storage::ObSessionTabletInfo *> &tablet_infos,
    common::ObMySQLTransaction &trans,
    const int64_t schema_version = common::OB_INVALID_VERSION)
    : tenant_id_(tenant_id),
      tablet_infos_(tablet_infos),
      trans_(&trans),
      allocator_("SessTabDelH"),
      schema_version_(schema_version),
      timeout_us_(MIN(THIS_WORKER.get_timeout_remain(), 10000000/* us */))
  {}
  ~ObSessionTabletDeleteHelper() = default;
  int do_work();
  static int delete_session_tablets_by_table_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t table_id);
  /// @brief Delete session tablets with GC-oriented batch(For GC only).
  ///   per-table failures are filtered
  ///   (see remove_failed_tables inside check_and_lock_tables) so tablets that
  ///   passed checks can still be deleted in the same invocation.
  ///   It will try to DELETE tablets whose table schema is no
  ///   longer present (e.g. table already dropped while the tablet still exists),
  ///
  /// @return OB_SUCCESS on success, or an error code if a non-recoverable step fails.
  int do_work_for_gc(ObSessionTabletGCTaskSummary &summary);
  void set_timeout_us(int64_t timeout_us) { timeout_us_ = timeout_us; }
  TO_STRING_KV(K_(tenant_id), K_(tablet_infos));
private:
  /// @brief Remove entries whose data table id exists in @p failed_data_tb_id_set
  ///        from @p tablet_ids_for_delete and @p table_schemas_for_delete in place.
  /// @pre tablet_ids_for_delete.count() == table_schemas_for_delete.count()
  ///
  /// @param[in]     failed_data_tb_id_set       Set of data table ids whose
  ///                                            tablets should be excluded.
  /// @param[in,out] tablet_ids_for_delete       Tablet id array to be filtered.
  /// @param[in,out] table_schemas_for_delete    Corresponding table schema array
  ///                                            to be filtered.
  /// @return OB_SUCCESS on success, OB_INVALID_ARGUMENT if the two arrays have
  ///         different sizes or contain invalid entries.
  static int remove_failed_tables(
      const hash::ObHashSet<uint64_t> &failed_data_tb_id_set,
      /*out*/common::ObIArray<ObTabletID> &tablet_ids_for_delete,
      /*out*/common::ObIArray<const ObTableSchema *> &table_schemas_for_delete);
private:
  int lock_table_for_delete(
      const ObTableSchema &table_schema,
      const common::ObIArray<ObTabletID> &tablet_ids);

  /// @brief Validate schemas and acquire locks for session tablets before deletion.
  /// Iterates over @c tablet_infos_ and, for each entry owned by the creator
  /// session, fetches its table schema, verifies it is a non-partitioned Oracle
  /// temporary table (v2), and acquires a table lock via @c lock_table_for_delete.
  /// Successfully checked tablets and their schemas are appended to the output
  /// arrays.
  ///
  /// NOTE: When @p is_atomic_batch is FALSE, failures on individual tables are
  /// tolerated: the failed data table ids are collected and later removed from
  /// the output arrays via @c remove_failed_tables so that the remaining tablets
  /// can still be deleted. When @p is_atomic_batch is TRUE, any single failure
  /// causes the entire operation to fail.
  ///
  /// @param[in]  is_atomic_batch             If true, any single table failure
  ///                                            aborts the whole batch.
  /// @param[out] tablet_ids_for_delete       Receives the tablet ids that
  ///                                            passed validation and locking.
  /// @param[out] table_schemas_for_delete    Receives the corresponding table
  ///                                           schemas.
  /// @param[out] schema_missing_tablet_infos Tablets whose table schema is missing.
  /// @return OB_SUCCESS on success, or an appropriate error code on failure.
  int check_and_lock_tables(
      const bool is_atomic_batch,
      share::schema::ObSchemaGetterGuard &schema_guard,
      /*out*/common::ObIArray<ObTabletID> &tablet_ids_for_delete,
      /*out*/common::ObIArray<const ObTableSchema *> &table_schemas_for_delete,
      /*out*/common::ObIArray<ObSessionTabletInfo *> &schema_missing_tablet_infos);
  int delete_tablets(const ObIArray<common::ObTabletID> &tablet_ids, const int64_t schema_version);
  int delete_schema_missing_tablets(const ObIArray<ObSessionTabletInfo *> &tablet_infos, const int64_t schema_version);
  int mds_remove_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObMySQLTransaction &trans);
private:
  uint64_t tenant_id_;
  // The ls_id of the tablet may change after migration, but the ls_id in the tablet_info_ does not get updated.
  ObIArray<storage::ObSessionTabletInfo *> &tablet_infos_;
  common::ObMySQLTransaction *trans_;
  common::ObArenaAllocator allocator_;
  int64_t schema_version_;
  int64_t timeout_us_;
};

class ObSessionTabletGCHelper final
{
public:
  ObSessionTabletGCHelper(
    const uint64_t tenant_id)
    : tenant_id_(tenant_id)
    {}
  ~ObSessionTabletGCHelper() = default;
  int do_work();
  int get_local_leader_ls_ids(common::ObIArray<share::ObLSID> &ls_ids) const;
  // OBCDC will report an error if it can access the tablet but cannot access the schema.
  // Therefore, as long as a temporary table exists and either the main or related table's tablet has not been GC'd,
  // it must be mutually exclusive with DDL operations.
  // is_table_has_active_session only checks whether the tablet exists, not whether the session is alive.
  static int is_table_has_active_session(
    const share::schema::ObSimpleTableSchemaV2 *table_schema,
    const obrpc::ObAlterTableArg *alter_table_arg = nullptr);
  static int is_table_has_active_session(
    const uint64_t tenant_id,
    const ObString &db_name,
    const ObString &table_name,
    const obrpc::ObAlterTableArg *alter_table_arg = nullptr);
  /// @brief: batch interface for drop database
  static int is_any_table_has_active_session(
      const common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas);
  static const int64_t MAX_GC_COUNT = 600;
  static const int64_t NUM_OF_TABLET_GROUP = 4;
  static const int64_t TABLET_GROUP_SIZE = 16;
  static const int64_t BATCH_DELETE_SESSION_TABLET_COUNT = TABLET_GROUP_SIZE * NUM_OF_TABLET_GROUP;
  TO_STRING_KV(K_(tenant_id));
private:
  static bool need_check_table(
    const share::schema::ObSimpleTableSchemaV2 *schema,
    const obrpc::ObAlterTableArg *alter_table_arg);
  /// @brief fetch all relative tables' id by @param primary_table_id
  /// @param[out] table_ids: append-only array(won't be reset in this method)
  static int fetch_all_relative_table_ids(
    const uint64_t tenant_id,
    const uint64_t primary_table_id,
    const int64_t schema_version,
    common::ObMySQLProxy &sql_proxy,
    ObSchemaService &schema_svr,
    /*out*/common::ObIArray<common::ObTableID> &table_ids);
  static int check_if_any_table_has_active_session(
    const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy,
    const common::ObIArray<ObTableID> &table_ids,
    /*out*/bool &has_active_session);
  static int is_table_has_active_session(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t schema_version,
    bool &has_active_session);
  // Group session tablet infos by session_id and sequence
  static int group_by_session_and_seq(
    const ObIArray<storage::ObSessionTabletInfo *> &session_tablet_infos_for_delete,
    ObIArray<common::ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE>> &session_tablet_infos_for_delete_grouped/* out */);

private:
  int refresh_tenant_schema_if_need(int64_t &latest_schema_version);

private:
  uint64_t tenant_id_;
};

/// Broadcast drop GTT v2 session tablet to all alive observers; the creator
/// observer performs the storage delete for all is_creator_ entries in one
/// inner transaction. @p table_ids should carry the main table along with its
/// index and lob aux tables so the broadcast is atomic per truncate.
/// See ob_drop_gtt_v2_session_tablet_rpc.
int dispatch_drop_gtt_v2_session_tablet_on_creator(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint64_t session_id);

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_HELPER_H
