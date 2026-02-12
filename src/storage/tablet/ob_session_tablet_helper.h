/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

class ObSessionTabletDeleteHelper final
{
public:
  ObSessionTabletDeleteHelper(
    const uint64_t tenant_id,
    ObIArray<storage::ObSessionTabletInfo *> &tablet_infos,
    common::ObMySQLTransaction &trans)
    : tenant_id_(tenant_id),
      tablet_infos_(tablet_infos),
      trans_(&trans),
      allocator_("SessTabDelH"),
      timeout_us_(MIN(THIS_WORKER.get_timeout_remain(), 10000000/* us */))
  {}
  ~ObSessionTabletDeleteHelper() = default;
  int do_work();
  void set_timeout_us(int64_t timeout_us) { timeout_us_ = timeout_us; }
  TO_STRING_KV(K_(tenant_id), K_(tablet_infos));
private:
  int delete_tablets(const ObIArray<common::ObTabletID> &tablet_ids, const int64_t schema_version);
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
  static const int64_t MAX_GC_COUNT = 600;
  static const int64_t NUM_OF_TABLET_GROUP = 4;
  static const int64_t TABLET_GROUP_SIZE = 16;
  static const int64_t BATCH_DELETE_SESSION_TABLET_COUNT = TABLET_GROUP_SIZE * NUM_OF_TABLET_GROUP;
  TO_STRING_KV(K_(tenant_id));
private:
  static int is_table_has_active_session(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t schema_version,
    bool &has_active_session);
  // Group session tablet infos by session_id and sequence
  static int group_by_session_and_seq(
    const ObIArray<storage::ObSessionTabletInfo *> &session_tablet_infos_for_delete,
    ObIArray<common::ObSEArray<storage::ObSessionTabletInfo *, TABLET_GROUP_SIZE>> &session_tablet_infos_for_delete_grouped/* out */);
  uint64_t tenant_id_;
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_TABLET_OB_SESSION_TABLET_HELPER_H
