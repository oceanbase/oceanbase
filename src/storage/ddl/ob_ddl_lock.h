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

#ifndef OCEANBASE_STORAGE_OB_DDL_LOCK_H_
#define OCEANBASE_STORAGE_OB_DDL_LOCK_H_

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_table_schema.h"
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
namespace storage
{

// handle both table lock and online ddl lock for ddl
class ObDDLLock {
public:
  static bool need_lock(const share::schema::ObTableSchema &table_schema);

  static int lock_for_add_drop_index_in_trans(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      ObMySQLTransaction &trans);
  static int lock_for_add_drop_index(
      const share::schema::ObTableSchema &data_table_schema,
      const common::ObIArray<ObTabletID> *inc_data_tablet_ids,
      const common::ObIArray<ObTabletID> *del_data_tablet_ids,
      const share::schema::ObTableSchema &index_schema,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);
  static int unlock_for_add_drop_index(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);

  static int lock_for_add_lob_in_trans(
      const share::schema::ObTableSchema &data_table_schema,
      ObMySQLTransaction &trans);

  static int lock_for_add_partition_in_trans(
      const share::schema::ObTableSchema &table_schema,
      ObMySQLTransaction &trans);

  static int lock_for_drop_partition_in_trans(
      const share::schema::ObTableSchema &table_schema,
      const ObIArray<ObTabletID> &del_tablet_ids,
      ObMySQLTransaction &trans);

  static int lock_for_common_ddl_in_trans(const share::schema::ObTableSchema &table_schema, ObMySQLTransaction &trans);
  static int lock_for_common_ddl(
      const share::schema::ObTableSchema &table_schema,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);
  static int unlock_for_common_ddl(
      const share::schema::ObTableSchema &table_schema,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);

  static int lock_for_offline_ddl(
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObTableSchema *hidden_table_schema_to_check_bind,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);
  static int unlock_for_offline_ddl(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);

private:
  static int lock_table_lock_in_trans(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const ObIArray<ObTabletID> &tablet_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);
  static int do_table_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const bool is_lock,
    ObMySQLTransaction &trans);
  static int do_table_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const bool is_lock,
    ObMySQLTransaction &trans);
  static int check_tablet_in_same_ls(
      const share::schema::ObTableSchema &lhs_schema,
      const share::schema::ObTableSchema &rhs_schema,
      ObMySQLTransaction &trans);
  static constexpr int64_t DEFAULT_TIMEOUT = 0;
};

class ObOnlineDDLLock {
public:
  /*
   * acquire online ddl lock for transfer and release after trans end
   *
   * @return
   * - OB_SUCCESS:                  successful
   * - OB_TRY_LOCK_ROW_CONFLICT:    failed and guarantee not locked
   * - other:                       unknown, transaction must abort
   */
  static int lock_for_transfer_in_trans(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const ObTabletID &tablet_id,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  /*
   * acquire online ddl lock for transfer
   *
   * @return
   * - OB_SUCCESS:  successful
   * - other:       unknown, transaction must be abort
   */
  static int lock_for_transfer(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const ObTabletID &tablet_id,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  /*
   * release online ddl lock for transfer
   *
   * @return
   * - OB_SUCCESS:                  all locks are unlocked here
   * - OB_OBJ_LOCK_NOT_EXIST:       at least one lock is not locked, others are unlocked here
   * - other:                       unknown, transaction must be abort
   */
  static int unlock_for_transfer(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const ObTabletID &tablet_id,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  static int lock_table_in_trans(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  static int lock_tablets_in_trans(
      const uint64_t tenant_id,
      const ObIArray<ObTabletID> &tablet_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  static int lock_table(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  static int lock_tablets(
      const uint64_t tenant_id,
      const ObIArray<ObTabletID> &tablet_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans);

  static int unlock_table(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans,
      bool &some_lock_not_exist);

  static int unlock_tablets(
      const uint64_t tenant_id,
      const ObIArray<ObTabletID> &tablet_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      const int64_t timeout_us,
      ObMySQLTransaction &trans,
      bool &some_lock_not_exist);
};

} // namespace storage
} // namespace oceanbase
#endif
