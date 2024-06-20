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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

class ObTableLockServiceTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTableLockServiceTest() : ObSimpleClusterTestBase("test_ob_lock_service_") {}
  void get_table_id(const char* tname, uint64_t &table_id);
  void get_table_part_ids(const uint64_t table_id, ObIArray<ObObjectID> &part_ids);
  void get_table_tablets(const uint64_t table_id, ObTabletIDArray &tablet_list);
};

void ObTableLockServiceTest::get_table_id(const char* tname, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  static bool need_init = true;
  if (need_init) {
    need_init = false;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  }
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    ObSqlString sql;
    table_id = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select table_id from __all_table where table_name='%s'", tname));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_uint("table_id", table_id));
    }
  }
}

void ObTableLockServiceTest::get_table_part_ids(const uint64_t table_id,
                                                ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;
  int64_t latest_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = nullptr;
  share::ObTenantSwitchGuard tenant_guard;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  part_ids.reset();
  ret = tenant_guard.switch_to(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  ret = schema_service->get_schema_version_in_inner_table(sql_proxy,
                                                          schema_status,
                                                          latest_schema_version);
  ret = schema_service->async_refresh_schema(tenant_id,
                                             latest_schema_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service->get_tenant_schema_guard(tenant_id,
                                                schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_schema);
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  share::schema::ObPartitionSchemaIter partition_iter(*table_schema,
                                       check_partition_mode);
  ObObjectID obj_id;
  while (OB_SUCC(partition_iter.next_object_id(obj_id))) {
    ret = part_ids.push_back(obj_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

void ObTableLockServiceTest::get_table_tablets(const uint64_t table_id,
                                               ObTabletIDArray &tablet_list)
{
  int ret = OB_SUCCESS;
  int64_t latest_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = nullptr;
  share::ObTenantSwitchGuard tenant_guard;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  tablet_list.reset();
  ret = tenant_guard.switch_to(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  ret = schema_service->get_schema_version_in_inner_table(sql_proxy,
                                                          schema_status,
                                                          latest_schema_version);
  ret = schema_service->async_refresh_schema(tenant_id,
                                             latest_schema_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service->get_tenant_schema_guard(tenant_id,
                                                schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_schema);
  if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
    ret = tablet_list.push_back(table_schema->get_tablet_id());
    ASSERT_EQ(OB_SUCCESS, ret);
  } else {
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    share::schema::ObPartitionSchemaIter partition_iter(*table_schema,
                                         check_partition_mode);
    ObTabletID tablet_id;
    while (OB_SUCC(partition_iter.next_tablet_id(tablet_id))) {
      ret = tablet_list.push_back(tablet_id);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(ObTableLockServiceTest, observer_start)
{
  LOG_INFO("observer_start succ");
}

TEST_F(ObTableLockServiceTest, test_ctx)
{
  const uint64_t table_id = 1;
  int64_t timeout_us = 0;
  int64_t retry_timeout_us = 0;

  // 1. TRY LOCK && NOT DEAD LOCK AVOID
  ObTableLockService::ObTableLockCtx ctx_try_lock(ObTableLockTaskType::LOCK_TABLE, table_id, timeout_us, retry_timeout_us);
  ASSERT_TRUE(ctx_try_lock.is_try_lock());
  ASSERT_FALSE(ctx_try_lock.is_deadlock_avoid_enabled());

  // 2. TIMEOUT && NOT DEAD LOCK AVOID
  timeout_us = 15;
  retry_timeout_us = 15;
  ObTableLockService::ObTableLockCtx ctx_no_try_lock(ObTableLockTaskType::LOCK_TABLE, table_id, timeout_us, retry_timeout_us);
  ASSERT_FALSE(ctx_no_try_lock.is_try_lock());
  ASSERT_FALSE(ctx_no_try_lock.is_deadlock_avoid_enabled());

  // 3. TIMEOUT && DEAD LOCK AVOID
  timeout_us = 60 * 1000 * 1000;
  retry_timeout_us = 15;
  ObTableLockService::ObTableLockCtx ctx_deadlock_avoid(ObTableLockTaskType::LOCK_TABLE, table_id, timeout_us, retry_timeout_us);
  LOG_INFO("ObTableLockServiceTest::test_ctx", K(ctx_deadlock_avoid));
  ASSERT_FALSE(ctx_deadlock_avoid.is_try_lock());
  ASSERT_TRUE(ctx_deadlock_avoid.is_deadlock_avoid_enabled());
}

TEST_F(ObTableLockServiceTest, iter_ls)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObTableLockServiceTest::iter_ls");
  share::ObTenantSwitchGuard tenant_guard;
  ObSharedGuard<ObLSIterator> ls_iter;
  if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter,
                                                     ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", KR(ret));
  } else {
    ObLS *ls = NULL;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("scan next ls failed.", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ls", KR(ret));
      } else {
        const share::ObLSID &ls_id = ls->get_ls_id();
        LOG_INFO("get ls:", K(ls_id), KPC(ls));
      }
    }
  }
}

TEST_F(ObTableLockServiceTest, create_table)
{
  LOG_INFO("ObTableLockServiceTest::create_table");
  // 1. CREATE ONE PART TABLE
  // 2. CREATE MULTI PART TABLE
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // 1. ONE PART TABLE
  OB_LOG(INFO, "create_table one part table start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql.assign_fmt(
                   "create table t_one_part (id int, data int, primary key(id)) "
                  );
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  OB_LOG(INFO, "create_table one part table succ");
  OB_LOG(INFO, "insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_one_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  OB_LOG(INFO, "check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) row_cnt from t_one_part"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    ASSERT_EQ(row_cnt, 1);
  }

  // 2. MULTI PART TABLE
  OB_LOG(INFO, "create_table multi part table start");
  {
    ObSqlString sql;
    sql.assign_fmt(
      "create table t_multi_part (id int, data int, primary key(id)) "
      "partition by range(id) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than MAXVALUE)");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  OB_LOG(INFO, "create_table multi part table succ");
  OB_LOG(INFO, "insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 101, 101));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 202, 202));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  OB_LOG(INFO, "check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) row_cnt from t_multi_part"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    ASSERT_EQ(row_cnt, 3);
  }
}

TEST_F(ObTableLockServiceTest, lock_table)
{
  LOG_INFO("ObTableLockServiceTest::lock_table");
  int ret = OB_SUCCESS;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner_by_value(1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner_by_value(2));
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  share::ObTenantSwitchGuard tenant_guard;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));
  // 1. LOCK TABLE
  // 1.1 lock one part table
  LOG_INFO("ObTableLockServiceTest::lock_table 1.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.2 lock multi part table
  LOG_INFO("ObTableLockServiceTest::lock_table 1.2");
  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. UNLOCK TABLE
  // 2.1 unlock one part table
  LOG_INFO("ObTableLockServiceTest::lock_table 2.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 unlock multi part table
  LOG_INFO("ObTableLockServiceTest::lock_table 2.2");
  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. UNLOCK NOT EXIST LOCK
  // 3.1 check unlock with no lock
  LOG_INFO("ObTableLockServiceTest::lock_table 3.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.2 check unlock with no lock of the specified owner
  LOG_INFO("ObTableLockServiceTest::lock_table 3.2");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.3 check unlock with no lock of specified lock mode
  LOG_INFO("ObTableLockServiceTest::lock_table 3.3");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               SHARE,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 4. LOCK TWICE
  LOG_INFO("ObTableLockServiceTest::lock_table 4.0");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_part)
{
  LOG_INFO("ObTableLockServiceTest::lock_part");
  int ret = OB_SUCCESS;
  // 1. LOCK PARTITION
  // 1. lock multi part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObSEArray<ObObjectID, 1> part_ids;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner_by_value(1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner_by_value(2));
  ObLockPartitionRequest lock_arg;
  ObUnLockPartitionRequest unlock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1. LOCK MULTI PART TABLE
  // 1.1 lock multi part table
  LOG_INFO("ObTableLockServiceTest::lock_part 1.1");
  part_ids.reset();
  get_table_id("t_multi_part", table_id);
  get_table_part_ids(table_id, part_ids);

  lock_mode = ROW_EXCLUSIVE;
  lock_arg.owner_id_ = out_trans_owner_1;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.table_id_ = table_id;
  lock_arg.part_object_id_ = part_ids[0];
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock_partition(*tx_desc,
                                                 tx_param,
                                                 lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock
  LOG_INFO("ObTableLockServiceTest::lock_part 1.2");
  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2. COMMIT
  LOG_INFO("ObTableLockServiceTest::lock_part 2");
  int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. check again
  LOG_INFO("ObTableLockServiceTest::lock_part 3");
  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 4. UNLOCK
  LOG_INFO("ObTableLockServiceTest::unlock_part 4");
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));
  // unlock part
  LOG_INFO("ObTableLockServiceTest::lock_part 4.1");
  part_ids.reset();
  get_table_id("t_multi_part", table_id);
  get_table_part_ids(table_id, part_ids);

  lock_mode = ROW_EXCLUSIVE;
  unlock_arg.owner_id_ = out_trans_owner_1;
  unlock_arg.lock_mode_ = lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;
  unlock_arg.table_id_ = table_id;
  unlock_arg.part_object_id_ = part_ids[0];

  ret = MTL(ObTableLockService*)->unlock_partition(*tx_desc,
                                                   tx_param,
                                                   unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // commit
  LOG_INFO("ObTableLockServiceTest::lock_part 4.2");
  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // check again
  LOG_INFO("ObTableLockServiceTest::lock_part 4.3");
  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_tablet)
{
  // 1. LOCK TABLET
  // 1.1 lock tablet of one part table
  // 1.2 lock tablet of multi part table
  // 2. UNLOCK TABLET
  // 2.1 unlock tablet of one part table
  // 2.2 unlock tablet of multi part table
  int ret = OB_SUCCESS;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner_by_value(1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner_by_value(2));
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  share::ObTenantSwitchGuard tenant_guard;
  ObTabletIDArray tablet_list;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));
  // 1. LOCK TABLE
  // 1.1 lock one part table
  LOG_INFO("ObTableLockServiceTest::lock_tablet 1.1");
  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.2 lock multi part table
  LOG_INFO("ObTableLockServiceTest::lock_tablet 1.2");
  get_table_id("t_multi_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. UNLOCK TABLE
  // 2.1 unlock one part table
  LOG_INFO("ObTableLockServiceTest::lock_tablet 2.1");
  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 unlock multi part table
  LOG_INFO("ObTableLockServiceTest::lock_tablet 2.2");
  get_table_id("t_multi_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. UNLOCK NOT EXIST LOCK
  // 3.1 check unlock with no lock
  LOG_INFO("ObTableLockServiceTest::lock_tablet 3.1");
  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.2 check unlock with no lock of the specified owner
  LOG_INFO("ObTableLockServiceTest::lock_tablet 3.2");
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.3 check unlock with no lock of specified lock mode
  LOG_INFO("ObTableLockServiceTest::lock_tablet 3.3");
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                SHARE,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 4. LOCK TWICE
  LOG_INFO("ObTableLockServiceTest::lock_tablet 4.0");
  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, in_trans_lock_table)
{
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table");
  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  // 1.2 lock multi part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID in_trans_owner(ObTableLockOwnerID::default_owner());
  ObTableLockOwnerID out_trans_owner(ObTableLockOwnerID::get_owner_by_value(1));
  ObLockTableRequest lock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1.1 lock one part table
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table 1.1");
  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock_table(*tx_desc,
                                             tx_param,
                                             lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table 1.2");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner);
  ASSERT_EQ(OB_EAGAIN, ret);
  // 2. LOCK MULTI PART TABLE
  // 2.1 lock multi part table
  // lock upgrade
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table 2.1");
  get_table_id("t_multi_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock_table(*tx_desc,
                                             tx_param,
                                             lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check lock
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table 2.2");
  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner);
  ASSERT_EQ(OB_EAGAIN, ret);
  // 3. CLEAN
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_table 3");
  const int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_out_trans_after_in_trans)
{
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans");
  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner_by_value(1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner_by_value(2));
  ObTableLockOwnerID in_trans_owner(ObTableLockOwnerID::default_owner());
  ObLockTableRequest lock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1. ONLY IN_TRANS TEST
  // 1.1 lock in_trans
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 1.1");
  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock_table(*tx_desc,
                                             tx_param,
                                             lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 1.2");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 1.3. commit lock
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 1.3");
  int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.4 recheck lock after commit
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 1.4");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.5 unlock check lock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 1.4");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. BOTH OUT_TRANS AND IN_TRANS
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));
  // 2.1 lock in_trans lock
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.1");
  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock_table(*tx_desc,
                                             tx_param,
                                             lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.2 lock out_trans lock
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.2");
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = out_trans_owner_1;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock_table(*tx_desc,
                                             tx_param,
                                             lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.3 check lock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.3");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2.4 commit lock
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.4");
  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.5 recheck lock after commit
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.5");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2.6 unlock out_trans lock
  lock_mode = ROW_EXCLUSIVE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.6");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.7 recheck after unlock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.7");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.8 unlock check lock
  lock_mode = SHARE;
  LOG_INFO("ObTableLockServiceTest::lock_out_trans_after_in_trans 2.8");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, in_trans_lock_obj)
{
  LOG_INFO("ObTableLockServiceTest::in_trans_lock_obj");
  int ret = OB_SUCCESS;

  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTransService *txs = nullptr;
  uint64_t obj_id1 = 1010;
  ObTableLockMode lock_mode1 = SHARE;
  ObTableLockOwnerID OWNER_ONE;
  ObLockObjRequest lock_arg;
  OWNER_ONE.convert_from_value(1);

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  LOG_INFO("ObTableLockServiceTest::in_trans_lock_obj 1.1");
  lock_arg.lock_mode_ = lock_mode1;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_COMMON_OBJ;
  lock_arg.obj_id_ = obj_id1;
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock_obj(*tx_desc1,
                                           tx_param,
                                           lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("ObTableLockServiceTest::in_trans_lock_obj 1.2");
  ObTxDesc *tx_desc2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));
  lock_arg.lock_mode_ = lock_mode1;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_COMMON_OBJ;
  lock_arg.obj_id_ = obj_id1;
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock_obj(*tx_desc2,
                                           tx_param,
                                           lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("ObTableLockServiceTest::in_trans_lock_obj 1.3");
  ObTxDesc *tx_desc3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  ObTableLockMode lock_mode2 = EXCLUSIVE;
  lock_arg.lock_mode_ = lock_mode2;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_COMMON_OBJ;
  lock_arg.obj_id_ = obj_id1;
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock_obj(*tx_desc3,
                                           tx_param,
                                           lock_arg);
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);

  LOG_INFO("ObTableLockServiceTest::in_trans_lock_obj 1.4");
  const int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);
}

}// end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  OB_LOGGER.set_mod_log_levels("storage.tablelock:debug");
  OB_LOGGER.set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
