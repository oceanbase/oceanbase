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
#include <stdlib.h>
#define USING_LOG_PREFIX TABLELOCK
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"
#include "mittest/mtlenv/tablelock/table_lock_common_env.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_ob_obj_lock_garbage_collector";
static const char *BORN_CASE_NAME = "ObOBJLockGCBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObOBJLockGCAfterRestartTest";

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{
// We modify the exection interval of the obj_lcoK_garbage_collector here,
// to make sure that the empty obj locks can be recycled in time.
// int64_t ObOBJLockGarbageCollector::GARBAGE_COLLECT_EXEC_INTERVAL = 1_s;
// int64_t ObOBJLockGarbageCollector::GARBAGE_COLLECT_PRECISION = 100_ms;
}  // namespace tablelock
}  // namespace transaction
namespace unittest
{
#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define READ_SQL_BY_CONN(conn, sql_str)         \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));

class ObOBJLockGarbageCollectorTestBase : public ObSimpleClusterTestBase
{
public:
  ObOBJLockGarbageCollectorTestBase() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void get_ls(const uint64_t tenant_id, const ObLSID ls_id, ObLS *&ls)
  {
    LOG_INFO("get_ls start");
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
    LOG_INFO("get_ls end");
  }

  void get_table_id(const char* tname, uint64_t &table_id)
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

  void get_table_tablets(const uint64_t table_id,
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
    schema_service = MTL(ObTenantSchemaService *)->get_schema_service();
    ret = schema_service->get_schema_version_in_inner_table(
        sql_proxy, schema_status, latest_schema_version);
    ret =
        schema_service->async_refresh_schema(tenant_id, latest_schema_version);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = schema_service->get_tenant_schema_guard(tenant_id, schema_guard);
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

  // This function is not atomic, it means that it may get obj lock even
  // if it was recycled just now. However, we can coontrol the obj lock
  // by the execution logic in the test case to avoid this situation.
  void table_has_obj_lock(const uint64_t table_id, bool &has_obj_lock)
  {
    int ret = OB_SUCCESS;
    ObLS *ls = nullptr;
    ObLockIDIterator lock_id_iter;
    ObLockID target_lock_id;
    ObLockID curr_lock_id;

    get_ls(OB_SYS_TENANT_ID, share::ObLSID(share::ObLSID::SYS_LS_ID), ls);
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(OB_SUCCESS, ls->get_lock_id_iter(lock_id_iter));
    ASSERT_EQ(true, lock_id_iter.is_ready());

    ASSERT_EQ(OB_SUCCESS,
              transaction::tablelock::get_lock_id(table_id, target_lock_id));
    has_obj_lock = false;

    do {
      if (OB_SUCC(lock_id_iter.get_next(curr_lock_id))) {
        if (target_lock_id == curr_lock_id) {
          has_obj_lock = true;
          break;
        }
      }
    } while (OB_SUCC(ret));
  }

  void wakeup_gc_thread()
  {
    ObTableLockService *tablelock_service = nullptr;
    tablelock_service = MTL(transaction::tablelock::ObTableLockService *);
    ASSERT_NE(nullptr, tablelock_service);
    ASSERT_EQ(OB_SUCCESS, tablelock_service->garbage_collect_right_now());
  }

  void get_lock_memtable(ObLockMemtable *&lock_memtable)
  {
    ObLS *ls = nullptr;
    ObTableHandleV2 table_handle;
    get_ls(OB_SYS_TENANT_ID, share::ObLSID(share::ObLSID::SYS_LS_ID), ls);
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(OB_SUCCESS, ls->get_lock_table()->get_lock_memtable(table_handle));
    ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));
  }

  void init_test_lock_op()
  {
    // table_id = 1 and trans_id = 1 are valid in real observer,
    // so we modify them to a differnt value to avoid conflict
    DEFAULT_TABLE = 123456;
    DEFAULT_TRANS_ID = 123456;
    init_default_lock_test_value();
  }
};

class ObOBJLockGCBeforeRestartTest : public ObOBJLockGarbageCollectorTestBase {
};
class ObOBJLockGCAfterRestartTest : public ObOBJLockGarbageCollectorTestBase {
};

TEST_F(ObOBJLockGCBeforeRestartTest, create_table)
{
  LOG_INFO("ObOBJLockGCBeforeRestartTest::create_table");
  // 1. CREATE ONE PART TABLE
  // 2. CREATE MULTI PART TABLE
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // 1. ONE PART TABLE
  LOG_INFO("create_table one part table start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(
        OB_SUCCESS,
        sql.assign_fmt(
            "create table t_one_part (id int, data int, primary key(id))"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("create_table one part table succ");

  LOG_INFO("insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("insert into t_one_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("select count(*) row_cnt from t_one_part"));
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
  LOG_INFO("create_table multi part table start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(
        OB_SUCCESS,
        sql.assign_fmt(
            "create table t_multi_part (id int, data int, primary key(id)) "
            "partition by range(id) (partition p0 values less than (100), "
            "partition p1 values less than (200), partition p2 values less "
            "than MAXVALUE)"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("create_table multi part table succ");

  LOG_INFO("insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("insert into t_multi_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(
        OB_SUCCESS,
        sql.assign_fmt("insert into t_multi_part values(%d, %d)", 101, 101));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(
        OB_SUCCESS,
        sql.assign_fmt("insert into t_multi_part values(%d, %d)", 202, 202));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("select count(*) row_cnt from t_multi_part"));
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

TEST_F(ObOBJLockGCBeforeRestartTest, obj_lock_gc_with_tablelock_service)
{
  LOG_INFO("ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service");
  int ret = OB_SUCCESS;
  ObTableLockOwnerID OWNER_ONE;
  ObTableLockOwnerID OWNER_TWO;
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  share::ObTenantSwitchGuard tenant_guard;
  bool has_obj_lock;
  OWNER_ONE.convert_from_value(1);
  OWNER_TWO.convert_from_value(2);

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  // 1. LOCK TABLE AND UNLOCK TABLE
  // 1.1 lock one part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);

  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 1.2 lock multi part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.2");
  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             OWNER_TWO);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.3 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.3");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 1.4 unlock one part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.4");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.5 check obj lock status
  // the obj lock is still be there, due to gc thread
  // will try to recycle empty obj lock every 10 mins
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.5");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 1.6 unlock multi part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.6");
  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               OWNER_TWO);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.7 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.7");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 1.8 wake up gc thread to clear empty obj locks
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 1.8");
  wakeup_gc_thread();
  // wait gc thread to recycle obj lock
  sleep(2);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);
  get_table_id("t_one_part", table_id);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);

  // 2. UNLOCK TABLE AND LOCK TABLE
  // 2.1 unlock one part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 2.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               SHARE,
                                               OWNER_ONE);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 2.2 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 2.2");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);
  // 2.3 lock one part table
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 2.3");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             SHARE,
                                             OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.4 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 2.4");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);

  // 3. LOCK WITH DIFFERNT OWNER AND UNLOCK
  // 3.1 lock one part table with different owner
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.1");
  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             SHARE,
                                             OWNER_TWO);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.2 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.2");
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 3.3 unlock previous lock which is owned by owner one
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.3");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               SHARE,
                                               OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.4 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.4");
  wakeup_gc_thread();
  // wait gc thread to recycle obj lock
  sleep(2);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 3.5 unlock current lock which is owned by owner two
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.5");
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               SHARE,
                                               OWNER_TWO);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.6 check obj lock status
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 3.6");
  wakeup_gc_thread();
  // wait gc thread to recycle obj lock
  sleep(2);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);

  // 4. LOCK TABLE FOR RESTART
  LOG_INFO(
      "ObOBJLockGCBeforeRestartTest::obj_lock_gc_with_tablelock_service 4");
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
}

TEST_F(ObOBJLockGCBeforeRestartTest, op_list_gc_with_mock_lock_map)
{
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map");
  ObLockMemtable *lock_memtable = nullptr;
  ObOBJLock *obj_lock = nullptr;
  bool has_obj_lock;
  share::SCN commit_scn;
  share::SCN commit_version;
  share::ObTenantSwitchGuard tenant_guard;

  commit_scn.set_base();
  commit_version.set_base();

  get_lock_memtable(lock_memtable);
  ObOBJLockMap &obj_lock_map = lock_memtable->obj_lock_map_;
  init_test_lock_op();

  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  // 1. REPLAY UNLOCK OP AND LOCK OP,
  // THEN COMMIT UNLOCK OP BEFORE LOCK OP
  // 1.1 recover unlock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.1");
  ASSERT_EQ(OB_SUCCESS,
            obj_lock_map.recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP));
  // 1.2 check obj lock exists
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.2");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.get_obj_lock_with_ref_(
                          DEFAULT_TABLE_LOCK_ID, obj_lock));
  ASSERT_NE(nullptr, obj_lock);
  // 1.3 recover lock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.3");
  ASSERT_EQ(OB_SUCCESS,
            obj_lock_map.recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP));
  // 1.4 verify obj lock status by log
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.4");
  obj_lock->print();
  // 1.5 commit unlock op
  // We will try to compact lock ops if there's paired and committed
  // lock op in the op_list when commit unlock op. However, this unlock
  // op cannot be compactted here, because the lock op in the op_list
  // is still running. You can find that it tried to compact but failed
  // (by is_compcat = false) from the log.
  // This situation will occur during replyaing in the followers.
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.5");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.update_lock_status(
                            DEFAULT_OUT_TRANS_UNLOCK_OP, commit_version,
                            commit_scn, COMMIT_LOCK_OP_STATUS));
  // 1.6 commit lock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.6");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.update_lock_status(
                            DEFAULT_OUT_TRANS_LOCK_OP, commit_version,
                            commit_scn, COMMIT_LOCK_OP_STATUS));
  // 1.7 verify obj lock status by log
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.7");
  obj_lock->print();
  // revert obj lock
  obj_lock_map.lock_map_.revert(obj_lock);
  // 1.8 check obj lock status
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.8");
  table_has_obj_lock(DEFAULT_TABLE, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 1.9 wake up gc thread
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 1.9");
  wakeup_gc_thread();
  // wait gc thread to recycle obj lock
  sleep(2);
  table_has_obj_lock(DEFAULT_TABLE, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);

  // 2. REPLAY UNLOCK OP AND LOCK OP,
  // THEN COMMIT LOCK OP BEFORE UNLOCK OP
  // The lock ops will be compacted when the unlock op is committed,
  // so there's no need to gc it in this situationl.
  // 2.1 recover unlock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.1");
  ASSERT_EQ(OB_SUCCESS,
            obj_lock_map.recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP));
  // 2.2 check obj lock exists
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.2");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.get_obj_lock_with_ref_(
                          DEFAULT_TABLE_LOCK_ID, obj_lock));
  ASSERT_NE(nullptr, obj_lock);
  // 2.3 recover lock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.3");
  ASSERT_EQ(OB_SUCCESS,
            obj_lock_map.recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP));
  // 2.4 verify obj lock status by log
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.4");
  obj_lock->print();
  // 2.5 commit lock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.5");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.update_lock_status(
                            DEFAULT_OUT_TRANS_LOCK_OP, commit_version,
                            commit_scn, COMMIT_LOCK_OP_STATUS));
  // 2.6 commit unlock op
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.6");
  ASSERT_EQ(OB_SUCCESS, obj_lock_map.update_lock_status(
                            DEFAULT_OUT_TRANS_UNLOCK_OP, commit_version,
                            commit_scn, COMMIT_LOCK_OP_STATUS));
  // 2.7 verify obj lock status by log
  // You will find that obj lock is still there, due to we move the
  // gc process to the gc thread in the backend. However, the obj lock
  // is empty, i.e. there's no lock ops in it. Because the compaction
  // process will execute directly if the lock op which will be committed
  // is an out trans unlock lock op.
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.7");
  obj_lock->print();
  // revert obj lock
  obj_lock_map.lock_map_.revert(obj_lock);
  // 2.8 check obj lock status
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.8");
  table_has_obj_lock(DEFAULT_TABLE, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 2.9 wake up gc thread
  LOG_INFO("ObOBJLockGCAfterRestartTest::op_list_gc_with_mock_lock_map 2.9");
  wakeup_gc_thread();
  // wait gc thread to recycle obj lock
  sleep(2);
  table_has_obj_lock(DEFAULT_TABLE, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);
}

TEST_F(ObOBJLockGCAfterRestartTest, obj_lock_gc_after_restart)
{
  // You may find that gc thread tried to compact
  // the obj lock of this table in force compaction
  // mode from the log file. It means that the gc
  // thread start successfully before leader comes
  // back to work.
  // (The gc thread will compact table lock ops in
  // force mode only when it's called during the
  // period when a follower is switching to leader)
  LOG_INFO("ObOBJLockGCAfterRestartTest::obj_lock_gc_after_restart");
  uint64_t table_id;
  bool has_obj_lock;
  // 1. check obj lock status of table t_one_part
  LOG_INFO("ObOBJLockGCAfterRestartTest::obj_lock_gc_after_restart 1");
  get_table_id("t_one_part", table_id);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_TRUE(has_obj_lock);
  // 2. check obj lock status of table t_multi_part
  LOG_INFO("ObOBJLockGCAfterRestartTest::obj_lock_gc_after_restart 2");
  get_table_id("t_multi_part", table_id);
  table_has_obj_lock(table_id, has_obj_lock);
  ASSERT_FALSE(has_obj_lock);
}
}  // namespace unittest
}  // namespace oceanbase


int main(int argc, char **argv)
{
  // std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  // oceanbase::unittest::init_gtest_output(gtest_file_name);
  ObSimpleServerRestartHelper restart_helper(argc,
                                             argv,
                                             TEST_FILE_NAME,
                                             BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(10); // sleep 10s for schema restore
  OB_LOGGER.set_mod_log_levels("storage.tablelock:debug");  // it seems doesn't work
  OB_LOGGER.set_enable_async_log(false);
  return restart_helper.run();
}
