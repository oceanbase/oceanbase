/**
 * Copyright (c) 2022 OceanBase
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
#include "mtlenv/mock_tenant_module_env.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"

namespace oceanbase
{
namespace unittest
{
static const char *default_tenant_name = "tt1";
static const char *default_snapshot_name = "snapshot_1";

class TestTenantSnapshotService : public ObSimpleClusterTestBase
{
public:
  TestTenantSnapshotService() : ObSimpleClusterTestBase("test_tenant_snapshot_service_") {}
  virtual ~TestTenantSnapshotService(){}
  void user_create_tenant_snapshot(ObTenantSnapshotID &snapshot_id,
                                   const int tenant_id,
                                   ObString tenant_name = default_tenant_name,
                                   ObString snapshot_name = default_snapshot_name);
  void user_drop_tenant_snapshot(ObString tenant_name = default_tenant_name,
                                 ObString snapshot_name = default_snapshot_name);
  bool is_tenant_snapshot_meta_exist(const ObTenantSnapshotID &snapshot_id);
  bool is_ls_snapshot_meta_exist(const ObTenantSnapshotID &snapshot_id);
  void open_log_archive(const int tenant_id = 1,
                        ObString tenant_name = default_tenant_name);
};

void TestTenantSnapshotService::open_log_archive(const int tenant_id, ObString tenant_name)
{
  int ret = OB_SUCCESS;
  char cur_dir[OB_MAX_FILE_NAME_LENGTH];
  ASSERT_NE(nullptr, getcwd(cur_dir, OB_MAX_FILE_NAME_LENGTH));
  SERVER_LOG(INFO, "get cwd as log_archive_dest", K(ObString(cur_dir)));

  MTL_SWITCH(tenant_id) {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObSqlString sql;
    int64_t affected_rows = -1;
    sql.assign_fmt("alter system set archive_lag_target = '10s' tenant = %s", tenant_name.ptr());
    ASSERT_EQ(OB_SUCCESS, sql_proxy->write(tenant_id, sql.ptr(), affected_rows));

    sql.assign_fmt("alter system set log_archive_dest = 'location=file://%s/log_arch' tenant = %s",
                                                                        cur_dir, tenant_name.ptr());
    ASSERT_EQ(OB_SUCCESS, sql_proxy->write(tenant_id, sql.ptr(), affected_rows));

    sql.assign_fmt("alter system archivelog tenant = %s", tenant_name.ptr());
    ASSERT_EQ(OB_SUCCESS, sql_proxy->write(tenant_id, sql.ptr(), affected_rows));

    sql.assign_fmt("ALTER SYSTEM MINOR FREEZE TENANT = %s", tenant_name.ptr());
    ASSERT_EQ(OB_SUCCESS, sql_proxy->write(tenant_id, sql.ptr(), affected_rows));

    SERVER_LOG(INFO, "open log archive service succ");
    sleep(60); // wait for archive log
    SERVER_LOG(INFO, "ready to create tenant snapshot after sleep");
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTenantSnapshotService::user_create_tenant_snapshot(ObTenantSnapshotID &snapshot_id,
                                                            const int tenant_id,
                                                            ObString tenant_name,
                                                            ObString snapshot_name)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "TestTenantSnapshotService begin creating tenant snapshot...");
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;

  uint64_t tmp_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t max_retry_cnt = 1;

  ObTenantSnapItem item;
  ObTenantSnapshotTableOperator table_operator;
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_EQ(OB_SUCCESS, table_operator.init(tenant_id, GCTX.sql_proxy_));
  do {
    if (OB_FAIL(rootserver::ObTenantSnapshotUtil::create_tenant_snapshot(tenant_name,
                                                                        snapshot_name,
                                                                        tmp_tenant_id,
                                                                        snapshot_id))) {
        if (OB_ERR_TENANT_SNAPSHOT == ret) {
          ASSERT_LE(0, max_retry_cnt);
          ASSERT_NE(nullptr, GCTX.sql_proxy_);
          sql.assign_fmt("ALTER SYSTEM MINOR FREEZE TENANT = %s", tenant_name.ptr());
          ASSERT_EQ(OB_SUCCESS, sql_proxy->write(tenant_id, sql.ptr(), affected_rows));
          sleep(20);   // wait for observer creating tenant snapshot and ls_snap
          max_retry_cnt++;
          SERVER_LOG(INFO, "wait for creating minor freeze complete", K(max_retry_cnt), K(snapshot_name));
        } else {
          SERVER_LOG(WARN, "fail to create tenant snapshot",
              KR(ret), K(tenant_name), K(snapshot_name), K(tmp_tenant_id));
          break;
        }
    } else {
      SERVER_LOG(INFO, "create tenant snapshot succ", K(tenant_name), K(snapshot_name), K(snapshot_id));
      break; // create snapshot succ
    }
  } while (max_retry_cnt < 20);

  int count = 0;
  while(OB_SUCC(ret)){  // wait for tenant snapshot status become NORMAL
    ob_usleep(1 * 1000 * 1000L); // 1s
    item.reset();
    ASSERT_EQ(OB_SUCCESS, table_operator.get_tenant_snap_item(snapshot_id, true, item));
    SERVER_LOG(WARN, "get tenant snapshot", K(item));
    if (ObTenantSnapStatus::NORMAL == item.get_status()) {
      SERVER_LOG(INFO, "TestTenantSnapshotService create tenant snapshot succ", K(snapshot_id), K(item));
      break;
    } else if (ObTenantSnapStatus::CREATING == item.get_status()
               || ObTenantSnapStatus::DECIDED == item.get_status()) {
      SERVER_LOG(INFO, "notify_scheduler one more time", K(++count));
      ASSERT_EQ(OB_SUCCESS, rootserver::ObTenantSnapshotUtil::notify_scheduler(tenant_id));
    } else {
      SERVER_LOG(ERROR, "unexpected tenant snapshot status", KR(ret), K(item));
      ASSERT_TRUE(false);
    }
  }
  SERVER_LOG(INFO, "TestTenantSnapshotService create tenant snapshot finish", K(snapshot_id), K(item));
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTenantSnapshotService::user_drop_tenant_snapshot(ObString tenant_name, ObString snapshot_name)
{
  ASSERT_EQ(OB_SUCCESS, rootserver::ObTenantSnapshotUtil::drop_tenant_snapshot(tenant_name, snapshot_name));
}

bool TestTenantSnapshotService::is_tenant_snapshot_meta_exist(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService *);
  EXPECT_NE(nullptr, tenant_snapshot_service);

  bool is_tenant_snap_exist = false;
  ObArray<ObTenantSnapshotID> snapshot_ids;
  tenant_snapshot_service->meta_handler_.get_all_tenant_snapshot(snapshot_ids);
  ARRAY_FOREACH_N(snapshot_ids, i, count) {
    if (snapshot_ids.at(i).id() == snapshot_id.id()) {
      is_tenant_snap_exist = true;
    }
  }
  SERVER_LOG(INFO, "after create tenant snapshot, all tenant snapshot ids", K(snapshot_ids.size()), K(snapshot_ids));
  return is_tenant_snap_exist;
}

bool TestTenantSnapshotService::is_ls_snapshot_meta_exist(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService *);
  EXPECT_NE(nullptr, tenant_snapshot_service);
  ObArray<ObLSID> ls_ids;
  tenant_snapshot_service->meta_handler_.get_all_ls_snapshot(snapshot_id, ls_ids);
  SERVER_LOG(INFO, "after create tenant snapshot, get ls_ids", K(ls_ids));
  return ls_ids.size() > 0;
}

TEST_F(TestTenantSnapshotService, test_create_tenant_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_NE(0, tenant_id);
  open_log_archive(); // open log archive before creating tenant snapshot
  ASSERT_FALSE(HasFatalFailure());

  MTL_SWITCH(tenant_id)
  {
    SERVER_LOG(INFO, "switch to tenant", K(MTL_ID()), K(tenant_id));
    ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService *);
    ASSERT_NE(nullptr, tenant_snapshot_service);

    ObTenantSnapshotID snapshot_id;
    user_create_tenant_snapshot(snapshot_id, tenant_id);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(true, is_tenant_snapshot_meta_exist(snapshot_id));
    ASSERT_EQ(true, is_ls_snapshot_meta_exist(snapshot_id));

    // simulate delayed rpc try to create the same snapshot
    obrpc::ObInnerCreateTenantSnapshotArg create_arg;
    create_arg.set_tenant_id(tenant_id);
    create_arg.set_tenant_snapshot_id(snapshot_id);
    ASSERT_EQ(true, create_arg.is_valid());
    tenant_snapshot_service->running_mode_ = ObTenantSnapshotService::NORMAL;
    tenant_snapshot_service->run_in_normal_mode_();
    ASSERT_EQ(ObTenantSnapshotService::NORMAL, tenant_snapshot_service->running_mode_);
    ASSERT_EQ(true, tenant_snapshot_service->meta_loaded_);
    // create_tenant_snapshot() do nothing for existed snapshot
    ASSERT_EQ(OB_SUCCESS, tenant_snapshot_service->create_tenant_snapshot(create_arg));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTenantSnapshotService, test_drop_tenant_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_NE(0, tenant_id);

  MTL_SWITCH(tenant_id)
  {
    SERVER_LOG(INFO, "switch to tenant", K(MTL_ID()), K(tenant_id));
    ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService *);
    ASSERT_NE(nullptr, tenant_snapshot_service);

    ObTenantSnapItem item;
    ObTenantSnapshotTableOperator table_operator;
    ASSERT_EQ(OB_SUCCESS, table_operator.init(tenant_id, GCTX.sql_proxy_));
    ASSERT_EQ(OB_SUCCESS, table_operator.get_tenant_snap_item(default_snapshot_name, false, item));

    ObTenantSnapshotID snapshot_id(item.get_tenant_snapshot_id());
    ASSERT_EQ(true, is_tenant_snapshot_meta_exist(snapshot_id));
    ASSERT_EQ(true, is_ls_snapshot_meta_exist(snapshot_id));

    user_drop_tenant_snapshot(default_tenant_name, default_snapshot_name);
    ASSERT_FALSE(HasFatalFailure());
    obrpc::ObInnerDropTenantSnapshotArg drop_arg;
    drop_arg.set_tenant_id(tenant_id);
    drop_arg.set_tenant_snapshot_id(snapshot_id);
    ASSERT_EQ(true, drop_arg.is_valid());
    tenant_snapshot_service->running_mode_ = ObTenantSnapshotService::NORMAL;
    tenant_snapshot_service->run_in_normal_mode_();
    ASSERT_EQ(ObTenantSnapshotService::NORMAL, tenant_snapshot_service->running_mode_);
    ASSERT_EQ(true, tenant_snapshot_service->meta_loaded_);
    ASSERT_EQ(OB_SUCCESS, tenant_snapshot_service->drop_tenant_snapshot(drop_arg));
    sleep(10); // wait for background gc complete
    ASSERT_EQ(false, is_tenant_snapshot_meta_exist(snapshot_id));
    ASSERT_EQ(false, is_ls_snapshot_meta_exist(snapshot_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}