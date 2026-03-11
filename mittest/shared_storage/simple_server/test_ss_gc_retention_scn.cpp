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

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include "storage/tx_storage/ob_ls_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "close_modules/shared_storage/storage/incremental/ob_shared_meta_service.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_sstablelist_define.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector.h"
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

oceanbase::unittest::ObMockPalfKV PALF_KV;

int get_sslog_table_guard(const ObSSLogTableType type, const int64_t tenant_id, ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type) {
  case ObSSLogTableType::SSLOG_TABLE: {
    void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
    if (nullptr == proxy) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
      if (OB_FAIL(sslog_table_proxy->init())) {
        SSLOG_LOG(WARN, "fail to inint", K(ret));
      } else {
        guard.set_sslog_proxy((ObISSLogProxy *)proxy);
      }
    }
    break;
  }
  case ObSSLogTableType::SSLOG_PALF_KV: {
    void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
    if (nullptr == proxy) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
      guard.set_sslog_proxy((ObISSLogProxy *)proxy);
    }
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    SSLOG_LOG(WARN, "invalid sslog type", K(type));
    break;
  }
  }

  return ret;
}
}  // namespace sslog
}  // namespace oceanbase

namespace oceanbase
{
char *shared_storage_info = NULL;
namespace storage
{

class MockSSGCForRetention : public ObSSGarbageCollector
{
public:
  static int test_reserved_scn_calculation(const SCN &snapshot,
                                           const SCN &expected_retention_scn,
                                           SCN &actual_reserved_scn)
  {
    int ret = OB_SUCCESS;
    uint64_t tablet_version_retention_time = 0;
    SCN tablet_version_retention_scn;
    SCN reserved_scn;

    // Simulate the logic in ob_ss_garbage_collector.cpp lines 638-645
    if (OB_FAIL(get_ss_tablet_version_retention_time_(tablet_version_retention_time))) {
      LOG_WARN("get ss tablet retention time in ns failed", KR(ret), K(snapshot));
    } else if (OB_FAIL(get_ss_tablet_version_retention_scn_(tablet_version_retention_scn))) {
      LOG_WARN("get ss tablet retention scn failed", KR(ret), K(snapshot));
    } else if (FALSE_IT(reserved_scn.convert_from_ts(snapshot.convert_to_ts() - tablet_version_retention_time))) {
    } else if (!tablet_version_retention_scn.is_min()) {
      reserved_scn = SCN::min(reserved_scn, tablet_version_retention_scn);
    }

    if (OB_SUCC(ret)) {
      actual_reserved_scn = reserved_scn;
      LOG_INFO("reserved_scn calculation result",
               K(snapshot),
               K(tablet_version_retention_time),
               K(tablet_version_retention_scn),
               K(reserved_scn),
               K(expected_retention_scn));
    }
    return ret;
  }
};

}  // namespace storage
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::share;
class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1002;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

void wait_sys_to_leader()
{
  share::ObTenantSwitchGuard tenant_guard;
  int ret = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObLS *ls = nullptr;
  ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(ls_id, ls->get_ls_id());
  for (int i = 0; i < 100; i++) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (role == ObRole::LEADER) {
      ret = OB_SUCCESS;
      break;
    }
    ob_usleep(10 * 1000);
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

// Helper function: Query retention record directly from internal table
int query_retention_record_from_table(ObSimpleClusterTestBase &test_base,
                                      const uint64_t tenant_id,
                                      const ObSSGCRetentionTaskType task_type,
                                      const int64_t task_id,
                                      const share::ObLSID &ls_id,
                                      share::SCN &retention_scn,
                                      bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  retention_scn.reset();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(RunCtx.tenant_id_);

  common::ObMySQLProxy &sql_proxy = test_base.get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  const int64_t task_type_val = static_cast<int64_t>(task_type);
  int64_t ls_id_val = share::ObLSID::INVALID_LS_ID;

  if (task_type == ObSSGCRetentionTaskType::MIGRATE && ls_id.is_valid()) {
    ls_id_val = ls_id.id();
  }

  if (OB_FAIL(sql.assign_fmt("SELECT snapshot_version FROM oceanbase.%s "
                             "WHERE task_type = %ld AND task_id = %ld AND tenant_id = %lu AND ls_id = %ld",
                             share::OB_ALL_SS_GC_RESERVED_SNAPSHOT_TNAME,
                             task_type_val,
                             task_id,
                             tenant_id,
                             ls_id_val))) {
    LOG_WARN("failed to construct SELECT SQL", KR(ret), K(task_type), K(task_id), K(tenant_id), K(ls_id_val));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute SELECT from __all_ss_gc_reserved_snapshot",
                 KR(ret),
                 K(sql),
                 K(task_type),
                 K(task_id),
                 K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          found = false;
          ret = OB_SUCCESS;
          LOG_INFO("retention snapshot not found in table", K(task_type), K(task_id), K(tenant_id), K(ls_id_val));
        } else {
          LOG_WARN("failed to get next row", KR(ret));
        }
      } else {
        found = true;
        uint64_t snapshot_version = 0;
        EXTRACT_UINT_FIELD_MYSQL(*result, "snapshot_version", snapshot_version, uint64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extract snapshot_version", KR(ret));
        } else if (OB_FAIL(retention_scn.convert_for_inner_table_field(snapshot_version))) {
          LOG_WARN("failed to convert snapshot_version to SCN", KR(ret), K(snapshot_version));
        }
      }
    }
  }

  return ret;
}

// Helper function: Verify if record exists in internal table
int verify_retention_record_in_table(ObSimpleClusterTestBase &test_base,
                                     const uint64_t tenant_id,
                                     const ObSSGCRetentionTaskType task_type,
                                     const int64_t task_id,
                                     const share::ObLSID &ls_id,
                                     const share::SCN &expected_scn,
                                     bool should_exist = true)
{
  int ret = OB_SUCCESS;
  bool found = false;
  share::SCN actual_scn;

  if (OB_FAIL(query_retention_record_from_table(test_base, tenant_id, task_type, task_id, ls_id, actual_scn, found))) {
    LOG_WARN("failed to query retention record", KR(ret));
  } else if (should_exist && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("retention record should exist but not found", KR(ret), K(tenant_id), K(task_type), K(task_id), K(ls_id));
  } else if (!should_exist && found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("retention record should not exist but found", KR(ret), K(tenant_id), K(task_type), K(task_id), K(ls_id));
  } else if (should_exist && found) {
    if (actual_scn != expected_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
        "retention scn mismatch", KR(ret), K(tenant_id), K(task_type), K(task_id), K(expected_scn), K(actual_scn));
    } else {
      LOG_INFO("retention record verified successfully via direct table query",
               K(tenant_id),
               K(task_type),
               K(task_id),
               K(ls_id),
               K(actual_scn));
    }
  } else {
    // !should_exist && !found, as expected
    LOG_INFO("retention record correctly not found", K(tenant_id), K(task_type), K(task_id), K(ls_id));
  }

  return ret;
}

class ObSharedStorageRetentionScnTest : public ObSimpleClusterTestBase
{
public:
  ObSharedStorageRetentionScnTest() :
    ObSimpleClusterTestBase("test_inc_shared_storage_retention_scn_", "50G", "50G", "50G")
  {}

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

TEST_F(ObSharedStorageRetentionScnTest, create_tenant)
{
  TRANS_LOG(INFO, "create_tenant start");
  wait_sys_to_leader();
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    if (OB_FAIL(create_tenant("tt1"))) {
      TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret), K(retry_cnt));
      ob_usleep(15 * 1000 * 1000);  // 15s
    }
    retry_cnt++;
  } while (OB_FAIL(ret) && retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS,
            get_curr_simple_server().init_sql_proxy2(
              "sys", "oceanbase", false, common::sqlclient::ObMySQLConnection::OCEANBASE_MODE));
}

TEST_F(ObSharedStorageRetentionScnTest, test_adjust_retention_scn)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));

  // Get current timestamp as test baseline
  const int64_t current_time_us = ObTimeUtility::current_time();
  SCN current_snapshot;
  current_snapshot.convert_from_ts(current_time_us);

  LOG_INFO("test setup", K(current_time_us), K(current_snapshot));

  ObSSGarbageCollectorService *ss_gc_srv = MTL(ObSSGarbageCollectorService *);
  ASSERT_NE(nullptr, ss_gc_srv);

  // Get tablet_version_retention_time for all tests
  uint64_t tablet_version_retention_time = 0;
  const uint64_t tenant_id = RunCtx.tenant_id_;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    ASSERT_TRUE(tenant_config.is_valid());
    tablet_version_retention_time = tenant_config->_ss_tablet_version_retention_time;
  }

  // Store retention_scn values for later tests
  SCN task1_retention_scn;
  SCN task2_retention_scn;

  // Test 0: Verify that setting retention_scn too old (before current_gc_reserved_scn) returns OB_SNAPSHOT_DISCARDED
  {
    const int64_t task_id_0 = 1000;
    // Set retention_scn to 2 hours before current time, which should be < current_gc_reserved_scn
    // (since current_gc_reserved_scn = current_time - retention_time, typically 1 hour)
    const int64_t retention_time_us = current_time_us - 2 * 3600000000LL;  // 2 hours ago
    SCN retention_scn;
    retention_scn.convert_from_ts(retention_time_us);

    // Call adjust_tablet_version_retention_scn to set retention SCN
    ObSSGCBackupRetentionInfo retention_info_0(task_id_0, retention_scn);
    int ret = ss_gc_srv->adjust_tablet_version_retention_scn(retention_info_0);
    ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
    LOG_INFO("verify that too old retention_scn returns OB_SNAPSHOT_DISCARDED",
             K(ret),
             K(retention_scn),
             K(task_id_0),
             K(tablet_version_retention_time));
  }

  // Test 1: Set retention_scn to a time that is >= current_gc_reserved_scn (should succeed)
  {
    const int64_t task_id_1 = 1001;
    SCN actual_reserved_scn;

    // Set retention_scn to be >= current_gc_reserved_scn
    // Use current_time - retention_time/2 to ensure it's >= current_gc_reserved_scn
    // (since current_gc_reserved_scn = current_time - retention_time when no existing retention)
    const int64_t retention_time_us = current_time_us - tablet_version_retention_time / 2;
    SCN retention_scn;
    retention_scn.convert_from_ts(retention_time_us);
    task1_retention_scn = retention_scn;  // Store for later tests

    // Call adjust_tablet_version_retention_scn to set retention SCN
    ObSSGCBackupRetentionInfo retention_info_1(task_id_1, retention_scn);
    ASSERT_EQ(OB_SUCCESS, ss_gc_srv->adjust_tablet_version_retention_scn(retention_info_1));
    LOG_INFO("adjust retention scn successfully", K(retention_scn), K(task_id_1));

    sleep(1);

    // Verify: Directly read table to check if record is correctly written
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(
                *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, task_id_1, share::ObLSID(), retention_scn, true));

    // Test reserved_scn calculation
    ASSERT_EQ(
      OB_SUCCESS,
      MockSSGCForRetention::test_reserved_scn_calculation(current_snapshot, retention_scn, actual_reserved_scn));

    // Verify result: reserved_scn should be the minimum of retention_scn and the value calculated based on
    // retention_time
    SCN expected_reserved_scn;
    expected_reserved_scn.convert_from_ts(current_time_us - tablet_version_retention_time);
    SCN expected_final_reserved_scn = SCN::min(expected_reserved_scn, retention_scn);

    LOG_INFO("test1 result",
             K(actual_reserved_scn),
             K(expected_final_reserved_scn),
             K(retention_scn),
             K(expected_reserved_scn));
    ASSERT_EQ(actual_reserved_scn, expected_final_reserved_scn);
  }

  // Test 2: Adjust retention_scn to a time that is >= current_gc_reserved_scn (using new task_id)
  {
    const int64_t task_id_2 = 1002;
    SCN actual_reserved_scn;

    // Set retention_scn to be >= current_gc_reserved_scn
    // Use current_time - retention_time/3 to ensure it's >= current_gc_reserved_scn
    const int64_t retention_time_us = current_time_us - tablet_version_retention_time / 3;
    SCN retention_scn;
    retention_scn.convert_from_ts(retention_time_us);
    task2_retention_scn = retention_scn;  // Store for later tests

    // Call adjust_tablet_version_retention_scn to set retention SCN
    ObSSGCBackupRetentionInfo retention_info_2(task_id_2, retention_scn);
    ASSERT_EQ(OB_SUCCESS, ss_gc_srv->adjust_tablet_version_retention_scn(retention_info_2));
    LOG_INFO("adjust retention scn to 30 minutes ago", K(retention_scn), K(task_id_2));

    sleep(1);

    // Verify: Directly read table to check if record is correctly written
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(
                *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, task_id_2, share::ObLSID(), retention_scn, true));

    // Verify: Task 1's record still exists
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(
                *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, 1001, share::ObLSID(), task1_retention_scn, true));

    // Test reserved_scn calculation (should take the minimum of both tasks)
    ASSERT_EQ(
      OB_SUCCESS,
      MockSSGCForRetention::test_reserved_scn_calculation(current_snapshot, retention_scn, actual_reserved_scn));

    // Verify result: reserved_scn should be the minimum of both tasks' retention_scn and the value calculated based on
    // retention_time
    SCN expected_reserved_scn;
    expected_reserved_scn.convert_from_ts(current_time_us - tablet_version_retention_time);
    SCN min_retention_scn = SCN::min(retention_scn, task1_retention_scn);
    SCN expected_final_reserved_scn = SCN::min(expected_reserved_scn, min_retention_scn);

    LOG_INFO("test2 result",
             K(actual_reserved_scn),
             K(expected_final_reserved_scn),
             K(retention_scn),
             K(expected_reserved_scn),
             K(min_retention_scn));
    ASSERT_EQ(actual_reserved_scn, expected_final_reserved_scn);
  }

  // Test 3: Remove first task, verify only second task remains
  {
    const int64_t task_id_1 = 1001;
    const int64_t task_id_2 = 1002;
    const uint64_t tenant_id = RunCtx.tenant_id_;
    ASSERT_EQ(OB_SUCCESS, ss_gc_srv->remove_tablet_version_retention_scn(ObSSGCRetentionTaskType::BACKUP, task_id_1));
    LOG_INFO("removed task_id_1 retention scn", K(task_id_1));

    sleep(1);

    // Verify: Directly read table to check if record is deleted
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(
                *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, task_id_1, share::ObLSID(), SCN(), false));

    // Verify: Task 2's record still exists (use the retention_scn set in Test 2)
    ASSERT_EQ(
      OB_SUCCESS,
      verify_retention_record_in_table(
        *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, task_id_2, share::ObLSID(), task2_retention_scn, true));

    SCN actual_reserved_scn;
    SCN retention_scn = task2_retention_scn;

    // Test reserved_scn calculation (should only have second task now)
    ASSERT_EQ(
      OB_SUCCESS,
      MockSSGCForRetention::test_reserved_scn_calculation(current_snapshot, retention_scn, actual_reserved_scn));

    // Verify result: reserved_scn should be the minimum of second task's retention_scn and the value calculated based
    // on retention_time
    SCN expected_reserved_scn;
    expected_reserved_scn.convert_from_ts(current_time_us - 3600000000LL);  // 1 hour ago
    SCN expected_final_reserved_scn = SCN::min(expected_reserved_scn, retention_scn);

    LOG_INFO("test3 result",
             K(actual_reserved_scn),
             K(expected_final_reserved_scn),
             K(retention_scn),
             K(expected_reserved_scn));
    ASSERT_EQ(actual_reserved_scn, expected_final_reserved_scn);
  }

  // Test 4: Remove second task, verify no retention point exists
  {
    const int64_t task_id_2 = 1002;
    const uint64_t tenant_id = RunCtx.tenant_id_;
    ASSERT_EQ(OB_SUCCESS, ss_gc_srv->remove_tablet_version_retention_scn(ObSSGCRetentionTaskType::BACKUP, task_id_2));
    LOG_INFO("removed task_id_2 retention scn", K(task_id_2));

    sleep(1);

    // Verify: Directly read table to check if record is deleted
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(
                *this, tenant_id, ObSSGCRetentionTaskType::BACKUP, task_id_2, share::ObLSID(), SCN(), false));

    SCN actual_reserved_scn;
    SCN expected_retention_scn = SCN::min_scn();

    // Test reserved_scn calculation
    ASSERT_EQ(OB_SUCCESS,
              MockSSGCForRetention::test_reserved_scn_calculation(
                current_snapshot, expected_retention_scn, actual_reserved_scn));

    // Verify result: When no retention_scn exists, reserved_scn should be calculated based on retention_time
    SCN expected_reserved_scn;
    expected_reserved_scn.convert_from_ts(current_time_us - 3600000000LL);  // Default 1 hour
    LOG_INFO("test4 result", K(actual_reserved_scn), K(expected_reserved_scn));
    ASSERT_EQ(actual_reserved_scn, expected_reserved_scn);
  }
}

// Test both_tenants interface
TEST_F(ObSharedStorageRetentionScnTest, test_both_tenants_interface)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));

  // Get current timestamp as test baseline
  const int64_t current_time_us = ObTimeUtility::current_time();
  const uint64_t current_tenant_id = RunCtx.tenant_id_;

  // The both_tenants interface automatically identifies the current tenant type and switches to the other tenant
  // If current is meta tenant, it automatically switches to user tenant, and vice versa
  // For verification, we need to get the actual meta and user tenant IDs
  // Note: For system tenant, there may be no corresponding meta/user tenant relationship
  // Here we mainly verify if the interface call succeeds and if records are correctly written to the current tenant

  ObSSGarbageCollectorService *ss_gc_srv = MTL(ObSSGarbageCollectorService *);
  ASSERT_NE(nullptr, ss_gc_srv);

  // Test 1: Use both_tenants interface to set retention point
  {
    const int64_t backup_task_id = 2001;
    const int64_t retention_time_us = current_time_us - 1800000000LL;  // 0.5 hour ago
    SCN retention_scn;
    retention_scn.convert_from_ts(retention_time_us);

    // Call adjust_tablet_version_retention_scn_for_both_tenants
    // The interface automatically handles switching between meta and user tenants
    ObSSGCBackupRetentionInfo retention_info(backup_task_id, retention_scn);
    ASSERT_EQ(OB_SUCCESS, ss_gc_srv->adjust_tablet_version_retention_scn_for_both_tenants(retention_info));
    LOG_INFO("adjust retention scn for both tenants", K(retention_scn), K(backup_task_id), K(current_tenant_id));

    sleep(1);

    // Verify: Directly read table to check if current tenant's record is correctly written
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(*this,
                                               current_tenant_id,
                                               ObSSGCRetentionTaskType::BACKUP,
                                               backup_task_id,
                                               share::ObLSID(),
                                               retention_scn,
                                               true));

    // Verify: Directly read table to check if meta tenant's record is correctly written
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(*this,
                                               gen_meta_tenant_id(current_tenant_id),
                                               ObSSGCRetentionTaskType::BACKUP,
                                               backup_task_id,
                                               share::ObLSID(),
                                               retention_scn,
                                               true));

    LOG_INFO("both tenants retention record verified for current tenant", K(current_tenant_id), K(backup_task_id));
  }

  // Test 2: Use both_tenants interface to remove retention point
  {
    const int64_t backup_task_id = 2001;

    // Call remove_tablet_version_retention_scn_for_both_tenants
    // The interface automatically handles switching between meta and user tenants
    ASSERT_EQ(
      OB_SUCCESS,
      ss_gc_srv->remove_tablet_version_retention_scn_for_both_tenants(ObSSGCRetentionTaskType::BACKUP, backup_task_id));
    LOG_INFO("remove retention scn for both tenants", K(backup_task_id), K(current_tenant_id));

    sleep(1);

    // Verify: Directly read table to check if current tenant's record is deleted
    ASSERT_EQ(
      OB_SUCCESS,
      verify_retention_record_in_table(
        *this, current_tenant_id, ObSSGCRetentionTaskType::BACKUP, backup_task_id, share::ObLSID(), SCN(), false));

    // Verify: Directly read table to check if meta tenant's record is deleted
    ASSERT_EQ(OB_SUCCESS,
              verify_retention_record_in_table(*this,
                                               gen_meta_tenant_id(current_tenant_id),
                                               ObSSGCRetentionTaskType::BACKUP,
                                               backup_task_id,
                                               share::ObLSID(),
                                               SCN(),
                                               false));

    LOG_INFO(
      "both tenants retention record removed verified for current tenant", K(current_tenant_id), K(backup_task_id));
  }
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char *)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(
    buf,
    sizeof(buf),
    "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
    oceanbase::unittest::S3_BUCKET,
    cur_time_ns,
    oceanbase::unittest::S3_ENDPOINT,
    oceanbase::unittest::S3_AK,
    oceanbase::unittest::S3_SK,
    oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
    case 't': time_sec = atoi(optarg); break;
    case 'l':
      log_level = optarg;
      oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
      break;
    default: break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
