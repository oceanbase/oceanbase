// owner: muwei.ym
// owner group: storage_ha

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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "test_transfer_common_fun.h"
#include "env/ob_simple_server_restart_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "storage/high_availability/ob_transfer_handler.h"  //ObTransferHandler
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_service.h"

using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
using namespace share::schema;
using namespace common;
using namespace share;
using namespace transaction::tablelock;
using namespace rootserver;

static uint64_t g_tenant_id;
static uint64_t g_index = 0;
static share::SCN g_recycle_scn;
static const char *TEST_FILE_NAME = "test_reorg_info_table_mini_and_minor";

class TestMemberTable : public unittest::ObSimpleClusterTestBase
{
public:
  TestMemberTable() : unittest::ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  int check_member_table_sstable_count(
      const ObITable::TableType &type,
      const int64_t sstable_count);
  int check_member_table_recycle_scn();
  int generate_member_table_data();
  int check_member_table_data_recycle();
};

int TestMemberTable::check_member_table_sstable_count(
    const ObITable::TableType &type,
    const int64_t sstable_count)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(1);
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;
  const int64_t current_time = ObTimeUtil::current_time();
  const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
  const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else {
    int64_t index = 0;
    while (OB_SUCC(ret)) {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      ObTableStoreIterator iter;
      int64_t tmp_mini_count = 0;
      ObTabletID tablet_id(ObTabletID::LS_REORG_INFO_TABLET_ID);
      if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(ret));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
      } else if (OB_FAIL(tablet->get_all_tables(iter))) {
        LOG_WARN("failed to get all tables", K(ret), KPC(tablet));
      } else {
        while (OB_SUCC(ret)) {
          ObITable *table = nullptr;
          if (OB_FAIL(iter.get_next(table))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next table", K(ret), KPC(tablet));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (table->get_table_type() == type) {
            tmp_mini_count++;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (tmp_mini_count >= sstable_count) {
          break;
        } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait timeout", K(ret));
        } else {
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int TestMemberTable::check_member_table_recycle_scn()
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(1);
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;
  const int64_t current_time = ObTimeUtil::current_time();
  const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
  const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms
  ObTabletReorgInfoTable *reorg_info_table = nullptr;
  ObTabletReorgInfoTableService *table_service = nullptr;

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_ISNULL(reorg_info_table = ls->get_reorg_info_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet reorg info table should not be NULL", K(ret), KPC(ls));
  } else if (OB_ISNULL(table_service = MTL(ObTabletReorgInfoTableService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret));
  } else {
    table_service->wakeup();
    SCN can_recycle_scn;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(reorg_info_table->get_can_recycle_scn(can_recycle_scn))) {
        LOG_WARN("failed to get can recycle scn", K(ret));
      } else if (can_recycle_scn.is_valid_and_not_min()) {
        break;
      } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait timeout", K(ret));
      } else {
        usleep(SLEEP_INTERVAL);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(reorg_info_table->get_can_recycle_scn(g_recycle_scn))) {
        LOG_WARN("failed to get recycle scn", K(ret));
      }
    }
  }
  return ret;
}

int TestMemberTable::generate_member_table_data()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  //prepare member table data
  ObArray<ObTabletReorgInfoData> member_table_data;
  const int64_t transfer_out_transfer_seq = 0;
  const int64_t transfer_in_transfer_seq = 1;
  const share::SCN transfer_out_scn(share::SCN::base_scn());
  const share::SCN transfer_in_scn = SCN::scn_inc(transfer_out_scn);
  const int64_t base_tablet_id = 1000000;
  const ObTabletStatus transfer_out_status(ObTabletStatus::TRANSFER_OUT);
  const ObTabletStatus transfer_in_status(ObTabletStatus::TRANSFER_IN);
  const ObLSID dest_ls_id(1002);
  const ObLSID src_ls_id(1001);
  const SCN src_reorgainzation_scn(SCN::min_scn());

  //transfer out tablet
  for (int64_t i = g_index; OB_SUCC(ret) && i < g_index + 100; i++) {
    const ObTabletID tablet_id(base_tablet_id + i);
    const share::SCN reorganization_scn = transfer_out_scn;
    if (OB_FAIL(ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, transfer_out_status,
        dest_ls_id, transfer_out_transfer_seq, transfer_out_scn, src_reorgainzation_scn, member_table_data))) {
      LOG_WARN("failed to gen transfer reorg info data", K(ret), K(tablet_id));
    }
  }

  //transfer in tablet
  for (int64_t i = g_index; OB_SUCC(ret) && i < g_index + 100; i++) {
    const ObTabletID tablet_id(base_tablet_id + i);
    const share::SCN reorganization_scn = transfer_in_scn;
    if (OB_FAIL(ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, transfer_in_status,
        src_ls_id, transfer_in_transfer_seq, transfer_in_scn, src_reorgainzation_scn, member_table_data))) {
      LOG_WARN("failed to gen transfer reorg info data", K(ret), K(tablet_id));
    }
  }

  ObLSID ls_id(1);
  ObMySQLTransaction trans;
  ObTabletReorgInfoTableWriteOperator write_op;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&inner_sql_proxy, g_tenant_id))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(write_op.init(trans, g_tenant_id, ls_id))) {
    LOG_WARN("failed to init write op", K(ret));
  } else if (OB_FAIL(write_op.insert_rows(member_table_data))) {
    LOG_WARN("failed to insert rows", K(ret));
  }

  int tmp_ret = trans.end(OB_SUCC(ret));
  g_index += 100;
  return ret;
}

int TestMemberTable::check_member_table_data_recycle()
{
  int ret = OB_SUCCESS;
  ObLSID ls_id(1);
  ObTabletReorgInfoTableReadOperator read_op;
  ObTabletReorgInfoData data;
  SCN commit_scn;
  int64_t row_count = 0;

  if (OB_FAIL(read_op.init(g_tenant_id, ls_id))) {
    LOG_WARN("failed to init member table read operation", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      data.reset();
      if (OB_FAIL(read_op.get_next(data, commit_scn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next member table data", K(ret));
        }
      } else if (commit_scn <= g_recycle_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("minor do not recycle version", K(ret), K(data), K(commit_scn), K(g_recycle_scn));
      } else {
        ++row_count;
      }
    }
  }
  LOG_INFO("check member table data recycle", K(ret), K(row_count), K(g_recycle_scn));
  return ret;
}

TEST_F(TestMemberTable, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
}

TEST_F(TestMemberTable, construct_member_table_data1)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ret = generate_member_table_data();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, test_member_table_mini1)
{
  int ret = OB_SUCCESS;
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  //set transfer service wakup interval
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant = all_user"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  const ObITable::TableType type = ObITable::MINI_SSTABLE;
  const int64_t mini_count = 1;
  ret = check_member_table_sstable_count(type, mini_count);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, construct_member_table_data2)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ret = generate_member_table_data();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, test_member_table_mini2)
{
  int ret = OB_SUCCESS;
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  //set transfer service wakup interval
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant = all_user"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  const ObITable::TableType type = ObITable::MINI_SSTABLE;
  const int64_t mini_count = 2;
  ret = check_member_table_sstable_count(type, mini_count);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, construct_member_table_data3)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ret = generate_member_table_data();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, check_member_table_recycle_version)
{
  int ret = OB_SUCCESS;
  g_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ret = check_member_table_recycle_scn();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMemberTable, test_member_table_minor)
{
  int ret = OB_SUCCESS;
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  //set transfer service wakup interval
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant = all_user"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  const ObITable::TableType type = ObITable::MINOR_SSTABLE;
  const int64_t mini_count = 1;
  ret = check_member_table_sstable_count(type, mini_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = check_member_table_data_recycle();
  ASSERT_EQ(OB_SUCCESS, ret);
}


} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
