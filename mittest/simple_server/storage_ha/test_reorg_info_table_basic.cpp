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

static const char *TEST_FILE_NAME = "reorg_info_table_basic";
static ObTransferTask g_task;

class TestMemberTable : public unittest::ObSimpleClusterTestBase
{
public:
  TestMemberTable() : unittest::ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(TestMemberTable, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
}

TEST_F(TestMemberTable, insert_with_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));

  //prepare member table data
  ObArray<ObTabletReorgInfoData> reorg_info_data;
  ObTabletID tablet_id(111);
  SCN reorganization_scn(SCN::min_scn());
  SCN transfer_scn(SCN::min_scn());
  ObLSID relative_ls_id(1001);
  const ObTabletStatus tablet_status(ObTabletStatus::TRANSFER_OUT);
  int64_t transfer_seq = 0;
  const SCN src_reorgainzation_scn(SCN::min_scn());
  ret = ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, tablet_status,
      relative_ls_id, transfer_seq, transfer_scn, src_reorgainzation_scn, reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  //insert
  ObLSID ls_id(1);
  ObMySQLTransaction trans1;
  ObTabletReorgInfoTableWriteOperator write_op1;
  ASSERT_EQ(OB_SUCCESS, trans1.start(&inner_sql_proxy, g_tenant_id));
  ret = write_op1.init(trans1, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op1.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = trans1.end(OB_SUCC(ret));
  ASSERT_EQ(OB_SUCCESS, ret);

  //conflict rowkey

  ObMySQLTransaction trans2;
  ObTabletReorgInfoTableWriteOperator write_op2;
  ASSERT_EQ(OB_SUCCESS, trans2.start(&inner_sql_proxy, g_tenant_id));
  ret = write_op2.init(trans2, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op2.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);
  ret = trans2.end(OB_SUCC(ret));
  ASSERT_EQ(OB_SUCCESS, ret);

  //single get
  LOG_WARN("victor start do read");
  const ObTabletReorgInfoData &old_data = reorg_info_data.at(0);
  ObTabletReorgInfoData get_data;
  ObTabletReorgInfoTableReadOperator read_op;
  ret = read_op.init(g_tenant_id, ls_id, old_data.key_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = read_op.get_next(get_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_WARN("member table data", K(old_data), K(get_data));
  ret = read_op.get_next(get_data);
  ASSERT_EQ(OB_ITER_END, ret);
  //ASSERT_EQ(old_data, get_data);
  LOG_WARN("victor end do read");
}

TEST_F(TestMemberTable, insert_without_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));

  //prepare member table data
  ObArray<ObTabletReorgInfoData> reorg_info_data;
  ObTabletID tablet_id(112);
  SCN reorganization_scn(SCN::min_scn());
  SCN transfer_scn(SCN::min_scn());
  ObLSID relative_ls_id(1001);
  ObTabletStatus tablet_status(ObTabletStatus::TRANSFER_OUT);
  int64_t transfer_seq = 0;
  const SCN src_reorgainzation_scn(SCN::min_scn());
  ret = ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, tablet_status,
      relative_ls_id, transfer_seq, transfer_scn, src_reorgainzation_scn, reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  //insert
  ObLSID ls_id(1);
  ObSingleConnectionProxy single_connection;
  ret = single_connection.connect(g_tenant_id, 0, &inner_sql_proxy);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletReorgInfoTableWriteOperator write_op;
  ret = write_op.init(single_connection, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
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
