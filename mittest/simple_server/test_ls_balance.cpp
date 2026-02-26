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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/scn.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#define  private public
#include "rootserver/ob_tenant_balance_service.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_life_manager.h"
#include "rootserver/ob_ls_balance_helper.h"


namespace oceanbase
{
using namespace unittest;
using namespace palf;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
using namespace rootserver;
class TestLSBalance : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSBalance() : unittest::ObSimpleClusterTestBase("test_ls_balance") {}
protected:
  uint64_t tenant_id_;
};

TEST_F(TestLSBalance, Dupls)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  ObSqlString sql;
  //common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  tenant_id_ = 1002;
  ObLSLifeAgentManager ls_life(get_curr_simple_server().get_observer().get_mysql_proxy());
  ObLSStatusOperator status_operator;
  ObLSStatusInfoArray ls_array;
  //非法参数检查
  share::SCN create_scn;
  ObLSStatusInfo info;
  ObZone zone_priority("z1");
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::DUPLICATE_FLAG);
  ObUnitIDList unit_list;
  ObLSStatusInfo sys_ls;
  ret = sys_ls.init(tenant_id_, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(sys_ls));
  //创建新日志流
  ObLSID ls_id(1002);
  ret = info.init(tenant_id_, ls_id, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_list);
  ASSERT_EQ(OB_SUCCESS, ret);

  //创建新日志流
  ObLSStatusInfo new_status_info2;
  ObLSID ls_id3(1003);
  ret = new_status_info2.init(tenant_id_, ls_id3, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(new_status_info2));

  MTL_SWITCH(tenant_id_) {
    ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObLSBalanceTaskHelper ls_balance(allocator);
    ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
    ObArray<share::ObUnit> unit_array;
    ObBalanceJobDesc job_desc;
    ret = rootserver::ObTenantBalanceService::gather_tenant_balance_desc(tenant_id_, job_desc, unit_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    //case 1. init
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id_, ls_array, job_desc, unit_array, tenant_role, &get_curr_simple_server().get_observer().get_mysql_proxy()));
    LOG_INFO("testtest", "tenant_info", ls_balance.tenant_info_);
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task(false));
    LOG_INFO("testtest", "job", ls_balance.job_, "task", ls_balance.task_array_);

  }

}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
