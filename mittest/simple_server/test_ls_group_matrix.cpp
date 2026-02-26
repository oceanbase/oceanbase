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

#define private public
#define protected public
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/balance/ob_balance_job_table_operator.h"
#include "share/balance/ob_balance_task_table_operator.h"
#include "share/balance/ob_balance_task_helper_operator.h"
#include "rootserver/ob_ls_balance_helper.h"//ObLSBalanceTaskHelper
#include "share/transfer/ob_transfer_info.h"
#include "share/ob_ls_id.h"
#include "share/unit/ob_unit_info.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_i_life_manager.h"
#include "rootserver/ob_tenant_balance_service.h" // ObTenantBalanceService


namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace rootserver;
using namespace common;
class TestLSGroupMatrix : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSGroupMatrix() : unittest::ObSimpleClusterTestBase("test_ls_group_matrix") {}
protected:
  uint64_t tenant_id_;
  int construct_unit_array(
      const ObZoneUnitCntList &unit_list,
      ObArray<ObUnit> &zone_array);
};

TEST_F(TestLSGroupMatrix, ChooseZoneCmp)
{
  int ret = OB_SUCCESS;

  ObZone zone1("zone1");
  ObAddr server1(common::ObAddr::IPV4, "127.1.1.1", 2882);

  bool is_balance_true = true;
  bool is_balance_false = false;

  bool in_locality_true = true;
  bool in_locality_false = false;

  ObUnitIDList unit_id_list1;
  ASSERT_EQ(OB_SUCCESS, unit_id_list1.push_back(ObDisplayUnitID(1001)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list1.push_back(ObDisplayUnitID(1002)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list1.push_back(ObDisplayUnitID(1003)));

  ObLSGroupStat ls_group_stat1;
  ls_group_stat1.lg_id_ = 1001;
  ASSERT_EQ(OB_SUCCESS, ls_group_stat1.current_unit_list_.assign(unit_id_list1));

  ObLSGroupStat ls_group_stat2;
  ls_group_stat2.lg_id_ = 1002;
  ASSERT_EQ(OB_SUCCESS, ls_group_stat2.current_unit_list_.assign(unit_id_list1));

  ObLSGroupStat ls_group_stat3;
  ls_group_stat3.lg_id_ = 1003;
  ASSERT_EQ(OB_SUCCESS, ls_group_stat3.current_unit_list_.assign(unit_id_list1));

  ObUnit unit1;
  unit1.unit_id_ = 1000;
  unit1.unit_group_id_ = 1000;
  unit1.status_ = ObUnit::Status::UNIT_STATUS_ACTIVE;
  unit1.zone_ = zone1;
  unit1.resource_pool_id_ = 1000;
  unit1.server_ = server1;

  ObUnitLSStat unit_ls_stat1; // has 1 ls group
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat1.init(unit1));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat1.add_ls_group(&ls_group_stat1));

  ObUnitLSStat unit_ls_stat2; // has 2 ls groups
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat2.init(unit1));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat2.add_ls_group(&ls_group_stat1));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat2.add_ls_group(&ls_group_stat2));

  ObUnitLSStat unit_ls_stat3; // has 3 ls groups
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat3.init(unit1));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat3.add_ls_group(&ls_group_stat1));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat3.add_ls_group(&ls_group_stat2));
  ASSERT_EQ(OB_SUCCESS, unit_ls_stat3.add_ls_group(&ls_group_stat3));

  // zone_stat1:
  //   is_balance    : true
  //   in_locality   : true
  //   ls_group_count: 1
  ObZoneLSStat zone_stat1;
  zone_stat1.zone_ = zone1;
  zone_stat1.is_balance_ = is_balance_true;
  zone_stat1.is_in_locality_ = in_locality_true;
  ASSERT_EQ(OB_SUCCESS, zone_stat1.add_unit_ls_info(&unit_ls_stat1));

  // zone_stat2:
  //   is_balance    : false
  //   in_locality   : true
  //   ls_group_count: 1
  ObZoneLSStat zone_stat2;
  zone_stat2.zone_ = zone1;
  zone_stat2.is_balance_ = is_balance_false;
  zone_stat2.is_in_locality_ = in_locality_true;
  ASSERT_EQ(OB_SUCCESS, zone_stat2.add_unit_ls_info(&unit_ls_stat1));

  // zone_stat3:
  //   is_balance    : true
  //   in_locality   : false
  //   ls_group_count: 1
  ObZoneLSStat zone_stat3;
  zone_stat3.zone_ = zone1;
  zone_stat3.is_balance_ = is_balance_true;
  zone_stat3.is_in_locality_ = in_locality_false;
  ASSERT_EQ(OB_SUCCESS, zone_stat3.add_unit_ls_info(&unit_ls_stat1));

  // zone_stat4:
  //   is_balance    : true
  //   in_locality   : true
  //   ls_group_count: 1+1
  ObZoneLSStat zone_stat4;
  zone_stat4.zone_ = zone1;
  zone_stat4.is_balance_ = is_balance_true;
  zone_stat4.is_in_locality_ = in_locality_true;
  ASSERT_EQ(OB_SUCCESS, zone_stat4.add_unit_ls_info(&unit_ls_stat1));
  ASSERT_EQ(OB_SUCCESS, zone_stat4.add_unit_ls_info(&unit_ls_stat1));

  // zone_stat5:
  //   is_balance    : true
  //   in_locality   : true
  //   ls_group_count: 1+3
  ObZoneLSStat zone_stat5;
  zone_stat5.zone_ = zone1;
  zone_stat5.is_balance_ = is_balance_true;
  zone_stat5.is_in_locality_ = in_locality_true;
  ASSERT_EQ(OB_SUCCESS, zone_stat5.add_unit_ls_info(&unit_ls_stat1));
  ASSERT_EQ(OB_SUCCESS, zone_stat5.add_unit_ls_info(&unit_ls_stat3));

  ChooseZoneCmp zone_cmp;
  // test priority: is_balance true > false
  ASSERT_TRUE(zone_cmp(&zone_stat1, &zone_stat2));
  LOG_INFO("zone_stat1 has higher priority because is_balance = true", K(zone_stat1), K(zone_stat2));

  // test priority: in_locality true > false
  ASSERT_TRUE(zone_cmp(&zone_stat1, &zone_stat3));
  LOG_INFO("zone_stat1 has higher priority because in_locality = true", K(zone_stat1), K(zone_stat3));

  // test priority: ls_group_cnt should more balanced
  ASSERT_TRUE(zone_cmp(&zone_stat4, &zone_stat5));
  LOG_INFO("zone_stat4 has higher priority because ls_group count more balanced", K(zone_stat4), K(zone_stat5));

  // test priority: in_locality > is_balance
  ASSERT_TRUE(zone_cmp(&zone_stat2, &zone_stat3));
  LOG_INFO("zone_stat2 has higher priority because in_locality > is_balance", K(zone_stat2), K(zone_stat3));

  // test priority: is_balance > variance score of ls_group_count
  ASSERT_TRUE(zone_cmp(&zone_stat5, &zone_stat2));
  LOG_INFO("zone_stat5 has higher priority because is_balance > variance score of ls_group_count", K(zone_stat5), K(zone_stat2));

  // test priority: in_locality > variance score of ls_group_count
  ASSERT_TRUE(zone_cmp(&zone_stat5, &zone_stat3));
  LOG_INFO("zone_stat5 has higher priority because in_locality > variance score of ls_group_count", K(zone_stat5), K(zone_stat3));

  ObArray<const ObZoneLSStat*> zone_stat_array;
  ASSERT_EQ(OB_SUCCESS, zone_stat_array.push_back(&zone_stat1));
  ASSERT_EQ(OB_SUCCESS, zone_stat_array.push_back(&zone_stat2));
  ASSERT_EQ(OB_SUCCESS, zone_stat_array.push_back(&zone_stat3));
  ASSERT_EQ(OB_SUCCESS, zone_stat_array.push_back(&zone_stat5));

  lib::ob_sort(zone_stat_array.begin(), zone_stat_array.end(), zone_cmp);
  ASSERT_EQ(OB_SUCCESS, zone_cmp.get_ret());
  // zone_array should in order: zone_stat1, zone_stat5, zone_stat2, zone_stat3
  ASSERT_TRUE(&zone_stat1 == zone_stat_array.at(0));
  ASSERT_TRUE(&zone_stat5 == zone_stat_array.at(1));
  ASSERT_TRUE(&zone_stat2 == zone_stat_array.at(2));
  ASSERT_TRUE(&zone_stat3 == zone_stat_array.at(3));

  // check variance score
  double variance_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, zone_stat1.calculate_variance_score(variance_score));
  LOG_INFO("variance score should be 0", K(variance_score), K(zone_stat1));
  ASSERT_TRUE(fabs(variance_score - 0.0) < OB_DOUBLE_EPSINON);

  ASSERT_EQ(OB_SUCCESS, zone_stat2.calculate_variance_score(variance_score));
  LOG_INFO("variance score should be 0", K(variance_score), K(zone_stat2));
  ASSERT_TRUE(fabs(variance_score - 0.0) < OB_DOUBLE_EPSINON);

  ASSERT_EQ(OB_SUCCESS, zone_stat3.calculate_variance_score(variance_score));
  LOG_INFO("variance score should be 0", K(variance_score), K(zone_stat3));
  ASSERT_TRUE(fabs(variance_score - 0.0) < OB_DOUBLE_EPSINON);

  ASSERT_EQ(OB_SUCCESS, zone_stat4.calculate_variance_score(variance_score));
  LOG_INFO("variance score should be 0", K(variance_score), K(zone_stat4));
  ASSERT_TRUE(fabs(variance_score - 0.0) < OB_DOUBLE_EPSINON);

  ASSERT_EQ(OB_SUCCESS, zone_stat5.calculate_variance_score(variance_score));
  LOG_INFO("variance score should not be 0", K(variance_score), K(zone_stat5));
  ASSERT_TRUE(fabs(variance_score - 1.0) < OB_DOUBLE_EPSINON);
}

int TestLSGroupMatrix::construct_unit_array(
    const ObZoneUnitCntList &unit_list,
    ObArray<ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  int64_t unit_id = 1000;
  int64_t ug_id = 2000;
  int64_t last_cnt = 0;
  uint64_t tenant_id = 1002;
  ObZone primary_zone("z1");
  ObZone primary_zone2("z2");
  ObAddr server1(common::ObAddr::IPV4, "127.1.1.1", 2882);
  ARRAY_FOREACH(unit_list, idx) {
    const ObDisplayZoneUnitCnt &unit_cnt = unit_list.at(idx);
    if (last_cnt == 0 || last_cnt == unit_cnt.get_unit_cnt()) {
      ug_id = 2000;
      last_cnt = unit_cnt.get_unit_cnt();
    } else {
      ug_id = last_cnt + 2000;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_cnt.get_unit_cnt(); ++i) {
      ObUnit unit;
      unit.unit_id_ = unit_id++;
      unit.unit_group_id_ = ug_id++;
      unit.status_ = ObUnit::Status::UNIT_STATUS_ACTIVE;
      unit.zone_ = unit_cnt.get_zone();
      unit.resource_pool_id_ = 1001;
      unit.server_ = server1;
      if (OB_FAIL(unit_array.push_back(unit))) {
        LOG_WARN("push back", KR(ret), K(unit));
      }
    }
  }
  return ret;
}

TEST_F(TestLSGroupMatrix, ObLSGroupMatrix)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  common::ObArray<share::ObLSGroupUnitListOp> ls_op_array;
  common::ObArray<share::ObUnitUGOp> unit_ug_op_array;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  common::ObMySQLProxy *sql_proxy = get_curr_observer().get_gctx().sql_proxy_;
  int64_t primary_zone_num = 1;
  uint64_t tenant_id = 1002;
  ObArenaAllocator allocator("LSGroupMatrix" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObTenantLSBalanceInfo balance_job(allocator);
  // construct unit_id from 1000 to 1005
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObDisplayZoneUnitCnt z3("zone2", 5);

  // construct homo deployed zone unit_array
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ret = construct_unit_array(zone_list, unit_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  // construct hetero deployed zone unit_array
  ObZoneUnitCntList hetero_zone_list;
  ret = hetero_zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = hetero_zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> hetero_unit_array;
  ret = construct_unit_array(hetero_zone_list, hetero_unit_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  // initial parameters
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObArray<ObLSStatusInfo> ls_array;
  ObBalanceJobDesc job_desc;

  // construct 5 unit location for 5 log streams
  ObUnitIDList sys_unit_id_list;
  ObUnitIDList unit_id_list_1000_1003;
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1003.push_back(ObDisplayUnitID(1000)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1003.push_back(ObDisplayUnitID(1003)));

  ObUnitIDList unit_id_list_1001_1004;
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1001_1004.push_back(ObDisplayUnitID(1001)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1001_1004.push_back(ObDisplayUnitID(1004)));

  ObUnitIDList unit_id_list_1002_1005;
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1002_1005.push_back(ObDisplayUnitID(1002)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1002_1005.push_back(ObDisplayUnitID(1005)));

  ObUnitIDList unit_id_list_1000_1006;
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1006.push_back(ObDisplayUnitID(1000)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1006.push_back(ObDisplayUnitID(1006)));

  ObUnitIDList unit_id_list_1000_1007;
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1007.push_back(ObDisplayUnitID(1000)));
  ASSERT_EQ(OB_SUCCESS, unit_id_list_1000_1007.push_back(ObDisplayUnitID(1007)));

  ObLSStatusInfo sys_ls;
  ASSERT_EQ(OB_SUCCESS, sys_ls.init(tenant_id, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, sys_unit_id_list));

  // construct 5 log streams
  ObLSID ls_id_1001(1001);
  ObLSStatusInfo ls_info_1001;
  ASSERT_EQ(OB_SUCCESS, ls_info_1001.init(tenant_id, ls_id_1001, 1001, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list_1000_1003));

  ObLSID ls_id_1002(1002);
  ObLSStatusInfo ls_info_1002;
  ASSERT_EQ(OB_SUCCESS, ls_info_1002.init(tenant_id, ls_id_1002, 1002, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list_1001_1004));

  ObLSID ls_id_1003(1003);
  ObLSStatusInfo ls_info_1003;
  ASSERT_EQ(OB_SUCCESS, ls_info_1003.init(tenant_id, ls_id_1003, 1003, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list_1002_1005));

  ObLSID ls_id_1004(1004);
  ObLSStatusInfo ls_info_1004;
  ASSERT_EQ(OB_SUCCESS, ls_info_1004.init(tenant_id, ls_id_1004, 1004, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list_1000_1006));

  ObLSID ls_id_1005(1005);
  ObLSStatusInfo ls_info_1005;
  ASSERT_EQ(OB_SUCCESS, ls_info_1005.init(tenant_id, ls_id_1005, 1005, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list_1000_1007));

  // construct ls_1 and ls_1001(on unit 1000, 1003)
  ls_array.reset();
  job_desc.reset();
  job.reset();
  task_array.reset();
  balance_job.reset();
  ASSERT_EQ(OB_SUCCESS, job_desc.init_without_job(tenant_id, zone_list, primary_zone_num, 1, true, true, false));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(sys_ls));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1001));

  // test job with homo-deployed
  MTL_SWITCH(tenant_id) {
    ASSERT_EQ(OB_SUCCESS, balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role));
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, ObBalanceJobID(), &job, &task_array, &ls_op_array, &unit_ug_op_array);
    ASSERT_EQ(OB_SUCCESS, lg_cnt.balance(false));
    //LOG_INFO("matrix info should like this : ls_group_matrix={is_inited_:true, row_count_:1, column_count_:3}");
  }

  // test job with hetero_deployed
  // construct ls_1
  // and ls_1001(on unit 1000, 1003)
  // and ls_1002(on unit 1001, 1004)
  // and ls_1003(on unit 1002, 1005)
  ls_array.reset();
  job_desc.reset();
  job.reset();
  task_array.reset();
  balance_job.reset();

  ASSERT_EQ(OB_SUCCESS, job_desc.init_without_job(tenant_id, hetero_zone_list, primary_zone_num, 1, true, true, false));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(sys_ls));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1001));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1002));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1003));

  MTL_SWITCH(tenant_id) {
    ASSERT_EQ(OB_SUCCESS, balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, hetero_unit_array, tenant_role));
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, ObBalanceJobID(), &job, &task_array, &ls_op_array, &unit_ug_op_array);
    ASSERT_EQ(OB_SUCCESS, lg_cnt.balance(false));
    //LOG_INFO("matrix info should like this : ls_group_matrix={is_inited_:true, row_count_:3, column_count_:5}");
  }


  // construct ls_1
  // and ls_1001(on unit 1000, 1003)
  // and ls_1002(on unit 1001, 1003)
  // and ls_1003(on unit 1002, 1004)
  // and ls_1004(on unit 1000, 1005)
  // and ls_1005(on unit 1000, 1006)
  ls_array.reset();
  job_desc.reset();
  job.reset();
  task_array.reset();
  balance_job.reset();

  ASSERT_EQ(OB_SUCCESS, job_desc.init_without_job(tenant_id, hetero_zone_list, primary_zone_num, 1, true, true, false));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(sys_ls));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1001));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1002));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1003));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1004));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(ls_info_1005));

  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    ASSERT_EQ(OB_SUCCESS, balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, hetero_unit_array, tenant_role));
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, ObBalanceJobID(), &job, &task_array, &ls_op_array, &unit_ug_op_array);
    ASSERT_EQ(OB_SUCCESS, lg_cnt.balance(false));
    //LOG_INFO("matrix info should like this : ls_group_matrix={is_inited_:true, row_count_:5, column_count_:3}");
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
