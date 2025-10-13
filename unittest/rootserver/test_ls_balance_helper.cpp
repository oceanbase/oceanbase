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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define  private public
#include "rootserver/ob_ls_balance_helper.h"
#include "rootserver/ob_balance_ls_primary_zone.h"
#include "share/ls/ob_ls_status_operator.h"
#include "rootserver/ob_primary_ls_service.h"
#include "share/ls/ob_ls_operator.h"
#include "rootserver/balance/ob_partition_balance_helper.h"
#include "share/balance/ob_balance_job_table_operator.h"
namespace oceanbase {
using namespace common;
using namespace share;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace rootserver{
class TestLSBalanceHelper : public testing::Test
{
public:
  TestLSBalanceHelper() {}
  virtual ~TestLSBalanceHelper(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}
  int construct_unit_ls_array(const ObZoneUnitCntList &unit_list, ObArray<ObUnit> &zone_array, ObArray<ObLSStatusInfo> &ls_array);
};

int TestLSBalanceHelper::construct_unit_ls_array(const ObZoneUnitCntList &unit_list,
    ObArray<ObUnit> &unit_array,
    ObArray<ObLSStatusInfo> &ls_array)
{
  int ret = OB_SUCCESS;
  unit_array.reuse();
  ls_array.reuse();
  int64_t unit_id = 1000;
  int64_t ug_id = 2000;
  int64_t last_cnt = 0;
  uint64_t tenant_id = 1002;
  ObZone primary_zone("z1");
  ObZone primary_zone2("z2");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObLSStatusInfo sys_ls;
  ObUnitIDList unit_id_list;
  ObArray<ObUnitIDList> unit_id_list_array;
  ObAddr server1(common::ObAddr::IPV4, "127.1.1.1", 2882);
  ret = sys_ls.init(tenant_id, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  if (OB_FAIL(ls_array.push_back(sys_ls))) {
    LOG_WARN("push back", KR(ret), K(sys_ls));
  }
  ObArray<ObArray<int64_t>> unit_id_list_tmp;
  ARRAY_FOREACH(unit_list, idx) {
    const ObDisplayZoneUnitCnt &unit_cnt = unit_list.at(idx);
    if (last_cnt == 0 || last_cnt == unit_cnt.get_unit_cnt()) {
      ug_id = 2000;
      last_cnt = unit_cnt.get_unit_cnt();
    } else {
      ug_id = last_cnt + 2000;
    }
    ObArray<int64_t> unit_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_cnt.get_unit_cnt(); ++i) {
      ObUnit unit;
      unit.unit_id_ = unit_id++;
      unit.unit_group_id_ = ug_id++;
      unit.status_ = ObUnit::Status::UNIT_STATUS_ACTIVE;
      unit.zone_ = unit_cnt.get_zone();
      unit.resource_pool_id_ = 1001;
      unit.server_ = server1;
      unit_ids.push_back(unit.unit_id_);
      if (OB_FAIL(unit_array.push_back(unit))) {
        LOG_WARN("push back", KR(ret), K(unit));
      }
    }
    unit_id_list_tmp.push_back(unit_ids);
  }
  //构造unit_list的笛卡尔积
  unit_id_list_array.push_back(unit_id_list);

  FOREACH_CNT_X(unit_id_list10, unit_id_list_tmp, OB_SUCC(ret)) {
    ObArray<ObUnitIDList> new_result;
    FOREACH_X(old_array, unit_id_list_array, OB_SUCC(ret)) {
      FOREACH_X(unit_id10, *unit_id_list10, OB_SUCC(ret)) {
        ObUnitIDList new_array = *old_array;
        if (OB_FAIL(new_array.push_back(ObDisplayUnitID(*unit_id10)))) {
          LOG_WARN("push back failed", KR(ret), K(new_array), "unit_id", *unit_id10);
        } else if (OB_FAIL(new_result.push_back(new_array))) {
          LOG_WARN("push back failed", KR(ret), K(new_array));
        }
      }
    }
    if (FAILEDx(unit_id_list_array.assign(new_result))) {
      LOG_WARN("assign failed", KR(ret), K(new_result));
    }
  }
  LOG_INFO("testtest", K(unit_id_list_array));

  //构造出所有的unit_list组合，每个组合里面塞两个日志流
  ARRAY_FOREACH(unit_id_list_array, idx) {
    ObLSStatusInfo normal_ls;
    ObLSStatusInfo normal_ls1;
    ObLSID ls_id(idx + 1001);
    ObLSID ls_id2(idx + 1000 + 1001);
    normal_ls.init(tenant_id, ls_id, idx + 1001, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list_array.at(idx));
    normal_ls1.init(tenant_id, ls_id2, idx + 1001, share::OB_LS_NORMAL, 0, primary_zone2, flag, unit_id_list_array.at(idx));
    ls_array.push_back(normal_ls);
    ls_array.push_back(normal_ls1);
    if(idx > 200) {
      break;
    }
  }
  return ret;
}

TEST_F(TestLSBalanceHelper, unit_ls)
{
  int ret = OB_SUCCESS;
  //1024
  ObFixedBitSet<OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET> ls_index;
  //一个0
  ret = ls_index.add_member(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, ls_index.num_members());
  int64_t index = -1;
  ret = ls_index.find_first(index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, index);
  ret = ls_index.del_member(index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, ls_index.num_members());

  //一个1024OB_INVALID_ARGUMENT
  ret = ls_index.add_member(1024);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = ls_index.add_member(-1);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  int64_t size = sizeof(ls_index);
  LOG_INFO("test for fixedbitset", K(size));

  //一个1， 一个5，一个1023
  ret = ls_index.add_member(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, ls_index.num_members());
  ret = ls_index.find_first(index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, index);
  ret = ls_index.add_member(5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, ls_index.num_members());
  ret = ls_index.find_next(1, index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, index);
  ret = ls_index.add_member(1023);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, ls_index.num_members());
  ret = ls_index.find_next(5, index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1023, index);
  ret = ls_index.del_member(5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, ls_index.num_members());
  ret = ls_index.find_first(index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, index);
  ret = ls_index.find_next(1, index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1023, index);

}

TEST_F(TestLSBalanceHelper, ls_balance1)
{
  //同构unit 3：3：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
  job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("TESTTEST", "size", sizeof(balance_job));
  balance_job.reset();
  ret = job_desc.zone_unit_num_list_.remove(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, balance_job.zone_array_.at(0).is_in_locality_);
  ASSERT_EQ(false, balance_job.zone_array_.at(1).is_in_locality_);
  ASSERT_EQ(true, balance_job.zone_array_.at(2).is_in_locality_);

  ls_array.reset();
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObLSStatusInfo sys_ls;
  ObUnitIDList unit_id_list;
  ret = sys_ls.init(tenant_id, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  if (OB_FAIL(ls_array.push_back(sys_ls))) {
    LOG_WARN("push back", KR(ret), K(sys_ls));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job1(allocator);
  ret = balance_job1.init_tenant_ls_balance_info(tenant_id, ls_array,
       job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job1.get_valid_unit_list(unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("unit_id_list", K(unit_id_list));
}
TEST_F(TestLSBalanceHelper, ls_balance2)
{
  //异构unit 2：2：3：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 2);
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ObDisplayZoneUnitCnt z4("zone4", 3);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("TESTTEST", "size", sizeof(balance_job));
  ls_array.reset();
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObLSStatusInfo sys_ls;
  ObUnitIDList unit_id_list;
  ret = sys_ls.init(tenant_id, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  if (OB_FAIL(ls_array.push_back(sys_ls))) {
    LOG_WARN("push back", KR(ret), K(sys_ls));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job1(allocator);
  ret = balance_job1.init_tenant_ls_balance_info(tenant_id, ls_array,
       job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job1.get_valid_unit_list(unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("unit_id_list", K(unit_id_list));
  ObLSStatusInfo normal_ls;
  ObLSID ls_id(1002);
  ret = normal_ls.init(tenant_id, ls_id, 1001, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  ret = ls_array.push_back(normal_ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  balance_job1.reset();
  ret = balance_job1.init_tenant_ls_balance_info(tenant_id, ls_array,
       job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job1.get_valid_unit_list(unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("unit_id_list", K(unit_id_list));
  ObLSID ls_id1(1003);
  ret = normal_ls.init(tenant_id, ls_id, 1002, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  ret = ls_array.push_back(normal_ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  balance_job1.reset();
 ret = balance_job1.init_tenant_ls_balance_info(tenant_id, ls_array,
       job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job1.get_valid_unit_list(unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("unit_id_list", K(unit_id_list));



}
TEST_F(TestLSBalanceHelper, dup_ls)
{
  //两条dup日志流
  //异构unit 2：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 2);
  ObZoneUnitCntList zone_list;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSFlag flag(share::ObLSFlag::DUPLICATE_FLAG);
  ObLSStatusInfo dup_ls;
  ObUnitIDList unit_id_list;
  ObLSID ls_id(3004);
  ret = dup_ls.init(tenant_id, ls_id, 0, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.push_back(dup_ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSID ls_id2(3005);
  ret = dup_ls.init(tenant_id, ls_id2, 0, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.push_back(dup_ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
  job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);

  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObMySQLProxy sql_proxy;
  ObDupLSBalance dup_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = dup_balance.balance(true);
  LOG_INFO("testtest", K(job));
  job.reset();
  ret = dup_balance.balance(false);
  LOG_INFO("testtest", K(job), K(task_array));

}

TEST_F(TestLSBalanceHelper, ls_balance3)
{
  //异构unit 4:4:5
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 4);
  ObDisplayZoneUnitCnt z2("zone2", 4);
  ObDisplayZoneUnitCnt z3("zone3", 5);
  ObDisplayZoneUnitCnt z4("zone4", 5);
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  zone_list.at(2).unit_cnt_ = 4;
  unit_array.at(10).status_ = ObUnit::Status::UNIT_STATUS_DELETING;
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, balance_job.zone_array_.at(0).valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(1).valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(2).valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(2).deleting_unit_array_.count());
  ASSERT_EQ(5, balance_job.zone_array_.at(3).valid_unit_array_.count());

  LOG_INFO("TESTTEST", "size", sizeof(balance_job), K(balance_job.zone_array_), K(balance_job.ug_array_));
  balance_job.reset();
  zone_list.at(3).unit_cnt_ = 4;
  unit_array.at(15).status_ = ObUnit::Status::UNIT_STATUS_DELETING;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, balance_job.zone_array_.at(0).valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(1).valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(2).valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(2).deleting_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(3).valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(3).deleting_unit_array_.count());

  LOG_INFO("TESTTEST", "size", sizeof(balance_job), K(balance_job.zone_array_), K(balance_job.ug_array_));

}

TEST_F(TestLSBalanceHelper, unit_group_balance)
{
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObMySQLProxy sql_proxy;
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);

  // lambda for construct ls and push back to ls array
  auto construct_ls = [&ret, &ls_array, &tenant_id]
      (const uint64_t ls_id, const std::initializer_list<int64_t> unit_ids)
  {
    ObUnitIDList unit_id_list;
    for (const auto& unit_id : unit_ids) {
      ret = unit_id_list.push_back(ObDisplayUnitID(unit_id));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ObLSStatusInfo ls_status;
    ls_status.init(tenant_id, ObLSID(ls_id), ls_id, share::OB_LS_NORMAL, 0, ObZone("zone1"), share::ObLSFlag::NORMAL_FLAG, unit_id_list);
    ret = ls_array.push_back(ls_status);
    ASSERT_EQ(OB_SUCCESS, ret);
  };

  // CASE 1:
  // 异构 unit 2:2:3:3，分为两组分别重整unit group
  // 异构zone1:
  // UG2000: U0[1,2,3] U2[1,4,5]
  // UG2001: U1[4,5,6] U3[2,3,6]
  // 异构zone2:
  // UG2002: U4[1,2] U7[1,2]
  // UG2003: U5[3,4] U8[5,6]
  // UG2004: U6[5,6] U9[3,4]
  // 预期结果：
  // U2 UG2000-> UG2001
  // U3 UG2001-> UG2000
  // U8 UG2003-> UG2004
  // U9 UG2004-> UG2003
  {
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 2);
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ObDisplayZoneUnitCnt z4("zone4", 3);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ls_array.reuse();
  // sys_ls
  construct_ls(1, {});
  // normal ls
  construct_ls(1001, {1000, 1002, 1004, 1007});
  construct_ls(1002, {1000, 1003, 1004, 1007});
  construct_ls(1003, {1000, 1003, 1005, 1009});
  construct_ls(1004, {1001, 1002, 1005, 1009});
  construct_ls(1005, {1001, 1002, 1006, 1008});
  construct_ls(1006, {1001, 1003, 1006, 1008});

  ObBalanceJobDesc job_desc;
  ObTenantLSBalanceInfo balance_job(allocator);

  ret = job_desc.init_without_job(1, zone_list, 1, 1, true, true, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObUnitGroupBalance unit_group_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_group_balance.balance(true);
  lib::ob_sort(unit_op_array.begin(), unit_op_array.end(), [](ObUnitUGOp &lhs, ObUnitUGOp &rhs){ return lhs.unit_id_ < rhs.unit_id_; });
  ASSERT_EQ(4, unit_op_array.count());
  LOG_INFO("CASE 1 result", KR(ret), K(unit_op_array));
  ASSERT_EQ(ObUnitUGOp(1002, 2000, 2001), unit_op_array.at(0));
  ASSERT_EQ(ObUnitUGOp(1003, 2001, 2000), unit_op_array.at(1));
  ASSERT_EQ(ObUnitUGOp(1008, 2003, 2004), unit_op_array.at(2));
  ASSERT_EQ(ObUnitUGOp(1009, 2004, 2003), unit_op_array.at(3));
  }

  // CASE 2:
  // 4:4
  // UG2000: U0[1,3]   U4[1,3]
  // UG2001: U1[2,5,8] U5[2,4]
  // UG2002: U2[4,6,7] U6[5,7,8]
  // UG2003: U3[9]     U7[6,9]
  //预期结果：
  // U1 -> UG2002
  // U2 -> UG2001
  {
  ObDisplayZoneUnitCnt z1("zone1", 4);
  ObDisplayZoneUnitCnt z2("zone2", 4);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ls_array.reuse();
  // sys_ls
  construct_ls(1, {});
  // normal ls
  construct_ls(1001, {1000, 1004});
  construct_ls(1002, {1001, 1005});
  construct_ls(1003, {1000, 1004});
  construct_ls(1004, {1002, 1005});
  construct_ls(1005, {1001, 1006});
  construct_ls(1006, {1002, 1007});
  construct_ls(1007, {1002, 1006});
  construct_ls(1008, {1001, 1006});
  construct_ls(1009, {1003, 1007});

  ObBalanceJobDesc job_desc;
  ObTenantLSBalanceInfo balance_job(allocator);

  ret = job_desc.init_without_job(1, zone_list, 1, 1, true, true, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObUnitGroupBalance unit_group_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_group_balance.balance(true);
  lib::ob_sort(unit_op_array.begin(), unit_op_array.end(), [](ObUnitUGOp &lhs, ObUnitUGOp &rhs){ return lhs.unit_id_ < rhs.unit_id_; });
  LOG_INFO("CASE 2 result", KR(ret), K(unit_op_array));
  ASSERT_EQ(2, unit_op_array.count());
  ASSERT_EQ(ObUnitUGOp(1001, 2001, 2002), unit_op_array.at(0));
  ASSERT_EQ(ObUnitUGOp(1002, 2002, 2001), unit_op_array.at(1));
  }

  // CASE 3:
  // 3:3
  // UG2000: U0[1,2,3,4,5,6,7] U3[10]
  // UG2001: U1[8,9,10,11,12,13] U4[1,2,3,8,9]
  // UG2002: U2[14,15,16,17,18] U5[4,5,6,7,11,12,13,14,15,16,17,18]
  //预期结果：
  // U3 -> UG2001
  // U4 -> UG2000
  {
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ls_array.reuse();
  // sys_ls
  construct_ls(1, {});
  // normal ls
  construct_ls(1001, {1000, 1004});
  construct_ls(1002, {1000, 1004});
  construct_ls(1003, {1000, 1004});
  construct_ls(1004, {1000, 1005});
  construct_ls(1005, {1000, 1005});
  construct_ls(1006, {1000, 1005});
  construct_ls(1007, {1000, 1005});
  construct_ls(1008, {1001, 1004});
  construct_ls(1009, {1001, 1004});
  construct_ls(1010, {1001, 1003});
  construct_ls(1011, {1001, 1005});
  construct_ls(1012, {1001, 1005});
  construct_ls(1013, {1001, 1005});
  construct_ls(1014, {1002, 1005});
  construct_ls(1015, {1002, 1005});
  construct_ls(1016, {1002, 1005});
  construct_ls(1017, {1002, 1005});
  construct_ls(1018, {1002, 1005});

  ObBalanceJobDesc job_desc;
  ObTenantLSBalanceInfo balance_job(allocator);

  ret = job_desc.init_without_job(1, zone_list, 1, 1, true, true, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObUnitGroupBalance unit_group_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_group_balance.balance(true);
  lib::ob_sort(unit_op_array.begin(), unit_op_array.end(), [](ObUnitUGOp &lhs, ObUnitUGOp &rhs){ return lhs.unit_id_ < rhs.unit_id_; });
  LOG_INFO("CASE 3 result", KR(ret), K(unit_op_array));
  ASSERT_EQ(2, unit_op_array.count());
  ASSERT_EQ(ObUnitUGOp(1003, 2000, 2001), unit_op_array.at(0));
  ASSERT_EQ(ObUnitUGOp(1004, 2001, 2000), unit_op_array.at(1));
  }
}

TEST_F(TestLSBalanceHelper, unit_list_balance)
{
  //异构unit 2：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = job_desc.zone_unit_num_list_.remove(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx).current_unit_list_.count());
  }
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObMySQLProxy sql_proxy;
  ObUnitListBalance unit_list_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(1, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
  }
  LOG_INFO("testtest", K(lsg_op_array));
  //验证加一个zone
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ret = zone_list.push_back(z3);
  job_desc.reset();
  ret = job_desc.init_without_job(2, zone_list, 1, 1, false, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObLSStatusInfo> ls_array1;
  unit_array.reset();
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array1);
  ASSERT_EQ(OB_SUCCESS, ret);
  balance_job.reset();
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx).current_unit_list_.count());
  }
  lsg_op_array.reset();
  ObUnitListBalance unit_list_balance2(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance2.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
  }
  LOG_INFO("testtest", K(lsg_op_array));

  //验证enable_rebalance等于true的时候，多个ls_group在unit_group之间均衡
  job_desc.reset();
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObLSStatusInfo ls_info1;
  ObUnitIDList unit_id_list;
  ret = unit_id_list.push_back(ObDisplayUnitID(1000));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = unit_id_list.push_back(ObDisplayUnitID(1002));
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSID ls_id(3004);
  ret = ls_info1.init(tenant_id, ls_id, 3004, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.push_back(ls_info1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSID ls_id1(3005);
  ls_info1.reset();
  ret = ls_info1.init(tenant_id, ls_id1, 3005, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list);
  ret = ls_array.push_back(ls_info1);
  ASSERT_EQ(OB_SUCCESS, ret);
  balance_job.reset();
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx).current_unit_list_.count());
  }
  lsg_op_array.reset();
  ObUnitListBalance unit_list_balance3(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance3.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
  }
  LOG_INFO("testtest", K(lsg_op_array));


}

TEST_F(TestLSBalanceHelper, unit_list_balance2)
{
  //异构unit 2：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ObDisplayZoneUnitCnt z4("zone4", 3);
  ObZoneUnitCntList zone_list;

  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  //1001, 1002日志流的z4可以直接继承，由于z2和z3是同一个unit_group_id
  ret = ls_array.at(1).unit_id_list_.push_back(ObDisplayUnitID(1005));
  ret = ls_array.at(2).unit_id_list_.push_back(ObDisplayUnitID(1005));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.at(3).unit_id_list_.push_back(ObDisplayUnitID(1009));
  ret = ls_array.at(4).unit_id_list_.push_back(ObDisplayUnitID(1009));
  ASSERT_EQ(OB_SUCCESS, ret);

  //1003，1004需要挑选最小的
  ret = ls_array.at(5).unit_id_list_.push_back(ObDisplayUnitID(1005));
  ret = ls_array.at(6).unit_id_list_.push_back(ObDisplayUnitID(1005));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.at(7).unit_id_list_.push_back(ObDisplayUnitID(1009));
  ret = ls_array.at(8).unit_id_list_.push_back(ObDisplayUnitID(1009));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.at(9).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ret = ls_array.at(10).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.at(11).unit_id_list_.push_back(ObDisplayUnitID(1008));
  ret = ls_array.at(12).unit_id_list_.push_back(ObDisplayUnitID(1008));
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z4);
  ASSERT_EQ(OB_SUCCESS, ret);
  //重新搞一个unit_array，但是不要ls_array
  unit_array.reset();
  ObArray<ObLSStatusInfo> ls_array1;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array1);
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, false, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx).current_unit_list_.count());
  }
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObMySQLProxy sql_proxy;
  ObUnitListBalance unit_list_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance.balance(true);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(4, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
  }
  LOG_INFO("testtest", K(lsg_op_array));

}

TEST_F(TestLSBalanceHelper, ls_group_location_balance)
{
  //异构unit 2：3
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 2);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObDisplayZoneUnitCnt z3("zone3", 3);
  ObDisplayZoneUnitCnt z4("zone4", 3);
  ObZoneUnitCntList zone_list;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  unit_array.reset();
  ret = zone_list.push_back(z3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObLSStatusInfo> ls_array1;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array1);
  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  //z2,z3,z4不一样，但是由于z2是均衡的，所以z3和z4和z2对齐。
  //剩下的日志流组只在z2上有记录，也不会搞出多余的unit_id_list
  ret = ls_array.at(1).unit_id_list_.push_back(ObDisplayUnitID(1006));
  ret = ls_array.at(2).unit_id_list_.push_back(ObDisplayUnitID(1006));
  ret = ls_array.at(1).unit_id_list_.push_back(ObDisplayUnitID(1010));
  ret = ls_array.at(2).unit_id_list_.push_back(ObDisplayUnitID(1010));

  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
  job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    if (0 != idx) {
      ASSERT_EQ(2, balance_job.ls_group_array_.at(idx).current_unit_list_.count());
    }
  }
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObMySQLProxy sql_proxy;
  ObLSGroupLocationBalance ls_group_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = ls_group_balance.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    if (idx == 0) {
      ASSERT_EQ(4, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
    } else {
      ASSERT_EQ(2, balance_job.ls_group_array_.at(idx).target_unit_list_.count());
    }
  }
  ASSERT_EQ(1002, balance_job.ls_group_array_.at(0).target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(0).target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(0).target_unit_list_.at(3).id());
  LOG_INFO("testtest", K(lsg_op_array));

  //设置z2不均衡
  ls_array.at(1).unit_id_list_.remove(1);
  ls_array.at(2).unit_id_list_.remove(1);

  //z2,z3,z4不相等,还是会选择z2，但是是由于他是第一个
  ret = ls_array.at(3).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ret = ls_array.at(4).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ret = ls_array.at(3).unit_id_list_.push_back(ObDisplayUnitID(1008));
  ret = ls_array.at(4).unit_id_list_.push_back(ObDisplayUnitID(1008));

  ASSERT_EQ(OB_SUCCESS, ret);

  //z3,z4相等
  //1003，1004需要挑选最小的
  ret = ls_array.at(5).unit_id_list_.push_back(ObDisplayUnitID(1006));
  ret = ls_array.at(6).unit_id_list_.push_back(ObDisplayUnitID(1006));
  ret = ls_array.at(5).unit_id_list_.push_back(ObDisplayUnitID(1009));
  ret = ls_array.at(6).unit_id_list_.push_back(ObDisplayUnitID(1009));

  //z2,z4相等
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_array.at(7).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ret = ls_array.at(8).unit_id_list_.push_back(ObDisplayUnitID(1007));
  ret = ls_array.at(7).unit_id_list_.push_back(ObDisplayUnitID(1008));
  ret = ls_array.at(8).unit_id_list_.push_back(ObDisplayUnitID(1008));
  balance_job.reset();

  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  lsg_op_array.reset();
  LOG_INFO("testtest", K(balance_job.ls_group_array_));
  ObLSGroupLocationBalance ls_group_balance1(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = ls_group_balance1.balance(true);
  ASSERT_EQ(1007, balance_job.ls_group_array_.at(0).target_unit_list_.at(1).id());
  ASSERT_EQ(1010, balance_job.ls_group_array_.at(0).target_unit_list_.at(2).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(1).target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(1).target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(1).target_unit_list_.at(3).id());

  ASSERT_EQ(1003, balance_job.ls_group_array_.at(2).target_unit_list_.at(1).id());
  ASSERT_EQ(1006, balance_job.ls_group_array_.at(2).target_unit_list_.at(2).id());
  ASSERT_EQ(1009, balance_job.ls_group_array_.at(2).target_unit_list_.at(3).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(3).target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(3).target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(3).target_unit_list_.at(3).id());

  LOG_INFO("testtest", K(lsg_op_array));


  balance_job.reset();
  //设置z2的某一个unit为deleting
  unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
  unit_array.at(2).unit_group_id_ = 2001;
  unit_array.at(4).unit_group_id_ = 2000;
  //只有一个在deleting状态的unit上面
  ret = ls_array.at(9).unit_id_list_.remove(0);
  ret = ls_array.at(10).unit_id_list_.remove(0);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  lsg_op_array.reset();
  LOG_INFO("testtest", K(balance_job.ls_group_array_));
  ObLSGroupLocationBalance ls_group_balance2(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = ls_group_balance2.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("testtest", K(lsg_op_array));
  ASSERT_EQ(1007, balance_job.ls_group_array_.at(0).target_unit_list_.at(1).id());
  ASSERT_EQ(1010, balance_job.ls_group_array_.at(0).target_unit_list_.at(2).id());

  ASSERT_EQ(1004, balance_job.ls_group_array_.at(1).target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(1).target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(1).target_unit_list_.at(3).id());

  ASSERT_EQ(1004, balance_job.ls_group_array_.at(2).target_unit_list_.at(1).id());
  ASSERT_EQ(1006, balance_job.ls_group_array_.at(2).target_unit_list_.at(2).id());
  ASSERT_EQ(1009, balance_job.ls_group_array_.at(2).target_unit_list_.at(3).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(3).target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(3).target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(3).target_unit_list_.at(3).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(4).target_unit_list_.at(0).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(5).target_unit_list_.at(1).id());


}
}
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#undef private
