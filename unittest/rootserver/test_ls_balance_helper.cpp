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

// 复用校验宏：检查 ObTenantLSBalanceInfo 的关键结构及嵌套元素均有效
#define ASSERT_BALANCE_JOB_VALID(bj)                                                       \
  do {                                                                                     \
    LOG_INFO("testtest", "unit_info_count", (bj).unit_info_.count(), K((bj).unit_info_));  \
    LOG_INFO("testtest", "normal_ls_info_count", (bj).normal_ls_info_.count(), K((bj).normal_ls_info_)); \
    LOG_INFO("testtest", "ls_group_array_count", (bj).ls_group_array_.count(), K((bj).ls_group_array_)); \
    LOG_INFO("testtest", "unit_array_count", (bj).unit_array_.count(), K((bj).unit_array_)); \
    LOG_INFO("testtest", "ug_array_count", (bj).ug_array_.count(), K((bj).ug_array_));     \
    LOG_INFO("testtest", "zone_array_count", (bj).zone_array_.count(), K((bj).zone_array_)); \
    ARRAY_FOREACH((bj).unit_array_, idx) {                                                 \
      LOG_INFO("testtest unit_array", K(idx), KPC((bj).unit_array_.at(idx)));              \
      ASSERT_TRUE((bj).unit_array_.at(idx)->is_valid());                                   \
      ASSERT_TRUE((bj).unit_array_.at(idx)->unit_->is_valid());                            \
      ASSERT_TRUE((bj).unit_array_.at(idx)->zone_stat_->is_valid());                       \
      ASSERT_TRUE((bj).unit_array_.at(idx)->unit_group_stat_->is_valid());                 \
      ARRAY_FOREACH((bj).unit_array_.at(idx)->ls_group_info_, ls_idx) {                    \
        ASSERT_TRUE((bj).unit_array_.at(idx)->ls_group_info_.at(ls_idx)->is_valid());      \
      }                                                                                    \
    }                                                                                      \
    ARRAY_FOREACH((bj).ls_group_array_, idx) {                                             \
      LOG_INFO("testtest ls_group_array", K(idx), KPC((bj).ls_group_array_.at(idx)));      \
      ASSERT_TRUE((bj).ls_group_array_.at(idx)->is_valid());                               \
    }                                                                                      \
    ARRAY_FOREACH((bj).ug_array_, idx) {                                                   \
      LOG_INFO("testtest ug_array", K(idx), KPC((bj).ug_array_.at(idx)));                  \
      ASSERT_TRUE((bj).ug_array_.at(idx)->is_valid());                                     \
      ARRAY_FOREACH((bj).ug_array_.at(idx)->unit_info_, unit_idx) {                        \
        ASSERT_TRUE((bj).ug_array_.at(idx)->unit_info_.at(unit_idx)->is_valid());          \
      }                                                                                    \
      ARRAY_FOREACH((bj).ug_array_.at(idx)->ls_group_info_, ls_idx) {                      \
        ASSERT_TRUE((bj).ug_array_.at(idx)->ls_group_info_.at(ls_idx)->is_valid());        \
      }                                                                                    \
    }                                                                                      \
    ARRAY_FOREACH((bj).zone_array_, idx) {                                                 \
      LOG_INFO("testtest zone_array", K(idx), KPC((bj).zone_array_.at(idx)));              \
      ASSERT_TRUE((bj).zone_array_.at(idx)->is_valid());                                   \
      ARRAY_FOREACH((bj).zone_array_.at(idx)->valid_unit_array_, unit_idx) {               \
        ASSERT_TRUE((bj).zone_array_.at(idx)->valid_unit_array_.at(unit_idx)->is_valid()); \
      }                                                                                    \
      ARRAY_FOREACH((bj).zone_array_.at(idx)->deleting_unit_array_, unit_idx) {            \
        ASSERT_TRUE((bj).zone_array_.at(idx)->deleting_unit_array_.at(unit_idx)->is_valid()); \
      }                                                                                    \
    }                                                                                      \
  } while(0)

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
    if(idx >= 999) {
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

TEST_F(TestLSBalanceHelper, sort_ls_by_primary_zone_round_robin)
{
  using namespace share;
  int ret = OB_SUCCESS;

  // build helper instance (pointers can be null since function doesn't use members)
  ObTenantLSBalanceInfo *tenant_info = nullptr;
  ObMySQLProxy *sql_proxy = nullptr;
  ObBalanceJobID job_id;
  ObBalanceJob *job = nullptr;
  ObArray<ObBalanceTask> *task_array = nullptr;
  ObArray<ObLSGroupUnitListOp> *lsg_op_array = nullptr;
  ObArray<ObUnitUGOp> *unit_ug_op_array = nullptr;
  ObLSCountBalance sorter(tenant_info, sql_proxy, job_id, job, task_array, lsg_op_array, unit_ug_op_array);

  // common params
  const uint64_t tenant_id = 1234;
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObUnitIDList unit_id_list;

  // equal counts: z1:2, z2:2, z3:2
  ObLSStatusInfo a1; ASSERT_EQ(OB_SUCCESS, a1.init(tenant_id, ObLSID(1), 1, share::OB_LS_NORMAL, 0, ObZone("z1"), flag, unit_id_list));
  ObLSStatusInfo a2; ASSERT_EQ(OB_SUCCESS, a2.init(tenant_id, ObLSID(2), 2, share::OB_LS_NORMAL, 0, ObZone("z1"), flag, unit_id_list));
  ObLSStatusInfo b1; ASSERT_EQ(OB_SUCCESS, b1.init(tenant_id, ObLSID(3), 3, share::OB_LS_NORMAL, 0, ObZone("z2"), flag, unit_id_list));
  ObLSStatusInfo b2; ASSERT_EQ(OB_SUCCESS, b2.init(tenant_id, ObLSID(4), 4, share::OB_LS_NORMAL, 0, ObZone("z2"), flag, unit_id_list));
  ObLSStatusInfo c1; ASSERT_EQ(OB_SUCCESS, c1.init(tenant_id, ObLSID(5), 5, share::OB_LS_NORMAL, 0, ObZone("z3"), flag, unit_id_list));
  ObLSStatusInfo c2; ASSERT_EQ(OB_SUCCESS, c2.init(tenant_id, ObLSID(6), 6, share::OB_LS_NORMAL, 0, ObZone("z3"), flag, unit_id_list));

  ObArray<ObLSStatusInfo*> ls_ptrs_equal;
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&a1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&a2));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&b1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&b2));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&c1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_equal.push_back(&c2));

  ObArray<ObLSStatusInfo*> sorted_equal;
  ret = sorter.sort_ls_array_by_primary_zone_round_robin(ls_ptrs_equal, sorted_equal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, sorted_equal.count());
  // expected order: z1,z2,z3,z1,z2,z3 -> a1,b1,c1,a2,b2,c2
  ASSERT_EQ(&a1, sorted_equal.at(0));
  ASSERT_EQ(&b1, sorted_equal.at(1));
  ASSERT_EQ(&c1, sorted_equal.at(2));
  ASSERT_EQ(&a2, sorted_equal.at(3));
  ASSERT_EQ(&b2, sorted_equal.at(4));
  ASSERT_EQ(&c2, sorted_equal.at(5));

  // unequal counts: z1:3, z2:1, z3:2
  ObLSStatusInfo a3; ASSERT_EQ(OB_SUCCESS, a3.init(tenant_id, ObLSID(7), 7, share::OB_LS_NORMAL, 0, ObZone("z1"), flag, unit_id_list));
  ObArray<ObLSStatusInfo*> ls_ptrs_unequal;
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&a1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&a2));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&a3));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&b1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&c1));
  ASSERT_EQ(OB_SUCCESS, ls_ptrs_unequal.push_back(&c2));

  ObArray<ObLSStatusInfo*> sorted_unequal;
  ret = sorter.sort_ls_array_by_primary_zone_round_robin(ls_ptrs_unequal, sorted_unequal);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, sorted_unequal.count());
  // expected order by first-seen zones A,B,CRR: A1,B1,C1,A2,C2,A3
  ASSERT_EQ(&a1, sorted_unequal.at(0));
  ASSERT_EQ(&b1, sorted_unequal.at(1));
  ASSERT_EQ(&c1, sorted_unequal.at(2));
  ASSERT_EQ(&a2, sorted_unequal.at(3));
  ASSERT_EQ(&c2, sorted_unequal.at(4));
  ASSERT_EQ(&a3, sorted_unequal.at(5));
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
  ASSERT_EQ(true, balance_job.zone_array_.at(0)->is_in_locality_);
  ASSERT_EQ(false, balance_job.zone_array_.at(1)->is_in_locality_);
  ASSERT_EQ(true, balance_job.zone_array_.at(2)->is_in_locality_);

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
  ASSERT_EQ(4, balance_job.zone_array_.at(0)->valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(1)->valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(2)->valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(2)->deleting_unit_array_.count());
  ASSERT_EQ(5, balance_job.zone_array_.at(3)->valid_unit_array_.count());

  LOG_INFO("TESTTEST", "size", sizeof(balance_job), K(balance_job.zone_array_), K(balance_job.ug_array_));
  balance_job.reset();
  zone_list.at(3).unit_cnt_ = 4;
  unit_array.at(15).status_ = ObUnit::Status::UNIT_STATUS_DELETING;
  ret = job_desc.init_without_job(2, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, balance_job.zone_array_.at(0)->valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(1)->valid_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(2)->valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(2)->deleting_unit_array_.count());
  ASSERT_EQ(4, balance_job.zone_array_.at(3)->valid_unit_array_.count());
  ASSERT_EQ(1, balance_job.zone_array_.at(3)->deleting_unit_array_.count());

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
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx)->current_unit_list_.count());
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
    ASSERT_EQ(1, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
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
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx)->current_unit_list_.count());
  }
  lsg_op_array.reset();
  ObUnitListBalance unit_list_balance2(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance2.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
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
    ASSERT_EQ(2, balance_job.ls_group_array_.at(idx)->current_unit_list_.count());
  }
  lsg_op_array.reset();
  ObUnitListBalance unit_list_balance3(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance3.balance(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
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
    ASSERT_EQ(3, balance_job.ls_group_array_.at(idx)->current_unit_list_.count());
  }
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObMySQLProxy sql_proxy;
  ObUnitListBalance unit_list_balance(&balance_job, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
  ret = unit_list_balance.balance(true);
  ARRAY_FOREACH(balance_job.ls_group_array_, idx) {
    ASSERT_EQ(4, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
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
      ASSERT_EQ(2, balance_job.ls_group_array_.at(idx)->current_unit_list_.count());
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
      ASSERT_EQ(4, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
    } else {
      ASSERT_EQ(2, balance_job.ls_group_array_.at(idx)->target_unit_list_.count());
    }
  }
  ASSERT_EQ(1002, balance_job.ls_group_array_.at(0)->target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(0)->target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(0)->target_unit_list_.at(3).id());
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
  ASSERT_EQ(1007, balance_job.ls_group_array_.at(0)->target_unit_list_.at(1).id());
  ASSERT_EQ(1010, balance_job.ls_group_array_.at(0)->target_unit_list_.at(2).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(1)->target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(1)->target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(1)->target_unit_list_.at(3).id());

  ASSERT_EQ(1003, balance_job.ls_group_array_.at(2)->target_unit_list_.at(1).id());
  ASSERT_EQ(1006, balance_job.ls_group_array_.at(2)->target_unit_list_.at(2).id());
  ASSERT_EQ(1009, balance_job.ls_group_array_.at(2)->target_unit_list_.at(3).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(3)->target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(3)->target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(3)->target_unit_list_.at(3).id());

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
  ASSERT_EQ(1007, balance_job.ls_group_array_.at(0)->target_unit_list_.at(1).id());
  ASSERT_EQ(1010, balance_job.ls_group_array_.at(0)->target_unit_list_.at(2).id());

  ASSERT_EQ(1004, balance_job.ls_group_array_.at(1)->target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(1)->target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(1)->target_unit_list_.at(3).id());

  ASSERT_EQ(1004, balance_job.ls_group_array_.at(2)->target_unit_list_.at(1).id());
  ASSERT_EQ(1006, balance_job.ls_group_array_.at(2)->target_unit_list_.at(2).id());
  ASSERT_EQ(1009, balance_job.ls_group_array_.at(2)->target_unit_list_.at(3).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(3)->target_unit_list_.at(1).id());
  ASSERT_EQ(1005, balance_job.ls_group_array_.at(3)->target_unit_list_.at(2).id());
  ASSERT_EQ(1008, balance_job.ls_group_array_.at(3)->target_unit_list_.at(3).id());

  // 非倍数缩容优化：lg 1005 在 z2 上的 unit 1003 已是 DELETING（仅在 z2 上有 unit），
  // 在 z2 同构 zone 组下属于 deleting-only LSG。
  // z2 normal UG 个数为 2，但 deleting-only LSG 个数为 1，非整除，
  // 改造后保留 unit_list 不动，等 LSGroupCountBalance 通过 split 凑齐倍数。
  ASSERT_EQ(1003, balance_job.ls_group_array_.at(4)->target_unit_list_.at(0).id());

  ASSERT_EQ(1002, balance_job.ls_group_array_.at(5)->target_unit_list_.at(1).id());

}

TEST_F(TestLSBalanceHelper, balance_strategy_count_to_location_no_cancel)
{
  // 非倍数缩容优化：LSGroupCountBalance 完成 split 后回到 LSGroupLocationBalance，
  // 不应被识别为 strategy rollback 而 CANCELING。
  ObBalanceStrategy s_location(ObBalanceStrategy::LB_LS_GROUP_LOCATION);
  ObBalanceStrategy s_count(ObBalanceStrategy::LB_LS_GROUP_COUNT);
  ObBalanceStrategy s_unit_group(ObBalanceStrategy::LB_UNIT_GROUP);
  // count -> location 允许（本优化新增）
  ASSERT_TRUE(s_location.can_be_next_ls_balance_strategy(s_count));
  // location -> count 仍然允许（前进）
  ASSERT_TRUE(s_count.can_be_next_ls_balance_strategy(s_location));
  // count -> unit_group 不允许（旧的回退）
  ASSERT_FALSE(s_unit_group.can_be_next_ls_balance_strategy(s_count));
  // location -> location 不允许（自身相等）
  ASSERT_FALSE(s_location.can_be_next_ls_balance_strategy(s_location));
}


TEST_F(TestLSBalanceHelper, non_divisible_shrink_location_balance)
{
  // unit 布局（build_units(uzl_4x4) 生成，缩容前）：
  //   z1(4): unit 1000(ug2000), 1001(ug2001), 1002(ug2002), 1003(ug2003)
  //   z2(4): unit 1004(ug2000), 1005(ug2001), 1006(ug2002), 1007(ug2003)
  // 稳态 4 个 LSG（build_4x4_ls 构建）：
  //   lg 1001 on ug2000: unit_list={1000,1004}
  //   lg 1002 on ug2001: unit_list={1001,1005}
  //   lg 1003 on ug2002: unit_list={1002,1006}
  //   lg 1004 on ug2003: unit_list={1003,1007}
  // job_desc 的 zone_list 用缩容后的 pool unit_cnt（不含 DELETING）
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObTenantRole primary_role(ObTenantRole::Role::PRIMARY_TENANT);
  ObMySQLProxy sql_proxy;
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;

  auto construct_ls = [&ret, &ls_array, &tenant_id]
      (const uint64_t ls_id, const int64_t ls_group_id, const char *pz, const std::initializer_list<int64_t> unit_ids)
  {
    ObUnitIDList unit_id_list;
    for (const auto& uid : unit_ids) {
      ret = unit_id_list.push_back(ObDisplayUnitID(uid));
    }
    ObLSStatusInfo ls_status;
    ls_status.init(tenant_id, ObLSID(ls_id), ls_group_id, share::OB_LS_NORMAL, 0,
        ObZone(pz), share::ObLSFlag::NORMAL_FLAG, unit_id_list);
    ret = ls_array.push_back(ls_status);
  };
  auto find_lg = [](ObTenantLSBalanceInfo &bj, uint64_t lg_id) -> ObLSGroupStat* {
    for (int64_t i = 0; i < bj.ls_group_array_.count(); ++i) {
      if (bj.ls_group_array_.at(i)->lg_id_ == lg_id) return bj.ls_group_array_.at(i);
    }
    return nullptr;
  };
  auto has_unit = [](const ObUnitIDList &list, uint64_t uid) -> bool {
    for (int64_t i = 0; i < list.count(); ++i) {
      if (list.at(i).id() == uid) return true;
    }
    return false;
  };
  auto build_units = [&](const ObZoneUnitCntList &zl) {
    ObArray<ObLSStatusInfo> unused;
    unit_array.reset();
    ret = construct_unit_ls_array(zl, unit_array, unused);
    ASSERT_EQ(OB_SUCCESS, ret);
  };
  auto build_4x4_ls = [&]() {
    ls_array.reset();
    construct_ls(1, 0, "zone1", {});
    construct_ls(1001, 1001, "zone1", {1000, 1004});
    construct_ls(2001, 1001, "zone2", {1000, 1004});
    construct_ls(1002, 1002, "zone1", {1001, 1005});
    construct_ls(2002, 1002, "zone2", {1001, 1005});
    construct_ls(1003, 1003, "zone1", {1002, 1006});
    construct_ls(2003, 1003, "zone2", {1002, 1006});
    construct_ls(1004, 1004, "zone1", {1003, 1007});
    construct_ls(2004, 1004, "zone2", {1003, 1007});
  };
  ObZoneUnitCntList uzl_4x4;
  ASSERT_EQ(OB_SUCCESS, uzl_4x4.push_back(ObDisplayZoneUnitCnt("zone1", 4)));
  ASSERT_EQ(OB_SUCCESS, uzl_4x4.push_back(ObDisplayZoneUnitCnt("zone2", 4)));

  // ===== 1.1 生效场景 =====

  // 场景 A: 同构 DELETING 4→3
  // unit z1(4),z2(4), ug2003 DELETING, job_desc z1(3),z2(3) → target=3
  // lg 1004 ({1003,1007}) deleting-only, 1%3≠0 → preserved
  {
    build_units(uzl_4x4);
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObZoneUnitCntList zl;
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 3)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 3)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1003));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario A-1 (preserved) passed", KPC(lg));

    // A-2: split 后凑成倍数，LocationBalance 正常分配
    // 补 2 个 LSG 凑成 3 个 deleting-only, 3%3==0
    construct_ls(1005, 1005, "zone1", {1003, 1007});
    construct_ls(2005, 1005, "zone2", {1003, 1007});
    construct_ls(1006, 1006, "zone1", {1003, 1007});
    construct_ls(2006, 1006, "zone2", {1003, 1007});

    ObTenantLSBalanceInfo bj2(allocator);
    ret = bj2.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb2(&bj2, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb2.balance(true));
    ObLSGroupStat *lg4 = find_lg(bj2, 1004);
    ObLSGroupStat *lg5 = find_lg(bj2, 1005);
    ObLSGroupStat *lg6 = find_lg(bj2, 1006);
    ASSERT_TRUE(OB_NOT_NULL(lg4) && OB_NOT_NULL(lg5) && OB_NOT_NULL(lg6));
    ASSERT_FALSE(has_unit(lg4->target_unit_list_, 1003));
    ASSERT_FALSE(has_unit(lg5->target_unit_list_, 1003));
    ASSERT_FALSE(has_unit(lg6->target_unit_list_, 1003));
    LOG_INFO("scenario A-2 (split then allocate) passed");
  }

  // 场景 B: 异构 DELETING 4:4→4:3 (only z2 shrinks)
  // unit z1(4),z2(4), only unit 1007(z2) DELETING
  // job_desc z1(4),z2(3) → hetero, target=LCM(4,3)=12
  // lg 1004: z2 组 deleting-only, 1%3≠0 → preserved
  {
    build_units(uzl_4x4);
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObZoneUnitCntList zl;
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 4)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 3)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario B-1 (preserved) passed", KPC(lg));

    // B-2: split 后凑成倍数，LocationBalance 正常分配
    // 补 2 个 LSG 凑成 3 个 deleting-only, 3%3==0
    construct_ls(1005, 1005, "zone1", {1003, 1007});
    construct_ls(2005, 1005, "zone2", {1003, 1007});
    construct_ls(1006, 1006, "zone1", {1003, 1007});
    construct_ls(2006, 1006, "zone2", {1003, 1007});

    ObTenantLSBalanceInfo bj2(allocator);
    ret = bj2.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb2(&bj2, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb2.balance(true));
    ObLSGroupStat *lg4 = find_lg(bj2, 1004);
    ObLSGroupStat *lg5 = find_lg(bj2, 1005);
    ObLSGroupStat *lg6 = find_lg(bj2, 1006);
    ASSERT_TRUE(OB_NOT_NULL(lg4) && OB_NOT_NULL(lg5) && OB_NOT_NULL(lg6));
    ASSERT_FALSE(has_unit(lg4->target_unit_list_, 1007));
    ASSERT_FALSE(has_unit(lg5->target_unit_list_, 1007));
    ASSERT_FALSE(has_unit(lg6->target_unit_list_, 1007));
    LOG_INFO("scenario B-2 (split then allocate) passed");
  }

  // 场景 C: GTS 独占 同构 4→3
  // unit z1(4),z2(4), enable_gts=true → target=LCM(3,3)=3
  // 手动设各 zone 最后一个 unit 为 gts_standalone → valid=3
  // lg 1004 deleting-only (gts units), 1%3≠0 → preserved
  {
    build_units(uzl_4x4);
    build_4x4_ls();

    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, uzl_4x4, 1, 1, true, true, true);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    ARRAY_FOREACH(bj.zone_array_, zidx) {
      ObZoneLSStat *zs = bj.zone_array_.at(zidx);
      if (OB_NOT_NULL(zs) && zs->is_in_locality_ && zs->valid_unit_array_.count() > 0) {
        ObUnitLSStat *last = zs->valid_unit_array_.at(zs->valid_unit_array_.count() - 1);
        ret = zs->set_unit_gts_standalone(*last);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1003));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario C (GTS) passed", KPC(lg));
  }

  // ===== 1.2 不生效场景 =====
  // 基于 unit z1(4),z2(4) + ug2003 DELETING + job_desc z1(3),z2(3) 逐个切换条件

  // 场景 D: 非 deleting-only（LSG 同时在 normal UG 上有 unit）
  // lg 1004 unit_list={1003,1004}，unit 1004 在 ug2000(normal) → NOT deleting-only
  // lg 1005 unit_list={1008}，unit 1008 在 zone3 上，不在 locality 内 → NOT deleting-only
  {
    ObZoneUnitCntList uzl_4x4x1 = uzl_4x4;
    uzl_4x4x1.push_back(ObDisplayZoneUnitCnt("zone3", 1));
    build_units(uzl_4x4x1);
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();
    // 覆盖 lg 1004 的两个 LS，让 unit_list 包含 normal UG 的 unit
    ls_array.pop_back();
    ls_array.pop_back();
    construct_ls(1004, 1004, "zone1", {1003, 1004});
    construct_ls(2004, 1004, "zone2", {1003, 1004});
    // 再加一个 unit_list 仅在非 locality zone 的 lg 1005, unit_list: 1008(z3)
    construct_ls(1005, 1005, "zone3", {1008});

    ObZoneUnitCntList zl;  // 新 zl 不包含 z3, 仅包含 locality 内的 z1, z2
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 3)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 3)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1003));
    lg = find_lg(bj, 1005); // 1005 被修改，多补了一个 locality 内的 unit_list
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_TRUE(has_unit(lg->target_unit_list_, 1008));
    ASSERT_TRUE(lg->target_unit_list_.count() == 2);
    LOG_INFO("scenario D passed", KPC(lg));
  }

  // 场景 E: del_only_cnt 整除 normal_ug (倍数缩容) (2%2==0 → need_split=false)
  // unit z1(4),z2(4), ug2002+ug2003 DELETING → valid=2
  // job_desc z1(2),z2(2) → target=2, 4 LSG ≠ 2
  // 2 deleting-only LSG, 2%2==0 → need_split=false → re-allocated
  {
    build_units(uzl_4x4);
    unit_array.at(2).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(6).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObZoneUnitCntList zl;
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 2)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 2)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1003);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1002));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1006));
    LOG_INFO("scenario E passed", KPC(lg));
  }

  // 场景 F: 备库 → need_split=false → re-allocated
  {
    build_units(uzl_4x4);
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObZoneUnitCntList zl;
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 3)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 3)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantRole standby_role(ObTenantRole::Role::STANDBY_TENANT);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, standby_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1003));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario F passed", KPC(lg));
  }

  // 场景 G: enable_transfer=false → need_split=false → re-allocated
  {
    build_units(uzl_4x4);
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObZoneUnitCntList zl;
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone1", 3)));
    ASSERT_EQ(OB_SUCCESS, zl.push_back(ObDisplayZoneUnitCnt("zone2", 3)));
    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, zl, 1, 1, true, false, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1003));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario G passed", KPC(lg));
  }

  // 场景 H: LSG count == target → need_split=false → re-allocated
  // job_desc z1(4),z2(4) → target=LCM(4,4)=4 = LSG 数
  {
    build_units(uzl_4x4);
    unit_array.at(3).status_ = ObUnit::UNIT_STATUS_DELETING;
    unit_array.at(7).status_ = ObUnit::UNIT_STATUS_DELETING;
    build_4x4_ls();

    ObBalanceJobDesc jd;
    ret = jd.init_without_job(tenant_id, uzl_4x4, 1, 1, true, true, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObTenantLSBalanceInfo bj(allocator);
    ret = bj.init_tenant_ls_balance_info(tenant_id, ls_array, jd, unit_array, primary_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, bj.ls_group_array_.count());
    lsg_op_array.reset();
    ObLSGroupLocationBalance lb(&bj, &sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ASSERT_EQ(OB_SUCCESS, lb.balance(true));
    ObLSGroupStat *lg = find_lg(bj, 1004);
    ASSERT_TRUE(OB_NOT_NULL(lg));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1003));
    ASSERT_FALSE(has_unit(lg->target_unit_list_, 1007));
    LOG_INFO("scenario H passed", KPC(lg));
  }
}

TEST_F(TestLSBalanceHelper, large_cluster_tenant_ls_balance_info)
{
  // 测试大集群(unit_num>20)的情况下,ObTenantLSBalanceInfo可以正常构造
  uint64_t tenant_id = 500;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  ObDisplayZoneUnitCnt z1("zone1", 49);
  ObDisplayZoneUnitCnt z2("zone2", 50);
  ObZoneUnitCntList zone_list;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ret = construct_unit_ls_array(zone_list, unit_array, ls_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBalanceJobDesc job_desc;
  ret = job_desc.init_without_job(tenant_id, zone_list, 1, 1, true, true, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantLSBalanceInfo balance_job(allocator);
  ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array,
      job_desc, unit_array, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_BALANCE_JOB_VALID(balance_job);

  // 追加用例：100个zone，每个zone的unit_num=1，能够正常构造
  {
    ObZoneUnitCntList zone_list2;
    for (int i = 1; OB_SUCC(ret) && i <= 100; ++i) {
      char buf[32];
      snprintf(buf, sizeof(buf), "zone%d", i);
      ObDisplayZoneUnitCnt zi(ObZone(buf), 1);
      ret = zone_list2.push_back(zi);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ObArray<ObUnit> unit_array2;
    ObArray<ObLSStatusInfo> ls_array2;
    ret = construct_unit_ls_array(zone_list2, unit_array2, ls_array2);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObBalanceJobDesc job_desc2;
    ret = job_desc2.init_without_job(tenant_id, zone_list2, 1, 1, true, true, true);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTenantLSBalanceInfo balance_job2(allocator);
    ret = balance_job2.init_tenant_ls_balance_info(tenant_id, ls_array2, job_desc2, unit_array2, tenant_role);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(100, balance_job2.zone_array_.count());
    ARRAY_FOREACH(balance_job2.zone_array_, idx) {
      ASSERT_TRUE(balance_job2.zone_array_.at(idx)->is_valid());
      ASSERT_EQ(1, balance_job2.zone_array_.at(idx)->valid_unit_array_.count());
    }
    ASSERT_BALANCE_JOB_VALID(balance_job2);
  }
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
