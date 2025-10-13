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
class TestBalanceOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestBalanceOperator() : unittest::ObSimpleClusterTestBase("test_balance_operator") {}
protected:
  uint64_t tenant_id_;
  int construct_unit_array(const ObZoneUnitCntList &unit_list,
      ObArray<ObUnit> &zone_array);
};


int TestBalanceOperator::construct_unit_array(const ObZoneUnitCntList &unit_list,
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

TEST_F(TestBalanceOperator, BalanceJob)
{
  int ret = OB_SUCCESS;
  tenant_id_ = OB_SYS_TENANT_ID;
  //ObBalanceJob INIT
  ObBalanceJob job;
  ObBalanceJobID job_id(1);
  int64_t job_start_time = 1;
  int64_t job_finish_time = 1;
  ObBalanceJobType job_type;
  ObBalanceJobStatus job_status;
  ObString comment;
  ObBalanceStrategy balance_strategy(ObBalanceStrategy::LB_EXPAND);

  ASSERT_EQ(OB_INVALID_ARGUMENT, job.init(0, job_id, job_type, job_status, comment, balance_strategy));
  job_type = ObBalanceJobType(ObString("LS_BALANCE"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, job.init(tenant_id_, job_id, job_type, job_status, comment, balance_strategy));
  job_status = ObBalanceJobStatus(ObString("DOING"));
  ASSERT_EQ(OB_SUCCESS, job.init(tenant_id_, job_id, job_type, job_status, comment, balance_strategy));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  LOG_INFO("[MITTEST]balance_job", K(job));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  //insert
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobTableOperator::insert_new_job(job, sql_proxy));
  ObBalanceJob new_job;
  //select
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobTableOperator::get_balance_job(OB_SYS_TENANT_ID, false, sql_proxy, new_job, job_start_time, job_finish_time));
  LOG_INFO("[MITTEST]balance_job", K(new_job));
  ASSERT_EQ(new_job.get_tenant_id(), job.get_tenant_id());
  ASSERT_EQ(new_job.get_job_id(), job.get_job_id());
  ASSERT_EQ(new_job.get_job_type(), job_type);
  ASSERT_EQ(new_job.get_job_status(), job_status);

  ASSERT_EQ(OB_OP_NOT_ALLOW, ObBalanceJobTableOperator::clean_job(OB_SYS_TENANT_ID, job_id, sql_proxy));
  //update
  ObBalanceJobStatus new_job_status(2);
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobTableOperator::update_job_status(OB_SYS_TENANT_ID, job_id, job_status, new_job_status, false, ObString(), sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobTableOperator::get_balance_job(OB_SYS_TENANT_ID, false, sql_proxy, new_job, job_start_time, job_finish_time));
  ASSERT_EQ(new_job.get_job_status(), new_job_status);
  //clean
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobTableOperator::clean_job(OB_SYS_TENANT_ID, job_id, sql_proxy));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObBalanceJobTableOperator::get_balance_job(OB_SYS_TENANT_ID, false, sql_proxy, new_job, job_start_time, job_finish_time));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from __all_balance_job_history"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    int64_t start_time = 0;
    int64_t finish_time = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_time("create_time", start_time));
    ASSERT_EQ(OB_SUCCESS, result->get_time("finish_time", finish_time));
    LOG_INFO("[MITTEST]job status", K(start_time), K(finish_time));
  }

  // test ObBalanceStrategy
  ObBalanceStrategy strategy;
  for(int64_t i = 0; i <= ObBalanceStrategy::MAX_STRATEGY; ++i) {
    strategy = static_cast<ObBalanceStrategy::STRATEGY>(i);
    ASSERT_TRUE(0 == strcmp(strategy.str(), ObBalanceStrategy::BALANCE_STRATEGY_STR_ARRAY[i]));
    LOG_INFO("TEST: ObBalanceStrategy", K(i), K(strategy));
  }
  strategy = static_cast<ObBalanceStrategy::STRATEGY>(-1);
  ASSERT_TRUE(0 == strcmp(strategy.str(), "unknown"));
  LOG_INFO("TEST: ObBalanceStrategy unknown", K(strategy));
}

TEST_F(TestBalanceOperator, BalanceTask)
{
  int ret = OB_SUCCESS;
  //ObBalanceTask INIT
  ObBalanceTask task;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObBalanceJobID job_id;
  ObBalanceTaskID task_id;
  int64_t ls_group_id = 0;
  int64_t start_time = 0;
  int64_t finish_time = 0;
  int64_t src_ls_id = 0;
  int64_t dest_ls_id = 0;
  //TODO
  ObTransferTaskID transfer_task_id;
  ObString task_type, task_status;
  ObString part_list_str, finished_part_list_str;
  ObString parent_list_str, child_list_str;
  ObTransferPartList part_list, finished_part_list;
  ObBalanceTaskIDList parent_list, child_list;
  ObString comment;
  ObString balance_strategy_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list_str, finished_part_list_str,
      parent_list_str, child_list_str, comment, balance_strategy_str));
  job_id = 1;
  task_id = 2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list_str, finished_part_list_str,
      parent_list_str, child_list_str, comment, balance_strategy_str));
  start_time = 1;
  src_ls_id = 1;
  dest_ls_id = 1;
  task_type = ObString("LS_SPLIT");
  task_status = ObString("CREATE_LS");
  ObTransferPartInfo part_info;
  ObObjectID obj_id = 1;
  part_list_str = ObString("1:1");
  parent_list_str = ObString("1");
  ASSERT_EQ(OB_SUCCESS, part_info.init(obj_id, obj_id));
  ObBalanceTaskID parent_task_id(1);
  ASSERT_EQ(OB_SUCCESS, parent_list.push_back(parent_task_id));
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info));
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list_str, finished_part_list_str,
      parent_list_str, child_list_str, comment, balance_strategy_str));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

 //insert
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::insert_new_task(task, sql_proxy));

 //select
 ObBalanceTask new_task;
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
 ASSERT_EQ(new_task.get_parent_task_list().at(0), ObBalanceTaskID(1));
 LOG_INFO("[MITTEST]balance_task", K(new_task));
 //update balance task
 ObBalanceTaskStatus transfer_status = ObBalanceTaskStatus::BALANCE_TASK_STATUS_TRANSFER;
 common::ObMySQLTransaction trans2;
 ASSERT_EQ(OB_SUCCESS, trans2.start(&sql_proxy, tenant_id));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::update_task_status(new_task, transfer_status, trans2));
 ASSERT_EQ(OB_SUCCESS, trans2.end(true));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
 LOG_INFO("[MITTEST]balance_task", K(new_task));
 //start transfer task
 transfer_task_id = 1;
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::start_transfer_task(OB_SYS_TENANT_ID, task_id, transfer_task_id, sql_proxy));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
 LOG_INFO("[MITTEST]balance_task", K(new_task));
 //finish transfer task
 finished_part_list.reset();
 ObTransferPartList to_do_part_list;
 bool all_part_transferred = false;
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::finish_transfer_task(new_task, transfer_task_id, finished_part_list, sql_proxy, to_do_part_list, all_part_transferred));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
 LOG_INFO("[MITTEST]balance_task", K(new_task));
 //remove parent task
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::remove_parent_task(OB_SYS_TENANT_ID, task_id, parent_task_id, sql_proxy));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
 ObBalanceTaskArray task_array;
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::load_can_execute_task(OB_SYS_TENANT_ID, task_array, sql_proxy));
 LOG_INFO("[MITTEST]balance_task", K(new_task), K(task_array));

 //clean task
 //update task status into completed before clean task
 ObBalanceTaskStatus completed_status = ObBalanceTaskStatus::BALANCE_TASK_STATUS_COMPLETED;
 common::ObMySQLTransaction trans1;
 ASSERT_EQ(OB_SUCCESS, trans1.start(&sql_proxy, tenant_id));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::update_task_status(new_task, completed_status, trans1));
 ASSERT_EQ(OB_SUCCESS, trans1.end(true));
 ObMySQLTransaction trans;
 ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id));
 ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::clean_task(OB_SYS_TENANT_ID, task_id, trans));
 ASSERT_EQ(OB_SUCCESS, trans.end(true));

 ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObBalanceTaskTableOperator::get_balance_task(OB_SYS_TENANT_ID, task_id , false, sql_proxy, new_task, start_time, finish_time));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from __all_balance_task_history"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    int64_t start_time = 0;
    int64_t finish_time = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_time("create_time", start_time));
    ASSERT_EQ(OB_SUCCESS, result->get_time("finish_time", finish_time));
    LOG_INFO("[MITTEST]balance_task", K(start_time), K(finish_time));
  }
}

TEST_F(TestBalanceOperator, balance_execute)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  common::ObArray<share::ObLSGroupUnitListOp> op_array;
  common::ObArray<share::ObUnitUGOp> unit_op_array;
  common::ObMySQLProxy *sql_proxy = get_curr_observer().get_gctx().sql_proxy_;
  int64_t primary_zone_num = 1;
  uint64_t tenant_id = 1002;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObTenantLSBalanceInfo balance_job(allocator);
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 3);
  ObZoneUnitCntList zone_list;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObUnit> unit_array;
  ret = construct_unit_array(zone_list, unit_array);
 //日志流id分布：1000, 1001, 1002
 //1003, 1004, 1005
  ObArray<ObLSStatusInfo> ls_array;
  ObBalanceJobDesc job_desc;
  ObBalanceJobID job_id;
  ret = job_desc.init_without_job(tenant_id, zone_list, primary_zone_num, 1, true, true, false);
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ObLSStatusInfo sys_ls;
  ObUnitIDList unit_id_list;
  ret = sys_ls.init(tenant_id, SYS_LS, 0, share::OB_LS_NORMAL, 0, primary_zone, flag, unit_id_list);
  if (OB_FAIL(ls_array.push_back(sys_ls))) {
    LOG_WARN("push back", KR(ret), K(sys_ls));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSID ls_id(1001);
  ObLSStatusInfo info;
  ret = unit_id_list.push_back(ObDisplayUnitID(1000));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = unit_id_list.push_back(ObDisplayUnitID(1003));
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id, 1001, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  //日志流组个数少
  share::ObBalanceJob job;
  common::ObArray<share::ObBalanceTask> task_array;
  MTL_SWITCH(tenant_id) {
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(4, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[3].get_task_type());
  }
  //日志流组个数少
  ObLSID ls_id2(1002);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id2, 1001, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(6, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[3].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[4].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[5].get_task_type());
  }
  ls_array.at(2).ls_group_id_ = 1002;
  ObLSID ls_id3(1003);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id3, 1003, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  ObLSID ls_id4(1004);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id4, 1004, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(8, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[3].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[4].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[5].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[6].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[7].get_task_type());

  }
  //2->3
  //有两个日志流组，把1003和1004的日志流组分别修改成1001，1002
  ls_array.at(3).ls_group_id_ = 1001;
  ls_array.at(4).ls_group_id_ = 1002;
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(1, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());

  }
  //2->3 修改ls_group_id1002 的unit_list到另外一个unit_group
  ObUnitIDList unit_id_list1;
  ret = unit_id_list1.push_back(ObDisplayUnitID(1001));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = unit_id_list1.push_back(ObDisplayUnitID(1004));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ls_array.at(1).unit_id_list_.assign(unit_id_list1));
  ASSERT_EQ(OB_SUCCESS, ls_array.at(3).unit_id_list_.assign(unit_id_list1));
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(1, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());

  }
  //变更成异构zone
  zone_list.at(0).unit_cnt_ = 2;
  zone_list.at(1).unit_cnt_ = 4;
  unit_array.reset();
  ret = construct_unit_array(zone_list, unit_array);
  //unit 分布是：1000：2000 ； 1001：2001
  //1002：20002 1003：2003 1004：2004 1005：2005
  //当前只有两个日志流组，都需要扩容
  //日志流组分别在1000 + 1003 1001 + 1004
  ret = job_desc.init_without_job(tenant_id, zone_list, primary_zone_num, 2, true, true, false);
  LOG_INFO("testtest", K(ls_array));
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(2, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());

  }
  //构造出6个日志流组的情况
  ObUnitIDList unit_id_list2;
  ret = unit_id_list2.push_back(ObDisplayUnitID(1000));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = unit_id_list2.push_back(ObDisplayUnitID(1005));
  ASSERT_EQ(OB_SUCCESS, ret);
  ls_array.at(2).ls_group_id_ = 1003;
  ls_array.at(3).ls_group_id_ = 1004;
  ObLSID ls_id5(1005);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id5, 1003, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  ObLSID ls_id6(1006);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id6, 1005, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list2));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  ObLSID ls_id7(1007);
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id7, 1006, share::OB_LS_NORMAL, 0, "z1", flag, unit_id_list2));
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));


  //ls1 :1001  ls2: 1002 在1001+ 1004
  //ls3 + ls5 : 1003  ls4 :1004在1000+1003
  //ls6:1005 ls7 1006 在1000 + 1005
  LOG_INFO("testtest", K(ls_array));
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(9, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_CREATE), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), task_array[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[3].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[4].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[5].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), task_array[6].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[7].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[8].get_task_type());
  }
  ObUnitIDList unit_id_list3;
  ret = unit_id_list3.push_back(ObDisplayUnitID(1000));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = unit_id_list3.push_back(ObDisplayUnitID(1002));
  ASSERT_EQ(OB_SUCCESS, ret);
  ls_array.at(4).unit_id_list_ = unit_id_list3;
  ASSERT_EQ(OB_SUCCESS, ls_array.at(4).unit_id_list_.assign(unit_id_list3));
  LOG_INFO("testtest", K(ls_array.at(4)));
  //跨cell合并
  MTL_SWITCH(tenant_id) {
    balance_job.reset();
    job.reset();
    task_array.reset();
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, job_id, &job, &task_array, &op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("testtest", K(job), K(task_array));
    ASSERT_EQ(2, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());

  }


}
/*
  //case7 8->6
  //case 8 6->9
  //case 9 7->4

*/
//验证merge任务在transfer结束后再次设置part_list然后结束
TEST_F(TestBalanceOperator, merge_task)
{
  ObBalanceTask task;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObBalanceJobID job_id(1);
  ObBalanceTaskID task_id(1);
  int64_t ls_group_id = OB_INVALID_ID;
  int64_t start_time = 0;
  int64_t finish_time = 0;
  int64_t src_ls_id = 1;
  int64_t dest_ls_id = 2;
  //TODO
  ObTransferTaskID transfer_task_id;
  ObString task_type, task_status;
  ObString part_list_str, finished_part_list_str;
  ObString parent_list_str, child_list_str;
  ObTransferPartList part_list, finished_part_list;
  ObBalanceTaskIDList parent_list, child_list;
  task_type = ObString("LS_MERGE");
  task_status = ObString("TRANSFER");
  ObString comment;
  ObString balance_strategy_str;
  //防止后台线程结束这个任务
  ASSERT_EQ(OB_SUCCESS, parent_list.push_back(task_id));
  ASSERT_EQ(OB_SUCCESS, task.init(
        tenant_id, job_id, task_id,
        ObBalanceTaskType(task_type),
        ObBalanceTaskStatus(task_status), ls_group_id,
        ObLSID(src_ls_id), ObLSID(dest_ls_id),
        transfer_task_id, part_list_str, finished_part_list_str,
        parent_list_str, child_list_str, comment, balance_strategy_str));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::insert_new_task(task, sql_proxy));
  //设置part_list
  ObTransferPartInfo part_info(50001, 50001);
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info));
  part_list_str = ObString("50001:50001");
  transfer_task_id = ObTransferTaskID(1);
  ASSERT_EQ(OB_SUCCESS, task.init(
        tenant_id, job_id, task_id,
        ObBalanceTaskType(task_type),
        ObBalanceTaskStatus(task_status), ls_group_id,
        ObLSID(src_ls_id), ObLSID(dest_ls_id),
        transfer_task_id, part_list_str, finished_part_list_str,
        parent_list_str, child_list_str, comment, balance_strategy_str));
  LOG_INFO("testtest7: start set part list");
  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::update_task_part_list(tenant_id, task_id, part_list, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::start_transfer_task(tenant_id, task_id, transfer_task_id, sql_proxy));
  ObTransferPartList to_do_part_list;
  bool all_part_transferred = false;
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::finish_transfer_task(task, transfer_task_id, part_list, sql_proxy, to_do_part_list, all_part_transferred));
  ASSERT_EQ(0, to_do_part_list.count());
  ASSERT_EQ(true, all_part_transferred);
  LOG_INFO("testtest8: start set part list");
  common::ObMySQLTransaction trans1;
  ASSERT_EQ(OB_SUCCESS, trans1.start(&sql_proxy, tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::update_task_part_list(tenant_id, task_id, part_list, trans1));
  ASSERT_EQ(OB_SUCCESS, trans1.end(true));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::start_transfer_task(tenant_id, task_id, transfer_task_id, sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::finish_transfer_task(task, transfer_task_id, part_list, sql_proxy, to_do_part_list, all_part_transferred));
  ASSERT_EQ(0, to_do_part_list.count());
  ASSERT_EQ(true, all_part_transferred);
  LOG_INFO("testtest9: start set part list");
  common::ObMySQLTransaction trans2;
  ASSERT_EQ(OB_SUCCESS, trans2.start(&sql_proxy, tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::update_task_part_list(tenant_id, task_id, part_list, trans2));
  ASSERT_EQ(OB_SUCCESS, trans2.end(true));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::start_transfer_task(tenant_id, task_id, transfer_task_id, sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::finish_transfer_task(task, transfer_task_id, part_list, sql_proxy, to_do_part_list, all_part_transferred));
  ASSERT_EQ(0, to_do_part_list.count());
  ASSERT_EQ(true, all_part_transferred);
}

TEST_F(TestBalanceOperator, test_ls_group_count_balance_disable_transfer)
{
  // test expand ls_group_count when enable_transfer is false
  uint64_t tenant_id = 1002;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = get_curr_observer().get_gctx().sql_proxy_;
  common::ObArray<share::ObLSGroupUnitListOp> lsg_op_array;
  ObArray<ObUnit> unit_array;
  ObArray<ObLSStatusInfo> ls_array;
  ObTenantRole tenant_role(ObTenantRole::Role::PRIMARY_TENANT);

  // lambda for construct ls and push back to ls array
  auto construct_ls = [&ret, &ls_array, &tenant_id]
      (const uint64_t ls_id, const int64_t ls_group_id, const std::initializer_list<int64_t> unit_ids)
  {
    ObUnitIDList unit_id_list;
    for (const auto& unit_id : unit_ids) {
      ret = unit_id_list.push_back(ObDisplayUnitID(unit_id));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ObLSStatusInfo ls_status;
    ls_status.init(tenant_id, ObLSID(ls_id), ls_group_id, share::OB_LS_NORMAL, 0, ObZone("zone1"), share::ObLSFlag::NORMAL_FLAG, unit_id_list);
    ret = ls_array.push_back(ls_status);
    ASSERT_EQ(OB_SUCCESS, ret);
  };

  /* case 1: all ls cnt > all ls group cnt
  row_1: ls_cnt < target_lsg_cnt, all cells in one row can not expand
  row_2: ls_cnt < target_lsg_cnt, part of cells in one row can not expand
  row_3: ls_cnt >= target_lsg_cnt, part of cells in one row can not expand
  括号包裹数字代表一个日志流组，日志流个数为括号中数字
  +-----+------+------+------+------+
  |     |col_1 |col_2 |col_3 |col_4 |
  +-----+------+------+------+------+
  |row_1| (1)  |  (1) |      |      |
  +-----+------+------+------+------+
  |row_2| (1)  |      | (2)  |      | -> expand 1 lsg in cell(row_2, col_3)
  +-----+------+------+------+------+
  |row_3| (1)  |      |(1)(2)|      | -> expand 1 lsg in cell(row_3, col_3)
  +-----+------+------+------+------+
  z1 units: 1000, 1001, 1002
  z2 units: 1003, 1004, 1005, 1006
  */
  // 9 LS, 7 LS group
  {
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 4);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = construct_unit_array(zone_list, unit_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ls_array.reuse();
  // sys_ls
  construct_ls(1, 0, {});
  // normal ls
  construct_ls(1001, 1001, {1000, 1003});
  construct_ls(1002, 1002, {1001, 1003});
  construct_ls(1003, 1003, {1002, 1003});
  construct_ls(1004, 1004, {1000, 1004});
  construct_ls(1005, 1005, {1001, 1005});  // alter to new lsg
  construct_ls(1006, 1005, {1001, 1005});
  construct_ls(1007, 1006, {1002, 1005});
  construct_ls(1008, 1007, {1002, 1005});  // alter to new lsg
  construct_ls(1009, 1007, {1002, 1005});

  ObBalanceJobDesc job_desc;
  ObTenantLSBalanceInfo balance_job(allocator);

  ret = job_desc.init_without_job(1, zone_list, 1, 1, true, false, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  MTL_SWITCH(tenant_id) {
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    common::ObArray<share::ObUnitUGOp> unit_op_array;
    share::ObBalanceJob job;
    common::ObArray<share::ObBalanceTask> task_array;
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, job.is_valid());
    ASSERT_EQ(2, task_array.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), task_array[1].get_task_type());
    ASSERT_EQ(1005, task_array[0].get_src_ls_id().id_);
    ASSERT_EQ(1008, task_array[1].get_src_ls_id().id_);
    LOG_INFO("ls group count balance case 1", K(job), K(task_array));
  }
  }


  /* case 2: all ls cnt == all ls group cnt, all cells in all rows can not expand
  won't do balance and generate no job
  +-----+------+------+------+------+
  |row_1| (1)  |      |(1)(1)|      |
  +-----+------+------+------+------+
  |row_2| (1)  |      | (1)  | (1)  |
  +-----+------+------+------+------+
  |row_3|      |      |      |      |
  +-----+------+------+------+------+
  z1 units: 1000, 1001, 1002
  z2 units: 1003, 1004, 1005, 1006
  */
  // 6 LS, 6 LS group
  {
  ObDisplayZoneUnitCnt z1("zone1", 3);
  ObDisplayZoneUnitCnt z2("zone2", 4);
  ObZoneUnitCntList zone_list;
  ret = zone_list.push_back(z1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_list.push_back(z2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = construct_unit_array(zone_list, unit_array);
  ASSERT_EQ(OB_SUCCESS, ret);

  ls_array.reuse();
  // sys_ls
  construct_ls(1, 0, {});
  // normal ls
  construct_ls(1001, 1001, {1000, 1003});
  construct_ls(1002, 1002, {1001, 1003});
  construct_ls(1003, 1003, {1000, 1005});
  construct_ls(1004, 1004, {1000, 1005});
  construct_ls(1005, 1005, {1001, 1005});
  construct_ls(1006, 1006, {1001, 1006});

  ObBalanceJobDesc job_desc;
  ObTenantLSBalanceInfo balance_job(allocator);

  ret = job_desc.init_without_job(1, zone_list, 1, 1, true, false, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  MTL_SWITCH(tenant_id) {
    ret = balance_job.init_tenant_ls_balance_info(tenant_id, ls_array, job_desc, unit_array, tenant_role);
    ASSERT_EQ(OB_SUCCESS, ret);
    common::ObArray<share::ObUnitUGOp> unit_op_array;
    share::ObBalanceJob job;
    common::ObArray<share::ObBalanceTask> task_array;
    ObLSGroupCountBalance lg_cnt(&balance_job, sql_proxy, ObBalanceJobID(), &job, &task_array, &lsg_op_array, &unit_op_array);
    ret = lg_cnt.balance(true);
    ASSERT_EQ(OB_SUCCESS, ret);
    job.reset();
    ret = lg_cnt.balance(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, job.is_valid());
    LOG_INFO("ls group count balance case 2", K(job), K(task_array));
  }
  }
}

TEST_F(TestBalanceOperator, ls_balance_helper)
{
  ObBalanceTaskHelper task;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObBalanceTaskHelperOp invalid_op;
  int64_t ls_group_id = OB_INVALID_ID;
  ObLSID src_ls_id(1);
  ObLSID dest_ls_id(2);
  SCN invalid_scn;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, invalid_scn, invalid_op,
      src_ls_id, dest_ls_id, ls_group_id));
  ObBalanceTaskHelperOp op(1);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, invalid_scn, op,
      src_ls_id, dest_ls_id, ls_group_id));
  uint64_t ts = 10000000000000;
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(ts));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, scn, op,
      src_ls_id, dest_ls_id, ls_group_id));
  ls_group_id = 0;
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, scn, op,
      src_ls_id, dest_ls_id, ls_group_id));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::insert_ls_balance_task(task, sql_proxy));

  ObArray<ObBalanceTaskHelper> new_task;
  SCN op_scn;
  ASSERT_EQ(OB_SUCCESS, op_scn.convert_from_ts(ts));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
        tenant_id, sql_proxy, op_scn, new_task));
  ASSERT_EQ(1, new_task.count());
  ASSERT_EQ(new_task.at(0).get_operation_scn().convert_to_ts(), ts);
  ASSERT_EQ(ls_group_id, new_task.at(0).get_ls_group_id());
  ts += 10000000;
  ObBalanceTaskHelperOp new_op(0);
  ls_group_id = 1001;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(ts));
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, scn, new_op,
      src_ls_id, dest_ls_id, ls_group_id));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::insert_ls_balance_task(task, sql_proxy));
  ASSERT_EQ(OB_SUCCESS, op_scn.convert_from_ts(ts));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
        tenant_id, sql_proxy, op_scn, new_task));
  ASSERT_EQ(2, new_task.count());
  ts = 10000000000000;
  ASSERT_EQ(new_task.at(0).get_operation_scn().convert_to_ts(), ts);
  ASSERT_EQ(new_task.at(0).get_ls_group_id(), 0);
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::remove_task(tenant_id,
        new_task.at(0).get_operation_scn(), sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
        tenant_id, sql_proxy, op_scn, new_task));
  ASSERT_EQ(1, new_task.count());

  ASSERT_EQ(ls_group_id, new_task.at(0).get_ls_group_id());
}

TEST_F(TestBalanceOperator, ObBalanceJobDesc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1002;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObArenaAllocator allocator;
  // test ObBalanceJobDescOperator
  ObBalanceJobID job_id(123);
  int64_t primary_zone_num = 3;
  int64_t ls_scale_out_factor = 1;
  bool enable_rebalance = true;
  bool enable_transfer = true;
  bool enable_gts_standalone = true;
  ObZoneUnitCntList zone_unit_list;
  ObDisplayZoneUnitCnt zone1(ObZone("zone1"), 2);
  ObDisplayZoneUnitCnt zone2(ObZone("zone2"), 3);
  ObDisplayZoneUnitCnt zone3(ObZone("zone3"), 3);
  ASSERT_EQ(OB_SUCCESS, zone_unit_list.push_back(zone1));
  ASSERT_EQ(OB_SUCCESS, zone_unit_list.push_back(zone2));
  ASSERT_EQ(OB_SUCCESS, zone_unit_list.push_back(zone3));
  ObBalanceJobDesc job_desc;
  ASSERT_EQ(OB_SUCCESS, job_desc.init(tenant_id, job_id, zone_unit_list, primary_zone_num, ls_scale_out_factor, enable_rebalance, enable_transfer, enable_gts_standalone));
  LOG_INFO("TEST: test print job_desc", K(job_desc));
  ObBalanceJobDesc job_desc_from_table;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObBalanceJobDescOperator::get_balance_job_desc(tenant_id, job_id, inner_sql_proxy, job_desc_from_table));
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::insert_balance_job_desc(tenant_id, job_id, job_desc, inner_sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::get_balance_job_desc(tenant_id, job_id, inner_sql_proxy, job_desc_from_table));
  LOG_INFO("TEST: job_desc from table", K(job_desc_from_table));
  ASSERT_TRUE(job_desc_from_table.get_job_id() == job_id
      && job_desc_from_table.get_ls_cnt_in_group() == primary_zone_num * ls_scale_out_factor
      && job_desc_from_table.get_enable_rebalance() == enable_rebalance
      && job_desc_from_table.get_enable_transfer() == enable_transfer
      && job_desc_from_table.get_enable_gts_standalone() == enable_gts_standalone);
  ASSERT_TRUE(job_desc_from_table.get_zone_unit_num_list().count() == zone_unit_list.count());
  ARRAY_FOREACH(job_desc_from_table.get_zone_unit_num_list(), idx) {
    const ObDisplayZoneUnitCnt &from_table = job_desc_from_table.get_zone_unit_num_list().at(idx);
    const ObDisplayZoneUnitCnt &true_value = zone_unit_list.at(idx);
    ASSERT_TRUE(from_table.get_zone() == true_value.get_zone() && from_table.get_unit_cnt() == true_value.get_unit_cnt());
  }

  // test zone_unit_num_list with max_num
  ObZoneUnitCntList max_zone_unit_list;
  for(int64_t i = 0; i < MAX_ZONE_NUM; ++i) {
    ObSqlString zone_name;
    ASSERT_EQ(OB_SUCCESS, zone_name.assign_fmt("zone_max_length_128_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyza%ld", i));
    ASSERT_EQ(OB_SUCCESS, max_zone_unit_list.push_back(ObDisplayZoneUnitCnt(ObZone(zone_name.ptr()), 33)));
  }
  ObString max_zone_unit_list_str;
  ASSERT_EQ(OB_SUCCESS, max_zone_unit_list.to_display_str(allocator, max_zone_unit_list_str));
  LOG_INFO("TEST: max_zone_unit_list_str", K(max_zone_unit_list_str));
  ObZoneUnitCntList max_zone_unit_list_from_str;
  ASSERT_EQ(OB_SUCCESS, max_zone_unit_list_from_str.parse_from_display_str(max_zone_unit_list_str));
  ASSERT_TRUE(max_zone_unit_list_from_str == max_zone_unit_list);

  // test zone_unit_num_list replica_type
  // if replica_type is F, formatted str has no replica type
  // if replica_type is L, formatted str has replica type, format is zone:replia_type:unit_num, e.g. z1:LOGONLY:2
  ObZoneUnitCntList zone_unit_cnt_list;
  ObZoneUnitCntList zone_unit_cnt_list_from_str;
  ObDisplayZoneUnitCnt zone1_replica_type_f(ObZone("zone1"), 3, ObReplicaType::REPLICA_TYPE_FULL);
  ObDisplayZoneUnitCnt zone2_replica_type_l(ObZone("zone2"), 3, ObReplicaType::REPLICA_TYPE_LOGONLY);
  ASSERT_EQ(OB_SUCCESS, zone_unit_cnt_list.push_back(zone1_replica_type_f));
  ASSERT_EQ(OB_SUCCESS, zone_unit_cnt_list.push_back(zone2_replica_type_l));
  ObString zone_unit_cnt_list_str;
  ASSERT_EQ(OB_SUCCESS, zone_unit_cnt_list.to_display_str(allocator, zone_unit_cnt_list_str));
  ASSERT_EQ(OB_SUCCESS, zone_unit_cnt_list_from_str.parse_from_display_str(zone_unit_cnt_list_str));
  ASSERT_TRUE(zone_unit_cnt_list_from_str == zone_unit_cnt_list);
  ASSERT_EQ(0, STRNCMP("zone1:3,zone2:LOGONLY:3", zone_unit_cnt_list_str.ptr(), zone_unit_cnt_list_str.length()));

  ObString parameter_list_str;
  job_desc.enable_rebalance_ = false;
  job_desc.enable_transfer_ = false;
  job_desc.enable_gts_standalone_ = false;
  job_desc.ls_scale_out_factor_ = 10;
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::construct_parameter_list_str_(job_desc, allocator, parameter_list_str));
  LOG_INFO("TEST: construct_parameter_list_str_", K(parameter_list_str), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::parse_parameter_from_str_(parameter_list_str, ls_scale_out_factor, enable_rebalance, enable_transfer, enable_gts_standalone));
  ASSERT_TRUE(10 == ls_scale_out_factor && !enable_rebalance && !enable_transfer && !enable_gts_standalone);
  job_desc.enable_rebalance_ = true;
  job_desc.enable_transfer_ = true;
  job_desc.enable_gts_standalone_ = true;
  job_desc.ls_scale_out_factor_ = 1;
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::construct_parameter_list_str_(job_desc, allocator, parameter_list_str));
  LOG_INFO("TEST: construct_parameter_list_str_", K(parameter_list_str), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, ObBalanceJobDescOperator::parse_parameter_from_str_(parameter_list_str, ls_scale_out_factor, enable_rebalance, enable_transfer, enable_gts_standalone));
  ASSERT_TRUE(1 == ls_scale_out_factor && enable_rebalance && enable_transfer && enable_gts_standalone);

  // test check_if_need_cancel_by_job_desc_
  ObBalanceJob job;
  bool need_cancel = false;
  ObSqlString comment;
  ObSqlString old_comment;
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  ObTenantBalanceService *balance_service = MTL(ObTenantBalanceService*);
  ObBalanceJobType job_type = ObBalanceJobType(ObString("LS_BALANCE"));
  ObBalanceJobStatus job_status = ObBalanceJobStatus(ObString("DOING"));
  ObBalanceStrategy balance_strategy(ObBalanceStrategy::LB_EXPAND);
  ASSERT_EQ(OB_SUCCESS, job.init(tenant_id, job_id, job_type, job_status, comment.string(), balance_strategy));
  ASSERT_EQ(OB_SUCCESS, balance_service->gather_stat_());
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff enable_rebalance
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.enable_rebalance_ = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff enable_transfer
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.enable_transfer_ = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff enable_gts_standalone
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.enable_gts_standalone_ = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff ls_scale_out_factor
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.ls_scale_out_factor_ = 3;
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff primary_zone_num
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.primary_zone_num_ = 1;
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));

  // diff zone_unit_cnt_list
  need_cancel = false;
  ASSERT_EQ(OB_SUCCESS, balance_service->job_desc_.assign(job_desc));
  balance_service->job_desc_.zone_unit_num_list_.push_back(ObDisplayZoneUnitCnt(ObZone("zone4"), 1));
  ASSERT_EQ(OB_SUCCESS, balance_service->check_if_need_cancel_by_job_desc_(job, need_cancel, comment));
  ASSERT_TRUE(need_cancel && 0 != comment.string().compare(old_comment.string()));
  LOG_INFO("TEST: check_if_need_cancel_by_job_desc_", K(comment), K(need_cancel), K(balance_service->job_desc_), K(job_desc));
  ASSERT_EQ(OB_SUCCESS, old_comment.assign(comment));
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
