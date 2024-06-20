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
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#define  private public
#include "share/balance/ob_balance_job_table_operator.h"
#include "share/balance/ob_balance_task_table_operator.h"
#include "share/balance/ob_balance_task_helper_operator.h"
#include "rootserver/ob_ls_balance_helper.h"//ObLSBalanceTaskHelper
#include "share/transfer/ob_transfer_info.h"
#include "share/ob_ls_id.h"
#include "share/unit/ob_unit_info.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_i_life_manager.h"



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
};

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
  int64_t primary_zone_num = 0;
  int64_t unit_group_num = 0;
  ObString comment;
  ObString balance_strategy(share::LS_BALANCE_BY_EXPAND);

  ASSERT_EQ(OB_INVALID_ARGUMENT, job.init(0, job_id, job_type, job_status, primary_zone_num, unit_group_num, comment, balance_strategy));
  job_type = ObBalanceJobType(ObString("LS_BALANCE"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, job.init(tenant_id_, job_id, job_type, job_status, primary_zone_num, unit_group_num, comment, balance_strategy));
  job_status = ObBalanceJobStatus(ObString("DOING"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, job.init(tenant_id_, job_id, job_type, job_status, primary_zone_num, unit_group_num, comment, balance_strategy));
  primary_zone_num = 1;
  unit_group_num = 1;
  ASSERT_EQ(OB_SUCCESS, job.init(tenant_id_, job_id, job_type, job_status, primary_zone_num, unit_group_num, comment, balance_strategy));
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
  ASSERT_EQ(new_job.get_primary_zone_num(), job.get_primary_zone_num());
  ASSERT_EQ(new_job.get_unit_group_num(), job.get_unit_group_num());
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
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list, finished_part_list,
      parent_list, child_list, comment));
  job_id = 1;
  task_id = 2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list, finished_part_list,
      parent_list, child_list, comment));
  start_time = 1;
  src_ls_id = 1;
  dest_ls_id = 1;
  task_type = ObString("LS_SPLIT");
  task_status = ObString("CREATE_LS");
  ObTransferPartInfo part_info;
  ObObjectID obj_id = 1;
  ASSERT_EQ(OB_SUCCESS, part_info.init(obj_id, obj_id));
  ObBalanceTaskID parent_task_id(1);
  ASSERT_EQ(OB_SUCCESS, parent_list.push_back(parent_task_id));
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info));
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list, finished_part_list,
      parent_list, child_list, comment));
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
  common::ObMySQLProxy *sql_proxy = get_curr_observer().get_gctx().sql_proxy_;
  int64_t primary_zone_num = 1;
  uint64_t tenant_id = 1002;
  ObSimpleUnitGroup unit_group1(1001, share::ObUnit::UNIT_STATUS_ACTIVE);
  ObSimpleUnitGroup unit_group2(1002, share::ObUnit::UNIT_STATUS_ACTIVE);
  ObSimpleUnitGroup unit_group3(1003, share::ObUnit::UNIT_STATUS_ACTIVE);
  ObSimpleUnitGroup unit_group4(1004, share::ObUnit::UNIT_STATUS_ACTIVE);
  ObArray<share::ObSimpleUnitGroup> unit_group;
  ASSERT_EQ(OB_SUCCESS, unit_group.push_back(unit_group1));
  ObLSID ls_id(1001);
  ObLSFlag ls_flag;
  ObLSStatusInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id, 1, share::OB_LS_NORMAL, 1001, "z1", ls_flag));
  ObLSStatusInfoArray ls_array;
  ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
  MTL_SWITCH(tenant_id) {
  {
    ObLSBalanceTaskHelper ls_balance;
    //case 1. init
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id, ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest1", K(ls_balance.unit_group_balance_array_));
    //case 2. no need balance

    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(false, need_balance);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ls_balance.generate_ls_balance_task());
  }

  //case 3 1->2;
  primary_zone_num = 2;
  MTL_SWITCH(tenant_id) {
    ObLSBalanceTaskHelper ls_balance;
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest3", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    ASSERT_EQ(primary_zone_num, ls_balance.job_.primary_zone_num_);
    ASSERT_EQ(ObBalanceJobType(ObBalanceJobType::BALANCE_JOB_LS), ls_balance.job_.job_type_);
    ASSERT_EQ(ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING), ls_balance.job_.job_status_);
    ASSERT_EQ(1, ls_balance.task_array_.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), ls_balance.task_array_[0].get_task_type());
  }

  //case 4 1-3
  primary_zone_num = 3;
  MTL_SWITCH(tenant_id) {
    ObLSBalanceTaskHelper ls_balance;
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest4", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    ASSERT_EQ(primary_zone_num, ls_balance.job_.primary_zone_num_);
    ASSERT_EQ(ObBalanceJobType(ObBalanceJobType::BALANCE_JOB_LS), ls_balance.job_.job_type_);
    ASSERT_EQ(ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING), ls_balance.job_.job_status_);
    ASSERT_EQ(2, ls_balance.task_array_.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), ls_balance.task_array_[0].get_task_type());
  }


  //case5 2-3
  MTL_SWITCH(tenant_id) {
    ObLSID ls_id2(1002);
    ObLSStatusInfo info2;
    ObLSBalanceTaskHelper ls_balance;
    ASSERT_EQ(OB_SUCCESS, info2.init(tenant_id, ls_id2, 1, share::OB_LS_NORMAL, 1001, "z2", ls_flag));
    ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info2));
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest5", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    ASSERT_EQ(primary_zone_num, ls_balance.job_.primary_zone_num_);
    ASSERT_EQ(ObBalanceJobType(ObBalanceJobType::BALANCE_JOB_LS), ls_balance.job_.job_type_);
    ASSERT_EQ(ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING), ls_balance.job_.job_status_);
    ASSERT_EQ(3, ls_balance.task_array_.count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), ls_balance.task_array_[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[2].get_task_type());
    //ASSERT_EQ(2, ls_balance.task_array_[2].parent_list_.count());
  }


  //case 6 ls group not in unit group
  MTL_SWITCH(tenant_id) {
    ObLSID ls_id3(1003);
    ObLSStatusInfo info3;
    ObLSBalanceTaskHelper ls_balance;
    ASSERT_EQ(OB_SUCCESS, info3.init(tenant_id, ls_id3, 1, share::OB_LS_NORMAL, 1002, "z2", ls_flag));
    ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info3));
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest6", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    LOG_INFO("testtest6", K(ls_balance.get_balance_job()), K(ls_balance.get_balance_tasks()));
    ASSERT_EQ(1, ls_balance.get_balance_tasks().count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[0].get_task_type());

  }
  }

  //case7 8->6
  MTL_SWITCH(tenant_id) {
    ASSERT_EQ(OB_SUCCESS, unit_group.push_back(unit_group2));
    ls_array.reset();
    ObLSBalanceTaskHelper ls_balance;
    for (int64_t i = 1001; i < 1009; ++i) {
      ObLSID ls_id(i);
      ObLSStatusInfo info;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id, 1, share::OB_LS_NORMAL, 1001, "z2", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
      i++;
      ObLSID ls_id1(i);
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id1, 2, share::OB_LS_NORMAL, 1002, "z2", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
    }
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest7", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    LOG_INFO("testtest7", K(ls_balance.get_balance_job()), K(ls_balance.get_balance_tasks()));
    ASSERT_EQ(6, ls_balance.get_balance_tasks().count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[3].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[4].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[5].get_task_type());

  }
  //case 8 6->9
  MTL_SWITCH(tenant_id) {
    ASSERT_EQ(OB_SUCCESS, unit_group.push_back(unit_group3));
    ls_array.reset();
    ObLSBalanceTaskHelper ls_balance;
    for (int64_t i = 1001; i < 1006; ++i) {
      ObLSID ls_id(i);
      ObLSStatusInfo info;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id, 1, share::OB_LS_NORMAL, 1001, "z2", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
      i++;
      ObLSID ls_id1(i);
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id1, 2, share::OB_LS_NORMAL, 1002, "z2", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
    }
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest8", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    LOG_INFO("testtest8", K(ls_balance.get_balance_job()), K(ls_balance.get_balance_tasks()));
    ASSERT_EQ(15, ls_balance.get_balance_tasks().count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), ls_balance.task_array_[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_SPLIT), ls_balance.task_array_[1].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[2].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[3].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[4].get_task_type());

  }

  //case 9 7->4
  MTL_SWITCH(tenant_id) {
    ASSERT_EQ(OB_SUCCESS, unit_group.push_back(unit_group4));
    ObLSBalanceTaskHelper ls_balance;
    ls_array.reset();
    for (int64_t i = 1001; i < 1007; ++i) {
      ObLSID ls_id(i);
      ObLSStatusInfo info;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id, 1, share::OB_LS_NORMAL, 1001, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
      i++;
      ObLSID ls_id1(i);
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ls_id1, 2, share::OB_LS_NORMAL, 1002, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
    }
    primary_zone_num = 1;
    ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
    LOG_INFO("testtest9", K(ls_balance.unit_group_balance_array_));
    bool need_balance = false;
    ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
    ASSERT_EQ(true, need_balance);
    ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
    LOG_INFO("testtest9", K(ls_balance.get_balance_job()), K(ls_balance.get_balance_tasks()));
    ASSERT_EQ(2, ls_balance.get_balance_tasks().count());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[0].get_task_type());
    ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[1].get_task_type());
    ls_array.reset();
    for (int64_t i = 1001; i < 1006; ++i) {
      ObLSStatusInfo info;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id,  ObLSID(i), i%3, share::OB_LS_NORMAL, 1001, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
      i++;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ObLSID(i), i%3, share::OB_LS_NORMAL, 1002, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
      i++;
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ObLSID(i), i%3, share::OB_LS_NORMAL, 1003, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
    }
      ASSERT_EQ(OB_SUCCESS, info.init(tenant_id, ObLSID(1007), 3, share::OB_LS_NORMAL, 1004, "z1", ls_flag));
      ASSERT_EQ(OB_SUCCESS, ls_array.push_back(info));
    primary_zone_num = 1;
    {
      ObLSBalanceTaskHelper ls_balance;
      ASSERT_EQ(OB_SUCCESS, ls_balance.init(tenant_id,ls_array, unit_group, primary_zone_num, sql_proxy));
      LOG_INFO("testtest9", K(ls_balance.unit_group_balance_array_));
      ASSERT_EQ(OB_SUCCESS, ls_balance.check_need_ls_balance(need_balance));
      ASSERT_EQ(true, need_balance);
      ASSERT_EQ(OB_SUCCESS, ls_balance.generate_ls_balance_task());
      LOG_INFO("testtest9", K(ls_balance.get_balance_job()), K(ls_balance.get_balance_tasks()));
      ASSERT_EQ(9, ls_balance.get_balance_tasks().count());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[0].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[1].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[2].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[3].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[4].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[5].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_TRANSFER), ls_balance.task_array_[6].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_ALTER), ls_balance.task_array_[7].get_task_type());
      ASSERT_EQ(ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE), ls_balance.task_array_[8].get_task_type());

    }
  }
}

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
  //防止后台线程结束这个任务
  ASSERT_EQ(OB_SUCCESS, parent_list.push_back(task_id));
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list, finished_part_list,
      parent_list, child_list, comment));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, ObBalanceTaskTableOperator::insert_new_task(task, sql_proxy));
  //设置part_list
  ObTransferPartInfo part_info(50001, 50001);
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info));
  transfer_task_id = ObTransferTaskID(1);
  ASSERT_EQ(OB_SUCCESS, task.init(
      tenant_id, job_id, task_id,
      ObBalanceTaskType(task_type),
      ObBalanceTaskStatus(task_status), ls_group_id,
      ObLSID(src_ls_id), ObLSID(dest_ls_id),
      transfer_task_id, part_list, finished_part_list,
      parent_list, child_list, comment));
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
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
