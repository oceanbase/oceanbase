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
#include "share/balance/ob_transfer_partition_task_table_operator.h"
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
class TestTransferPartition : public unittest::ObSimpleClusterTestBase
{
public:
  TestTransferPartition() : unittest::ObSimpleClusterTestBase("test_transfer_partition_task") {}
protected:

  uint64_t tenant_id_;
};

TEST_F(TestTransferPartition, TransferPartitionTask)
{
  int ret = OB_SUCCESS;
//  ASSERT_EQ(OB_SUCCESS, create_tenant());
  //ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  tenant_id_ = 1;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObTransferPartitionTask task;
  ObBalanceJobID job_id(1);
  ObBalanceJobID invalid_job;
  ObTransferPartInfo part_info(1, 1);
  ObTransferPartInfo invalid_part;
  ObLSID invalid_ls;
  ObLSID ls(1);
  ObTransferPartitionTaskID invalid_task_id;
  ObTransferPartitionTaskID task_id(1);
  ObTransferPartitionTaskStatus status(ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING);
  ObTransferPartitionTaskStatus invalid_status;
  ObString comment;
  // case 1: 验证task的init
  //simple_init不校验task_id，但是init校验
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.simple_init(tenant_id_, invalid_part, ls, task_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.simple_init(tenant_id_, part_info, invalid_ls, task_id));
  ASSERT_EQ(OB_SUCCESS, task.simple_init(tenant_id_, part_info, ls, invalid_task_id));
  ASSERT_EQ(OB_SUCCESS, task.simple_init(tenant_id_, part_info, ls, task_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(tenant_id_, part_info, ls, invalid_task_id,
        job_id, ObTransferTaskID(1), status, comment));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(tenant_id_, part_info, ls, task_id,
        job_id, ObTransferTaskID(1), invalid_status, comment));
  ASSERT_EQ(OB_SUCCESS, task.init(tenant_id_, part_info, ls, task_id, job_id,
        ObTransferTaskID(1), status, comment));
  // case 2: 验证task的插入
  ObMySQLTransaction trans;
  ObArray<ObTransferPartitionTask> task_array;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_SUCCESS, task.simple_init(tenant_id_, part_info, ls, task_id));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info, ls, trans));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(1, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(status, task_array.at(0).get_task_status());

  // case 3: 验证不同的事务，在第一个事务没有提交之前，另外一个事务不能写入
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ObTransferPartitionTask new_task;
  ObTransferPartInfo part_info2(1, 2);
  ASSERT_EQ(OB_SUCCESS, new_task.simple_init(tenant_id_, part_info2, ls, task_id));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info2, ls, trans));
  ObMySQLTransaction trans1;
  ASSERT_EQ(OB_SUCCESS, trans1.start(&sql_proxy, tenant_id_));
  ObTransferPartInfo part_info3(1, 3);
  ASSERT_EQ(OB_SUCCESS, new_task.simple_init(tenant_id_, part_info3, ls, task_id));
  ASSERT_EQ(OB_TRANS_TIMEOUT, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info3, ls, trans1));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));
  ASSERT_EQ(OB_SUCCESS, trans1.end(false));
  ASSERT_EQ(OB_SUCCESS, trans1.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info3, ls, trans1));
  ASSERT_EQ(OB_SUCCESS, new_task.simple_init(tenant_id_, part_info2, ls, task_id));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info2, ls, trans1));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans1));
  ASSERT_EQ(3, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(OB_SUCCESS, trans1.end(true));
  // case 4: 验证table_id,object_id不能重复
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ObTransferPartitionTaskTableOperator::insert_new_task(tenant_id_, part_info, ls, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));

  // case 5: 验证使用第一个task_id开始执行
  ObTransferPartitionTaskID task_id2 = task_array.at(1).get_task_id();
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTransferPartitionTaskTableOperator::set_all_tasks_schedule(
        tenant_id_, task_id2, job_id, 1, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::set_all_tasks_schedule(
        tenant_id_, task_id2, job_id, 2, trans));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(3, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(1, task_array.at(0).get_balance_job_id().id());
  ASSERT_EQ(1, task_array.at(1).get_balance_job_id().id());
  ASSERT_EQ(-1, task_array.at(2).get_balance_job_id().id());

  //case 6: 开始transfer，三个分区放进去会不匹配
  ObTransferPartList part_list;
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info));
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info2));
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info3));
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  //ASSERT_EQ(OB_ERR_UNEXPECTED, ObTransferPartitionTaskTableOperator::start_transfer_task(tenant_id_, part_list, ObTransferTaskID(100), trans));
  ASSERT_EQ(OB_SUCCESS, part_list.remove(1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTransferPartitionTaskTableOperator::start_transfer_task(tenant_id_,
        invalid_job, part_list, ObTransferTaskID(100), trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));
  ObBalanceJobID tmp_job_id(999);
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::start_transfer_task(tenant_id_,
       tmp_job_id, part_list, ObTransferTaskID(100), trans));

 ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(3, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(-1, task_array.at(0).get_transfer_task_id().id());
  ASSERT_EQ(-1, task_array.at(1).get_transfer_task_id().id());
  ASSERT_EQ(-1, task_array.at(2).get_transfer_task_id().id());
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::start_transfer_task(tenant_id_,
       job_id, part_list, ObTransferTaskID(100), trans));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(3, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(100, task_array.at(0).get_transfer_task_id().id());
  ASSERT_EQ(100, task_array.at(1).get_transfer_task_id().id());
  ASSERT_EQ(-1, task_array.at(2).get_transfer_task_id().id());

  //case 7: 结束transfer
  ASSERT_EQ(OB_SUCCESS, part_list.remove(0));
  ASSERT_EQ(OB_SUCCESS, part_list.push_back(part_info2));
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ObTransferPartitionTaskID max_task_id(1000);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTransferPartitionTaskTableOperator::finish_task(tenant_id_, part_list, max_task_id, status, "", trans));
  ObTransferPartitionTaskStatus status1(ObTransferPartitionTaskStatus::TRP_TASK_STATUS_FAILED);
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::finish_task(tenant_id_, part_list,max_task_id, status1,"12343453", trans));
 ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(1, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select *, time_to_usec(create_time) as create_1, time_to_usec(finish_time) as finish from __all_transfer_partition_task_history"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    int64_t start_time = 0;
    int64_t finish_time = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, tenant_id_, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ObTransferPartitionTask new_task3;
    ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::fill_cell_(tenant_id_, result, new_task3));
    ASSERT_EQ(OB_SUCCESS, result->get_int("create_1", start_time));
    ASSERT_EQ(OB_SUCCESS, result->get_int("finish", finish_time));

    LOG_INFO("[MITTEST]balance_task", K(start_time), K(finish_time), K(new_task3));
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::fill_cell_(tenant_id_, result, new_task3));
    ASSERT_EQ(OB_SUCCESS, result->get_int("create_1", start_time));
    ASSERT_EQ(OB_SUCCESS, result->get_int("finish", finish_time));
    LOG_INFO("[MITTEST]balance_task", K(start_time), K(finish_time), K(new_task3));

  }

  //case 8: 验证rollback
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTransferPartitionTaskTableOperator::rollback_all_to_waitting(
        tenant_id_, ObBalanceJobID(), trans));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::rollback_all_to_waitting(
        tenant_id_, job_id, trans));
  ASSERT_EQ(OB_SUCCESS, ObTransferPartitionTaskTableOperator::load_all_task(tenant_id_,
        task_array, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(1, task_array.count());
  LOG_INFO("[MITTEST]new task", K(task_array));
  ASSERT_EQ(-1, task_array.at(0).get_balance_job_id().id());
  ASSERT_EQ(-1, task_array.at(0).get_transfer_task_id().id());
  ASSERT_EQ(status, task_array.at(0).get_task_status());

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
