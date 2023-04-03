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

#include "ob_mock_utils.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/executor/ob_task_event.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_local_job_executor.h"
#include "sql/executor/ob_local_task_executor.h"
#include "sql/executor/ob_distributed_task_runner.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "rpc/obrpc/ob_rpc_packet.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

ObMockSqlExecutorRpc::ObMockSqlExecutorRpc()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 5; ++i) {
   for (int64_t j = 1; OB_SUCC(ret) && j <= 10; ++j) {
      ObFakePartitionKey key;
      key.table_id_ = i;
      key.partition_id_ = j;
      ObPartitionLocation location;
      ObReplicaLocation replica_loc;
      replica_loc.server_.set_ip_addr("127.0.0.1", (int32_t)j);
      replica_loc.role_ = LEADER;
      if (OB_SUCCESS != (ret = location.add(replica_loc))) {
        SQL_ENG_LOG(WARN, "fail to add replica location", K(ret), K(i), K(j));
      }
    }
  }

  partition_service_.set_col_num(TEST_MOCK_COL_NUM);
  for (int64_t i = 0; OB_SUCC(ret) && i < 100; ++i) {
    ObNewRow row;
    ObObj objs[TEST_MOCK_COL_NUM];
    row.count_ = TEST_MOCK_COL_NUM;
    row.cells_ = objs;
    for (int64_t j = 0; j < TEST_MOCK_COL_NUM; ++j) {
      row.cells_[j].set_int(i);
    }
    partition_service_.add_row(row);
  }
}

ObMockSqlExecutorRpc::~ObMockSqlExecutorRpc()
{
}

/*
 * 提交一个异步task执行请求, 在OB_TASK_NOTIFY_FETCH消息的驱动下收取结果数据
 */
int ObMockSqlExecutorRpc::task_submit(
    ObExecContext &ctx,
    ObTask &task,
    const common::ObAddr &svr)
{
  UNUSED(ctx);
  UNUSED(task);
  UNUSED(svr);
  int ret = OB_SUCCESS;

  const static int64_t S_BUF_SIZE = 100000;
  char s_buf[S_BUF_SIZE];
  int64_t s_buf_pos = 0;
  if (OB_SUCCESS != (ret = task.serialize(s_buf, S_BUF_SIZE, s_buf_pos))) {
    SQL_EXE_LOG(WARN, "fail to serialize task", K(ret));
  } else {
    ObExecContext exec_ctx;

    ObMockSqlExecutorRpc rpc;
    ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
    ObMockRemoteExecuteStreamHandle resp_handler(allocator);
    ObTaskExecutorCtx *executor_ctx = exec_ctx.get_task_executor_ctx();
    executor_ctx->set_task_executor_rpc(&rpc);
    executor_ctx->set_task_response_handler(resp_handler);
    executor_ctx->set_server(svr);
    executor_ctx->set_partition_service(&partition_service_);

    //exec_ctx.set_query_ip_port(0);
    ObPhysicalPlan *phy_plan = ObPhysicalPlan::alloc();
    //ObPhyOperatorFactory *phy_op_factory = new ObPhyOperatorFactory();
    //phy_plan->set_operator_factory(phy_op_factory);

    ObTask new_task;
    new_task.set_deserialize_param(exec_ctx, *phy_plan);
    int64_t new_pos = 0;
    if (OB_SUCCESS != (ret = new_task.deserialize(s_buf, s_buf_pos, new_pos))) {
      SQL_EXE_LOG(WARN, "fail to deserialize", K(ret));
    } else {
      //执行plan
      ObPhyOperator *root_op = phy_plan->get_main_query();
      ObDistributedTransmitInput *trans_input = dynamic_cast<ObDistributedTransmitInput *>(ctx.get_phy_op_input(root_op->get_id()));
      OB_ASSERT(NULL != trans_input);
      ObSliceID sid = trans_input->get_ob_slice_id();
      ObDistributedTaskRunner task_runner;
      if (OB_SUCCESS != (ret = task_runner.execute(exec_ctx, *phy_plan))) {
        SQL_EXE_LOG(WARN, "fail execute task", K(sid));
      }
      //返回task event
      ObTaskEvent *task_event = new ObTaskEvent();
      ObTaskLocation task_loc;
      //ObAddr remote_server;
      //remote_server.set_ip_addr("127.0.0.1", 9999);
      task_loc.set_server(task.remote_server_);
      task_loc.set_ob_task_id(sid.get_ob_task_id());
      task_event->init(task_loc, ret);
      if (OB_SUCCESS != (ret = ObMockPacketQueueThread::get_instance()->packet_queue_.push(task_event))) {
        SQL_EXE_LOG(WARN, "fail to push packet into queue", K(ret));
      } else {
        SQL_EXE_LOG(INFO, "succeed to push packet into queue", "task_event", to_cstring(*task_event));
      }
    }
  }
  return ret;
}
/*
 * 发送一个task并阻塞等待，直到对端返回执行状态
 * 将执行句柄保存在handler中, 随后可以通过handler收取数据
 * */
int ObMockSqlExecutorRpc::task_execute(
    ObExecContext &ctx,
    ObTask &task,
    const common::ObAddr &svr,
    RemoteExecuteStreamHandle &handler)
{
  UNUSED(task);
  UNUSED(svr);
  UNUSED(handler);

  ObMockSqlExecutorRpc rpc;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
  ObMockRemoteExecuteStreamHandle resp_handler(allocator);
  ObTaskExecutorCtx *executor_ctx = ctx.get_task_executor_ctx();
  executor_ctx->set_task_executor_rpc(&rpc);
  executor_ctx->set_task_response_handler(resp_handler);
  executor_ctx->set_server(svr);
  executor_ctx->set_partition_service(&partition_service_);
  SQL_EXE_LOG(INFO, "task_execute");
  return OB_SUCCESS;
}
/*
 * 发送杀死一个task的命令并阻塞等待对端返回执行状态
 * */
int ObMockSqlExecutorRpc::task_kill(
    ObTaskInfo &task,
    const common::ObAddr &svr)
{
  UNUSED(task);
  UNUSED(svr);
  return OB_SUCCESS;
}
/*
 * Task在Worker端执行完成，通知Scheduler启动Task读取结果
 * */
int ObMockSqlExecutorRpc::task_complete(
    ObTaskEvent &task_event,
    const common::ObAddr &svr)
{
  UNUSED(task_event);
  UNUSED(svr);
  return OB_SUCCESS;
}

/*
 * 发送一个task的执行结果，不等待返回
 * */
int ObMockSqlExecutorRpc::task_notify_fetch(
    ObTaskEvent &task_event,
    const common::ObAddr &svr)
{
  UNUSED(task_event);
  UNUSED(svr);
  return OB_SUCCESS;
}
/*
 * 获取一个task的中间结果的所有scanner，阻塞等待直到所有的scanner都返回
 * */
int ObMockSqlExecutorRpc::task_fetch_result(
    const ObSliceID &ob_slice_id,
    const common::ObAddr &svr,
    FetchResultStreamHandle &handler)
{
  int ret = OB_SUCCESS;
  ObScanner &scanner = *handler.get_result();
  ObObj tmp_objs[TEST_MOCK_COL_NUM];
  oceanbase::common::ObNewRow tmp_row;
  tmp_row.count_ = TEST_MOCK_COL_NUM;
  tmp_row.cells_ = tmp_objs;
  ObIntermResultIterator iter;
  ObIntermResultManager *ir_mgr = ObIntermResultManager::get_instance();
  assert(NULL != ir_mgr);
  ObTaskLocation task_loc;
  task_loc.set_server(svr);
  task_loc.set_ob_task_id(ob_slice_id.get_ob_task_id());
  ObIntermResultInfo ir_info;
  ir_info.init(ob_slice_id);
  if (OB_SUCCESS != (ret = ir_mgr->get_result(ir_info, iter))) {
    SQL_EXE_LOG(WARN, "fail to get interm result iterator", K(ret),
                "ob_slice_id", to_cstring(ob_slice_id));
  } else if (task_location_exist(task_loc)) {
    //FIXME 暂时做成这样，第二次访问同一个location时返回OB_ITER_END
    ret = OB_ITER_END;
  } else {
    //FIXME 暂时只弄一个scanner，第二次访问同一个location返回OB_ITER_END
    while (OB_SUCC(ret)) {
      if (OB_SUCCESS != (ret = iter.get_next_row(tmp_row))) {
        if (OB_ITER_END != ret) {
          SQL_EXE_LOG(WARN, "fail to get next row", K(ret));
        } else {
          SQL_EXE_LOG(DEBUG, "iter has no more rows");
        }
      } else if (OB_SUCCESS != (ret = scanner.add_row(tmp_row))) {
        SQL_EXE_LOG(WARN, "fail to add row to scanner", K(ret));
      } else {
        SQL_EXE_LOG(DEBUG, "success to add row to scanner");
      }
    }
    if (OB_ITER_END == ret) {
      if (OB_SUCCESS != (ret = task_loc_array_.push_back(task_loc))) {
        SQL_EXE_LOG(WARN, "fail to push back to task location array", K(ret));
      } else{
        ret = OB_SUCCESS;

        SQL_EXE_LOG(DEBUG, "get a scanner",
                    "job_id", task_loc.get_job_id(),
                    "task_id", task_loc.get_task_id(),
                    "row_count", scanner.get_row_count());

      }
    }
  }
  SQL_EXE_LOG(INFO, "get interm result", K(ret), K(svr), K(ob_slice_id));
  return ret;
}

bool ObMockSqlExecutorRpc::task_location_exist(ObTaskLocation task_loc)
{
  bool loc_exist = false;
  for (int64_t i = 0; i < task_loc_array_.count(); ++i) {
    if (task_loc_array_.at(i).get_job_id() == task_loc.get_job_id()
        && task_loc_array_.at(i).get_task_id() == task_loc.get_task_id()) {
      loc_exist = true;
      break;
    }
  }
  return loc_exist;
}

bool ObMockFetchResultStreamHandle::has_more()
{
  return false;
}

int ObMockRemoteExecuteStreamHandle::get_more(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  static const int64_t col_num = TEST_MOCK_COL_NUM;
  for (int64_t i = 0; i < 1000; ++i) {
    ObNewRow row;
    ObObj objs[col_num];
    row.count_ = col_num;
    row.cells_ = objs;
    for (int64_t j = 0; j < col_num; ++j) {
      row.cells_[j].set_int(i);
    }
    if (OB_SUCCESS != (ret = scanner.add_row(row))) {
      SQL_EXE_LOG(ERROR, "fail to add row", K(i), K(ret));
    }
  }
  return ret;
}


bool ObMockRemoteExecuteStreamHandle::has_more()
{
  return false;
}

int ObMockFetchResultStreamHandle::get_more(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  ObObj tmp_objs[TEST_MOCK_COL_NUM];
  oceanbase::common::ObNewRow tmp_row;
  tmp_row.count_ = TEST_MOCK_COL_NUM;
  tmp_row.cells_ = tmp_objs;
  ObIntermResultIterator iter;
  ObIntermResultManager *ir_mgr = ObIntermResultManager::get_instance();
  assert(NULL != ir_mgr);
  ObTaskLocation task_loc;
  task_loc.set_server(server_);
  task_loc.set_ob_task_id(ob_slice_id_.get_ob_task_id());
  ObIntermResultInfo ir_info;
  ir_info.init(ob_slice_id_);
  if (OB_SUCCESS != (ret = ir_mgr->get_result(ir_info, iter))) {
    SQL_EXE_LOG(WARN, "fail to get interm result iterator", K(ret),
                "ob_slice_id", to_cstring(ob_slice_id_));
  } else if (task_location_exist(task_loc)) {
    //FIXME 暂时做成这样，第二次访问同一个location时返回OB_ITER_END
    ret = OB_ITER_END;
  } else {
    //FIXME 暂时只弄一个scanner，第二次访问同一个location返回OB_ITER_END
    while (OB_SUCC(ret)) {
      if (OB_SUCCESS != (ret = iter.get_next_row(tmp_row))) {
        if (OB_ITER_END != ret) {
          SQL_EXE_LOG(WARN, "fail to get next row", K(ret));
        } else {
          SQL_EXE_LOG(DEBUG, "iter has no more rows");
        }
      } else if (OB_SUCCESS != (ret = scanner.add_row(tmp_row))) {
        SQL_EXE_LOG(WARN, "fail to add row to scanner", K(ret));
      } else {
        SQL_EXE_LOG(DEBUG, "success to add row to scanner");
      }
    }
    if (OB_ITER_END == ret) {
      if (OB_SUCCESS != (ret = task_loc_array_.push_back(task_loc))) {
        SQL_EXE_LOG(WARN, "fail to push back to task location array", K(ret));
      } else{
        ret = OB_SUCCESS;

        SQL_EXE_LOG(DEBUG, "get a scanner",
                    "job_id", task_loc.get_job_id(),
                    "task_id", task_loc.get_task_id(),
                    "row_count", scanner.get_row_count());

      }
    }
  }
  SQL_EXE_LOG(INFO, "get interm result", K(ret), K_(server), K_(ob_slice_id));
  return ret;
}


bool ObMockFetchResultStreamHandle::task_location_exist(ObTaskLocation task_loc)
{
  bool loc_exist = false;
  for (int64_t i = 0; i < task_loc_array_.count(); ++i) {
    if (task_loc_array_.at(i).get_job_id() == task_loc.get_job_id()
        && task_loc_array_.at(i).get_task_id() == task_loc.get_task_id()) {
      loc_exist = true;
      break;
    }
  }
  return loc_exist;
}

ObMockPacketQueueThread *ObMockPacketQueueThread::instance_ = NULL;
obutil::Mutex ObMockPacketQueueThread::locker_;

ObMockPacketQueueThread::ObMockPacketQueueThread()
{
  packet_queue_.init(8192, common::ObModIds::OB_SQL_EXECUTOR_TASK_EVENT);
  set_thread_count(THREAD_COUNT);
}

ObMockPacketQueueThread *ObMockPacketQueueThread::get_instance()
{
  if (NULL == instance_) {
    locker_.lock();
    if (NULL == instance_) {
      instance_ = new ObMockPacketQueueThread();
      if (NULL == instance_) {
        SQL_EXE_LOG(ERROR, "instance is NULL, unexpected");
      } else {
        //empty
      }
    }
    locker_.unlock();
  }
  return instance_;
}

void ObMockPacketQueueThread::run(obsys::CThread *thread, void *arg)
{
  SQL_EXE_LOG(INFO, "mock packet queue is running...");

  UNUSED(arg);
  int ret = OB_SUCCESS;
  int64_t timeout_us = INT64_MAX;
  void *msg = NULL;
  ObDistributedSchedulerManager *sc_manager = ObDistributedSchedulerManager::get_instance();
  while(OB_SUCCESS == (ret = ObMockPacketQueueThread::get_instance()->packet_queue_.pop(timeout_us, msg))) {
    ObTaskEvent *packet = reinterpret_cast<ObTaskEvent*>(msg);
    if (NULL == packet) {
      SQL_EXE_LOG(ERROR, "packet is NULL");
    } else if (OB_SUCCESS != (ret = sc_manager->signal_scheduler(*packet))) {
      SQL_EXE_LOG(WARN, "fail to signal scheduler", K(ret));
    } else {
      //empty
    }
  }
}

}/* ns sql*/
}/* ns oceanbase */
