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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_MOCK_SQL_EXECUTOR_RPC_
#define OCEANBASE_SQL_EXECUTOR_OB_MOCK_SQL_EXECUTOR_RPC_

#include "sql/executor/ob_executor_rpc_impl.h"
#include "lib/queue/ob_spop_mpush_queue.h"
#include "../engine/table/ob_fake_partition_service.h"
#include "create_op_util.h"

namespace oceanbase
{
namespace sql
{
static const int64_t TEST_MOCK_COL_NUM = 3;

class ObMockSqlExecutorRpc : public ObExecutorRpcImpl
{
public:
  ObMockSqlExecutorRpc();
  virtual ~ObMockSqlExecutorRpc();
  /*
   * 提交一个异步task执行请求, 在OB_TASK_NOTIFY_FETCH消息的驱动下收取结果数据
   */
  virtual int task_submit(
      ObExecContext &ctx,
      ObTask &task,
      const common::ObAddr &svr);
  /*
   * 发送一个task并阻塞等待，直到对端返回执行状态
   * 将执行句柄保存在handler中, 随后可以通过handler收取数据
   * */
  virtual int task_execute(
      ObExecContext &ctx,
      ObTask &task,
      const common::ObAddr &svr,
      RemoteExecuteStreamHandle &handler);
  /*
   * 发送杀死一个task的命令并阻塞等待对端返回执行状态
   * */
  virtual int task_kill(
      ObTaskInfo &task,
      const common::ObAddr &svr);
  /*
   * Task在Worker端执行完成，通知Scheduler启动Task读取结果
   * */
  virtual int task_complete(
      ObTaskEvent &task_event,
      const common::ObAddr &svr);

  /*
   * 发送一个task的执行结果，不等待返回
   * */
  virtual int task_notify_fetch(
      ObTaskEvent &task_event,
      const common::ObAddr &svr);
  /*
   * 获取一个task的中间结果的所有scanner，阻塞等待直到所有的scanner都返回
   * */
  virtual int task_fetch_result(
      const ObSliceID &ob_slice_id,
      const common::ObAddr &svr,
      FetchResultStreamHandle &handler);

public:
  storage::ObFakePartitionService partition_service_;

private:
  bool task_location_exist(ObTaskLocation task_loc);
private:
  common::ObArray<ObTaskLocation> task_loc_array_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMockSqlExecutorRpc);
};

class ObMockRemoteExecuteStreamHandle : public RemoteExecuteStreamHandle
{
public:
  ObMockRemoteExecuteStreamHandle(common::ObIAllocator &alloc) : RemoteExecuteStreamHandle(alloc)
  {}
  ~ObMockRemoteExecuteStreamHandle()
  {}
  virtual int get_more(ObScanner &scanner);
  virtual bool has_more();
};


class ObMockFetchResultStreamHandle : public FetchResultStreamHandle
{
public:
  ObMockFetchResultStreamHandle(common::ObIAllocator &alloc) : FetchResultStreamHandle(alloc)
  {}
  ~ObMockFetchResultStreamHandle()
  {}
  virtual int get_more(ObScanner &scanner);
  virtual bool has_more();

  void set_server(const common::ObAddr &server) { server_ = server; }
  void set_slice_id(const ObSliceID &ob_slice_id) { ob_slice_id_ = ob_slice_id; }
private:
  bool task_location_exist(ObTaskLocation task_loc);
private:
  // task_submit的时候填入进来
  common::ObArray<ObTaskLocation> task_loc_array_;
  common::ObAddr server_;
  ObSliceID ob_slice_id_;
};

/************************************模拟 packet queue********************************/
class ObMockPacketQueueThread : public share::ObThreadPool
{
public:
  static const int64_t THREAD_COUNT = 1;
  static ObMockPacketQueueThread *get_instance();

  ObMockPacketQueueThread();
  virtual ~ObMockPacketQueueThread() {}

  void run1();

  common::ObSPopMPushQueue packet_queue_;
private:
  static ObMockPacketQueueThread *instance_;
  static obutil::Mutex locker_;
};

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_MOCK_SQL_EXECUTOR_RPC_ */
