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

#ifndef SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_
#define SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_

#include "sql/engine/ob_exec_context.h"
#include "storage/transaction/ob_trans_define.h"
#include "lib/allocator/ob_safe_arena.h"
#include "share/rpc/ob_batch_rpc.h"

namespace oceanbase {
namespace sql {

/*
 *      /-->-- FORBIDDEN
 *     /
 *    /---------->----------\
 *   /                       \
 * SENT -->-- RUNNING -->-- FINISHED -->-- RETURNED
 *   \             \                        /
 *    \------>------\----------->----------/
 *
 */
enum ObTaskStatus {
  TS_INVALID,
  TS_SENT,
  TS_RUNNING,
  TS_FINISHED,
  TS_FORBIDDEN,
  TS_NEED_WAIT_ABOVE,
  TS_RETURNED,
};

class ObReporter {
public:
  ObReporter();
  ObReporter(const ObTaskID& task_id);
  virtual ~ObReporter();
  bool operator==(const ObReporter& rv) const;
  int assign(const ObReporter& rv);

public:
  void set_task_id(const ObTaskID& task_id);
  void set_exec_svr(const common::ObAddr& exec_svr);
  void set_status(ObTaskStatus status);
  int reset_part_key();
  int add_part_key(const common::ObPartitionKey& part_key);
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  const common::ObAddr& get_exec_svr() const
  {
    return exec_svr_;
  }
  const common::ObPartitionArray get_part_keys() const
  {
    return part_keys_;
  }
  ObTaskStatus get_task_status() const
  {
    return status_;
  }  // allow dirty read.
  bool need_wait() const
  {
    return TS_INVALID < status_ && status_ < TS_NEED_WAIT_ABOVE;
  }
  TO_STRING_KV(K(task_id_), K(exec_svr_), K(part_keys_), K(status_));

private:
  // const attributes.
  ObTaskID task_id_;
  common::ObAddr exec_svr_;
  common::ObPartitionArray part_keys_;
  // other attributes.
  ObTaskStatus status_;
  common::ObSpinLock lock_;

private:
  static const int64_t TTL_THRESHOLD = 5;
};

typedef common::Ob2DArray<ObReporter, common::OB_MALLOC_NORMAL_BLOCK_SIZE> ObReporterArray;

class ObDistributedSchedulerManager;
class ObAPMiniTaskMgr;
class ObTransResultCollector {
public:
  ObTransResultCollector()
      : trans_result_(NULL),
        err_code_(OB_SUCCESS),
        lock_(),
        rpc_tenant_id_(OB_INVALID_TENANT_ID),
        trans_id_(),
        sql_no_(0),
        exec_rpc_(NULL),
        dist_task_mgr_(NULL),
        mini_task_mgr_(NULL),
        reporters_()
  {}
  virtual ~ObTransResultCollector()
  {}

public:
  int init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc, ObDistributedSchedulerManager* dist_task_mgr,
      ObAPMiniTaskMgr* mini_task_mgr);
  int wait_all_task(int64_t timeout, const bool is_build_index = false);
  const TransResult* get_trans_result()
  {
    return trans_result_;
  }
  void reset();

  int send_task(const ObTaskInfo& task_info);
  int recv_result(const ObTaskID& task_id, const TransResult& trans_result);
  int set_task_status(const ObTaskID& task_id, ObTaskStatus status);

private:
  int set_task_info(const ObTaskInfo& task_info, ObTaskStatus status);
  int get_reporter(const ObTaskID& task_id, bool allow_exist, ObReporter*& reporter);
  int ping_reporter(ObReporter& reporter);
  void wait_reporter_event(int64_t wait_timeout);

private:
  TransResult* trans_result_;
  int err_code_;
  common::ObSpinLock lock_;

  uint64_t rpc_tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t sql_no_;
  ObExecutorRpcImpl* exec_rpc_;
  ObDistributedSchedulerManager* dist_task_mgr_;
  ObAPMiniTaskMgr* mini_task_mgr_;
  ObReporterArray reporters_;
  obrpc::SingleWaitCond reporter_cond_;

private:
  static const int64_t TTL_THRESHOLD = 5;
  static const int64_t WAIT_ONCE_TIME = 500000;  // 500ms.
};

}  // namespace sql
}  // namespace oceanbase
#endif /* SRC_SQL_EXECUTOR_OB_TRANS_RESULT_COLLECTOR_H_ */
