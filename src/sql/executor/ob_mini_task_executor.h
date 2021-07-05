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

#ifndef DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_
#define DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_trans_result_collector.h"
#include "lib/allocator/ob_safe_arena.h"
namespace oceanbase {
namespace sql {
class ObAPMiniTaskMgr : public common::ObDLinkBase<ObAPMiniTaskMgr> {
  static const int64_t MAX_FINISH_QUEUE_CAPACITY = 512;

public:
  ObAPMiniTaskMgr()
      : ref_count_(0),
        mgr_rcode_(common::OB_SUCCESS),
        rcode_addrs_(),
        allocator_(common::ObModIds::OB_SQL_EXECUTOR_MINI_TASK_EVENT),
        trans_result_(),
        lock_()
  {}
  virtual ~ObAPMiniTaskMgr()
  {}

  int32_t get_type()
  {
    return 0;
  }
  static ObAPMiniTaskMgr* alloc();
  static void free(ObAPMiniTaskMgr* item);
  inline int64_t inc_ref_count()
  {
    return ATOMIC_AAF((uint64_t*)&ref_count_, 1);
  }
  inline int64_t def_ref_count()
  {
    return ATOMIC_SAF((uint64_t*)&ref_count_, 1);
  }
  int init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc);
  virtual void reset() override;
  void set_mgr_rcode(int mgr_rcode)
  {
    mgr_rcode_ = mgr_rcode;
  }
  int atomic_push_mgr_rcode_addr(const ObAddr& addr);
  common::ObIArray<ObAddr>& get_rcode_addr()
  {
    return rcode_addrs_;
  }
  int get_mgr_rcode() const
  {
    return mgr_rcode_;
  }
  int save_task_result(
      const common::ObAddr& task_addr, int64_t task_id, int32_t ret_code, const ObMiniTaskResult& result);
  int pop_task_event(int64_t timeout, ObMiniTaskEvent*& complete_task);
  void close_task_event(ObMiniTaskEvent* task_event);

  inline int send_task(const ObTaskInfo& task_info)
  {
    return trans_result_.send_task(task_info);
  }
  int merge_trans_result(const ObTaskID& task_id, const ObMiniTaskResult& result);
  int set_task_status(const ObTaskID& task_id, ObTaskStatus status);
  int wait_all_task(int64_t timeout)
  {
    return trans_result_.wait_all_task(timeout);
  }

private:
  int64_t ref_count_;
  int mgr_rcode_;
  common::ObArray<ObAddr> rcode_addrs_;
  common::ObSafeArena allocator_;
  common::ObLightyQueue finish_queue_;
  ObTransResultCollector trans_result_;
  // for the on_timeout() of mini task callback.
  mutable common::ObSpinLock lock_;
};

typedef common::ObGlobalFactory<ObAPMiniTaskMgr, 1, common::ObModIds::OB_SQL_EXECUTOR_MINI_TASK_MGR>
    ObAPMiniTaskMgrGFactory;
typedef common::ObTCFactory<ObAPMiniTaskMgr, 1, common::ObModIds::OB_SQL_EXECUTOR_MINI_TASK_MGR>
    ObApMiniTaskMgrTCFactory;

inline ObAPMiniTaskMgr* ObAPMiniTaskMgr::alloc()
{
  ObAPMiniTaskMgr* ap_mini_task_mgr = NULL;
  if (OB_ISNULL(ObApMiniTaskMgrTCFactory::get_instance())) {
    SQL_EXE_LOG(ERROR, "get ap mini task mgr factory instance failed");
    ap_mini_task_mgr = NULL;
  } else {
    ap_mini_task_mgr = ObApMiniTaskMgrTCFactory::get_instance()->get(0);
  }
  return ap_mini_task_mgr;
}

inline void ObAPMiniTaskMgr::free(ObAPMiniTaskMgr* item)
{
  if (item != NULL) {
    int64_t ref_count = item->def_ref_count();
    if (OB_LIKELY(0 == ref_count)) {
      // nobody reference this object, so free it
      if (OB_ISNULL(ObApMiniTaskMgrTCFactory::get_instance())) {
        SQL_EXE_LOG(ERROR, "get ap mini task mgr factory instance failed");
      } else {
        item->reset();
        ObApMiniTaskMgrTCFactory::get_instance()->put(item);
        item = NULL;
      }
    } else if (OB_UNLIKELY(ref_count < 0)) {
      SQL_EXE_LOG(ERROR, "ref_count is invalid", K(ref_count));
    }
  }
}

class ObSQLSessionInfo;
class ObMiniTaskExecutor {
public:
  explicit ObMiniTaskExecutor(common::ObIAllocator& allocator) : ap_mini_task_mgr_(NULL)
  {
    UNUSED(allocator);
  }
  virtual ~ObMiniTaskExecutor()
  {
    destroy();
  }
  void destroy();
  int init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc);
  int merge_trans_result(const ObTaskID& task_id, const ObMiniTaskResult& task_result);
  int wait_all_task(int64_t timeout);
  static int add_invalid_servers_to_retry_info(
      const int ret, const ObIArray<ObAddr>& addr, ObQueryRetryInfo& retry_info);

protected:
  int mini_task_local_execute(ObExecContext& query_ctx, ObMiniTask& task, ObMiniTaskResult& task_result);
  int sync_fetch_local_result(ObExecContext& ctx, const ObPhyOperator& root_op, common::ObScanner& result);
  int sync_fetch_local_result(ObExecContext& ctx, const ObOpSpec& root_spec, ObScanner& result);
  int check_scanner_errcode(const ObMiniTaskResult& src);
  int check_scanner_errcode(const ObMiniTaskEvent& complete_task, ObMiniTaskRetryInfo& retry_info);
  int wait_ap_task_finish(
      ObExecContext& ctx, int64_t ap_task_cnt, ObMiniTaskResult& result, ObMiniTaskRetryInfo& retry_info);
  int pop_ap_mini_task_event(ObExecContext& ctx, ObMiniTaskEvent*& complete_task);

protected:
  ObAPMiniTaskMgr* ap_mini_task_mgr_;
};

class ObDMLMiniTaskExecutor : public ObMiniTaskExecutor {
public:
  explicit ObDMLMiniTaskExecutor(common::ObIAllocator& allocator) : ObMiniTaskExecutor(allocator)
  {}
  virtual ~ObDMLMiniTaskExecutor()
  {}
  int execute(ObExecContext& ctx, const ObMiniJob& mini_job, common::ObIArray<ObTaskInfo*>& task_list, bool table_first,
      ObMiniTaskResult& task_result);
  // sync execute
  int mini_task_execute(
      ObExecContext& ctx, const ObMiniJob& mini_job, ObTaskInfo& task_info, ObMiniTaskResult& task_result);
  // async execute
  int mini_task_submit(ObExecContext& ctx, const ObMiniJob& mini_job, common::ObIArray<ObTaskInfo*>& task_info_list,
      int64_t start_idx, ObMiniTaskResult& task_result);
  int build_mini_task_op_input(ObExecContext& ctx, ObTaskInfo& task_info, const ObMiniJob& mini_job);
  int build_mini_task_op_input(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& root_op);
  int build_mini_task_op_input(ObExecContext& ctx, ObTaskInfo& task_info, const ObOpSpec& root_spec);
  int build_mini_task(ObExecContext& ctx, const ObMiniJob& mini_job, ObTaskInfo& task_info, ObMiniTask& task);
};

class ObLookupMiniTaskExecutor : public ObMiniTaskExecutor {
public:
  explicit ObLookupMiniTaskExecutor(common::ObIAllocator& allocator) : ObMiniTaskExecutor(allocator)
  {}
  virtual ~ObLookupMiniTaskExecutor()
  {}
  int execute(ObExecContext& ctx, common::ObIArray<ObMiniTask>& task_list,
      common::ObIArray<ObTaskInfo*>& task_info_list, ObMiniTaskRetryInfo& retry_info, ObMiniTaskResult& task_result);
  int execute_one_task(ObExecContext& ctx, ObMiniTask& task, ObTaskInfo* task_info, int64_t& ap_task_cnt,
      ObMiniTaskRetryInfo& retry_info);
  int fill_lookup_task_op_input(ObExecContext& ctx, ObMiniTask& task, ObTaskInfo* task_info,
      const ObPhyOperator& root_op, const bool retry_execution);
  int fill_lookup_task_op_input(
      ObExecContext& ctx, ObTaskInfo* task_info, const ObOpSpec& root_spec, const bool retry_execution);
  int retry_overflow_task(ObExecContext& ctx, common::ObIArray<ObMiniTask>& task_list,
      common::ObIArray<ObTaskInfo*>& task_info_list, ObMiniTaskRetryInfo& retry_info, ObMiniTaskResult& task_result);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_ */
