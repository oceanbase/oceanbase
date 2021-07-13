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

#ifndef OCEANBASE_SQL_EXECUTOR_RPC_IMPL_
#define OCEANBASE_SQL_EXECUTOR_RPC_IMPL_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_scanner.h"
#include "share/rpc/ob_batch_rpc.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_executor_rpc_proxy.h"

namespace oceanbase {
namespace obrpc {
class ObExecutorRpcProxy;
}

namespace sql {
class ObQueryRetryInfo;
class ObAPMiniTaskMgr;

/* The life cycle of MySteamHandler is the entire SQL statement,
 *  the same as ObResultSet, essentially a variable on the stack. reference:obmp_query.cpp */
template <obrpc::ObRpcPacketCode pcode>
class MyStreamHandle {
public:
  typedef typename obrpc::ObExecutorRpcProxy::SSHandle<pcode> MyHandle;
  typedef common::ObScanner MyResult;
  explicit MyStreamHandle(const char* label) : result_(label), rc_(common::OB_SUCCESS)
  {}
  virtual ~MyStreamHandle()
  {}
  void reset()
  {
    result_.reset();
    rc_ = common::OB_SUCCESS;
  }
  const common::ObAddr& get_dst_addr() const
  {
    return handle_.get_dst_addr();
  }
  MyHandle& get_handle()
  {
    return handle_;
  }
  MyResult* get_result()
  {
    MyResult* ret_result = NULL;
    if (!result_.is_inited()) {
      SQL_EXE_LOG(ERROR, "result_ is not inited");
    } else {
      ret_result = &result_;
    }
    return ret_result;
  }

  int reset_and_init_result()
  {
    int ret = common::OB_SUCCESS;
    result_.reset();
    if (!result_.is_inited() && OB_FAIL(result_.init())) {
      SQL_EXE_LOG(WARN, "fail to init result", K(ret));
    }
    return ret;
  }

  void set_result_code(int code)
  {
    rc_ = code;
  }
  int get_result_code()
  {
    return rc_;
  }

  void set_task_id(const ObTaskID& task_id)
  {
    task_id_ = task_id;
  }
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }

private:
  ObTaskID task_id_;
  MyHandle handle_;
  MyResult result_;
  int rc_;
};

template <obrpc::ObRpcPacketCode pcode>
class MySSHandle {
public:
  typedef typename obrpc::ObExecutorRpcProxy::SSHandle<pcode> MyHandle;
  typedef ObIntermResultItem MyResult;
  explicit MySSHandle(const char* label) : result_(label), rc_(common::OB_SUCCESS)
  {}
  virtual ~MySSHandle()
  {}
  void reset()
  {
    result_.reset();
    rc_ = common::OB_SUCCESS;
  }
  MyHandle& get_handle()
  {
    return handle_;
  }
  MyResult* get_result()
  {
    MyResult* ret_result = NULL;
    if (!result_.is_valid()) {
      SQL_EXE_LOG(ERROR, "result_ is not valid", K(result_));
    } else {
      ret_result = &result_;
    }
    return ret_result;
  }

  int reset_result()
  {
    int ret = common::OB_SUCCESS;
    result_.reset();
    if (OB_UNLIKELY(!result_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_EXE_LOG(ERROR, "after reset, result_ is invalid", K(ret), K(result_));
    }
    return ret;
  }

  void set_result_code(int code)
  {
    rc_ = code;
  }
  int get_result_code()
  {
    return rc_;
  }

private:
  MyHandle handle_;
  MyResult result_;
  int rc_;
};

typedef MyStreamHandle<obrpc::OB_REMOTE_EXECUTE> RemoteStreamHandle;
typedef MyStreamHandle<obrpc::OB_REMOTE_SYNC_EXECUTE> RemoteStreamHandleV2;
typedef MyStreamHandle<obrpc::OB_TASK_FETCH_RESULT> FetchResultStreamHandle;
typedef MySSHandle<obrpc::OB_TASK_FETCH_INTERM_RESULT> FetchIntermResultStreamHandle;

class RemoteExecuteStreamHandle {
public:
  RemoteExecuteStreamHandle(const char* label)
      : use_remote_protocol_v2_(false), sync_stream_handle_(label), sync_stream_handle_v2_(label)
  {}
  ~RemoteExecuteStreamHandle() = default;
  void set_use_remote_protocol_v2()
  {
    use_remote_protocol_v2_ = true;
  }
  void reset()
  {
    if (use_remote_protocol_v2_) {
      sync_stream_handle_v2_.reset();
    } else {
      sync_stream_handle_.reset();
    }
  }
  const common::ObAddr& get_dst_addr() const
  {
    const common::ObAddr* dst_addr = nullptr;
    if (use_remote_protocol_v2_) {
      dst_addr = &sync_stream_handle_v2_.get_dst_addr();
    } else {
      dst_addr = &sync_stream_handle_.get_dst_addr();
    }
    return *dst_addr;
  }

  int reset_and_init_result()
  {
    int ret = common::OB_SUCCESS;
    if (use_remote_protocol_v2_) {
      ret = sync_stream_handle_v2_.reset_and_init_result();
    } else {
      ret = sync_stream_handle_.reset_and_init_result();
    }
    return ret;
  }

  void set_result_code(int code)
  {
    if (use_remote_protocol_v2_) {
      sync_stream_handle_v2_.set_result_code(code);
    } else {
      sync_stream_handle_.set_result_code(code);
    }
  }
  int get_result_code()
  {
    int ret = common::OB_SUCCESS;
    if (use_remote_protocol_v2_) {
      ret = sync_stream_handle_v2_.get_result_code();
    } else {
      ret = sync_stream_handle_.get_result_code();
    }
    return ret;
  }

  void set_task_id(const ObTaskID& task_id)
  {
    if (use_remote_protocol_v2_) {
      sync_stream_handle_v2_.set_task_id(task_id);
    } else {
      sync_stream_handle_.set_task_id(task_id);
    }
  }
  const ObTaskID& get_task_id() const
  {
    const ObTaskID* task_id = nullptr;
    if (use_remote_protocol_v2_) {
      task_id = &(sync_stream_handle_v2_.get_task_id());
    } else {
      task_id = &(sync_stream_handle_.get_task_id());
    }
    return *task_id;
  }
  common::ObScanner* get_result()
  {
    common::ObScanner* result = nullptr;
    if (use_remote_protocol_v2_) {
      result = sync_stream_handle_v2_.get_result();
    } else {
      result = sync_stream_handle_.get_result();
    }
    return result;
  }
  bool has_more()
  {
    bool bret = false;
    if (use_remote_protocol_v2_) {
      bret = sync_stream_handle_v2_.get_handle().has_more();
    } else {
      bret = sync_stream_handle_.get_handle().has_more();
    }
    return bret;
  }
  int abort()
  {
    int ret = common::OB_SUCCESS;
    if (use_remote_protocol_v2_) {
      ret = sync_stream_handle_v2_.get_handle().abort();
    } else {
      ret = sync_stream_handle_.get_handle().abort();
    }
    return ret;
  }
  int get_more(common::ObScanner& result)
  {
    int ret = common::OB_SUCCESS;
    if (use_remote_protocol_v2_) {
      ret = sync_stream_handle_v2_.get_handle().get_more(result);
    } else {
      ret = sync_stream_handle_.get_handle().get_more(result);
    }
    return ret;
  }
  RemoteStreamHandle& get_remote_stream_handle()
  {
    return sync_stream_handle_;
  }
  RemoteStreamHandleV2& get_remote_stream_handle_v2()
  {
    return sync_stream_handle_v2_;
  }

private:
  bool use_remote_protocol_v2_;
  RemoteStreamHandle sync_stream_handle_;
  RemoteStreamHandleV2 sync_stream_handle_v2_;
};

class ObExecutorRpcCtx {
public:
  static const uint64_t INVALID_CLUSTER_VERSION = 0;

public:
  ObExecutorRpcCtx(uint64_t rpc_tenant_id, int64_t timeout_timestamp, uint64_t min_cluster_version,
      ObQueryRetryInfo* retry_info, ObSQLSessionInfo* session, bool is_plain_select,
      ObAPMiniTaskMgr* ap_mini_task_mgr = NULL)
      : rpc_tenant_id_(rpc_tenant_id),
        timeout_timestamp_(timeout_timestamp),
        min_cluster_version_(min_cluster_version),
        retry_info_(retry_info),
        session_(session),
        is_plain_select_(is_plain_select),
        ap_mini_task_mgr_(ap_mini_task_mgr)
  {}
  ~ObExecutorRpcCtx()
  {}

  uint64_t get_rpc_tenant_id() const
  {
    return rpc_tenant_id_;
  }
  inline int64_t get_timeout_timestamp() const
  {
    return timeout_timestamp_;
  }
  // equal to INVALID_CLUSTER_VERSION, means this description is serialized from the old observer on the remote
  inline bool min_cluster_version_is_valid() const
  {
    return INVALID_CLUSTER_VERSION != min_cluster_version_;
  }
  inline ObAPMiniTaskMgr* get_ap_mini_task_mgr()
  {
    return ap_mini_task_mgr_;
  }
  inline uint64_t get_min_cluster_version() const
  {
    return min_cluster_version_;
  }
  inline const ObQueryRetryInfo* get_retry_info() const
  {
    return retry_info_;
  }
  inline ObQueryRetryInfo* get_retry_info_for_update() const
  {
    return retry_info_;
  }
  bool is_retry_for_rpc_timeout() const
  {
    return is_plain_select_;
  }
  int check_status() const;
  TO_STRING_KV(K_(rpc_tenant_id), K_(timeout_timestamp), K_(min_cluster_version), K_(retry_info), K_(is_plain_select));

private:
  uint64_t rpc_tenant_id_;
  int64_t timeout_timestamp_;
  uint64_t min_cluster_version_;
  // retry_info_ == NULL means that this rpc does not need to give feedback to the retry module
  ObQueryRetryInfo* retry_info_;
  const ObSQLSessionInfo* session_;
  bool is_plain_select_;  // stmt_type == T_SELECT && not select...for update
  ObAPMiniTaskMgr* ap_mini_task_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExecutorRpcCtx);
};

class ObDistributedSchedulerManager;
class ObExecutorPingRpcCtx {
public:
  ObExecutorPingRpcCtx(uint64_t rpc_tenant_id, int64_t wait_timeout, ObDistributedSchedulerManager* dist_task_mgr,
      ObAPMiniTaskMgr* mini_task_mgr)
      : rpc_tenant_id_(rpc_tenant_id),
        wait_timeout_(wait_timeout),
        dist_task_mgr_(dist_task_mgr),
        mini_task_mgr_(mini_task_mgr)
  {}
  ~ObExecutorPingRpcCtx()
  {}
  uint64_t get_rpc_tenant_id() const
  {
    return rpc_tenant_id_;
  }
  int64_t get_wait_timeout() const
  {
    return wait_timeout_;
  }
  ObDistributedSchedulerManager* get_dist_task_mgr()
  {
    return dist_task_mgr_;
  }
  ObAPMiniTaskMgr* get_mini_task_mgr()
  {
    return mini_task_mgr_;
  }

private:
  uint64_t rpc_tenant_id_;
  int64_t wait_timeout_;
  ObDistributedSchedulerManager* dist_task_mgr_;
  ObAPMiniTaskMgr* mini_task_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExecutorPingRpcCtx);
};

#define OB_SQL_REMOTE_TASK_TYPE 1
#define OB_SQL_REMOTE_RESULT_TYPE 2

// All calls to rpc must use the to function to support concurrent calls
class ObExecutorRpcImpl {
public:
  ObExecutorRpcImpl() : proxy_(NULL), batch_rpc_(nullptr)
  {}
  virtual ~ObExecutorRpcImpl()
  {}
  /*
   * set rpc proxy
   * */
  int init(obrpc::ObExecutorRpcProxy* rpc_proxy, obrpc::ObBatchRpc* batch_rpc);

  /*
   * Submit an asynchronous task execution request,
   * and receive the result data driven by the OB_TASK_NOTIFY_FETCH message
   */
  virtual int task_submit(
      ObExecutorRpcCtx& rpc_ctx, ObTask& task, const common::ObAddr& svr, const TransResult* trans_result) const;

  /*
   * Send a task and block and wait until the peer returns to the execution state.
   * Save the execution handle in the handler, and then receive data through the handler
   * */
  virtual int task_execute(ObExecutorRpcCtx& rpc_ctx, ObTask& task, const common::ObAddr& svr,
      RemoteExecuteStreamHandle& handler, bool& has_sent_task, bool& has_transfer_err);
  virtual int task_execute_v2(ObExecutorRpcCtx& rpc_ctx, ObRemoteTask& task, const common::ObAddr& svr,
      RemoteExecuteStreamHandle& handler, bool& has_sent_task, bool& has_transfer_err);
  virtual int remote_task_submit(
      ObExecutorRpcCtx& rpc_ctx, ObRemoteTask& task, const common::ObAddr& svr, bool& has_sent_task);
  virtual int remote_post_result_async(
      ObExecutorRpcCtx& rpc_ctx, ObRemoteResult& remote_result, const common::ObAddr& svr, bool& has_sent_result);
  int remote_task_batch_submit(const uint64_t tenant_id, const common::ObAddr& server, const int64_t cluster_id,
      const ObRemoteTask& task, bool& has_sent_task);
  int remote_batch_post_result(const uint64_t tenant_id, const common::ObAddr& server, const int64_t cluster_id,
      const ObRemoteResult& result, bool& has_sent_result);
  virtual int mini_task_execute(ObExecutorRpcCtx& rpc_ctx, ObMiniTask& task, ObMiniTaskResult& result);
  virtual int mini_task_submit(ObExecutorRpcCtx& rpc_ctx, ObMiniTask& task);
  virtual int ping_sql_task(ObExecutorPingRpcCtx& ping_rpc_ctx, ObPingSqlTask& task);
  /*
   * Send a command to kill a task and block waiting for the peer to return to the execution state
   * */
  virtual int task_kill(ObExecutorRpcCtx& rpc_ctx, const ObTaskID& task_id, const common::ObAddr& svr);
  /*
   * Task is executed on the Worker side,
   * and the Scheduler is notified to start the Task to read the result
   * */
  virtual int task_complete(ObExecutorRpcCtx& rpc_ctx, ObTaskCompleteEvent& task_event, const common::ObAddr& svr);

  /*
   * Send the execution result of a task without waiting for return
   * */
  virtual int task_notify_fetch(ObExecutorRpcCtx& rpc_ctx, ObTaskEvent& task_event, const common::ObAddr& svr);
  /*
   * Get all the scanners of the intermediate result of a task,
   * block and wait until all the scanners return
   * */
  virtual int task_fetch_result(ObExecutorRpcCtx& rpc_ctx, const ObSliceID& ob_slice_id, const common::ObAddr& svr,
      FetchResultStreamHandle& handler);
  /*
   * Get all interm result items of the intermediate result of a task,
   * block and wait until all items are returned
   * */
  virtual int task_fetch_interm_result(ObExecutorRpcCtx& rpc_ctx, const ObSliceID& ob_slice_id,
      const common::ObAddr& svr, FetchIntermResultStreamHandle& handler);

  /*
   * Send a command to remove the intermediate result corresponding to an ObSliceID and
   * block waiting for the peer to return to the execution state
   * */
  virtual int close_result(ObExecutorRpcCtx& rpc_ctx, ObSliceID& ob_slice_id, const common::ObAddr& svr);

  // Fetch interm result by slice_id and interm result item index,
  // return interm result item and total interm result item count of this slice.
  virtual int fetch_interm_result_item(ObExecutorRpcCtx& rpc_ctx, const common::ObAddr& dst,
      const ObSliceID& ob_slice_id, const int64_t fetch_index, ObFetchIntermResultItemRes& res);

  obrpc::ObExecutorRpcProxy* get_proxy()
  {
    return proxy_;
  }

private:
  void deal_with_rpc_timeout_err(ObExecutorRpcCtx& rpc_ctx, int& err, const common::ObAddr& dist_server) const;
  int get_sql_batch_req_type(int64_t execution_id) const;

private:
  /* functions */
  /* variables */
  obrpc::ObExecutorRpcProxy* proxy_;
  obrpc::ObBatchRpc* batch_rpc_;
  /* other */
  DISALLOW_COPY_AND_ASSIGN(ObExecutorRpcImpl);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_RPC_IMPL_ */
//// end of header file
