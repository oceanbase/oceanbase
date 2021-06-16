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

#ifndef OBDEV_SRC_SQL_OB_QUERY_EXEC_CTX_MGR_H_
#define OBDEV_SRC_SQL_OB_QUERY_EXEC_CTX_MGR_H_
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_execution_id.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/obmp_base.h"

namespace oceanbase {
namespace sql {
class ObITaskExecCtx;
typedef common::LinkHashNode<ObExecutionID> TaskExecCtxHashNode;
typedef common::LinkHashValue<ObExecutionID> TaskExecCtxHashValue;

class ObQueryExecCtx {
  friend ObClassConstructor<ObQueryExecCtx>;
  friend ObClassAllocator<ObQueryExecCtx>;

public:
  static ObQueryExecCtx* alloc(ObSQLSessionInfo& session);
  static void free(ObQueryExecCtx* query_ctx);
  inline int64_t inc_ref_count()
  {
    return ATOMIC_AAF((uint64_t*)&ref_count_, 1);
  }
  inline int64_t dec_ref_count()
  {
    return ATOMIC_SAF((uint64_t*)&ref_count_, 1);
  }
  observer::ObMySQLResultSet& get_result_set()
  {
    return result_set_;
  }
  void set_cur_task_ctx(ObITaskExecCtx* cur_task_ctx)
  {
    cur_task_ctx_ = cur_task_ctx;
  }
  ObITaskExecCtx* get_cur_task_ctx()
  {
    return cur_task_ctx_;
  }
  lib::MemoryContext& get_mem_context()
  {
    return mem_context_;
  }
  void set_cached_schema_guard_info(ObTenantCachedSchemaGuardInfo& info)
  {
    cache_schema_info_ = &info;
  }
  ObTenantCachedSchemaGuardInfo* get_cached_schema_info()
  {
    return cache_schema_info_;
  }
  share::schema::ObSchemaGetterGuard* get_schema_guard()
  {
    return ((NULL == cache_schema_info_) ? NULL : &(cache_schema_info_->get_schema_guard()));
  }

private:
  ObQueryExecCtx(ObSQLSessionInfo& session, lib::MemoryContext& mem_context)
      : mem_context_(mem_context),
        result_set_(session, mem_context.get_arena_allocator()),
        ref_count_(0),
        cur_task_ctx_(nullptr),
        cache_schema_info_(NULL)
  {}
  ~ObQueryExecCtx()
  {}

private:
  lib::MemoryContext& mem_context_;
  observer::ObMySQLResultSet result_set_;
  volatile int64_t ref_count_;
  ObITaskExecCtx* cur_task_ctx_;
  ObTenantCachedSchemaGuardInfo* cache_schema_info_;
};

class ObITaskExecCtx {
public:
  ObITaskExecCtx() : ref_count_(0), ctx_lock_(), execution_id_(), query_exec_ctx_(nullptr)
  {}
  virtual ~ObITaskExecCtx()
  {
    if (query_exec_ctx_ != nullptr) {
      ObSQLSessionInfo& session = query_exec_ctx_->get_result_set().get_session();
      ObQueryExecCtx::free(query_exec_ctx_);
      if (OB_NOT_NULL(GCTX.session_mgr_)) {
        GCTX.session_mgr_->revert_session(&session);
      }
      query_exec_ctx_ = nullptr;
    }
  }
  inline int64_t inc_ref_count()
  {
    return ATOMIC_AAF((uint64_t*)&ref_count_, 1);
  }
  inline int64_t dec_ref_count()
  {
    return ATOMIC_SAF((uint64_t*)&ref_count_, 1);
  }
  common::ObSpinLock& get_ctx_lock()
  {
    return ctx_lock_;
  }
  void set_execution_id(const ObExecutionID& execution_id)
  {
    execution_id_ = execution_id;
  }
  const ObExecutionID& get_execution_id() const
  {
    return execution_id_;
  }
  ObQueryExecCtx* get_query_exec_ctx()
  {
    return query_exec_ctx_;
  }

protected:
  volatile int64_t ref_count_;
  common::ObSpinLock ctx_lock_;
  ObExecutionID execution_id_;
  ObQueryExecCtx* query_exec_ctx_;
};

class ObRemoteTaskCtx : public TaskExecCtxHashValue, public ObITaskExecCtx {
public:
  ObRemoteTaskCtx()
      : ObITaskExecCtx(),
        sql_ctx_(),
        retry_ctrl_(),
        mppacket_sender_(),
        mysql_request_(nullptr),
        is_exiting_(false),
        is_running_(false)
  {}
  ~ObRemoteTaskCtx()
  {}
  int init(ObQueryExecCtx& query_ctx);
  void clean_query_ctx();
  ObSqlCtx& get_sql_ctx()
  {
    return sql_ctx_;
  }
  observer::ObMPPacketSender& get_mppacket_sender()
  {
    return mppacket_sender_;
  }
  observer::ObRemotePlanDriver& get_remote_plan_driver()
  {
    return remote_plan_driver_;
  }
  observer::ObQueryRetryCtrl& get_retry_ctrl()
  {
    return retry_ctrl_;
  }
  void set_mysql_request(rpc::ObRequest* mysql_request)
  {
    mysql_request_ = mysql_request;
  }
  rpc::ObRequest* get_mysql_request()
  {
    return mysql_request_;
  }
  bool is_exiting() const
  {
    return is_exiting_;
  }
  void set_is_exiting(bool is_exiting)
  {
    is_exiting_ = is_exiting;
  }
  bool is_running() const
  {
    return is_running_;
  }
  void set_is_running(bool is_running)
  {
    is_running_ = is_running;
  }
  void set_runner_svr(const common::ObAddr& runner_svr)
  {
    runner_svr_ = runner_svr;
  }
  const common::ObAddr& get_runner_svr() const
  {
    return runner_svr_;
  }

private:
  ObSqlCtx sql_ctx_;
  observer::ObQueryRetryCtrl retry_ctrl_;
  observer::ObMPPacketSender mppacket_sender_;
  union {
    observer::ObRemotePlanDriver remote_plan_driver_;
  };
  rpc::ObRequest* mysql_request_;
  bool is_exiting_;
  bool is_running_;
  common::ObAddr runner_svr_;
};

class ObTaskExecCtxAlloc {
public:
  ObQueryExecCtx* alloc_value()
  {
    return NULL;
  }
  void free_value(ObRemoteTaskCtx* p);
  TaskExecCtxHashNode* alloc_node(ObRemoteTaskCtx* p)
  {
    UNUSED(p);
    return op_reclaim_alloc(TaskExecCtxHashNode);
  }
  void free_node(TaskExecCtxHashNode* node)
  {
    if (NULL != node) {
      op_reclaim_free(node);
      node = NULL;
    }
  }
};

static const int64_t SHRINK_THRESHOLD = 128;
typedef common::ObLinkHashMap<ObExecutionID, ObRemoteTaskCtx, ObTaskExecCtxAlloc, common::RefHandle, SHRINK_THRESHOLD>
    TaskExecCtxMap;

class GCQueryExecCtx {
public:
  GCQueryExecCtx()
  {}
  ~GCQueryExecCtx()
  {}
  bool operator()(const ObExecutionID& execution_id, ObRemoteTaskCtx* task_ctx);
};

class ObQueryExecCtxMgr : public lib::TGRunnable {
public:
  ObQueryExecCtxMgr() : tg_id_(-1), task_exec_ctx_map_()
  {}
  virtual ~ObQueryExecCtxMgr()
  {}
  int init();
  void destroy();
  int create_task_exec_ctx(const ObExecutionID& execution_id, ObQueryExecCtx& query_ctx, ObRemoteTaskCtx*& task_ctx);
  int get_task_exec_ctx(const ObExecutionID& execution_id, ObRemoteTaskCtx*& task_ctx);
  void revert_task_exec_ctx(ObRemoteTaskCtx* task_ctx);
  int drop_task_exec_ctx(const ObExecutionID& execution_id);

public:
  void run1();

private:
  static const int64_t GC_QUERY_CTX_INTERVAL_US = 100 * 1000;  // 100ms
  int tg_id_;
  TaskExecCtxMap task_exec_ctx_map_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_OB_QUERY_EXEC_CTX_MGR_H_ */
