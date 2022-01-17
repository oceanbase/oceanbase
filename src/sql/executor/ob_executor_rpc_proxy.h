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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_task_event.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace sql {
class ObMiniTask;
class ObMiniTaskResult;
}  // namespace sql

namespace sql {

struct ObBKGDDistExecuteArg {
  OB_UNIS_VERSION(1);

public:
  ObBKGDDistExecuteArg() : tenant_id_(OB_INVALID_ID), scheduler_id_(0)
  {}
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(scheduler_id), K_(return_addr), K(serialized_task_.length()));

  bool is_valid() const
  {
    return OB_INVALID_ID != tenant_id_ && task_id_.is_valid() && scheduler_id_ > 0 && return_addr_.is_valid() &&
           !serialized_task_.empty();
  }

  uint64_t tenant_id_;
  ObTaskID task_id_;
  uint64_t scheduler_id_;
  common::ObAddr return_addr_;
  common::ObString serialized_task_;
};

struct ObBKGDTaskCompleteArg {
  OB_UNIS_VERSION(1);

public:
  ObBKGDTaskCompleteArg() : scheduler_id_(0), return_code_(common::OB_SUCCESS)
  {}
  TO_STRING_KV(K_(task_id), K_(scheduler_id), K_(return_code), K_(event));

  ObTaskID task_id_;
  uint64_t scheduler_id_;
  int return_code_;
  ObTaskCompleteEvent event_;
};

struct ObFetchIntermResultItemArg {
  OB_UNIS_VERSION(1);

public:
  ObFetchIntermResultItemArg() : index_(OB_INVALID_INDEX)
  {}

  ObSliceID slice_id_;
  int64_t index_;

  TO_STRING_KV(K_(slice_id), K_(index));
};

struct ObFetchIntermResultItemRes {
  OB_UNIS_VERSION(1);

public:
  ObFetchIntermResultItemRes() : total_item_cnt_(-1)
  {}

  ObIntermResultItem result_item_;
  int64_t total_item_cnt_;

  TO_STRING_KV(K_(result_item), K_(total_item_cnt));
};

}  // namespace sql

namespace obrpc {
class ObExecutorRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObExecutorRpcProxy);

  RPC_SS(@PR5 task_execute, obrpc::OB_REMOTE_EXECUTE, (sql::ObTask), common::ObScanner);
  RPC_SS(@PR5 remote_task_execute, obrpc::OB_REMOTE_SYNC_EXECUTE, (sql::ObRemoteTask), common::ObScanner);
  RPC_SS(@PR5 task_fetch_result, obrpc::OB_TASK_FETCH_RESULT, (sql::ObSliceID), common::ObScanner);
  RPC_SS(@PR5 task_fetch_interm_result, obrpc::OB_TASK_FETCH_INTERM_RESULT, (sql::ObSliceID), sql::ObIntermResultItem);
  RPC_S(@PR4 fetch_interm_result_item, obrpc::OB_FETCH_INTERM_RESULT_ITEM, (sql::ObFetchIntermResultItemArg),
      sql::ObFetchIntermResultItemRes);
  // task_submit not used after cluser version upgrade to 1.3.0
  // Remain this for compatibility
  RPC_S(@PR5 task_submit, obrpc::OB_DIST_EXECUTE, (sql::ObTask));
  RPC_S(@PR5 task_kill, obrpc::OB_TASK_KILL, (sql::ObTaskID));
  RPC_S(@PR5 task_notify_fetch, obrpc::OB_TASK_NOTIFY_FETCH, (sql::ObTaskEvent));
  RPC_S(@PR5 task_complete, obrpc::OB_TASK_COMPLETE, (sql::ObTaskCompleteEvent));
  RPC_S(@PR5 mini_task_execute, obrpc::OB_MINI_TASK_EXECUTE, (sql::ObMiniTask), sql::ObMiniTaskResult);
  RPC_S(@PR5 bkgd_task_submit, obrpc::OB_BKGD_DIST_EXECUTE, (sql::ObBKGDDistExecuteArg));
  RPC_S(@PR5 bkgd_task_complete, obrpc::OB_BKGD_TASK_COMPLETE, (sql::ObBKGDTaskCompleteArg));
  RPC_S(
      @PR5 check_build_index_task_exist, OB_CHECK_BUILD_INDEX_TASK_EXIST, (ObCheckBuildIndexTaskExistArg), obrpc::Bool);
  RPC_AP(@PR5 close_result, obrpc::OB_CLOSE_RESULT, (sql::ObSliceID));
  // ap_task_submit async process of task submit.
  // The task complete event would process in IO thread.
  RPC_AP(@PR5 ap_task_submit, obrpc::OB_AP_DIST_EXECUTE, (sql::ObTask), sql::ObTaskCompleteEvent);
  RPC_AP(@PR5 ap_mini_task_submit, obrpc::OB_AP_MINI_DIST_EXECUTE, (sql::ObMiniTask), sql::ObMiniTaskResult);
  RPC_AP(@PR5 ap_ping_sql_task, obrpc::OB_AP_PING_SQL_TASK, (sql::ObPingSqlTask), sql::ObPingSqlTaskResult);
  RPC_AP(@PR5 remote_task_submit, obrpc::OB_REMOTE_ASYNC_EXECUTE, (sql::ObRemoteTask));
  RPC_AP(@PR5 remote_post_result, obrpc::OB_REMOTE_POST_RESULT, (sql::ObRemoteResult));
};
}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_ */
//// end of header file
