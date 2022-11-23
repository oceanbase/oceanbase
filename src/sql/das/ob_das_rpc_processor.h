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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_
#include "ob_das_extra_data.h"
#include "sql/das/ob_das_task.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/das/ob_das_rpc_proxy.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_factory.h"
#include "sql/engine/ob_des_exec_context.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace sql
{
typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<obrpc::OB_DAS_SYNC_ACCESS> > ObDASSyncRpcProcessor;
typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<obrpc::OB_DAS_SYNC_FETCH_RESULT> > ObDASSyncFetchResRpcProcessor;
typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<obrpc::OB_DAS_ASYNC_ERASE_RESULT> > ObDASAsyncEraseResRpcProcessor;

class ObDASSyncAccessP : public ObDASSyncRpcProcessor
{
public:
  ObDASSyncAccessP(const observer::ObGlobalContext &gctx)
    : das_factory_(CURRENT_CONTEXT->get_arena_allocator()),
      exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      frame_info_(CURRENT_CONTEXT->get_arena_allocator()),
      das_remote_info_()
  {
    set_preserve_recv_data();
  }

  virtual ~ObDASSyncAccessP() {}
  virtual int init();
  virtual int before_process();
  virtual int process();
  virtual int after_process(int error_code);
  virtual void cleanup() override;
  static ObDASTaskFactory *&get_das_factory()
  {
    RLOCAL(ObDASTaskFactory*, g_das_fatory);
    return g_das_fatory;
  }
private:
  ObDASTaskFactory das_factory_;
  ObDesExecContext exec_ctx_;
  ObExprFrameInfo frame_info_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  ObDASRemoteInfo das_remote_info_;
};

class ObDASSyncFetchP : public ObDASSyncFetchResRpcProcessor
{
public:
  ObDASSyncFetchP() {}
  ~ObDASSyncFetchP() {}
  virtual int process() override;
  virtual int after_process(int error_code);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASSyncFetchP);
};

class ObDASAsyncEraseP : public ObDASAsyncEraseResRpcProcessor
{
public:
  ObDASAsyncEraseP() {}
  ~ObDASAsyncEraseP() {}
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncEraseP);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_ */
