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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_executor_rpc_processor.h"

#include "sql/dtl/ob_dtl_interm_result_manager.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace observer;
using namespace storage;
using namespace transaction;
namespace sql
{

ObWorkerSessionGuard::ObWorkerSessionGuard(ObSQLSessionInfo *session)
{
  THIS_WORKER.set_session(session);
  if (nullptr != session) {
    session->set_thread_id(GETTID());
  }
}

ObWorkerSessionGuard::~ObWorkerSessionGuard()
{
  THIS_WORKER.set_session(NULL);
}

int ObRpcEraseIntermResultP::preprocess_arg()
{
  return OB_SUCCESS;
}

int ObRpcEraseIntermResultP::process()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive erase interm result request", K(arg_));
  dtl::ObDTLIntermResultKey dtl_int_key;
  ObIArray<uint64_t> &interm_result_ids = arg_.interm_result_ids_;
  for (int64_t i = 0; OB_SUCC(ret) && i < interm_result_ids.count(); ++i) {
    dtl_int_key.channel_id_ = interm_result_ids.at(i);
    if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager*)->erase_interm_result_info(dtl_int_key))) {
      LOG_WARN("failed to erase interm result info in manager.", K(ret));
    }
  }
  return ret;
}

} /* sql */
} /* oceanbase */
