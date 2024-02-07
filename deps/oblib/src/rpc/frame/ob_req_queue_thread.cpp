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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_req_queue_thread.h"

#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_profile_log.h"
#include "lib/profile/ob_profile_type.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/ob_define.h"
#include "lib/worker.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;
using namespace oceanbase::lib;

ObReqQueue::ObReqQueue(int capacity)
    : wait_finish_(true),
      push_worker_count_(0),
      queue_(),
      qhandler_(NULL),
      host_()
{
  queue_.set_limit(capacity);
}

ObReqQueue::~ObReqQueue()
{
  LOG_INFO("begin to destroy queue", K(queue_.size()));
}

int ObReqQueue::init(const int64_t tenant_id)
{
  UNUSED(tenant_id);
  return OB_SUCCESS;
}

void ObReqQueue::set_qhandler(ObiReqQHandler *qhandler)
{
  if (OB_ISNULL(qhandler)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(qhandler));
  }
  qhandler_ = qhandler;
}

bool ObReqQueue::push(ObRequest *req, int max_queue_len, bool block)
{
  bool bret = true;
  if (max_queue_len > 0 && queue_.size() >= max_queue_len) {
    if (!block) {
      bret =  false;
    }
  }

  if (!OB_ISNULL(req)) {
    req->set_enqueue_timestamp(ObTimeUtility::current_time());
  }

  if (bret) {
    bret = OB_LIKELY(OB_SUCCESS == queue_.push(req, 0));
  }
  return bret;
}

oceanbase::rpc::ObRequest *ObReqQueue::pop()
{
  ObLink *task = NULL;
  int64_t timeout = 0;
  ObRequest *req = NULL;
  if (queue_.size() > 0 && OB_LIKELY(OB_SUCCESS == queue_.pop(task, timeout)) && OB_NOT_NULL(task)) {
    req = static_cast<ObRequest *>(task);
  }
  return req;
}

void ObReqQueue::set_host(const ObAddr &host)
{
  host_ = host;
}

int ObReqQueue::process_task(ObLink *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_ISNULL(qhandler_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("queue pop NULL task", K(task), K(ret), K(qhandler_));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_ROOT_CONTEXT)
      .set_properties(USE_TL_PAGE_OPTIONAL);
    CREATE_WITH_TEMP_CONTEXT(param) {
      ObRequest *req = static_cast<ObRequest *>(task);

      // init trace id
      if (ObRequest::OB_RPC == req->get_type()) {
        // internal RPC request
        const obrpc::ObRpcPacket &packet
            = static_cast<const obrpc::ObRpcPacket&>(req->get_packet());
        const uint64_t *trace_id = packet.get_trace_id();
        if (0 == trace_id[0]) {
          // new trace id
          ObCurTraceId::init(host_);
        } else {
          ObCurTraceId::set(trace_id);
        }

#ifdef ERRSIM
        THIS_WORKER.set_module_type(packet.get_module_type());
#endif
        // Do not set thread local log level while log level upgrading (OB_LOGGER.is_info_as_wdiag)
        if (OB_LOGGER.is_info_as_wdiag()) {
          ObThreadLogLevelUtils::clear();
        } else {
          int8_t log_level = packet.get_log_level();
          if (OB_LOG_LEVEL_NONE != log_level) {
            ObThreadLogLevelUtils::init(log_level);
          }
        }
      } else {
        // mysql command request
        ObCurTraceId::init(host_);
      }
      //Set the chid of the source package to the thread
      // int64_t st = ::oceanbase::common::ObTimeUtility::current_time();
      // PROFILE_LOG(DEBUG, HANDLE_PACKET_START_TIME PCODE, st, packet->get_pcode());

      // setup and init warning buffer
      // For general SQL processing, the rpc processing function entry uses set_tsi_warning_buffer to set the session warning buffer
      // The warning buffer is set to the thread part; but for the handler of the task remote execution, because
      // After the error message reaches the process() function, it needs to be used when serializing result_code
      // Therefore, the warning buffer member of the session cannot be used. Therefore, one is set by default.
      ob_setup_default_tsi_warning_buffer();
      ob_reset_tsi_warning_buffer();
      // go!
      qhandler_->handlePacketQueue(req, nullptr);
      // int64_t ed = ::oceanbase::common::ObTimeUtility::current_time();
      // PROFILE_LOG(DEBUG, HANDLE_PACKET_END_TIME PCODE, ed, packet->get_pcode());
      ObCurTraceId::reset();
      ObThreadLogLevelUtils::clear();
    }
  }

  return ret;
}

void ObReqQueue::loop()
{
  int ret = OB_SUCCESS;
  int64_t timeout = 3000 * 1000;
  ObLink *task = NULL;
  if (OB_ISNULL(qhandler_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(qhandler_));
  } else if (OB_FAIL(qhandler_->onThreadCreated(nullptr))) {
    LOG_ERROR("do thread created fail, thread will exit", K(ret));
  } else {
    // The main loop threads process tasks.
    while (!Thread::current().has_set_stop()) {
      if (OB_FAIL(queue_.pop(task, timeout))) {
        LOG_DEBUG("queue pop task fail", K(&queue_));
      } else if (NULL != task) {
        process_task(task);  // ignore return code.
      } else {
        // unexpected
        LOG_ERROR("queue pop successfully but task is NULL");
      }
    }  // main loop

    if (!wait_finish_) {
      LOG_INFO("exiting queue thread without wait finish", K(queue_.size()));
    } else {
      while(get_push_worker_count() != 0); // wait to push finish
      LOG_INFO("exiting queue thread and wait remain finish", K(queue_.size()));
      // Process remains if we should wait until all task has been
      // processed before exiting this thread. Previous return code
      // isn't significant, we just ignore it to make progress. When
      // queue pop a normal task we process it until pop fails.
      ret = OB_SUCCESS;
      while (queue_.size() > 0 && OB_SUCC(ret)) {
        if (OB_FAIL(queue_.pop(task, timeout))) {
          LOG_DEBUG("queue pop task fail", K(&queue_));
          if(OB_ENTRY_NOT_EXIST == ret) {
            // lightyqueue may return OB_ENTRY_NOT_EXIST when tasks existing
            ret = OB_SUCCESS;
          }
        } else if (NULL != task) {
          process_task(task);  // ignore return code.
        } else {
          // unexpected
          LOG_ERROR("queue pop successfully but task is NULL");
        }
      }
    }

    // No matter error occurred before or not.
    if (OB_FAIL(qhandler_->onThreadDestroy(nullptr))) {
      OB_LOG(ERROR, "handle thread destroy fail", K(ret));
    }
  }
}
