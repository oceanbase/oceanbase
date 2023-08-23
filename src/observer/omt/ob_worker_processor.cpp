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

#define USING_LOG_PREFIX SERVER_OMT

#include "ob_worker_processor.h"
#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/profile/ob_perf_event.h"  // SET_PERF_EVENT
#include "lib/profile/ob_trace_id_adaptor.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_translator.h"
#include "rpc/frame/ob_req_processor.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_th_worker.h"
#include "lib/utility/ob_hang_fatal_error.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;

ObWorkerProcessor::ObWorkerProcessor(
    ObReqTranslator &xlator,
    const common::ObAddr &myaddr)
    : translator_(xlator), myaddr_(myaddr)
{}

void ObWorkerProcessor::th_created()
{
  translator_.th_init();
}

void ObWorkerProcessor::th_destroy()
{
  translator_.th_destroy();
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_WORKER_PROCESS_REQUEST)
#endif

OB_NOINLINE int ObWorkerProcessor::process_err_test()
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = EN_WORKER_PROCESS_REQUEST;
  LOG_DEBUG("process err_test", K(ret));
#endif
  return ret;
}

inline int ObWorkerProcessor::process_one(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  ObReqProcessor *processor = NULL;

  if (OB_FAIL(process_err_test())) {
    LOG_WARN("ignore request with err_test", K(ret));
  } else if (OB_FAIL(translator_.translate(req, processor))) {
    LOG_WARN("translate request fail", K(ret));
    on_translate_fail(&req, ret);
  } else if (OB_ISNULL(processor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected condition", K(ret));
  } else {
    NG_TRACE(before_processor_run);
    req.on_process_begin();
    req.set_trace_point(ObRequest::OB_EASY_REQUEST_WORKER_PROCESSOR_RUN);
    if (OB_FAIL(processor->run())) {
      LOG_WARN("process request fail", K(ret));
    }
    translator_.release(processor);
  }

  return ret;
}

int ObWorkerProcessor::process(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;

  if (THE_TRACE != nullptr) {
    THE_TRACE->reset();
  }
  OB_ATOMIC_EVENT_RESET_RECORDER();
  PERF_RESET_RECORDER();
  const bool enable_trace_log = lib::is_trace_log_enabled();
  const int64_t q_time = THIS_THWORKER.get_query_start_time() - req.get_receive_timestamp();
  NG_TRACE_EXT(process_begin,
               OB_ID(in_queue_time), q_time,
               OB_ID(receive_ts), req.get_receive_timestamp(),
               OB_ID(enqueue_ts), req.get_enqueue_timestamp());
  ObRequest::Type req_type = req.get_type(); // bugfix note: must be obtained in advance
  if (ObRequest::OB_RPC == req_type) {
    // internal RPC request
    const obrpc::ObRpcPacket &packet
        = static_cast<const obrpc::ObRpcPacket&>(req.get_packet());
    NG_TRACE_EXT(start_rpc, OB_ID(addr), RPC_REQ_OP.get_peer(&req), OB_ID(pcode), packet.get_pcode());
    ObCurTraceId::set(req.generate_trace_id(myaddr_));

#ifdef ERRSIM
    THIS_WORKER.set_module_type(packet.get_module_type());
#endif

    // Do not set thread local log level while log level upgrading (OB_LOGGER.is_info_as_wdiag)
    if (OB_LOGGER.is_info_as_wdiag()) {
      ObThreadLogLevelUtils::clear();
    } else {
      if (enable_trace_log && OB_LOG_LEVEL_NONE != packet.get_log_level()) {
        ObThreadLogLevelUtils::init(packet.get_log_level());
      }
    }
  } else if (ObRequest::OB_MYSQL == req_type) {
    NG_TRACE_EXT(start_sql, OB_ID(addr), SQL_REQ_OP.get_peer(&req));
    // mysql command request
    ObCurTraceId::set(req.generate_trace_id(myaddr_));
  }
  // record trace id
  ObTraceIdAdaptor trace_id_adaptor;
  trace_id_adaptor.set(ObCurTraceId::get());
  NG_TRACE_EXT(query_begin, OB_ID(trace_id), trace_id_adaptor);
  //NG_TRACE(query_begin);

  // setup and init warning buffer
  // For general SQL processing, the rpc processing function entry uses set_tsi_warning_buffer to set the session
  // The warning buffer is set to the thread part; but for the handler of the task remote execution, because
  // The error message needs to be used when serializing result_code until after the process() function
  // Therefore, the warning buffer member of the session cannot be used. Therefore, one is set by default.
  ob_setup_default_tsi_warning_buffer();
  ob_reset_tsi_warning_buffer();

  //Set the chid of the source package to the thread
  // int64_t st = ::oceanbase::common::ObTimeUtility::current_time();
  // PROFILE_LOG(DEBUG, HANDLE_PACKET_START_TIME PCODE, st, packet->get_pcode());
  // go!
  try {
    in_try_stmt = true;
    if (OB_FAIL(process_one(req))) {
      LOG_WARN("process request fail", K(ret));
    }
    in_try_stmt = false;
  } catch (OB_BASE_EXCEPTION &except) {
    _LOG_ERROR("Exception caught!!! errno = %d, exception info = %s", except.get_errno(), except.what());
    in_try_stmt = false;
  }

  // cleanup
  ObCurTraceId::reset();
  if (enable_trace_log) {
    ObThreadLogLevelUtils::clear();
  }
  PERF_GATHER_DATA();
  //LOG_INFO("yzf debug", "atomic_op", ATOMIC_EVENT_RECORDER);
  OB_ATOMIC_EVENT_GATHER_DATA();
  return ret;
}
