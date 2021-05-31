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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_srv_xlator.h"

#include "share/ob_tenant_mgr.h"
#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/rpc/ob_batch_processor.h"
#include "share/rpc/ob_blacklist_req_processor.h"
#include "share/rpc/ob_blacklist_resp_processor.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/engine/px/ob_px_rpc_processor.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "sql/ob_sql_task.h"
#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "storage/transaction/ob_gts_rpc.h"
#include "storage/transaction/ob_dup_table_rpc.h"
#include "storage/transaction/ob_gts_response_handler.h"
#include "storage/transaction/ob_weak_read_service_rpc_define.h"  // weak_read_service
#include "election/ob_election_rpc.h"
#include "clog/ob_log_rpc_processor.h"
#include "clog/ob_log_external_rpc.h"
#include "clog/ob_clog_sync_rpc.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_srv_task.h"
#include "observer/mysql/obmp_query.h"
#include "observer/mysql/obmp_ping.h"
#include "observer/mysql/obmp_quit.h"
#include "observer/mysql/obmp_connect.h"
#include "observer/mysql/obmp_init_db.h"
#include "observer/mysql/obmp_default.h"
#include "observer/mysql/obmp_change_user.h"
#include "observer/mysql/obmp_error.h"
#include "observer/mysql/obmp_statistic.h"
#include "observer/mysql/obmp_stmt_prepare.h"
#include "observer/mysql/obmp_stmt_execute.h"
#include "observer/mysql/obmp_stmt_close.h"

using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::clog;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;

#define PROCESSOR_BEGIN(pcode) switch (pcode) {

#define PROCESSOR_END()     \
  default:                  \
    ret = OB_NOT_SUPPORTED; \
    }

#define NEW_MYSQL_PROCESSOR(ObMySQLP, ...)                        \
  do {                                                            \
    ObIAllocator* alloc = &THIS_WORKER.get_sql_arena_allocator(); \
    ObMySQLP* p = OB_NEWx(ObMySQLP, alloc, __VA_ARGS__);          \
    if (OB_ISNULL(p)) {                                           \
      ret = OB_ALLOCATE_MEMORY_FAILED;                            \
    } else if (OB_FAIL(p->init())) {                              \
      SERVER_LOG(ERROR, "Init " #ObMySQLP "fail", K(ret));        \
      worker_allocator_delete(p);                                 \
      p = NULL;                                                   \
    } else {                                                      \
      processor = p;                                              \
    }                                                             \
  } while (0)

#define MYSQL_PROCESSOR(ObMySQLP, ...)          \
  case ObMySQLP::COM: {                         \
    NEW_MYSQL_PROCESSOR(ObMySQLP, __VA_ARGS__); \
    break;                                      \
  }

void ObSrvRpcXlator::register_rpc_process_function(int pcode, RPCProcessFunc func)
{
  if (pcode >= MAX_PCODE || pcode < 0) {
    SERVER_LOG(ERROR, "(SHOULD NEVER HAPPEN) input pcode is out of range in server rpc xlator", K(pcode));
    ob_abort();
  } else if (funcs_[pcode] != nullptr) {
    SERVER_LOG(ERROR, "(SHOULD NEVER HAPPEN) duplicate pcode in server rpc xlator", K(pcode));
    ob_abort();
  } else {
    funcs_[pcode] = func;
  }
}

ObIAllocator& oceanbase::observer::get_sql_arena_allocator()
{
  return THIS_WORKER.get_sql_arena_allocator();
}

int ObSrvRpcXlator::translate(rpc::ObRequest& req, ObReqProcessor*& processor)
{
  int ret = OB_SUCCESS;
  processor = NULL;
  const ObRpcPacket& pkt = reinterpret_cast<const ObRpcPacket&>(req.get_packet());
  int pcode = pkt.get_pcode();

  if (OB_UNLIKELY(pcode < 0 || pcode >= MAX_PCODE || funcs_[pcode] == nullptr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support packet", K(pkt), K(ret));
  } else {
    ret = funcs_[pcode](gctx_, processor, session_handler_);
  }

  if (OB_SUCC(ret) && NULL == processor) {
    ret = OB_NOT_SUPPORTED;
  }

  if (!OB_SUCC(ret) && NULL != processor) {
    ob_delete(processor);
    processor = NULL;
  }

  return ret;
}

int ObSrvXlator::th_init()
{
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(rpc_xlator_.th_init())) {
    LOG_ERROR("init rpc translator for thread fail", K(ret));
  } else if (OB_FAIL(mysql_xlator_.th_init())) {
    LOG_ERROR("init mysql translator for thread fail", K(ret));
  }
  return ret;
}

int ObSrvXlator::th_destroy()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(rpc_xlator_.th_destroy())) {
    LOG_ERROR("destroy rpc translator for thread fail", K(ret));
  } else if (OB_FAIL(mysql_xlator_.th_destroy())) {
    LOG_ERROR("destroy mysql translator for thread fail", K(ret));
  }
  return ret;
}

int ObSrvMySQLXlator::translate(rpc::ObRequest& req, ObReqProcessor*& processor)
{
  int ret = OB_SUCCESS;
  processor = NULL;

  if (ObRequest::OB_MYSQL != req.get_type()) {
    LOG_ERROR("can't translate non-mysql request");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(req.get_ez_req()) || OB_ISNULL(req.get_ez_req()->ms) || OB_ISNULL(req.get_ez_req()->ms->c)) {
    LOG_ERROR("req member is null");
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (req.is_in_connected_phase()) {
      ret = get_mp_connect_processor(processor);
    } else {
      const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req.get_packet());
      switch (pkt.get_cmd()) {
        MYSQL_PROCESSOR(ObMPQuery, gctx_);
        MYSQL_PROCESSOR(ObMPQuit, gctx_);
        MYSQL_PROCESSOR(ObMPPing, gctx_);
        MYSQL_PROCESSOR(ObMPInitDB, gctx_);
        MYSQL_PROCESSOR(ObMPChangeUser, gctx_);
        MYSQL_PROCESSOR(ObMPStatistic, gctx_);
        MYSQL_PROCESSOR(ObMPStmtPrepare, gctx_);
        MYSQL_PROCESSOR(ObMPStmtExecute, gctx_);
        MYSQL_PROCESSOR(ObMPStmtClose, gctx_);
        case obmysql::OB_MYSQL_COM_FIELD_LIST: {
          ObSMConnection* conn = reinterpret_cast<ObSMConnection*>(req.get_ez_req()->ms->c->user_data);
          if (OB_ISNULL(conn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(conn), K(ret));
          } else if (conn->is_proxy_) {
            const char* sup_proxy_min_version = "1.7.6";
            uint64_t min_proxy_version = 0;
            if (OB_FAIL(ObClusterVersion::get_version(sup_proxy_min_version, min_proxy_version))) {
              LOG_WARN("failed to get version", K(ret));
            } else if (conn->proxy_version_ < min_proxy_version) {
              NEW_MYSQL_PROCESSOR(ObMPDefault, gctx_);
            } else {
              NEW_MYSQL_PROCESSOR(ObMPQuery, gctx_);
            }
          } else {
            NEW_MYSQL_PROCESSOR(ObMPQuery, gctx_);
          }
          break;
        }
        default:
          NEW_MYSQL_PROCESSOR(ObMPDefault, gctx_);
          break;
      }
      if (OB_SUCC(ret) && pkt.get_cmd() == obmysql::OB_MYSQL_COM_FIELD_LIST) {
        if (OB_ISNULL(static_cast<ObMPQuery*>(processor))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(static_cast<ObMPQuery*>(processor)));
        } else {
          static_cast<ObMPQuery*>(processor)->set_is_com_filed_list();
        }
      }
      if (OB_SUCC(ret) && pkt.get_cmd() == obmysql::OB_MYSQL_COM_STMT_PREPARE) {
        ObSMConnection* conn = reinterpret_cast<ObSMConnection*>(req.get_ez_req()->ms->c->user_data);
        if (OB_ISNULL(conn) || OB_ISNULL(dynamic_cast<ObMPStmtPrepare*>(processor))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(dynamic_cast<ObMPStmtPrepare*>(processor)));
        } else {
          uint64_t proxy_version = conn->is_proxy_ ? conn->proxy_version_ : 0;
          static_cast<ObMPStmtPrepare*>(processor)->set_proxy_version(proxy_version);
        }
      }
    }
    if (OB_FAIL(ret) && NULL != processor) {
      worker_allocator_delete(processor);
      processor = NULL;
    }
  }

  return ret;
}

ObReqProcessor* ObSrvXlator::get_processor(ObRequest& req)
{
  int ret = OB_SUCCESS;
  ObReqProcessor* processor = NULL;

  // 1. create processor by request type.
  if (req.get_discard_flag() == true) {
    ret = OB_WAITQUEUE_TIMEOUT;
  } else if (ObRequest::OB_MYSQL == req.get_type()) {
    ret = mysql_xlator_.translate(req, processor);
  } else if (ObRequest::OB_RPC == req.get_type()) {
    const obrpc::ObRpcPacket& pkt = reinterpret_cast<const obrpc::ObRpcPacket&>(req.get_packet());
    ret = rpc_xlator_.translate(req, processor);
    if (OB_SUCC(ret)) {
      THIS_WORKER.set_timeout_ts(req.get_receive_timestamp() + pkt.get_timeout());
    }
  } else if (ObRequest::OB_TASK == req.get_type() || ObRequest::OB_GTS_TASK == req.get_type() ||
             ObRequest::OB_SQL_TASK == req.get_type()) {
    processor = &static_cast<ObSrvTask&>(req).get_processor();
  } else {
    LOG_WARN("can't translate packet", "type", req.get_type());
    ret = OB_UNKNOWN_PACKET;
  }

  // destroy processor if alloc before but translate fail.
  if (OB_FAIL(ret) && NULL != processor) {
    worker_allocator_delete(processor);
    processor = NULL;
  }

  if (OB_ISNULL(processor)) {
    if (ObRequest::OB_RPC == req.get_type()) {
      processor = get_error_rpc_processor(ret);
    } else if (ObRequest::OB_MYSQL == req.get_type()) {
      processor = get_error_mysql_processor(ret);
      (static_cast<ObMPError*>(processor))->set_need_disconnect(true);
    }
  }

  return processor;
}

static RLOCAL(ByteBuf<sizeof(ObErrorP) + sizeof(ObMPError)>, epbuf);

int ObSrvXlator::release(ObReqProcessor* processor)
{
  int ret = OB_SUCCESS;
  if (NULL == processor) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(processor), K(ret));
  } else if (reinterpret_cast<char*>(processor) == epbuf) {
    processor->destroy();
    processor->~ObReqProcessor();
  } else {
    processor->destroy();

    // task request is allocated when new task composed, then delete
    // here.
    if (processor->has_task_mark()) {
      ObRequest* req = const_cast<ObRequest*>(processor->get_ob_request());
      if (NULL == req) {
        LOG_ERROR("task request is invalid!");
      } else if (ObRequest::OB_TASK == req->get_type()) {
        // Deal with sqltask memory release
        ob_delete(req);
        req = NULL;
      } else if (ObRequest::OB_GTS_TASK == req->get_type()) {
        // Deal with the memory release of the transaction task
        ObGtsResponseTaskFactory::free(static_cast<ObGtsResponseTask*>(req));
        // op_reclaim_free(req);
        req = NULL;
      } else if (ObRequest::OB_SQL_TASK == req->get_type()) {
        ObSqlTaskFactory::get_instance().free(static_cast<ObSqlTask*>(req));
        req = NULL;
      } else {
        // do nothing
      }
    } else {
      worker_allocator_delete(processor);
      processor = NULL;
    }
  }
  return ret;
}

ObReqProcessor* ObSrvXlator::get_error_rpc_processor(const int ret)
{
  ObErrorP* p = new (&epbuf[0]) ObErrorP(ret);
  return p;
}

ObReqProcessor* ObSrvXlator::get_error_mysql_processor(const int ret)
{
  ObMPError* p = new (&epbuf[0]) ObMPError(ret);
  return p;
}

int ObSrvMySQLXlator::get_mp_connect_processor(ObReqProcessor*& ret_proc)
{
  int ret = OB_SUCCESS;
  ObMPConnect* proc = NULL;
  void* buf = THIS_WORKER.get_sql_arena_allocator().alloc(sizeof(ObMPConnect));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for ObMPConnect", K(ret));
  } else {
    proc = new (buf) ObMPConnect(gctx_);
    if (OB_FAIL(proc->init())) {
      LOG_ERROR("init ObMPConnect fail", K(ret));
      worker_allocator_delete(proc);
      proc = NULL;
    }
  }
  ret_proc = proc;
  return ret;
}
