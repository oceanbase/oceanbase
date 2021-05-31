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

#include "ob_batch_processor.h"
#include "storage/ob_partition_service.h"
#include "election/ob_election_mgr.h"
#include "clog/ob_clog_mgr.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_task.h"
#include "observer/omt/ob_th_worker.h"
#include "observer/omt/ob_tenant.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace obrpc {
int ObBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps_)) {
    ret = OB_NOT_INIT;
  } else {
    int64_t req_pos = 0;
    ObBatchPacket::Req* req = NULL;
    common::ObAddr sender;
    if (arg_.src_addr_.is_valid()) {
      sender = arg_.src_addr_;
    } else {
      sender.set_ipv4_server_id(arg_.src_);
    }
    // Get the cluster_id of the source
    const int64_t src_cluster_id = get_src_cluster_id();
    common::ObPartitionKey pkey;
    int64_t clog_batch_nodelay_cnt = 0;
    int64_t clog_batch_cnt = 0;
    int64_t trx_batch_cnt = 0;
    int64_t sql_batch_cnt = 0;
    while (NULL != (req = arg_.next(req_pos))) {
      const uint32_t flag = (req->type_ >> 24);
      const uint32_t batch_type = ((req->type_ >> 16) & 0xff);
      const uint32_t msg_type = req->type_ & 0xffff;
      ObCurTraceId::TraceId trace_id;
      char* buf = (char*)(req + 1);
      int64_t pos = 0;
      if (1 == flag) {
        if (OB_FAIL(trace_id.deserialize(buf, req->size_, pos))) {
          RPC_LOG(WARN, "decode trace id failed", K(ret));
        } else {
          ObCurTraceId::set(trace_id);
        }
      }
      if (OB_SUCC(ret)) {
        switch (batch_type) {
          case ELECTION_GROUP_BATCH_REQ:
            handle_election_group_req(sender, msg_type, buf + pos, req->size_ - (int32_t)pos);
            break;
          case CLOG_BATCH_REQ_NODELAY:
            // go through
          case CLOG_BATCH_REQ_NODELAY2:
            clog_batch_nodelay_cnt++;
            if (OB_SUCCESS == pkey.deserialize(buf, req->size_, pos)) {
              handle_clog_req(sender, src_cluster_id, msg_type, pkey, buf + pos, req->size_ - (int32_t)pos);
            }
            break;
          case CLOG_BATCH_REQ:
            clog_batch_cnt++;
            if (OB_SUCCESS == pkey.deserialize(buf, req->size_, pos)) {
              handle_clog_req(sender, src_cluster_id, msg_type, pkey, buf + pos, req->size_ - (int32_t)pos);
            }
            break;
          case ELECTION_BATCH_REQ:
            if (OB_SUCCESS == pkey.deserialize(buf, req->size_, pos)) {
              handle_election_req(sender, msg_type, pkey, buf + pos, req->size_ - (int32_t)pos);
            }
            break;
          case TRX_BATCH_REQ_NODELAY:
            // go through
          case TRX_BATCH_REQ:
            trx_batch_cnt++;
            if (OB_SUCCESS == pkey.deserialize(buf, req->size_, pos)) {
              handle_trx_req(sender, msg_type, pkey, buf + pos, req->size_ - (int32_t)pos);
            }
            break;
          case SQL_BATCH_REQ_NODELAY1:
            // go through
          case SQL_BATCH_REQ_NODELAY2:
            sql_batch_cnt++;
            if (OB_SUCCESS == pkey.deserialize(buf, req->size_, pos)) {
              handle_sql_req(sender, msg_type, pkey, buf + pos, req->size_ - (int32_t)pos);
            }
            break;
          default:
            RPC_LOG(ERROR, "unknown batch req type", K(req->type_));
            break;
        }
      }
    }
    if (REACH_TIME_INTERVAL(3000000)) {
      RPC_LOG(INFO,
          "batch rpc statistics",
          K(clog_batch_nodelay_cnt),
          K(clog_batch_cnt),
          K(trx_batch_cnt),
          K(sql_batch_cnt));
    }
  }
  return ret;
}

int ObBatchP::handle_clog_req(common::ObAddr& sender, const int64_t src_cluster_id, int type,
    common::ObPartitionKey& pkey, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  clog::ObCLogMgr* clog_mgr = NULL;
  if (OB_ISNULL(clog_mgr = static_cast<clog::ObCLogMgr*>(ps_->get_clog_mgr()))) {
  } else {
    clog_mgr->get_log_executor().handle_clog_req(sender, src_cluster_id, type, pkey, buf, size);
  }
  return ret;
}

int ObBatchP::handle_election_req(
    common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  election::ObElectionMgr* election_mgr = NULL;
  UNUSED(sender);
  if (OB_ISNULL(election_mgr = static_cast<election::ObElectionMgr*>(ps_->get_election_mgr()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = election_mgr->handle_election_req(type, pkey, buf, size);
  }
  return ret;
}
int ObBatchP::handle_election_group_req(common::ObAddr& sender, int type, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  election::ObElectionMgr* election_mgr = NULL;
  UNUSED(sender);
  if (OB_ISNULL(election_mgr = static_cast<election::ObElectionMgr*>(ps_->get_election_mgr()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = election_mgr->handle_election_group_req(type, buf, size);
  }
  return ret;
}

int ObBatchP::handle_trx_req(
    common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService* trans_service = NULL;
  UNUSED(sender);
  if (OB_ISNULL(trans_service = ps_->get_trans_service())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = trans_service->handle_trx_req(type, pkey, buf, size);
  }
  return ret;
}

int ObBatchP::handle_sql_req(
    common::ObAddr& sender, int type, common::ObPartitionKey& pkey, const char* buf, int32_t size)
{
  UNUSED(pkey);
  UNUSED(sender);
  int ret = OB_SUCCESS;
  ObReqTimestamp req_ts;
  req_ts.receive_timestamp_ = get_receive_timestamp();
  req_ts.enqueue_timestamp_ = get_enqueue_timestamp();
  req_ts.run_timestamp_ = get_run_timestamp();
  sql::ObSql* sql_engine = GCTX.sql_engine_;
  omt::ObTenant* cur_tenant = THIS_THWORKER.get_tenant();
  if (OB_ISNULL(sql_engine) || OB_ISNULL(cur_tenant)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(size > ObSqlTask::MAX_SQL_TASK_SIZE) || OB_SQL_REMOTE_RESULT_TYPE == type) {
    ret = sql_engine->handle_batch_req(type, req_ts, buf, size);
  } else {
    sql::ObSqlTask* task = ObSqlTaskFactory::get_instance().alloc(cur_tenant->id());
    if (OB_ISNULL(task)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG(ERROR, "alloc sql task failed", K(ret));
    } else {
      if (OB_FAIL(task->init(type, req_ts, buf, size, sql_engine))) {
        RPC_LOG(WARN, "init sql task failed", K(ret));
      } else if (OB_FAIL(cur_tenant->recv_request(*task))) {
        RPC_LOG(WARN, "push sql task failed", K(ret));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        ObSqlTaskFactory::get_instance().free(task);
        task = NULL;
      }
    }
  }
  return ret;
}
};  // namespace obrpc
};  // end namespace oceanbase
