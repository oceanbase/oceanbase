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

#include "ob_async_cmd_driver.h"

#include "lib/profile/ob_perf_event.h"
#include "obsm_row.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "observer/mysql/obmp_query.h"
#include "observer/mysql/obmp_stmt_prexecute.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer
{

ObAsyncCmdDriver::ObAsyncCmdDriver(const ObGlobalContext &gctx,
                                 const ObSqlCtx &ctx,
                                 sql::ObSQLSessionInfo &session,
                                 ObQueryRetryCtrl &retry_ctrl,
                                 ObIMPPacketSender &sender,
                                 bool is_prexecute)
    : ObQueryDriver(gctx, ctx, session, retry_ctrl, sender, is_prexecute)
{
}

ObAsyncCmdDriver::~ObAsyncCmdDriver()
{
}

/*
 * for now, ObAsyncCmdDriver is just an implementation of async end trans
 */
int ObAsyncCmdDriver::response_result(ObMySQLResultSet &result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sql_execution);
  int ret = OB_SUCCESS;
  ObSqlEndTransCb &sql_end_cb = session_.get_mysql_end_trans_cb();
  ObEndTransCbPacketParam pkt_param;
  result.set_end_trans_async(true);
  ObCurTraceId::TraceId *cur_trace_id = NULL;
  if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else if (is_prexecute_
    && OB_FAIL(response_query_header(result, false, false, true))) {
    LOG_WARN("flush buffer fail before send async ok packet.", K(ret));
  } else if (OB_FAIL(sql_end_cb.set_packet_param(pkt_param.fill(result, session_, *cur_trace_id)))) {
    LOG_ERROR("fail to set packet param", K(ret));
  } else if (OB_FAIL(result.open())) {
    //once open failed, nothing will be responded to clients
    //so, we need to decide to retry or not here.
    int cli_ret = OB_SUCCESS;
    int close_ret = OB_SUCCESS;
    if ((close_ret = result.close()) != OB_SUCCESS) { //should not use OB_FAIL which will overwrite the ret
      LOG_WARN("close result set fail", K(close_ret));
    }
    if (!result.is_async_end_trans_submitted()) {
      retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret, is_prexecute_);
      LOG_WARN("result set open failed, check if need retry",
               K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
      ret = cli_ret;
    } else {
      LOG_WARN("result set open failed, async end trans submmited, don't retry", K(ret));
    }
    //send error packet in sql thread
    if (!OB_SUCC(ret) && !retry_ctrl_.need_retry() && (!result.is_async_end_trans_submitted())) {
      int sret = OB_SUCCESS;
      bool is_partition_hit = session_.partition_hit().get_bool();
      if (OB_SUCCESS != (sret = sender_.send_error_packet(ret, NULL, is_partition_hit))) {
        LOG_WARN("send error packet fail", K(sret), K(ret));
      }
    }
  } else {
    //what if begin;select 1; select 2; commit; commit;
    //we should still have to respond a packet to client in terms of the last commit
    if (OB_UNLIKELY(!result.is_async_end_trans_submitted())) {
      ObOKPParam ok_param;
      ok_param.affected_rows_ = 0;
      ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
      ok_param.has_more_result_ = result.has_more_result();
      if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
        LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
      }
    }
    int close_ret = OB_SUCCESS;
    if (OB_SUCCESS != (close_ret = result.close())) {
      LOG_WARN("close result failed", K(close_ret));
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
