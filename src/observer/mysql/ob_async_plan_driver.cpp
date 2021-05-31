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

#include "ob_async_plan_driver.h"

#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/profile/ob_perf_event.h"
#include "obsm_row.h"
#include "observer/mysql/obmp_query.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer {

ObAsyncPlanDriver::ObAsyncPlanDriver(const ObGlobalContext& gctx, const ObSqlCtx& ctx, sql::ObSQLSessionInfo& session,
    ObQueryRetryCtrl& retry_ctrl, ObIMPPacketSender& sender)
    : ObQueryDriver(gctx, ctx, session, retry_ctrl, sender)
{}

ObAsyncPlanDriver::~ObAsyncPlanDriver()
{}

int ObAsyncPlanDriver::response_result(ObMySQLResultSet& result)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId* cur_trace_id = NULL;
  if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else if (OB_FAIL(result.sync_open())) {
    LOG_WARN("failed to do result set open", K(ret));
  } else if (OB_FAIL(result.update_last_insert_id())) {
    LOG_WARN("failed to update last insert id after open", K(ret));
  } else {
    ObSqlEndTransCb& sql_end_cb = session_.get_mysql_end_trans_cb();
    ObEndTransCbPacketParam pkt_param;
    if (OB_FAIL(sql_end_cb.set_packet_param(pkt_param.fill(result, session_, *cur_trace_id)))) {
      LOG_ERROR("fail set packet param", K(ret));
    } else {
      result.set_end_trans_async(true);
      THIS_WORKER.disable_retry();
    }
  }

  if (!OB_SUCC(ret)) {
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
    if (retry_ctrl_.need_retry()) {
      result.set_end_trans_async(false);
    }
    int cret = result.close();
    if (retry_ctrl_.need_retry()) {
      LOG_WARN("result set open failed, will retry", K(ret), K(cli_ret), K(cret), K(retry_ctrl_.need_retry()));
    } else {
      LOG_WARN("result set open failed, let's leave process(). EndTransCb will clean this mess",
          K(ret),
          K(cli_ret),
          K(cret),
          K(retry_ctrl_.need_retry()));
    }
    ret = cli_ret;
  } else if (result.is_with_rows()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("SELECT should not use async method. wrong code!!!", K(ret));
  } else if (OB_FAIL(result.close())) {
    LOG_WARN("result close failed, let's leave process(). EndTransCb will clean this mess", K(ret));
  }

  bool async_resp_used = result.is_async_end_trans_submitted();
  if (async_resp_used && retry_ctrl_.need_retry()) {
    LOG_ERROR("the async request is ok, couldn't send request again");
  }
  LOG_DEBUG("test if async end trans submitted", K(ret), K(async_resp_used), K(retry_ctrl_.need_retry()));

  if (!OB_SUCC(ret) && !async_resp_used && !retry_ctrl_.need_retry()) {
    int sret = OB_SUCCESS;
    bool is_partition_hit = session_.partition_hit().get_bool();
    if (OB_SUCCESS != (sret = sender_.send_error_packet(ret, NULL, is_partition_hit))) {
      LOG_WARN("send error packet fail", K(sret), K(ret));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
