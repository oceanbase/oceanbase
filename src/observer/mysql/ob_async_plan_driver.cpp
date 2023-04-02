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

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer
{

ObAsyncPlanDriver::ObAsyncPlanDriver(const ObGlobalContext &gctx,
                                     const ObSqlCtx &ctx,
                                     sql::ObSQLSessionInfo &session,
                                     ObQueryRetryCtrl &retry_ctrl,
                                     ObIMPPacketSender &sender,
                                     bool is_prexecute)
    : ObQueryDriver(gctx, ctx, session, retry_ctrl, sender, is_prexecute)
{
}

ObAsyncPlanDriver::~ObAsyncPlanDriver()
{
}

int ObAsyncPlanDriver::response_result(ObMySQLResultSet &result)
{
  ObActiveSessionGuard::get_stat().in_sql_execution_ = true;
  int ret = OB_SUCCESS;
  // result.open 后 pkt_param 所需的 last insert id 等各项参数都已经计算完毕
  // 对于异步增删改的情况，需要提前update last insert id，以确保回调pkt_param参数正确
  // 后续result set close的时候，还会进行一次store_last_insert_id的调用
  ObCurTraceId::TraceId *cur_trace_id = NULL;
  if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else if (OB_FAIL(result.open())) {
    LOG_WARN("failed to do result set open", K(ret));
  } else if (OB_FAIL(result.update_last_insert_id_to_client())) {
    LOG_WARN("failed to update last insert id after open", K(ret));
  } else {
    if (is_prexecute_ && OB_FAIL(sender_.flush_buffer(false))) {
      LOG_WARN("flush buffer fail before send async ok packet.", K(ret));
    } else {
      result.set_end_trans_async(true);
    }
  }

  if (OB_SUCCESS != ret) {
    // 如果try_again为true，说明这条SQL需要重做。考虑到重做之前我们需要回滚整个事务，会调用EndTransCb
    // 所以这里设置一个标记，告诉EndTransCb这种情况下不要给客户端回包。
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret, is_prexecute_);
    if (retry_ctrl_.need_retry()) {
      result.set_end_trans_async(false);
    }
    // close背后的故事：
    // if (try_again) {
    //   result.close()结束后返回到这里，然后运行重试逻辑
    // } else {
    //   result.close()结束后，process流程应该干净利落地结束，有什么事留到callback里面做
    // }
    int cret = result.close();
    if (retry_ctrl_.need_retry()) {
      LOG_WARN("result set open failed, will retry",
               K(ret), K(cli_ret), K(cret), K(retry_ctrl_.need_retry()));
    } else {
      LOG_WARN("result set open failed, let's leave process(). EndTransCb will clean this mess",
               K(ret), K(cli_ret), K(cret), K(retry_ctrl_.need_retry()));
    }
    ret = cli_ret;
  } else if (result.is_with_rows()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("SELECT should not use async method. wrong code!!!", K(ret));
  } else if (OB_FAIL(result.close())) {
    LOG_WARN("result close failed, let's leave process(). EndTransCb will clean this mess", K(ret));
  }

  // 仅在end_trans执行后（无论成功与否）才会设置，意味着一定会回调
  // 之所以设计这么一个值，是因为open/close可能“半途而废”，根本没走到
  // end_trans()接口。这个变量相当于一个最终的确认。
  bool async_resp_used = result.is_async_end_trans_submitted();
  if (async_resp_used && retry_ctrl_.need_retry()) {
    LOG_ERROR("the async request is ok, couldn't send request again");
  }
  LOG_DEBUG("test if async end trans submitted",
            K(ret), K(async_resp_used), K(retry_ctrl_.need_retry()));

  //if the error code is ob_timeout, we add more error info msg for dml query.
  if (OB_TIMEOUT == ret) {
    LOG_USER_ERROR(OB_TIMEOUT, THIS_WORKER.get_timeout_ts() - session_.get_query_start_time());
  }

  // 错误处理，没有走异步的时候负责回错误包
  if (!OB_SUCC(ret) && !async_resp_used && !retry_ctrl_.need_retry()) {
    int sret = OB_SUCCESS;
    bool is_partition_hit = session_.get_err_final_partition_hit(ret);
    if (OB_SUCCESS != (sret = sender_.send_error_packet(ret, NULL, is_partition_hit))) {
      LOG_WARN("send error packet fail", K(sret), K(ret));
    }
    //根据与事务层的约定，无论end_participant和end_stmt是否成功，
    //判断事务commit或回滚是否成功都只看最后的end_trans是否成功，
    //而SQL是要保证一定要调到end_trans的，调end_trans的时候判断了是否需要断连接，
    //所以这里不需要判断是否需要断连接了
  }
  ObActiveSessionGuard::get_stat().in_sql_execution_ = false;
  return ret;
}


}/* ns observer*/
}/* ns oceanbase */


