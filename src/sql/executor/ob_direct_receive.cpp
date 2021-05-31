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

#include "sql/executor/ob_direct_receive.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "share/ob_scanner.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/monitor/ob_exec_stat_collector.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObDirectReceive::ObDirectReceiveCtx::ObDirectReceiveCtx(ObExecContext& ctx)
    : ObPhyOperatorCtx(ctx),
      scanner_(NULL),
      scanner_iter_(),
      all_data_empty_(false),
      cur_data_empty_(true),
      first_request_received_(false),
      found_rows_(0)
{}

ObDirectReceive::ObDirectReceiveCtx::~ObDirectReceiveCtx()
{}
//
//
//
///////////////////// end Context /////////////////////////////
//
//

ObDirectReceiveInput::ObDirectReceiveInput() : ObReceiveInput()
{}

ObDirectReceiveInput::~ObDirectReceiveInput()
{}

void ObDirectReceiveInput::reset()
{
  ObReceiveInput::reset();
}

int ObDirectReceiveInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

ObPhyOperatorType ObDirectReceiveInput::get_phy_op_type() const
{
  return PHY_DIRECT_RECEIVE;
}

OB_SERIALIZE_MEMBER((ObDirectReceiveInput, ObReceiveInput));

//
//
///////////////////// End Input /////////////////////////////////
//
//

ObDirectReceive::ObDirectReceive(ObIAllocator& alloc) : ObReceive(alloc)
{}

ObDirectReceive::~ObDirectReceive()
{}

int ObDirectReceive::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;

  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObDirectReceiveCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ssucc to create op ctx, but op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObDirectReceive::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObDirectReceiveCtx* recv_ctx = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObDirectReceiveCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get phy op ctx", "op_id", get_id(), "op_type", get_type());
  } else {
    recv_ctx->all_data_empty_ = false;
    recv_ctx->cur_data_empty_ = true;
    recv_ctx->first_request_received_ = false;
    if (OB_FAIL(setup_next_scanner(ctx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to setup first scanner", K(ret));
      } else {
        recv_ctx->all_data_empty_ = true;
      }
    } else {
      recv_ctx->cur_data_empty_ = false;
    }
  }
  return ret;
}

int ObDirectReceive::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool has_got_a_row = false;
  RemoteExecuteStreamHandle* resp_handler = NULL;
  ObDirectReceiveCtx* recv_ctx = GET_PHY_OPERATOR_CTX(ObDirectReceiveCtx, ctx, get_id());
  // Sometimes we need send user variables to remote end to complete query successfully
  // And, of course, we will get the new value for user variable.
  // Scanner contains the updated values.
  // so we update the user variables in terms of scanners here.
  if (OB_ISNULL(recv_ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_ERR_TASK_SKIPPED == resp_handler->get_result_code()) {
    ret = OB_ITER_END;
    LOG_WARN("this remote task is skipped", K(ret));
  }

  /* following is an state machine */
  while (OB_SUCC(ret) && false == has_got_a_row) {
    if (recv_ctx->all_data_empty_) { /* all data is read */
      ret = OB_ITER_END;
    } else if (recv_ctx->cur_data_empty_) { /* current scanner is read */
      /* send RPC request, remote returns a scanner */
      if (OB_FAIL(setup_next_scanner(ctx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to setup next scanner", K(ret));
        } else {
          recv_ctx->all_data_empty_ = true; /* no more scanner */
        }
      } else {
        recv_ctx->cur_data_empty_ = false; /* scanner is filled once again */
      }
    } else { /* current scanner is readable */
      if (OB_FAIL(get_next_row_from_cur_scanner(*recv_ctx, row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row from cur scanner", K(ret));
        } else {
          // current scanner is read
          recv_ctx->cur_data_empty_ = true;
          ret = OB_SUCCESS;
        }
      } else {
        // retrive one row, exit loop
        has_got_a_row = true;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("plan ctx is NULL", K(ret));
    } else {
      plan_ctx->set_found_rows(recv_ctx->found_rows_);
    }
  }
  return ret;
}

int ObDirectReceive::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  RemoteExecuteStreamHandle* resp_handler = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else {
    if (resp_handler->has_more()) {
      if (OB_FAIL(resp_handler->abort())) {
        LOG_WARN("fail to abort", K(ret));
      } else {
        ObSQLSessionInfo* session = ctx.get_my_session();
        ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
        ObExecutorRpcImpl* rpc = NULL;
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
          LOG_WARN("get task executor rpc failed", K(ret));
        } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(rpc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session or plan ctx or rpc is NULL", K(ret));
        } else {
          ObQueryRetryInfo retry_info;
          ObExecutorRpcCtx rpc_ctx(session->get_effective_tenant_id(),
              plan_ctx->get_timeout_timestamp(),
              ctx.get_task_exec_ctx().get_min_cluster_version(),
              &retry_info,
              session,
              plan_ctx->is_plain_select_stmt());
          int tmp_ret = rpc->task_kill(rpc_ctx, resp_handler->get_task_id(), resp_handler->get_dst_addr());
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("kill task failed", K(tmp_ret), K(resp_handler->get_task_id()), K(resp_handler->get_dst_addr()));
          }
        }
      }
    } else {
    }
  }
  return ret;
}

int ObDirectReceive::setup_next_scanner(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  RemoteExecuteStreamHandle* resp_handler = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObDirectReceiveCtx* recv_ctx = GET_PHY_OPERATOR_CTX(ObDirectReceiveCtx, ctx, get_id());
  if (OB_ISNULL(recv_ctx)) {
    LOG_WARN("fail get phy op ctx");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get phy plan ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get my session", K(ret));
  }

  if (OB_SUCC(ret)) {
    /* reads data first time, result has been retrived in task_submit called by Scheduler */
    if (!recv_ctx->first_request_received_) {
      ObScanner* scanner = resp_handler->get_result();
      if (OB_ISNULL(scanner)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        // set last_insert_id no matter success or fail
        plan_ctx->set_last_insert_id_to_client(scanner->get_last_insert_id_to_client());
        plan_ctx->set_last_insert_id_session(scanner->get_last_insert_id_session());
        plan_ctx->set_last_insert_id_changed(scanner->get_last_insert_id_changed());
        int tmp_ret = OB_SUCCESS;
        ObExecStatCollector& collector = ctx.get_exec_stat_collector();
        if (OB_SUCCESS != (tmp_ret = collector.add_raw_stat(scanner->get_extend_info()))) {
          LOG_WARN("fail to collected raw extend info in scanner", K(tmp_ret));
        }
        if (OB_FAIL(scanner->get_err_code())) {
          int add_ret = OB_SUCCESS;
          const char* err_msg = scanner->get_err_msg();
          FORWARD_USER_ERROR(ret, err_msg);
          LOG_WARN("while fetching first scanner, the remote rcode is not OB_SUCCESS",
              K(ret),
              K(err_msg),
              "dst_addr",
              to_cstring(resp_handler->get_dst_addr()));
          if (is_data_not_readable_err(ret)) {
            // slave replays log's copy
            ObQueryRetryInfo& retry_info = my_session->get_retry_info_for_update();
            if (OB_UNLIKELY(OB_SUCCESS !=
                            (add_ret = retry_info.add_invalid_server_distinctly(resp_handler->get_dst_addr(), true)))) {
              LOG_WARN("fail to add remote addr to invalid servers distinctly",
                  K(ret),
                  K(add_ret),
                  K(resp_handler->get_dst_addr()),
                  K(retry_info));
            }
          }
        } else {
          recv_ctx->scanner_ = scanner;
          recv_ctx->scanner_iter_ = scanner->begin();
          recv_ctx->first_request_received_ = true;
          plan_ctx->set_affected_rows(scanner->get_affected_rows());
          recv_ctx->found_rows_ += scanner->get_found_rows();
          if (OB_FAIL(plan_ctx->set_row_matched_count(scanner->get_row_matched_count()))) {
            LOG_WARN("fail to set row matched count", K(ret), K(scanner->get_row_matched_count()));
          } else if (OB_FAIL(plan_ctx->set_row_duplicated_count(scanner->get_row_duplicated_count()))) {
            LOG_WARN("fail to set row duplicate count", K(ret), K(scanner->get_row_duplicated_count()));
            //          } else if (OB_FAIL(my_session->get_trans_result().merge_result(scanner->get_trans_result()))) {
            //            LOG_WARN("merge trans result to session failed", K(ret));
            /**
             * ObRemoteTaskExecutor::execute() has called merge_result() before here, that is a
             * better place to call merge_result(), especially when any operation failed between
             * there and here.
             */
          } else if (OB_FAIL(plan_ctx->merge_implicit_cursors(scanner->get_implicit_cursors()))) {
            LOG_WARN("merge implicit cursors failed", K(ret), K(scanner->get_implicit_cursors()));
          }
        }
      }
    } else {
      ObScanner* result_scanner = NULL;
      if (resp_handler->has_more()) {
        if (OB_FAIL(resp_handler->reset_and_init_result())) {
          LOG_WARN("fail reset and init result", K(ret));
        } else if (OB_ISNULL(result_scanner = resp_handler->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc result, but result scanner is NULL", K(ret));
        } else if (OB_FAIL(resp_handler->get_more(*result_scanner))) {
          LOG_WARN("fail wait response", K(ret), "dst_addr", to_cstring(resp_handler->get_dst_addr()));
        } else if (OB_FAIL(result_scanner->get_err_code())) {
          int add_ret = OB_SUCCESS;
          const char* err_msg = result_scanner->get_err_msg();
          FORWARD_USER_ERROR(ret, err_msg);
          LOG_WARN("while getting more scanner, the remote rcode is not OB_SUCCESS",
              K(ret),
              K(err_msg),
              "dst_addr",
              to_cstring(resp_handler->get_dst_addr()));
          if (is_data_not_readable_err(ret)) {
            ObQueryRetryInfo& retry_info = my_session->get_retry_info_for_update();
            if (OB_UNLIKELY(OB_SUCCESS !=
                            (add_ret = retry_info.add_invalid_server_distinctly(resp_handler->get_dst_addr(), true)))) {
              LOG_WARN("fail to add remote addr to invalid servers distinctly",
                  K(ret),
                  K(add_ret),
                  K(resp_handler->get_dst_addr()),
                  K(retry_info));
            }
          }
        } else {
          recv_ctx->scanner_ = result_scanner;
          recv_ctx->scanner_iter_ = recv_ctx->scanner_->begin();
          recv_ctx->found_rows_ += recv_ctx->scanner_->get_found_rows();
        }
      } else {
        ret = OB_ITER_END;
        // only successful select affect last_insert_id
        // for select, last_insert_id may changed because last_insert_id(#) called
        // last_insert_id values should be the last row calling last_insert_id(#)
        plan_ctx->set_last_insert_id_session(recv_ctx->scanner_->get_last_insert_id_session());
        plan_ctx->set_last_insert_id_changed(recv_ctx->scanner_->get_last_insert_id_changed());
        int tmp_ret = OB_SUCCESS;
        ObExecStatCollector& collector = ctx.get_exec_stat_collector();
        if (OB_SUCCESS != (tmp_ret = collector.add_raw_stat(recv_ctx->scanner_->get_extend_info()))) {
          LOG_WARN("fail to collected raw extend info in scanner", K(tmp_ret));
        }
        if (OB_SUCCESS !=
            (tmp_ret = plan_ctx->get_table_row_count_list().assign(recv_ctx->scanner_->get_table_row_counts()))) {
          LOG_WARN("fail to set table row count", K(ret), K(recv_ctx->scanner_->get_table_row_counts()));
        }
        LOG_DEBUG("remote table row counts",
            K(recv_ctx->scanner_->get_table_row_counts()),
            K(plan_ctx->get_table_row_count_list()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(my_session->replace_user_variables(ctx, recv_ctx->scanner_->get_session_var_map()))) {
      LOG_WARN("replace user variables failed", K(ret));
    }
  }
  return ret;
}

int ObDirectReceive::get_next_row_from_cur_scanner(ObDirectReceiveCtx& op_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_ctx.scanner_iter_.get_next_row(op_ctx.get_cur_row()))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row", K(ret));
    } else {
    }
  } else {
    row = &op_ctx.get_cur_row();
  }
  return ret;
}

int ObDirectReceive::rescan(ObExecContext& ctx) const
{
  UNUSED(ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Distributed rescan");
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
