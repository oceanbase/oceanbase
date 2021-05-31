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

#include "sql/executor/ob_direct_receive_op.h"
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
OB_SERIALIZE_MEMBER((ObDirectReceiveSpec, ObOpSpec));

ObDirectReceiveOp::ObDirectReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObReceiveOp(exec_ctx, spec, input),
      scanner_(NULL),
      scanner_iter_(),
      all_data_empty_(false),
      cur_data_empty_(true),
      first_request_received_(false),
      found_rows_(0)
{}

int ObDirectReceiveOp::inner_open()
{
  int ret = OB_SUCCESS;
  all_data_empty_ = false;         /* Whether all the scanner data has been read */
  cur_data_empty_ = true;          /* Whether the current scanner data has been read */
  first_request_received_ = false; /* Whether the plan has been sent to the remote server */
  // Receive the first scanner. For DML such as insert and update, need to get affected_rows etc.
  if (OB_FAIL(setup_next_scanner())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to setup first scanner", K(ret));
    } else {
      all_data_empty_ = true; /* no more scanner */
    }
  } else {
    cur_data_empty_ = false; /* scanner is filled up again, can continue reading */
  }
  return ret;
}
/*
 * state:cur_data_empty_, all_data_empty_
 */
int ObDirectReceiveOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool has_got_a_row = false;
  RemoteExecuteStreamHandle* resp_handler = NULL;
  // Sometimes we need send user variables to remote end to complete query successfully
  // And, of course, we will get the new value for user variable.
  // Scanner contains the updated values.
  // so we update the user variables in terms of scanners here.
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx_, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_ERR_TASK_SKIPPED == resp_handler->get_result_code()) {
    // skip
    ret = OB_ITER_END;
    LOG_WARN("this remote task is skipped", K(ret));
  }

  /* It will be easier to understand the following code with state machine thinking */
  while (OB_SUCC(ret) && false == has_got_a_row) {
    if (all_data_empty_) { /* All data has been read */
      ret = OB_ITER_END;
    } else if (cur_data_empty_) { /* The current scanner has been read over */
      /* send RPC req and remote server response Scanner */
      if (OB_FAIL(setup_next_scanner())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to setup next scanner", K(ret));
        } else {
          all_data_empty_ = true; /* no more scanner */
        }
      } else {
        cur_data_empty_ = false; /* scanner is filled up again, can continue reading */
      }
    } else { /* current scanner is readable */
      if (OB_FAIL(get_next_row_from_cur_scanner())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row from cur scanner", K(ret));
        } else {
          // curr scanner read over
          cur_data_empty_ = true;
          ret = OB_SUCCESS;  // set ret be OB_SUCCESS for loop
        }
      } else {
        // get one row then break loop
        has_got_a_row = true;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    plan_ctx->set_found_rows(found_rows_);
  }
  return ret;
}

int ObDirectReceiveOp::inner_close()
{
  int ret = OB_SUCCESS;
  RemoteExecuteStreamHandle* resp_handler = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx_, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else {
    if (resp_handler->has_more()) {
      if (OB_FAIL(resp_handler->abort())) {
        LOG_WARN("fail to abort", K(ret));
      } else {
        ObSQLSessionInfo* session = ctx_.get_my_session();
        ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
        ObExecutorRpcImpl* rpc = NULL;
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx_, rpc))) {
          LOG_WARN("get task executor rpc failed", K(ret));
        } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(rpc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session or plan ctx or rpc is NULL", K(ret));
        } else {
          ObQueryRetryInfo retry_info;
          ObExecutorRpcCtx rpc_ctx(session->get_effective_tenant_id(),
              plan_ctx->get_timeout_timestamp(),
              ctx_.get_task_exec_ctx().get_min_cluster_version(),
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

int ObDirectReceiveOp::setup_next_scanner()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  RemoteExecuteStreamHandle* resp_handler = NULL;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_stream_handler(ctx_, resp_handler))) {
    LOG_WARN("fail get task response handler", K(ret));
  } else if (OB_ISNULL(resp_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    // Read the data the first time, and the result has been obtained when task_submit() is called by the Scheduler
    if (!first_request_received_) {
      ObScanner* scanner = resp_handler->get_result();
      if (OB_ISNULL(scanner)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        // set last_insert_id no matter success or fail
        plan_ctx->set_last_insert_id_to_client(scanner->get_last_insert_id_to_client());
        plan_ctx->set_last_insert_id_session(scanner->get_last_insert_id_session());
        plan_ctx->set_last_insert_id_changed(scanner->get_last_insert_id_changed());
        int tmp_ret = OB_SUCCESS;
        ObExecStatCollector& collector = ctx_.get_exec_stat_collector();
        if (OB_SUCCESS != (tmp_ret = collector.add_raw_stat(scanner->get_extend_info()))) {
          LOG_WARN("fail to collected raw extend info in scanner", K(tmp_ret));
        }
        if (OB_FAIL(scanner->get_err_code())) {
          int add_ret = OB_SUCCESS;
          const char* err_msg = scanner->get_err_msg();
          // after FORWARD_USER_ERROR(ret, err_msg),if err_msg length > 0,
          // then return err_msg
          // Otherwise, the err_msg returned is the default error message corresponding to ret.
          // use FORWARD_USER_ERROR(ret, err_msg) can qualify.
          FORWARD_USER_ERROR(ret, err_msg);
          LOG_WARN("while fetching first scanner, the remote rcode is not OB_SUCCESS",
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
          scanner_ = scanner;
          first_request_received_ = true;
          // INSERT,UPDATE,DELETE,The Scanner returned for the first time contains the affected row
          plan_ctx->set_affected_rows(scanner->get_affected_rows());
          found_rows_ += scanner->get_found_rows();
          if (OB_FAIL(scanner->get_datum_store().begin(scanner_iter_))) {
            LOG_WARN("fail to init datum store iter", K(ret));
          } else if (OB_FAIL(plan_ctx->set_row_matched_count(scanner->get_row_matched_count()))) {
            LOG_WARN("fail to set row matched count", K(ret), K(scanner->get_row_matched_count()));
          } else if (OB_FAIL(plan_ctx->set_row_duplicated_count(scanner->get_row_duplicated_count()))) {
            LOG_WARN("fail to set row duplicate count", K(ret), K(scanner->get_row_duplicated_count()));
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
    } else { /* Subsequent request, send SESSION_NEXT to the remote end through Handle */
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
            // Add the remote observer to the invalid servers of retry info
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
          scanner_ = result_scanner;
          found_rows_ += scanner_->get_found_rows();
          if (OB_FAIL(scanner_->get_datum_store().begin(scanner_iter_))) {
            LOG_WARN("fail to init datum store iter", K(ret));
          }
        }
      } else {
        ret = OB_ITER_END;
        // only successful select affect last_insert_id
        // for select, last_insert_id may changed because last_insert_id(#) called
        // last_insert_id values should be the last row calling last_insert_id(#)
        plan_ctx->set_last_insert_id_session(scanner_->get_last_insert_id_session());
        plan_ctx->set_last_insert_id_changed(scanner_->get_last_insert_id_changed());
        int tmp_ret = OB_SUCCESS;
        ObExecStatCollector& collector = ctx_.get_exec_stat_collector();
        if (OB_SUCCESS != (tmp_ret = collector.add_raw_stat(scanner_->get_extend_info()))) {
          LOG_WARN("fail to collected raw extend info in scanner", K(tmp_ret));
        }
        if (OB_SUCCESS != (tmp_ret = plan_ctx->get_table_row_count_list().assign(scanner_->get_table_row_counts()))) {
          LOG_WARN("fail to set table row count", K(ret), K(scanner_->get_table_row_counts()));
        }
        LOG_DEBUG(
            "remote table row counts", K(scanner_->get_table_row_counts()), K(plan_ctx->get_table_row_count_list()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(my_session->replace_user_variables(scanner_->get_session_var_map()))) {
      LOG_WARN("replace user variables failed", K(ret));
    }
  }

  return ret;
}

int ObDirectReceiveOp::get_next_row_from_cur_scanner()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scanner_iter_.get_next_row(MY_SPEC.output_, eval_ctx_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row", K(ret));
    } else {
    }
  } else {
    LOG_DEBUG("direct receive next row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
  }
  return ret;
}

int ObDirectReceiveOp::rescan()
{
  // not support the rescan operation of the remote operator
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Distributed rescan");
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
