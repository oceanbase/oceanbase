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

#define USING_LOG_PREFIX BALANCE
#include "ob_balance_task_execute_service.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"//trans
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "share/schema/ob_schema_struct.h"//ObTenantInfo
#include "share/schema/ob_multi_version_schema_service.h"//ObMultiVersionSchemaService
#include "share/schema/ob_part_mgr_util.h"//ObPartitionSchemaIter
#include "share/ob_unit_table_operator.h" //ObUnitTableOperator
#include "share/balance/ob_balance_job_table_operator.h"//ObBalanceJob
#include "share/balance/ob_transfer_partition_task_table_operator.h"//set transfer
#include "share/ob_primary_zone_util.h"//get_primary_zone
#include "share/rc/ob_tenant_base.h"//MTL
#include "share/ls/ob_ls_operator.h"//ls_op
#include "share/ls/ob_ls_status_operator.h"//status_op
#include "share/ls/ob_ls_table_operator.h"//lst_operator->get
#include "share/location_cache/ob_location_service.h"//get_leader
#include "share/rpc/ob_async_rpc_proxy.h"//wait_all
#include "share/transfer/ob_transfer_task_operator.h"//get_history_task
#include "rootserver/ob_tenant_transfer_service.h"//transfer
#include "rootserver/balance/ob_ls_all_part_builder.h"   // ObLSAllPartBuilder
#include "rootserver/ob_root_utils.h"//get_rs_default_timeout_ctx
#include "observer/ob_server_struct.h"//GCTX

#define ISTAT(fmt, args...) FLOG_INFO("[BALANCE_EXECUTE] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[BALANCE_EXECUTE] " fmt, ##args)


namespace oceanbase
{
using namespace common;
using namespace share;

namespace rootserver
{
//////////////ObBalanceTaskExecuteService
int ObBalanceTaskExecuteService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("BalanceExec",
          lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    task_comment_.reset();
    tenant_id_ = MTL_ID();
    task_array_.reset();
    inited_ = true;
  }
  return ret;
}

void ObBalanceTaskExecuteService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_comment_.reset();
  task_array_.reset();
  sql_proxy_ = NULL;
  inited_ = false;
}

int ObBalanceTaskExecuteService::wait_tenant_ready_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema ptr is null", KR(ret), KP(GCTX.schema_service_));
  } else {
    bool is_ready = false;
    while (!is_ready && !has_set_stop()) {
      ret = OB_SUCCESS;

      share::schema::ObSchemaGetterGuard schema_guard;
      const share::schema::ObTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
        LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id_));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id_));
      } else if (!tenant_schema->is_normal()) {
        ret = OB_NEED_WAIT;
        WSTAT("tenant schema not ready, no need tenant balance", KR(ret));
      } else {
        is_ready = true;
      }

      if (! is_ready) {
        idle(10 * 1000 *1000);
      }
    }

    if (has_set_stop()) {
      WSTAT("thread has been stopped", K(is_ready), K(tenant_id_));
      ret = OB_IN_STOP_STATE;
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::try_update_task_comment_(
    const share::ObBalanceTask &task, const common::ObSqlString &comment,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(task));
  } else if (comment.empty() || 0 == task.get_comment().string().case_compare(comment.ptr())) {
    //comment is empty or commet is same, no need to update
  } else if (OB_FAIL(ObBalanceTaskTableOperator::update_task_comment(tenant_id_,
      task.get_balance_task_id(), comment.string(), sql_client))) {
    LOG_WARN("failed to update task comment", KR(ret), K(task), K(comment), K(tenant_id_));
  }
  return ret;
}

void ObBalanceTaskExecuteService::do_work()
{
  int ret = OB_SUCCESS;
  ISTAT("balance task execute thread", K(tenant_id_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(wait_tenant_ready_())) {
    LOG_WARN("wait tenant ready fail", KR(ret), K(tenant_id_));
  } else {
    int64_t idle_time_us = 100 * 1000L;
    int tmp_ret = OB_SUCCESS;
    while (!has_set_stop()) {
      idle_time_us = 1 * 1000 * 1000L;
      ObCurTraceId::init(GCONF.self_addr_);
      task_array_.reset();
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_EXECUTE_WORK);
      //TODO, check schema ready
     if (OB_FAIL(ObBalanceTaskTableOperator::load_can_execute_task(
             tenant_id_, task_array_, *sql_proxy_))) {
        LOG_WARN("failed to load all balance task", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(execute_task_())) {
        LOG_WARN("failed to execute balance task", KR(ret));
      }
      if (OB_FAIL(ret)) {
        idle_time_us = 100 * 1000;
      }
      ISTAT("finish one round", KR(ret), K(task_array_), K(idle_time_us), K(task_comment_));
      task_comment_.reset();
      idle(idle_time_us);
    }// end while
  }
}

int ObBalanceTaskExecuteService::finish_task_(
    const share::ObBalanceTask &task,
    const ObBalanceTaskStatus finish_task_status,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(! task.is_valid()
      || ! finish_task_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task), K(finish_task_status));
  }
  // clean finish task, move to history
  else if (OB_FAIL(ObBalanceTaskTableOperator::clean_task(tenant_id_, task.get_balance_task_id(), trans))) {
    LOG_WARN("failed to clean task", KR(ret), K(tenant_id_), K(task));
  }
  // clean parent info of completed task
  // ignore failed status tasks
  else if (finish_task_status.is_completed()) {
    const ObBalanceTaskIDList &child_list = task.get_child_task_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_list.count(); ++i) {
      if (OB_FAIL(ObBalanceTaskTableOperator::remove_parent_task(tenant_id_, child_list.at(i),
      task.get_balance_task_id(), trans))) {
        LOG_WARN("failed to clean parent info of task", KR(ret), K(child_list), K(i), K(task));
      }
    }
  }
  ISTAT("clean finished task", KR(ret), K(task), K(finish_task_status));
  return ret;
}

int ObBalanceTaskExecuteService::update_task_status_(
    const share::ObBalanceTask &task,
    const share::ObBalanceJobStatus &job_status,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObBalanceTaskStatus finish_task_status;
  bool task_is_finished = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(! task.is_valid()
      || !job_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task), K(job_status));
  } else if (task.get_task_status().is_finish_status()) {
    task_is_finished = true;
    finish_task_status = task.get_task_status();
  } else {
    ObBalanceTaskStatus next_task_status = task.get_next_status(job_status);
    if (OB_UNLIKELY(!next_task_status.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("next task status is invalid", KR(ret), K(task));
    } else if (OB_FAIL(ObBalanceTaskTableOperator::update_task_status(
                   task, next_task_status, trans))) {
      LOG_WARN("failed to update task status", KR(ret), K(tenant_id_), K(task),
               K(next_task_status));
    } else {
      task_is_finished = (next_task_status.is_finish_status());
      // get latest task status
      finish_task_status = next_task_status;
    }
    ISTAT("update task status", KR(ret), "old_status", task.get_task_status(),
          K(next_task_status), K(task_is_finished), K(task), K(job_status), K(task_comment_));
  }

      // finish task as soon as possible in one trans after task is finished
  if (OB_FAIL(ret)) {
  } else if (task_is_finished &&
             OB_FAIL(finish_task_(task, finish_task_status, trans))) {
    LOG_WARN("fail to finish task", KR(ret), K(finish_task_status),
             K(task));
  }
  return ret;
}

int ObBalanceTaskExecuteService::process_current_task_status_(
    const share::ObBalanceTask &task, const share::ObBalanceJob &job,
    ObMySQLTransaction &trans,
    bool &skip_next_status)
{
  int ret = OB_SUCCESS;
  skip_next_status = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else {
    if (task.get_task_status().is_init()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_INIT);
      if (OB_FAIL(process_init_task_(task, trans, skip_next_status))) {
        LOG_WARN("failed to init trans", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_create_ls()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_CREATE_LS);
      if (OB_FAIL(wait_ls_to_target_status_(task.get_dest_ls_id(),
                                            share::OB_LS_NORMAL, skip_next_status))) {
        LOG_WARN("failed to wait ls to normal", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_transfer()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_TRANSFER);
      bool all_part_transfered = false;
      if (OB_FAIL(execute_transfer_in_trans_(task, job, trans, all_part_transfered))) {
        LOG_WARN("failed to execute transfer in trans", KR(ret), K(task), K(job));
      } else if (!all_part_transfered) {
        skip_next_status = true;
      } else if (task.get_task_type().is_merge_task()) {
        DEBUG_SYNC(BEFORE_DROPPING_LS_IN_BALANCE_MERGE_TASK);
        if (OB_FAIL(set_ls_to_dropping_(task.get_src_ls_id(), trans))) {
          LOG_WARN("failed to set ls dropping", KR(ret), K(task));
        }
      }
    } else if (task.get_task_status().is_alter_ls()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_ALTER_LS);
      if (OB_FAIL(wait_alter_ls_(task, skip_next_status))) {
        LOG_WARN("failed to wait alter ls", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_set_merge_ls()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_SET_MERGE);
      if (OB_FAIL(set_ls_to_merge_(task, trans))) {
        LOG_WARN("failed to set ls to merge", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_drop_ls()) {
      DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_DROP_LS);
      if (OB_FAIL(wait_ls_to_target_status_(task.get_src_ls_id(),
                                            share::OB_LS_WAIT_OFFLINE, skip_next_status))) {
        LOG_WARN("failed to wait to wait offline", KR(ret), K(task));
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          // ls status already drop end
        }
      }
    } else {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("unexpected task status", KR(ret), K(task));
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::execute_task_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      ObBalanceJob job;
      task_comment_.reset();
      bool skip_next_status = false;
      common::ObMySQLTransaction trans;
      const ObBalanceTask &task = task_array_.at(i);
      const ObBalanceTaskID task_id = task.get_balance_task_id();
      ObBalanceTask task_in_trans;//for update
      ObTimeoutCtx timeout_ctx;
      const int64_t balance_task_execute_timeout = GCONF.internal_sql_execute_timeout + 100 * 1000 * 1000L; // +100s
      DEBUG_SYNC(BEFORE_EXECUTE_BALANCE_TASK);
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, balance_task_execute_timeout))) {
        LOG_WARN("failed to get rs default timeout ctx", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
        LOG_WARN("failed to start trans", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(get_balance_job_task_for_update_(task, job, task_in_trans, trans))) {
        LOG_WARN("failed to get job", KR(ret), K(task));
      } else if (task_in_trans.get_task_status().is_finish_status()) {
      } else {
        if (job.get_job_status().is_doing()) {
          if (OB_FAIL(process_current_task_status_(task_in_trans, job, trans, skip_next_status))) {
            LOG_WARN("failed to process current task status", KR(ret), K(task_in_trans));
          }
        } else if (job.get_job_status().is_canceling()) {
          if (OB_FAIL(cancel_current_task_status_(task_in_trans, job, trans, skip_next_status))) {
            LOG_WARN("failed to cancel current task", KR(ret), K(task_in_trans));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("balance job status not expected", KR(ret), K(task_in_trans), K(job));
        }
      }
      if (!task_comment_.empty()) {
        if (FAILEDx(try_update_task_comment_(task_in_trans, task_comment_, trans))) {
          LOG_WARN("failed to update task commet", KR(ret), K(task_in_trans),
              K(task_comment_));
        }
      }

        // move on to next status or clean up
      if (OB_SUCC(ret) && !skip_next_status) {
        if (OB_FAIL(update_task_status_(task_in_trans, job.get_job_status(), trans))) {
          LOG_WARN("failed to update task status", KR(ret), K(task_in_trans), K(job));
        }
      }


      if (trans.is_started()) {
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }

      if (task_in_trans.get_task_status().is_set_merge_ls()) {
        DEBUG_SYNC(AFTER_BLOCK_TABLET_IN_WHEN_LS_MERGE);
      }
      ISTAT("process task", KR(ret), K(task_in_trans), K(job), K(task_comment_));
      //isolate error of each task
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::get_balance_job_task_for_update_(
    const ObBalanceTask &task, ObBalanceJob &job, ObBalanceTask &task_in_trans,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  job.reset();
  task_in_trans.reset();;
  int64_t start_time = 0; //no use
  int64_t finish_time = 0; //no use
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(tenant_id_, true,
                  trans, job, start_time, finish_time))) {
    LOG_WARN("failed to get balance job", KR(ret), K(tenant_id_));
  } else if (task.get_job_id() != job.get_job_id()) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("job not expected", KR(ret), K(task), K(job));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::get_balance_task(tenant_id_,
          task.get_balance_task_id(), true, trans, task_in_trans, start_time, finish_time))) {
    LOG_WARN("failed to get balance task", KR(ret), K(tenant_id_), K(task));
  }
  return ret;
}

int ObBalanceTaskExecuteService::cancel_current_task_status_(
    const share::ObBalanceTask &task, const share::ObBalanceJob &job,
    ObMySQLTransaction &trans, bool &skip_next_status)
{
  int ret = OB_SUCCESS;
  skip_next_status = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else {
    if (task.get_task_status().is_transfer()) {
      if (!task.get_current_transfer_task_id().is_valid()) {
        //no transfer, no need to todo
      } else {
        //try to wait transfer end
        ObTenantTransferService *transfer_service =
            MTL(ObTenantTransferService *);
        if (OB_ISNULL(transfer_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("transfer service is null", KR(ret));
        } else if (OB_FAIL(transfer_service->try_cancel_transfer_task(
                       task.get_current_transfer_task_id()))) {
          LOG_WARN("failed to cancel transfer task", KR(ret), K(task));
          skip_next_status = true;
          int tmp_ret = ret;
          ret = OB_SUCCESS;
          if (OB_FAIL(task_comment_.assign_fmt("Fail to cancel transfer task %ld, result is %d",
                task.get_current_transfer_task_id().id(), tmp_ret))) {
            LOG_WARN("failed to assign fmt", KR(ret), KR(tmp_ret), K(task));
          }
        } else if (job.get_job_type().is_transfer_partition()) {
          ObTransferTask transfer_task;
          int64_t create_time = 0, finish_time = 0;//no use
          if (OB_FAIL(ObTransferTaskOperator::get_history_task(trans, tenant_id_,
                  task.get_current_transfer_task_id(), transfer_task,
                  create_time, finish_time))) {
            LOG_WARN("failed to get history task", KR(ret), K(tenant_id_), K(task));
          } else if (OB_FAIL(finish_transfer_partition_task_(transfer_task, job, trans))) {
            LOG_WARN("failed to finish transfer partition task", KR(ret),
               K(transfer_task), K(job));
          }
        }
      }
    } else {
      //init, create ls, alter_ls, drop ls, set_ls_merge
      //no need wait task end
    }
    if (OB_SUCC(ret) && !task.get_task_status().is_init()
        && task.get_task_type().is_merge_task() && !skip_next_status) {
      //rollback flag
      ObLSAttrOperator ls_op(tenant_id_, sql_proxy_);
      share::ObLSAttr ls_info;
      share::ObLSFlag flag;
      if (OB_FAIL(ls_op.get_ls_attr(task.get_src_ls_id(), true, trans, ls_info))) {
        LOG_WARN("failed to get ls attr", KR(ret), K(task));
        if (OB_ENTRY_NOT_EXIST == ret) {
          //while task in dropping status, ls may not exist
          ret = OB_SUCCESS;
        }
      } else if (!ls_info.get_ls_flag().is_block_tablet_in()) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("ls must in block tablet in", KR(ret), K(ls_info));
      } else {
        share::ObLSFlag new_flag = ls_info.get_ls_flag();
        new_flag.clear_block_tablet_in();
        if (OB_FAIL(ls_op.update_ls_flag_in_trans(
                ls_info.get_ls_id(), ls_info.get_ls_flag(), new_flag, trans))) {
          LOG_WARN("failed to update ls flag", KR(ret), K(ls_info));
        }
      }
      ISTAT("rollback flag of the ls", KR(ret), K(ls_info));
    }
    ISTAT("cancel task", KR(ret), K(task), K(task_comment_));
    //clear other init task which parent_list not empty
    if (OB_FAIL(ret)) {
    } else if (skip_next_status) {
    } else if (OB_FAIL(task_comment_.assign_fmt("Canceled on %s status",
            task.get_task_status().to_str()))) {
      LOG_WARN("failed to assign fmt", KR(ret), K(task));
    } else if (OB_FAIL(cancel_other_init_task_(task, trans))) {
      LOG_WARN("failed to cancel other init task", KR(ret), K(task));
    }
  }
  return ret;
}
int ObBalanceTaskExecuteService::cancel_other_init_task_(
    const share::ObBalanceTask &task, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObBalanceTaskArray task_array;
  int tmp_ret = OB_SUCCESS;
  ObSqlString comment;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::get_job_cannot_execute_task(
    tenant_id_, task.get_job_id(), task_array, trans))) {
    LOG_WARN("failed to get job init task", KR(ret), K(tenant_id_), K(task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      comment.reset();
      const ObBalanceTask &other_task = task_array.at(i);
      if (!other_task.get_task_status().is_init()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task parent not empty, must be init", KR(ret), K(other_task));
      } else {
        //set task status to failed
        if (OB_FAIL(comment.assign_fmt("Canceled due to parent task %ld being canceled",
                task.get_balance_task_id().id()))) {
          LOG_WARN("failed to assign fmt", KR(tmp_ret), K(task), K(other_task));
        } else if (OB_FAIL(try_update_task_comment_(other_task, comment, trans))) {
          LOG_WARN("failed to update task comment", KR(tmp_ret), KR(ret), K(task), K(comment));
        } else if (OB_FAIL(update_task_status_(
               other_task,
               share::ObBalanceJobStatus(
                 share::ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELING),
               trans))) {
          LOG_WARN("failed to update task status", KR(ret), K(other_task));
        }
      }
      ISTAT("cancel task", KR(ret), K(other_task), K(task_comment_));
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::process_init_task_(const ObBalanceTask &task,
                                                    ObMySQLTransaction &trans,
                                                    bool &skip_next_status)
{
  int ret = OB_SUCCESS;
  skip_next_status = false;
  ObLSAttrOperator ls_op(tenant_id_, sql_proxy_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid() || !task.get_task_status().is_init())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (task.get_task_type().is_split_task()) {
    //insert a ls attr
    share::ObLSAttr ls_info;
    share::ObLSFlag flag;
    SCN create_scn;
    if (OB_FAIL(wait_can_create_new_ls_(create_scn))) {
      LOG_WARN("failed to wait create new ls", KR(ret), K(tenant_id_));
      if (OB_NEED_WAIT == ret && !task_comment_.empty()) {
        //为了保证task_comment可以更新到表里，先重置错误码
        //但是要跳过日志流的创建，设置skip_next_status等于true
        ret = OB_SUCCESS;
        skip_next_status = true;
      }
    } else if (OB_FAIL(ls_info.init(task.get_dest_ls_id(), task.get_ls_group_id(), flag,
                             share::OB_LS_CREATING, share::OB_LS_OP_CREATE_PRE, create_scn))) {
      LOG_WARN("failed to init new operation", KR(ret), K(create_scn), K(task),
          K(skip_next_status), K(task_comment_));
      //TODO msy164651
    } else if (OB_FAIL(ls_op.insert_ls(ls_info, share::NORMAL_SWITCHOVER_STATUS, &trans))) {
      LOG_WARN("failed to insert new operation", KR(ret), K(ls_info));
    }
    ISTAT("create new ls", KR(ret), K(ls_info), K(task));
  } else if (task.get_task_type().is_alter_task()) {
    share::ObLSAttr ls_info;
    if (OB_FAIL(ls_op.get_ls_attr(task.get_src_ls_id(), true, trans, ls_info))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(task));
    } else if (ls_info.get_ls_group_id() == task.get_ls_group_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group is is same, no need alter", KR(ret), K(ls_info), K(task));
    } else if (OB_FAIL(ls_op.alter_ls_group_in_trans(ls_info,
            task.get_ls_group_id(), trans))) {
      LOG_WARN("failed to alter ls group in trans", KR(ret), K(ls_info), K(task));
    }
    ISTAT("alter ls group id", KR(ret), K(ls_info), K(task));
  } else if (task.get_task_type().is_merge_task()) {
    share::ObLSAttr ls_info;
    if (OB_FAIL(ls_op.get_ls_attr(task.get_src_ls_id(), true, trans, ls_info))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(task));
    } else if (ls_info.get_ls_flag().is_block_tablet_in()) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("ls already in block tablet in", KR(ret), K(ls_info));
    } else {
      share::ObLSFlag new_flag = ls_info.get_ls_flag();
      new_flag.set_block_tablet_in();
      if (OB_FAIL(ls_op.update_ls_flag_in_trans(
          ls_info.get_ls_id(),
          ls_info.get_ls_flag(),
          new_flag,
          trans))) {
        LOG_WARN("failed to update ls flag", KR(ret), K(ls_info));
      }
    }
    ISTAT("update ls flag", KR(ret), K(ls_info), K(task));
  } else if (task.get_task_type().is_transfer_task()) {
    //nothing todo
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task type is invalid", KR(ret), K(task));
  }
  return ret;
}

int ObBalanceTaskExecuteService::wait_ls_to_target_status_(const ObLSID &ls_id,
    const share::ObLSStatus ls_status, bool &skip_next_status)
{
  int ret = OB_SUCCESS;
  skip_next_status = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || share::ls_is_empty_status(ls_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_status));
  } else {
    ObLSStatusInfo status_info;
    ObLSStatusOperator ls_status_op;
    if (OB_FAIL(ls_status_op.get_ls_status_info(tenant_id_, ls_id, status_info, *sql_proxy_))) {
      LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id_), K(ls_id));
    } else if (ls_status == status_info.status_) {
      //nothing
    } else {
      skip_next_status = true;
      if (OB_FAIL(task_comment_.assign_fmt("Wait for status of LS %ld to change from %s to %s",
      ls_id.id(), share::ls_status_to_str(status_info.status_), share::ls_status_to_str(ls_status)))) {
        LOG_WARN("failed to assign fmt", KR(ret), K(ls_id), K(ls_status), K(status_info));
      }
      WSTAT("need wait, ls not in target status", KR(ret), K(ls_status), K(status_info), K(task_comment_));
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::wait_alter_ls_(const share::ObBalanceTask &task, bool &skip_next_status)
{
  int ret = OB_SUCCESS;
  skip_next_status = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!task.get_task_status().is_alter_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else {
    ObLSStatusInfo status_info;
    ObLSStatusOperator ls_status_op;
    if (OB_FAIL(ls_status_op.get_ls_status_info(tenant_id_,
          task.get_src_ls_id(), status_info, *sql_proxy_))) {
      LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id_), K(task));
    } else if (status_info.ls_group_id_ == task.get_ls_group_id()) {
      //nothing
    } else {
      skip_next_status = true;
      if (OB_FAIL(task_comment_.assign_fmt("Wait for LS group id of LS %ld to change from %lu to %lu",
      task.get_src_ls_id().id(), status_info.ls_group_id_, task.get_ls_group_id()))) {
        LOG_WARN("failed to assign sql", KR(ret), K(task), K(status_info));
      }
      WSTAT("need wait, alter ls not ready", KR(ret), K(status_info), K(task), K(task_comment_));
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::execute_transfer_in_trans_(
    const ObBalanceTask &task, const share::ObBalanceJob &job,
    ObMySQLTransaction &trans,
    bool &all_part_transferred)
{
  int ret = OB_SUCCESS;
  ObTenantTransferService *transfer_service = MTL(ObTenantTransferService*);
  const ObTransferTaskID cur_transfer_task_id = task.get_current_transfer_task_id();
  bool transfer_task_executing = false;
  ObTransferPartList transfer_all_part_list;
  ObTransferPartList transfer_finished_part_list;
  ObTransferPartList to_do_part_list;
  ObTransferTask transfer_task;
  all_part_transferred = false;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid()
                         || !task.get_task_status().is_transfer()
                         || !job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task), K(job));
  } else if (OB_ISNULL(transfer_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service is null", KR(ret));
  } else if (OB_FAIL(to_do_part_list.assign(task.get_part_list()))) {
    LOG_WARN("assign failed", KR(ret), K(task));
  } else if (!cur_transfer_task_id.is_valid()) {
    // No transfer task is executing
    // NEED new task when transfer is finished
    all_part_transferred = (task.get_part_list().count() == 0);
  } else if (OB_FAIL(transfer_service->try_clear_transfer_task(
                 cur_transfer_task_id, transfer_task,
                 transfer_all_part_list, transfer_finished_part_list))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("failed to clear transfer task", KR(ret), K(task));
    } else {
      // Transfer task is still executing
      all_part_transferred = false;
      transfer_task_executing = true;
      ret = OB_SUCCESS;
    }
  }
  // Finish current Transfer Task of Balance Task
  else if (OB_FAIL(ObBalanceTaskTableOperator::finish_transfer_task(
               task, cur_transfer_task_id, transfer_finished_part_list, trans,
               to_do_part_list, all_part_transferred))) {
    LOG_WARN("failed to finish tranfer task", KR(ret),
        K(task), K(cur_transfer_task_id), K(transfer_finished_part_list));
  } else if (job.get_job_type().is_transfer_partition()) {
    if (OB_FAIL(finish_transfer_partition_task_(transfer_task, job, trans))) {
      LOG_WARN("failed to finish transfer partition task", KR(ret),
               K(transfer_task), K(job));
    }
  }

  // if transfer is not finished and transfer task not executing, generate new task
  if (OB_SUCC(ret) && !all_part_transferred && !transfer_task_executing) {
    ObTransferTask transfer_task;
    if (OB_FAIL(transfer_service->generate_transfer_task(
        trans,
        task.get_src_ls_id(),
        task.get_dest_ls_id(),
        to_do_part_list,
        task.get_balance_task_id(),
        transfer_task))) {
      LOG_WARN("failed to generate task id", KR(ret), K(to_do_part_list), K(task));
    } else if (OB_FAIL(ObBalanceTaskTableOperator::start_transfer_task(
        tenant_id_,
        task.get_balance_task_id(),
        transfer_task.get_task_id(),
        trans))) {
      LOG_WARN("failed to generate new transfer task", KR(ret), K(tenant_id_), K(task), K(transfer_task));
    } else {
      transfer_service->wakeup();
      if (job.get_job_type().is_transfer_partition()) {
        if (OB_FAIL(try_start_transfer_partition_task_(job, transfer_task.get_part_list(),
                transfer_task.get_task_id(), task.get_dest_ls_id(), trans))) {
          LOG_WARN("failed to start transfer partition task", KR(ret),
           K(to_do_part_list), K(task), K(job), K(transfer_task));
        }
      }
    }
    ISTAT("generate new transfer task", KR(ret), K(transfer_task), K(to_do_part_list), K(task), K(task_comment_));
  }

  // double check for merge task to make sure that all partitions on src_ls have been transferred
  // because some tables (e.g. hidden table) cannot be prevented from being created on the src ls when ls is merging
  if (OB_SUCC(ret) && task.get_task_type().is_merge_task() && all_part_transferred) {
    all_part_transferred = false;
    if (OB_FAIL(get_and_update_merge_ls_part_list_(trans, task, all_part_transferred))) {
      if (OB_NEED_RETRY == ret) {
        all_part_transferred = false;
        ISTAT("get and update merge ls part list failed because schema is old, need retry",
            KR(ret), K(all_part_transferred));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("double check all part transferred failed", KR(ret), K(all_part_transferred));
      }
    }
  }

  return ret;
}


int ObBalanceTaskExecuteService::set_ls_to_merge_(const share::ObBalanceTask &task, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool all_part_transferred = false;
  if (OB_UNLIKELY(!task.get_task_status().is_set_merge_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(get_and_update_merge_ls_part_list_(trans, task, all_part_transferred))) {
    LOG_WARN("get and update merge ls part list failed", KR(ret), K(task), K(all_part_transferred));
  }
  return ret;
}

int ObBalanceTaskExecuteService::get_and_update_merge_ls_part_list_(
    ObMySQLTransaction &trans,
    const share::ObBalanceTask &task,
    bool &all_part_transferred)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  share::ObTransferPartList part_list;
  const ObLSID &src_ls = task.get_src_ls_id();
  schema::ObMultiVersionSchemaService *schemaS = GCTX.schema_service_;

  all_part_transferred = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schemaS) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  }
  // build all part on src LS
  else if (OB_FAIL(ObLSAllPartBuilder::build(tenant_id_, src_ls, *schemaS, *sql_proxy_, part_list))) {
    // need retry, it is normal
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("build all part of src LS fail, need retry", KR(ret), K(src_ls), K(tenant_id_), K(task));
    } else {
      LOG_WARN("build all part of src LS fail", KR(ret), K(src_ls), K(tenant_id_), K(task));
    }
  } else if (0 == part_list.count()) {
    all_part_transferred = true;
    ISTAT("there is no partition on src ls, no need update part list", K(task),
        K(all_part_transferred), K(part_list));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::update_task_part_list(
      tenant_id_,
      task.get_balance_task_id(),
      part_list,
      trans))) {
    LOG_WARN("failed to update merge ls part list", KR(ret), K(tenant_id_), K(task), K(part_list));
  }
  int64_t finish_time = ObTimeUtility::current_time();

  ISTAT("build all part list for src LS of merge task and update finish", KR(ret),
      "cost", finish_time - start_time,
      K_(tenant_id), K(src_ls),
      K(all_part_transferred),
      "part_list_count", part_list.count(),
      K(task));
  return ret;
}

int ObBalanceTaskExecuteService::set_ls_to_dropping_(const ObLSID &ls_id, ObMySQLTransaction &trans)
{
 int ret = OB_SUCCESS;
  ObLSAttrOperator ls_op(tenant_id_, sql_proxy_);
  //TODO exclusion lock of ls
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_op.update_ls_status_in_trans(ls_id, share::OB_LS_NORMAL,
                                                     share::OB_LS_DROPPING,
                                                     share::NORMAL_SWITCHOVER_STATUS,
                                                     trans))) {
    LOG_WARN("failed to update ls status", KR(ret), K(ls_id));
  }
  return ret;
}

//在一个merge_ls在执行的时候，都保证日志流一定会推到wait_offline状态。
//这里的目的不是检查现有的资源是否足够创建日志流，只是为了保证
//这一轮job新创建的日志流的create_scn一定会大于上一轮job产生的wait_offline日志流的offline_scn。
ERRSIM_POINT_DEF(EN_SET_MAX_OFFLINE_SCN);
int ObBalanceTaskExecuteService::wait_can_create_new_ls_(share::SCN &create_scn)
{
  int ret = OB_SUCCESS;
  create_scn.reset();
  share::SCN offline_scn;
  int64_t offline_ls_count = 0;
  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (cluster_version < CLUSTER_VERSION_4_2_2_0) {
    //版本号没有推到4220版本，获取不到offline_scn
    if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id_, create_scn))) {
      LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(get_max_offline_scn_(offline_scn, offline_ls_count))) {
    LOG_WARN("failed to get max offline scn", KR(ret));
  } else if (0 == offline_ls_count) {
    if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id_, create_scn))) {
      LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
    }
  } else if (OB_UNLIKELY(!offline_scn.is_valid() || 0 > offline_ls_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offline scn is invalid", KR(ret), K(offline_scn), K(offline_ls_count));
  } else {
    const int64_t start_time = ObTimeUtility::fast_current_time();
    const int64_t TIMEOUT = GCONF.rpc_timeout;
    //for test
    if (EN_SET_MAX_OFFLINE_SCN) {
      LOG_INFO("set offline scn to max", K(offline_scn));
      offline_scn.set_max();
    }
    do {
      if (ObTimeUtility::fast_current_time() - start_time > TIMEOUT) {
        ret = OB_NEED_WAIT;
        LOG_WARN("stmt is timeout", KR(ret), K(start_time), K(TIMEOUT),
            K(create_scn), K(offline_scn));
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(task_comment_.assign(
                "Wait timed out for GTS to exceed max offline scn for all LS"))) {
          LOG_WARN("failed to assign task comment", KR(tmp_ret), K(offline_scn));
        }
      } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id_, create_scn))) {
        LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
      } else if (create_scn > offline_scn) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_EAGAIN;
        LOG_WARN("create scn is smaller than offline scn, need wait", KR(ret),
            K(create_scn), K(offline_scn), K(offline_ls_count));
        // waiting 100ms
        ob_usleep(100L * 1000L);
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

//finish transfer partition task 只有在目的端匹配的情况下才可以
int ObBalanceTaskExecuteService::finish_transfer_partition_task_(
    const ObTransferTask &transfer_task, const share::ObBalanceJob &job,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!transfer_task.is_valid()
        || !transfer_task.get_status().is_finish_status()
        || !job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(transfer_task), K(job));
  }
  const ObTransferPartList &not_exist_list = transfer_task.get_not_exist_part_list();
  const ObTransferPartList &finish_list = transfer_task.get_part_list();

  if (OB_SUCC(ret) && not_exist_list.count() > 0) {
    //这里的分区不存在不一定是分区真的不存在，可能仅是不在源端日志流上
    //这里需要double check
    ObString comment("Need retry, partition may be dropped or be transferred");
    ISTAT("Part not exist during transfer, try again", K(not_exist_list));
    if (OB_FAIL(ObTransferPartitionTaskTableOperator::rollback_from_doing_to_waiting(
            tenant_id_, job.get_job_id(), not_exist_list, comment, trans))) {
      LOG_WARN("failed to finish task", KR(ret), K(tenant_id_),
          K(not_exist_list), K(transfer_task), K(job), K(comment));
    }
  }
  if (OB_SUCC(ret) && transfer_task.get_status().is_completed_status()
      && finish_list.count() > 0) {
    ObString comment("Partition transfer to dest LS");
    ObTransferPartitionTaskStatus status =
      ObTransferPartitionTaskStatus::TRP_TASK_STATUS_COMPLETED;
    ObLSID task_dest_ls;
    ObTransferPartList new_finish_list;
    ObTransferPartitionTaskID max_task_id;
    if (OB_FAIL(load_finish_transfer_part_tasks_(transfer_task, job,
            new_finish_list, max_task_id, task_dest_ls, trans))) {
      LOG_WARN("failed to get finish tranfer part task", KR(ret), K(transfer_task), K(job));
    } else if (0 == new_finish_list.count()) {
      ISTAT("all transfer partition may be dropped", K(transfer_task));
    } else if (transfer_task.get_dest_ls() != task_dest_ls) {
      // 一个transfer partition任务可能会有多次transfer
      // 任务可以执行成功，一定是transfer到目的端
      LOG_INFO("dest ls not match, no need finish transfer task", KR(ret),
          K(task_dest_ls), K(transfer_task));
    } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::finish_task(
            tenant_id_, new_finish_list, max_task_id, status, comment, trans))) {
      LOG_WARN("failed to finish task", KR(ret), K(tenant_id_),
          K(new_finish_list), K(max_task_id));
    }
  } else {
    //other status no need finish task
  }
  return ret;
}

int ObBalanceTaskExecuteService::get_max_offline_scn_(share::SCN &offline_scn, int64_t &offline_ls_count)
{
  int ret = OB_SUCCESS;
  offline_scn.reset();
  offline_ls_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)
      || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_),
        KP(GCTX.location_service_));
  } else {
   ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    ObArray<int> return_code_array;
    if (OB_FAIL(get_ls_offline_scn_by_rpc_(proxy, offline_ls_count, return_code_array))) {
      LOG_WARN("failed to get ls offline scn", KR(ret));
    } else if (0 == offline_ls_count) {
      //nothing todo
    } else if (return_code_array.count() != offline_ls_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("offline count not equal to return code array", KR(ret),
          K(offline_ls_count), K(return_code_array));
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret),
          "return_cnt", return_code_array.count());
    } else {
      offline_scn.set_min();
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        if (OB_FAIL(return_code_array.at(i))) {
          LOG_WARN("send rpc is failed", KR(ret), K(i));
        } else {
          const obrpc::ObGetLSReplayedScnRes *result = proxy.get_results().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", KR(ret), K(i));
          } else if (!result->get_offline_scn().is_valid()) {
            ret = OB_NEED_WAIT;
            LOG_WARN("offline scn is invalid", KR(ret), KPC(result));
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(task_comment_.assign_fmt("Wait for LS %ld to write offline log",
                    result->get_ls_id().id()))) {
              LOG_WARN("failed to assign task comment", KR(tmp_ret), KPC(result));
            }
          } else if (result->get_offline_scn() > offline_scn) {
            offline_scn = result->get_offline_scn();
            LOG_INFO("get offline scn", K(offline_scn), KPC(result));
          }
        }
      }//end for
    }
  }

  return ret;
}

int ObBalanceTaskExecuteService::get_ls_offline_scn_by_rpc_(
    ObGetLSReplayedScnProxy &proxy,
    int64_t &offline_ls_count,
    ObIArray<int> &return_code_array)
{
  int ret = OB_SUCCESS;
  offline_ls_count = 0;
  ObArray<ObLSStatusInfo> status_info_array;
  ObLSStatusOperator ls_status_op;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)
      || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_),
        KP(GCTX.location_service_));
  } else if (OB_FAIL(ls_status_op.get_all_ls_status_by_order(tenant_id_,
          status_info_array, *sql_proxy_))) {
    LOG_WARN("failed to get ls status info array", KR(ret), K(tenant_id_));
  } else {
    obrpc::ObGetLSReplayedScnArg arg;
    ObAddr leader;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      ObLSStatusInfo &info = status_info_array.at(i);
      if (info.ls_is_dropping()) {
        //存在dropping状态的日志流，不确定是不是上一轮负载均衡任务的残留
        //先等dropping变成wait_offline吧
        ret = OB_NEED_WAIT;
        LOG_WARN("has dropping ls, need wait", K(ret), K(info));
        if (OB_TMP_FAIL(task_comment_.assign_fmt("Wait for LS %ld in DROPPING status to become OFFLINE",
                info.ls_id_.id()))) {
          LOG_WARN("failed to assign task comment", KR(tmp_ret), K(info));
        }
      } else if (!info.ls_is_wait_offline()) {
        //负载均衡过程中不会存在tenant_dropping的状态,其他状态不考虑
      } else {
        offline_ls_count++;
        const int64_t timeout = ctx.get_timeout();
        if (OB_FAIL(arg.init(tenant_id_, info.ls_id_, false))) {
          LOG_WARN("failed to init arg", KR(ret), K(arg));
          //实际上没有必要一定是leader副本，只是去leader上比较方面，所以只要
          //获取回来就不用校验
        } else if (OB_FAIL(GCTX.location_service_->get_leader(
                GCONF.cluster_id, tenant_id_, info.ls_id_, false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id_), K(info));
        } else if (OB_FAIL(proxy.call(leader, timeout, tenant_id_, arg))) {
          LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout),
              K(tenant_id_), K(arg));
        }
        if (OB_FAIL(ret)) {
          if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(
                  GCONF.cluster_id, tenant_id_, info.ls_id_))) {
            LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id_), K(info));
          }
        }
      }//end else
    }//end for
    if (0 == offline_ls_count) {
      //nothing todo
    } else if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBalanceTaskExecuteService::load_finish_transfer_part_tasks_(
    const ObTransferTask &transfer_task,
    const share::ObBalanceJob &job,
    ObTransferPartList &new_finish_list,
    ObTransferPartitionTaskID &max_task_id,
    ObLSID &dest_ls, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  new_finish_list.reset();
  max_task_id.reset();
  dest_ls.reset();
  ObArray<ObTransferPartitionTask> task_array;
  const ObTransferPartList &finish_list = transfer_task.get_part_list();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!transfer_task.is_valid()
        || !transfer_task.get_status().is_completed_status()
        || !job.is_valid() || !job.get_job_type().is_transfer_partition()
        || 0 >= finish_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(transfer_task), K(job));
  } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::load_part_list_task(
          tenant_id_, job.get_job_id(), finish_list, task_array, trans))) {
    LOG_WARN("failed to get part list task", KR(ret), K(tenant_id_),
        K(finish_list));
  } else if (0 == task_array.count()) {
    //可能所有相关的任务的分区都已经被删除，不需要在处理transfer_partition表
  } else {
    //检查所有的分区目的端是否一致
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      const ObTransferPartitionTask &task = task_array.at(i);
      if (dest_ls.is_valid() && task.get_dest_ls() != dest_ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task need have same dest ls", KR(ret), K(task),
            K(dest_ls), K(task_array));
      } else if (OB_FAIL(new_finish_list.push_back(task.get_part_info()))) {
        LOG_WARN("failed to push back", KR(ret), K(task));
      } else {
        dest_ls = task.get_dest_ls();
        if (!max_task_id.is_valid() || max_task_id < task.get_task_id()) {
          max_task_id = task.get_task_id();
        }
      }
    }//end for
  }
  return ret;
}

//start可以有多次
int ObBalanceTaskExecuteService::try_start_transfer_partition_task_(
    const share::ObBalanceJob &job,
    const ObTransferPartList &part_list,
    const ObTransferTaskID &transfer_id,
    const ObLSID &dest_ls,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!transfer_id.is_valid() || 0 >= part_list.count()
        || !dest_ls.is_valid() || !job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer task is invalid", KR(ret), K(transfer_id),
        K(dest_ls), K(part_list), K(job));
  } else if (OB_FAIL(
        ObTransferPartitionTaskTableOperator::start_transfer_task(
          tenant_id_, job.get_job_id(), part_list, transfer_id, trans))) {
    LOG_WARN("failed to start transfer task", KR(ret), K(tenant_id_),
          K(part_list), K(transfer_id), K(job));
  }
  return ret;
}

}
}

