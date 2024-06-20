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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/cmd/ob_load_data_rpc.h"

#include "observer/ob_server_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/serialization.h"
#include "share/ob_tenant_mgr.h"
#include "storage/access/ob_dml_param.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "lib/string/ob_string.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace sql
{

int ObLoadbuffer::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t len = src.length() + 1;
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src string length is invalid", K(ret), K(src), K(len));
  } else if (NULL == (buf = static_cast<char*>(field_data_allocator_.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory, ", K(ret), K(src), K(len));
  } else {
    MEMCPY(buf, src.ptr(), len - 1);
    buf[len - 1] = '\0';
    dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
  }
  return ret;
}

int ObLoadbuffer::store_row(const ObIArray<ObString> &row_strs,
                            int64_t cur_line_number)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_column_num_; ++i) {
    const ObString &src_str = row_strs.at(i);
    ObString &dst_str = insert_values_.at(stored_pos_);
    if (OB_FAIL(deep_copy_str(src_str, dst_str))) {
      LOG_WARN("deep copy string failed", K(ret));
    }
    stored_pos_++;
  }
  file_line_number_.at(stored_row_cnt_) = cur_line_number;
  stored_row_cnt_++;
  return ret;
}

int ObLoadbuffer::set_allocator_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant_id must be init first", K(tenant_id_));
  } else {
    field_data_allocator_.set_tenant_id(tenant_id_);
    array_allocator_.set_tenant_id(tenant_id_);
  }
  return ret;
}

int ObLoadbuffer::prepare_insert_info(const ObIArray<ObString> &column_names, ObExprValueBitSet &expr_value_bitset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_bitset_.add_members(expr_value_bitset))) {
    LOG_WARN("prepare expr bitset failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
    if (OB_FAIL(insert_column_names_.push_back(column_names.at(i)))) {
      LOG_WARN("insert column names push failed", K(ret));
    }
  }
  return ret;
}

int ObLoadbuffer::init_array()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < LOAD_BUFFER_MAX_ROW_COUNT * insert_column_num_; ++i) {
    if (OB_FAIL(insert_values_.push_back(ObString::make_empty_string()))) {
      LOG_WARN("insert values push empty string failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < LOAD_BUFFER_MAX_ROW_COUNT; ++i) {
    if (OB_FAIL(file_line_number_.push_back(-1))) {
      LOG_WARN("insert values push empty string failed", K(ret));
    }
  }
  return ret;
}

int ObRpcLoadDataShuffleTaskExecuteP::process()
{
  UNUSED(gctx_);
  int ret = OB_SUCCESS;
  ObShuffleTask &task = arg_;
  //ObShuffleResult &result = result_;
  ObShuffleTaskHandle *handle = nullptr;
  ObLoadDataStat *job_status = nullptr;

  LOG_DEBUG("LOAD DATA receiving shuffle task", "task_id", task.task_id_);

  if (OB_FAIL(ObGlobalLoadDataStatMap::getInstance()->get_job_status(task.gid_, job_status))) {
    LOG_WARN("fail to get job, main thread has already quit", K(ret), K(task));
  } else if (OB_ISNULL(job_status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job status is null", K(ret));
  } else {
    if (OB_FAIL(task.shuffle_task_handle_.get_arg(handle))) { //check identifier
      LOG_ERROR("fail to get arg", K(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("handle is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard_))) {
      //Confirmed with the load data owner that the inability to calculate the correct tablet_id here will not affect the execution,
      //so we use the latest schema version to obtain the guard
      LOG_WARN("get tenant schema guard failed", KR(ret));
    } else  {
      handle->exec_ctx.get_sql_ctx()->schema_guard_ = &schema_guard_;
      if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
        ret = OB_TIMEOUT;
        LOG_WARN("LOAD DATA shuffle task timeout", K(ret), K(task));
      } else if (OB_FAIL(ObLoadDataSPImpl::exec_shuffle(task.task_id_, handle))) {
        LOG_WARN("fail to exec shuffle task", K(ret));
      }
      handle->result.exec_ret_ = ret;
    }
    MEM_BARRIER(); //use handle ptr before release job ref
    job_status->release();
  }

  return OB_SUCCESS;
}

int ObRpcLoadDataShuffleTaskCallBack::release_resouce()
{
  int ret = OB_SUCCESS;
  int ret1 = complete_task_list_.push_back(handle_);
  MEM_BARRIER();
  int ret2 = task_controller_.on_task_finished();
  if (OB_FAIL(ret1) || OB_FAIL(ret2)) {
    LOG_ERROR("shuffle call back release resource failed", K(ret1), K(ret2));
  }
  return ret;
}

int ObRpcLoadDataShuffleTaskCallBack::process() {
  //int ret = OB_SUCCESS;
  //ObShuffleResult &result = result_;
  LOG_DEBUG("LOAD DATA shuffle task callback process");
  handle_->result.process_us_ = ObTimeUtil::current_time() - get_send_ts();
  //handle_->result = result;
  release_resouce();

  return OB_SUCCESS;
}

void ObRpcLoadDataShuffleTaskCallBack::on_timeout() {
  LOG_WARN_RET(OB_TIMEOUT, "LOAD DATA main thread shuffle task rpc timeout");
  if (OB_NOT_NULL(handle_)) {
    handle_->result.flags_.set_bit(ObTaskResFlag::RPC_TIMEOUT);
  }
  release_resouce();
}

void ObRpcLoadDataShuffleTaskCallBack::set_args(const Request &arg)
{
  UNUSED(arg);
}

int ObRpcLoadDataInsertTaskCallBack::release_resouce()
{
  int ret = OB_SUCCESS;
  int ret1 = complete_task_list_.push_back(insert_task_);
  MEM_BARRIER();
  int ret2 = task_controller_.on_task_finished();
  if (OB_FAIL(ret1) || OB_FAIL(ret2)) {
    LOG_ERROR("push back buffer failed", K(ret1), K(ret2));
  }
  return ret;
}

int ObRpcLoadDataInsertTaskCallBack::process() {
  int ret = OB_SUCCESS;
  LOG_DEBUG("LOAD DATA insert task callback process", K(result_));
  if (OB_NOT_NULL(insert_task_)) {
    if (OB_FAIL(insert_task_->result_.assign(result_))) {
      LOG_WARN("fail to assign result", K(ret));
    } else {
      insert_task_->result_recv_ts_ = ObTimeUtil::current_time();
      insert_task_->process_us_ = insert_task_->result_recv_ts_ - get_send_ts();
    }
  }
  release_resouce();
  return OB_SUCCESS;
}

void ObRpcLoadDataInsertTaskCallBack::set_args(const Request &arg)
{
  UNUSED(arg);
}

void ObRpcLoadDataInsertTaskCallBack::on_timeout() {
  int64_t task_id = -2; //undefined
  if (OB_ISNULL(insert_task_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "insert task is null on timeout");
  } else {
    task_id = insert_task_->task_id_;
    insert_task_->result_.flags_.set_bit(ObTaskResFlag::RPC_TIMEOUT);
  }
  LOG_WARN_RET(OB_TIMEOUT, "LOAD DATA main thread insert task rpc timeout", K(task_id));
  release_resouce();
}


int ObRpcLoadDataTaskExecuteP::process()
{
  int ret = OB_SUCCESS;
  ObLoadbuffer &buffer = arg_;
  ObLoadResult &result = result_;
  ObIArray<ObString> &column_names = buffer.get_insert_keys();
  ObIArray<ObString> &values = buffer.get_insert_values();
  ObString &table_name = buffer.get_table_name();
  ObExprValueBitSet &expr_bitset = buffer.get_expr_bitset();
  uint64_t tenant_id = buffer.get_tenant_id();
  int64_t task_id = buffer.get_task_id();
  ObTabletID tablet_id = buffer.get_tablet_id();
  ObLoadDupActionType insert_mode = buffer.get_load_mode();
  int64_t insert_column_number = column_names.count();
  int64_t total_values_number = buffer.get_stored_pos();  //values.count() will always be 1000
  int64_t total_row_number = buffer.get_stored_row_count();
  int64_t succ_row_count = 0;
  int need_wait_minor_freeze = false;

  if (OB_UNLIKELY(total_values_number <= 0)
      || OB_UNLIKELY(insert_column_number <= 0)
      || OB_UNLIKELY(total_values_number != insert_column_number * (total_values_number / insert_column_number))
      || OB_UNLIKELY(total_row_number != total_values_number / insert_column_number)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert values are invalid", K(ret), K(total_values_number),
                                          K(insert_column_number), K(total_row_number), K(buffer));
  } else if (!escape_data_buffer_.set_data(str_buf_, OB_MAX_DEFAULT_VALUE_LENGTH)) { //for escaped value
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid escape_buf", K(ret));
  } else {
    //1. try insert as one stmt
    ObSqlString batch_insert_sql;
    int exec_ret = OB_SUCCESS;
    int64_t affected_rows = 0;
    if (OB_FAIL(ObLoadDataUtils::build_insert_sql_string_head(insert_mode,
                                                              table_name,
                                                              column_names,
                                                              batch_insert_sql))) {
      LOG_WARN("gen insert sql column_names failed", K(ret));
    } else if (OB_FAIL(ObLoadDataUtils::append_values_in_remote_process(insert_column_number,
                                                                        total_values_number,
                                                                        expr_bitset,
                                                                        values,
                                                                        batch_insert_sql,
                                                                        escape_data_buffer_))) {
      LOG_WARN("append values failed", K(ret));
    } else if (OB_UNLIKELY(OB_SUCCESS != (exec_ret = gctx_.sql_proxy_->write(tenant_id,
                                                                             batch_insert_sql.ptr(),
                                                                             affected_rows,
                                                                             get_compatibility_mode())))) {
      LOG_WARN("fail to execute insert sql in batch", K(exec_ret), K(affected_rows), K(task_id), K(get_compatibility_mode()));
    } else {
      //succ!
      succ_row_count = affected_rows;
    }

    //2. if failed, try insert separately
    if (OB_SUCC(ret) && OB_SUCCESS != exec_ret) {
      ObSqlString seperate_insert_sql_head;
      ObSqlString seperate_insert_sql;

      if (OB_FAIL(ObLoadDataUtils::build_insert_sql_string_head(insert_mode,
                                                                table_name,
                                                                column_names,
                                                                seperate_insert_sql_head))) {
        LOG_WARN("gen insert sql column_names failed", K(ret));
      }
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < total_row_number; ++row_idx) {
        if (OB_FAIL(seperate_insert_sql.assign(seperate_insert_sql_head.string()))) {
          LOG_WARN("assign insert column_names failed", K(ret));
        } else if (OB_FAIL(ObLoadDataUtils::append_values_in_remote_process(insert_column_number,
                                                                            insert_column_number,
                                                                            expr_bitset,
                                                                            values,
                                                                            seperate_insert_sql,
                                                                            escape_data_buffer_,
                                                                            row_idx))) {
          LOG_WARN("append values failed", K(ret));
        } else {
          exec_ret = OB_SUCCESS;
          affected_rows = 0;
          if (OB_UNLIKELY(OB_SUCCESS != (exec_ret = gctx_.sql_proxy_->write(tenant_id,
                                                                            seperate_insert_sql.ptr(),
                                                                            affected_rows,
                                                                            get_compatibility_mode())))) {
            LOG_WARN("LOAD DATA row insert failed in remote process", K(exec_ret), K(seperate_insert_sql), K(get_compatibility_mode()));
            if (OB_FAIL(result.row_number_.push_back(static_cast<int16_t>(row_idx)))) {
              LOG_WARN("push back row number failed", K(ret));
            } else if (OB_FAIL(result.row_err_code_.push_back(exec_ret))) {
              LOG_WARN("push back row err code failed", K(ret));
            }
          } else {
            //succ!
            succ_row_count++;
          }
        }
      }
    }
  }
  //check memory
  int memory_check_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  storage::ObTenantFreezer *freezer = nullptr;
  if (tenant_id != MTL_ID()) {
    memory_check_ret = guard.switch_to(tenant_id);
  }
  if (OB_UNLIKELY(OB_SUCCESS != memory_check_ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("switch tenant failed", K(tenant_id), K(memory_check_ret));
  } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
  } else {
    int64_t active_memstore_used = 0;
    int64_t total_memstore_used = 0;
    int64_t major_freeze_trigger = 0;
    int64_t memstore_limit = 0;
    int64_t freeze_cnt = 0;
    if (OB_UNLIKELY(OB_SUCCESS != (memory_check_ret =
                                   freezer->get_tenant_memstore_cond(active_memstore_used,
                                                                     total_memstore_used,
                                                                     major_freeze_trigger,
                                                                     memstore_limit,
                                                                     freeze_cnt)))) {
      LOG_WARN("fail to get memstore used", K(memory_check_ret));
    } else {
      if (static_cast<double>(total_memstore_used)
          > static_cast<double>(major_freeze_trigger) * 1.02) {
        need_wait_minor_freeze = true;
      }
    }
    LOG_DEBUG("load data check tenant memory usage", K(active_memstore_used),
                                                     K(total_memstore_used),
                                                     K(major_freeze_trigger),
                                                     K(memstore_limit),
                                                     K(freeze_cnt));
  }
  if (OB_SUCC(ret) && OB_SUCCESS != memory_check_ret) {
    ret = memory_check_ret; //cover ret
  }
  //summarize return value
  if (OB_FAIL(ret)) {
    ObLoadDataUtils::set_flag(result.task_flags_,
                              static_cast<int64_t>(ObLoadTaskResultFlag::RPC_REMOTE_PROCESS_ERROR));
  }
  if (need_wait_minor_freeze) {
    ObLoadDataUtils::set_flag(result.task_flags_,
                              static_cast<int64_t>(ObLoadTaskResultFlag::NEED_WAIT_MINOR_FREEZE));
  }
  if (succ_row_count != total_row_number) {
    ObLoadDataUtils::set_flag(result.task_flags_,
                              static_cast<int64_t>(ObLoadTaskResultFlag::HAS_FAILED_ROW));
  }
  if (succ_row_count == 0) {
    ObLoadDataUtils::set_flag(result.task_flags_,
                              static_cast<int64_t>(ObLoadTaskResultFlag::ALL_ROWS_FAILED));
  }
  result.affected_rows_ = succ_row_count;
  result.failed_rows_ = total_row_number - succ_row_count;
  result.task_id_ = task_id;
  result.tablet_id_ = tablet_id;
  LOG_DEBUG("load data in remote process!", K(task_id), K(succ_row_count),
            K(total_row_number), K(need_wait_minor_freeze), K(lib::is_oracle_mode()));
  return OB_SUCCESS;
}

OB_INLINE void ObRpcLoadDataTaskCallBack::set_args(const Request &arg)
{
  UNUSED(arg);
}

void ObRpcLoadDataTaskCallBack::on_timeout()
{
  int ret = OB_SUCCESS;
  ObLoadbuffer *buffer = static_cast<ObLoadbuffer*>(request_buffer_ptr_);
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("buffer is null", K(ret));
  } else {
    buffer->set_returned_timestamp(ObTimeUtility::current_time());
    ObLoadDataUtils::set_flag(buffer->get_task_status(),
                              static_cast<int64_t>(ObLoadTaskResultFlag::TIMEOUT));
    ObLoadDataUtils::set_flag(buffer->get_task_status(),
                              static_cast<int64_t>(ObLoadTaskResultFlag::HAS_FAILED_ROW));
    ObLoadDataUtils::set_flag(buffer->get_task_status(),
                              static_cast<int64_t>(ObLoadTaskResultFlag::ALL_ROWS_FAILED));
    //TODO wjh: check task real status, is all rows truely failed?
    if (OB_FAIL(complete_task_list_.push_back(buffer))) {
      LOG_ERROR("push back buffer failed", K(ret));
    }
    MEM_BARRIER();
    if (OB_FAIL(task_controller_.on_task_finished())) {
      // overwrite ret
      LOG_ERROR("error on task finish", K(ret));
    }
    LOG_WARN("load data task is timeout", K(ret), KPC(buffer));
  }
}

int ObRpcLoadDataTaskCallBack::process()
{
  int ret = OB_SUCCESS;
  ObLoadResult &result = result_;
  ObLoadbuffer *buffer = static_cast<ObLoadbuffer*>(request_buffer_ptr_);
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task buffer is null", K(ret));
  } else {
    buffer->set_returned_timestamp(ObTimeUtility::current_time());
    buffer->set_task_status(result.task_flags_);
    if (OB_UNLIKELY(ObLoadDataUtils::has_flag(buffer->get_task_status(),
                                              static_cast<int64_t>(ObLoadTaskResultFlag::HAS_FAILED_ROW)))) {
      if (OB_UNLIKELY(result.failed_rows_ != result.row_err_code_.count()
                      || result.failed_rows_ != result.row_number_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of failed rows is not the same, impossible");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < result.failed_rows_; ++i) {
          if (OB_FAIL(buffer->get_failed_row_idx().push_back(result.row_number_.at(i)))) {
            LOG_WARN("push back failed", K(ret));
          } else if (OB_FAIL(buffer->get_error_code_array().push_back(result.row_err_code_.at(i)))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      ObLoadDataUtils::set_flag(buffer->get_task_status(),
                                static_cast<int64_t>(ObLoadTaskResultFlag::RPC_CALLBACK_PROCESS_ERROR));
    }

    //whatever ret is, the task has finished
    int second_ret = OB_SUCCESS;
    if (OB_SUCCESS != (second_ret = complete_task_list_.push_back(buffer))) {
      //buffer will be auto released by the allocator
      if (OB_SUCCESS == ret) {
        ret = second_ret;
      }
      LOG_ERROR("push to complete list failed", K(second_ret));
    }
    MEM_BARRIER();
    if (OB_SUCCESS != (second_ret = task_controller_.on_task_finished())) {
      //signal main thread failed, fatal error. TODO wjh: handle this error
      if (OB_SUCCESS == ret) {
        ret = second_ret;
      }
      LOG_ERROR("on task finished failed", K(ret));
    }
  }
  LOG_DEBUG("in local cb process!", K(result), K(ret));
  return OB_SUCCESS;
}

int ObParallelTaskController::init(int64_t max_parallelism)
{
  int ret = OB_SUCCESS;
  max_parallelism_ = max_parallelism;
  if (OB_FAIL(vacant_cond_.init(common::ObWaitEventIds::ASYNC_RPC_PROXY_COND_WAIT))) {
    LOG_WARN("init vacant cond failed", K(ret));
  }
  return ret;
}

int ObParallelTaskController::on_next_task()
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(vacant_cond_);

  if (ATOMIC_AAF(&processing_cnt_, 1) > max_parallelism_) {
    ret = vacant_cond_.wait();
  }

  return ret;
}

void ObParallelTaskController::wait_all_task_finish(const char *task_name, int64_t until_ts)
{
  int64_t wait_duration_ms = 0;
  int64_t begin_ts = ObTimeUtil::current_time();
  int64_t processing_count = 0;
  bool is_too_long = false;
  LOG_DEBUG("start wait_all_task_finish", K(task_name));
  while ((processing_count = get_processing_task_cnt()) > 0) {
    ob_usleep(1000 * 10); //wait 10m
    wait_duration_ms += 10;
    if (0 == wait_duration_ms % 1000) {
      int64_t current_ts = ObTimeUtil::current_time();
      if (current_ts > until_ts) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "waiting load data task too long and exceed max waiting timestamp",
                  K(begin_ts), K(until_ts), K(current_ts));
      }
    }
    if (!is_too_long && wait_duration_ms > 10 * 1000) {
      is_too_long = true;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "LOAD DATA, waiting task finish too long",
               K(task_name), K(processing_count), K(wait_duration_ms), K(until_ts));
    }
  }
  if (is_too_long) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "LOAD DATA finish waiting long task", K(wait_duration_ms));
  }
}


int ObParallelTaskController::on_task_finished()
{
  int ret = OB_SUCCESS;
  if(max_parallelism_ == ATOMIC_AAF(&processing_cnt_, -1)) {
    ObThreadCondGuard guard(vacant_cond_);
    ret = vacant_cond_.signal();
  }
  return ret;
}

int ObRpcLoadDataInsertTaskExecuteP::process()
{
  UNUSED(gctx_);
  int ret = OB_SUCCESS;
  ObInsertTask &task = arg_;
  ObInsertResult &result = result_;
  ObWarningBuffer *warning_buf = NULL;
  bool need_wait_freeze = false;

  LOG_DEBUG("LOAD DATA receiving insert task", "task_id", task.task_id_);

  if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
    ret = OB_TIMEOUT;
    LOG_WARN("LOAD DATA shuffle task timeout", K(ret), K(task));
  } else if (OB_FAIL(ObLoadDataSPImpl::exec_insert(task, result))) {
    LOG_WARN("fail to exec insert", K(ret));
  }

  result.exec_ret_ = ret;

  if (OB_FAIL(ret) && OB_NOT_NULL(warning_buf = ob_get_tsi_warning_buffer())) {
    int ret_backup = ret;
    result.err_line_no_ = warning_buf->get_error_line();
    OZ (ob_write_string(result.allocator_, warning_buf->get_err_msg(), result.err_msg_));
    ret = ret_backup;
    LOG_DEBUG("check warning buffer", "msg", warning_buf->get_err_msg(),
              "line", warning_buf->get_error_line(),
              "column", warning_buf->get_error_column(),
              "code", warning_buf->get_err_code());
  }

  int temp_ret = ObLoadDataBase::memory_check_remote(task.tenant_id_, need_wait_freeze);
  if (OB_SUCCESS != temp_ret) {
    LOG_WARN("LOAD DATA remote memory check failed", K(temp_ret), K(task.tenant_id_));
  }

  result.flags_.reset();
  if (need_wait_freeze) {
    result.flags_.set_bit(ObTaskResFlag::NEED_WAIT_MINOR_FREEZE);
  }

  return OB_SUCCESS;
}

int ObInsertResult::assign(const ObInsertResult &other)
{
  int ret = OB_SUCCESS;
  flags_ = other.flags_;
  exec_ret_ = other.exec_ret_;
  err_line_no_ = other.err_line_no_;

  if (OB_FAIL(ob_write_string(allocator_, other.err_msg_, err_msg_))) {
    LOG_WARN("fail to write string", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE(ObLoadbuffer)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              task_id_,
              tablet_id_,
              table_id_,
              tenant_id_,
              stored_pos_,
              stored_row_cnt_,
              insert_mode_,
              insert_column_num_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_name_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize error", K(ret));
  } else if (OB_FAIL(insert_column_names_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize column names error", K(ret));
  } else if (OB_FAIL(insert_values_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize row store error", K(ret));
  } else if (OB_FAIL(expr_bitset_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize expr bitset error", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadbuffer)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              task_id_,
              tablet_id_,
              table_id_,
              tenant_id_,
              stored_pos_,
              stored_row_cnt_,
              insert_mode_,
              insert_column_num_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_name_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize error", K(ret));
  } else if (OB_FAIL(insert_column_names_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize row store error", K(ret));
  } else if (OB_FAIL(insert_values_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize row store error", K(ret));
  } else if (OB_FAIL(expr_bitset_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize expr bitset error", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadbuffer)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              task_id_,
              tablet_id_,
              table_id_,
              tenant_id_,
              stored_pos_,
              stored_row_cnt_,
              insert_mode_,
              insert_column_num_);
  len += table_name_.get_serialize_size();
  len += insert_column_names_.get_serialize_size();
  len += insert_values_.get_serialize_size();
  len += expr_bitset_.get_serialize_size();
  return len;
}

OB_DEF_SERIALIZE(ObLoadResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              task_id_,
              tablet_id_,
              affected_rows_,
              failed_rows_,
              task_flags_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_number_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize row_number_ error", K(ret));
  } else if (OB_FAIL(row_err_code_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize row_err_code_ error", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              task_id_,
              tablet_id_,
              affected_rows_,
              failed_rows_,
              task_flags_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_number_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize row_number_ error", K(ret));
  } else if (OB_FAIL(row_err_code_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize row_err_code_ error", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadResult)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              task_id_,
              tablet_id_,
              affected_rows_,
              failed_rows_,
              task_flags_);
  len += row_number_.get_serialize_size();
  len += row_err_code_.get_serialize_size();
  return len;
}

OB_SERIALIZE_MEMBER(ObShuffleTask,
                    task_id_,
                    shuffle_task_handle_,
                    gid_);
OB_SERIALIZE_MEMBER(ObShuffleResult,
                    task_id_,
                    flags_,
                    exec_ret_,
                    row_cnt_);

OB_SERIALIZE_MEMBER(ObInsertTask,
                    tenant_id_,
                    task_id_,
                    row_count_,
                    column_count_,
                    insert_stmt_head_,
                    insert_value_data_,
                    timezone_,
                    sql_mode_);
OB_SERIALIZE_MEMBER(ObInsertResult,
                    flags_,
                    exec_ret_,
                    err_line_no_,
                    err_msg_);

}
}
