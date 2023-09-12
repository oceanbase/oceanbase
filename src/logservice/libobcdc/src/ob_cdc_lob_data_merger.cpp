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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_data_merger.h"
#include "ob_cdc_lob_aux_meta_storager.h"    // ObCDCLobAuxMetaStorager
#include "ob_log_instance.h"                 // TCTX
#include "ob_log_formatter.h"                // IObLogFormatter
#include "ob_log_trace_id.h"                 // ObLogTraceIdGuard

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
ObCDCLobDataMerger::ObCDCLobDataMerger() :
    is_inited_(false),
    round_value_(0),
    lob_data_list_task_count_(0),
    err_handler_(nullptr)
{
}

ObCDCLobDataMerger::~ObCDCLobDataMerger()
{
  destroy();
}

int ObCDCLobDataMerger::init(
    const int64_t thread_num,
    const int64_t queue_size,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCLobDataMerger has been initialized", KR(ret));
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(LobDataMergerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init LobDataMergerThread queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else {
    round_value_ = 0;
    err_handler_ = &err_handler;
    is_inited_ = true;

    LOG_INFO("ObCDCLobDataMerger init succ", K(thread_num), K(queue_size));
  }

  return ret;
}

void ObCDCLobDataMerger::destroy()
{
  if (is_inited_) {
    LobDataMergerThread::destroy();

    is_inited_ = false;
    round_value_ = 0;
    err_handler_ = nullptr;

    LOG_INFO("ObCDCLobDataMerger destroy succ", "thread_num", get_thread_num());
  }
}

int ObCDCLobDataMerger::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_FAIL(LobDataMergerThread::start())) {
    LOG_ERROR("ObCDCLobDataMerger start thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("ObCDCLobDataMerger start threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObCDCLobDataMerger::stop()
{
  if (is_inited_) {
    LobDataMergerThread::mark_stop_flag();
    LobDataMergerThread::stop();
    LOG_INFO("ObCDCLobDataMerger stop threads succ", "thread_num", get_thread_num());
  }
}

int ObCDCLobDataMerger::push(
    ObLobDataOutRowCtxList &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(task));
  } else {
    ATOMIC_INC(&lob_data_list_task_count_);

    if (OB_FAIL(push_task_(task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_task_", KR(ret));
      }
    }
  }

  return ret;
}

int ObCDCLobDataMerger::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  set_cdc_thread_name("LobDtMerger", thread_index);
  ObLogTraceIdGuard trace_guard;
  LobColumnFragmentCtx *task = static_cast<LobColumnFragmentCtx *>(data);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(handle_task_(*task, thread_index, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_task_ fail", KR(ret), KPC(task), K(thread_index));
    }
  } else {
  }

  if (is_in_stop_status(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && nullptr != err_handler_) {
    err_handler_->handle_error(ret, "LobDataMergerThread thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObCDCLobDataMerger::push_task_(
    ObLobDataOutRowCtxList &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else {
    const uint64_t tenant_id = task.get_tenant_id();
    const uint64_t aux_lob_meta_tid = task.get_aux_lob_meta_table_id();
    ObIAllocator &allocator = task.get_allocator();
    ObLobDataGetCtxList &lob_data_get_ctx_list = task.get_lob_data_get_ctx_list();
    ObLobDataGetCtx *cur_lob_data_get_ctx = lob_data_get_ctx_list.head_;

    while (OB_SUCC(ret) && ! is_in_stop_status(stop_flag) && cur_lob_data_get_ctx) {
      if (OB_FAIL(push_lob_column_(allocator, task, *cur_lob_data_get_ctx, stop_flag))) {
        LOG_ERROR("push_lob_column_ failed", KR(ret));
      } else {
        cur_lob_data_get_ctx = cur_lob_data_get_ctx->get_next();
      }
    }

    if (is_in_stop_status(stop_flag)) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int ObCDCLobDataMerger::push_lob_column_(
    ObIAllocator &allocator,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    ObLobDataGetCtx &lob_data_get_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const ObLobDataOutRowCtx *lob_data_out_row_ctx = nullptr;
  static const int64_t PUSH_LOB_DATA_MERGER_TIMEOUT = 1 * _MSEC_;

  if (OB_FAIL(lob_data_get_ctx.get_lob_out_row_ctx(lob_data_out_row_ctx))) {
    LOG_ERROR("lob_data_get_ctx get_lob_out_row_ctx failed", KR(ret), K(lob_data_get_ctx));
  } else if (OB_ISNULL(lob_data_out_row_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_data_out_row_ctx is nullptr", KR(ret), K(lob_data_get_ctx));
  } else {
    LOG_DEBUG("push_lob_column_", K(lob_data_get_ctx), K(lob_data_out_row_ctx_list));
    const bool is_empty_sql = (ObLobDataOutRowCtx::OpType::EMPTY_SQL == lob_data_out_row_ctx->op_);
    const auto seq_no_st = transaction::ObTxSEQ::cast_from_int(lob_data_out_row_ctx->seq_no_st_);
    const uint32_t seq_no_cnt = lob_data_out_row_ctx->seq_no_cnt_;
    const uint32_t del_seq_no_cnt = lob_data_out_row_ctx->del_seq_no_cnt_;
    LobColumnFragmentCtxList new_lob_col_fra_ctx_list;
    LobColumnFragmentCtxList old_lob_col_fra_ctx_list;

    if (is_empty_sql) {
      // do nothing
    } else if (lob_data_get_ctx.is_insert()) {
      if (OB_FAIL(lob_data_get_ctx.new_lob_col_ctx_.init(seq_no_cnt, allocator))) {
        LOG_ERROR("lob_data_get_ctx new_lob_col_ctx_ init failed", KR(ret), K(seq_no_cnt),
            K(lob_data_get_ctx));
      } else if (OB_FAIL(get_lob_col_fra_ctx_list_(true/*is_new_col*/, seq_no_st, seq_no_cnt, allocator,
              lob_data_get_ctx, new_lob_col_fra_ctx_list))) {
        LOG_ERROR("get_lob_col_fra_ctx_list_ failed", KR(ret), K(seq_no_st), K(seq_no_cnt),
            K(new_lob_col_fra_ctx_list));
      }
    } else if (lob_data_get_ctx.is_update()) {
      const int64_t insert_seq_no_cnt = seq_no_cnt - del_seq_no_cnt;
      // NOTICE:
      // 1. Update LOB column data from in_row to out_row, the del_seq_no_cnt is 0
      // 2. Update LOB column data from out_row to empty string, the insert_seq_no_cnt is 0
      //
      // 3. Currently, LOB column data is stored in out_row in these cases:
      // 3.1  Length of column data is larger than 4K
      // 3.2. Length of column data is less than 4K(even if column data is empty string),
      //      but was larger than 4K(stored out_row) and not update to NULL until this trans.

      if (OB_FAIL(lob_data_get_ctx.old_lob_col_ctx_.init(del_seq_no_cnt, allocator))) {
        LOG_ERROR("lob_data_get_ctx old_lob_col_ctx_ init failed", KR(ret), K(del_seq_no_cnt),
            K(lob_data_get_ctx));
      } else if (OB_FAIL(get_lob_col_fra_ctx_list_(false/*is_new_col*/, seq_no_st, del_seq_no_cnt,
              allocator, lob_data_get_ctx, old_lob_col_fra_ctx_list))) {
        LOG_ERROR("get_lob_col_fra_ctx_list_ failed", KR(ret), K(seq_no_st), K(del_seq_no_cnt),
            K(old_lob_col_fra_ctx_list));
      } else if (OB_FAIL(lob_data_get_ctx.new_lob_col_ctx_.init(insert_seq_no_cnt, allocator))) {
        LOG_ERROR("lob_data_get_ctx new_lob_col_ctx_ init failed", KR(ret), K(seq_no_cnt), K(del_seq_no_cnt),
            K(lob_data_get_ctx));
      } else if (OB_FAIL(get_lob_col_fra_ctx_list_(true/*is_new_col*/, seq_no_st + del_seq_no_cnt, insert_seq_no_cnt,
              allocator, lob_data_get_ctx, new_lob_col_fra_ctx_list))) {
        LOG_ERROR("get_lob_col_fra_ctx_list_ failed", KR(ret), K(seq_no_st), K(del_seq_no_cnt),
            K(new_lob_col_fra_ctx_list));
      }
    } else if (lob_data_get_ctx.is_delete()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected dml_type", KR(ret), K(lob_data_get_ctx));
    }

    if (OB_SUCC(ret)) {
      bool is_all_lob_col_handle_done = false;

      if (is_empty_sql) {
        lob_data_out_row_ctx_list.inc_lob_col_count(is_all_lob_col_handle_done);

        if (is_all_lob_col_handle_done) {
          if (OB_FAIL(try_to_push_task_into_formatter_(lob_data_out_row_ctx_list, stop_flag))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("try_to_push_task_into_formatter_ failed", KR(ret));
            }
          }
        }
      } else {
        // Try to push all old LobColumnFragmentCtx task
        if (OB_FAIL(push_lob_col_fra_ctx_list_(old_lob_col_fra_ctx_list, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push_lob_col_fra_ctx_list_ failed", KR(ret));
          }
        // Try to push all new LobColumnFragmentCtx task
        } else if (OB_FAIL(push_lob_col_fra_ctx_list_(new_lob_col_fra_ctx_list, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push_lob_col_fra_ctx_list_ failed", KR(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObCDCLobDataMerger::get_lob_col_fra_ctx_list_(
    const bool is_new_col,
    const transaction::ObTxSEQ &seq_no_start,
    const uint32_t seq_no_cnt,
    ObIAllocator &allocator,
    ObLobDataGetCtx &lob_data_get_ctx,
    LobColumnFragmentCtxList &lob_col_fra_ctx_list)
{
  int ret = OB_SUCCESS;
  transaction::ObTxSEQ seq_no = seq_no_start;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < seq_no_cnt; ++idx, ++seq_no) {
    LobColumnFragmentCtx *lob_col_fragment_ctx
      = static_cast<LobColumnFragmentCtx *>(allocator.alloc(sizeof(LobColumnFragmentCtx)));

    if (OB_ISNULL(lob_col_fragment_ctx)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc LobColumnFragmentCtx memory failed", KR(ret));
    } else {
      new(lob_col_fragment_ctx) LobColumnFragmentCtx(lob_data_get_ctx);

      lob_col_fragment_ctx->reset(is_new_col, seq_no, idx, seq_no_cnt);

      if (OB_FAIL(lob_col_fra_ctx_list.add(lob_col_fragment_ctx))) {
        LOG_ERROR("lob_col_fra_ctx_list add failed", KR(ret), KPC(lob_col_fragment_ctx));
      }
    }
  } // for

  return ret;
}

int ObCDCLobDataMerger::push_lob_col_fra_ctx_list_(
    LobColumnFragmentCtxList &lob_col_fra_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  static const int64_t PUSH_LOB_DATA_MERGER_TIMEOUT = 1 * _MSEC_;
  LobColumnFragmentCtx *cur_lob_col_fragment_ctx = lob_col_fra_ctx_list.head_;

  if (lob_col_fra_ctx_list.num_ <= 0) {
    // do nothing
  } else {
    while (OB_SUCC(ret) && ! is_in_stop_status(stop_flag) && cur_lob_col_fragment_ctx) {
      uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
      void *push_task = static_cast<void *>(cur_lob_col_fragment_ctx);
      ret = OB_TIMEOUT;

      while (OB_TIMEOUT == ret && ! is_in_stop_status(stop_flag)) {
        if (OB_FAIL(LobDataMergerThread::push(push_task, hash_value, PUSH_LOB_DATA_MERGER_TIMEOUT))) {
          if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push task into LobDataMergerThread fail", K(ret), K(push_task), K(hash_value));
          }
        }
      }

      if (OB_SUCC(ret)) {
        cur_lob_col_fragment_ctx = cur_lob_col_fragment_ctx->get_next();
      }
    }
  }

  return ret;
}

int ObCDCLobDataMerger::handle_task_(
    LobColumnFragmentCtx &task,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else {
    ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;
    ObLobDataGetCtx &lob_data_get_ctx = task.host_;
    ObLobDataOutRowCtxList *lob_data_out_row_ctx_list = static_cast<ObLobDataOutRowCtxList *>(lob_data_get_ctx.host_);
    const IStmtTask *stmt_task = lob_data_out_row_ctx_list->get_stmt_task();
    const ObLobData *new_lob_data = lob_data_get_ctx.new_lob_data_;
    const bool is_new_col = task.is_new_col_;
    ObString **fragment_cb_array= lob_data_get_ctx.get_fragment_cb_array(is_new_col);

    if (OB_ISNULL(lob_data_out_row_ctx_list) || OB_ISNULL(new_lob_data) || OB_ISNULL(fragment_cb_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob_data_out_row_ctx_list or new_lob_data or fragment_cb_array is nullptr", KR(ret),
          K(lob_data_out_row_ctx_list), K(new_lob_data), K(fragment_cb_array));
    } else if (OB_ISNULL(stmt_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("stmt_task is nullptr", KR(ret), KPC(lob_data_out_row_ctx_list));
    } else {
      const PartTransTask &part_trans_task = stmt_task->get_host();
      const int64_t commit_version = part_trans_task.get_trans_commit_version();
      const uint64_t tenant_id = lob_data_out_row_ctx_list->get_tenant_id();
      const transaction::ObTransID &trans_id = lob_data_out_row_ctx_list->get_trans_id();
      const uint64_t aux_lob_meta_tid = lob_data_out_row_ctx_list->get_aux_lob_meta_table_id();
      const ObLobId &lob_id = new_lob_data->id_;
      const uint32_t idx = task.idx_;
      LobAuxMetaKey lob_aux_meta_key(commit_version, tenant_id, trans_id, aux_lob_meta_tid, lob_id, task.seq_no_);
      const char *lob_data_ptr = nullptr;
      int64_t lob_data_len = 0;
      ObIAllocator &allocator = lob_data_out_row_ctx_list->get_allocator();
      // We need retry to get the lob data based on lob_aux_meta_key when return OB_ENTRY_NOT_EXIST,
      // because LobAuxMeta table data and primary table data are processed concurrently.
      RETRY_FUNC_ON_ERROR_WITH_USLEEP_MS(OB_ENTRY_NOT_EXIST, 1 * _MSEC_, stop_flag, lob_aux_meta_storager, get, allocator, lob_aux_meta_key,
          lob_data_ptr, lob_data_len);

      if (OB_SUCC(ret)) {
        LOG_DEBUG("lob_aux_meta_storager get succ", K(lob_aux_meta_key), K(lob_data_len), K(task));
        fragment_cb_array[idx]->assign_ptr(lob_data_ptr, lob_data_len);
        uint32_t col_ref_cnt = lob_data_get_ctx.dec_col_ref_cnt(is_new_col);

        if (0 == col_ref_cnt) {
          if (OB_FAIL(handle_when_all_lob_col_fragment_progress_done_(
                  task, lob_data_get_ctx, *lob_data_out_row_ctx_list, stop_flag))) {
            LOG_ERROR("handle_when_all_lob_col_fragment_progress_done_ failed", KR(ret));
          }
        }
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("lob_aux_meta_storager get failed", KR(ret), K(lob_aux_meta_key));
      }
    }
  }

  if (is_in_stop_status(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObCDCLobDataMerger::handle_when_all_lob_col_fragment_progress_done_(
    LobColumnFragmentCtx &task,
    ObLobDataGetCtx &lob_data_get_ctx,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const ObLobData *lob_data = nullptr;
  const ObLobDataOutRowCtx *lob_data_out_row_ctx = nullptr;
  const bool is_new_col = task.is_new_col_;
  ObString **fragment_cb_array= lob_data_get_ctx.get_fragment_cb_array(is_new_col);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_ISNULL(lob_data = lob_data_get_ctx.get_lob_data(is_new_col))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new_lob_data is nullptr", KR(ret), K(task));
  } else if (OB_FAIL(lob_data_get_ctx.get_lob_out_row_ctx(lob_data_out_row_ctx))) {
    LOG_ERROR("lob_data_get_ctx get_lob_out_row_ctx failed", KR(ret), K(lob_data_get_ctx));
  } else {
    LOG_DEBUG("lob_aux_meta_storager handle last fragment", K(task), K(lob_data_get_ctx));
    const bool is_new_col = task.is_new_col_;
    const uint32_t seq_no_cnt = task.ref_cnt_;
    const uint64_t lob_total_len = lob_data->byte_size_;
    char *buf = nullptr;

    if (OB_UNLIKELY(0 >= lob_total_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob_data_len is 0, there should be no outrow lob_col_value", K(task), K(lob_data));
    } else if (OB_ISNULL(buf = static_cast<char *>(lob_data_out_row_ctx_list.get_allocator().alloc(sizeof(char) * (lob_total_len + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("buf is nullptr", KR(ret), K(is_new_col), K(task), K(lob_data));
    } else {
      uint64_t pos = 0;
      bool is_lob_col_value_handle_done = false;
      bool is_all_lob_col_handle_done = false;

      for (uint32_t idx = 0; OB_SUCC(ret) && idx < seq_no_cnt; ++idx) {
        ObString *str_ptr = fragment_cb_array[idx];
        const int64_t len = str_ptr->length();
        const char *ptr = str_ptr->ptr();

        if (pos + len > lob_total_len) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("buf not enough, not expected", KR(ret), K(pos), K(len), K(lob_total_len));
        } else {
          MEMCPY(buf + pos, ptr, len);
          pos += len;
        }
      }

      if (OB_SUCC(ret)) {
        buf[pos] = '\0';

        if (OB_FAIL(lob_data_get_ctx.set_col_value(is_new_col, buf, pos))) {
          LOG_ERROR("lob_data_get_ctx set_col_value failed", KR(ret), K(pos));
        } else {
          lob_data_get_ctx.inc_lob_col_value_count(is_lob_col_value_handle_done);

          if (is_lob_col_value_handle_done) {
            lob_data_out_row_ctx_list.inc_lob_col_count(is_all_lob_col_handle_done);
          }

          // TODO debug remove
          /*
          int64_t buf_len = strlen(buf);
          int char_len = sizeof(char_len);
          LOG_INFO("handle_when_all_lob_col_fragment_progress_done_", "md5", calc_md5_cstr(buf, pos),
              K(buf_len), K(pos),
              K(char_len), K(lob_total_len));
          */
          // remove

          if (is_all_lob_col_handle_done) {
            if (OB_FAIL(try_to_push_task_into_formatter_(lob_data_out_row_ctx_list, stop_flag))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("try_to_push_task_into_formatter_ failed", KR(ret));
              }
            }
          }
        }
      }

      LOG_DEBUG("handle_when_all_lob_col_fragment_progress_done_", K(is_lob_col_value_handle_done), K(is_all_lob_col_handle_done), K(seq_no_cnt), K(pos));
    }
  }

  return ret;
}

int ObCDCLobDataMerger::try_to_push_task_into_formatter_(
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const bool is_ddl = lob_data_out_row_ctx_list.is_ddl();
  IStmtTask *stmt_task = lob_data_out_row_ctx_list.get_stmt_task();
  DmlStmtTask *dml_stmt_task = nullptr;
  IObLogFormatter *formatter = TCTX.formatter_;

  if (is_ddl) {
    // is_ddl, do nothing
  } else {
    if (OB_ISNULL(formatter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("formatter is nullptr", KR(ret));
    } else if (OB_ISNULL(dml_stmt_task = static_cast<DmlStmtTask*>(stmt_task))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("dml_stmt_task is nullptr", KR(ret), K(lob_data_out_row_ctx_list));
    } else if (OB_FAIL(formatter->push_single_task(dml_stmt_task, stop_flag))) {
      LOG_ERROR("formatter push_single_task failed", KR(ret), KPC(dml_stmt_task));
    } else {
      // stat
      ATOMIC_DEC(&lob_data_list_task_count_);
    }
  }

  return ret;
}

void ObCDCLobDataMerger::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num; ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[STAT] [LobDataMerger] [%ld/%ld] TASK_COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

} // namespace libobcdc
} // namespace oceanbase
