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

#include "ob_cdc_lob_ctx.h"
#include "lib/utility/ob_print_utils.h"    // databuff_printf
#include "ob_log_utils.h"                   // md5

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
int ObLobColCtx::init(
    const uint64_t seq_no_cnt,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(seq_no_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(seq_no_cnt));
  } else if (OB_UNLIKELY(seq_no_cnt == 0)) {
    // 1. Update LOB column data from in_row to out_row, the del_seq_no_cnt is 0
    // 2. Update LOB column data from out_row to empty string, the insert_seq_no_cnt is 0
    set_col_value(COLUMN_VALUE_IS_EMPTY, 0);
    LOG_DEBUG("seq_no_cnt is 0, use empty string as lob_column_value", KPC(this));
  } else {
    ObString **fragment_cb_array =
      static_cast<ObString **>(allocator.alloc(seq_no_cnt * sizeof(ObString*)));
    for (int64_t idx = 0; OB_SUCC(ret) && idx < seq_no_cnt; ++idx) {
      *(fragment_cb_array + idx) = static_cast<ObString *>(allocator.alloc(sizeof(ObString)));
    }

    if (OB_ISNULL(fragment_cb_array)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc fragment_cb_array memory failed", KR(ret), K(seq_no_cnt));
    } else {
      set_col_ref_cnt(seq_no_cnt);
      fragment_cb_array_ = fragment_cb_array;
    }
  }

  return ret;
}

int ObLobColCtx::set_col_value(
    const char *buf,
    const uint64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(buf), K(buf_len));
  } else {
    lob_column_value_.assign_ptr(buf, buf_len);
  }

  return ret;
}

void ObLobDataGetCtx::reset()
{
  type_ = ObLobDataGetTaskType::FULL_LOB;
  host_ = nullptr;
  column_id_ = common::OB_INVALID_ID;
  dml_flag_.reset();
  new_lob_data_ = nullptr;
  old_lob_data_ = nullptr;
  lob_col_value_handle_done_count_ = 0;
  new_lob_col_ctx_.reset();
  old_lob_col_ctx_.reset();
  next_ = nullptr;
}

void ObLobDataGetCtx::reset(
    void *host,
    const uint64_t column_id,
    const blocksstable::ObDmlRowFlag &dml_flag,
    const common::ObLobData *new_lob_data)
{
  host_ = host;
  column_id_ = column_id;
  dml_flag_ = dml_flag;
  new_lob_data_ = new_lob_data;

  // set task type according to ObLobDataOutRowCtx::op
  const ObLobDataOutRowCtx *lob_data_out_row_ctx = nullptr;
  if (OB_ISNULL(new_lob_data_)) {
    LOG_DEBUG("new_lob_data_ is null", K(column_id), K(dml_flag), KP(host));
  } else if (OB_ISNULL(lob_data_out_row_ctx = reinterpret_cast<const ObLobDataOutRowCtx *>(new_lob_data_->buffer_))) {
    LOG_DEBUG("lob_data_out_row_ctx is null", K(column_id), K(dml_flag), KP(host), KPC(new_lob_data_));
  } else if (lob_data_out_row_ctx->is_diff()) {
    type_ = ObLobDataGetTaskType::EXT_INFO_LOG;
    LOG_DEBUG("lob_data_out_row_ctx is diff", K(column_id), KPC(new_lob_data_), KPC(lob_data_out_row_ctx));
  }
}

int ObLobDataGetCtx::get_lob_out_row_ctx(const ObLobDataOutRowCtx *&lob_data_out_row_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(new_lob_data_)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    lob_data_out_row_ctx = reinterpret_cast<const ObLobDataOutRowCtx *>(new_lob_data_->buffer_);
  }

  return ret;
}

ObLobId ObLobDataGetCtx::get_lob_id() const
{
  ObLobId lob_id;
  switch (get_type()) {
    case ObLobDataGetTaskType::FULL_LOB:
      if (OB_NOT_NULL(new_lob_data_)) {
        lob_id = new_lob_data_->id_;
      } else {
        LOG_DEBUG("new_lob_data_ is null", KPC(this));
      }
      break;
    default:
      break;
  }
  return lob_id;
}

int ObLobDataGetCtx::get_data_length(const bool is_new_col, uint64_t &data_length) const
{
  int ret = OB_SUCCESS;
  switch (get_type()) {
    case ObLobDataGetTaskType::FULL_LOB: {
      const ObLobData *lob_data = nullptr;
      if (OB_ISNULL(lob_data = get_lob_data(is_new_col))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("new_lob_data is nullptr", KR(ret), KPC(this));
      } else {
        data_length = lob_data->byte_size_;
      }
      break;
    }
    case ObLobDataGetTaskType::EXT_INFO_LOG: {
      const ObLobDataOutRowCtx *lob_data_out_row_ctx = nullptr;
      if (! is_new_col) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("is_new_col must be true", KR(ret), KPC(this));
      } else if (OB_FAIL(get_lob_out_row_ctx(lob_data_out_row_ctx))) {
        LOG_ERROR("get_lob_out_row_ctx failed", KR(ret), KPC(this));
      } else {
        data_length = lob_data_out_row_ctx->modified_len_;
      }
      break;
    }
    default:
      break;
  }
  return ret;
}

int ObLobDataGetCtx::set_col_value(const bool is_new_col, const char *buf, const uint64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (is_new_col) {
    ret = new_lob_col_ctx_.set_col_value(buf, buf_len);
  } else {
    ret = old_lob_col_ctx_.set_col_value(buf, buf_len);
  }

  return ret;
}

// TODO LOB phase ii
void ObLobDataGetCtx::inc_lob_col_value_count(bool &is_lob_col_value_handle_done)
{
  int8_t total_value_count = 0;

  if (is_insert()) {
    total_value_count = 1;
  // if current outrow data is ext_info_log or partial_json
  // there is no before-image output. so total_value_count is one.
  } else if (is_ext_info_log()) {
    total_value_count = 1;
  } else if (is_update()) {
    if (nullptr != old_lob_data_ && old_lob_data_->byte_size_ > 0) {
      total_value_count += 1;
    }
    if (nullptr != new_lob_data_ && new_lob_data_->byte_size_ > 0) {
      total_value_count += 1;
    }
  }

  is_lob_col_value_handle_done = (total_value_count == ATOMIC_AAF(&lob_col_value_handle_done_count_, 1));
  LOG_DEBUG("inc_lob_col_value_count", K(total_value_count), K_(lob_col_value_handle_done_count));
}

int64_t ObLobDataGetCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    ObLobDataOutRowCtxList *lob_data_out_row_ctx = static_cast<ObLobDataOutRowCtxList *>(host_);

    if (nullptr != lob_data_out_row_ctx) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "tenant_id=%ld, tx_id=%s, aux_tid=%ld, ",
          lob_data_out_row_ctx->get_tenant_id(),
          to_cstring(lob_data_out_row_ctx->get_trans_id()),
          lob_data_out_row_ctx->get_aux_lob_meta_table_id());
    }

    (void)common::databuff_printf(buf, buf_len, pos,
        "column_id=%ld, dml_flag=%s, dml_serialize_flag=%d, ref_cnt[new=%d, old=%d], handle_cnt=%d, type=%d, ",
        column_id_, dml_flag_.getFlagStr(), dml_flag_.get_serialize_flag(), new_lob_col_ctx_.get_col_ref_cnt(),
        old_lob_col_ctx_.get_col_ref_cnt(), lob_col_value_handle_done_count_, type_);

    if (nullptr != new_lob_data_) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "byte_size=%ld, lob_id=%s ",
           new_lob_data_->byte_size_, to_cstring(new_lob_data_->id_));

      const ObLobDataOutRowCtx *lob_data_out_row_ctx =
        reinterpret_cast<const ObLobDataOutRowCtx *>(new_lob_data_->buffer_);

      if (nullptr != lob_data_out_row_ctx) {
        (void)common::databuff_printf(buf, buf_len, pos,
            "lob_out_row=%s", to_cstring(*lob_data_out_row_ctx));
      }
    }
  }

  return pos;
}

void ObLobDataOutRowCtxList::reset(
    IStmtTask *stmt_task,
    const uint64_t tenant_id,
    const transaction::ObTransID &trans_id,
    const uint64_t aux_lob_meta_table_id,
    const bool is_ddl)
{
  stmt_task_ = stmt_task;
  is_ddl_ = is_ddl;
  tenant_id_ = tenant_id;
  trans_id_ = trans_id;
  aux_lob_meta_table_id_ = aux_lob_meta_table_id;
}

int ObLobDataOutRowCtxList::set_old_lob_data(
    const uint64_t column_id,
    const common::ObLobData *old_lob_data)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx *lob_data_get_ctx = lob_data_get_ctxs_.head_;
  bool is_found = false;

  if (OB_ISNULL(old_lob_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(column_id), K(old_lob_data));
  } else {
    while (OB_SUCC(ret) && ! is_found && nullptr !=lob_data_get_ctx) {
      if (column_id == lob_data_get_ctx->column_id_) {
        is_found = true;
        lob_data_get_ctx->set_old_lob_data(old_lob_data);
      }
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    } // while
  }

  if (! is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObLobDataOutRowCtxList::get_lob_column_value(
    const uint64_t column_id,
    const bool is_new_col,
    common::ObString *&col_str)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx *lob_data_get_ctx = lob_data_get_ctxs_.head_;
  bool is_found = false;

  if (OB_UNLIKELY(! is_all_lob_callback_done())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_ctx_cols is_all_lob_callback_done is false, not expected", KR(ret), KPC(this));
  } else {
    while (OB_SUCC(ret) && ! is_found && nullptr !=lob_data_get_ctx) {
      if (column_id == lob_data_get_ctx->column_id_) {
        is_found = true;
        if (is_new_col) {
          col_str = &(lob_data_get_ctx->get_new_lob_column_value());
        } else {
          col_str = &(lob_data_get_ctx->get_old_lob_column_value());
        }
      }
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    } // while
  }

  if (! is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    LOG_DEBUG("get_lob_column_value", K(column_id),
        "md5", calc_md5_cstr(col_str->ptr(), col_str->length()),
        "buf_len", col_str->length());
  }

  return ret;
}

int ObLobDataOutRowCtxList::get_lob_data_get_ctx(
    const uint64_t column_id,
    ObLobDataGetCtx *&result)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObLobDataGetCtx *lob_data_get_ctx = lob_data_get_ctxs_.head_;
  if (OB_UNLIKELY(! is_all_lob_callback_done())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_ctx_cols is_all_lob_callback_done is false, not expected", KR(ret), KPC(this));
  } else {
    while (OB_SUCC(ret) && ! is_found && nullptr != lob_data_get_ctx) {
      if (column_id == lob_data_get_ctx->column_id_) {
        is_found = true;
        result = lob_data_get_ctx;
      }
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    } // while
  }

  if (! is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

uint64_t ObLobDataOutRowCtxList::get_table_id_of_lob_aux_meta_key(const ObLobDataGetCtx &lob_data_get_ctx) const
{
  uint64_t table_id = 0;
  switch (lob_data_get_ctx.get_type()) {
    case ObLobDataGetTaskType::FULL_LOB:
      table_id = get_aux_lob_meta_table_id();
      break;
    default:
      break;
  }
  return table_id;
}

int64_t ObLobDataOutRowCtxList::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (nullptr != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos,
          "tenant_id=%ld, tx_id=%s, aux_tid=%ld, lob_col_cnt=%ld/%ld,",
          tenant_id_, to_cstring(trans_id_), aux_lob_meta_table_id_, lob_col_get_succ_count_, get_total_lob_count());

    ObLobDataGetCtx *head = lob_data_get_ctxs_.head_;
    ObLobDataGetCtx *tail = lob_data_get_ctxs_.tail_;

    if (nullptr != head) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "head=%s,", to_cstring(*head));
    }

    if (nullptr != tail && head != tail) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "tail=%s", to_cstring(*tail));
    }
  }

  return pos;
}

} // namespace libobcdc
} // namespace oceanbase
