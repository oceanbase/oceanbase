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

#ifndef OCEANBASE_LIBOBCDC_LOB_CTX_H_
#define OCEANBASE_LIBOBCDC_LOB_CTX_H_

#include "common/object/ob_object.h"        // ObLobData, ObLobDataOutRowCtx
#include "storage/blocksstable/ob_datum_row.h" // ObDmlFlag
#include "lib/atomic/ob_atomic.h"           // ATOMIC_**
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "ob_log_lighty_list.h"             // LightyList

namespace oceanbase
{
namespace libobcdc
{
struct ObLobColCtx
{
  ObLobColCtx() { reset(); }
  ~ObLobColCtx() { reset(); }

  void reset()
  {
    fragment_cb_array_ = nullptr;
    lob_column_value_.reset();
    col_ref_cnt_ = 0;
  }

  int init(
      const uint64_t seq_no_cnt,
      common::ObIAllocator &allocator);

  int set_col_value(
      const char *buf,
      const uint64_t buf_len);

  common::ObString &get_lob_column_value() { return lob_column_value_; }

  void set_col_ref_cnt(const uint32_t col_ref_cnt) { ATOMIC_SET(&col_ref_cnt_, col_ref_cnt); }
  uint32_t dec_col_ref_cnt() { return ATOMIC_SAF(&col_ref_cnt_, 1); }
  uint32_t get_col_ref_cnt() const { return ATOMIC_LOAD(&col_ref_cnt_); }

  TO_STRING_KV("col_ref_cnt", get_col_ref_cnt());

  common::ObString **fragment_cb_array_;
  common::ObString lob_column_value_;
  uint32_t col_ref_cnt_;
};

struct ObLobDataGetCtx
{
  ObLobDataGetCtx() { reset(); }
  ~ObLobDataGetCtx() { reset(); }

  void reset();
  void reset(
      void *host,
      const uint64_t column_id,
      const blocksstable::ObDmlFlag &dml_flag,
      const common::ObLobData *new_lob_data);
  void set_old_lob_data(const common::ObLobData *old_lob_data) { old_lob_data_ = old_lob_data; }

  bool is_insert() const { return blocksstable::ObDmlFlag::DF_INSERT == dml_flag_; }
  bool is_update() const { return blocksstable::ObDmlFlag::DF_UPDATE == dml_flag_; }
  bool is_delete() const { return blocksstable::ObDmlFlag::DF_DELETE == dml_flag_; }

  const common::ObLobData *get_lob_data(const bool is_new_col)
  {
    const common::ObLobData *lob_data_ptr;

    if (is_new_col) {
      lob_data_ptr = new_lob_data_;
    } else {
      lob_data_ptr = old_lob_data_;
    }

    return lob_data_ptr;
  }
  const common::ObLobData *get_new_lob_data() { return new_lob_data_; }
  const common::ObLobData *get_old_lob_data() { return old_lob_data_; }
  int get_lob_out_row_ctx(const ObLobDataOutRowCtx *&lob_data_out_row_ctx);

  common::ObString **get_fragment_cb_array(const bool is_new_col)
  {
    common::ObString **res_str = nullptr;

    if (is_new_col) {
      res_str = new_lob_col_ctx_.fragment_cb_array_;
    } else {
      res_str = old_lob_col_ctx_.fragment_cb_array_;
    }

    return res_str;
  }

  uint32_t dec_col_ref_cnt(const bool is_new_col)
  {
    uint32_t ref_cnt = 0;

    if (is_new_col) {
      ref_cnt = new_lob_col_ctx_.dec_col_ref_cnt();
    } else {
      ref_cnt = old_lob_col_ctx_.dec_col_ref_cnt();
    }

    return ref_cnt;
  }

  int set_col_value(const bool is_new_col, const char *buf, const uint64_t buf_len);
  common::ObString &get_new_lob_column_value() { return new_lob_col_ctx_.get_lob_column_value(); }
  common::ObString &get_old_lob_column_value() { return old_lob_col_ctx_.get_lob_column_value(); }

  ObLobDataGetCtx *get_next() { return next_; }
  void set_next(ObLobDataGetCtx *next) { next_ = next; }
  void inc_lob_col_value_count(bool &is_lob_col_value_handle_done);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  void *host_;    // ObLobDataOutRowCtxList
  uint64_t column_id_;
  blocksstable::ObDmlFlag dml_flag_;
  const common::ObLobData *new_lob_data_;
  const common::ObLobData *old_lob_data_;
  int8_t lob_col_value_handle_done_count_;
  ObLobColCtx new_lob_col_ctx_;
  ObLobColCtx old_lob_col_ctx_;
  ObLobDataGetCtx *next_;
};

typedef LightyList<ObLobDataGetCtx> ObLobDataGetCtxList;

struct LobColumnFragmentCtx
{
  LobColumnFragmentCtx(ObLobDataGetCtx &host) :
    host_(host),
    is_new_col_(0),
    seq_no_(),
    idx_(-1),
    ref_cnt_(-1),
    next_(nullptr)
  {}

  void reset(
    const bool is_new_col,
    const transaction::ObTxSEQ &seq_no,
    const uint32_t idx,
    const uint32_t ref_cnt)
  {
    is_new_col_ = is_new_col;
    seq_no_ = seq_no;
    idx_ = idx;
    ref_cnt_ = ref_cnt;
  }

  bool is_valid() const
  {
    return seq_no_.is_valid() && (idx_ < ref_cnt_);
  }

  LobColumnFragmentCtx *get_next() { return next_; }
  void set_next(LobColumnFragmentCtx *next) { next_ = next; }

  TO_STRING_KV(
      K_(host),
      K_(is_new_col),
      K_(seq_no),
      K_(idx),
      K_(ref_cnt));

  ObLobDataGetCtx &host_;
  int8_t is_new_col_ : 1;
  transaction::ObTxSEQ seq_no_;
  uint64_t idx_ : 32;
  uint64_t ref_cnt_ : 32;
  LobColumnFragmentCtx *next_;
};
typedef LightyList<LobColumnFragmentCtx> LobColumnFragmentCtxList;

class IStmtTask;
class ObLobDataOutRowCtxList
{
public:
  ObLobDataOutRowCtxList(common::ObIAllocator &allocator) :
    allocator_(allocator),
    is_ddl_(0),
    stmt_task_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    aux_lob_meta_table_id_(OB_INVALID_ID),
    lob_data_get_ctxs_(),
    lob_col_get_succ_count_(0)
  {
  }
  ~ObLobDataOutRowCtxList() { reset(); }

  void reset()
  {
    is_ddl_ = 0;
    stmt_task_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
    trans_id_.reset();
    aux_lob_meta_table_id_ = OB_INVALID_ID;
    lob_data_get_ctxs_.reset();
    lob_col_get_succ_count_ = 0;
  }

  void reset(
      IStmtTask *dml_stmt_task,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_table_id,
      const bool is_ddl);

  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
      && trans_id_.is_valid()
      && common::OB_INVALID_ID != aux_lob_meta_table_id_
      && get_total_lob_count() > 0;
  }

  common::ObIAllocator &get_allocator() { return allocator_; }
  bool is_ddl() const { return is_ddl_; }
  IStmtTask *get_stmt_task() { return stmt_task_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  uint64_t get_aux_lob_meta_table_id() const { return aux_lob_meta_table_id_; }

  int64_t get_total_lob_count() const { return lob_data_get_ctxs_.num_; }
  bool has_out_row_lob() const { return get_total_lob_count() > 0; }
  ObLobDataGetCtxList &get_lob_data_get_ctx_list() { return lob_data_get_ctxs_; }
  void add(ObLobDataGetCtx *lob_data_get_ctx) { lob_data_get_ctxs_.add(lob_data_get_ctx); }
  int set_old_lob_data(
      const uint64_t column_id,
      const common::ObLobData *old_lob_data);
  int get_lob_column_value(
      const uint64_t column_id,
      const bool is_new_col,
      common::ObString *&col_str);

  bool is_all_lob_callback_done() const { return get_total_lob_count() == ATOMIC_LOAD(&lob_col_get_succ_count_); }
  void inc_lob_col_count(bool &is_all_lob_col_handle_done)
  {
    is_all_lob_col_handle_done = (get_total_lob_count() == ATOMIC_AAF(&lob_col_get_succ_count_, 1));
  }

public:
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  common::ObIAllocator &allocator_;
  int8_t is_ddl_ : 1;
  IStmtTask *stmt_task_;    // primary table row
  uint64_t tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t aux_lob_meta_table_id_;
  ObLobDataGetCtxList lob_data_get_ctxs_;    // Table may contains one or more LOB columns
  int64_t lob_col_get_succ_count_;

  DISALLOW_COPY_AND_ASSIGN(ObLobDataOutRowCtxList);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
