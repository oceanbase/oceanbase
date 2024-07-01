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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/engine/ob_operator.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_domain_utils.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/access/ob_dml_param.h"
namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObDASDMLBaseCtDef,
                    table_id_,
                    index_tid_,
                    rowkey_cnt_,
                    spk_cnt_,
                    schema_version_,
                    column_ids_,
                    column_types_,
                    column_accuracys_,
                    old_row_projector_,
                    new_row_projector_,
                    tz_info_,
                    table_param_,
                    encrypt_meta_,
                    flags_);

OB_DEF_SERIALIZE(ObDASDMLBaseRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASDMLBaseRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASDMLBaseRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  return len;
}

// add by dkz
OB_SERIALIZE_MEMBER((ObDASInsRtDef, ObDASDMLBaseRtDef),
                    need_fetch_conflict_,
                    direct_insert_task_id_,
                    use_put_,
                    ddl_task_id_);


OB_SERIALIZE_MEMBER((ObDASLockRtDef, ObDASDMLBaseRtDef),
                    for_upd_wait_time_);

OB_SERIALIZE_MEMBER((ObDASInsCtDef, ObDASDMLBaseCtDef),
                    table_rowkey_cids_,
                    table_rowkey_types_);

OB_SERIALIZE_MEMBER((ObDASUpdCtDef, ObDASDMLBaseCtDef),
                    updated_column_ids_);

OB_SERIALIZE_MEMBER((ObDASLockCtDef, ObDASDMLBaseCtDef),
                    lock_flag_);

ObDASDMLIterator::~ObDASDMLIterator()
{
  if (nullptr != domain_iter_) {
    domain_iter_->~ObDomainDMLIterator();
    domain_iter_ = nullptr;
  }
}

int ObDASDMLIterator::get_next_domain_index_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(domain_iter_) && OB_FAIL(ObDomainDMLIterator::create_domain_dml_iterator(
          allocator_, row_projector_, write_iter_, das_ctdef_, main_ctdef_, domain_iter_))) {
    LOG_WARN("fail to create domain index dml iterator", K(ret));
  } else if (OB_FAIL(domain_iter_->get_next_domain_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next domain_row", K(ret));
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_domain_index_rows(ObNewRow *&rows, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(domain_iter_) && OB_FAIL(ObDomainDMLIterator::create_domain_dml_iterator(
          allocator_, row_projector_, write_iter_, das_ctdef_, main_ctdef_, domain_iter_))) {
    LOG_WARN("fail to create domain index dml iterator", K(ret));
  } else if (OB_FAIL(domain_iter_->get_next_domain_rows(rows, row_count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next domain rows", K(ret));
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_)) {
    if (OB_FAIL(ob_create_row(allocator_, row_projector_->count(), cur_row_))) {
      LOG_WARN("create current row failed", K(ret), K(row_projector_));
    } else if (OB_FAIL(write_buffer_.begin(write_iter_))) {
      LOG_WARN("begin write iterator failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && das_ctdef_->table_param_.get_data_table().is_domain_index()) {
    if (OB_FAIL(get_next_domain_index_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next domain index row", K(ret), K(das_ctdef_->table_param_.get_data_table()));
      }
    }
  } else {
    if (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow *sr = nullptr;
      if (OB_FAIL(write_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                        *sr,
                                                        *row_projector_,
                                                        allocator_,
                                                        *cur_row_))) {
        LOG_WARN("project storage row failed", K(ret));
      } else {
        row = cur_row_;
        LOG_TRACE("get next row from dml das iterator", KPC(sr), KPC(row), K(das_ctdef_));
      }
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASDMLIterator::get_next_rows(ObNewRow *&rows, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  const bool is_domain_index = das_ctdef_->table_param_.get_data_table().is_domain_index();
  row_count = 0;
  if (1 == batch_size_) {
    if (OB_FAIL(get_next_row(rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next row", K(ret), K_(batch_size), K(is_domain_index));
      }
    } else {
      row_count = 1;
    }
  } else {
    if (OB_ISNULL(cur_rows_)) {
      if (OB_FAIL(ob_create_rows(allocator_, batch_size_, row_projector_->count(), cur_rows_))) {
        LOG_WARN("Failed to create rows", K(ret), K_(row_projector));
      } else if (OB_FAIL(write_buffer_.begin(write_iter_))) {
        LOG_WARN("Failed to begin write iterator", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_domain_index) {
      if (OB_FAIL(get_next_domain_index_rows(rows, row_count))) {
        LOG_WARN("fail to get next domain index rows", K(ret));
      }
    } else {
      while (OB_SUCC(ret) && row_count < batch_size_) {
        const ObChunkDatumStore::StoredRow *sr = nullptr;
        if (OB_FAIL(write_iter_.get_next_row(sr))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("Failed to get next row from result iterator", K(ret));
          }
        } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                           *sr,
                                                           *row_projector_,
                                                           allocator_,
                                                           cur_rows_[row_count]))) {
          LOG_WARN("Failed to project storage row", K(ret));
        } else {
          ++row_count;
          LOG_TRACE("Get next rows from dml das iterator", KPC(sr), K(cur_rows_[row_count - 1]), K_(das_ctdef));
        }
      }
      if (OB_SUCC(ret) || OB_LIKELY(OB_ITER_END == ret)) {
        if (0 == row_count) {
          ret = OB_ITER_END;
        } else {
          rows = cur_rows_;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObDASDMLIterator::rewind(const ObDASDMLBaseCtDef *das_ctdef)
{
  int ret = common::OB_SUCCESS;
  cur_row_ = nullptr;
  cur_rows_ = nullptr;
  set_ctdef(das_ctdef);
  if (OB_NOT_NULL(domain_iter_)) {
    if (OB_FAIL(domain_iter_->rewind())) {
      LOG_WARN("fail to rewind for domain iterator", K(ret));
    }
  }
  return ret;
}

void ObDASDMLIterator::set_ctdef(const ObDASDMLBaseCtDef *das_ctdef)
{
  das_ctdef_ = das_ctdef;
  row_projector_ = !das_ctdef_->old_row_projector_.empty() ?
                   &das_ctdef_->old_row_projector_ :
                   &das_ctdef_->new_row_projector_;
  if (OB_NOT_NULL(domain_iter_)) {
    if (!das_ctdef->table_param_.get_data_table().is_domain_index()) {
      // This table isn't domain index, nothing to do.
    } else if (domain_iter_->is_same_domain_type(das_ctdef)) {
      // The das_ctdef and das_ctdef_ are either full-text search or multi-value index.
      domain_iter_->set_ctdef(das_ctdef, row_projector_);
    } else {
      // need to reset domain iter
      domain_iter_->~ObDomainDMLIterator();
      domain_iter_ = nullptr;
    }
  }
}

int ObDASMLogDMLIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml iterator cannot be null", KR(ret), K_(row_iter));
  } else if (OB_FAIL(row_iter_->get_next_row(row))) {
    LOG_WARN("failed to get next row from dml iterator", KR(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row cannot be null", KR(ret), KP(row));
  } else {
    if (OB_FAIL(ObDASUtils::generate_mlog_row(tablet_id_,
                                              dml_param_,
                                              *row,
                                              op_type_,
                                              is_old_row_))) {
      LOG_WARN("failed to generate mlog rows", KR(ret));
    } else if (DAS_OP_TABLE_UPDATE == op_type_) {
      is_old_row_ = !is_old_row_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::init(ObIAllocator &allocator,
                                         int64_t datum_cnt,
                                         bool strip_lob_locator)
{
  int ret = DatumShadowStoredRow::init(allocator, datum_cnt);
  strip_lob_locator_ = strip_lob_locator;
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::init(ObIAllocator &allocator,
                                         const ObIArray<ObObjMeta> &col_types,
                                         bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(allocator, col_types.count(), strip_lob_locator))) {
    LOG_WARN("init datum buffer failed", K(ret), K(col_types));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_types.count(); ++i) {
    ObObjDatumMapType obj_datum_map = ObDatum::get_obj_datum_map_type(
        col_types.at(i).get_type());
    if (OB_LIKELY(OBJ_DATUM_NULL != obj_datum_map)) {
      total_reserved_size_ += ObDatum::get_reserved_size(obj_datum_map);
    }
  }
  if (OB_SUCC(ret) && total_reserved_size_ > 0) {
    if (OB_ISNULL(reserved_buffer_ = static_cast<char *>(allocator.alloc(total_reserved_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(total_reserved_size_), K(ret));
    }
  }
  column_types_ = &col_types;
  reserve_datum_buf_ = true;
  if (OB_SUCC(ret)) {
    reset_datum_ptr();
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::shadow_copy(const ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL datums or count mismatch", K(ret), KPC(store_row_), K(exprs.count()));
  } else {
    ObDatum *datum = nullptr;
    ObDatum *cells = store_row_->cells();
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      if (OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
        LOG_WARN("failed to evaluate expr datum", K(ret), K(i));
      } else if (lib::is_oracle_mode() && !datum->is_null()
                 && exprs.at(i)->obj_meta_.is_lob_locator()
                 && strip_lob_locator_) {
        ObString payload;
        if (column_types_ != nullptr && !column_types_->at(i).is_lob()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column type", K(ret), K(column_types_->at(i)));
        } else if (OB_FAIL(datum->get_lob_locator().get_payload(payload))) {
          LOG_WARN("get lob payload failed", K(ret));
        } else {
          cells[i].set_string(payload);
        }
      } else {
        cells[i] = *datum;
      }
      if (OB_SUCC(ret)) {
        store_row_->row_size_ += cells[i].len_;
      }
    }
    saved_ = true;
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::shadow_copy(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != row.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL datums or count mismatch", K(ret), KPC(store_row_), K(row));
  } else {
    ObDatum *cells = store_row_->cells();
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
      if (lib::is_oracle_mode() && row.get_cell(i).is_lob_locator() && strip_lob_locator_) {
        ObString payload;
        if (column_types_ != nullptr && !column_types_->at(i).is_lob()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column type", K(ret), K(column_types_->at(i)));
        } else if (OB_FAIL(row.get_cell(i).get_lob_locator()->get_payload(payload))) {
          LOG_WARN("get lob payload failed", K(ret));
        } else {
          cells[i].set_string(payload);
        }
      } else if (OB_FAIL(cells[i].from_obj(row.get_cell(i)))) {
        LOG_WARN("shadow copy obj failed", K(ret), K(i), K(row));
      }
      if (OB_SUCC(ret)) {
        //add the data length of datum
        store_row_->row_size_ += cells[i].len_;
      }
    }
    saved_ = true;
  }
  return ret;
}

ObDASWriteBuffer::ObDASWriteBuffer()
{
  memset(this, 0, sizeof(ObDASWriteBuffer));
}

ObDASWriteBuffer::~ObDASWriteBuffer()
{
  if (datum_store_ != nullptr) {
    datum_store_->~ObChunkDatumStore();
    datum_store_ = nullptr;
  }
}

int ObDASWriteBuffer::init(ObIAllocator &das_alloc,
                           uint32_t row_extend_size,
                           uint64_t tenant_id,
                           const char *label,
                           int64_t mem_ctx_id)
{
  int ret = OB_SUCCESS;
  mem_attr_.tenant_id_ = tenant_id;
  mem_attr_.label_ = label;
  mem_attr_.ctx_id_ = mem_ctx_id;
  das_alloc_ = &das_alloc;
  row_extend_size_ = row_extend_size;
  return ret;
}

int ObDASWriteBuffer::init_dml_shadow_row(int64_t column_cnt, bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_shadow_row_)) {
    void *buff = das_alloc_->alloc(sizeof(DmlShadowRow));
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate dml shadow row failed", K(ret));
    } else {
      dml_shadow_row_ = new(buff) DmlShadowRow();
      if (OB_FAIL(dml_shadow_row_->init(*das_alloc_, column_cnt, strip_lob_locator))) {
        LOG_WARN("init dml shadow row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASWriteBuffer::try_add_row(const ObIArray<ObExpr*> &exprs,
                                  ObEvalCtx *ctx,
                                  const int64_t memory_limit,
                                  DmlRow *&stored_row,
                                  bool &row_added,
                                  bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_shadow_row_)) {
    if (OB_FAIL(init_dml_shadow_row(exprs.count(), strip_lob_locator))) {
      LOG_WARN("init dml shadow row failed", K(ret));
    }
  } else {
    dml_shadow_row_->reuse();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml_shadow_row_->shadow_copy(exprs, *ctx))) {
      LOG_WARN("shadow copy dml row failed", K(ret));
    } else if (OB_FAIL(try_add_row(*dml_shadow_row_, memory_limit, row_added, &stored_row))) {
      LOG_WARN("try add row with shadow row failed", KK(ret));
    } else if (!row_added) {
      // buff已经满了，add row 失败
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stored row is null", K(ret));
    } else {
      LOG_DEBUG("add dml_row pay_load here", KPC(stored_row));
    }

  }
  return ret;
}

int ObDASWriteBuffer::try_add_row(const DmlShadowRow &sr,
                                  const int64_t memory_limit,
                                  bool &row_added,
                                  DmlRow **stored_row)
{
  int ret = OB_SUCCESS;
  const DmlRow *lsr = sr.get_store_row();
  int64_t row_size = lsr->row_size_;
  int64_t simulate_len = - EVENT_CALL(EventTable::EN_DAS_WRITE_ROW_LIST_LEN);
  int64_t final_row_list_len = simulate_len > 0 ? simulate_len : DAS_WRITE_ROW_LIST_LEN;
  if (OB_UNLIKELY(row_size + get_mem_used() > memory_limit && get_mem_used() > 0)) {
    //if the size of the first row exceeds memory_limit,
    //writing is also allowed,
    //ensuring that there is at least one row of data
    row_added = false;
  } else if (OB_LIKELY(buffer_list_.size_ < final_row_list_len)) {
    //link write row buffer to dlist
    //avoid to create ObChunkDatumStore,
    //because it is too heavy for small dml queries
    ret = add_row_to_dlist(sr, row_added, stored_row);
  } else {
    ret = add_row_to_store(sr, memory_limit, row_added, stored_row);
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::create_link_buffer(int64_t row_size, DmlRow *&row_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  DmlRow *dml_row = nullptr;
  int64_t NODE_HEADER_SIZE = sizeof(LinkNode);
  // Note that whether it is a deserialization process or a local add_row, the length of row_size here includes two parts
  // Store the length of ObDatum itself row_size + extended row_extend_size_length
  int64_t buffer_len = NODE_HEADER_SIZE + row_size;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(das_alloc_->alloc(buffer_len, mem_attr_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf failed", K(ret), K(buffer_len));
  } else {
    reinterpret_cast<LinkNode*>(buf)->next_ = nullptr;
    dml_row = new(buf + NODE_HEADER_SIZE) DmlRow();
    if (buffer_list_.tailer_.next_ != nullptr) {
      char *row_ptr = reinterpret_cast<char *>(buffer_list_.tailer_.next_);
      LinkNode *last_node = reinterpret_cast<LinkNode*>(row_ptr - NODE_HEADER_SIZE);
      last_node->next_ = dml_row;
      buffer_list_.tailer_.next_ = dml_row;
    } else {
      buffer_list_.header_.next_ = dml_row;
      buffer_list_.tailer_.next_ = dml_row;
    }
    if (OB_SUCC(ret)) {
      buffer_list_.mem_used_ += buffer_len;
      buffer_list_.size_ += 1;
      row_buffer = dml_row;
    }
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::add_row_to_dlist(const ObIArray<ObExpr*> &exprs,
                                                 ObEvalCtx *ctx,
                                                 int64_t row_size,
                                                 bool &row_added)
{
  int ret = OB_SUCCESS;
  DmlRow *dml_row = nullptr;
  if (OB_FAIL(create_link_buffer(row_size + row_extend_size_, dml_row))) {
    LOG_WARN("create link buffer failed", K(ret));
  } else if (OB_FAIL(DmlRow::build(dml_row, exprs, *ctx, (char *)dml_row, row_size))) {
    LOG_WARN("build stored row failed", K(ret));
  } else {
    row_added = true;
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::add_row_to_dlist(const ObChunkDatumStore::ShadowStoredRow &sr,
                                                 bool &row_added,
                                                 DmlRow **stored_row)
{
  int ret = OB_SUCCESS;
  DmlRow *dml_row = nullptr;
  const DmlRow *lsr = sr.get_store_row();
  if (OB_FAIL(create_link_buffer(lsr->row_size_ + row_extend_size_, dml_row))) {
    LOG_WARN("create link buffer failed", K(ret));
  } else {
    char *buf = dml_row->payload_;
    int64_t buf_size = lsr->row_size_ + row_extend_size_ - ROW_HEAD_SIZE;
    if (OB_FAIL(dml_row->copy_shadow_datums(lsr->cells(), lsr->cnt_, buf, buf_size,
                                            lsr->row_size_ + row_extend_size_, row_extend_size_/*extra_size*/))) {
      LOG_WARN("failed to deep copy row", K(ret), K(lsr->row_size_), K(buf_size));
    } else {
      row_added = true;
      if (stored_row != nullptr) {
        *stored_row = dml_row;
      }
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::create_datum_store()
{
  int ret = OB_SUCCESS;
  void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore), mem_attr_);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate chunk datum store failed", K(ret));
  } else {
    datum_store_ = new(buf) ObChunkDatumStore(mem_attr_.label_);
    if (OB_FAIL(datum_store_->init(UINT64_MAX,
                                   mem_attr_.tenant_id_,
                                   mem_attr_.ctx_id_,
                                   mem_attr_.label_,
                                   false/*enable_dump*/,
                                   row_extend_size_))) {
      LOG_WARN("init datum store failed", K(ret), K(mem_attr_));
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::add_row_to_store(const ObIArray<ObExpr*> &exprs,
                                                   ObEvalCtx *ctx,
                                                   const int64_t memory_limit,
                                                   bool &row_added)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_store_)) {
    if (OB_FAIL(create_datum_store())) {
      LOG_WARN("create datum store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_store_->try_add_row(exprs, ctx, memory_limit - buffer_list_.mem_used_, row_added))) {
      LOG_WARN("try add row to store failed", K(ret), K(memory_limit), K_(buffer_list_.mem_used));
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::add_row_to_store(const ObChunkDatumStore::ShadowStoredRow &sr,
                                                   const int64_t memory_limit,
                                                   bool &row_added,
                                                   ObChunkDatumStore::StoredRow **stored_sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_store_)) {
    if (OB_FAIL(create_datum_store())) {
      LOG_WARN("create datum store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_store_->try_add_row(sr, memory_limit - buffer_list_.mem_used_, row_added, stored_sr))) {
      LOG_WARN("try add row to store failed", K(ret), K(memory_limit), K_(buffer_list_.mem_used));
    }
  }
  return ret;
}

int ObDASWriteBuffer::begin(Iterator &it)
{
  int ret = OB_SUCCESS;
  it.cur_row_ = buffer_list_.header_.next_;
  if (OB_UNLIKELY(datum_store_ != nullptr)) {
    void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore::Iterator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das allocator failed", K(ret));
    } else {
      it.datum_iter_ = new(buf) ObChunkDatumStore::Iterator();
      ret = datum_store_->begin(*it.datum_iter_);
    }
  }
  return ret;
}

int ObDASWriteBuffer::begin(NewRowIterator &it, const ObIArray<ObObjMeta> &col_types)
{
  int ret = OB_SUCCESS;
  it.cur_row_ = buffer_list_.header_.next_;
  if (OB_UNLIKELY(datum_store_ != nullptr)) {
    void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore::Iterator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das allocator failed", K(ret));
    } else {
      it.datum_iter_ = new(buf) ObChunkDatumStore::Iterator();
      ret = datum_store_->begin(*it.datum_iter_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_create_row(*das_alloc_, col_types.count(), it.cur_new_row_))) {
      LOG_WARN("create new row failed", K(ret));
    } else {
      it.col_types_ = &col_types;
    }
  }
  return ret;
}

int ObDASWriteBuffer::dump_data(const ObDASDMLBaseCtDef &das_base_ctdef) const
{
  int ret = OB_SUCCESS;
  ObNewRow *new_row = NULL;
  ObNewRow *old_row = NULL;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  int64_t rownum = 0;
  ObArenaAllocator tmp_alloc;
  ObDatum *trans_info_datum = nullptr;
  ObDASWriteBuffer::Iterator write_iter_tmp;
  ObString trans_info_str;

  if (OB_FAIL(const_cast<ObDASWriteBuffer*>(this)->begin(write_iter_tmp))) {
    LOG_WARN("get write iter failed", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(write_iter_tmp.get_next_row(store_row))) {
    trans_info_str.reset();
    if (OB_ISNULL(store_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!das_base_ctdef.old_row_projector_.empty()) {
        // create old row
        if (OB_ISNULL(old_row)
            && OB_FAIL(ob_create_row(tmp_alloc, das_base_ctdef.old_row_projector_.count(), old_row))) {
          LOG_WARN("create old row buffer failed", K(ret), K(das_base_ctdef.old_row_projector_.count()));
        } else if (OB_FAIL(ObDASUtils::project_storage_row(das_base_ctdef,
                                                            *store_row,
                                                            das_base_ctdef.old_row_projector_,
                                                            tmp_alloc,
                                                            *old_row))) {
          LOG_WARN("project storage row failed", K(ret), K(das_base_ctdef.old_row_projector_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!das_base_ctdef.new_row_projector_.empty()) {
        if (OB_ISNULL(new_row)
            && OB_FAIL(ob_create_row(tmp_alloc, das_base_ctdef.new_row_projector_.count(), new_row))) {
          LOG_WARN("create new row buffer failed", K(ret), K(das_base_ctdef.new_row_projector_.count()));
        } else if (OB_FAIL(ObDASUtils::project_storage_row(das_base_ctdef,
                                                           *store_row,
                                                           das_base_ctdef.new_row_projector_,
                                                           tmp_alloc,
                                                           *new_row))) {
          LOG_WARN("project storage row failed", K(ret), K(das_base_ctdef.new_row_projector_));
        }

      }
    }

    if (OB_SUCC(ret)) {
      if (row_extend_size_ > ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE) {
        // It means the payload contain trans_info string
        char *buf = static_cast<char *>(store_row->get_extra_payload());
        int32_t *str_len = reinterpret_cast<int32_t *>(store_row->get_extra_payload());
        int64_t pos = sizeof(int32_t);
        trans_info_str.assign(buf + pos, *str_len);
      }
    }

    if (OB_SUCC(ret)) {
      // do print
      LOG_INFO("DASWriteBuffer dump", K(rownum), "task_type", das_base_ctdef.op_type_,
          K(trans_info_str), KPC(new_row), KPC(old_row), KPC(store_row));
      rownum++;
    }
  } // end while

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObDASWriteBuffer::DmlRow *ObDASWriteBuffer::get_next_dml_row(DmlRow *cur_row)
{
  int64_t NODE_HEADER_SIZE = sizeof(LinkNode);
  char *row_ptr = reinterpret_cast<char *>(cur_row);
  LinkNode *link_node = reinterpret_cast<LinkNode*>(row_ptr - NODE_HEADER_SIZE);
  return link_node->next_;
}

OB_DEF_SERIALIZE(ObDASWriteBuffer)
{
  int ret = OB_SUCCESS;
  bool has_more = (datum_store_ != nullptr);
  OZ(serialize_buffer_list(buf, buf_len, pos));
  OB_UNIS_ENCODE(has_more);
  if (has_more) {
    OB_UNIS_ENCODE(*datum_store_);
  }
  OB_UNIS_ENCODE(row_extend_size_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASWriteBuffer)
{
  int64_t len = 0;
  bool has_more = (datum_store_ != nullptr);
  len += get_buffer_list_serialize_size();
  OB_UNIS_ADD_LEN(has_more);
  if (has_more) {
    OB_UNIS_ADD_LEN(*datum_store_);
  }
  OB_UNIS_ADD_LEN(row_extend_size_);
  return len;
}

OB_DEF_DESERIALIZE(ObDASWriteBuffer)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  OZ(deserialize_buffer_list(buf, data_len, pos));
  OB_UNIS_DECODE(has_more);
  if (OB_SUCC(ret) && has_more) {
    void *buffer = das_alloc_->alloc(sizeof(ObChunkDatumStore), mem_attr_);
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate chunk row store failed", K(ret));
    } else {
      datum_store_ = new(buffer) ObChunkDatumStore(mem_attr_.label_);
      OB_UNIS_DECODE(*datum_store_);
    }
  }
  OB_UNIS_DECODE(row_extend_size_);
  return ret;
}

int ObDASWriteBuffer::serialize_buffer_list(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(buffer_list_.size_);
  if (OB_SUCC(ret)) {
    DmlRow *dml_row = buffer_list_.header_.next_;
    while (OB_SUCC(ret) && dml_row != nullptr) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, dml_row->row_size_))) {
        LOG_WARN("serialize row size failed", K(ret), K(dml_row->row_size_));
      } else if (dml_row->row_size_ > buf_len - pos) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("serialize write buffer overflow", K(ret), K(buf_len), K(pos), K(dml_row->row_size_));
      } else {
        DmlRow *tmp_row = reinterpret_cast<DmlRow*>(buf + pos);
        MEMCPY(tmp_row, dml_row, dml_row->row_size_);
        DmlRow::unswizzling_datum(tmp_row->cells(), dml_row->cnt_, dml_row->payload_);
        pos += dml_row->row_size_;
        dml_row = ObDASWriteBuffer::get_next_dml_row(dml_row);
      }
    }
  }
  return ret;
}

int64_t ObDASWriteBuffer::get_buffer_list_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(buffer_list_.size_);
  DmlRow *dml_row = buffer_list_.header_.next_;
  while (dml_row != nullptr) {
    OB_UNIS_ADD_LEN(dml_row->row_size_);
    len += dml_row->row_size_;
    dml_row = ObDASWriteBuffer::get_next_dml_row(dml_row);
  }

  return len;
}

int ObDASWriteBuffer::deserialize_buffer_list(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t list_size = 0;
  OB_UNIS_DECODE(list_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < list_size; ++i) {
    int64_t row_size = 0;
    DmlRow *row_buffer = nullptr;
    OB_UNIS_DECODE(row_size);
    OZ(create_link_buffer(row_size, row_buffer));
    if (OB_SUCC(ret)) {
      //deserialize row buffer data
      MEMCPY(row_buffer, buf + pos, row_size);
      row_buffer->swizzling(row_buffer->payload_);
      pos += row_buffer->row_size_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::Iterator::get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    sr = cur_row_;
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_iter_->get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("returned stored row is NULL", K(ret));
  }
  return ret;
}

int ObDASWriteBuffer::Iterator::get_next_row_skip_const(ObEvalCtx &ctx, const ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    ret = cur_row_->to_expr(exprs, ctx);
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else {
    ret = datum_iter_->get_next_row(ctx, exprs);
  }

  return ret;
}

int ObDASWriteBuffer::NewRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const DmlRow *sr = nullptr;
  row = nullptr;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    sr = cur_row_;
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_iter_->get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from datum iter failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && sr != nullptr) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sr->cnt_; ++i) {
      if (OB_FAIL(sr->cells()[i].to_obj(cur_new_row_->cells_[i], col_types_->at(i)))) {
        LOG_WARN("convert datum to ObNewRow failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      row = cur_new_row_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::get_stored_row_size(const ObIArray<ObExpr*> &exprs,
                                          ObEvalCtx &ctx,
                                          int64_t &size)
{
  return ObChunkDatumStore::Block::row_store_size(exprs, ctx, size);
}
}  // namespace sql
}  // namespace oceanbase
