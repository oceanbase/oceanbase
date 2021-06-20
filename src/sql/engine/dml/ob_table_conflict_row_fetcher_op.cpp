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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_factory.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher_op.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher.h"  // for mock_fetcher
#include "sql/engine/dml/ob_table_modify.h"

namespace oceanbase {
using namespace storage;
using namespace share;
namespace sql {

OB_DEF_SERIALIZE(ObTCRFetcherOpInput)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("conflict fetcher seri", K(part_conflict_rows_));
  OB_UNIS_ENCODE(part_conflict_rows_.count());
  ARRAY_FOREACH(part_conflict_rows_, i)
  {
    if (OB_ISNULL(part_conflict_rows_.at(i).conflict_datum_store_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null");
    }
    OB_UNIS_ENCODE(part_conflict_rows_.at(i).part_key_);
    OB_UNIS_ENCODE(*part_conflict_rows_.at(i).conflict_datum_store_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTCRFetcherOpInput)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(part_conflict_rows_.count());
  ARRAY_FOREACH_NORET(part_conflict_rows_, i)
  {
    if (part_conflict_rows_.at(i).conflict_datum_store_ != NULL) {
      OB_UNIS_ADD_LEN(part_conflict_rows_.at(i).part_key_);
      OB_UNIS_ADD_LEN(*part_conflict_rows_.at(i).conflict_datum_store_);
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObTCRFetcherOpInput)
{
  int ret = OB_SUCCESS;
  int64_t conflict_row_cnt = 0;
  void* ptr = NULL;
  ObChunkDatumStore* datum_store = NULL;
  OB_UNIS_DECODE(conflict_row_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_row_cnt; ++i) {
    OV(OB_NOT_NULL(ptr = alloc_->alloc(sizeof(ObChunkDatumStore))), OB_ALLOCATE_MEMORY_FAILED);
    OX(datum_store = new (ptr) ObChunkDatumStore(alloc_));
    CK(OB_NOT_NULL(datum_store));
    OZ(datum_store->init(UINT64_MAX,
        common::OB_SERVER_TENANT_ID,
        ObCtxIds::DEFAULT_CTX_ID,
        common::ObModIds::OB_SQL_CHUNK_ROW_STORE,
        false /*enable_dump*/));
    if (OB_SUCC(ret)) {
      ObPartConflictDatumStore conflict_datum_store;
      OB_UNIS_DECODE(conflict_datum_store.part_key_);
      OB_UNIS_DECODE(*datum_store);
      if (OB_SUCC(ret)) {
        conflict_datum_store.conflict_datum_store_ = datum_store;
        if (OB_FAIL(part_conflict_rows_.push_back(conflict_datum_store))) {
          LOG_WARN("store conflict datum_store failed", K(ret));
        }
      }
      if (OB_FAIL(ret) && datum_store != NULL) {
        datum_store->~ObChunkDatumStore();
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("conflict fetcher desc", K(part_conflict_rows_));
  }
  return ret;
}

int ObTCRFetcherOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  alloc_ = &exec_ctx_.get_allocator();
  ObPartConflictDatumStore conflict_datum_store;
  ObIArray<ObTaskInfo::ObPartLoc>& part_locs = task_info.get_range_location().part_locs_;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_locs.count(); ++i) {
    uint64_t part_key_ref_id = part_locs.at(i).part_key_ref_id_;
    if (part_key_ref_id == spec_.get_id()) {
      conflict_datum_store.part_key_ = part_locs.at(i).partition_key_;
      conflict_datum_store.conflict_datum_store_ = part_locs.at(i).datum_store_;
      if (OB_FAIL(part_conflict_rows_.push_back(conflict_datum_store))) {
        LOG_WARN("store part conflict row failed", K(ret));
      }
    }
  }
  LOG_DEBUG("build TCRFetcher Input", K(ret), K_(part_conflict_rows), K(task_info));
  return ret;
}

int ObConflictDatumIterator::init(sql::ObChunkDatumStore* datum_store)
{
  int ret = OB_SUCCESS;
  OZ(datum_store->begin(row_iter_));
  return ret;
}

int ObConflictDatumIterator::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* sr = NULL;
  if (OB_FAIL(row_iter_.get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char* buf = NULL;
    CK(OB_NOT_NULL(alloc_));
    if (NULL == checker_row_.cells_) {
      OV(OB_NOT_NULL(buf = reinterpret_cast<char*>(alloc_->alloc(sizeof(ObObj) * col_cnt_))),
          OB_ALLOCATE_MEMORY_FAILED);
      OX(checker_row_.cells_ = new (buf) ObObj[col_cnt_]);
      OX(checker_row_.count_ = col_cnt_);
    }
    if (OB_SUCC(ret) && col_cnt_ != sr->cnt_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("count is diff", K(col_cnt_), K(sr->cnt_), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
      OZ(sr->cells()[i].to_obj(checker_row_.cells_[i], exprs_[i]->obj_meta_, exprs_[i]->obj_datum_map_));
    }
  }
  if (OB_SUCC(ret)) {
    row = &checker_row_;
    LOG_DEBUG("conflict checker row", K(checker_row_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObTableConflictRowFetcherSpec, ObOpSpec), table_id_, index_tid_, conf_col_ids_, access_col_ids_,
    conflict_exprs_, access_exprs_, only_data_table_);

int ObTableConflictRowFetcherOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam dml_param;
  dml_param.output_exprs_ = &MY_SPEC.access_exprs_;
  dml_param.op_ = this;
  dml_param.op_filters_ = NULL;
  dml_param.row2exprs_projector_ = &row2exprs_projector_;
  OZ(ObTableModify::init_dml_param_se(ctx_, MY_SPEC.index_tid_, MY_SPEC.only_data_table_, NULL, dml_param));
  OZ(fetch_conflict_rows(dml_param));
  LOG_DEBUG("open conflict row fetcher");
  return ret;
}

int ObTableConflictRowFetcherOp::inner_close()
{
  for (int64_t i = 0; i < dup_row_iter_arr_.count(); ++i) {
    if (dup_row_iter_arr_.at(i) != NULL) {
      ObQueryIteratorFactory::free_insert_dup_iter(dup_row_iter_arr_.at(i));
      dup_row_iter_arr_.at(i) = NULL;
    }
  }
  return ObOperator::inner_close();
}

int ObTableConflictRowFetcherOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObTCRFetcherOpInput* fetcher_input = dynamic_cast<ObTCRFetcherOpInput*>(get_input());
  CK(OB_NOT_NULL(fetcher_input));
  ObNewRow* dup_row = NULL;
  if (OB_SUCC(ret)) {
    bool find_next_iter = false;
    do {
      if (find_next_iter) {
        ++cur_row_idx_;
        find_next_iter = false;
      }
      if (OB_UNLIKELY(cur_row_idx_ >= dup_row_iter_arr_.count())) {
        ret = OB_ITER_END;
        LOG_DEBUG("fetch conflict row iterator end");
      } else {
        ObNewRowIterator* dup_row_iter = dup_row_iter_arr_.at(cur_row_idx_);
        if (OB_ISNULL(dup_row_iter)) {
          find_next_iter = true;
        } else if (OB_FAIL(dup_row_iter->get_next_row(dup_row))) {
          // may return not implemented for engine 3.0
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            find_next_iter = true;
          } else {
            LOG_WARN("get next row from duplicated iter failed", K(ret));
          }
        } else if (OB_ISNULL(dup_row)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), KP(dup_row));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.access_exprs_.count(); i++) {
            const ObObj& cell = dup_row->cells_[i];
            ObDatum& datum = MY_SPEC.access_exprs_.at(i)->locate_datum_for_write(eval_ctx_);
            ObExpr* expr = MY_SPEC.access_exprs_.at(i);
            if (cell.is_null()) {
              datum.set_null();
            } else if (cell.get_type() != expr->datum_meta_.type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type mismatch", K(ret), K(i), K(cell.get_type()), K(*expr));
            } else if (OB_FAIL(datum.from_obj(cell, expr->obj_datum_map_))) {
              LOG_WARN("convert obj to datum failed", K(ret));
            } else {
              MY_SPEC.access_exprs_.at(i)->get_eval_info(eval_ctx_).evaluated_ = true;
            }
          }  // for end
          if (OB_SUCC(ret)) {
            LOG_DEBUG("fetch dup row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.access_exprs_), K(*dup_row));
          }
        }
      }
    } while (OB_SUCC(ret) && find_next_iter);
  }
  return ret;
}

int ObTableConflictRowFetcherOp::fetch_conflict_rows(ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  ObTCRFetcherOpInput* fetcher_input = dynamic_cast<ObTCRFetcherOpInput*>(get_input());
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = ctx_.get_my_session();
  ObPartitionService* partition_service = NULL;
  CK(OB_NOT_NULL(fetcher_input));
  CK(OB_NOT_NULL(my_session));
  CK(OB_NOT_NULL(executor_ctx));
  OV(OB_NOT_NULL(partition_service = executor_ctx->get_partition_service()));
  if (OB_SUCC(ret)) {
    ObIArray<ObPartConflictDatumStore>& part_conflict_rows = fetcher_input->part_conflict_rows_;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_conflict_rows.count(); ++i) {
      const ObPartitionKey& pkey = part_conflict_rows.at(i).part_key_;
      LOG_DEBUG("exprs", K(MY_SPEC.conflict_exprs_), K(MY_SPEC.access_exprs_));
      ObConflictDatumIterator conf_iter(
          MY_SPEC.conflict_exprs_.get_data(), MY_SPEC.conflict_exprs_.count(), &ctx_.get_allocator());
      OZ(conf_iter.init(part_conflict_rows.at(i).conflict_datum_store_));
      OZ(partition_service->fetch_conflict_rows(my_session->get_trans_desc(),
          dml_param,
          pkey,
          MY_SPEC.conf_col_ids_,
          MY_SPEC.access_col_ids_,
          conf_iter,
          dup_row_iter_arr_));
      LOG_DEBUG("after fetch conflict rows", K(dup_row_iter_arr_));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
