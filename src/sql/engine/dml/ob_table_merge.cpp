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
#include "sql/engine/dml/ob_table_merge.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace storage;
using storage::INSERT_RETURN_ONE_DUP;
using storage::ObPartitionService;

namespace sql {

//////////////////////////// ObTableMergeCtx ////////////////////////////////////
ObTableMerge::ObTableMergeCtx::ObTableMergeCtx(ObExecContext& ctx)
    : ObTableModifyCtx(ctx),
      update_row_(),
      target_rowkey_(),
      old_row_(),
      new_row_(),
      insert_row_(),
      insert_row_store_(ctx.get_allocator(), ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false),
      affected_rows_(0)
{}

ObTableMerge::ObTableMergeCtx::~ObTableMergeCtx()
{}

int ObTableMerge::ObTableMergeCtx::init(ObExecContext& ctx, bool has_insert_clause, bool has_update_clause,
    int64_t insert_row_column_count, int64_t target_rowkey_column_count, int64_t update_row_columns_count)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (has_insert_clause) {
    if (insert_row_column_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(insert_row_column_count), K(ret));
    } else if (OB_FAIL(alloc_row_cells(insert_row_column_count, insert_row_))) {
      LOG_WARN("fail to allocate row cells", K(insert_row_column_count), K(ret));
    }
  }

  if (OB_SUCC(ret) && has_update_clause) {
    if (OB_FAIL(alloc_row_cells(insert_row_column_count, old_row_))) {
      LOG_WARN("fail to allocate row cells", K(insert_row_column_count), K(ret));
    } else if (OB_FAIL(alloc_row_cells(insert_row_column_count, new_row_))) {
      LOG_WARN("fail to allocate row cells", K(insert_row_column_count), K(ret));
    } else if (OB_FAIL(alloc_row_cells(target_rowkey_column_count, target_rowkey_))) {
      LOG_WARN("fail to allocate row cells", K(target_rowkey_column_count), K(ret));
    } else if (OB_FAIL(alloc_row_cells(update_row_columns_count, update_row_))) {
      LOG_WARN("fail to create cur row", K(ret), K(update_row_columns_count));
    }
  }
  return ret;
}
//////////////////////////// ObTableMerge ////////////////////////////////////

ObTableMerge::ObTableMerge(common::ObIAllocator& alloc)
    : ObTableModify(alloc),
      has_insert_clause_(false),
      has_update_clause_(false),
      match_conds_(),
      delete_conds_(),
      update_conds_(),
      insert_conds_(),
      rowkey_desc_(alloc),
      scan_column_ids_(alloc),
      update_related_column_ids_(alloc),
      updated_column_ids_(alloc),
      delete_column_ids_(alloc),
      updated_column_infos_(alloc),
      assignment_infos_(alloc),
      old_projector_(NULL),
      old_projector_size_(0),
      new_projector_(NULL),
      new_projector_size_(0),
      delete_projector_(NULL),
      delete_projector_size_(0),
      update_origin_projector_(nullptr),
      update_origin_projector_size_(0)
{}

int ObTableMerge::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableMergeCtx* merge_ctx = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  int64_t update_row_column_count = scan_column_ids_.count() + assignment_infos_.count();
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_FAIL(merge_ctx->init(ctx,
                 has_insert_clause_,
                 has_update_clause_,
                 column_ids_.count(),
                 rowkey_desc_.count(),
                 update_row_column_count))) {
    LOG_WARN("fail to init merge ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*merge_ctx, true))) {
    LOG_WARN("fail to init cur row", K(ret));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL((my_session = GET_MY_SESSION(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child op", K(ret), K(child_op_));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    merge_ctx->dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    merge_ctx->dml_param_.schema_version_ = schema_version;
    merge_ctx->dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    merge_ctx->dml_param_.sql_mode_ = my_session->get_sql_mode();
    merge_ctx->dml_param_.tz_info_ = TZ_INFO(my_session);
    merge_ctx->dml_param_.table_param_ = &table_param_;
    merge_ctx->dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    // merge operator uses the output fileds to create its insert row
    // the projector_ is better to be insert_row_projector_
    merge_ctx->get_cur_row().projector_ = NULL;
    merge_ctx->get_cur_row().projector_size_ = 0;
    if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_merge(ctx))) {
        LOG_WARN("do table merge failed", K(ret));
      }
    }
  }

  return ret;
}
int ObTableMerge::do_table_merge(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTableMergeCtx* merge_ctx = NULL;
  const ObNewRow* child_row = NULL;
  ObExprCtx* expr_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObPartitionKey* pkey;
  if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (merge_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(merge_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(expr_ctx = &merge_ctx->expr_ctx_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("the expr ctx is null", K(ret));
  } else if (OB_ISNULL((my_session = GET_MY_SESSION(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL((executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL((partition_service = executor_ctx->get_partition_service()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, merge_ctx->part_infos_))) {
    LOG_WARN("failed to get partition location", K(ret));
  } else if (merge_ctx->part_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of part info is not right", K(ret), K(merge_ctx->part_infos_.count()));
  } else if (OB_ISNULL(pkey = &merge_ctx->part_infos_.at(0).partition_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition key is null", K(ret), K(*pkey));
  } else if (OB_FAIL(set_autoinc_param_pkey(ctx, *pkey))) {
    LOG_WARN("set autoinc param pkey failed", K(*pkey), K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(child_op_->get_next_row(ctx, child_row))) {
      bool is_match = false;
      if (OB_FAIL(copy_cur_row_by_projector(*merge_ctx, child_row))) {
        LOG_WARN("failed to copy cur row by projector", K(ret));
      } else if (OB_FAIL(check_is_match(*expr_ctx, *child_row, is_match))) {
        LOG_WARN("failed to check is match", K(ret));
      } else if (is_match) {  // execute update
        if (has_update_clause_ &&
            OB_FAIL(process_update(partition_service, *pkey, ctx, *expr_ctx, merge_ctx->dml_param_, child_row))) {
          LOG_WARN("fail to process update", K(ret));
        }
      } else {  // execute insert
        if (has_insert_clause_ && OB_FAIL(process_insert(partition_service,
                                      my_session->get_trans_desc(),
                                      merge_ctx->dml_param_,
                                      *pkey,
                                      ctx,
                                      *expr_ctx,
                                      child_row))) {
          LOG_WARN("fail to insert row", K(ret));
        }
      }
    }  // end while
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to do table merge", K(ret));
    }

    if (OB_ITER_END == ret &&
        OB_FAIL(insert_all_rows(ctx, partition_service, my_session->get_trans_desc(), merge_ctx->dml_param_, *pkey))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("insert all rows failed", K(ret));
      }
    }

    // deal with different ret
    if (OB_ITER_END != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to deal with merge into", K(ret));
      }
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      plan_ctx->add_affected_rows(merge_ctx->affected_rows_);
    }
  }
  return ret;
}

int ObTableMerge::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* merge_ctx = NULL;
  if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get merge operator context failed", K(ret), K(get_id()));
  } else {
    merge_ctx->reset();
    if (OB_FAIL(ObTableModify::rescan(ctx))) {
      LOG_WARN("rescan child operator failed", K(ret));
    } else if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_merge(ctx))) {
        LOG_WARN("do table merge failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableMerge::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id(), get_type(), op_ctx));
  OV(OB_NOT_NULL(op_ctx));
  return ret;
}

int ObTableMerge::add_match_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(match_conds_, expr);
}

int ObTableMerge::add_delete_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(delete_conds_, expr);
}

int ObTableMerge::add_update_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(update_conds_, expr);
}

int ObTableMerge::add_insert_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(insert_conds_, expr);
}

int ObTableMerge::calc_delete_condition(ObExecContext& ctx, const ObNewRow& input_row, bool& is_true_cond) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* merge_ctx = NULL;
  if (delete_conds_.is_empty()) {
    is_true_cond = false;
  } else if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id), K(ret));
  } else if (calc_condition(merge_ctx->expr_ctx_, merge_ctx->new_row_, input_row, delete_conds_, is_true_cond)) {
    LOG_WARN("fail to calc condition", K(ret));
  }
  return ret;
}

int ObTableMerge::calc_condition(ObExprCtx& expr_ctx, const ObNewRow& row,
    const common::ObDList<ObSqlExpression>& cond_exprs, bool& is_true_cond) const
{
  int ret = OB_SUCCESS;
  bool is_filtered = false;
  is_true_cond = true;
  if (OB_FAIL(filter_row(expr_ctx, row, cond_exprs, is_filtered))) {
    LOG_WARN("failed to filter row", K(ret));
  } else if (is_filtered) {
    is_true_cond = false;
  }
  return ret;
}

int ObTableMerge::calc_condition(ObExprCtx& expr_ctx, const ObNewRow& left_row, const ObNewRow& right_row,
    const common::ObDList<ObSqlExpression>& cond_exprs, bool& is_true_cond) const
{
  int ret = OB_SUCCESS;
  bool is_true = false;
  is_true_cond = true;
  ObObj result;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(cond_exprs.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(expr_ctx.calc_buf_));
  } else {
    DLIST_FOREACH(p, cond_exprs)
    {
      if (OB_ISNULL(p)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is NULL", K(ret));
      } else if (OB_FAIL(p->calc(expr_ctx, left_row, right_row, result))) {
        LOG_WARN("failed to calc expression", K(ret), K(*p), "op_type", ob_phy_operator_type_str(get_type()));
      } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
        LOG_WARN("failed to call is true", K(ret));
      } else if (!is_true) {
        is_true_cond = false;
        break;
      }
    }
  }
  return ret;
}

int ObTableMerge::process_insert(ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
    const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey, ObExecContext& ctx,
    ObExprCtx& expr_ctx, const ObNewRow* input_row) const
{
  int ret = OB_SUCCESS;
  bool is_true_cond = false;
  ObTableMergeCtx* merge_ctx = NULL;
  int64_t cur_affected = 0;
  ObNewRowIterator* duplicated_rows = NULL;
  OV(OB_NOT_NULL(input_row));
  OV(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id())));
  OZ(calc_condition(expr_ctx, *input_row, insert_conds_, is_true_cond));
  if (OB_SUCC(ret) && is_true_cond) {
    OZ(calc_insert_row(ctx, expr_ctx, input_row));
    OZ(check_row_null(ctx, merge_ctx->insert_row_, column_infos_), merge_ctx->insert_row_);
    bool is_filtered = false;
    OZ(ObPhyOperator::filter_row_for_check_cst(
        merge_ctx->expr_ctx_, merge_ctx->insert_row_, check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
    OZ(ForeignKeyHandle::do_handle_new_row(*merge_ctx, fk_args_, merge_ctx->insert_row_), merge_ctx->insert_row_);
    OZ(partition_service->insert_row(trans_desc,
        dml_param,
        pkey,
        column_ids_,
        primary_key_ids_,
        merge_ctx->insert_row_,
        INSERT_RETURN_ONE_DUP,
        cur_affected,
        duplicated_rows));
    OX(merge_ctx->affected_rows_ += cur_affected);
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      OZ(merge_ctx->insert_row_store_.add_row(merge_ctx->insert_row_, false));
    }
    if (nullptr != duplicated_rows) {
      (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
      duplicated_rows = nullptr;
    }
  }
  return ret;
}

int ObTableMerge::insert_all_rows(ObExecContext& ctx, ObPartitionService* partition_service,
    const transaction::ObTransDesc& trans_desc, const storage::ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* merge_ctx = NULL;
  ObRowStore::Iterator insert_row_iter;
  ObNewRow* insert_row = nullptr;
  int64_t cur_affected = 0;
  ObNewRowIterator* duplicated_rows = NULL;
  CK(OB_NOT_NULL(partition_service));
  CK(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id())));

  insert_row_iter = merge_ctx->insert_row_store_.begin();
  while (OB_SUCC(ret) && (OB_SUCC(insert_row_iter.get_next_row(insert_row, nullptr)))) {
    if (OB_FAIL(partition_service->insert_row(trans_desc,
            dml_param,
            pkey,
            column_ids_,
            primary_key_ids_,
            *insert_row,
            INSERT_RETURN_ONE_DUP,
            cur_affected,
            duplicated_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("fail to insert row", K(pkey), K(column_ids_), K(ret));
      }
    } else {
      merge_ctx->affected_rows_ += cur_affected;
    }

    if (nullptr != duplicated_rows) {
      (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
    }
  }
  return ret;
}

int ObTableMerge::calc_insert_row(ObExecContext& ctx, ObExprCtx& expr_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* merge_ctx = NULL;
  if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_FAIL(calculate_row(expr_ctx, *const_cast<ObNewRow*>(insert_row)))) {
    LOG_WARN("failed to calculate row", K(ret));
  } else if (OB_FAIL(copy_row(*insert_row, merge_ctx->insert_row_, projector_, projector_size_))) {
    LOG_WARN("failed to copy row", K(ret));
  }
  return ret;
}

int ObTableMerge::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModify::inner_close(ctx))) {
    LOG_WARN("fail to do inner close", K(ret));
  }
  return ret;
}

int ObTableMerge::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  UNUSED(ctx);
  UNUSED(row);
  return OB_ITER_END;
}

int64_t ObTableMerge::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID, table_id_, N_REF_TID, index_tid_, N_CID, column_ids_);
  return pos;
}

int ObTableMerge::set_rowkey_desc(const common::ObIArray<int64_t>& rowkey_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_desc_.init(rowkey_desc.count()))) {
    LOG_WARN("fail to init rowkey_desc", K(rowkey_desc), K(ret));
  } else if (OB_FAIL(append(rowkey_desc_, rowkey_desc))) {
    LOG_WARN("failed to append rowkey desc", K(ret));
  }
  return ret;
}

int ObTableMerge::check_is_match(ObExprCtx& expr_ctx, const ObNewRow& input_row, bool& is_match) const
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (rowkey_desc_.empty()) {
    // for compatibility, rowkey_desc_ may not be given in a lower version
    if (has_update_clause_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey desc is not expected to be empty", K(ret), K(rowkey_desc_), K(has_update_clause_));
    } else if (OB_FAIL(calc_condition(expr_ctx, input_row, match_conds_, is_match))) {
      LOG_WARN("failed to calc condition", K(ret));
    }
  } else {
    // create table t (c1 int, c2 int) partition by hash(c1) partitions 4;
    // the primary key is (c1, pk_inc_), and c1 can be null
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < rowkey_desc_.count(); ++i) {
      int64_t idx = rowkey_desc_.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= input_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey index is invalid", K(ret), K(rowkey_desc_), K(i));
      } else {
        is_match = !input_row.cells_[idx].is_null();
      }
    }
  }
  return ret;
}

int ObTableMerge::process_update(ObPartitionService* partition_service, const ObPartitionKey& pkey, ObExecContext& ctx,
    ObExprCtx& expr_ctx, storage::ObDMLBaseParam& dml_param, const ObNewRow* input_row) const
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  bool need_delete = false;
  bool is_conflict = false;
  OV(OB_NOT_NULL(input_row));
  OZ(calc_condition(expr_ctx, *input_row, update_conds_, need_update));
  if (need_update) {
    OZ(generate_origin_row(ctx, input_row, is_conflict));
    OZ(calc_update_row(ctx, input_row));
    OZ(calc_delete_condition(ctx, *input_row, need_delete));
    OV(!is_conflict, need_delete ? OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS : OB_ERR_UPDATE_TWICE);
    OZ(update_row(partition_service, pkey, need_delete, ctx, dml_param));
  }
  return ret;
}

int ObTableMerge::generate_origin_row(ObExecContext& ctx, const ObNewRow* input_row, bool& is_conflict) const
{
  int ret = OB_SUCCESS;
  bool is_distinct = true;
  is_conflict = false;
  ObTableMergeCtx* merge_ctx = nullptr;
  CK(OB_NOT_NULL(update_origin_projector_) && OB_LIKELY(update_origin_projector_size_ > 0) &&
      OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id())));

  if (OB_SUCC(ret)) {
    merge_ctx->get_update_row().projector_ = NULL;
    merge_ctx->get_update_row().projector_size_ = 0;
    if (OB_FAIL(copy_row(
            *input_row, merge_ctx->get_update_row(), update_origin_projector_, update_origin_projector_size_))) {
      LOG_WARN("failed to copy row", K(ret));
    } else if (OB_FAIL(check_rowkey_whether_distinct(ctx,
                   merge_ctx->get_update_row(),
                   primary_key_ids_.count(),
                   T_HASH_DISTINCT,
                   merge_ctx->rowkey_dist_ctx_,
                   is_distinct))) {
      LOG_WARN("faield to check whether row distinct", K(ret));
    } else if (!is_distinct) {
      is_conflict = true;
    }
  }
  return ret;
}

int ObTableMerge::calc_update_row(ObExecContext& ctx, const ObNewRow* input_row) const
{
  int ret = OB_SUCCESS;
  ObTableMergeCtx* merge_ctx = NULL;
  ObExprCtx expr_ctx;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id())) || OB_ISNULL(plan_ctx) ||
      OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is NULL", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expression context failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignment_infos_.count(); i++) {
    int64_t result_index = OB_INVALID_INDEX;
    ObColumnExpression* expr = assignment_infos_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get assignment info", K(ret), K(i), K(expr));
    } else if (OB_INVALID_INDEX == (result_index = expr->get_result_index())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid result index", K(result_index));
    } else if (OB_FAIL(expr->calc(expr_ctx, *input_row, merge_ctx->get_update_row().cells_[result_index]))) {
      LOG_WARN("calculate value result failed", K(ret), K(i), K(*expr));
    }
  }
  if (OB_SUCC(ret)) {
    // prepare old_row and new_row.
    OZ(copy_row(merge_ctx->get_update_row(), merge_ctx->old_row_, old_projector_, old_projector_size_));
    OZ(copy_row(merge_ctx->get_update_row(), merge_ctx->new_row_, new_projector_, new_projector_size_));
  }
  return ret;
}

int ObTableMerge::check_updated_row(const ObNewRow& row1, const ObNewRow& row2, bool& is_changed) const
{
  int ret = OB_SUCCESS;
  OV(row1.get_count() == row2.get_count());
  OV(OB_NOT_NULL(row1.cells_) && OB_NOT_NULL(row2.cells_));
  OX(is_changed = false);
  for (int64_t i = 0; OB_SUCC(ret) && !is_changed && i < row1.get_count(); i++) {
    if (!row1.get_cell(i).strict_equal(row2.get_cell(i))) {
      OX(is_changed = true);
    }
  }
  return ret;
}

int ObTableMerge::add_assignment_expr(ObColumnExpression* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr));
  } else if (OB_FAIL(assignment_infos_.push_back(expr))) {
    LOG_WARN("fail to add expr", K(ret));
  }
  return ret;
}

int ObTableMerge::set_old_projector(int32_t* projector, int64_t projector_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(projector) || OB_UNLIKELY(projector_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(projector), K(projector_size));
  } else {
    old_projector_ = projector;
    old_projector_size_ = projector_size;
  }
  return ret;
}

int ObTableMerge::set_updated_column_info(
    int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp)
{
  int ret = OB_SUCCESS;
  ColumnContent column;
  column.projector_index_ = project_index;
  column.auto_filled_timestamp_ = auto_filled_timestamp;
  CK(array_index >= 0 && array_index < updated_column_ids_.count());
  CK(array_index >= 0 && array_index < updated_column_infos_.count());
  if (OB_SUCC(ret)) {
    updated_column_ids_.at(array_index) = column_id;
    updated_column_infos_.at(array_index) = column;
  }
  return ret;
}

int ObTableMerge::update_row(ObPartitionService* partition_service, const ObPartitionKey& pkey, bool need_delete,
    ObExecContext& ctx, ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableMergeCtx* merge_ctx = NULL;
  ObNewRowIterator* duplicated_rows = NULL;
  int64_t cur_affected = 0;
  OV(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id())));
  OV(OB_NOT_NULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)));
  OV(OB_NOT_NULL(my_session = GET_MY_SESSION(ctx)));
  OV(OB_NOT_NULL(partition_service));
  OZ(check_row_null(ctx, merge_ctx->new_row_, column_infos_), merge_ctx->new_row_);
  bool is_filtered = false;
  OZ(ObPhyOperator::filter_row_for_check_cst(
      merge_ctx->expr_ctx_, merge_ctx->new_row_, check_constraint_exprs_, is_filtered));
  OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  OZ(ForeignKeyHandle::do_handle(*merge_ctx, fk_args_, merge_ctx->old_row_, merge_ctx->new_row_),
      merge_ctx->old_row_,
      merge_ctx->new_row_);
  // do update.
  bool is_row_changed = false;
  OZ(check_updated_row(merge_ctx->old_row_, merge_ctx->new_row_, is_row_changed));

  if (need_delete) {
    OZ(partition_service->delete_row(my_session->get_trans_desc(), dml_param, pkey, column_ids_, merge_ctx->old_row_));
    OZ(ForeignKeyHandle::do_handle_old_row(*merge_ctx, fk_args_, merge_ctx->new_row_), merge_ctx->new_row_);
  } else {
    if (!is_row_changed) {
      OZ(lock_row(partition_service, pkey, ctx, dml_param));
    } else {
      OZ(partition_service->delete_row(
          my_session->get_trans_desc(), dml_param, pkey, column_ids_, merge_ctx->old_row_));
      OZ(partition_service->insert_row(my_session->get_trans_desc(),
          dml_param,
          pkey,
          column_ids_,
          primary_key_ids_,
          merge_ctx->new_row_,
          INSERT_RETURN_ONE_DUP,
          cur_affected,
          duplicated_rows));
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_SUCCESS;
        OZ(merge_ctx->insert_row_store_.add_row(merge_ctx->new_row_, false));
      }
      if (nullptr != duplicated_rows) {
        (void)partition_service->revert_insert_iter(pkey, duplicated_rows);
        duplicated_rows = nullptr;
      }
      OX(merge_ctx->affected_rows_ += 1);
    }
  }
  return ret;
}

int ObTableMerge::lock_row(ObPartitionService* partition_service, const ObPartitionKey& pkey, ObExecContext& ctx,
    storage::ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObTableMergeCtx* merge_ctx = NULL;
  if (OB_ISNULL((executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL((partition_service))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else {
    CK(OB_NOT_NULL(merge_ctx->target_rowkey_.cells_));
    CK(merge_ctx->target_rowkey_.get_count() <= merge_ctx->old_row_.get_count());
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_ctx->target_rowkey_.get_count(); ++i) {
      merge_ctx->target_rowkey_.cells_[i] = merge_ctx->old_row_.get_cell(i);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableModify::lock_row(ctx, merge_ctx->target_rowkey_, dml_param, pkey))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("fail to lock row", K(ret), K(merge_ctx->old_row_));
        }
      } else {
        merge_ctx->affected_rows_ += 1;
      }
    }
  }
  return ret;
}

int ObTableMerge::copy_row(const ObNewRow& old_row, ObNewRow& new_row, int32_t* projector, int64_t projector_size) const
{
  int ret = OB_SUCCESS;
  ObNewRow tmp = old_row;
  if (OB_LIKELY(projector != NULL && projector_size > 0)) {
    tmp.projector_ = projector;
    tmp.projector_size_ = projector_size;
  }
  if (OB_UNLIKELY(tmp.get_count() > new_row.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old row is larger than the new row", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp.get_count(); ++i) {
    new_row.cells_[i] = tmp.get_cell(i);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableMerge)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, has_insert_clause_, has_update_clause_);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialize_dlist(match_conds_, buf, buf_len, pos))) {
    LOG_WARN("fail to serialize dlist", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialize_dlist(delete_conds_, buf, buf_len, pos))) {
    LOG_WARN("fail to serialize dlist", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialize_dlist(update_conds_, buf, buf_len, pos))) {
    LOG_WARN("fail to serialize dlist", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialize_dlist(insert_conds_, buf, buf_len, pos))) {
    LOG_WARN("fail to serialize dlist", K(buf_len), K(pos), K(ret));
  }

  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(rowkey_desc_);
    OB_UNIS_ENCODE(scan_column_ids_);
    OB_UNIS_ENCODE(update_related_column_ids_);
    OB_UNIS_ENCODE(updated_column_ids_);
    OB_UNIS_ENCODE(delete_column_ids_);
    OB_UNIS_ENCODE(updated_column_infos_);
    OB_UNIS_ENCODE(primary_key_ids_);
  }

  if (OB_SUCC(ret)) {  // serialize assigment_infos_
    int64_t assign_count = assignment_infos_.count();
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, assign_count))) {
      LOG_WARN("fail to encode assignment count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < assign_count; i++) {
        if (OB_ISNULL(assignment_infos_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid assignment", K(i), K(&assignment_infos_));
        } else if (OB_FAIL(assignment_infos_.at(i)->serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to encode assignment info", K(i), K(ret));
          break;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (has_update_clause_) {  // has update clause
    // The compatibility issue is not considered here,
    // because the merge function has not been used before
    if (OB_ISNULL(old_projector_) || OB_ISNULL(new_projector_) || OB_ISNULL(update_origin_projector_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("projector is NULL", K(old_projector_), K(new_projector_), K(ret));
    } else {
      OB_UNIS_ENCODE_ARRAY(old_projector_, old_projector_size_);
      OB_UNIS_ENCODE_ARRAY(new_projector_, new_projector_size_);
      OB_UNIS_ENCODE_ARRAY(update_origin_projector_, update_origin_projector_size_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (false == delete_conds_.is_empty()) {  // has delete caluse
    if (OB_ISNULL(delete_projector_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("projector is NULL", K(ret));
    } else {
      OB_UNIS_ENCODE_ARRAY(delete_projector_, delete_projector_size_);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableModify::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableMerge)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, has_insert_clause_, has_update_clause_);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is NULL", K(ret));
  } else {
    OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, match_conds_, my_phy_plan_);
    OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, delete_conds_, my_phy_plan_);
    OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, update_conds_, my_phy_plan_);
    OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, insert_conds_, my_phy_plan_);
  }

  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(rowkey_desc_);
    OB_UNIS_DECODE(scan_column_ids_);
    OB_UNIS_DECODE(update_related_column_ids_);
    OB_UNIS_DECODE(updated_column_ids_);
    OB_UNIS_DECODE(delete_column_ids_);
    OB_UNIS_DECODE(updated_column_infos_);
    OB_UNIS_DECODE(primary_key_ids_);
  }

  if (OB_SUCC(ret)) {  // deserialize assignment_infos_
    int64_t assign_count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &assign_count))) {
      LOG_WARN("fail to decode assign count", K(ret));
    } else if (OB_FAIL(init_array_size<>(assignment_infos_, assign_count))) {
      LOG_WARN("fail to init assignment array", K(ret), K(assign_count));
    } else {
      ObColumnExpression* expr = NULL;
      for (int64_t i = 0; i < assign_count && OB_SUCC(ret); i++) {
        if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
          LOG_WARN("make sql expr failed", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("make sql expr fail", K(ret), K(expr));
        } else if (OB_FAIL(expr->deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize expression", K(ret));
        } else if (OB_FAIL(add_assignment_expr(expr))) {
          LOG_WARN("fail to add expr", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (has_update_clause_ &&
             OB_FAIL(deserialize_projector(buf, data_len, pos, old_projector_, old_projector_size_))) {
    LOG_WARN("fail to deserialize projector", K(data_len), K(pos), K(ret));
  } else if (has_update_clause_ &&
             OB_FAIL(deserialize_projector(buf, data_len, pos, new_projector_, new_projector_size_))) {
    LOG_WARN("fail to deserialize projector", K(data_len), K(pos), K(ret));
  } else if (has_update_clause_ && OB_FAIL(deserialize_projector(
                                       buf, data_len, pos, update_origin_projector_, update_origin_projector_size_))) {
    LOG_WARN("fail to deserialize projector", K(data_len), K(pos), K(ret));
  } else if (false == delete_conds_.is_empty() &&
             OB_FAIL(deserialize_projector(buf, data_len, pos, delete_projector_, delete_projector_size_))) {
    LOG_WARN("fail to deserialize projector", K(data_len), K(pos), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableModify::deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize physical operator failed", K(ret));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableMerge)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, has_insert_clause_, has_update_clause_);

  len += get_dlist_serialize_size(match_conds_);
  len += get_dlist_serialize_size(delete_conds_);
  len += get_dlist_serialize_size(update_conds_);
  len += get_dlist_serialize_size(insert_conds_);

  OB_UNIS_ADD_LEN(rowkey_desc_);
  OB_UNIS_ADD_LEN(scan_column_ids_);
  OB_UNIS_ADD_LEN(update_related_column_ids_);
  OB_UNIS_ADD_LEN(updated_column_ids_);
  OB_UNIS_ADD_LEN(delete_column_ids_);
  OB_UNIS_ADD_LEN(updated_column_infos_);
  OB_UNIS_ADD_LEN(primary_key_ids_);

  OB_UNIS_ADD_LEN(assignment_infos_.count());
  for (int64_t i = 0; i < assignment_infos_.count(); i++) {
    if (OB_ISNULL(assignment_infos_.at(i))) {
      LOG_WARN("invalid assignment info", K(i), K(&assignment_infos_));
    } else {
      OB_UNIS_ADD_LEN(*(assignment_infos_.at(i)));
    }
  }

  if (has_update_clause_) {  // has update clause
    if (OB_ISNULL(old_projector_) || OB_ISNULL(new_projector_) || OB_ISNULL(update_origin_projector_)) {
      LOG_ERROR("projector is NULL", K(old_projector_), K(new_projector_), K(update_origin_projector_));
    } else {
      OB_UNIS_ADD_LEN_ARRAY(old_projector_, old_projector_size_);
      OB_UNIS_ADD_LEN_ARRAY(new_projector_, new_projector_size_);
      OB_UNIS_ADD_LEN_ARRAY(update_origin_projector_, update_origin_projector_size_);
    }
  }

  if (false == delete_conds_.is_empty()) {  // has delete caluse
    if (OB_ISNULL(delete_projector_)) {
      LOG_ERROR("projector is NULL");
    } else {
      OB_UNIS_ADD_LEN_ARRAY(delete_projector_, delete_projector_size_);
    }
  }

  len += ObTableModify::get_serialize_size();
  return len;
}

int ObTableMerge::deserialize_projector(
    const char* buf, const int64_t data_len, int64_t& pos, int32_t*& projector, int64_t& projector_size)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(projector_size);
  if (0 < projector_size) {
    if (OB_ISNULL(my_phy_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "invalid phy plan", K(my_phy_plan_), K(ret));
    } else {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL((projector = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * projector_size))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K(ret));
      } else {
        OB_UNIS_DECODE_ARRAY(projector, projector_size);
      }
    }
  } else {
    LOG_WARN("projector size is invalid", K(projector_size), K(ret));
    projector = NULL;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
