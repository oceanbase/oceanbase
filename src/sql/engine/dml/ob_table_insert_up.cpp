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
#include "ob_table_insert_up.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_factory.h"
#include "sql/engine/ob_physical_plan.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace sql;
using namespace storage;

namespace sql {
ObTableInsertUp::ObTableInsertUpCtx::ObTableInsertUpCtx(ObExecContext& ctx)
    : ObTableModifyCtx(ctx),
      rowkey_row_(),
      found_rows_(0),
      get_count_(0),
      update_row_(),
      update_row_projector_size_(0),
      update_row_projector_(NULL),
      insert_row_(),
      part_infos_(),
      dml_param_(),
      cur_gi_task_iter_end_(false)
{}

ObTableInsertUp::ObTableInsertUpCtx::~ObTableInsertUpCtx()
{}

ObNewRow& ObTableInsertUp::ObTableInsertUpCtx::get_update_row()
{
  return update_row_;
}

int ObTableInsertUp::ObTableInsertUpCtx::init(ObExecContext& ctx, int64_t update_row_projector_size,
    int64_t primary_key_count, int64_t update_row_columns_count, int64_t insert_row_column_count)
{
  int ret = OB_SUCCESS;
  update_row_projector_size_ = update_row_projector_size;
  if (OB_UNLIKELY(0 >= update_row_projector_size) || OB_UNLIKELY(0 >= primary_key_count) ||
      OB_UNLIKELY(0 >= update_row_columns_count) || OB_UNLIKELY(0 >= insert_row_column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(update_row_projector_size),
        K(primary_key_count),
        K(update_row_columns_count),
        K(insert_row_column_count));
  } else if (OB_FAIL(alloc_row_cells(update_row_columns_count, update_row_))) {
    LOG_WARN("fail to create cur row", K(ret), K(update_row_columns_count));
  } else if (OB_FAIL(alloc_row_cells(insert_row_column_count, insert_row_))) {
    SQL_CG_LOG(WARN, "fail to create insert row", K(ret), K(insert_row_column_count));
  }
  if (OB_SUCC(ret)) {
    update_row_projector_ =
        static_cast<int32_t*>(ctx.get_allocator().alloc(sizeof(int32_t) * update_row_projector_size_));
    if (OB_ISNULL(update_row_projector_)) {
      SQL_CG_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(alloc_row_cells(primary_key_count, rowkey_row_))) {
      LOG_WARN("fail to create row", K(ret), K(primary_key_count));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < update_row_projector_size_; i++) {
      update_row_projector_[i] = static_cast<int32_t>(i);
    }
    update_row_.count_ = update_row_columns_count;
    update_row_.projector_ = update_row_projector_;
    update_row_.projector_size_ = update_row_projector_size_;
  }
  return ret;
}

void ObTableInsertUp::ObTableInsertUpCtx::reset_update_row()
{
  update_row_.projector_ = update_row_projector_;
  update_row_.projector_size_ = update_row_projector_size_;
}

OB_DEF_SERIALIZE(ObTableInsertUp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      scan_column_ids_,
      update_related_column_ids_,
      updated_column_ids_,
      updated_column_infos_,
      primary_key_ids_);

  if (OB_FAIL(ret)) {
    SQL_ENG_LOG(WARN, "Fail to deserialize data, ", K(ret));
  }
  if (OB_SUCC(ret)) {
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
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE_ARRAY(old_projector_, old_projector_size_);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE_ARRAY(new_projector_, new_projector_size_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableModify::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableInsertUp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      scan_column_ids_,
      update_related_column_ids_,
      updated_column_ids_,
      updated_column_infos_,
      primary_key_ids_);
  if (OB_FAIL(ret)) {
    SQL_ENG_LOG(WARN, "Fail to deserialize data", K(ret));
  }
  if (OB_SUCC(ret)) {
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
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(old_projector_size_);
    if (0 < old_projector_size_) {
      if (OB_ISNULL(my_phy_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(ERROR, "invalid phy plan", K(my_phy_plan_));
      } else {
        ObIAllocator& alloc = my_phy_plan_->get_allocator();
        if (OB_ISNULL((old_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * old_projector_size_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("no memory");
        } else {
          OB_UNIS_DECODE_ARRAY(old_projector_, old_projector_size_);
        }
      }
    } else {
      old_projector_ = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(new_projector_size_);
    if (0 < new_projector_size_) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL((new_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * new_projector_size_))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory");
      } else {
        OB_UNIS_DECODE_ARRAY(new_projector_, new_projector_size_);
      }
    } else {
      new_projector_ = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableModify::deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize physical operator failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableInsertUp)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      scan_column_ids_,
      update_related_column_ids_,
      updated_column_ids_,
      updated_column_infos_,
      primary_key_ids_);
  OB_UNIS_ADD_LEN(assignment_infos_.count());
  for (int64_t i = 0; i < assignment_infos_.count(); i++) {
    if (OB_ISNULL(assignment_infos_.at(i))) {
      LOG_WARN("invalid assignment info", K(i), K(&assignment_infos_));
    } else {
      OB_UNIS_ADD_LEN(*(assignment_infos_.at(i)));
    }
  }
  OB_UNIS_ADD_LEN_ARRAY(old_projector_, old_projector_size_);
  OB_UNIS_ADD_LEN_ARRAY(new_projector_, new_projector_size_);
  len += ObTableModify::get_serialize_size();
  return len;
}

ObTableInsertUp::ObTableInsertUp(ObIAllocator& alloc)
    : ObTableModify(alloc),
      old_projector_(NULL),
      old_projector_size_(0),
      new_projector_(NULL),
      new_projector_size_(0),
      scan_column_ids_(alloc),
      update_related_column_ids_(alloc),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      assignment_infos_(alloc)
{}

ObTableInsertUp::~ObTableInsertUp()
{}

void ObTableInsertUp::reset()
{
  old_projector_ = NULL;
  old_projector_size_ = 0;
  new_projector_ = NULL;
  new_projector_size_ = 0;
  scan_column_ids_.reset();
  update_related_column_ids_.reset();
  updated_column_ids_.reset();
  updated_column_infos_.reset();
  assignment_infos_.reset();
  ObTableModify::reset();
}

void ObTableInsertUp::reuse()
{
  old_projector_ = NULL;
  old_projector_size_ = 0;
  new_projector_ = NULL;
  new_projector_size_ = 0;
  scan_column_ids_.reuse();
  update_related_column_ids_.reuse();
  updated_column_ids_.reuse();
  updated_column_infos_.reuse();
  assignment_infos_.reuse();
  ObTableModify::reuse();
}

int64_t ObTableInsertUp::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID,
      table_id_,
      N_CID,
      column_ids_,
      N_OLD_PROJECTOR,
      ObArrayWrap<int32_t>(old_projector_, projector_size_),
      "new_projector",
      ObArrayWrap<int32_t>(new_projector_, new_projector_size_),
      "projector",
      ObArrayWrap<int32_t>(projector_, projector_size_),
      N_REFERED_CID,
      scan_column_ids_,
      N_CID_UPDATE,
      update_related_column_ids_,
      N_PRIMARY_CID,
      primary_key_ids_,
      N_UPDATED_CID,
      updated_column_ids_);
  return pos;
}

int ObTableInsertUp::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableInsertUpCtx* update_ctx = NULL;
  if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_ISNULL((update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id())))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (update_ctx->cur_gi_task_iter_end_) {
    ret = OB_ITER_END;
  } else {
    if (update_ctx->get_count_ == 0) {
      update_ctx->get_update_row().projector_ = old_projector_;
      update_ctx->get_update_row().projector_size_ = old_projector_size_;
      row = const_cast<const ObNewRow*>(&update_ctx->get_update_row());
      OB_LOG(DEBUG, "get next row, old row", K(*row));
    } else if (update_ctx->get_count_ == 1) {
      update_ctx->get_update_row().projector_ = new_projector_;
      update_ctx->get_update_row().projector_size_ = new_projector_size_;
      row = const_cast<const ObNewRow*>(&update_ctx->get_update_row());
      OB_LOG(DEBUG, "get next row, new row", K(*row));
      ObNewRow old_row;
      old_row.cells_ = update_ctx->get_update_row().cells_;
      old_row.count_ = update_ctx->get_update_row().count_;
      old_row.projector_ = old_projector_;
      old_row.projector_size_ = old_projector_size_;
      if (OB_FAIL(check_row_null(ctx, *row, column_infos_))) {
        LOG_WARN("failed to check row null", K(ret), K(*row));
      } else if (OB_FAIL(ForeignKeyHandle::do_handle(*update_ctx, fk_args_, old_row, *row))) {
        LOG_WARN("failed to handle foreign key", K(ret), K(old_row), K(*row));
      }
    } else {
      ret = OB_ITER_END;
    }
    update_ctx->get_count_++;
  }
  return ret;
}

int ObTableInsertUp::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  return OB_NOT_SUPPORTED;
}

int ObTableInsertUp::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableInsertUpCtx* update_ctx = NULL;
  NG_TRACE(insertup_open);
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_FAIL(
                 ObTableModify::init_dml_param(ctx, index_tid_, *this, false, &table_param_, update_ctx->dml_param_))) {
    LOG_WARN("failed to init dml param", K(ret));
  } else {
    update_ctx->dml_param_.is_ignore_ = is_ignore_;
  }
  if (OB_SUCC(ret) && gi_above_) {
    if (OB_FAIL(get_gi_task(ctx))) {
      LOG_WARN("get granule iterator task failed", K(ret));
    }
  }
  // do table replace
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_table_insert_up(ctx))) {
      LOG_WARN("do table delete failed", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUp::do_table_insert_up(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertUpCtx* insert_update_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionKey* pkey = NULL;

  const ObNewRow* insert_row = NULL;
  ObNewRowIterator* duplicated_rows = NULL;
  ObExprCtx expr_ctx;

  int64_t affected_rows = 0;
  int64_t cur_affected = 0;
  int64_t n_duplicated_rows = 0;
  int64_t update_count = 0;
  if (OB_ISNULL(insert_update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get insert update operator context failed", K(ret), K(get_id()));
  } else if (insert_update_ctx->iter_end_) {
    insert_update_ctx->cur_gi_task_iter_end_ = true;
    LOG_DEBUG("can't get gi task, iter end", K(insert_update_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, insert_update_ctx->part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (insert_update_ctx->part_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of part info is not right", K(ret), K(insert_update_ctx->part_infos_.count()));
  } else if (OB_ISNULL(pkey = &insert_update_ctx->part_infos_.at(0).partition_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition key is null", K(ret), K(pkey));
  } else if (OB_FAIL(set_autoinc_param_pkey(ctx, *pkey))) {
    LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expression context failed", K(ret));
  } else {
    NG_TRACE(insertup_start_do);
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = child_op_->get_next_row(ctx, insert_row))) {
      NG_TRACE_TIMES(2, insertup_start_calc_insert_row);
      if (OB_FAIL(calc_insert_row(ctx, expr_ctx, insert_row))) {
        LOG_WARN("fail to calc insert row", K(ret));
      } else if (OB_ISNULL(insert_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert row is NULL", K(ret));
      } else if (OB_FAIL(check_row_null(ctx, *insert_row, column_infos_))) {
        LOG_WARN("fail to check_row_null", K(ret), K(*insert_row));
      } else if (OB_FAIL(ForeignKeyHandle::do_handle_new_row(*insert_update_ctx, fk_args_, *insert_row))) {
        LOG_WARN("fail to handle foreign key", K(ret), K(*insert_row));
      } else {
        insert_update_ctx->found_rows_++;
        // 1. Try to insert and require the storage layer to return the first row that encounters conflicts
        //(including primary key conflicts and unique key conflicts)
        duplicated_rows = NULL;
        cur_affected = 0;
        NG_TRACE_TIMES(2, insertup_start_insert_row);
        ret = partition_service->insert_row(my_session->get_trans_desc(),
            insert_update_ctx->dml_param_,
            *pkey,
            column_ids_,
            primary_key_ids_,
            insert_update_ctx->insert_row_,
            INSERT_RETURN_ONE_DUP,
            cur_affected,
            duplicated_rows);
        affected_rows += cur_affected;
        NG_TRACE_TIMES(2, insertup_end_insert_row);
        SQL_ENG_LOG(DEBUG, "insert row on duplicate key", K(ret), K(affected_rows), KPC(insert_row));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(init_autoinc_param(ctx, *pkey))) {
            LOG_WARN("fail to init autoinc param", K(ret));
          }
        }
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          // 2. There are conflicting rows,
          // get the primary key column and related index columns of the conflicting row
          ret = OB_SUCCESS;
          if (OB_FAIL(process_on_duplicate_update(ctx,
                  duplicated_rows,
                  insert_update_ctx->dml_param_,
                  n_duplicated_rows,
                  affected_rows,
                  update_count))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
              LOG_WARN("fail to proecess on duplicate update", K(ret));
            }
          }
        }  // end if primary_key_duplicate
        NG_TRACE_TIMES(2, revert_insert_iter);
        if (OB_UNLIKELY(duplicated_rows != NULL)) {
          ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
          duplicated_rows = NULL;
        }
        NG_TRACE_TIMES(2, revert_insert_iter_end);
      }
    }  // end while

    if (OB_ITER_END != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to deal with insert on duplicate key", K(ret));
      }
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      insert_update_ctx->cur_gi_task_iter_end_ = true;
      ret = OB_SUCCESS;
      SQL_ENG_LOG(DEBUG, "insert on dupliate key finish", K(affected_rows));
      plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                      ? affected_rows + n_duplicated_rows
                                      : affected_rows);
      plan_ctx->add_row_matched_count(insert_update_ctx->found_rows_);
      plan_ctx->add_row_duplicated_count(update_count);
      if (OB_SUCCESS != (ret = plan_ctx->sync_last_value_local())) {
        // sync last user specified value after iter ends(compatible with MySQL)
        LOG_WARN("failed to sync last value", K(ret));
      }
    }
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    NG_TRACE(sync_auto_value);
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
  }

  return ret;
}

int ObTableInsertUp::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertUpCtx* insert_up_ctx = NULL;
  if (OB_FAIL(ObTableModify::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(insert_up_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get replace operator context failed", K(ret), K(get_id()));
  } else {
    insert_up_ctx->reset();
    if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_insert_up(ctx))) {
        LOG_WARN("do table insert update failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableInsertUp::init_autoinc_param(ObExecContext& ctx, const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else {
    plan_ctx->record_last_insert_id_cur_stmt();
    // insert success, auto-increment value consumed; clear saved auto-increment value
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).pkey_ = pkey;
      if (NULL != autoinc_params.at(i).cache_handle_) {
        autoinc_params.at(i).cache_handle_->last_row_dup_flag_ = false;
        autoinc_params.at(i).cache_handle_->last_value_to_confirm_ = 0;
      }
    }
  }
  return ret;
}

int ObTableInsertUp::calc_insert_row(ObExecContext& ctx, ObExprCtx& expr_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  ObTableInsertUpCtx* update_ctx = NULL;
  if (OB_ISNULL((update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id())))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid calc buf", K(expr_ctx.calc_buf_));
  } else {
    expr_ctx.calc_buf_->reset();
    update_ctx->reset_update_row();
    if (OB_FAIL(copy_cur_row(*update_ctx, insert_row))) {
      LOG_WARN("fail to copy row", K(ret), K(*insert_row));
    } else if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy cur row fail", K(ret), K(insert_row));
    } else if (OB_FAIL(calculate_row(expr_ctx, *const_cast<ObNewRow*>(insert_row))) ||
               OB_FAIL(validate_row(expr_ctx, expr_ctx.column_conv_ctx_, *const_cast<ObNewRow*>(insert_row)))) {
      LOG_WARN("calculate or validate row failed", K(ret), K(*insert_row));
      int64_t err_col_idx = (expr_ctx.err_col_idx_) % column_ids_.count();
      log_user_error_inner(ret, err_col_idx, update_ctx->found_rows_ + 1, ctx);
    } else {
      for (int64_t i = 0; i < insert_row->projector_size_; i++) {
        update_ctx->insert_row_.cells_[i] = insert_row->get_cell(i);
      }
    }
  }
  return ret;
}

int ObTableInsertUp::process_on_duplicate_update(ObExecContext& ctx, ObNewRowIterator* duplicated_rows,
    storage::ObDMLBaseParam& dml_param, int64_t& n_duplicated_rows, int64_t& affected_rows, int64_t& update_count) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  int64_t cur_affected = 0;
  common::ObNewRowIterator* result = NULL;
  storage::ObTableScanParam scan_param;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObPhyTableLocation* table_location = NULL;
  ObNewRow* dup_row = NULL;
  ObTableInsertUpCtx* update_ctx = NULL;
  ObNewRow* row = NULL;
  bool is_row_changed = false;
  const ObPartitionReplicaLocation* part_replica = NULL;
  NG_TRACE_TIMES(2, insertup_before_scan);
  if (OB_ISNULL(duplicated_rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(duplicated_rows));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL((my_session = GET_MY_SESSION(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL((executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL((update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id())))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL((partition_service = executor_ctx->get_partition_service()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(
                 ObTaskExecutorCtxUtil::get_phy_table_location(*executor_ctx, table_id_, index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(table_id_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, *table_location, part_replica))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (OB_FAIL(duplicated_rows->get_next_row(dup_row))) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_FAIL(build_scan_param(ctx, *part_replica, dup_row, scan_param))) {
    LOG_WARN("fail to build scan param", K(ret), K(dup_row));
  } else if (OB_FAIL(partition_service->table_scan(scan_param, result))) {
    LOG_WARN("fail to table scan", K(ret));
  } else if (OB_FAIL(result->get_next_row(row))) {
    LOG_WARN("fail to get next row", K(ret), K(update_related_column_ids_));
  } else {
    const ObNewRow& source_row =
        GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200 ? update_ctx->get_cur_row() : update_ctx->insert_row_;
    if (OB_FAIL(calc_update_rows(ctx, source_row, *row, is_row_changed))) {
      LOG_WARN("fail to calc rows for update", K(ret), K(result));
    }
  }
  // update
  if (OB_SUCC(ret)) {
    if (is_row_changed) {
      update_count++;
      ObDMLRowIterator dml_row_iter(ctx, *this);
      NG_TRACE_TIMES(2, insertup_start_update_row);
      if (OB_FAIL(dml_row_iter.init())) {
        LOG_WARN("init dml row iterator failed", K(ret));
      } else if (OB_FAIL(partition_service->update_rows(my_session->get_trans_desc(),
                     dml_param,
                     scan_param.pkey_,
                     update_related_column_ids_,
                     updated_column_ids_,
                     &dml_row_iter,
                     cur_affected))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN(
              "update rows to partition storage failed", K(ret), K(update_related_column_ids_), K(updated_column_ids_));
        }
      } else {
        NG_TRACE_TIMES(2, insertup_end_update_row);
        // The affected-rows value per row is 1 if the row is inserted as a new row,
        // 2 if an existing row is updated, and 0 if an existing row is set to its current values
        affected_rows = affected_rows + cur_affected + 1;
        SQL_ENG_LOG(DEBUG, "update row", K(ret), K(affected_rows));
      }
    } else {
      n_duplicated_rows++;
      // lock row
      update_ctx->get_update_row().projector_ = old_projector_;
      update_ctx->get_update_row().projector_size_ = projector_size_;
      for (int64_t i = 0; i < primary_key_ids_.count(); i++) {
        update_ctx->rowkey_row_.cells_[i] = update_ctx->get_update_row().get_cell(i);
      }
      if (OB_FAIL(lock_row(ctx, update_ctx->rowkey_row_, dml_param, scan_param.pkey_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to lock row", K(ret), K(update_ctx->get_update_row()));
        }
      } else {
        NG_TRACE_TIMES(2, lock_row);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ITER_END != (ret = duplicated_rows->get_next_row(dup_row))) {
      LOG_WARN("get next row fail. row count should be only one", K(ret));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get next row fail ,expected return code is OB_ITER_END");
      }
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ITER_END != (ret = result->get_next_row(row))) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("scan result more than one row.", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (result != NULL) {
    NG_TRACE_TIMES(2, revert_scan_iter);
    int revert_return = OB_SUCCESS;
    if (NULL != result && OB_SUCCESS != (revert_return = partition_service->revert_scan_iter(result))) {
      LOG_WARN("fail to revert scan iter", K(revert_return));
      if (OB_SUCC(ret)) {
        ret = revert_return;
      }
    } else {
      result = NULL;
    }
    NG_TRACE_TIMES(2, revert_scan_iter_end);
  }
  return ret;
}

int ObTableInsertUp::build_scan_param(ObExecContext& ctx, const ObPartitionReplicaLocation& part_replica,
    const ObNewRow* dup_row, storage::ObTableScanParam& scan_param) const
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(dup_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(dup_row));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL((executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL((my_session = GET_MY_SESSION(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(
                 ObTaskExecutorCtxUtil::get_phy_table_location(*executor_ctx, table_id_, index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(table_id_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else {
    ObRowkey key(dup_row->cells_, dup_row->count_);
    common::ObNewRange range;
    scan_param.key_ranges_.reset();
    if (OB_FAIL(range.build_range(index_tid_, key))) {
      LOG_WARN("fail to build key range", K(ret), K_(index_tid), K(key));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else {
      scan_param.timeout_ = plan_ctx->get_ps_timeout_timestamp();
      ObQueryFlag query_flag(ObQueryFlag::Forward,  // scan_order
          false,                                    // daily_merge
          false,                                    // optimize
          false,                                    // sys scan
          false,                                    // full_row
          false,                                    // index_back
          false,                                    // query_stat
          ObQueryFlag::MysqlMode,                   // sql_mode
          true                                      // read_latest
      );
      scan_param.scan_flag_.flag_ = query_flag.flag_;
      scan_param.reserved_cell_count_ = scan_column_ids_.count();
      scan_param.for_update_ = false;
      scan_param.column_ids_.reset();
      if (OB_FAIL(scan_param.column_ids_.assign(scan_column_ids_))) {
        LOG_WARN("fail to assign column id", K(ret));
      } else if (OB_FAIL(part_replica.get_partition_key(scan_param.pkey_))) {
        LOG_WARN("get partition key fail", K(ret), K(part_replica));
      } else if (OB_ISNULL(my_phy_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid physical plan", K(ret), K(my_phy_plan_));
      } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, scan_param.schema_version_))) {
        LOG_WARN("fail to get table schema version", K(ret), K(index_tid_));
      } else if (OB_FAIL(wrap_expr_ctx(ctx, scan_param.expr_ctx_))) {
        LOG_WARN("wrap expr ctx failed", K(ret));
      } else {
        SQL_ENG_LOG(DEBUG, "set scan param", K(scan_column_ids_));
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.trans_desc_ = &my_session->get_trans_desc();
        scan_param.index_id_ = index_tid_;
        scan_param.sql_mode_ = my_session->get_sql_mode();
      }
      if (OB_SUCC(ret)) {
        scan_param.virtual_column_exprs_.reset();
        DLIST_FOREACH(node, virtual_column_exprs_)
        {
          const ObColumnExpression* expr = static_cast<const ObColumnExpression*>(node);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node or node expr is NULL", K(ret));
          } else if (OB_FAIL(scan_param.virtual_column_exprs_.push_back(expr))) {
            LOG_WARN("fail to push back virtual column expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableInsertUp::calc_update_rows(
    ObExecContext& ctx, const ObNewRow& insert_row, const ObNewRow& duplicate_row, bool& is_row_changed) const
{
  int ret = OB_SUCCESS;
  ObTableInsertUpCtx* update_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  is_row_changed = false;
  NG_TRACE_TIMES(2, insertup_start_calc_update_row);
  if (OB_ISNULL((update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id())))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else {
    for (int64_t i = 0; i < duplicate_row.get_count(); i++) {
      update_ctx->get_update_row().cells_[i] = duplicate_row.get_cell(i);
    }
    SQL_ENG_LOG(DEBUG, "get conflict row", K(ret), K(update_ctx->get_update_row()));
  }
  if (OB_SUCC(ret)) {
    // before calc new row, to be compatible with MySQL
    // 1. disable operation to sync user specified value for auto-increment column because duplicate
    //    hidden pk will be placed in first place of row
    //    ATTENTION: suppose two auto-increment column at most here
    // 2. set duplicate flag to reuse auto-generated value
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).sync_flag_ = false;
      if (NULL != autoinc_params.at(i).cache_handle_) {
        autoinc_params.at(i).cache_handle_->last_row_dup_flag_ = true;
      }
    }
    if (OB_FAIL(calc_new_row(ctx, duplicate_row, insert_row, is_row_changed))) {
      LOG_WARN("fail to calc new row", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUp::calc_new_row(
    ObExecContext& ctx, const ObNewRow& scan_result_row, const ObNewRow& insert_row, bool& is_row_changed) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  is_row_changed = false;
  bool is_auto_col_changed = false;
  ObTableInsertUpCtx* update_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL((update_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id())))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan ctx", K(plan_ctx));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expression context failed", K(ret));
  } else {
    // if auto-increment value used when duplicate, reset last_insert_id_cur_stmt
    // TODO check if auto-increment value used actually
    if (plan_ctx->get_autoinc_id_tmp() == plan_ctx->get_last_insert_id_cur_stmt()) {
      plan_ctx->set_last_insert_id_cur_stmt(0);
    }

    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    ObSQLUtils::set_insert_update_scope(expr_ctx.cast_mode_);
    NG_TRACE_TIMES(2, insertup_calc_new_row);
    for (int64_t i = 0; i < assignment_infos_.count() && OB_SUCC(ret); i++) {
      int64_t old_value_index = old_projector_[updated_column_infos_.at(i).projector_index_];
      ObObj* val = NULL;
      int64_t result_index = -1;
      ObColumnExpression* expr = assignment_infos_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get assignment info", K(i), K(expr));
      } else if (-1 == (result_index = expr->get_result_index())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid result index", K(result_index));
      } else if (old_value_index < 0 || old_value_index >= update_ctx->get_update_row().count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid old value index", K(old_value_index), "cell count", update_ctx->get_update_row().count_);
      } else if (OB_FAIL(expr->calc(expr_ctx,
                     insert_row,
                     update_ctx->get_update_row(),
                     update_ctx->get_update_row().cells_[result_index]))) {
        LOG_WARN("calculate value result failed", K(ret), K(i), K(*expr));
        break;
      } else {
        val = &update_ctx->get_update_row().cells_[result_index];
        SQL_ENG_LOG(DEBUG,
            "calc new row",
            K(i),
            K(insert_row),
            K(update_ctx->get_update_row()),
            K(*val),
            K(*expr),
            K(old_value_index));
        if (!updated_column_infos_.at(i).auto_filled_timestamp_) {
          if (!val->strict_equal(update_ctx->get_update_row().cells_[old_value_index])) {
            is_row_changed = true;
          }
        }
      }

      // update auto-increment column in duplicate-update
      if (OB_SUCC(ret)) {
        NG_TRACE_TIMES(2, insertup_auto_increment);
        if (OB_FAIL(update_auto_increment(ctx, expr_ctx, val, i, is_auto_col_changed))) {
          LOG_WARN("fail to update auto increment column", K(ret));
        }
      }
      SQL_ENG_LOG(DEBUG,
          "calc new row",
          K(ret),
          K(i),
          K(*val),
          K(update_ctx->get_update_row().cells_[old_value_index]),
          K(update_ctx->get_update_row().cells_[result_index]));
    }  // end for

    // For insert on duplicate scenarios, there is no need to support MySQL behavior.
    // Assuming we support the behavior of MySQL,
    // it means that we are trying hard to increase the self-value-added value of the table globally,
    // instead of just keeping increments within the partition.
    // For the goal of global increase, we theoretically cannot achieve it,
    // and there is no go after it.
    // Therefore, I decided to abandon the compatible behavior in the insert on duplicate scenario.
    // However, the behavior of pushing up self-value added globally when inserting a certain value is still maintained.
    // Because this has a certain application scenario:
    // For example, the user wants to globally push the self-value-added value of all partitions
    // to a certain value at a certain point in time after that.
    // Based on this decision, delete all the code below. In order to facilitate traceability,
    // only comment out the code and keep it for a certain period of time.
    // to be compatible with MySQL
    // some value for auto-increment column may come from update stmt; we should sync it
    // for example
    // create table t1(c1 int unique, c2 int auto_increment primary key, c3 int);
    // insert into t1 values (1, 1, 1);
    // update t1 set c2 = 10 where c1 = 1;
    // insert into t1 values (1, 100, 100) on duplicate key update c2 = 10, c1=2;
    // insert into t1 values (3, null, 100);
    // the last insert should generate 11 for c2
    // If the value of the auto-increment column c1=10,
    // it is not entered through the insert statement,
    // then the auto-increment value 10 will not be synced out.
    // But if this line is inserted on again duplicate,
    // and if the update is not self-increment,
    // the old value 10 of this self-increment column will be synced out;
    UNUSED(scan_result_row);

    NG_TRACE_TIMES(2, insertup_end_auto_increment);
    if (OB_SUCC(ret)) {
      update_ctx->get_count_ = 0;
    }
  }
  return ret;
}

int ObTableInsertUp::update_auto_increment(const ObExecContext& ctx, const ObExprCtx& expr_ctx, const ObObj* val,
    int64_t assignemnt_index, bool& is_auto_col_changed) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret), K(plan_ctx));
  } else if (OB_ISNULL(val) || OB_UNLIKELY(assignemnt_index < 0) ||
             OB_UNLIKELY(assignemnt_index >= updated_column_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(val), K(updated_column_ids_));
  } else {
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    AutoincParam* autoinc_param = NULL;
    for (int64_t j = 0; OB_SUCC(ret) && j < autoinc_params.count(); ++j) {
      if (updated_column_ids_[assignemnt_index] == autoinc_params.at(j).autoinc_col_id_) {
        autoinc_param = &autoinc_params.at(j);
        break;
      }
    }
    if (NULL != autoinc_param) {
      is_auto_col_changed = true;
      uint64_t casted_value = 0;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      const ObObj& tmp_obj_val = *val;
      EXPR_GET_UINT64_V2(tmp_obj_val, casted_value);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to cast value; still go on", K(ret));
        ret = OB_SUCCESS;
      } else {
        CacheHandle* cache_handle = autoinc_param->cache_handle_;
        if (!OB_ISNULL(cache_handle) && true == cache_handle->last_row_dup_flag_ &&
            0 != cache_handle->last_value_to_confirm_) {
          // auto-increment value has been generated for this row
          if (casted_value == cache_handle->last_value_to_confirm_) {
            // column may be updated, but updated value is the same with old value
            cache_handle->last_row_dup_flag_ = false;
            cache_handle->last_value_to_confirm_ = 0;
          } else if (cache_handle->in_range(casted_value)) {
            // update value in generated range
            ret = OB_ERR_AUTO_INCREMENT_CONFLICT;
            LOG_WARN("update value in auto-generated range", K(ret));
          } else {
            autoinc_param->value_to_sync_ = casted_value;
            autoinc_param->sync_flag_ = true;
          }
        } else {
          // no auto-increment value generated; user specify a value
          // mark sync flag to sync update value
          autoinc_param->value_to_sync_ = casted_value;
          autoinc_param->sync_flag_ = true;
        }
      }
    }
  }
  NG_TRACE_TIMES(2, insertup_end_auto_increment);
  return ret;
}

int ObTableInsertUp::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, insertup_end);
  if (OB_FAIL(ObTableModify::inner_close(ctx))) {
    LOG_WARN("fail to do inner close", K(ret));
  }
  return ret;
}

int ObTableInsertUp::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  UNUSED(ctx);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

int ObTableInsertUp::add_assignment_expr(ObColumnExpression* expr)
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
int ObTableInsertUp::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableInsertUpCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create table insert update context failed", K(ret));
  } else {
    ObTableInsertUpCtx* update_ctx = static_cast<ObTableInsertUpCtx*>(op_ctx);
    int64_t update_row_column_count = scan_column_ids_.count() + assignment_infos_.count();
    if (OB_FAIL(update_ctx->init(
            ctx, old_projector_size_, primary_key_ids_.count(), update_row_column_count, column_ids_.count()))) {
      LOG_WARN("fail to init update ctx", K(ret));
    } else if (OB_FAIL(init_cur_row(*update_ctx, true))) {
      LOG_WARN("fail to init cur row", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUp::set_updated_column_info(
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
}  // namespace sql
}  // namespace oceanbase
