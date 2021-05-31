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

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/executor/ob_task_spliter.h"
#include "storage/ob_i_store.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql {

// make MY_SPEC macro available.
OB_INLINE static const ObTableModifySpec& get_my_spec(const ObTableModifyOp& op)
{
  return op.get_spec();
}

int ObTableModifyOp::DMLRowIterator::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObTableModifyOp::DMLRowIterator::setup_project_row(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_LIKELY(cnt > 0)) {
    if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObObj) * cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc projector row failed");
    } else {
      project_row_.cells_ = new (ptr) ObObj[cnt];
      project_row_.count_ = cnt;
    }
  }

  return ret;
}

int ObTableModifyOp::DMLRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("plan ctx is null", K(ret));
  }
  const ObExprPtrIArray* output = NULL;
  while (OB_SUCC(ret) && !got_row) {
    if (OB_FAIL(op_.prepare_next_storage_row(output))) {
      if (OB_ITER_END == ret) {
        if (plan_ctx->get_bind_array_count() <= 0 ||
            plan_ctx->get_bind_array_idx() >= plan_ctx->get_bind_array_count()) {
          // no bind array or reach binding array end, do nothing
        } else if (FALSE_IT(plan_ctx->inc_bind_array_idx())) {
          // do nothing
        } else if (OB_FAIL(op_.switch_iterator())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("switch op iterator failed", K(ret), "op_type", op_.get_spec().op_name());
          }
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("get next row from operator failed", K(ret));
      }
    } else if (OB_ISNULL(output)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL output returned", K(ret));
    } else {
      LOG_DEBUG("insert output row", "row", ROWEXPR2STR(*ctx_.get_eval_ctx(), *output));
      got_row = true;
    }
  }

  // convert output to old style row
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(project_row_.count_ <= 0) && OB_FAIL(setup_project_row(output->count()))) {
      LOG_WARN("setup project row failed", K(ret));
    } else if (OB_UNLIKELY(output->count() > project_row_.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output count not match", K(ret), K(project_row_.count_), K(output->count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output->count(); i++) {
        ObExpr* expr = output->at(i);
        ObDatum* datum = NULL;
        if (OB_FAIL(expr->eval(op_.get_eval_ctx(), datum))) {
          LOG_WARN("expr evaluate failed", K(ret), K(expr));
        } else if (OB_FAIL(datum->to_obj(project_row_.cells_[i], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret), K(datum), K(expr));
        }
      }  // for end
    }
  }
  if (OB_SUCC(ret)) {
    row = &project_row_;
    LOG_DEBUG("output row", K(project_row_));
  }

  return ret;
}

int ObTableModifyOp::DMLRowIterator::get_next_rows(common::ObNewRow*& rows, int64_t& row_count)
{
  int ret = get_next_row(rows);
  row_count = (OB_SUCCESS == ret) ? 1 : 0;

  return ret;
}

void ObTableModifyOp::DMLRowIterator::reset()
{
  if (OB_LIKELY(project_row_.cells_)) {
    ctx_.get_allocator().free(project_row_.cells_);
    project_row_.cells_ = NULL;
    project_row_.count_ = 0;
  }
}

ObTableModifySpec::ObTableModifySpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      table_id_(OB_INVALID_ID),
      index_tid_(OB_INVALID_ID),
      is_ignore_(false),
      from_multi_table_dml_(false),
      column_ids_(alloc),
      primary_key_ids_(alloc),
      column_infos_(alloc),
      column_conv_infos_(alloc),
      storage_row_output_(alloc),
      returning_exprs_(alloc),
      check_constraint_exprs_(alloc),
      fk_args_(alloc),
      tg_event_(0),
      table_param_(alloc),
      need_filter_null_row_(false),
      distinct_algo_(T_DISTINCT_NONE),
      gi_above_(false),
      is_returning_(false),
      lock_row_flag_expr_(NULL),
      is_pdml_index_maintain_(false),
      need_skip_log_user_error_(false),
      table_location_uncertain_(false)
{}

bool ObTableModifySpec::has_foreign_key() const
{
  return fk_args_.count() > 0;
}

int32_t ObTableModifySpec::get_column_idx(uint64_t column_id)
{
  int32_t column_idx = -1;
  for (int i = 0; i < column_ids_.count(); i++) {
    if (column_id == column_ids_.at(i)) {
      column_idx = i;
    }
  }
  if (-1 == column_idx) {
    LOG_WARN("can not find column id", K(column_id), K(column_ids_));
  }
  return column_idx;
}

int ObTableModifySpec::add_column_id(uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_id));
  } else if (OB_FAIL(column_ids_.push_back(column_id))) {
    LOG_WARN("fail to push back to column_ids", K(ret), K(column_id));
  }
  return ret;
}

int ObTableModifySpec::add_column_info(const ColumnContent& column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_infos_.push_back(column))) {
    LOG_WARN("failed to push column info", K(ret), K(column));
  }
  return ret;
}

int ObTableModifySpec::add_column_conv_info(const ObExprResType& res_type, const uint64_t column_flags,
    ObIAllocator& allocator, const ObIArray<ObString>* str_values /*NULL*/)
{
  int ret = OB_SUCCESS;
  ObColumnConvInfo column_info(allocator);
  column_info.column_flags_ = column_flags;
  ObExprResType dst_type = res_type;
  if (ObLobType == dst_type.get_type()) {
    dst_type.set_type(ObLongTextType);
  }
  if (OB_FAIL(column_info.type_.assign(dst_type))) {
    LOG_WARN("copy res type failed", K(ret), K(dst_type));
  } else if (NULL != str_values) {
    if (OB_FAIL(column_info.str_values_.assign(*str_values))) {
      LOG_WARN("copy str_value failed", K(ret), KPC(str_values));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(column_conv_infos_.push_back(column_info))) {
    LOG_WARN("failed to push column conv info", K(ret), K(column_info.type_), K_(column_info.column_flags));
  }
  return ret;
}

int ObTableModifySpec::init_foreign_key_args(int64_t fk_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fk_args_.init(fk_count))) {
    LOG_WARN("failed to init foreign key stmts", K(ret), K(fk_count));
  }
  return ret;
}

int ObTableModifySpec::add_foreign_key_arg(const ObForeignKeyArg& fk_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fk_args_.push_back(fk_arg))) {
    LOG_WARN("failed to push foreign key stmt arg", K(fk_arg), K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableModifySpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObTableModifySpec, ObOpSpec));
  OB_UNIS_ENCODE(table_id_);
  OB_UNIS_ENCODE(column_ids_);
  OB_UNIS_ENCODE(column_infos_);
  OB_UNIS_ENCODE(is_ignore_);
  OB_UNIS_ENCODE(column_conv_infos_);
  OB_UNIS_ENCODE(primary_key_ids_);
  OB_UNIS_ENCODE(storage_row_output_);
  OB_UNIS_ENCODE(returning_exprs_);
  OB_UNIS_ENCODE(from_multi_table_dml_);
  OB_UNIS_ENCODE(index_tid_);
  OB_UNIS_ENCODE(fk_args_);
  OB_UNIS_ENCODE(check_constraint_exprs_);
  OB_UNIS_ENCODE(need_filter_null_row_);
  OB_UNIS_ENCODE(distinct_algo_);
  OB_UNIS_ENCODE(gi_above_);
  OB_UNIS_ENCODE(is_returning_);
  OB_UNIS_ENCODE(lock_row_flag_expr_);
  OB_UNIS_ENCODE(is_pdml_index_maintain_);
  OB_UNIS_ENCODE(need_skip_log_user_error_);
  OB_UNIS_ENCODE(table_location_uncertain_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableModifySpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObTableModifySpec, ObOpSpec));
  if (OB_ISNULL(plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is null", K(ret));
  }
  OB_UNIS_DECODE(table_id_);
  OB_UNIS_DECODE(column_ids_);
  OB_UNIS_DECODE(column_infos_);
  OB_UNIS_DECODE(is_ignore_);
  OB_UNIS_DECODE(column_conv_infos_);
  OB_UNIS_DECODE(primary_key_ids_);
  OB_UNIS_DECODE(storage_row_output_);
  OB_UNIS_DECODE(returning_exprs_);
  OB_UNIS_DECODE(from_multi_table_dml_);
  OB_UNIS_DECODE(index_tid_);
  OB_UNIS_DECODE(fk_args_);

  OB_UNIS_DECODE(check_constraint_exprs_);
  OB_UNIS_DECODE(need_filter_null_row_);
  OB_UNIS_DECODE(distinct_algo_);
  OB_UNIS_DECODE(gi_above_);
  OB_UNIS_DECODE(is_returning_);
  OB_UNIS_DECODE(lock_row_flag_expr_);
  OB_UNIS_DECODE(is_pdml_index_maintain_);
  OB_UNIS_DECODE(need_skip_log_user_error_);
  OB_UNIS_DECODE(table_location_uncertain_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableModifySpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObTableModifySpec, ObOpSpec));
  OB_UNIS_ADD_LEN(table_id_);
  OB_UNIS_ADD_LEN(column_ids_);
  OB_UNIS_ADD_LEN(column_infos_);
  OB_UNIS_ADD_LEN(is_ignore_);
  OB_UNIS_ADD_LEN(column_conv_infos_);
  OB_UNIS_ADD_LEN(primary_key_ids_);
  OB_UNIS_ADD_LEN(storage_row_output_);
  OB_UNIS_ADD_LEN(returning_exprs_);
  OB_UNIS_ADD_LEN(from_multi_table_dml_);
  OB_UNIS_ADD_LEN(index_tid_);
  OB_UNIS_ADD_LEN(fk_args_);
  OB_UNIS_ADD_LEN(check_constraint_exprs_);
  OB_UNIS_ADD_LEN(need_filter_null_row_);
  OB_UNIS_ADD_LEN(distinct_algo_);
  OB_UNIS_ADD_LEN(gi_above_);
  OB_UNIS_ADD_LEN(is_returning_);
  OB_UNIS_ADD_LEN(lock_row_flag_expr_);
  OB_UNIS_ADD_LEN(is_pdml_index_maintain_);
  OB_UNIS_ADD_LEN(need_skip_log_user_error_);
  OB_UNIS_ADD_LEN(table_location_uncertain_);
  return len;
}

int ObTableModifyOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  bool from_multi_table_dml = false;
  LOG_DEBUG("table modify task info", K(task_info));
  if (OB_SUCC(ret)) {
    location_idx_ = task_info.get_location_idx();
    from_multi_table_dml = get_spec().from_multi_table_dml();
  }
  if (OB_SUCC(ret) && from_multi_table_dml) {
    int64_t part_key_cnt = task_info.get_range_location().part_locs_.count();
    part_infos_.reset();
    part_infos_.set_allocator(&exec_ctx_.get_allocator());
    if (OB_FAIL(part_infos_.init(part_key_cnt))) {
      LOG_WARN("init part keys array failed", K(ret), K(part_key_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_key_cnt; ++i) {
      uint64_t part_key_ref_id = task_info.get_range_location().part_locs_.at(i).part_key_ref_id_;
      const ObPartitionKey& part_key = task_info.get_range_location().part_locs_.at(i).partition_key_;
      int64_t row_cnt = 0;
      ObRowStore* row_store = task_info.get_range_location().part_locs_.at(i).row_store_;
      ObChunkDatumStore* datum_store = task_info.get_range_location().part_locs_.at(i).datum_store_;
      if (OB_ISNULL(row_store) && OB_NOT_NULL(datum_store)) {
        row_cnt = datum_store->get_row_cnt();
      } else if (OB_ISNULL(datum_store) && OB_NOT_NULL(row_store)) {
        row_cnt = row_store->get_row_count();
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(row_store), K(datum_store), K(ret));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (part_key_ref_id == spec_.id_) {
        DMLPartInfo part_info;
        part_info.partition_key_ = part_key;
        part_info.part_row_cnt_ = row_cnt;
        if (OB_FAIL(part_infos_.push_back(part_info))) {
          LOG_WARN("store partition info failed", K(ret));
        }
        LOG_DEBUG("push part info", K(part_key_ref_id), K(spec_.id_), K(part_infos_));
      }
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTableModifyOpInput, location_idx_, part_infos_);

ObTableModifyOp::ObTableModifyOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(ctx, spec, input),
      sql_proxy_(NULL),
      inner_conn_(NULL),
      tenant_id_(0),
      saved_conn_(),
      is_nested_session_(false),
      foreign_key_checks_(false),
      need_close_conn_(false),
      rowkey_dist_ctx_(NULL),
      tg_old_row_(),
      tg_new_row_(),
      tg_when_point_params_(),
      tg_stmt_point_params_(),
      tg_row_point_params_(),
      tg_all_params_(NULL),
      iter_end_(false),
      saved_session_(NULL),
      fk_self_ref_row_res_infos_()
{}

int ObTableModifyOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_foreign_key_operation())) {
    LOG_WARN("failed to init foreign key operation", K(ret));
  }

  return ret;
}

int ObTableModifyOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (rowkey_dist_ctx_ != nullptr) {
    rowkey_dist_ctx_->reuse();
  }
  if (OB_FAIL(ObOperator::switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch iterator failed", K(ret));
    }
  }

  return ret;
}

int ObTableModifyOp::inner_close()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  // release cache_handle for auto-increment
  share::ObAutoincrementService& auto_service = share::ObAutoincrementService::get_instance();
  ObIArray<share::AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
  for (int64_t i = 0; i < autoinc_params.count(); ++i) {
    if (NULL != autoinc_params.at(i).cache_handle_) {
      auto_service.release_handle(autoinc_params.at(i).cache_handle_);
    }
  }
  // see ObMultiPartUpdate::inner_open().
  if (get_spec().has_foreign_key() && need_foreign_key_checks()) {
    close_inner_conn();
  }

  return ret;
}

int ObTableModifyOp::set_autoinc_param_pkey(const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  int64_t auto_increment_cache_size = -1;
  if (OB_FAIL(my_session->get_auto_increment_cache_size(auto_increment_cache_size))) {
    LOG_WARN("fail to get increment factor", K(ret));
  } else {
    ObIArray<share::AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).pkey_ = pkey;
      autoinc_params.at(i).is_ignore_ = get_spec().is_ignore_;
      autoinc_params.at(i).auto_increment_cache_size_ = auto_increment_cache_size;
    }
  }

  return ret;
}

int ObTableModifyOp::lock_row(
    const ObExprPtrIArray& row, storage::ObDMLBaseParam& dml_param, const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  const int64_t pk_cnt = MY_SPEC.primary_key_ids_.count();
  if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (row.count() < pk_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is smaller than primary key column count", K(ret), K(row.count()), K(pk_cnt));
  } else if (OB_FAIL(project_row(row.get_data(), pk_cnt, lock_row_))) {
    LOG_WARN("project expr to row failed", K(ret));
  } else {
    ObLockFlag lock_flag = LF_NONE;
    // There is not only the primary key column in the row,
    // but we ensure that the primary key is listed first
    // partition_service internally obtains the number of primary key columns from the schema
    bool old_flag = dml_param.only_data_table_;
    dml_param.only_data_table_ = true;  // Row lock can be added to the main table
    if (OB_FAIL(partition_service->lock_rows(
            my_session->get_trans_desc(), dml_param, dml_param.timeout_, pkey, lock_row_, lock_flag))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to lock rows", K(ret));
      }
    } else {
      SQL_ENG_LOG(DEBUG, "lock rows", K(lock_row_));
    }
    dml_param.only_data_table_ = old_flag;
  }
  return ret;
}

int ObTableModifyOp::project_row(ObExpr* const* exprs, const int64_t cnt, common::ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == exprs) || OB_UNLIKELY(cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL exprs or invalid cnt", K(ret), KP(exprs), K(cnt));
  } else if (row.get_count() <= 0) {
    // ctx_.get_allocator() is arena allocator, no need to free.
    ObObj* cells = static_cast<ObObj*>(ctx_.get_allocator().alloc(sizeof(ObObj) * cnt));
    if (OB_ISNULL(cells)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (cells) ObObj[cnt];
      row.cells_ = cells;
      row.count_ = cnt;
    }
  } else if (OB_UNLIKELY(row.get_count() < cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row cell count changed", K(ret), K(row.get_count()), K(cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
    ObDatum* datum = NULL;
    const ObExpr* e = exprs[i];
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (OB_FAIL(e->eval(eval_ctx_, datum))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else if (OB_FAIL(datum->to_obj(row.cells_[i], e->obj_meta_, e->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    }
  }
  return ret;
}

int ObTableModifyOp::project_row(
    const ObDatum* datums, ObExpr* const* exprs, const int64_t cnt, common::ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(datums));
  CK(OB_NOT_NULL(exprs));
  CK(cnt > 0);

  if (row.get_count() <= 0) {
    // ctx_.get_allocator() is arena allocator, no need to free.
    ObObj* cells = static_cast<ObObj*>(ctx_.get_allocator().alloc(sizeof(ObObj) * cnt));
    if (OB_ISNULL(cells)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (cells) ObObj[cnt];
      row.cells_ = cells;
      row.count_ = cnt;
    }
  } else if (OB_UNLIKELY(row.get_count() < cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row cell count changed", K(ret), K(row.get_count()), K(cnt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    if (OB_FAIL(datums[i].to_obj(row.cells_[i], exprs[i]->obj_meta_, exprs[i]->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    }
  }
  return ret;
}

OB_INLINE int ObTableModifyOp::init_foreign_key_operation()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify_ctx or phy_plan_ctx is NULL", K(ret), KP(phy_plan_ctx));
  } else if ((share::is_mysql_mode() && phy_plan_ctx->need_foreign_key_checks()) || share::is_oracle_mode()) {
    if (MY_SPEC.has_foreign_key() && OB_FAIL(open_inner_conn())) {
      LOG_WARN("failed to open inner conn", K(ret));
    }
    // set fk check even if ret != OB_SUCCESS. see ObTableModifyOp::inner_close()
    set_foreign_key_checks();
  }
  return ret;
}

int ObTableModifyOp::get_gi_task()
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo gi_task_info;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  // If the dml query part is enabled to use px:
  // 1. If dml uses PX for query, then you need to get the corresponding gi task through tsc_op_id
  // 2. If dml does not use px for query, get the corresponding gi task directly through the current dml op id
  const bool use_px = spec_.plan_->is_use_px();
  // If it is currently an INSERT operator, get the corresponding GI Task through the op id of the operator itself
  int64_t op_id = OB_INVALID_INDEX;
  if (IS_DML(MY_SPEC.type_)) {
    op_id = MY_SPEC.id_;
  }
  if (OB_FAIL(ctx_.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (use_px && op_id == OB_INVALID_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan op id is invalid", K(ret), K(use_px));
  } else if (use_px && OB_FAIL(gi_prepare_map->get_refactored(op_id, gi_task_info))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get prepare gi task", K(ret), K(MY_SPEC.get_id()));
    } else {
      LOG_DEBUG("no prepared task info, set table modify to end", K(MY_SPEC.get_id()), K(op_id), K(this), K(lbt()));
      // The current DML operator cannot get the task from GI,
      // which means the current DML operator iter end
      iter_end_ = true;
      ret = OB_SUCCESS;
    }
  } else if (!use_px && OB_FAIL(gi_prepare_map->get_refactored(MY_SPEC.id_, gi_task_info))) {
    LOG_WARN("failed to get prepare gi task", K(ret), K(MY_SPEC.id_));
  } else if (OB_ISNULL(get_input())) {
    // Never be NULL if operator register INPUT, never reach here.
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::translate_pid_to_ldx(ctx_.get_task_exec_ctx(),
                 gi_task_info.partition_id_,
                 MY_SPEC.table_id_,
                 MY_SPEC.index_tid_,
                 get_input()->location_idx_))) {
    LOG_WARN("failed to do gi task scan", K(ret));
  } else {
    ctx_.set_expr_partition_id(gi_task_info.partition_id_);
    LOG_DEBUG("DML operator consume a task", K(ret), K(gi_task_info), KPC(get_input()), K(MY_SPEC.id_), K(get_input()));
  }
  return ret;
}

int ObTableModifyOp::calc_part_id(const ObExpr* calc_part_id_expr, ObIArray<int64_t>& part_ids, int64_t& part_idx)
{
  int ret = OB_SUCCESS;
  ObDatum* partition_id_datum = NULL;
  if (OB_ISNULL(calc_part_id_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc part id expr is null", K(ret));
  } else if (OB_FAIL(calc_part_id_expr->eval(eval_ctx_, partition_id_datum))) {
    LOG_WARN("fail to calc part id", K(ret), K(calc_part_id_expr));
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id_datum->get_int()) {
    ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
    LOG_WARN("no partition matched", K(ret), K(child_->get_spec().output_));
  } else {
    if (OB_FAIL(add_var_to_array_no_dup(part_ids, partition_id_datum->get_int(), &part_idx))) {
      LOG_WARN("Failed to add var to array no dup", K(ret));
    }
  }

  return ret;
}

int ObTableModifyOp::check_row_null(const ObExprPtrIArray& row, const ObIArray<ColumnContent>& column_infos) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2220) {
    if (row.count() != column_infos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row.count() != column_infos.count()", K(row.count()), K(column_infos.count()), K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < column_infos.count(); i++) {
      ObDatum* datum = NULL;
      const bool is_nullable = column_infos.at(i).is_nullable_;
      if (OB_FAIL(row.at(i)->eval(eval_ctx_, datum))) {
        const ObTableInsertOp* insert_op = dynamic_cast<const ObTableInsertOp*>(this);
        const ObTableUpdateOp* update_op = dynamic_cast<const ObTableUpdateOp*>(this);
        if (nullptr != insert_op) {
          // compatible with old code
          log_user_error_inner(ret, i, insert_op->curr_row_num_ + 1, MY_SPEC.column_infos_);
        } else if (nullptr != update_op) {
          // compatible with old code
          log_user_error_inner(ret, i, update_op->get_found_rows() + 1, MY_SPEC.column_infos_);
        }
      } else if (!is_nullable && datum->is_null()) {
        if (MY_SPEC.is_ignore_) {
          ObObj zero_obj;
          if (is_oracle_mode()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dml with ignore not supported in oracle mode");
          } else if (OB_FAIL(ObObjCaster::get_zero_value(
                         column_infos.at(i).column_type_, column_infos.at(i).coll_type_, zero_obj))) {
            LOG_WARN("get column default zero value failed", K(ret), K(column_infos.at(i)));
          } else if (OB_FAIL(datum->from_obj(zero_obj))) {
            LOG_WARN("assign zero obj to datum failed", K(ret), K(zero_obj));
          } else {
            // output warning msg
            const ObString& column_name = column_infos.at(i).column_name_;
            LOG_USER_WARN(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
          }
        } else {
          const ObString& column_name = column_infos.at(i).column_name_;
          ret = OB_BAD_NULL_ERROR;
          LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
        }
      }
    }
  }
  return ret;
}

int ObTableModifyOp::get_part_location(
    const ObPhyTableLocation& table_location, const share::ObPartitionReplicaLocation*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  int64_t location_idx = 0;
  if (table_location.get_partition_location_list().count() > 1) {
    if (get_input() != NULL) {
      location_idx = get_input()->get_location_idx();
    } else {
      ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
      LOG_WARN("Multi-partition DML not supported", K(ret), K(table_location));
    }
  }
  CK(location_idx >= 0 && location_idx < table_location.get_partition_cnt());
  if (OB_SUCC(ret)) {
    out = &(table_location.get_partition_location_list().at(location_idx));
    LOG_DEBUG("get part location", KPC(out), K(location_idx));
  }

  return ret;
}

int ObTableModifyOp::get_part_location(ObIArray<DMLPartInfo>& part_keys)
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  const share::ObPartitionReplicaLocation* part_location = NULL;
  ObTaskExecutorCtx& executor_ctx = ctx_.get_task_exec_ctx();
  ObTableModifyOpInput* dml_input = get_input();
  if (dml_input != NULL && !dml_input->part_infos_.empty()) {
    if (OB_FAIL(part_keys.assign(dml_input->part_infos_))) {
      LOG_WARN("assign part keys failed", K(ret));
    }
    LOG_DEBUG("get part location assign part keys", K(ret), K(part_keys));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 executor_ctx, get_spec().table_id_, get_spec().index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(get_spec().table_id_), K(get_spec().index_tid_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(
        "non-partition dml is not supported!", "# partitions", table_location->get_partition_location_list().count());
  } else if (OB_FAIL(get_part_location(*table_location, part_location))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (OB_ISNULL(part_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part_location", K(part_location), K(ret));
  } else {
    DMLPartInfo part_info;
    part_info.partition_key_.init(
        get_spec().index_tid_, part_location->get_partition_id(), part_location->get_partition_cnt());
    part_info.part_row_cnt_ = -1;
    if (OB_FAIL(part_keys.push_back(part_info))) {
      LOG_WARN("store partition key failed", K(ret));
    }
  }

  return ret;
}

// update full row contain old column scanned from storage and updated column calculated by update statement
int ObTableModifyOp::check_row_value(bool& updated, const common::ObIArrayWrap<ColumnContent>& update_column_infos,
    const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, update_start_check_row);
  updated = false;
  FOREACH_CNT_X(info, update_column_infos, OB_SUCC(ret) && !updated)
  {
    // The update operation will record the index listed in the projector to be updated
    // The location of the new row and the old row can be found through the index of the projector
    const uint64_t idx = info->projector_index_;
    if (idx >= old_row.count() || idx > new_row.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), K(*info), K(old_row.count()), K(new_row.count()));
    } else if (OB_LIKELY(!info->auto_filled_timestamp_)) {
      ObDatum* old_datum = NULL;
      ObDatum* new_datum = NULL;
      if (OB_FAIL(old_row.at(idx)->eval(eval_ctx_, old_datum)) ||
          OB_FAIL(new_row.at(idx)->eval(eval_ctx_, new_datum))) {
        LOG_WARN("evaluate value failed", K(ret));
      } else {
        updated = !ObDatum::binary_equal(*old_datum, *new_datum);
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t flag = ObActionFlag::OP_MIN_OBJ;
    if (OB_UNLIKELY(!updated)) {
      flag = ObActionFlag::OP_LOCK_ROW;
    }
    OZ(mark_lock_row_flag(flag));
  }
  NG_TRACE_TIMES(2, update_end_check_row);
  return ret;
}

int ObTableModifyOp::mark_lock_row_flag(int64_t flag)
{
  int ret = OB_SUCCESS;
  ObExpr* expr = get_spec().lock_row_flag_expr_;
  if (OB_ISNULL(expr)) {
    // do nothing
  } else {
    expr->locate_datum_for_write(eval_ctx_).set_int(flag);
    expr->get_eval_info(eval_ctx_).evaluated_ = true;
  }

  return ret;
}

int ObTableModifyOp::check_rowkey_whether_distinct(const ObExprPtrIArray& row, int64_t rowkey_cnt,
    DistinctType distinct_algo, SeRowkeyDistCtx*& rowkey_dist_ctx, bool& is_dist)
{
  int ret = OB_SUCCESS;
  is_dist = true;
  if (T_DISTINCT_NONE != distinct_algo) {
    if (T_HASH_DISTINCT == distinct_algo) {
      ObIAllocator& allocator = eval_ctx_.exec_ctx_.get_allocator();
      if (OB_ISNULL(rowkey_dist_ctx)) {
        // create rowkey distinct context
        void* buf = allocator.alloc(sizeof(SeRowkeyDistCtx));
        ObSQLSessionInfo* my_session = eval_ctx_.exec_ctx_.get_my_session();
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), "size", sizeof(SeRowkeyDistCtx));
        } else if (OB_ISNULL(my_session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("my session is null", K(ret));
        } else {
          rowkey_dist_ctx = new (buf) SeRowkeyDistCtx();
          int64_t match_rows =
              get_spec().rows_ > MIN_ROWKEY_DISTINCT_BUCKET_NUM ? get_spec().rows_ : MIN_ROWKEY_DISTINCT_BUCKET_NUM;
          // match_rows is the result of the optimizer's estimation. If this value is large,
          // Directly creating a hashmap with so many buckets will apply
          // There is no memory, here is a limit of 64k to prevent the error of insufficient memory
          const int64_t max_bucket_num =
              match_rows > MAX_ROWKEY_DISTINCT_BUCKET_NUM ? MAX_ROWKEY_DISTINCT_BUCKET_NUM : match_rows;
          if (OB_FAIL(rowkey_dist_ctx->create(max_bucket_num,
                  ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_BUCKET,
                  ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_NODE,
                  my_session->get_effective_tenant_id()))) {
            LOG_WARN("create rowkey distinct context failed", K(ret), "rows", get_spec().rows_);
          }
        }
      }
      if (OB_SUCC(ret)) {
        SeRowkeyItem rowkey_item;
        if (OB_ISNULL(rowkey_dist_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowkey_dist_ctx cannot be NULL", K(ret));
        } else if (OB_FAIL(rowkey_item.init(row, eval_ctx_, allocator, rowkey_cnt))) {
          LOG_WARN("init rowkey item failed", K(ret));
        } else {
          ret = rowkey_dist_ctx->exist_refactored(rowkey_item);
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
            is_dist = false;
          } else if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(rowkey_item.copy_datum_data(allocator))) {
              LOG_WARN("deep_copy rowkey item failed", K(ret));
            } else if (OB_FAIL(rowkey_dist_ctx->set_refactored(rowkey_item))) {
              LOG_WARN("set rowkey item failed", K(ret));
            }
          } else {
            LOG_WARN("check if rowkey item exists failed", K(ret));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Check Update/Delete row with merge distinct");
    }
  }
  return ret;
}

bool ObTableModifyOp::init_returning_store()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_returning_) {
    const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    int64_t sort_area_size = ctx_.get_my_session()->get_tenant_sort_area_size();
    if (OB_FAIL(returning_datum_store_.init(
            sort_area_size, tenant_id, ObCtxIds::DEFAULT_CTX_ID, ObModIds::OB_SQL_CHUNK_ROW_STORE, true))) {
      LOG_WARN("init chunk datum store failed", K(ret));
    }
  }
  return ret;
}

int ObTableModifyOp::open_inner_conn()
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnectionPool* pool = NULL;
  ObSQLSessionInfo* session = NULL;
  ObISQLConnection* conn;
  if (OB_ISNULL(sql_proxy_ = ctx_.get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is NULL", K(ret));
  } else if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL != session->get_inner_conn()) {
    // do nothing.
  } else if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool*>(sql_proxy_->get_pool()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection pool is NULL", K(ret));
  } else if (INNER_POOL != pool->get_type()) {
    LOG_WARN("connection pool type is not inner", K(ret), K(pool->get_type()));
  } else if (OB_FAIL(pool->acquire(session, conn))) {
    LOG_WARN("failed to acquire inner connection", K(ret));
  } else {
    /**
     * session is the only data struct which can pass through multi layer nested sql,
     * so we put inner conn in session to share it within multi layer nested sql.
     */
    session->set_inner_conn(conn);
    need_close_conn_ = true;
  }
  if (OB_SUCC(ret)) {
    inner_conn_ = static_cast<ObInnerSQLConnection*>(session->get_inner_conn());
    tenant_id_ = session->get_effective_tenant_id();
    is_nested_session_ = session->is_nested_session();
  }
  return ret;
}

int ObTableModifyOp::close_inner_conn()
{
  /**
   * we can call it even if open_inner_conn() failed, because only the one who call
   * open_inner_conn() succeed first will do close job by "if (need_close_conn_)".
   */
  int ret = OB_SUCCESS;
  if (need_close_conn_) {
    ObSQLSessionInfo* session = ctx_.get_my_session();
    if (OB_ISNULL(sql_proxy_) || OB_ISNULL(session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql_proxy of session is NULL", K(ret), KP(sql_proxy_), KP(session));
    } else {
      OZ(sql_proxy_->close(static_cast<ObInnerSQLConnection*>(session->get_inner_conn()), true));
      OX(session->set_inner_conn(NULL));
    }
    need_close_conn_ = false;
  }
  sql_proxy_ = NULL;
  inner_conn_ = NULL;
  return ret;
}

int ObTableModifyOp::begin_nested_session(bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(inner_conn_->begin_nested_session(get_saved_session(), saved_conn_, skip_cur_stmt_tables))) {
    LOG_WARN("failed to begin nested session", K(ret));
  }
  return ret;
}

int ObTableModifyOp::end_nested_session()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(inner_conn_->end_nested_session(get_saved_session(), saved_conn_))) {
    LOG_WARN("failed to end nested session", K(ret));
  }
  return ret;
}

int ObTableModifyOp::set_foreign_key_cascade(bool is_cascade)
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->set_foreign_key_cascade(is_cascade));
  return ret;
}

int ObTableModifyOp::get_foreign_key_cascade(bool& is_cascade) const
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->get_foreign_key_cascade(is_cascade));
  return ret;
}

int ObTableModifyOp::set_foreign_key_check_exist(bool is_check_exist)
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->set_foreign_key_check_exist(is_check_exist));
  return ret;
}

int ObTableModifyOp::get_foreign_key_check_exist(bool& is_check_exist) const
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->get_foreign_key_check_exist(is_check_exist));
  return ret;
}

int ObTableModifyOp::execute_write(const char* sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is NULL");
  } else if (OB_FAIL(inner_conn_->execute_write(tenant_id_, sql, affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(tenant_id_), K(sql));
  }
  return ret;
}

int ObTableModifyOp::execute_read(const char* sql, ObMySQLProxy::MySQLResult& res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection or sql proxy is NULL", K(ret), KP(inner_conn_), KP(sql_proxy_));
  } else if (OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is NULL");
  } else if (OB_FAIL(inner_conn_->execute_read(tenant_id_, sql, res))) {
    LOG_WARN("failed to execute sql", K(ret), K(tenant_id_), K(sql));
  }
  return ret;
}

int ObTableModifyOp::check_stack()
{
  int ret = OB_SUCCESS;
  const int max_stack_deep = 16;
  bool is_stack_overflow = false;
  ObSQLSessionInfo* session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("sql session is NULL", K(ret));
  } else if (session->get_nested_count() > max_stack_deep) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(max_stack_deep), K(session->get_nested_count()));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("fail to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  }
  return ret;
}

int ObTableModifyOp::check_rowkey_is_null(const ObExprPtrIArray& row, int64_t rowkey_cnt, bool& is_null) const
{
  int ret = OB_SUCCESS;
  is_null = false;
  CK(row.count() >= rowkey_cnt);
  ObDatum* datum = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !is_null && i < rowkey_cnt; ++i) {
    if (OB_FAIL(row.at(i)->eval(eval_ctx_, datum))) {
      LOG_WARN("eval expr failed", K(ret), K(i));
    } else {
      is_null = datum->is_null();
    }
  }
  return ret;
}

// eval exprs check_constraint_exprs[beg_idx, end_idx)
int ObTableModifyOp::filter_row_for_check_cst(
    const ExprFixedArray& check_constraint_exprs, bool& filtered, int64_t beg_idx, int64_t end_idx) const
{
  ObDatum* datum = NULL;
  int ret = OB_SUCCESS;
  filtered = false;
  if (beg_idx == OB_INVALID_ID && end_idx == OB_INVALID_ID) {
    beg_idx = 0;
    end_idx = check_constraint_exprs.count();
  } else if (OB_UNLIKELY(end_idx > check_constraint_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("end_idx must be less equal than check_constraint_exprs.count()",
        K(ret),
        K(end_idx),
        K(check_constraint_exprs.count()));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(beg_idx < 0 || end_idx < beg_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected beg_idx or end_idx", K(ret), K(beg_idx), K(end_idx));
    }
    for (int64_t i = beg_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      ObExpr* e = check_constraint_exprs.at(i);
      OB_ASSERT(NULL != e);
      if (OB_FAIL(e->eval(eval_ctx_, datum))) {
        LOG_WARN("expr evaluate failed", K(ret), "expr", e);
      } else {
        OB_ASSERT(ob_is_int_tc(e->datum_meta_.type_));
        if (!datum->is_null() && 0 == datum->get_int()) {
          filtered = true;
          break;
        }
      }
    }
  }
  return ret;
}

OperatorOpenOrder ObTableModifyOp::get_operator_open_order() const
{
  OperatorOpenOrder open_order = OPEN_CHILDREN_FIRST;
  if (NULL != get_input()) {
    if (OB_UNLIKELY(get_spec().from_multi_table_dml())) {
      // For multi-partition dml, since the data is dynamically generated,
      // the plan can only be generated in advance
      if (OB_UNLIKELY(get_input()->part_infos_.empty())) {
        open_order = OPEN_NONE;
      }
    } else {
      if (spec_.plan_->is_use_px()) {
        // If PX is turned on in dml, there are two situations:
        // 1. PDML: In pdml, pdml-op does not need its corresponding input to provide runtime parameters,
        //    so it will return directly
        // open_order = OPEN_CHILDREN_FIRST
        // 2. DML+PX: In this case, the parameters of dml operation are completely plugged in by the GI operator on its
        // head,
        //    such as this plan:
        //   PX COORD
        //    PX TRANSMIT
        //      GI (FULL PARTITION WISE)
        //       DELETE
        //        TSC
        // Such a plan requires GI to drive deletion. Every time GI iterates a new task, rescan delete it.
        // The information of the corresponding Task is stuffed into the delete operator.
        open_order = OPEN_CHILDREN_FIRST;
      }
    }
  }
  return open_order;
}

int ObTableModifyOp::restore_and_reset_fk_res_info()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fk_self_ref_row_res_infos_.count(); ++i) {
    const ForeignKeyHandle::ObFkRowResInfo& restore_info = fk_self_ref_row_res_infos_.at(i);
    restore_info.rt_expr_->locate_expr_datum(eval_ctx_).set_datum(restore_info.ori_datum_);
  }
  OX(fk_self_ref_row_res_infos_.reset());
  return ret;
}

void ObTableModifyOp::log_user_error_inner(
    int ret, int64_t col_idx, int64_t row_num, const ObIArray<ColumnContent>& column_infos) const
{
  if (OB_DATA_OUT_OF_RANGE == ret && (0 <= col_idx && col_idx < column_infos.count())) {
    ObString column_name = column_infos.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx_.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx_.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, column_name.length(), column_name.ptr(), row_num);
  } else if (OB_ERR_DATA_TOO_LONG == ret && (0 <= col_idx && col_idx < column_infos.count())) {
    ObString column_name = column_infos.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx_.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx_.get_my_session()->get_local_collation_connection());
    if (lib::is_mysql_mode() || !MY_SPEC.need_skip_log_user_error_) {
      // compatible with old code
      LOG_USER_ERROR(OB_ERR_DATA_TOO_LONG, column_name.length(), column_name.ptr(), row_num);
    }
  } else if (OB_ERR_VALUE_LARGER_THAN_ALLOWED == ret && col_idx >= 0) {
    const ObString& column_name = column_infos.at(col_idx).column_name_;
    LOG_USER_ERROR(OB_ERR_VALUE_LARGER_THAN_ALLOWED);
  } else if ((OB_INVALID_DATE_VALUE == ret || OB_INVALID_DATE_FORMAT == ret) &&
             (0 <= col_idx && col_idx < column_infos.count())) {
    ObString column_name = column_infos.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx_.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx_.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_ERR_INVALID_DATE_MSG_FMT_V2, column_name.length(), column_name.ptr(), row_num);
  } else {
    LOG_WARN("fail to operate row", K(ret));
  }
}
int ObTableModifyOp::ForeignKeyHandle::do_handle_old_row(
    ObTableModifyOp& modify_op, const ObForeignKeyArgArray& fk_args, const ObExprPtrIArray& old_row)
{
  int ret = OB_SUCCESS;
  static const ExprFixedArray tmp_new_row;
  if (OB_FAIL(do_handle(modify_op, fk_args, old_row, tmp_new_row))) {
    LOG_WARN("do_handle failed", K(ret), K(fk_args), K(old_row));
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::do_handle_new_row(
    ObTableModifyOp& modify_op, const ObForeignKeyArgArray& fk_args, const ObExprPtrIArray& new_row)
{
  int ret = OB_SUCCESS;
  static const ExprFixedArray tmp_old_row;
  if (OB_FAIL(do_handle(modify_op, fk_args, tmp_old_row, new_row))) {
    LOG_WARN("do_handle failed", K(ret), K(fk_args), K(new_row));
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::do_handle(ObTableModifyOp& op, const ObForeignKeyArgArray& fk_args,
    const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row)
{
  int ret = OB_SUCCESS;
  if (op.need_foreign_key_checks()) {
    LOG_DEBUG("do foreign_key_handle", K(fk_args), K(old_row), K(new_row));
    if (OB_FAIL(op.check_stack())) {
      LOG_WARN("fail to check stack", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < fk_args.count(); i++) {
      const ObForeignKeyArg& fk_arg = fk_args.at(i);
      if (OB_SUCC(ret) && !new_row.empty()) {
        if (ACTION_CHECK_EXIST == fk_arg.ref_action_) {
          // insert or update.
          bool is_foreign_key_cascade = false;
          if (OB_FAIL(op.get_foreign_key_cascade(is_foreign_key_cascade))) {
            LOG_WARN("failed to get foreign key cascade", K(ret), K(fk_arg), K(new_row));
          } else if (is_foreign_key_cascade) {
            // nested update can not check parent row.
            LOG_DEBUG("skip foreign_key_check_exist in nested session");
          } else if (OB_FAIL(check_exist(op, fk_arg, new_row, false))) {
            LOG_WARN("failed to check exist", K(ret), K(fk_arg), K(new_row));
          }
        }
      }
      if (OB_SUCC(ret) && !old_row.empty()) {
        if (ACTION_RESTRICT == fk_arg.ref_action_ || ACTION_NO_ACTION == fk_arg.ref_action_) {
          // update or delete.
          bool has_changed = false;
          if (OB_FAIL(value_changed(op, fk_arg.columns_, old_row, new_row, has_changed))) {
            LOG_WARN("failed to check if foreign key value has changed", K(ret), K(fk_arg), K(old_row), K(new_row));
          } else if (!has_changed) {
            // nothing.
          } else if (OB_FAIL(check_exist(op, fk_arg, old_row, true))) {
            LOG_WARN("failed to check exist", K(ret), K(fk_arg), K(old_row));
          }
        } else if (ACTION_CASCADE == fk_arg.ref_action_) {
          // update or delete.
          bool is_self_ref = false;
          if (OB_FAIL(is_self_ref_row(op.eval_ctx_, old_row, fk_arg, is_self_ref))) {
            LOG_WARN("is_self_ref_row failed", K(ret), K(old_row), K(fk_arg));
          } else if (new_row.empty() && is_self_ref && op.is_nested_session()) {
            // delete self refercnced row should not cascade delete.
          } else if (OB_FAIL(cascade(op, fk_arg, old_row, new_row))) {
            LOG_WARN("failed to cascade", K(ret), K(fk_arg), K(old_row), K(new_row));
          } else if (!new_row.empty() && is_self_ref) {
            // we got here only when:
            //  1. handling update operator and
            //  2. foreign key constraint is self reference
            // need to change %new_row and %old_row
            // xxx_row_res_info helps to restore row. restore row before get_next_row()
            op.fk_self_ref_row_res_infos_.reset();
            for (int64_t i = 0; OB_SUCC(ret) && i < fk_arg.columns_.count(); i++) {
              const int32_t val_idx = fk_arg.columns_.at(i).idx_;
              const int32_t name_idx = fk_arg.columns_.at(i).name_idx_;
              if (OB_SUCC(ret)) {
                const ObDatum& updated_value = new_row.at(val_idx)->locate_expr_datum(op.eval_ctx_);
                ObDatum& new_row_name_col = new_row.at(name_idx)->locate_expr_datum(op.eval_ctx_);
                ObDatum& old_row_name_col = old_row.at(name_idx)->locate_expr_datum(op.eval_ctx_);

                OZ(op.fk_self_ref_row_res_infos_.push_back(ObFkRowResInfo{new_row.at(name_idx), new_row_name_col}));
                OZ(op.fk_self_ref_row_res_infos_.push_back(ObFkRowResInfo{old_row.at(name_idx), old_row_name_col}));
                OX(new_row_name_col = updated_value);
                OX(old_row_name_col = updated_value);
              }
            }
          }
        }
      }  // if (old_row.is_valid())
    }    // for
  } else {
    LOG_DEBUG("skip foreign_key_handle");
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::value_changed(ObTableModifyOp& op, const ObIArray<ObForeignKeyColumn>& columns,
    const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row, bool& has_changed)
{
  int ret = OB_SUCCESS;
  has_changed = false;
  if (!old_row.empty() && !new_row.empty()) {
    ObDatum* old_row_col = NULL;
    ObDatum* new_row_col = NULL;
    ObExprCmpFuncType cmp_func = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has_changed && i < columns.count(); ++i) {
      int64_t col_idx = columns.at(i).idx_;
      CK(col_idx < old_row.count());
      CK(col_idx < new_row.count());
      CK(OB_NOT_NULL(old_row.at(col_idx)));
      CK(OB_NOT_NULL(new_row.at(col_idx)));
      OZ(old_row.at(col_idx)->eval(op.eval_ctx_, old_row_col));
      OZ(new_row.at(col_idx)->eval(op.eval_ctx_, new_row_col));
      OX(has_changed = (false == ObDatum::binary_equal(*old_row_col, *new_row_col)));
    }
  } else {
    has_changed = true;
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::check_exist(
    ObTableModifyOp& op, const ObForeignKeyArg& fk_arg, const ObExprPtrIArray& row, bool expect_zero)
{
  int ret = OB_SUCCESS;
  static const char* SELECT_FMT_MYSQL = "select /*+ no_parallel */ 1 from `%.*s`.`%.*s` where %.*s limit 2 for update";
  static const char* SELECT_FMT_ORACLE =
      "select /*+ no_parallel */ 1 from %.*s.%.*s where %.*s and rownum <= 2 for update";
  const char* select_fmt = lib::is_mysql_mode() ? SELECT_FMT_MYSQL : SELECT_FMT_ORACLE;
  char stmt_buf[2048] = {0};
  char where_buf[2048] = {0};
  int64_t stmt_pos = 0;
  int64_t where_pos = 0;
  const ObString& database_name = fk_arg.database_name_;
  const ObString& table_name = fk_arg.table_name_;
  if (row.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is invalid", K(ret));
  } else if (OB_FAIL(gen_where(op.eval_ctx_,
                 where_buf,
                 sizeof(where_buf),
                 where_pos,
                 fk_arg.columns_,
                 row,
                 op.get_obj_print_params()))) {
    if (OB_LIKELY(OB_ERR_NULL_VALUE == ret)) {
      // skip check exist if any column in foreign key is NULL.
      ret = OB_SUCCESS;
      stmt_pos = 0;
    } else {
      LOG_WARN("failed to gen foreign key where", K(ret), K(row), K(fk_arg.columns_));
    }
  } else if (OB_FAIL(databuff_printf(stmt_buf,
                 sizeof(stmt_buf),
                 stmt_pos,
                 select_fmt,
                 database_name.length(),
                 database_name.ptr(),
                 table_name.length(),
                 table_name.ptr(),
                 static_cast<int>(where_pos),
                 where_buf))) {
    LOG_WARN("failed to print stmt", K(ret), K(table_name), K(where_buf));
  } else {
    stmt_buf[stmt_pos++] = 0;
  }
  if (OB_SUCC(ret) && stmt_pos > 0) {
    LOG_DEBUG("foreign_key_check_exist", "stmt", stmt_buf, K(row), K(fk_arg));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(op.begin_nested_session(fk_arg.is_self_ref_))) {
        LOG_WARN("failed to begin nested session", K(ret), K(stmt_buf));
      } else if (OB_FAIL(op.set_foreign_key_check_exist(true))) {
        LOG_WARN("failed to set foreign key cascade", K(ret));
      } else {
        // must call end_nested_session() if begin_nested_session() success.
        bool is_zero = false;
        if (OB_FAIL(op.execute_read(stmt_buf, res))) {
          LOG_WARN("failed to execute stmt", K(ret), K(stmt_buf));
        } else {
          // must call res.get_result()->close() if execute_read() success.
          if (OB_ISNULL(res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is NULL", K(ret));
          } else if (OB_FAIL(res.get_result()->next())) {
            if (OB_ITER_END == ret) {
              is_zero = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get next", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (is_zero != expect_zero) {
              bool is_self_ref = false;
              // oracle for update does not support aggregate functions, so when is_zero is false
              // judge whether only one row is affected by whether the second record can be obtained
              bool is_affect_only_one = false;
              if (!is_zero && OB_FAIL(res.get_result()->next())) {
                if (OB_ITER_END == ret) {
                  is_affect_only_one = true;
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get next", K(ret));
                }
              }
              /**
               *  expect_zero is false when fk_arg.ref_action_ is equal to
               *  ACTION_CHECK_EXIST, if the is_zero != expect_zero condition is
               *  true, then is_zero is true, need to exclude the case of self reference.
               *  other cases return OB_ERR_NO_REFERENCED_ROW.
               *
               *  expect_zero is true when fk_arg.ref_action_ is equal to
               *  ACTION_RESTRICT or ACTION_NO_ACTION, if is_zero != expect_zero condition
               *  is true, then is_zero is false, need to exclude the case of self reference and
               *  only affect one row. other cases return OB_ERR_ROW_IS_REFERENCED.
               */
              if (OB_FAIL(is_self_ref_row(op.eval_ctx_, row, fk_arg, is_self_ref))) {
                LOG_WARN("is_self_ref_row failed", K(ret), K(row), K(fk_arg));
              } else if (is_zero && !is_self_ref) {
                ret = OB_ERR_NO_REFERENCED_ROW;
                LOG_WARN("parent row is not exist", K(ret), K(fk_arg), K(row));
              } else if (!is_zero && (!is_self_ref || !is_affect_only_one)) {
                ret = OB_ERR_ROW_IS_REFERENCED;
                LOG_WARN("child row is exist", K(ret), K(fk_arg), K(row));
              }
            }
          }
          int close_ret = res.get_result()->close();
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("failed to close", K(close_ret));
            if (OB_SUCCESS == ret) {
              ret = close_ret;
            }
          }
        }
        int reset_ret = op.set_foreign_key_check_exist(false);
        if (OB_SUCCESS != reset_ret) {
          LOG_WARN("failed to reset foreign key cascade", K(reset_ret));
          if (OB_SUCCESS == ret) {
            ret = reset_ret;
          }
        }
        int end_ret = op.end_nested_session();
        if (OB_SUCCESS != end_ret) {
          LOG_WARN("failed to end nested session", K(end_ret));
          if (OB_SUCCESS == ret) {
            ret = end_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::cascade(
    ObTableModifyOp& op, const ObForeignKeyArg& fk_arg, const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row)
{
  static const char* UPDATE_FMT_MYSQL = "update `%.*s`.`%.*s` set %.*s where %.*s";
  static const char* UPDATE_FMT_ORACLE = "update %.*s.%.*s set %.*s where %.*s";
  static const char* DELETE_FMT_MYSQL = "delete from `%.*s`.`%.*s` where %.*s";
  static const char* DELETE_FMT_ORACLE = "delete from %.*s.%.*s where %.*s";
  int ret = OB_SUCCESS;
  char stmt_buf[2048] = {0};
  char where_buf[2048] = {0};
  int64_t stmt_pos = 0;
  int64_t where_pos = 0;
  const ObString& database_name = fk_arg.database_name_;
  const ObString& table_name = fk_arg.table_name_;
  if (old_row.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old row is invalid", K(ret));
  } else if (OB_FAIL(gen_where(op.eval_ctx_,
                 where_buf,
                 sizeof(where_buf),
                 where_pos,
                 fk_arg.columns_,
                 old_row,
                 op.get_obj_print_params()))) {
    if (OB_LIKELY(OB_ERR_NULL_VALUE == ret)) {
      // skip cascade if any column in foreign key is NULL.
      ret = OB_SUCCESS;
      stmt_pos = 0;
    } else {
      LOG_WARN("failed to gen foreign key where", K(ret), K(old_row), K(fk_arg.columns_));
    }
  } else {
    if (!new_row.empty()) {
      // update.
      const char* update_fmt = lib::is_mysql_mode() ? UPDATE_FMT_MYSQL : UPDATE_FMT_ORACLE;
      char set_buf[2048] = {0};
      int64_t set_pos = 0;
      if (OB_FAIL(gen_set(
              op.eval_ctx_, set_buf, sizeof(set_buf), set_pos, fk_arg.columns_, new_row, op.get_obj_print_params()))) {
        LOG_WARN("failed to gen foreign key set", K(ret), K(new_row), K(fk_arg.columns_));
      } else if (OB_FAIL(databuff_printf(stmt_buf,
                     sizeof(stmt_buf),
                     stmt_pos,
                     update_fmt,
                     database_name.length(),
                     database_name.ptr(),
                     table_name.length(),
                     table_name.ptr(),
                     static_cast<int>(set_pos),
                     set_buf,
                     static_cast<int>(where_pos),
                     where_buf))) {
        LOG_WARN("failed to print stmt", K(ret), K(table_name), K(set_buf), K(where_buf));
      } else {
        stmt_buf[stmt_pos++] = 0;
      }
    } else {
      // delete.
      const char* delete_fmt = lib::is_mysql_mode() ? DELETE_FMT_MYSQL : DELETE_FMT_ORACLE;
      if (OB_FAIL(databuff_printf(stmt_buf,
              sizeof(stmt_buf),
              stmt_pos,
              delete_fmt,
              database_name.length(),
              database_name.ptr(),
              table_name.length(),
              table_name.ptr(),
              static_cast<int>(where_pos),
              where_buf))) {
        LOG_WARN("failed to print stmt", K(ret), K(table_name), K(where_buf));
      } else {
        stmt_buf[stmt_pos++] = 0;
      }
    }
  }
  if (OB_SUCC(ret) && stmt_pos > 0) {
    LOG_DEBUG("foreign_key_cascade", "stmt", stmt_buf, K(old_row), K(new_row), K(fk_arg));
    if (OB_FAIL(op.begin_nested_session(fk_arg.is_self_ref_))) {
      LOG_WARN("failed to begin nested session", K(ret));
    } else {
      // must call end_nested_session() if begin_nested_session() success.
      // skip modify_ctx.set_foreign_key_cascade when cascade update and self ref.
      if (!(fk_arg.is_self_ref_ && !new_row.empty()) && OB_FAIL(op.set_foreign_key_cascade(true))) {
        LOG_WARN("failed to set foreign key cascade", K(ret));
      } else if (OB_FAIL(op.execute_write(stmt_buf))) {
        LOG_WARN("failed to execute stmt", K(ret), K(stmt_buf));
      }
      int reset_ret = op.set_foreign_key_cascade(false);
      if (OB_SUCCESS != reset_ret) {
        LOG_WARN("failed to reset foreign key cascade", K(reset_ret));
        if (OB_SUCCESS == ret) {
          ret = reset_ret;
        }
      }
      int end_ret = op.end_nested_session();
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end nested session", K(end_ret));
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
      }
    }
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::gen_set(ObEvalCtx& eval_ctx, char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row, const ObObjPrintParams& print_params)
{
  return gen_column_value(eval_ctx, buf, len, pos, columns, row, ", ", print_params, false);
}

int ObTableModifyOp::ForeignKeyHandle::gen_where(ObEvalCtx& eval_ctx, char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row, const ObObjPrintParams& print_params)
{
  return gen_column_value(eval_ctx, buf, len, pos, columns, row, " and ", print_params, true);
}

int ObTableModifyOp::ForeignKeyHandle::gen_column_value(ObEvalCtx& eval_ctx, char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row, const char* delimiter,
    const ObObjPrintParams& print_params, bool forbid_null)
{
  static const char* COLUMN_FMT_MYSQL = "`%.*s` = ";
  static const char* COLUMN_FMT_ORACLE = "%.*s = ";
  const char* column_fmt = lib::is_mysql_mode() ? COLUMN_FMT_MYSQL : COLUMN_FMT_ORACLE;
  int ret = OB_SUCCESS;
  ObDatum* col_datum = NULL;
  if (OB_ISNULL(buf) || OB_ISNULL(delimiter) || OB_ISNULL(print_params.tz_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), KP(delimiter), K(print_params.tz_info_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    OZ(row.at(columns.at(i).idx_)->eval(eval_ctx, col_datum));
    if (OB_SUCC(ret)) {
      const ObString& col_name = columns.at(i).name_;
      const ObObjMeta& col_obj_meta = row.at(columns.at(i).idx_)->obj_meta_;
      ObObj col_obj;
      if (forbid_null && col_datum->is_null()) {
        ret = OB_ERR_NULL_VALUE;
        // NO LOG.
      } else if (OB_FAIL(databuff_printf(buf, len, pos, column_fmt, col_name.length(), col_name.ptr()))) {
        LOG_WARN("failed to print column name", K(ret), K(col_name));
      } else if (OB_FAIL(col_datum->to_obj(col_obj, col_obj_meta))) {
        LOG_WARN("to_obj failed", K(ret), K(*col_datum), K(col_obj_meta));
      } else if (OB_FAIL(col_obj.print_sql_literal(buf, len, pos, print_params))) {
        LOG_WARN("failed to print column value", K(ret), K(*col_datum), K(col_obj));
      } else if (OB_FAIL(databuff_printf(buf, len, pos, delimiter))) {
        LOG_WARN("failed to print delimiter", K(ret), K(delimiter));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos -= STRLEN(delimiter);
    buf[pos++] = 0;
  }
  return ret;
}

int ObTableModifyOp::ForeignKeyHandle::is_self_ref_row(
    ObEvalCtx& eval_ctx, const ObExprPtrIArray& row, const ObForeignKeyArg& fk_arg, bool& is_self_ref)
{
  int ret = OB_SUCCESS;
  is_self_ref = fk_arg.is_self_ref_;
  ObDatum* name_col = NULL;
  ObDatum* val_col = NULL;
  for (int64_t i = 0; is_self_ref && i < fk_arg.columns_.count(); i++) {
    const int32_t name_idx = fk_arg.columns_.at(i).name_idx_;
    const int32_t val_idx = fk_arg.columns_.at(i).idx_;
    ObExprCmpFuncType cmp_func = row.at(name_idx)->basic_funcs_->null_first_cmp_;
    // TODO qubin.qb: uncomment below block revert the defensive check
    // OZ((cmp_func = (row.at(name_idx)->basic_funcs_->null_first_cmp_))
    //   != row.at(val_idx)->basic_funcs_->null_first_cmp_);
    OZ(row.at(name_idx)->eval(eval_ctx, name_col));
    OZ(row.at(val_idx)->eval(eval_ctx, val_col));
    OX(is_self_ref = (0 == cmp_func(*name_col, *val_col)));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
