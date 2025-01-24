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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_opt_selectivity.h"
#include "sql/rewrite/ob_query_range_define.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "sql/optimizer/ob_access_path_estimation.h"
#include "sql/optimizer/ob_sel_estimator.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{
inline double revise_ndv(double ndv) { return ndv < 1.0 ? 1.0 : ndv; }

void OptSelectivityCtx::init_op_ctx(ObLogicalOperator *child_op)
{
  if (OB_NOT_NULL(child_op)) {
    init_op_ctx(&child_op->get_output_equal_sets(),
                child_op->get_card(),
                &child_op->get_ambient_card());
  } else {
    init_op_ctx(NULL, -1, NULL);
  }
}

int OptSelectivityCtx::get_ambient_card(const uint64_t table_id, double &table_ambient_card) const
{
  int ret = OB_SUCCESS;
  table_ambient_card = -1.0;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_NOT_NULL(get_ambient_card())) {
    uint64_t table_index = get_stmt()->get_table_bit_index(table_id);
    if (OB_UNLIKELY(table_index < 1) ||
        OB_UNLIKELY(table_index >= get_ambient_card()->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table index", K(table_index), K(table_id), KPC(get_stmt()));
    } else {
      table_ambient_card = get_ambient_card()->at(table_index);
    }
  }
  return ret;
}

ObEstCorrelationModel &ObIndependentModel::get_model()
{
  static ObIndependentModel model;
  return model;
}

ObEstCorrelationModel &ObPartialCorrelationModel::get_model()
{
  static ObPartialCorrelationModel model;
  return model;
}

ObEstCorrelationModel &ObFullCorrelationModel::get_model()
{
  static ObFullCorrelationModel model;
  return model;
}

ObEstCorrelationModel &ObEstCorrelationModel::get_correlation_model(ObEstCorrelationType type)
{
  switch (type) {
    case ObEstCorrelationType::INDEPENDENT: return ObIndependentModel::get_model();
    case ObEstCorrelationType::PARTIAL: return ObPartialCorrelationModel::get_model();
    case ObEstCorrelationType::FULL: return ObFullCorrelationModel::get_model();
    default: break;
  }
  return ObPartialCorrelationModel::get_model();
}

double ObIndependentModel::combine_filters_selectivity(ObIArray<double> &selectivities) const
{
  double combine_selectivity = 1.0;
  for (int64_t i = 0; i < selectivities.count(); i ++) {
    combine_selectivity *= selectivities.at(i);
  }
  return combine_selectivity;
}

double ObPartialCorrelationModel::combine_filters_selectivity(ObIArray<double> &selectivities) const
{
  double selectivity = 1.0;
  if (!selectivities.empty()) {
    double exp = 1.0;
    lib::ob_sort(&selectivities.at(0), &selectivities.at(0) + selectivities.count());
    for (int64_t i = 0; i < selectivities.count(); i ++) {
      selectivity *= std::pow(selectivities.at(i), 1 / exp);
      exp *= 2;
    }
  }
  return selectivity;
}

double ObFullCorrelationModel::combine_filters_selectivity(ObIArray<double> &selectivities) const
{
  double combine_selectivity = 1.0;
  for (int64_t i = 0; i < selectivities.count(); i ++) {
    combine_selectivity = std::min(combine_selectivity, selectivities.at(i));
  }
  return combine_selectivity;
}

int OptColumnMeta::assign(const OptColumnMeta &other)
{
  int ret = OB_SUCCESS;
  column_id_ = other.column_id_;
  ndv_ = other.ndv_;
  num_null_ = other.num_null_;
  avg_len_ = other.avg_len_;
  hist_scale_ = other.hist_scale_;
  min_val_ = other.min_val_;
  max_val_ = other.max_val_;
  min_max_inited_ = other.min_max_inited_;
  cg_macro_blk_cnt_ = other.cg_macro_blk_cnt_;
  cg_micro_blk_cnt_ = other.cg_micro_blk_cnt_;
  cg_skip_rate_ = other.cg_skip_rate_;
  base_ndv_ = other.base_ndv_;
  return ret;
}

void OptColumnMeta::init(const uint64_t column_id,
                         const double ndv,
                         const double num_null,
                         const double avg_len,
                         const int64_t cg_macro_blk_cnt /*default 0*/,
                         const int64_t cg_micro_blk_cnt /*default 0*/,
                         const double cg_skip_rate /*default 1.0*/)
{
  column_id_ = column_id;
  ndv_ = ndv;
  num_null_ = num_null;
  avg_len_ = avg_len;
  cg_macro_blk_cnt_ = cg_macro_blk_cnt;
  cg_micro_blk_cnt_ = cg_micro_blk_cnt;
  cg_skip_rate_ = cg_skip_rate;
  base_ndv_ = ndv;
}

int OptTableMeta::assign(const OptTableMeta &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  ref_table_id_ = other.ref_table_id_;
  rows_ = other.rows_;
  stat_type_ = other.stat_type_;
  ds_level_ = other.ds_level_;
  stat_locked_ = other.stat_locked_;
  distinct_rows_ = other.distinct_rows_;
  table_partition_info_ = other.table_partition_info_;
  base_meta_info_ = other.base_meta_info_;
  real_rows_ = other.real_rows_;
  stale_stats_ = other.stale_stats_;
  base_rows_ = other.base_rows_;

  if (OB_FAIL(all_used_parts_.assign(other.all_used_parts_))) {
    LOG_WARN("failed to assign all used parts", K(ret));
  } else if (OB_FAIL(all_used_tablets_.assign(other.all_used_tablets_))) {
    LOG_WARN("failed to assign all used tablets", K(ret));
  } else if (OB_FAIL(column_metas_.assign(other.column_metas_))) {
    LOG_WARN("failed to assign all csata", K(ret));
  } else if (OB_FAIL(pk_ids_.assign(other.pk_ids_))) {
    LOG_WARN("failed to assign pk ids", K(ret));
  } else if (OB_FAIL(stat_parts_.assign(other.stat_parts_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(hist_parts_.assign(other.hist_parts_))) {
    LOG_WARN("failed to assign", K(ret));
  }

  return ret;
}

int OptTableMeta::init(const uint64_t table_id,
                       const uint64_t ref_table_id,
                       const ObTableType table_type,
                       const int64_t rows,
                       const OptTableStatType stat_type,
                       int64_t micro_block_count,
                       ObSqlSchemaGuard &schema_guard,
                       ObIArray<int64_t> &all_used_part_id,
                       common::ObIArray<ObTabletID> &all_used_tablets,
                       ObIArray<uint64_t> &column_ids,
                       common::ObIArray<int64_t> &stat_part_id,
                       common::ObIArray<int64_t> &hist_part_id,
                       const double scale_ratio,
                       const OptSelectivityCtx &ctx,
                       const ObTablePartitionInfo *table_partition_info,
                       const ObTableMetaInfo *base_meta_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;

  //init common member variable
  table_id_ = table_id;
  ref_table_id_ = ref_table_id;
  rows_ = rows;
  stat_type_ = stat_type;
  scale_ratio_ = scale_ratio;
  micro_block_count_ = micro_block_count;
  table_partition_info_ = table_partition_info;
  base_meta_info_ = base_meta_info;
  real_rows_ = -1.0;
  base_rows_ = rows;
  if (OB_FAIL(all_used_parts_.assign(all_used_part_id))) {
    LOG_WARN("failed to assign all used partition ids", K(ret));
  } else if (OB_FAIL(all_used_tablets_.assign(all_used_tablets))) {
    LOG_WARN("failed to assign all used partition ids", K(ret));
  } else if (OB_FAIL(stat_parts_.assign(stat_part_id))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(hist_parts_.assign(hist_part_id))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id_, ref_table_id_, ctx.get_stmt(), table_schema))) {
    LOG_WARN("failed to get table schmea", K(ret), K(ref_table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema", K(ret), K(ref_table_id_));
  } else if (OB_FAIL(column_metas_.prepare_allocate(column_ids.count()))) {
    LOG_WARN("failed to init column metas", K(ret));
  } else {/*do nothing*/}

  // init pkey ids
  const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("failed to get column id", K(ret));
    } else if (column_id < OB_END_RESERVED_COLUMN_ID_NUM) {
      if (table_schema->is_heap_table()) {
        // partition column will add to primary key for heap table
        pk_ids_.reset();
      } else { /* do nothing */ }
      break;
    } else if (OB_FAIL(pk_ids_.push_back(column_id))) {
      LOG_WARN("failed to push back column id", K(ret));
    }
  }
  //init column ndv
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (OB_FAIL(init_column_meta(ctx, column_ids.at(i), column_metas_.at(i)))) {
      LOG_WARN("failed to init column ", K(ret));
    }
  }
  return ret;
}

int OptTableMeta::init_column_meta(const OptSelectivityCtx &ctx,
                                   const uint64_t column_id,
                                   OptColumnMeta &col_meta)
{
  int ret = OB_SUCCESS;
  ObGlobalColumnStat stat;
  bool is_single_pkey = (1 == pk_ids_.count() && pk_ids_.at(0) == column_id) ||
                         column_id == OB_HIDDEN_PK_INCREMENT_COLUMN_ID;
  int64_t global_ndv = 0;
  int64_t num_null = 0;
  if (use_default_stat() || stat_parts_.empty()) {
    col_meta.set_default_meta(rows_);
  } else if (OB_ISNULL(ctx.get_opt_stat_manager()) || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_opt_stat_manager()),
                                    K(ctx.get_session_info()));
  } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(ctx.get_session_info()->get_effective_tenant_id(),
                                                                 ref_table_id_,
                                                                 stat_parts_,
                                                                 column_id,
                                                                 rows_,
                                                                 scale_ratio_,
                                                                 stat))) {
    LOG_WARN("failed to get column stats", K(ret), KPC(this));
  } else if (OB_FAIL(refine_column_stat(stat, rows_, col_meta))) {
    LOG_WARN("failed to refine column stat", K(ret));
  }
  if (OB_SUCC(ret) && is_single_pkey) {
    col_meta.set_ndv(rows_);
    col_meta.set_num_null(0);
  }
  if (OB_SUCC(ret)) {
    if (rows_ < col_meta.get_ndv()) {
      col_meta.set_ndv(rows_);
    }
    if (rows_ < col_meta.get_num_null()) {
      col_meta.set_num_null(rows_);
    }
    col_meta.set_column_id(column_id);
    col_meta.set_avg_len(stat.avglen_val_);
    col_meta.set_cg_macro_blk_cnt(stat.cg_macro_blk_cnt_);
    col_meta.set_cg_micro_blk_cnt(stat.cg_micro_blk_cnt_);
    col_meta.set_cg_skip_rate(stat.cg_skip_rate_);
    col_meta.set_base_ndv(col_meta.get_ndv());
  }
  if (OB_SUCC(ret) && col_meta.get_avg_len() == 0) {
    const ObColumnRefRawExpr *column_expr = NULL;
    if (OB_ISNULL(ctx.get_plan()) ||
        OB_ISNULL(column_expr = ctx.get_plan()->get_column_expr_by_id(
                      table_id_, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ctx.get_plan()), K(table_id_), K(column_id));
    } else {
      col_meta.set_avg_len(ObOptEstCost::get_estimate_width_from_type(column_expr->get_result_type()));
    }
  }
  return ret;
}

int OptTableMeta::add_column_meta_no_dup(const uint64_t column_id,
                                         const OptSelectivityCtx &ctx)
{
  int ret = OB_SUCCESS;
  OptColumnMeta *col_meta = NULL;
  if (NULL != OptTableMeta::get_column_meta(column_id)) {
    /* do nothing */
  } else if (OB_ISNULL(col_meta = column_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for column meta", K(ret));
  } else if (OB_FAIL(init_column_meta(ctx, column_id, *col_meta))) {
    LOG_WARN("failed to init column meta", K(ret));
  }
  return ret;
}

int OptTableMeta::get_increase_rows_ratio(ObOptimizerContext &ctx, double &increase_rows_ratio) const
{
  int ret = OB_SUCCESS;
  increase_rows_ratio = 0.0;
  if (real_rows_ >= 0) {
    // do nothing
  } else if (NULL == table_partition_info_ || NULL == base_meta_info_ ||
             !base_meta_info_->has_opt_stat_ || ctx.use_default_stat()) {
    const_cast<double &>(real_rows_) = rows_;
  } else {
    ObTableMetaInfo table_meta(ref_table_id_);
    table_meta.assign(*base_meta_info_);
    table_meta.table_row_count_ = 0.0;
    table_meta.row_count_ = 0.0;
    if (OB_FAIL(ObAccessPathEstimation::estimate_full_table_rowcount(ctx,
                                                                     *table_partition_info_,
                                                                     table_meta))) {
      LOG_WARN("failed to estimate full table rowcount", K(ret));
    } else {
      const_cast<double &>(real_rows_) = table_meta.table_row_count_;
    }
  }
  if (OB_SUCC(ret) && rows_ > OB_DOUBLE_EPSINON && real_rows_ > rows_) {
    increase_rows_ratio = (real_rows_ - rows_) / rows_;
  }
  return ret;
}

const OptColumnMeta* OptTableMeta::get_column_meta(const uint64_t column_id) const
{
  const OptColumnMeta* column_meta = NULL;
  for (int64_t i = 0; NULL == column_meta && i < column_metas_.count(); ++i) {
    if (column_metas_.at(i).get_column_id() == column_id) {
      column_meta = &column_metas_.at(i);
    }
  }
  return column_meta;
}

OptColumnMeta* OptTableMeta::get_column_meta(const uint64_t column_id)
{
  OptColumnMeta* column_meta = NULL;
  for (int64_t i = 0; NULL == column_meta && i < column_metas_.count(); ++i) {
    if (column_metas_.at(i).get_column_id() == column_id) {
      column_meta = &column_metas_.at(i);
    }
  }
  return column_meta;
}

int OptTableMeta::refine_column_stat(const ObGlobalColumnStat &stat,
                                     double rows,
                                     OptColumnMeta &col_meta)
{
  int ret = OB_SUCCESS;
  if (0 == stat.ndv_val_ && 0 == stat.null_val_) {
    col_meta.set_default_meta(rows);
  } else if (0 == stat.ndv_val_ && stat.null_val_ > 0) {
    col_meta.set_ndv(1);
    col_meta.set_num_null(stat.null_val_);
  } else {
    col_meta.set_ndv(stat.ndv_val_);
    col_meta.set_num_null(stat.null_val_);
  }
  return ret;
}

int OptTableMetas::copy_table_meta_info(const OptTableMeta &src_meta, OptTableMeta *&dst_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_metas_.push_back(src_meta))) {
    LOG_WARN("failed to push back table meta");
  } else {
    dst_meta = &table_metas_.at(table_metas_.count() - 1);
  }
  return ret;
}

int OptTableMetas::copy_table_meta_info(const OptTableMetas &table_metas, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  OptTableMeta *dummy_meta = NULL;
  if (OB_ISNULL(table_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table meta", K(ret), K(table_id));
  } else if (OB_FAIL(copy_table_meta_info(*table_meta, dummy_meta))) {
    LOG_WARN("failed to copy table meta info", K(ret));
  }
  return ret;
}

int OptTableMetas::add_base_table_meta_info(OptSelectivityCtx &ctx,
                                            const uint64_t table_id,
                                            const uint64_t ref_table_id,
                                            const ObTableType table_type,
                                            const int64_t rows,
                                            const int64_t micro_block_count,
                                            ObIArray<int64_t> &all_used_part_id,
                                            ObIArray<ObTabletID> &all_used_tablets,
                                            ObIArray<uint64_t> &column_ids,
                                            const OptTableStatType stat_type,
                                            common::ObIArray<int64_t> &stat_part_id,
                                            common::ObIArray<int64_t> &hist_part_id,
                                            const double scale_ratio,
                                            int64_t last_analyzed,
                                            bool is_stat_locked,
                                            const ObTablePartitionInfo *table_partition_info,
                                            const ObTableMetaInfo *base_meta_info,
                                            bool stale_stats)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = ctx.get_sql_schema_guard();
  OptTableMeta *table_meta = NULL;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null sql schema guard", K(schema_guard));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else if (OB_FAIL(table_meta->init(table_id, ref_table_id, table_type, rows, stat_type, micro_block_count,
                                      *schema_guard, all_used_part_id, all_used_tablets,
                                      column_ids, stat_part_id, hist_part_id, scale_ratio, ctx,
                                      table_partition_info, base_meta_info))) {
    LOG_WARN("failed to init new tstat", K(ret));
  } else {
    table_meta->set_version(last_analyzed);
    table_meta->set_stat_locked(is_stat_locked);
    table_meta->set_stale_stats(stale_stats);
    LOG_TRACE("add base table meta info success", K(*table_meta));
  }
  return ret;
}

// set stmt child is select stmt, not generate table. To mentain meta info for set stmt,
// mock a table for set stmt child. e.g. first child stmt use table id = 1, second child stmt
// use table_id = 2, ...
int OptTableMetas::add_set_child_stmt_meta_info(const ObSelectStmt *parent_stmt,
                                                const ObSelectStmt *child_stmt,
                                                const uint64_t table_id,
                                                const OptTableMetas &child_table_metas,
                                                const OptSelectivityCtx &child_ctx,
                                                const double child_rows)
{
  int ret = OB_SUCCESS;
  OptTableMeta *table_meta = NULL;
  OptColumnMeta *column_meta = NULL;
  ObSEArray<ObRawExpr*, 1> exprs;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObRawExpr *select_expr = NULL;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(parent_stmt), K(child_stmt));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else {
    const double table_rows = child_rows;
    table_meta->set_table_id(table_id);
    table_meta->set_ref_table_id(OB_INVALID_ID);
    table_meta->set_rows(table_rows);
    table_meta->set_base_rows(table_rows);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_select_item_size(); ++i) {
      exprs.reset();
      double ndv = 1;
      double num_null = 0;
      double avg_len = 0;
      if (OB_ISNULL(select_expr = child_stmt->get_select_items().at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null select expr", K(ret));
      } else if (OB_FAIL(select_exprs.push_back(select_expr))) {
        LOG_WARN("Failed to push back select expr", K(ret));
      } else if (select_expr->is_set_op_expr()) {
        const int64_t set_epxr_idx = static_cast<ObSetOpRawExpr *>(select_expr)->get_idx();
        if (OB_FAIL(get_set_stmt_output_statistics(*child_stmt, child_table_metas,
                                                   set_epxr_idx, ndv, num_null, avg_len))) {
          LOG_WARN("failed to get set stmt output statistics", K(ret));
        } else {
          ndv = std::min(ndv, table_rows);
        }
      } else if (OB_FAIL(exprs.push_back(select_expr))) {
        LOG_WARN("Failed to push back column expr", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(child_table_metas,
                                                              child_ctx,
                                                              exprs,
                                                              table_rows,
                                                              ndv))) {
        LOG_WARN("failed to calculate distinct", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(column_meta = table_meta->get_column_metas().alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate place holder for column meta", K(ret));
        } else {
          column_meta->init(OB_APP_MIN_COLUMN_ID + i, revise_ndv(ndv), num_null, avg_len);
          column_meta->set_min_max_inited(true);
        }
      }
    }
    if (OB_SUCC(ret)) {
      double distinct_rows = 0.0;
      if (child_stmt->is_set_stmt()) {
        if (OB_FAIL(get_set_stmt_output_ndv(*child_stmt, child_table_metas, distinct_rows))) {
          LOG_WARN("failed to get set stmt output statistics", K(ret));
        }
      } else {
        if (parent_stmt->is_set_distinct() &&
            OB_FAIL(ObOptSelectivity::calculate_distinct(child_table_metas,
                                                        child_ctx,
                                                        select_exprs,
                                                        table_rows,
                                                        distinct_rows))) {
          LOG_WARN("failed to calculate distinct", K(ret));
        }
      }
      table_meta->set_distinct_rows(distinct_rows);
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed add set table meta info", K(child_table_metas), K(*this));
    }
  }
  return ret;
}

int OptTableMetas::add_generate_table_meta_info(const ObDMLStmt *parent_stmt,
                                                const ObSelectStmt *child_stmt,
                                                const uint64_t table_id,
                                                const OptTableMetas &child_table_metas,
                                                const OptSelectivityCtx &child_ctx,
                                                const double child_rows)
{
  int ret = OB_SUCCESS;
  OptTableMeta *table_meta = NULL;
  OptColumnMeta *column_meta = NULL;
  ObSEArray<ColumnItem, 8> column_items;
  ObSEArray<ObRawExpr*, 1> exprs;
  ObRawExpr *select_expr = NULL;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt) || OB_ISNULL(child_ctx.get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(parent_stmt), K(child_stmt));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else if (OB_FAIL(parent_stmt->get_column_items(table_id, column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    const double table_rows = child_rows < 1.0 ? 1.0 : child_rows;
    double base_table_rows = table_rows;
    bool is_child_single_spj = child_stmt->is_spj() && 1 == child_stmt->get_table_size();
    table_meta->set_table_id(table_id);
    table_meta->set_ref_table_id(OB_INVALID_ID);
    table_meta->set_rows(table_rows);
    if (is_child_single_spj) {
      const TableItem *table = child_stmt->get_table_item(0);
      if (OB_NOT_NULL(table)) {
        base_table_rows = child_table_metas.get_base_rows(table->table_id_);
        table_meta->set_base_rows(base_table_rows);
      }
    } else {
      table_meta->set_base_rows(table_rows);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      const ColumnItem &column_item = column_items.at(i);
      exprs.reset();
      double ndv = 1;
      double num_null = 0;
      double avg_len = 0;
      int64_t idx = column_item.column_id_ - OB_APP_MIN_COLUMN_ID;
      if (OB_UNLIKELY(idx < 0 || idx >= child_stmt->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect column", K(ret), K(column_item), K(child_stmt->get_select_items()));
      } else if (OB_ISNULL(select_expr = child_stmt->get_select_item(idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null select expr", K(ret));
      } else if (select_expr->is_set_op_expr()) {
        const int64_t set_epxr_idx = static_cast<ObSetOpRawExpr *>(select_expr)->get_idx();
        if (OB_FAIL(get_set_stmt_output_statistics(*child_stmt, child_table_metas,
                                                   set_epxr_idx, ndv, num_null, avg_len))) {
          LOG_WARN("failed to get set stmt output statistics", K(ret));
        } else {
          ndv = std::min(ndv, table_rows);
        }
      } else if (OB_FAIL(exprs.push_back(select_expr))) {
        LOG_WARN("Failed to push back column expr", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(child_table_metas,
                                                              child_ctx,
                                                              exprs,
                                                              table_rows,
                                                              ndv))) {
        LOG_WARN("failed to calculate distinct", K(ret));
      }
      if (OB_SUCC(ret)) {
        /*TODO:@yibo num_null*/
        if (OB_ISNULL(column_meta = table_meta->get_column_metas().alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate place holder for column meta", K(ret));
        } else {
          column_meta->init(column_item.column_id_, revise_ndv(ndv), num_null, avg_len);
          column_meta->set_min_max_inited(true);
          ObObj maxobj;
          ObObj minobj;
          maxobj.set_max_value();
          minobj.set_min_value();
          avg_len = 0;
          if (OB_FAIL(ObOptSelectivity::calculate_expr_avg_len(child_table_metas, child_ctx, select_expr, avg_len))) {
            LOG_WARN("failed to get avg len", K(ret));
          } else if (OB_FAIL(ObOptSelectivity::calc_expr_min_max(child_table_metas, child_ctx, select_expr, minobj, maxobj))) {
            LOG_WARN("failed to calc expr min max", KPC(select_expr));
          }

          if (OB_SUCC(ret)) {
            column_meta->set_min_value(minobj);
            column_meta->set_max_value(maxobj);
            column_meta->set_avg_len(avg_len);
          }
          if (OB_SUCC(ret) && is_child_single_spj) {
            double base_ndv = ndv;
            double nns = 1.0;
            if (OB_FAIL(ObOptSelectivity::calc_expr_basic_info(child_table_metas,
                                                               child_ctx,
                                                               select_expr,
                                                               NULL,
                                                               &nns,
                                                               &base_ndv))) {
              LOG_WARN("failed to calculate distinct", K(ret));
            } else {
              nns = ObOptSelectivity::revise_between_0_1(nns);
              base_ndv = revise_ndv(base_ndv);
              column_meta->set_num_null(table_rows * (1 - nns));
              column_meta->set_base_ndv(base_ndv);
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed add generate table meta info", K(child_table_metas), K(*this));
    }
  }
  return ret;
}

int OptTableMetas::add_values_table_meta_info(const ObDMLStmt *stmt,
                                              const uint64_t table_id,
                                              const OptSelectivityCtx &ctx,
                                              ObValuesTableDef *table_def)
{
  int ret = OB_SUCCESS;
  OptTableMeta *table_meta = NULL;
  OptColumnMeta *column_meta = NULL;
  ObSEArray<ColumnItem, 8> column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), KP(stmt), KP(table_def));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else if (OB_FAIL(stmt->get_column_items(table_id, column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    table_meta->set_table_id(table_id);
    table_meta->set_ref_table_id(OB_INVALID_ID);
    table_meta->set_rows(table_def->row_cnt_);
    table_meta->set_base_rows(table_def->row_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      const ColumnItem &column_item = column_items.at(i);
      int64_t idx = column_item.column_id_ - OB_APP_MIN_COLUMN_ID;
      if (OB_UNLIKELY(idx >= table_def->column_ndvs_.count() ||
                      idx >= table_def->column_nnvs_.count()) ||
          OB_ISNULL(column_meta = table_meta->get_column_metas().alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate place holder for column meta", K(ret));
      } else {
        double avg_len = ObOptEstCost::get_estimate_width_from_type(column_item.expr_->get_result_type());
        column_meta->init(column_item.column_id_,
                          revise_ndv(table_def->column_ndvs_.at(idx)),
                          table_def->column_nnvs_.at(idx),
                          avg_len);
      }
    }
  }
  return ret;
}

int OptTableMetas::get_set_stmt_output_statistics(const ObSelectStmt &stmt,
                                                  const OptTableMetas &child_table_metas,
                                                  const int64_t idx,
                                                  double &ndv,
                                                  double &num_null,
                                                  double &avg_len)
{
  int ret = OB_SUCCESS;
  ndv = 0;
  num_null = 0;
  avg_len = 0;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID + idx;
  const OptColumnMeta *column_meta = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_set_query().count(); ++i) {
    if (OB_ISNULL(stmt.get_set_query().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null set query", K(ret));
    } else if (OB_INVALID_ID != stmt.get_set_query().at(i)->get_dblink_id()) {
      // skip
    } else if (OB_ISNULL(column_meta = child_table_metas.get_column_meta_by_table_id(i, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta info", K(ret));
    } else {
      double cur_ndv = column_meta->get_ndv();
      double cur_num_null = ObSelectStmt::SetOperator::UNION == stmt.get_set_op() && !stmt.is_set_distinct() ?
                            column_meta->get_num_null() : std::min(column_meta->get_num_null(), 1.0);
      double cur_avg_len = column_meta->get_avg_len();
      if (0 == i) {
        ndv = cur_ndv;
        num_null = cur_num_null;
        avg_len = cur_avg_len;
      } else {
        ObSelectStmt::SetOperator set_type = stmt.is_recursive_union() ? ObSelectStmt::SetOperator::RECURSIVE : stmt.get_set_op();
        ndv = ObOptSelectivity::get_set_stmt_output_count(ndv, cur_ndv, set_type);
        num_null = ObOptSelectivity::get_set_stmt_output_count(num_null, cur_num_null, set_type);
        if (ObSelectStmt::SetOperator::UNION == set_type) {
          avg_len = std::max(avg_len, cur_avg_len);
        } else if (ObSelectStmt::SetOperator::INTERSECT == set_type) {
          avg_len = std::min(avg_len, cur_avg_len);
        } else if (ObSelectStmt::SetOperator::EXCEPT == set_type) {
          avg_len = std::min(avg_len, cur_avg_len);
        } else if (ObSelectStmt::SetOperator::RECURSIVE == set_type) {
          avg_len = std::max(avg_len, cur_avg_len);
        }
      }
    }
  }
  return ret;
}

int OptTableMetas::get_set_stmt_output_ndv(const ObSelectStmt &stmt,
                                           const OptTableMetas &child_table_metas,
                                           double &ndv)
{
  int ret = OB_SUCCESS;
  ndv = 0;
  const OptTableMeta *table_meta = NULL;
  ObSelectStmt::SetOperator set_type = stmt.is_recursive_union() ? ObSelectStmt::SetOperator::RECURSIVE : stmt.get_set_op();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_set_query().count(); ++i) {
    if (OB_ISNULL(stmt.get_set_query().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null set query", K(ret));
    } else if (OB_INVALID_ID != stmt.get_set_query().at(i)->get_dblink_id()) {
      // skip
    } else if (OB_ISNULL(table_meta = child_table_metas.get_table_meta_by_table_id(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table meta info", K(ret));
    } else if (0 == i) {
      ndv = table_meta->get_distinct_rows();
    } else {
      ndv = ObOptSelectivity::get_set_stmt_output_count(ndv, table_meta->get_distinct_rows(), set_type);
    }
  }
  return ret;
}

const OptTableMeta* OptTableMetas::get_table_meta_by_table_id(const uint64_t table_id) const
{
  const OptTableMeta *table_meta = NULL;
  for (int64_t i = 0; NULL == table_meta && i < table_metas_.count(); ++i) {
    if (table_id == table_metas_.at(i).get_table_id()) {
      table_meta = &table_metas_.at(i);
    }
  }
  return table_meta;
}

OptTableMeta* OptTableMetas::get_table_meta_by_table_id(const uint64_t table_id)
{
  OptTableMeta *table_meta = NULL;
  for (int64_t i = 0; NULL == table_meta && i < table_metas_.count(); ++i) {
    if (table_id == table_metas_.at(i).get_table_id()) {
      table_meta = &table_metas_.at(i);
    }
  }
  return table_meta;
}

const OptColumnMeta* OptTableMetas::get_column_meta_by_table_id(const uint64_t table_id,
                                                                const uint64_t column_id) const
{
  const OptColumnMeta *column_meta = NULL;
  const OptTableMeta *table_meta = get_table_meta_by_table_id(table_id);
  if (OB_NOT_NULL(table_meta)) {
    column_meta = table_meta->get_column_meta(column_id);
  }
  return column_meta;
}

const OptDynamicExprMeta* OptTableMetas::get_dynamic_expr_meta(const ObRawExpr *expr) const
{
  const OptDynamicExprMeta* dynamic_expr_meta = NULL;
  for (int64_t i = 0; NULL == dynamic_expr_meta && i < dynamic_expr_metas_.count(); ++i) {
    if (expr == dynamic_expr_metas_.at(i).get_expr()) {
      dynamic_expr_meta = &dynamic_expr_metas_.at(i);
    }
  }
  return dynamic_expr_meta;
}

double OptTableMetas::get_rows(const uint64_t table_id) const
{
  const OptTableMeta *table_meta = get_table_meta_by_table_id(table_id);
  double rows = 1.0;
  if (OB_NOT_NULL(table_meta)) {
    rows = table_meta->get_rows();
  }
  return rows;
}

double OptTableMetas::get_base_rows(const uint64_t table_id) const
{
  const OptTableMeta *table_meta = get_table_meta_by_table_id(table_id);
  double rows = 1.0;
  if (OB_NOT_NULL(table_meta)) {
    rows = table_meta->get_base_rows();
  }
  return rows;
}

int ObOptSelectivity::calculate_conditional_selectivity(const OptTableMetas &table_metas,
                                                        const OptSelectivityCtx &ctx,
                                                        common::ObIArray<ObRawExpr *> &total_filters,
                                                        common::ObIArray<ObRawExpr *> &append_filters,
                                                        double &total_sel,
                                                        double &conditional_sel,
                                                        ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  double new_sel = 1.0;
  if (OB_FAIL(append(total_filters, append_filters))) {
    LOG_WARN("failed to append filters", K(ret));
  } else if (total_sel > OB_DOUBLE_EPSINON) {
    if (OB_FAIL(calculate_selectivity(table_metas,
                                      ctx,
                                      total_filters,
                                      new_sel,
                                      all_predicate_sel))) {
      LOG_WARN("failed to calculate selectivity", K(total_filters), K(ret));
    } else {
      new_sel = std::min(new_sel, total_sel);
      conditional_sel = new_sel / total_sel;
      total_sel = new_sel;
    }
  } else if (OB_FAIL(calculate_selectivity(table_metas,
                                           ctx,
                                           append_filters,
                                           conditional_sel,
                                           all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(append_filters), K(ret));
  } else {
    total_sel *= conditional_sel;
  }
  return ret;
}

int ObOptSelectivity::calculate_selectivity(const OptTableMetas &table_metas,
                                            const OptSelectivityCtx &ctx,
                                            ObIArray<ObSelEstimator *> &sel_estimators,
                                            double &selectivity,
                                            common::ObIArray<ObExprSelPair> &all_predicate_sel,
                                            bool record_range_sel)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  ObSEArray<double, 4> selectivities;
  ObSEArray<const ObRawExpr *, 4> eigen_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_estimators.count(); ++i) {
    ObSelEstimator *estimator = sel_estimators.at(i);
    double tmp_selectivity = 0.0;
    if (OB_ISNULL(sel_estimators.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("estimator is null", K(ret), K(sel_estimators));
    } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, tmp_selectivity, all_predicate_sel))) {
      LOG_WARN("failed to get sel", K(ret), KPC(estimator));
    } else {
      tmp_selectivity = revise_between_0_1(tmp_selectivity);
    }
    if (OB_SUCC(ret) && record_range_sel &&
        ObSelEstType::COLUMN_RANGE == estimator->get_type()) {
      ObRangeSelEstimator *range_estimator = static_cast<ObRangeSelEstimator *>(estimator);
      if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel,
                                          ObExprSelPair(range_estimator->get_column_expr(), tmp_selectivity, true)))) {
        LOG_WARN("failed to add selectivity to plan", K(ret), KPC(range_estimator), K(tmp_selectivity));
      }
    }

    // Use the minimum selectivity from estimators with the same eigen expression
    if (OB_SUCC(ret)) {
      int64_t idx = -1;
      const ObRawExpr *eigen_expr = estimator->get_eigen_expr();
      if (NULL == eigen_expr || !ObOptimizerUtil::find_equal_expr(eigen_exprs, eigen_expr, idx)) {
        if (OB_FAIL(eigen_exprs.push_back(eigen_expr)) ||
            OB_FAIL(selectivities.push_back(tmp_selectivity))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (OB_UNLIKELY(idx < 0 || idx >= selectivities.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", K(idx), K(selectivities), K(eigen_exprs));
      } else {
        selectivities.at(idx) = std::min(tmp_selectivity, selectivities.at(idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    selectivity = ctx.get_correlation_model().combine_filters_selectivity(selectivities);
  }
  LOG_DEBUG("calculate predicates selectivity", K(selectivity), K(selectivities), K(eigen_exprs), K(sel_estimators));
  return ret;
}

int ObOptSelectivity::calculate_selectivity(const OptTableMetas &table_metas,
                                            const OptSelectivityCtx &ctx,
                                            const ObIArray<ObRawExpr*> &predicates,
                                            double &selectivity,
                                            ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  ObSEArray<ObSelEstimator *, 4> sel_estimators;
  ObSEArray<double, 4> selectivities;
  ObSelEstimatorFactory factory;
  if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    factory.get_allocator().set_tenant_id(ctx.get_session_info()->get_effective_tenant_id());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    const ObRawExpr *qual = predicates.at(i);
    ObSelEstimator *estimator = NULL;
    double single_sel = false;
    if (OB_FAIL(factory.create_estimator(ctx, qual, estimator))) {
      LOG_WARN("failed to create estimator", KPC(qual));
    } else if (OB_FAIL(ObSelEstimator::append_estimators(sel_estimators, estimator))) {
      LOG_WARN("failed to append estimators", KPC(qual));
    } else if (ObOptimizerUtil::find_item(all_predicate_sel, ObExprSelPair(qual, 0))) {
      // do nothing
    } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, single_sel, all_predicate_sel))) {
      LOG_WARN("failed to calculate one qual selectivity", KPC(estimator), K(qual), K(ret));
    } else if (FALSE_IT(single_sel = revise_between_0_1(single_sel))) {
      // never reach
    } else if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel, ObExprSelPair(qual, single_sel)))) {
      LOG_WARN("fail ed to add selectivity to plan", K(ret), K(qual), K(selectivity));
    } else {
      // We remember each predicate's selectivity in the plan so that we can reorder them
      // in the vector of filters according to their selectivity.
      LOG_PRINT_EXPR(TRACE, "calculate one qual selectivity", *qual, K(single_sel));
    }
  }
  if (FAILEDx(calculate_selectivity(table_metas, ctx, sel_estimators, selectivity, all_predicate_sel, true))) {
    LOG_WARN("failed to calculate estimator selectivity", K(ret), K(selectivities), K(sel_estimators));
  }
  return ret;
}

int ObOptSelectivity::calculate_join_selectivity(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObIArray<ObRawExpr*> &predicates,
                                                 double &selectivity,
                                                 ObIArray<ObExprSelPair> &all_predicate_sel,
                                                 bool is_outerjoin_filter)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  ObSEArray<ObSelEstimator *, 4> sel_estimators;
  ObSelEstimatorFactory factory;
  double reliable_sel_upper_bound = 1.0;
  double default_complex_sel = 1.0;
  bool is_complex_join = !is_outerjoin_filter &&
                        (INNER_JOIN == ctx.get_assumption_type() ||
                         IS_OUTER_JOIN(ctx.get_assumption_type()));
  bool cnt_complex_qual = false;
  if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    factory.get_allocator().set_tenant_id(ctx.get_session_info()->get_effective_tenant_id());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    const ObRawExpr *qual = predicates.at(i);
    ObSelEstimator *estimator = NULL;
    double single_sel = false;
    if (OB_FAIL(factory.create_estimator(ctx, qual, estimator))) {
      LOG_WARN("failed to create estimator", KPC(qual));
    } else if (OB_FAIL(ObSelEstimator::append_estimators(sel_estimators, estimator))) {
      LOG_WARN("failed to append estimators", KPC(qual));
    } else if (ObOptimizerUtil::find_item(all_predicate_sel, ObExprSelPair(qual, 0))) {
      // do nothing
    } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, single_sel, all_predicate_sel))) {
      LOG_WARN("failed to calculate one qual selectivity", KPC(estimator), K(qual), K(ret));
    } else if (FALSE_IT(single_sel = revise_between_0_1(single_sel))) {
      // never reach
    } else if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel, ObExprSelPair(qual, single_sel)))) {
      LOG_WARN("fail ed to add selectivity to plan", K(ret), K(qual), K(selectivity));
    } else {
      // We remember each predicate's selectivity in the plan so that we can reorder them
      // in the vector of filters according to their selectivity.
      LOG_PRINT_EXPR(TRACE, "calculate one qual selectivity", *qual, K(single_sel));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_estimators.count(); ++i) {
    ObSelEstimator *estimator = sel_estimators.at(i);
    double tmp_selectivity = 0.0;
    if (OB_ISNULL(sel_estimators.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("estimator is null", K(ret), K(sel_estimators));
    } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, tmp_selectivity, all_predicate_sel))) {
      LOG_WARN("failed to get sel", K(ret), KPC(estimator));
    } else {
      tmp_selectivity = revise_between_0_1(tmp_selectivity);
      selectivity *= tmp_selectivity;
      if (estimator->is_complex_join_qual()) {
        cnt_complex_qual = true;
      } else {
        reliable_sel_upper_bound = std::min(reliable_sel_upper_bound, tmp_selectivity);
      }
    }
  }

  /**
   * For complex inner/outer join (which has at least two kinds of quals or one complex join qual),
   * we assume that each row of the small table matches at least one row of the large table.
  */
  if (OB_SUCC(ret) && ctx.check_opt_compat_version(COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0,
                                                   COMPAT_VERSION_4_3_5)) {
    is_complex_join &= sel_estimators.count() > 1 || cnt_complex_qual;
    if (is_complex_join) {
      if (LEFT_SEMI_JOIN == ctx.get_join_type()) {
        default_complex_sel = ctx.get_left_row_count() > 1.0 ?
                              std::min(1.0, ctx.get_right_row_count() / ctx.get_left_row_count()) :
                              1.0;
      } else if (RIGHT_SEMI_JOIN == ctx.get_join_type()) {
        default_complex_sel = ctx.get_right_row_count() > 1.0 ?
                              std::min(1.0, ctx.get_left_row_count() / ctx.get_right_row_count()) :
                              1.0;
      } else {
        double max_rowcnt = MAX(ctx.get_left_row_count(), ctx.get_right_row_count());
        default_complex_sel = max_rowcnt > 1.0 ? 1.0 / max_rowcnt : 1.0;
      }
      /**
       * If the join has some normal quals, the selectivity could not be greater than the minimal one
       * e.g. For join condition `A = B and M = N and X like Y`,
       *      join_selectivity should be less than `A = B` and 'M = N',
       *      but could be greater than `X like Y`.
       *      Because we assume that the selectivity of single equal quals is reliable.
      */
      default_complex_sel = std::min(default_complex_sel, reliable_sel_upper_bound);
      selectivity = std::max(selectivity, default_complex_sel);
    }
  }
  LOG_TRACE("succeed to calculate join selectivity",
      K(selectivity), K(is_complex_join), K(default_complex_sel), K(reliable_sel_upper_bound), K(ctx));
  return ret;
}

//try to calc complex predicate selectivity by dynamic sampling
int ObOptSelectivity::calc_complex_predicates_selectivity_by_ds(const OptTableMetas &table_metas,
                                                                const OptSelectivityCtx &ctx,
                                                                const ObIArray<ObRawExpr*> &predicates,
                                                                ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  ObSEArray<OptSelectivityDSParam, 4> ds_params;
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    if (OB_FAIL(resursive_extract_valid_predicate_for_ds(table_metas, ctx, predicates.at(i), ds_params))) {
      LOG_WARN("failed to resursive extract valid predicate for ds", K(ret));
    } else {/*do nothing*/}
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_params.count(); ++i) {
    if (OB_FAIL(calc_selectivity_by_dynamic_sampling(ctx, ds_params.at(i), all_predicate_sel))) {
      LOG_WARN("failed to calc selectivity by dynamic sampling", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptSelectivity::calc_selectivity_by_dynamic_sampling(const OptSelectivityCtx &ctx,
                                                           const OptSelectivityDSParam &ds_param,
                                                           ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to calc selectivity by dynamic sampling", K(ds_param));
  OPT_TRACE("begin to process filter dynamic sampling estimation");
  ObDSTableParam ds_table_param;
  ObSEArray<ObDSResultItem, 4> ds_result_items;
  bool specify_ds = false;
  if (OB_FAIL(ObDynamicSamplingUtils::get_ds_table_param(const_cast<ObOptimizerContext &>(ctx.get_opt_ctx()),
                                                         ctx.get_plan(),
                                                         ds_param.table_meta_,
                                                         ds_table_param,
                                                         specify_ds))) {
    LOG_WARN("failed to get ds table param", K(ret), K(ds_table_param));
  } else if (!ds_table_param.is_valid()) {
    //do nothing
  } else if (OB_FAIL(add_ds_result_items(ds_param.quals_,
                                         ds_param.table_meta_->get_ref_table_id(),
                                         ds_result_items))) {
    LOG_WARN("failed to init ds result items", K(ret));
  } else {
    ObArenaAllocator allocator("ObOpTableDS", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_session_info()->get_effective_tenant_id());
    ObDynamicSampling dynamic_sampling(const_cast<ObOptimizerContext &>(ctx.get_opt_ctx()), allocator);
    int64_t start_time = ObTimeUtility::current_time();
    bool throw_ds_error = false;
    if (OB_FAIL(dynamic_sampling.estimate_table_rowcount(ds_table_param, ds_result_items, throw_ds_error))) {
      if (!throw_ds_error) {
        LOG_WARN("failed to estimate filter rowcount caused by some reason, please check!!!", K(ret),
                K(start_time), K(ObTimeUtility::current_time() - start_time), K(ds_table_param),
                K(ctx.get_session_info()->get_current_query_string()));
        if (OB_FAIL(ObDynamicSamplingUtils::add_failed_ds_table_list(ds_param.table_meta_->get_ref_table_id(),
                                                                     ds_param.table_meta_->get_all_used_parts(),
                                                                     const_cast<ObOptimizerContext &>(ctx.get_opt_ctx()).get_failed_ds_tab_list()))) {
          LOG_WARN("failed to add failed ds table list", K(ret));
        }
      } else {
        LOG_WARN("failed to dynamic sampling", K(ret), K(start_time), K(ds_table_param));
      }
    } else if (OB_FAIL(add_ds_result_into_selectivity(ds_result_items,
                                                      ds_param.table_meta_->get_ref_table_id(),
                                                      all_predicate_sel))) {
      LOG_WARN("failed to add ds result into selectivity", K(ret));
    } else {
      const_cast<OptTableMeta *>(ds_param.table_meta_)->set_ds_level(ds_table_param.ds_level_);
    }
  }
  OPT_TRACE("end to process filter dynamic sampling estimation");
  return ret;
}

int ObOptSelectivity::add_ds_result_into_selectivity(const ObIArray<ObDSResultItem> &ds_result_items,
                                                     const uint64_t ref_table_id,
                                                     ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObDSResultItem *basic_item = NULL;
  if (OB_ISNULL(basic_item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_BASIC_STAT,
                                                                        ref_table_id,
                                                                        ds_result_items)) ||
      OB_ISNULL(basic_item->stat_handle_.stat_) ||
      OB_UNLIKELY(basic_item->stat_handle_.stat_->get_sample_block_ratio() <= 0 ||
                  basic_item->stat_handle_.stat_->get_sample_block_ratio() > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(basic_item), K(ds_result_items));
  } else if (basic_item->stat_handle_.stat_->get_rowcount() == 0 &&
             basic_item->stat_handle_.stat_->get_sample_block_ratio() != 100.0) {
    //do nothing
  } else {
    int64_t rowcount = basic_item->stat_handle_.stat_->get_rowcount();
    rowcount = rowcount < 1 ? 1 : rowcount;
    for (int64_t i = 0; OB_SUCC(ret) && i < ds_result_items.count(); ++i) {
      if (ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_OUTPUT_STAT) {
        if (OB_ISNULL(ds_result_items.at(i).stat_handle_.stat_) ||
            OB_UNLIKELY(ds_result_items.at(i).exprs_.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(basic_item), K(ds_result_items));
        } else {
          int64_t filter_row_count = ds_result_items.at(i).stat_handle_.stat_->get_rowcount();
          double sample_ratio = ds_result_items.at(i).stat_handle_.stat_->get_sample_block_ratio();
          filter_row_count = filter_row_count != 0 ? filter_row_count : static_cast<int64_t>(100.0 / sample_ratio);
          double selectivity = 1.0 * filter_row_count / rowcount;
          if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel, ObExprSelPair(ds_result_items.at(i).exprs_.at(0), selectivity)))) {
            LOG_WARN("failed to add selectivity to plan", K(ret), K(ds_result_items.at(i).exprs_.at(0)), K(selectivity));
          } else {
            LOG_TRACE("Succeed to add ds result into selectivity", K(ds_result_items.at(i)), K(selectivity), K(rowcount));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptSelectivity::add_ds_result_items(const ObIArray<ObRawExpr*> &quals,
                                          const uint64_t ref_table_id,
                                          ObIArray<ObDSResultItem> &ds_result_items)
{
  int ret = OB_SUCCESS;
  //add rowcount
  ObDSResultItem basic_item(ObDSResultItemType::OB_DS_BASIC_STAT, ref_table_id);
  if (OB_FAIL(ds_result_items.push_back(basic_item))) {
    LOG_WARN("failed to push back", K(ret));
  }
  //add filter
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObDSResultItem tmp_item(ObDSResultItemType::OB_DS_OUTPUT_STAT, ref_table_id);
    if (OB_FAIL(tmp_item.exprs_.push_back(quals.at(i)))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(ds_result_items.push_back(tmp_item))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::resursive_extract_valid_predicate_for_ds(const OptTableMetas &table_metas,
                                                               const OptSelectivityCtx &ctx,
                                                               const ObRawExpr *qual,
                                                               ObIArray<OptSelectivityDSParam> &ds_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (qual->get_relation_ids().num_members() == 1) {//single table filter
    if (qual->has_flag(CNT_DYNAMIC_PARAM) ||
        qual->has_flag(CNT_SUB_QUERY)) {//can't do dynamic sampling
      //do nothing
    } else if (qual->has_flag(CNT_AGG) ||
               qual->is_const_expr() ||
               qual->is_column_ref_expr() ||
               T_OP_EQ == qual->get_expr_type() ||
               T_OP_NSEQ == qual->get_expr_type() ||
               T_OP_IN == qual->get_expr_type() ||
               T_OP_NOT_IN == qual->get_expr_type() ||
               T_OP_IS == qual->get_expr_type() ||
               T_OP_IS_NOT == qual->get_expr_type() ||
               IS_RANGE_CMP_OP(qual->get_expr_type()) ||
               T_OP_BTW == qual->get_expr_type() ||
               T_OP_NOT_BTW == qual->get_expr_type() ||
               T_OP_NE == qual->get_expr_type()) {
      //here we can try use selectivity calc formula by opt stats, and we don't use dynmaic sampling.
    } else if (T_OP_NOT == qual->get_expr_type() ||
               T_OP_AND == qual->get_expr_type() ||
               T_OP_OR == qual->get_expr_type()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < qual->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(resursive_extract_valid_predicate_for_ds(table_metas,
                                                                        ctx,
                                                                        qual->get_param_expr(i),
                                                                        ds_params)))) {
          LOG_WARN("failed to resursive extract valid predicate for ds", K(ret));
        }
      }
    } else if (T_OP_LIKE == qual->get_expr_type()) {
      bool can_calc_sel = false;
      double selectivity = 1.0;
      if (OB_FAIL(ObLikeSelEstimator::can_calc_like_sel(ctx, *qual, can_calc_sel))) {
        LOG_WARN("failed to get like selectivity", K(ret));
      } else if (can_calc_sel) {
        //do nothing
      } else if (OB_FAIL(add_valid_ds_qual(qual, table_metas, ds_params))) {
        LOG_WARN("failed to add valid ds qual", K(ret));
      }
    //filter can't use selectivity calc formula, such as :lnnvl, regexp, not like and so on.
    } else if (OB_FAIL(add_valid_ds_qual(qual, table_metas, ds_params))) {
      LOG_WARN("failed to add valid ds qual", K(ret));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObOptSelectivity::add_valid_ds_qual(const ObRawExpr *qual,
                                        const OptTableMetas &table_metas,
                                        ObIArray<OptSelectivityDSParam> &ds_params)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> table_ids;
  ObSEArray<ObRawExpr *, 1> quals;
  bool no_use = false;
  if (OB_FAIL(quals.push_back(const_cast<ObRawExpr*>(qual)))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(ObDynamicSamplingUtils::check_ds_can_use_filters(quals, no_use))) {
    LOG_WARN("failed to check ds can use filters", K(ret));
  } else if (no_use) {
    //do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(qual, table_ids))) {
    LOG_WARN("failed to extract table ids", K(ret));
  } else if (OB_UNLIKELY(table_ids.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(table_ids), KPC(qual));
  } else {
    const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_ids.at(0));
    if (OB_ISNULL(table_meta) || OB_INVALID_ID == table_meta->get_ref_table_id() ||
        !table_meta->use_opt_stat()) {
      // do nothing
    } else {
      bool found_it = false;
      for (int64_t i = 0; OB_SUCC(ret) && !found_it && i < ds_params.count(); ++i) {
        if (ds_params.at(i).table_meta_ == table_meta) {
          found_it = true;
          if (OB_FAIL(add_var_to_array_no_dup(ds_params.at(i).quals_, const_cast<ObRawExpr*>(qual)))) {
            LOG_WARN("failed to add var to array no dup", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && !found_it) {
        OptSelectivityDSParam ds_param;
        ds_param.table_meta_ = table_meta;
        if (OB_FAIL(ds_param.quals_.push_back(const_cast<ObRawExpr*>(qual)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(ds_params.push_back(ds_param))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      LOG_TRACE("succeed to add valid ds qual", K(ds_params));
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_qual_selectivity(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObRawExpr &qual,
                                                 double &selectivity,
                                                 ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  ObSelEstimatorFactory factory;
  ObSelEstimator *estimator = NULL;
  if (OB_FAIL(factory.create_estimator(ctx, &qual, estimator))) {
    LOG_WARN("failed to create estimator", K(qual));
  } else if (OB_FAIL(estimator->get_sel(table_metas, ctx, selectivity, all_predicate_sel))) {
    LOG_WARN("failed to calculate one qual selectivity", KPC(estimator), K(qual), K(ret));
  } else if (FALSE_IT(selectivity = revise_between_0_1(selectivity))) {
    // never reach
  } else if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel, ObExprSelPair(&qual, selectivity)))) {
    LOG_WARN("fail ed to add selectivity to plan", K(ret), K(qual), K(selectivity));
  } else {
    // We remember each predicate's selectivity in the plan so that we can reorder them
    // in the vector of filters according to their selectivity.
    LOG_PRINT_EXPR(TRACE, "calculate one qual selectivity", qual, K(selectivity));
  }
  return ret;
}

int ObOptSelectivity::update_table_meta_info(const OptTableMetas &base_table_metas,
                                             OptTableMetas &update_table_metas,
                                             const OptSelectivityCtx &ctx,
                                             const uint64_t table_id,
                                             double filtered_rows,
                                             const common::ObIArray<ObRawExpr*> &quals,
                                             common::ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const OptTableMeta *base_table_meta = base_table_metas.get_table_meta_by_table_id(table_id);
  OptTableMeta *table_meta = NULL;
  const ObLogPlan *log_plan = NULL;
  ObSEArray<OptSelInfo, 8> column_sel_infos;
  filtered_rows = filtered_rows < 1.0 ? 1.0 : filtered_rows;
  if (OB_ISNULL(base_table_meta) || OB_ISNULL(log_plan = ctx.get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(base_table_meta), K(log_plan));
  } else if (OB_FAIL(update_table_metas.copy_table_meta_info(*base_table_meta, table_meta))) {
    LOG_WARN("failed to copy table meta info", K(ret));
  } else {
    double origin_rows = table_meta->get_rows();
    table_meta->set_rows(filtered_rows);
    table_meta->clear_base_table_info();
    if (filtered_rows >= origin_rows) {
      // only update table rows
    } else if (OB_FAIL(!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                     COMPAT_VERSION_4_3_3) ?
                       classify_quals_deprecated(ctx, quals, all_predicate_sel, column_sel_infos) :
                       classify_quals(base_table_metas, ctx, quals, all_predicate_sel, column_sel_infos))) {
      LOG_WARN("failed to classify quals", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_meta->get_column_metas().count(); ++i) {
        OptColumnMeta &column_meta = table_meta->get_column_metas().at(i);
        OptSelInfo *sel_info = NULL;
        for (int64_t j = 0; NULL == sel_info && j < column_sel_infos.count(); ++j) {
          if (column_sel_infos.at(j).column_id_ == column_meta.get_column_id()) {
            sel_info = &column_sel_infos.at(j);
          }
        }
        /**
         * ndv的缩放分两步
         * 第一步：
         *   1. 如果存在某一列上的非复杂谓词，直接使用非复杂谓词计算该列第一步的ndv和rows
         *   2. 如果某一列只有复杂谓词，则直接使用缩放公式计算该列第一步的ndv和rows
         * 第二步：
         *   使用第一步得到的ndv和rows作为column的原始ndv和rows，再基于所有过滤谓词过滤后的行数，
         *   使用缩放公式缩放列的ndv。
         */
        double origin_ndv = column_meta.get_ndv();
        double step1_ndv = column_meta.get_ndv();
        double step2_ndv = column_meta.get_ndv();
        double step1_row = origin_rows;
        double null_num = column_meta.get_num_null();
        double hist_scale = -1;
        if (null_num > origin_rows - origin_ndv) {
          null_num = std::max(origin_rows - origin_ndv, 0.0);
        }
        double nns = origin_rows <= OB_DOUBLE_EPSINON ? 1.0 : 1 - revise_between_0_1(null_num / origin_rows);
        bool null_reject = false;
        // update null number
        if (null_num > 0) {
          const ObColumnRefRawExpr *column_expr = log_plan->get_column_expr_by_id(
                  table_meta->get_table_id(), column_meta.get_column_id());
          if (OB_ISNULL(column_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null expr", K(ret), K(column_meta.get_column_id()));
          } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(quals,
                                                                         column_expr,
                                                                         null_reject))) {
            LOG_WARN("failed to check has null reject condition", K(ret));
          } else if (null_reject) {
            null_num = 0;
          } else {
            null_num = null_num * filtered_rows / origin_rows;
          }
        }
        // step 1
        if (OB_NOT_NULL(sel_info)) {
          if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                            COMPAT_VERSION_4_3_3)) {
            step1_row *= sel_info->selectivity_;
            hist_scale = sel_info->selectivity_;
            if (sel_info->equal_count_ > 0) {
              step1_ndv = sel_info->equal_count_;
            } else if (sel_info->has_range_exprs_) {
              step1_ndv *= sel_info->range_selectivity_;
            } else {
              step1_ndv = scale_distinct(step1_row, origin_rows, column_meta.get_ndv());
            }
          } else {
            double step1_sel = sel_info->selectivity_;
            double direct_ndv_sel = 1.0;
            if (origin_rows * step1_sel < filtered_rows &&
                origin_rows > OB_DOUBLE_EPSINON) {
              step1_sel = filtered_rows / origin_rows;
            }
            if (null_reject) {
              if (step1_sel <= nns && nns > OB_DOUBLE_EPSINON) {
                direct_ndv_sel = step1_sel / nns;
              } else {
                direct_ndv_sel = 1.0;
              }
            } else {
              // complex quals, the selectivity might be default
              // do not handle null
              direct_ndv_sel = step1_sel;
            }
            step1_row *= step1_sel;
            hist_scale = step1_sel;
            if (sel_info->equal_count_ > 0) {
              step1_ndv = sel_info->equal_count_;
            } else {
              step1_ndv *= direct_ndv_sel;
            }
          }
        }
        // step 2
        if (filtered_rows < step1_row) {
          step2_ndv = scale_distinct(filtered_rows, step1_row, step1_ndv);
        } else {
          step2_ndv = step1_ndv;
        }
        // set new column meta
        if (OB_SUCC(ret)) {
          column_meta.set_ndv(revise_ndv(step2_ndv));
          column_meta.set_num_null(null_num);
          column_meta.set_hist_scale(hist_scale);
        }

        if (OB_SUCC(ret) && OB_NOT_NULL(sel_info)) {
          if (sel_info->max_ < sel_info->min_ ||
              sel_info->max_ < column_meta.get_min_value() ||
              sel_info->min_ > column_meta.get_max_value()) {
            // invalid min max
            column_meta.get_min_value().set_min_value();
            column_meta.get_max_value().set_max_value();
          } else {
            if (!sel_info->min_.is_null() && sel_info->min_ > column_meta.get_min_value()) {
              column_meta.set_min_value(sel_info->min_);
            }
            if (!sel_info->max_.is_null() && sel_info->max_ < column_meta.get_max_value()) {
              column_meta.set_max_value(sel_info->max_);
            }
          }
        }
      }
    }
  }
  LOG_TRACE("show table meta after update", KPC(table_meta));
  return ret;
}

/**
 * 计算equal join condition的左右表选择率
 * left_selectivity = right_ndv / left_ndv
 * right_selectivity = left_ndv / right_ndv
 * 如果left_rows > 0表示需要缩放左表的NDV
 * 如果right_rows > 0表示需要缩放右表的NDV
 */
int ObOptSelectivity::calc_sel_for_equal_join_cond(const OptTableMetas &table_metas,
                                                  const OptSelectivityCtx &ctx,
                                                  const ObIArray<ObRawExpr *>& conds,
                                                  const ObRelIds &left_ids,
                                                  double &left_selectivity,
                                                  double &right_selectivity)
{
  int ret = OB_SUCCESS;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double tmp_ndv = 1.0;
  double rows = 1.0;
  const ObRawExpr *left = NULL;
  const ObRawExpr *right = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    const ObRawExpr *expr = conds.at(i);
    if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->has_flag(IS_JOIN_COND))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected condition", KPC(expr), K(i), K(ret));
    } else if (2 != expr->get_param_count() ||
               OB_ISNULL(left = expr->get_param_expr(0)) ||
               OB_ISNULL(right = expr->get_param_expr(1))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left, left)) ||
               OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right, right))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else {
      if (!left->get_relation_ids().is_subset(left_ids)) {
        std::swap(left, right);
      }
      if (!left->is_column_ref_expr()) {
        left_ndv *= 1 / DEFAULT_EQ_SEL;
      } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *left, &tmp_ndv, NULL, NULL, &rows))) {
        LOG_WARN("failed to get column basic info", K(ret));
      } else {
        if (ctx.get_row_count_1() > 0.0 && ctx.get_row_count_1() < rows) {
          tmp_ndv = scale_distinct(ctx.get_row_count_1(), rows, tmp_ndv);
        }
        left_ndv *= tmp_ndv;
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (!right->is_column_ref_expr()) {
        right_ndv *= 1 / DEFAULT_EQ_SEL;
      } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *right, &tmp_ndv, NULL, NULL, &rows))) {
        LOG_WARN("failed to get column basic info", K(ret));
      } else {
        if (ctx.get_row_count_2() > 0.0 && ctx.get_row_count_2() < rows) {
          tmp_ndv = scale_distinct(ctx.get_row_count_2(), rows, tmp_ndv);
        }
        right_ndv *= tmp_ndv;
      }
    }
  }
  if (OB_SUCC(ret)) {
    left_selectivity = right_ndv / left_ndv;
    right_selectivity = left_ndv / right_ndv;
    left_selectivity = revise_between_0_1(left_selectivity);
    right_selectivity = revise_between_0_1(right_selectivity);
  }
  return ret;
}

int ObOptSelectivity::get_column_range_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObColumnRefRawExpr &col_expr,
                                           const ObRawExpr &qual,
                                           const bool need_out_of_bounds,
                                           double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> quals;
  if (OB_FAIL(quals.push_back(const_cast<ObRawExpr *>(&qual)))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(get_column_range_sel(table_metas, ctx, col_expr, quals, need_out_of_bounds, selectivity))) {
    LOG_WARN("failed to get column range selectivity", K(ret));
  }
  return ret;
}

int ObOptSelectivity::get_column_range_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObColumnRefRawExpr &col_expr,
                                           const ObIArray<ObRawExpr* > &quals,
                                           const bool need_out_of_bounds,
                                           double &selectivity)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = ctx.get_stmt();
  uint64_t tid = col_expr.get_table_id();
  uint64_t cid = col_expr.get_column_id();
  ObArenaAllocator allocator("ObSelRange", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_session_info()->get_effective_tenant_id());
  ObQueryRangeArray ranges;
  ObSEArray<ColumnItem, 1> column_items;
  bool use_hist = false;
  ObHistRangeSelHelper helper;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (OB_FAIL(check_column_in_current_level_stmt(stmt, col_expr))) {
    LOG_WARN("failed to check if column is in current level stmt", K(col_expr), K(ret));
  } else if (OB_FAIL(get_column_query_range(ctx, tid, cid, quals,
                                            column_items, allocator, ranges))) {
    LOG_WARN("failed to get column query range", K(ret));
  } else {
    selectivity = 0.0;
    double not_null_sel = 0;
    if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, col_expr, NULL, &not_null_sel))) {
      LOG_WARN("failed to get column ndv and nns", K(ret));
    } else if (OB_FAIL(helper.init(table_metas, ctx, col_expr))) {
      LOG_WARN("failed to get histogram by column", K(ret));
    } else if  (helper.is_valid()) {
      double hist_scale = 0.0;
      if (OB_FAIL(helper.set_ranges(ranges))) {
        LOG_WARN("failed to set ranges", K(ret));
      } else if (OB_FAIL(helper.get_sel(ctx, selectivity))) {
        LOG_WARN("failed to get range sel by histogram", K(ret));
      } else {
        selectivity *= not_null_sel;
        use_hist = true;
        LOG_TRACE("Succeed to get range density ", K(selectivity), K(not_null_sel));
      }
    } else {
      double range_sel = 1.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
        if (OB_ISNULL(ranges.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null range", K(ret), K(i));
        } else if (ranges.at(i)->is_whole_range()) {
          range_sel = DEFAULT_INEQ_SEL;
        } else if (OB_FAIL(get_single_newrange_selectivity(table_metas, ctx, column_items,
                                                           *ranges.at(i), range_sel))) {
          LOG_WARN("Failed to get single newrange sel", K(ret));
        }
        selectivity += range_sel;
      }
    }

    if (OB_SUCC(ret)) {
      //for range filter, selectivity no more than not_null_sel
      if (selectivity > not_null_sel) {
        selectivity = not_null_sel;
      }
    }
    LOG_TRACE("Get column range sel", K(selectivity), K(quals));
  }
  if (OB_SUCC(ret) && need_out_of_bounds &&
      ctx.check_opt_compat_version(COMPAT_VERSION_4_2_1_BP7, COMPAT_VERSION_4_2_2,
                                   COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                   COMPAT_VERSION_4_3_2)) {
    ObObj min_value;
    ObObj max_value;
    if (use_hist) {
      min_value = helper.get_min_value();
      max_value = helper.get_max_value();
    } else {
      if (OB_FAIL(get_column_min_max(table_metas, ctx, col_expr, min_value, max_value))) {
        LOG_WARN("failed to get column min max", K(ret));
      }
    }
    if (FAILEDx(refine_out_of_bounds_sel(table_metas, ctx, col_expr, ranges,
                                         min_value, max_value, selectivity))) {
      LOG_WARN("failed to refine out of bounds sel", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::get_column_range_min_max(const OptSelectivityCtx &ctx,
                                               const ObColumnRefRawExpr *col_expr,
                                               const ObIArray<ObRawExpr* > &quals,
                                               ObObj &obj_min,
                                               ObObj &obj_max)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = ctx.get_stmt();
  uint64_t tid = 0;
  uint64_t cid = 0;
  ObArenaAllocator allocator("ObSelRange", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_session_info()->get_effective_tenant_id());
  ObQueryRangeArray ranges;
  ObSEArray<ColumnItem, 1> column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr) ||
      FALSE_IT(tid = col_expr->get_table_id()) ||
      FALSE_IT(cid = col_expr->get_column_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), KPC(col_expr));
  } else if (OB_FAIL(check_column_in_current_level_stmt(stmt, *col_expr))) {
    LOG_WARN("failed to check if column is in current level stmt", KPC(col_expr), K(ret));
  } else if (OB_FAIL(get_column_query_range(ctx, tid, cid, quals,
                                            column_items, allocator, ranges))) {
    LOG_WARN("failed to get column query range", K(ret));
  } else if (OB_ISNULL(column_items.at(0).expr_) ||
             OB_UNLIKELY(ranges.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range", K(ret), K(column_items), K(ranges));
  } else if (ranges.at(0)->is_whole_range() ||
             ranges.at(0)->empty()) {
    // do nothing
  } else {
    bool is_valid = true;
    ObObj tmp_min, tmp_max;
    tmp_min.set_max_value();
    tmp_max.set_min_value();
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < ranges.count(); ++i) {
      if (OB_ISNULL(ranges.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null range", K(ret), K(i));
      } else if (ranges.at(i)->is_whole_range() ||
                 ranges.at(i)->empty()) {
        is_valid = false;
      } else {
        const ObRowkey &startkey = ranges.at(i)->get_start_key();
        const ObRowkey &endkey = ranges.at(i)->get_end_key();
        tmp_min = std::min(tmp_min, startkey.get_obj_ptr()[0]);
        tmp_max = std::max(tmp_max, endkey.get_obj_ptr()[0]);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_obj(ctx.get_allocator(), tmp_min, obj_min))) {
      LOG_WARN("fail to deep copy ObObj", K(ret), K(obj_min));
    } else if (OB_FAIL(ob_write_obj(ctx.get_allocator(), tmp_max, obj_max))) {
      LOG_WARN("fail to deep copy ObObj", K(ret), K(obj_min));
    }
    LOG_TRACE("Get column range min max", K(obj_min), K(obj_max), K(quals));
  }

  return ret;
}

int ObOptSelectivity::get_single_newrange_selectivity(const OptTableMetas &table_metas,
                                                      const OptSelectivityCtx &ctx,
                                                      const ObIArray<ColumnItem> &range_columns,
                                                      const ObNewRange &range,
                                                      double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0;
  if (range.is_whole_range()) {
    //Whole range
  } else if (range.empty()) {
    selectivity = 0;
  } else {
    const ObRowkey &startkey = range.get_start_key();
    const ObRowkey &endkey = range.get_end_key();
    if (startkey.get_obj_cnt() == endkey.get_obj_cnt() &&
        range_columns.count() == startkey.get_obj_cnt()) {
      selectivity = 1.0;
      double tmp_selectivity = 1.0;
      int64_t column_num = startkey.get_obj_cnt();
      //if prefix of range is single value, process next column;
      //stop at first range column
      bool last_column = false;
      const ObColumnRefRawExpr *col_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !last_column && i < column_num; ++i) {
        // if (is_like_sel && startobj->is_string_type()
        //       && startobj->get_string().length() > 0 && '\0' == startobj->get_string()[0]) {
        //     column_selectivity = DEFAULT_INEQ_SEL;
        //     range_selectivity *= column_selectivity;
        // }
        if (OB_ISNULL(col_expr = range_columns.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(col_expr));
        } else if (OB_FAIL(calc_column_range_selectivity(
            table_metas, ctx, *col_expr,
            startkey.get_obj_ptr()[i],
            endkey.get_obj_ptr()[i],
            (col_expr->get_type_class() != ObFloatTC) && (col_expr->get_type_class() != ObDoubleTC),
            range.border_flag_,
            last_column,
            tmp_selectivity))) {
          LOG_WARN("failed to calculate column range selectivity", K(ret));
        } else {
          selectivity *= tmp_selectivity;
        }
      }
    }
  }
  return ret;
}

int ObOptSelectivity::calc_column_range_selectivity(const OptTableMetas &table_metas,
                                                    const OptSelectivityCtx &ctx,
                                                    const ObRawExpr &column_expr,
                                                    const ObObj &start_obj,
                                                    const ObObj &end_obj,
                                                    const bool discrete,
                                                    const ObBorderFlag border_flag,
                                                    bool &last_column,
                                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  last_column = false;
  selectivity = DEFAULT_SEL;
  double ndv = 0;
  double not_null_sel = 0;
  ObObj maxobj;
  ObObj minobj;
  maxobj.set_max_value();
  minobj.set_min_value();

  if ((start_obj.is_min_value() && end_obj.is_max_value()) ||
      (start_obj.is_max_value() && end_obj.is_min_value()) ||
      (start_obj.is_max_value() && end_obj.is_max_value()) ||
      (start_obj.is_min_value() && end_obj.is_min_value())) {
    last_column = true;
    selectivity = 1.0;
    LOG_TRACE("[RANGE COL SEL] Col is whole range", K(selectivity), K(last_column));
  } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, column_expr, &ndv, &not_null_sel))) {
    LOG_WARN("failed to get column ndv and nns", K(ret));
  } else if (OB_FAIL(get_column_min_max(table_metas, ctx, column_expr, minobj, maxobj))) {
    LOG_WARN("failed to get column min max", K(ret));
  } else if (!minobj.is_min_value() && !maxobj.is_max_value()) {
    ObObj minscalar;
    ObObj maxscalar;
    ObObj startscalar;
    ObObj endscalar;
    ObObj *new_start_obj = NULL;
    ObObj *new_end_obj = NULL;
    ObArenaAllocator tmp_alloc("ObOptSel");
    bool convert2sortkey = ctx.check_opt_compat_version(COMPAT_VERSION_4_2_1_BP5, COMPAT_VERSION_4_2_2,
                                                        COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                        COMPAT_VERSION_4_3_1);
    if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&start_obj, tmp_alloc, new_start_obj)) ||
        OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&end_obj, tmp_alloc, new_end_obj))) {
      LOG_WARN("failed to convert valid obj for opt stats", K(ret), K(start_obj), K(end_obj),
                                                            KPC(new_start_obj), KPC(new_end_obj));
    } else if (OB_ISNULL(new_start_obj) || OB_ISNULL(new_end_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(new_start_obj), K(new_end_obj));
    } else if (new_start_obj->is_null() && new_end_obj->is_null()) {
      selectivity = 1 - not_null_sel;
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(&minobj, &maxobj,
                                                                    new_start_obj, new_end_obj,
                                                                    &minscalar, &maxscalar,
                                                                    &startscalar, &endscalar,
                                                                    convert2sortkey))) {
      LOG_WARN("failed to convert obj to scalars", K(ret));
    } else if (OB_FAIL(do_calc_range_selectivity(minscalar.get_double(),
                                                 maxscalar.get_double(),
                                                 startscalar,
                                                 endscalar,
                                                 ndv,
                                                 discrete,
                                                 border_flag,
                                                 last_column,
                                                 selectivity))) {
      LOG_WARN("failed to do calc range selectivity", K(ret));
    } else {
      selectivity = std::max(selectivity, 1.0 / std::max(ndv, 1.0));
      selectivity *= not_null_sel;
    }
  } else {
    bool is_half = start_obj.is_min_value() || end_obj.is_max_value() ||
                   start_obj.is_max_value() || end_obj.is_min_value();
    if (lib::is_oracle_mode()) {
      is_half = is_half || (!start_obj.is_null() && (end_obj.is_null()));
    } else {
      is_half = is_half || (start_obj.is_null() && !(end_obj.is_null()));
    }

    if (is_half) {
      selectivity = OB_DEFAULT_HALF_OPEN_RANGE_SEL;
      last_column = true;
      LOG_TRACE("[RANGE COL SEL] default half open range sel", K(selectivity), K(last_column));
    } else {
      //startobj and endobj cannot be min/max in this branch, no need to defend
      ObObj startscalar;
      ObObj endscalar;
      bool convert2sortkey = ctx.check_opt_compat_version(COMPAT_VERSION_4_2_1_BP5, COMPAT_VERSION_4_2_2,
                                                          COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                          COMPAT_VERSION_4_3_1);
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(NULL, NULL, &start_obj, &end_obj,
                                                               NULL, NULL, &startscalar, &endscalar,
                                                               convert2sortkey))) {
        LOG_WARN("failed to convert objs to scalars", K(ret));
      } else {
        LOG_TRACE("range column est", K(start_obj), K(end_obj), K(startscalar), K(endscalar));
        if (startscalar.is_double() && endscalar.is_double()) {
          if (fabs(endscalar.get_double() - startscalar.get_double()) < OB_DOUBLE_EPSINON) {
            selectivity = EST_DEF_VAR_EQ_SEL;
            LOG_TRACE("[RANGE COL SEL] default single value sel", K(selectivity), K(last_column));
          } else {
            selectivity = OB_DEFAULT_CLOSED_RANGE_SEL;
            last_column = true;
            LOG_TRACE("[RANGE COL SEL] default range value sel", K(selectivity), K(last_column));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptSelectivity::do_calc_range_selectivity(const double min,
                                                const double max,
                                                const ObObj &scalar_start,
                                                const ObObj &scalar_end,
                                                const double ndv,
                                                const bool discrete,
                                                const ObBorderFlag &border_flag,
                                                bool &last_column,
                                                double &selectivity)
{
  int ret = OB_SUCCESS;
  last_column = true;
  selectivity = DEFAULT_SEL;
  ObBorderFlag flags = border_flag;
  if (ndv <= 0) {
    selectivity = 0.0;
  } else if (!(scalar_start.is_double() || scalar_start.is_min_value() || scalar_start.is_max_value()) ||
             !(scalar_end.is_double() || scalar_end.is_min_value() || scalar_end.is_max_value())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected obj type", K(ret), K(scalar_start.get_type()), K(scalar_end.get_type()));
  } else {
    selectivity = 0.0;
    double start = 0.0;
    double end = 0.0;
    last_column = true;
    LOG_TRACE("[DO CALC RANGE] begin calc range expr sel", K(scalar_start), K(scalar_end), K(min), K(max));
    if (scalar_start.is_min_value() || scalar_start.is_max_value()) {
      start = min;
      flags.set_inclusive_start();
    } else {
      start = scalar_start.get_double();
    }
    if (scalar_end.is_min_value() || scalar_end.is_max_value()) {
      end = max;
      flags.set_inclusive_end();
    } else {
      end = scalar_end.get_double();
    }
    if (fabs(start - end) < OB_DOUBLE_EPSINON) {
      selectivity = 1.0 / ndv; //Single value
      last_column = false;
      LOG_TRACE("[DO CALC RANGE] single value");
    } else {
      if (start < min) {
        start = min;
        flags.set_inclusive_start();
      }
      if (end > max) {
        end = max;
        flags.set_inclusive_end();
      }
      if (start > end) { // (start is min_value and end < min) or (end is max and start > max)
        selectivity = 0.0;
      } else if (fabs(max - min) < OB_DOUBLE_EPSINON) {
        selectivity = fabs(end - start) < OB_DOUBLE_EPSINON ? 1.0 : 0.0;
      } else {
        selectivity = (end - start) / (max - min);
        selectivity = revise_range_sel(selectivity, ndv, discrete,
                                       flags.inclusive_start(),
                                       flags.inclusive_end());
      }
    }
  }
  return ret;
}

double ObOptSelectivity::revise_range_sel(double selectivity,
                                          double distinct,
                                          bool discrete,
                                          bool include_start,
                                          bool include_end)
{
  if (discrete) {
    if (!include_start && !include_end) {
      selectivity -= 1.0 / distinct;
    } else if (include_start && include_end) {
      selectivity += 1.0 / distinct;
    } else { }//do nothing
  } else if (include_start && include_end) {
    selectivity += 2.0 / distinct;
  } else if (include_start || include_end) {
    selectivity += 1.0 / distinct;
  } else if (selectivity == 1.0) {
    //if not include both start and end, and the selectivity is 1.0, should minus 1/ndv
    //eg: min-max: [1, 4], start-end is (1,4] or [1, 4);
    selectivity -= 1.0 / distinct;
  }
  return revise_between_0_1(selectivity);
}

int ObOptSelectivity::refine_out_of_bounds_sel(const OptTableMetas &table_metas,
                                               const OptSelectivityCtx &ctx,
                                               const ObColumnRefRawExpr &col_expr,
                                               const ObQueryRangeArray &ranges,
                                               const ObObj &min_val,
                                               const ObObj &max_val,
                                               double &selectivity)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = col_expr.get_table_id();
  uint64_t column_id = col_expr.get_column_id();
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  double increase_rows_ratio = 0.0;
  double not_null_sel = 0;
  if (NULL == table_meta || min_val.is_min_value() || max_val.is_min_value() ||
      min_val.is_max_value() || max_val.is_max_value()) {
    // do nothing
  } else if (OB_FAIL(table_meta->get_increase_rows_ratio(ctx.get_opt_ctx(), increase_rows_ratio))) {
    LOG_WARN("failed to get extra rows", K(ret));
  } else if (increase_rows_ratio < OB_DOUBLE_EPSINON) {
    // do nothing
  } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, col_expr, NULL, &not_null_sel))) {
    LOG_WARN("failed to get column ndv and nns", K(ret));
  } else {
    bool contain_inf = false;
    double out_of_bounds_sel = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange *range = NULL;
      if (OB_ISNULL(range = ranges.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null range", K(ret), K(i));
      } else if (range->is_whole_range() || range->empty() ||
                 1 != range->get_start_key().get_obj_cnt() ||
                 1 != range->get_end_key().get_obj_cnt()) {
        // do nothing
      } else {
        const ObObj &startobj = range->get_start_key().get_obj_ptr()[0];
        const ObObj &endobj = range->get_end_key().get_obj_ptr()[0];
        double tmp_sel = 0.0;
        if (startobj.is_null() && endobj.is_null()) {
          // do nothing
        } else if (startobj.is_min_value() || endobj.is_max_value() ||
                   startobj.is_null()  || endobj.is_null()) {
          contain_inf = true;
        } else if (OB_FAIL(get_single_range_out_of_bounds_sel(min_val, max_val, startobj, endobj, tmp_sel))) {
          LOG_WARN("failed to calc single out of bounds sel", K(ret));
        } else {
          out_of_bounds_sel += tmp_sel;
        }
      }
    }
    selectivity += std::min(out_of_bounds_sel, increase_rows_ratio) * not_null_sel;
    if (contain_inf) {
      selectivity = std::max(selectivity, DEFAULT_OUT_OF_BOUNDS_SEL * increase_rows_ratio * not_null_sel);
    }
    selectivity = revise_between_0_1(selectivity);
    LOG_TRACE("succeed to refine out of bounds selectivity",
        K(selectivity), K(out_of_bounds_sel), K(contain_inf), K(increase_rows_ratio));
  }
  return ret;
}

int ObOptSelectivity::get_single_range_out_of_bounds_sel(const ObObj &min_val,
                                                         const ObObj &max_val,
                                                         const ObObj &start_val,
                                                         const ObObj &end_val,
                                                         double &selectivity)
{
  int ret = OB_SUCCESS;
  ObObj *new_start = NULL;
  ObObj *new_end = NULL;
  ObObj min_scalar;
  ObObj max_scalar;
  ObObj start_scalar;
  ObObj end_scalar;
  ObArenaAllocator tmp_alloc("ObOptSel");
  selectivity = 0.0;
  if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&start_val, tmp_alloc, new_start)) ||
      OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&end_val, tmp_alloc, new_end))) {
    LOG_WARN("failed to convert valid obj for opt stats", K(ret), K(start_val), K(end_val),
                                                          KPC(new_start), KPC(new_end));
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min_val, &max_val, new_start, new_end,
      &min_scalar, &max_scalar, &start_scalar, &end_scalar))) {
    LOG_WARN("failed to convert objs to scalars", K(ret));
  } else {
    double max_val = max_scalar.get_double();
    double min_val = min_scalar.get_double();
    double start_val = start_scalar.get_double();
    double end_val = end_scalar.get_double();
    double out_of_bounds_sel = 0.0;
    if (max_val - min_val < OB_DOUBLE_EPSINON ||
        end_val - start_val < OB_DOUBLE_EPSINON) {
      // do nothing
    } else if (start_val <= min_val && end_val >= max_val) {
      // include the whole range
      selectivity = 1.0;
    } else if (start_val >= min_val && end_val <= max_val) {
      // within the bound
      selectivity = 0.0;
    } else if (start_val < min_val) {
      selectivity = (std::min(min_val, end_val) - start_val) / (max_val - min_val);
    } else if (end_val > max_val) {
      selectivity = (end_val - std::max(max_val, start_val)) / (max_val - min_val);
    }
  }
  return ret;
}

int ObOptSelectivity::check_column_in_current_level_stmt(const ObDMLStmt *stmt,
                                                         const ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool is_in = false;
  if (OB_FAIL(column_in_current_level_stmt(stmt, expr, is_in))) {
    LOG_WARN("failed to check if column is in current level stmt", K(expr), K(ret));
  } else if (!is_in) {
    // TODO:@yibo should not reach here
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr), K(stmt->get_table_items()), KPC(stmt));
  }
  return ret;
}

int ObOptSelectivity::column_in_current_level_stmt(const ObDMLStmt *stmt,
                                                   const ObRawExpr &expr,
                                                   bool &is_in)
{
  int ret = OB_SUCCESS;
  is_in = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(stmt), K(ret));
  } else if (stmt->is_select_stmt() && static_cast<const ObSelectStmt*>(stmt)->is_set_stmt()) {
    // TODO:@yibo 这里看起来没什么意义，后面检查一下
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt);
    const ObIArray<ObSelectStmt*> &child_query = select_stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && !is_in && i < child_query.count(); ++i) {
      ret = SMART_CALL(column_in_current_level_stmt(child_query.at(i), expr, is_in));
    }
  } else if (expr.is_column_ref_expr()) {
    // TODO:@yibo 正常走到这里的时候，上层stmt的column都被抽成？了
    const ObColumnRefRawExpr &b_expr = static_cast<const ObColumnRefRawExpr&>(expr);
    const TableItem *table_item = stmt->get_table_item_by_id(b_expr.get_table_id());
    if (NULL != table_item) {
      is_in = true;
    }
  }
  return ret;
}

int ObOptSelectivity::get_column_basic_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObRawExpr &expr,
                                           double *distinct_sel_ptr,
                                           double *null_sel_ptr)
{
  int ret = OB_SUCCESS;
  double row_count;
  double ndv;
  double num_null;
  if (OB_FAIL(get_column_basic_info(table_metas, ctx, expr, &ndv, &num_null, NULL, &row_count))) {
    LOG_WARN("failed to get column basic info", K(ret));
  } else {
    double null_sel = row_count <= OB_DOUBLE_EPSINON ? 0.0 : revise_between_0_1(num_null / row_count);
    double distinct_sel = ndv <= OB_DOUBLE_EPSINON ? 0.0 : revise_between_0_1((1 - null_sel) / ndv);
    assign_value(distinct_sel, distinct_sel_ptr);
    assign_value(null_sel, null_sel_ptr);
    LOG_TRACE("column basic sel info", K(distinct_sel), K(null_sel));
  }
  return ret;
}

int ObOptSelectivity::get_column_ndv_and_nns(const OptTableMetas &table_metas,
                                             const OptSelectivityCtx &ctx,
                                             const ObRawExpr &expr,
                                             double *ndv_ptr,
                                             double *not_null_sel_ptr)
{
  int ret = OB_SUCCESS;
  double row_count;
  double ndv;
  double num_null;
  if (OB_FAIL(get_column_basic_info(table_metas, ctx, expr, &ndv, &num_null, NULL, &row_count))) {
    LOG_WARN("failed to get column basic info", K(ret));
  } else {
    double not_null_sel = row_count <= OB_DOUBLE_EPSINON ? 1.0 : 1 - revise_between_0_1(num_null / row_count);
    assign_value(ndv, ndv_ptr);
    assign_value(not_null_sel, not_null_sel_ptr);
    LOG_TRACE("column ndv and not null sel", K(ndv), K(not_null_sel), K(row_count), K(num_null));
  }
  return ret;
}

int ObOptSelectivity::get_column_min_max(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr &expr,
                                         ObObj &min_obj,
                                         ObObj &max_obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!expr.is_column_ref_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid expr", K(ret), K(expr));
  } else {
    const ObColumnRefRawExpr &column_expr = static_cast<const ObColumnRefRawExpr&>(expr);
    uint64_t table_id = column_expr.get_table_id();
    uint64_t column_id = column_expr.get_column_id();
    const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
    OptColumnMeta *column_meta = const_cast<OptColumnMeta *>(
                table_metas.get_column_meta_by_table_id(table_id, column_id));
    const ObTableSchema *table_schema = NULL;
    ObSqlSchemaGuard *schema_guard = const_cast<OptSelectivityCtx&>(ctx).get_sql_schema_guard();
    ObGlobalColumnStat stat;
    if (OB_NOT_NULL(table_meta) && OB_NOT_NULL(column_meta)) {
      if (column_meta->get_min_max_inited()) {
        min_obj = column_meta->get_min_value();
        max_obj = column_meta->get_max_value();
      } else if (OB_ISNULL(ctx.get_opt_stat_manager()) || OB_ISNULL(ctx.get_session_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(ctx.get_opt_stat_manager()),
                                        K(ctx.get_session_info()));
      } else if (table_meta->use_opt_stat() &&
                 OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(ctx.get_session_info()->get_effective_tenant_id(),
                                                                     table_meta->get_ref_table_id(),
                                                                     table_meta->get_stat_parts(),
                                                                     column_id,
                                                                     table_meta->get_rows(),
                                                                     table_meta->get_scale_ratio(),
                                                                     stat,
                                                                     &ctx.get_allocator()))) {
        LOG_WARN("failed to get column stat", K(ret));
      } else {
        column_meta->set_min_max_inited(true);
        column_meta->set_min_value(stat.min_val_);
        column_meta->set_max_value(stat.max_val_);
        min_obj = column_meta->get_min_value();
        max_obj = column_meta->get_max_value();
        LOG_TRACE("var basic stat min/max", K(min_obj), K(max_obj));
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_column_hist_scale(const OptTableMetas &table_metas,
                                            const OptSelectivityCtx &ctx,
                                            const ObRawExpr &expr,
                                            double &hist_scale)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  hist_scale = 0.0;
  if (OB_UNLIKELY(!expr.is_column_ref_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid expr", K(ret), K(expr));
  } else {
    const ObColumnRefRawExpr &column_expr = static_cast<const ObColumnRefRawExpr&>(expr);
    const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(column_expr.get_table_id());
    if (OB_NOT_NULL(table_meta)) {
      const OptColumnMeta *column_meta = table_meta->get_column_meta(column_expr.get_column_id());
      if (OB_ISNULL(column_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column meta not find", K(ret), K(*table_meta), K(column_expr));
      } else {
        hist_scale = column_meta->get_hist_scale();
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_column_basic_info(const OptTableMetas &table_metas,
                                            const OptSelectivityCtx &ctx,
                                            const ObRawExpr &expr,
                                            double *ndv_ptr,
                                            double *num_null_ptr,
                                            double *avg_len_ptr,
                                            double *row_count_ptr,
                                            DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!expr.is_column_ref_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid expr", K(ret), K(expr));
  } else {
    const ObColumnRefRawExpr &column_expr = static_cast<const ObColumnRefRawExpr&>(expr);
    bool need_default = false;
    double ndv = 0;
    double num_null = 0;
    double avg_len = 0;
    double row_count = 0;
    double cur_rowcnt = ctx.get_current_rows();
    double base_ndv = 0;

    if (OB_FAIL(get_column_basic_from_meta(table_metas,
                                           column_expr,
                                           need_default,
                                           row_count,
                                           ndv,
                                           num_null,
                                           avg_len,
                                           base_ndv))) {
      LOG_WARN("failed to get column basic from meta", K(ret));
    } else if (need_default &&
               OB_FAIL(get_var_basic_default(row_count, ndv, num_null, avg_len, base_ndv))) {
      LOG_WARN("failed to get var default info", K(ret));
    } else {
      if (num_null > row_count - ndv) {
        num_null = row_count - ndv > 0 ? row_count - ndv : 0;
      }
      switch (est_type) {
        case DistinctEstType::BASE:
          ndv = base_ndv;
          cur_rowcnt = -1.0;
          break;
        case DistinctEstType::CURRENT: {
          cur_rowcnt = ctx.get_current_rows();
          if (NULL != ctx.get_ambient_card() &&
              !ctx.get_ambient_card()->empty() &&
              ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                           COMPAT_VERSION_4_3_3)) {
            ObSEArray<int64_t, 1> table_idx;
            if (OB_FAIL(expr.get_relation_ids().to_array(table_idx))) {
              LOG_WARN("failed to get table idx", K(ret), K(expr));
            } else if (OB_UNLIKELY(table_idx.count() != 1) ||
                      OB_UNLIKELY(table_idx.at(0) < 1) ||
                      OB_UNLIKELY(table_idx.at(0) >= ctx.get_ambient_card()->count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected expr relation ids", K(expr), K(table_idx), K(ctx));
            } else {
              cur_rowcnt = ctx.get_ambient_card()->at(table_idx.at(0));
            }
          }
          break;
        }
      }
      if (OB_SUCC(ret) && cur_rowcnt > 0.0 && cur_rowcnt < row_count) {
        ndv = scale_distinct(cur_rowcnt, row_count, ndv);
      }

      LOG_TRACE("show column basic info",
          K(row_count), K(cur_rowcnt), K(num_null), K(avg_len), K(ndv), K(est_type));

      // set return
      if (OB_SUCC(ret)) {
        assign_value(row_count, row_count_ptr);
        assign_value(ndv, ndv_ptr);
        assign_value(num_null, num_null_ptr);
        assign_value(avg_len, avg_len_ptr);
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_column_basic_from_meta(const OptTableMetas &table_metas,
                                                 const ObColumnRefRawExpr &column_expr,
                                                 bool &use_default,
                                                 double &row_count,
                                                 double &ndv,
                                                 double &num_null,
                                                 double &avg_len,
                                                 double &base_ndv)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = column_expr.get_table_id();
  uint64_t column_id = column_expr.get_column_id();
  use_default = false;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  if (OB_NOT_NULL(table_meta)) {
    row_count = table_meta->get_rows();
    const OptColumnMeta *column_meta = table_meta->get_column_meta(column_id);
    if (OB_ISNULL(column_meta)) {
      use_default = true;
    } else {
      ndv = column_meta->get_ndv();
      num_null = column_meta->get_num_null();
      avg_len = column_meta->get_avg_len();
      base_ndv = column_meta->get_base_ndv();
    }
  } else {
    use_default = true;
  }
  return ret;
}

int ObOptSelectivity::get_var_basic_default(double &row_count,
                                            double &ndv,
                                            double &null_num,
                                            double &avg_len,
                                            double &base_ndv)
{
  int ret = OB_SUCCESS;
  row_count = 1.0;
  ndv = 1.0;
  null_num = static_cast<double>(row_count * EST_DEF_COL_NULL_RATIO);
  avg_len = DEFAULT_COLUMN_SIZE;
  base_ndv = 1.0;
  return ret;
}

int ObOptSelectivity::get_histogram_by_column(const OptTableMetas &table_metas,
                                              const OptSelectivityCtx &ctx,
                                              uint64_t table_id,
                                              uint64_t column_id,
                                              ObOptColumnStatHandle &column_stat)
{
  int ret = OB_SUCCESS;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  if (OB_ISNULL(table_meta) || OB_INVALID_ID == table_meta->get_ref_table_id()) {
    // do nothing
  } else if (NULL == ctx.get_opt_stat_manager() ||
             !table_meta->use_opt_stat()) {
    // do nothing
  } else if (table_meta->get_hist_parts().count() != 1) {
    // consider to use the global histogram here
  } else if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_session_info()));
  } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(
            ctx.get_session_info()->get_effective_tenant_id(),
            table_meta->get_ref_table_id(),
            table_meta->get_hist_parts().at(0),
            column_id,
            column_stat))) {
    LOG_WARN("failed to get column stat", K(ret));
  }
  return ret;
}

int ObOptSelectivity::get_compare_value(const OptSelectivityCtx &ctx,
                                        const ObColumnRefRawExpr *col,
                                        const ObRawExpr *calc_expr,
                                        ObObj &expr_value,
                                        bool &can_cmp)
{
  int ret = OB_SUCCESS;
  bool type_safe = false;
  bool got_result = false;
  can_cmp = true;
  if (OB_ISNULL(ctx.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!calc_expr->is_static_scalar_const_expr()) {
    can_cmp = false;
  } else if (OB_FAIL(ObRelationalExprOperator::is_equal_transitive(col->get_result_type(),
                                                                   calc_expr->get_result_type(),
                                                                   type_safe)))  {
    LOG_WARN("failed to check is type safe", K(ret));
  } else if (!type_safe) {
    can_cmp = false;
    LOG_TRACE("cannot compare column and const using the column type", K(ret));
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               calc_expr,
                                                               expr_value,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (!got_result) {
    can_cmp = false;
  } else if (expr_value.get_type() != col->get_result_type().get_type() ||
             expr_value.get_collation_type() != col->get_result_type().get_collation_type()) {
    if (OB_FAIL(convert_obj_to_expr_type(ctx, col, CM_NONE, expr_value))) {
      LOG_WARN("failed to convert obj", K(ret), K(expr_value));
    }
  }
  return ret;
}


/**
 * @brief ObOptSelectivity::get_bucket_bound_idx
 * find the bucket which satisfy;
 *   bucket[idx].ev_ <= value < bucket[idx+1].ev_;
 */
int ObOptSelectivity::get_bucket_bound_idx(const ObHistogram &hist,
                                           const ObObj &value,
                                           int64_t &idx,
                                           bool &is_equal)
{
  int ret = OB_SUCCESS;
  int64_t left = 0;
  int64_t right = hist.get_bucket_size() - 1;
  idx = -1;
  is_equal = false;
  if (OB_LIKELY(hist.get_bucket_size() > 0)) {
    while (OB_SUCC(ret) && left <= right) {
      int64_t mid = (right + left) / 2;
      int eq_cmp = 0;
      if (OB_FAIL(hist.get(mid).endpoint_value_.compare(value, eq_cmp))) {
        LOG_WARN("failed to compare object", K(ret));
      } else if (eq_cmp > 0) {
        // value < bucket[mid].ev
        right = mid - 1;
      } else {
        // bucket[mid].ev < value
        left = mid + 1;
        is_equal = is_equal || (0 == eq_cmp);
      }
    }
    if (OB_SUCC(ret)) {
      idx = right;
    }
  }
  return ret;
}

/**
  * We want to compute the frequence of objs satisfying a equal predicate
  *
  * for col = value
  *
  * try to find bucket b[i] which satisfy: b[i].ev <= value
  * if b[i].ev == value, then the result is b[i].erc
  * else the result is density * sample_size
  *
  */
int ObOptSelectivity::get_equal_pred_sel(const ObHistogram &histogram,
                                         const ObObj &value,
                                         const double sample_size_scale,
                                         double &density)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  bool is_equal = false;
  ObObj *new_value = NULL;
  ObArenaAllocator tmp_alloc("ObOptSel");
  if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&value, tmp_alloc, new_value))) {
    LOG_WARN("failed to convert valid obj for opt stats", K(ret), K(value), KPC(new_value));
  } else if (OB_ISNULL(new_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(new_value), K(value));
  } else if (OB_FAIL(get_bucket_bound_idx(histogram, *new_value, idx, is_equal))) {
    LOG_WARN("failed to get bucket bound idx", K(ret));
  } else if (idx < 0 || idx >= histogram.get_bucket_size() || !is_equal) {
    density = histogram.get_density();
  } else {
    density = static_cast<double>(histogram.get(idx).endpoint_repeat_count_)
        / histogram.get_sample_size();
  }
  if (OB_SUCC(ret) && sample_size_scale > 0) {
    density /= sample_size_scale;
  }
  return ret;
}

int ObOptSelectivity::get_range_sel_by_histogram(const OptSelectivityCtx &ctx,
                                                 const ObHistogram &histogram,
                                                 const ObQueryRangeArray &ranges,
                                                 bool no_whole_range,
                                                 const double sample_size_scale,
                                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    const ObNewRange *range = ranges.at(i);
    double tmp_selectivity = 0;

    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL range", K(ret));
    } else if (no_whole_range && range->is_whole_range()) {
      tmp_selectivity = DEFAULT_INEQ_SEL;
    } else {
      const ObRowkey &startkey = range->get_start_key();
      const ObRowkey &endkey = range->get_end_key();

      if (startkey.get_obj_cnt() == endkey.get_obj_cnt() &&
          1 == startkey.get_obj_cnt()) {
        const ObObj *startobj = &startkey.get_obj_ptr()[0];
        const ObObj *endobj = &endkey.get_obj_ptr()[0];
        ObObj *new_startobj = NULL;
        ObObj *new_endobj = NULL;
        ObArenaAllocator tmp_alloc("ObOptSel");
        if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(startobj, tmp_alloc, new_startobj)) ||
            OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(endobj, tmp_alloc, new_endobj))) {
          LOG_WARN("failed to convert valid obj for opt stats", K(ret), KPC(startobj), KPC(endobj),
                                                                KPC(new_startobj), KPC(new_endobj));
        } else if (OB_ISNULL(new_startobj) || OB_ISNULL(new_endobj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(new_startobj), K(new_endobj));
        } else if (OB_FAIL(get_range_pred_sel(ctx,
                                              histogram,
                                              *new_startobj,
                                              range->border_flag_.inclusive_start(),
                                              *new_endobj,
                                              range->border_flag_.inclusive_end(),
                                              tmp_selectivity))) {
          LOG_WARN("failed to get range density", K(ret));
        } else if (sample_size_scale > 0) {
          tmp_selectivity /= sample_size_scale;
        }
      }
    }

    if (OB_SUCC(ret)) {
      selectivity += tmp_selectivity;
      LOG_TRACE("single range histogram selectivity", K(*range), K(tmp_selectivity));
    }
  }
  LOG_TRACE("histogram selectivity", K(ranges), K(selectivity), K(ret));
  return ret;
}

/**
  * We want to compute the frequencey of objs satisfying a lt/le predicate
  *
  * I. for col < maxv
  *    try to find bucket b[i] which satisfy:  b[i].ev <= maxv < b[i+1].ev
  *    if maxv == b[i].ev, then the result would be b[i].enum - b[i].erc
  *    else (maxv > b[i].ev), then the result would be b[i].enum
  *
  * II. for col <= maxv
  *     try to find bucket b[i] which satisfy: b[i].ev <= maxv < b[i+1].ev
  *     then the result would be b[i].enum
  *
  * two corner cases exists:
  * 1. maxv < b[0].ev, we have no idea how many elements in the first buckets do saitfy obj < maxv
  * 2. b[count()-1].ev < maxv, all elements in the histograms satisfy the predicate
  *
  */
int ObOptSelectivity::get_less_pred_sel(const OptSelectivityCtx &ctx,
                                        const ObHistogram &histogram,
                                        const ObObj &maxv,
                                        const bool inclusive,
                                        double &density)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  bool is_equal = false;
  if (maxv.is_min_value()) {
    density = 0.0;
  } else if (maxv.is_max_value()) {
    density = 1.0;
  } else if (OB_FAIL(get_bucket_bound_idx(histogram, maxv, idx, is_equal))) {
    LOG_WARN("failed to get bucket bound idx", K(ret));
  } else if (idx < 0) {
    density = histogram.get_density();
  } else if (idx >= histogram.get_bucket_size()) {
    density = 1.0;
  } else if (is_equal) {
    double frequency = histogram.get(idx).endpoint_num_ -
        (inclusive ? 0 : histogram.get(idx).endpoint_repeat_count_);
    density = frequency / histogram.get_sample_size();
  } else {
    double last_bucket_count = 0;
    if (idx + 1 < histogram.get_bucket_size()) {
      // b[i].ev < maxv < b[i+1].ev
      // estimate how many elements (smaller than maxv) in bucket[i+1] there are
      ObObj minscalar, maxscalar, startscalar, endscalar;
      ObObj minobj(histogram.get(idx).endpoint_value_);
      ObObj maxobj(histogram.get(idx+1).endpoint_value_);
      ObObj startobj(minobj), endobj(maxv);
      bool convert2sortkey = ctx.check_opt_compat_version(COMPAT_VERSION_4_2_1_BP5, COMPAT_VERSION_4_2_2,
                                                          COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                          COMPAT_VERSION_4_3_1);
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(
                    &minobj, &maxobj, &startobj, &endobj,
                    &minscalar, &maxscalar, &startscalar, &endscalar,
                    convert2sortkey))) {
        LOG_WARN("failed to convert objs to scalars", K(ret));
      } else if (maxscalar.get_double() - minscalar.get_double() > OB_DOUBLE_EPSINON) {
        last_bucket_count = histogram.get(idx+1).endpoint_num_ -
                            histogram.get(idx+1).endpoint_repeat_count_ -
                            histogram.get(idx).endpoint_num_;
        last_bucket_count *= (endscalar.get_double() - startscalar.get_double()) /
                             (maxscalar.get_double() - minscalar.get_double());
      }
    }
    density = static_cast<double>(histogram.get(idx).endpoint_num_ + last_bucket_count)
        / histogram.get_sample_size();
  }
  LOG_TRACE("link bug", K(density), K(maxv), K(inclusive), K(idx), K(is_equal));
  return ret;
}

/**
  * We want to compute the frequence of objs satisfying a gt/ge predicate
  *
  * for minv <(=) col
  *
  *  I. f(minv < col) can be converted as sample_size - f(col <= minv)
  * II. f(minv <= col) can be converted as sample_size - f(col < minv)
  *
  */
int ObOptSelectivity::get_greater_pred_sel(const OptSelectivityCtx &ctx,
                                           const ObHistogram &histogram,
                                           const ObObj &minv,
                                           const bool inclusive,
                                           double &density)
{
  int ret = OB_SUCCESS;
  double less_sel = 0;
  if (OB_FAIL(get_less_pred_sel(ctx,
                                histogram,
                                minv,
                                !inclusive,
                                less_sel))) {
    LOG_WARN("failed to get less predicate selectivity", K(ret));
  } else {
    density = 1.0 - less_sel;
    LOG_TRACE("link bug", K(density), K(minv), K(inclusive));
  }
  return ret;
}

/**
  * We want to compute the frequence of objs satisfying a range predicate
  *
  * for minv <(=) col <(=) maxv
  *
  * the problem can be converted as f(col <(=) maxv) + f(minv <(=) col) - sample_size
  *
  */
int ObOptSelectivity::get_range_pred_sel(const OptSelectivityCtx &ctx,
                                         const ObHistogram &histogram,
                                         const ObObj &minv,
                                         const bool min_inclusive,
                                         const ObObj &maxv,
                                         const bool max_inclusive,
                                         double &density)
{
  int ret = OB_SUCCESS;
  double less_sel = 0;
  double greater_sel = 0;
  if (OB_FAIL(get_greater_pred_sel(ctx, histogram, minv, min_inclusive, greater_sel))) {
    LOG_WARN("failed to get greater predicate selectivity", K(ret));
  } else if (OB_FAIL(get_less_pred_sel(ctx, histogram, maxv, max_inclusive, less_sel))) {
    LOG_WARN("failed to get less predicate selectivity", K(ret));
  } else {
    density = less_sel + greater_sel - 1.0;
    density = density <= 0 ? histogram.get_density() : density;
  }
  return ret;
}

int ObOptSelectivity::get_column_query_range(const OptSelectivityCtx &ctx,
                                             const uint64_t table_id,
                                             const uint64_t column_id,
                                             const ObIArray<ObRawExpr *> &quals,
                                             ObIArray<ColumnItem> &column_items,
                                             ObIAllocator &alloc,
                                             ObQueryRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  const ObLogPlan *log_plan = ctx.get_plan();
  const ParamStore *params = ctx.get_params();
  ObExecContext *exec_ctx = ctx.get_opt_ctx().get_exec_ctx();
  ObSQLSessionInfo *session_info = ctx.get_session_info();
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_session_info());
  const ColumnItem* column_item = NULL;
  bool dummy_all_single_value_ranges = true;
  if (OB_ISNULL(log_plan) || OB_ISNULL(exec_ctx) || OB_ISNULL(session_info) ||
      OB_ISNULL(column_item = log_plan->get_column_item_by_id(table_id, column_id)) ||
      OB_ISNULL(ctx.get_stmt()) || OB_ISNULL(ctx.get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan), K(exec_ctx), K(session_info), K(column_item));
  } else if (OB_FAIL(column_items.push_back(*column_item))) {
    LOG_WARN("failed to push back column item", K(ret));
  } else if (ctx.get_opt_ctx().enable_new_query_range()) {
    ObPreRangeGraph pre_range_graph(alloc);
    if (OB_FAIL(pre_range_graph.preliminary_extract_query_range(column_items, quals, exec_ctx,
                                                                NULL, params, false, true))) {
      LOG_WARN("failed to preliminary extract query range", K(column_items), K(quals));
    } else if (OB_FAIL(pre_range_graph.get_tablet_ranges(alloc, *exec_ctx, ranges,
                                                         dummy_all_single_value_ranges,
                                                         dtc_params))) {
      LOG_WARN("failed to get tablet ranges");
    }
  } else {
    ObQueryRange query_range(alloc);
    bool is_in_range_optimization_enabled = false;
    if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(ctx.get_stmt()->get_query_ctx()->get_global_hint(),
                                                                  ctx.get_session_info(),
                                                                  is_in_range_optimization_enabled))) {
      LOG_WARN("failed to check in range optimization enabled", K(ret));
    } else if (OB_FAIL(query_range.preliminary_extract_query_range(column_items,
                                                                  quals,
                                                                  dtc_params,
                                                                  ctx.get_opt_ctx().get_exec_ctx(),
                                                                  ctx.get_opt_ctx().get_query_ctx(),
                                                                  NULL,
                                                                  params,
                                                                  false,
                                                                  true,
                                                                  is_in_range_optimization_enabled))) {
      LOG_WARN("failed to preliminary extract query range", K(ret));
    } else if (!query_range.need_deep_copy()) {
      if (OB_FAIL(query_range.direct_get_tablet_ranges(alloc, *exec_ctx, ranges,
                                                      dummy_all_single_value_ranges, dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      }
    } else if (OB_FAIL(query_range.final_extract_query_range(*exec_ctx, dtc_params))) {
      LOG_WARN("failed to final extract query range", K(ret));
    } else if (OB_FAIL(query_range.get_tablet_ranges(ranges, dummy_all_single_value_ranges, dtc_params))) {
      LOG_WARN("failed to get tablet ranges", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptSelectivity::check_mutex_or(const ObRawExpr &qual, bool &is_mutex)
{
  int ret = OB_SUCCESS;
  is_mutex = true;
  const ObRawExpr *child_expr = NULL;
  const ObRawExpr *column_expr = NULL;
  const ObRawExpr *first_column_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && is_mutex && i < qual.get_param_count(); ++i) {
    if (OB_FAIL(get_simple_mutex_column(qual.get_param_expr(i), column_expr))) {
      LOG_WARN("failed to get simple mutex column", K(ret));
    } else if (NULL == column_expr) {
      is_mutex = false;
    } else if (0 == i) {
      first_column_expr = column_expr;
    } else {
      is_mutex = first_column_expr->same_as(*column_expr);
    }
  }
  return ret;
}

// 列上最常见的互斥谓词
// c1 = 1 or c1 = 2
// c1 = 1 or c1 is null
int ObOptSelectivity::get_simple_mutex_column(const ObRawExpr *qual, const ObRawExpr *&column)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  column = NULL;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret));
  } else if (T_OP_EQ == qual->get_expr_type() ||
             T_OP_NSEQ == qual->get_expr_type() ||
             T_OP_IS == qual->get_expr_type()) {
    if (OB_ISNULL(left_expr = qual->get_param_expr(0)) ||
        OB_ISNULL(right_expr = qual->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected NULL", K(ret), K(left_expr), K(right_expr));
    } else if (left_expr->is_column_ref_expr() && !right_expr->has_flag(CNT_COLUMN)) {
      column = left_expr;
    } else if (right_expr->is_column_ref_expr() && !left_expr->has_flag(CNT_COLUMN)) {
      column = right_expr;
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_table_ambient_cardinality(const OptTableMetas &table_metas,
                                                          const OptSelectivityCtx &ctx,
                                                          const ObRelIds &rel_ids,
                                                          const double cur_rows,
                                                          double &table_ambient_card,
                                                          DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  if (NULL != ctx.get_ambient_card() &&
      !ctx.get_ambient_card()->empty() &&
      DistinctEstType::CURRENT == est_type) {
    table_ambient_card = 1.0;
    for (int64_t i = 0; i < ctx.get_ambient_card()->count(); i ++) {
      if (rel_ids.has_member(i)) {
        table_ambient_card *= std::max(1.0, ctx.get_ambient_card()->at(i));
      }
    }
  } else {
    ObSEArray<uint64_t, 4> table_ids;
    const ObDMLStmt *stmt = ctx.get_stmt();
    table_ambient_card = 1.0;
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null stmt", K(ret));
    } else if (OB_FAIL(stmt->relids_to_table_ids(rel_ids, table_ids))) {
      LOG_WARN("faile to get table ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i ++) {
      table_ambient_card *= std::max(1.0,
        DistinctEstType::BASE == est_type ?
        table_metas.get_base_rows(table_ids.at(i)) :
        table_metas.get_rows(table_ids.at(i)));
    }
  }
  if (cur_rows > 0) {
    table_ambient_card = std::min(cur_rows, table_ambient_card);
  }
  return ret;
}

int ObOptSelectivity::calculate_distinct_in_single_table(const OptTableMetas &table_metas,
                                                         const OptSelectivityCtx &ctx,
                                                         const ObRelIds &rel_id,
                                                         const common::ObIArray<ObRawExpr*>& exprs,
                                                         const double cur_rows,
                                                         DistinctEstType est_type,
                                                         double &rows)
{
  int ret = OB_SUCCESS;
  rows = 1.0;
  ObSEArray<ObRawExpr*, 4> special_exprs;
  ObSEArray<double, 4> expr_ndv;
  ObSEArray<ObRawExpr*, 4> filtered_exprs;
  double ambient_card = -1.0;
  //classify expr and get ndv
  if (OB_UNLIKELY(rel_id.num_members() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected relation id", K(ret));
  } else if (OB_FAIL(calculate_table_ambient_cardinality(table_metas, ctx, rel_id, cur_rows, ambient_card, est_type))) {
    LOG_WARN("failed to calculate ambient card", K(ret));
  } else if (OB_FAIL(filter_column_by_equal_set(table_metas, ctx, est_type, exprs, filtered_exprs))) {
    LOG_WARN("failed filter column by equal set", K(ret));
  } else if (OB_FAIL(calculate_expr_ndv(filtered_exprs, expr_ndv, table_metas, ctx, ambient_card, est_type))) {
    LOG_WARN("fail to calculate expr ndv", K(ret));
  } else if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                           COMPAT_VERSION_4_3_3)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_ndv.count(); ++i) {
      if (0 == i) {
        rows *= expr_ndv.at(i);
      } else {
        rows *= expr_ndv.at(i) / std::sqrt(2);
      }
    }
  } else {
    rows = combine_ndvs(ambient_card, expr_ndv);
  }
  LOG_TRACE("succeed to calculate distinct in single table", K(rel_id), K(ambient_card), K(rows), K(expr_ndv), K(exprs));

  return ret;
}

int ObOptSelectivity::remove_dummy_distinct_exprs(ObIArray<OptDistinctHelper> &helpers,
                                                  ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i ++) {
    ObRawExpr *expr = exprs.at(i);
    bool is_dummy = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(exprs));
    } else if (expr->has_flag(CNT_WINDOW_FUNC) ||
               expr->is_column_ref_expr()) {
      // do nothing
    } else if (OB_FAIL(check_expr_in_distinct_helper(expr, helpers, is_dummy))) {
      LOG_WARN("failed to check expr", K(ret));
    }
    if (OB_SUCC(ret) && !is_dummy) {
      if (OB_FAIL(new_exprs.push_back(expr))) {
        LOG_WARN("failed to push back");
      }
    }
  }
  if (OB_SUCC(ret) && new_exprs.count() != exprs.count()) {
    LOG_DEBUG("remove dummy distinct exprs", K(exprs), K(new_exprs));
    if (OB_FAIL(exprs.assign(new_exprs))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::check_expr_in_distinct_helper(const ObRawExpr *expr,
                                                    const ObIArray<OptDistinctHelper> &helpers,
                                                    bool &is_dummy_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  bool found = false;
  is_dummy_expr = true;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_dummy_expr && i < column_exprs.count(); i ++) {
    ObRawExpr *col_expr = column_exprs.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(column_exprs));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < helpers.count(); j ++) {
      if (col_expr->get_relation_ids() == helpers.at(j).rel_id_) {
        found = ObOptimizerUtil::find_item(helpers.at(j).exprs_, col_expr);
      }
    }
    if (!found) {
      is_dummy_expr = false;
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_distinct(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObIArray<ObRawExpr*>& exprs,
                                         const double origin_rows,
                                         double &rows,
                                         const bool need_refine,
                                         DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  rows = 1;
  ObSEArray<double, 4> single_ndvs;
  ObSEArray<OptDistinctHelper, 2> helpers;
  ObSEArray<ObRawExpr *, 2> special_exprs;
  /**
   * 1. 将 exprs 根据基表分组，sepcial exprs 中保存不在基表计算的表达式，例如 window function 等
   * 2. 基表内计算 NDV 后（每张表的 NDV 最大值受基表行数限制），再根据当前行数计算联合 NDV
  */
  if (OB_FAIL(classify_exprs(ctx, exprs, helpers, special_exprs))) {
    LOG_WARN("failed to classify_exprs", K(ret));
  } else if (OB_FAIL(remove_dummy_distinct_exprs(helpers, special_exprs))) {
    LOG_WARN("failed to remove dummy exprs", K(ret));
  } else if (OB_FAIL(calculate_expr_ndv(special_exprs, single_ndvs, table_metas, ctx, origin_rows, est_type))) {
    LOG_WARN("fail to calculate expr ndv", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < helpers.count(); i ++) {
    OptDistinctHelper &helper = helpers.at(i);
    double single_table_ndv = 1.0;
    if (OB_FAIL(remove_dummy_distinct_exprs(helpers, helper.exprs_))) {
      LOG_WARN("failed to remove dummy exprs", K(ret));
    } else if (OB_FAIL(calculate_distinct_in_single_table(
        table_metas, ctx, helper.rel_id_, helper.exprs_, need_refine ? origin_rows : -1, est_type, single_table_ndv))) {
      LOG_WARN("failed to calculate distinct in single table", K(helper.exprs_));
    } else if (OB_FAIL(single_ndvs.push_back(single_table_ndv))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                    COMPAT_VERSION_4_3_3)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < single_ndvs.count(); ++i) {
      if (0 == i) {
        rows *= single_ndvs.at(i);
      } else {
        rows *= single_ndvs.at(i) / std::sqrt(2);
      }
    }
    //refine
    if (OB_SUCC(ret) && need_refine && origin_rows >= 0.0) {
      rows = std::min(rows, origin_rows);
    }
  } else {
    rows = combine_ndvs(need_refine ? origin_rows : -1, single_ndvs);
  }
  LOG_TRACE("succeed to calculate distinct", K(ctx), K(origin_rows), K(rows), K(single_ndvs), K(exprs));
  return ret;
}

int ObOptSelectivity::calculate_distinct(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr& expr,
                                         const double origin_rows,
                                         double &rows,
                                         const bool need_refine,
                                         DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> expr_array;
  if (OB_FAIL(expr_array.push_back(const_cast<ObRawExpr*>(&expr)))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(calculate_distinct(table_metas, ctx, expr_array, origin_rows, rows, need_refine, est_type))) {
    LOG_WARN("failed to calculate distinct", K(expr));
  }
  return ret;
}

double ObOptSelectivity::combine_two_ndvs(double ambient_card, double ndv1, double ndv2)
{
  ndv1 = std::max(1.0, ndv1);
  ndv2 = std::max(1.0, ndv2);
  double max_ndv = std::max(ndv1, ndv2);
  double min_ndv = std::min(ndv1, ndv2);
  double combine_ndv = 1.0;
  if (ambient_card >= 0.0) {
    // due to the precision, we need to refine the result
    combine_ndv = ndv1 * ndv2 * (1 - pow(1 - 1 / min_ndv , ambient_card / max_ndv));
    combine_ndv = std::max(combine_ndv, max_ndv);
    combine_ndv = std::min(ambient_card, combine_ndv);
  } else {
    combine_ndv = ndv1 * ndv2 / std::sqrt(2);
    combine_ndv = std::max(combine_ndv, max_ndv);
  }
  return combine_ndv;
}

double ObOptSelectivity::combine_ndvs(double ambient_card, ObIArray<double> &ndvs)
{
  double ndv = 1.0;
  if (!ndvs.empty()) {
    lib::ob_sort(&ndvs.at(0), &ndvs.at(0) + ndvs.count());
    ndv = ndvs.at(0);
    for (int64_t i = 1; i < ndvs.count(); i ++) {
      ndv = combine_two_ndvs(ambient_card, ndv, ndvs.at(i));
    }
  }
  if (ambient_card >= 0.0) {
    ndv = std::min(ambient_card, ndv);
  }
  return ndv;
}

int ObOptSelectivity::classify_exprs(const OptSelectivityCtx &ctx,
                                     const ObIArray<ObRawExpr*>& exprs,
                                     ObIArray<OptDistinctHelper> &helpers,
                                     ObIArray<ObRawExpr*>& special_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(classify_exprs(ctx, exprs.at(i), helpers, special_exprs))) {
      LOG_WARN("failed to classify_exprs", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::classify_exprs(const OptSelectivityCtx &ctx,
                                     ObRawExpr *expr,
                                     ObIArray<OptDistinctHelper> &helpers,
                                     ObIArray<ObRawExpr*>& special_exprs)
{
  int ret = OB_SUCCESS;
  bool is_special = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(expr), K(ret));
  } else if (OB_FAIL(check_is_special_distinct_expr(ctx, expr, is_special))) {
    LOG_WARN("failed to check expr", K(ret));
  } else if (is_special) {
    if (expr->has_flag(CNT_WINDOW_FUNC) ||
        expr->get_relation_ids().num_members() != 1) {
      if (OB_FAIL(add_var_to_array_no_dup(special_exprs, expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else {
      if (OB_FAIL(add_expr_to_distinct_helper(helpers, expr->get_relation_ids(), expr))) {
        LOG_WARN("failed to add expr to helper", K(ret));
      }
    }
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(add_expr_to_distinct_helper(helpers, expr->get_relation_ids(), expr))) {
      LOG_WARN("failed to add expr to helper", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(classify_exprs(ctx, expr->get_param_expr(i), helpers, special_exprs)))) {
        LOG_WARN("failed to classify_exprs", K(ret));
      }
    }
  }
  LOG_DEBUG("succeed to classify distinct exprs", K(helpers), K(special_exprs));
  return ret;
}

int ObOptSelectivity::add_expr_to_distinct_helper(ObIArray<OptDistinctHelper> &helpers,
                                                  const ObRelIds &rel_id,
                                                  ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  OptDistinctHelper *helper = NULL;
  for (int64_t idx = 0; NULL == helper && idx < helpers.count(); idx ++) {
    if (helpers.at(idx).rel_id_.equal(rel_id)) {
      helper = &helpers.at(idx);
    }
  }
  if (NULL == helper) {
    if (OB_ISNULL(helper = helpers.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(helper->rel_id_.add_members(rel_id))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_var_to_array_no_dup(helper->exprs_, expr))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObOptSelectivity::calculate_expr_ndv(const ObIArray<ObRawExpr*>& exprs,
                                         ObIArray<double>& expr_ndv,
                                         const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const double origin_rows,
                                         DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    double ndv = 0.0;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), K(i));
    } else if (expr->is_column_ref_expr()) {
      if (OB_FAIL(check_column_in_current_level_stmt(ctx.get_stmt(), *expr))) {
        LOG_WARN("failed to check column in current level stmt", K(ret));
      } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *expr, &ndv, NULL, NULL, NULL, est_type))) {
        LOG_WARN("failed to get column basic info", K(ret), K(*expr));
      } else if (OB_FAIL(expr_ndv.push_back(ndv))) {
        LOG_WARN("failed to push back expr", K(ret), K(ndv));
      }
    } else if (OB_FAIL(calculate_special_ndv(table_metas, expr, ctx, ndv, origin_rows, est_type))) {
      LOG_WARN("failed to calculate special expr ndv", K(ret), K(ndv));
    } else if (OB_FAIL(expr_ndv.push_back(ndv))) {
      LOG_WARN("failed to push back", K(ret), K(ndv));
    }
  }
  return ret;
}

int ObOptSelectivity::check_is_special_distinct_expr(const OptSelectivityCtx &ctx,
                                                     const ObRawExpr *expr,
                                                     bool &is_special)
{
  int ret = OB_SUCCESS;
  is_special = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(expr), K(ret));
  } else if (expr->is_win_func_expr()) {
    is_special = true;
  } else if (!ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                           COMPAT_VERSION_4_3_3)) {
    is_special = false;
  } else if (expr->is_const_expr()) {
    is_special = false;
  } else if (T_OP_MOD == expr->get_expr_type()) {
    is_special = expr->get_param_count() == 2 &&
                 OB_NOT_NULL(expr->get_param_expr(1)) &&
                 expr->get_param_expr(1)->is_static_scalar_const_expr();
  } else if (T_FUN_SYS_SUBSTR == expr->get_expr_type() || T_FUN_SYS_SUBSTRB == expr->get_expr_type()) {
    if (expr->get_param_count() == 2) {
      is_special = OB_NOT_NULL(expr->get_param_expr(1)) && expr->get_param_expr(1)->is_static_scalar_const_expr();
    } else if (expr->get_param_count() == 3) {
      is_special = OB_NOT_NULL(expr->get_param_expr(2)) && expr->get_param_expr(2)->is_static_scalar_const_expr();
    }
  } else if (is_dense_time_expr_type(expr->get_expr_type()) ||
             T_FUN_SYS_MONTH_NAME == expr->get_expr_type() ||
             T_FUN_SYS_DAY_NAME == expr->get_expr_type()) {
    is_special = expr->get_param_count() == 1;
  } else if (T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
    is_special = expr->get_param_count() == 2 &&
                 OB_NOT_NULL(expr->get_param_expr(0)) &&
                 expr->get_param_expr(0)->is_static_scalar_const_expr();
  } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
    const ObRawExpr *param_expr = NULL;
    bool is_monotonic = false;
    if (OB_UNLIKELY(expr->get_param_count() < 2) ||
        OB_ISNULL(param_expr = expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", KPC(expr));
    } else if (expr->get_data_type() != ObDateType && expr->get_data_type() != ObMySQLDateType) {
      is_special = false;
    } else if (OB_FAIL(ObObjCaster::is_cast_monotonic(param_expr->get_data_type(), expr->get_data_type(), is_special))) {
      LOG_WARN("check cast monotonic error", KPC(expr), K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_special_ndv(const OptTableMetas &table_metas,
                                            const ObRawExpr* expr,
                                            const OptSelectivityCtx &ctx,
                                            double &special_ndv,
                                            const double origin_rows,
                                            DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *param_expr = NULL;
  special_ndv = std::max(origin_rows, 1.0);
  bool is_special = false;
  bool need_refine_by_param_expr = true;
  if (OB_FAIL(check_is_special_distinct_expr(ctx, expr, is_special))) {
    LOG_WARN("failed to check expr", K(ret));
  } else if (OB_UNLIKELY(!is_special)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", KPC(expr), K(ret));
  } else if (expr->is_win_func_expr()) {
    if (OB_FAIL(calculate_winfunc_ndv(table_metas, expr, ctx, special_ndv, origin_rows))) {
      LOG_WARN("failed to calculate windown function ndv", K(ret));
    }
    need_refine_by_param_expr = false;
  } else if (T_OP_MOD == expr->get_expr_type()) {
    param_expr = expr->get_param_expr(0);
    const ObRawExpr *const_expr = expr->get_param_expr(1);
    bool valid = false;
    if (OB_FAIL(calc_const_numeric_value(ctx, const_expr, special_ndv, valid))) {
      LOG_WARN("failed to calc const value", K(ret));
    } else {
      special_ndv = std::abs(special_ndv);
    }
  } else if (T_FUN_SYS_SUBSTR == expr->get_expr_type() || T_FUN_SYS_SUBSTRB == expr->get_expr_type()) {
    double substr_len = 0.0;
    double dummy = 0.0;
    need_refine_by_param_expr = false; // substr ndv will not be greater than its param
    if (OB_FAIL(calculate_expr_avg_len(table_metas, ctx, expr, substr_len))) {
      LOG_WARN("failed to calc expr length", K(ret));
    } else if (OB_FAIL(calculate_substrb_info(table_metas,
                                              ctx,
                                              expr->get_param_expr(0),
                                              substr_len - ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH,
                                              origin_rows,
                                              special_ndv,
                                              dummy,
                                              est_type))) {
      LOG_WARN("failed to calculate substr ndv", K(ret));
    }
  } else if (is_dense_time_expr_type(expr->get_expr_type())
             || (T_FUN_SYS_CAST == expr->get_expr_type()
                 && (expr->get_data_type() == ObDateType
                     || expr->get_data_type() == ObMySQLDateType))
             || T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
    if (T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
      param_expr = expr->get_param_expr(1);
    } else {
      param_expr = expr->get_param_expr(0);
    }
    ObObj min_value;
    ObObj max_value;
    bool use_default = false;
    double min_scalar = 0.0;
    double max_scalar = 0.0;
    if (OB_FAIL(calc_expr_min_max(table_metas,
                                  ctx,
                                  expr,
                                  min_value,
                                  max_value))) {
      LOG_WARN("failed to calculate expr min max", K(ret));
    } else if (min_value.is_min_value() || max_value.is_max_value() ||
               !(min_value.is_integer_type() || min_value.is_number() || min_value.is_date() || min_value.is_mysql_date()) ||
               !(max_value.is_integer_type() || max_value.is_number() || max_value.is_date() || max_value.is_mysql_date())) {
      use_default = true;
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&min_value, min_scalar)) ||
               OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&max_value, max_scalar))) {
      LOG_WARN("failed to convert obj to double", K(ret), K(min_value), K(max_value));
    } else {
      special_ndv = max_scalar - min_scalar + 1;
    }
    if (OB_SUCC(ret) && !use_default) {
      if (T_FUN_SYS_YEARWEEK_OF_DATE == expr->get_expr_type()) {
        special_ndv *= 54.0 / 100.0;
      } else if (T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
        ObObj result;
        bool got_result = false;
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                              expr->get_param_expr(0),
                                                              result,
                                                              got_result,
                                                              ctx.get_allocator()))) {
          LOG_WARN("fail to calc_const_or_calculable_expr", K(ret));
        } else if (!got_result || result.is_null() || !result.is_int()) {
          // do nothing
        } else if (DATE_UNIT_YEAR_MONTH == result.get_int()) {
          special_ndv *= 12.0 / 100.0;
        }
      }
    }
  } else if (T_FUN_SYS_MONTH_NAME == expr->get_expr_type()) {
    special_ndv = 12;
    param_expr = expr->get_param_expr(0);
  } else if (T_FUN_SYS_DAY_NAME == expr->get_expr_type()) {
    special_ndv = 7;
    param_expr = expr->get_param_expr(0);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected special expr", KPC(expr));
  }
  if (OB_SUCC(ret) && need_refine_by_param_expr && NULL != param_expr) {
    double ndv_upper_bound = 1.0;
    if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas,
                                              ctx,
                                              *param_expr,
                                              origin_rows,
                                              ndv_upper_bound,
                                              true,
                                              est_type)))) {
      LOG_WARN("failed to calculate distinct", K(ret), KPC(param_expr));
    } else {
      special_ndv = std::min(special_ndv, ndv_upper_bound);
    }
  }
  special_ndv = revise_ndv(special_ndv);
  return ret;
}

int ObOptSelectivity::calculate_winfunc_ndv(const OptTableMetas &table_metas,
                                            const ObRawExpr* expr,
                                            const OptSelectivityCtx &ctx,
                                            double &special_ndv,
                                            const double origin_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_win_func_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(expr), K(ret));
  } else {
    double part_order_ndv = 1.0;
    double order_ndv = 1.0;
    double part_ndv = 1.0;
    ObSEArray<ObRawExpr*, 4> part_exprs;
    ObSEArray<ObRawExpr*, 4> order_exprs;
    ObSEArray<ObRawExpr*, 4> part_order_exprs;
    const ObWinFunRawExpr *win_expr = reinterpret_cast<const ObWinFunRawExpr*>(expr);
    const ObIArray<OrderItem> &order_items = win_expr->get_order_items();
    if (OB_UNLIKELY(origin_rows < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("win function ndv depends on current rows", K(ret), K(origin_rows));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      const OrderItem &order_item = order_items.at(i);
      ObRawExpr *order_expr = order_item.expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pointer", K(ret));
      } else if (OB_FAIL(order_exprs.push_back(order_expr))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(part_exprs.assign(win_expr->get_partition_exprs()))) {
      LOG_WARN("fail to assign exprs", K(ret));
    } else if (OB_FAIL(part_order_exprs.assign(part_exprs))) {
      LOG_WARN("fail to assign exprs", K(ret));
    } else if (OB_FAIL(append(part_order_exprs, order_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, part_order_exprs, origin_rows, part_order_ndv)))) {
      LOG_WARN("failed to calculate_distinct", K(ret));
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, order_exprs, origin_rows, order_ndv)))) {
      LOG_WARN("failed to calculate_distinct", K(ret));
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, part_exprs, origin_rows, part_ndv)))) {
      LOG_WARN("failed to calculate_distinct", K(ret));
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (T_WIN_FUN_ROW_NUMBER == win_expr->get_func_type()) {
      special_ndv = origin_rows/part_ndv;
    } else if ((T_FUN_COUNT == win_expr->get_func_type() && order_exprs.count() != 0) ||
                T_WIN_FUN_RANK == win_expr->get_func_type() ||
                T_WIN_FUN_DENSE_RANK == win_expr->get_func_type() ||
                T_WIN_FUN_PERCENT_RANK == win_expr->get_func_type() ||
                T_WIN_FUN_CUME_DIST == win_expr->get_func_type()) {
      special_ndv = scale_distinct(origin_rows/part_ndv, origin_rows, order_ndv);
    } else if (T_WIN_FUN_NTILE == win_expr->get_func_type()) {
      ObSEArray<ObRawExpr *, 4> param_exprs;
      ObRawExpr* const_expr = NULL;
      ObObj result,out_ptr;
      bool got_result = false;
      if (OB_FAIL(param_exprs.assign(win_expr->get_func_params()))) {
        LOG_WARN("fail to assign exprs", K(ret));
      } else if (param_exprs.count() == 0|| OB_ISNULL(const_expr = param_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(param_exprs.count()), K(const_expr), K(ret));
      } else if (const_expr->is_static_const_expr()) {
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                              const_expr,
                                                              result,
                                                              got_result,
                                                              ctx.get_allocator()))) {
          LOG_WARN("fail to calc_const_or_calculable_expr", K(ret));
        } else if (!got_result || result.is_null() || !ob_is_numeric_type(result.get_type())) {
          special_ndv = origin_rows/part_ndv;
        } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_scalar_obj(&result, &out_ptr))) {
          LOG_WARN("Failed to convert obj using old method", K(ret));
        } else {
          double scalar = static_cast<double>(out_ptr.get_double());
          special_ndv = std::min(origin_rows/part_ndv, scalar);
        }
      }
    } else if (T_FUN_MIN == win_expr->get_func_type()||
               T_FUN_MEDIAN == win_expr->get_func_type()||
               T_FUN_MAX == win_expr->get_func_type()) {
      ObSEArray<ObRawExpr *, 4> param_exprs;
      ObAggFunRawExpr* aggr_expr =  win_expr->get_agg_expr();
      double param_ndv = 1.0;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pointer", K(aggr_expr), K(ret));
      } else if (OB_FAIL(param_exprs.assign(aggr_expr->get_real_param_exprs()))) {
        LOG_WARN("fail to assign exprs", K(ret));
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv)))) {
        LOG_WARN("failed to calculate_distinct", K(ret));
      } else {
        special_ndv = std::min(part_order_ndv, param_ndv);
      }
    } else if (T_WIN_FUN_NTH_VALUE == win_expr->get_func_type() ||
               T_WIN_FUN_FIRST_VALUE == win_expr->get_func_type() ||
               T_WIN_FUN_LAST_VALUE == win_expr->get_func_type()) {
      ObSEArray<ObRawExpr *, 4> param_exprs;
      double param_ndv = 1.0;
      if (OB_FAIL(param_exprs.assign(win_expr->get_func_params()))) {
        LOG_WARN("fail to assign exprs", K(ret));
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv)))) {
        LOG_WARN("failed to calculate_distinct", K(ret));
      } else {
        special_ndv = std::min(part_order_ndv, param_ndv);
      }
    } else if (T_WIN_FUN_LEAD  == win_expr->get_func_type() ||
               T_WIN_FUN_LAG  == win_expr->get_func_type()) {
      ObSEArray<ObRawExpr *, 4> param_exprs;
      double param_ndv = 1.0;
      if (OB_FAIL(param_exprs.assign(win_expr->get_func_params()))) {
        LOG_WARN("fail to assign exprs", K(ret));
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv)))) {
        LOG_WARN("failed to calculate_distinct", K(ret));
      } else {
        special_ndv = param_ndv;
      }
    } else {
      special_ndv = part_order_ndv;
    }
    LOG_TRACE("calculate window function ndv", KPC(win_expr), K(special_ndv));
  }
  special_ndv = revise_ndv(special_ndv);
  return ret;
}

// 仅保留一个 ndv 最小的 distinct expr, 加入到 filtered_exprs 中;
// 再把不在 equal set 中的列加入到 filtered_exprs 中,
int ObOptSelectivity::filter_column_by_equal_set(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 DistinctEstType est_type,
                                                 const ObIArray<ObRawExpr*> &column_exprs,
                                                 ObIArray<ObRawExpr*> &filtered_exprs)
{
  int ret = OB_SUCCESS;
  const EqualSets *equal_sets = ctx.get_equal_sets();
  if (NULL == equal_sets || DistinctEstType::CURRENT != est_type) {
    if (OB_FAIL(append(filtered_exprs, column_exprs))) {
      LOG_WARN("failed to append filtered exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      bool find = false;
      double dummy = 0;
      ObRawExpr *filtered_expr = NULL;
      if (OB_FAIL(get_min_ndv_by_equal_set(table_metas,
                                           ctx,
                                           column_exprs.at(i),
                                           find,
                                           filtered_expr,
                                           dummy))) {
        LOG_WARN("failed to find the expr with min ndv", K(ret));
      } else if (!find && FALSE_IT(filtered_expr = column_exprs.at(i))) {
        // never reach
      } else if (OB_FAIL(add_var_to_array_no_dup(filtered_exprs, filtered_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObOptSelectivity::filter_one_column_by_equal_set(const OptTableMetas &table_metas,
                                                     const OptSelectivityCtx &ctx,
                                                     const ObRawExpr *column_expr,
                                                     const ObRawExpr *&filtered_expr)
{
  int ret = OB_SUCCESS;
  const EqualSets *equal_sets = ctx.get_equal_sets();
  if (NULL == equal_sets) {
    filtered_expr = column_expr;
  } else {
    bool find = false;
    double dummy = 0;
    ObRawExpr *tmp_expr = NULL;
    if (OB_FAIL(get_min_ndv_by_equal_set(table_metas,
                                         ctx,
                                         column_expr,
                                         find,
                                         tmp_expr,
                                         dummy))) {
      LOG_WARN("failed to find the expr with min ndv", K(ret));
    } else if (!find) {
      filtered_expr = column_expr;
    } else {
      filtered_expr = tmp_expr;
    }
  }
  return ret;
}

/**
 * Find the expr_ptr in the eq_sets that is equal to col_expr and has the minimum ndv
*/
int ObOptSelectivity::get_min_ndv_by_equal_set(const OptTableMetas &table_metas,
                                               const OptSelectivityCtx &ctx,
                                               const ObRawExpr *col_expr,
                                               bool &find,
                                               ObRawExpr *&expr,
                                               double &ndv)
{
  int ret = OB_SUCCESS;
  ObBitSet<> col_added;
  find = false;
  const EqualSets *eq_sets = ctx.get_equal_sets();
  if (OB_ISNULL(eq_sets) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!col_expr->is_column_ref_expr()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < eq_sets->count(); i++) {
      const ObRawExprSet *equal_set = eq_sets->at(i);
      if (OB_ISNULL(equal_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null equal set", K(ret));
      } else if (!ObOptimizerUtil::find_item(*equal_set, col_expr)) {
        //do nothing
      } else {
        find = true;
        int64_t min_idx = OB_INVALID_INDEX_INT64;
        double min_ndv = 0;
        for (int64_t k = 0; OB_SUCC(ret) && k < equal_set->count(); ++k) {
          double tmp_ndv = 0;
          bool is_in = false;
          if (OB_ISNULL(equal_set->at(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get column exprs", K(ret));
          } else if (!equal_set->at(k)->is_column_ref_expr()) {
            //do nothing
          } else if (OB_FAIL(column_in_current_level_stmt(ctx.get_stmt(),
                                                          *equal_set->at(k),
                                                          is_in))) {
            LOG_WARN("failed to check column in current level stmt", K(ret));
          } else if (!is_in) {
            //do nothing
          } else if (OB_FAIL(get_column_basic_info(table_metas,
                                                   ctx,
                                                   *equal_set->at(k),
                                                   &tmp_ndv,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   DistinctEstType::CURRENT))) {
            LOG_WARN("failed to get var basic sel", K(ret));
          } else if (OB_INVALID_INDEX_INT64 == min_idx || tmp_ndv < min_ndv) {
            min_idx = k;
            min_ndv = tmp_ndv;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (min_idx < 0 || min_idx >= equal_set->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect idx", K(min_idx), K(ret));
        } else {
          expr = equal_set->at(min_idx);
          ndv = min_ndv;
        }
      }
    }
  }
  return ret;
}

int ObOptSelectivity::is_columns_contain_pkey(const OptTableMetas &table_metas,
                                              const ObIArray<ObRawExpr *> &exprs,
                                              bool &is_pkey,
                                              bool &is_union_pkey,
                                              uint64_t *table_id_ptr)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> col_ids;
  uint64_t table_id = OB_INVALID_INDEX;
  if (OB_FAIL(extract_column_ids(exprs, col_ids, table_id))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(table_metas, col_ids, table_id, is_pkey, is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else {
    assign_value(table_id, table_id_ptr);
  }
  return ret;
}

int ObOptSelectivity::is_columns_contain_pkey(const OptTableMetas &table_metas,
                                              const ObIArray<uint64_t> &col_ids,
                                              const uint64_t table_id,
                                              bool &is_pkey,
                                              bool &is_union_pkey)
{
  int ret = OB_SUCCESS;
  is_pkey = true;
  is_union_pkey = false;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  if (OB_ISNULL(table_meta)) {
    is_pkey = false;
  } else if (table_meta->get_pkey_ids().empty()) {
    // 没有显式主键, 默认隐式主键不会作为选择条件
    is_pkey = false;
  } else {
    const ObIArray<uint64_t> &pkey_ids = table_meta->get_pkey_ids();
    for (int64_t i = 0; is_pkey && i < pkey_ids.count(); ++i) {
      bool find = false;
      for (int64_t j =0; !find && j < col_ids.count(); ++j) {
        if (pkey_ids.at(i) == col_ids.at(j)) {
          find = true;
        }
      }
      is_pkey = find;
    }
    is_union_pkey = pkey_ids.count() > 1;
  }
  return ret;
}

int ObOptSelectivity::extract_column_ids(const ObIArray<ObRawExpr *> &col_exprs,
                                         ObIArray<uint64_t> &col_ids,
                                         uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *column_expr = NULL;
  table_id = OB_INVALID_INDEX;
  bool from_same_table = true;
  for (int64_t i = 0; OB_SUCC(ret) && from_same_table && i < col_exprs.count(); ++i) {
    ObRawExpr *cur_expr = col_exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (!cur_expr->is_column_ref_expr()) {
      // do nothing
    } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr *>(cur_expr))) {
    } else if (OB_FAIL(col_ids.push_back(column_expr->get_column_id()))) {
      LOG_WARN("failed to push back column id", K(ret));
    } else if (OB_INVALID_INDEX == table_id) {
      table_id = column_expr->get_table_id();
    } else if (table_id != column_expr->get_table_id()) {
      from_same_table = false;
      table_id = OB_INVALID_INDEX;
    }
  }
  return ret;
}

int ObOptSelectivity::classify_quals_deprecated(const OptSelectivityCtx &ctx,
                                     const ObIArray<ObRawExpr*> &quals,
                                     ObIArray<ObExprSelPair> &all_predicate_sel,
                                     ObIArray<OptSelInfo> &column_sel_infos)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *qual = NULL;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  OptSelInfo *sel_info = NULL;
  double tmp_selectivity = 1.0;
  ObSelEstimatorFactory factory;
  ObSEArray<ObSelEstimator *, 4> range_estimators;
  if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    factory.get_allocator().set_tenant_id(ctx.get_session_info()->get_effective_tenant_id());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    column_exprs.reset();
    uint64_t column_id = OB_INVALID_ID;
    ObColumnRefRawExpr *column_expr = NULL;
    ObSelEstimator *range_estimator = NULL;
    if (OB_ISNULL(qual = quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(qual, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (1 == column_exprs.count()) {
      column_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
      column_id = column_expr->get_column_id();
      if (!qual->has_flag(CNT_DYNAMIC_PARAM) &&
          OB_FAIL(ObRangeSelEstimator::create_estimator(factory, ctx, *qual, range_estimator))) {
        LOG_WARN("failed to create estimator", K(ret));
      } else if (NULL != range_estimator &&
                 OB_FAIL(ObSelEstimator::append_estimators(range_estimators, range_estimator))) {
        LOG_WARN("failed to append estimators", K(ret));
      }
    } else {
      // use OB_INVALID_ID represent qual contain more than one column
    }

    if (OB_SUCC(ret) && OB_INVALID_ID != column_id && OB_NOT_NULL(column_expr)) {
      sel_info = NULL;
      int64_t offset = 0;
      if (OB_FAIL(get_opt_sel_info(column_sel_infos, column_expr->get_column_id(), sel_info))) {
        LOG_WARN("failed to get opt sel info", K(ret));
      } else if (sel_info->has_range_exprs_) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(all_predicate_sel,
                                            ObExprSelPair(column_expr, 0, true),
                                            &offset)) {
        sel_info->range_selectivity_ = all_predicate_sel.at(offset).sel_;
        sel_info->has_range_exprs_ = true;
      }
    }

    if (OB_SUCC(ret) && OB_INVALID_ID != column_id) {
      if (OB_LIKELY(get_qual_selectivity(all_predicate_sel, qual, tmp_selectivity))) {
        // parse qual and set sel info
        sel_info->selectivity_ *= tmp_selectivity;
        uint64_t temp_equal_count = 0;
        if (OB_FAIL(extract_equal_count(*qual, temp_equal_count))) {
          LOG_WARN("failed to extract equal count", K(ret));
        } else if (0 == sel_info->equal_count_) {
          sel_info->equal_count_ = temp_equal_count;
        } else if (temp_equal_count > 0) {
          sel_info->equal_count_ = std::min(sel_info->equal_count_, temp_equal_count);
        }
      } else {
        //do nothing, maybe from dynamic sampling.
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < range_estimators.count(); ++i) {
    column_exprs.reset();
    uint64_t column_id = OB_INVALID_ID;
    const ObColumnRefRawExpr *column_expr = NULL;
    ObRangeSelEstimator *range_estimator = NULL;
    ObObj obj_min;
    ObObj obj_max;
    if (OB_ISNULL(range_estimator = static_cast<ObRangeSelEstimator *>(range_estimators.at(i))) ||
        OB_UNLIKELY(ObSelEstType::COLUMN_RANGE != range_estimator->get_type()) ||
        OB_ISNULL(column_expr = range_estimator->get_column_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::get_column_range_min_max(
        ctx, column_expr, range_estimator->get_range_exprs(), obj_min, obj_max))) {
      LOG_WARN("failed to get min max", K(ret));
    } else if (OB_FAIL(get_opt_sel_info(column_sel_infos, column_expr->get_column_id(), sel_info))) {
      LOG_WARN("failed to get opt sel info", K(ret));
    } else {
      if (!obj_min.is_null()) {
        sel_info->min_ = obj_min;
      }
      if (!obj_max.is_null()) {
        sel_info->max_ = obj_max;
      }
    }
  }
  return ret;
}

int ObOptSelectivity::classify_quals(const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx,
                                     const ObIArray<ObRawExpr*> &quals,
                                     ObIArray<ObExprSelPair> &all_predicate_sel,
                                     ObIArray<OptSelInfo> &column_sel_infos)
{
  int ret = OB_SUCCESS;
  ObRawExpr *qual = NULL;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  OptSelInfo *sel_info = NULL;
  double tmp_selectivity = 1.0;
  ObSelEstimatorFactory factory;
  ObSEArray<ObSelEstimator *, 4> estimators;
  ObObj obj_min;
  ObObj obj_max;
  if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    factory.get_allocator().set_tenant_id(ctx.get_session_info()->get_effective_tenant_id());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    column_exprs.reset();
    uint64_t column_id = OB_INVALID_ID;
    ObColumnRefRawExpr *column_expr = NULL;
    ObSelEstimator *range_estimator = NULL;
    uint64_t temp_equal_count = 0;
    sel_info = NULL;
    if (OB_ISNULL(qual = quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(qual, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (1 == column_exprs.count()) {
      column_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
      column_id = column_expr->get_column_id();
      if (OB_FAIL(get_opt_sel_info(column_sel_infos, column_expr->get_column_id(), sel_info))) {
        LOG_WARN("failed to get opt sel info", K(ret));
      } else if (OB_FAIL(sel_info->quals_.push_back(qual))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(extract_equal_count(*qual, temp_equal_count))) {
        LOG_WARN("failed to extract equal count", K(ret));
      } else if (0 == sel_info->equal_count_) {
        sel_info->equal_count_ = temp_equal_count;
      } else if (temp_equal_count > 0) {
        sel_info->equal_count_ = std::min(sel_info->equal_count_, temp_equal_count);
      }
    } else {
      // use OB_INVALID_ID represent qual contain more than one column
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_sel_infos.count(); ++i) {
    estimators.reuse();
    OptSelInfo &sel_info = column_sel_infos.at(i);
    ObRangeSelEstimator *range_estimator = NULL;
    obj_min.set_min_value();
    obj_max.set_max_value();
    if (OB_FAIL(factory.create_estimators(ctx, sel_info.quals_, estimators))) {
      LOG_WARN("failed to create estimators", K(ret));
    } else if (OB_FAIL(calculate_selectivity(table_metas, ctx, estimators, sel_info.selectivity_, all_predicate_sel))) {
      LOG_WARN("failed to calc sel", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && NULL == range_estimator && j < estimators.count(); j ++) {
      if (OB_ISNULL(estimators.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(sel_info), K(estimators));
      } else if (ObSelEstType::COLUMN_RANGE == estimators.at(j)->get_type()) {
        range_estimator = static_cast<ObRangeSelEstimator *>(estimators.at(j));
        if (OB_FAIL(ObOptSelectivity::get_column_range_min_max(
            ctx, range_estimator->get_column_expr(), range_estimator->get_range_exprs(), obj_min, obj_max))) {
          LOG_WARN("failed to get min max", K(ret));
        } else {
          if (!obj_min.is_null()) {
            sel_info.min_ = obj_min;
          }
          if (!obj_max.is_null()) {
            sel_info.max_ = obj_max;
          }
        }
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_opt_sel_info(ObIArray<OptSelInfo> &column_sel_infos,
                                       const uint64_t column_id,
                                       OptSelInfo *&sel_info)
{
  int ret = OB_SUCCESS;
  sel_info = NULL;
  bool found = false;
  for (int64_t j = 0; !found && j < column_sel_infos.count(); ++j) {
    if (column_sel_infos.at(j).column_id_ == column_id) {
      sel_info = &column_sel_infos.at(j);
      found = true;
    }
  }
  if (NULL == sel_info) {
    if (OB_ISNULL(sel_info = column_sel_infos.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate place holder for sel info", K(ret));
    } else {
      sel_info->column_id_ = column_id;
    }
  }
  return ret;
}

bool ObOptSelectivity::get_qual_selectivity(ObIArray<ObExprSelPair> &all_predicate_sel,
                                            const ObRawExpr *qual,
                                            double &selectivity)
{
  bool find = false;
  selectivity = 1.0;
  for (int64_t i = 0; !find && i < all_predicate_sel.count(); ++i) {
    if (all_predicate_sel.at(i).expr_ == qual) {
      selectivity = all_predicate_sel.at(i).sel_;
      find = true;
    }
  }
  return find;
}

// parse qual to get equal condition number
int ObOptSelectivity::extract_equal_count(const ObRawExpr &qual, uint64_t &equal_count)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *l_expr = NULL;
  const ObRawExpr *r_expr = NULL;
  if (T_OP_EQ == qual.get_expr_type() || T_OP_NSEQ == qual.get_expr_type()) {
    if (OB_ISNULL(l_expr = qual.get_param_expr(0)) || OB_ISNULL(r_expr = qual.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if ((l_expr->is_column_ref_expr() &&
               r_expr->is_const_expr()) ||
               (r_expr->is_column_ref_expr() &&
               l_expr->is_const_expr())) {
      equal_count = 1;
    }
  } else if (T_OP_IN == qual.get_expr_type()) {
    if (OB_ISNULL(l_expr = qual.get_param_expr(0)) || OB_ISNULL(r_expr = qual.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if (l_expr->is_column_ref_expr() && T_OP_ROW == r_expr->get_expr_type()) {
      equal_count = r_expr->get_param_count();
    }
  } else if (T_OP_AND == qual.get_expr_type() || T_OP_OR == qual.get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < qual.get_param_count(); ++i) {
      uint64_t tmp_equal_count = 0;
      const ObRawExpr *child_expr = qual.get_param_expr(i);
      if (OB_ISNULL(child_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(extract_equal_count(*child_expr, tmp_equal_count))) {
        LOG_WARN("failed to extract equal count", K(ret));
      } else if (T_OP_AND == qual.get_expr_type()) {
        if (0 == equal_count) {
          equal_count = tmp_equal_count;
        } else if (tmp_equal_count > 0) {
          equal_count = std::min(equal_count, tmp_equal_count);
        }
      } else {
        equal_count += tmp_equal_count;
      }
    }
  } else { /* do nothing */ }
  return ret;
}

/**
 * 此公式参考自 Join Selectivity by Jonathan Lewis.
 * 可以理解为 ndv 个值平均分布在原始的行(rows)中, 然后从中选取部分行(selected_rows),
 * 公式的结果为选中的行的 non-distinct value (new_ndv).
 */
double ObOptSelectivity::scale_distinct(double selected_rows,
                                        double rows,
                                        double ndv)
{
  double new_ndv = ndv;
  if (selected_rows > rows) {
    // enlarge, ref:
    // - http://mysql.taobao.org/monthly/2016/05/09/
    // - Haas and Stokes in IBM Research Report RJ 10025
    // 根据 partition 信息缩放
    // 使用公式 n * d / (n - f1 + f1 * n/N)
    // 其中, N(selected_rows) 需要放大到的行数; n(rows) 当前的行数; f1 则是只出现一次的值的数据. 如果假设平均分布, 那么 f1 = d * 2 - n; d 则是 ndv。
    //
    ndv = (ndv > rows ? rows : ndv); // revise ndv
    double f1 = ndv * 2 - rows;
    if (f1 > 0) {
      new_ndv = (rows * ndv) / (rows - f1 + f1 * rows / selected_rows);
    }
  } else if (selected_rows < rows) {
    // 缩小, 参考 select_without_replacement
    if (ndv > OB_DOUBLE_EPSINON && rows > OB_DOUBLE_EPSINON) {
      new_ndv = ndv * (1 - std::pow(1 - selected_rows / rows, rows / ndv));
    }
    new_ndv = std::min(new_ndv, selected_rows);
  }
  new_ndv = revise_ndv(new_ndv);
  return new_ndv;
}

/**
  * compute the number of rows satisfying a equal join predicate `left_column = right_column`
  *
  * left_hist and right_hist must be frequency histogram
  * If is_semi is true, it means this is a semi join predicate.
  *
  */
int ObOptSelectivity::get_join_pred_rows(const ObHistogram &left_hist,
                                         const ObHistogram &right_hist,
                                         const ObJoinType join_type,
                                         double &rows)
{
  int ret = OB_SUCCESS;
  rows = 0;
  int64_t lidx = 0;
  int64_t ridx = 0;
  while (OB_SUCC(ret) && lidx < left_hist.get_bucket_size() && ridx < right_hist.get_bucket_size()) {
    int eq_cmp = 0;
    if (OB_FAIL(left_hist.get(lidx).endpoint_value_.compare(right_hist.get(ridx).endpoint_value_,
                                                            eq_cmp))) {
      LOG_WARN("failed to compare histogram endpoint value", K(ret),
              K(left_hist.get(lidx).endpoint_value_), K(right_hist.get(ridx).endpoint_value_));
    } else if (0 == eq_cmp) {
      if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
        rows += left_hist.get(lidx).endpoint_repeat_count_;
      } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
        rows += right_hist.get(ridx).endpoint_repeat_count_;
      } else {
        rows += left_hist.get(lidx).endpoint_repeat_count_ * right_hist.get(ridx).endpoint_repeat_count_;
      }
      ++lidx;
      ++ridx;
    } else if (eq_cmp > 0) {
      // left_endpoint_value > right_endpoint_value
      ++ridx;
    } else {
      ++lidx;
    }
  }
  return ret;
}

//will useful in dynamic sampling join in the future
// int ObOptSelectivity::calculate_join_selectivity_by_dynamic_sampling(const OptTableMetas &table_metas,
//                                                                      const OptSelectivityCtx &ctx,
//                                                                      const ObIArray<ObRawExpr*> &predicates,
//                                                                      double &selectivity,
//                                                                      bool &is_calculated)
// {
//   int ret = OB_SUCCESS;
//   ObOptDSJoinParam ds_join_param;
//   is_calculated = false;
//   if (OB_FAIL(collect_ds_join_param(table_metas, ctx, predicates, ds_join_param))) {
//     LOG_WARN("failed to collect ds join param", K(ret));
//   } else if (ds_join_param.is_valid()) {
//     ObArenaAllocator allocator("ObOpJoinDS", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_opt_ctx().get_session_info()->get_effective_tenant_id());
//     ObDynamicSampling dynamic_sampling(const_cast<ObOptimizerContext&>(ctx.get_opt_ctx()),
//                                           allocator,
//                                           ObDynamicSamplingType::OB_JOIN_DS);
//     uint64_t join_output_cnt = 0;
//     int64_t start_time = ObTimeUtility::current_time();
//     OPT_TRACE("begin to process join dynamic sampling estimation");
//     int tmp_ret = dynamic_sampling.estimate_join_rowcount(ds_join_param, join_output_cnt);
//     OPT_TRACE("end to process join dynamic sampling estimation", static_cast<int64_t>(tmp_ret), join_output_cnt);
//     if (OB_FAIL(tmp_ret)) {
//       if (tmp_ret == OB_TIMEOUT) {
//         LOG_INFO("failed to estimate join rowcount caused by timeout", K(start_time),
//                                                 K(ObTimeUtility::current_time()), K(ds_join_param));
//       } else {
//         ret = tmp_ret;
//         LOG_WARN("failed to estimate join rowcount by dynamic sampling", K(ret));
//       }
//     } else if (OB_UNLIKELY(join_output_cnt != 0 &&
//                            ctx.get_row_count_1() * ctx.get_row_count_2() == 0)) {
//       ret = OB_ERR_UNEXPECTED;
//       LOG_WARN("get unexpected error", K(ret), K(ctx.get_row_count_1()), K(ctx.get_row_count_2()));
//     } else {
//       selectivity = join_output_cnt == 0 ? 0 : revise_between_0_1(join_output_cnt / (ctx.get_row_count_1() * ctx.get_row_count_2()));
//       is_calculated = true;
//       LOG_TRACE("succeed to calculate join selectivity by dynamic sampling", K(selectivity),
//                                                        K(join_output_cnt), K(ctx.get_row_count_1()),
//                                                        K(ctx.get_row_count_2()), K(ds_join_param));
//     }
//   }
//   return ret;
// }

//maybe we can use basic table dynamic sampling info for join.
// int ObOptSelectivity::collect_ds_join_param(const OptTableMetas &table_metas,
//                                             const OptSelectivityCtx &ctx,
//                                             const ObIArray<ObRawExpr*> &predicates,
//                                             ObOptDSJoinParam &ds_join_param)
// {
//   int ret = OB_SUCCESS;
//   ObSEArray<uint64_t, 2> table_ids;
//   const ObDMLStmt *stmt = ctx.get_stmt();
//   bool no_use = false;
//   ObSEArray<ObRawExpr*, 4> tmp_raw_exprs;
//   if (OB_ISNULL(stmt)) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected null", K(ret), K(stmt));
//   } else if (ctx.get_join_type() != INNER_JOIN &&
//              ctx.get_join_type() != LEFT_OUTER_JOIN &&
//              ctx.get_join_type() != RIGHT_OUTER_JOIN &&
//              ctx.get_join_type() != FULL_OUTER_JOIN) {
//     //now dynamic sampling just for inner join and outer join
//   } else if (ObAccessPathEstimation::check_ds_can_use_filters(predicates, tmp_raw_exprs, no_use)) {
//     LOG_WARN("failed to check ds can use filters", K(ret));
//   } else if (no_use || predicates.empty()) {
//     //do nothing
//   } else if (OB_FAIL(ObRawExprUtils::extract_table_ids_from_exprs(predicates, table_ids))) {
//     LOG_WARN("failed to extract table ids from exprs", K(ret));
//   } else if (OB_UNLIKELY(table_ids.count() != 2)) {
//     //do nothing, just skip.
//     LOG_TRACE("get unexpected table cnt from join conditicons, can't use join dynamic sampling",
//                                                        K(table_ids), K(table_metas), K(predicates));
//   // } else if (OB_FAIL(ObAccessPathEstimation::get_dynamic_sampling_max_timeout(ctx.get_opt_ctx(),
//   //                                                                 ds_join_param.max_ds_timeout_))) {
//   //   LOG_WARN("failed to get dynamic sampling max timeout", K(ret));
//   } else {
//     bool succ_to_get_param = true;
//     ds_join_param.join_type_ = ctx.get_join_type();
//     ds_join_param.join_conditions_ = &predicates;
//     ds_join_param.max_ds_timeout_ = ctx.get_opt_ctx().get_max_ds_timeout();
//     for (int64_t i = 0; OB_SUCC(ret) && succ_to_get_param && i < table_ids.count(); ++i) {
//       ObOptDSBaseParam &base_param = (i == 0 ? ds_join_param.left_table_param_ :
//                                                ds_join_param.right_table_param_);
//       const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_ids.at(i));
//       const TableItem *table_item = stmt->get_table_item_by_id(table_ids.at(i));
//       if (OB_ISNULL(table_meta) || OB_ISNULL(table_item) ||
//           OB_UNLIKELY(OB_INVALID_ID == table_meta->get_ref_table_id())) {
//         succ_to_get_param = false;
//         LOG_TRACE("get invalid table info, can't use dynamic sampling", KPC(table_meta),
//                                                                         KPC(table_item));
//       } else if (table_meta->get_ds_base_param().is_valid()) {
//         if (table_meta->get_ds_base_param().ds_level_ != ObDynamicSamplingLevel::ADS_DYNAMIC_SAMPLING) {
//           succ_to_get_param = false;
//         } else if (OB_FAIL(base_param.assign(table_meta->get_ds_base_param()))) {
//           LOG_WARN("failed to assign", K(ret));
//         } else {
//           base_param.index_id_ = table_meta->get_ref_table_id();//reset the index id
//           LOG_TRACE("succed get base param for join dynamic sampling", K(base_param));
//         }
//       } else {
//         succ_to_get_param = false;
//       }
//     }
//   }
//   return ret;
// }


int ObOptSelectivity::remove_ignorable_func_for_est_sel(const ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool is_ignorable = true;
  while(OB_SUCC(ret) && is_ignorable) {
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (T_FUN_SYS_CAST == expr->get_expr_type() ||
               T_FUN_SYS_CONVERT == expr->get_expr_type() ||
               T_FUN_SYS_TO_DATE == expr->get_expr_type() ||
               T_FUN_SYS_TO_CHAR == expr->get_expr_type() ||
               T_FUN_SYS_TO_NCHAR == expr->get_expr_type() ||
               T_FUN_SYS_TO_NUMBER == expr->get_expr_type() ||
               T_FUN_SYS_TO_BINARY_FLOAT == expr->get_expr_type() ||
               T_FUN_SYS_TO_BINARY_DOUBLE == expr->get_expr_type() ||
               T_FUN_SYS_SET_COLLATION == expr->get_expr_type() ||
               T_FUN_SYS_TO_TIMESTAMP == expr->get_expr_type() ||
               T_FUN_SYS_TO_TIMESTAMP_TZ  == expr->get_expr_type()) {
      if (OB_UNLIKELY(1 > expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param count", K(ret), KPC(expr));
      } else {
        expr = expr->get_param_expr(0);
      }
    } else {
      is_ignorable = false;
    }
  }
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  }
  return ret;
}

int ObOptSelectivity::remove_ignorable_func_for_est_sel(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *const_expr = expr;
  ret = remove_ignorable_func_for_est_sel(const_expr);
  expr = const_cast<ObRawExpr *>(const_expr);
  return ret;
}

double ObOptSelectivity::get_set_stmt_output_count(double count1, double count2, ObSelectStmt::SetOperator set_type)
{
  double output_count = 0.0;
  // we consider the worst-case scenario
  switch (set_type) {
    // Assuming there are no identical values in both branches.
    case ObSelectStmt::SetOperator::UNION:     output_count = count1 + count2; break;
    // Assuming that all values appear as much as possible in both branches
    case ObSelectStmt::SetOperator::INTERSECT: output_count = std::min(count1, count2); break;
    // Assuming that none of the values in the right branch appear in the left branch
    case ObSelectStmt::SetOperator::EXCEPT:    output_count = count1; break;
    // Assuming the ratio between the output rowcount in each iteration and the previous iteration remains constant.
    // And the recursion branch continues until the number of rows exceeds 100 times the number of rows in the non-recursive branch and does not exceed 7 iterations.
    case ObSelectStmt::SetOperator::RECURSIVE: {
      count1 = std::max(1.0, count1);
      output_count = count1;
      double recursive_count = count2;
      int64_t i = 0;
      do {
        output_count += recursive_count;
        recursive_count *= count2 / count1;
        i ++;
      } while (i < 7 && output_count <= 100 * count1);
      break;
    }
    default: output_count = count1 + count2; break;
  }
  return output_count;
}

int ObOptSelectivity::calculate_expr_avg_len(const OptTableMetas &table_metas,
                                             const OptSelectivityCtx &ctx,
                                             const ObRawExpr *expr,
                                             double &avg_len)
{
  int ret = OB_SUCCESS;
  // default
  avg_len = ObOptEstCost::get_estimate_width_from_type(expr->get_result_type());
  const OptDynamicExprMeta *dynamic_expr_meta = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(get_column_avg_len(table_metas, ctx, expr, avg_len))) {
      LOG_WARN("failed to get avg len", K(ret));
    }
  } else if (expr->is_static_scalar_const_expr()) {
    ObObj value;
    bool get_value = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                          expr,
                                                          value,
                                                          get_value,
                                                          ctx.get_allocator()))) {
      LOG_WARN("Failed to get const or calculable expr value", K(ret));
    } else if (get_value) {
      avg_len = ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH + value.get_deep_copy_size();
    }
  } else if (T_OP_CNN == expr->get_expr_type()) {
    avg_len = ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i ++) {
      double child_len = 0;
      if (OB_FAIL(SMART_CALL(calculate_expr_avg_len(table_metas, ctx, expr->get_param_expr(i), child_len)))) {
        LOG_WARN("failed to calc child avg len", K(ret), KPC(expr));
      } else {
        avg_len += child_len - ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
      }
    }
  } else if (expr->is_sys_func_expr()) {
    if (T_FUN_SYS_REPLACE == expr->get_expr_type() ||
        (T_FUN_SYS_CAST == expr->get_expr_type() && CM_IS_IMPLICIT_CAST(expr->get_extra()) )) {
      if (OB_UNLIKELY(expr->get_param_count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr param", KPC(expr));
      } else if (OB_FAIL(SMART_CALL(calculate_expr_avg_len(table_metas, ctx, expr->get_param_expr(0), avg_len)))) {
        LOG_WARN("failed to calc child avg len", K(ret), KPC(expr));
      }
    } else if (T_FUN_SYS_SUBSTR == expr->get_expr_type() || T_FUN_SYS_SUBSTRB == expr->get_expr_type()) {
      double pos = 1;
      double sub_len = -1;
      double child_len = 0;
      ObObj value;
      bool get_value = false;
      if (OB_UNLIKELY(expr->get_param_count() < 2) ||
          OB_ISNULL(expr->get_param_expr(1)) ||
          (expr->get_param_count() == 3 && OB_ISNULL(expr->get_param_expr(2)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr param", KPC(expr));
      } else if (OB_FAIL(SMART_CALL(calculate_expr_avg_len(table_metas, ctx, expr->get_param_expr(0), child_len)))) {
        LOG_WARN("failed to calc child avg len", K(ret), KPC(expr));
      } else if (expr->get_param_expr(1)->is_static_scalar_const_expr()) {
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                              expr->get_param_expr(1),
                                                              value,
                                                              get_value,
                                                              ctx.get_allocator()))) {
          LOG_WARN("Failed to get const or calculable expr value", K(ret));
        } else if (!get_value) {
          // do nothing
        } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&value, pos))) {
          LOG_WARN("failed to convert obj to double", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (3 == expr->get_param_count() && expr->get_param_expr(2)->is_static_scalar_const_expr()) {
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                              expr->get_param_expr(2),
                                                              value,
                                                              get_value,
                                                              ctx.get_allocator()))) {
          LOG_WARN("Failed to get const or calculable expr value", K(ret));
        } else if (!get_value) {
          // do nothing
        } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&value, sub_len))) {
          LOG_WARN("failed to convert obj to double", K(ret));
        }
      }
      avg_len = child_len;
      if (OB_SUCC(ret)) {
        avg_len -= ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
        if (pos <= 0) {
          avg_len = std::min(avg_len, -pos);
        } else if (pos >= 1) {
          avg_len -= pos - 1;
        }
        if (sub_len >= 0) {
          avg_len = std::min(avg_len, sub_len);
        }
        avg_len += ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
      }
    }
  } else if (OB_NOT_NULL(dynamic_expr_meta = table_metas.get_dynamic_expr_meta(expr))) {
    avg_len = dynamic_expr_meta->get_avg_len();
  } else if (expr->is_exec_param_expr()) {
    if (OB_FAIL(SMART_CALL(calculate_expr_avg_len(
        table_metas, ctx, static_cast<const ObExecParamRawExpr *>(expr)->get_ref_expr(), avg_len)))) {
       LOG_WARN("failed to calc ref avg len", K(ret), KPC(expr));
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObOptSelectivity::get_column_avg_len(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr *expr,
                                         double &avg_len)
{
  int ret = OB_SUCCESS;
  const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr *>(expr);
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_column_ref_expr()) ||
      OB_ISNULL(ctx.get_opt_stat_manager()) ||
      OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null param", K(ret), KPC(expr), K(ctx.get_opt_stat_manager()));
  } else {
    uint64_t table_id = column_expr->get_table_id();
    uint64_t column_id = column_expr->get_column_id();
    uint64_t ref_table_id;
    ObSEArray<int64_t, 1> part_ids;
    ObSEArray<int64_t, 1> global_part_ids;
    const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
    ObGlobalColumnStat stat;
    TableItem *table = NULL;
    avg_len = ObOptEstCost::get_estimate_width_from_type(expr->get_result_type());
    if (OB_NOT_NULL(table_meta)) {
      const OptColumnMeta *column_meta = table_meta->get_column_meta(column_id);
      if (OB_NOT_NULL(column_meta)) {
        avg_len = column_meta->get_avg_len();
      }
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_substrb_info(const OptTableMetas &table_metas,
                                             const OptSelectivityCtx &ctx,
                                             const ObRawExpr *str_expr,
                                             const double substrb_len,
                                             const double cur_rows,
                                             double &substr_ndv,
                                             double &nns,
                                             DistinctEstType est_type)
{
  int ret = OB_SUCCESS;
  double expr_len = 0.0;
  double expr_ndv = 0.0;
  nns = 1.0;
  substr_ndv = 1.0;
  if (OB_ISNULL(str_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (substrb_len <= 0) {
    substr_ndv = 1.0;
  } else if (OB_FAIL(calculate_expr_avg_len(table_metas,
                                            ctx,
                                            str_expr,
                                            expr_len))) {
    LOG_WARN("failed to get expr length", K(ret), KPC(str_expr));
  } else if (OB_FAIL(calculate_distinct(table_metas,
                                        ctx,
                                        *str_expr,
                                        cur_rows,
                                        expr_ndv,
                                        true,
                                        est_type))) {
    LOG_WARN("failed to calculate distinct", K(ret));
  } else if (OB_FAIL(calculate_expr_nns(table_metas, ctx, str_expr, nns))) {
    LOG_WARN("failed to calculate nns", KPC(str_expr));
  } else {
    expr_len -= ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
    if (nns > OB_DOUBLE_EPSINON) {
      expr_len /= nns;
    }
    if (expr_len < OB_DOUBLE_EPSINON || expr_len <= substrb_len) {
      substr_ndv = expr_ndv;
    } else {
      substr_ndv = std::pow(expr_ndv, substrb_len / expr_len);
    }
  }
  LOG_TRACE("succeed to calculate substrb ndv", K(substr_ndv), K(expr_ndv), K(substrb_len), K(expr_len));
  return ret;
}

int ObOptSelectivity::calculate_expr_nns(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObRawExpr *expr,
                                         double &nns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *expr, NULL, &nns))) {
      LOG_WARN("failed to get column ndv and nns", K(ret));
    }
  } else {
    // todo: wuyuming.wym null propagate expr
    nns = 1.0;
  }
  return ret;
}

int ObOptSelectivity::calc_expr_min_max(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr *expr,
                                        ObObj &min_value,
                                        ObObj &max_value)
{
  int ret = OB_SUCCESS;
  min_value.set_min_value();
  max_value.set_max_value();
  if (OB_ISNULL(expr) || OB_ISNULL(ctx.get_opt_ctx().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (expr->get_result_type().is_ext()) {
    // do nothing
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(ObOptSelectivity::get_column_min_max(table_metas, ctx, *expr,
                                                     min_value, max_value))) {
      LOG_WARN("failed to get min max", K(ret));
    }
  } else if (expr->is_static_scalar_const_expr()) {
    ObObj result;
    bool got_result = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                          expr,
                                                          result,
                                                          got_result,
                                                          ctx.get_allocator()))) {
      LOG_WARN("fail to calc_const_or_calculable_expr", K(ret));
    } else if (!got_result || result.is_null()) {
      // do nothing
    } else {
      min_value = result;
      max_value = result;
    }
  } else if (expr->get_param_count() < 1) {
    // do nothing
  } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
    bool is_monotonic = false;
    const ObRawExpr *param_expr = expr->get_param_expr(0);
    if (OB_FAIL(ObObjCaster::is_cast_monotonic(param_expr->get_data_type(), expr->get_data_type(), is_monotonic))) {
      LOG_WARN("check cast monotonic error", KPC(expr), K(ret));
    } else if (!is_monotonic) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(calc_expr_min_max(table_metas, ctx, param_expr,
                                                    min_value, max_value)))) {
      LOG_WARN("failed to calc date min max", KPC(expr));
    } else if (min_value.is_min_value() || max_value.is_min_value() ||
               min_value.is_max_value() || max_value.is_max_value() ||
               min_value.is_null() || max_value.is_null()) {
      // do nothing
    } else if (OB_FAIL(convert_obj_to_expr_type(ctx, expr, expr->get_extra(), min_value))) {
      ret = OB_SUCCESS;
      min_value.set_min_value();
    } else if (OB_FAIL(convert_obj_to_expr_type(ctx, expr, expr->get_extra(), max_value))) {
      ret = OB_SUCCESS;
      max_value.set_max_value();
    }
  } else if (T_FUN_SYS_DATE == expr->get_expr_type()) {
    if (OB_FAIL(SMART_CALL(calc_expr_min_max(table_metas, ctx, expr->get_param_expr(0),
                                             min_value, max_value)))) {
      LOG_WARN("failed to calc date min max", KPC(expr));
    }
  } else if (is_dense_time_expr_type(expr->get_expr_type()) ||
             T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
    const ObRawExpr *param_expr = NULL;
    ObItemType est_type = expr->get_expr_type();
    int64_t extract_type = DATE_UNIT_MAX;
    if (T_FUN_SYS_EXTRACT == expr->get_expr_type()) {
      bool valid = false;
      ObObj result;
      param_expr = expr->get_param_expr(1);
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                            expr->get_param_expr(0),
                                                            result,
                                                            valid,
                                                            ctx.get_allocator()))) {
        LOG_WARN("fail to calc_const_or_calculable_expr", K(ret));
      } else if (valid && result.is_int()) {
        extract_type = result.get_int();
        switch (extract_type) {
          case DATE_UNIT_DAY: est_type = T_FUN_SYS_DAY; break;
          case DATE_UNIT_WEEK: est_type = T_FUN_SYS_WEEK; break;
          case DATE_UNIT_MONTH: est_type = T_FUN_SYS_MONTH; break;
          case DATE_UNIT_QUARTER: est_type = T_FUN_SYS_QUARTER; break;
          case DATE_UNIT_SECOND: est_type = T_FUN_SYS_SECOND; break;
          case DATE_UNIT_MINUTE: est_type = T_FUN_SYS_MINUTE; break;
          case DATE_UNIT_HOUR: est_type = T_FUN_SYS_HOUR; break;
          case DATE_UNIT_YEAR: est_type = T_FUN_SYS_YEAR; break;
           // we only estimate the min/max value by year, so yearmonth is similar with yearweek
          case DATE_UNIT_YEAR_MONTH: est_type = T_FUN_SYS_YEARWEEK_OF_DATE; break;
          default: break;
        }
      }
    } else {
      param_expr = expr->get_param_expr(0);
    }
    if (OB_SUCC(ret)) {
      bool use_default = false;
      int64_t min_int_value = 0;
      int64_t max_int_value = 0;
      switch (est_type) {
        case T_FUN_SYS_MONTH:           min_int_value = 1; max_int_value = 12;  break;
        case T_FUN_SYS_DAY_OF_MONTH:
        case T_FUN_SYS_DAY:             min_int_value = 1; max_int_value = 31;  break;
        case T_FUN_SYS_DAY_OF_YEAR:     min_int_value = 1; max_int_value = 366; break;
        case T_FUN_SYS_WEEK_OF_YEAR:
        case T_FUN_SYS_WEEK:            min_int_value = 0; max_int_value = 53;  break;
        case T_FUN_SYS_WEEKDAY_OF_DATE: min_int_value = 0; max_int_value = 6;   break;
        case T_FUN_SYS_DAY_OF_WEEK:     min_int_value = 1; max_int_value = 7;   break;
        case T_FUN_SYS_QUARTER:         min_int_value = 1; max_int_value = 4;   break;
        case T_FUN_SYS_HOUR:            min_int_value = 0; max_int_value = 23;  break;
        case T_FUN_SYS_MINUTE:          min_int_value = 0; max_int_value = 59;  break;
        case T_FUN_SYS_SECOND:          min_int_value = 0; max_int_value = 59;  break;
        case T_FUN_SYS_YEAR:
        case T_FUN_SYS_YEARWEEK_OF_DATE: {
          if (OB_FAIL(calc_year_min_max(table_metas,
                                        ctx,
                                        param_expr,
                                        min_int_value,
                                        max_int_value,
                                        use_default))) {
            LOG_WARN("failed to calculate expr min max", K(ret));
          } else if (!use_default && T_FUN_SYS_YEARWEEK_OF_DATE == est_type) {
            // approximately
            min_int_value = min_int_value * 100;
            max_int_value = max_int_value * 100 + 100;
          }
          break;
        }
        default: use_default = true;  break;
      }
      if (OB_SUCC(ret) && !use_default) {
        min_value.set_int(min_int_value);
        max_value.set_int(max_int_value);
        if (OB_FAIL(convert_obj_to_expr_type(ctx, expr, CM_NONE, min_value))) {
          LOG_WARN("failed to convert obj", K(ret), K(min_value));
        } else if (OB_FAIL(convert_obj_to_expr_type(ctx, expr, CM_NONE, min_value))) {
          LOG_WARN("failed to convert obj", K(ret), K(max_value));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int cmp_result = 0;
    bool can_cmp = min_value.can_compare(max_value);
    if (min_value.is_min_value() || max_value.is_max_value()) {
      // do nothing
    } else if (can_cmp &&
        OB_FAIL(min_value.compare(max_value, cmp_result))) {
      ret = OB_SUCCESS;
      min_value.set_min_value();
      max_value.set_max_value();
    } else if (!can_cmp || 1 == cmp_result ||
               min_value.is_null() || max_value.is_null()) {
      min_value.set_min_value();
      max_value.set_max_value();
    }
  }
  return ret;
}

int ObOptSelectivity::calc_year_min_max(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr *expr,
                                        int64_t &min_year,
                                        int64_t &max_year,
                                        bool &use_default)
{
  int ret = OB_SUCCESS;
  ObObj min_value;
  ObObj max_value;
  ObObj min_year_obj;
  ObObj max_year_obj;
  ObTime min_time;
  ObTime max_time;
  use_default = false;
  ObDateSqlMode date_sql_mode;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx.get_session_info()) || OB_ISNULL(ctx.get_opt_ctx().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ObDateTimeTC != expr->get_type_class() &&
             ObDateTC != expr->get_type_class() &&
             ObMySQLDateTC != expr->get_type_class() &&
             ObMySQLDateTimeTC != expr->get_type_class() &&
             ObOTimestampTC != expr->get_type_class()) {
    use_default = true;
  } else if (OB_FAIL(SMART_CALL(calc_expr_min_max(table_metas,
                                                  ctx,
                                                  expr,
                                                  min_value,
                                                  max_value)) )) {
    LOG_WARN("failed to calculate expr min max", K(ret));
  } else if (min_value.is_min_value() || max_value.is_max_value()) {
    use_default = true;
  } else if (FALSE_IT(date_sql_mode.init(ctx.get_session_info()->get_sql_mode()))) {
  } else if (OB_FAIL(ob_obj_to_ob_time_with_date(min_value,
                                                 get_timezone_info(ctx.get_session_info()),
                                                 min_time,
                                                 get_cur_time(ctx.get_opt_ctx().get_exec_ctx()->get_physical_plan_ctx()),
                                                 date_sql_mode))) {
    ret = OB_SUCCESS;
    use_default = true;
  } else if (OB_FAIL(ob_obj_to_ob_time_with_date(max_value,
                                                 get_timezone_info(ctx.get_session_info()),
                                                 max_time,
                                                 get_cur_time(ctx.get_opt_ctx().get_exec_ctx()->get_physical_plan_ctx()),
                                                 date_sql_mode))) {
    ret = OB_SUCCESS;
    use_default = true;
  } else {
    min_year = min_time.parts_[DT_YEAR];
    max_year = max_time.parts_[DT_YEAR];
  }
  return ret;
}

int ObOptSelectivity::calc_const_numeric_value(const OptSelectivityCtx &ctx,
                                               const ObRawExpr *expr,
                                               double &value,
                                               bool &succ)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool got_result = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!expr->is_static_scalar_const_expr()) {
    succ = false;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               expr,
                                                               result,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("fail to calc_const_or_calculable_expr", K(ret));
  } else if (!got_result || result.is_null() || !ob_is_numeric_type(result.get_type())) {
    succ = false;
  } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&result, value))) {
    LOG_WARN("Failed to convert obj using old method", K(ret));
  } else {
    succ = true;
  }
  return ret;
}

int ObOptSelectivity::convert_obj_to_expr_type(const OptSelectivityCtx &ctx,
                                               const ObRawExpr *expr,
                                               ObCastMode cast_mode,
                                               ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx.get_opt_ctx().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(expr));
  } else {
    ObObj tmp;
    const ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(ctx.get_session_info());
    ObCastCtx cast_ctx(&ctx.get_allocator(),
                       &dtc_params,
                       get_cur_time(ctx.get_opt_ctx().get_exec_ctx()->get_physical_plan_ctx()),
                       cast_mode,
                       expr->get_result_type().get_collation_type());
    ObAccuracy res_acc;
    if (expr->get_result_type().is_decimal_int()) {
      res_acc = expr->get_result_type().get_accuracy();
      cast_ctx.res_accuracy_ = &res_acc;
    }
    if (OB_FAIL(ObObjCaster::to_type(expr->get_result_type().get_type(),
                                     expr->get_result_type().get_collation_type(),
                                     cast_ctx,
                                     obj,
                                     tmp))) {
      LOG_WARN("failed to cast value", K(ret));
    } else {
      obj = tmp;
    }
  }
  return ret;
}

double ObOptSelectivity::calc_equal_filter_sel(const OptSelectivityCtx &ctx,
                                               bool is_same_expr,
                                               ObItemType op_type,
                                               double left_ndv,
                                               double right_ndv,
                                               double left_nns,
                                               double right_nns)
{
  double selectivity = 0.0;
  if (is_same_expr) {
    // same table same column
    if (T_OP_NSEQ == op_type) {
      selectivity = 1.0;
    } else if (T_OP_EQ == op_type) {
      selectivity = left_nns;
    } else if (T_OP_NE == op_type) {
      selectivity = 0.0;
    }
  } else if (ctx.check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                          COMPAT_VERSION_4_3_3)) {
    double combine_ndv = combine_two_ndvs(ctx.get_current_rows(), left_ndv, right_ndv);
    combine_ndv = std::max(1.0, combine_ndv);
    selectivity = std::min(left_ndv, right_ndv) / combine_ndv;
    if (T_OP_NSEQ == op_type) {
      selectivity = selectivity * left_nns * right_nns + (1 - left_nns) * (1 - right_nns);
    } else if (T_OP_EQ == op_type) {
      selectivity = selectivity * left_nns * right_nns;
    } else if (T_OP_NE == op_type) {
      selectivity = std::max(1 - selectivity, 1 / combine_ndv / 2.0) * left_nns * right_nns ;
    }
  } else {
    // deprecated
    if (T_OP_NSEQ == op_type) {
      selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv)
          + (1 - left_nns) * (1 - right_nns);
    } else if (T_OP_EQ == op_type) {
      selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv);
    } else if (T_OP_NE == op_type) {
      selectivity = left_nns * right_nns * (1 - 1/std::max(left_ndv, right_ndv));
    }
  }

  return selectivity;
}

double ObOptSelectivity::calc_equal_join_sel(const OptSelectivityCtx &ctx,
                                             ObItemType op_type,
                                             double left_ndv,
                                             double right_ndv,
                                             double left_nns,
                                             double right_nns,
                                             double left_base_ndv,
                                             double right_base_ndv)

{
  double selectivity = 0.0;
  ObJoinType join_type = ctx.get_join_type();
  left_base_ndv = MAX(left_ndv, left_base_ndv);
  right_base_ndv = MAX(right_ndv, right_base_ndv);
  if (IS_RIGHT_STYLE_JOIN(join_type)) {
    std::swap(left_ndv, right_ndv);
    std::swap(left_nns, right_nns);
    std::swap(left_base_ndv, right_base_ndv);
  }
  if (T_OP_NE == op_type && IS_SEMI_ANTI_JOIN(join_type)) {
    if (right_ndv > 1.0) {
      // if right ndv > 1.0, then there must exist one value not equal to left value
      selectivity = left_nns;
    } else {
      selectivity = left_nns * std::max(1 - 1 / left_ndv, 1 / left_ndv);
    }
  } else if (IS_ANTI_JOIN(ctx.get_join_type()) && ctx.check_opt_compat_version(COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0,
                                                                               COMPAT_VERSION_4_3_5)) {
    // use base containment assumption only for anti join
    selectivity = (std::min(left_base_ndv, right_base_ndv) / left_base_ndv) * (right_ndv / right_base_ndv) * left_nns;
    if (T_OP_NSEQ == op_type && 1 - right_nns > 0) {
      selectivity += (1 - left_nns);
    }
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    selectivity = (std::min(left_ndv, right_ndv) / left_ndv) * left_nns;
    if (T_OP_NSEQ == op_type && 1 - right_nns > 0) {
      selectivity += (1 - left_nns);
    }
  } else {
    // inner/outer join
    double max_ndv = std::max(left_ndv, right_ndv);
    if (T_OP_NSEQ == op_type) {
      selectivity = left_nns * right_nns / max_ndv + (1 - left_nns) * (1 - right_nns);
    } else if (T_OP_EQ == op_type) {
      selectivity = left_nns * right_nns / max_ndv;
    } else if (T_OP_NE == op_type) {
      selectivity = left_nns * right_nns * std::max(1 - 1 / max_ndv, 1 / max_ndv);
    }
  }
  if (IS_ANTI_JOIN(ctx.get_join_type())) {
    selectivity = std::min(selectivity, 1 - DEFAULT_ANTI_JOIN_SEL);
  }
  return selectivity;
}

int ObOptSelectivity::calc_expr_basic_info(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObRawExpr *expr,
                                           double *ndv,
                                           double *nns,
                                           double *base_ndv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(remove_ignorable_func_for_est_sel(expr))) {
    LOG_WARN("failed to remove ignorable function", K(ret));
  } else if (expr->is_column_ref_expr()) {
    double num_null = 0;
    double min_ndv = 0;
    if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *expr, ndv, nns))) {
      LOG_WARN("failed to get ndv and nns", K(ret));
    } else if ((OB_NOT_NULL(ndv) || OB_NOT_NULL(nns)) && OB_NOT_NULL(ctx.get_equal_sets())) {
      bool find = false;
      ObRawExpr *dummy = NULL;
      if (OB_FAIL(get_min_ndv_by_equal_set(table_metas,
                                           ctx,
                                           expr,
                                           find,
                                           dummy,
                                           min_ndv))) {
        LOG_WARN("failed to find the expr with min ndv", K(ret));
      } else if (find) {
        assign_value(min_ndv, ndv);
        assign_value(1.0, nns);
      }
    }
    if (OB_NOT_NULL(base_ndv)) {
      if (OB_FAIL(get_column_basic_info(
              table_metas, ctx, *expr, base_ndv, NULL, NULL, NULL, DistinctEstType::BASE))) {
        LOG_WARN("failed to get base ndv", K(ret), KPC(expr));
      }
    }
  } else {
    if (OB_NOT_NULL(ndv) &&
        OB_FAIL(calculate_distinct(table_metas,
                                   ctx,
                                   *expr,
                                   ctx.get_current_rows(),
                                   *ndv))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    } else if (OB_NOT_NULL(nns) &&
               OB_FAIL(calculate_expr_nns(table_metas,
                                          ctx,
                                          expr,
                                          *nns))) {
      LOG_WARN("failed to calculate nns", K(ret));
    } else if (OB_NOT_NULL(base_ndv) &&
               OB_FAIL(calculate_distinct(table_metas,
                                          ctx,
                                          *expr,
                                          ctx.get_current_rows(),
                                          *base_ndv,
                                          false,
                                          DistinctEstType::BASE))) {
      LOG_WARN("Failed to calculate distinct", K(ret));
    }
  }
  return ret;
}

int ObHistSelHelper::init(const OptTableMetas &table_metas,
                          const OptSelectivityCtx &ctx,
                          const ObColumnRefRawExpr &col)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = col.get_table_id();
  uint64_t column_id = col.get_column_id();
  column_expr_ = &col;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  is_valid_ = false;
  handlers_.reuse();
  part_rows_.reuse();
  if (OB_ISNULL(table_meta) || OB_INVALID_ID == table_meta->get_ref_table_id()) {
    // do nothing
  } else if (NULL == ctx.get_opt_stat_manager() ||
             !table_meta->use_opt_stat() ||
             table_meta->get_hist_parts().empty()) {
    // do nothing
  } else if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_session_info()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_meta->get_hist_parts().count(); i ++) {
      ObOptColumnStatHandle column_stat;
      ObGlobalTableStat stat;
      if (OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(
                    ctx.get_session_info()->get_effective_tenant_id(),
                    table_meta->get_ref_table_id(),
                    table_meta->get_hist_parts().at(i),
                    column_id,
                    column_stat))) {
        LOG_WARN("failed to get column stat", K(ret));
      } else if (column_stat.stat_ == NULL ||
                column_stat.stat_->get_last_analyzed() <= 0 ||
                !column_stat.stat_->get_histogram().is_valid()) {
        // do nothing
      } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_table_stat(
                    ctx.get_session_info()->get_effective_tenant_id(),
                    table_meta->get_ref_table_id(),
                    table_meta->get_hist_parts().at(i),
                    1.0,
                    stat))) {
        LOG_WARN("failed to get table stats", K(ret));
      } else if (OB_FAIL(handlers_.push_back(column_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(part_rows_.push_back(stat.get_row_count()))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        is_valid_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && is_valid_) {
    const OptColumnMeta *column_meta = table_meta->get_column_meta(column_id);
    if (OB_ISNULL(column_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column meta not find", K(ret), KPC(table_meta), K(column_id));
    } else {
      hist_scale_ = column_meta->get_hist_scale();
      double distinct_sel = 1.0 / std::max(1.0, column_meta->get_base_ndv());
      if (handlers_.count() == 1) {
        density_ = std::min(handlers_.at(0).stat_->get_histogram().get_density(),
                            distinct_sel);
      } else {
        density_ = distinct_sel;
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_valid_ = false;
  }
  return ret;
}

int ObHistSelHelper::get_sel(const OptSelectivityCtx &ctx,
                             double &sel)
{
  int ret = OB_SUCCESS;
  ObObj expr_value;
  double total_part_rows = 0;
  double rows = 0;
  bool is_rare = true;
  sel = 1.0;
  if (OB_UNLIKELY(handlers_.empty()) || OB_UNLIKELY(!is_valid_) ||
      OB_UNLIKELY(handlers_.count() != part_rows_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected handlers", K(ret), K(is_valid_), K(handlers_), K(part_rows_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < handlers_.count(); i ++) {
    double tmp_selectivity = 1.0;
    ObOptColumnStatHandle &column_stat = handlers_.at(i);
    bool tmp_is_rare = false;
    if (OB_FAIL(inner_get_sel(ctx,
                              column_stat.stat_->get_histogram(),
                              tmp_selectivity,
                              tmp_is_rare))) {
      LOG_WARN("failed to inner calc sel", K(ret));
    } else {
      tmp_selectivity = ObOptSelectivity::revise_between_0_1(tmp_selectivity);
      rows += tmp_selectivity * part_rows_.at(i);
      total_part_rows += part_rows_.at(i);
      is_rare &= tmp_is_rare;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_rare || total_part_rows <= OB_DOUBLE_EPSINON) {
      sel = density_;
    } else {
      sel = rows / total_part_rows;
    }
  }
  return ret;
}

int ObHistEqualSelHelper::inner_get_sel(const OptSelectivityCtx &ctx,
                                        const ObHistogram &histogram,
                                        double &sel,
                                        bool &is_rare_value)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  bool is_equal = false;
  ObObj *new_value = NULL;
  ObArenaAllocator tmp_alloc("ObOptSel");
  if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(&compare_value_, tmp_alloc, new_value))) {
    LOG_WARN("failed to convert valid obj for opt stats", K(ret), K(compare_value_), KPC(new_value));
  } else if (OB_ISNULL(new_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(new_value), K(compare_value_));
  } else if (OB_FAIL(ObOptSelectivity::get_bucket_bound_idx(histogram, *new_value, idx, is_equal))) {
    LOG_WARN("failed to get bucket bound idx", K(ret));
  } else if (idx < 0 || idx >= histogram.get_bucket_size() || !is_equal) {
    sel = 0.0;
    is_rare_value = true;
  } else {
    sel = static_cast<double>(histogram.get(idx).endpoint_repeat_count_)
        / histogram.get_sample_size();
  }
  if (OB_SUCC(ret) && hist_scale_ > 0) {
    sel /= hist_scale_;
  }
  return ret;
}

int ObHistRangeSelHelper::init(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               const ObColumnRefRawExpr &col)
{
  int ret = OB_SUCCESS;
  hist_min_value_.set_max_value();
  hist_max_value_.set_min_value();
  if (OB_FAIL(ObHistSelHelper::init(table_metas, ctx, col))) {
    LOG_WARN("failed to init", K(ret));
  } else if (!is_valid_) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handlers_.count(); i ++) {
      double tmp_selectivity = 1.0;
      ObOptColumnStatHandle &column_stat = handlers_.at(i);
      const ObHistogram &histogram = column_stat.stat_->get_histogram();
      int64_t cnt = histogram.get_bucket_size();
      const ObObj &cur_min_value = histogram.get(0).endpoint_value_;
      const ObObj &cur_max_value = histogram.get(cnt - 1).endpoint_value_;
      int cmp_res = 0;
      if (OB_UNLIKELY(cnt < 0)) {
        LOG_WARN("unexpected count", K(ret), K(histogram));
      } else if (OB_FAIL(hist_min_value_.compare(cur_min_value, cmp_res))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp_res > 0) {
        hist_min_value_ = cur_min_value;
      }
      if (FAILEDx(hist_max_value_.compare(cur_max_value, cmp_res))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp_res < 0) {
        hist_max_value_ = cur_max_value;
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_valid_ = false;
  }
  return ret;
}

int ObHistRangeSelHelper::inner_get_sel(const OptSelectivityCtx &ctx,
                                        const ObHistogram &histogram,
                                        double &sel,
                                        bool &is_rare_value)
{
  is_rare_value = false;
  return ObOptSelectivity::get_range_sel_by_histogram(ctx, histogram, ranges_, true, hist_scale_, sel);
}

}//end of namespace sql
}//end of namespace oceanbase
