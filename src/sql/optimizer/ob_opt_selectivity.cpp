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
#include "sql/optimizer/ob_opt_selectivity.h"
#include <math.h>
#include "common/object/ob_obj_compare.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "sql/optimizer/ob_access_path_estimation.h"
#include "sql/optimizer/ob_sel_estimator.h"
#include "sql/optimizer/ob_opt_est_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{
inline double revise_ndv(double ndv) { return ndv < 1.0 ? 1.0 : ndv; }

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

  if (OB_FAIL(all_used_parts_.assign(other.all_used_parts_))) {
    LOG_WARN("failed to assign all used parts", K(ret));
  } else if (OB_FAIL(all_used_tablets_.assign(other.all_used_tablets_))) {
    LOG_WARN("failed to assign all used tablets", K(ret));
  } else if (OB_FAIL(column_metas_.assign(other.column_metas_))) {
    LOG_WARN("failed to assign all csata", K(ret));
  } else if (OB_FAIL(pk_ids_.assign(other.pk_ids_))) {
    LOG_WARN("failed to assign pk ids", K(ret));
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
                       ObIArray<int64_t> &all_used_global_parts,
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
  if (OB_FAIL(all_used_parts_.assign(all_used_part_id))) {
    LOG_WARN("failed to assign all used partition ids", K(ret));
  } else if (OB_FAIL(all_used_tablets_.assign(all_used_tablets))) {
    LOG_WARN("failed to assign all used partition ids", K(ret));
  } else if (OB_FAIL(all_used_global_parts_.assign(all_used_global_parts))) {
    LOG_WARN("failed to assign all used partition ids", K(ret));
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
  bool is_single_pkey = (1 == pk_ids_.count() && pk_ids_.at(0) == column_id);
  if (is_single_pkey) {
    col_meta.set_ndv(rows_);
    col_meta.set_num_null(0);
  } else if (use_default_stat()) {
    col_meta.set_default_meta(rows_);
  } else if (OB_ISNULL(ctx.get_opt_stat_manager()) || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_opt_stat_manager()),
                                    K(ctx.get_session_info()));
  } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(ctx.get_session_info()->get_effective_tenant_id(),
                                                                 ref_table_id_,
                                                                 all_used_parts_,
                                                                 column_id,
                                                                 all_used_global_parts_,
                                                                 rows_,
                                                                 scale_ratio_,
                                                                 stat))) {
    LOG_WARN("failed to get column stats", K(ret));
  } else if (0 == stat.ndv_val_ && 0 == stat.null_val_) {
    col_meta.set_default_meta(rows_);
  } else if (0 == stat.ndv_val_ && stat.null_val_ > 0) {
    col_meta.set_ndv(1);
    col_meta.set_num_null(stat.null_val_);
  } else {
    col_meta.set_ndv(stat.ndv_val_);
    col_meta.set_num_null(stat.null_val_);
  }

  if (OB_SUCC(ret)) {
    col_meta.set_column_id(column_id);
    col_meta.set_avg_len(stat.avglen_val_);
    col_meta.set_cg_macro_blk_cnt(stat.cg_macro_blk_cnt_);
    col_meta.set_cg_micro_blk_cnt(stat.cg_micro_blk_cnt_);
    col_meta.set_cg_skip_rate(stat.cg_skip_rate_);
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

void OptTableMeta::set_ndv_for_all_column(double ndv)
{
  for (int64_t i = 0; i < column_metas_.count(); ++i) {
    column_metas_.at(i).set_ndv(ndv);
  }
  return;
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
                                            ObIArray<int64_t> &all_used_global_parts,
                                            const double scale_ratio,
                                            int64_t last_analyzed,
                                            bool is_stat_locked,
                                            const ObTablePartitionInfo *table_partition_info,
                                            const ObTableMetaInfo *base_meta_info)
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
                                      column_ids, all_used_global_parts, scale_ratio, ctx,
                                      table_partition_info, base_meta_info))) {
    LOG_WARN("failed to init new tstat", K(ret));
  } else {
    table_meta->set_version(last_analyzed);
    table_meta->set_stat_locked(is_stat_locked);
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
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(parent_stmt), K(child_stmt));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else if (OB_FAIL(parent_stmt->get_column_items(table_id, column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    const double table_rows = child_rows < 1.0 ? 1.0 : child_rows;
    table_meta->set_table_id(table_id);
    table_meta->set_ref_table_id(OB_INVALID_ID);
    table_meta->set_rows(table_rows);
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
          if (select_expr->is_column_ref_expr() &&
              OB_FAIL(ObOptSelectivity::get_column_min_max(child_table_metas, child_ctx, *select_expr, minobj, maxobj))) {
            LOG_WARN("failed to get column min max", K(ret));
          } else {
            column_meta->set_min_value(minobj);
            column_meta->set_max_value(maxobj);
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
  ObArenaAllocator tmp_alloc("ObOptSel");
  ObSelEstimatorFactory factory(tmp_alloc);
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
  if (OB_SUCC(ret) && OB_FAIL(selectivities.prepare_allocate(sel_estimators.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret), K(selectivities), K(sel_estimators));
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
      selectivities.at(i) = revise_between_0_1(tmp_selectivity);
      if (ObSelEstType::RANGE == estimator->get_type()) {
        ObRangeSelEstimator *range_estimator = static_cast<ObRangeSelEstimator *>(estimator);
        if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel,
                                            ObExprSelPair(range_estimator->get_column_expr(), tmp_selectivity, true)))) {
          LOG_WARN("failed to add selectivity to plan", K(ret), KPC(range_estimator), K(tmp_selectivity));
        }
      }
    }
  }
  selectivity = ObOptSelectivity::get_filters_selectivity(selectivities, ctx.get_dependency_type());
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
      if (!throw_ds_error && !ObAccessPathEstimation::is_retry_ret(ret)) {
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
    for (int64_t i = 0; OB_SUCC(ret) && i < ds_result_items.count(); ++i) {
      if (ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_FILTER_OUTPUT_STAT) {
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
    ObDSResultItem tmp_item(ObDSResultItemType::OB_DS_FILTER_OUTPUT_STAT, ref_table_id);
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
  ObArenaAllocator tmp_alloc("ObOptSel");
  ObSelEstimatorFactory factory(tmp_alloc);
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
    } else if (OB_FAIL(classify_quals(ctx, quals, all_predicate_sel, column_sel_infos))) {
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
        double step1_ndv = column_meta.get_ndv();
        double step2_ndv = column_meta.get_ndv();
        double step1_row = origin_rows;
        double null_num = column_meta.get_num_null();
        double hist_scale = -1;
        // step 1
        if (OB_NOT_NULL(sel_info)) {
          step1_row *= sel_info->selectivity_;
          hist_scale = sel_info->selectivity_;
          if (sel_info->equal_count_ > 0) {
            step1_ndv = sel_info->equal_count_;
          } else if (sel_info->has_range_exprs_) {
            step1_ndv *= sel_info->range_selectivity_;
          } else {
            step1_ndv = scale_distinct(step1_row, origin_rows, column_meta.get_ndv());
          }
        }
        // step 2
        if (filtered_rows < step1_row) {
          step2_ndv = scale_distinct(filtered_rows, step1_row, step1_ndv);
        } else {
          step2_ndv = step1_ndv;
        }
        // update null number
        if (null_num > 0) {
          bool null_reject = false;
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
  ObQueryRange query_range;
  ObQueryRangeArray ranges;
  ObSEArray<ColumnItem, 1> column_items;
  bool use_hist = false;
  ObOptColumnStatHandle handler;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (OB_FAIL(check_column_in_current_level_stmt(stmt, col_expr))) {
    LOG_WARN("failed to check if column is in current level stmt", K(col_expr), K(ret));
  } else if (OB_FAIL(get_column_query_range(ctx, tid, cid, quals,
                                            column_items, query_range, ranges))) {
    LOG_WARN("failed to get column query range", K(ret));
  } else {
    selectivity = 0.0;
    double not_null_sel = 0;
    if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, col_expr, NULL, &not_null_sel))) {
      LOG_WARN("failed to get column ndv and nns", K(ret));
    } else if (OB_FAIL(get_histogram_by_column(table_metas, ctx, tid, cid, handler))) {
      LOG_WARN("failed to get histogram by column", K(ret));
    } else if  (NULL != handler.stat_ &&
                handler.stat_->get_last_analyzed() > 0 &&
                handler.stat_->get_histogram().is_valid()) {
      double hist_scale = 0.0;
      if (OB_FAIL(get_column_hist_scale(table_metas, ctx, col_expr, hist_scale))) {
        LOG_WARN("failed to get columnn hist sample scale", K(ret));
      } else if (OB_FAIL(get_range_sel_by_histogram(ctx,
                                                    handler.stat_->get_histogram(),
                                                    ranges,
                                                    true,
                                                    hist_scale,
                                                    selectivity))) {
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
      ((ctx.get_compat_version() >= COMPAT_VERSION_4_2_1_BP7 && ctx.get_compat_version() < COMPAT_VERSION_4_2_2) ||
       (ctx.get_compat_version() >= COMPAT_VERSION_4_2_4 && ctx.get_compat_version() < COMPAT_VERSION_4_3_0) ||
        ctx.get_compat_version() >= COMPAT_VERSION_4_3_2)) {
    ObObj min_value;
    ObObj max_value;
    min_value.set_min_value();
    max_value.set_max_value();
    if (use_hist) {
      int64_t cnt = handler.stat_->get_histogram().get_bucket_size();
      min_value = handler.stat_->get_histogram().get(0).endpoint_value_;
      max_value = handler.stat_->get_histogram().get(cnt - 1).endpoint_value_;
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
  ObQueryRange query_range;
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
                                            column_items, query_range, ranges))) {
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
    bool convert2sortkey =
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_1_BP5 && ctx.get_compat_version() < COMPAT_VERSION_4_2_2) ||
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_4 && ctx.get_compat_version() < COMPAT_VERSION_4_3_0) ||
        ctx.get_compat_version() >= COMPAT_VERSION_4_3_1;
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
      bool convert2sortkey =
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_1_BP5 && ctx.get_compat_version() < COMPAT_VERSION_4_2_2) ||
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_4 && ctx.get_compat_version() < COMPAT_VERSION_4_3_0) ||
        ctx.get_compat_version() >= COMPAT_VERSION_4_3_1;
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
                                                                     table_meta->get_all_used_parts(),
                                                                     column_id,
                                                                     table_meta->get_all_used_global_parts(),
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
                                           double *row_count_ptr)
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
    if (OB_FAIL(get_column_basic_from_meta(table_metas,
                                           column_expr,
                                           need_default,
                                           row_count,
                                           ndv,
                                           num_null,
                                           avg_len))) {
      LOG_WARN("failed to get column basic from meta", K(ret));
    } else if (need_default && OB_FAIL(get_var_basic_default(row_count, ndv, num_null, avg_len))) {
      LOG_WARN("failed to get var default info", K(ret));
    } else {
      if (num_null > row_count - ndv) {
        num_null = row_count - ndv > 0 ? row_count - ndv : 0;
      }
      if (ctx.get_current_rows() > 0.0 && ctx.get_current_rows() < row_count) {
        ndv = scale_distinct(ctx.get_current_rows(), row_count, ndv);
      }

      LOG_TRACE("show column basic info", K(row_count), K(ctx.get_current_rows()), K(num_null), K(avg_len), K(ndv));

      // set return
      assign_value(row_count, row_count_ptr);
      assign_value(ndv, ndv_ptr);
      assign_value(num_null, num_null_ptr);
      assign_value(avg_len, avg_len_ptr);
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
                                                 double &avg_len)
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
    }
  } else {
    use_default = true;
  }
  return ret;
}

int ObOptSelectivity::get_var_basic_default(double &row_count,
                                            double &ndv,
                                            double &null_num,
                                            double &avg_len)
{
  int ret = OB_SUCCESS;
  row_count = 1.0;
  ndv = 1.0;
  null_num = static_cast<double>(row_count * EST_DEF_COL_NULL_RATIO);
  avg_len = DEFAULT_COLUMN_SIZE;
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
             !table_meta->use_opt_stat() ||
             table_meta->use_opt_global_stat()) {
    // do nothing
  } else if (table_meta->get_all_used_parts().count() != 1) {
    // consider to use the global histogram here
  } else if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_session_info()));
  } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(
            ctx.get_session_info()->get_effective_tenant_id(),
            table_meta->get_ref_table_id(),
            table_meta->get_all_used_parts().at(0),
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
  } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(col->get_result_type(),
                                                             col->get_result_type(),
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
    const ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(ctx.get_session_info());
    ObObj dest_value;
    ObCastCtx cast_ctx(&ctx.get_allocator(),
                       &dtc_params,
                       CM_NONE,
                       col->get_result_type().get_collation_type());
    if (OB_FAIL(ObObjCaster::to_type(col->get_result_type().get_type(),
                                     col->get_result_type().get_collation_type(),
                                     cast_ctx,
                                     expr_value,
                                     dest_value))) {
      LOG_WARN("failed to cast value", K(ret));
    } else {
      expr_value = dest_value;
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
      bool convert2sortkey =
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_1_BP5 && ctx.get_compat_version() < COMPAT_VERSION_4_2_2) ||
        (ctx.get_compat_version() >= COMPAT_VERSION_4_2_4 && ctx.get_compat_version() < COMPAT_VERSION_4_3_0) ||
        ctx.get_compat_version() >= COMPAT_VERSION_4_3_1;
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
                                             ObQueryRange &query_range,
                                             ObQueryRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  const ObLogPlan *log_plan = ctx.get_plan();
  const ParamStore *params = ctx.get_params();
  ObExecContext *exec_ctx = ctx.get_opt_ctx().get_exec_ctx();
  ObIAllocator &allocator = ctx.get_allocator();
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx.get_session_info());
  const ColumnItem* column_item = NULL;
  bool dummy_all_single_value_ranges = true;
  bool is_in_range_optimization_enabled = false;
  if (OB_ISNULL(log_plan) || OB_ISNULL(exec_ctx) ||
      OB_ISNULL(column_item = log_plan->get_column_item_by_id(table_id, column_id)) ||
      OB_ISNULL(ctx.get_stmt()) || OB_ISNULL(ctx.get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan), K(exec_ctx), K(column_item));
  } else if (OB_FAIL(column_items.push_back(*column_item))) {
    LOG_WARN("failed to push back column item", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(ctx.get_stmt()->get_query_ctx()->get_global_hint(),
                                                                       ctx.get_session_info(),
                                                                       is_in_range_optimization_enabled))) {
    LOG_WARN("failed to check in range optimization enabled", K(ret));
  } else if (OB_FAIL(query_range.preliminary_extract_query_range(column_items,
                                                                 quals,
                                                                 dtc_params,
                                                                 ctx.get_opt_ctx().get_exec_ctx(),
                                                                 NULL,
                                                                 params,
                                                                 false,
                                                                 true,
                                                                 is_in_range_optimization_enabled))) {
    LOG_WARN("failed to preliminary extract query range", K(ret));
  } else if (!query_range.need_deep_copy()) {
    if (OB_FAIL(query_range.direct_get_tablet_ranges(allocator, *exec_ctx, ranges,
                                                     dummy_all_single_value_ranges, dtc_params))) {
      LOG_WARN("failed to get tablet ranges", K(ret));
    }
  } else if (OB_FAIL(query_range.final_extract_query_range(*exec_ctx, dtc_params))) {
    LOG_WARN("failed to final extract query range", K(ret));
  } else if (OB_FAIL(query_range.get_tablet_ranges(ranges, dummy_all_single_value_ranges, dtc_params))) {
    LOG_WARN("failed to get tablet ranges", K(ret));
  } else { /*do nothing*/ }
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

int ObOptSelectivity::calculate_distinct(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObIArray<ObRawExpr*>& exprs,
                                         const double origin_rows,
                                         double &rows,
                                         const bool need_refine)
{
  int ret = OB_SUCCESS;
  rows = 1;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  ObSEArray<ObRawExpr*, 4> special_exprs;
  ObSEArray<double, 4> expr_ndv;
  ObSEArray<ObRawExpr*, 4> filtered_exprs;
  //classify expr and get ndv
  if (OB_FAIL(classify_exprs(exprs, column_exprs, special_exprs, table_metas, ctx))) {
    LOG_WARN("failed to classify_exprs", K(ret));
  } else if (OB_FAIL(filter_column_by_equal_set(table_metas, ctx, column_exprs, filtered_exprs))) {
    LOG_WARN("failed filter column by equal set", K(ret));
  } else if (OB_FAIL(calculate_expr_ndv(filtered_exprs, expr_ndv, table_metas, ctx, origin_rows))) {
    LOG_WARN("fail to calculate expr ndv", K(ret));
  } else if (OB_FAIL(calculate_expr_ndv(special_exprs, expr_ndv, table_metas, ctx, origin_rows))) {
    LOG_WARN("fail to calculate special expr ndv", K(ret));
  }
  //calculate rows
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_ndv.count(); ++i) {
    if (0 == i) {
      rows *= expr_ndv.at(i);
    } else {
      rows *= expr_ndv.at(i) / std::sqrt(2);
    }
  }
  //refine
  if (OB_SUCC(ret) && need_refine) {
    rows = std::min(rows, origin_rows);
    LOG_TRACE("succeed to calculate distinct", K(origin_rows), K(rows), K(exprs));
  }
  return ret;
}

int ObOptSelectivity::classify_exprs(const ObIArray<ObRawExpr*>& exprs,
                                     ObIArray<ObRawExpr*>& column_exprs,
                                     ObIArray<ObRawExpr*>& special_exprs,
                                     const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *child_expr = NULL;
    if (OB_ISNULL(child_expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), K(i));
    } else if (OB_FAIL(classify_exprs(child_expr, column_exprs, special_exprs, table_metas, ctx))) {
      LOG_WARN("failed to classify_exprs", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::classify_exprs(ObRawExpr* expr,
                                     ObIArray<ObRawExpr*>& column_exprs,
                                     ObIArray<ObRawExpr*>& special_exprs,
                                     const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(expr), K(ret));
  } else if (is_special_expr(*expr)) {
    if (OB_FAIL(add_var_to_array_no_dup(special_exprs, expr))) {
      LOG_WARN("fail to add expr to array", K(ret));
    }
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(add_var_to_array_no_dup(column_exprs, expr))) {
      LOG_WARN("fail to add expr to array", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *child_expr = NULL;
      if (OB_ISNULL(child_expr = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret), K(i));
      } else if (OB_FAIL(classify_exprs(child_expr, column_exprs, special_exprs, table_metas, ctx))) {
        LOG_WARN("failed to classify_exprs", K(ret));
      }
    }
  }
  return ret;
}

bool ObOptSelectivity::is_special_expr(const ObRawExpr &expr) {
  bool is_special = false;
  if (expr.is_win_func_expr()) {
    is_special = true;
  }
  return is_special;
}

int ObOptSelectivity::calculate_expr_ndv(const ObIArray<ObRawExpr*>& exprs,
                                         ObIArray<double>& expr_ndv,
                                         const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const double origin_rows)
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
      } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *expr, &ndv, NULL, NULL, NULL))) {
        LOG_WARN("failed to get column basic info", K(ret), K(*expr));
      } else if (OB_FAIL(expr_ndv.push_back(ndv))) {
        LOG_WARN("failed to push back expr", K(ret), K(ndv));
      }
    } else if (OB_FAIL(calculate_special_ndv(table_metas, expr, ctx, ndv, origin_rows))) {
      LOG_WARN("failed to calculate special expr ndv", K(ret), K(ndv));
    } else if (OB_FAIL(expr_ndv.push_back(ndv))) {
      LOG_WARN("failed to push back", K(ret), K(ndv));
    }
  }
  return ret;
}

int ObOptSelectivity::calculate_special_ndv(const OptTableMetas &table_metas,
                                            const ObRawExpr* expr,
                                            const OptSelectivityCtx &ctx,
                                            double &special_ndv,
                                            const double origin_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(expr), K(ret));
  } else if (expr->is_win_func_expr()) {
    double part_order_ndv = 1.0;
    double order_ndv = 1.0;
    double part_ndv = 1.0;
    ObSEArray<ObRawExpr*, 4> part_exprs;
    ObSEArray<ObRawExpr*, 4> order_exprs;
    ObSEArray<ObRawExpr*, 4> part_order_exprs;
    const ObWinFunRawExpr *win_expr = reinterpret_cast<const ObWinFunRawExpr*>(expr);
    const ObIArray<OrderItem> &order_items = win_expr->get_order_items();
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
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, part_order_exprs, origin_rows, part_order_ndv, false)))) {
        LOG_WARN("failed to calculate_distinct", K(ret));
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, order_exprs, origin_rows, order_ndv, false)))) {
      LOG_WARN("failed to calculate_distinct", K(ret));
    } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, part_exprs, origin_rows, part_ndv, false)))) {
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
      const ParamStore *params = ctx.get_params();
      if (OB_FAIL(param_exprs.assign(win_expr->get_func_params()))) {
        LOG_WARN("fail to assign exprs", K(ret));
      } else if (param_exprs.count() == 0|| OB_ISNULL(const_expr = param_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(param_exprs.count()), K(const_expr), K(ret));
      } else if (ObOptEstUtils::is_calculable_expr(*const_expr, params->count())) {
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
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv, false)))) {
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
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv, false)))) {
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
      } else if (OB_FAIL(SMART_CALL(calculate_distinct(table_metas, ctx, param_exprs, origin_rows, param_ndv, false)))) {
        LOG_WARN("failed to calculate_distinct", K(ret));
      } else {
        special_ndv = param_ndv;
      }
    } else {
      special_ndv = part_order_ndv;
    }
    LOG_TRACE("calculate win expr ndv",  K(win_expr->get_func_type()), K(part_exprs.count()), K(order_exprs.count()));
  }
  special_ndv = revise_ndv(special_ndv);
  return ret;
}

// 仅保留一个 ndv 最小的 distinct expr, 加入到 filtered_exprs 中;
// 再把不在 equal set 中的列加入到 filtered_exprs 中,
int ObOptSelectivity::filter_column_by_equal_set(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObIArray<ObRawExpr*> &column_exprs,
                                                 ObIArray<ObRawExpr*> &filtered_exprs)
{
  int ret = OB_SUCCESS;
  const EqualSets *equal_sets = ctx.get_equal_sets();
  if (NULL == equal_sets) {
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
      } else if (OB_FAIL(filtered_exprs.push_back(filtered_expr))) {
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
  if (OB_ISNULL(eq_sets)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
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
                                                      NULL))) {
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
                                              const ObIArray<ObRawExpr *> &col_exprs,
                                              bool &is_pkey,
                                              bool &is_union_pkey)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> col_ids;
  uint64_t table_id;
  if (OB_FAIL(extract_column_ids(col_exprs, col_ids, table_id))) {
    LOG_WARN("failed to extract column ids", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(table_metas, col_ids, table_id, is_pkey, is_union_pkey))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
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
  for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); ++i) {
    ObRawExpr *cur_expr = col_exprs.at(i);
    if (OB_ISNULL(cur_expr) || OB_UNLIKELY(!cur_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr *>(cur_expr))) {
    } else if (OB_FAIL(col_ids.push_back(column_expr->get_column_id()))) {
      LOG_WARN("failed to push back column id", K(ret));
    } else if (0 == i) {
      table_id = column_expr->get_table_id();
    } else if (OB_UNLIKELY(table_id != column_expr->get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("columns not belong to same table", K(ret), K(*column_expr), K(table_id));
    }
  }
  return ret;
}

int ObOptSelectivity::classify_quals(const OptSelectivityCtx &ctx,
                                     const ObIArray<ObRawExpr*> &quals,
                                     ObIArray<ObExprSelPair> &all_predicate_sel,
                                     ObIArray<OptSelInfo> &column_sel_infos)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *qual = NULL;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  OptSelInfo *sel_info = NULL;
  double tmp_selectivity = 1.0;
  ObArenaAllocator tmp_alloc("ObOptSel");
  ObSelEstimatorFactory factory(tmp_alloc);
  ObSEArray<ObSelEstimator *, 4> range_estimators;
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
        OB_UNLIKELY(ObSelEstType::RANGE != range_estimator->get_type()) ||
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
                                         const bool is_semi,
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
      if (is_semi) {
        rows += left_hist.get(lidx).endpoint_repeat_count_;
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

double ObOptSelectivity::get_filters_selectivity(ObIArray<double> &selectivities, FilterDependencyType type)
{
  double selectivity = 0.0;
  if (FilterDependencyType::INDEPENDENT == type) {
    selectivity = 1.0;
    for (int64_t i = 0; i < selectivities.count(); i ++) {
      selectivity *= selectivities.at(i);
    }
  } else if (FilterDependencyType::MUTEX_OR == type) {
    selectivity = 0.0;
    for (int64_t i = 0; i < selectivities.count(); i ++) {
      selectivity += selectivities.at(i);
    }
  } else if (FilterDependencyType::EXPONENTIAL_BACKOFF == type) {
    selectivity = 1.0;
    if (!selectivities.empty()) {
      double exp = 1.0;
      lib::ob_sort(&selectivities.at(0), &selectivities.at(0) + selectivities.count());
      for (int64_t i = 0; i < selectivities.count(); i ++) {
        selectivity *= std::pow(selectivities.at(i), exp);
        exp /= 2;
      }
    }
  }
  selectivity = revise_between_0_1(selectivity);
  return selectivity;
}

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

}//end of namespace sql
}//end of namespace oceanbase
