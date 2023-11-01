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
  return ret;
}

void OptColumnMeta::init(const uint64_t column_id,
                         const double ndv,
                         const double num_null,
                         const double avg_len)
{
  column_id_ = column_id;
  ndv_ = ndv;
  num_null_ = num_null;
  avg_len_ = avg_len;
}

int OptTableMeta::assign(const OptTableMeta &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  ref_table_id_ = other.ref_table_id_;
  rows_ = other.rows_;
  stat_type_ = other.stat_type_;
  ds_level_ = other.ds_level_;

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
                       ObSqlSchemaGuard &schema_guard,
                       ObIArray<int64_t> &all_used_part_id,
                       common::ObIArray<ObTabletID> &all_used_tablets,
                       ObIArray<uint64_t> &column_ids,
                       ObIArray<int64_t> &all_used_global_parts,
                       const double scale_ratio,
                       const OptSelectivityCtx &ctx)
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
  int64_t global_ndv = 0;
  int64_t num_null = 0;
  if (is_single_pkey) {
    global_ndv = rows_;
    num_null = 0;
  } else if (use_default_stat()) {
    global_ndv = std::min(rows_, 100.0);
    num_null = rows_ * EST_DEF_COL_NULL_RATIO;
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
    global_ndv = std::min(rows_, 100.0);
    num_null = rows_ * EST_DEF_COL_NULL_RATIO;
  } else if (0 == stat.ndv_val_ && stat.null_val_ > 0) {
    global_ndv = 1;
    num_null = stat.null_val_;
  } else {
    global_ndv = stat.ndv_val_;
    num_null = stat.null_val_;
  }

  if (OB_SUCC(ret)) {
    col_meta.init(column_id, global_ndv, num_null, stat.avglen_val_);
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
                                            ObIArray<int64_t> &all_used_part_id,
                                            ObIArray<ObTabletID> &all_used_tablets,
                                            ObIArray<uint64_t> &column_ids,
                                            const OptTableStatType stat_type,
                                            ObIArray<int64_t> &all_used_global_parts,
                                            const double scale_ratio,
                                            int64_t last_analyzed)
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
  } else if (OB_FAIL(table_meta->init(table_id, ref_table_id, table_type, rows, stat_type,
                                      *schema_guard, all_used_part_id, all_used_tablets,
                                      column_ids, all_used_global_parts, scale_ratio, ctx))) {
    LOG_WARN("failed to init new tstat", K(ret));
  } else {
    table_meta->set_version(last_analyzed);
    LOG_TRACE("add base table meta info success", K(*table_meta));
  }
  return ret;
}

// set stmt child is select stmt, not generate table. To mentain meta info for set stmt,
// mock a table for set stmt child. e.g. first child stmt use table id = 1, second child stmt
// use table_id = 2, ...
int OptTableMetas::add_set_child_stmt_meta_info(const ObDMLStmt *parent_stmt,
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
  ObRawExpr *select_expr = NULL;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(parent_stmt), K(child_stmt));
  } else if (OB_ISNULL(table_meta = table_metas_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate place holder for table meta", K(ret));
  } else {
    const double table_rows = child_rows < 1.0 ? 1.0 : child_rows;
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
          if (select_expr->is_column_ref_expr()) {
            ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(select_expr);
            const OptColumnMeta *child_column_meta = child_table_metas.get_column_meta_by_table_id(
                        col->get_table_id(), col->get_column_id());
            if (OB_NOT_NULL(child_column_meta) && child_column_meta->get_min_max_inited()) {
              column_meta->set_min_value(child_column_meta->get_min_value());
              column_meta->set_max_value(child_column_meta->get_max_value());
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
    } else if (ObSelectStmt::SetOperator::UNION == stmt.get_set_op()) {
      // ndv1 + ndv2
      ndv += column_meta->get_ndv();
      num_null += column_meta->get_num_null();
      avg_len = std::max(avg_len, column_meta->get_avg_len());
    } else if (ObSelectStmt::SetOperator::INTERSECT == stmt.get_set_op()) {
      // min(ndv1, ndv2)
      if (0 == i) {
        ndv = column_meta->get_ndv();
        num_null = column_meta->get_num_null();
        avg_len = column_meta->get_avg_len();
      } else {
        ndv = std::min(ndv, column_meta->get_ndv());
        num_null = std::min(num_null, column_meta->get_num_null());
        avg_len = std::min(avg_len, column_meta->get_avg_len());
      }
    } else if (ObSelectStmt::SetOperator::EXCEPT == stmt.get_set_op()) {
      // max(ndv1 - ndv2, 1)
      if (0 == i) {
        ndv = column_meta->get_ndv();
        num_null = column_meta->get_num_null();
        avg_len = column_meta->get_avg_len();
      } else {
        ndv = std::max(ndv - column_meta->get_ndv(), 1.0);
        num_null = std::max(num_null - column_meta->get_num_null(), 1.0);
        avg_len = std::min(avg_len, column_meta->get_avg_len());
      }
    } else if (ObSelectStmt::SetOperator::RECURSIVE == stmt.get_set_op()) {
      // ndv1
      ndv = column_meta->get_ndv();
      num_null = column_meta->get_num_null();
      avg_len = column_meta->get_avg_len();
      break;
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
  bool is_calculated = false;
  ObSEArray<ObRawExpr *, 4> join_conditions;
  ObSEArray<RangeExprs, 3> range_conditions;
  ObRawExpr *qual = NULL;
  double tmp_selectivity = 1.0;
  bool need_skip = false;
  //we calc some complex predicates selectivity by dynamic sampling
  if (OB_FAIL(calc_complex_predicates_selectivity_by_ds(table_metas, ctx, predicates,
                                                        all_predicate_sel))) {
    LOG_WARN("failed to calc complex predicates selectivity by ds", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    qual = predicates.at(i);
    LOG_TRACE("calculate qual selectivity", "expr", PNAME(qual));
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_FAIL(check_qual_later_calculation(table_metas, ctx, *qual, all_predicate_sel,
                                                    join_conditions,range_conditions, need_skip))) {
      LOG_WARN("failed to check qual later calculation", K(ret));
    } else if (need_skip) {
      // do nothing
    } else if (OB_FAIL(calculate_qual_selectivity(table_metas, ctx, *qual,
                                                  tmp_selectivity, all_predicate_sel))) {
      LOG_WARN("failed to calculate one qual selectivity", K(*qual), K(ret));
    } else {
      tmp_selectivity = revise_between_0_1(tmp_selectivity);
      selectivity *= tmp_selectivity;
    }
    LOG_TRACE("after calculate one qual selectivity", K(need_skip), K(tmp_selectivity), K(selectivity));
  }
  for (int64_t i = 0 ; OB_SUCC(ret) && i < range_conditions.count() ; ++i) {
    tmp_selectivity = 1.0;
    ObColumnRefRawExpr *col_expr = range_conditions.at(i).column_expr_;
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_column_range_sel(table_metas, ctx, *col_expr,
                                            range_conditions.at(i).range_exprs_,
                                            tmp_selectivity))) {
      LOG_WARN("failed to get column range selectivity", K(ret));
    } else {
      tmp_selectivity = revise_between_0_1(tmp_selectivity);
      selectivity *= tmp_selectivity;
      if (range_conditions.at(i).range_exprs_.count() > 1) {
        if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel,
                                            ObExprSelPair(col_expr, tmp_selectivity, true)))) {
          LOG_WARN("failed to add selectivity to plan", K(ret), K(qual), K(tmp_selectivity));
        }
      }
    }
  }
  if (OB_SUCC(ret) && ctx.get_left_rel_ids() != NULL && ctx.get_right_rel_ids() != NULL) {
    tmp_selectivity = 1.0;
    if (1 == join_conditions.count()) {
      // only one join condition, calculate selectivity directly
      if (OB_ISNULL(join_conditions.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_equal_sel(table_metas, ctx, *join_conditions.at(0), tmp_selectivity))) {
        LOG_WARN("Failed to get equal selectivity", K(ret));
      } else {
        LOG_PRINT_EXPR(TRACE, "get single equal expr selectivity", *join_conditions.at(0), K(tmp_selectivity));
      }
    } else if (join_conditions.count() > 1) {
      // 存在多个连接条件，检查是否涉及联合主键
      if (OB_FAIL(get_equal_sel(table_metas, ctx, join_conditions, tmp_selectivity))) {
        LOG_WARN("failed to get equal sel");
      } else {
        LOG_TRACE("get multi equal expr selectivity", K(join_conditions), K(tmp_selectivity));
      }
    }
    if (OB_SUCC(ret)) {
      tmp_selectivity = revise_between_0_1(tmp_selectivity);
      selectivity *= tmp_selectivity;
    }
  }
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
                                                         true,
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
      if (OB_FAIL(get_like_sel(table_metas, ctx, *qual, selectivity, can_calc_sel))) {
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

int ObOptSelectivity::check_qual_later_calculation(const OptTableMetas &table_metas,
                                                   const OptSelectivityCtx &ctx,
                                                   ObRawExpr &qual,
                                                   ObIArray<ObExprSelPair> &all_pred_sel,
                                                   ObIArray<ObRawExpr *> &join_conditions,
                                                   ObIArray<RangeExprs> &range_conditions,
                                                   bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  if (OB_FAIL(is_simple_join_condition(qual,
                                       ctx.get_left_rel_ids(),
                                       ctx.get_right_rel_ids(),
                                       need_skip,
                                       join_conditions))) {
    LOG_WARN("failed to check is simple join condition", K(ret));
  } else if (!need_skip && OB_FAIL(ObOptEstUtils::extract_simple_cond_filters(qual,
                                                                              need_skip,
                                                                              range_conditions))) {
    LOG_WARN("failed to extract simple cond filters", K(ret));
  } else if (need_skip) {
    // calculate qual selectivity if qual not in all_predicate_sel
    double tmp_sel = 1.0;
    if (ObOptimizerUtil::find_item(all_pred_sel, ObExprSelPair(&qual, 0))) {
      // do nothing
    } else if (OB_FAIL(calculate_qual_selectivity(table_metas, ctx, qual, tmp_sel, all_pred_sel))) {
      LOG_WARN("failed to calculate one qual selectivity", K(qual), K(ret));
    }
  }
  return ret;
}

/**
 * check if qual is a simple join condition.
 * This recommend each side of `=` belong to different subtree.
 */
int ObOptSelectivity::is_simple_join_condition(ObRawExpr &qual,
                                               const ObRelIds *left_rel_ids,
                                               const ObRelIds *right_rel_ids,
                                               bool &is_valid,
                                               ObIArray<ObRawExpr *> &join_conditions)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (NULL == left_rel_ids || NULL == right_rel_ids) {
    // do nothing
  } else if (T_OP_EQ == qual.get_expr_type() || T_OP_NSEQ == qual.get_expr_type()) {
    ObRawExpr *expr0 = qual.get_param_expr(0);
    ObRawExpr *expr1 = qual.get_param_expr(1);
    if (OB_ISNULL(expr0) || OB_ISNULL(expr1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null exprs", K(ret), K(expr0), K(expr1));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(expr0, expr0)) ||
               OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(expr1, expr1))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (!expr0->is_column_ref_expr() || !expr1->is_column_ref_expr()) {
      // do nothing
    } else if ((left_rel_ids->is_superset(expr0->get_relation_ids()) &&
                right_rel_ids->is_superset(expr1->get_relation_ids())) ||
               (left_rel_ids->is_superset(expr1->get_relation_ids()) &&
                right_rel_ids->is_superset(expr0->get_relation_ids()))) {
      if (OB_FAIL(join_conditions.push_back(&qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        is_valid = true;
      }
    } else { /* do nothing */ }
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
  selectivity = 1.0;
  double tmp_sel = 1.0;
  int64_t idx = 0;
  if (qual.has_flag(CNT_AGG)) {
    if (OB_FAIL(get_agg_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get agg expr selectivity", K(ret), K(qual));
    }
  } else if (qual.is_const_expr()) {
    if (OB_FAIL(get_const_sel(ctx, qual, selectivity))) {
      LOG_WARN("failed to get const expr selectivity", K(ret), K(qual));
    }
  } else if (qual.is_column_ref_expr()) {
    if (OB_FAIL(get_column_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get column selectivity", K(ret), K(qual));
    }
  } else if (T_OP_EQ == qual.get_expr_type() || T_OP_NSEQ == qual.get_expr_type()) {
    if (OB_FAIL(get_equal_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get equal selectivity", K(ret));
    }
  } else if (T_OP_IN == qual.get_expr_type() || T_OP_NOT_IN == qual.get_expr_type()) {
    if (OB_FAIL(get_in_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get in selectivity", K(ret));
    }
  } else if (T_OP_IS == qual.get_expr_type()  || T_OP_IS_NOT == qual.get_expr_type()) {
    if (OB_FAIL(get_is_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get is selectivity", K(ret));
    }
  } else if (IS_RANGE_CMP_OP(qual.get_expr_type())) {
    if (OB_FAIL(get_range_cmp_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get range cmp selectivity", K(ret));
    }
  } else if (T_OP_LIKE == qual.get_expr_type()) {
    bool can_calc_sel = false;
    if (OB_FAIL(get_like_sel(table_metas, ctx, qual, selectivity, can_calc_sel))) {
      LOG_WARN("failed to get like selectivity", K(ret));
    } else if (can_calc_sel) {//do nothing
    //try find the calc sel from dynamic sampling
    } else if (ObOptimizerUtil::find_item(all_predicate_sel, ObExprSelPair(&qual, 0), &idx)) {
      selectivity = all_predicate_sel.at(idx).sel_;
    }
  } else if (T_OP_BTW == qual.get_expr_type() || T_OP_NOT_BTW == qual.get_expr_type()) {
    if (OB_FAIL(get_btw_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get between selectivity", K(ret));
    }
  } else if (T_OP_NOT == qual.get_expr_type()) {
    if (OB_FAIL(get_not_sel(table_metas, ctx, qual, selectivity, all_predicate_sel))) {
      LOG_WARN("failed to get not selectivity", K(ret));
    }
  } else if (T_OP_NE == qual.get_expr_type()) {
    if (OB_FAIL(get_ne_sel(table_metas, ctx, qual, selectivity))) {
      LOG_WARN("failed to get not selectivity", K(ret));
    }
  } else if (T_OP_AND == qual.get_expr_type()) {
    double tmp_selectivity = 1.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < qual.get_param_count(); ++i) {
      const ObRawExpr *child_expr = qual.get_param_expr(i);
      if (OB_ISNULL(child_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(calculate_qual_selectivity(table_metas, ctx, *child_expr,
                                                    tmp_selectivity, all_predicate_sel))) {
        LOG_WARN("failed to callculate one qual selectivity", K(child_expr), K(ret));
      } else {
        selectivity *= tmp_selectivity;
      }
    }
  } else if (T_OP_OR == qual.get_expr_type()) {
    double tmp_selectivity = 1.0;
    selectivity = 0;
    bool is_mutex = false;;
    if (OB_FAIL(check_mutex_or(qual, is_mutex))) {
      LOG_WARN("failed to check mutex or", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < qual.get_param_count(); ++i) {
        const ObRawExpr *child_expr = qual.get_param_expr(i);
        if (OB_ISNULL(child_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret));
        } else if (OB_FAIL(calculate_qual_selectivity(table_metas, ctx, *child_expr,
                                                      tmp_selectivity, all_predicate_sel))) {
          LOG_WARN("failed to callculate one qual selectivity", K(child_expr), K(ret));
        } else if (0 == i || is_mutex) {
          selectivity += tmp_selectivity;
        } else {
          selectivity += tmp_selectivity - tmp_selectivity * selectivity;
        }
      }
    }
  } else if (qual.is_spatial_expr()) {
    selectivity = DEFAULT_SPATIAL_SEL;
  } else if (ObOptimizerUtil::find_item(all_predicate_sel, ObExprSelPair(&qual, 0), &idx)) {
    selectivity = all_predicate_sel.at(idx).sel_;
  } else { //任何处理不了的表达式，都认为是0.5的选择率		  } else { //任何处理不了的表达式，都认为是0.5的选择率
    selectivity = DEFAULT_SEL;
  }
  if (OB_SUCC(ret)) {
    LOG_PRINT_EXPR(TRACE, "calculate one qual selectivity", qual, K(selectivity));
    // We remember each predicate's selectivity in the plan so that we can reorder them
    // in the vector of filters according to their selectivity.
    if (OB_FAIL(add_var_to_array_no_dup(all_predicate_sel, ObExprSelPair(&qual, selectivity)))) {
      LOG_WARN("failed to add selectivity to plan", K(ret), K(qual), K(selectivity));
    }
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
    if (filtered_rows >= origin_rows) {
      // only update table rows
    } else if (OB_FAIL(classify_quals(quals, all_predicate_sel, column_sel_infos))) {
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
      }
    }
  }
  LOG_TRACE("show table meta after update", KPC(table_meta));
  return ret;
}

int ObOptSelectivity::get_const_sel(const OptSelectivityCtx &ctx,
                                    const ObRawExpr &qual,
                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  const ParamStore *params = ctx.get_params();
  const ObDMLStmt *stmt = ctx.get_stmt();
  if (OB_ISNULL(params) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(params), K(stmt));
  } else if (ObOptEstUtils::is_calculable_expr(qual, params->count())) {
    ObObj const_value;
    bool got_result = false;
    bool is_true = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                          &qual,
                                                          const_value,
                                                          got_result,
                                                          ctx.get_allocator()))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      selectivity = DEFAULT_SEL;
    } else if (OB_FAIL(ObObjEvaluator::is_true(const_value, is_true))) {
      LOG_WARN("failed to check is const value true", K(ret));
    } else {
      selectivity = is_true ? 1.0 : 0.0;
    }
  } else {
    selectivity = DEFAULT_SEL;
  }
  return ret;
}

int ObOptSelectivity::get_column_sel(const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx,
                                     const ObRawExpr &qual,
                                     double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  double distinct_sel = 0.0;
  double null_sel = 0.0;
  if (!ob_is_string_or_lob_type(qual.get_data_type())) {
    if (OB_FAIL(check_column_in_current_level_stmt(ctx.get_stmt(), qual))) {
      LOG_WARN("Failed to check column in cur level stmt", K(ret));
    } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, qual, &distinct_sel, &null_sel))) {
      LOG_WARN("Failed to calc basic equal sel", K(ret));
    } else {
      selectivity = 1.0 - distinct_sel - null_sel;
    }
  }
  return ret;
}

int ObOptSelectivity::get_equal_sel(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr &qual,
                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = qual.get_param_expr(0);
  const ObRawExpr *right_expr = qual.get_param_expr(1);
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(qual), K(left_expr), K(right_expr));
  } else if (OB_FAIL(get_equal_sel(table_metas, ctx, *left_expr, *right_expr,
                                   T_OP_NSEQ == qual.get_expr_type(), selectivity))) {
    LOG_WARN("failed to get equal sel", K(ret));
  }
  return ret;
}

int ObOptSelectivity::get_equal_sel(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr &left_expr,
                                    const ObRawExpr &right_expr,
                                    const bool null_safe,
                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  if (T_OP_ROW == left_expr.get_expr_type() && T_OP_ROW == right_expr.get_expr_type()) {
    // normally row equal row will unnest as `var = var and var = var ...`
    selectivity = 1.0;
    double tmp_selectivity = 1.0;
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    const ObRawExpr *l_row = &left_expr;
    const ObRawExpr *r_row = &right_expr;
    // (c1, c2) in ((const1, const2)) may transform to (c1, c2) = ((const1, const2))
    if (left_expr.get_param_count() == 1 && OB_NOT_NULL(left_expr.get_param_expr(0)) &&
        T_OP_ROW == left_expr.get_param_expr(0)->get_expr_type()) {
      l_row = left_expr.get_param_expr(0);
    }
    if (right_expr.get_param_count() == 1 && OB_NOT_NULL(right_expr.get_param_expr(0)) &&
        T_OP_ROW == right_expr.get_param_expr(0)->get_expr_type()) {
      r_row = right_expr.get_param_expr(0);
    }
    if (OB_UNLIKELY(l_row->get_param_count() != r_row->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", KPC(l_row), KPC(l_row), K(ret));
    } else {
      int64_t num = l_row->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        if (OB_ISNULL(l_expr = l_row->get_param_expr(i)) ||
            OB_ISNULL(r_expr = r_row->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret), K(l_expr), K(r_expr), K(i));
        } else if (OB_FAIL(SMART_CALL(get_equal_sel(table_metas, ctx, *l_expr,
                                                    *r_expr, null_safe, tmp_selectivity)))) {
          LOG_WARN("failed to get equal selectivity", K(ret));
        } else {
          selectivity *= tmp_selectivity;
        }
      }
    }
  } else if ((left_expr.has_flag(CNT_COLUMN) && !right_expr.has_flag(CNT_COLUMN)) ||
             (!left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN))) {
    // column = const
    const ObRawExpr *cnt_col_expr = left_expr.has_flag(CNT_COLUMN) ? &left_expr : &right_expr;
    const ObRawExpr &calc_expr = left_expr.has_flag(CNT_COLUMN) ? right_expr : left_expr;
    ObOptColumnStatHandle handler;
    ObObj expr_value;
    bool can_use_hist = false;
    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(cnt_col_expr, cnt_col_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (cnt_col_expr->is_column_ref_expr()) {
      const ObColumnRefRawExpr* col = static_cast<const ObColumnRefRawExpr*>(cnt_col_expr);
      if (OB_FAIL(get_histogram_by_column(table_metas, ctx, col->get_table_id(),
                                          col->get_column_id(), handler))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (handler.stat_ == NULL || !handler.stat_->get_histogram().is_valid()) {
        // do nothing
      } else if (OB_FAIL(get_compare_value(ctx, col, &calc_expr, expr_value, can_use_hist))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        can_use_hist = false;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (can_use_hist) {
        double nns = 0;
        double hist_scale = 0;
        if (OB_FAIL(get_column_hist_scale(table_metas, ctx, *cnt_col_expr, hist_scale))) {
          LOG_WARN("failed to get columnn hist sample scale", K(ret));
        } else if (OB_FAIL(get_equal_pred_sel(handler.stat_->get_histogram(), expr_value,
                                              hist_scale, selectivity))) {
          LOG_WARN("Failed to get equal density", K(ret));
        } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *cnt_col_expr, NULL, &nns))) {
          LOG_WARN("failed to get column ndv and nns", K(ret));
        } else {
          selectivity *= nns;
        }
      } else if (OB_FAIL(get_simple_equal_sel(table_metas, ctx, *cnt_col_expr,
                                              &calc_expr, null_safe, selectivity))) {
        LOG_WARN("failed to get simple equal selectivity", K(ret));
      }
      LOG_TRACE("succeed to get equal predicate sel", K(can_use_hist), K(selectivity));
    }
  } else if (left_expr.has_flag(CNT_COLUMN) && right_expr.has_flag(CNT_COLUMN)) {
    if (OB_FAIL(get_cntcol_op_cntcol_sel(table_metas, ctx, left_expr, right_expr,
                                         null_safe ? T_OP_NSEQ : T_OP_EQ, selectivity))) {
      LOG_WARN("failed to get contain column equal contain column selectivity", K(ret));
    } else {
      LOG_TRACE("succeed to get contain column equal contain column sel", K(selectivity), K(ret));
    }
  } else {
    // CONST_PARAM = CONST_PARAM
    const ParamStore *params = ctx.get_params();
    if (OB_ISNULL(params)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Params is NULL", K(ret));
    } else if (ObOptEstUtils::is_calculable_expr(left_expr, params->count()) &&
               ObOptEstUtils::is_calculable_expr(right_expr, params->count())) {
      // 1 in (c1, 2, 3) will reach this branch
      bool equal = false;
      if (OB_FAIL(ObOptEstUtils::if_expr_value_equal(const_cast<ObOptimizerContext &>(ctx.get_opt_ctx()),
                                                     ctx.get_stmt(),
                                                     left_expr, right_expr, null_safe, equal))) {
        LOG_WARN("Failed to check hvae equal expr", K(ret));
      } else {
        selectivity = equal ? 1.0 : 0.0;
      }
    } else {
      selectivity = DEFAULT_EQ_SEL;
    }
  }
  return ret;
}

int ObOptSelectivity::get_simple_equal_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObRawExpr &cnt_col_expr,
                                           const ObRawExpr *calculable_expr,
                                           const bool null_safe,
                                           double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObColumnRefRawExpr*, 2> column_exprs;
  bool only_monotonic_op = true;
  const ObColumnRefRawExpr *column_expr = NULL;
  double distinct_sel = 1.0;
  double null_sel = 1.0;
  bool is_null_value = false;
  if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(&cnt_col_expr,
                                                                column_exprs,
                                                                only_monotonic_op))) {
    LOG_WARN("failed to extract column exprs with op check", K(ret));
  } else if (!only_monotonic_op || column_exprs.count() > 1) {
    // cnt_col_expr contain not monotonic op OR has more than 1 column
    selectivity = DEFAULT_EQ_SEL;
  } else if (OB_UNLIKELY(1 != column_exprs.count()) ||
             OB_ISNULL(column_expr = column_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect column expr", K(column_exprs), K(cnt_col_expr), K(column_expr));
  } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *column_expr,
                                          &distinct_sel, &null_sel))) {
    LOG_WARN("failed to get column basic selelectivity", K(ret));
  } else if (NULL == calculable_expr) {
    selectivity = distinct_sel;
  } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(ctx.get_params(),
                                                       *calculable_expr,
                                                       ctx.get_opt_ctx().get_exec_ctx(),
                                                       ctx.get_allocator(),
                                                       is_null_value))) {
    LOG_WARN("failed to check if expr value null", K(ret));
  } else if (!is_null_value) {
    selectivity = distinct_sel;
  } else if (null_safe) {
    selectivity = null_sel;
  } else {
    selectivity = 0.0;
  }
  return ret;
}

int ObOptSelectivity::get_cntcol_op_cntcol_sel(const OptTableMetas &table_metas,
                                               const OptSelectivityCtx &ctx,
                                               const ObRawExpr &input_left_expr,
                                               const ObRawExpr &input_right_expr,
                                               ObItemType op_type,
                                               double &selectivity)
{
  int ret = OB_SUCCESS;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double left_nns = 0.0;
  double right_nns = 0.0;
  selectivity = DEFAULT_EQ_SEL;
  const ObRawExpr* left_expr = &input_left_expr;
  const ObRawExpr* right_expr = &input_right_expr;
  if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr)) ||
      OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right_expr, right_expr))) {
    LOG_WARN("failed to check is lossless column cast", K(ret));
  } else if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
  } else if (left_expr->is_column_ref_expr() && right_expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr* left_col = NULL;
    const ObColumnRefRawExpr* right_col = NULL;
    if (OB_FAIL(filter_one_column_by_equal_set(table_metas, ctx, left_expr, left_expr))) {
      LOG_WARN("failed filter column by equal set", K(ret));
    } else if (OB_FAIL(filter_one_column_by_equal_set(table_metas, ctx, right_expr, right_expr))) {
      LOG_WARN("failed filter column by equal set", K(ret));
    } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *left_expr, &left_ndv, &left_nns))) {
      LOG_WARN("failed to get column basic sel", K(ret));
    } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *right_expr,
                                              &right_ndv, &right_nns))) {
      LOG_WARN("failed to get column basic sel", K(ret));
    } else if (FALSE_IT(left_col = static_cast<const ObColumnRefRawExpr*>(left_expr)) ||
               FALSE_IT(right_col = static_cast<const ObColumnRefRawExpr*>(right_expr))) {
      // never reach
    } else if (left_expr->get_relation_ids() == right_expr->get_relation_ids()) {
      if (left_col->get_column_id() == right_col->get_column_id()) {
        // same table same column
        if (T_OP_NSEQ == op_type) {
          selectivity = 1.0;
        } else if (T_OP_EQ == op_type) {
          selectivity = left_nns;
        } else if (T_OP_NE == op_type) {
          selectivity = 0.0;
        }
      } else {
        //same table different column
        if (T_OP_NSEQ == op_type) {
          selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv)
              + (1 - left_nns) * (1 - right_nns);
        } else if (T_OP_EQ == op_type) {
          selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv);
        } else if (T_OP_NE == op_type) {
          selectivity = left_nns * right_nns * (1 - 1/std::max(left_ndv, right_ndv));
        }
      }
    } else {
      // different table
      ObOptColumnStatHandle left_handler;
      ObOptColumnStatHandle right_handler;
      obj_cmp_func cmp_func = NULL;
      bool calc_with_hist = false;
      if (!ObObjCmpFuncs::can_cmp_without_cast(left_col->get_result_type(),
                                               right_col->get_result_type(),
                                               CO_EQ, cmp_func))  {
        // do nothing
      } else if (OB_FAIL(get_histogram_by_column(table_metas, ctx, left_col->get_table_id(),
                                                 left_col->get_column_id(), left_handler))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (OB_FAIL(get_histogram_by_column(table_metas, ctx, right_col->get_table_id(),
                                                 right_col->get_column_id(), right_handler))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (left_handler.stat_ != NULL && right_handler.stat_ != NULL &&
                 left_handler.stat_->get_histogram().is_frequency() &&
                 right_handler.stat_->get_histogram().is_frequency()) {
        calc_with_hist = true;
      }
      if (OB_FAIL(ret)) {
      } else if (IS_SEMI_ANTI_JOIN(ctx.get_join_type())) {
        if (OB_ISNULL(ctx.get_left_rel_ids()) || OB_ISNULL(ctx.get_right_rel_ids())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ctx.get_left_rel_ids()), K(ctx.get_right_rel_ids()));
        } else if (left_expr->get_relation_ids().overlap(*ctx.get_right_rel_ids()) ||
                   right_expr->get_relation_ids().overlap(*ctx.get_left_rel_ids())) {
          std::swap(left_ndv, right_ndv);
          std::swap(left_nns, right_nns);
        }
        if (OB_SUCC(ret)) {
          if (calc_with_hist) {
            double total_rows = 0;
            double left_rows = 0;
            double left_null = 0;
            double right_rows = 0;
            double right_null = 0;
            if (OB_FAIL(get_join_pred_rows(left_handler.stat_->get_histogram(),
                                          right_handler.stat_->get_histogram(),
                                          true, total_rows))) {
              LOG_WARN("failed to get join pred rows", K(ret));
            } else if (OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(), ctx,
                                                    *left_expr, NULL, &left_null, NULL, &left_rows))) {
              LOG_WARN("failed to get column basic info", K(ret));
            } else if (OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(), ctx,
                                                    *right_expr, NULL, &right_null, NULL, &right_rows))) {
              LOG_WARN("failed to get column basic info", K(ret));
            } else if (T_OP_NSEQ == op_type) {
              total_rows += right_null > 0 ? left_null : 0;
              selectivity = total_rows / left_rows;
            } else if (T_OP_EQ == op_type) {
              selectivity = total_rows / left_rows;
            } else if (T_OP_NE == op_type) {
              selectivity = ((left_rows - left_null) * (right_rows - right_null) - total_rows)
                  / left_rows / right_rows;
            }
          } else {
            /**
             * ## non NULL safe
             *  a) semi: `(min(ndv1, ndv2) / ndv1) * (1.0 - nullfrac1)`
             * ## NULL safe
             *  a) semi: `(min(ndv1, ndv2) / ndv1) * (1.0 - nullfrac1) + nullfrac2 > 0 && nullsafe ? nullfrac1: 0`
             */
            if (IS_LEFT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
              if (T_OP_NSEQ == op_type) {
                selectivity = (std::min(left_ndv, right_ndv) / left_ndv) * left_nns;
                if (1 - right_nns > 0) {
                  selectivity += (1 - left_nns);
                }
              } else if (T_OP_EQ == op_type) {
                selectivity = (std::min(left_ndv, right_ndv) / left_ndv) * left_nns;
              } else if (T_OP_NE == op_type) {
                if (right_ndv > 1.0) {
                  // if right ndv > 1.0, then there must exist one value not equal to left value
                  selectivity = left_nns;
                } else {
                  selectivity = (1 - 1 / left_ndv) * left_nns;
                }
              }
            } else {
              if (T_OP_NSEQ == op_type) {
                selectivity = (std::min(left_ndv, right_ndv) / right_ndv) * right_nns;
                if (1 - left_nns > 0) {
                  selectivity += (1 - right_nns);
                }
              } else if (T_OP_EQ == op_type) {
                selectivity = (std::min(left_ndv, right_ndv) / right_ndv) * right_nns;
              } else if (T_OP_NE == op_type) {
                if (left_ndv > 1.0) {
                  // if left ndv > 1.0, then there must exist one value not equal to right value
                  selectivity = right_nns;
                } else {
                  selectivity = (1 - 1 / right_ndv) * right_nns;
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && selectivity >= 1.0 && IS_ANTI_JOIN(ctx.get_join_type())) {
          selectivity = 1 - DEFAULT_ANTI_JOIN_SEL;
        }
      } else {
        // inner join, outer join
        if (calc_with_hist) {
          // use frequency histogram calculate selectivity
          double total_rows = 0;
          double left_rows = 0;
          double left_null = 0;
          double right_rows = 0;
          double right_null = 0;
          if (OB_FAIL(get_join_pred_rows(left_handler.stat_->get_histogram(),
                                         right_handler.stat_->get_histogram(),
                                         false, total_rows))) {
            LOG_WARN("failed to get join pred rows", K(ret));
          } else if (OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(), ctx,
                                                   *left_expr, NULL, &left_null, NULL, &left_rows))) {
            LOG_WARN("failed to get column basic info", K(ret));
          } else if (OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(), ctx,
                                                   *right_expr, NULL, &right_null, NULL, &right_rows))) {
            LOG_WARN("failed to get column basic info", K(ret));
          } else if (T_OP_NSEQ == op_type) {
            selectivity = (total_rows + left_null * right_null) / left_rows / right_rows;
          } else if (T_OP_EQ == op_type) {
            selectivity = total_rows / left_rows / right_rows;
          } else if (T_OP_NE == op_type) {
            selectivity = ((left_rows - left_null) * (right_rows - right_null) - total_rows)
                / left_rows / right_rows;
          }
        } else {
          /**
           * ## non NULL safe
           * (1.0 - nullfrac1) * (1.0 - nullfrac2) / MAX(nd1, nd2)
           * ## NULL safe
           * (1.0 - nullfrac1) * (1.0 - nullfrac2) / MAX(nd1, nd2) + nullfraf1 * nullfrac2
           * 目前不会特殊考虑 outer join 的选择率, 而是在外层对行数进行 revise.
           */
          if (T_OP_NSEQ == op_type) {
            selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv)
                + (1 - left_nns) * (1 - right_nns);
          } else if (T_OP_EQ == op_type) {
            selectivity = left_nns * right_nns / std::max(left_ndv, right_ndv);
          } else if (T_OP_NE == op_type) {
            selectivity = left_nns * right_nns * (1 - 1/std::max(left_ndv, right_ndv));
          }
        }
      }
    }
  } else if (left_expr->is_column_ref_expr() || right_expr->is_column_ref_expr()) {
    // col1 = func(col2), selectivity is 1 / ndv(col1)
    // inner join and semi join use same formula
    /**
     *  some test with generated table in oracle:
     *  select * from t1,t2,(select c1 as agg from t3) v where t1.c1 + t2.c1 = v.agg;
     *    => sel(t1.c1 + t2.c1 = v.agg) = 1/ndv(v.agg)
     *  select * from t1,t2,(select c1+c2 as agg from t3) v where t1.c1 + t2.c1 = v.agg;
     *    => sel(t1.c1 + t2.c1 = v.agg) = 1/100
     *  select * from t1,t2,(select max(c1) as agg from t3) v where t1.c1 + t2.c1 = v.agg;
     *    => sel(t1.c1 + t2.c1 = v.agg) = 1/100
     *  it seems like in oracle, if an equal condition is `col1 = fun(cols)` and col1 is from a
     *  generated table. oracle will check whether col1 is refered a basic column. If not, use
     *  default
     */
    const ObRawExpr* column_expr = left_expr->is_column_ref_expr() ? left_expr : right_expr;
    if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *column_expr, &selectivity))) {
      LOG_WARN("failed to get column basic selelectivity", K(ret));
    } else if (T_OP_NE == op_type) {
      selectivity = 1 - selectivity;
    }
  } else {
    // func(col) = func(col)
    double left_sel = 0.0;
    double right_sel = 0.0;
    if (OB_FAIL(get_simple_equal_sel(table_metas, ctx, *left_expr, NULL,
                                     T_OP_NSEQ == op_type, left_sel))) {
      LOG_WARN("Failed to get simple predicate sel", K(ret));
    } else if (OB_FAIL(get_simple_equal_sel(table_metas, ctx, *right_expr, NULL,
                                            T_OP_NSEQ == op_type, right_sel))) {
      LOG_WARN("Failed to get simple predicate sel", K(ret));
    } else {
      selectivity = std::min(left_sel, right_sel);
      if (T_OP_NE == op_type) {
        selectivity = 1 - selectivity;
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_equal_sel(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    ObIArray<ObRawExpr *> &quals,
                                    double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> left_exprs;
  ObSEArray<ObRawExpr *, 4> right_exprs;
  ObSEArray<bool, 4> null_safes;
  bool is_valid;
  if (OB_ISNULL(ctx.get_left_rel_ids()) || OB_ISNULL(ctx.get_right_rel_ids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get unexpected null", K(ret), K(ctx));
  } else if (OB_FAIL(is_valid_multi_join(quals, is_valid))) {
    LOG_WARN("failed to check is valid multi join", K(ret));
  } else if (!is_valid) {
    // multi join condition related to more than two table. Calculate selectivity for each join
    // condition independently.
    for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
      ObRawExpr *cur_expr = quals.at(i);
      double tmp_sel = 1.0;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_equal_sel(table_metas, ctx, *cur_expr, tmp_sel))) {
        LOG_WARN("failed to get equal selectivity", K(ret));
      } else {
        selectivity *= tmp_sel;
      }
    }
  } else if (OB_FAIL(extract_join_exprs(quals, *ctx.get_left_rel_ids(), *ctx.get_right_rel_ids(),
                                        left_exprs, right_exprs, null_safes))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (OB_FAIL(get_cntcols_eq_cntcols_sel(table_metas, ctx, left_exprs, right_exprs,
                                                null_safes, selectivity))) {
    LOG_WARN("Failed to get equal sel", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObOptSelectivity::extract_join_exprs(ObIArray<ObRawExpr *> &quals,
                                         const ObRelIds &left_rel_ids,
                                         const ObRelIds &right_rel_ids,
                                         ObIArray<ObRawExpr *> &left_exprs,
                                         ObIArray<ObRawExpr *> &right_exprs,
                                         ObIArray<bool> &null_safes)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr *cur_expr = quals.at(i);
    if (OB_ISNULL(cur_expr) ||
        OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
        OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr), K(left_expr), K(right_expr));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr)) ||
               OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right_expr, right_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_UNLIKELY(!left_expr->is_column_ref_expr() || !right_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all expr should be column ref", K(ret), K(*cur_expr));
    } else if (left_rel_ids.is_superset(left_expr->get_relation_ids()) &&
               right_rel_ids.is_superset(right_expr->get_relation_ids())) {
      // do nothing
    } else if (left_rel_ids.is_superset(right_expr->get_relation_ids()) &&
               right_rel_ids.is_superset(left_expr->get_relation_ids())) {
      std::swap(left_expr, right_expr);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(left_expr), K(right_expr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(left_exprs.push_back(left_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(right_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(null_safes.push_back(T_OP_NSEQ == cur_expr->get_expr_type()))) {
        LOG_WARN("failed to push back null safe", K(ret));
      }
    }
  }
  return ret;
}

int ObOptSelectivity::get_cntcols_eq_cntcols_sel(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObIArray<ObRawExpr *> &left_exprs,
                                                 const ObIArray<ObRawExpr *> &right_exprs,
                                                 const ObIArray<bool> &null_safes,
                                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_EQ_SEL;
  ObSEArray<double, 4> left_ndvs;
  ObSEArray<double, 4> right_ndvs;
  ObSEArray<double, 4> left_not_null_sels;
  ObSEArray<double, 4> right_not_null_sels;
  double left_ndv = 1.0;
  double right_ndv = 1.0;
  double left_nns = 1.0;
  double right_nns = 1.0;
  double left_rows = 1.0;
  double right_rows = 1.0;
  double left_origin_rows = 1.0;
  double right_origin_rows = 1.0;
  bool left_contain_pk = false;
  bool right_contain_pk = false;
  bool is_union_pk = false;
  bool refine_right_ndv = false;
  bool refine_left_ndv = false;

  if (OB_ISNULL(ctx.get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_columns_contain_pkey(table_metas, left_exprs,
                                             left_contain_pk, is_union_pk))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FALSE_IT(refine_right_ndv = left_contain_pk && is_union_pk)) {
  } else if (OB_FAIL(is_columns_contain_pkey(table_metas, right_exprs,
                                             right_contain_pk, is_union_pk))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (OB_FALSE_IT(refine_left_ndv = right_contain_pk && is_union_pk)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_exprs.count(); ++i) {
      if (OB_ISNULL(left_exprs.at(i)) || OB_ISNULL(right_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *left_exprs.at(i),
                                                &left_ndv, &left_nns))) {
        LOG_WARN("failed to get left ndv and nns", K(ret));
      } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *right_exprs.at(i),
                                                &right_ndv, &right_nns))) {
        LOG_WARN("failed to get left ndv and nns", K(ret));
      } else if (OB_FAIL(left_not_null_sels.push_back(left_nns))) {
        LOG_WARN("failed to push back not null sel", K(ret));
      } else if (OB_FAIL(right_not_null_sels.push_back(right_nns))) {
        LOG_WARN("failed to push back not null sel", K(ret));
      } else if (OB_FAIL(left_ndvs.push_back(left_ndv))) {
        LOG_WARN("failed to push back ndv", K(ret));
      } else if (OB_FAIL(right_ndvs.push_back(right_ndv))) {
        LOG_WARN("failed to push back ndv", K(ret));
      } else if (0 == i) {
        if (OB_FAIL(get_column_basic_info(table_metas, ctx, *left_exprs.at(i),
                                          NULL, NULL, NULL, &left_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *right_exprs.at(i),
                                                 NULL, NULL, NULL, &right_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (refine_right_ndv &&
                  OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(),
                                                ctx, *left_exprs.at(i),
                                                NULL, NULL, NULL, &left_origin_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        } else if (refine_left_ndv &&
                  OB_FAIL(get_column_basic_info(ctx.get_plan()->get_basic_table_metas(),
                                                ctx, *right_exprs.at(i),
                                                NULL, NULL, NULL, &right_origin_rows))) {
          LOG_WARN("failed to get column basic info", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calculate_distinct(table_metas, ctx, left_exprs, left_rows, left_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (OB_FAIL(calculate_distinct(table_metas, ctx, right_exprs, right_rows, right_ndv))) {
    LOG_WARN("Failed to calculate distinct", K(ret));
  } else if (IS_SEMI_ANTI_JOIN(ctx.get_join_type())) {
    /**
     * 对于 semi anti join, 选择率描述的是外表行数为基础的选择率
     * # FORMULA
     * ## non NULL safe
     *  a) semi:  `(min(left_ndv, right_ndv) / left_ndv) * left_not_null_sel(i)`
     * ## NULL safe
     *  a) semi:  non NULL safe selectivity + `nullsafe(i) && left_not_null_sel(i) < 1.0 ? null_sel(i) * selectivity(j) [where j != i]: 0`
     */
    if (IS_LEFT_SEMI_ANTI_JOIN(ctx.get_join_type())) {
      selectivity = std::min(left_ndv, right_ndv) / left_ndv;
      for (int64_t i = 0; i < left_not_null_sels.count(); ++i) {
        selectivity *= left_not_null_sels.at(i);
      }
      // 处理 null safe，这里假设多列上同时为null即小概率事件，只考虑特定列上为null的情况
      for (int64_t i = 0; i < null_safes.count(); ++i) {
        if (OB_UNLIKELY(null_safes.at(i) && right_not_null_sels.at(i) < 1.0)) {
          double factor = 1.0;
          for (int64_t j = 0; j < null_safes.count(); ++j) {
            if (i == j) {
              factor *= (1 - left_not_null_sels.at(j));
            } else {
              factor *= left_not_null_sels.at(j) * std::min(left_ndvs.at(j), right_ndvs.at(j)) / left_ndvs.at(j);
            }
          }
          selectivity += factor;
        }
      }
    } else {
      selectivity = std::min(left_ndv, right_ndv) / right_ndv;
      for (int64_t i = 0; i < right_not_null_sels.count(); ++i) {
        selectivity *= right_not_null_sels.at(i);
      }
      // 处理 null safe，这里假设多列上同时为null即小概率事件，只考虑特定列上为null的情况
      for (int64_t i = 0; i < null_safes.count(); ++i) {
        if (OB_UNLIKELY(null_safes.at(i) && right_not_null_sels.at(i) < 1.0)) {
          double factor = 1.0;
          for (int64_t j = 0; j < null_safes.count(); ++j) {
            if (i == j) {
              factor *= (1 - right_not_null_sels.at(j));
            } else {
              factor *= right_not_null_sels.at(j) * std::min(left_ndvs.at(j), right_ndvs.at(j)) / right_ndvs.at(j);
            }
          }
          selectivity += factor;
        }
      }
    }
  } else {
    /**
     * # FORMULA
     * ## non NULL safe
     * 1 / MAX(ndv1, ndv2) * not_null_frac1_col1 * not_null_frac2_col1 * not_null_frac1_col2 * not_null_frac2_col2 * ...
     * ## NULL safe
     * non NULL safe selectivity + `nullsafe(i) ? (1 - not_null_frac1_col(i)) * (1 - not_null_frac2_col(i)) * selectivity(col(j)) [where j != i]: 0`
     * 目前不会特殊考虑 outer join 的选择率, 而是在外层对行数进行 revise.
     */
    if (left_contain_pk == right_contain_pk) {
      // 两侧都不是主键或都是主键, 不做修正
    } else if (refine_right_ndv) {
      // 一侧有主键时, 认为是主外键连接, 外键上最大的ndv为即为主键的原始ndv
      right_ndv = std::min(right_ndv, left_origin_rows);
    } else if (refine_left_ndv) {
      left_ndv = std::min(left_ndv, right_origin_rows);
    } else {
      // do nothing
    }
    selectivity = 1.0 / std::max(left_ndv, right_ndv);
    for (int64_t i = 0; i < left_not_null_sels.count(); ++i) {
      selectivity *= left_not_null_sels.at(i) * right_not_null_sels.at(i);
    }
    // 处理null safe, 这里假设多列上同时为null即小概率事件，只考虑特定列上为null的情况
    for (int64_t i = 0; i < null_safes.count(); ++i) {
      if (null_safes.at(i)) {
        double factor = 1.0;
        for (int64_t j = 0; j < null_safes.count(); ++j) {
          if (i == j) {
            factor *= (1 - left_not_null_sels.at(j)) * (1 - right_not_null_sels.at(j));
          } else {
            factor *= left_not_null_sels.at(j) * right_not_null_sels.at(j) / std::max(left_ndvs.at(j), right_ndvs.at(j));
          }
        }
        selectivity += factor;
      } else {/* do nothing */}
    }
  }
  LOG_TRACE("selectivity of `col_ref1 =|<=> col_ref1 and col_ref2 =|<=> col_ref2`", K(selectivity));
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

int ObOptSelectivity::get_in_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 const ObRawExpr &qual,
                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = 0.0;
  double tmp_selectivity = 1.0;
  double distinct_sel = 1.0;
  double null_sel = 0.0;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  const ObRawExpr *param_expr = NULL;
  bool contain_null = false;
  if (OB_UNLIKELY(2 != qual.get_param_count()) ||
      OB_ISNULL(left_expr = qual.get_param_expr(0)) ||
      OB_ISNULL(right_expr = qual.get_param_expr(1)) ||
      T_OP_ROW != right_expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect expr", K(ret), K(qual), K(left_expr), K(right_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (OB_LIKELY(left_expr->is_column_ref_expr() && !right_expr->has_flag(CNT_COLUMN))) {
    ObOptColumnStatHandle handler;
    ObObj expr_value;
    bool histogram_valid = false;
    const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(left_expr);
    hash::ObHashSet<ObObj> obj_set;
    double hist_scale = 0;
    if (OB_FAIL(obj_set.create(hash::cal_next_prime(right_expr->get_param_count()), 
                               "OptSelHashSet", "OptSelHashSet"))) {
      LOG_WARN("failed to create hash set", K(ret), K(right_expr->get_param_count()));
    } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *left_expr, &distinct_sel, &null_sel))) {
      LOG_WARN("failed to get column basic selectivity", K(ret));
    } else if (OB_FAIL(get_column_hist_scale(table_metas, ctx, *left_expr, hist_scale))) {
      LOG_WARN("failed to get columnn hist sample scale", K(ret));
    } else if (OB_FAIL(get_histogram_by_column(table_metas, ctx,
                                               col->get_table_id(),
                                               col->get_column_id(),
                                               handler))) {
      LOG_WARN("failed to get histogram by column", K(ret));
    } else if (handler.stat_ != NULL && handler.stat_->get_histogram().is_valid()) {
      histogram_valid = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); ++i) {
      // bool can_use_hist = false;
      bool get_value = false;
      if (OB_ISNULL(param_expr = right_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(get_compare_value(ctx, col, param_expr, expr_value, get_value))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        get_value = false;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (histogram_valid && get_value) {
          double null_sel = 0;
          if (OB_HASH_EXIST == obj_set.exist_refactored(expr_value)) {
            // duplicate value, do nothing
          } else if (OB_FAIL(obj_set.set_refactored(expr_value))) {
            LOG_WARN("failed to set refactorcd", K(ret), K(expr_value));
          } else if (OB_FAIL(get_equal_pred_sel(handler.stat_->get_histogram(),
                                                expr_value,
                                                hist_scale,
                                                tmp_selectivity))) {
            LOG_WARN("failed to get equal density", K(ret));
          } else {
            selectivity += tmp_selectivity * (1 - null_sel);
          }
        } else if (!get_value) {
          // invalid value, for example c1 in (exec_param). Do not check obj exists.
          if (param_expr->get_result_type().is_null()) {
            contain_null = true;
          } else {
            selectivity += distinct_sel;
          }
        } else if (OB_HASH_EXIST == obj_set.exist_refactored(expr_value)) {
          // do nothing
        } else if (OB_FAIL(obj_set.set_refactored(expr_value))) {
          LOG_WARN("failed to set refactorcd", K(ret), K(expr_value));
        } else if (expr_value.is_null()) {
          contain_null = true;
        } else {
          selectivity += distinct_sel;
        }
      }
    }
    if (obj_set.created()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = obj_set.destroy())) {
        LOG_WARN("failed to destroy hash set", K(tmp_ret), K(ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); ++i) {
      if (OB_ISNULL(param_expr = right_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (OB_FAIL(get_equal_sel(table_metas, ctx, *left_expr, *param_expr,
                                       false, tmp_selectivity))) {
        LOG_WARN("Failed to get equal sel", K(ret), KPC(left_expr));
      } else {
        selectivity += tmp_selectivity;
      }
    }
  }

  selectivity = revise_between_0_1(selectivity);
  if (OB_SUCC(ret) && T_OP_NOT_IN == qual.get_expr_type()) {
    selectivity = 1.0 - selectivity;
    if (contain_null) {
      selectivity = 0.0;
    } else if (left_expr->has_flag(CNT_COLUMN) && !right_expr->has_flag(CNT_COLUMN)) {
      ObSEArray<ObRawExpr*, 2> cur_vars;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(left_expr, cur_vars))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (1 == cur_vars.count()) { // only one column, consider null_sel
        if (OB_ISNULL(cur_vars.at(0))) {
          LOG_WARN("expr is null", K(ret));
        } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *cur_vars.at(0),
                                                &distinct_sel, &null_sel))) {
          LOG_WARN("failed to get column basic sel", K(ret));
        } else if (distinct_sel > ((1.0 - null_sel) / 2.0)) {
          // ndv < 2
          // TODO: @yibo 这个refine过程不太理解
          selectivity = distinct_sel / 2.0;
        } else {
          selectivity -= null_sel;
          selectivity = std::max(distinct_sel, selectivity); // at least one distinct_sel
        }
      } else { }//do nothing
    }
  }
  return ret;
}

int ObOptSelectivity::get_is_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 const ObRawExpr &qual,
                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  const ParamStore *params = ctx.get_params();
  const ObDMLStmt *stmt = ctx.get_stmt();
  const ObRawExpr *left_expr = qual.get_param_expr(0);
  const ObRawExpr *right_expr = qual.get_param_expr(1);
  ObObj result;
  bool got_result = false;
  if (OB_ISNULL(params) || OB_ISNULL(stmt) || OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(params), K(stmt), K(left_expr), K(right_expr));
  } else if (OB_UNLIKELY(!ObOptEstUtils::is_calculable_expr(*right_expr, params->count()))) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.get_opt_ctx().get_exec_ctx(),
                                                               right_expr,
                                                               result,
                                                               got_result,
                                                               ctx.get_allocator()))) {
    LOG_WARN("failed to calculate const or calculable expr", K(ret));
  } else if (!got_result) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (left_expr->is_column_ref_expr()) {
    if (OB_FAIL(check_column_in_current_level_stmt(stmt, *left_expr))) {
      LOG_WARN("Failed to check column whether is in current stmt", K(ret));
    } else if (OB_LIKELY(result.is_null())) {
      if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *left_expr, NULL, &selectivity))) {
        LOG_WARN("Failed to get var distinct sel", K(ret));
      }
    } else if (result.is_tinyint() &&
               !ob_is_string_or_lob_type(left_expr->get_data_type())) {
      double distinct_sel = 0.0;
      double null_sel = 0.0;
      if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *left_expr, &distinct_sel, &null_sel))) {
        LOG_WARN("Failed to get var distinct sel", K(ret));
      } else {
        //distinct_num < 2. That is distinct_num only 1,(As double and statistics not completely accurate,
        //use (1 - null_sel)/ 2.0 to check)
        if (distinct_sel > (1 - null_sel) / 2.0) {
          //Ihe formula to calc sel of 'c1 is true' is (1 - distinct_sel(var = 0) - null_sel).
          //If distinct_num is 1, the sel would be 0.0.
          //But we don't kown whether distinct value is 0. So gess the selectivity: (1 - null_sel)/2.0
          distinct_sel = (1- null_sel) / 2.0;//don't kow the value, just get half.
        }
        selectivity = (result.is_true()) ? (1 - distinct_sel - null_sel) : distinct_sel;
      }
    } else { }//default sel
  } else {
    //TODO func(cnt_column)
  }

  if (T_OP_IS_NOT == qual.get_expr_type()) {
    selectivity = 1.0 - selectivity;
  }
  return ret;
}

int ObOptSelectivity::get_range_cmp_sel(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr &qual,
                                        double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_INEQ_SEL;
  const ObRawExpr *left_expr = qual.get_param_expr(0);
  const ObRawExpr *right_expr = qual.get_param_expr(1);
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(left_expr), K(right_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left_expr, left_expr)) ||
             OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right_expr, right_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if ((left_expr->is_column_ref_expr() && right_expr->is_const_expr()) ||
             (left_expr->is_const_expr() && right_expr->is_column_ref_expr())) {
    const ObRawExpr *col_expr = left_expr->is_column_ref_expr() ? left_expr : right_expr;
    if (OB_FAIL(get_column_range_sel(table_metas, ctx,
                                     static_cast<const ObColumnRefRawExpr&>(*col_expr),
                                     qual, selectivity))) {
      LOG_WARN("Failed to get column range sel", K(qual), K(ret));
    }
  } else if (T_OP_ROW == left_expr->get_expr_type() && T_OP_ROW == right_expr->get_expr_type()) {
    //only deal (col1, xx, xx) CMP (const, xx, xx)
    if (left_expr->get_param_count() == 1 && OB_NOT_NULL(left_expr->get_param_expr(0)) &&
        T_OP_ROW == left_expr->get_param_expr(0)->get_expr_type()) {
      left_expr = left_expr->get_param_expr(0);
    }
    if (right_expr->get_param_count() == 1 && OB_NOT_NULL(right_expr->get_param_expr(0)) &&
        T_OP_ROW == right_expr->get_param_expr(0)->get_expr_type()) {
      right_expr = right_expr->get_param_expr(0);
    }
    if (left_expr->get_param_count() != right_expr->get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param count should be equal",
                  K(left_expr->get_param_count()), K(right_expr->get_param_count()));
    } else if (left_expr->get_param_count() <= 1) {
      // do nothing
    } else if (OB_ISNULL(left_expr = left_expr->get_param_expr(0)) ||
               OB_ISNULL(right_expr = right_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
    } else if ((left_expr->is_column_ref_expr() && right_expr->is_const_expr()) ||
               (left_expr->is_const_expr() && right_expr->is_column_ref_expr())) {
      const ObRawExpr *col_expr = (left_expr->is_column_ref_expr()) ? (left_expr) : (right_expr);
      if (OB_FAIL(get_column_range_sel(table_metas, ctx,
                                       static_cast<const ObColumnRefRawExpr&>(*col_expr),
                                       qual, selectivity))) {
        LOG_WARN("failed to get column range sel", K(ret));
      }
    } else { /* no dothing */ }
  }
  return ret;
}

int ObOptSelectivity::get_column_range_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObColumnRefRawExpr &col_expr,
                                           const ObRawExpr &qual,
                                           double &selectivity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> quals;
  if (OB_FAIL(quals.push_back(const_cast<ObRawExpr *>(&qual)))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(get_column_range_sel(table_metas, ctx, col_expr, quals, selectivity))) {
    LOG_WARN("failed to get column range selectivity", K(ret));
  }
  return ret;
}

int ObOptSelectivity::get_column_range_sel(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObColumnRefRawExpr &col_expr,
                                           const ObIArray<ObRawExpr* > &quals,
                                           double &selectivity)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = ctx.get_stmt();
  uint64_t tid = col_expr.get_table_id();
  uint64_t cid = col_expr.get_column_id();
  ObQueryRange query_range;
  ObQueryRangeArray ranges;
  ObSEArray<ColumnItem, 1> column_items;
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
    ObOptColumnStatHandle handler;
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
      } else if (OB_FAIL(get_range_sel_by_histogram(handler.stat_->get_histogram(),
                                                    ranges,
                                                    true,
                                                    hist_scale,
                                                    selectivity))) {
        LOG_WARN("failed to get range sel by histogram", K(ret));
      } else {
        selectivity *= not_null_sel;
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
                                                                    &startscalar, &endscalar))) {
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
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(NULL, NULL, &start_obj, &end_obj,
                                                               NULL, NULL, &startscalar, &endscalar))) {
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

int ObOptSelectivity::get_like_sel(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const ObRawExpr &qual,
                                   double &selectivity,
                                   bool &can_calc_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_INEQ_SEL;
  const ObRawExpr *variable = NULL;
  const ObRawExpr *pattern = NULL;
  const ObRawExpr *escape = NULL;
  const ParamStore *params = ctx.get_params();
  can_calc_sel = false;
  if (3 != qual.get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("like expr should have 3 param", K(ret), K(qual));
  } else if (OB_ISNULL(params) ||
             OB_ISNULL(variable = qual.get_param_expr(0)) ||
             OB_ISNULL(pattern = qual.get_param_expr(1)) ||
             OB_ISNULL(escape = qual.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null params", K(ret), K(params), K(variable), K(pattern), K(escape));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(variable, variable))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (variable->is_column_ref_expr() &&
             ObOptEstUtils::is_calculable_expr(*pattern, params->count()) &&
             ObOptEstUtils::is_calculable_expr(*escape, params->count())) {
    bool is_start_with = false;
    if (is_lob_storage(variable->get_data_type())) {
      // no statistics for lob type, use default selectivity
      selectivity = DEFAULT_CLOB_LIKE_SEL;
    } else if (OB_FAIL(ObOptEstUtils::if_expr_start_with_patten_sign(params, pattern, escape,
                                                                     ctx.get_opt_ctx().get_exec_ctx(),
                                                                     ctx.get_allocator(),
                                                                     is_start_with))) {
      LOG_WARN("failed to check if expr start with percent sign", K(ret));
    } else if (is_start_with) {
      // do nothing
    } else if (OB_FAIL(get_column_range_sel(table_metas, ctx,
                                            static_cast<const ObColumnRefRawExpr&>(*variable),
                                            qual, selectivity))) {
      LOG_WARN("Failed to get column range selectivity", K(ret));
    } else {
      can_calc_sel = true;
    }
  }
  return ret;
}

int ObOptSelectivity::get_btw_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &qual,
                                  double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  const ObRawExpr *cmp_expr = NULL;
  const ObRawExpr *l_expr = NULL;
  const ObRawExpr *r_expr = NULL;
  const ObRawExpr *col_expr = NULL;
  const ParamStore *params = ctx.get_params();
  if (3 != qual.get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("between expr should have 3 param", K(ret), K(qual));
  } else if (OB_ISNULL(params) ||
             OB_ISNULL(cmp_expr = qual.get_param_expr(0)) ||
             OB_ISNULL(l_expr = qual.get_param_expr(1)) ||
             OB_ISNULL(r_expr = qual.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null params", K(ret), K(params), K(cmp_expr), K(l_expr), K(r_expr));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(cmp_expr, cmp_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (cmp_expr->is_column_ref_expr() &&
             ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) &&
             ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
    col_expr = cmp_expr;
  } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) &&
             l_expr->is_column_ref_expr() &&
             ObOptEstUtils::is_calculable_expr(*r_expr, params->count())) {
    col_expr = l_expr;
  } else if (ObOptEstUtils::is_calculable_expr(*cmp_expr, params->count()) &&
             ObOptEstUtils::is_calculable_expr(*l_expr, params->count()) &&
             r_expr->is_column_ref_expr()) {
    col_expr = r_expr;
  }
  if (NULL != col_expr) {
    if (OB_FAIL(get_column_range_sel(table_metas, ctx,
                                     static_cast<const ObColumnRefRawExpr&>(*col_expr),
                                     qual, selectivity))) {
      LOG_WARN("failed to get column range sel", K(ret));
    }
  }
  return ret;
}

int ObOptSelectivity::get_not_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &qual,
                                  double &selectivity,
                                  ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  const ObRawExpr *child_expr = qual.get_param_expr(0);
  double tmp_selectivity = 1.0;
  ObSEArray<ObRawExpr*, 3> cur_vars;
  if (OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (OB_FAIL(calculate_qual_selectivity(table_metas, ctx, *child_expr,
                                                tmp_selectivity, all_predicate_sel))) {
    LOG_WARN("failed to calculate one qual selectivity", K(child_expr), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(child_expr, cur_vars))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (1 == cur_vars.count() &&
             T_OP_IS != child_expr->get_expr_type() &&
             T_OP_IS_NOT != child_expr->get_expr_type() &&
             T_OP_NSEQ != child_expr->get_expr_type()) { // for only one column, consider null_sel
    double null_sel = 1.0;
    if (OB_ISNULL(cur_vars.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *cur_vars.at(0), NULL, &null_sel))) {
      LOG_WARN("failed to get column basic sel", K(ret));
    } else {
      selectivity = 1.0 - null_sel - tmp_selectivity;
    }
  } else {
    // for other condition, it's is too hard to consider null_sel, so ignore it.
    // t_op_is, t_op_nseq , they are null safe exprs, don't consider null_sel.
    selectivity = 1.0 - tmp_selectivity;
  }
  return ret;
}

int ObOptSelectivity::get_ne_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 const ObRawExpr &qual,
                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = qual.get_param_expr(0);
  const ObRawExpr *right_expr = qual.get_param_expr(1);
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(qual), K(left_expr), K(right_expr));
  } else if (OB_FAIL(get_ne_sel(table_metas, ctx, *left_expr, *right_expr, selectivity))) {
    LOG_WARN("failed to get equal sel", K(ret));
  }
  return ret;
}

int ObOptSelectivity::get_ne_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 const ObRawExpr &l_expr,
                                 const ObRawExpr &r_expr,
                                 double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_SEL;
  if (T_OP_ROW == l_expr.get_expr_type() && T_OP_ROW == r_expr.get_expr_type()) {
    // (var1, var2) != (var3, var4) => var1 != var3 or var2 != var4
    selectivity = 0;
    double tmp_selectivity = 1.0;
    const ObRawExpr *l_param = NULL;
    const ObRawExpr *r_param = NULL;
    const ObRawExpr *l_row = &l_expr;
    const ObRawExpr *r_row = &r_expr;
    if (l_expr.get_param_count() == 1 && OB_NOT_NULL(l_expr.get_param_expr(0)) &&
        T_OP_ROW == l_expr.get_param_expr(0)->get_expr_type()) {
      l_row = l_expr.get_param_expr(0);
    }
    if (r_expr.get_param_count() == 1 && OB_NOT_NULL(r_expr.get_param_expr(0)) &&
        T_OP_ROW == r_expr.get_param_expr(0)->get_expr_type()) {
      r_row = r_expr.get_param_expr(0);
    }
    if (OB_UNLIKELY(l_row->get_param_count() != r_row->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", KPC(l_row), KPC(r_row), K(ret));
    } else {
      int64_t num = l_row->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        if (OB_ISNULL(l_param = l_row->get_param_expr(i)) ||
            OB_ISNULL(r_param = r_row->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret), K(l_row), K(r_row), K(i));
        } else if (OB_FAIL(SMART_CALL(get_ne_sel(table_metas, ctx, *l_param,
                                                 *r_param, tmp_selectivity)))) {
          LOG_WARN("failed to get equal selectivity", K(ret));
        } else {
          selectivity += tmp_selectivity - selectivity * tmp_selectivity;
        }
      }
    }
  } else if (l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN)) {
    if (OB_FAIL(get_cntcol_op_cntcol_sel(table_metas, ctx, l_expr, r_expr, T_OP_NE, selectivity))) {
      LOG_WARN("failed to get cntcol op cntcol sel", K(ret));
    }
  } else if ((l_expr.has_flag(CNT_COLUMN) && !r_expr.has_flag(CNT_COLUMN)) ||
             (!l_expr.has_flag(CNT_COLUMN) && r_expr.has_flag(CNT_COLUMN))) {
    const ObRawExpr *cnt_col_expr = l_expr.has_flag(CNT_COLUMN) ? &l_expr : &r_expr;
    const ObRawExpr *const_expr = l_expr.has_flag(CNT_COLUMN) ? &r_expr : &l_expr;
    ObSEArray<const ObColumnRefRawExpr *, 2> column_exprs;
    bool only_monotonic_op = true;
    bool null_const = false;
    double ndv = 1.0;
    double nns = 0;
    bool can_use_hist = false;
    ObObj expr_value;
    ObOptColumnStatHandle handler;
    if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(cnt_col_expr, cnt_col_expr))) {
      LOG_WARN("failed to check is lossless column cast", K(ret));
    } else if (cnt_col_expr->is_column_ref_expr()) {
      // column != const
      const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr*>(cnt_col_expr);
      if (OB_FAIL(get_histogram_by_column(table_metas, ctx, col->get_table_id(),
                                          col->get_column_id(), handler))) {
        LOG_WARN("failed to get histogram by column", K(ret));
      } else if (handler.stat_ == NULL || !handler.stat_->get_histogram().is_valid()) {
        // do nothing
      } else if (OB_FAIL(get_compare_value(ctx, col, const_expr, expr_value, can_use_hist))) {
        // cast may failed due to invalid type or value out of range.
        // Then use ndv instead of histogram
        can_use_hist = false;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (can_use_hist) {
        double hist_scale = 0;
        if (OB_FAIL(get_column_hist_scale(table_metas, ctx, *cnt_col_expr, hist_scale))) {
          LOG_WARN("failed to get columnn hist sample scale", K(ret));
        } else if (OB_FAIL(get_equal_pred_sel(handler.stat_->get_histogram(), expr_value, hist_scale, selectivity))) {
          LOG_WARN("Failed to get equal density", K(ret));
        } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *cnt_col_expr, NULL, &nns))) {
          LOG_WARN("failed to get column ndv and nns", K(ret));
        } else {
          selectivity = (1.0 - selectivity) * nns;
        }
      } else if (OB_FAIL(ObOptEstUtils::extract_column_exprs_with_op_check(cnt_col_expr,
                                                                           column_exprs,
                                                                           only_monotonic_op))) {
        LOG_WARN("failed to extract column exprs with op check", K(ret));
      } else if (!only_monotonic_op || column_exprs.count() > 1) {
        selectivity = DEFAULT_SEL; //cnt_col_expr contain not monotonic op OR has more than 1 var
      } else if (OB_UNLIKELY(1 != column_exprs.count()) || OB_ISNULL(column_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected contain column expr", K(ret), K(*cnt_col_expr));
      } else if (OB_FAIL(ObOptEstUtils::if_expr_value_null(ctx.get_params(),
                                                          *const_expr,
                                                          ctx.get_opt_ctx().get_exec_ctx(),
                                                          ctx.get_allocator(),
                                                          null_const))) {
        LOG_WARN("Failed to check whether expr null value", K(ret));
      } else if (null_const) {
        selectivity = 0.0;
      } else if (OB_FAIL(get_column_ndv_and_nns(table_metas, ctx, *column_exprs.at(0), &ndv, &nns))) {
        LOG_WARN("failed to get column ndv and nns", K(ret));
      } else if (ndv < 2.0) {
        //The reason doing this is similar as get_is_sel function.
        //If distinct_num is 1, As formula, selectivity of 'c1 != 1' would be 0.0.
        //But we don't know the distinct value, so just get the half selectivity.
        selectivity = nns / ndv / 2.0;
      } else {
        selectivity = nns * (1.0 - 1 / ndv);
      }
    }
  } else { }//do nothing
  return ret;
}

int ObOptSelectivity::get_agg_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &qual,
                                  double &selectivity)
{
  int ret = OB_SUCCESS;
  const double origin_rows = ctx.get_row_count_1(); // rows before group by
  const double grouped_rows = ctx.get_row_count_2();// rows after group by
  bool is_valid = false;
  const ObRawExpr *aggr_expr = NULL;
  const ObRawExpr *const_expr1 = NULL;
  const ObRawExpr *const_expr2 = NULL;
  selectivity = DEFAULT_AGG_RANGE;
  ObItemType type = qual.get_expr_type();
  // for aggregate function in having clause, only support
  // =  <=>  !=  >  >=  <  <=  [not] btw  [not] in
  if (-1.0 == origin_rows || -1.0 == grouped_rows) {
    // 不是在group by层计算的having filter，使用默认选择率
    // e.g. select * from t7 group by c1 having count(*) > (select c1 from t8 limit 1);
    //      该sql中having filter需要在subplan filter中计算
  } else if ((type >= T_OP_EQ && type <= T_OP_NE) ||
             T_OP_IN == type || T_OP_NOT_IN == type ||
             T_OP_BTW == type || T_OP_NOT_BTW == type) {
    if (OB_FAIL(is_valid_agg_qual(qual, is_valid, aggr_expr, const_expr1, const_expr2))) {
      LOG_WARN("failed to check is valid agg qual", K(ret));
    } else if (!is_valid) {
      /* use default selectivity */
    } else if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_MAX == aggr_expr->get_expr_type() ||
               T_FUN_MIN == aggr_expr->get_expr_type() ||
               T_FUN_COUNT == aggr_expr->get_expr_type()) {
      if (T_OP_EQ == type || T_OP_NSEQ == type) {
        selectivity = DEFAULT_AGG_EQ;
      } else if (T_OP_NE == type || IS_RANGE_CMP_OP(type)) {
        selectivity = DEFAULT_AGG_RANGE;
      } else if (T_OP_BTW == type) {
        // agg(col) btw const1 and const2  <=> agg(col) > const1 AND agg(col) < const2
        selectivity = DEFAULT_AGG_RANGE * DEFAULT_AGG_RANGE;
      } else if (T_OP_NOT_BTW == type) {
        // agg(col) not btw const1 and const2  <=> agg(col) < const1 OR agg(col) > const2
        // 计算方式参考OR
        selectivity = DEFAULT_AGG_RANGE + DEFAULT_AGG_RANGE;
      } else if (T_OP_IN == type) {
        /**
         *  oracle 对 max/min/count(col) in (const1, const2, const3, ...)的选择率估计
         *  当const的数量小于等于5时，每增加一个const值，选择率增加 DEFAULT_AGG_EQ(0.01)
         *  当const的数量大于5时，每增加一个const值，选择率增加
         *      DEFAULT_AGG_EQ - 0.001 * (const_num - 5)
         *  # 这里的选择率增加量采用线性下降其实并不是很精确，oracle的选择率增加量可能采用了了指数下降，
         *    在测试过程中测试了1-30列递增的情况，线性下降和指数下降区别不大。
         */
        int64_t N;
        if(OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else if (FALSE_IT(N = const_expr1->get_param_count())) {
        } else if (N < 6) {
          selectivity = DEFAULT_AGG_EQ * N;
        } else {
          N = std::min(N, 15L);
          selectivity = DEFAULT_AGG_EQ * 5 + (DEFAULT_AGG_EQ - 0.0005 * (N - 4)) * (N - 5);
        }
      } else if (T_OP_NOT_IN == type) {
        // agg(col) not in (const1, const2, ...) <=> agg(col) != const1 and agg(col) != const2 and ...
        if(OB_ISNULL(const_expr1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null");
        } else {
          selectivity = std::pow(DEFAULT_AGG_RANGE, const_expr1->get_param_count());
        }
      } else { /* use default selectivity */ }
    } else if (T_FUN_SUM == aggr_expr->get_expr_type() || T_FUN_AVG == aggr_expr->get_expr_type()) {
      LOG_TRACE("show group by origen rows and grouped rows", K(origin_rows), K(grouped_rows));
      double rows_per_group = grouped_rows == 0.0 ? origin_rows : origin_rows / grouped_rows;
      if (OB_FAIL(get_agg_sel_with_minmax(table_metas, ctx, *aggr_expr, const_expr1,
                                          const_expr2, type, selectivity, rows_per_group))) {
        LOG_WARN("failed to get agg sel with minmax", K(ret));
      }
    } else { /* not max/min/count/sum/avg, use default selectivity */ }
  } else { /* use default selectivity */ }
  return ret;
}

int ObOptSelectivity::get_agg_sel_with_minmax(const OptTableMetas &table_metas,
                                              const OptSelectivityCtx &ctx,
                                              const ObRawExpr &aggr_expr,
                                              const ObRawExpr *const_expr1,
                                              const ObRawExpr *const_expr2,
                                              const ObItemType type,
                                              double &selectivity,
                                              const double rows_per_group)
{
  int ret = OB_SUCCESS;
  selectivity = DEFAULT_AGG_RANGE;
  const ParamStore *params = ctx.get_params();
  const ObDMLStmt *stmt = ctx.get_stmt();
  ObExecContext *exec_ctx = ctx.get_opt_ctx().get_exec_ctx();
  ObIAllocator &alloc = ctx.get_allocator();
  ObObj result1;
  ObObj result2;
  bool got_result;
  double distinct_sel = 1.0;
  ObObj maxobj;
  ObObj minobj;
  maxobj.set_max_value();
  minobj.set_min_value();
  if (OB_ISNULL(aggr_expr.get_param_expr(0)) || OB_ISNULL(params) ||
      OB_ISNULL(stmt) || OB_ISNULL(const_expr1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_expr.get_param_expr(0)),
                                    K(params), K(stmt), K(const_expr1));
  } else if (!aggr_expr.get_param_expr(0)->is_column_ref_expr()) {
    // 只处理sum(column)的形式，sum(column + 1)/sum(column1 + column2)都是用默认选择率
  } else if (OB_FAIL(get_column_basic_sel(table_metas, ctx, *aggr_expr.get_param_expr(0),
                                          &distinct_sel, NULL))) {
    LOG_WARN("failed to get column basic sel", K(ret));
  } else if (OB_FAIL(get_column_min_max(table_metas, ctx, *aggr_expr.get_param_expr(0),
                                        minobj, maxobj))) {
    LOG_WARN("failed to get column min max", K(ret));
  } else if (minobj.is_min_value() || maxobj.is_max_value()) {
    // do nothing
  } else if (T_OP_IN == type || T_OP_NOT_IN == type) {
    if (OB_UNLIKELY(T_OP_ROW != const_expr1->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should be row", K(ret), K(*const_expr1));
    } else {
      // 如果row超过5列，则计算5列上的选择率，再按比例放大
      int64_t N = const_expr1->get_param_count() > 5 ? 5 :const_expr1->get_param_count();
      selectivity = T_OP_IN == type ? 0.0 : 1.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        double tmp_sel = T_OP_IN == type ? DEFAULT_AGG_EQ : DEFAULT_AGG_RANGE;
        const ObRawExpr *sub_expr = NULL;
        ObObj tmp_result;
        if (OB_ISNULL(sub_expr = const_expr1->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptEstUtils::is_calculable_expr(*sub_expr, params->count())) {
        } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                     sub_expr,
                                                                     tmp_result,
                                                                     got_result,
                                                                     alloc))) {
          LOG_WARN("failed to calc const or calculable expr", K(ret));
        } else if (!got_result) {
          // do nothing
        } else {
          tmp_sel = get_agg_eq_sel(maxobj, minobj, tmp_result, distinct_sel, rows_per_group,
                                   T_OP_IN == type, T_FUN_SUM == aggr_expr.get_expr_type());
        }
        if (T_OP_IN == type) {
          selectivity += tmp_sel;
        } else {
          selectivity *= tmp_sel;
        }
      }
      if (OB_SUCC(ret)) {
        selectivity *= static_cast<double>(const_expr1->get_param_count())
              / static_cast<double>(N);
      }
    }
  } else if (!ObOptEstUtils::is_calculable_expr(*const_expr1, params->count())) {
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                               const_expr1,
                                                               result1,
                                                               got_result,
                                                               alloc))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (!got_result) {
    // do nothing
  } else if (T_OP_EQ == type || T_OP_NSEQ == type) {
    selectivity = get_agg_eq_sel(maxobj, minobj, result1, distinct_sel, rows_per_group,
                                 true, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_NE == type) {
    selectivity = get_agg_eq_sel(maxobj, minobj, result1, distinct_sel, rows_per_group,
                                 false, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (IS_RANGE_CMP_OP(type)) {
    selectivity = get_agg_range_sel(maxobj, minobj, result1, rows_per_group,
                                    type, T_FUN_SUM == aggr_expr.get_expr_type());
  } else if (T_OP_BTW == type || T_OP_NOT_BTW == type) {
    if (OB_ISNULL(const_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptEstUtils::is_calculable_expr(*const_expr2, params->count())) {
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                 const_expr2,
                                                                 result2,
                                                                 got_result,
                                                                 alloc))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      // do nothing
    } else {
      selectivity = get_agg_btw_sel(maxobj, minobj, result1, result2, rows_per_group,
                                    type, T_FUN_SUM == aggr_expr.get_expr_type());
    }
  } else { /* do nothing */ }
  return ret;
}

// 计算sum/avg(col) =/<=>/!= const的选择率
double ObOptSelectivity::get_agg_eq_sel(const ObObj &maxobj,
                                        const ObObj &minobj,
                                        const ObObj &constobj,
                                        const double distinct_sel,
                                        const double rows_per_group,
                                        const bool is_eq,
                                        const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_EQ;
  if (constobj.is_null()) {
    // sum/avg(col)的结果中不会存在null，即使是null safe equal选择率依然为0
    sel_ret = 0.0;
  } else if (minobj.is_integer_type() ||
             (minobj.is_number() && minobj.get_meta().get_obj_meta().get_scale() == 0) ||
             (minobj.is_unumber() && minobj.get_meta().get_obj_meta().get_scale() == 0)) {
    double const_val;
    double min_val;
    double max_val;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val)) ||
        OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val)) ||
        OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg eq sel", K(max_val), K(min_val), K(const_val));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      int64_t length = max_val - min_val + 1;
      if (is_eq) {
        sel_ret = 1.0 / length;
        if (const_val < min_val) {
          sel_ret -= sel_ret * (min_val - const_val) / length;
        } else if (const_val > max_val) {
          sel_ret -= sel_ret * (const_val - max_val) / length;
        } else {}
      } else {
        sel_ret = 1.0 - 1.0 / length;
      }
    }
  } else {
    // 对于非整数的类型，认为sum/avg(col)后 ndv 不会发生显著变化，直接使用该列原有的ndv计算
    sel_ret = is_eq ? distinct_sel : 1.0 - distinct_sel;
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

// 计算sum/avg(col) >/>=/</<= const的选择率
double ObOptSelectivity::get_agg_range_sel(const ObObj &maxobj,
                                           const ObObj &minobj,
                                           const ObObj &constobj,
                                           const double rows_per_group,
                                           const ObItemType type,
                                           const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj.is_null()) {
    // sum/avg(col)的结果中不会存在null，因此选择率为0
    sel_ret = 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj, const_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg range sel", K(max_val), K(min_val), K(const_val));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      double length = max_val - min_val + 1.0;
      if (T_OP_GE == type || T_OP_GT == type) {
        if (T_OP_GT == type) {
          // c1 > 1 <=> c1 >= 2, 对非int类型的列并不精确
          const_val += 1.0;
        }
        if (const_val <= min_val) {
          sel_ret = 1.0;
        } else if (const_val <= max_val) {
          sel_ret = (max_val - const_val + 1.0) / length;
        } else {
          sel_ret = 1.0 / length;
          sel_ret -= sel_ret * (const_val - max_val) / length;
        }
      } else if (T_OP_LE == type || T_OP_LT == type) {
        if (T_OP_LT == type) {
          // c1 < 1 <=> c1 <= 0, 对非int类型的列并不精确
          const_val -= 1.0;
        }
        if (const_val >= max_val) {
          sel_ret = 1.0;
        } else if (const_val >= min_val) {
          sel_ret = (const_val - min_val + 1.0) / length;
        } else {
          sel_ret = 1.0 / length;
          sel_ret -= sel_ret * (min_val - const_val) / length;
        }
      } else { /* do nothing */ }
    }
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

// 计算sum/avg(col) [not] between const1 and const2的选择率
double ObOptSelectivity::get_agg_btw_sel(const ObObj &maxobj,
                                         const ObObj &minobj,
                                         const ObObj &constobj1,
                                         const ObObj &constobj2,
                                         const double rows_per_group,
                                         const ObItemType type,
                                         const bool is_sum)
{
  int ret = OB_SUCCESS;
  double sel_ret = DEFAULT_AGG_RANGE;
  if (constobj1.is_null() || constobj2.is_null()) {
    sel_ret= 0.0;
  } else {
    double min_val;
    double max_val;
    double const_val1;
    double const_val2;
    // 如果转化的时候出错，就使用默认的选择率
    if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&minobj, min_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&maxobj, max_val))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj1, const_val1))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else if (OB_FAIL(ObOptEstObjToScalar::convert_obj_to_double(&constobj2, const_val2))) {
      LOG_WARN("failed to convert obj to double", K(ret));
    } else {
      LOG_TRACE("get values for agg between sel", K(max_val), K(min_val), K(const_val1), K(const_val2));
      if (is_sum) {
        min_val *= rows_per_group;
        max_val *= rows_per_group;
      }
      double length = max_val - min_val + 1.0;
      if (T_OP_BTW == type) {
        if (const_val1 > const_val2) {
          sel_ret = 0.0;
        } else {
          double tmp_min = std::max(const_val1, min_val);
          double tmp_max = std::min(const_val2, max_val);
          sel_ret = (tmp_max - tmp_min + 1.0) / length;
        }
      } else if (T_OP_NOT_BTW == type){
        if (const_val1 > const_val2) {
          sel_ret = 1.0;
        } else {
          double tmp_min = std::max(const_val1, min_val);
          double tmp_max = std::min(const_val2, max_val);
          sel_ret = 1 - (tmp_max - tmp_min + 1.0) / length;
        }
      } else { /* do nothing */ }
    }
  }
  sel_ret = revise_between_0_1(sel_ret);
  return sel_ret;
}

int ObOptSelectivity::is_valid_agg_qual(const ObRawExpr &qual,
                                        bool &is_valid,
                                        const ObRawExpr *&aggr_expr,
                                        const ObRawExpr *&const_expr1,
                                        const ObRawExpr *&const_expr2)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObRawExpr *expr0 = NULL;
  const ObRawExpr *expr1 = NULL;
  const ObRawExpr *expr2 = NULL;
  if (T_OP_BTW == qual.get_expr_type() || T_OP_NOT_BTW == qual.get_expr_type()) {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) ||
        OB_ISNULL(expr1 = qual.get_param_expr(1)) ||
        OB_ISNULL(expr2 = qual.get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr0->has_flag(IS_AGG) &&
               expr1->is_const_expr() &&
               expr2->is_const_expr()) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
      const_expr2 = expr2;
    } else { /* do nothing */ }
  } else {
    if (OB_ISNULL(expr0 = qual.get_param_expr(0)) || OB_ISNULL(expr1 = qual.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_OP_IN == qual.get_expr_type() || T_OP_NOT_IN == qual.get_expr_type()) {
      if (!qual.has_flag(CNT_SUB_QUERY) &&
          expr0->has_flag(IS_AGG) &&
          T_OP_ROW == expr1->get_expr_type()) {
        is_valid = true;
        aggr_expr = expr0;
        const_expr1 = expr1;
      } else { /* do nothing */ }
    } else if (expr0->has_flag(IS_AGG) &&
               expr1->is_const_expr()) {
      is_valid = true;
      aggr_expr = expr0;
      const_expr1 = expr1;
    } else if (expr0->is_const_expr() &&
               expr1->has_flag(IS_AGG)) {
      is_valid = true;
      aggr_expr = expr1;
      const_expr1 = expr0;
    } else { /* do nothing */ }
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
    LOG_TRACE("column ndv and not null sel", K(ndv), K(not_null_sel));
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

int ObOptSelectivity::get_range_sel_by_histogram(const ObHistogram &histogram,
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
        } else if (OB_FAIL(get_range_pred_sel(histogram,
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
int ObOptSelectivity::get_less_pred_sel(const ObHistogram &histogram,
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
      if (OB_FAIL(ObOptEstObjToScalar::convert_objs_to_scalars(
                    &minobj, &maxobj, &startobj, &endobj,
                    &minscalar, &maxscalar, &startscalar, &endscalar))) {
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
int ObOptSelectivity::get_greater_pred_sel(const ObHistogram &histogram,
                                           const ObObj &minv,
                                           const bool inclusive,
                                           double &density)
{
  int ret = OB_SUCCESS;
  double less_sel = 0;
  if (OB_FAIL(get_less_pred_sel(histogram,
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
int ObOptSelectivity::get_range_pred_sel(const ObHistogram &histogram,
                                         const ObObj &minv,
                                         const bool min_inclusive,
                                         const ObObj &maxv,
                                         const bool max_inclusive,
                                         double &density)
{
  int ret = OB_SUCCESS;
  double less_sel = 0;
  double greater_sel = 0;
  if (OB_FAIL(get_greater_pred_sel(histogram, minv, min_inclusive, greater_sel))) {
    LOG_WARN("failed to get greater predicate selectivity", K(ret));
  } else if (OB_FAIL(get_less_pred_sel(histogram, maxv, max_inclusive, less_sel))) {
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

/**
 * 算法流程:
 * 1. 简化所有的group by表达式, 取出group by中所有的列
 * 2. 对于多个列存在于同一个 Equal Set, 只保留 ndv 最小的一个.
 * 3. distinct number = ndv(column1) * (ndv(column2) / sqrt(2)) * (ndv(column3) / sqrt(2)) * ...
 */
int ObOptSelectivity::calculate_distinct(const OptTableMetas &table_metas,
                                         const OptSelectivityCtx &ctx,
                                         const ObIArray<ObRawExpr*>& exprs,
                                         const double origin_rows,
                                         double &rows,
                                         const bool need_refine)
{
  int ret = OB_SUCCESS;
  rows = 1;
  // 记录各个列的ndv中的最大值
  ObSEArray<ObRawExpr*, 16> column_exprs;
  ObSEArray<ObRawExpr*, 16> filtered_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(exprs, column_exprs))) {
    LOG_WARN("failed to extract all column", K(ret));
  } else if (OB_FAIL(filter_column_by_equal_set(table_metas, ctx, column_exprs, filtered_exprs))) {
    LOG_WARN("failed filter column by equal set", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < filtered_exprs.count(); ++i) {
    ObRawExpr *column_expr = filtered_exprs.at(i);
    double ndv = 0.0;
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret), K(i));
    } else if (OB_FAIL(check_column_in_current_level_stmt(ctx.get_stmt(), *column_expr))) {
      LOG_WARN("failed to check column in current level stmt", K(ret));
    } else if (OB_FAIL(get_column_basic_info(table_metas, ctx, *column_expr, &ndv, NULL, NULL, NULL))) {
      LOG_WARN("failed to get column basic info", K(ret), K(*column_expr));
    } else if (0 == i) {
      rows *= ndv;
    } else {
      rows *= ndv / std::sqrt(2);
    }
  }
  if (OB_SUCC(ret) && need_refine) {
    rows = std::min(rows, origin_rows);
    LOG_TRACE("succeed to calculate distinct", K(origin_rows), K(rows), K(exprs));
  }
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

/**
 *  check if multi join condition only related to two table
 */
int ObOptSelectivity::is_valid_multi_join(ObIArray<ObRawExpr *> &quals,
                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_UNLIKELY(quals.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quals should have more than 1 exprs", K(ret));
  } else if (OB_ISNULL(quals.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObRelIds &rel_ids = quals.at(0)->get_relation_ids();
    is_valid = rel_ids.num_members() == 2;
    for (int64_t i = 1; OB_SUCC(ret) && is_valid && i < quals.count(); ++i) {
      ObRawExpr *cur_expr = quals.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!rel_ids.equal(cur_expr->get_relation_ids())) {
        is_valid = false;
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

int ObOptSelectivity::classify_quals(const ObIArray<ObRawExpr*> &quals,
                                     ObIArray<ObExprSelPair> &all_predicate_sel,
                                     ObIArray<OptSelInfo> &column_sel_infos)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *qual = NULL;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  OptSelInfo *sel_info = NULL;
  double tmp_selectivity = 1.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    column_exprs.reset();
    uint64_t column_id = OB_INVALID_ID;
    ObColumnRefRawExpr *column_expr = NULL;
    if (OB_ISNULL(qual = quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(qual, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (1 == column_exprs.count()) {
      column_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
      column_id = column_expr->get_column_id();
    } else {
      // use OB_INVALID_ID represent qual contain more than one column
    }

    if (OB_SUCC(ret) && OB_INVALID_ID != column_id && OB_NOT_NULL(column_expr)) {
      sel_info = NULL;
      for (int64_t j = 0; j < column_sel_infos.count(); ++j) {
        if (column_sel_infos.at(j).column_id_ == column_id) {
          sel_info = &column_sel_infos.at(j);
          break;
        }
      }
      if (NULL == sel_info) {
        if (OB_ISNULL(sel_info = column_sel_infos.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate place holder for sel info", K(ret));
        } else {
          sel_info->column_id_ = column_id;
          int64_t offset = 0;
          if (ObOptimizerUtil::find_item(all_predicate_sel,
                                         ObExprSelPair(column_expr, 0, true),
                                         &offset)) {
            sel_info->range_selectivity_ = all_predicate_sel.at(offset).sel_;
            sel_info->has_range_exprs_ = true;
          }
        }
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


}//end of namespace sql
}//end of namespace oceanbase
