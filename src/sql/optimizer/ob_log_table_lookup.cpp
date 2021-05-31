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
#include "sql/optimizer/ob_log_table_lookup.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

int ObLogTableLookup::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogTableLookup* table_lookup = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogTableScan", K(ret));
  } else if (OB_ISNULL(table_lookup = static_cast<ObLogTableLookup*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOpertor* to ObLogTableScan*", K(ret));
  } else {
    table_lookup->table_id_ = table_id_;
    table_lookup->ref_table_id_ = ref_table_id_;
    table_lookup->index_id_ = index_id_;
    table_lookup->set_index_name(index_name_);
    table_lookup->set_table_name(table_name_);
    table_lookup->set_index_back_scan(index_back_scan_);
    table_lookup->calc_part_id_expr_ = calc_part_id_expr_;
    out = table_lookup;
  }
  return ret;
}

int ObLogTableLookup::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  reset_local_ordering();
  return ret;
}

int ObLogTableLookup::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_back_scan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(index_back_scan_), K(ret));
  } else {
    index_back_scan_->set_branch_id(branch_id_);
    index_back_scan_->set_id(id_);
    if (OB_FAIL(index_back_scan_->allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr for children operator", K(ret));
    } else if (!is_plan_root() && OB_FAIL(append(output_exprs_, index_back_scan_->get_output_exprs()))) {
      LOG_WARN("failed to append output exprs", K(ret));
    } else if (OB_FAIL(append_not_produced_exprs(ctx.not_produced_exprs_))) {
      LOG_WARN("fail to append not produced exprs", K(ret));
    }
  }
  return ret;
}

int ObLogTableLookup::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObLogicalOperator* child = NULL;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to check is basic sharing info", K(ret));
  } else if (is_basic) {
    /*do nothing*/
  } else if (!get_pushdown_filter_exprs().empty() || child->get_card() > 1000) {
    if (OB_FAIL(get_sharding_info().copy_with_part_keys(child->get_sharding_info()))) {
      LOG_WARN("failed to deep copy sharding info from first child", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange", K(ret));
  } else {
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
  }
  if (OB_SUCC(ret)) {
    get_plan()->set_location_type(ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN);
  }
  return ret;
}

int ObLogTableLookup::compute_property(Path* path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(path->parent_) || OB_UNLIKELY(path->path_type_ != ACCESS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path or join order is null", K(ret), K(path));
  } else if (OB_FAIL(ObLogicalOperator::compute_property(path))) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    set_card(path->parent_->get_output_rows());
    set_op_cost(static_cast<const AccessPath*>(path)->index_back_cost_);
    set_cost(path->cost_);
  }
  return ret;
}

int ObLogTableLookup::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  ObLogTableScan* scan_child = NULL;
  double query_range_row_count = 0.0;
  double index_back_row_count = 0.0;
  UNUSED(parent);
  if (need_row_count >= get_card()) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null children node", K(ret));
  } else if (OB_UNLIKELY(log_op_def::LOG_TABLE_SCAN != child->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpceted child type", K(child->get_type()), K(ret));
  } else if (FALSE_IT(scan_child = static_cast<ObLogTableScan*>(child))) {
    /*do nothing*/
  } else if (OB_ISNULL(scan_child->get_est_cost_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("est cost info is not set", K(ret));
  } else if (OB_UNLIKELY(scan_child->get_est_cost_info()->table_filter_sel_ <= 0.0 ||
                         scan_child->get_est_cost_info()->postfix_filter_sel_ <= 0.0)) {
    /*do nothing*/
  } else {
    index_back_row_count = need_row_count / scan_child->get_est_cost_info()->table_filter_sel_;
    query_range_row_count = index_back_row_count / scan_child->get_est_cost_info()->postfix_filter_sel_;
    if (query_range_row_count < 0.0 || index_back_row_count < 0.0 ||
        scan_child->query_range_row_count_ < OB_DOUBLE_EPSINON) {
      /*do nothing*/
    } else {
      re_est = true;
      double cost = 0;
      double index_back_cost = 0;
      double physical_query_range_row_count =
          query_range_row_count * scan_child->phy_query_range_row_count_ / scan_child->query_range_row_count_;
      LOG_TRACE("start to re-estimate for table look up operator",
          K(index_back_row_count),
          K(query_range_row_count),
          K(physical_query_range_row_count));
      if (OB_FAIL(ObOptEstCost::cost_table_one_batch(*scan_child->est_cost_info_,
              scan_child->est_cost_info_->batch_type_,
              true,
              scan_child->est_cost_info_->table_scan_param_.column_ids_.count(),
              query_range_row_count,
              physical_query_range_row_count,
              cost,
              index_back_cost))) {
        LOG_WARN("failed to estimate cost", K(ret));
      } else {
        LOG_TRACE("succeed to re-estimate for table lookup operator", K(cost), K(index_back_cost));
        scan_child->set_card(index_back_row_count);
        scan_child->set_cost(cost - index_back_cost);
        scan_child->set_op_cost(cost - index_back_cost);
        card_ = need_row_count;
        op_cost_ = index_back_cost;
        cost_ = cost;
      }
    }
  }
  return ret;
}

uint64_t ObLogTableLookup::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(table_id_, hash_value);
  hash_value = do_hash(ref_table_id_, hash_value);
  hash_value = do_hash(index_id_, hash_value);
  hash_value = do_hash(table_name_, hash_value);
  hash_value = do_hash(index_name_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogTableLookup::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  UNUSED(checker);
  return ret;
}

int ObLogTableLookup::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  if (OB_ISNULL(index_back_scan_) || OB_ISNULL(index_back_scan_->get_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_back_scan_), K(ret));
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    const ObPhyPartitionLocationInfoIArray& partitions =
        index_back_scan_->table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list();
    const bool two_level = (schema::PARTITION_LEVEL_TWO == index_back_scan_->table_partition_info_->get_part_level());
    ObSEArray<int64_t, 128> pids;
    int64_t N = partitions.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObOptPartLoc& part_loc = partitions.at(i).get_partition_location();
      int64_t pid = part_loc.get_partition_id();
      if (OB_FAIL(pids.push_back(pid))) {
        LOG_WARN("failed to add partition id", K(pid), K(i), K(ret));
      } else {
        std::sort(pids.begin(), pids.end(), compare_partition_id);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalOperator::explain_print_partitions(pids, two_level, buf, buf_len, pos))) {
        LOG_WARN("Failed to print partitions");
      }
    }
  }
  return ret;
}

int ObLogTableLookup::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(calc_part_id_expr_));

  return ret;
}

int ObLogTableLookup::init_calc_part_id_expr()
{
  int ret = OB_SUCCESS;
  calc_part_id_expr_ = NULL;
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
  ObSQLSessionInfo* session = NULL;
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  ObRawExpr* new_part_expr = NULL;
  ObRawExpr* new_subpart_expr = NULL;
  if (OB_ISNULL(get_plan()) || OB_INVALID_ID == ref_table_id_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (!session->use_static_typing_engine()) {
    // do nothing
  } else {
    share::schema::ObSchemaGetterGuard* schema_guard = NULL;
    const share::schema::ObTableSchema* table_schema = NULL;
    if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ref_table_id_), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(table_schema));
    } else {
      bool no_primary_key = table_schema->is_no_pk_table();
      ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
      if (OB_FAIL(get_part_exprs(table_id_, ref_table_id_, part_level, part_expr, subpart_expr))) {
        LOG_WARN("fail to get part exprs", K(ret));
      }
      new_part_expr = part_expr;
      new_subpart_expr = subpart_expr;
      if (OB_SUCC(ret) && !no_primary_key && NULL != part_expr) {
        if (OB_FAIL(replace_gen_column(part_expr, new_part_expr))) {
          LOG_WARN("fail to replace generate column", K(ret), K(part_expr));
        }
      }
      if (OB_SUCC(ret) && !no_primary_key && NULL != subpart_expr) {
        if (OB_FAIL(replace_gen_column(subpart_expr, new_subpart_expr))) {
          LOG_WARN("fail to replace generate column", K(ret), K(subpart_expr));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(expr_factory,
                *session,
                ref_table_id_,
                part_level,
                new_part_expr,
                new_subpart_expr,
                calc_part_id_expr_))) {
          LOG_WARN("fail to build table location expr", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObLogTableLookup::replace_gen_column(ObRawExpr* part_expr, ObRawExpr*& new_part_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  new_part_expr = part_expr;
  if (OB_ISNULL(part_expr)) {
    // do nothing
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("fail to extract column exprs", K(part_expr), K(ret));
  } else {
    bool cnt_gen_columns = false;
    for (int64_t i = 0; !cnt_gen_columns && OB_SUCC(ret) && i < column_exprs.count(); i++) {
      if (OB_ISNULL(column_exprs.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (static_cast<ObColumnRefRawExpr*>(column_exprs.at(i))->is_generated_column()) {
        cnt_gen_columns = true;
      }
    }
    if (OB_SUCC(ret) && cnt_gen_columns) {
      ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
      new_part_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, part_expr, new_part_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("fail to copy part expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
          ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i));
          if (!col_expr->is_generated_column()) {
            // do nothing
          } else if (OB_ISNULL(col_expr->get_dependant_expr())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("generate column's dependant expr is null", K(ret));
          } else if (OB_FAIL(
                         ObRawExprUtils::replace_ref_column(new_part_expr, col_expr, col_expr->get_dependant_expr()))) {
            LOG_WARN("fail to replace ref column", K(ret));
          }
        }  // for cols end
      }
    }
  }

  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
