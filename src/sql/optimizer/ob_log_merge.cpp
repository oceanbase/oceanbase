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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_log_merge.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/ob_phy_table_location.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace share;
using namespace oceanbase::share::schema;

int ObLogMerge::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogInsert::print_my_plan_annotation(buf, buf_len, pos, type))) {
    LOG_WARN("fail to print plan annotaqqtion", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_ISNULL(match_condition_exprs_) || OB_ISNULL(update_condition_exprs_) ||
             OB_ISNULL(insert_condition_exprs_) || OB_ISNULL(delete_condition_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some condition exprs is NULL",
        KPC(match_condition_exprs_),
        KPC(update_condition_exprs_),
        KPC(insert_condition_exprs_),
        KPC(delete_condition_exprs_),
        K(ret));
  } else {
    const ObIArray<ObRawExpr*>& match_conds = *match_condition_exprs_;
    const ObIArray<ObRawExpr*>& update_conds = *update_condition_exprs_;
    const ObIArray<ObRawExpr*>& delete_conds = *delete_condition_exprs_;
    const ObIArray<ObRawExpr*>& insert_conds = *insert_condition_exprs_;
    EXPLAIN_PRINT_EXPRS(match_conds, type);
    if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    EXPLAIN_PRINT_EXPRS(insert_conds, type);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }

    EXPLAIN_PRINT_EXPRS(update_conds, type);
    if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    EXPLAIN_PRINT_EXPRS(delete_conds, type);
  }
  return ret;
}

int ObLogMerge::add_all_table_assignments_to_ctx(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  int64_t table_cnt = tables_assignments_->count();
  if (table_cnt == 0) {
    // do nothing
  } else if (table_cnt == 1) {
    ObSEArray<ObRawExpr*, 8> exprs;
    const ObAssignments& assigns = tables_assignments_->at(0).assignments_;
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      if (OB_ISNULL(assigns.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exprs.push_back(assigns.at(i).expr_))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_merge_exprs_to_ctx(ctx, exprs))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    LOG_WARN("unexpected assignment table count", K(table_cnt));
  }
  return ret;
}

int ObLogMerge::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_all_table_assignments_to_ctx(ctx))) {
    LOG_WARN("fail to add all table assignment", K(ret));
  } else if (NULL != match_condition_exprs_ && OB_FAIL(add_merge_exprs_to_ctx(ctx, *match_condition_exprs_))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (NULL != insert_condition_exprs_ && OB_FAIL(add_merge_exprs_to_ctx(ctx, *insert_condition_exprs_))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (NULL != update_condition_exprs_ && OB_FAIL(add_merge_exprs_to_ctx(ctx, *update_condition_exprs_))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (OB_FAIL(add_delete_exprs_to_ctx(ctx))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (nullptr != value_vector_ && OB_FAIL(add_merge_exprs_to_ctx(ctx, *value_vector_))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (OB_FAIL(add_all_source_table_columns_to_ctx(ctx))) {
    LOG_WARN("fail to add source table columns to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  }
  return ret;
}

int ObLogMerge::add_merge_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  uint64_t producer_id = OB_INVALID_ID;
  ObSEArray<ObRawExpr*, 8> subquery_exprs;
  ObSEArray<ObRawExpr*, 8> non_subquery_exprs;
  if (OB_FAIL(classify_merge_subquery_expr(exprs, subquery_exprs, non_subquery_exprs))) {
    LOG_WARN("failed to classify merge subquery exprs", K(ret));
  } else if (!non_subquery_exprs.empty() && OB_FAIL(add_exprs_to_ctx(ctx, non_subquery_exprs))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_next_producer_id(child, producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, subquery_exprs, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogMerge::classify_merge_subquery_expr(
    const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs, ObIArray<ObRawExpr*>& non_subquery_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      bool has_subquery = false;
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(
                     exprs.at(i), &get_plan()->get_onetime_exprs(), has_subquery))) {
        LOG_WARN("failed to check whether contain subquery", K(ret));
      } else if (has_subquery) {
        ret = subquery_exprs.push_back(exprs.at(i));
      } else {
        ret = non_subquery_exprs.push_back(exprs.at(i));
      }
    }
  }
  return ret;
}

int ObLogMerge::add_delete_exprs_to_ctx(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  ObSEArray<ObRawExpr*, 8> subquery_exprs;
  ObSEArray<ObRawExpr*, 8> non_subquery_exprs;
  if (OB_ISNULL(delete_condition_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(classify_merge_subquery_expr(*delete_condition_exprs_, subquery_exprs, non_subquery_exprs))) {
    LOG_WARN("failed to classify merge subquery exprs", K(ret));
  } else {
    if (!non_subquery_exprs.empty()) {
      ObSEArray<ObRawExpr*, 8> delete_column_exprs;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(non_subquery_exprs, delete_column_exprs))) {
        LOG_WARN("fail to extract column exprs", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, delete_column_exprs))) {
        LOG_WARN("fail to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret) && !subquery_exprs.empty()) {
      ObLogicalOperator* child = NULL;
      uint64_t producer_id = OB_INVALID_ID;
      if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_next_producer_id(child, producer_id))) {
        LOG_WARN("failed to get next producer id", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, subquery_exprs, producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogMerge::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogInsert::check_output_dep_specific(checker))) {
    LOG_WARN("ObLogDelUpd::check_output_dep_specific fails", K(ret));
  } else {
    if (NULL != match_condition_exprs_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < match_condition_exprs_->count(); i++) {
        if (OB_ISNULL(match_condition_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*match_condition_exprs_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    if (OB_SUCC(ret) && NULL != insert_condition_exprs_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_condition_exprs_->count(); i++) {
        if (OB_ISNULL(insert_condition_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*insert_condition_exprs_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    if (OB_SUCC(ret) && NULL != update_condition_exprs_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < update_condition_exprs_->count(); i++) {
        if (OB_ISNULL(update_condition_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*update_condition_exprs_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    if (OB_SUCC(ret) && NULL != delete_condition_exprs_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_condition_exprs_->count(); i++) {
        if (OB_ISNULL(delete_condition_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*delete_condition_exprs_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    if (OB_SUCC(ret) && NULL != value_vector_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < value_vector_->count(); i++) {
        if (OB_ISNULL(value_vector_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*value_vector_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    if (OB_SUCC(ret) && NULL != rowkey_exprs_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
        if (OB_ISNULL(rowkey_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*rowkey_exprs_->at(i)))) {
          LOG_WARN("failed to check expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

uint64_t ObLogMerge::hash(uint64_t seed) const
{
  if (NULL != match_condition_exprs_) {
    HASH_PTR_ARRAY(*match_condition_exprs_, seed);
  }
  if (NULL != insert_condition_exprs_) {
    HASH_PTR_ARRAY(*insert_condition_exprs_, seed);
  }
  if (NULL != update_condition_exprs_) {
    HASH_PTR_ARRAY(*update_condition_exprs_, seed);
  }
  if (NULL != delete_condition_exprs_) {
    HASH_PTR_ARRAY(*delete_condition_exprs_, seed);
  }
  if (NULL != value_vector_) {
    HASH_PTR_ARRAY(*value_vector_, seed);
  }
  if (NULL != rowkey_exprs_) {
    HASH_PTR_ARRAY(*rowkey_exprs_, seed);
  }
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogMerge::add_all_source_table_columns_to_ctx(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(table_columns_), !table_columns_->empty());

  OZ(add_exprs_to_ctx(ctx, *table_columns_));

  return ret;
}

int ObLogMerge::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(ObLogDelUpd::inner_append_not_produced_exprs(raw_exprs));
  if (OB_SUCC(ret)) {
    if (nullptr != match_condition_exprs_ && OB_FAIL(raw_exprs.append(*match_condition_exprs_))) {
      LOG_WARN("append_array_no_dup failed", K(ret));
    }
    if (nullptr != insert_condition_exprs_ && OB_FAIL(raw_exprs.append(*insert_condition_exprs_))) {
      LOG_WARN("append_array_no_dup failed", K(ret));
    }
    if (nullptr != update_condition_exprs_ && OB_FAIL(raw_exprs.append(*update_condition_exprs_))) {
      LOG_WARN("append_array_no_dup failed", K(ret));
    }
    if (nullptr != delete_condition_exprs_ && OB_FAIL(raw_exprs.append(*delete_condition_exprs_))) {
      LOG_WARN("append_array_no_dup failed", K(ret));
    }
    if (nullptr != rowkey_exprs_ && OB_FAIL(raw_exprs.append(*rowkey_exprs_))) {
      LOG_WARN("append_array_no_dup failed", K(ret));
    }
  }
  OZ(ObLogInsert::inner_append_not_produced_exprs(raw_exprs));
  return ret;
}

const char* ObLogMerge::get_name() const
{
  const char* ret = "NOT SET";
  if (is_multi_part_dml()) {
    ret = "MULTI PARTITION MERGE";
  } else {
    ret = "MERGE";
  }

  return ret;
}
