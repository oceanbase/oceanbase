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
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_granule_iterator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

const char *ObLogJoinFilter::get_name() const
{
  const char *name = NULL;
  if (is_partition_filter()) {
    name = is_create_ ? "PART JOIN FILTER CREATE" : "INVALID JOIN FILTER";
  } else {
    name = is_create_ ? "JOIN FILTER CREATE" : "JOIN FILTER USE";
  }
  return name;
}

int ObLogJoinFilter::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *first_child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(first_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first_child is null", K(ret));
  } else {
    // refine this
    set_op_cost(0.0);
    set_cost(first_child->get_cost());
    set_card(first_child->get_card());
  }
  return ret;
}

int ObLogJoinFilter::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL != calc_tablet_id_expr_ && OB_FAIL(all_exprs.push_back(calc_tablet_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, join_exprs_))) {
    LOG_WARN("failed to add exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

uint64_t ObLogJoinFilter::hash(uint64_t seed) const
{
  seed = do_hash(is_create_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

int ObLogJoinFilter::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, join_exprs_))) {
    LOG_WARN("failed to replace join exprs", K(ret));
  } else if (OB_NOT_NULL(calc_tablet_id_expr_)
      && OB_FAIL(replace_expr_action(replacer, calc_tablet_id_expr_))) {
    LOG_WARN("failed to replace calc_tablet_id_expr_", K(ret));
  }
  return ret;
}

int ObLogJoinFilter::get_plan_item_info(PlanText &plan_text,
                                        ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  bool is_first = false;
  static const char *join_filter_type_name[] =
      {"bloom", "range", "in"};
  int64_t arr_len = sizeof(join_filter_type_name) / sizeof(const char *);
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else if (OB_INVALID_ID != get_filter_id()) {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF(":RF%04ld", get_filter_id()))) {
      LOG_WARN("failed to print str", K(ret));
    }
    END_BUF_PRINT(plan_item.object_alias_,
                  plan_item.object_alias_len_);
  }
  if (OB_SUCC(ret) && is_create_) {
    BEGIN_BUF_PRINT;
    int64_t idx = 0;
    const char *type_name = nullptr;
    if (OB_FAIL(BUF_PRINTF("RF_TYPE("))) {
      LOG_WARN("fail to print rf", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < join_filter_types_.count(); ++i) {
      idx = (int64_t)(join_filter_types_.at(i)) - 1;
      if (idx >= arr_len || idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret));
      } else if (FALSE_IT(type_name = join_filter_type_name[idx])) {
      } else if (is_first && OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                    (int)strlen(type_name),
                                    type_name))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        is_first = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to print rf", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!join_exprs_.empty()) {
        if (OB_FAIL(BUF_PRINTF(", RF_EXPR["))) {
          LOG_WARN("fail to print rf", K(ret));
        } else {
          if (OB_ISNULL(calc_tablet_id_expr_)) {
            int cnt = join_exprs_.count();
            for (int i = 0; i < cnt && OB_SUCC(ret); ++i) {
              if (OB_FAIL(join_exprs_.at(i)->get_name(buf, buf_len, pos, type))) {
                LOG_WARN("fail to get name", K(ret));
              } else if (i != cnt - 1 && OB_FAIL(BUF_PRINTF(", "))) {
                LOG_WARN("fail to print buf", K(ret));
              }
            }
          } else {
            if (OB_FAIL(calc_tablet_id_expr_->get_name(buf, buf_len, pos, type))) {
              LOG_WARN("fail to get name", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(BUF_PRINTF("]"))) {
            LOG_WARN("fail to print buf", K(ret));
          }
        }
      }
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}
