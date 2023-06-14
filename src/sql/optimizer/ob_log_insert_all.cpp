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
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "ob_log_insert_all.h"
#include "sql/ob_phy_table_location.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/engine/expr/ob_expr_column_conv.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace share;
using namespace oceanbase::share::schema;

const char* ObLogInsertAll::get_name() const
{
  const char *ret = "MULTI TABLE INSERT";
  return ret;
}

int ObLogInsertAll::get_plan_item_info(PlanText &plan_text,
                                       ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(print_table_infos(ObString::make_string("columns"),
                                  buf,
                                  buf_len,
                                  pos,
                                  type))) {
      LOG_WARN("failed to print table infos", K(ret));
    } else if (is_multi_conditions_insert()) {
      if (is_multi_insert_first() &&
          OB_FAIL(BUF_PRINTF(", multi_table_first(%s),\n      ", "true"))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (!is_multi_insert_first() && OB_FAIL(BUF_PRINTF(",\n      "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (get_insert_all_table_info() == NULL) {
        //do nothing
      } else if (OB_ISNULL(get_stmt()) ||
                 OB_UNLIKELY(get_table_list().count() != get_insert_all_table_info()->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_insert_all_table_info()->count()), K(get_stmt()),
                                          K(ret));
      } else {
        int64_t pre_idx = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < get_insert_all_table_info()->count(); ++i) {
          ObInsertAllTableInfo* table_info = get_insert_all_table_info()->at(i);
          const TableItem *table_item = nullptr;
          if (OB_ISNULL(table_info) ||
              OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(table_info->table_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(table_item), K(table_info), K(ret));
          } else if (table_info->when_cond_idx_ == pre_idx) {
            BUF_PRINTF(",\"%.*s\"", table_item->table_name_.length(),table_item->table_name_.ptr());
          } else if (table_info->when_cond_idx_ != -1) {//when
            if (pre_idx != -1) {
              BUF_PRINTF("}; ");
            }
            const ObIArray<ObRawExpr *> &when_exprs = table_info->when_cond_exprs_;
            EXPLAIN_PRINT_EXPRS(when_exprs, type);
            BUF_PRINTF(" then: table_list{");
            BUF_PRINTF("\"%.*s\"", table_item->table_name_.length(), table_item->table_name_.ptr());
            pre_idx = table_info->when_cond_idx_;
          } else {//else
            BUF_PRINTF("}; ELSE: table_list{");
            BUF_PRINTF("\"%.*s\"", table_item->table_name_.length(), table_item->table_name_.ptr());
            pre_idx = table_info->when_cond_idx_;
          }
        }
        if (OB_SUCC(ret)) {
          BUF_PRINTF("},\n      ");
          int64_t cnt = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
            const IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
            if (OB_ISNULL(dml_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("dml info is null", K(ret), K(dml_info));
            } else if (!dml_info->is_primary_index_) {
              continue;
            } else {
              const ObIArray<ObRawExpr *> &conv_exprs = dml_info->column_convert_exprs_;
              EXPLAIN_PRINT_EXPRS(conv_exprs, type);
              if (cnt < get_table_list().count() - 1) {
                BUF_PRINTF(",\n      ");
              }
              ++ cnt;
            }
          }
        }
      }
    } else if (OB_FAIL(BUF_PRINTF(",\n      "))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else {
      int64_t cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
        const IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
        if (OB_ISNULL(dml_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dml info is null", K(ret), K(dml_info));
        } else if (!dml_info->is_primary_index_) {
          continue;
        } else {
          const ObIArray<ObRawExpr *> &conv_exprs = dml_info->column_convert_exprs_;
          EXPLAIN_PRINT_EXPRS(conv_exprs, type);
          if (cnt < get_table_list().count() - 1) {
            BUF_PRINTF(",\n      ");
          }
          ++ cnt;
        }
      }
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogInsertAll::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_all_table_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_all_table_info_), K(ret));
  } else if (OB_FAIL(ObLogInsert::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to allocate expr pre", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_all_table_info_->count(); ++i) {
      if (OB_ISNULL(insert_all_table_info_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret), K(i));
      } else if (OB_FAIL(append(all_exprs, insert_all_table_info_->at(i)->when_cond_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

// int ObLogInsertAll::generate_rowid_expr_for_trigger()
// {
//   int ret = OB_SUCCESS;
//   // oracle mode does not have replace or insert on duplicate
//   if (lib::is_oracle_mode() && !has_instead_of_trigger()) {
//     for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
//       bool has_trg = false;
//       IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
//       if (OB_ISNULL(dml_info)) {
//         ret = OB_ERR_UNEXPECTED;
//         LOG_WARN("dml info is null", K(ret), K(dml_info));
//       } else if (!dml_info->is_primary_index_) {
//         // do nothing
//       } else if (OB_FAIL(check_has_trigger(dml_info->ref_table_id_, has_trg))) {
//         LOG_WARN("failed to check has trigger", K(ret));
//       } else if (!has_trg) {
//         // do nothing
//       } else if (OB_FAIL(generate_old_rowid_expr(*dml_info))) {
//         LOG_WARN("failed to generate rowid expr", K(ret));
//       } else if (OB_FAIL(generate_insert_new_rowid_expr(*dml_info))) {
//         LOG_WARN("failed to generate new rowid expr", K(ret));
//       } else { /*do nothing*/ }
//     }
//   }
//   return ret;
// }

// int ObLogInsertAll::generate_multi_part_partition_id_expr()
// {
//   int ret = OB_SUCCESS;
//   for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
//     if (OB_ISNULL(get_index_dml_infos().at(i))) {
//       ret = OB_ERR_UNEXPECTED;
//       LOG_WARN("index dml info is null", K(ret));
//     } else if (OB_FAIL(generate_old_calc_partid_expr(*get_index_dml_infos().at(i)))) {
//       LOG_WARN("failed to generate calc partid expr", K(ret));
//     } else if (OB_FAIL(generate_insert_new_calc_partid_expr(*get_index_dml_infos().at(i)))) {
//       LOG_WARN("failed to generate new calc partid expr", K(ret));
//     } else { /*do nothing*/ }
//   }
//   return ret;
// }

int ObLogInsertAll::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_all_table_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret));
  } else if (OB_FAIL(ObLogInsert::inner_replace_op_exprs(replacer))) {
    LOG_WARN("failed to replace op exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_all_table_info_->count(); ++i) {
    if (OB_ISNULL(insert_all_table_info_->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret), K(i));
    } else if (OB_FAIL(replace_exprs_action(replacer,
                                            insert_all_table_info_->at(i)->when_cond_exprs_))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}
