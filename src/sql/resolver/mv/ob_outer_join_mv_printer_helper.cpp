/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_outer_join_mv_printer_helper.h"
#include "sql/resolver/mv/ob_mv_printer.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObOuterJoinMVPrinterHelper::init_outer_join_mv_printer_helper()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(right_table_idxs_.prepare_allocate(printer_.mv_def_stmt_.get_table_size()))) {
    LOG_WARN("failed to prepare allocate table idx array", K(ret), K(printer_.mv_def_stmt_.get_table_size()));
  }
  return ret;
}

/**
 * @brief ObOuterJoinMVPrinterHelper::gen_refresh_dmls_for_table
 *
 * For OuterJoinMJV, generate refresh dml for table,
 * For OuterJoinMAV, generate inner delta mav for table.
 */
int ObOuterJoinMVPrinterHelper::gen_refresh_dmls_for_table(const TableItem *table,
                                                           const JoinedTable *upper_table,
                                                           ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    const TableItem *first_table = NULL;  // outer join padded null side
    const TableItem *second_table = NULL; // outer join non-padded null side
    if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      first_table = joined_table->left_table_;
      second_table = joined_table->right_table_;
    } else {
      first_table = joined_table->right_table_;
      second_table = joined_table->left_table_;
    }
    if (OB_FAIL(SMART_CALL(gen_refresh_dmls_for_table(first_table,
                                                      joined_table,
                                                      dml_stmts)))) {
      LOG_WARN("failed to gen refresh dmls for the first table", K(ret));
    } else if (OB_FAIL(SMART_CALL(gen_refresh_dmls_for_table(second_table,
                                                             upper_table,
                                                             dml_stmts)))) {
      LOG_WARN("failed to gen refresh dmls for the second table", K(ret));
    }
  } else {
    int64_t delta_table_idx = -1;
    if (OB_FAIL(printer_.mv_def_stmt_.get_table_item_idx(table, delta_table_idx))) {
      LOG_WARN("failed to get delta table idx", K(ret));
    } else if (printer_.is_table_skip_refresh(*table)) {
      // do nothing, no need to refresh
    } else if (NULL == upper_table || INNER_JOIN == upper_table->joined_type_) {
      if (OB_FAIL(gen_refresh_dmls_for_inner_join(table,
                                                  delta_table_idx,
                                                  dml_stmts))) {
        LOG_WARN("failed to gen refresh dmls for inner join", K(ret));
      }
    } else if (OB_FAIL(gen_refresh_dmls_for_left_join(table,
                                                      delta_table_idx,
                                                      upper_table,
                                                      dml_stmts))) {
      LOG_WARN("failed to gen refresh dmls for left join", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(update_table_idx_array(delta_table_idx,
                                                       upper_table))) {
      LOG_WARN("failed to update table idx array", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMVPrinterHelper::update_table_idx_array(const int64_t delta_table_idx,
                                                       const JoinedTable *upper_table)
{
  int ret = OB_SUCCESS;
  ObRelIds join_cond_relids;
  if (OB_FAIL(refreshed_table_idxs_.add_member(delta_table_idx))) {
    LOG_WARN("failed to add member", K(ret), K(delta_table_idx));
  } else if (NULL != upper_table) {
    for (int64_t i = 0; OB_SUCC(ret) && i < upper_table->join_conditions_.count(); ++i) {
      const ObRawExpr *expr = upper_table->join_conditions_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null expr", K(ret), K(i));
      } else if (OB_FAIL(join_cond_relids.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_table_idxs_.count(); ++i) {
      if (!join_cond_relids.has_member(i + 1) || delta_table_idx == i) {
        // do nothing
      } else if (OB_FAIL(right_table_idxs_.at(i).add_member(delta_table_idx))) {
        LOG_WARN("failed to add member", K(ret), K(delta_table_idx));
      } else if (OB_FAIL(right_table_idxs_.at(i).add_members(right_table_idxs_.at(delta_table_idx)))) {
        LOG_WARN("failed to add members", K(ret), K(delta_table_idx));
      }
    }
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase