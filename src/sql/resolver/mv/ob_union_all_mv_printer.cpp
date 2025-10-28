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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_union_all_mv_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/mv/ob_simple_mav_printer.h"
#include "sql/resolver/mv/ob_simple_mjv_printer.h"
#include "sql/resolver/mv/ob_simple_join_mav_printer.h"
#include "sql/resolver/mv/ob_outer_join_mjv_printer.h"
#include "sql/resolver/mv/ob_outer_join_mav_printer.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObUnionAllMVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt*, 8> cur_dml_stmts;
  dml_stmts.reuse();
  ctx_.marker_idx_ = marker_idx_;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_refresh_types_.count(); ++i) {
    cur_dml_stmts.reuse();
    if (OB_ISNULL(mv_def_stmt_.get_set_query(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i));
    } else if (OB_FAIL(gen_child_refresh_dmls(child_refresh_types_.at(i),
                                              *mv_def_stmt_.get_set_query(i),
                                              cur_dml_stmts))) {
      LOG_WARN("failed to get child refresh dmls", K(ret), K(i), K(child_refresh_types_.at(i)),
                                          KPC(mv_def_stmt_.get_set_query(i)));
    } else if (OB_FAIL(append(dml_stmts, cur_dml_stmts))) {
      LOG_WARN("failed to append dml stmts", K(ret));
    }
  }
  ctx_.marker_idx_ = OB_INVALID_INDEX;
  return ret;
}

int ObUnionAllMVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  return OB_NOT_SUPPORTED;
}

int ObUnionAllMVPrinter::gen_child_refresh_dmls(const ObMVRefreshableType refresh_type,
                                                const ObSelectStmt &child_sel_stmt,
                                                ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>, 8> child_expand_aggrs;
  dml_stmts.reuse();
  if (OB_ISNULL(mlog_tables_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(mlog_tables_));
  } else if (child_sel_stmt.has_group_by()
             && OB_FAIL(get_child_expand_aggrs(child_sel_stmt, child_expand_aggrs))) {
    LOG_WARN("failed to get child expand aggrs", K(ret));
  } else {
    switch (refresh_type) {
      case OB_MV_FAST_REFRESH_SIMPLE_MAV: {
        ObSimpleMAVPrinter printer(ctx_, mv_schema_, mv_container_schema_, child_sel_stmt, *mlog_tables_, child_expand_aggrs);
        if (OB_FAIL(printer.gen_child_refresh_dmls_for_union_all(dml_stmts))) {
          LOG_WARN("failed to gen child refresh dmls for union all", K(ret));
        }
        break;
      }
      case OB_MV_FAST_REFRESH_SIMPLE_MJV: {
        ObSimpleMJVPrinter printer(ctx_, mv_schema_, mv_container_schema_, child_sel_stmt, *mlog_tables_);
        if (OB_FAIL(printer.gen_child_refresh_dmls_for_union_all(dml_stmts))) {
          LOG_WARN("failed to gen child refresh dmls for union all", K(ret));
        }
        break;
      }
      case OB_MV_FAST_REFRESH_OUTER_JOIN_MJV: {
        ObOuterJoinMJVPrinter printer(ctx_, mv_schema_, mv_container_schema_, child_sel_stmt, *mlog_tables_);
        if (OB_FAIL(printer.gen_child_refresh_dmls_for_union_all(dml_stmts))) {
          LOG_WARN("failed to gen child refresh dmls for union all", K(ret));
        }
        break;
      }
      case OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV: {
        ObSimpleJoinMAVPrinter printer(ctx_, mv_schema_, mv_container_schema_, child_sel_stmt, *mlog_tables_, child_expand_aggrs);
        if (OB_FAIL(printer.gen_child_refresh_dmls_for_union_all(dml_stmts))) {
          LOG_WARN("failed to gen child refresh dmls for union all", K(ret));
        }
        break;
      }
      case OB_MV_FAST_REFRESH_OUTER_JOIN_MAV: {
        ObOuterJoinMAVPrinter printer(ctx_, mv_schema_, mv_container_schema_, child_sel_stmt, *mlog_tables_, child_expand_aggrs);
        if (OB_FAIL(printer.gen_child_refresh_dmls_for_union_all(dml_stmts))) {
          LOG_WARN("failed to gen child refresh dmls for union all", K(ret));
        }
        break;
      }
      default:  {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported child refresh type for union all", K(ret), K(refresh_type));
        break;
      }
    }
  }
  return ret;
}

int ObUnionAllMVPrinter::get_child_expand_aggrs(const ObSelectStmt &child_sel_stmt,
                                                ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &child_expand_aggrs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expand_aggrs_.count(); ++i) {
    if (ObOptimizerUtil::find_item(child_sel_stmt.get_aggr_items(), expand_aggrs_.at(i).first)) {
      if (OB_FAIL(child_expand_aggrs.push_back(expand_aggrs_.at(i)))) {
        LOG_WARN("failed to push back expand aggr", K(ret));
      }
    }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
