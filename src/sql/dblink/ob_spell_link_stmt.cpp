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
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/dml/ob_group_by_checker.h"
#include "lib/worker.h"

using namespace oceanbase::sql;
using namespace oceanbase::sql::log_op_def;

const ObString GenLinkStmtPostContext::JOIN_ON_           = " on (1 = 1)";
const ObString GenLinkStmtPostContext::LEFT_BRACKET_      = "(";
const ObString GenLinkStmtPostContext::RIGHT_BRACKET_     = ")";
const ObString GenLinkStmtPostContext::SEP_DOT_           = ".";
const ObString GenLinkStmtPostContext::SEP_COMMA_         = ", ";
const ObString GenLinkStmtPostContext::SEP_AND_           = " and ";
const ObString GenLinkStmtPostContext::SEP_SPACE_         = " ";
const ObString GenLinkStmtPostContext::UNION_ALL_         = "all ";
const ObString GenLinkStmtPostContext::ORDER_ASC_         = " asc";
const ObString GenLinkStmtPostContext::ORDER_DESC_        = " desc";
const ObString GenLinkStmtPostContext::ROWNUM_            = "rownum";
const ObString GenLinkStmtPostContext::LESS_EQUAL_        = " <= ";
const ObString GenLinkStmtPostContext::SELECT_CLAUSE_     = "select ";
const ObString GenLinkStmtPostContext::SELECT_DIS_CLAUSE_ = "select distinct ";
const ObString GenLinkStmtPostContext::FROM_CLAUSE_       = " from ";
const ObString GenLinkStmtPostContext::WHERE_CLAUSE_      = " where (1 = 1)";
const ObString GenLinkStmtPostContext::GROUPBY_CLAUSE_    = " group by ";
const ObString GenLinkStmtPostContext::GROUPBY_ROLLUP_    = " group by rollup(";
const ObString GenLinkStmtPostContext::COMMA_ROLLUP_      = ", rollup(";
const ObString GenLinkStmtPostContext::HAVING_CLAUSE_     = " having ";
const ObString GenLinkStmtPostContext::ORDERBY_CLAUSE_    = " order by ";
const ObString GenLinkStmtPostContext::WHERE_EXISTS_      = " where exists (select 1 from ";
const ObString GenLinkStmtPostContext::WHERE_NOT_EXISTS_  = " where not exists (select 1 from ";
const ObString GenLinkStmtPostContext::DOUBLE_QUOTATION_  = "\"";
const ObString GenLinkStmtPostContext::ASTERISK_          = "*";
const ObString GenLinkStmtPostContext::NULL_STR_          = "";


ObJoinType GenLinkStmtPostContext::reverse_join_type(ObJoinType join_type)
{
  ObJoinType ret = UNKNOWN_JOIN;
  switch (join_type) {
    case INNER_JOIN: 
      ret = INNER_JOIN;
      break;
    case LEFT_OUTER_JOIN:
      ret = RIGHT_OUTER_JOIN;
      break;
    case RIGHT_OUTER_JOIN:
      ret = LEFT_OUTER_JOIN;
      break;
    case FULL_OUTER_JOIN:
      ret = FULL_OUTER_JOIN;
      break;
    default:
      // do nothing
      break;
  }
  return ret;
}

const ObString &GenLinkStmtPostContext::join_type_str(ObJoinType join_type) const
{
  static const ObString join_type_strs[] =
  {
    " unknown join",
    " inner join ",
    " left join ",
    " right join ",
    " full join ",
    " where exists ",
    " where exists ",
    " where not exists ",
    " where not exists ",
    " connect by ",
  };
  if (OB_UNLIKELY(join_type < UNKNOWN_JOIN || join_type > CONNECT_BY_JOIN)) {
    join_type = UNKNOWN_JOIN;
  }
  return join_type_strs[join_type];
}

const ObString &GenLinkStmtPostContext::set_type_str(ObSelectStmt::SetOperator set_type) const
{
  static const ObString set_type_strs[] =
  {
    " none ",
    " union ",
    " intersect ",
    " minus ",
    " recursive ",
  };
  if (OB_UNLIKELY(set_type <= ObSelectStmt::NONE || set_type >= ObSelectStmt::SET_OP_NUM)) {
    set_type = ObSelectStmt::NONE;
  }
  return set_type_strs[set_type];
}

int64_t GenLinkStmtPostContext::get_order_item_index(const ObRawExpr *order_item_expr,
                                                     const ObRawExprIArray &output_exprs)
{
  int64_t index = -1;
  if (OB_NOT_NULL(order_item_expr)) {
    for (int64_t i = 0; i < output_exprs.count(); i++) {
      if (OB_NOT_NULL(output_exprs.at(i)) &&
          order_item_expr == output_exprs.at(i)) {
        index = i;
        break;
      }
    }
  }
  return index;
}

int GenLinkStmtPostContext::fill_string(const ObString &str, bool with_double_quotation)
{
  int ret = OB_SUCCESS;
  int64_t expected_tmp_buf_len = tmp_buf_pos_ + str.length() + 1 + 
                                (with_double_quotation ? (2 * DOUBLE_QUOTATION_.length()) : 0);
  if (OB_ISNULL(tmp_buf_) || tmp_buf_len_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill string failed, invalid argument", 
              K(dblink_id_), KP(tmp_buf_), K(tmp_buf_len_), K(str), K(tmp_buf_pos_), K(ret));
  } else if (expected_tmp_buf_len > tmp_buf_len_ && 
             OB_FAIL(extend_tmp_buf(expected_tmp_buf_len))) {
    LOG_WARN("failed to extend tmp_buf", K(tmp_buf_len_), K(dblink_id_), K(ret));
  } else {
    if (with_double_quotation) {
      MEMCPY(tmp_buf_ + tmp_buf_pos_, DOUBLE_QUOTATION_.ptr(), DOUBLE_QUOTATION_.length());
      tmp_buf_pos_ += DOUBLE_QUOTATION_.length();
    }
    MEMCPY(tmp_buf_ + tmp_buf_pos_, str.ptr(), str.length());
    tmp_buf_pos_ += str.length();
    if (with_double_quotation) {
      MEMCPY(tmp_buf_ + tmp_buf_pos_, DOUBLE_QUOTATION_.ptr(), DOUBLE_QUOTATION_.length());
      tmp_buf_pos_ += DOUBLE_QUOTATION_.length();
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_strings(const ObStringIArray &strs, const ObString &sep, bool skip_first_sep)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_buf_) || tmp_buf_len_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill string failed, invalid argument", 
              K(dblink_id_), KP(tmp_buf_), K(tmp_buf_len_), K(tmp_buf_pos_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < strs.count(); ++i) {
      if (!(0 == i && skip_first_sep) && OB_FAIL(fill_string(sep))) {
        LOG_WARN("failed to fill sep", K(sep), K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(strs.at(i)))) {
        LOG_WARN("failed to fill strs.at(i)", K(strs.at(i)), K(i), K(dblink_id_), K(ret));
      }
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_expr(const ObRawExpr *expr,
                                      const ObString &sep,
                                      bool is_bool_expr,
                                      bool fill_column_alias,
                                      const ObRawExpr *subplanscan_expr)
{
  int ret = OB_SUCCESS;
  const ObString &alias_column_name = expr->get_alias_column_name();
  if (OB_ISNULL(expr) || OB_ISNULL(tmp_buf_) || tmp_buf_len_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("faill expr failed, invalid argument", 
              K(dblink_id_), KP(expr), KP(tmp_buf_), K(tmp_buf_len_), K(tmp_buf_pos_), K(ret));
  } else if (OB_FAIL(fill_string(sep))) {
    LOG_WARN("failed to fill sep", K(sep), K(dblink_id_), K(ret));
  } else {

    int temp_ret = OB_SUCCESS;
    do {
      temp_ret = OB_SUCCESS;
      int64_t temp_pos = tmp_buf_pos_;
      ObRawExprPrinter expr_printer; // fill expr to tem_buf_ using expr_printer
      expr_printer.init(tmp_buf_, tmp_buf_len_, &tmp_buf_pos_, schema_guard_, NULL);
      expr_printer.set_gen_unique_name(&gen_unique_alias_);
      if (OB_SUCCESS != (temp_ret = expr_printer.do_print(const_cast<ObRawExpr *>(expr), 
                                                          T_DBLINK_SCOPE, 
                                                          false, 
                                                          is_bool_expr))) {
        if (OB_SIZE_OVERFLOW == temp_ret ||
            OB_BUF_NOT_ENOUGH == temp_ret) {
          tmp_buf_pos_ = temp_pos;
          if(OB_FAIL(extend_tmp_buf())) {
            LOG_WARN("failed to extend tmp_buf", K(tmp_buf_len_), K(dblink_id_), K(ret));
          }
        } else {
          ret = temp_ret;
          LOG_WARN("failed to print raw expr", KPC(expr), K(dblink_id_), K(ret));
        }
      }
    // If the length of tmp_buf_ is not enough, so that the error OB_SIZE_OVERFLOW / OB_BUF_NOT_ENOUGH is reported, 
    // then expand tmp_buf_.
    } while(OB_SUCC(ret) && (OB_SIZE_OVERFLOW == temp_ret || OB_BUF_NOT_ENOUGH == temp_ret));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if ((!alias_column_name.empty() || NULL != subplanscan_expr) &&
               OB_FAIL(fill_string(SEP_SPACE_))) {
      LOG_WARN("failed to fill SEP_SPACE_", K(SEP_SPACE_), K(dblink_id_), K(ret));
    } else if (NULL == subplanscan_expr && 
              !alias_column_name.empty() && 
              fill_column_alias && 
              OB_FAIL(fill_string(alias_column_name, true))) {
      LOG_WARN("failed to fill alias_column_name", K(alias_column_name), K(dblink_id_), K(ret));
    } else if (NULL != subplanscan_expr) {
      /**
      As shown in the following plan.
      The output of operator 2 is "VIEW1.TB.C2". 
      This is a problem that the optimizer group has not 
      maintained the subplanscan output column name. 
      In order to deal with this problem, 
      dblink need to add an alias name "TB.C2" to "VIEW1.TB.C2" 
      to ensure that dblink reverse-spelled SQL conforms to the SQL syntex.
      
      explain select * from t1@lcq_link_1 ta where exists 
      (select * from t2@lcq_link_1 tb where tb.c2 = ta.c1 and tb.c1 > 1) and ta.c1 < 5
      | ================================================
      |ID|OPERATOR             |NAME |EST. ROWS|COST |
      ------------------------------------------------
      |0 |LINK                 |     |10000    |88854|
      |1 | HASH RIGHT SEMI JOIN|     |10000    |88854|
      |2 |  SUBPLAN SCAN       |VIEW1|1001     |41016|
      |3 |   TABLE SCAN        |TB   |1001     |41001|
      |4 |  TABLE SCAN         |TA   |10000    |40790|
      ================================================

      Outputs & filters: 
      -------------------------------------
        0 - output([TA.C1], [TA.C2], [TA.C3]), filter(nil), dblink_id=1100611139403793,
            link_stmt=
        1 - output([TA.C1], [TA.C2], [TA.C3]), filter(nil), 
            equal_conds([VIEW1.TB.C2 = TA.C1]), other_conds(nil)
        2 - output([VIEW1.TB.C2]), filter(nil), 
            access([VIEW1.TB.C2])
        3 - output([TB.C2]), filter([TB.C2 < 5], [TB.C1 > 1]), 
            access([TB.C2], [TB.C1]), partitions(p0)
        4 - output([TA.C1], [TA.C2], [TA.C3]), filter([TA.C1 < 5]), 
            access([TA.C1], [TA.C2], [TA.C3]), partitions(p0)
       */
      ObString column_name;
      if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(alloc_, 
                      static_cast<const ObColumnRefRawExpr *>(subplanscan_expr)->get_column_name(),
                      column_name, true))) {
        LOG_WARN("fail to generate new name with escape character", K(ret));
      } else if (!column_name.empty() && OB_FAIL((fill_string(column_name, true)))) {
        LOG_WARN("failed to fill expr column_name", K(column_name), K(dblink_id_), K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief 
 * @param exprs, expr array need to be filled into tmp_buf_
 * @param sep, used to separate expr in tmp_buf_
 * @param is_bool_expr, means expr in exprs maybe a bool expr
 * @param skip_first_sep, some times filling operation need to skip the first sep
 * @param skip_nl_param, means expr in exprs maybe pushed down from nestloop join 
 * @param fill_column_alias, means fill expr like that #"table_name"."column_name" "alias_column_name"#
 * @param subplanscan_outputs, This is just a patch about the wrong column_name 
 * of the output of the subplanscan operator that the optimizer does not maintain well.
 * Here is a sql case that need to use @param subplanscan_outputs.
  explain select * from t1@lcq_link_1 ta where exists (select * from t2@lcq_link_1 tb where tb.c2 = ta.c1 and tb.c1 > 1) and ta.c1 < 5\G
  *************************** 1. row ***************************
  Query Plan: ===============================================
  |ID|OPERATOR            |NAME |EST. ROWS|COST |
  -----------------------------------------------
  |0 |HASH RIGHT SEMI JOIN|     |10000    |88854|
  |1 | SUBPLAN SCAN       |VIEW1|1001     |41016|
  |2 |  LINK              |     |1001     |41001|
  |3 |   TABLE SCAN       |TB   |1001     |41001|
  |4 | LINK               |     |10000    |40790|
  |5 |  TABLE SCAN        |TA   |10000    |40790|
  ===============================================

  Outputs & filters:
  -------------------------------------
    0 - output([TA.C1], [TA.C2], [TA.C3]), filter(nil),
        equal_conds([VIEW1.TB.C2 = TA.C1]), other_conds(nil)
    1 - output([VIEW1.TB.C2]), filter(nil),
        access([VIEW1.TB.C2])
    2 - output([TB.C2]), filter(nil), dblink_id=1100611139403803,
        link_stmt=select "TB"."C2" from LCQ1.T2 TB where (1 = 1) and ("TB"."C2" < 5) and ("TB"."C1" > 1)
    3 - output([TB.C2]), filter([TB.C2 < 5], [TB.C1 > 1]),
        access([TB.C2], [TB.C1]), partitions(p0)
    4 - output([TA.C1], [TA.C2], [TA.C3]), filter(nil), dblink_id=1100611139403803,
        link_stmt=select "TA"."C1", "TA"."C2", "TA"."C3" from LCQ1.T1 TA where (1 = 1) and ("TA"."C1" < 5)
    5 - output([TA.C1], [TA.C2], [TA.C3]), filter([TA.C1 < 5]),
        access([TA.C1], [TA.C2], [TA.C3]), partitions(p0)
 */
int GenLinkStmtPostContext::fill_exprs(const ObRawExprIArray &exprs,
                                       const ObString &sep,
                                       bool is_bool_expr,
                                       bool skip_first_sep,
                                       bool skip_nl_param,
                                       bool fill_column_alias,
                                       const ObRawExprIArray *subplanscan_outputs)
{
  int ret = OB_SUCCESS;
  bool is_first = true;
  bool in_nl_param = false;
  if (NULL != subplanscan_outputs &&
      exprs.count() != subplanscan_outputs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of subplanscan outputs does not match count of exprs", 
             K(exprs.count()), K(subplanscan_outputs->count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (exprs.at(i)->is_column_ref_expr() &&
        static_cast<const ObColumnRefRawExpr *>(exprs.at(i))->is_hidden_column()) {
      // do nothing.
    } else if (skip_nl_param && OB_FAIL(expr_in_nl_param(exprs.at(i), in_nl_param))) {
      LOG_WARN("failed to judge if expr in nl param", K(ret), K(i), KPC(exprs.at(i)));
    } else if (in_nl_param) {
      // do nothing
    } else {
      if (OB_FAIL(fill_expr(exprs.at(i), 
                            (is_first && skip_first_sep) ? NULL_STR_ : sep,
                            is_bool_expr, 
                            fill_column_alias,
                            NULL == subplanscan_outputs ? NULL : subplanscan_outputs->at(i)))) {
        LOG_WARN("failed to fill expr", K(dblink_id_), K(i), KPC(exprs.at(i)), K(ret));
      }
      is_first = false;
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_limit_expr(const ObRawExpr *expr,
                                            const ObString &sep)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill limit expr failed, invalid argument", 
              K(ret), KP(expr), KP(tmp_buf_), K(tmp_buf_len_), K(tmp_buf_pos_));
  } else if (OB_FAIL(fill_string(sep))) {
    LOG_WARN("failed to fill sep", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(ROWNUM_))) {
    LOG_WARN("failed to fill ROWNUM_", K(ROWNUM_), K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_expr(expr, LESS_EQUAL_))) {
    LOG_WARN("failed to fill limit expr", K(dblink_id_), KP(expr), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::fill_limit_exprs(const ObRawExprIArray &exprs,
                                             const ObString &sep)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(fill_limit_expr(exprs.at(i), sep))) {
      LOG_WARN("failed to fill expr", K(dblink_id_), K(i), KP(exprs.at(i)), K(ret));
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_all_filters(const LinkSpellNodeStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_exprs(status.filter_exprs_, 
                         SEP_AND_, 
                         true,    // status.filter_exprs_ maybe a bool expr
                         false,   // don't need skip first SEP_COMMA_
                         true))) {// need to skip nest loop join param
    LOG_WARN("failed to fill status's filter_exprs_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(status.range_conds_, 
                                SEP_AND_, 
                                true,     // status.range_conds_ maybe a bool expr
                                false,    // don't need skip first SEP_COMMA_
                                true))) { // need to skip nest loop join param
    LOG_WARN("failed to fill status's range_conds_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_limit_exprs(status.limit_count_exprs_, SEP_AND_))) {
    LOG_WARN("failed to fill status's limit_count_exprs_", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::is_filters_empty(const LinkSpellNodeStatus &status, bool &is_empty, bool skip_nl_param)
{
  int ret = OB_SUCCESS;
  is_empty = false;
  if (status.limit_count_exprs_.empty()) {
    if (status.filter_exprs_.empty() &&
        status.range_conds_.empty()) {
      is_empty = true;
    } else if (skip_nl_param) {
      bool is_all_nl_param = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_all_nl_param && i < status.filter_exprs_.count(); ++i) {
        expr_in_nl_param(status.filter_exprs_.at(i), is_all_nl_param);
      }
      for (int64_t i = 0; OB_SUCC(ret) && is_all_nl_param && i < status.range_conds_.count(); ++i) {
        expr_in_nl_param(status.range_conds_.at(i), is_all_nl_param);
      }
      is_empty = is_all_nl_param;
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_groupby(LinkSpellNodeStatus &status, const ObRawExprIArray &output_exprs, const ObSelectStmt *ref_stmt)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode() || (status.groupby_exprs_.empty() && status.rollup_exprs_.empty())) {
    // do nothing
  } else {
    // if column ref expr not in grouby, function will add it to groupby_exprs_
    if (OB_FAIL(ObGroupByChecker::dblink_check_groupby(ref_stmt,  
                                                status.groupby_exprs_, 
                                                status.rollup_exprs_,
                                                output_exprs))) {
      LOG_WARN("failed to check by expr", K(dblink_id_), K(ret));
    }
  }
  if (OB_FAIL(ret) || (status.groupby_exprs_.empty() && status.rollup_exprs_.empty())) {
    // do nothing
  } else {
    if (!status.groupby_exprs_.empty() && !status.rollup_exprs_.empty()) { // group by ... rollup(...)
      if (OB_FAIL(fill_string(GROUPBY_CLAUSE_))) {
        LOG_WARN("failed to fill GROUPBY_CLAUSE_", K(GROUPBY_CLAUSE_), K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_exprs(status.groupby_exprs_,
                                    SEP_COMMA_,   
                                    false,    // groupby_exprs_ is not bool expr
                                    true))) { // need to skip first SEP_COMMA_ when filling first groupby_exprs_
        LOG_WARN("failed to fill groupby_exprs_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(COMMA_ROLLUP_))) {
        LOG_WARN("failed to fill COMMA_ROLLUP_", K(COMMA_ROLLUP_), K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_exprs(status.rollup_exprs_, 
                                    SEP_COMMA_, 
                                    false,    // rollup_exprs_ is not bool expr
                                    true))) { // need to skip first SEP_COMMA_ when filling first rollup_exprs_
        LOG_WARN("failed to fill rollup_exprs_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(RIGHT_BRACKET_))) {
        LOG_WARN("failed to fill RIGHT_BRACKET_", K(RIGHT_BRACKET_), K(dblink_id_), K(ret));
      }
    } else if (!status.groupby_exprs_.empty()) { // group by ...
      if (OB_FAIL(fill_string(GROUPBY_CLAUSE_))) {
        LOG_WARN("failed to fill GROUPBY_CLAUSE_", K(GROUPBY_CLAUSE_), K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_exprs(status.groupby_exprs_, 
                                    SEP_COMMA_, 
                                    false,    // groupby_exprs_ is not bool expr
                                    true))) { // need to skip first SEP_COMMA_ when filling first groupby_exprs_
        LOG_WARN("failed to fill groupby_exprs_", K(dblink_id_), K(ret));
      } 
    } else if (!status.rollup_exprs_.empty()) { // group by rollup(...)
      if (OB_FAIL(fill_string(GROUPBY_ROLLUP_))) {
        LOG_WARN("failed to fill GROUPBY_ROLLUP_", K(GROUPBY_ROLLUP_), K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_exprs(status.rollup_exprs_, 
                                    SEP_COMMA_, 
                                    false,    // rollup_exprs_ is not bool expr
                                    true))) { // need to skip first SEP_COMMA_ when filling first rollup_exprs_
        LOG_WARN("failed to fill groupby_exprs_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(RIGHT_BRACKET_))) {
        LOG_WARN("failed to fill RIGHT_BRACKET_", K(RIGHT_BRACKET_), K(dblink_id_), K(ret));
      }
    } 
    // having ...
    if (OB_SUCC(ret) && !status.having_filter_exprs_.empty() && 
        OB_FAIL(fill_string(HAVING_CLAUSE_))) {
      LOG_WARN("failed to fill HAVING_CLAUSE_", K(HAVING_CLAUSE_), K(dblink_id_), K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(fill_exprs(status.having_filter_exprs_, 
                                                  SEP_AND_, 
                                                  true,     // having_filter_exprs_ maybe a bool expr
                                                  true,     // need to skip first SEP_COMMA_ when filling first having_filter_exprs_
                                                  true))) { // need to skip nest loop join param
      LOG_WARN("failed to fill filter_exprs_", K(dblink_id_), K(ret));
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_orderby_strs(const ObOrderItemIArray &order_items,
                                              const ObRawExprIArray &output_exprs)
{
  /*
   * OceanBase(ADMIN@TEST)>explain
   *     -> select a - 3, x from test.t1@my_link1 union select c * 2, y from test.t2@my_link1 order by 1;
   * | =================================================
   * |ID|OPERATOR             |NAME|EST. ROWS|COST   |
   * -------------------------------------------------
   * |0 |LINK                 |    |0        |1070149|
   * |1 | MERGE UNION DISTINCT|    |200000   |1070149|
   * |2 |  SORT               |    |100000   |496872 |
   * |3 |   TABLE SCAN        |T1  |100000   |61860  |
   * |4 |  SORT               |    |100000   |496872 |
   * |5 |   TABLE SCAN        |T2  |100000   |61860  |
   * =================================================
   *
   * Outputs & filters:
   * -------------------------------------
   *   0 - output([UNION((T1.A - 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]), filter(nil), dblink_id=1100611139403793,
   *       link_stmt=(select (T1.A - 3), T1.X from (select T1.A, T1.X from T1) T1) union (select (T2.C * 2), T2.Y from (select T2.C, T2.Y from T2) T2) order by 1 asc, 2 asc
   *   1 - output([UNION((T1.A - 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]), filter(nil)
   *   2 - output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T1.A - 3), ASC], [cast(T1.X, VARCHAR(10 BYTE)), ASC])
   *   3 - output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil),
   *       access([T1.A], [T1.X]), partitions(p0)
   *   4 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T2.C * 2), ASC], [cast(T2.Y, VARCHAR(10 BYTE)), ASC])
   *   5 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]), filter(nil),
   *       access([T2.C], [T2.Y]), partitions(p0)
   *
   * that is why we need output_exprs, the output of op 1 'MERGE UNION DISTINCT' have no
   * column alias name, the op_ordering of op 1 'LINK' is 'UNION((T1.A - 3), (T2.C * 2))'.
   * both 'order by UNION((T1.A - 3), (T2.C * 2))' and 'order by T1.A - 3' cause error:
   * invalid identifier 'A' in 'order clause'.
   * so the only way is 'order by 1', we can compare the raw expr pointer in order_items
   * and output_exprs to get the index.
   */
  int ret = OB_SUCCESS;
  ObConstRawExpr index_expr;
  ObObj index_value;
  ObRawExpr *item_expr = NULL;
  if (!order_items.empty() && OB_FAIL(fill_string(ORDERBY_CLAUSE_))) {
    LOG_WARN("failed to fill ORDERBY_CLAUSE_", K(ORDERBY_CLAUSE_), K(dblink_id_), K(ret));
  }
  bool is_first = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    int64_t item_index = get_order_item_index(order_items.at(i).expr_, output_exprs);
    if (item_index < 0) {
      item_expr = order_items.at(i).expr_;
    } else {
      index_value.set_int(item_index + 1);
      index_expr.set_value(index_value);
      item_expr = static_cast<ObRawExpr *>(&index_expr);
    }
    if (OB_FAIL(fill_expr(item_expr, is_first ? NULL_STR_ : SEP_COMMA_, false))) {
      LOG_WARN("failed to fill expr", K(ret), K(dblink_id_), K(i));
    } else if ((order_items.at(i).order_type_ == NULLS_FIRST_ASC ||
                order_items.at(i).order_type_ == NULLS_LAST_ASC) &&
               OB_FAIL(fill_string(ORDER_ASC_))) {
      LOG_WARN("failed to fill ORDER_ASC_", K(ORDER_ASC_), K(dblink_id_), K(ret));
    } else if ((order_items.at(i).order_type_ == NULLS_FIRST_DESC ||
                order_items.at(i).order_type_ == NULLS_LAST_DESC) &&
               OB_FAIL(fill_string(ORDER_DESC_))) {
      LOG_WARN("failed to fill ORDER_DESC_", K(ORDER_DESC_), K(dblink_id_), K(ret));
    }
    is_first = false;
  }
  return ret;
}

int GenLinkStmtPostContext::add_status()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(status_array_.push_back(LinkSpellNodeStatus()))) {
    LOG_WARN("failed to push back new status into status_array_", 
              K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_status_filter(const LinkSpellNodeStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(save_filter_exprs(status.filter_exprs_))) {
    LOG_WARN("failed to filter_exprs_ into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_range_conds(status.range_conds_))) {
    LOG_WARN("failed to range_conds_ into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_limit_count_expr(status.limit_count_exprs_))) {
    LOG_WARN("failed to limit_count_exprs_ into status_array_", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_filter_exprs(const ObRawExprIArray &filter_exprs)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    ObRawExprIArray &temp = status_array_.at(status_array_.count() - 1).filter_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
      if (OB_FAIL(temp.push_back(filter_exprs.at(i)))) {
        LOG_WARN("failed to push back filter_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_pushdown_filter_exprs(const ObRawExprIArray &pushdown_filter_exprs)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    ObRawExprIArray &temp = status_array_.at(status_array_.count() - 1).pushdown_filter_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filter_exprs.count(); ++i) {
      if (OB_FAIL(temp.push_back(pushdown_filter_exprs.at(i)))) {
        LOG_WARN("failed to push back pushdown_filter_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }

  return ret;
}

int GenLinkStmtPostContext::save_range_conds(const ObRawExprIArray &range_conds)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    ObRawExprIArray &temp = status_array_.at(status_array_.count() - 1).range_conds_;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_conds.count(); ++i) {
      if (OB_FAIL(temp.push_back(range_conds.at(i)))) {
        LOG_WARN("failed to push back range_conds into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_limit_count_expr(ObRawExpr *limit_count_expr)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    ObRawExprIArray &temp = status_array_.at(status_array_.count() - 1).limit_count_exprs_;
    if (NULL != limit_count_expr && OB_FAIL(temp.push_back(limit_count_expr))) {
      LOG_WARN("failed to push back limit_count_expr into status_array_", 
                K(dblink_id_), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_limit_count_expr(const ObRawExprIArray &limit_count_exprs)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    ObRawExprIArray &temp = status_array_.at(status_array_.count() - 1).limit_count_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < limit_count_exprs.count(); ++i) {
      if (OB_FAIL(temp.push_back(limit_count_exprs.at(i)))) {
        LOG_WARN("failed to push back limit_count_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_group_by_info(const ObRawExprIArray &groupby_exprs,
                                               const ObRawExprIArray &rollup_exprs,
                                               const ObRawExprIArray &having_filter_exprs)
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    LinkSpellNodeStatus &temp = status_array_.at(status_array_.count() - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); ++i) {
      if (OB_FAIL(temp.groupby_exprs_.push_back(groupby_exprs.at(i)))) {
        LOG_WARN("failed to push back groupby_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); ++i) {
      if (OB_FAIL(temp.rollup_exprs_.push_back(rollup_exprs.at(i)))) {
        LOG_WARN("failed to push back rollup_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < having_filter_exprs.count(); ++i) {
      if (OB_FAIL(temp.having_filter_exprs_.push_back(having_filter_exprs.at(i)))) {
        LOG_WARN("failed to push back having_filter_exprs into status_array_", 
                  K(dblink_id_), K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_distinct()
{
  int ret = OB_SUCCESS;
  if (!status_array_.empty()) {
    status_array_.at(status_array_.count() - 1).is_distinct_ = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::save_partial_sql()
{
  int ret = OB_SUCCESS;
  char *partial_sql_buf = NULL;
  if (status_array_.empty()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(dblink_id_), K(ret));
  } else if (OB_ISNULL(tmp_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp_buf_ is NULL", K(dblink_id_), K(ret));
  } else if (OB_ISNULL(partial_sql_buf = static_cast<char *>(alloc_.alloc(tmp_buf_pos_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;;
    LOG_WARN("failed to alloc partial_sql_buf", K(dblink_id_), K(partial_sql_buf), K(ret));
  } else {
    MEMCPY(partial_sql_buf, tmp_buf_, tmp_buf_pos_);
    status_array_.at(status_array_.count() - 1).partial_sql_ = ObString(tmp_buf_pos_, partial_sql_buf);
  }
  return ret;
}

int GenLinkStmtPostContext::expr_in_nl_param(ObRawExpr *expr, bool &in_nl_param)
{
  in_nl_param = false;
  return do_expr_in_nl_param(expr, in_nl_param);
}

int GenLinkStmtPostContext::do_expr_in_nl_param(ObRawExpr *expr, bool &in_nl_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (expr->get_expr_type() == T_QUESTIONMARK) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(expr);
    if (has_exist_in_array(nl_param_idxs_, const_expr->get_value().get_unknown())) {
      in_nl_param = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !in_nl_param && i < expr->get_param_count(); i++) {
      ObRawExpr *child_expr = expr->get_param_expr(i);
      if (OB_FAIL(do_expr_in_nl_param(child_expr, in_nl_param))) {
        LOG_WARN("failed to judge if expr in nl param", K(ret), K(i), KPC(child_expr));
      }
    }
  }
  return ret;
}

int GenLinkStmtPostContext::extend_tmp_buf(int64_t need_length)
{
  int ret = OB_SUCCESS;
  tmp_buf_len_ = (0 >= need_length) ? tmp_buf_len_ * 2 : need_length;
  char *new_tmp_buf = NULL;
  if (OB_ISNULL(tmp_buf_) || tmp_buf_len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extend tmp_buf_ failed", K(dblink_id_), KP(tmp_buf_), K(tmp_buf_len_), K(ret));
  } else if (OB_ISNULL(new_tmp_buf = static_cast<char *>(alloc_.alloc(tmp_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("falied to alloc new_tmp_buf", K(ret), K(tmp_buf_len_));
  } else {
    MEMCPY(new_tmp_buf, tmp_buf_, tmp_buf_pos_);
    tmp_buf_ = new_tmp_buf;
  }
  return ret;
}

int GenLinkStmtPostContext::init()
{
  int ret = OB_SUCCESS;
  // The memory space of 1024 bytes is used for reverse spelling sql.
  //  If the length of 1024 is not enough, it will be dynamically expanded
  tmp_buf_len_ = 1024; 
  if (OB_ISNULL(tmp_buf_ = static_cast<char *>(alloc_.alloc(tmp_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("falied to alloc tmp_buf_", K(ret), K(tmp_buf_len_));
  } else if (OB_FAIL(gen_unique_alias_.init())) {
    LOG_WARN("failed to init gen_unique_alias_", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void GenLinkStmtPostContext::reset(uint64_t dblink_id)
{
  status_array_.reuse();
  gen_unique_alias_.reset();
  dblink_id_ = dblink_id;
  tmp_buf_pos_ = 0;
}

void GenLinkStmtPostContext::check_dblink_id(uint64_t dblink_id)
{
  if (dblink_id_ != dblink_id) {
    reset(dblink_id);
  }
}

int GenLinkStmtPostContext::append_nl_param_idx(int64_t param_idx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("GenLinkStmtPostContext is not inited", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(nl_param_idxs_, param_idx))) {
    LOG_WARN("failed to add param_idx to nl_param_idxs", K(ret), K(param_idx));
  }
  return ret;
}

int GenLinkStmtPostContext::spell_table_scan(TableItem *table_item, 
                                             const ObRawExprIArray &filter_exprs, 
                                             const ObRawExprIArray &startup_exprs, 
                                             const ObRawExprIArray &range_conds, 
                                             const ObRawExprIArray &pushdown_filter_exprs,
                                             ObRawExpr *limit_count_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item is NULL", K(table_item), K(ret));
  } else {
    bool have_alias = !table_item->alias_name_.empty();
    ObString &database_name = table_item->link_database_name_; 
    ObString table_name = table_item->table_name_;
    ObString unique_alias_name = have_alias ? table_item->alias_name_ : table_name;
    if (OB_FAIL(gen_unique_alias_.get_unique_name(table_item->table_id_, unique_alias_name)))
    {
      LOG_WARN("failed to get unique name", K(ret), K(unique_alias_name));
    }
    char *partial_sql_buf = NULL;
    int64_t buf_len = database_name.length() + 
                      table_name.length() + 
                      unique_alias_name.length() + 2;  // 2 = '.' + ' '
    int64_t pos = 0;

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(partial_sql_buf = static_cast<char *>(alloc_.alloc(buf_len)))) {
      /**
        tmp_buf_ will not be used when reverse-spelling the tablescan operator, 
        because the length of the partial_sql of the tablescan operator 
        can be known before the reverse-spelling, and a corresponding 
        length of memory can be directly allocated to store the partial_sql.
      */
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc partial_sql_buf", K(partial_sql_buf));
    } else {
      if (!database_name.empty()) {
        MEMCPY(partial_sql_buf + pos, database_name.ptr(), database_name.length());
        pos += database_name.length();
        partial_sql_buf[pos++] = '.';
      }
      MEMCPY(partial_sql_buf + pos, table_name.ptr(), table_name.length());
      pos += table_name.length();
      if (0 != table_name.compare(unique_alias_name)) {
        partial_sql_buf[pos++] = ' ';
        MEMCPY(partial_sql_buf + pos, unique_alias_name.ptr(), unique_alias_name.length());
        pos += unique_alias_name.length();
      }
      if (OB_FAIL(add_status())) {
        LOG_WARN("failed to add new status into status_array_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(save_pushdown_filter_exprs(pushdown_filter_exprs))) {
        LOG_WARN("failed to save pushdown_filter_exprs into status_array_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(save_filter_exprs(filter_exprs))) {
        LOG_WARN("failed to save filter_exprs into status_array_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(save_filter_exprs(startup_exprs))) {
        LOG_WARN("failed to save startup_exprs into status_array_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(save_range_conds(range_conds))) {
        LOG_WARN("failed to save range_conds into status_array_", K(dblink_id_), K(ret));
      } else if (NULL != limit_count_expr && OB_FAIL(save_limit_count_expr(limit_count_expr))) {
        LOG_WARN("failed to save limit_count_expr into status_array_", K(dblink_id_), K(ret));
      } else {
        status_array_.at(status_array_.count() - 1).partial_sql_ = ObString(pos, partial_sql_buf);
      }
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_join_on(const LinkSpellNodeStatus &left_child_status,
                                        const LinkSpellNodeStatus &right_child_status,
                                        const ObRawExprIArray &join_conditions, 
                                        const ObRawExprIArray &join_filters,
                                        ObJoinType join_type,
                                        bool right_child_is_join)
{
  /**
  If logical plan like that,
  Query Plan: =====================================
  |ID|OPERATOR    |NAME|EST. ROWS|COST|
  -------------------------------------
  |0 |HASH JOIN   |    |3        |143 |
  |1 | TABLE SCAN |T3  |3        |46  |
  |2 | HASH JOIN  |    |3        |95  |
  |3 |  TABLE SCAN|T1  |3        |46  |
  |4 |  TABLE SCAN|T2  |3        |46  |
  =====================================
  reverse spelled sql like "select * from t1 join t2 on t1.c1 = t2.c2 join t3 on t2.c3= t3.c3;"  is correct.
  reverse spelled sql like "select * from t3 join t1 join t2 on t1.c1 = t2.c2 on t2.c3= t3.c3;"  is wrong.

  So we need parameter right_child_is_join to handle this case
  */
  int ret = OB_SUCCESS;
  const ObString &join_str = join_type_str(join_type);
  const ObString &reverse_join_str = join_type_str(reverse_join_type(join_type));
  if (OB_FAIL(fill_string(
              right_child_is_join ? right_child_status.partial_sql_ : left_child_status.partial_sql_))) {
    LOG_WARN("failed to fill partial_sql_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(
             right_child_is_join ? reverse_join_str : join_str))) {

  } else if (OB_FAIL(fill_string(
             right_child_is_join ? left_child_status.partial_sql_ : right_child_status.partial_sql_))) {
    LOG_WARN("failed to fill partial_sql_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(JOIN_ON_))) {
    LOG_WARN("failed to fill JOIN_ON_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(join_conditions, SEP_AND_))) {
    LOG_WARN("failed to fill join_conditions", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(join_filters, SEP_AND_))) {
    LOG_WARN("failed to fill join_filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(right_child_status.pushdown_filter_exprs_, SEP_AND_))) {
    LOG_WARN("failed to fill join_filters", K(dblink_id_), K(ret));
  } else {
    switch (join_type) {
      case INNER_JOIN: {
        if (OB_FAIL(fill_all_filters(left_child_status))) {
          LOG_WARN("failed to fill all filters", K(dblink_id_), K(ret));
        } else if (OB_FAIL(fill_all_filters(right_child_status))) {
          LOG_WARN("failed to fill all filters", K(dblink_id_), K(ret));
        }
        break;
      }
      case LEFT_OUTER_JOIN: {
        if (OB_FAIL(fill_all_filters(right_child_status))) {
          LOG_WARN("failed to fill all filters", K(dblink_id_), K(ret));
        } else if (OB_FAIL(save_status_filter(left_child_status))) {
          LOG_WARN("failed to save left child status's filter", K(dblink_id_), K(ret));
        } 
        break;
      }
      case RIGHT_OUTER_JOIN: {
        if (OB_FAIL(fill_all_filters(left_child_status))) {
          LOG_WARN("failed to fill all filters", K(dblink_id_), K(ret));
        } else if (OB_FAIL(save_status_filter(right_child_status))) {
          LOG_WARN("failed to save left child status's filter", K(dblink_id_), K(ret));
        }
        break;
      }
      case FULL_OUTER_JOIN: {
        bool is_left_empty = false;
        bool is_right_empty = false;
        if (OB_FAIL(is_filters_empty(left_child_status, is_left_empty, true))) {
          LOG_WARN("failed to judge left child status's filter is empty", K(dblink_id_), K(ret));
        } else if (OB_FAIL(is_filters_empty(right_child_status, is_right_empty, true))) {
          LOG_WARN("failed to judge left righ status's filter is empty", K(dblink_id_), K(ret));
        } else if (!is_left_empty || !is_right_empty) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child of full join have filter", K(is_left_empty), K(is_right_empty), K(dblink_id_), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join type is invalid", K(ret), K(join_type));
        break;
      }
    }
  }
  return ret;
}

int GenLinkStmtPostContext::fill_semi_exists(LinkSpellNodeStatus &left_child_status,
                                            LinkSpellNodeStatus &right_child_status,
                                            const ObRawExprIArray &join_conditions, 
                                            const ObRawExprIArray &join_filters,
                                            ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  LinkSpellNodeStatus left_status = left_child_status;
  LinkSpellNodeStatus right_status = right_child_status;
  if (RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type) {
    left_status = right_child_status;
    right_status = left_child_status;
  }
  bool is_right_empty = false;
  if (false && OB_FAIL(is_filters_empty(right_status, is_right_empty, true))) {
    LOG_WARN("failed to judge right child status's filter is empty", K(dblink_id_), K(ret));
  } else if (false && !is_right_empty) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of semi join or anti join have filter", K(join_conditions), K(join_filters), K(left_status), K(right_status), K(join_type), K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(LEFT_BRACKET_))) {
    LOG_WARN("dblink failed to fill LEFT_BRACKET_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(SELECT_CLAUSE_))) {
    LOG_WARN("failed to fill SELECT_CLAUSE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(ASTERISK_))) {
    LOG_WARN("failed to fill ASTERISK_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(FROM_CLAUSE_))) {
    LOG_WARN("failed to fill FROM_CLAUSE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(left_status.partial_sql_))) {
    LOG_WARN("failed to fill left child sql", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string((LEFT_SEMI_JOIN == join_type || 
                                  RIGHT_SEMI_JOIN == join_type) ? 
                                  WHERE_EXISTS_ : WHERE_NOT_EXISTS_))) {
    LOG_WARN("failed to fill WHERE_EXISTS_ or WHERE_NOT_EXISTS_", K(join_type), K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(right_status.partial_sql_))) {
    LOG_WARN("failed to fill right child sql", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(WHERE_CLAUSE_))) {
    LOG_WARN("failed to fill WHERE_CLAUSE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(join_conditions, SEP_AND_))) {
    LOG_WARN("failed to fill join_conditions", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(join_filters, SEP_AND_))) {
    LOG_WARN("failed to fill join_filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(right_child_status.pushdown_filter_exprs_, SEP_AND_))) { // nest loop join does not have right style join
    LOG_WARN("failed to fill join_filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(RIGHT_BRACKET_))) {
    LOG_WARN("failed to fill RIGHT_BRACKET_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(RIGHT_BRACKET_))) {
    LOG_WARN("failed to fill RIGHT_BRACKET_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(SEP_SPACE_))) {
    LOG_WARN("failed to fill SEP_SPACE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(semi_join_name(left_status.partial_sql_)))) {
    LOG_WARN("failed to fill SEP_SPACE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_status_filter(left_status))) {
    LOG_WARN("failed to child's filters", K(dblink_id_), K(ret));
  }
  return ret;
}

ObString GenLinkStmtPostContext::semi_join_name(ObString left_child_sql)
{
  ObString name;
  if (!left_child_sql.empty()) {
    char *ptr = left_child_sql.ptr() + left_child_sql.length() - 1;
    int64_t length = 0;
    for (int64_t i = 0; (' ' != *ptr && '.' != *ptr) && i < left_child_sql.length(); ++i) {
      ++length;
      --ptr;
    }
    if (0 != length && (' ' == *ptr || '.' == *ptr)) {
      ++ptr;
    }
    name.assign(ptr, length);
  }
  LOG_WARN("semi join name", K(left_child_sql), K(name));
  return name;
}

int GenLinkStmtPostContext::spell_join(ObJoinType join_type, 
                                      bool right_child_is_join,
                                      const ObRawExprIArray &filter_exprs,
                                      const ObRawExprIArray &startup_exprs,
                                      const ObRawExprIArray &join_conditions, 
                                      const ObRawExprIArray &join_filters)
{
  int ret = OB_SUCCESS;
  tmp_buf_pos_ = 0;
  LinkSpellNodeStatus right_child_status;
  LinkSpellNodeStatus left_child_status;
  if (status_array_.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of status_array_ is wrong", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (OB_FAIL(status_array_.pop_back(right_child_status))) {
    LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
  } else if (OB_FAIL(status_array_.pop_back(left_child_status))) {
    LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
  } else if (OB_FAIL(add_status())) {
    LOG_WARN("failed to add new status into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_filter_exprs(filter_exprs))) {
    LOG_WARN("failed to save filter_exprs into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_filter_exprs(startup_exprs))) {
    LOG_WARN("failed to save startup_exprs into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_pushdown_filter_exprs(left_child_status.pushdown_filter_exprs_))) { // deal with plan with multi nest loop join 
    LOG_WARN("failed to save pushdown_filter_exprs into status_array_", K(dblink_id_), K(ret));
  } else {
    switch (join_type) {
      case INNER_JOIN:          /*no break*/
      case LEFT_OUTER_JOIN:     /*no break*/
      case RIGHT_OUTER_JOIN:    /*no break*/
      case FULL_OUTER_JOIN: {
        if (OB_FAIL(fill_join_on(left_child_status,
                                right_child_status,
                                join_conditions, 
                                join_filters,
                                join_type,
                                right_child_is_join))) {
          LOG_WARN("fill join on sql failed", K(join_type), K(dblink_id_), K(ret));
        }
        break;
      }
      case LEFT_SEMI_JOIN:    /*no break*/
      case RIGHT_SEMI_JOIN:   /*no break*/
      case LEFT_ANTI_JOIN:    /*no break*/
      case RIGHT_ANTI_JOIN: {
        if (OB_FAIL(fill_semi_exists(left_child_status,
                                      right_child_status,
                                      join_conditions, 
                                      join_filters,
                                      join_type))) {
          LOG_WARN("fill semi exists sql failed", K(join_type), K(dblink_id_), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join type is invalid", K(join_type), K(ret));
        break;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(save_partial_sql())) {
      LOG_WARN("failed to save partial sql", K(join_type), K(ret));
    }
  }
  return ret;
}

/**
 Part of the operators will not be generated by partial_sql 
 during the reverse spelling process, but the corresponding information 
 of those operation is attached to the status of other operators.
 Those operators includes group_by, count and distinct.

 Note:the reverse spelling of the order operator is performed on the link operator
 */
int GenLinkStmtPostContext::spell_group_by(const ObRawExprIArray &startup_exprs,
                                           const ObRawExprIArray &groupby_exprs,
                                           const ObRawExprIArray &rollup_exprs,
                                           const ObRawExprIArray &filter_exprs)
{
  int ret = OB_SUCCESS;
  if (status_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (!startup_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("groupby op have startup_exprs", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_group_by_info(groupby_exprs,
                                rollup_exprs,
                                filter_exprs))) {
    LOG_WARN("failed to save groupby info into context", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::spell_count(const ObRawExprIArray &startup_exprs,
                                        const ObRawExprIArray &filter_exprs,
                                        ObRawExpr *limit_count_expr)
{
  int ret = OB_SUCCESS;
  if (status_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_filter_exprs(filter_exprs))) {
    LOG_WARN("failed to save filter_exprs into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_filter_exprs(startup_exprs))) {
    LOG_WARN("failed to save startup_exprs into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_limit_count_expr(limit_count_expr))) {
    LOG_WARN("failed to save limit_count_expr into status_array_", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::spell_distinct(const ObRawExprIArray &startup_exprs,
                                           const ObRawExprIArray &filter_exprs)
{
  int ret = OB_SUCCESS;
  if (status_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (!startup_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count op have startup_exprs", K(dblink_id_), K(ret));
  } else if (!filter_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count op have filter_exprs", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_distinct())) {
    LOG_WARN("failed to save distinct into status_array_", K(dblink_id_), K(ret));
  }
  return ret;
}

int GenLinkStmtPostContext::spell_set(const ObSelectStmt *ref_stmt,
                                      ObLogSet *set_op_ptr,
                                      const ObRawExprIArray &startup_exprs,
                                      const ObRawExprIArray &filter_exprs,
                                      ObSelectStmt::SetOperator set_op,
                                      bool is_distinct,
                                      bool is_parent_distinct)
{
  int ret = OB_SUCCESS;
  int64_t num_of_child = set_op_ptr->get_num_of_child();
  tmp_buf_pos_ = 0;
  char *partial_sql_buf = NULL;
  if (status_array_.count() < num_of_child) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of status_array_ is wrong", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (!startup_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set op have startup_exprs", K(dblink_id_), K(ret));
  } else if (!filter_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set op have filter_exprs", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(LEFT_BRACKET_))) {
    LOG_WARN("dblink failed to fill LEFT_BRACKET_", K(dblink_id_), K(ret));
  } else {
    const ObString &set_string = set_type_str(set_op);
    StatusArray child_status_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < num_of_child; ++i) {
      LinkSpellNodeStatus child_status;
      if (OB_FAIL(status_array_.pop_back(child_status))) {
        LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
      } else if (OB_FAIL(child_status_array.push_back(child_status))) {
        LOG_WARN("failed to push back child_status into child_status_array", 
                 K(dblink_id_), K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(add_status())) {
      LOG_WARN("failed to add new status into status_array_", K(dblink_id_), K(ret));
    }
    status_array_.at(status_array_.count() - 1).is_set_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < num_of_child; ++i) {
      LinkSpellNodeStatus child_status;
      const ObRawExprIArray &output_expr = set_op_ptr->get_child(i)->get_output_exprs();
      if (OB_FAIL(child_status_array.pop_back(child_status))) {
        LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
      } else if (child_status.is_set_) {
        if (OB_FAIL(fill_string(child_status.partial_sql_))) {
          LOG_WARN("dblink failed to partial_sql_", K(dblink_id_), K(ret));
        }
      } else if (OB_FAIL(fill_string(child_status.is_distinct_ ? SELECT_DIS_CLAUSE_ : SELECT_CLAUSE_))) {
        LOG_WARN("dblink failed to fill select clause", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_exprs(output_expr,
                                    SEP_COMMA_,
                                    false,      // output_exprs is not a bool expr
                                    true        // need to skip first SEP_COMMA_ when filling first output_expr
                                    ))) {
        LOG_WARN("dblink failed to fill output expr", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(FROM_CLAUSE_))) {
        LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_string(child_status.partial_sql_))) {
        LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
      } else if (!child_status.is_filters_empty() && OB_FAIL(fill_string(WHERE_CLAUSE_))) {
        LOG_WARN("dblink failed to fill where clause", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_all_filters(child_status))) {
        LOG_WARN("dblink failed to fill filters", K(dblink_id_), K(ret));
      } else if (OB_FAIL(fill_groupby(child_status, output_expr, ref_stmt))) {
        LOG_WARN("dblink failed to group by", K(dblink_id_), K(ret));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (num_of_child - 1 != i && OB_FAIL(fill_string(set_string))) {
        LOG_WARN("dblink failed to fill set_string", K(dblink_id_), K(ret));
      } else if (num_of_child - 1 != i && ObSelectStmt::UNION == set_op && 
        !(is_distinct || is_parent_distinct) && OB_FAIL(fill_string(UNION_ALL_))) {
        LOG_WARN("dblink failed to fill UNION_ALL_", K(dblink_id_), K(ret));
      } else if (OB_FAIL(save_pushdown_filter_exprs(child_status.pushdown_filter_exprs_))) {
        LOG_WARN("failed to save pushdown_filter_exprs into status_array_", K(dblink_id_), K(ret));
      } 
    }
    if (OB_SUCC(ret) && OB_FAIL(fill_string(RIGHT_BRACKET_))) {
      LOG_WARN("dblink failed to fill RIGHT_BRACKET_", K(dblink_id_), K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(save_partial_sql())) {
      LOG_WARN("failed to save partial sql into status_array_", K(dblink_id_), K(ret));
    }
  }
  return ret;
}

int GenLinkStmtPostContext::spell_subplan_scan(const ObSelectStmt *ref_stmt,
                                               const ObRawExprIArray &output_exprs,
                                               const ObRawExprIArray &child_output_exprs,
                                               const ObRawExprIArray &startup_exprs,
                                               const ObRawExprIArray &filter_exprs,
                                               ObString &subquery_name)
{
  int ret = OB_SUCCESS;
  LinkSpellNodeStatus child_status;
  tmp_buf_pos_ = 0;
  if (status_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (!startup_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subplanscan op have startup_exprs", K(dblink_id_), K(ret));
  } else if (!filter_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subplanscan op have filter_exprs", K(dblink_id_), K(ret));
  } else if (OB_FAIL(status_array_.pop_back(child_status))) {
    LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(LEFT_BRACKET_))) {
    LOG_WARN("dblink failed to fill LEFT_BRACKET_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(child_status.is_distinct_ ? 
                                 SELECT_DIS_CLAUSE_ : SELECT_CLAUSE_))) {
    LOG_WARN("dblink failed to fill select clause", K(dblink_id_), K(ret));
  } else if (child_output_exprs.empty() && OB_FAIL(fill_string(ASTERISK_))) {
    LOG_WARN("dblink failed to fill ASTERISK_", K(dblink_id_), K(ret));
  } else if (!child_output_exprs.empty() && 
             OB_FAIL(fill_exprs(child_output_exprs, 
                                SEP_COMMA_,
                                false,      // output_exprs is not a bool expr
                                true,       // need to skip first SEP_COMMA_ when filling first output_expr
                                false,      // don't need deal nest loop join param
                                true,       // need to fill column alias name too when filling "tablename.columnname"
                                &output_exprs))) {
    LOG_WARN("dblink failed to fill output expr", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(FROM_CLAUSE_))) {
    LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(child_status.partial_sql_))) {
    LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
  } else if (!child_status.is_filters_empty() && OB_FAIL(fill_string(WHERE_CLAUSE_))) {
    LOG_WARN("dblink failed to fill where clause", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_all_filters(child_status))) {
    LOG_WARN("dblink failed to fill filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_groupby(child_status, child_output_exprs, ref_stmt))) {
    LOG_WARN("dblink failed to group by", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(RIGHT_BRACKET_))) {
    LOG_WARN("dblink failed to fill RIGHT_BRACKET_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(SEP_SPACE_))) {
    LOG_WARN("dblink failed to fill SEP_SPACE_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(subquery_name))) {
    LOG_WARN("dblink failed to fill subquery_name", K(dblink_id_), K(ret));
  } else if (OB_FAIL(add_status())) {
    LOG_WARN("failed to add new status into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_pushdown_filter_exprs(child_status.pushdown_filter_exprs_))) {
    LOG_WARN("failed to save pushdown_filter_exprs into status_array_", K(dblink_id_), K(ret));
  } else if (OB_FAIL(save_partial_sql())) {
    LOG_WARN("failed to save partial sql into status_array_", K(dblink_id_), K(ret));
  }
  gen_unique_alias_.reset();
  return ret;
}

int GenLinkStmtPostContext::spell_link(const ObSelectStmt *ref_stmt,
                                       char **stmt_fmt_buf,
                                       int32_t &stmt_fmt_len,
                                       const ObOrderItemIArray &op_ordering,
                                       const ObRawExprIArray &output_exprs,
                                       const ObRawExprIArray &startup_exprs,
                                       const ObRawExprIArray &filter_exprs)
{
  int ret = OB_SUCCESS;
  LinkSpellNodeStatus child_status;
  tmp_buf_pos_ = 0;
  char *sql_buf = NULL;
  if (status_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status_array_ is empty", K(status_array_.count()), K(dblink_id_), K(ret));
  } else if (!filter_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set op have filter_exprs", K(dblink_id_), K(ret));
  } else if (OB_FAIL(status_array_.pop_back(child_status))) {
    LOG_WARN("status_array_ failed to pop_back", K(dblink_id_), K(ret));
  } else if (child_status.is_set_) {
    /**
    CREATE OR REPLACE VIEW v1 as select c1, c2 from t1@lcq_link_1 intersect select c3, c2 from t1@lcq_link_1;
    explain select * from v1 order by 1\G
    Query Plan: ====================================================
    |ID|OPERATOR                 |NAME|EST. ROWS|COST  |
    ----------------------------------------------------
    |0 |LINK                     |    |100000   |512225|
    |1 | SORT                    |    |100000   |512225|
    |2 |  HASH INTERSECT DISTINCT|    |100000   |220557|
    |3 |   TABLE SCAN            |T1  |100000   |38681 |
    |4 |   TABLE SCAN            |T1  |100000   |38681 |
    ====================================================
    */
    if (OB_FAIL(fill_string(child_status.partial_sql_))) {
      LOG_WARN("dblink failed to partial_sql_", K(dblink_id_), K(ret));
    } else if (OB_FAIL(fill_orderby_strs(op_ordering, output_exprs))) {
    LOG_WARN("dblink failed to fill order by", K(dblink_id_), K(ret));
  }
  } else if (OB_FAIL(fill_string(child_status.is_distinct_ ? SELECT_DIS_CLAUSE_ : SELECT_CLAUSE_))) {
    LOG_WARN("dblink failed to fill select clause", K(dblink_id_), K(ret));
  } else if (output_exprs.empty() && OB_FAIL(fill_string(ASTERISK_))) {
    LOG_WARN("dblink failed to fill ASTERISK_", K(dblink_id_), K(ret));
  } else if (!output_exprs.empty() && 
             OB_FAIL(fill_exprs(output_exprs, 
                                SEP_COMMA_,
                                false,      // output_exprs is not a bool expr
                                true,       // need to skip first SEP_COMMA_ when filling first output_expr
                                false,      // don't need deal nest loop join param
                                true))) {   // need to fill column alias name too when filling "tablename.columnname"
    LOG_WARN("dblink failed to fill output expr", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(FROM_CLAUSE_))) {
    LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_string(child_status.partial_sql_))) {
    LOG_WARN("dblink failed to fill from clause", K(dblink_id_), K(ret));
  } else if (!child_status.is_filters_empty() && OB_FAIL(fill_string(WHERE_CLAUSE_))) {
    LOG_WARN("dblink failed to fill where clause", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_all_filters(child_status))) {
    LOG_WARN("dblink failed to fill filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_exprs(startup_exprs, SEP_AND_, true))) { // param "true" means startup_exprs maybe a bool expr
    LOG_WARN("dblink failed to fill filters", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_groupby(child_status, output_exprs, ref_stmt))) {
    LOG_WARN("dblink failed to fill group by", K(dblink_id_), K(ret));
  } else if (OB_FAIL(fill_orderby_strs(op_ordering, output_exprs))) {
    LOG_WARN("dblink failed to fill order by", K(dblink_id_), K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(sql_buf = static_cast<char *>(alloc_.alloc(tmp_buf_pos_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;;
    LOG_WARN("failed to alloc sql_buf", K(sql_buf));
  } else {
    MEMCPY(sql_buf, tmp_buf_, tmp_buf_pos_);
    *stmt_fmt_buf = sql_buf;
    stmt_fmt_len = static_cast<int32_t>(tmp_buf_pos_);
  }
  return ret;
}
