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
#include "sql/optimizer/ob_log_link.h"
#include "sql/optimizer/ob_log_plan.h"
//#include "sql/ob_sql_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase {
namespace sql {

ObLogLink::ObLogLink(ObLogPlan& plan)
    : ObLogicalOperator(plan),
      allocator_(plan.get_allocator()),
      link_stmt_(plan.get_allocator(), output_exprs_),
      stmt_fmt_buf_(NULL),
      stmt_fmt_buf_len_(0),
      stmt_fmt_len_(0),
      param_infos_()
{}

int ObLogLink::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogLink* link = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogTableScan", K(ret));
  } else if (OB_ISNULL(link = static_cast<ObLogLink*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOpertor* to ObLogLink*", K(ret));
  } else if (OB_FAIL(link->assign_stmt_fmt(stmt_fmt_buf_, stmt_fmt_len_))) {
    LOG_WARN("failed to assign stmt fmt", K(ret));
  } else if (OB_FAIL(link->assign_param_infos(param_infos_))) {
    LOG_WARN("failed to assign param infos", K(ret));
  } else {
    out = link;
  }
  return ret;
}

int ObLogLink::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  UNUSED(type);
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (type != EXPLAIN_BASIC && OB_FAIL(BUF_PRINTF(", dblink_id=%lu,", get_dblink_id()))) {
    LOG_WARN("BUF_PRINTF failed", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      link_stmt="))) {
    LOG_WARN("BUF_PRINTF failed", K(ret));
  } else if ((len = link_stmt_.to_string(buf + pos, buf_len - pos)) <= 0) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("link stmt to string failed", K(ret), K(link_stmt_));
  } else {
    pos += len;
  }
  return ret;
}

int ObLogLink::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLogLink::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  /**
   * TODO
   * add database_name in select_strs, or in from_strs ?
   * can not use "use db" because two tables in different database can join.
   */

  /*
   * OceanBase(ADMIN@TEST)>explain
   *     -> select (aa + 1) * (aa - 1), xx from (select a aa, x xx from test.t1@my_link1 lt1 union select c, y from
   * test.t2@my_link1 lt2) tt; | ================================================ |ID|OPERATOR             |NAME|EST.
   * ROWS|COST  |
   * ------------------------------------------------
   * |0 |LINK                 |    |0        |371744|
   * |1 | SUBPLAN SCAN        |TT  |200000   |371744|
   * |2 |  HASH UNION DISTINCT|    |200000   |344139|
   * |3 |   TABLE SCAN        |LT1 |100000   |61860 |
   * |4 |   TABLE SCAN        |LT2 |100000   |61860 |
   * ================================================
   *
   * Outputs & filters:
   * -------------------------------------
   *   0 - output([((TT.AA + 1) * (TT.AA - 1))], [TT.XX]), filter(nil), dblink_id=1100611139403793,
   *       link_stmt=select ((TT.AA + 1) * (TT.AA - 1)), TT.XX from ((select LT1.A AA, LT1.X XX from T1 LT1) union
   * (select LT2.C, LT2.Y from T2 LT2)) TT 1 - output([TT.XX], [((TT.AA + 1) * (TT.AA - 1))]), filter(nil),
   *       access([TT.AA], [TT.XX])
   *   2 - output([UNION(LT1.A, LT2.C)], [UNION(cast(LT1.X, VARCHAR(10 BYTE)), cast(LT2.Y, VARCHAR(10 BYTE)))]),
   * filter(nil) 3 - output([LT1.A], [cast(LT1.X, VARCHAR(10 BYTE))]), filter(nil), access([LT1.A], [LT1.X]),
   * partitions(p0) 4 - output([LT2.C], [cast(LT2.Y, VARCHAR(10 BYTE))]), filter(nil), access([LT2.C], [LT2.Y]),
   * partitions(p0)
   *
   * that is why we must fill select clause in link, even if every log op will fill it if
   * empty: the output of op 1 'SUBPLAN SCAN' is '[TT.XX], [((TT.AA + 1) * (TT.AA - 1))]',
   * while we need 'select (TT.AA + 1) * (TT.AA - 1), TT.XX', the sequence of these two
   * exprs is opposite.
   * the sequence of output exprs in op 0 'LINK' is correct.
   *
   * here is another example, the sequence of op 0 'LINK' and op 1 'SORT' is correct, but
   * op 2 'TABLE SCAN' is wrong.
   *
   * OceanBase(ADMIN@TEST)>explain basic
   *     -> select mod(a, 3), x from test.t1@my_link1 order by 1, 2;
   *
   * | ======================
   * |ID|OPERATOR    |NAME|
   * ----------------------
   * |0 |LINK        |    |
   * |1 | SORT       |    |
   * |2 |  TABLE SCAN|T1  |
   * ======================
   *
   * Outputs & filters:
   * -------------------------------------
   *   0 - output([(T1.A % 3)], [T1.X]), filter(nil), dblink_id=1100611139403793,
   *       link_stmt=select T1.X, MOD(T1.A, 3) from T1 order by 1 asc, 2 asc
   *   1 - output([(T1.A % 3)], [T1.X]), filter(nil), sort_keys([(T1.A % 3), ASC], [T1.X, ASC])
   *   2 - output([T1.X], [(T1.A % 3)]), filter(nil),
   *       access([T1.A], [T1.X]), partitions(p0)
   */

  /*
   * OceanBase(ADMIN@TEST)>explain
   *     -> select a - 3, concat(b, x) from t1 union select c, d from t2;
   *
   * | ============================================
   * |ID|OPERATOR           |NAME|EST. ROWS|COST|
   * --------------------------------------------
   * |0 |HASH UNION DISTINCT|    |10       |85  |
   * |1 | TABLE SCAN        |T1  |5        |37  |
   * |2 | TABLE SCAN        |T2  |5        |37  |
   * ============================================
   *
   * Outputs & filters:
   * -------------------------------------
   *   0 - output([UNION((T1.A - 3), cast(T2.C, DECIMAL(-1, -85)))], [UNION(CONCAT(T1.B, T1.X), cast(T2.D, VARCHAR(20
   * BYTE)))]), filter(nil) 1 - output([(T1.A - 3)], [CONCAT(T1.B, T1.X)]), filter(nil), access([T1.A], [T1.B], [T1.X]),
   * partitions(p0) 2 - output([cast(T2.C, DECIMAL(-1, -85))], [cast(T2.D, VARCHAR(20 BYTE))]), filter(nil),
   *   access([T2.C], [T2.D]), partitions(p0)
   *
   * OceanBase(ADMIN@TEST)>select T1.A - 3 from (select a - 3, concat(b, x) from t1 union select c, d from t2);
   * ERROR-00904: invalid identifier 'T1.A' in 'field list'
   *
   * that is why we must NOT fill select clause if the child is set, even if we can extract
   * 'T1.A - 3' from 'UNION((T1.A - 3), cast(T2.C, DECIMAL(-1, -85)))'.
   * so just keep select clause empty.
   * ps: set op include union / intersect / except / minus.
   */

  /*     merge-join
   *     /        \
   * table-scan   link
   *               |
   *           merge-join
   *           /        \
   *         sort      sort
   *
   * that is why we must fill order by clause using the op_ordering of link rather
   * than sort, the top merge-join need the 'op_ordering' of link.
   */

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
   *   0 - output([UNION((T1.A - 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]),
   * filter(nil), dblink_id=1100611139403793, link_stmt=(select (T1.A - 3), T1.X from (select T1.A, T1.X from T1) T1)
   * union (select (T2.C * 2), T2.Y from (select T2.C, T2.Y from T2) T2) order by 1 asc, 2 asc 1 - output([UNION((T1.A -
   * 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]), filter(nil) 2 -
   * output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T1.A - 3), ASC], [cast(T1.X,
   * VARCHAR(10 BYTE)), ASC]) 3 - output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil), access([T1.A],
   * [T1.X]), partitions(p0) 4 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T2.C *
   * 2), ASC], [cast(T2.Y, VARCHAR(10 BYTE)), ASC]) 5 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]),
   * filter(nil), access([T2.C], [T2.Y]), partitions(p0)
   *
   * another reason that we must fill order by clause using the op_ordering of link
   * rather than sort, the orig sort above union is pushed down to table scan.
   */

  int ret = OB_SUCCESS;
  if (OB_FAIL(link_stmt_.init(&link_ctx))) {
    LOG_WARN("failed to init link stmt", K(ret), K(dblink_id_));
  } else if (OB_FAIL(link_stmt_.fill_orderby_strs(get_op_ordering(), output_exprs_))) {
    LOG_WARN("failed to fill link stmt orderby strs", K(ret), K(get_op_ordering()));
  } else {
    link_ctx.dblink_id_ = dblink_id_;
    link_ctx.link_stmt_ = &link_stmt_;
  }
  return ret;
}

int ObLogLink::gen_link_stmt_fmt()
{
  int ret = OB_SUCCESS;
  if (!link_stmt_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("link stmt is not inited", K(ret));
  } else if (OB_FAIL(gen_link_stmt_fmt_buf())) {
    LOG_WARN("failed to gen link stmt fmt buf", K(ret), K(link_stmt_));
  } else if (OB_FAIL(gen_link_stmt_param_infos())) {
    LOG_WARN("failed to gen link stmt param infos", K(ret), K(link_stmt_));
  }
  return ret;
}

int ObLogLink::gen_link_stmt_fmt_buf()
{
  int ret = OB_SUCCESS;
  stmt_fmt_buf_len_ = link_stmt_.get_total_size();
  if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char*>(allocator_.alloc(stmt_fmt_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt buf", K(ret), K(stmt_fmt_buf_len_));
  } else if (OB_FAIL(link_stmt_.gen_stmt_fmt(stmt_fmt_buf_, stmt_fmt_buf_len_, stmt_fmt_len_))) {
    LOG_WARN("failed to gen link stmt fmt", K(ret));
  }
  return ret;
}

// see comment in ObConstRawExpr::get_name_internal().
int ObLogLink::gen_link_stmt_param_infos()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf_) || stmt_fmt_len_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt fmt is not inited", KP(stmt_fmt_buf_), K(stmt_fmt_len_));
  } else {
    param_infos_.reset();
  }
  int64_t param_pos = 0;
  int64_t param_idx = 0;
  const int64_t param_len = ObLinkStmtParam::get_param_len();
  while (OB_SUCC(ret) && param_idx >= 0) {
    if (OB_FAIL(ObLinkStmtParam::read_next(stmt_fmt_buf_, stmt_fmt_len_, param_pos, param_idx))) {
      LOG_WARN("failed to read next param", K(ret));
    } else if (param_idx < 0) {
      // skip.
    } else if (OB_FAIL(param_infos_.push_back(
                   ObParamPosIdx(static_cast<int32_t>(param_pos), static_cast<int32_t>(param_idx))))) {
      LOG_WARN("failed to push back param pos idx", K(ret), K(param_pos), K(param_idx));
    } else {
      param_pos += param_len;
    }
  }
  return ret;
}

int ObLogLink::assign_param_infos(const ObIArray<ObParamPosIdx>& param_infos)
{
  int ret = OB_SUCCESS;
  param_infos_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_infos.count(); i++) {
    if (OB_FAIL(param_infos_.push_back(param_infos.at(i)))) {
      LOG_WARN("failed to push back param info", K(ret), K(param_infos.at(i)));
    }
  }
  return ret;
}

int ObLogLink::assign_stmt_fmt(const char* stmt_fmt_buf, int32_t stmt_fmt_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf) || stmt_fmt_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(stmt_fmt_buf), K(stmt_fmt_len));
  } else if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char*>(allocator_.alloc(stmt_fmt_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt buf", K(ret), K(stmt_fmt_len));
  } else {
    MEMCPY(stmt_fmt_buf_, stmt_fmt_buf, stmt_fmt_len);
    stmt_fmt_buf_len_ = stmt_fmt_len;
    stmt_fmt_len_ = stmt_fmt_len;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
