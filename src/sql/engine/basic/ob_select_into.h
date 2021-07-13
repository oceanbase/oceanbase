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

#ifndef SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_H_
#define SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_H_

#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace sql {
class ObSelectInto : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  class ObSelectIntoCtx;

public:
  explicit ObSelectInto(common::ObIAllocator& alloc);
  virtual ~ObSelectInto();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;

  void set_into_type(ObItemType into_type)
  {
    into_type_ = into_type;
  }

  int set_user_vars(const common::ObIArray<common::ObString>& user_vars, common::ObIAllocator& phy_alloc);

  int set_outfile_name(const common::ObObj& outfile_name, common::ObIAllocator& phy_alloc);

  int set_filed_str(const common::ObObj& filed_str, common::ObIAllocator& phy_alloc);

  int set_line_str(const common::ObObj& line_str, common::ObIAllocator& phy_alloc);

  void set_is_optional(bool is_optional)
  {
    is_optional_ = is_optional;
  }
  void set_closed_cht(char closed_cht)
  {
    closed_cht_ = closed_cht;
  }

private:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  // int get_next_row(ObExecContext &ctx, const ObNewRow *&row) const;
  // into outfile 'a.test'
  int into_outfile(ObSelectIntoCtx& into_ctx, const common::ObNewRow& row) const;
  // into dumpfile 'a.test'
  int into_dumpfile(ObSelectIntoCtx& into_ctx, const common::ObNewRow& row) const;
  // into @a,@b
  int into_varlist(const common::ObNewRow& row, const ObExprCtx& expr_ctx) const;
  int get_row_str(const common::ObNewRow& row, const int64_t buf_len, bool is_first_row, char* buf, int64_t& pos,
      ObSelectIntoCtx& into_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObSelectInto);

private:
  ObItemType into_type_;
  common::ObSEArray<common::ObString, 16> user_vars_;
  common::ObObj outfile_name_;
  common::ObObj filed_str_;
  common::ObObj line_str_;
  char closed_cht_;
  bool is_optional_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_H_ */
