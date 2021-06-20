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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_LRPAD_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_LRPAD_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprBaseLRpad : public ObStringExprOperator {
public:
  enum LRpadType { LPAD_TYPE = 0, RPAD_TYPE = 1 };
  explicit ObExprBaseLRpad(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num);

  virtual ~ObExprBaseLRpad();

  static int calc_type(ObExprResType& type, ObExprResType& text, ObExprResType& len, ObExprResType* pad_text,
      common::ObExprTypeCtx& type_ctx);

  static int padding(LRpadType type, const common::ObCollationType coll_type, const char* text,
      const int64_t& text_size, const char* pad, const int64_t& pad_size, const int64_t& prefix_size,
      const int64_t& repeat_count,
      const bool& pad_space,  // for oracle
      common::ObIAllocator* allocator, char*& result, int64_t& size);

  int calc(const LRpadType type, const common::ObObj& text, const common::ObObj& len, const common::ObObj& pad_text,
      common::ObExprCtx& expr_ctx, common::ObObj& result) const;

  // @param [in/out] use result.collation as input param
  // for ObExprPad
  static int calc_mysql(const LRpadType type, const ObExprResType result_type, const common::ObObj& text,
      const common::ObObj& len, const common::ObObj& pad_text, const ObSQLSessionInfo* session,
      common::ObIAllocator* allocator, common::ObObj& result);

  static int get_padding_info_mysql(const common::ObCollationType& cs, const common::ObString& str_text,
      const int64_t& len, const common::ObString& str_padtext, const int64_t max_result_size, int64_t& repeat_count,
      int64_t& prefix_size, int64_t& size);

  static int calc_oracle(const LRpadType type, const ObExprResType result_type, const common::ObObj& text,
      const common::ObObj& len, const common::ObObj& pad_text, common::ObExprCtx& expr_ctx, common::ObObj& result);
  static int calc_type_length_mysql(const ObExprResType result_type, const common::ObObj& text,
      const common::ObObj& pad_text, const common::ObObj& len, const ObBasicSessionInfo* session, int64_t& result_size);
  static int calc_type_length_oracle(const ObExprResType& result_type, const common::ObObj& text,
      const common::ObObj& pad_text, const common::ObObj& len, int64_t& result_size);

  static int get_padding_info_oracle(const common::ObCollationType cs, const common::ObString& str_text,
      const int64_t& width, const common::ObString& str_padtext, const int64_t max_result_size, int64_t& repeat_count,
      int64_t& prefix_size, bool& pad_space);
  // for engine 3.0
  static int calc_mysql_pad_expr(const ObExpr& expr, ObEvalCtx& ctx, LRpadType pad_type, ObDatum& res);
  static int calc_mysql(const LRpadType pad_type, const ObExpr& expr, const common::ObDatum& text,
      const common::ObDatum& len, const common::ObDatum& pad_text, const ObSQLSessionInfo& session,
      common::ObIAllocator& res_alloc, ObDatum& res);
  static int calc_oracle_pad_expr(const ObExpr& expr, ObEvalCtx& ctx, LRpadType pad_type, ObDatum& res);
  static int calc_oracle(LRpadType pad_type, const ObExpr& expr, const common::ObDatum& text,
      const common::ObDatum& len, const common::ObDatum& pad_text, common::ObIAllocator& res_alloc, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBaseLRpad);
};

class ObExprLpad : public ObExprBaseLRpad {
public:
  explicit ObExprLpad(common::ObIAllocator& alloc);
  virtual ~ObExprLpad();

  virtual int calc_result_type3(ObExprResType& type, ObExprResType& text, ObExprResType& len, ObExprResType& pad_text,
      common::ObExprTypeCtx& type_ctx) const override;

  virtual int calc_result3(common::ObObj& result, const common::ObObj& text, const common::ObObj& len,
      const common::ObObj& pad_text, common::ObExprCtx& expr_ctx) const override;
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_mysql_lpad_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLpad);
};

class ObExprRpad : public ObExprBaseLRpad {
public:
  explicit ObExprRpad(common::ObIAllocator& alloc);
  explicit ObExprRpad(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name);

  virtual ~ObExprRpad();

  virtual int calc_result_type3(ObExprResType& type, ObExprResType& text, ObExprResType& len, ObExprResType& pad_text,
      common::ObExprTypeCtx& type_ctx) const override;

  virtual int calc_result3(common::ObObj& result, const common::ObObj& text, const common::ObObj& len,
      const common::ObObj& pad_text, common::ObExprCtx& expr_ctx) const override;
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_mysql_rpad_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRpad);
};

class ObExprOracleLpad : public ObExprBaseLRpad {
public:
  explicit ObExprOracleLpad(common::ObIAllocator& alloc);
  virtual ~ObExprOracleLpad();

  virtual int calc_result_typeN(ObExprResType& type, ObExprResType* types_array, int64_t param_num,
      common::ObExprTypeCtx& type_ctx) const override;

  virtual int calc_resultN(common::ObObj& result, const common::ObObj* objs_array, int64_t param_num,
      common::ObExprCtx& expr_ctx) const override;
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_oracle_lpad_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleLpad);
};

class ObExprOracleRpad : public ObExprBaseLRpad {
public:
  explicit ObExprOracleRpad(common::ObIAllocator& alloc);
  virtual ~ObExprOracleRpad();

  virtual int calc_result_typeN(ObExprResType& type, ObExprResType* types_array, int64_t param_num,
      common::ObExprTypeCtx& type_ctx) const override;

  virtual int calc_resultN(common::ObObj& result, const common::ObObj* objs_array, int64_t param_num,
      common::ObExprCtx& expr_ctx) const override;
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_oracle_rpad_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleRpad);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_RPAD_
