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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class HandleCharEscape;

class ObExprQuote: public ObStringExprOperator
{
public:
  friend class HandleCharEscape;
  ObExprQuote();
  explicit  ObExprQuote(common::ObIAllocator &alloc);
  virtual ~ObExprQuote();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_quote_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum);
  static int eval_quote_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  static const int16_t APPEND_LEN = 2;
  static const int16_t LEN_OF_NULL = 4;
  // quote string
  static int calc(common::ObString &res_str, common::ObString str,
                  common::ObCollationType coll_type, common::ObIAllocator *allocator);
  static int string_write_buf(const common::ObString &str, char *buf, const int64_t buf_len, int64_t &pos);

  // Vector evaluation helpers
  static bool has_special_chars(const common::ObString &str, common::ObCollationType coll_type);
  static int process_string_simple(const ObString &str, const ObString &quote_char, ObCollationType coll_type,
                                   char *buf, const int64_t buf_len, int64_t &pos);
  static int process_string_with_special_chars(const ObString &str, ObCollationType coll_type,
    char *buf, const int64_t buf_len, int64_t &pos);

  // Common helper for escaping special characters
  static int escape_char_in_string(int wchar, const common::ObString &escape_char,
                                    const common::ObString &quote_char, common::ObCollationType coll_type,
                                    const common::ObString &code_point, char *buf, const int64_t buf_len, int64_t &pos);

  template <typename IN_VEC, typename OUT_VEC>
  static int inner_eval_quote_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DISALLOW_COPY_AND_ASSIGN(ObExprQuote);

};
// Functor for checking special characters in byte iteration
class CheckByteForSpecialChar
{
public:
  explicit CheckByteForSpecialChar(bool &has_special) : has_special_(has_special) {}
  int operator()(unsigned char byte, int64_t index)
  {
    if (byte == '\0' || byte == '\'' || byte == '\\' || byte == '\032') {
      has_special_ = true;
    }
    return OB_SUCCESS;
  }
private:
  bool &has_special_;
};

// Functor for checking special characters in character iteration
class CheckCharForSpecialChar
{
public:
  explicit CheckCharForSpecialChar(bool &has_special) : has_special_(has_special) {}
  int operator()(ObString code_point, int wchar)
  {
    if (wchar == '\0' || wchar == '\'' || wchar == '\\' || wchar == '\032') {
      has_special_ = true;
    }
    return OB_SUCCESS;
  }
private:
  bool &has_special_;
};

// Functor for handling character escaping in QUOTE function
class HandleCharEscape
{
public:
  HandleCharEscape(const ObString &escape_char, const ObString &quote_char,
                   ObCollationType coll_type, char *buf, int64_t buf_len, int64_t &pos)
      : escape_char_(escape_char), quote_char_(quote_char), coll_type_(coll_type),
        buf_(buf), buf_len_(buf_len), pos_(pos) {}
  int operator()(ObString code_point, int wchar)
  {
    return ObExprQuote::escape_char_in_string(wchar, escape_char_, quote_char_,
                                               coll_type_, code_point, buf_, buf_len_, pos_);
  }
private:
  const ObString &escape_char_;
  const ObString &quote_char_;
  ObCollationType coll_type_;
  char *buf_;
  int64_t buf_len_;
  int64_t &pos_;
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_ */
