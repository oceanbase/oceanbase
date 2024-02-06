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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SOUNDEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SOUNDEX_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSoundex : public ObFuncExprOperator
{
public:
  explicit  ObExprSoundex(common::ObIAllocator &alloc);
  virtual ~ObExprSoundex() {};
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int8_t get_character_code(const int32_t wchar);
  static int convert_str_to_soundex(const ObString &input,
                                    const ObCollationType input_cs_type,
                                    const bool use_original_algo,
                                    const bool fix_min_len,
                                    char *buf, const int64_t len, int64_t &pos,
                                    bool &is_first,
                                    int8_t &last_soundex_code);
  static int calc(const ObString &input, const ObCollationType intput_cs_type,
                  const ObCollationType res_cs_type,
                  common::ObIAllocator &tmp_alloc, common::ObIAllocator &res_alloc,
                  ObString &out);
  static int calc_text(const ObDatum &input_datum,
                       const ObObjType input_type,
                       const ObObjType res_type,
                       const ObCollationType input_cs_type,
                       const ObCollationType res_cs_type,
                       const bool input_has_lob_header,
                       ObIAllocator &tmp_alloc, ObIAllocator &res_alloc,
                       ObString &out,
                       bool has_lob_header);
  static int eval_soundex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  static const int64_t MIN_RESULT_LENGTH = 4;
  // if result collation type is nonascii such as utf16, we need charset convert in the end.
  // str_allocator_ is used to allocate memory for result before charset convert.
  common::ObArenaAllocator str_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObExprSoundex);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SOUNDEX_ */
