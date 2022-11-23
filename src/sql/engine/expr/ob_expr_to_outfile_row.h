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

#ifndef _OB_SQL_EXPR_TO_OUTFILE_ROW_H_
#define _OB_SQL_EXPR_TO_OUTFILE_ROW_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprOutFileInfo : public ObExprOperatorCtx
{
public:
  ObExprOutFileInfo()
    : field_(),
      line_(),
      enclose_(),
      escape_(),
      is_optional_(false),
      wchar_field_(0),
      wchar_line_(0),
      wchar_enclose_(0),
      wchar_escape_(0),
      print_params_(),
      tmp_buf_(nullptr),
      tmp_buf_len_(0),
      buf_(nullptr),
      buf_len_(0){}

  common::ObObj field_;
  common::ObObj line_;
  common::ObObj enclose_;
  common::ObObj escape_;
  bool is_optional_;
  int32_t wchar_field_;
  int32_t wchar_line_;
  int32_t wchar_enclose_;
  int32_t wchar_escape_;
  common::ObObjPrintParams print_params_;

  char *tmp_buf_;
  int64_t tmp_buf_len_;
  char *buf_;
  int64_t buf_len_;

  TO_STRING_KV(K_(field), K_(line), K_(enclose), K_(escape), K_(is_optional), K_(wchar_field),
               K_(wchar_line), K_(wchar_enclose), K_(wchar_escape), K_(print_params));
};


class ObExprToOutfileRow : public ObStringExprOperator
{

public:
  static const int64_t INTERNAL_BUG_LEN = OB_MAX_ROW_LENGTH;
  explicit  ObExprToOutfileRow(common::ObIAllocator &alloc);
  virtual ~ObExprToOutfileRow();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  virtual bool need_rt_ctx() const override { return true; }
  static int to_outfile_str(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &datum);
  static int extract_fisrt_wchar_from_varhcar(const common::ObObj &obj, int32_t &wchar);
  static int print_field(char *buf, const int64_t buf_len, int64_t &pos, const common::ObObj &obj,
                         ObExprOutFileInfo &out_info);
  static int print_wchar_to_buf(char *buf, const int64_t buf_len, int64_t &pos,
                                int32_t wchar, common::ObCollationType coll_type);
  static int copy_string_to_buf(char *buf, const int64_t buf_len, int64_t &pos,
                                const common::ObString &str);
  static int calc_outfile_info(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               common::ObIAllocator &allocator,
                               ObExprOutFileInfo &out_info);
  static int extend_buffer(ObExprOutFileInfo &out_info,
                           common::ObIAllocator &allocator);
private:
  enum ParameterEnum {
    PARAM_FIELD = 0,
    PARAM_LINE,
    PARAM_ENCLOSED,
    PARAM_OPTIONAL,
    PARAM_ESCAPED,
    PARAM_CHARSET,
    PARAM_SELECT_ITEM
  };
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprToOutfileRow) const;
};

}
}
#endif /* _OB_SQL_EXPR_TO_OUTFILE_ROW_H_ */
