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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_I18N_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_I18N_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{

class ObExprUtlI18nStringToRaw : public ObStringExprOperator
{
public:
  explicit ObExprUtlI18nStringToRaw(common::ObIAllocator &alloc);
  virtual ~ObExprUtlI18nStringToRaw();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  const common::ObObj &obj2,
                  common::ObCastCtx &cast_ctx,
                  const ObSQLSessionInfo *session_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlI18nStringToRaw);
};

class ObExprUtlI18nRawToChar : public ObStringExprOperator
{
public:
  explicit ObExprUtlI18nRawToChar(common::ObIAllocator &alloc);
  virtual ~ObExprUtlI18nRawToChar();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  const common::ObObj &obj2,
                  common::ObCastCtx &cast_ctx,
                  const ObSQLSessionInfo *session_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlI18nRawToChar);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_I18N_ */
