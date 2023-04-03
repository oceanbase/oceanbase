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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_TRANSLATE_
#define OCEANBASE_SQL_ENGINE_EXPR_TRANSLATE_

#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase
{
namespace sql
{

class ObExprTranslate : public ObStringExprOperator
{
  public:
    explicit ObExprTranslate(common::ObIAllocator &alloc);
    virtual ~ObExprTranslate();

    virtual int calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types,
                                  int64_t param_num,
                                  common::ObExprTypeCtx &type_ctx) const;

    virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;

    static int translate_hashmap(const common::ObString &ori_str,
                                 const common::ObString &from_str,
                                 const common::ObString &to_str,
                                 common::ObCollationType cs_type,
                                 common::ObIAllocator &tmp_alloc,
                                 common::ObIAllocator &res_alloc,
                                 common::ObString &res_str,
                                 bool &is_null);
  private:
    typedef common::hash::ObHashMap<common::ObString, common::ObString> StringHashMap;

    static int insert_map(const common::ObString &key_str,
                          const common::ObString &val_str,
                          common::ObIAllocator &allocator,
                          const common::ObFixedArray<size_t, common::ObIAllocator> &byte_num_key_str,
                          const common::ObFixedArray<size_t, common::ObIAllocator> &byte_num_val_str,
                          StringHashMap &ret_map);

    DISALLOW_COPY_AND_ASSIGN(ObExprTranslate);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_TRANSLATE_ */
