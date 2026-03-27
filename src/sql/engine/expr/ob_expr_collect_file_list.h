/**
* Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_COLLECT_FILE_LIST_
#define OCEANBASE_SQL_ENGINE_EXPR_COLLECT_FILE_LIST_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

struct ObExprCollectFileListRes
{
  OB_UNIS_VERSION(1);
public:
  ObString file_urls_;
  int64_t sizes_;
  int64_t last_modify_times_;

  TO_STRING_KV(K_(file_urls),
              K_(sizes),
              K_(last_modify_times));
};

class ObExprCollectFileList : public ObFuncExprOperator
{
public:
  explicit ObExprCollectFileList(common::ObIAllocator &alloc);
  ~ObExprCollectFileList() override {};

  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const override;

  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int collect_file_list(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  static int prepare_serialize(ObIAllocator &allocator,
                               const ObIArray<ObString> &files,
                               const ObIArray<int64_t> &sizes,
                               const ObIArray<int64_t> &last_modify_times,
                               ObFixedArray<ObExprCollectFileListRes, ObIAllocator> &res);

};

}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_COLLECT_FILE_LIST_
