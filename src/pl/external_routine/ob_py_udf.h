/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"
#include "pl/external_routine/ob_external_resource.h"

namespace oceanbase
{
namespace pl
{

class ObPyUDFExecutor
{
public:
  ObPyUDFExecutor(sql::ObExecContext &exec_ctx_, const sql::ObExprUDFInfo &udf_info)
    : exec_ctx_(exec_ctx_),
      udf_info_(udf_info),
      is_inited_(false)
      {}
  virtual ~ObPyUDFExecutor() {}

  int init();
  int execute(int64_t batch_size,
              const ObIArray<ObObjMeta> &arg_types,
              const ObIArray<ObIArray<ObObj> *> &args,
              ObIAllocator &result_allocator,
              ObIArray<ObObj> &result);
private:
  int build_udf_args(ObPySubInterContext &ctx,
                     const ObIArray<ObIArray<ObObj>*> &args,
                     ObPyObject *&py_args);
  int get_url_py_cache(ObExternalResourceCache<ObExternalURLPy> *&cache);

  int get_schema_py_cache(ObExternalResourceCache<ObExternalSchemaPy> *&cache);

private:
  sql::ObExecContext &exec_ctx_;
  const sql::ObExprUDFInfo &udf_info_;
  bool is_inited_;
};

} // namespace pl
} // namespace oceanbase


#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_PYTHON_UDF_H_
