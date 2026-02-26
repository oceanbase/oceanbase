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
