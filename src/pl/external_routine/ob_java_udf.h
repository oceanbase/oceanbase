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

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDF_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDF_H_

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

class ObJavaUDFExecutor
{
public:
  ObJavaUDFExecutor(sql::ObExecContext &exec_ctx_, const sql::ObExprUDFInfo &udf_info)
    : exec_ctx_(exec_ctx_),
      udf_info_(udf_info)
  {  }

  int init();

  int execute(int64_t batch_size,
              const ObIArray<ObObjMeta> &arg_types,
              const ObIArray<ObIArray<ObObj>*> &args,
              ObIAllocator &result_allocator,
              ObIArray<ObObj> &result);

private:
  int get_url_jar_cache(ObExternalResourceCache<ObExternalURLJar> *&cache);
  int get_schema_jar_cache(ObExternalResourceCache<ObExternalSchemaJar> *&cache);

  int build_udf_args(const ObIArray<ObObjMeta> &arg_types,
                     const ObIArray<ObIArray<ObObj> *> &args,
                     JNIEnv &env,
                     jclass object_clazz,
                     jobjectArray &java_types,
                     jobjectArray &java_args);

   static int get_ob_type_to_java_map(const ObObjMeta &obj_meta, JNIEnv &env, ObIAllocator &alloc, ObToJavaTypeMapperBase *&functor);

   static int get_java_type_to_ob_map(const ObObjMeta &obj_meta, JNIEnv &env, ObIAllocator &alloc, ObFromJavaTypeMapperBase *&functor);

private:
  sql::ObExecContext &exec_ctx_;
  const sql::ObExprUDFInfo &udf_info_;
  bool is_inited_ = false;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDF_H_
