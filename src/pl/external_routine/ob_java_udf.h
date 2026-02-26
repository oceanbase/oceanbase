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

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"
#include "pl/external_routine/ob_external_resource.h"

namespace oceanbase
{

namespace pl
{

class ObJavaUDFExecutor
{
friend class ObJavaUDAFExecutor;

public:
  ObJavaUDFExecutor(sql::ObExecContext &exec_ctx_, const ObString &entry)
    : exec_ctx_(exec_ctx_),
      entry_(entry)
  {  }

  int init(int64_t udf_id,
           ObExternalRoutineType type,
           const ObString &url,
           const ObString &resource);

  int execute(int64_t batch_size,
              const char *method_name,
              const ObIArray<ObObjMeta> &arg_types,
              const ObIArray<ObIArray<ObObj>*> &args,
              const ObExprResType &res_type,
              ObIAllocator &result_allocator,
              ObIArray<ObObj> &result);

  inline void set_need_infer_result_size(bool need_infer_result_size) { need_infer_result_size_ = need_infer_result_size; }
  inline bool need_infer_result_size() const { return need_infer_result_size_; }

private:
  static int get_url_jar_cache(ObExecContext &exec_ctx, ObExternalResourceCache<ObExternalURLJar> *&cache);
  static int get_schema_jar_cache(ObExecContext &exec_ctx, ObExternalResourceCache<ObExternalSchemaJar> *&cache);

  static int get_class_loader(ObExecContext &exec_ctx,
                              int64_t udf_id,
                              ObExternalRoutineType type,
                              const ObString &url,
                              const ObString &resource,
                              jobject &class_loader);

  static int build_udf_args(int64_t batch_size,
                            const ObIArray<ObObjMeta> &arg_types,
                            const ObIArray<ObIArray<ObObj> *> &args,
                            JNIEnv &env,
                            ObIAllocator &arg_alloc,
                            jclass object_clazz,
                            jobjectArray &java_types,
                            jobject &java_args);

  static int get_ob_type_to_java_map(const ObObjMeta &obj_meta,
                                     int64_t batch_size,
                                     JNIEnv &env,
                                     ObIAllocator &alloc,
                                     ObToJavaTypeMapperBase *&functor);

  static int get_java_type_to_ob_map(const ObExprResType &res_type,
                                     JNIEnv &env,
                                     ObIAllocator &alloc,
                                     int64_t batch_size,
                                     ObSQLSessionInfo &session,
                                     ObFromJavaTypeMapperBase *&functor);

private:
  static int get_executor_class(JNIEnv &env, jclass &executor_class, jmethodID &execute)
  {
    int ret = OB_SUCCESS;

    static std::tuple<ObLatchMutex, jclass, jmethodID> cached_executor;

    executor_class = nullptr;
    execute = nullptr;

    ObLatchMutexGuard guard(std::get<0>(cached_executor), ObLatchIds::JAVA_EXECUTOR_CLASS_LOCK);

    if (OB_ISNULL(std::get<1>(cached_executor))) {
      jclass loader_class = nullptr;
      jmethodID init = nullptr;
      jobject loader = nullptr;
      jmethodID load_class_method = nullptr;
      jstring class_name = nullptr;
      jobject class_obj = nullptr;
      jclass global_executor = nullptr;
      jmethodID execute_method = nullptr;

      loader_class = env.FindClass("com/oceanbase/internal/ObJavaUDFExecutorClassLoader");

      if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find ObJavaUDFExecutorClassLoader class", K(ret));
      } else if (OB_ISNULL(loader_class)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL ObJavaUDFExe  cutorClassLoader class", K(ret));
      } else if (FALSE_IT(init = env.GetMethodID(loader_class, "<init>", "()V"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find init method of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(init)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL init method of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (FALSE_IT(loader = env.NewObject(loader_class, init))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to create new ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(loader)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "unexpected NULL init method of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (FALSE_IT(load_class_method = env.GetMethodID(loader_class, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to GetMethodID loadClass from ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(load_class_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL loadClass", K(ret));
      } else if (OB_ISNULL(class_name = env.NewStringUTF("com.oceanbase.internal.ObJavaUDFExecutor"))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL string", K(ret));
      } else if (FALSE_IT(class_obj = env.CallObjectMethod(loader, load_class_method, class_name))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to create call loadClass of ObJavaUDFExecutorClassLoader", K(ret));
      } else if (OB_ISNULL(class_obj)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL ObJavaUDFExecutor class", K(ret));
      } else if (
          FALSE_IT(
              execute_method = env.GetStaticMethodID(
                  static_cast<jclass>(class_obj),
                  "execute",
                  "("
                    "Ljava/lang/Object;"       // obj, UDF object
                    "Ljava/lang/String;"       // methodName, java String type
                    "J"                        // timeoutTs, java long type
                    "[Ljava/lang/Object;"      // argTypes, java Object[] type
                    "Ljava/nio/ByteBuffer;"    // argValues, java ByteBuffer type
                    "Ljava/lang/Object;"       // resultType, java Object type
                  ")"
                  "Ljava/nio/ByteBuffer;"      // return value, java ByteBuffer type
                ))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        PL_LOG(WARN, "failed to find execute method in ObJavaUDFExecutor class", K(ret));
      } else if (OB_ISNULL(execute_method)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL execute method of ObJavaUDFExecutor class", K(ret));
      } else if (OB_ISNULL(global_executor = static_cast<jclass>(env.NewGlobalRef(class_obj)))) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN,"unexpected NULL NewGlobalRef", K(ret));
      } else {
        std::get<1>(cached_executor) = global_executor;
        std::get<2>(cached_executor) = execute_method;
      }

      ObJavaUtils::delete_local_ref(class_obj, &env);
      ObJavaUtils::delete_local_ref(class_name, &env);
      ObJavaUtils::delete_local_ref(loader, &env);
      ObJavaUtils::delete_local_ref(loader_class, &env);
    }

    if (OB_SUCC(ret)) {
      executor_class = std::get<1>(cached_executor);
      execute = std::get<2>(cached_executor);
    }

    return ret;
  }

private:
  bool is_inited_ = false;
  sql::ObExecContext &exec_ctx_;
  ObString entry_;
  jobject loader_ = nullptr;
  bool need_infer_result_size_ = false;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDF_H_
