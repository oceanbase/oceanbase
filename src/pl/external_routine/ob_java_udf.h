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
friend class ObOraJavaRoutineExecutor;
friend class ObOraJavaSessionState;

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

  static int build_udf_args(const ObSQLSessionInfo &session,
                            ObSchemaGetterGuard &schema_guard,
                            int64_t batch_size,
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
  bool is_inited_ = false;
  sql::ObExecContext &exec_ctx_;
  ObString entry_;
  jobject loader_ = nullptr;
  bool need_infer_result_size_ = false;
};

struct ObOraJavaRoutineInfo
{
public:
  static int parse_java_routine_info(ObIAllocator &alloc, const ObString &entry, ObOraJavaRoutineInfo &info);

private:
  static int parse_class_method_name(ObIAllocator &alloc,
                                    const char *ptr,
                                     int64_t paren_pos,
                                     ObString &class_name,
                                     ObString &method_name);

  static int parse_param_types(ObIAllocator &alloc,
                               const char *ptr,
                               int64_t paren_pos,
                               int64_t close_paren_pos,
                               ObIArray<ObString> &params_types);

  static int parse_return_type(ObIAllocator &alloc,
                               const char *ptr,
                               int64_t len,
                               int64_t close_paren_pos,
                               ObString &return_type);

public:
  ObOraJavaRoutineInfo()
    : class_name_(),
      method_name_(),
      params_types_(),
      return_type_()
  {  }

  inline bool is_valid() const
  {
    return !class_name_.empty() && !method_name_.empty();
  }

  TO_STRING_KV(K_(class_name), K_(method_name), K_(params_types), K_(return_type));

public:
  ObString class_name_;
  ObString method_name_;
  ObSEArray<ObString, 8> params_types_;
  ObString return_type_;
};

// ObOraJavaSessionState is cached in session, so it may be used in multiple threads,
// therefore, do not cache JNIEnv and local references in this class.
class ObOraJavaSessionState
{
  friend class ObOraJavaRoutineExecutor;

public:
  ObOraJavaSessionState(ObSQLSessionInfo &session)
    : alloc_(ObMemAttr(MTL_ID(), "PlJavaUdf")),
      session_(session)
  {  }

  ~ObOraJavaSessionState();

  int init();

public:
  static int init_session_state(ObSQLSessionInfo &session);

  private:
  struct JarCacheKey {
  public:
    JarCacheKey()
      : jar_id_(OB_INVALID_ID),
        class_name_()
    {  }

    JarCacheKey(uint64_t jar_id, const ObString &class_name)
      : jar_id_(jar_id),
        class_name_(class_name)
    {  }

    int hash(uint64_t &res) const
    {
      res = murmurhash(&jar_id_, sizeof(jar_id_), 0);
      res = murmurhash(class_name_.ptr(), class_name_.length(), res);
      return OB_SUCCESS;
    }

    bool operator==(const JarCacheKey &other) const
    {
      return jar_id_ == other.jar_id_ && class_name_ == other.class_name_;
    }

  public:
    TO_STRING_KV(K_(jar_id), K_(class_name));

  public:
    uint64_t jar_id_;
    ObString class_name_;

  };

private:
  bool is_inited_ = false;
  ObArenaAllocator alloc_;
  ObSQLSessionInfo &session_;
  jobject session_state_ = nullptr;
  jmethodID session_state_execute_method_ = nullptr;
  ObHashMap<JarCacheKey, ObString> jar_cache_;

};

class ObOraJavaRoutineExecutor
{
public:
  static jbyteArray jni_fetch_class(JNIEnv *env, jobject session_state, jlong executor_ptr, jbyteArray class_name);

  static jboolean jni_check_class_obsolete(JNIEnv *env, jobject session_state, jlong executor_ptr, jbyteArray class_name, jlong java_class_id);

public:
  ObOraJavaRoutineExecutor(ObPLExecCtx &ctx, const ObPLFunction &func)
    : is_inited_(false),
      ctx_(ctx),
      func_(func)
  {  }

  int is_valid() const;
  int init();

  int execute(int64_t argc, int64_t *argv);

  TO_STRING_KV(K_(is_inited), K_(func), KPC(func_.get_ora_java_routine_info()));

private:
  static int build_udf_arg_arrays(JNIEnv &env,
                                  ObIAllocator &alloc,
                                  const ObPLFunction &func,
                                  int64_t argc,
                                  int64_t *argv,
                                  ObIArray<ObObjMeta> &arg_types,
                                  ObIArray<ObIArray<ObObj> *> &args,
                                  jclass object_clazz,
                                  jobjectArray &java_type_names);

  int handle_function_result(JNIEnv *env, jobject java_result, ObFromJavaTypeMapperBase *result_mapper);

  int collect_enabled_privs();

private:
  bool is_inited_;
  ObPLExecCtx &ctx_;
  const ObPLFunction &func_;

};

class ObJavaUDFPermission
{
public:
  ObJavaUDFPermission()
    : is_grant_(false),
      type_(),
      name_(),
      action_()
  {  }

  ObJavaUDFPermission(bool is_grant,
                      const ObString &type,
                      const ObString &name,
                      const ObString &action)
    : is_grant_(is_grant),
      type_(type),
      name_(name),
      action_(action)
  {  }

private:
  void to_protobuf(ObPl__JavaUdf__Permission &permission) const
  {
    permission.is_grant = is_grant_;
    permission.type.len = type_.length();
    permission.type.data = (uint8_t*)(type_.ptr());
    permission.name.len = name_.length();
    permission.name.data = (uint8_t*)(name_.ptr());
    permission.action.len = action_.length();
    permission.action.data = (uint8_t*)(action_.ptr());
  }

public:
  TO_STRING_KV(K_(is_grant), K_(type), K_(name), K_(action));

public:
  static int collect_enabled_permissions(const ObSQLSessionInfo &session,
                                         ObSchemaGetterGuard &schema_guard,
                                         ObIArray<ObJavaUDFPermission> &permissions);

  static int collect_enabled_permissions_of_grantee(const uint64_t tenant_id,
                                                    const uint64_t grantee,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    ObIArray<ObJavaUDFPermission> &permissions);

  static int permissions_to_protobuf(const ObIArray<ObJavaUDFPermission> &permission_array,
                                     ObIAllocator &alloc,
                                     decltype(ObPl__JavaUdf__BatchedArgs::n_permissions) &n_permissions,
                                     decltype(ObPl__JavaUdf__BatchedArgs::permissions) &permissions);

private:
  bool is_grant_;
  ObString type_;
  ObString name_;
  ObString action_;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_OB_JAVA_UDF_H_
