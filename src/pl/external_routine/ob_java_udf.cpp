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

#define USING_LOG_PREFIX PL

#include "pl/external_routine/ob_java_udf.h"

#include "lib/allocator/page_arena.h"
#include "sql/ob_spi.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/jni_env/ob_java_env.h"
#include "lib/jni_env/ob_jni_connector.h"

namespace oceanbase
{

namespace pl
{

int ObJavaUDFExecutor::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  }

  return ret;
}

int ObJavaUDFExecutor::execute(int64_t batch_size,
                               const ObIArray<ObObjMeta> &arg_types,
                               const ObIArray<ObIArray<ObObj> *> &args,
                               ObIAllocator &result_allocator,
                               ObIArray<ObObj> &result)
{
  int ret = OB_SUCCESS;

  using share::schema::ObExternalRoutineType;

  jobject loader = nullptr;

  CK (arg_types.count() == args.count());
  CK (OB_NOT_NULL(exec_ctx_.get_my_session()));

  for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
    CK (OB_NOT_NULL(args.at(i)));
    CK (args.at(i)->count() == batch_size);
  }

  if OB_SUCC(ret) {
    switch (udf_info_.external_routine_type_) {
    case ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_URL: {
      ObExternalResourceCache<ObExternalURLJar> *cache = nullptr;

      if (OB_FAIL(get_url_jar_cache(cache))) {
        LOG_WARN("failed to get url jar cache", K(ret));
      } else if (OB_ISNULL(cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL jar cache", K(ret));
      } else if (OB_FAIL(cache->get_resource(udf_info_.udf_id_, udf_info_.external_routine_url_, loader))) {
        LOG_WARN("failed to get class loader from jar cache", K(ret));
      } else if (OB_ISNULL(loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL class loader", K(ret));
      }
      break;
    }
    case ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_RES: {
      ObExternalResourceCache<ObExternalSchemaJar> *cache = nullptr;
      ObSchemaGetterGuard *schema_guard = nullptr;
      uint64_t database_id;

      if (OB_FAIL(get_schema_jar_cache(cache))) {
        LOG_WARN("failed to get schema jar cache", K(ret));
      } else if (OB_ISNULL(cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL jar cache", K(ret));
      } else if (OB_ISNULL(exec_ctx_.get_sql_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL exec_ctx_.get_sql_ctx()", K(ret));
      } else if (OB_ISNULL(schema_guard = exec_ctx_.get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL exec_ctx_.get_sql_ctx()->schema_guard_", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == (database_id = exec_ctx_.get_my_session()->get_database_id()))) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no db selected", K(ret));
      } else if (OB_FAIL(cache->get_resource(udf_info_.udf_id_,
                                             std::make_pair(database_id, udf_info_.external_routine_resource_),
                                             loader,
                                             *schema_guard))) {
        LOG_WARN("failed to get class loader from jar cache", K(ret), K(udf_info_), K(database_id));
      } else if (OB_ISNULL(loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL class loader", K(ret));
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected external routine type", K(ret), K(udf_info_));
    }
  }

  if (OB_SUCC(ret)) {
    JNIEnv *env = nullptr;

    if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
      LOG_WARN("failed to get_jni_env", K(ret));
    } else {
      jclass executor_clazz = nullptr;
      jmethodID execute_method = nullptr;
      jstring clazz_name = nullptr;
      jstring method_name = nullptr;
      jclass object_clazz =nullptr;
      jobjectArray java_types = nullptr;
      jobjectArray java_args = nullptr;
      jobjectArray results = nullptr;
      int64_t timeout_ts = exec_ctx_.get_my_session()->get_query_timeout_ts();

      if (OB_ISNULL(executor_clazz = env->FindClass("com/oceanbase/internal/ObJavaUDFExecutor"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find ObJavaUDFExecutor class", K(ret));
      } else if (
          OB_ISNULL(
              execute_method = env->GetStaticMethodID(
                  executor_clazz,
                  "execute",
                  "("
                    "Ljava/lang/ClassLoader;"  // classLoader, java ClassLoader type
                    "Ljava/lang/String;"       // clazzName, java String type
                    "Ljava/lang/String;"       // methodName, java String type
                    "J"                        // timeoutTs, java long type
                    "J"                        // batchSize, java long type
                    "[Ljava/lang/Object;"      // argTypes, java Object[] type
                    "[[Ljava/lang/Object;"     // argValues, java Object[][] type
                    "Ljava/lang/Object;"       // resultType, java Object type
                    "[Ljava/lang/Object;"      // result, java Object type
                  ")"
                  "V"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find execute method in ObJavaUDFExecutor class", K(ret));
      } else if (OB_ISNULL(clazz_name = env->NewStringUTF(udf_info_.external_routine_entry_.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to NewStringUTF", K(ret), K(udf_info_.external_routine_entry_));
      } else if (OB_ISNULL(method_name = env->NewStringUTF("evaluate"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to NewStringUTF execute", K(ret));
      } else if (OB_ISNULL(object_clazz = env->FindClass("java/lang/Object"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find java/lang/Object class", K(ret));
      } else if (OB_FAIL(build_udf_args(arg_types, args, *env, object_clazz, java_types, java_args))) {
        LOG_WARN("failed to build udf args", K(ret));
      } else if (OB_ISNULL(results = env->NewObjectArray(batch_size, object_clazz, nullptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get udf results", K(ret));
      } else {
        ObFromJavaTypeMapperBase *result_mapper = nullptr;

        if (OB_FAIL(get_java_type_to_ob_map(udf_info_.result_type_, *env, result_allocator, result_mapper))) {
          LOG_WARN("failed to get_result_mapper", K(ret));
        } else if (OB_ISNULL(result_mapper)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper", K(ret));
        } else if (OB_ISNULL(result_mapper->get_java_type_class())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper java type class", K(ret));
        } else {
          env->CallStaticVoidMethod(executor_clazz,
                                    execute_method,
                                    loader,
                                    clazz_name,
                                    method_name,
                                    timeout_ts,
                                    batch_size,
                                    java_types,
                                    java_args,
                                    result_mapper->get_java_type_class(),
                                    results);
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
          LOG_WARN("failed to execute Java UDF", K(ret), K(udf_info_), K(args));
        }

        if (OB_SUCC(ret)) {
          for (jsize i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
            ObObj tmp_obj;
            ObObj res_obj;
            jobject curr = env->GetObjectArrayElement(results, i);
            if (OB_ISNULL(curr)) {
              res_obj.set_null();
            } else if (OB_FAIL((*result_mapper)(curr, tmp_obj))) {
              LOG_WARN("failed to map java value to ob value", K(ret), K(i), K(tmp_obj));
            } else if (OB_FAIL(ObSPIService::spi_convert(*exec_ctx_.get_my_session(),
                                                         result_allocator,
                                                         tmp_obj,
                                                         udf_info_.result_type_,
                                                         res_obj,
                                                         false))) {
              LOG_WARN("failed to ObSPIService::spi_convert", K(ret), K(tmp_obj), K(res_obj));
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(result.push_back(res_obj))) {
                LOG_WARN("failed to push result to result array", K(ret), K(i), K(res_obj));
              }
            }

            ObJavaUtils::delete_local_ref(curr, env);
          }
        }

        if (OB_NOT_NULL(result_mapper)) {
          result_mapper->~ObFromJavaTypeMapperBase();
          result_mapper = nullptr;
        }
      }

      // always delete local ref
      ObJavaUtils::delete_local_ref(executor_clazz, env);
      ObJavaUtils::delete_local_ref(clazz_name, env);
      ObJavaUtils::delete_local_ref(method_name, env);
      ObJavaUtils::delete_local_ref(object_clazz, env);
      ObJavaUtils::delete_local_ref(java_types, env);
      ObJavaUtils::delete_local_ref(java_args, env);
      ObJavaUtils::delete_local_ref(results, env);
    }
  }

  return ret;
}

int ObJavaUDFExecutor::build_udf_args(const ObIArray<ObObjMeta> &arg_types,
                                      const ObIArray<ObIArray<ObObj>*> &args,
                                      JNIEnv &env,
                                      jclass object_clazz,
                                      jobjectArray &java_types,
                                      jobjectArray &java_args)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(java_types = env.NewObjectArray(args.count(), object_clazz, nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get udf arg types", K(ret));
  } else if (OB_ISNULL(java_args = env.NewObjectArray(args.count(), object_clazz, nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get udf arg values", K(ret));
  } else {
    ObArenaAllocator alloc;
    for (jsize i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
      ObToJavaTypeMapperBase *map_fun = nullptr;
      jobjectArray curr_column = nullptr;

      // TODO(heyongyi.hyy): cache map functors to a hash map
      if (OB_FAIL(get_ob_type_to_java_map(arg_types.at(i), env, alloc, map_fun))) {
        LOG_WARN("failed to get type map", K(ret));
      } else if (OB_ISNULL(map_fun)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid map fun", K(ret), K(map_fun));
      } else if (OB_ISNULL(map_fun->get_java_type_class())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL java type class", K(ret), K(map_fun), K(i), K(arg_types));
      } else if (FALSE_IT(env.SetObjectArrayElement(java_types, i, map_fun->get_java_type_class()))) {
        // unreachable
      } else if (OB_ISNULL(curr_column = env.NewObjectArray(args.at(i)->count(), object_clazz, nullptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get udf args row", K(ret));
      } else {
        CK (OB_NOT_NULL(args.at(i)));

        for (jsize j = 0; OB_SUCC(ret) && j < args.at(i)->count(); ++j) {
          jobject java_value = nullptr;
          const ObObj &ob_value = args.at(i)->at(j);

          if (ob_value.is_null()) {
            java_value = nullptr;
          } else if (arg_types.at(i).get_type() != ob_value.get_meta().get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected unmatch ObObjMeta", K(ret), K(i), K(arg_types.at(i)), K(ob_value));
          } else if (OB_FAIL((*map_fun)(ob_value, java_value))) {
            LOG_WARN("failed to map ob value to java value", K(ret), K(ob_value));
          } else {
            env.SetObjectArrayElement(curr_column, j, java_value);
          }

          ObJavaUtils::delete_local_ref(java_value, &env);
        }
      }

      if (OB_SUCC(ret)) {
        env.SetObjectArrayElement(java_args, i, curr_column);
      }

      if (OB_NOT_NULL(map_fun)) {
        map_fun->~ObToJavaTypeMapperBase();
        alloc.free(map_fun);
        map_fun = nullptr;
      }

      ObJavaUtils::delete_local_ref(curr_column, &env);
    }
  }

  return ret;
}

#define ALLOC_TYPE_MAPPER(MAPPER, ...)                                                                                 \
  do {                                                                                                                 \
    MAPPER *mapper = static_cast<MAPPER *>(alloc.alloc(sizeof(MAPPER)));                                               \
    if (OB_ISNULL(mapper)) {                                                                                           \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                                                 \
      LOG_WARN("failed to alloc memory for "#MAPPER, K(ret));                                                          \
    } else if (OB_ISNULL(mapper = new(mapper)MAPPER(__VA_ARGS__))) {                                                   \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      LOG_WARN("failed to construct "#MAPPER, K(ret));                                                                 \
    } else if (OB_FAIL(mapper->init())) {                                                                              \
      LOG_WARN("failed to init "#MAPPER, K(ret));                                                                      \
    } else {                                                                                                           \
      functor = mapper;                                                                                                \
    }                                                                                                                  \
  } while (0)

int ObJavaUDFExecutor::get_ob_type_to_java_map(const ObObjMeta &obj_meta,
                                               JNIEnv &env,
                                               ObIAllocator &alloc,
                                               ObToJavaTypeMapperBase *&functor)
{
  int ret = OB_SUCCESS;

  switch (obj_meta.get_type()) {
  case ObNullType: {
    ALLOC_TYPE_MAPPER(ObToJavaNullTypeMapper, env);
    break;
  }

  case ObTinyIntType:
  case ObUTinyIntType: {
    ALLOC_TYPE_MAPPER(ObToJavaByteTypeMapper, env);
    break;
  }

  case ObSmallIntType:
  case ObUSmallIntType: {
    ALLOC_TYPE_MAPPER(ObToJavaShortTypeMapper, env);
    break;
  }

  case ObMediumIntType:
  case ObUMediumIntType:
  case ObInt32Type:
  case ObUInt32Type: {
    ALLOC_TYPE_MAPPER(ObToJavaIntegerTypeMapper, env);
    break;
  }

  case ObBitType:
  case ObIntType:
  case ObUInt64Type: {
    ALLOC_TYPE_MAPPER(ObToJavaLongTypeMapper, env);
    break;
  }

  case ObFloatType:
  case ObUFloatType: {
    ALLOC_TYPE_MAPPER(ObToJavaFloatTypeMapper, env);
    break;
  }

  case ObDoubleType:
  case ObUDoubleType: {
    ALLOC_TYPE_MAPPER(ObToJavaDoubleTypeMapper, env);
    break;
  }

  case ObNumberType:
  case ObUNumberType:
  case ObNumberFloatType: {
    ALLOC_TYPE_MAPPER(ObToJavaBigDecimalTypeMapper, env);
    break;
  }

  case ObVarcharType:
  case ObCharType:
  case ObHexStringType:
  case ObNVarchar2Type:
  case ObNCharType:
  case ObTinyTextType:
  case ObTextType:
  case ObMediumTextType:
  case ObLongTextType:
  case ObLobType: {
    if (CHARSET_BINARY != obj_meta.get_charset_type()) {
      ALLOC_TYPE_MAPPER(ObToJavaStringTypeMapper, env);
    } else {
      ALLOC_TYPE_MAPPER(ObToJavaByteBufferTypeMapper, env);
    }
    break;
  }

  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type in Java UDF is not supported yet", K(ret), K(obj_meta));
    LOG_USER_WARN(OB_NOT_SUPPORTED, "type in Java UDF is");
  }
  }

  return ret;
}

int ObJavaUDFExecutor::get_java_type_to_ob_map(const ObObjMeta &obj_meta,
                                               JNIEnv &env,
                                               ObIAllocator &alloc,
                                               ObFromJavaTypeMapperBase *&functor)
{
  int ret = OB_SUCCESS;

  switch (obj_meta.get_type()) {
  case ObNullType: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of ob type", K(ret));
    break;
  }

  case ObTinyIntType:
  case ObUTinyIntType: {
    ALLOC_TYPE_MAPPER(ObFromJavaByteTypeMapper, env, alloc);
    break;
  }

  case ObSmallIntType:
  case ObUSmallIntType: {
    ALLOC_TYPE_MAPPER(ObFromJavaShortTypeMapper, env, alloc);
    break;
  }

  case ObMediumIntType:
  case ObUMediumIntType:
  case ObInt32Type:
  case ObUInt32Type: {
    ALLOC_TYPE_MAPPER(ObFromJavaIntegerTypeMapper, env, alloc);
    break;
  }

  case ObBitType:
  case ObIntType:
  case ObUInt64Type: {
    ALLOC_TYPE_MAPPER(ObFromJavaLongTypeMapper, env, alloc);
    break;
  }

  case ObFloatType:
  case ObUFloatType: {
    ALLOC_TYPE_MAPPER(ObFromJavaFloatTypeMapper, env, alloc);
    break;
  }

  case ObDoubleType:
  case ObUDoubleType: {
    ALLOC_TYPE_MAPPER(ObFromJavaDoubleTypeMapper, env, alloc);
    break;
  }

  case ObNumberType:
  case ObUNumberType:
  case ObNumberFloatType: {
    ALLOC_TYPE_MAPPER(ObFromJavaBigDecimalTypeMapper, env, alloc);
    break;
  }

  case ObVarcharType:
  case ObCharType:
  case ObHexStringType:
  case ObNVarchar2Type:
  case ObNCharType:
  case ObTinyTextType:
  case ObTextType:
  case ObMediumTextType:
  case ObLongTextType:
  case ObLobType: {
    if (CHARSET_BINARY != obj_meta.get_charset_type()) {
      ALLOC_TYPE_MAPPER(ObFromJavaStringTypeMapper, env, alloc);
    } else {
      ALLOC_TYPE_MAPPER(ObFromJavaByteBufferTypeMapper, env, alloc);
    }
    break;
  }

  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type in Java UDF is not supported yet", K(ret), K(obj_meta));
    LOG_USER_WARN(OB_NOT_SUPPORTED, "type in Java UDF is");
  }
  }

  return ret;
}

int ObJavaUDFExecutor::get_url_jar_cache(ObExternalResourceCache<ObExternalURLJar> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalURLJar>;

  Cache *result = nullptr;

  if (OB_NOT_NULL(exec_ctx_.get_external_url_resource_cache())) {
    result = static_cast<Cache*>(exec_ctx_.get_external_url_resource_cache());
  } else {
    result = static_cast<Cache *>(exec_ctx_.get_allocator().alloc(sizeof(Cache)));

    if (OB_ISNULL(result)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for jar cache", K(ret));
    } else if (OB_ISNULL(result = new (result) Cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct jar cache", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("failed to init jar cache", K(ret));

      result->~Cache();
      exec_ctx_.get_allocator().free(result);
      result = nullptr;
    } else {
      exec_ctx_.set_external_url_resource_cache(result);
    }
  }

  if (OB_SUCC(ret)) {
    cache = result;
  }

  return ret;
}

int ObJavaUDFExecutor::get_schema_jar_cache(ObExternalResourceCache<ObExternalSchemaJar> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalSchemaJar>;

  Cache *result = nullptr;

  ObSQLSessionInfo *session = nullptr;

  if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL exec_ctx_.get_my_session()", K(ret));
  } else if (OB_NOT_NULL(session->get_external_resource_schema_cache())) {
    result = static_cast<Cache*>(session->get_external_resource_schema_cache());
  } else {
    result = static_cast<Cache*>(session->get_session_allocator().alloc(sizeof(Cache)));

    if (OB_ISNULL(result)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for jar cache", K(ret));
    } else if (OB_ISNULL(result = new (result) Cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct jar cache", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("failed to init jar cache", K(ret));

      result->~Cache();
      ob_free(result);
      result = nullptr;
    } else {
      session->set_external_resource_schema_cache(result);
    }
  }

  if (OB_SUCC(ret)) {
    cache = result;
  }

  return ret;
}

} // namespace pl
} // namespace oceanbase
