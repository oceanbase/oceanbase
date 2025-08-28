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
#include "pl/external_routine/proto/ob_pl_java_udf.pb-c.h"

namespace oceanbase
{

namespace pl
{

int ObJavaUDFExecutor::init(int64_t udf_id,
                            ObExternalRoutineType type,
                            const ObString &url,
                            const ObString &resource)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_class_loader(exec_ctx_, udf_id, type, url, resource, loader_))) {
    LOG_WARN("failed to get class loader", K(ret));
  } else if (OB_ISNULL(loader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class loader", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObJavaUDFExecutor::get_class_loader(ObExecContext &exec_ctx,
                                        int64_t udf_id,
                                        ObExternalRoutineType type,
                                        const ObString &url,
                                        const ObString &resource,
                                        jobject &class_loader)
{
  int ret = OB_SUCCESS;

  jobject loader = nullptr;

  class_loader = nullptr;

  if (ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_URL == type) {
    ObExternalResourceCache<ObExternalURLJar> *cache = nullptr;

    if (OB_FAIL(get_url_jar_cache(exec_ctx, cache))) {
      LOG_WARN("failed to get url jar cache", K(ret));
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL jar cache", K(ret));
    } else if (OB_FAIL(cache->get_resource(udf_id, url, loader))) {
      LOG_WARN("failed to get class loader from jar cache", K(ret));
    } else if (OB_ISNULL(loader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL class loader", K(ret));
    }
  } else if (ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_RES == type) {
    ObExternalResourceCache<ObExternalSchemaJar> *cache = nullptr;
    ObSchemaGetterGuard *schema_guard = nullptr;
    uint64_t database_id;

    if (OB_FAIL(get_schema_jar_cache(exec_ctx, cache))) {
      LOG_WARN("failed to get schema jar cache", K(ret));
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL jar cache", K(ret));
    } else if (OB_ISNULL(exec_ctx.get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL exec_ctx_.get_sql_ctx()", K(ret));
    } else if (OB_ISNULL(schema_guard = exec_ctx.get_sql_ctx()->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL exec_ctx_.get_sql_ctx()->schema_guard_", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == (database_id = exec_ctx.get_my_session()->get_database_id()))) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no db selected", K(ret));
    } else if (OB_FAIL(cache->get_resource(udf_id,
                                           std::make_pair(database_id, resource),
                                           loader,
                                           *schema_guard))) {
      LOG_WARN("failed to get class loader from jar cache", K(ret), K(udf_id), K(database_id), K(resource));
    } else if (OB_ISNULL(loader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL class loader", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected external routine type", K(ret), K(udf_id), K(type));
  }

  if (OB_SUCC(ret)) {
    class_loader = loader;
  }

  return ret;
}

int ObJavaUDFExecutor::execute(int64_t batch_size,
                               const char *method,
                               const ObIArray<ObObjMeta> &arg_types,
                               const ObIArray<ObIArray<ObObj> *> &args,
                               const ObExprResType &res_type,
                               ObIAllocator &result_allocator,
                               ObIArray<ObObj> &result)
{
  int ret = OB_SUCCESS;

  CK (is_inited_);
  CK (OB_NOT_NULL(loader_));

  CK (arg_types.count() == args.count());
  CK (OB_NOT_NULL(exec_ctx_.get_my_session()));

  for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
    CK (OB_NOT_NULL(args.at(i)));
    CK (args.at(i)->count() == batch_size);
  }

  if (OB_SUCC(ret)) {
    JNIEnv *env = nullptr;

    if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
      LOG_WARN("failed to get_jni_env", K(ret));
    } else {
      jclass executor_clazz = nullptr;
      jmethodID execute_method = nullptr;
      jclass class_loader_clazz = nullptr;
      jmethodID loader_constructor = nullptr;
      jmethodID find_class_method = nullptr;
      jstring class_name = nullptr;
      jclass udf_clazz = nullptr;
      jmethodID udf_obj_constructor = nullptr;
      jobject udf_obj = nullptr;
      jstring method_name = nullptr;
      jclass object_clazz =nullptr;
      jobjectArray java_types = nullptr;
      jobject java_args = nullptr;
      jobject results = nullptr;
      int64_t timeout_ts = std::min(exec_ctx_.get_my_session()->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());
      void *buffer = nullptr;
      int64_t buffer_size = OB_INVALID_SIZE;

      if (OB_FAIL(get_executor_class(*env, executor_clazz, execute_method))) {
        LOG_WARN("failed to get_executor_classloader", K(ret));
      } else if (OB_ISNULL(executor_clazz) || OB_ISNULL(execute_method)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL loader or method", K(ret), K(executor_clazz), K(execute_method));
      } else if (OB_FAIL(ObJavaUtils::get_udf_loader_class(*env, class_loader_clazz, loader_constructor, find_class_method))) {
        LOG_WARN("failed to get_udf_loader_class", K(ret));
      } else if (OB_ISNULL(class_loader_clazz) || OB_ISNULL(find_class_method)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL class_loader_clazz or find_class_method", K(ret), K(class_loader_clazz), K(find_class_method));
      } else if (OB_ISNULL(class_name = env->NewStringUTF(entry_.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to NewStringUTF", K(ret), K(entry_));
      } else if (FALSE_IT(udf_clazz = static_cast<jclass>(env->CallObjectMethod(loader_, find_class_method, class_name)))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
        LOG_WARN("failed to findClass of ObJavaUDFClassLoader", K(ret));
      } else if (OB_ISNULL(udf_clazz)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL udf_obj", K(ret));
      } else if (FALSE_IT(udf_obj_constructor = env->GetMethodID(udf_clazz, "<init>", "()V"))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
        LOG_WARN("failed to get udf_obj_constructor", K(ret));
      } else if (OB_ISNULL(udf_obj_constructor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL udf_obj_constructor", K(ret));
      } else if (FALSE_IT(udf_obj = env->NewObject(udf_clazz, udf_obj_constructor))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
        LOG_WARN("failed to construct udf_obj", K(ret));
      } else if (OB_ISNULL(udf_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL udf_obj", K(ret));
      } else if (OB_ISNULL(method_name = env->NewStringUTF(method))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to NewStringUTF execute", K(ret));
      } else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "java/lang/Object", object_clazz))) {
        LOG_WARN("failed to find java/lang/Object class", K(ret));
      } else if (OB_FAIL(build_udf_args(batch_size, arg_types, args, *env, result_allocator, object_clazz, java_types, java_args))) {
        LOG_WARN("failed to build udf args", K(ret));
      } else {
        ObFromJavaTypeMapperBase *result_mapper = nullptr;

        if (OB_FAIL(get_java_type_to_ob_map(res_type, *env, result_allocator, batch_size, *exec_ctx_.get_my_session(), result_mapper))) {
          LOG_WARN("failed to get_result_mapper", K(ret));
        } else if (OB_ISNULL(result_mapper)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper", K(ret));
        } else if (OB_ISNULL(result_mapper->get_java_type_class())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper java type class", K(ret));
        } else {
          int64_t java_start_ts = ObTimeUtil::current_time();

          results = env->CallStaticObjectMethod(executor_clazz,
                                                execute_method,
                                                udf_obj,
                                                method_name,
                                                timeout_ts,
                                                java_types,
                                                java_args,
                                                result_mapper->get_java_type_class());

          LOG_TRACE("finished to execute Java executor", "total_time", ObTimeUtil::current_time() - java_start_ts);
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
          LOG_WARN("failed to execute Java UDF", K(ret), K(entry_), K(args));
        } else if (OB_ISNULL(results)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL Java UDF results", K(ret));
        } else if (OB_ISNULL(buffer = env->GetDirectBufferAddress(results))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL GetDirectBufferAddress", K(ret));
        } else if (0 > (buffer_size = env->GetDirectBufferCapacity(results))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL GetDirectBufferCapacity", K(ret), K(buffer_size));
        }

        if (OB_SUCC(ret)) {
          ProtobufCAllocator c_alloc = {ObJavaUtils::protobuf_c_allocator_alloc,
                                        ObJavaUtils::protobuf_c_allocator_free,
                                        &result_allocator};
          ObPl__JavaUdf__Values *values = ob_pl__java_udf__values__unpack(&c_alloc, buffer_size, static_cast<uint8_t*>(buffer));

          if (OB_ISNULL(values)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for ob_pl__java_udf__values__unpack", K(ret), K(buffer));
          } else if (need_infer_result_size()) {
            result_mapper->set_batch_size(values->null_map.len);
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (0 == result_mapper->get_batch_size()) {
            // do nothing
          } else if (OB_FAIL((*result_mapper)(*values, result))) {
              LOG_WARN("failed to map java value to ob value", K(ret), K(result));
          }

          if (OB_NOT_NULL(values)) {
            ob_pl__java_udf__values__free_unpacked(values, &c_alloc);
          }
        }

        if (OB_NOT_NULL(result_mapper)) {
          result_mapper->~ObFromJavaTypeMapperBase();
          result_mapper = nullptr;
        }
      }

      // always delete local ref
      ObJavaUtils::delete_local_ref(class_name, env);
      ObJavaUtils::delete_local_ref(udf_clazz, env);
      ObJavaUtils::delete_local_ref(udf_obj, env);
      ObJavaUtils::delete_local_ref(method_name, env);
      ObJavaUtils::delete_local_ref(java_types, env);
      ObJavaUtils::delete_local_ref(java_args, env);
      ObJavaUtils::delete_local_ref(results, env);
    }
  }

  return ret;
}

int ObJavaUDFExecutor::build_udf_args(int64_t batch_size,
                                      const ObIArray<ObObjMeta> &arg_types,
                                      const ObIArray<ObIArray<ObObj> *> &args,
                                      JNIEnv &env,
                                      ObIAllocator &arg_alloc,
                                      jclass object_clazz,
                                      jobjectArray &java_types,
                                      jobject &java_args)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(java_types = env.NewObjectArray(args.count(), object_clazz, nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get udf arg types", K(ret));
  } else {
    ObArenaAllocator alloc;
    ObPl__JavaUdf__BatchedArgs batched_args;
    ob_pl__java_udf__batched_args__init(&batched_args);
    batched_args.batch_size = batch_size;

    if (0 < args.count()) {
      using Arg = std::remove_pointer_t<decltype(batched_args.args)>;
      Arg *arg_buffer = nullptr;
      int64_t arg_buffer_size = args.count() * sizeof(Arg);

      if (OB_ISNULL(arg_buffer = static_cast<Arg*>(arg_alloc.alloc(arg_buffer_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for arg buffer", K(ret), K(arg_buffer_size));
      } else {
        batched_args.args = arg_buffer;
        batched_args.n_args = args.count();
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
        ObToJavaTypeMapperBase *map_fun = nullptr;
        jobjectArray curr_column = nullptr;

        // TODO(heyongyi.hyy): cache map functors to a hash map
        if (OB_FAIL(get_ob_type_to_java_map(arg_types.at(i), batch_size, env, alloc, map_fun))) {
          LOG_WARN("failed to get type map", K(ret));
        } else if (OB_ISNULL(map_fun)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid map fun", K(ret), K(map_fun));
        } else if (OB_ISNULL(map_fun->get_java_type_class())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL java type class", K(ret), K(map_fun), K(i), K(arg_types));
        } else if (FALSE_IT(env.SetObjectArrayElement(java_types, i, map_fun->get_java_type_class()))) {
          // unreachable
        } else {
          CK (OB_NOT_NULL(args.at(i)));

          for (jsize j = 0; OB_SUCC(ret) && j < args.at(i)->count(); ++j) {
            const ObObj &ob_value = args.at(i)->at(j);

            if (arg_types.at(i).get_type() != ob_value.get_meta().get_type() && !ob_value.is_null()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected unmatch ObObjMeta", K(ret), K(i), K(arg_types.at(i)), K(ob_value));
            } else if (OB_FAIL((*map_fun)(ob_value, j))) {
              LOG_WARN("failed to map ob value to java value", K(ret), K(ob_value));
            }
          }

          if (OB_SUCC(ret)) {
            arg_buffer[i] = map_fun->get_arg_values();
          }
        }

        if (OB_NOT_NULL(map_fun)) {
          map_fun->~ObToJavaTypeMapperBase();
          alloc.free(map_fun);
          map_fun = nullptr;
        }

        ObJavaUtils::delete_local_ref(curr_column, &env);
      }
    }

    if (OB_SUCC(ret)) {
      int64_t size = ob_pl__java_udf__batched_args__get_packed_size(&batched_args);
      uint8_t *buffer = nullptr;

      if (OB_ISNULL(buffer =static_cast<decltype(buffer)>(arg_alloc.alloc(size)))){
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed allocate memory for batched args serialize", K(ret), K(size));
      } else if (OB_UNLIKELY(0 > ob_pl__java_udf__batched_args__pack(&batched_args, buffer))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected protobuf serialization failure", K(ret), K(size), K(args));
      } else if (FALSE_IT(java_args = env.NewDirectByteBuffer(buffer, size))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(&env))) {
        LOG_WARN("failed to call NewDirectByteBuffer", K(ret), K(buffer), K(size));
      } else if (OB_ISNULL(java_args)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NewDirectByteBuffer result", K(ret), K(buffer), K(size));
      }
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
    } else if (OB_ISNULL(mapper = new(mapper)MAPPER(env, alloc, batch_size, ##__VA_ARGS__))) {                         \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      LOG_WARN("failed to construct "#MAPPER, K(ret));                                                                 \
    } else if (OB_FAIL(mapper->init())) {                                                                              \
      LOG_WARN("failed to init "#MAPPER, K(ret));                                                                      \
    } else {                                                                                                           \
      functor = mapper;                                                                                                \
    }                                                                                                                  \
  } while (0)

int ObJavaUDFExecutor::get_ob_type_to_java_map(const ObObjMeta &obj_meta,
                                               int64_t batch_size,
                                               JNIEnv &env,
                                               ObIAllocator &alloc,
                                               ObToJavaTypeMapperBase *&functor)
{
  int ret = OB_SUCCESS;

  switch (obj_meta.get_type()) {
  case ObTinyIntType:
  case ObUTinyIntType: {
    ALLOC_TYPE_MAPPER(ObToJavaByteTypeMapper);
    break;
  }

  case ObSmallIntType:
  case ObUSmallIntType: {
    ALLOC_TYPE_MAPPER(ObToJavaShortTypeMapper);
    break;
  }

  case ObMediumIntType:
  case ObUMediumIntType:
  case ObInt32Type:
  case ObUInt32Type: {
    ALLOC_TYPE_MAPPER(ObToJavaIntegerTypeMapper);
    break;
  }

  case ObBitType:
  case ObIntType:
  case ObUInt64Type: {
    ALLOC_TYPE_MAPPER(ObToJavaLongTypeMapper);
    break;
  }

  case ObFloatType:
  case ObUFloatType: {
    ALLOC_TYPE_MAPPER(ObToJavaFloatTypeMapper);
    break;
  }

  case ObDoubleType:
  case ObUDoubleType: {
    ALLOC_TYPE_MAPPER(ObToJavaDoubleTypeMapper);
    break;
  }

  case ObNumberType:
  case ObUNumberType:
  case ObNumberFloatType: {
    ALLOC_TYPE_MAPPER(ObToJavaBigDecimalTypeMapper);
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
      ALLOC_TYPE_MAPPER(ObToJavaStringTypeMapper);
    } else {
      ALLOC_TYPE_MAPPER(ObToJavaByteBufferTypeMapper);
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

int ObJavaUDFExecutor::get_java_type_to_ob_map(const ObExprResType &res_type,
                                               JNIEnv &env,
                                               ObIAllocator &alloc,
                                               int64_t batch_size,
                                               ObSQLSessionInfo &session,
                                               ObFromJavaTypeMapperBase *&functor)
{
  int ret = OB_SUCCESS;

#define ALLOC_TYPE_MAPPER_EXT(MAPPER, ...) ALLOC_TYPE_MAPPER(MAPPER, res_type, session, ##__VA_ARGS__)

  switch (res_type.get_type()) {
  case ObNullType: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of ob type", K(ret));
    break;
  }

  case ObTinyIntType:
  case ObUTinyIntType: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaByteTypeMapper);
    break;
  }

  case ObSmallIntType:
  case ObUSmallIntType: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaShortTypeMapper);
    break;
  }

  case ObMediumIntType:
  case ObUMediumIntType:
  case ObInt32Type:
  case ObUInt32Type: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaIntegerTypeMapper);
    break;
  }

  case ObBitType:
  case ObIntType:
  case ObUInt64Type: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaLongTypeMapper);
    break;
  }

  case ObFloatType:
  case ObUFloatType: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaFloatTypeMapper);
    break;
  }

  case ObDoubleType:
  case ObUDoubleType: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaDoubleTypeMapper);
    break;
  }

  case ObNumberType:
  case ObUNumberType:
  case ObNumberFloatType: {
    ALLOC_TYPE_MAPPER_EXT(ObFromJavaBigDecimalTypeMapper);
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
    if (CHARSET_BINARY != res_type.get_charset_type()) {
      ALLOC_TYPE_MAPPER_EXT(ObFromJavaStringTypeMapper);
    } else {
      ALLOC_TYPE_MAPPER_EXT(ObFromJavaByteBufferTypeMapper);
    }
    break;
  }

  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type in Java UDF is not supported yet", K(ret), K(res_type));
    LOG_USER_WARN(OB_NOT_SUPPORTED, "type in Java UDF is");
  }
  }

  return ret;
}

int ObJavaUDFExecutor::get_url_jar_cache(ObExecContext &exec_ctx, ObExternalResourceCache<ObExternalURLJar> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalURLJar>;

  Cache *result = nullptr;

  if (OB_NOT_NULL(exec_ctx.get_external_url_resource_cache())) {
    result = static_cast<Cache*>(exec_ctx.get_external_url_resource_cache());
  } else {
    result = static_cast<Cache *>(exec_ctx.get_allocator().alloc(sizeof(Cache)));

    if (OB_ISNULL(result)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for jar cache", K(ret));
    } else if (OB_ISNULL(result = new (result) Cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct jar cache", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("failed to init jar cache", K(ret));

      result->~Cache();
      exec_ctx.get_allocator().free(result);
      result = nullptr;
    } else {
      exec_ctx.set_external_url_resource_cache(result);
    }
  }

  if (OB_SUCC(ret)) {
    cache = result;
  }

  return ret;
}

int ObJavaUDFExecutor::get_schema_jar_cache(ObExecContext &exec_ctx, ObExternalResourceCache<ObExternalSchemaJar> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalSchemaJar>;

  Cache *result = nullptr;

  ObSQLSessionInfo *session = nullptr;

  if (OB_ISNULL(session = exec_ctx.get_my_session())) {
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
