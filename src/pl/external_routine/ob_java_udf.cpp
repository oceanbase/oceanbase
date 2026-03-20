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
  CK (OB_NOT_NULL(exec_ctx_.get_sql_ctx()));
  CK (OB_NOT_NULL(exec_ctx_.get_sql_ctx()->schema_guard_));

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

      if (OB_FAIL(ObJavaUtils::get_executor_class(*env, executor_clazz, execute_method))) {
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
      } else if (OB_FAIL(build_udf_args(*exec_ctx_.get_my_session(),
                                        *exec_ctx_.get_sql_ctx()->schema_guard_,
                                        batch_size,
                                        arg_types,
                                        args,
                                        *env,
                                        result_allocator,
                                        object_clazz,
                                        java_types,
                                        java_args))) {
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
                                                exec_ctx_.get_my_session()->get_server_sid(),
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

int ObJavaUDFExecutor::build_udf_args(const ObSQLSessionInfo &session,
                                      ObSchemaGetterGuard &schema_guard,
                                      int64_t batch_size,
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
    ObSEArray<ObJavaUDFPermission, 8> permissions;
    ObPl__JavaUdf__BatchedArgs batched_args;
    ob_pl__java_udf__batched_args__init(&batched_args);
    batched_args.batch_size = batch_size;

    if (OB_FAIL(ObJavaUDFPermission::collect_enabled_permissions(session, schema_guard, permissions))) {
      LOG_WARN("failed to collect enabled permissions", K(ret), K(permissions));
    } else if (OB_FAIL(ObJavaUDFPermission::permissions_to_protobuf(permissions, alloc, batched_args.n_permissions, batched_args.permissions))) {
      LOG_WARN("failed to convert permissions to protobuf", K(ret), K(permissions));
    }

    if (OB_SUCC(ret) && 0 < args.count()) {
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

  case ObRawType: {
    ALLOC_TYPE_MAPPER(ObToJavaByteBufferTypeMapper);
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
      session->get_session_allocator().free(result);
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

int ObOraJavaRoutineInfo::parse_class_method_name(ObIAllocator &alloc,
                                                  const char *ptr,
                                                  int64_t paren_pos,
                                                  ObString &class_name,
                                                  ObString &method_name)
{
  int ret = OB_SUCCESS;

  int64_t dot_pos = -1;
  for (int64_t i = paren_pos - 1; i >= 0; --i) {
    if (ptr[i] == '.') {
      dot_pos = i;
      break;
    }
  }

  if (dot_pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid java routine entry, missing '.' for class.method separator", K(ret));
  } else if (dot_pos == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid java routine entry, class name is empty", K(ret));
  } else if (dot_pos + 1 >= paren_pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid java routine entry, method name is empty", K(ret));
  } else {
    ObString tmp_class_name(static_cast<int32_t>(dot_pos), ptr);
    ObString tmp_method_name(static_cast<int32_t>(paren_pos - dot_pos - 1), ptr + dot_pos + 1);

    if (OB_FAIL(ob_write_string(alloc, tmp_class_name, class_name, true))) {
      LOG_WARN("failed to write class name", K(ret), K(tmp_class_name), K(class_name));
    } else if (OB_FAIL(ob_write_string(alloc, tmp_method_name, method_name, true))) {
      LOG_WARN("failed to write method name", K(ret), K(tmp_method_name), K(method_name));
    }
  }

  return ret;
}

int ObOraJavaRoutineInfo::parse_param_types(ObIAllocator &alloc,
                                            const char *ptr,
                                            int64_t paren_pos,
                                            int64_t close_paren_pos,
                                            ObIArray<ObString> &params_types)
{
  int ret = OB_SUCCESS;

  int64_t params_start = paren_pos + 1;
  int64_t params_len = close_paren_pos - params_start;

  if (params_len > 0) {
    int64_t param_start = params_start;
    for (int64_t i = params_start; OB_SUCC(ret) && i <= close_paren_pos; ++i) {
      if (i == close_paren_pos || ptr[i] == ',') {
        ObString param_type_raw(static_cast<int32_t>(i - param_start), ptr + param_start);
        ObString param_type = param_type_raw.trim();

        if (param_type.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid java routine entry, param type is empty", K(ret), K(param_type), K(ObString(params_len, ptr + params_start)));
        } else {
          ObString buffer;

          if (OB_FAIL(ob_write_string(alloc, param_type, buffer, true))) {
            LOG_WARN("failed to write param type", K(ret), K(param_type), K(buffer));
          } else if (OB_FAIL(params_types.push_back(buffer))) {
            LOG_WARN("failed to push back param type", K(ret), K(buffer));
          } else {
            param_start = i + 1;
          }
        }
      }
    }
  }

  return ret;
}

int ObOraJavaRoutineInfo::parse_return_type(ObIAllocator &alloc,
                                            const char *ptr,
                                            int64_t len,
                                            int64_t close_paren_pos,
                                            ObString &return_type)
{
  int ret = OB_SUCCESS;

  const ObString return_keyword("return ");

  ObString remaining_raw(static_cast<int32_t>(len - close_paren_pos - 1), ptr + close_paren_pos + 1);
  ObString remaining = remaining_raw.trim();

  if (!remaining.empty()) {
    if (!remaining.prefix_match(return_keyword)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid java routine entry, unexpected content after ')', expected 'return' keyword", K(ret), K(remaining));
    } else {
      ObString return_type_raw(static_cast<int32_t>(remaining.length() - return_keyword.length()),
                               remaining.ptr() + return_keyword.length());
      ObString tmp_return_type = return_type_raw.trim();

      if (tmp_return_type.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid java routine entry, return type is empty after 'return' keyword", K(ret), K(remaining));
      } else if (OB_FAIL(ob_write_string(alloc, tmp_return_type, return_type, true))) {
        LOG_WARN("failed to write return type", K(ret), K(tmp_return_type), K(return_type));
      }
    }
  }

  return ret;
}

int ObOraJavaRoutineInfo::parse_java_routine_info(ObIAllocator &alloc, const ObString &entry, ObOraJavaRoutineInfo &info)
{
  int ret = OB_SUCCESS;

  if (entry.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entry is empty", K(ret), K(lbt()));
  } else {
    const char *ptr = entry.ptr();
    int64_t len = entry.length();

    int64_t paren_pos = -1;
    for (int64_t i = 0; i < len; ++i) {
      if (ptr[i] == '(') {
        paren_pos = i;
        break;
      }
    }

    if (paren_pos < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid java routine entry, missing '('", K(ret), K(entry));
    } else {
      int64_t close_paren_pos = -1;
      for (int64_t i = len - 1; i > paren_pos; --i) {
        if (ptr[i] == ')') {
          close_paren_pos = i;
          break;
        }
      }

      if (close_paren_pos < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid java routine entry, missing ')'", K(ret), K(entry));
      } else {
        if (OB_FAIL(parse_class_method_name(alloc, ptr, paren_pos, info.class_name_, info.method_name_))) {
          LOG_WARN("failed to parse class and method name", K(ret), K(entry));
        } else if (OB_FAIL(parse_param_types(alloc, ptr, paren_pos, close_paren_pos, info.params_types_))) {
          LOG_WARN("failed to parse param types", K(ret), K(entry));
        } else if (OB_FAIL(parse_return_type(alloc, ptr, len, close_paren_pos, info.return_type_))) {
          LOG_WARN("failed to parse return type", K(ret), K(entry));
        } else if (!info.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid java routine entry", K(ret), K(entry), K(info));
        }
      }
    }
  }

  return ret;
}

ObOraJavaSessionState::~ObOraJavaSessionState()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(session_state_)) {
    JNIEnv *env = nullptr;

    if (OB_FAIL(ObJniConnector::java_env_init())) {
      LOG_WARN("failed to init java env", K(ret));
    } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (OB_ISNULL(env)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL jni env", K(ret));
    } else {
      ObJavaUtils::delete_global_ref(session_state_, env);
      session_state_ = nullptr;
    }
  }

  LOG_INFO("ObOraJavaSessionState destroyed", K(ret));
}

int ObOraJavaSessionState::init_session_state(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(session.get_ora_java_session_state())) {
    // do nothing
  } else {
    ObOraJavaSessionState *state = static_cast<ObOraJavaSessionState *>(session.get_session_allocator().alloc(sizeof(ObOraJavaSessionState)));

    if (OB_ISNULL(state)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for session state", K(ret));
    } else if (OB_ISNULL(state = new (state) ObOraJavaSessionState(session))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct session state", K(ret));
    } else if (OB_FAIL(state->init())) {
      state->~ObOraJavaSessionState();
      session.get_session_allocator().free(state);
      state = nullptr;
      LOG_WARN("failed to init java session state", K(ret));
    } else {
      session.set_ora_java_session_state(state);
    }
  }

  return ret;
}

int ObOraJavaSessionState::init()
{
  int ret = OB_SUCCESS;

  static char fetch_class_name[] = "fetchClass";
  static char fetch_class_sig[] = "(J[B)[B";

  static char check_class_obsolete_name[] = "checkClassObsolete";
  static char check_class_obsolete_sig[] = "(J[BJ)Z";

  static const JNINativeMethod jni_methods[] = {
    {fetch_class_name, fetch_class_sig, reinterpret_cast<void*>(&ObOraJavaRoutineExecutor::jni_fetch_class)},
    {check_class_obsolete_name, check_class_obsolete_sig, reinterpret_cast<void*>(&ObOraJavaRoutineExecutor::jni_check_class_obsolete)},
  };

  JNIEnv *env = nullptr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(jar_cache_.create(1024, ObMemAttr(MTL_ID(), "PlJarCache")))) {
    LOG_WARN("failed to create jar cache", K(ret));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  } else {
    jint jni_ret = JNI_OK;

    jobject loader = nullptr;
    jmethodID load_class_method = nullptr;
    jstring class_name = nullptr;
    jclass class_obj = nullptr;
    jmethodID state_init = nullptr;
    jobject state_obj = nullptr;
    jobject global_state_obj = nullptr;

    if (OB_FAIL(ObJavaUtils::get_executor_class_loader(*env, loader, load_class_method))) {
     LOG_WARN("failed to get_executor_class_loader", K(ret), K(loader), K(load_class_method));
    } else if (OB_ISNULL(loader) || OB_ISNULL(load_class_method)) {
      ret = OB_ERR_UNEXPECTED;
     LOG_WARN("unexpected NULL executor class loader or load class method", K(ret), K(loader), K(load_class_method));
    } else if (OB_ISNULL(class_name = env->NewStringUTF("com.oceanbase.internal.ObOraSessionState"))) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL string", K(ret));
    } else if (FALSE_IT(class_obj = static_cast<jclass>(env->CallObjectMethod(loader, load_class_method, class_name)))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
     LOG_WARN("failed to create call loadClass of ObJavaUDFExecutorClassLoader", K(ret));
    } else if (OB_ISNULL(class_obj)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL ObOraSessionState class", K(ret));
    } else if (FALSE_IT(jni_ret = env->RegisterNatives(class_obj, jni_methods, ARRAYSIZEOF(jni_methods)))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to register native methods of ObOraSessionState", K(ret), K(jni_ret));
    } else if (JNI_OK != jni_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected JNI return value", K(ret), K(jni_ret));
    } else if (FALSE_IT(state_init = env->GetMethodID(class_obj, "<init>", "(J)V"))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
     LOG_WARN("failed to get methodID of ObOraSessionState", K(ret));
    } else if (OB_ISNULL(state_init)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL state_init", K(ret));
    } else if (FALSE_IT(session_state_execute_method_ =
                            env->GetMethodID(class_obj,
                                             "execute",
                                             "("
                                             "J"
                                             "Ljava/lang/String;"
                                             "Ljava/lang/String;"
                                             "J"
                                             "[Ljava/lang/Object;"
                                             "[Ljava/lang/Object;"
                                             "Ljava/nio/ByteBuffer;"
                                             "Ljava/lang/Object;"
                                             ")"
                                             "Ljava/nio/ByteBuffer;"))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
     LOG_WARN("failed to get methodID of execute method of ObOraSessionState", K(ret));
    } else if (OB_ISNULL(session_state_execute_method_)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL session_state_execute_method_", K(ret));
    } else if (FALSE_IT(state_obj = env->NewObject(class_obj, state_init, static_cast<jlong>(session_.get_server_sid())))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
     LOG_WARN("failed to create new ObOraSessionState", K(ret));
    } else if (OB_ISNULL(state_obj)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL state_obj", K(ret));
    } else if (OB_ISNULL(session_state_ = env->NewGlobalRef(state_obj))) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,"unexpected NULL NewGlobalRef", K(ret));
    } else {
      is_inited_ = true;
    }

    ObJavaUtils::delete_local_ref(class_name, env);
    ObJavaUtils::delete_local_ref(class_obj, env);
    ObJavaUtils::delete_local_ref(state_obj, env);
  }

  return ret;
}

int ObOraJavaRoutineExecutor::is_valid() const
{
  int ret = OB_SUCCESS;

  if (ObExternalRoutineType::EXTERNAL_ORACLE_JAVA_ROUTINE != func_.get_external_routine_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("routine type is not external oracle java routine", K(ret), K(func_));
  } else if (OB_ISNULL(func_.get_ora_java_routine_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ora java routine info is null", K(ret), K(func_));
  } else if (!func_.get_ora_java_routine_info()->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid java routine entry", K(ret), K(func_));
  } else if (OB_ISNULL(ctx_.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL exec_ctx_", K(ret));
  } else if (OB_ISNULL(ctx_.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL my_session", K(ret));
  } else if (OB_ISNULL(ctx_.exec_ctx_->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL sql_ctx", K(ret));
  } else if (OB_ISNULL(ctx_.exec_ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema_guard", K(ret));
  } else if (OB_ISNULL(ctx_.result_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL result_allocator", K(ret));
  }

  return ret;
}

int ObOraJavaRoutineExecutor::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(is_valid())) {
    LOG_WARN("invalid ObOraJavaRoutineExecutor", K(ret), K(lbt()));
  } else if (OB_FAIL(ObOraJavaSessionState::init_session_state(*ctx_.exec_ctx_->get_my_session()))) {
    LOG_WARN("failed to init java session state", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObOraJavaRoutineExecutor::execute(int64_t argc, int64_t *argv)
{
  int ret = OB_SUCCESS;

  ObOraJavaSessionState *session_state = nullptr;
  JNIEnv *env = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOraJavaRoutineExecutor not init", K(ret));
  } else if (OB_FAIL(is_valid())) {
    LOG_WARN("invalid ObOraJavaRoutineExecutor", K(ret), K(lbt()));
  } else if (OB_ISNULL(session_state = ctx_.exec_ctx_->get_my_session()->get_ora_java_session_state())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ora java session state", K(ret), K(lbt()));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  } else {
    const ObOraJavaRoutineInfo &info = *func_.get_ora_java_routine_info();
    jlong executor_ptr = reinterpret_cast<jlong>(this);
    jstring class_name = nullptr;
    jstring method_name = nullptr;
    jobjectArray java_types = nullptr;
    jobjectArray java_type_names = nullptr;
    jobject java_args = nullptr;
    jclass object_clazz = nullptr;
    ObSEArray<ObObjMeta, 8> arg_types;
    ObSEArray<ObIArray<ObObj>*, 8> args;

    if (OB_ISNULL(class_name = env->NewStringUTF(info.class_name_.ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL class_name", K(ret), K(info));
    } else if (OB_ISNULL(method_name = env->NewStringUTF(info.method_name_.ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL method_name", K(ret), K(info));
    } else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "java/lang/Object", object_clazz))) {
      LOG_WARN("failed to find java/lang/Object class", K(ret));
    } else if (OB_FAIL(build_udf_arg_arrays(*env, ctx_.local_expr_alloc_, func_, argc, argv, arg_types, args, object_clazz, java_type_names))) {
      LOG_WARN("failed to build udf arg arrays", K(ret));
    } else if (OB_FAIL(ObJavaUDFExecutor::build_udf_args(*ctx_.exec_ctx_->get_my_session(),
                                                         *ctx_.exec_ctx_->get_sql_ctx()->schema_guard_,
                                                         1,
                                                         arg_types,
                                                         args,
                                                         *env,
                                                         ctx_.local_expr_alloc_,
                                                         object_clazz,
                                                         java_types,
                                                         java_args))) {
      LOG_WARN("failed to build udf args", K(ret));
    } else {
      ObFromJavaTypeMapperBase *result_mapper = nullptr;
      jobject res_java_type = nullptr;

      if (func_.is_function()) {
        const ObPLDataType &pl_type = func_.get_ret_type();
        ObObjMeta meta;
        ObExprResType res_type;

        if (pl_type.is_obj_type()) {
          meta = pl_type.get_data_type()->get_meta_type();
          meta.set_scale(meta.is_bit() ? pl_type.get_data_type()->get_accuracy().get_precision()
                                       : pl_type.get_data_type()->get_accuracy().get_scale());
          res_type.set_meta(meta);
          res_type.set_accuracy(pl_type.get_data_type()->get_accuracy());
        } else {
          meta.set_ext();
          res_type.set_meta(meta);
          res_type.set_extend_type(pl_type.get_type());
          res_type.set_udt_id(pl_type.get_user_type_id());
        }

        if (OB_FAIL(ObJavaUDFExecutor::get_java_type_to_ob_map(res_type, *env, *ctx_.result_allocator_, 1, *ctx_.exec_ctx_->get_my_session(), result_mapper))) {
          LOG_WARN("failed to get_result_mapper", K(ret));
        } else if (OB_ISNULL(result_mapper)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper", K(ret));
        } else if (OB_ISNULL(res_java_type = result_mapper->get_java_type_class())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result_mapper java type class", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t java_start_ts = ObTimeUtil::current_time();
        int64_t timeout_ts = std::min(ctx_.exec_ctx_->get_my_session()->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());

        jobject java_result = env->CallObjectMethod(session_state->session_state_,
                                                    session_state->session_state_execute_method_,
                                                    executor_ptr,
                                                    class_name,
                                                    method_name,
                                                    timeout_ts,
                                                    java_types,
                                                    java_type_names,
                                                    java_args,
                                                    res_java_type);

        if (OB_FAIL(ObJavaUtils::exception_check(env))) {
          if (OB_NOT_NULL(ctx_.status_) && OB_SUCCESS != *ctx_.status_) {
            ret = *ctx_.status_;
          }
          LOG_WARN("failed to call execute method of ObOraSessionState", K(ret));
        } else if (OB_NOT_NULL(result_mapper)) {
          if (OB_FAIL(handle_function_result(env, java_result, result_mapper))) {
            LOG_WARN("failed to handle function result", K(ret));
          }
        }

        ObJavaUtils::delete_local_ref(java_result, env);
      }

      if (OB_NOT_NULL(result_mapper)) {
        result_mapper->~ObFromJavaTypeMapperBase();
        result_mapper = nullptr;
      }
    }

    ObJavaUtils::delete_local_ref(class_name, env);
    ObJavaUtils::delete_local_ref(method_name, env);
    ObJavaUtils::delete_local_ref(java_types, env);
    ObJavaUtils::delete_local_ref(java_type_names, env);
    ObJavaUtils::delete_local_ref(java_args, env);
  }

  LOG_INFO("finished to ObOraJavaRoutineExecutor::execute", K(ret), KPC(func_.get_ora_java_routine_info()));

  return ret;
}

jbyteArray ObOraJavaRoutineExecutor::jni_fetch_class(JNIEnv *env, jobject session_state, jlong executor_ptr, jbyteArray class_name)
{
  int ret = OB_SUCCESS;

  ObOraJavaRoutineExecutor *executor = reinterpret_cast<ObOraJavaRoutineExecutor*>(executor_ptr);

  jbyteArray java_content = nullptr;

  if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  } else if (OB_ISNULL(session_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL session_state", K(ret));
  } else if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL executor", K(ret), K(executor_ptr), KPC(executor));
  } else if (OB_FAIL(executor->is_valid())) {
    LOG_WARN("invalid ObOraJavaRoutineExecutor", K(ret), KPC(executor));
  } else if (OB_ISNULL(executor->ctx_.exec_ctx_->get_my_session()->get_ora_java_session_state())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL java session state", K(ret));
  } else if (OB_ISNULL(class_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class_name", K(ret));
  } else {
    ObSchemaGetterGuard &schema_guard = *executor->ctx_.exec_ctx_->get_sql_ctx()->schema_guard_;
    ObSQLSessionInfo &session = *executor->ctx_.exec_ctx_->get_my_session();
    ObOraJavaSessionState &state = *session.get_ora_java_session_state();
    jsize class_name_length = env->GetArrayLength(class_name);
    jbyte *class_name_ptr = env->GetByteArrayElements(class_name, nullptr);
    ObString class_name_str(class_name_length, reinterpret_cast<const char*>(class_name_ptr));

    const ObSimpleExternalResourceSchema *class_schema = nullptr;
    ObSqlString sql;
    ObMySQLProxy *sql_proxy = nullptr;

    ObObj obj;
    int64_t jar_id = OB_INVALID_ID;

    if (OB_FAIL(schema_guard.get_external_resource_schema(MTL_ID(), session.get_database_id(), class_name_str, class_schema))) {
      LOG_WARN("failed to get external resource schema", K(ret), K(session.get_database_id()), K(class_name_str));
    } else if (OB_ISNULL(class_schema)) {
      // ClassNotFound
    } else if (ObSimpleExternalResourceSchema::JAVA_ORACLE_JAR_CLASS_TYPE != class_schema->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected external resource schema type", K(ret), KPC(class_schema));
    } else if (OB_ISNULL(sql_proxy = executor->ctx_.exec_ctx_->get_sql_proxy())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL sql proxy", K(ret));
    } else if (OB_FAIL(sql.append_fmt(
              "SELECT resource_id, content FROM %s WHERE tenant_id = 0 AND resource_id = (SELECT content FROM %s WHERE tenant_id = 0 AND resource_id = %lu AND schema_version = %ld)",
              OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
              OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
              class_schema->get_resource_id(),
              class_schema->get_schema_version()))) {
        LOG_WARN("failed to append sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = nullptr;

        if (OB_FAIL(sql_proxy->read(res, MTL_ID(), sql.ptr()))) {
          LOG_WARN("failed to read sql", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL result", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to read result", K(ret), K(sql));
        } else if (OB_FAIL(result->get_int("resource_id", jar_id))) {
          LOG_WARN("failed to get resource id from result", K(ret), K(sql));
        } else if (OB_FAIL(result->get_obj("content", obj))) {
          LOG_WARN("failed to get obj from result", K(ret), K(sql), K(obj));
        } else {
          ObOraJavaSessionState::JarCacheKey jar_key(jar_id, class_name_str);
          ObString content;

          if (OB_FAIL(state.jar_cache_.get_refactored(jar_key, content))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;

              ObArenaAllocator tmp_alloc(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_ARENA)));
              ObString jar_binary;

              if (obj.is_lob_storage()) {
                if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, obj, jar_binary))) {
                  LOG_WARN("failed to read_real_string_data", K(ret), K(obj), K(jar_binary));
                }
              } else {
                if (OB_FAIL(obj.get_string(jar_binary))) {
                  LOG_WARN("failed to get_string", K(ret), K(obj), K(jar_binary));
                }
              }

              if (OB_SUCC(ret)) {
                ObSEArray<std::pair<ObString, ObString>, 8> classes;
                jobject buffer_handle = nullptr;

                if (OB_FAIL(ObJavaUtils::trans_jar_to_classes(jar_binary, classes, *env, buffer_handle))) {
                  LOG_WARN("failed to trans_jar_to_classes", K(ret));
                } else if (OB_ISNULL(buffer_handle) || classes.empty()) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected NULL buffer_handle or classes is empty", K(ret), K(buffer_handle), K(classes));
                } else {
                  for (int64_t i = 0; OB_SUCC(ret) && i < classes.count(); ++i) {
                    ObString class_name;
                    ObString class_binary;

                    if (OB_FAIL(ob_write_string(state.alloc_, classes.at(i).first, class_name))) {
                      LOG_WARN("failed to write class name", K(ret), K(classes.at(i).first));
                    } else if (OB_FAIL(ob_write_string(state.alloc_, classes.at(i).second, class_binary))) {
                      LOG_WARN("failed to write class binary", K(ret), K(classes.at(i).second));
                    } else if (OB_FAIL(state.jar_cache_.set_refactored(ObOraJavaSessionState::JarCacheKey(jar_id, class_name), class_binary))) {
                      LOG_WARN("failed to set refactored to jar cache", K(ret), K(jar_id), K(class_name));
                    } else if (class_name_str == class_name) {
                      content = class_binary;
                    }
                  }
                }

                ObJavaUtils::delete_local_ref(buffer_handle, env);
              }
            } else {
              LOG_WARN("failed to get refactored from jar cache", K(ret), K(jar_key));
            }
          } else {
            LOG_INFO("[to hyy] success to get jar cache from session state", K(ret), K(jar_key));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_UNLIKELY(content.empty())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected empty content", K(ret), K(jar_key));
          } else {
            java_content = env->NewByteArray(sizeof(uint64_t) + content.length());
            jbyte *buffer = nullptr;

            if (OB_FAIL(ObJavaUtils::exception_check(env))) {
              LOG_WARN("failed to new byte array", K(ret));
            } else if (OB_ISNULL(java_content)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected NULL java_content", K(ret));
            } else if (OB_ISNULL(buffer = env->GetByteArrayElements(java_content, nullptr))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected NULL buffer", K(ret));
            } else {
              uint64_t java_class_id = htonll(class_schema->get_resource_id());
              MEMCPY(buffer, &java_class_id, sizeof(java_class_id));
              MEMCPY(buffer + sizeof(java_class_id), content.ptr(), content.length());
              env->ReleaseByteArrayElements(java_content, buffer, 0);
              buffer = nullptr;
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to read result", K(ret), K(sql));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected row count > 1 in result set", K(ret), K(sql));
            }
          }
        }
      }
    }

    LOG_INFO("finished to ObOraJavaRoutineExecutor::jni_fetch_class",
             K(ret),
             KPC(executor),
             K(class_name_str),
             K(java_content),
             K(jar_id),
             KPC(class_schema),
             K(state.jar_cache_.size()));

    env->ReleaseByteArrayElements(class_name, class_name_ptr, JNI_ABORT);

    if (OB_FAIL(ret) && OB_NOT_NULL(executor->ctx_.status_)) {
      *executor->ctx_.status_ = ret;
    }
  }

  return java_content;
}

jboolean ObOraJavaRoutineExecutor::jni_check_class_obsolete(JNIEnv *env, jobject session_state, jlong executor_ptr, jbyteArray class_name, jlong java_class_id)
{
  int ret = OB_SUCCESS;
  jboolean is_obsolete = JNI_TRUE;

  ObOraJavaRoutineExecutor *executor = reinterpret_cast<ObOraJavaRoutineExecutor*>(executor_ptr);

  jbyteArray java_content = nullptr;

  if (OB_ISNULL(env)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL jni env", K(ret));
  } else if (OB_ISNULL(session_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL session_state", K(ret));
  } else if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL executor", K(ret), K(executor_ptr), KPC(executor));
  } else if (OB_FAIL(executor->is_valid())) {
    LOG_WARN("invalid ObOraJavaRoutineExecutor", K(ret), KPC(executor));
  } else if (OB_ISNULL(class_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class_name", K(ret));
  } else {
    ObSchemaGetterGuard &schema_guard = *executor->ctx_.exec_ctx_->get_sql_ctx()->schema_guard_;
    ObSQLSessionInfo &session = *executor->ctx_.exec_ctx_->get_my_session();
    jsize class_name_length = env->GetArrayLength(class_name);
    jbyte *class_name_ptr = env->GetByteArrayElements(class_name, nullptr);
    ObString class_name_str(class_name_length, reinterpret_cast<const char *>(class_name_ptr));

    const ObSimpleExternalResourceSchema *class_schema = nullptr;

    if (OB_FAIL(schema_guard.get_external_resource_schema(MTL_ID(), session.get_database_id(), class_name_str, class_schema))) {
      LOG_WARN("failed to get external resource schema", K(ret), K(session.get_database_id()), K(class_name_str));
    } else if (OB_ISNULL(class_schema)) {
      // ClassNotFound
    } else if (ObSimpleExternalResourceSchema::JAVA_ORACLE_JAR_CLASS_TYPE != class_schema->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected external resource schema type", K(ret), KPC(class_schema));
    } else if (class_schema->get_resource_id() == java_class_id) {
      is_obsolete = JNI_FALSE;
    }

    LOG_INFO("finished to ObOraJavaRoutineExecutor::jni_check_class_obsolete",
             K(ret), KPC(executor), K(class_name_str), K(java_class_id), K(is_obsolete));

    env->ReleaseByteArrayElements(class_name, class_name_ptr, JNI_ABORT);

    if (OB_SUCC(ret) && JNI_TRUE == is_obsolete) {
      ret = OB_ERR_JAVA_SESSION_STATE_CHANGED;
      LOG_WARN("class has changed, Java session state cleared", K(ret), K(class_name_str), KPC(class_schema));
      LOG_USER_ERROR(OB_ERR_JAVA_SESSION_STATE_CHANGED, class_name_str.length(), class_name_str.ptr());
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(executor->ctx_.status_)) {
      *executor->ctx_.status_ = ret;
    }
  }

  return is_obsolete;
}

int ObOraJavaRoutineExecutor::handle_function_result(JNIEnv *env, jobject java_result, ObFromJavaTypeMapperBase *result_mapper)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(env));
  CK (OB_NOT_NULL(ctx_.result_));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(java_result)) {
    ctx_.result_->set_null();
  } else {
    void *buffer = nullptr;
    int64_t buffer_size = OB_INVALID_SIZE;

    if (OB_ISNULL(buffer = env->GetDirectBufferAddress(java_result))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GetDirectBufferAddress", K(ret));
    } else if (0 > (buffer_size = env->GetDirectBufferCapacity(java_result))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GetDirectBufferCapacity", K(ret), K(buffer_size));
    } else {
      ProtobufCAllocator c_alloc = {ObJavaUtils::protobuf_c_allocator_alloc,
                                    ObJavaUtils::protobuf_c_allocator_free,
                                    ctx_.result_allocator_};
      ObPl__JavaUdf__Values *values = ob_pl__java_udf__values__unpack(&c_alloc, buffer_size, static_cast<uint8_t *>(buffer));
      ObSEArray<ObObj, 1> result;

      if (OB_ISNULL(values)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN(
            "failed to allocate memory for ob_pl__java_udf__values__unpack", K(ret), K(buffer));
      } else if (OB_FAIL((*result_mapper)(*values, result))) {
        LOG_WARN("failed to map java value to ob value", K(ret), K(result));
      } else if (1 != result.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result count", K(ret), K(result));
      } else {
        *ctx_.result_ = result.at(0);
      }

      if (OB_NOT_NULL(values)) {
        ob_pl__java_udf__values__free_unpacked(values, &c_alloc);
      }
    }
  }

  return ret;
}

int ObOraJavaRoutineExecutor::build_udf_arg_arrays(JNIEnv &env,
                                                   ObIAllocator &alloc,
                                                   const ObPLFunction &func,
                                                   int64_t argc,
                                                   int64_t *argv,
                                                   ObIArray<ObObjMeta> &arg_types,
                                                   ObIArray<ObIArray<ObObj> *> &args,
                                                   jclass object_clazz,
                                                   jobjectArray &java_type_names)
{
  int ret = OB_SUCCESS;

  const ObOraJavaRoutineInfo *info_ptr = nullptr;
  const ObIArray<ObPLDataType> &variables = func.get_variables();

  CK (OB_NOT_NULL(info_ptr = func.get_ora_java_routine_info()));
  CK (OB_NOT_NULL(object_clazz));

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    const ObOraJavaRoutineInfo &info = *info_ptr;

    CK (argc == variables.count());
    CK (argc == info.params_types_.count());

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(java_type_names = env.NewObjectArray(argc, object_clazz, nullptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create java type names array", K(ret), K(argc));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < argc; ++i) {
      const ObPLDataType &pl_type = variables.at(i);
      const ObObjMeta *meta = pl_type.get_meta_type();

      if (OB_ISNULL(meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL meta type", K(ret), K(i), K(pl_type));
      } else if (OB_FAIL(arg_types.push_back(*meta))) {
        LOG_WARN("failed to push back arg type", K(ret), K(i), K(*meta));
      } else {
        using ColumnType = ObSEArray<ObObj, 1>;
        ColumnType *arg_values = static_cast<ColumnType*>(alloc.alloc(sizeof(ColumnType)));

        if (OB_ISNULL(arg_values)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for arg values", K(ret), K(i));
        } else if (OB_ISNULL(arg_values = new (arg_values) ColumnType())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to construct ColumnType", K(ret), K(i));
        } else if (OB_UNLIKELY(0 == argv[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL argv", K(ret), K(i), K(lbt()));
        } else {
          const ObObj &param = *reinterpret_cast<ObObj *>(argv[i]);

          if (OB_FAIL(arg_values->push_back(param))) {
            LOG_WARN("failed to push back arg value", K(ret), K(i), K(param));
          } else if (OB_FAIL(args.push_back(arg_values))) {
            LOG_WARN("failed to push back args", K(ret), K(i));
          }
        }
      }

      if (OB_SUCC(ret)) {
        const ObString &java_type_name = info.params_types_.at(i);
        jstring j_type_name = env.NewStringUTF(java_type_name.ptr());

        if (OB_ISNULL(j_type_name)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create java type name string", K(ret), K(i), K(java_type_name));
        } else {
          env.SetObjectArrayElement(java_type_names, i, j_type_name);
          ObJavaUtils::delete_local_ref(j_type_name, &env);
        }
      }
    }
  }

  return ret;
}

int ObJavaUDFPermission::permissions_to_protobuf(
    const ObIArray<ObJavaUDFPermission> &permission_array,
    ObIAllocator &alloc,
    decltype(ObPl__JavaUdf__BatchedArgs::n_permissions) &n_permissions,
    decltype(ObPl__JavaUdf__BatchedArgs::permissions) &permissions)
{
  int ret = OB_SUCCESS;

  n_permissions = 0;
  permissions = nullptr;

  if (0 < permission_array.count()) {
    char *buffer = nullptr;

    using PermissionPtr = std::remove_pointer_t<std::remove_reference_t<decltype(permissions)>>;
    using Permission = std::remove_pointer_t<PermissionPtr>;

    int64_t permission_ptr_buffer_size = permission_array.count() * sizeof(PermissionPtr);
    permission_ptr_buffer_size = (permission_ptr_buffer_size + alignof(Permission) - 1) & ~(alignof(Permission) - 1);

    int64_t permission_buffer_size = permission_array.count() * sizeof(Permission);

    int64_t total_buffer_size = permission_ptr_buffer_size + permission_buffer_size;

    if (OB_ISNULL(buffer = static_cast<char*>(alloc.alloc(total_buffer_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for permission buffer",
               K(ret), K(total_buffer_size), K(permission_ptr_buffer_size),  K(permission_buffer_size));
    } else if (FALSE_IT(MEMSET(buffer, 0, total_buffer_size))) {
      // unreachable
    } else {
      PermissionPtr *permission_ptr_buffer = reinterpret_cast<PermissionPtr*>(buffer);
      Permission *permission_buffer = reinterpret_cast<Permission*>(buffer + permission_ptr_buffer_size);

      for (int64_t i = 0; OB_SUCC(ret) && i < permission_array.count(); ++i) {
        ob_pl__java_udf__permission__init(permission_buffer + i);
        permission_array.at(i).to_protobuf(permission_buffer[i]);
        permission_ptr_buffer[i] = permission_buffer + i;
      }

      if (OB_SUCC(ret)) {
        n_permissions = permission_array.count();
        permissions = permission_ptr_buffer;
      }
    }
  }

  return ret;
}

int ObJavaUDFPermission::collect_enabled_permissions_of_grantee(const uint64_t tenant_id,
                                                                const uint64_t grantee,
                                                                ObSchemaGetterGuard &schema_guard,
                                                                ObIArray<ObJavaUDFPermission> &permissions)
{
  int ret = OB_SUCCESS;

  ObSEArray<const ObSimpleJavaPolicySchema *, 8> java_policy_schemas;

  if (OB_FAIL(schema_guard.get_java_policy_schemas_of_grantee(tenant_id, grantee, java_policy_schemas))) {
    LOG_WARN("failed to get java policy schemas of grantee", K(ret), K(tenant_id), K(grantee));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < java_policy_schemas.count(); ++i) {
      const ObSimpleJavaPolicySchema *java_policy_schema = java_policy_schemas.at(i);
      if (OB_ISNULL(java_policy_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL java policy schema", K(ret), K(i));
      } else if (!java_policy_schema->is_enabled()) {
        // do nothing
      } else {
        ObJavaUDFPermission permission(java_policy_schema->is_grant(),
                                       java_policy_schema->get_type_name(),
                                       java_policy_schema->get_name(),
                                       java_policy_schema->get_action());

        if (OB_FAIL(permissions.push_back(permission))) {
          LOG_WARN("failed to push back permission", K(ret), K(permission));
        }
      }
    }
  }

  LOG_INFO("finished to collect grantee java policy schema", K(ret), K(tenant_id), K(grantee), K(java_policy_schemas));

  return ret;
}

int ObJavaUDFPermission::collect_enabled_permissions(const ObSQLSessionInfo &session,
                                                     ObSchemaGetterGuard &schema_guard,
                                                     ObIArray<ObJavaUDFPermission> &permissions)
{
  int ret = OB_SUCCESS;

  ObSEArray<const ObSimpleJavaPolicySchema *, 8> java_policy_schemas;
  uint64_t tenant_id = session.get_effective_tenant_id();

  if (OB_FAIL(ObJavaUDFPermission::collect_enabled_permissions_of_grantee(tenant_id, session.get_priv_user_id(), schema_guard, permissions))) {
    LOG_WARN("failed to collect enabled permissions of grantee", K(ret), K(tenant_id), K(session.get_priv_user_id()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < session.get_enable_role_ids().count(); ++i) {
      const uint64_t role_id = session.get_enable_role_ids().at(i);
      if (OB_FAIL(ObJavaUDFPermission::collect_enabled_permissions_of_grantee(tenant_id, role_id, schema_guard, permissions))) {
        LOG_WARN("failed to collect enabled permissions of grantee", K(ret), K(tenant_id), K(role_id));
      }
    }
  }

  LOG_INFO("finished to collect grantee permissions",
           K(ret), K(tenant_id), K(session.get_priv_user_id()), K(session.get_enable_role_ids()), K(permissions));

  return ret;
}

}  // namespace pl
}  // namespace oceanbase
