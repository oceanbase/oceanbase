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

#include "pl/external_routine/ob_java_udaf.h"

#include "share/datum/ob_datum.h"
#include "pl/external_routine/ob_java_udf.h"

namespace oceanbase
{

namespace pl
{

int ObJavaUDAFExecutor::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_INVALID_ID == aggr_info_.pl_agg_udf_type_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid udf id", K(ret), K(aggr_info_));
  } else if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to init java env", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObJavaUDFExecutor::get_class_loader(eval_ctx_.exec_ctx_,
                                                         aggr_info_.pl_agg_udf_type_id_,
                                                         aggr_info_.external_routine_type_,
                                                         aggr_info_.external_routine_url_,
                                                         aggr_info_.external_routine_resource_,
                                                         loader_))) {
    LOG_WARN("failed to get class loader", K(ret));
  } else if (OB_ISNULL(loader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class loader", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObJavaUDAFExecutor::execute(ObDatum &result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(loader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL class loader", K(ret));
  } else if (OB_ISNULL(eval_ctx_.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL session", K(ret));
  } else if (OB_ISNULL(aggr_info_.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL expr", K(ret));
  } else {
    // common allocator for whole UDAF execution
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_ARENA)));

    // temporary allocator for each batch
    ObArenaAllocator batch_allocator(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_ARENA)));

    const int64_t arg_count = aggr_info_.pl_agg_udf_params_type_.count();
    const int64_t batch_size = std::max(eval_ctx_.get_batch_size(), 512L);

    const ObChunkDatumStore::StoredRow *stored_row = nullptr;

    using ColumnType = ObSEArray<ObObj, 512>;

    ObSEArray<ObObjMeta, 8> arg_types;
    ObSEArray<ObIArray<ObObj> *, 8> args;

    JNIEnv *env = nullptr;

    jclass executor_clazz = nullptr;
    jmethodID execute_method = nullptr;

    jclass class_loader_clazz = nullptr;
    jmethodID loader_constructor = nullptr;
    jmethodID find_class_method = nullptr;
    jclass object_clazz = nullptr;

    jstring class_name = nullptr;
    jclass udf_clazz = nullptr;
    jobject udf_obj = nullptr;
    jmethodID udf_constructor = nullptr;

    jstring iterate_name = nullptr;
    jstring terminate_name = nullptr;

    int64_t timeout_ts = eval_ctx_.exec_ctx_.get_my_session()->get_query_timeout_ts();

    ObString entry;

    for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; ++i) {
      ColumnType *column = nullptr;

      if (OB_FAIL(arg_types.push_back(aggr_info_.pl_agg_udf_params_type_.at(i)))) {
        LOG_WARN("failed to push_back", K(ret), K(i), K(aggr_info_.pl_agg_udf_params_type_));
      } else if (OB_ISNULL(column = static_cast<ColumnType*>(allocator.alloc(sizeof(ColumnType))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for column", K(ret), K(i));
      } else if (OB_ISNULL(column = new(column)ColumnType())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to construct column", K(ret));
      } else if (OB_FAIL(args.push_back(column))) {
        LOG_WARN("failed to push_back column to args", K(ret), K(i), KPC(column));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
      LOG_WARN("failed to get_jni_env", K(ret));
    } else if (OB_ISNULL(env)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL env", K(ret));
    } else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "java/lang/Object", object_clazz))) {
      LOG_WARN("failed to get_cached_class", K(ret));
    } else if (OB_ISNULL(object_clazz)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL object_clazz", K(ret));
    } else if (OB_FAIL(ObJavaUDFExecutor::get_executor_class(*env, executor_clazz, execute_method))) {
      LOG_WARN("failed to get_executor_class", K(ret));
    } else if (OB_ISNULL(executor_clazz) || OB_ISNULL(execute_method)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL executor_clazz or execute_method", K(ret), K(executor_clazz), K(execute_method));
    } else if (OB_FAIL(ObJavaUtils::get_udf_loader_class(*env, class_loader_clazz, loader_constructor, find_class_method))) {
      LOG_WARN("failed to get_udf_loader_class", K(ret));
    } else if (OB_ISNULL(class_loader_clazz) || OB_ISNULL(find_class_method)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL class_loader_clazz or find_class_method", K(ret), K(class_loader_clazz), K(find_class_method));
    } else if (OB_FAIL(ob_write_string(allocator, aggr_info_.external_routine_entry_, entry, true))) {
      LOG_WARN("failed to ob_write_string", K(ret), K(aggr_info_));
    } else if (FALSE_IT(class_name = env->NewStringUTF(entry.ptr()))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to NewStringUTF", K(ret));
    } else if (OB_ISNULL(class_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL class_name", K(ret));
    } else if (FALSE_IT(udf_clazz = static_cast<jclass>(env->CallObjectMethod(loader_, find_class_method, class_name)))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to findClass", K(ret));
    } else if (OB_ISNULL(udf_clazz)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL udf_clazz", K(ret));
    } else if (FALSE_IT(udf_constructor = env->GetMethodID(udf_clazz, "<init>", "()V"))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to <init> of udf_clazz", K(ret));
    } else if (OB_ISNULL(udf_constructor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL udf_constructor", K(ret));
    } else if (FALSE_IT(udf_obj = env->NewObject(udf_clazz, udf_constructor))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to newObject", K(ret));
    } else if (OB_ISNULL(udf_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL udf_obj", K(ret));
    } else if (FALSE_IT(iterate_name = env->NewStringUTF("iterate"))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to NewStringUTF", K(ret));
    } else if (OB_ISNULL(iterate_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL iterate_name", K(ret));
    } else if (FALSE_IT(terminate_name = env->NewStringUTF("terminate"))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
      LOG_WARN("failed to NewStringUTF", K(ret));
    } else if (OB_ISNULL(terminate_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL terminate_name", K(ret));
    }

    // iterate phase
    while (OB_SUCC(ret)) {
      int64_t row_count = 0;

      batch_allocator.reuse();

      for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; ++i) {
        args.at(i)->reuse();
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (OB_FAIL(extra_result_.get_next_row(stored_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else if (OB_ISNULL(stored_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL stored_row", K(ret));
        } else if (stored_row->cnt_ != arg_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected stored_row count", K(ret), K(stored_row->cnt_), K(arg_count));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < arg_count; ++j) {
            ObObj obj;
            if (OB_FAIL(stored_row->cells()[j].to_obj(obj, aggr_info_.param_exprs_.at(j)->obj_meta_))) {
              LOG_WARN("failed to convert to obj", K(ret), K(i), K(j), K(stored_row->cells()[j]));
            } else if (OB_FAIL(args.at(j)->push_back(obj))) {
              LOG_WARN("failed to push_back", K(ret), K(i), K(j), K(obj));
            }
          }

          if (OB_SUCC(ret)) {
            ++row_count;
          }
        }
      }

      if (OB_SUCC(ret) && row_count > 0) {
        jobjectArray java_types = nullptr;
        jobject java_args = nullptr;

        jobject result = nullptr;

        if (OB_FAIL(ObJavaUDFExecutor::build_udf_args(row_count,
                                                      arg_types,
                                                      args,
                                                      *env,
                                                      batch_allocator,
                                                      object_clazz,
                                                      java_types,
                                                      java_args))) {
          LOG_WARN("failed to build_udf_args", K(ret));
        } else if (FALSE_IT(result = env->CallStaticObjectMethod(executor_clazz,
                                                                 execute_method,
                                                                 udf_obj,
                                                                 iterate_name,
                                                                 timeout_ts,
                                                                 java_types,
                                                                 java_args,
                                                                 nullptr))) {
          // unreachable
        } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
          LOG_WARN("failed to execute Java UDAF iterate", K(ret));
        }

        ObJavaUtils::delete_local_ref(java_types, env);
        ObJavaUtils::delete_local_ref(java_args, env);
        ObJavaUtils::delete_local_ref(result, env);
      }

      if (batch_size != row_count) {
        break;
      }
    }

    // terminate phase
    if (OB_SUCC(ret)) {
      jobjectArray java_types = nullptr;
      jobject java_args = nullptr;
      jobject terminate_result = nullptr;

      ObFromJavaTypeMapperBase *result_mapper = nullptr;

      void *buffer = nullptr;
      int64_t buffer_size = OB_INVALID_SIZE;

      batch_allocator.reuse();
      arg_types.reuse();
      args.reuse();

      if (OB_FAIL(ObJavaUDFExecutor::build_udf_args(1,
                                                    arg_types,
                                                    args,
                                                    *env,
                                                    batch_allocator,
                                                    object_clazz,
                                                    java_types,
                                                    java_args))) {
        LOG_WARN("failed to build_udf_args", K(ret));
      } else if (OB_FAIL(ObJavaUDFExecutor::get_java_type_to_ob_map(aggr_info_.pl_result_type_,
                                                                    *env,
                                                                    batch_allocator,
                                                                    1,
                                                                    *eval_ctx_.exec_ctx_.get_my_session(),
                                                                    result_mapper))) {
        LOG_WARN("failed to get_java_type_to_ob_map", K(ret));
      } else if (OB_ISNULL(result_mapper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result_mapper", K(ret));
      } else if (OB_ISNULL(result_mapper->get_java_type_class())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL result_mapper java type class", K(ret));
      } else if (FALSE_IT(terminate_result = env->CallStaticObjectMethod(executor_clazz,
                                                                         execute_method,
                                                                         udf_obj,
                                                                         terminate_name,
                                                                         timeout_ts,
                                                                         java_types,
                                                                         java_args,
                                                                         result_mapper->get_java_type_class()))) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::exception_check(env))) {
        LOG_WARN("failed to execute Java UDAF terminate", K(ret));
      } else if (OB_ISNULL(terminate_result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL terminate_result", K(ret));
      } else if (OB_ISNULL(buffer = env->GetDirectBufferAddress(terminate_result))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL GetDirectBufferAddress", K(ret));
      } else if (0 > (buffer_size = env->GetDirectBufferCapacity(terminate_result))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL GetDirectBufferCapacity", K(ret), K(buffer_size));
      } else {
        ObSEArray<ObObj, 1> tmp_result;

        ProtobufCAllocator c_alloc = {ObJavaUtils::protobuf_c_allocator_alloc,
                                      ObJavaUtils::protobuf_c_allocator_free,
                                      &batch_allocator};
        ObPl__JavaUdf__Values *values = ob_pl__java_udf__values__unpack(&c_alloc, buffer_size, static_cast<uint8_t*>(buffer));

        if (OB_ISNULL(values)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for ob_pl__java_udf__values__unpack", K(ret), K(buffer));
        } else if (OB_FAIL((*result_mapper)(*values, tmp_result))) {
          LOG_WARN("failed to map java value to ob value", K(ret), K(tmp_result));
        } else if (1 != tmp_result.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tmp_result count", K(ret), K(tmp_result));
        } else if (OB_FAIL(result.from_obj(tmp_result.at(0), aggr_info_.expr_->obj_datum_map_))) {
          LOG_WARN("failed to from_obj", K(ret), K(tmp_result));
        } else if (OB_FAIL(aggr_info_.expr_->deep_copy_datum(eval_ctx_, result))) {
          LOG_WARN("failed to deep_copy_datum", K(ret), K(result));
        }

        if (OB_NOT_NULL(values)) {
          ob_pl__java_udf__values__free_unpacked(values, &c_alloc);
        }
      }

      if (OB_NOT_NULL(result_mapper)) {
        result_mapper->~ObFromJavaTypeMapperBase();
        result_mapper = nullptr;
      }

      ObJavaUtils::delete_local_ref(java_types, env);
      ObJavaUtils::delete_local_ref(java_args, env);
      ObJavaUtils::delete_local_ref(terminate_result, env);
    }

    ObJavaUtils::delete_local_ref(class_name, env);
    ObJavaUtils::delete_local_ref(udf_clazz, env);
    ObJavaUtils::delete_local_ref(udf_obj, env);
    ObJavaUtils::delete_local_ref(iterate_name, env);
    ObJavaUtils::delete_local_ref(terminate_name, env);

    for (int64_t i = 0; i < args.count(); ++i) {
      if (OB_NOT_NULL(args.at(i))) {
        args.at(i)->~ObIArray();
        args.at(i) = nullptr;
      }
    }
    args.reset();
  }

  return ret;
}

}  // namespace pl
}  // namespace oceanbase
