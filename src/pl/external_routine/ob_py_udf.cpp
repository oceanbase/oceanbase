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

#include "ob_py_udf.h"
#include "sql/ob_spi.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/external_routine/ob_py_utils.h"


namespace oceanbase
{

namespace pl
{

int ObPyUDFExecutor::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(ObPyUtils::verify_py_valid())) {
    LOG_WARN("failed to verify py valid", K(ret));
  } else if (OB_ISNULL(exec_ctx_.get_py_sub_inter_ctx())) {
    ObPySubInterContext *sub_ctx = nullptr;
    sub_ctx = static_cast<ObPySubInterContext *>(exec_ctx_.get_allocator().alloc(sizeof(ObPySubInterContext)));
    LOG_INFO("alloc sub context", K(sub_ctx), K(ret));
    if (OB_ISNULL(sub_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sub context", K(ret));
    } else if (OB_ISNULL(sub_ctx = new (sub_ctx) ObPySubInterContext())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new sub context", K(ret));
    } else if (OB_FAIL(sub_ctx->init())) {
      LOG_WARN("failed to init sub context", K(ret));
      sub_ctx->~ObPySubInterContext();
      exec_ctx_.get_allocator().free(sub_ctx);
      sub_ctx = nullptr;
    } else {
      exec_ctx_.set_py_sub_inter_ctx(sub_ctx);
    }
  }
  return ret;
}

int ObPyUDFExecutor::execute(int64_t batch_size,
                             const ObIArray<ObObjMeta> &arg_types,
                             const ObIArray<ObIArray<ObObj> *> &args,
                             ObIAllocator &result_allocator,
                             ObIArray<ObObj> &result)
{
  int ret = OB_SUCCESS;

  using share::schema::ObExternalRoutineType;

  ObPyObject *pyfunc = nullptr;

  CK (arg_types.count() == args.count());
  CK (OB_NOT_NULL(exec_ctx_.get_my_session()));

  for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
    CK (OB_NOT_NULL(args.at(i)));
    CK (args.at(i)->count() == batch_size);
  }

  ObPySubInterContext *sub_inter_ctx = nullptr;
  ObPyThreadState *tstate = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(exec_ctx_.get_py_sub_inter_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL sub_inter_ctx", K(ret));
    } else if (OB_ISNULL(sub_inter_ctx = static_cast<ObPySubInterContext *>(exec_ctx_.get_py_sub_inter_ctx()))){
      LOG_WARN("sub_inter_ctx is NULL", K(ret));
    } else if (OB_ISNULL(tstate = sub_inter_ctx->acquire_sub_inter())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL tstate", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    switch (udf_info_.external_routine_type_) {
      case ObExternalRoutineType::EXTERNAL_PY_UDF_FROM_URL: {
        ObExternalResourceCache<ObExternalURLPy> *cache = nullptr;

        if (OB_FAIL(get_url_py_cache(cache))) {
          LOG_WARN("failed to get url py cache", K(ret));
        } else if (OB_ISNULL(cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL py cache", K(ret));
        } else if (OB_FAIL(cache->get_resource(udf_info_.udf_id_,
                                               udf_info_.external_routine_url_,
                                               pyfunc,
                                               tstate,
                                               udf_info_.external_routine_entry_,
                                               udf_info_.udf_id_))) {
          LOG_WARN("failed to get pyfunc cache", K(ret));
        } else if (OB_ISNULL(pyfunc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL pyfunc", K(ret));
        }
        break;
      }
      case ObExternalRoutineType::EXTERNAL_PY_UDF_FROM_RES: {
        ObExternalResourceCache<ObExternalSchemaPy> *cache = nullptr;
        ObSchemaGetterGuard *schema_guard = nullptr;
        uint64_t database_id;

        if (OB_FAIL(get_schema_py_cache(cache))) {
          LOG_WARN("failed to get schema py  cache", K(ret));
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
                                              pyfunc,
                                              *schema_guard,
                                              tstate,
                                              udf_info_.external_routine_entry_,
                                              udf_info_.udf_id_))) {
          LOG_WARN("failed to get pyfunc from py cache", K(ret), K(udf_info_), K(database_id));
        } else if (OB_ISNULL(pyfunc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL pyfunc", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected external routine type", K(ret), K(udf_info_));
    }
  }

  if (OB_SUCC(ret)) {
    ObPyObject *py_args = nullptr;
    ObPyObject *py_res = nullptr;
    ObPyObject *instance = nullptr;
    // ob_py_thread_state_swap(tstate);
    if (0 == ob_py_callable_check(pyfunc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pyfunc is not callable", K(ret));
    } else if (OB_FAIL(build_udf_args(*sub_inter_ctx, args, py_args))) {
      LOG_WARN("failed to build udf args", K(ret));
    } else if (OB_ISNULL(instance = ob_py_object_call_object(pyfunc, nullptr))) {
      LOG_INFO("failed to call object", K(ret));
      if (OB_FAIL(ObPyUtils::exception_check())) {
        LOG_WARN("failed to check exception", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (args.count() == 0) {
        py_res = ob_py_object_call_method(instance, "evaluate", nullptr);
      } else {
        py_res = ob_py_object_call_method(instance, "evaluate", "O", py_args);
      }
      if (OB_ISNULL(py_res)) {
        LOG_INFO("failed to evaluate", K(ret));
        if (OB_FAIL(ObPyUtils::exception_check())) {
          LOG_WARN("failed to check exception", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObObj res_obj;
      ObObj tmp_obj;
      if (OB_FAIL(ObPyUtils::ob_from_py_type(*sub_inter_ctx,
                                             result_allocator,
                                             py_res,
                                             tmp_obj,
                                             udf_info_.result_type_.get_obj_meta()))) {
        LOG_WARN("failed to convert py res to ob obj", K(ret));
      } else if (OB_FAIL(ObSPIService::spi_convert(*exec_ctx_.get_my_session(),
                                                   result_allocator,
                                                   tmp_obj,
                                                   udf_info_.result_type_,
                                                   res_obj,
                                                   false))) {
        LOG_WARN("failed to convert ob obj to res type", K(ret), K(tmp_obj), K(udf_info_.result_type_), K(res_obj));
      } else if (OB_FAIL(result.push_back(res_obj))) {
        LOG_WARN("failed to push back result", K(ret));
      }
    }
    ObPyUtils::xdec_ref(py_res);
    ObPyUtils::xdec_ref(instance);
    ObPyUtils::xdec_ref(py_args);
  }
  return ret;
}

int ObPyUDFExecutor::build_udf_args(ObPySubInterContext &ctx,
                                    const ObIArray<ObIArray<ObObj>*> &args,
                                    ObPyObject *&py_args)
{
  int ret = OB_SUCCESS;
  py_args = nullptr;
  int64_t arg_count = args.count();
  if (arg_count > 0) {
    py_args = ob_py_tuple_new(arg_count);
    if (OB_ISNULL(py_args)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for py args", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; ++i) {
      ObPyObject *o = nullptr;
      if (OB_FAIL(ObPyUtils::ob_to_py_type(ctx, args.at(i)->at(0), o))) {
        LOG_WARN("ob to py type failed", K(ret), K(args.at(i)));
      } else if (OB_ISNULL(o)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL py object", K(ret));
      } else if (-1 == ob_py_tuple_set_item(py_args, i, o)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set item", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(py_args)) {
      ObPyUtils::xdec_ref(py_args);
      py_args = nullptr;
    }
  }

  return ret;
}

int ObPyUDFExecutor::get_url_py_cache(ObExternalResourceCache<ObExternalURLPy> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalURLPy>;

  Cache *result = nullptr;

  if (OB_NOT_NULL(exec_ctx_.get_external_py_url_resource_cache())) {
    result = static_cast<Cache *>(exec_ctx_.get_external_py_url_resource_cache());
  } else {
    result = static_cast<Cache *>(exec_ctx_.get_allocator().alloc(sizeof(Cache)));

    if (OB_ISNULL(result)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for py cache", K(ret));
    } else if (OB_ISNULL(result = new (result) Cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct py cache", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("failed to init py cache", K(ret));

      result->~Cache();
      exec_ctx_.get_allocator().free(result);
      result = nullptr;
    } else {
      exec_ctx_.set_external_py_url_resource_cache(result);
    }
  }

  if (OB_SUCC(ret)) {
    cache = result;
  }

  return ret;
}

int ObPyUDFExecutor::get_schema_py_cache(ObExternalResourceCache<ObExternalSchemaPy> *&cache)
{
  int ret = OB_SUCCESS;

  using Cache = ObExternalResourceCache<ObExternalSchemaPy>;

  Cache *result = nullptr;

  if (OB_NOT_NULL(exec_ctx_.get_external_py_sch_resource_cache())) {
    result = static_cast<Cache *>(exec_ctx_.get_external_py_sch_resource_cache());
  } else {
    result = static_cast<Cache *>(exec_ctx_.get_allocator().alloc(sizeof(Cache)));

    if (OB_ISNULL(result)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for py cache", K(ret));
    } else if (OB_ISNULL(result = new (result) Cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct py cache", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("failed to init py cache", K(ret));

      result->~Cache();
      exec_ctx_.get_allocator().free(result);
      result = nullptr;
    } else {
      exec_ctx_.set_external_py_sch_resource_cache(result);
    }
  }

  if (OB_SUCC(ret)) {
    cache = result;
  }

  return ret;
}

}
} // namespace oceanbase
