/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_executor_factory.h"
#include "ob_table_cg_service.h"
#include "share/datum/ob_datum_util.h"

namespace oceanbase
{
namespace table
{

template <int TYPE>
  struct AllocTableExecutorHelper;

template <int TYPE>
  struct GenTableSpecHelper;

int report_not_registered(int type)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("executor not registered", K(ret), K(type));
  return ret;
}

template <>
struct AllocTableExecutorHelper<0>
{
  static int alloc(ObIAllocator &alloc,
                   ObTableCtx &ctx,
                   const ObTableApiSpec &spec,
                   ObTableApiExecutor *&executor)
  { return report_not_registered(0); }
};

template <>
struct GenTableSpecHelper<0>
{
  static int generate(ObIAllocator &alloc,
                      const ObTableExecutorType &type,
                      ObTableCtx &ctx,
                      ObTableApiSpec *&spec)
  { return report_not_registered(0); }
};

template <int TYPE>
struct AllocTableExecutorHelper
{
  static int alloc(ObIAllocator &alloc,
                   ObTableCtx &ctx,
                   const ObTableApiSpec &spec,
                   ObTableApiExecutor *&executor)
  {
    int ret = OB_SUCCESS;
    typedef typename ObTableApiExecutorTypeTraits<TYPE>::Executor ExecutorType;
    typedef typename ObTableApiExecutorTypeTraits<TYPE>::Spec SpecType;
    void *buf = alloc.alloc(sizeof(ExecutorType));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc table api executor", K(ret));
    } else {
      executor = new(buf) ExecutorType(ctx, static_cast<const SpecType&>(spec));
    }
    return ret;
  }
};

template <int TYPE>
struct GenTableSpecHelper
{
  static int generate(ObIAllocator &alloc,
                      const ObTableExecutorType &type,
                      ObTableCtx &ctx,
                      ObTableApiSpec *&spec)
  {
    int ret = OB_SUCCESS;
    typedef typename ObTableApiExecutorTypeTraits<TYPE>::Spec SpecType;
    void *buf = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc table api spec", K(ret));
    } else {
      SpecType *spec_ptr = new(buf) SpecType(alloc, type);
      if (OB_FAIL(ObTableSpecCgService::generate_spec(alloc, ctx, *spec_ptr))) {
        LOG_WARN("fail to generate table api spec", K(ret));
        spec_ptr->~SpecType();
        alloc.free(buf);
      } else {
        spec_ptr->set_expr_frame_info(ctx.get_expr_frame_info());
        spec = spec_ptr;
      }
    }
    return ret;
  }
};

static ObTableExecutorFactory::AllocFunc G_TABLE_API_ALLOC_FUNCS_ARRAY[TABLE_API_EXEC_MAX];
static_assert(TABLE_API_EXEC_MAX == ARRAYSIZEOF(G_TABLE_API_ALLOC_FUNCS_ARRAY),
              "alloc function array is too small");

ObTableExecutorFactory::AllocFunc *ObTableExecutorFactory::G_TABLE_API_ALLOC_FUNCS_ = G_TABLE_API_ALLOC_FUNCS_ARRAY;

template <int N>
struct TableApiInitAllocFunc
{
  static void init_array()
  {
    static constexpr int registered =ObTableApiExecutorTypeTraits<N>::registered_;
    G_TABLE_API_ALLOC_FUNCS_ARRAY[N] = ObTableExecutorFactory::AllocFunc {
          (&AllocTableExecutorHelper<N * registered>::alloc),
          (&GenTableSpecHelper<N * registered>::generate)
    };
  }
};

bool G_TABLE_API_ALLOC_FUNC_SET = common::ObArrayConstIniter<TABLE_API_EXEC_MAX, TableApiInitAllocFunc>::init();

int ObTableExecutorFactory::alloc_executor(ObIAllocator &alloc,
                                           ObTableCtx &ctx,
                                           const ObTableApiSpec &spec,
                                           ObTableApiExecutor *&executor)
{
  int ret = OB_SUCCESS;
  ObTableExecutorType exec_type = spec.get_type();
  if (!is_registered(exec_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table api executor is not supported", K(ret), K(exec_type));
  } else if (OB_FAIL(G_TABLE_API_ALLOC_FUNCS_[exec_type].exec_func_(alloc, ctx, spec, executor))) {
    LOG_WARN("fail to alloc table api executor", K(ret), K(exec_type));
  } else {}

  return ret;
}

int ObTableExecutorFactory::generate_spec(ObIAllocator &alloc,
                                          const ObTableExecutorType &type,
                                          ObTableCtx &ctx,
                                          ObTableApiSpec *&spec)
{
  int ret = OB_SUCCESS;
  if (!is_registered(type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table api executor is not supported", K(ret), K(type));
  } else if (OB_FAIL(G_TABLE_API_ALLOC_FUNCS_[type].gen_spec_func_(alloc, type, ctx, spec))) {
    LOG_WARN("fail to alloc table api executor", K(ret), K(type));
  } else {}

  return ret;
}

}  // namespace table
}  // namespace oceanbase