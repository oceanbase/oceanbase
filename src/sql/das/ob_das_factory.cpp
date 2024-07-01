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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_factory.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/das/ob_das_delete_op.h"
#include "sql/das/ob_das_update_op.h"
#include "sql/das/ob_das_lock_op.h"
#include "sql/das/ob_das_simple_op.h"
#include "sql/das/ob_das_extra_data.h"
#include "sql/das/ob_das_def_reg.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_ir_define.h"
#include "share/datum/ob_datum_util.h"

#define STORE_DAS_OBJ(obj_store, das_obj, class_name)       \
  if (OB_SUCC(ret)) {                                       \
    if (OB_FAIL(obj_store.store_obj(das_obj))) {            \
      LOG_WARN("store das object failed", K(ret));          \
      das_obj->~class_name();                               \
      das_obj = nullptr;                                    \
    }                                                       \
  }

namespace oceanbase
{
using namespace common;
namespace sql
{
template <int TYPE>
  struct AllocDASOpHelper;
template <int TYPE>
  struct AllocDASOpResultHelper;
template <int TYPE>
  struct AllocDASCtDefHelper;
template <int TYPE>
  struct AllocDASRtDefHelper;

int report_not_registered(int type)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("das op not registered", K(ret), K(type));
  return ret;
}

template <>
struct AllocDASOpHelper<0>
{
  static int alloc(ObIAllocator &, ObIDASTaskOp *&)
  { return report_not_registered(0); }
};

template <>
struct AllocDASOpResultHelper<0>
{
  static int alloc(ObIAllocator &, ObIDASTaskResult *&)
  { return report_not_registered(0); }
};

template <>
struct AllocDASCtDefHelper<0>
{
  static int alloc(ObIAllocator &, ObDASBaseCtDef *&)
  { return report_not_registered(0); }
};

template <>
struct AllocDASRtDefHelper<0>
{
  static int alloc(ObIAllocator &, ObDASBaseRtDef *&)
  { return report_not_registered(0); }
};

template <int TYPE>
struct AllocDASOpHelper
{
  static int alloc(ObIAllocator &alloc, ObIDASTaskOp *&das_op)
  {
    int ret = OB_SUCCESS;
    typedef typename das_reg::ObDASOpTypeTraits<TYPE>::DASOp OpType;
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = alloc.alloc(sizeof(OpType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate Op buffer failed", K(ret), K(sizeof(OpType)));
    } else {
      das_op = new(buffer) OpType(alloc);
    }
    return ret;
  }
};

template <int TYPE>
struct AllocDASOpResultHelper
{
  static int alloc(ObIAllocator &alloc, ObIDASTaskResult *&op_result)
  {
    int ret = OB_SUCCESS;
    typedef typename das_reg::ObDASOpTypeTraits<TYPE>::DASOpResult OpResultType;
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = alloc.alloc(sizeof(OpResultType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate OpResult buffer failed", K(ret), K(sizeof(OpResultType)));
    } else {
      op_result = new(buffer) OpResultType();
    }
    return ret;
  }
};

template <int TYPE>
struct AllocDASCtDefHelper
{
  static int alloc(ObIAllocator &alloc, ObDASBaseCtDef *&ctdef)
  {
    int ret = OB_SUCCESS;
    typedef typename das_reg::ObDASOpTypeTraits<TYPE>::DASCtDef CtDefType;
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = alloc.alloc(sizeof(CtDefType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate Op CtDef buffer failed", K(ret), K(sizeof(CtDefType)));
    } else {
      ctdef = new(buffer) CtDefType(alloc);
    }
    return ret;
  }
};

template <int TYPE>
struct AllocDASRtDefHelper
{
  static int alloc(ObIAllocator &alloc, ObDASBaseRtDef *&rtdef)
  {
    int ret = OB_SUCCESS;
    typedef typename das_reg::ObDASOpTypeTraits<TYPE>::DASRtDef RtDefType;
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = alloc.alloc(sizeof(RtDefType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate Op RtDef buffer failed", K(ret), K(sizeof(RtDefType)));
    } else {
      rtdef = new(buffer) RtDefType();
    }
    return ret;
  }
};

// define && init alloc function array
static ObDASTaskFactory::AllocFun G_DAS_ALLOC_FUNCTION_ARRAY[DAS_OP_MAX];
static_assert(DAS_OP_MAX == ARRAYSIZEOF(G_DAS_ALLOC_FUNCTION_ARRAY),
              "alloc function array is too small");

ObDASTaskFactory::AllocFun *ObDASTaskFactory::G_DAS_ALLOC_FUNS_ = G_DAS_ALLOC_FUNCTION_ARRAY;

template <int N>
struct DASInitAllocFunc
{
  static void init_array()
  {
    static constexpr int registered = das_reg::ObDASOpTypeTraits<N>::registered_;
    static constexpr int attached = das_reg::ObDASOpTypeTraits<N>::attached_;
    G_DAS_ALLOC_FUNCTION_ARRAY[N] = ObDASTaskFactory::AllocFun {
          ((registered && !attached) ? &AllocDASOpHelper<N * registered>::alloc : NULL),
          ((registered && !attached) ? &AllocDASOpResultHelper<N * registered>::alloc : NULL),
          (registered ? &AllocDASCtDefHelper<N * registered>::alloc : NULL),
          (registered ? &AllocDASRtDefHelper<N * registered>::alloc : NULL)
    };
  }
};

bool G_DAS_ALLOC_FUNC_SET = common::ObArrayConstIniter<DAS_OP_MAX, DASInitAllocFunc>::init();

ObDASTaskFactory::ObDASTaskFactory(ObIAllocator &allocator)
  : das_op_store_(allocator),
    das_result_store_(allocator),
    das_extra_data_store_(allocator),
    das_async_cb_store_(allocator),
    ctdef_store_(allocator),
    rtdef_store_(allocator),
    allocator_(allocator)

{
}

ObDASTaskFactory::~ObDASTaskFactory()
{
  cleanup();
}

int ObDASTaskFactory::create_das_ctdef(ObDASOpType op_type, ObDASBaseCtDef *&ctdef)
{
  int ret = OB_SUCCESS;
  ret = create_das_ctdef(op_type, allocator_, ctdef);
  STORE_DAS_OBJ(ctdef_store_, ctdef, ObDASBaseCtDef);
  return ret;
}

int ObDASTaskFactory::create_das_ctdef(ObDASOpType op_type, ObIAllocator &alloc, ObDASBaseCtDef *&ctdef)
{
  int ret = OB_SUCCESS;
  if (!is_registered(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das op type", K(ret), K(op_type));
  } else if (OB_FAIL(G_DAS_ALLOC_FUNCTION_ARRAY[op_type].ctdef_func_(alloc, ctdef))) {
    LOG_WARN("allocate das ctdef failed", K(ret), K(op_type));
  }
  return ret;
}

int ObDASTaskFactory::create_das_rtdef(ObDASOpType op_type, ObDASBaseRtDef *&rtdef)
{
  int ret = OB_SUCCESS;
  ret = create_das_rtdef(op_type, allocator_, rtdef);
  STORE_DAS_OBJ(rtdef_store_, rtdef, ObDASBaseRtDef);
  return ret;
}

int ObDASTaskFactory::create_das_rtdef(ObDASOpType op_type, ObIAllocator &alloc, ObDASBaseRtDef *&rtdef)
{
  int ret = OB_SUCCESS;
  if (!is_registered(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das op type", K(ret), K(op_type));
  } else if (OB_FAIL(G_DAS_ALLOC_FUNCTION_ARRAY[op_type].rtdef_func_(alloc, rtdef))) {
    LOG_WARN("allocate das rtdef failed", K(ret), K(op_type));
  }
  return ret;
}

int ObDASTaskFactory::create_das_task_op(ObDASOpType op_type, ObIDASTaskOp *&das_op)
{
  int ret = OB_SUCCESS;
  if (!is_registered(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das op type", K(ret), K(op_type));
  } else if (OB_FAIL(G_DAS_ALLOC_FUNCTION_ARRAY[op_type].op_func_(allocator_, das_op))) {
    LOG_WARN("allocate das task op failed", K(ret), K(op_type));
  } else {
    das_op->set_type(op_type);
  }
  STORE_DAS_OBJ(das_op_store_, das_op, ObIDASTaskOp);
  return ret;
}

int ObDASTaskFactory::create_das_task_result(ObDASOpType op_type, ObIDASTaskResult *&das_result)
{
  int ret = OB_SUCCESS;
  if (!is_registered(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das op type", K(ret), K(op_type));
  } else if (OB_FAIL(G_DAS_ALLOC_FUNCTION_ARRAY[op_type].result_func_(allocator_, das_result))) {
    LOG_WARN("allocate das task op result failed", K(ret), K(op_type));
  }
  STORE_DAS_OBJ(das_result_store_, das_result, ObIDASTaskResult);
  return ret;
}

int ObDASTaskFactory::create_das_extra_data(ObDASExtraData *&extra_result)
{
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  if (OB_ISNULL(buffer = allocator_.alloc(sizeof(ObDASExtraData)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc das extra result buffer failed", K(ret), K(sizeof(ObDASExtraData)));
  } else {
    extra_result = new(buffer) ObDASExtraData();
    STORE_DAS_OBJ(das_extra_data_store_, extra_result, ObDASExtraData);
  }
  return ret;
}

int ObDASTaskFactory::create_das_async_cb(
    const common::ObSEArray<ObIDASTaskOp *, 2> &task_ops,
    const ObMemAttr &attr,
    ObDASRef &das_ref,
    ObRpcDasAsyncAccessCallBack *&async_cb,
    int64_t timeout_ts) {
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  ObDasAsyncRpcCallBackContext *context = nullptr;
  if (OB_ISNULL(buffer = allocator_.alloc(sizeof(ObDasAsyncRpcCallBackContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate das async cb context memory", K(ret), K(sizeof(ObDasAsyncRpcCallBackContext)));
  } else if (FALSE_IT(context = new (buffer) ObDasAsyncRpcCallBackContext(das_ref, task_ops, timeout_ts))) {
  } else if (OB_FAIL(context->init(attr))) {
    LOG_WARN("fail to init das async cb context", K(ret));
  } else if (OB_ISNULL(buffer = allocator_.alloc(sizeof(ObRpcDasAsyncAccessCallBack)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate das async cb memory", K(ret), K(sizeof(ObRpcDasAsyncAccessCallBack)));
  } else {
    async_cb = new (buffer) ObRpcDasAsyncAccessCallBack(context);
    STORE_DAS_OBJ(das_async_cb_store_, async_cb, ObRpcDasAsyncAccessCallBack);
  }
  return ret;
}

void ObDASTaskFactory::cleanup()
{
  if (OB_LIKELY(!das_op_store_.empty())) {
    for (DasOpIter op_iter = das_op_store_.begin();
        !op_iter.is_end();
        ++op_iter) {
      ObIDASTaskOp *das_op = op_iter.get_item();
      if (das_op != nullptr) {
        das_op->~ObIDASTaskOp();
        das_op = nullptr;
      }
    }
    das_op_store_.clear();
  }
  if (OB_UNLIKELY(!das_result_store_.empty())) {
    for (DasResultIter result_iter = das_result_store_.begin();
        !result_iter.is_end();
        ++result_iter) {
      ObIDASTaskResult *op_result = result_iter.get_item();
      if (op_result != nullptr) {
        op_result->~ObIDASTaskResult();
        op_result = nullptr;
      }
    }
    das_result_store_.clear();
  }
  if (OB_UNLIKELY(!das_extra_data_store_.empty())) {
    for (DasExtraDataIter extra_iter = das_extra_data_store_.begin();
        !extra_iter.is_end();
        ++extra_iter) {
      ObDASExtraData *extra_result = extra_iter.get_item();
      if (extra_result != nullptr) {
        extra_result->~ObDASExtraData();
        extra_result = nullptr;
      }
    }
    das_extra_data_store_.clear();
  }
  if (OB_UNLIKELY(!das_async_cb_store_.empty())) {
    for (DasAsyncCallBackIter async_cb_iter = das_async_cb_store_.begin();
        !async_cb_iter.is_end();
        ++async_cb_iter) {
      ObRpcDasAsyncAccessCallBack *async_cb = async_cb_iter.get_item();
      if (async_cb != nullptr) {
        async_cb->get_async_cb_context()->~ObDasAsyncRpcCallBackContext();
        async_cb->~ObRpcDasAsyncAccessCallBack();
        async_cb = nullptr;
      }
    }
    das_async_cb_store_.clear();
  }
  if (OB_UNLIKELY(!ctdef_store_.empty())) {
    for (DasCtDefIter ctdef_iter = ctdef_store_.begin();
        !ctdef_iter.is_end();
        ++ctdef_iter) {
      ObDASBaseCtDef *ctdef = ctdef_iter.get_item();
      if (ctdef != nullptr) {
        ctdef->~ObDASBaseCtDef();
        ctdef = nullptr;
      }
    }
    ctdef_store_.clear();
  }
  if (OB_UNLIKELY(!rtdef_store_.empty())) {
    for (DasRtDefIter rtdef_iter = rtdef_store_.begin();
        !rtdef_iter.is_end();
        ++rtdef_iter) {
      ObDASBaseRtDef *rtdef = rtdef_iter.get_item();
      if (rtdef != nullptr) {
        rtdef->~ObDASBaseRtDef();
        rtdef = nullptr;
      }
    }
    rtdef_store_.clear();
  }
}
}  // namespace sql
}  // namespace oceanbase
