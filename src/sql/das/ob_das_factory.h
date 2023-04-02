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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_FACTORY_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_FACTORY_H_
#include "lib/list/ob_obj_store.h"
#include "sql/das/ob_das_task.h"
namespace oceanbase
{
namespace sql
{
class ObDASExtraData;
class ObIDASTaskResult;
class ObIDASTaskOp;
class ObIDASTaskResult;
class ObDASTaskArg;
class ObRpcDasAsyncAccessCallBack;
class ObDASRef;

class ObDASTaskFactory
{
  friend class ObDASRef;
public:
  explicit ObDASTaskFactory(common::ObIAllocator &allocator);
  ~ObDASTaskFactory();
  void cleanup();
  int create_das_task_op(ObDASOpType op_type, ObIDASTaskOp *&das_op);
  int create_das_task_result(ObDASOpType op_type, ObIDASTaskResult *&das_result);
  int create_das_extra_data(ObDASExtraData *&extra_result);
  int create_das_ctdef(ObDASOpType op_type, ObDASBaseCtDef *&ctdef);
  int create_das_rtdef(ObDASOpType op_type, ObDASBaseRtDef *&rtdef);
  int create_das_async_cb(const common::ObSEArray<ObIDASTaskOp *, 2> &task_ops,
                          const ObMemAttr &attr,
                          ObDASRef &das_ref,
                          ObRpcDasAsyncAccessCallBack *&async_cb,
                          int64_t timeout_ts);
  static int create_das_ctdef(ObDASOpType op_type, common::ObIAllocator &alloc, ObDASBaseCtDef *&ctdef);
  static int create_das_rtdef(ObDASOpType op_type, common::ObIAllocator &alloc, ObDASBaseRtDef *&rtdef);
  template <typename CtDef>
  static int alloc_das_ctdef(ObDASOpType op_type, common::ObIAllocator &alloc, CtDef *&ctdef);
  template <typename RtDef>
  static int alloc_das_rtdef(ObDASOpType op_type, common::ObIAllocator &alloc, RtDef *&rtdef);

  static inline bool is_registered(const ObDASOpType op_type)
  {
    return op_type >= 0 && op_type < DAS_OP_MAX && NULL != G_DAS_ALLOC_FUNS_[op_type].op_func_;
  }

  struct AllocFun
  {
    int (*op_func_)(ObIAllocator &, ObIDASTaskOp *&);
    int (*result_func_)(ObIAllocator &, ObIDASTaskResult *&);
    int (*ctdef_func_)(ObIAllocator &, ObDASBaseCtDef *&);
    int (*rtdef_func_)(ObIAllocator &, ObDASBaseRtDef *&);
  };
private:
  typedef common::ObObjStore<ObIDASTaskOp*, common::ObIAllocator&> DasOpStore;
  typedef DasOpStore::Iterator DasOpIter;
  typedef common::ObObjStore<ObIDASTaskResult*, common::ObIAllocator&> DasResultStore;
  typedef DasResultStore::Iterator DasResultIter;
  typedef common::ObObjStore<ObDASExtraData*, common::ObIAllocator&> DasExtraDataStore;
  typedef DasExtraDataStore::Iterator DasExtraDataIter;
  typedef common::ObObjStore<ObRpcDasAsyncAccessCallBack*, common::ObIAllocator&> DasAsyncCallBackStore;
  typedef DasAsyncCallBackStore::Iterator DasAsyncCallBackIter;
  typedef common::ObObjStore<ObDASBaseCtDef*, common::ObIAllocator&> DasCtDefStore;
  typedef DasCtDefStore::Iterator DasCtDefIter;
  typedef common::ObObjStore<ObDASBaseRtDef*, common::ObIAllocator&> DasRtDefStore;
  typedef DasRtDefStore::Iterator DasRtDefIter;

  DasOpStore das_op_store_;
  DasResultStore das_result_store_;
  DasExtraDataStore das_extra_data_store_;
  DasAsyncCallBackStore das_async_cb_store_;
  DasCtDefStore ctdef_store_;
  DasRtDefStore rtdef_store_;
  common::ObIAllocator &allocator_;
private:
  static AllocFun *G_DAS_ALLOC_FUNS_;
};

template <typename CtDef>
int ObDASTaskFactory::alloc_das_ctdef(ObDASOpType op_type,
                                      common::ObIAllocator &alloc,
                                      CtDef *&ctdef)
{
  int ret = common::OB_SUCCESS;
  ObDASBaseCtDef *base_ctdef = nullptr;
  ret = create_das_ctdef(op_type, alloc, base_ctdef);
  if (OB_SUCC(ret)) {
    ctdef = static_cast<CtDef*>(base_ctdef);
  }
  return ret;
}

template <typename RtDef>
int ObDASTaskFactory::alloc_das_rtdef(ObDASOpType op_type,
                                      common::ObIAllocator &alloc,
                                      RtDef *&rtdef)
{
  int ret = common::OB_SUCCESS;
  ObDASBaseRtDef *base_rtdef = nullptr;
  ret = create_das_rtdef(op_type, alloc, base_rtdef);
  if (OB_SUCC(ret)) {
    rtdef = static_cast<RtDef*>(base_rtdef);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_FACTORY_H_ */
