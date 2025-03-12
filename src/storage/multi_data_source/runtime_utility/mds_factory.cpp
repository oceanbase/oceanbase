/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "mds_factory.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <int IDX>
int deepcopy(const transaction::ObTransID &trans_id,
             const BufferCtx &old_ctx,
             BufferCtx *&new_ctx,
             ObIAllocator &allocator,
             const char *alloc_file,
             const char *alloc_func,
             const int64_t line) {
  int ret = OB_SUCCESS;
  ObTenantFreezer *tenant_freezer = MTL(ObTenantFreezer*);
  MDS_TG(1_ms);
  if (OB_ISNULL(tenant_freezer)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "MTL is not inited", KR(ret));
  } else if (IDX == old_ctx.get_binding_type_id()) {
    using ImplType = GET_CTX_TYPE_BY_TUPLE_IDX(IDX);
    ImplType *p_impl = nullptr;
    const ImplType *p_old_impl_ctx = static_cast<const ImplType *>(&old_ctx);
    MDS_ASSERT(OB_NOT_NULL(p_old_impl_ctx));
    const ImplType &old_impl_ctx = *p_old_impl_ctx;
    set_mds_mem_check_thread_local_info(MdsWriter(trans_id), typeid(ImplType).name(), alloc_file, alloc_func, line);
    // if pre_alloc buffer_ctx use it
    if (OB_NOT_NULL(new_ctx)) {
      ImplType *new_ctx_impl = dynamic_cast<ImplType *>(new_ctx);
      if (MDS_FAIL(common::meta::copy_or_assign(old_impl_ctx, *new_ctx_impl))) {
        MDS_LOG(WARN, "fail to assign old ctx to new", KR(ret), K(IDX));
      }
    } else if (CLICK() &&
        OB_ISNULL(p_impl = (ImplType *)allocator.alloc(sizeof(ImplType),
                                                       ObMemAttr(MTL_ID(),
                                                       "MDS_CTX_COPY",
                                                       ObCtxIds::MDS_CTX_ID)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "alloc memory failed", KR(ret), K(IDX));
    } else {
      CLICK();
      new (p_impl)ImplType();
      if (MDS_FAIL(common::meta::copy_or_assign(old_impl_ctx, *p_impl))) {
        p_impl->~ImplType();
        allocator.free(p_impl);
        MDS_LOG(WARN, "fail to assign old ctx to new", KR(ret), K(IDX));
      } else {
        new_ctx = p_impl;
        new_ctx->set_binding_type_id(old_ctx.get_binding_type_id());
      }
    }
    reset_mds_mem_check_thread_local_info();
  } else {
    ret = deepcopy<IDX + 1>(trans_id, old_ctx, new_ctx, allocator, alloc_file, alloc_func, line);
  }
  return ret;
}

template <>
int deepcopy<BufferCtxTupleHelper::get_element_size()>(const transaction::ObTransID &trans_id,
                                                       const BufferCtx &old_ctx,
                                                       BufferCtx *&new_ctx,
                                                       ObIAllocator &allocator,
                                                       const char *alloc_file,
                                                       const char *alloc_func,
                                                       const int64_t line)
{
  int ret = OB_ERR_UNEXPECTED;
  MDS_LOG(ERROR, "invalid old ctx", K(trans_id), K(old_ctx.get_binding_type_id()), K(alloc_file), K(alloc_func), K(line));
  return ret;
}

int MdsFactory::deep_copy_buffer_ctx(const transaction::ObTransID &trans_id,
                                     const BufferCtx &old_ctx,
                                     BufferCtx *&new_ctx,
                                     ObIAllocator &allocator,
                                     const char *alloc_file,
                                     const char *alloc_func,
                                     const int64_t line)
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  if (old_ctx.get_binding_type_id() == INVALID_VALUE) {
    ret = OB_INVALID_ARGUMENT;
    new_ctx = nullptr;// won't copy
    MDS_LOG(WARN, "invalid old_ctx", K(old_ctx.get_binding_type_id()));
  } else if (MDS_FAIL(deepcopy<0>(trans_id, old_ctx, new_ctx, allocator, alloc_file, alloc_func, line))) {
    MDS_LOG(WARN, "fail to deep copy buffer ctx", K(old_ctx.get_binding_type_id()));
  }
  return ret;
}

template <typename T, typename std::enable_if<std::is_base_of<MdsCtx, T>::value ||
                                              std::is_same<T, ObTransferDestPrepareTxCtx>::value ||
                                              std::is_same<T, ObMViewMdsOpCtx>::value ||
                                              std::is_same<T, ObTransferMoveTxCtx>::value, bool>::type = true>
void try_set_writer(T &ctx, const transaction::ObTransID &trans_id) {
  ctx.set_writer(MdsWriter(trans_id));
}

template <typename T, typename std::enable_if<!(std::is_base_of<MdsCtx, T>::value ||
                                                std::is_same<T, ObTransferDestPrepareTxCtx>::value ||
                                                std::is_same<T, ObMViewMdsOpCtx>::value ||
                                                std::is_same<T, ObTransferMoveTxCtx>::value), bool>::type = true>
void try_set_writer(T &ctx, const transaction::ObTransID &trans_id) {
  // do nothing
}

int MdsFactory::create_buffer_ctx(const transaction::ObTxDataSourceType &data_source_type,
                                  const transaction::ObTransID &trans_id,
                                  BufferCtx *&buffer_ctx,
                                  ObIAllocator &allocator,
                                  const char *alloc_file,
                                  const char *alloc_func,
                                  const int64_t line) {
  int ret = OB_SUCCESS;
  switch (data_source_type) {
    #define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
    #define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
    case transaction::ObTxDataSourceType::ENUM_NAME:\
    {\
      set_mds_mem_check_thread_local_info(MdsWriter(trans_id), typeid(BUFFER_CTX_TYPE).name(), alloc_file, alloc_func, line);\
      int64_t type_id = TupleTypeIdx<BufferCtxTupleHelper, BUFFER_CTX_TYPE>::value;\
      BUFFER_CTX_TYPE *ctx_impl = (BUFFER_CTX_TYPE *)\
                                   allocator.alloc(sizeof(BUFFER_CTX_TYPE),\
                                                   ObMemAttr(MTL_ID(),\
                                                   "MDS_CTX_CREATE",\
                                                   ObCtxIds::MDS_CTX_ID));\
      if (OB_ISNULL(ctx_impl)) {\
        ret = OB_ALLOCATE_MEMORY_FAILED;\
        MDS_LOG(WARN, "alloc memory failed", KR(ret));\
      } else {\
        new (ctx_impl) BUFFER_CTX_TYPE();\
        ctx_impl->set_binding_type_id(type_id);\
        try_set_writer(*ctx_impl, trans_id);\
        buffer_ctx = ctx_impl;\
      }\
      reset_mds_mem_check_thread_local_info();\
    }\
    break;
    #include "storage/multi_data_source/compile_utility/mds_register.h"
    #undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
    #undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
    default:
      ob_abort();
  }
  return ret;
}

}
}
}
