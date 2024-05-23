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

#include "buffer_ctx.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "runtime_utility/mds_factory.h"
#include "compile_utility/map_type_index_in_tuple.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/mds_writer.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

void BufferCtxNode::destroy_ctx() {
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~BufferCtx();
    MTL(mds::ObTenantMdsService*)->get_buffer_ctx_allocator().free(ctx_);
    ctx_ = nullptr;
  }
}

int BufferCtxNode::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (OB_NOT_NULL(ctx_)) {
    // 序列化时，如果ctx不为空，那么其类型必须是有效的，这里防御一下，否则反序列化的报错会增加排查难度
    MDS_ASSERT(ctx_->get_binding_type_id() != INVALID_VALUE);
    int64_t type_id = ctx_->get_binding_type_id();
    if (MDS_FAIL(serialization::encode(buf, buf_len, pos, type_id))) {
      MDS_LOG(ERROR, "serialize buffer ctx id failed", KR(ret), K(type_id));
    } else if (MDS_FAIL(ctx_->serialize(buf, buf_len, pos))) {
      MDS_LOG(ERROR, "serialize buffer ctx impl failed", KR(ret), K(type_id));
    } else {
      MDS_LOG(DEBUG, "serialize buffer ctx impl success", KR(ret), K(type_id), K(buf_len), K(pos));
    }
  } else {
    int64_t type_id = INVALID_VALUE;
    if (MDS_FAIL(serialization::encode(buf, buf_len, pos, type_id))) {
      MDS_LOG(ERROR, "serialize invalid buffer ctx id failed", KR(ret), K(type_id));
    } else {
      MDS_LOG(DEBUG, "serialize invalid buffer ctx id failed", KR(ret), K(type_id), K(buf_len), K(pos));
    }
  }
  return ret;
}

template <int IDX>
int deserialize_(BufferCtx *&ctx_, int64_t type_idx, const char *buf, const int64_t buf_len, int64_t &pos, ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (IDX == type_idx) {
    using ImplType = GET_CTX_TYPE_BY_TUPLE_IDX(IDX);
    ImplType *p_impl = nullptr;
    set_mds_mem_check_thread_local_info(MdsWriter(WriterType::UNKNOWN_WRITER, 0), typeid(ImplType).name());
    if (OB_ISNULL(p_impl = (ImplType *)allocator.alloc(sizeof(ImplType),
                                                       ObMemAttr(MTL_ID(),
                                                       "MDS_CTX_DESE",
                                                       ObCtxIds::MDS_CTX_ID)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(ERROR, "fail to alloc buffer ctx memory", KR(ret), K(type_idx), K(IDX));
    } else if (FALSE_IT(new (p_impl) ImplType())) {
    } else if (MDS_FAIL(p_impl->deserialize(buf, buf_len, pos))) {
      allocator.free(p_impl);
      p_impl = nullptr;
      MDS_LOG(ERROR, "deserialzed from buffer failed", KR(ret), K(type_idx), K(IDX));
    } else {
      ctx_ = p_impl;
      ctx_->set_binding_type_id(type_idx);
      MTL(ObTenantMdsService*)->update_mem_leak_debug_info(p_impl, [p_impl](const ObIntWarp &key,
                                                                            ObMdsMemoryLeakDebugInfo &value) -> bool {
        databuff_printf(value.tag_str_, TAG_SIZE, "%s", to_cstring(p_impl->get_writer()));
        return true;
      });
      MDS_LOG(INFO, "deserialize ctx success", KR(ret), K(*p_impl), K(type_idx), K(IDX), K(buf_len), K(pos), K(lbt()));
    }
    reset_mds_mem_check_thread_local_info();
  } else if (MDS_FAIL(deserialize_<IDX + 1>(ctx_, type_idx, buf, buf_len, pos, allocator))) {
    MDS_LOG(ERROR, "deserialzed from buffer failed", KR(ret), K(type_idx), K(IDX));
  }
  return ret;
}

template <>
int deserialize_<BufferCtxTupleHelper::get_element_size()>(BufferCtx *&ctx_,
                                                           int64_t type_idx,
                                                           const char *buf,
                                                           const int64_t buf_len,
                                                           int64_t &pos,
                                                           ObIAllocator &allocator)
{
  int ret = OB_ERR_UNEXPECTED;
  MDS_LOG(ERROR, "type idx out of tuple range", KR(ret), K(type_idx), K(BufferCtxTupleHelper::get_element_size()));
  return ret;
}

int BufferCtxNode::deserialize(const char *buf, const int64_t buf_len, int64_t &pos, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  int64_t type_idx = INVALID_VALUE;
  if (MDS_FAIL(serialization::decode(buf, buf_len, pos, type_idx))) {
    MDS_LOG(ERROR, "fail to deserialize buffer ctx id", KR(ret), K(type_idx));
  } else if (INVALID_VALUE == type_idx) {
    MDS_LOG(DEBUG, "deserialized INVALD buffer ctx", KR(ret), K(type_idx), K(buf_len), K(pos));
  } else if (MDS_FAIL(deserialize_<0>(ctx_, type_idx, buf, buf_len, pos, allocator))) {
    MDS_LOG(WARN, "deserialized buffer ctx failed", KR(ret), K(type_idx));
  }
  return ret;
}

int64_t BufferCtxNode::get_serialize_size(void) const
{
  int64_t size = 0;
  if (OB_NOT_NULL(ctx_)) {
    size += serialization::encoded_length(ctx_->get_binding_type_id());
    size += ctx_->get_serialize_size();
  } else {
    size += serialization::encoded_length(INVALID_VALUE);
  }
  return size;
}

}
}
}
