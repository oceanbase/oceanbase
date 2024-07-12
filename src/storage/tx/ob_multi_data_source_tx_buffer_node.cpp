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

#include "storage/tx/ob_multi_data_source_tx_buffer_node.h"
#include "lib/utility/utility.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/tx/ob_multi_data_source.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace transaction
{

ObTxBufferNode::ObTxBufferNode()
  : seq_no_(),
    type_(ObTxDataSourceType::UNKNOWN),
    data_()
{
  reset();
}

OB_SERIALIZE_MEMBER(ObTxBufferNode, type_, data_, register_no_, seq_no_);

int ObTxBufferNode::init(const ObTxDataSourceType type,
                         const ObString &data,
                         const share::SCN &base_scn,
                         const ObTxSEQ seq_no,
                         storage::mds::BufferCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type <= ObTxDataSourceType::UNKNOWN || type >= ObTxDataSourceType::MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(type));
  } else {
    reset();
    type_ = type;
    data_ = data;
    mds_base_scn_ = base_scn;
    seq_no_ = seq_no;
    buffer_ctx_node_.set_ctx(ctx);
  }
  return ret;
}

bool ObTxBufferNode::is_valid() const
{
  bool valid_member = false;
  valid_member = type_ > ObTxDataSourceType::UNKNOWN && type_ < ObTxDataSourceType::MAX_TYPE
                  && data_.length() > 0;
  return valid_member;
}

void ObTxBufferNode::reset()
{
  register_no_ = 0;
  seq_no_.reset();
  type_ = ObTxDataSourceType::UNKNOWN;
  data_.reset();
  has_submitted_ = false;
  has_synced_ = false;
  mds_base_scn_.reset();
}

int ObTxBufferNode::set_mds_register_no(const uint64_t register_no)
{
  int ret = OB_SUCCESS;
  if (register_no <= 0 || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(register_no), KPC(this));
  } else if (register_no_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid register no", K(ret), K(register_no), KPC(this));
  } else {
    register_no_ = register_no;
    // TRANS_LOG(INFO, "set register no in mds node", K(ret), KPC(this));
  }

  return ret;
}

bool ObTxBufferNode::allow_to_use_mds_big_segment() const
{
  return type_ == ObTxDataSourceType::DDL_TRANS;
}

void ObTxBufferNode::replace_data(const common::ObString &data)
{
  if (nullptr != data_.ptr()) {
    ob_free(data_.ptr());
    data_.assign_ptr(nullptr, 0);
  }

  data_ = data;
  has_submitted_ = false;
  has_synced_ = false;
}

bool ObTxBufferNode::operator==(const ObTxBufferNode &buffer_node) const
{
  bool is_same = false;

  if (has_submitted_ == buffer_node.has_submitted_
      && has_synced_ == buffer_node.has_synced_
      && mds_base_scn_ == buffer_node.mds_base_scn_
      && type_ == buffer_node.type_
      && data_ == buffer_node.data_
      && seq_no_ == buffer_node.seq_no_) {
    is_same = true;
  }

  return is_same;
}

ObTxBufferNodeWrapper::ObTxBufferNodeWrapper()
  : tx_id_(0),
    node_()
{
}

ObTxBufferNodeWrapper::~ObTxBufferNodeWrapper()
{
  ObIAllocator &allocator = MTL(share::ObSharedMemAllocMgr*)->tx_data_op_allocator();
  if (OB_NOT_NULL(node_.get_ptr())) {
    allocator.free(node_.get_ptr());
  }
  storage::mds::BufferCtx *buffer_ctx = const_cast<storage::mds::BufferCtx*>(node_.get_buffer_ctx_node().get_ctx());
  if (OB_NOT_NULL(buffer_ctx)) {
    // TODO destructor without allocator is safe?
    buffer_ctx->~BufferCtx();
    allocator.free(buffer_ctx);
  }
}

OB_DEF_SERIALIZE_SIZE(ObTxBufferNodeWrapper)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(tx_id_);
  len += node_.get_serialize_size();
  len += node_.get_buffer_ctx_node().get_serialize_size();
  return len;
}

OB_DEF_SERIALIZE(ObTxBufferNodeWrapper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, tx_id_))) {
    TRANS_LOG(WARN, "serialize node wrapper fail", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(node_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize node wrapper fail", KR(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(node_.get_buffer_ctx_node().serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize node wrapper fail", KR(ret), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTxBufferNodeWrapper)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = MTL(share::ObSharedMemAllocMgr*)->tx_data_op_allocator();
  char *node_buf = NULL;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &tx_id_))) {
    TRANS_LOG(WARN, "deserialize node wrapper fail", KR(ret), K(data_len), K(pos));
  } else if (OB_FAIL(node_.deserialize(buf, data_len, pos))) {
    TRANS_LOG(WARN, "deserialize node wrapper fail", KR(ret), K(data_len), K(pos));
  } else if (OB_ISNULL(node_buf = (char*)allocator.alloc(node_.get_data_size()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "deserialize node wrapper fail", KR(ret), K(data_len), K(pos));
  } else if (FALSE_IT(MEMCPY(node_buf, node_.get_ptr(), node_.get_data_size()))) {
  } else if (FALSE_IT((node_.get_data().assign_ptr(node_buf, node_.get_data_size())))) {
  } else if (OB_FAIL(node_.get_buffer_ctx_node().deserialize(buf, data_len, pos, allocator))) {
    TRANS_LOG(WARN, "deserialize node wrapper fail", KR(ret), K(data_len), K(pos));
  }
  return ret;
}

int ObTxBufferNodeWrapper::assign(ObIAllocator &allocator, const ObTxBufferNodeWrapper &wrapper)
{
  return assign(wrapper.get_tx_id(), wrapper.get_node(), allocator, false);
}

int ObTxBufferNodeWrapper::pre_alloc(int64_t tx_id, const ObTxBufferNode &node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = node.get_data_size();
  char *ptr = NULL;
  node_.register_no_ = node.register_no_;
  node_.type_ = node.type_;

  if (OB_ISNULL(ptr = (char*)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc mem fail", K(ret));
  } else {
    node_.get_data().assign_ptr(ptr, buf_len);
  }

  if (OB_SUCC(ret)) {
    mds::BufferCtx *new_ctx = nullptr;
    if (OB_ISNULL(node.get_buffer_ctx_node().get_ctx())) {
      // do nothing
    } else if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(node.type_,
                                                          ObTransID(tx_id),
                                                          new_ctx,
                                                          allocator))) {
      TRANS_LOG(WARN, "create buffer_ctx failed", KR(ret));
    } else {
      node_.get_buffer_ctx_node().set_ctx(new_ctx);
    }
  }
  return ret;
}

int ObTxBufferNodeWrapper::assign(int64_t tx_id,
                                  const ObTxBufferNode &node,
                                  ObIAllocator &allocator,
                                  bool has_pre_alloc)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = node.get_data_size();
  char *ptr = NULL;
  tx_id_ = tx_id;
  node_.register_no_ = node.register_no_;
  node_.has_submitted_ = node.has_submitted_;
  node_.has_synced_ = node.has_synced_;
  node_.mds_base_scn_ = node.mds_base_scn_;
  node_.type_ = node.type_;

  if (has_pre_alloc) {
    MEMCPY(node_.get_ptr(), const_cast<ObTxBufferNode&>(node).get_ptr(), buf_len);
  } else if (OB_ISNULL(ptr = (char*)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc mem fail", K(ret));
  } else {
    MEMCPY(ptr, const_cast<ObTxBufferNode&>(node).get_ptr(), buf_len);
    node_.get_data().assign_ptr(ptr, buf_len);
  }

  if (OB_SUCC(ret)) {
    mds::BufferCtx *new_ctx = nullptr;
    if (OB_ISNULL(node.get_buffer_ctx_node().get_ctx())) {
      // do nothing
    } else if (has_pre_alloc && FALSE_IT(new_ctx = const_cast<mds::BufferCtx*>(node_.get_buffer_ctx_node().get_ctx()))) {
    } else if (has_pre_alloc && OB_ISNULL(new_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "has_pre_alloc but new_ctx is null", KR(ret), K(node_));
    } else if (OB_FAIL(mds::MdsFactory::deep_copy_buffer_ctx(ObTransID(tx_id_),
                                                             *(node.get_buffer_ctx_node().get_ctx()),
                                                              new_ctx,
                                                              allocator))) {
      TRANS_LOG(WARN, "copy buffer_ctx failed", KR(ret));
    } else if (!has_pre_alloc) {
      node_.get_buffer_ctx_node().set_ctx(new_ctx);
    }
  }
  return ret;
}

}
}