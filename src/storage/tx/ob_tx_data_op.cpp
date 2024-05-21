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

#include "storage/tx/ob_tx_data_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tx_table/ob_tx_table.h"
#include "share/rc/ob_tenant_base.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObTxDummyOp);
ObTxDummyOp DEFAULT_TX_DUMMY_OP;

void ObTxDataOp::dec_ref() {
  int ret = OB_SUCCESS;
  if(ATOMIC_SAF(&ref_cnt_, 1) == 0) {
    if (OB_FAIL(tx_op_list_.check_stat())) {
      STORAGE_LOG(WARN, "dec_ref", KR(ret), KP(this), KPC(this));
      abort();
    }
    // to release tx_op
    for (int64_t idx = 0; idx < tx_op_list_.get_count(); idx++) {
      ObTxOp &tx_op = *tx_op_list_.at(idx);
      tx_op.release();
    }
    if (OB_NOT_NULL(tx_op_list_.get_ptr())) {
      op_allocator_->free(tx_op_list_.get_ptr());
    }
    // to release undo status
    if (OB_UNLIKELY(nullptr != undo_status_list_.head_)) {
      ObUndoStatusNode *node_ptr = undo_status_list_.head_;
      ObUndoStatusNode *node_to_free = nullptr;
      while (nullptr != node_ptr) {
        node_to_free = node_ptr;
        node_ptr = node_ptr->next_;
        tx_data_allocator_->free(node_to_free);
      }
    }
    tx_data_allocator_->free(this);
  }
}

int64_t ObTxDataOp::get_tx_op_size()
{
  int64_t tx_op_size = tx_op_list_.get_capacity() * sizeof(ObTxOp);
  for (int64_t idx = 0; idx < tx_op_list_.get_count(); idx++) {
    ObTxOp &tx_op = *tx_op_list_.at(idx);
    tx_op_size += tx_op.get_val_size();
  }
  return tx_op_size;
}

int64_t ObTxOpVector::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  common::databuff_printf(buf, buf_len, pos, "count=%ld", count_);
  for (int64_t idx = 0; idx < count_; idx++) {
    ObTxOp *op = &tx_op_[idx];
    common::databuff_printf(buf, buf_len, pos, " op(%ld)=(op_code:%ld, op_scn:%ld)",
        idx, op->get_op_code(), op->get_op_scn().convert_to_ts());
  }
  return pos;
}

ObTxOp *ObTxOpVector::at(int64_t idx)
{
  ObTxOp *tx_op = nullptr;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stat())) {
    STORAGE_LOG(WARN, "tx_op vector stat error", KPC(this), KR(ret), K(lbt()));
  } else if (idx >= 0 && idx < count_) {
    tx_op = &tx_op_[idx];
  } else {
    STORAGE_LOG(WARN, "out of range", KPC(this), K(idx));
  }
  return tx_op;
}

int ObTxOpVector::push_back(ObTxOp &tx_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stat())) {
    STORAGE_LOG(WARN, "tx_op vector stat error", KR(ret));
  } else if (count_ < capacity_) {
    tx_op_[count_] = tx_op;
    count_++;
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int ObTxOpVector::check_stat()
{
  int ret = OB_SUCCESS;
  if (count_ < 0 || capacity_ < 0 || count_ > capacity_
      || (count_ > 0 && OB_ISNULL(tx_op_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "check_stat failed", KPC(this), KP(this));
  }
  return ret;
}

int ObTxOpVector::try_extend_space(int64_t count, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(check_stat())) {
    STORAGE_LOG(WARN, "check_stat failed", KR(ret));
  } else if (count == 0) {
    // do nothing
  } else if (count_ + count <= capacity_) {
    // do nothing
  } else {
    ObTxOp *tx_op_ptr = nullptr;
    if (OB_ISNULL(tx_op_ptr = (ObTxOp*)allocator.alloc((count_ + count) * sizeof(ObTxOp)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (count_ > 0) {
        MEMCPY(tx_op_ptr, tx_op_, count_ * sizeof(ObTxOp));
      }
      if (OB_NOT_NULL(tx_op_)) {
        allocator.free(tx_op_);
      }
      tx_op_ = tx_op_ptr;
      capacity_ = count_ + count;
    }
  }
  return ret;
}

int64_t ObTxOpVector::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(count_);
  for (int64_t idx = 0; idx < count_; idx++) {
    len += tx_op_[idx].get_serialize_size();
  }
  return len;
}

int ObTxOpVector::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count_))) {
    STORAGE_LOG(WARN, "serialize fail", KR(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count_; idx++) {
      if (OB_FAIL(tx_op_[idx].serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "serialize fail", KR(ret));
      }
    }
  }
  return ret;
}

int ObTxOpVector::deserialize(const char *buf, const int64_t buf_len, int64_t &pos, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &count_))) {
    STORAGE_LOG(WARN, "deserialize fail", KR(ret), K(buf), K(buf_len), K(pos), K(count_));
  } else if (count_ > 0) {
    if (OB_ISNULL(tx_op_ = (ObTxOp*)allocator.alloc(count_ * sizeof(ObTxOp)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc mem fail", KR(ret), K(count_));
      count_ = 0; // reset count_ make vector clear
    } else {
      capacity_ = count_;
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count_; idx++) {
      new (&tx_op_[idx]) ObTxOp();
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count_; idx++) {
      if (OB_FAIL(tx_op_[idx].deserialize(buf, buf_len, pos, allocator))) {
        STORAGE_LOG(WARN, "deserialize fail", KR(ret));
      }
    }
  }
  return ret;
}

int ObTxDataOp::add_tx_op(ObTxOp &tx_op)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(tx_op_list_.try_extend_space(1, *op_allocator_))) {
    STORAGE_LOG(WARN, "try_extend_space fail", KR(ret), K(tx_op));
  } else if (OB_FAIL(tx_op_list_.push_back(tx_op))) {
    STORAGE_LOG(WARN, "push tx_op to array failed", KR(ret));
  }
  return ret;
}

int ObTxDataOp::reserve_tx_op_space(int64_t count)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(tx_op_list_.try_extend_space(count, *op_allocator_))) {
    STORAGE_LOG(WARN, "try_extend_space fail", KR(ret), K(count));
  }
  return ret;
}

int ObTxDataOp::add_tx_op_batch(transaction::ObTransID tx_id, share::ObLSID ls_id, share::SCN op_scn, ObTxOpArray &tx_op_batch)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard lock_guard(lock_);
  if (OB_FAIL(tx_op_list_.try_extend_space(tx_op_batch.count(), *op_allocator_))) {
    STORAGE_LOG(WARN, "try_extend_space fail", KR(ret), K(tx_op_batch));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tx_op_batch.count(); idx++) {
      if (OB_FAIL(tx_op_list_.push_back(tx_op_batch.at(idx)))) {
        STORAGE_LOG(WARN, "push tx_op to array failed", KR(ret));
      }
    }
    // !!! we must promise tx_op_batch atomic append into tx_op_list
    // otherwise tx_op replay filter with log_scn compare op_scn will cause serious problem
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "tx_op_batch is not atomic append", K(tx_id), K(ls_id), K(tx_op_list_), K(tx_op_batch), K(op_scn));
      ob_abort();
    }
  }
  STORAGE_LOG(INFO, "add_tx_op", K(ret), K(tx_id), K(ls_id), K(op_scn), K(tx_op_batch.count()), K(tx_op_list_.get_count()), K(tx_op_batch));
  return ret;
}

int64_t ObTxOp::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(int64_t(op_code_));
  len += op_scn_.get_serialize_size();

  #define SERIALIZE_TX_OP_TMP(OP_CODE, OP_TYPE)             \
  if (OB_NOT_NULL(op_val_) && op_code_ ==  OP_CODE) {       \
    OP_TYPE &op_obj = *((OP_TYPE*)op_val_);                 \
    len += op_obj.get_serialize_size();                     \
  }
  #define SERIALIZE_TX_OP(TYPE, UNUSED) SERIALIZE_TX_OP_TMP TYPE

  LST_DO2(SERIALIZE_TX_OP, (), TX_OP_MEMBERS);
  #undef SERIALIZE_TX_OP_TMP
  #undef SERIALIZE_TX_OP
  return len;
}

int ObTxOp::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, int64_t(op_code_)))) {
    STORAGE_LOG(WARN, "serialize fail", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(op_scn_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize fail", K(ret), K(buf_len), K(pos));
  } else if (OB_ISNULL(op_val_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx_op op_val is null", KR(ret), KPC(this));
  }
  #define SERIALIZE_TX_OP_TMP(OP_CODE, OP_TYPE)                       \
  if (OB_SUCC(ret) && op_code_ ==  OP_CODE) { \
    OP_TYPE &op_obj = *((OP_TYPE*)op_val_);                           \
    if (OB_FAIL(op_obj.serialize(buf, buf_len, pos))) {               \
      STORAGE_LOG(WARN, "serialize fail", KR(ret));                   \
    }                                                                 \
  }
  #define SERIALIZE_TX_OP(TYPE, UNUSED) SERIALIZE_TX_OP_TMP TYPE

  LST_DO2(SERIALIZE_TX_OP, (), TX_OP_MEMBERS);
  #undef SERIALIZE_TX_OP_TMP
  #undef SERIALIZE_TX_OP
  return ret;
}

int ObTxOp::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, (int64_t*)&op_code_))) {
    STORAGE_LOG(WARN, "deserialize fail", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(op_scn_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize fail", K(ret), K(data_len), K(pos));
  }
  #define DESERIALIZE_TX_OP_TMP(OP_CODE, OP_TYPE)                 \
  if (OB_SUCC(ret) && op_code_ ==  OP_CODE) {                     \
    OP_TYPE *op_obj = nullptr;                                    \
    if (OB_ISNULL(op_obj = (OP_TYPE*)allocator.alloc(sizeof(OP_TYPE)))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                           \
      STORAGE_LOG(WARN, "deserialize fail", K(ret), K(data_len), K(pos));        \
    } else if (FALSE_IT(new (op_obj) OP_TYPE())) {                               \
    } else if (OB_FAIL(op_obj->deserialize(buf, data_len, pos))) {    \
      STORAGE_LOG(WARN, "deserialize fail", KR(ret));               \
      allocator.free(op_obj);                                                    \
    } else if (FALSE_IT(op_val_ = op_obj)) {                                     \
    }                                                                            \
  }
  #define DESERIALIZE_TX_OP(TYPE, UNUSED)  DESERIALIZE_TX_OP_TMP TYPE

  LST_DO2(DESERIALIZE_TX_OP, (), TX_OP_MEMBERS);
  #undef DESERIALIZE_TX_OP_TMP
  #undef DESERIALIZE_TX_OP
  return ret;
}

void ObTxOp::release()
{
  ObIAllocator &allocator = MTL(ObSharedMemAllocMgr*)->tx_data_op_allocator();
  #define RELEASE_TX_OP_TMP(OP_CODE, OP_TYPE)                  \
  if (OB_NOT_NULL(op_val_) && op_code_ ==  OP_CODE             \
      && op_val_ != &DEFAULT_TX_DUMMY_OP) {                    \
    OP_TYPE *op_obj = (OP_TYPE*)op_val_;                       \
    release(*op_obj);                                          \
    allocator.free(op_obj);                                    \
  }
  #define RELEASE_TX_OP(TYPE, UNUSED) RELEASE_TX_OP_TMP TYPE

  LST_DO2(RELEASE_TX_OP, (), TX_OP_MEMBERS);
  #undef RELEASE_TX_OP_TMP
  #undef RELEASE_TX_OP
}

}  // namespace storage
}  // namespace oceanbase
