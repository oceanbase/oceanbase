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

#include "ob_tx_data_cache.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_tx_data_op.h"

namespace oceanbase {
namespace storage {

using namespace oceanbase::share;

ObTxDataCacheValue::~ObTxDataCacheValue() {
  destroy();
}

int ObTxDataCacheValue::init(const ObTxData &tx_data)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init tx data cache value twice", KR(ret), KPC(this));
  } else {
    // reserve or allocate buf to store tx data
    int64_t size = tx_data.size_need_cache();
    void *tx_data_buf = nullptr;
    if (TX_DATA_SLICE_SIZE == size) {
      // this tx data do not have undo actions, use reserved memory
      tx_data_buf = &reserved_buf_;
    } else if (OB_ISNULL(tx_data_buf = mtl_malloc(size, "TX_DATA_CACHE"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate memory for tx data cache value failed", KR(ret));
    } else {
      mtl_alloc_buf_ = tx_data_buf;
    }

    // deep copy tx data to the buf
    if (OB_FAIL(ret)){
    } else if (OB_FAIL(inner_deep_copy_(tx_data_buf, tx_data))) {
      STORAGE_LOG(WARN, "deep copy tx data failed", KR(ret), K(tx_data));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTxDataCacheValue::init(const ObTxDataCacheValue &tx_data_cache_val, void *tx_data_buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const ObTxData *tx_data = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "init tx data cache value twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(tx_data = tx_data_cache_val.get_tx_data())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "init tx data cache value with invalid argument", KR(ret), K(tx_data_cache_val));
  } else if (OB_FAIL(inner_deep_copy_(tx_data_buf, *tx_data))) {
    STORAGE_LOG(WARN, "inner deep copy tx data failed", KR(ret), KPC(tx_data));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTxDataCacheValue::destroy()
{
  if (OB_NOT_NULL(mtl_alloc_buf_)) {
    mtl_free(mtl_alloc_buf_);
  }
  is_inited_ = false;
  tx_data_ = nullptr;
  undo_node_array_ = nullptr;
}

int ObTxDataCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the tx data cache value to be copied is not inited", KR(ret), K(is_inited_), KP(tx_data_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(buf_len), K(size()));
  } else {
    ObTxDataCacheValue *tx_data_cache_value = new (buf) ObTxDataCacheValue();
    int64_t fixed_size = sizeof(*tx_data_cache_value);
    if (OB_FAIL(tx_data_cache_value->init(*this, buf + fixed_size, buf_len - fixed_size))) {
      STORAGE_LOG(WARN, "init tx data cache failed", KR(ret));
    } else {
      value = tx_data_cache_value;
    }
  }

  return ret;
}

int ObTxDataCacheValue::inner_deep_copy_(void *tx_data_buf, const ObTxData &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_data_buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid buf", KR(ret), KP(tx_data_buf), K(rhs));
  } else {
    tx_data_ = new (tx_data_buf) ObTxData();
    tx_data_->assign_without_undo(rhs);
    tx_data_->tx_data_allocator_ = rhs.tx_data_allocator_;
    tx_data_->op_allocator_ = rhs.op_allocator_;
    if (rhs.op_guard_.is_valid()) {
      ObTxDataOp *tx_data_op = new ((char*)tx_data_buf + TX_DATA_SLICE_SIZE) ObTxDataOp(tx_data_->tx_data_allocator_,
        tx_data_->op_allocator_);
      tx_data_->op_guard_.init(tx_data_op);
    }

    if (OB_LIKELY(!rhs.op_guard_.is_valid() || OB_ISNULL(rhs.op_guard_->get_undo_status_list().head_))) {
      // this tx data do not have undo status
    } else {
      undo_node_array_ = (ObUndoStatusNode *)((char *)tx_data_buf + TX_DATA_SLICE_SIZE + TX_DATA_SLICE_SIZE);
      // ignore mds op
      if (OB_FAIL(inner_deep_copy_undo_status_(rhs))) {
        STORAGE_LOG(WARN, "deep copy undo status node for tx data kv cache failed", KR(ret), K(rhs));
      }
    }
  }
  return ret;
}

int ObTxDataCacheValue::inner_deep_copy_undo_status_(const ObTxData &rhs)
{
  int ret = OB_SUCCESS;

  // use dummy head point to the first undo node
  ObUndoStatusNode dummy_head;
  dummy_head.next_ = rhs.op_guard_->get_undo_status_list().head_;
  ObUndoStatusNode *pre_node = &dummy_head;
  tx_data_->op_guard_->get_undo_status_list().undo_node_cnt_ = rhs.op_guard_->get_undo_status_list().undo_node_cnt_;
  for (int64_t i = 0; OB_SUCC(ret) && i < rhs.op_guard_->get_undo_status_list().undo_node_cnt_; i++) {
    ObUndoStatusNode *rhs_node = pre_node->next_;
    pre_node = rhs_node;
    if (OB_ISNULL(rhs_node)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "undo status list node count dismatach", KR(ret), K(i), K(rhs.op_guard_->get_undo_status_list().undo_node_cnt_));
    } else {
      undo_node_array_[i].assign_value(*rhs_node);
      undo_node_array_[i].next_ = nullptr;

      if (0 == i) {
        tx_data_->op_guard_->get_undo_status_list().head_ = undo_node_array_;
      } else {
        undo_node_array_[i-1].next_ = &undo_node_array_[i];
      }
    }
  }

  return ret;
}

int ObTxDataKVCache::get_row(const ObTxDataCacheKey &key, ObTxDataValueHandle &val_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tx data cache key", KR(ret), K(key));
  } else if (OB_FAIL(get(key, val_handle.value_, val_handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "get value from tx data kv cache failed", KR(ret), K(key));
    }
  } else if (OB_ISNULL(val_handle.value_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "get a nullptr from kv cache", KR(ret), K(key));
  }
  return ret;
}

int ObTxDataKVCache::put_row(const ObTxDataCacheKey &key, const ObTxDataCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tx data cache key or cache value", KR(ret), K(value));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    STORAGE_LOG(WARN, "put tx data cache row failed", KR(ret), K(key), K(value));
  } else {
    // put tx data cache row success
  }

  return ret;
}


}  // namespace storage
}  // namespace oceanbase
