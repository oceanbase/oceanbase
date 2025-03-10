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

#include "ob_row_hash_holder_map.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"

namespace oceanbase
{
namespace memtable
{

int RowHolderMapper::KeyWrapper::compare(const KeyWrapper &other) const {
  int ret = 0;
  if (v_ == other.v_) {
    ret = 0;
  } else if (v_ > other.v_) {
    ret = 1;
  } else {
    ret = -1;
  }
  return ret;
}

int RowHolderMapper::init() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(holder_map_.init("HashHolder", MTL_ID()))) {
    DETECT_LOG(ERROR, "init holder map failed", KR(ret));
  }
  return ret;
}

void RowHolderMapper::clear()
{
  holder_map_.clear();
}

int64_t RowHolderMapper::list_count() const
{
  return holder_map_.count();
}

int64_t RowHolderMapper::node_count() const
{
  return ATOMIC_LOAD(&total_callback_cnt_);
}

void RowHolderMapper::periodic_tasks() {
  if (!share::detector::ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    int64_t count = this->list_count();
    if (count != 0) {
      clear();
      DETECT_LOG(INFO, "clear RowHolderMapper cause deadlock is disabled", K(count));
    }
  }
  int64_t key_count = holder_map_.count();
  int64_t bucket_count = holder_map_.get_bkt_cnt();
  int64_t callback_count = ATOMIC_LOAD(&total_callback_cnt_);
  DETECT_LOG(INFO, "dump RowHolderMapper info", K(key_count), K(bucket_count), K(callback_count));
}

void RowHolderMapper::insert_hash_holder(const uint64_t hash,
                                         ObTxCallbackHashHolderLinker &linker,
                                         bool reverse_insert) {
  int ret = OB_SUCCESS;
  struct OP {
    OP(int64_t &callback_cnt, ObTxCallbackHashHolderLinker &linker, bool reverse_insert)
    : total_callback_cnt_(callback_cnt), linker_(linker), reverse_insert_(reverse_insert) {}
    bool operator()(const KeyWrapper &key, const ObTxCallbackHashHolderList &value) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(const_cast<ObTxCallbackHashHolderList &>(value).insert_callback(&linker_, reverse_insert_))) {
        DETECT_LOG(WARN, "failed to insert callback", KR(ret), K_(linker), K_(reverse_insert));
      }
      return (ret == OB_SUCCESS);
    }
    int64_t &total_callback_cnt_;
    ObTxCallbackHashHolderLinker &linker_;
    bool reverse_insert_;
  } op(total_callback_cnt_, linker, reverse_insert);
  ObTxCallbackHashHolderList list;
  if (OB_FAIL(list.insert_callback(&linker, false))) {
    DETECT_LOG(WARN, "failed insert callback to list", KR(ret), K(hash));
  } else if (OB_FAIL(holder_map_.insert_or_operate(KeyWrapper(hash), list, op))) {
    linker.reset_registered();
    DETECT_LOG(WARN, "failed to operate key", KR(ret), K(hash));
  } else {
    linker.set_hash_key(hash);
    ATOMIC_INC(&total_callback_cnt_);
  }
  return;
}

void RowHolderMapper::erase_hash_holder_record(const uint64_t hash,
                                               ObTxCallbackHashHolderLinker &linker,
                                               bool reverse_find) {
  int ret = OB_SUCCESS;
  struct OP {
    OP(int64_t &callback_cnt, ObTxCallbackHashHolderLinker &linker, bool reverse_find)
    : total_callback_cnt_(callback_cnt), linker_(linker), reverse_find_(reverse_find) {}
    bool operator()(const KeyWrapper &key, const ObTxCallbackHashHolderList &value) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(const_cast<ObTxCallbackHashHolderList &>(value).erase_callback(&linker_, reverse_find_))) {
        DETECT_LOG(WARN, "failed to erase callback", KR(ret), K_(linker), K(reverse_find_));
      } else {
        ATOMIC_DEC(&total_callback_cnt_);
      }
      return (value.size() == 0);
    }
    int64_t &total_callback_cnt_;
    ObTxCallbackHashHolderLinker &linker_;
    bool reverse_find_;
  } op(total_callback_cnt_, linker, reverse_find);
  if (OB_FAIL(holder_map_.erase_if(KeyWrapper(hash), op))) {
    if (ret != OB_EAGAIN) {
      DETECT_LOG(WARN, "failed to operate key", KR(ret), K(hash));
    }
  }
  return;
}

int RowHolderMapper::get_hash_holder(uint64_t hash, RowHolderInfo &holder_info) {
  int ret = OB_SUCCESS;
  struct OP {
    OP(RowHolderInfo &holder_info) : holder_info_(holder_info) {}
    bool operator()(const KeyWrapper &key, ObTxCallbackHashHolderList &value) {
      int ret = OB_SUCCESS;
      ObTxCallbackHashHolderLinker *head = value.head();
      if (OB_ISNULL(head)) {
        ret = OB_ERR_UNEXPECTED;
        DETECT_LOG(ERROR, "head should not be NULL", KR(ret), K(key));
      } else if (OB_FAIL(head->get_holder_info(holder_info_))) {
        DETECT_LOG(WARN, "failed to get holder info", KR(ret), K(key));
      }
      return (ret == OB_SUCCESS);
    }
    RowHolderInfo &holder_info_;
  } op(holder_info);
  if (OB_FAIL(holder_map_.operate(KeyWrapper(hash), op))) {
    DETECT_LOG(WARN, "failed to operate key", KR(ret), K(hash));
  }
  return ret;
}

}
}