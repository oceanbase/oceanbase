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
#include "ob_row_hash_holder_info.h"
#include "lib/ob_errno.h"
#include "ob_row_hash_holder_map.h"
#include "share/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "share/ob_task_define.h"

namespace oceanbase
{
namespace memtable
{

void RowHolderList::clear_all_nodes_(const uint64_t hash) {
  RowHolderNode *prev_node = list_tail_;
  while (prev_node) {
    RowHolderNode *current_node = prev_node;
    prev_node = current_node->prev_;
    map_->destroy(current_node, hash);
  }
  list_tail_ = nullptr;
}

RowHolderList::~RowHolderList() {
  DETECT_LOG(DEBUG, "list to destroy", K(*this));
  clear_all_nodes_(hash_val_);
  new (this) RowHolderList();
}

int RowHolderList::insert(const uint64_t hash,
                          const transaction::ObTransID &tx_id,
                          const transaction::ObTxSEQ &seq,
                          const share::SCN &scn) {
  #define PRINT_WRAPPER KR(ret), K(tx_id), K(seq), K(scn), KP(new_node), K(*this)
  int ret = OB_SUCCESS;
  RowHolderNode *new_node = nullptr;
  if (OB_UNLIKELY(!tx_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(ERROR, "insert invalid node", PRINT_WRAPPER);
  } else if (OB_FAIL(map_->create(new_node, hash, tx_id, seq, scn))) {// alloc object from cache/heap
    DETECT_LOG(WARN, "fail to create node", PRINT_WRAPPER);
  } else if (OB_FAIL(insert_new_node_into_list_(hash, new_node))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      DETECT_LOG(WARN, "fail to insert node", PRINT_WRAPPER);
    }
    map_->destroy(new_node, hash);
  } else {
    DETECT_LOG(DEBUG, "succ to insert node", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RowHolderList::erase(const uint64_t hash,
                         const transaction::ObTransID tx_id,
                         const transaction::ObTxSEQ seq) {
  #define PRINT_WRAPPER KR(ret), K(tx_id), K(seq), K(*this)
  int ret = OB_SUCCESS;
  if (OB_ISNULL(list_tail_)) {
    ret = OB_ENTRY_NOT_EXIST;
    DETECT_LOG(WARN, "list is empty", PRINT_WRAPPER);
  } else if (OB_UNLIKELY(list_tail_->holder_info_.tx_id_ != tx_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    DETECT_LOG(WARN, "list's tx_id not equal to tx_id, maybe meet hash conflict", PRINT_WRAPPER);
  } else {
    // find node to delete
    RowHolderNode *deleted_node = list_tail_;
    RowHolderNode **link_deleted_ptr = &list_tail_;
    while (OB_NOT_NULL(deleted_node)) {
      if (deleted_node->holder_info_.seq_ == seq) {
        break;
      } else {
        link_deleted_ptr = &(deleted_node->prev_);
        deleted_node = deleted_node->prev_;
      }
    }
    // do delete action
    if (OB_ISNULL(deleted_node)) {
      ret = OB_ENTRY_NOT_EXIST;
      DETECT_LOG(WARN, "not found this seq in list", PRINT_WRAPPER);
    } else {
      *link_deleted_ptr = deleted_node->prev_;
      map_->destroy(deleted_node, hash);
      DETECT_LOG(DEBUG, "erase node", K(link_deleted_ptr), KP(list_tail_), PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RowHolderList::insert_new_node_into_list_(const uint64_t hash, RowHolderNode *new_node) {
  #define PRINT_WRAPPER KR(ret), K(*new_node), K(*this)
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(list_tail_ && list_tail_->holder_info_.tx_id_ != new_node->holder_info_.tx_id_)) {// row key hash conflict
    bool need_replace = false;
    bool head_scn_valid = list_tail_->holder_info_.scn_.is_valid();
    bool new_node_scn_valid = new_node->holder_info_.scn_.is_valid();
    if (!head_scn_valid && !new_node_scn_valid) {// both write on leader
      need_replace = true;
      DETECT_LOG(WARN, "tx_id is not same, maybe meet rowkey hash conflict, reconstruct list", PRINT_WRAPPER);
    } else if (!head_scn_valid && new_node_scn_valid) {// head write on leader, new node write on follower, replace
      need_replace = true;
      DETECT_LOG(WARN, "tx_id is not same, and list head scn is not valid, new node scn is valid, maybe meet switch leader", PRINT_WRAPPER);
    } else if (head_scn_valid && !new_node_scn_valid) {// head write on follower, new node write on leader
      need_replace = true;
      DETECT_LOG(WARN, "tx_id is not same, and list head scn is valid, new node scn is not valid, this is not expected", PRINT_WRAPPER);
    } else {// both write on follower, maybe meet concurrent replay
      if (new_node->holder_info_.scn_ > list_tail_->holder_info_.scn_) {
        need_replace = true;
      } else {
        ret = OB_NO_NEED_UPDATE;
        DETECT_LOG(INFO, "tx_id is not same, but it's scn is smaller than current lis newest node, so no need insert", PRINT_WRAPPER);
      }
    }
    if (OB_SUCC(ret) && need_replace) {
      clear_all_nodes_(hash);
      ret = insert_new_node_into_list_(hash, new_node); // insert into empty list again
    }
  } else {
    RowHolderNode *iter = list_tail_;
    RowHolderNode **link_new_node_ptr = &list_tail_;
    while (OB_NOT_NULL(iter) && OB_SUCC(ret)) {// try insert to middle
      if (iter->holder_info_.seq_ > new_node->holder_info_.seq_) {// keep iterating
        link_new_node_ptr = &iter->prev_;
        iter = iter->prev_;
      } else {// iter is first node which seq is not greater than new_node(same seq may write more than one node)
        *link_new_node_ptr = new_node;
        new_node->prev_ = iter;
        DETECT_LOG(DEBUG, "link new node to list", PRINT_WRAPPER);
        break;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(iter)) {// insert failed, append to tail
      *link_new_node_ptr = new_node;
      DETECT_LOG(DEBUG, "link new node to list head", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int64_t RowHolderList::size() const {
  int64_t size = 0;
  if (list_tail_) {
    const RowHolderNode *iter = list_tail_;
    while (OB_NOT_NULL(iter)) {
      iter = iter->prev_;
      ++size;
    }
  }
  return size;
}

int64_t RowHolderList::to_string(char *buf, const int64_t buf_len) const {
  ObCStringHelper helper;
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "this:0x%lx, ", (unsigned long)this);
  if (!list_tail_) {
    common::databuff_printf(buf, buf_len, pos, "hash:%ld, EMPTY", hash_val_);
  } else {
    common::databuff_printf(buf, buf_len, pos, "hash:%ld, [%lx:%s]", hash_val_, (unsigned long)list_tail_, helper.convert(list_tail_->holder_info_));
    const RowHolderNode *node = list_tail_->prev_;
    constexpr int64_t MAX_PRINT_ITEM = 32;
    int64_t iter_item_cnt = 1;// head node has been printed
    while (OB_NOT_NULL(node) && ++iter_item_cnt) {
      if (OB_LIKELY(iter_item_cnt <= MAX_PRINT_ITEM)) {
        common::databuff_printf(buf, buf_len, pos, "<-[%lx:%s]", (unsigned long)node, helper.convert(node->holder_info_));
      }
      node = node->prev_;
    }
    if (iter_item_cnt > MAX_PRINT_ITEM) {
      common::databuff_printf(buf, buf_len, pos,
                              "<-%ld has printed, but there are %ld more...",
                              MAX_PRINT_ITEM, iter_item_cnt - MAX_PRINT_ITEM);
    }
  }
  return pos;
}

}
}
