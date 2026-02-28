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

#include "ob_tx_callback_hash_holder_helper.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "share/scn.h"
#include "storage/memtable/hash_holder/ob_row_holder_info.h"
#include <cassert>
#include "storage/memtable/mvcc/ob_mvcc.h"

namespace oceanbase
{
namespace memtable
{
struct FindNotNewerCallBackOP { // should be applied in for_each_node_from_new_to_old_ with reverse = false
  FindNotNewerCallBackOP(ObTxCallbackHashHolderLinker &node, ObTxCallbackHashHolderLinker *iter_end)
  : node_(node),
  find_node_(iter_end) {}
  int operator()(ObTxCallbackHashHolderLinker &node) {
    int ret = OB_SUCCESS;
    int result = 0;
    if (OB_FAIL(node.compare(node_, result))) {
      DETECT_LOG(WARN, "failed to get lhs holder info", KR(ret), K(node), K(node_), KP_(find_node));
    } else if (result <= 0) {
      find_node_ = &node;
      ret = OB_ITER_STOP;
    }
    return ret;
  }
  ObTxCallbackHashHolderLinker &node_;
  ObTxCallbackHashHolderLinker *find_node_;
};

struct FindNewerCallBackOP { // should be applied in for_each_node_from_new_to_old_ with reverse = true
  FindNewerCallBackOP(ObTxCallbackHashHolderLinker &node, ObTxCallbackHashHolderLinker *iter_end)
  : node_(node),
  find_node_(iter_end) {}
  int operator()(ObTxCallbackHashHolderLinker &node) {
    int ret = OB_SUCCESS;
    int result = 0;
    if (OB_FAIL(node.compare(node_, result))) {
      DETECT_LOG(WARN, "failed to get lhs holder info", KR(ret), K(node), K(node_), KP_(find_node));
    } else if (result > 0) {
      find_node_ = &node;
      ret = OB_ITER_STOP;
    }
    return ret;
  }
  ObTxCallbackHashHolderLinker &node_;
  ObTxCallbackHashHolderLinker *find_node_;
};

struct FindCallBackOP {
  FindCallBackOP(ObTxCallbackHashHolderLinker &node)
  : node_(node) {}
  int operator()(ObTxCallbackHashHolderLinker &node) {
    int ret = OB_SUCCESS;
    if (&node == &node_) {
      ret = OB_ITER_STOP;
    }
    return ret;
  }
  ObTxCallbackHashHolderLinker &node_;
};

int ObTxCallbackHashHolderLinker::get_holder_info(RowHolderInfo &holder_info) const
{
  ObITransCallback *trans_callback = CONTAINER_OF(this, ObITransCallback, hash_holder_linker_);
  return trans_callback->get_holder_info(holder_info);
}

int ObTxCallbackHashHolderLinker::compare(const ObTxCallbackHashHolderLinker &rhs, int &result)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(rhs), K(lhs_info), K(rhs_info)
  int ret = OB_SUCCESS;
  RowHolderInfo lhs_info;
  RowHolderInfo rhs_info;
  if (OB_FAIL(get_holder_info(lhs_info))) {
    DETECT_LOG(WARN, "failed to get lhs holder info", PRINT_WRAPPER);
  } else if (OB_FAIL(rhs.get_holder_info(rhs_info))) {
    DETECT_LOG(WARN, "failed to get lhs holder info", PRINT_WRAPPER);
  } else {
    if (lhs_info.scn_ < rhs_info.scn_) {
      result = -1;
    } else if (lhs_info.scn_ > rhs_info.scn_) {
      result = 1;
    } else {
      result = 0;
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ObTxCallbackHashHolderLinker::link_newer_node_(ObTxCallbackHashHolderLinker *new_node)
{
  ObTxCallbackHashHolderLinker *temp_newer = this->newer_node_;
  temp_newer->older_node_ = new_node;
  this->newer_node_ = new_node;
  new_node->older_node_ = this;
  new_node->newer_node_ = temp_newer;
}

void ObTxCallbackHashHolderLinker::link_older_node_(ObTxCallbackHashHolderLinker *old_node)
{
  ObTxCallbackHashHolderLinker *temp_older = this->older_node_;
  temp_older->newer_node_ = old_node;
  this->older_node_ = old_node;
  old_node->newer_node_ = this;
  old_node->older_node_ = temp_older;
}

int64_t ObTxCallbackHashHolderList::to_string(char *buffer, const int64_t buffer_len) const
{
  constexpr int64_t MAX_PRINT_NUM = 16;
  int64_t pos = 0;
  RowHolderInfo holder_info;
  databuff_printf(buffer, buffer_len, pos, "hash:%lu", sentinel_node_.older_node_->get_hash_key());
  ObTxCallbackHashHolderLinker *iter = sentinel_node_.older_node_;
  int64_t iter_num = MIN(MAX_PRINT_NUM, count_);
  for (int64_t idx = 0; idx < iter_num; ++idx) {
    if (idx == 0) {
      databuff_printf(buffer, buffer_len, pos, "(new->old){");
    } else {
      databuff_printf(buffer, buffer_len, pos, "->{");
    }
    iter->get_holder_info(holder_info);
    databuff_printf(buffer, buffer_len, pos, "txid:%ld, seq:%ld, scn:%ld", holder_info.tx_id_.get_id(), holder_info.seq_.get_seq(), holder_info.scn_.get_val_for_gts());
    databuff_printf(buffer, buffer_len, pos, "}");
    iter = iter->older_node_;
  }
  if (iter_num < MAX_PRINT_NUM) {
    databuff_printf(buffer, buffer_len, pos, "(print all %ld)", count_);
  } else {
    databuff_printf(buffer, buffer_len, pos, "...(print %ld of %ld)", iter_num, count_);
  }
  return pos;
}

int ObTxCallbackHashHolderList::insert_callback(ObTxCallbackHashHolderLinker *new_callback, bool reverse_find_position)
{
  #define PRINT_WRAPPER KR(ret), K_(sentinel_node), KPC(new_callback), K(reverse_find_position)
  int ret = OB_SUCCESS;
  if (OB_FAIL((reverse_find_position ? reverse_insert_callback_(new_callback) : insert_callback_(new_callback)))) {
    DETECT_LOG(WARN, "insert callback failed", PRINT_WRAPPER);
  } else {
    count_++;
    DETECT_LOG(DEBUG, "insert callback success", K(*this), PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTxCallbackHashHolderList::insert_callback_(ObTxCallbackHashHolderLinker *new_callback)
{
  int ret = OB_SUCCESS;
  FindNotNewerCallBackOP find_not_newer_callback_op(*new_callback, &sentinel_node_);
  if (OB_FAIL(for_each_node_(find_not_newer_callback_op, IterDirection::FROM_NEW_TO_OLD))) {
    if (OB_ITER_STOP == ret || OB_ITER_END == ret) {
      find_not_newer_callback_op.find_node_->link_newer_node_(new_callback);
      ret = OB_SUCCESS;
    } else {
      DETECT_LOG(WARN, "for each node return ERROR", KR(ret), KPC(sentinel_node_.older_node_), KPC(new_callback));
    }
  } else {
    DETECT_LOG(ERROR, "for each node return OB_SUCCESS", KR(ret), KPC(sentinel_node_.older_node_), KPC(new_callback));
  }
  return ret;
}

int ObTxCallbackHashHolderList::reverse_insert_callback_(ObTxCallbackHashHolderLinker *new_callback)
{
  int ret = OB_SUCCESS;
  FindNewerCallBackOP find_newer_callback_op(*new_callback, &sentinel_node_);
  if (OB_FAIL(for_each_node_(find_newer_callback_op, IterDirection::FROM_OLD_TO_NEW))) {
    if (OB_ITER_STOP == ret || OB_ITER_END == ret) {
      find_newer_callback_op.find_node_->link_older_node_(new_callback);
      ret = OB_SUCCESS;
    } else {
      DETECT_LOG(WARN, "for each node return ERROR", KR(ret), KPC(sentinel_node_.older_node_), KPC(new_callback));
    }
  } else {
    DETECT_LOG(ERROR, "for each node return OB_SUCCESS", KR(ret), KPC(sentinel_node_.older_node_), KPC(new_callback));
  }
  return ret;
}

int ObTxCallbackHashHolderList::erase_callback(ObTxCallbackHashHolderLinker *callback, bool reverse_find_position)
{
  #define PRINT_WRAPPER KR(ret), K_(sentinel_node), KPC(callback), K(reverse_find_position), K(*this)
  int ret = OB_SUCCESS;
  FindCallBackOP find_callback_op(*callback);
  IterDirection iter_direction = reverse_find_position ? IterDirection::FROM_OLD_TO_NEW : IterDirection::FROM_NEW_TO_OLD;
  if (OB_FAIL(for_each_node_(find_callback_op, iter_direction))) {
    if (OB_ITER_END == ret) {
      DETECT_LOG(DEBUG, "node not found", PRINT_WRAPPER);
    } else if (OB_ITER_STOP != ret) {
      DETECT_LOG(WARN, "find node meet error", PRINT_WRAPPER);
    } else {
      callback->newer_node_->older_node_ = callback->older_node_;
      callback->older_node_->newer_node_ = callback->newer_node_;
      callback->newer_node_ = nullptr;
      callback->older_node_ = nullptr;
      count_--;
      ret = OB_SUCCESS;
      DETECT_LOG(DEBUG, "erase callback", PRINT_WRAPPER);
    }
  } else {
    DETECT_LOG(ERROR, "for each node return OB_SUCCESS", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}