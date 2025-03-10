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

#ifndef OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_IPP_
#define OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_IPP_

#ifndef OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_IPP_
#define OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_IPP_
#include "ob_tx_callback_hash_holder_helper.h"
#endif

namespace oceanbase
{
namespace memtable
{

inline ObTxCallbackHashHolderList &ObTxCallbackHashHolderList::operator=(const ObTxCallbackHashHolderList &rhs) {
  int ret = OB_SUCCESS;
  sentinel_node_ = rhs.sentinel_node_;
  count_ = rhs.count_;
  ObTxCallbackHashHolderLinker *iter = &sentinel_node_;
  const ObTxCallbackHashHolderLinker *iter_end = &rhs.sentinel_node_;
  while (iter != iter_end) {
    ObTxCallbackHashHolderLinker *tmp_node = iter->older_node_;
    if (iter->older_node_ == &rhs.sentinel_node_) {
      iter->older_node_ = &sentinel_node_;
    }
    if (iter->newer_node_ == &rhs.sentinel_node_) {
      iter->newer_node_ = &sentinel_node_;
    }
    iter = tmp_node;
  }
  return *this;
}

template <typename OP>
int ObTxCallbackHashHolderList::for_each_node_(OP &op, IterDirection direction) {// normal iter is from newest to oldest
  int ret = OB_SUCCESS;
  ObTxCallbackHashHolderLinker *iter = ((direction == IterDirection::FROM_OLD_TO_NEW) ? sentinel_node_.newer_node_ : sentinel_node_.older_node_);
  ObTxCallbackHashHolderLinker *iter_end = &sentinel_node_;
  while (iter != iter_end) {
    if (OB_FAIL(op(*iter))) {
      break;
    } else if (direction == IterDirection::FROM_NEW_TO_OLD) {
      iter = iter->older_node_;
    } else {
      iter = iter->newer_node_;
    }
  };
  ret = ((OB_SUCCESS == ret) ? OB_ITER_END : ret);
  return ret;
}

inline ObTxCallbackHashHolderLinker *ObTxCallbackHashHolderList::head() {
  if (OB_UNLIKELY(count_ == 0)) {
    ob_abort();// this should not happen
  }
  return sentinel_node_.older_node_;
}

}
}

#endif