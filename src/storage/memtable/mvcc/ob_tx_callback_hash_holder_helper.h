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

#ifndef OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_
#define OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "share/ob_errno.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace memtable
{

class RowHolderInfo;
class ObMvccTransCallBack;
class ObTxCallbackHashHolderList;

// this struct will be linked as a cycle list
class ObTxCallbackHashHolderLinker {
  friend class ObTxCallbackHashHolderList;
public:
  ObTxCallbackHashHolderLinker() : hash_key_(0), newer_node_(nullptr), older_node_(nullptr) {}
  ObTxCallbackHashHolderLinker(const ObTxCallbackHashHolderLinker &) = default;
  ObTxCallbackHashHolderLinker &operator=(const ObTxCallbackHashHolderLinker &) = default;
  ~ObTxCallbackHashHolderLinker() { hash_key_ = 0; newer_node_ = nullptr; older_node_ = nullptr; }
  // A < B means A is older, i.e: A's redo_scn lower than B's redo scn
  // result > 0 means this > rhs;
  // result == 0 means this == rhs;
  // result < 0 means this < rhs;
  int compare(const ObTxCallbackHashHolderLinker &rhs, int &result);
  TO_STRING_KV(K_(hash_key), KP_(newer_node), KP_(older_node));
public:
  void set_hash_key(const uint64_t hash_key) { hash_key_ = hash_key; }
  uint64_t get_hash_key() { return hash_key_; }
  bool is_registerd() const { return OB_NOT_NULL(newer_node_) && OB_NOT_NULL(older_node_); }
  void reset_registered() { newer_node_ = nullptr; older_node_ = nullptr; }
  int get_holder_info(RowHolderInfo &holder_info) const;
protected:
  void link_newer_node_(ObTxCallbackHashHolderLinker *new_node);
  void link_older_node_(ObTxCallbackHashHolderLinker *new_node);
protected:
  uint64_t hash_key_;// this is calculated by tablet_id and RowKey, used to find self in map
  ObTxCallbackHashHolderLinker *newer_node_;
  ObTxCallbackHashHolderLinker *older_node_;
};

class ObTxCallbackHashHolderList {
public:
  enum class IterDirection {
    FROM_NEW_TO_OLD = 0,
    FROM_OLD_TO_NEW = 1,
  };
public:
  ObTxCallbackHashHolderList() : sentinel_node_(), count_(0) {
    sentinel_node_.newer_node_ = &sentinel_node_;
    sentinel_node_.older_node_ = &sentinel_node_;
  }
  ObTxCallbackHashHolderList(const ObTxCallbackHashHolderList &rhs) { *this = rhs; }
  ObTxCallbackHashHolderList &operator=(const ObTxCallbackHashHolderList &rhs);
  ~ObTxCallbackHashHolderList() { count_ = 0; }
public:
  // append logic on leader, insert logic for follower
  // callback ordered by scn
  int insert_callback(ObTxCallbackHashHolderLinker *new_callback, bool reverse_find_position);
  int erase_callback(ObTxCallbackHashHolderLinker *new_callback, bool reverse_find_position);
  int64_t size() const { return count_; }
  ObTxCallbackHashHolderLinker *head();
  int64_t to_string(char *buffer, const int64_t buffer_len) const;
private:
  template <typename OP>
  int for_each_node_(OP &op, IterDirection direction);
  int insert_callback_(ObTxCallbackHashHolderLinker *new_callback);
  int reverse_insert_callback_(ObTxCallbackHashHolderLinker *new_callback);
private:
  ObTxCallbackHashHolderLinker sentinel_node_;// newer_node is newest_, older_node_ is oldest
  int64_t count_;
};

}; // namespace memtable
}; // namespace oceanbase

#ifndef OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_IPP_
#define OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_IPP_
#include "ob_tx_callback_hash_holder_helper.ipp"
#endif

#endif // OCEANBASE_MEMTABLE_TX_CALLBACK_HASH_HOLDER_HELPER_H_