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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_INFO_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_INFO_H_

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "storage/tx/ob_trans_define_v4.h"
#include <utility>
#include "ob_row_hash_holder_monitor.h"

namespace oceanbase
{
namespace memtable
{

class ObMemtableKey;
class ObMvccTransNode;
class RowHolderMapper;

struct RowHolderInfo {// standard layout
  RowHolderInfo() : tx_id_(), seq_(), scn_() {}
  ~RowHolderInfo() { new (this) RowHolderInfo(); }
  RowHolderInfo(const transaction::ObTransID &tx_id,
                const transaction::ObTxSEQ &seq,
                const share::SCN &scn)
  : tx_id_(tx_id), seq_(seq), scn_(scn) {}
  RowHolderInfo(const RowHolderInfo &) = default;
  RowHolderInfo &operator=(const RowHolderInfo &) = default;
  bool is_valid() const { return tx_id_.is_valid(); }
  TO_STRING_KV(K_(tx_id), K_(seq), K_(scn));
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ seq_;
  share::SCN scn_;
};

struct RowHolderNode {// standard layout
  RowHolderNode() : holder_info_(), prev_(nullptr) {}
  ~RowHolderNode() {
    DETECT_LOG(DEBUG, "node to destroy", KP(this), K(*this));
    new (this) RowHolderNode();
  }
  RowHolderNode(const RowHolderNode &rhs) = delete;
  RowHolderNode &operator=(const RowHolderNode &rhs) = delete;
  RowHolderNode(const transaction::ObTransID &tx_id,
                const transaction::ObTxSEQ &seq,
                const share::SCN &scn)
  : holder_info_(tx_id, seq, scn), prev_(nullptr) {
    DETECT_LOG(DEBUG, "node create", K(*this));
  }
  bool is_valid() const { return holder_info_.is_valid(); }
  TO_STRING_KV(KP(this), K_(holder_info), K_(prev));
  RowHolderInfo holder_info_;
  RowHolderNode *prev_;
};

struct RowHolderList {// RAII
  RowHolderList() : list_tail_(nullptr), next_list_(nullptr), hash_val_(0), map_(nullptr) {}
  RowHolderList(RowHolderMapper *map) : list_tail_(), next_list_(nullptr), hash_val_(0), map_(map) {
    DETECT_LOG(DEBUG, "list create", K(*this));
  }
  ~RowHolderList();
  RowHolderList(const RowHolderList &rhs) = delete;
  RowHolderList &operator=(const RowHolderList &rhs) = delete;
  int insert(const uint64_t hash,
             const transaction::ObTransID &tx_id,
             const transaction::ObTxSEQ &seq,
             const share::SCN &scn);// append only for apply, insert or appened for replay
  int erase(const uint64_t hash,
            const transaction::ObTransID tx_id,
            const transaction::ObTxSEQ seq);// for row/lock released
  int64_t size() const;// O(N), just for unittest
  bool is_empty() const { return (list_tail_ == nullptr); }
  int insert_new_node_into_list_(const uint64_t hash, RowHolderNode *new_node);
  void clear_all_nodes_(const uint64_t hash);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  RowHolderNode *list_tail_;
  RowHolderList *next_list_;
  uint64_t hash_val_;
  RowHolderMapper *map_;
};

}
}
#endif