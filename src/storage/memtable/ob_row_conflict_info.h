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
#ifndef SRC_STORAGE_MEMTABLE_OB_ROW_CONFLICT_INFO_H
#define SRC_STORAGE_MEMTABLE_OB_ROW_CONFLICT_INFO_H
#include "lib/net/ob_addr.h"
#include "meta_programming/ob_mover.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/tx/ob_trans_define.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_unify_serialize.h"
#include "deps/oblib/src/common/meta_programming/ob_mover.h"
#include <utility>
#include "storage/tx/deadlock_adapter/ob_session_id_pair.h"

namespace oceanbase {
namespace storage {
struct ObRowConflictInfo
{
  OB_UNIS_VERSION(1);
public:
  ObRowConflictInfo()
  : conflict_happened_addr_(),
  conflict_ls_(),
  conflict_tablet_(),
  conflict_row_key_str_(),
  conflict_sess_id_pair_(),
  conflict_tx_scheduler_(),
  conflict_tx_id_(),
  conflict_tx_hold_seq_(),
  conflict_hash_(0),
  lock_seq_(0),
  abs_timeout_(0),
  lock_mode_(0),
  last_compact_cnt_(0),
  total_update_cnt_(0),
  assoc_sess_id_(0),
  holder_sess_id_(0),
  holder_tx_start_time_(0),
  client_session_id_(0) {}
  ~ObRowConflictInfo() = default;
  ObRowConflictInfo(const ObRowConflictInfo &) = delete;// disallow default copy
  ObRowConflictInfo &operator=(const ObRowConflictInfo &) = delete;// disallow default assign operator
  ObRowConflictInfo &operator=(meta::ObMover<ObRowConflictInfo> rhs_mover) {// support move assignment
    ObRowConflictInfo &rhs = rhs_mover.get_object();
    init(rhs.conflict_happened_addr_,
         rhs.conflict_ls_,
         rhs.conflict_tablet_,
         meta::ObMover<ObStringHolder>(rhs.conflict_row_key_str_),
         rhs.conflict_sess_id_pair_,
         rhs.conflict_tx_scheduler_,
         rhs.conflict_tx_id_,
         rhs.conflict_tx_hold_seq_);
    return *this;
  }
  int init(const common::ObAddr &, const share::ObLSID &,
           const common::ObTabletID &, meta::ObMover<ObStringHolder>,
           const transaction::SessionIDPair, const common::ObAddr &,
           const transaction::ObTransID &, const transaction::ObTxSEQ &) {
    int ret = OB_SUCCESS;
    return ret;
  }
  int assign(const ObRowConflictInfo &rhs) {
    int ret = OB_SUCCESS;
    return ret;
  }
  int assign(meta::ObMover<ObRowConflictInfo> rhs_rvalue) {// support move assign
    ObRowConflictInfo &rhs = rhs_rvalue.get_object();
    return init(rhs.conflict_happened_addr_,
                rhs.conflict_ls_,
                rhs.conflict_tablet_,
                meta::ObMover<ObStringHolder>(rhs.conflict_row_key_str_),
                rhs.conflict_sess_id_pair_,
                rhs.conflict_tx_scheduler_,
                rhs.conflict_tx_id_,
                rhs.conflict_tx_hold_seq_);
  }
  bool is_valid() const {
    return conflict_tx_scheduler_.is_valid() && conflict_tx_id_.is_valid();
  }
  TO_STRING_KV(K_(conflict_happened_addr), K_(conflict_ls), K_(conflict_tablet),
               K_(conflict_row_key_str), K_(conflict_sess_id_pair), K_(conflict_tx_scheduler),
               K_(conflict_tx_id), K_(conflict_tx_hold_seq), K_(conflict_hash), K_(lock_seq),
               K_(abs_timeout), K_(self_tx_id), K_(lock_mode), K_(last_compact_cnt), K_(total_update_cnt),
               K_(assoc_sess_id), K_(holder_sess_id), K_(holder_tx_start_time), K_(client_session_id));
  common::ObAddr conflict_happened_addr_;// distributed info
  share::ObLSID conflict_ls_;// resource related
  common::ObTabletID conflict_tablet_;// resource related
  common::ObStringHolder conflict_row_key_str_;// resource related
  transaction::SessionIDPair conflict_sess_id_pair_;// holder related
  common::ObAddr conflict_tx_scheduler_;// holder related
  transaction::ObTransID conflict_tx_id_;// holder related
  transaction::ObTxSEQ conflict_tx_hold_seq_;// holder use sql identified by this seq to add row lock
  uint64_t conflict_hash_; // conflict hash value
  int64_t lock_seq_; // lock wait slot sequence num, used for lock wait manager
  int64_t abs_timeout_; // query timeout ts
  transaction::ObTransID self_tx_id_; // tx id who acquires lock
  uint8_t lock_mode_; // used for table lock conflict
  int64_t last_compact_cnt_; // row compact cnt
  int64_t total_update_cnt_; // row update cnt
  uint32_t assoc_sess_id_; // trx's associate session id who acquires lock
  uint32_t holder_sess_id_; // lock holder session id
  int64_t holder_tx_start_time_; // lock holder trans start time
  uint32_t client_session_id_; // client session id
};
OB_SERIALIZE_MEMBER_TEMP(inline, ObRowConflictInfo, conflict_happened_addr_, conflict_ls_,
                         conflict_tablet_, conflict_row_key_str_, conflict_sess_id_pair_,
                         conflict_tx_scheduler_, conflict_tx_id_, conflict_tx_hold_seq_, conflict_hash_,
                         lock_seq_, abs_timeout_, self_tx_id_, lock_mode_, last_compact_cnt_,
                         total_update_cnt_, assoc_sess_id_, holder_sess_id_, holder_tx_start_time_, client_session_id_);
}
}
#endif