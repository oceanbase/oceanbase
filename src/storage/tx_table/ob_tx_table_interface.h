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

#ifndef OCEANBASE_STORAGE_OB_TRANS_TABLE_INTERFACE_
#define OCEANBASE_STORAGE_OB_TRANS_TABLE_INTERFACE_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/function/ob_function.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_tx_data_functor.h"

namespace oceanbase
{
namespace storage
{
class ObTxTable;
class ObTxTableGuard
{

public:
  ObTxTableGuard() : tx_table_(nullptr), epoch_(-1), mini_cache_() {}
  ~ObTxTableGuard() { reset(); }

  ObTxTableGuard(const ObTxTableGuard &guard) : ObTxTableGuard() { *this = guard; }

  ObTxTableGuard &operator=(const ObTxTableGuard &guard)
  {
    reset();
    tx_table_ = guard.tx_table_;
    epoch_ = guard.epoch_;
    return *this;
  }

  void reset()
  {
    if (OB_NOT_NULL(tx_table_)) {
      tx_table_ = nullptr;
      epoch_ = -1;
    }
    mini_cache_.reset();
  }

  int init(ObTxTable *tx_table);
  bool is_valid() const { return nullptr != tx_table_; }

  ObTxTable *get_tx_table() const { return tx_table_; }

public: // dalegate functions
  int check_row_locked(const transaction::ObTransID &read_tx_id,
                       const transaction::ObTransID data_tx_id,
                       const transaction::ObTxSEQ &sql_sequence,
                       storage::ObStoreRowLockState &lock_state);

  int check_sql_sequence_can_read(const transaction::ObTransID tx_id, const transaction::ObTxSEQ &sql_sequence, bool &can_read);

  int get_tx_state_with_scn(const transaction::ObTransID tx_id,
                            const share::SCN scn,
                            int64_t &state,
                            share::SCN &trans_version);

  int try_get_tx_state(const transaction::ObTransID tx_id,
                       int64_t &state,
                       share::SCN &trans_version,
                       share::SCN &recycled_scn);

  int lock_for_read(const transaction::ObLockForReadArg &lock_for_read_arg,
                    bool &can_read,
                    share::SCN &trans_version,
                    bool &is_determined_state);

  int lock_for_read(const transaction::ObLockForReadArg &lock_for_read_arg,
                    bool &can_read,
                    share::SCN &trans_version,
                    bool &is_determined_state,
                    ObCleanoutOp &cleanout_op,
                    ObReCheckOp &recheck_op);

  int cleanout_tx_node(const transaction::ObTransID tx_id,
                       memtable::ObMvccRow &value,
                       memtable::ObMvccTransNode &tnode,
                       const bool need_row_latch);

  int get_recycle_scn(share::SCN &recycle_scn);

  int self_freeze_task();

  bool check_ls_offline();

  void reuse() { mini_cache_.reset(); }

  TO_STRING_KV(KP_(tx_table), K_(epoch), K(mini_cache_));

private:
  ObTxTable *tx_table_;
  int64_t epoch_;
  ObTxDataMiniCache mini_cache_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
