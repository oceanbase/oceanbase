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

#ifndef OB_ROW_CONFLICT_HANDLER_H_
#define OB_ROW_CONFLICT_HANDLER_H_

#include <stdint.h>
namespace oceanbase {
namespace memtable {
class ObMvccAccessCtx;
class ObMvccValueIterator;
}
namespace transaction {
class ObTransID;
}
namespace common {
class ObTabletID;
class ObStoreRowkey;
}
namespace share {
class ObLSID;
}
namespace storage {
class ObStoreRowLockState;
class ObTxTableGuard;

class ObRowConflictHandler {
public:
  // There are 2 cases that can lead to row conflict in foreign key constraint check:
  // Case 1: the row is locked, mainly beacuse there's an uncommitted transaction on it.
  // If the check meet this case, we should call post_row_read_conflict to put it into
  // lock_wait_mgr and register deadlock decetion;
  //
  // Case 2: the row is committed, but the trans_version on it is larger than the
  // snapshot_version of current transaction, it will cause tsc.
  // If the check meet this case, we should return error code to sql layer, and it will
  // choose to retry or throw an exception according to the isolation level.
  static int check_foreign_key_constraint_for_memtable(memtable::ObMvccValueIterator *value_iter,
                                                       storage::ObStoreRowLockState &lock_state);
  static int check_foreign_key_constraint_for_sstable(const storage::ObTxTableGuard &tx_table_guard,
                                                      const transaction::ObTransID &read_trans_id,
                                                      const transaction::ObTransID &data_trans_id,
                                                      const int64_t sql_sequence,
                                                      const int64_t trans_version,
                                                      const int64_t snapshot_version,
                                                      storage::ObStoreRowLockState &lock_state);
  // TODO(yichang): This function is refered to ObMemtable::post_row_write_conflict_,
  // but remove the mem_ctx and tx_ctx in the implement. I think ObMemtable can call
  // this function, too. But it seems there's still a need to use mem_ctx to record
  // some statistics. Maybe we can move these statistics to tx_desc then.
  static int post_row_read_conflict(memtable::ObMvccAccessCtx &acc_ctx,
                                    const common::ObStoreRowkey &row_key,
                                    storage::ObStoreRowLockState &lock_state,
                                    const common::ObTabletID tablet_id,
                                    const share::ObLSID ls_id,
                                    const int64_t last_compact_cnt,
                                    const int64_t total_trans_node_cnt);
};
}
}
#endif
