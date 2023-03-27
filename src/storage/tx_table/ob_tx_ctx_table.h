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

#ifndef OCEANBASE_STORAGE_OB_TX_CTX_TABLE
#define OCEANBASE_STORAGE_OB_TX_CTX_TABLE

#include "share/ob_ls_id.h"
#include "storage/ob_i_store.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx_table/tx_table_local_buffer.h"

namespace oceanbase
{
namespace transaction
{
  class ObLSTxCtxMgr;
}
namespace storage
{

class ObTxCtxTableRecoverHelper
{
public:
  ObTxCtxTableRecoverHelper() : allocator_(), buf_(allocator_) { reset(); }
  ~ObTxCtxTableRecoverHelper() { destroy(); }
  void reset();
  void destroy();
  int recover(const blocksstable::ObDatumRow &row,
              ObTxDataTable &tx_data_table,
              transaction::ObLSTxCtxMgr *ls_tx_ctx_mgr);

  TO_STRING_KV(K_(in_multi_row_state), K_(prev_meta), K_(prev_end_pos));

private:
  int validate_extend_meta_(const ObTxCtxTableMeta &curr_meta);
  int append_curr_value_buf_(const char* in_buf, const int64_t in_buf_length);
  int buf_reserve_(const int64_t buf_length);
  char* get_buf_ptr_();
  void set_in_multi_row_state_();
  void clear_in_multi_row_state_();
  bool is_in_multi_row_state_() const;
  void finish_recover_one_tx_ctx_();
  virtual int recover_one_tx_ctx_(transaction::ObLSTxCtxMgr* ls_tx_ctx_mgr,
                                  ObTxCtxTableInfo& ctx_info);
private:
  ObArenaAllocator allocator_;
  bool in_multi_row_state_;
  struct ObTxCtxTableMeta prev_meta_;
  ObTxLocalBuffer buf_;
  int64_t prev_end_pos_;
  share::ObLSID ls_id_;
  ObTxCtxTableInfo ctx_info_;
};

//
// In OB 3.0, in order to support merge uncommitted txns, the concept of
// trans_status_table is proposed. Its purpose is to implement the checkpoint
// function of the database for uncommitted txns, that is, the txn log can be
// recycled before committing, thus supporting infinite transaction size for
// customers. For this we need to rely on following conditions:
//
// 1. Persist the new version of the data so as not to rely on the redo for the data
// 2. Persist the txn table so as not to rely on the redo of the log for txn state
// 3. Persist the old version of the data so as not to rely on the undo for the data
// 4. Persist the txn table so as not to rely on the undo of the log for txn state
// 5. Persist the txn table to maintain data filtering location (latest_log_id)
// 6. Persist the meta relies on freezing to maintain the playback location (recover_log_id)
//
// In OB 4.0, we reviewed the design of the txn state table based on the design
// of OB 3.0. We believe that the txn table requirements of redo and undo need
// to be decoupled. The txn table of REDO is only used for recovery during the
// redo process. Txns can be recovered without redo; Undo's transaction table
// life cycle will survive only to the recovery of uncommitted data.
//
// Therefore, we finally proposed three table designs: data table, txn context
// table and txn data table. Secondly We hope to maintain filter location
// and playback sites separately to modularly manage different table structures,
// remove dependencies between them, and freeze dumps independently.
//
class ObTxCtxTable
{
// TODO(handora.qc): Handle operator= error handling
public:
  ObTxCtxTable();
  ~ObTxCtxTable();

  int init(const share::ObLSID& ls_id);

  void reset();

  // offline means clean the in-memory state and we will later recover the state
  // using the on-disk state using interface online.
  int offline();

  ObTxCtxTable& operator=(const ObTxCtxTable& other);

  // We need the reference count to keep the life cycle of the ls_tx_ctx_mgr.
  // And the ObTxTable is a guard for automatical management of it.
  int acquire_ref_(const share::ObLSID& ls_id);

  int release_ref_();

  // We use the method to recover the tx_ctx_table for reboot.
  int recover(const blocksstable::ObDatumRow &row, ObTxDataTable &tx_data_table);

  int check_with_tx_data(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn);

  int dump_single_tx_data_2_text(const int64_t tx_id_int, FILE *fd);

public:
  transaction::ObLSTxCtxMgr *get_ls_tx_ctx_mgr();

private:
  share::ObLSID ls_id_;
  transaction::ObLSTxCtxMgr *ls_tx_ctx_mgr_;

private:
  ObTxCtxTableRecoverHelper recover_helper_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TX_CTX_TABLE
