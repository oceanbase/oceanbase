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

#include "ob_mvcc_acc_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
namespace oceanbase
{
using namespace transaction;
namespace memtable
{
int ObMvccAccessCtx::check_txn_status_if_read_uncommitted()
{
  int ret = OB_SUCCESS;
  if (snapshot_.tx_id_.is_valid() && mem_ctx_) {
    if (mem_ctx_->is_tx_rollbacked()) {
      if (mem_ctx_->is_for_replay()) {
        // goes here means the txn was killed due to LS's GC etc,
        // return NOT_MASTER
        ret = OB_NOT_MASTER;
      } else {
        // The txn has been killed during normal processing. So we return
        // OB_TRANS_KILLED to prompt this abnormal state.
        ret = OB_TRANS_KILLED;
        TRANS_LOG(WARN, "txn has terminated", K(ret), "tx_id", tx_id_);
      }
    }
  }
  return ret;
}

int ObMvccAccessCtx::get_write_seq(transaction::ObTxSEQ &seq) const
{
  int ret = OB_SUCCESS;
  // for update uk or pk, set branch part to 0, in orer to let tx-callback fall into single list
  if (tx_scn_.support_branch() && write_flag_.is_update_uk()) {
    const int branch = tx_scn_.get_branch();
    if (branch == 0) {
      seq = tx_scn_;
    } else if (OB_UNLIKELY(ObTxDesc::is_alloced_branch_id(branch))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "external branch not support concurrent update uk / pk", K(ret), KPC(this));
    } else {
      seq = ObTxSEQ(tx_scn_.get_seq(), 0);
    }
  } else {
    seq = tx_scn_;
  }
  return ret;
}
} // memtable
} // oceanbase
