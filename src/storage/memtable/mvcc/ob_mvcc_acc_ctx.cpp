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

#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
namespace oceanbase
{
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
}
}