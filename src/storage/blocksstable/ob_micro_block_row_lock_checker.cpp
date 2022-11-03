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

#define USING_LOG_PREFIX STORAGE
#include "ob_micro_block_row_lock_checker.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase {
namespace blocksstable {

ObMicroBlockRowLockChecker::ObMicroBlockRowLockChecker(common::ObIAllocator &allocator) :
    ObMicroBlockRowScanner(allocator),
    lock_state_(nullptr)

{
}

ObMicroBlockRowLockChecker::~ObMicroBlockRowLockChecker()
{
}

int ObMicroBlockRowLockChecker::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == read_info_ || nullptr == lock_state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null param", K(ret), KP_(read_info), KP_(lock_state));
  } else {
    transaction::ObTransID trans_id;
    int64_t sql_sequence = 0;
    ObMultiVersionRowFlag flag;
    const int64_t rowkey_cnt = read_info_->get_schema_rowkey_count();
    memtable::ObMvccAccessCtx &ctx = context_->store_ctx_->mvcc_acc_ctx_;
    const transaction::ObTransID &read_trans_id = ctx.get_tx_id();
    row = &row_;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to check row lock", K(ret), K_(macro_id));
        }
      } else if (OB_FAIL(reader_->get_multi_version_info(
                  current_,
                  rowkey_cnt,
                  flag,
                  trans_id,
                  lock_state_->trans_version_,
                  sql_sequence))) {
        LOG_WARN("failed to get multi version info", K(ret), K_(current), K(flag), K(trans_id),
                 KPC_(lock_state), K(sql_sequence), K_(macro_id));
      } else if (flag.is_uncommitted_row()) {
        ObTxTableGuard tx_table_guard = ctx.get_tx_table_guard();
        ObTxTable *tx_table = nullptr;
        int64 read_epoch = ObTxTable::INVALID_READ_EPOCH;
        if (!tx_table_guard.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tx table guard is invalid", KR(ret), K(ctx));
        } else if (FALSE_IT(tx_table = tx_table_guard.get_tx_table())) {
        } else if (FALSE_IT(read_epoch = tx_table_guard.epoch())) {
        } else if (OB_FAIL(
                       tx_table->check_row_locked(read_trans_id, trans_id, sql_sequence, read_epoch, *lock_state_))) {
        }

        STORAGE_LOG(DEBUG, "check row lock", K(ret), KPC_(range), K(read_trans_id), K(trans_id),
                    K(sql_sequence), KPC_(lock_state));
        if (0 != lock_state_->trans_version_ || // trans is commit
            lock_state_->is_locked_) {
          break;
        }
      } else { // committed row
        break;
      }
      current_++;
    }
  }

  return ret;
}

}
}
