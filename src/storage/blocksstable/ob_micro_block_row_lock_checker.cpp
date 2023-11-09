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
using namespace share;

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
    const ObRowHeader *row_header = nullptr;
    int64_t sql_sequence = 0;
    const int64_t rowkey_cnt = read_info_->get_schema_rowkey_count();
    memtable::ObMvccAccessCtx &ctx = context_->store_ctx_->mvcc_acc_ctx_;
    const transaction::ObTransID &read_trans_id = ctx.get_tx_id();
    int64_t trans_version = INT64_MAX;
    row = &row_;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to check row lock", K(ret), K_(macro_id));
        }
      } else if (OB_FAIL(reader_->get_multi_version_info(
                  current_,
                  rowkey_cnt,
                  row_header,
                  trans_version,
                  sql_sequence))) {
        LOG_WARN("failed to get multi version info", K(ret), K_(current), KPC(row_header),
                 KPC_(lock_state), K(sql_sequence), K_(macro_id));
      } else if (OB_ISNULL(row_header)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("row header is null", K(ret));
        // TODO(handora.qc): fix it
      } else if (OB_FAIL(lock_state_->trans_version_.convert_for_tx(trans_version))) {
        LOG_ERROR("convert failed", K(ret), K(trans_version));
      } else if (row_header->get_row_multi_version_flag().is_uncommitted_row()) {
        ObTxTableGuards tx_table_guards = ctx.get_tx_table_guards();
        ObTxTable *tx_table = nullptr;
        int64 read_epoch = ObTxTable::INVALID_READ_EPOCH;
        transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
        if (!tx_table_guards.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tx table guard is invalid", KR(ret), K(ctx));
        } else if (OB_FAIL(tx_table_guards.check_row_locked(read_trans_id,
                                                            row_header->get_trans_id(),
                                                            tx_sequence,
                                                            sstable_->get_end_scn(),
                                                            *lock_state_))) {
        } else if (lock_state_->is_locked_) {
          lock_state_->lock_dml_flag_ = row_header->get_row_flag().get_dml_flag();
        }

        STORAGE_LOG(DEBUG, "check row lock", K(ret), KPC_(range), K(read_trans_id), KPC(row_header),
                    K(tx_sequence), KPC_(lock_state));
        if (SCN::min_scn() != lock_state_->trans_version_ || // trans is commit
            lock_state_->is_locked_) {
          break;
        }
      } else { // committed row
        if (row_header->get_row_multi_version_flag().is_ghost_row()) {
          if (OB_UNLIKELY(!row_header->get_row_multi_version_flag().is_last_multi_version_row())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected row flag", K(ret), KPC(row_header));
          } else {
            ret = OB_ITER_END;
          }
        }
        current_++;
        break;
      }
      current_++;
    }
  }

  return ret;
}

}
}
