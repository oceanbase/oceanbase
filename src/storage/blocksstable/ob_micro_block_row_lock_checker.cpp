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

namespace oceanbase {
namespace blocksstable {

ObMicroBlockRowLockChecker::ObMicroBlockRowLockChecker()
{}

ObMicroBlockRowLockChecker::~ObMicroBlockRowLockChecker()
{}

int ObMicroBlockRowLockChecker::check_row_locked(const transaction::ObTransStateTableGuard& trans_table_guard,
    const transaction::ObTransID& read_trans_id, const common::ObStoreRowkey& rowkey,
    const ObFullMacroBlockMeta& full_meta, const ObMicroBlockData& block_data,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the micro block row lock checker has not been inited, ", K(ret));
  } else if (OB_ISNULL(context_) || OB_ISNULL(context_->store_ctx_) || OB_ISNULL(context_->store_ctx_->mem_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem ctx is NULL", K(ret));
  } else if (OB_FAIL(prepare_reader(full_meta))) {
    LOG_WARN("failed to prepare reader", K(ret));
  } else {
    ret = reader_->check_row_locked(*context_->store_ctx_->mem_ctx_,
        trans_table_guard,
        read_trans_id,
        block_data,
        rowkey,
        full_meta,
        rowkey_helper,
        lock_state);
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
