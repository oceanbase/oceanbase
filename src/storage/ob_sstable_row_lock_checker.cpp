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

#include "ob_sstable_row_lock_checker.h"
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

ObSSTableRowLockChecker::ObSSTableRowLockChecker() : store_row_(), lock_state_()
{}

ObSSTableRowLockChecker::~ObSSTableRowLockChecker()
{}

void ObSSTableRowLockChecker::set_row_snapshot(ObStoreRow& row)
{
  if (row.snapshot_version_ == INT64_MAX) {
    row.snapshot_version_ = sstable_->get_meta().max_merged_trans_version_;
  }
}

int ObSSTableRowLockChecker::fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  int64_t trans_version = 0;
  if (OB_SUCC(check_row_locked(read_handle, lock_state_))) {
    store_row_.snapshot_version_ = lock_state_.trans_version_;
    store_row = &store_row_;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
