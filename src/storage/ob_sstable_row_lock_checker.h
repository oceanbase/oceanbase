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

#ifndef OB_SSTABLE_ROW_LOCK_CHECKER_H_
#define OB_SSTABLE_ROW_LOCK_CHECKER_H_

#include "ob_sstable_row_getter.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
using namespace transaction;
namespace storage {

class ObSSTableRowLockChecker : public ObSSTableRowGetter {
public:
  ObSSTableRowLockChecker();
  virtual ~ObSSTableRowLockChecker();
  bool is_curr_row_locked() const
  {
    return lock_state_.is_locked_;
  }
  bool is_locked_by_trans(const transaction::ObTransID trans_id)
  {
    return lock_state_.is_locked_ && trans_id == lock_state_.lock_trans_id_;
  }
  transaction::ObTransID& get_lock_trans_id()
  {
    return lock_state_.lock_trans_id_;
  }

protected:
  virtual int fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  virtual void set_row_snapshot(ObStoreRow& row) override;

private:
  ObStoreRow store_row_;
  ObStoreRowLockState lock_state_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_SSTABLE_ROW_LOCK_CHECKER_H_ */
