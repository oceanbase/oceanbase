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

#ifndef OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_
#define OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_

#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase {
namespace blocksstable {

class ObMicroBlockRowLockChecker : public ObMicroBlockRowScanner {
public:
  ObMicroBlockRowLockChecker(common::ObIAllocator &allocator);
  virtual ~ObMicroBlockRowLockChecker();
  virtual int get_next_row(const ObDatumRow *&row) override;
  OB_INLINE void set_lock_state(ObStoreRowLockState *lock_state)
  { lock_state_ = lock_state; }
private:
  ObStoreRowLockState *lock_state_;
};

}
}
#endif /* OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_ */
