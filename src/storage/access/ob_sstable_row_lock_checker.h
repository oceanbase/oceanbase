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
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_lock_checker.h"

namespace oceanbase {
using namespace transaction;

namespace storage {

class ObSSTableRowLockChecker : public ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2, 2>> {
public:
  ObSSTableRowLockChecker();
  virtual ~ObSSTableRowLockChecker();
  virtual void reset() override;
  int check_row_locked(ObStoreRowLockState &lock_state);
protected:
  virtual int inner_open(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range) override;
private:
  OB_INLINE int init_micro_scanner();

private:
  const blocksstable::ObDatumRowkey *base_rowkey_;
  blocksstable::ObDatumRange multi_version_range_;
};

}
}
#endif /* OB_SSTABLE_ROW_LOCK_CHECKER_H_ */
