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
#include "storage/access/ob_sstable_row_multi_scanner.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_lock_checker.h"

namespace oceanbase {
using namespace transaction;

namespace storage {

class ObSSTableRowLockChecker : public ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2, 2>>
{
public:
  ObSSTableRowLockChecker();
  virtual ~ObSSTableRowLockChecker();
  virtual void reset() override;
  int check_row_locked(
    const bool check_exist,
    const share::SCN &snapshot_version,
    ObStoreRowLockState &lock_state,
    ObRowState &row_state);
  inline void set_iter_type(bool check_exist)
  {
    if (check_exist) {
      type_ = ObStoreRowIterator::IteratorRowLockAndDuplicationCheck;
    } else {
      type_ = ObStoreRowIterator::IteratorRowLockCheck;
    }
  }
protected:
  virtual int inner_open(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range) override;
private:
  int init_micro_scanner();
private:
  const blocksstable::ObDatumRowkey *base_rowkey_;
  blocksstable::ObDatumRange multi_version_range_;
};

class ObSSTableRowLockMultiChecker : public ObSSTableRowScanner<>
{
public:
  ObSSTableRowLockMultiChecker();
  virtual ~ObSSTableRowLockMultiChecker();
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) override;
  int check_row_locked(
      const bool check_exist,
      const share::SCN &snapshot_version);
protected:
  virtual int fetch_row(
      ObSSTableReadHandle &read_handle,
      const ObDatumRow *&store_row) override;
private:
  int init_micro_scanner();
  int open_cur_data_block(ObSSTableReadHandle &read_handle);
};

}
}
#endif /* OB_SSTABLE_ROW_LOCK_CHECKER_H_ */
