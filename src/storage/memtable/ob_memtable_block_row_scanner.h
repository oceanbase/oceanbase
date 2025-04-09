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

#ifndef OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_ROW_SCANNER_H_
#define OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_ROW_SCANNER_H_

#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase {
using namespace blocksstable;

namespace memtable {
class ObMemtableSingleRowReader;

// This is an abstract middle layer in order to reuse logic in ObIMicroBlockRowScanner
class ObMemtableBlockRowScanner : public blocksstable::ObIMicroBlockRowScanner {
public:
  ObMemtableBlockRowScanner(common::ObIAllocator &allocator)
      : ObIMicroBlockRowScanner(allocator)
  {}
  virtual ~ObMemtableBlockRowScanner()
  {}

  int init(const storage::ObTableIterParam &param,
           storage::ObTableAccessContext &context,
           ObMemtableSingleRowReader &single_row_reader);
  virtual int get_next_rows() override;

protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override;

private:
  int prefetch();
  int fetch_row(const ObDatumRow *&row);
  int inner_get_next_rows();
};

}  // end of namespace memtable
}  // end of namespace oceanbase
#endif
