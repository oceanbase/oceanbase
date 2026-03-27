/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROW_MULTI_SCANNER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROW_MULTI_SCANNER_H_
#include "ob_co_sstable_row_scanner.h"

namespace oceanbase
{
namespace storage
{
class ObCOSSTableRowMultiScanner : public ObCOSSTableRowScanner
{
public:
  ObCOSSTableRowMultiScanner()
      : ObCOSSTableRowScanner(),
      ranges_(nullptr)
  {
    this->type_ = ObStoreRowIterator::IteratorCOMultiScan;
  }
  virtual ~ObCOSSTableRowMultiScanner() {};
  virtual void reset() override;
  virtual void reuse() override;
private:
  virtual int init_row_scanner(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range) override;
  virtual int get_group_idx(int64_t &group_idx) override;
  const common::ObIArray<blocksstable::ObDatumRange> *ranges_;
};
}
}

#endif
