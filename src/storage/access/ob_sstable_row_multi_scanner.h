/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_OB_SSTABLE_ROW_MULTI_SCANNER_H_
#define OB_STORAGE_OB_SSTABLE_ROW_MULTI_SCANNER_H_

#include "ob_sstable_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_getter.h"

namespace oceanbase {
namespace storage {
template<typename PrefetchType = ObIndexTreeMultiPassPrefetcher<>>
class ObSSTableRowMultiScanner : public ObSSTableRowScanner<PrefetchType>
{
public:
  ObSSTableRowMultiScanner()
  {
    this->type_ = ObStoreRowIterator::IteratorMultiScan;
  }
  virtual ~ObSSTableRowMultiScanner();
  virtual void reset() override;
  virtual void reuse() override;
};

}
}
#endif //OB_STORAGE_OB_SSTABLE_ROW_MULTI_SCANNER_H_
