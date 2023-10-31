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
