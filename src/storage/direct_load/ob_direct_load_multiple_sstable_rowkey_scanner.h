/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#pragma once

#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block_reader.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleSSTable;
class ObDirectLoadTableDataDesc;

class ObDirectLoadMultipleSSTableRowkeyScanner
  : public ObIDirectLoadMultipleDatumRowkeyIterator
{
  typedef ObDirectLoadMultipleDatumRowkey RowkeyType;
  typedef ObDirectLoadSSTableDataBlockReader<RowkeyType> DataBlockReader;
public:
  ObDirectLoadMultipleSSTableRowkeyScanner();
  virtual ~ObDirectLoadMultipleSSTableRowkeyScanner();
  int init(ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc);
  int get_next_rowkey(const ObDirectLoadMultipleDatumRowkey *&rowkey) override;
private:
  int switch_next_fragment();
private:
  ObDirectLoadMultipleSSTable *sstable_;
  DataBlockReader data_block_reader_;
  int64_t fragment_idx_;
  bool is_inited_;
};

class ObDirectLoadSSTableRowkeyScanner : public ObIDirectLoadDatumRowkeyIterator
{
public:
  ObDirectLoadSSTableRowkeyScanner();
  virtual ~ObDirectLoadSSTableRowkeyScanner();
  int init(ObDirectLoadMultipleSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc);
  int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
private:
  ObDirectLoadMultipleSSTable *sstable_;
  ObDirectLoadMultipleSSTableRowkeyScanner scanner_;
  blocksstable::ObDatumRowkey rowkey_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
