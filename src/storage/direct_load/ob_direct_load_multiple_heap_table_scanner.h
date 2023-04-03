// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block.h"
#include "storage/direct_load/ob_direct_load_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_multiple_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scanner.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObDirectLoadMultipleHeapTable;

class ObDirectLoadMultipleHeapTableTabletWholeScanner
{
  typedef ObDirectLoadMultipleExternalRow RowType;
  typedef ObDirectLoadDataBlockReader<ObDirectLoadDataBlock::Header, RowType> DataBlockReader;
public:
  ObDirectLoadMultipleHeapTableTabletWholeScanner();
  ~ObDirectLoadMultipleHeapTableTabletWholeScanner();
  int init(ObDirectLoadMultipleHeapTable *heap_table, const common::ObTabletID &tablet_id,
           const ObDirectLoadTableDataDesc &table_data_desc);
  int get_next_row(const RowType *&external_row);
  TO_STRING_KV(KP_(heap_table), K_(tablet_id));
private:
  int switch_next_fragment();
private:
  ObDirectLoadMultipleHeapTable *heap_table_;
  common::ObTabletID tablet_id_;
  ObDirectLoadMultipleHeapTableTabletIndexWholeScanner index_scanner_;
  DataBlockReader data_block_reader_;
  bool is_iter_end_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
