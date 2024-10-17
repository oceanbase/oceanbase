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

 #ifndef _OB_TABLE_PRE_SORT_WRITER_
 #define _OB_TABLE_PRE_SORT_WRITER_

 #include "share/table/ob_table_load_row.h"
 #include "storage/blocksstable/ob_datum_row.h"
 #include "storage/direct_load/ob_direct_load_mem_define.h"
 #include "storage/direct_load/ob_direct_load_mem_chunk.h"
 #include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"


namespace oceanbase
{
namespace observer
{
class ObTableLoadPreSorter;
class ObTableLoadTransStoreWriter;
class ObTableLoadStoreCtx;
class ObTableLoadPreSortWriter {
public:
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  using RowType = ObDirectLoadConstExternalMultiPartitionRow;
  ObTableLoadPreSortWriter();
  ~ObTableLoadPreSortWriter();
  int init(ObTableLoadPreSorter *pre_sorter, ObTableLoadTransStoreWriter *store_writer);
  int write(int32_t session_id, const table::ObTableLoadTabletObjRowArray &row_array);
  int px_write(const ObTabletID &tablet_id, const blocksstable::ObDatumRow &row);
  int close_chunk();
private:
  int append_row(ObDirectLoadExternalMultiPartitionRow &external_row);
private:
  ObTableLoadTransStoreWriter *store_writer_;
  int64_t chunk_node_id_;
  ChunkType *chunk_;
  ObTableLoadPreSorter *pre_sorter_;
  bool is_inited_;
};
}
}

 #endif /* _OB_TABLE_LOAD_PRE_SORT_WRITER_ */