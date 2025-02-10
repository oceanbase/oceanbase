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

#include "share/table/ob_table_load_row_array.h"
#include "src/share/table/ob_table_load_row_array.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPreSorter;
class ObTableLoadTransStoreWriter;
class ObTableLoadErrorRowHandler;
class ObTableLoadMemChunkManager;

class ObTableLoadPreSortWriter
{
public:
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  using RowType = ObDirectLoadConstExternalMultiPartitionRow;
  ObTableLoadPreSortWriter();
  ~ObTableLoadPreSortWriter();
  int init(ObTableLoadPreSorter *pre_sorter,
           ObTableLoadTransStoreWriter *store_writer,
           ObTableLoadErrorRowHandler *error_row_handler);
  int write(int32_t session_id, const table::ObTableLoadTabletObjRowArray &row_array);
  int px_write(common::ObIVector *tablet_id_vector,
               const ObIArray<common::ObIVector *> &vectors,
               const sql::ObBatchRows &batch_rows,
               int64_t &affected_rows);
  int close();
private:
  int append_row(const ObTabletID &tablet_id,
                 const ObDirectLoadDatumRow &datum_row);
private:
  ObTableLoadPreSorter *pre_sorter_;
  ObTableLoadTransStoreWriter *store_writer_;
  ObTableLoadErrorRowHandler *error_row_handler_;
  ObDirectLoadMemContext *mem_ctx_;
  ObTableLoadMemChunkManager *chunks_manager_;
  int64_t chunk_node_id_;
  ChunkType *chunk_;
  ObDirectLoadDatumRow datum_row_;
  ObDirectLoadExternalMultiPartitionRow external_row_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase

#endif /* _OB_TABLE_LOAD_PRE_SORT_WRITER_ */
