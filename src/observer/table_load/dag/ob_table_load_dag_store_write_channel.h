/**
 * Copyright (c) 2025 OceanBase
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

#include "lib/hash/ob_hashmap.h"
#include "observer/table_load/dag/ob_table_load_dag_write_channel.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace storage
{
class ObIDirectLoadPartitionTableBuilder;
class ObDirectLoadTableHandleArray;
} // namespace storage
namespace observer
{
class ObTableLoadStoreWriteOp;

class ObTableLoadDagStoreWriteChannel final : public ObTableLoadDagWriteChannel
{
public:
  ObTableLoadDagStoreWriteChannel();
  virtual ~ObTableLoadDagStoreWriteChannel() = default;
  int init(ObTableLoadDag *dag, ObTableLoadStoreWriteOp *op);

protected:
  int create_writer(ObTableLoadDagChunkWriter *&writer, ObIAllocator &allocator) override;
  int do_close() override;

public:
  ObTableLoadStoreWriteOp *op_;
};

class ObTableLoadDagStoreChunkWriter final : public ObTableLoadDagChunkWriter
{
public:
  ObTableLoadDagStoreChunkWriter();
  virtual ~ObTableLoadDagStoreChunkWriter();
  int init(ObTableLoadDagWriteChannel *write_channel, ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *store_writer, const int32_t session_id) override;
  int append_row(const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row) override;
  int append_batch(common::ObIVector *tablet_id_vector,
                   const storage::ObDirectLoadBatchRows &batch_rows, int64_t &start) override;
  int close(ObTableLoadStoreTrans *trans, const int32_t session_id) override;

private:
  int new_table_builder(const common::ObTabletID &tablet_id,
                        ObIDirectLoadPartitionTableBuilder *&table_builder);
  int get_table_builder(const common::ObTabletID &tablet_id,
                        ObIDirectLoadPartitionTableBuilder *&table_builder);
  int inner_append_row(const common::ObTabletID &tablet_id,
                       const storage::ObDirectLoadDatumRow &datum_row);

private:
  typedef common::hash::ObHashMap<common::ObTabletID, storage::ObIDirectLoadPartitionTableBuilder *,
                                  common::hash::NoPthreadDefendMode>
    TableBuilderMap;

  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDagStoreWriteChannel *write_channel_;
  ObArenaAllocator table_builder_allocator_;
  ObSEArray<ObIDirectLoadPartitionTableBuilder *, 1> table_builders_;
  TableBuilderMap table_builder_map_;
  storage::ObDirectLoadDatumRow datum_row_;
  bool is_single_part_;
  bool is_closed_;
};

} // namespace observer
} // namespace oceanbase
