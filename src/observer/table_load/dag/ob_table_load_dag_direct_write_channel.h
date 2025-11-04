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

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDagInsertTableBatchRowDirectWriter;
} // namespace storage
namespace observer
{
class ObTableLoadDirectWriteOp;

class ObTableLoadDagDirectWriteChannel final : public ObTableLoadDagWriteChannel
{
public:
  ObTableLoadDagDirectWriteChannel();
  virtual ~ObTableLoadDagDirectWriteChannel() = default;
  int init(ObTableLoadDag *dag, ObTableLoadDirectWriteOp *op);

protected:
  int create_writer(ObTableLoadDagChunkWriter *&writer, ObIAllocator &allocator) override;
  int do_close() override;

public:
  ObTableLoadDirectWriteOp *op_;
};

class ObTableLoadDagDirectChunkWriter final : public ObTableLoadDagChunkWriter
{
  typedef storage::ObDirectLoadDagInsertTableBatchRowDirectWriter BatchWriter;

public:
  ObTableLoadDagDirectChunkWriter();
  virtual ~ObTableLoadDagDirectChunkWriter();
  int init(ObTableLoadDagWriteChannel *write_channel, ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *store_writer, const int32_t session_id) override;
  int append_row(const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row) override;
  int append_batch(common::ObIVector *tablet_id_vector,
                   const storage::ObDirectLoadBatchRows &batch_rows) override;
  int close(ObTableLoadStoreTrans *trans, const int32_t session_id) override;

private:
  int new_batch_writer(const common::ObTabletID &tablet_id, BatchWriter *&batch_writer);
  int get_batch_writer(const common::ObTabletID &tablet_id, BatchWriter *&batch_writer);

private:
  typedef common::hash::ObHashMap<common::ObTabletID, BatchWriter *,
                                  common::hash::NoPthreadDefendMode>
    BatchWriterMap;

  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDagDirectWriteChannel *write_channel_;
  ObArenaAllocator batch_writer_allocator_;
  ObSEArray<BatchWriter *, 1> batch_writers_;
  BatchWriterMap batch_writer_map_;
  ObArenaAllocator allocator_;
  int64_t max_batch_size_;
  uint16_t *selector_;
  uint16_t *tablet_offsets_;
  ObTabletID single_tablet_id_;
  bool is_single_part_;
  bool is_closed_;
};

} // namespace observer
} // namespace oceanbase
