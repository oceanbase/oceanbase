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

#include "observer/table_load/dag/ob_table_load_dag_mem_compact.h"
#include "observer/table_load/dag/ob_table_load_dag_write_channel.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPreSortWriteOp;

class ObTableLoadDagPreSortWriteChannel final : public ObTableLoadDagWriteChannel
{
  using ChunkType = ObTableLoadMemCompactCtx::ChunkType;

public:
  class SortChunkTask : public share::ObITask
  {
  public:
    SortChunkTask(ObITaskType task_type) : ObITask(task_type), chunk_(nullptr) {}
    void set_chunk(ChunkType *chunk) { chunk_ = chunk; }

  protected:
    ChunkType *chunk_;
  };

public:
  ObTableLoadDagPreSortWriteChannel();
  virtual ~ObTableLoadDagPreSortWriteChannel();
  int init(ObTableLoadDag *dag, ObTableLoadPreSortWriteOp *op);

  int get_chunk(int64_t &chunk_node_id, ChunkType *&chunk);
  int push_chunk(int64_t chunk_node_id);
  int close_chunk(int64_t chunk_node_id);

  int add_sort_chunk_task(SortChunkTask *task);
  bool no_more_closed_chunk() const { return no_more_closed_chunk_; }

protected:
  int create_writer(ObTableLoadDagChunkWriter *&writer, ObIAllocator &allocator) override;
  int do_flush() override;
  int do_close() override;

private:
  int acquire_chunk(ChunkType *&chunk);
  int add_closed_chunk(ChunkType *&chunk);
  int finish_add_closed_chunk();

private:
  struct ChunkNode
  {
  public:
    ChunkNode() : chunk_(nullptr), is_used_(false) {}
    ~ChunkNode() = default;
    bool is_used() const { return ATOMIC_LOAD(&is_used_); }
    bool set_used() { return (false == ATOMIC_VCAS(&is_used_, false, true)); }
    bool set_unused() { return (true == ATOMIC_VCAS(&is_used_, true, false)); }
    TO_STRING_KV(KP_(chunk), K_(is_used));

  public:
    ChunkType *chunk_;
    bool is_used_;
  };

public:
  ObTableLoadPreSortWriteOp *op_;
  ObTableLoadMemCompactCtx mem_compact_ctx_;

private:
  ObArray<ChunkNode> chunk_nodes_;
  lib::ObMutex acquire_chunk_mutex_;
  mutable lib::ObMutex closed_chunk_mutex_;
  ObArray<ChunkType *> closed_chunks_;
  ObArray<SortChunkTask *> sort_chunk_tasks_;
  bool finish_add_closed_chunk_;
  bool no_more_closed_chunk_;
};

class ObTableLoadDagPreSortChunkWriter final : public ObTableLoadDagChunkWriter
{
  using RowType = ObTableLoadMemCompactCtx::RowType;
  using ConstRowType = ObTableLoadMemCompactCtx::ConstRowType;
  using ChunkType = ObTableLoadMemCompactCtx::ChunkType;

public:
  ObTableLoadDagPreSortChunkWriter();
  virtual ~ObTableLoadDagPreSortChunkWriter();
  int init(ObTableLoadDagWriteChannel *write_channel, ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *store_writer, const int32_t session_id) override;
  int append_row(const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row) override;
  int append_batch(common::ObIVector *tablet_id_vector,
                   const storage::ObDirectLoadBatchRows &batch_rows) override;
  int close(ObTableLoadStoreTrans *trans, const int32_t session_id) override;

private:
  int inner_append_row(const common::ObTabletID &tablet_id,
                       const storage::ObDirectLoadDatumRow &datum_row);
  int push_chunk();

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDagPreSortWriteChannel *write_channel_;
  ObTableLoadMemCompactCtx *mem_compact_ctx_;
  int64_t chunk_node_id_;
  ChunkType *chunk_;
  storage::ObDirectLoadDatumRow datum_row_;
  RowType row_;
  bool is_closed_;
};

} // namespace observer
} // namespace oceanbase
