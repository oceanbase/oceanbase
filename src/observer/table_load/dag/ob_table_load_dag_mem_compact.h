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

#include "lib/atomic/ob_atomic.h"
#include "observer/table_load/dag/ob_table_load_dag_task.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "share/table/ob_table_load_handle.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_define.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDMLRowHandler;
} // namespace storage
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadMemCompactRoundCtx;
typedef table::ObTableLoadHandle<ObTableLoadMemCompactRoundCtx> ObTableLoadMemCompactRoundCtxHandle;

class ObTableLoadMemCompactCtx
{
public:
  typedef storage::ObDirectLoadExternalMultiPartitionRow RowType;
  typedef storage::ObDirectLoadConstExternalMultiPartitionRow ConstRowType;
  typedef storage::ObDirectLoadExternalMultiPartitionRowCompare CompareType;
  typedef storage::ObDirectLoadExternalMultiPartitionRowRange RangeType;
  typedef storage::ObDirectLoadExternalMultiPartitionRowChunk ChunkType;

  ObTableLoadMemCompactCtx();
  virtual ~ObTableLoadMemCompactCtx();
  void reset();
  int init();
  int acquire_chunk(ChunkType *&chunk);
  void release_chunk(ChunkType *chunk);

  // 控制round_ctx数目
  bool can_make_round_ctx() const { return ATOMIC_LOAD(&round_ctx_cnt_) < max_round_ctx_cnt_; }
  void dec_round_ctx_cnt() { ATOMIC_AAF(&round_ctx_cnt_, -1); }
  int make_round_ctx(ObTableLoadMemCompactRoundCtxHandle &round_ctx);

  int add_result_table(const storage::ObDirectLoadTableHandle &table_handle);
  const storage::ObDirectLoadTableHandleArray &get_result_tables_handle() const
  {
    return result_tables_handle_;
  }

  int64_t get_chunk_count() const { return chunk_allocator_.size(); }

public:
  ObTableLoadStoreCtx *store_ctx_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  const common::ObIArray<share::schema::ObColDesc> *column_descs_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObArray<share::ObEncParam> enc_params_; // for ObAdaptiveQS
  storage::ObDirectLoadDMLRowHandler *dml_row_handler_;

  int64_t max_round_ctx_cnt_;
  int64_t compact_chunk_cnt_;
  int64_t range_cnt_;

private:
  ObTableLoadObjectAllocator<ChunkType> chunk_allocator_;
  int64_t round_ctx_cnt_;
  lib::ObMutex mutex_;
  storage::ObDirectLoadTableHandleArray result_tables_handle_;
  bool is_inited_;
};

class ObTableLoadMemCompactRoundCtx
{
  using ChunkType = ObTableLoadMemCompactCtx::ChunkType;
  using RangeType = ObTableLoadMemCompactCtx::RangeType;

public:
  ObTableLoadMemCompactRoundCtx(ObTableLoadMemCompactCtx *ctx);
  ~ObTableLoadMemCompactRoundCtx();
  int init();
  // load/sort添加chunk
  int add_chunk(ChunkType *chunk);
  // sample添加range
  int add_range(const RangeType &range);
  // dump添加table
  int add_table(const int64_t range_idx, const storage::ObDirectLoadTableHandle &table_handle);

  bool empty() const { return chunks_.empty(); }
  const ObIArray<ChunkType *> &get_chunks() const { return chunks_; }
  const ObIArray<RangeType> &get_ranges() const { return ranges_; }
  const ObIArray<storage::ObDirectLoadTableHandle> &get_table_handles() const
  {
    return table_handles_;
  }

private:
  ObTableLoadMemCompactCtx *ctx_;
  int64_t range_cnt_;
  lib::ObMutex mutex_;
  ObArray<ChunkType *> chunks_;
  ObArray<RangeType> ranges_;
  ObArray<storage::ObDirectLoadTableHandle> table_handles_;
  bool is_inited_;
};

// base
class ObTableLoadMemCompactTaskBase : public ObTableLoadDagTaskBase
{
public:
  using RowType = ObTableLoadMemCompactCtx::RowType;
  using ConstRowType = ObTableLoadMemCompactCtx::ConstRowType;
  using CompareType = ObTableLoadMemCompactCtx::CompareType;
  using RangeType = ObTableLoadMemCompactCtx::RangeType;
  using ChunkType = ObTableLoadMemCompactCtx::ChunkType;

  ObTableLoadMemCompactTaskBase(ObTableLoadDag *dag, ObTableLoadMemCompactCtx *ctx);
  ObTableLoadMemCompactTaskBase(ObTableLoadMemCompactTaskBase *parent);
  virtual ~ObTableLoadMemCompactTaskBase() = default;

protected:
  ObTableLoadMemCompactCtx *ctx_;
  ObTableLoadMemCompactRoundCtxHandle round_ctx_;
};

// sample
class ObTableLoadMemCompactSampleTask final : public share::ObITask,
                                              public ObTableLoadMemCompactTaskBase
{
  static const int64_t DEFAULT_SAMPLE_TIMES = 50000;

public:
  ObTableLoadMemCompactSampleTask(ObTableLoadMemCompactTaskBase *parent);
  virtual ~ObTableLoadMemCompactSampleTask() = default;
  int process() override;
};

// dump
class ObTableLoadMemCompactDumpTask final : public share::ObITask,
                                            public ObTableLoadMemCompactTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadMemCompactDumpTask(ObTableLoadMemCompactTaskBase *parent, const int64_t range_idx);
  virtual ~ObTableLoadMemCompactDumpTask() = default;
  int process() override;

private:
  int generate_next_task(share::ObITask *&next_task) override;

private:
  const int64_t range_idx_;
};

// compact
class ObTableLoadMemCompactCompactTask final : public share::ObITask,
                                               public ObTableLoadMemCompactTaskBase
{
public:
  ObTableLoadMemCompactCompactTask(ObTableLoadMemCompactTaskBase *parent);
  virtual ~ObTableLoadMemCompactCompactTask() = default;
  int process() override;
};

} // namespace observer
} // namespace oceanbase
