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

#include "lib/list/ob_dlist.h"
#include "observer/table_load/dag/ob_table_load_dag_mem_compact.h"
#include "observer/table_load/dag/ob_table_load_dag_mem_sort.h"
#include "storage/direct_load/ob_direct_load_external_fragment.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPKMemSortLoader;

class ObTableLoadPKMemSorter
{
public:
  ObTableLoadPKMemSorter();
  virtual ~ObTableLoadPKMemSorter();
  int init(ObTableLoadDag *dag, ObTableLoadMemSortOp *op);
  int pop_loader(ObTableLoadPKMemSortLoader *&loader);
  int push_loader(ObTableLoadPKMemSortLoader *loader);
  bool is_finish() const;
  int close();

private:
  int construct_loaders();

public:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDag *dag_;
  ObTableLoadMemSortOp *op_;
  ObTableLoadMemCompactCtx mem_compact_ctx_;

private:
  ObTableLoadObjectAllocator<ObTableLoadPKMemSortLoader> loader_allocator_;
  mutable lib::ObMutex mutex_;
  common::ObDList<ObTableLoadPKMemSortLoader> idle_loader_list_; // 空闲的loader
  common::ObDList<ObTableLoadPKMemSortLoader> used_loader_list_; // 正在被读取的loader
  bool is_closed_;
  bool is_inited_;
};

// 一个fragment对应一个loader
class ObTableLoadPKMemSortLoader : public common::ObDLinkBase<ObTableLoadPKMemSortLoader>
{
  using RowType = ObTableLoadMemCompactCtx::RowType;
  using ConstRowType = ObTableLoadMemCompactCtx::ConstRowType;
  using ChunkType = ObTableLoadMemCompactCtx::ChunkType;
  typedef ObDirectLoadExternalBlockReader<RowType> ReaderType;

public:
  ObTableLoadPKMemSortLoader();
  ~ObTableLoadPKMemSortLoader() = default;
  int init(ObTableLoadPKMemSorter *mem_sorter,
           const storage::ObDirectLoadExternalFragment &fragment);
  int process(ChunkType &chunk);
  bool is_iter_end() const { return is_iter_end_; }

  TO_STRING_KV(KP_(mem_sorter), K_(fragment), KP_(row), K_(is_iter_end));

private:
  ObTableLoadPKMemSorter *mem_sorter_;
  storage::ObDirectLoadExternalFragment fragment_;
  ReaderType reader_;
  const RowType *row_;
  bool is_iter_end_;
  bool is_inited_;
};

// base
class ObTableLoadPKMemSortTaskBase : public ObTableLoadMemCompactTaskBase
{
public:
  ObTableLoadPKMemSortTaskBase(ObTableLoadDag *dag, ObTableLoadPKMemSorter *mem_sorter);
  ObTableLoadPKMemSortTaskBase(ObTableLoadPKMemSortTaskBase *parent);
  virtual ~ObTableLoadPKMemSortTaskBase() = default;

protected:
  ObTableLoadPKMemSorter *mem_sorter_;
};

// load
// 创建compact_chunk_cnt_个load_task, 每个load_task写一个chunk
class ObTableLoadPKMemSortLoadTask final : public share::ObITask,
                                           public ObTableLoadPKMemSortTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadPKMemSortLoadTask(ObTableLoadPKMemSortTaskBase *parent, const int64_t chunk_idx);
  ObTableLoadPKMemSortLoadTask(ObTableLoadPKMemSortLoadTask *parent);
  virtual ~ObTableLoadPKMemSortLoadTask();
  int process() override;

private:
  // 产生compact_chunk_cnt_-1个load_task
  int generate_next_task(share::ObITask *&next_task) override;

private:
  const int64_t chunk_idx_;
  ChunkType *chunk_;
  bool is_first_generation_;
};

// root
class ObTableLoadPKMemSortTask final : public share::ObITask, public ObTableLoadPKMemSortTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadPKMemSortTask(ObTableLoadDag *dag, ObTableLoadPKMemSorter *mem_sorter);
  ObTableLoadPKMemSortTask(ObTableLoadPKMemSortTask *parent);
  virtual ~ObTableLoadPKMemSortTask() = default;
  ObITaskPriority get_priority() override;
  int process() override;
};

} // namespace observer
} // namespace oceanbase
