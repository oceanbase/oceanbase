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

#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPKMemSorter;
class ObTableLoadHeapMemSorter;

class ObTableLoadDagParallelSSTableCompactor;
class ObTableLoadDagParallelHeapTableCompactor;

class ObTableLoadDagParallelMerger;

// merge_data
// TODO @suzhi.yt 删除旧代码后, 名字改回ObTableLoadMergeDataOp
class ObTableLoadMergeDataOp2 final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadMergeDataOp2(ObTableLoadTableBaseOp *parent) : ObTableLoadTableBaseOp(parent)
  {
    op_type_ = ObTableLoadOpType::MERGE_DATA_OP;
  }

  static int build(ObTableLoadTableOp *table_op, ObTableLoadMergeDataOp2 *&merge_data_op);
};

// mem_sort
class ObTableLoadMemSortOp final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadMemSortOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadMemSortOp();

public:
  ObTableLoadPKMemSorter *pk_mem_sorter_;
  ObTableLoadHeapMemSorter *heap_mem_sorter_;
};

// compact_data
class ObTableLoadCompactDataOp final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadCompactDataOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadCompactDataOp();

public:
  ObTableLoadDagParallelSSTableCompactor *sstable_compactor_;
  ObTableLoadDagParallelHeapTableCompactor *heap_table_compactor_;
};

// insert_sstable
class ObTableLoadInsertSSTableOp final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadInsertSSTableOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadInsertSSTableOp();

public:
  ObTableLoadDagParallelMerger *parallel_merger_;
};

} // namespace observer
} // namespace oceanbase
