/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "observer/table_load/ob_table_load_merge_compact_table_op.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadTableOpCtx;

class ObTableLoadDagHeapTableCompare
{
public:
  ObTableLoadDagHeapTableCompare();
  ~ObTableLoadDagHeapTableCompare();
  bool operator()(const ObDirectLoadTableHandle lhs, const ObDirectLoadTableHandle rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

class ObTableLoadDagParallelHeapTableCompactor
{
public:
  ObTableLoadDagParallelHeapTableCompactor();
  ~ObTableLoadDagParallelHeapTableCompactor();
  int init(ObTableLoadStoreCtx *store_ctx, ObTableLoadTableOpCtx *op_ctx);
  int close();

public:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadTableOpCtx *op_ctx_;
  ObDirectLoadTableHandleArray tables_handle_;
  ObDirectLoadTableHandleArray result_tables_handle_;
  mutable lib::ObMutex mutex_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
