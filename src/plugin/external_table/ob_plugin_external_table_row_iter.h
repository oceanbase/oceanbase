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

#pragma once

#include "sql/engine/table/ob_external_table_access_service.h"
#include "plugin/external_table/ob_external_struct.h"
#include "plugin/external_table/ob_external_arrow_data_loader.h"

namespace arrow {
class Array;
class RecordBatch;
} // namespace arrow

namespace oceanbase {

namespace sql {
class ObEvalCtx;
class ObExpr;
} // namespace sql

namespace plugin {

class ObExternalScanner;
class ObExternalDataEngine;

namespace external {
class ObArrowDataLoader;
}

class ObPluginExternalTableRowIterator final : public sql::ObExternalTableRowIterator
{
public:
  ObPluginExternalTableRowIterator();
  virtual ~ObPluginExternalTableRowIterator();

  int init(const storage::ObTableScanParam *scan_param) override;
  int rescan(const storage::ObTableScanParam *scan_param);
  void reset() override;
  void destroy();

  int get_next_rows(int64_t &count, int64_t capacity) override;

  int get_next_row(ObNewRow *&) override { return common::OB_ERR_UNEXPECTED; }
  int get_next_row() override;

private:
  int init_plugin_column_index_map_if_need(arrow::RecordBatch *record_batch);
  int init_external_scan_param(const storage::ObTableScanParam *scan_param);

  int open_scanner(const storage::ObTableScanParam *scan_param);

  int get_next_record_batch();

private:
  ObExternalScanner *         scanner_     = nullptr;
  ObExternalDataEngine *      data_engine_ = nullptr;
  ObIExternalDescriptor *     descriptor_  = nullptr;
  ObExternalTableScanParam    external_scan_param_;

  arrow::RecordBatch * record_batch_ = nullptr;
  int64_t              record_batch_offset_ = 0;

  /// map ext_file_column_exprs_ index to plugin schema column index
  ObArray<int64_t>                      plugin_column_index_map_;
  ObArray<external::ObArrowDataLoader*> plugin_column_loaders_;

  common::ObArenaAllocator arena_allocator_;
  common::ObMalloc  allocator_;
  sql::ObBitVector *bit_vector_cache_ = nullptr;

  sql::ObExternalIteratorState iterator_state_;
};

} // namespace plugin
} // namespace oceanbase
