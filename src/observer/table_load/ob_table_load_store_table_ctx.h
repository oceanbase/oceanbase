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

#include "lib/hash/ob_hashmap.h"
#include "ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObIDirectLoadPartitionTableBuilder;
class ObDirectLoadDMLRowHandler;
}
namespace observer
{
class ObTableLoadSchema;
class ObTableLoadMerger;
class ObTableLoadIndexTableProjector;
class ObTableLoadStoreCtx;
class ObTableLoadStoreTableCtx
{
private:
  typedef common::hash::ObHashMap<int64_t, ObIDirectLoadPartitionTableBuilder *> TABLE_BUILDER_MAP;
public:
  static const int64_t MACRO_BLOCK_WRITER_MEM_SIZE = 10 * 1024LL * 1024LL;
  ObTableLoadStoreTableCtx();
  ~ObTableLoadStoreTableCtx();
  int init(
    ObTableID table_id, bool is_index_table, ObTableLoadStoreCtx *store_ctx,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  TABLE_BUILDER_MAP& get_index_table_builder_map() { return index_table_builder_map_; }
  int close_index_table_builder();
  int get_index_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder);
  TO_STRING_KV(K_(is_inited));
  bool is_valid() {
    return is_inited_;
  }
private:
  int init_table_load_schema();
  int init_index_projector();
  int init_ls_partition_ids(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  int init_table_data_desc();
  int init_insert_table_ctx();
  int init_row_handler();
  int init_merger();
public:
  common::ObArenaAllocator allocator_;
  ObTableID table_id_;
  bool is_index_table_;
  ObTableLoadStoreCtx *store_ctx_;
  bool need_sort_;
  ObTableLoadSchema *schema_;
  ObTableLoadIndexTableProjector *project_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> ls_partition_ids_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> target_ls_partition_ids_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  storage::ObDirectLoadTableDataDesc lob_id_table_data_desc_;
  bool is_fast_heap_table_;
  bool is_multiple_mode_;
  storage::ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadDMLRowHandler * row_handler_;
  ObTableLoadMerger* merger_;
  TABLE_BUILDER_MAP index_table_builder_map_;
private:
  lib::ObMutex mutex_;
  bool is_inited_;
};

}
}