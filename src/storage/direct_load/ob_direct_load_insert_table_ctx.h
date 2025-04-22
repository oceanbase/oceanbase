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

#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableInsertSliceWriter;

struct ObDirectLoadInsertTableParam
{
public:
  ObDirectLoadInsertTableParam();
  ~ObDirectLoadInsertTableParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id),
               K_(dest_table_id),
               K_(schema_version),
               K_(snapshot_version),
               K_(execution_id),
               K_(ddl_task_id),
               K_(data_version),
               K_(session_cnt),
               K_(rowkey_column_count),
               K_(column_count),
               K_(is_partitioned_table),
               K_(is_heap_table),
               K_(online_opt_stat_gather),
               KP_(col_descs),
               KP_(cmp_funcs));
public:
  uint64_t table_id_;
  uint64_t dest_table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  int64_t session_cnt_;
  int64_t rowkey_column_count_;
  int64_t column_count_;
  bool is_partitioned_table_;
  bool is_heap_table_;
  bool online_opt_stat_gather_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
};

class ObDirectLoadInsertTableContext
{
public:
  ObDirectLoadInsertTableContext();
  ~ObDirectLoadInsertTableContext();
  int init(const ObDirectLoadInsertTableParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids);
  int add_sstable_slice(const common::ObTabletID &tablet_id,
                        const blocksstable::ObMacroDataSeq &start_seq,
                        common::ObNewRowIterator &iter,
                        int64_t &affected_rows);
  int construct_sstable_slice_writer(const common::ObTabletID &tablet_id,
                                     const blocksstable::ObMacroDataSeq &start_seq,
                                     ObSSTableInsertSliceWriter *&slice_writer,
                                     common::ObIAllocator &allocator);
  int notify_tablet_finish(const common::ObTabletID &tablet_id);
  int commit(table::ObTableLoadSqlStatistics &sql_statistics);
  void inc_row_count(int64_t row_count) { ATOMIC_AAF(&table_row_count_, row_count); }
  int64_t get_row_count() const { return table_row_count_; }
  TO_STRING_KV(K_(param), K_(tablet_finish_count), K_(table_row_count), K_(ddl_ctrl));

  //////////////////////// sql stats interface ////////////////////////
public:
  int get_sql_statistics(table::ObTableLoadSqlStatistics *&sql_statistics);
  // datum_row是带多版本列的完整行
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const blocksstable::ObDatumRow &datum_row);
private:
  int64_t get_sql_stat_column_count() const;
  int collect_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics);

private:
  common::ObArenaAllocator allocator_;
  common::ObSafeArenaAllocator safe_allocator_;
  ObDirectLoadInsertTableParam param_;
  sql::ObDDLCtrl ddl_ctrl_;
  int64_t tablet_count_;
  int64_t tablet_finish_count_ CACHE_ALIGNED;
  int64_t table_row_count_ CACHE_ALIGNED;
  common::hash::ObHashMap<int64_t, table::ObTableLoadSqlStatistics *> sql_stat_map_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
