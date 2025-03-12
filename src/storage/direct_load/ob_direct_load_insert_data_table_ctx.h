/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertDataTableContext;
class ObDirectLoadDatumRow;

class ObDirectLoadInsertDataTabletContext : public ObDirectLoadInsertTabletContext
{
  friend class ObDirectLoadInsertLobTabletContext;

public:
  ObDirectLoadInsertDataTabletContext();
  virtual ~ObDirectLoadInsertDataTabletContext();
  int init(ObDirectLoadInsertDataTableContext *table_ctx, const share::ObLSID &ls_id,
           const common::ObTabletID &origin_tablet_id, const common::ObTabletID &tablet_id);

  int open() override;
  int close() override;
  void cancel() override;

private:
  int create_tablet_direct_load();
  int open_tablet_direct_load();
  int close_tablet_direct_load(bool commit);

  //////////////////////// write interface ////////////////////////
public:
  int open_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq,
                         const int64_t slice_idx,
                         int64_t &slice_id) override;
  int fill_sstable_slice(const int64_t &slice_id, ObIStoreRowIterator &iter,
                         int64_t &affected_rows) override;
  int fill_sstable_slice(const int64_t &slice_id,
                         const blocksstable::ObBatchDatumRows &datum_rows) override;
  int close_sstable_slice(const int64_t slice_id, const int64_t slice_idx) override;

private:
  int record_closed_slice(const int64_t slice_idx);
  int get_prefix_merge_slice_idx(int64_t &slice_idx);

  // for ObDirectLoadInsertLobTabletContext
  int open_lob_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id);
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id,
                             share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id,
                             share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObBatchDatumRows &datum_rows);
  int fill_lob_meta_sstable_slice(const int64_t &lob_slice_id, ObIStoreRowIterator &iter,
                                  int64_t &affected_rows);
  int close_lob_sstable_slice(const int64_t slice_id);

  //////////////////////// rescan interface ////////////////////////
public:
  int calc_range(const int64_t thread_cnt) override;
  int fill_column_group(const int64_t thread_cnt, const int64_t thread_id) override;

  //////////////////////// members ////////////////////////
public:
  INHERIT_TO_STRING_KV("ObDirectLoadInsertTabletContext", ObDirectLoadInsertTabletContext,
                       K_(context_id), K_(direct_load_type), K_(start_scn), K_(open_err),
                       K_(is_create), K_(is_open), K_(is_closed), K_(is_cancel));

private:
  int64_t context_id_;
  ObDirectLoadType direct_load_type_;
  ObDirectLoadMgrAgent ddl_agent_;
  share::SCN start_scn_;
  ObTabletDirectLoadMgrHandle handle_;
  int open_err_;
  bool is_create_;
  bool is_open_;
  bool is_closed_;
  bool is_cancel_;
};

class ObDirectLoadInsertDataTableContext : public ObDirectLoadInsertTableContext
{
  friend class ObDirectLoadInsertDataTabletContext;
  typedef common::hash::ObHashMap<int64_t, table::ObTableLoadSqlStatistics *> SqlStatMap;

public:
  ObDirectLoadInsertDataTableContext();
  virtual ~ObDirectLoadInsertDataTableContext();
  void reset() override;
  int init(const ObDirectLoadInsertTableParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);

private:
  int create_all_tablet_contexts(
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);

  //////////////////////// sql stats interface ////////////////////////
public:
  int get_sql_statistics(table::ObTableLoadSqlStatistics *&sql_statistics) override;
  // 带多版本列的完整行
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const blocksstable::ObDatumRow &datum_row) override;
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const blocksstable::ObBatchDatumRows &datum_rows) override;
  // 中间过程数据
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const ObDirectLoadDatumRow &datum_row,
                            const ObDirectLoadRowFlag &row_flag) override;
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const IVectorPtrs &vectors, const int64_t row_idx,
                            const ObDirectLoadRowFlag &row_flag) override;
  int collect_sql_stats(table::ObTableLoadDmlStat &dml_stats,
                        table::ObTableLoadSqlStatistics &sql_statistics) override;

private:
  int64_t get_sql_stat_column_count() const;
  int collect_dml_stat(table::ObTableLoadDmlStat &dml_stats);
  int collect_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics);

private:
  SqlStatMap sql_stat_map_;
  sql::ObDDLCtrl ddl_ctrl_;
};

} // namespace storage
} // namespace oceanbase
