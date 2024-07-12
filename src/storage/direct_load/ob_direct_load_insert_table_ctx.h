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
#include "lib/lock/ob_mutex.h"
#include "share/ob_ls_id.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadDmlStat;
class ObTableLoadSqlStatistics;
} // namespace table
namespace storage
{
class ObDirectLoadInsertTableContext;

struct ObDirectLoadInsertTableParam
{
public:
  ObDirectLoadInsertTableParam();
  ~ObDirectLoadInsertTableParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id),
               K_(schema_version),
               K_(snapshot_version),
               K_(ddl_task_id),
               K_(data_version),
               K_(parallel),
               K_(reserved_parallel),
               K_(rowkey_column_count),
               K_(column_count),
               K_(lob_column_count),
               K_(is_partitioned_table),
               K_(is_heap_table),
               K_(is_column_store),
               K_(online_opt_stat_gather),
               K_(is_incremental),
               K_(trans_param),
               KP_(datum_utils),
               KP_(col_descs),
               KP_(cmp_funcs),
               K_(online_sample_percent));

public:
  uint64_t table_id_; // dest_table_id
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  int64_t parallel_;
  int64_t reserved_parallel_;
  int64_t rowkey_column_count_;
  int64_t column_count_; // 不包含多版本列
  int64_t lob_column_count_;
  bool is_partitioned_table_;
  bool is_heap_table_;
  bool is_column_store_;
  bool online_opt_stat_gather_;
  bool is_incremental_;
  ObDirectLoadTransParam trans_param_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
  double online_sample_percent_;
};

struct ObDirectLoadInsertTabletWriteCtx
{
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_interval_;
  TO_STRING_KV(K_(start_seq), K_(pk_interval));
};

class ObDirectLoadInsertTabletContext
{
  static const int64_t PK_CACHE_SIZE = 5000000;
  static const int64_t WRITE_BATCH_SIZE = 5000000;
public:
  ObDirectLoadInsertTabletContext();
  ~ObDirectLoadInsertTabletContext();
  OB_INLINE bool is_valid() const { return is_inited_; }

  OB_INLINE const share::ObLSID &get_ls_id() const { return ls_id_; }
  OB_INLINE const common::ObTabletID &get_origin_tablet_id() const { return origin_tablet_id_; }
  OB_INLINE const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  OB_INLINE const common::ObTabletID &get_lob_tablet_id() const { return lob_tablet_id_; }

  OB_INLINE ObDirectLoadInsertTableContext *get_table_ctx() { return table_ctx_; }
  OB_INLINE const ObDirectLoadInsertTableParam *get_param() const { return param_; }

#define DEFINE_INSERT_TABLE_PARAM_GETTER(type, name, def) \
  OB_INLINE type get_##name() const { return nullptr != param_ ? param_->name##_ : def; }

  DEFINE_INSERT_TABLE_PARAM_GETTER(uint64_t, table_id, OB_INVALID_ID);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, schema_version, OB_INVALID_VERSION);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, snapshot_version, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, ddl_task_id, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, data_version, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, parallel, 0)
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, reserved_parallel, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, rowkey_column_count, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, column_count, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, lob_column_count, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_partitioned_table, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_heap_table, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_column_store, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, online_opt_stat_gather, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_incremental, false);
  // ObDirectLoadTransParam trans_param_;
  DEFINE_INSERT_TABLE_PARAM_GETTER(const blocksstable::ObStorageDatumUtils *, datum_utils, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const common::ObIArray<share::schema::ObColDesc> *, col_descs, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const blocksstable::ObStoreCmpFuncs *, cmp_funcs, nullptr);

#undef DEFINE_INSERT_TABLE_PARAM_GETTER

  OB_INLINE bool has_lob_storage() const { return get_lob_column_count() > 0; }
  OB_INLINE bool need_rescan() const { return nullptr != param_ ? (!param_->is_incremental_ && param_->is_column_store_) : false; }
  OB_INLINE bool need_del_lob() const { return nullptr != param_ ? (param_->is_incremental_ && param_->lob_column_count_ > 0) : false; }

public:
  int init(ObDirectLoadInsertTableContext *table_ctx,
           const share::ObLSID &ls_id,
           const common::ObTabletID &origin_tablet_id,
           const common::ObTabletID &tablet_id);
  int open();
  int close();
  int cancel();
  int get_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx);
  int get_lob_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx);
  int init_datum_row(blocksstable::ObDatumRow &datum_row);
  int init_lob_datum_row(blocksstable::ObDatumRow &datum_row, const bool is_delete = true);
  int open_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id);
  int close_sstable_slice(const int64_t slice_id);
  int fill_sstable_slice(const int64_t &slice_id, ObIStoreRowIterator &iter,
                         int64_t &affected_rows);
  int open_lob_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id);
  int close_lob_sstable_slice(const int64_t slice_id);
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id,
                             share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObDatumRow &datum_row);
  int fill_lob_meta_sstable_slice(const int64_t &lob_slice_id,
                                  ObIStoreRowIterator &iter,
                                  int64_t &affected_rows);
  int calc_range(const int64_t thread_cnt);
  int fill_column_group(const int64_t thread_cnt, const int64_t thread_id);

  void inc_row_count(const int64_t row_count) { ATOMIC_AAF(&row_count_, row_count); }
  int64_t get_row_count() const { return ATOMIC_LOAD(&row_count_); }

  TO_STRING_KV(KPC_(table_ctx),
               KP_(param),
               K_(context_id),
               K_(ls_id),
               K_(origin_tablet_id),
               K_(tablet_id),
               K_(lob_tablet_id),
               K_(start_scn),
               K_(handle),
               K_(row_count),
               K_(open_err),
               K_(is_open),
               K_(is_create),
               K_(is_cancel));
private:
  int get_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval);
  int get_lob_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval);
  int refresh_pk_cache(const common::ObTabletID &tablet_id, share::ObTabletCacheInterval &pk_cache);
private:
  ObDirectLoadInsertTableContext *table_ctx_;
  const ObDirectLoadInsertTableParam *param_;
  int64_t context_id_;
  share::ObLSID ls_id_;
  common::ObTabletID origin_tablet_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_tablet_id_;
  lib::ObMutex mutex_;
  blocksstable::ObMacroDataSeq start_seq_;
  blocksstable::ObMacroDataSeq lob_start_seq_;
  share::ObTabletCacheInterval pk_cache_;
  share::ObTabletCacheInterval lob_pk_cache_;
  share::SCN start_scn_;
  ObTabletDirectLoadMgrHandle handle_;
  int64_t row_count_;
  int open_err_;
  volatile bool is_open_;
  bool is_create_;
  bool is_cancel_;
  bool is_inited_;
};

class ObDirectLoadInsertTableContext
{
private:
  typedef common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTabletContext *>
    TABLET_CTX_MAP;
  typedef common::hash::ObHashMap<int64_t, table::ObTableLoadSqlStatistics *> SQL_STAT_MAP;
public:
  ObDirectLoadInsertTableContext();
  ~ObDirectLoadInsertTableContext();
  void destory();
  int init(const ObDirectLoadInsertTableParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);
  int get_tablet_context(const common::ObTabletID &tablet_id,
                         ObDirectLoadInsertTabletContext *&tablet_ctx) const;
  int commit(table::ObTableLoadDmlStat &dml_stats,
             table::ObTableLoadSqlStatistics &sql_statistics);
  void cancel();

  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE ObDirectLoadInsertTableParam &get_param() { return param_; }
  OB_INLINE int64_t get_context_id() const { return ddl_ctrl_.context_id_; }

  OB_INLINE bool need_rescan() const { return (!param_.is_incremental_ && param_.is_column_store_); }
  OB_INLINE bool need_del_lob() const { return (param_.is_incremental_ && param_.lob_column_count_ > 0); }

  int64_t get_sql_stat_column_count() const;
  int get_sql_statistics(table::ObTableLoadSqlStatistics *&sql_statistics);
  int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                            const blocksstable::ObDatumRow &datum_row);

  TO_STRING_KV(K_(param), K_(ddl_ctrl));
private:
  int create_all_tablet_contexts(
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids);
  int collect_dml_stat(table::ObTableLoadDmlStat &dml_stats);
  int collect_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics);
private:
  common::ObArenaAllocator allocator_;
  common::ObSafeArenaAllocator safe_allocator_;
  ObDirectLoadInsertTableParam param_;
  TABLET_CTX_MAP tablet_ctx_map_; // origin_tablet_id => tablet_ctx
  SQL_STAT_MAP sql_stat_map_;
  sql::ObDDLCtrl ddl_ctrl_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
