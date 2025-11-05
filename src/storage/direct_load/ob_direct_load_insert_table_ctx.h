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
#include "storage/ddl/ob_direct_load_mgr_agent.h"
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
class ObDirectLoadInsertLobTableContext;
class ObDirectLoadInsertLobTabletContext;
class ObDirectLoadBatchRows;
class ObDirectLoadDatumRow;
class ObDirectLoadRowFlag;
class ObDDLIndependentDag;

struct ObDirectLoadInsertTableRowInfo
{
public:
  ObDirectLoadInsertTableRowInfo()
    : row_flag_(),
      mvcc_row_flag_(),
      trans_version_(INT64_MAX),
      trans_id_(),
      seq_no_(0),
      trans_version_vector_(nullptr),
      seq_no_vector_(nullptr)
  {
  }
  bool is_valid() const
  {
    return row_flag_.is_valid() & mvcc_row_flag_.is_valid() &&
           (INT64_MAX != trans_version_ || (trans_id_.is_valid() && seq_no_ > 0));
  }
  TO_STRING_KV(K_(row_flag),
               K_(mvcc_row_flag),
               K_(trans_version),
               K_(trans_id),
               K_(seq_no),
               KP_(trans_version_vector),
               KP_(seq_no_vector));

public:
  blocksstable::ObDmlRowFlag row_flag_;
  blocksstable::ObMultiVersionRowFlag mvcc_row_flag_;
  int64_t trans_version_;
  transaction::ObTransID trans_id_;
  int64_t seq_no_;
  ObIVector *trans_version_vector_;
  ObIVector *seq_no_vector_;
};

struct ObDirectLoadInsertTableResult final
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadInsertTableResult()
    : insert_row_count_(0), delete_row_count_(0)
  {
  }
  ~ObDirectLoadInsertTableResult() {}
  TO_STRING_KV(K_(insert_row_count), K_(delete_row_count));
public:
  int64_t insert_row_count_;
  int64_t delete_row_count_;
};

struct ObDirectLoadInsertTableParam
{
public:
  ObDirectLoadInsertTableParam();
  ~ObDirectLoadInsertTableParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(schema_version), K_(snapshot_version), K_(ddl_task_id),
               K_(data_version), K_(parallel), K_(reserved_parallel), K_(rowkey_column_count),
               K_(column_count), K_(lob_inrow_threshold), K_(is_partitioned_table),
               K_(is_table_without_pk), K_(is_table_with_hidden_pk_column), K_(is_index_table),
               K_(online_opt_stat_gather), K_(is_incremental), K_(is_inc_major), K_(reuse_pk), K_(trans_param), KP_(datum_utils),
               KP_(col_descs), KP_(cmp_funcs), KP_(col_nullables), KP_(lob_column_idxs),
               K_(online_sample_percent), K_(is_no_logging), K_(max_batch_size), K_(enable_dag), KP_(dag), K_(is_inc_major_log));

public:
  uint64_t table_id_; // 目标表的table_id, 目前用于填充统计信息收集结果
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  int64_t parallel_;
  int64_t reserved_parallel_;
  int64_t rowkey_column_count_;
  int64_t column_count_; // 不包含多版本列
  int64_t lob_inrow_threshold_;
  bool is_partitioned_table_;
  bool is_table_without_pk_;
  bool is_table_with_hidden_pk_column_;
  bool is_index_table_;
  bool online_opt_stat_gather_;
  bool is_insert_lob_;
  bool is_incremental_;
  bool is_inc_major_;
  bool reuse_pk_;
  ObDirectLoadTransParam trans_param_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
  sql::ObBitVector *col_nullables_;
  const common::ObIArray<int64_t> *lob_column_idxs_; // 不包含多版本列
  double online_sample_percent_;
  bool is_no_logging_;
  int64_t max_batch_size_;
  bool enable_dag_;
  ObDDLIndependentDag *dag_;
  bool is_inc_major_log_;
};

struct ObDirectLoadInsertTabletWriteCtx
{
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_interval_;
  int64_t slice_idx_;
  ObDirectLoadInsertTabletWriteCtx() : slice_idx_(0) {}
  TO_STRING_KV(K_(start_seq), K_(pk_interval), K_(slice_idx));
};

class ObDirectLoadInsertTabletContext
{
public:
  ObDirectLoadInsertTabletContext();
  virtual ~ObDirectLoadInsertTabletContext();
  OB_INLINE bool is_valid() const { return is_inited_; }
  virtual int open() = 0;
  virtual int close() = 0;
  virtual void cancel() = 0;

  //////////////////////// write interface ////////////////////////
  int get_slice_idx(int64_t &slice_idx);
  int get_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx);
  const blocksstable::ObMacroDataSeq &get_last_data_seq() { return start_seq_; }
  int get_row_info(ObDirectLoadInsertTableRowInfo &row_info, const bool is_delete = false);
  int init_datum_row(blocksstable::ObDatumRow &datum_row, const bool is_delete = false);
  virtual int open_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq,
                                 const int64_t slice_idx,
                                 int64_t &slice_id,
                                 ObDirectLoadMgrAgent &ddl_agent) = 0;
  virtual int fill_sstable_slice(const int64_t &slice_id, ObIStoreRowIterator &iter,
                                 ObDirectLoadMgrAgent &ddl_agent,
                                 int64_t &affected_rows) = 0;
  virtual int fill_sstable_slice(const int64_t &slice_id,
                                 const blocksstable::ObBatchDatumRows &datum_rows,
                                 ObDirectLoadMgrAgent &ddl_agent) = 0;
  virtual int close_sstable_slice(const int64_t slice_id,
                                  const int64_t slice_idx,
                                  ObDirectLoadMgrAgent &ddl_agent) = 0;
  virtual int get_ddl_agent(ObDirectLoadMgrAgent &tmp_agent) = 0;
protected:
  static const int64_t PK_CACHE_SIZE = 5000000;
  static const int64_t WRITE_BATCH_SIZE = 5000000;
  static int refresh_pk_cache(const common::ObTabletID &tablet_id,
                              share::ObTabletCacheInterval &pk_cache);
  virtual int get_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval);

  //////////////////////// rescan interface ////////////////////////
public:
  virtual int calc_range(const int64_t thread_cnt) { return OB_ERR_UNEXPECTED; }
  virtual int fill_column_group(const int64_t thread_cnt, const int64_t thread_id)
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int get_direct_load_type(ObDirectLoadType &direct_load_type) const { return OB_NOT_SUPPORTED; }
  //////////////////////// params ////////////////////////
  OB_INLINE ObDirectLoadInsertTableContext *get_table_ctx() { return table_ctx_; }
  OB_INLINE const ObDirectLoadInsertTableParam *get_param() const { return param_; }
  OB_INLINE const share::ObLSID &get_ls_id() const { return ls_id_; }
  OB_INLINE const common::ObTabletID &get_origin_tablet_id() const { return origin_tablet_id_; }
  OB_INLINE const common::ObTabletID &get_tablet_id() const { return tablet_id_; }

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
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, lob_inrow_threshold, -1);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_partitioned_table, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_table_without_pk, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_table_with_hidden_pk_column, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_index_table, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, online_opt_stat_gather, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_insert_lob, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_incremental, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_inc_major, false);
  // ObDirectLoadTransParam trans_param_;
  DEFINE_INSERT_TABLE_PARAM_GETTER(const blocksstable::ObStorageDatumUtils *, datum_utils, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const common::ObIArray<share::schema::ObColDesc> *, col_descs,
                                   nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const blocksstable::ObStoreCmpFuncs *, cmp_funcs, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const sql::ObBitVector *, col_nullables, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(const common::ObIArray<int64_t> *, lob_column_idxs, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_no_logging, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(int64_t, max_batch_size, 0);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, enable_dag, false);
  DEFINE_INSERT_TABLE_PARAM_GETTER(ObDDLIndependentDag *, dag, nullptr);
  DEFINE_INSERT_TABLE_PARAM_GETTER(bool, is_inc_major_log, false);
#undef DEFINE_INSERT_TABLE_PARAM_GETTER

  OB_INLINE bool has_lob_storage() const
  {
    return nullptr != param_ ? (!param_->is_index_table_ && !param_->lob_column_idxs_->empty()) : false;
  }
  OB_INLINE bool is_incremental() const
  {
    return nullptr != param_ ? param_->is_incremental_ : false;
  }

  void set_lob_tablet_ctx(ObDirectLoadInsertLobTabletContext *lob_tablet_ctx)
  {
    lob_tablet_ctx_ = lob_tablet_ctx;
  }
  ObDirectLoadInsertLobTabletContext *get_lob_tablet_ctx() { return lob_tablet_ctx_; }

  const ObDirectLoadInsertTableResult &get_insert_table_result() const
  {
    return insert_table_result_;
  }
  void update_insert_table_result(ObDirectLoadInsertTableResult &other)
  {
    ATOMIC_AAF(&insert_table_result_.insert_row_count_, other.insert_row_count_);
    ATOMIC_AAF(&insert_table_result_.delete_row_count_, other.delete_row_count_);
  }
  int64_t get_row_count() const
  {
    return ATOMIC_LOAD(&insert_table_result_.insert_row_count_) +
           ATOMIC_LOAD(&insert_table_result_.delete_row_count_);
  }

  VIRTUAL_TO_STRING_KV(KP_(table_ctx), KP_(param), K_(ls_id), K_(origin_tablet_id), K_(tablet_id),
                       K_(pk_tablet_id), KP_(lob_tablet_ctx), K_(insert_table_result), K_(is_inited));

protected:
  ObDirectLoadInsertTableContext *table_ctx_;
  const ObDirectLoadInsertTableParam *param_;
  share::ObLSID ls_id_;
  common::ObTabletID origin_tablet_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID pk_tablet_id_; // 从哪个tablet_id获取自增pk
  ObDirectLoadInsertLobTabletContext *lob_tablet_ctx_;
  lib::ObMutex mutex_;
  int64_t slice_idx_;
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_cache_;
  ObArray<int64_t> closed_slices_;
  ObDirectLoadInsertTableResult insert_table_result_;
  bool is_inited_;
};

class ObDirectLoadInsertTableContext
{
  friend class ObDirectLoadInsertTabletContext;

  typedef common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTabletContext *>
    TabletCtxMap;

public:
  ObDirectLoadInsertTableContext();
  virtual ~ObDirectLoadInsertTableContext();
  virtual int close() { return OB_SUCCESS; }
  void cancel();
  int get_tablet_context(const common::ObTabletID &tablet_id,
                         ObDirectLoadInsertTabletContext *&tablet_ctx) const;
  const ObDirectLoadInsertTableParam &get_param() const { return param_; }
  OB_INLINE TabletCtxMap &get_tablet_ctx_map() { return tablet_ctx_map_; }

  //////////////////////// sql stats interface ////////////////////////
public:
  virtual int get_sql_statistics(table::ObTableLoadSqlStatistics *&sql_statistics)
  {
    return OB_ERR_UNEXPECTED;
  }
  // 带多版本列的完整行
  virtual int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                                    const blocksstable::ObDatumRow &datum_row)
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                                    const blocksstable::ObBatchDatumRows &datum_rows)
  {
    return OB_ERR_UNEXPECTED;
  }
  // 中间过程数据
  virtual int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                                    const ObDirectLoadDatumRow &datum_row,
                                    const ObDirectLoadRowFlag &row_flag)
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                                    const ObDirectLoadBatchRows &batch_rows)
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int update_sql_statistics(table::ObTableLoadSqlStatistics &sql_statistics,
                                    const ObDirectLoadBatchRows &batch_rows,
                                    const uint16_t *selector,
                                    const int64_t size)
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int collect_sql_stats(table::ObTableLoadDmlStat &dml_stats,
                                table::ObTableLoadSqlStatistics &sql_statistics)
  {
    return OB_ERR_UNEXPECTED;
  }

protected:
  int inner_init();

protected:
  common::ObArenaAllocator allocator_;
  ObDirectLoadInsertTableParam param_;
  TabletCtxMap tablet_ctx_map_; // origin_tablet_id => tablet_ctx
  ObIVector *trans_version_vector_;
  ObIVector *seq_no_vector_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
