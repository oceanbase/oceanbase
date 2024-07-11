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

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_COMMON_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_COMMON_H

#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_bucket_lock.h"
#include "common/ob_tablet_id.h"
#include "common/row/ob_row_iterator.h"
#include "share/scn.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/ob_ddl_common.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/chunk_store/ob_compact_store.h"
#include "storage/ob_i_table.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace sql
{
class ObPxMultiPartSSTableInsertOp;
class ObExecContext;
}

namespace storage
{
class ObTablet;

struct ObBatchSliceWriteInfo final
{
public:
  ObBatchSliceWriteInfo()
    : data_tablet_id_(), // tablet id of the data table.
      ls_id_(),
      trans_version_(0),
      direct_load_type_(),
      trans_id_(),
      seq_no_(0),
      src_tenant_id_(0),
      tx_desc_(nullptr)
  { }
  ObBatchSliceWriteInfo(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id, const int64_t &trans_version,
      const ObDirectLoadType &direct_load_type, const transaction::ObTransID &trans_id, const int64_t &seq_no,
      const uint64_t src_tenant_id, transaction::ObTxDesc* tx_desc)
    : data_tablet_id_(tablet_id),
      ls_id_(ls_id),
      trans_version_(trans_version),
      direct_load_type_(direct_load_type),
      trans_id_(trans_id),
      seq_no_(seq_no),
      src_tenant_id_(src_tenant_id),
      tx_desc_(tx_desc)

  { }
  ~ObBatchSliceWriteInfo() = default;
  TO_STRING_KV(K(ls_id_), K(data_tablet_id_), K(trans_version_), K(direct_load_type_), K(src_tenant_id_), KPC(tx_desc_));
public:
  common::ObTabletID data_tablet_id_;
  share::ObLSID ls_id_;
  int64_t trans_version_;
  ObDirectLoadType direct_load_type_;
  transaction::ObTransID trans_id_;
  int64_t seq_no_; //
  uint64_t src_tenant_id_;
  transaction::ObTxDesc* tx_desc_;
};

struct ObTabletDirectLoadMgrKey final
{
public:
  ObTabletDirectLoadMgrKey()
    : tablet_id_(), is_full_direct_load_(false)
  { }
  ObTabletDirectLoadMgrKey(const common::ObTabletID &tablet_id, const bool is_full_direct_load)
    : tablet_id_(tablet_id), is_full_direct_load_(is_full_direct_load)
  { }
  ~ObTabletDirectLoadMgrKey() = default;
  uint64_t hash() const {
    return tablet_id_.hash() + murmurhash(&is_full_direct_load_, sizeof(is_full_direct_load_), 0);
  }
  int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
  bool is_valid() const { return tablet_id_.is_valid(); }
  bool operator == (const ObTabletDirectLoadMgrKey &other) const {
        return tablet_id_ == other.tablet_id_ && is_full_direct_load_ == other.is_full_direct_load_; }
  TO_STRING_KV(K_(tablet_id), K_(is_full_direct_load));
public:
  common::ObTabletID tablet_id_;
  bool is_full_direct_load_;
};

struct ObDirectLoadSliceInfo final
{
public:
  ObDirectLoadSliceInfo()
    : is_full_direct_load_(false), is_lob_slice_(false), ls_id_(), data_tablet_id_(), slice_id_(-1),
      context_id_(0), src_tenant_id_(MTL_ID())
    { }
  ~ObDirectLoadSliceInfo() = default;
  bool is_valid() const { return ls_id_.is_valid() && data_tablet_id_.is_valid() && slice_id_ >= 0 && context_id_ >= 0 && src_tenant_id_ > 0; }
  TO_STRING_KV(K_(is_full_direct_load), K_(is_lob_slice), K_(ls_id), K_(data_tablet_id), K_(slice_id), K_(context_id), K_(src_tenant_id));
public:
  bool is_full_direct_load_;
  bool is_lob_slice_;
  share::ObLSID ls_id_;
  common::ObTabletID data_tablet_id_;
  int64_t slice_id_;
  int64_t context_id_;
  uint64_t src_tenant_id_;
DISALLOW_COPY_AND_ASSIGN(ObDirectLoadSliceInfo);
};

struct ObTableSchemaItem final
{
public:
  ObTableSchemaItem()
    : is_column_store_(false), is_index_table_(false), is_unique_index_(false), rowkey_column_num_(0),
      compress_type_(NONE_COMPRESSOR), lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD)
  {}
  ~ObTableSchemaItem() { reset(); }
  void reset()
  {
    is_column_store_ = false;
    is_index_table_ = false;
    is_unique_index_ = false;
    rowkey_column_num_ = 0;
    compress_type_ = NONE_COMPRESSOR;
    lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
  }
  TO_STRING_KV(K(is_column_store_), K(is_index_table_), K(is_unique_index_), K(rowkey_column_num_),
               K(compress_type_), K_(lob_inrow_threshold));

public:
  bool is_column_store_;
  bool is_index_table_;
  bool is_unique_index_;
  int64_t rowkey_column_num_;
  common::ObCompressorType compress_type_;
  int64_t lob_inrow_threshold_;
};

struct ObColumnSchemaItem final
{
public:
  ObColumnSchemaItem()
    : is_valid_(false), col_type_(), col_accuracy_()
  {}
  ObColumnSchemaItem(const ObColumnSchemaItem &other)
  {
    *this = other;
  }
  ~ObColumnSchemaItem() { reset(); }
  void reset()
  {
    is_valid_ = false;
    col_type_.reset();
    col_accuracy_.reset();
  }
  ObColumnSchemaItem &operator=(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    return *this;
  }
  int assign(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K(is_valid_), K(col_type_), K(col_accuracy_));
public:
  bool is_valid_;
  common::ObObjMeta col_type_;
  ObAccuracy col_accuracy_;
};

// usued in replay replay and runtime execution
struct ObDirectInsertCommonParam final
{
public:
  ObDirectInsertCommonParam()
    : ls_id_(), tablet_id_(), direct_load_type_(DIRECT_LOAD_INVALID), data_format_version_(0), read_snapshot_(0)
  {}
  ~ObDirectInsertCommonParam() = default;
  bool is_valid() const { return ls_id_.is_valid() && tablet_id_.is_valid()
      && data_format_version_ >= 0 && read_snapshot_ >= 0 && DIRECT_LOAD_INVALID <= direct_load_type_ && direct_load_type_ <= DIRECT_LOAD_MAX;
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(direct_load_type), K_(data_format_version), K_(read_snapshot));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObDirectLoadType direct_load_type_;
  uint64_t data_format_version_;
  // read_snapshot_ is used to scan the source data.
  // For full direct load task, it is also the commit version of the target macro block.
  int64_t read_snapshot_;
};

// only used in runtime execution
struct ObDirectInsertRuntimeOnlyParam final
{
public:
  ObDirectInsertRuntimeOnlyParam()
    : exec_ctx_(nullptr), task_id_(0), table_id_(OB_INVALID_ID), schema_version_(0), task_cnt_(0), need_online_opt_stat_gather_(false), tx_desc_(nullptr), trans_id_(), seq_no_(0), parallel_(1)
  {}
  ~ObDirectInsertRuntimeOnlyParam() = default;
  bool is_valid() const { return OB_INVALID_ID != task_id_ && OB_INVALID_ID != table_id_ && schema_version_ > 0 && task_cnt_ >= 0; }
  TO_STRING_KV(KP_(exec_ctx), K_(task_id), K_(table_id), K_(schema_version), K_(task_cnt), K_(need_online_opt_stat_gather), KP_(tx_desc), K_(trans_id), K_(seq_no), K_(parallel));
public:
  sql::ObExecContext *exec_ctx_;
  int64_t task_id_;
  int64_t table_id_;
  int64_t schema_version_;
  int64_t task_cnt_;
  bool need_online_opt_stat_gather_;
  transaction::ObTxDesc *tx_desc_;
  // default value is invalid tx_id,
  // participant tx_id for the incremental direct load,
  // and invalid tx_id for the full_direct_load.
  transaction::ObTransID trans_id_;
  // default value is 0,
  // sequence number for the incremental direct load,
  // fixed 0 for the full direct load.
  int64_t seq_no_; //
  int64_t parallel_; // used to decide wehter need to use compress temp data in rescan task.
};

// full parameters used by runtime execution
struct ObTabletDirectLoadInsertParam final
{
public:
  ObTabletDirectLoadInsertParam()
    : common_param_(), runtime_only_param_(), is_replay_(false)
  {}
  ~ObTabletDirectLoadInsertParam() = default;
  bool is_valid() const {
      return (!is_replay_ && (common_param_.is_valid() && runtime_only_param_.is_valid()))
          || (is_replay_ && common_param_.is_valid());
  }
  int assign(const ObTabletDirectLoadInsertParam &other_param);
  TO_STRING_KV(K_(common_param), K_(runtime_only_param), K_(is_replay));
public:
  ObDirectInsertCommonParam common_param_;
  ObDirectInsertRuntimeOnlyParam runtime_only_param_;
  bool is_replay_;
};

// for ddl insert row.
class ObDDLInsertRowIterator : public ObIStoreRowIterator
{
public:
  ObDDLInsertRowIterator();
  virtual ~ObDDLInsertRowIterator();
  int init(
      sql::ObPxMultiPartSSTableInsertOp *op,
      const bool is_slice_empty,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t rowkey_cnt,
      const int64_t snapshot_version,
      const int64_t context_id,
      const int64_t parallel_idx);
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    const bool skip_lob = false;
    return get_next_row(skip_lob, row);
  }
  int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row);
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(current_tablet_id), K_(current_row), K_(is_slice_empty), K_(is_next_row_cached), K_(rowkey_count), K_(snapshot_version),
      K_(lob_slice_id), K_(lob_id_cache), K_(context_id), K_(macro_seq));
public:
  int switch_to_new_lob_slice();
  int close_lob_sstable_slice();
  inline int64_t get_lob_slice_id() { return lob_slice_id_; }
  inline share::ObTabletCacheInterval &get_lob_id_cache() { return lob_id_cache_; }
private:
  static const int64_t AUTO_INC_CACHE_SIZE = 5000000; // 500w.
  bool is_inited_;
  ObArenaAllocator lob_allocator_;
  sql::ObPxMultiPartSSTableInsertOp *op_;
  share::ObLSID ls_id_;
  common::ObTabletID current_tablet_id_; // data_tablet_id rather than lob_meta_tablet_id.
  blocksstable::ObDatumRow current_row_;
  bool is_next_row_cached_;
  bool is_slice_empty_; // without data.
  int64_t rowkey_count_;
  int64_t snapshot_version_;
  int64_t lob_slice_id_;
  share::ObTabletCacheInterval lob_id_cache_;
  int64_t context_id_;
  blocksstable::ObMacroDataSeq macro_seq_;
};

class ObLobMetaRowIterator : public ObIStoreRowIterator
{
public:
  ObLobMetaRowIterator();
  virtual ~ObLobMetaRowIterator();
  int init(ObLobMetaWriteIter *iter,
            const transaction::ObTransID &trans_id,
            const int64_t trans_version,
            const int64_t sql_no,
            const ObDirectLoadType direct_load_type);
  void reset();
  void reuse();
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override;

private:
  int64_t get_seq_no() const;

public:
  bool is_inited_;
  ObLobMetaWriteIter *iter_;
  transaction::ObTransID trans_id_;
  int64_t trans_version_;
  int64_t sql_no_;
  blocksstable::ObDatumRow tmp_row_;
  ObLobMetaWriteResult lob_meta_write_result_;
  ObDirectLoadType direct_load_type_;
};

struct ObTabletDDLParam final
{
public:
  ObTabletDDLParam();
  ~ObTabletDDLParam();
  bool is_valid() const;
  TO_STRING_KV(K_(direct_load_type),
               K_(ls_id),
               K_(start_scn),
               K_(commit_scn),
               K_(data_format_version),
               K_(table_key),
               K_(snapshot_version),
               K_(trans_id));
public:
  ObDirectLoadType direct_load_type_;
  share::ObLSID ls_id_;
  share::SCN start_scn_;
  share::SCN commit_scn_;
  uint64_t data_format_version_;
  ObITable::TableKey table_key_;
  int64_t snapshot_version_; // used for full direct load only.
  transaction::ObTransID trans_id_; // used for incremental direct load only
};

struct ObDDLTableMergeDagParam : public share::ObIDagInitParam
{
public:
  ObDDLTableMergeDagParam()
    : direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
      ls_id_(),
      tablet_id_(),
      rec_scn_(share::SCN::min_scn()),
      is_commit_(false),
      start_scn_(share::SCN::min_scn()),
      data_format_version_(0),
      snapshot_version_(0)
  { }
  bool is_valid() const
  {
    bool is_valid = data_format_version_ > 0 && snapshot_version_ > 0;
    if (is_full_direct_load(direct_load_type_)) {
      is_valid = ls_id_.is_valid() && tablet_id_.is_valid() && start_scn_.is_valid_and_not_min();
    } else if (is_incremental_direct_load(direct_load_type_)) {
      is_valid = ls_id_.is_valid() && tablet_id_.is_valid();
    }
    return is_valid;
  }
  virtual ~ObDDLTableMergeDagParam() = default;
  TO_STRING_KV(K_(direct_load_type), K_(ls_id), K_(tablet_id), K_(rec_scn), K_(is_commit), K_(start_scn),
               K_(data_format_version), K_(snapshot_version));
public:
  ObDirectLoadType direct_load_type_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN rec_scn_;
  bool is_commit_;
  share::SCN start_scn_; // start log ts at schedule, for skipping expired task
  uint64_t data_format_version_;
  int64_t snapshot_version_;
};

// column organization of row in ObChunkDatumStore::SotredRow
// <rowkey_columns, multi_version_columns, other_columns_if_exist>
// this organization is same as row of all_cg in macro block
// and match the column index in ObStorageColumnGroupSchema
class ObTabletSliceStore
{
public:
  ObTabletSliceStore() {}
  virtual ~ObTabletSliceStore() {}
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) = 0;
  virtual int close() = 0;
  virtual int64_t get_row_count() const { return 0; } // dummy one
  virtual int64_t get_next_block_start_seq() const { return -1; } // invalid block start seq.
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObChunkSliceStore : public ObTabletSliceStore
{
public:
  ObChunkSliceStore() : is_inited_(false), target_store_idx_(-1), row_cnt_(0), arena_allocator_(nullptr), cg_schemas_(), datum_stores_(), rowkey_column_count_(0)
  {
    cg_schemas_.set_attr(ObMemAttr(MTL_ID(), "ChunkSlicStoreC"));
    datum_stores_.set_attr(ObMemAttr(MTL_ID(), "ChunkSlicStoreD"));
  }
  virtual ~ObChunkSliceStore() { reset(); }
  int init(const int64_t rowkey_column_count, const ObStorageSchema *storage_schema, ObArenaAllocator &allocator,
           const ObIArray<ObColumnSchemaItem> &col_schema, const int64_t dir_id, const int64_t parallelism);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int close() override;
  void reset();
  virtual int64_t get_row_count() const { return row_cnt_; }
  TO_STRING_KV(K(is_inited_), K(target_store_idx_), K(row_cnt_), KP(arena_allocator_), K(datum_stores_), K(endkey_), K(rowkey_column_count_), K(cg_schemas_));
private:
  int prepare_datum_stores(const uint64_t tenant_id, const ObStorageSchema *storage_schema, ObIAllocator &allocator,
                           const ObIArray<ObColumnSchemaItem> &col_array, const int64_t dir_id, const int64_t parallelism);
  int64_t calc_chunk_limit(const ObStorageColumnGroupSchema &cg_schema);
public:
  bool is_inited_;
  int64_t target_store_idx_;
  int64_t row_cnt_;
  ObArenaAllocator *arena_allocator_;
  ObArray<ObStorageColumnGroupSchema> cg_schemas_;
  ObArray<sql::ObCompactStore *> datum_stores_;
  blocksstable::ObDatumRowkey endkey_;
  int64_t rowkey_column_count_;
};

class ObMacroBlockSliceStore: public ObTabletSliceStore
{
public:
  ObMacroBlockSliceStore()
   : is_inited_(false), ddl_redo_callback_(nullptr) {}
  virtual ~ObMacroBlockSliceStore() {
    if (ddl_redo_callback_ != nullptr) {
      common::ob_delete(ddl_redo_callback_);
    }
  }
  int init(
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &data_seq,
      const share::SCN &start_scn);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int close() override;
  virtual int64_t get_next_block_start_seq() const override { return macro_block_writer_.get_last_macro_seq(); }
  TO_STRING_KV(K(is_inited_), K(macro_block_writer_));
private:
  bool is_inited_;
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
};

class ObTabletDirectLoadMgr;

struct ObInsertMonitor final{
public:
  ObInsertMonitor(int64_t &tmp_insert_row, int64_t &cg_insert_row):inserted_row_cnt_(tmp_insert_row), inserted_cg_row_cnt_(cg_insert_row)
  {};
  ~ObInsertMonitor();
  void set(sql::ObMonitorNode &op_monitor_info);

public:
  int64_t &inserted_row_cnt_;
  int64_t &inserted_cg_row_cnt_;
};

class ObCOSliceWriter;
class ObDirectLoadSliceWriter final
{
public:
  ObDirectLoadSliceWriter();
  ~ObDirectLoadSliceWriter();
  int init(
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &start_seq);
  int fill_sstable_slice(
      const share::SCN &start_scn,
      const uint64_t table_id,
      const ObTabletID &curr_tablet_id,
      const ObStorageSchema *storage_schema,
      ObIStoreRowIterator *row_iter,
      const ObTableSchemaItem &schema_item,
      const ObDirectLoadType &direct_load_type,
      const ObArray<ObColumnSchemaItem> &column_items,
      const int64_t dir_id,
      const int64_t parallelism,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = NULL);
  int fill_lob_sstable_slice(
      const uint64_t table_id,
      ObIAllocator &allocator,
      ObIAllocator &iter_allocator,
      const share::SCN &start_scn,
      const ObBatchSliceWriteInfo &info,
      share::ObTabletCacheInterval &pk_interval,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      const int64_t lob_inrow_threshold,
      blocksstable::ObDatumRow &datum_row);
  // fill lob meta row into macro block
  int fill_lob_meta_sstable_slice(
      const share::SCN &start_scn,
      const uint64_t table_id,
      const ObTabletID &curr_tablet_id,
      ObIStoreRowIterator *row_iter,
      int64_t &affected_rows);
  int close();
  int fill_column_group(
      const ObStorageSchema *storage_schema,
      const share::SCN &start_scn,
      ObInsertMonitor *monitor_node = NULL);
  int fill_aggregated_column_group(
      const int64_t cg_idx,
      ObCOSliceWriter *cur_writer,
      ObIArray<sql::ObCompactStore *> &datum_stores);
  void set_row_offset(const int64_t row_offset) { row_offset_ = row_offset; }
  int64_t get_row_count() const { return nullptr == slice_store_ ? 0 : slice_store_->get_row_count(); }
  int64_t get_row_offset() const { return row_offset_; }
  blocksstable::ObMacroDataSeq &get_start_seq() { return start_seq_; }
  bool is_empty() const { return 0 == get_row_count(); }
  bool need_column_store() const { return need_column_store_; }
  ObTabletSliceStore *get_slice_store() const { return slice_store_; }
  void cancel() { ATOMIC_SET(&is_canceled_, true); }
  int64_t get_next_block_start_seq() const { return nullptr == slice_store_ ? start_seq_.get_data_seq() /*slice empty*/ : slice_store_->get_next_block_start_seq(); }
  TO_STRING_KV(K(is_inited_), K(need_column_store_), K(is_canceled_), K(start_seq_), KPC(slice_store_), K(row_offset_));
private:
  int fill_lob_into_memtable( // for version < 4.3.0.0
      ObIAllocator &allocator,
      const ObBatchSliceWriteInfo &info,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      const int64_t lob_inrow_threshold,
      blocksstable::ObDatumRow &datum_row);
  int fill_lob_into_macro_block( // for version >= 4.3.0.0
      ObIAllocator &allocator,
      ObIAllocator &iter_allocator,
      const share::SCN &start_scn,
      const ObBatchSliceWriteInfo &info,
      share::ObTabletCacheInterval &pk_interval,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      const int64_t lob_inrow_threshold,
      blocksstable::ObDatumRow &datum_row);

  int check_null(
      const bool is_index_table,
      const int64_t rowkey_column_cnt,
      const blocksstable::ObDatumRow &row_val) const;
  int prepare_slice_store_if_need(
      const int64_t schema_rowkey_column_num,
      const bool is_slice_store,
      const int64_t dir_id,
      const int64_t parallelism,
      const ObStorageSchema *storage_schema,
      const share::SCN &start_scn);
  int report_unique_key_dumplicated(
      const int ret_code,
      const uint64_t table_id,
      const blocksstable::ObDatumRow &datum_row,
      const common::ObTabletID &tablet_id,
      int &report_ret_code);
  int prepare_iters(
      ObIAllocator &allocator,
      ObIAllocator &iter_allocator,
      blocksstable::ObStorageDatum &datum,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t trans_version,
      const ObObjType &obj_type,
      const ObCollationType &cs_type,
      const ObLobId &lob_id,
      const transaction::ObTransID trans_id,
      const int64_t seq_no,
      const int64_t timeout_ts,
      const int64_t lob_inrow_threshold,
      const uint64_t src_tenant_id,
      const ObDirectLoadType direct_load_type,
      transaction::ObTxDesc* tx_desc,
      ObLobMetaRowIterator *&row_iter);
  int mock_chunk_store(const int64_t row_cnt);
private:
  bool is_inited_;
  bool need_column_store_;
  bool is_canceled_;
  blocksstable::ObMacroDataSeq start_seq_;
  ObTabletDirectLoadMgr *tablet_direct_load_mgr_;
  ObTabletSliceStore *slice_store_;
  ObLobMetaWriteIter *meta_write_iter_;
  ObLobMetaRowIterator *row_iterator_;
  common::ObArenaAllocator allocator_;
  int64_t row_offset_;
};

class ObCOSliceWriter
{
public:
  ObCOSliceWriter() : is_inited_(false), cg_idx_(-1), cg_schema_(nullptr), data_desc_(true /*is ddl*/) {}
  ~ObCOSliceWriter() {}
  int init(
      const ObStorageSchema *storage_schema,
      const int64_t cg_idx,
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &start_seq,
      const int64_t row_id_offset,
      const share::SCN &start_scn);
  void reset();
  int append_row(
      const sql::ObChunkDatumStore::StoredRow *stored_row);
  int project_cg_row(
      const ObStorageColumnGroupSchema &cg_schema,
      const sql::ObChunkDatumStore::StoredRow *stored_row,
      blocksstable::ObDatumRow &cg_row);
  int close();
  bool is_inited() { return is_inited_; }
  TO_STRING_KV(K(is_inited_), K(cg_idx_), KPC(cg_schema_), K(macro_block_writer_), K(data_desc_), K(cg_row_));
private:
  bool is_inited_;
  int64_t cg_idx_;
  const ObStorageColumnGroupSchema *cg_schema_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObSSTableIndexBuilder index_builder_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
  storage::ObDDLRedoLogWriterCallback flush_callback_;
  blocksstable::ObDatumRow cg_row_;
};

struct ObTabletDirectLoadExecContextId final
{
public:
  ObTabletDirectLoadExecContextId()
    : tablet_id_(), context_id_(OB_INVALID_ID)
  {}
  ~ObTabletDirectLoadExecContextId() = default;
  uint64_t hash() const {
    return tablet_id_.hash() + murmurhash(&context_id_, sizeof(context_id_), 0);
  }
  int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
  bool is_valid() const { return tablet_id_.is_valid() && context_id_ >= 0; }
  bool operator == (const ObTabletDirectLoadExecContextId &other) const {
        return tablet_id_ == other.tablet_id_ && context_id_ == other.context_id_; }
  TO_STRING_KV(K_(tablet_id), K_(context_id));
public:
  common::ObTabletID tablet_id_;
  int64_t context_id_;
};

struct ObTabletDirectLoadExecContext final
{
public:
  ObTabletDirectLoadExecContext()
    : start_scn_(), execution_id_(0), seq_interval_task_id_(0)
  {}
  ~ObTabletDirectLoadExecContext() { reset(); }
  void reset() {
    start_scn_.reset();
    execution_id_ = 0;
    seq_interval_task_id_ = 0;
  }
  TO_STRING_KV(K_(start_scn), K_(execution_id), K_(seq_interval_task_id));
public:
  share::SCN start_scn_;
  int64_t execution_id_;
  int64_t seq_interval_task_id_;
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_OB_DIRECT_LOAD_COMMON_H
