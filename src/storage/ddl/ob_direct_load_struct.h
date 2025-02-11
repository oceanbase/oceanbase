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
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/ob_ddl_common.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "sql/engine/basic/chunk_store/ob_compact_store.h"
#include "storage/ob_i_table.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/ddl/ob_ddl_seq_generator.h"

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
class ObInsertMonitor;
class ObCOSliceWriter;

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
  ObTabletDirectLoadMgrKey() // hash needed.
    : tablet_id_(), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), context_id_(0)
  { }

  ObTabletDirectLoadMgrKey(const common::ObTabletID &tablet_id, const ObDirectLoadType &type, const int64_t ctx_id) // make sure type and ctx_id is correct.
    : tablet_id_(tablet_id)
  {
    if (is_shared_storage_dempotent_mode(type)) {
      direct_load_type_ = DIRECT_LOAD_DDL_V2;
      context_id_ = ctx_id;
    } else if (is_full_direct_load(type)) {
      direct_load_type_ = DIRECT_LOAD_DDL;
      context_id_ = 0;
    } else {
      direct_load_type_ = DIRECT_LOAD_INCREMENTAL;
      context_id_ = ctx_id;
    }
  }
  ObTabletDirectLoadMgrKey(const common::ObTabletID &tablet_id, const ObDirectLoadType &type) // constructor for shared nothing only.
    : tablet_id_(tablet_id), context_id_(0)
  {
    direct_load_type_ = is_full_direct_load(type) ? DIRECT_LOAD_DDL : DIRECT_LOAD_INCREMENTAL;
  }
  ObTabletDirectLoadMgrKey(const common::ObTabletID &tablet_id, const int64_t ctx_id) // constructor for shared storage only.
    : tablet_id_(tablet_id), direct_load_type_(DIRECT_LOAD_DDL_V2), context_id_(ctx_id)
  {
  }
  ~ObTabletDirectLoadMgrKey() = default;
  uint64_t hash() const {
    return tablet_id_.hash() + murmurhash(&direct_load_type_, sizeof(direct_load_type_), 0)
        + murmurhash(&context_id_, sizeof(context_id_), 0);
  }
  int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
  bool is_valid() const {
    return tablet_id_.is_valid() && is_valid_direct_load(direct_load_type_) &&
      (((is_shared_storage_dempotent_mode(direct_load_type_) || is_incremental_direct_load(direct_load_type_)) ? context_id_ > 0 : context_id_ == 0)); }
  bool operator == (const ObTabletDirectLoadMgrKey &other) const {
        return tablet_id_ == other.tablet_id_ && direct_load_type_ == other.direct_load_type_
            && context_id_ == other.context_id_; }
  TO_STRING_KV(K_(tablet_id), K_(direct_load_type), K_(context_id));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadType direct_load_type_;
  int64_t context_id_;
};

struct ObDirectLoadSliceInfo final
{
public:
  ObDirectLoadSliceInfo()
    : is_full_direct_load_(false), is_lob_slice_(false), ls_id_(), data_tablet_id_(), slice_id_(-1),
      context_id_(0), src_tenant_id_(MTL_ID()), is_task_finish_(false), total_slice_cnt_(-1), slice_idx_(0), merge_slice_idx_(0)
    { }
  ~ObDirectLoadSliceInfo() = default;
  bool is_valid() const { return ls_id_.is_valid() && data_tablet_id_.is_valid() && slice_id_ >= 0 && context_id_ >= 0 && src_tenant_id_ > 0; }
  TO_STRING_KV(K_(is_full_direct_load), K_(is_lob_slice), K_(ls_id), K_(data_tablet_id), K_(slice_id), K_(context_id), K_(src_tenant_id), K_(is_task_finish), K_(total_slice_cnt), K_(slice_idx), K_(merge_slice_idx));
public:
  bool is_full_direct_load_;
  bool is_lob_slice_;
  share::ObLSID ls_id_;
  common::ObTabletID data_tablet_id_;
  int64_t slice_id_;
  int64_t context_id_;
  uint64_t src_tenant_id_;
  bool is_task_finish_;
  int64_t total_slice_cnt_;
  int64_t slice_idx_;
  int64_t merge_slice_idx_;
DISALLOW_COPY_AND_ASSIGN(ObDirectLoadSliceInfo);
};

struct ObTableSchemaItem final
{
public:
  ObTableSchemaItem()
    : is_column_store_(false), is_index_table_(false), is_unique_index_(false), has_lob_rowkey_(false),
      rowkey_column_num_(0), compress_type_(NONE_COMPRESSOR), lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
      vec_idx_param_(), vec_dim_(0)
  {}
  ~ObTableSchemaItem() { reset(); }
  void reset()
  {
    is_column_store_ = false;
    is_index_table_ = false;
    is_unique_index_ = false;
    has_lob_rowkey_ = false;
    rowkey_column_num_ = 0;
    compress_type_ = NONE_COMPRESSOR;
    lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
    vec_idx_param_.reset();
    vec_dim_ = 0;
  }
  TO_STRING_KV(K(is_column_store_), K(is_index_table_), K(is_unique_index_), K_(has_lob_rowkey),
    K(rowkey_column_num_), K(compress_type_), K_(lob_inrow_threshold), K_(vec_idx_param), K_(vec_dim));

public:
  bool is_column_store_;
  bool is_index_table_;
  bool is_unique_index_;
  bool has_lob_rowkey_;
  int64_t rowkey_column_num_;
  common::ObCompressorType compress_type_;
  int64_t lob_inrow_threshold_;
  ObString vec_idx_param_;
  int64_t vec_dim_;
};

struct ObColumnSchemaItem final
{
public:
  ObColumnSchemaItem()
    : is_valid_(false), col_type_(), col_accuracy_(), column_flags_(0)
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
    column_flags_ = 0;
  }
  ObColumnSchemaItem &operator=(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    column_flags_ = other.column_flags_;
    return *this;
  }
  int assign(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    column_flags_ = other.column_flags_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K(is_valid_), K(col_type_), K(col_accuracy_), K(column_flags_));
public:
  bool is_valid_;
  common::ObObjMeta col_type_;
  ObAccuracy col_accuracy_;
  int64_t column_flags_;
};

// usued in replay replay and runtime execution
struct ObDirectInsertCommonParam final
{
public:
  ObDirectInsertCommonParam()
    : ls_id_(), tablet_id_(), direct_load_type_(DIRECT_LOAD_INVALID), data_format_version_(0), read_snapshot_(0), replay_normal_in_cs_replica_(false), is_no_logging_(false)

  {}
  ~ObDirectInsertCommonParam() = default;
  bool is_valid() const { return ls_id_.is_valid() && tablet_id_.is_valid()
      && data_format_version_ >= 0 && read_snapshot_ >= 0 && DIRECT_LOAD_INVALID <= direct_load_type_ && direct_load_type_ <= DIRECT_LOAD_MAX;
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(direct_load_type), K_(data_format_version), K_(read_snapshot), K_(replay_normal_in_cs_replica), K_(is_no_logging));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObDirectLoadType direct_load_type_;
  uint64_t data_format_version_;
  // read_snapshot_ is used to scan the source data.
  // For full direct load task, it is also the commit version of the target macro block.
  int64_t read_snapshot_;
  bool replay_normal_in_cs_replica_; // when ddl and add cs replica are concurrent, leader may write normal clog
  bool is_no_logging_;
};

// only used in runtime execution
struct ObDirectInsertRuntimeOnlyParam final
{
public:
  ObDirectInsertRuntimeOnlyParam()
    : exec_ctx_(nullptr),
      task_id_(0),
      table_id_(OB_INVALID_ID),
      schema_version_(0),
      task_cnt_(0),
      need_online_opt_stat_gather_(false),
      tx_desc_(nullptr),
      trans_id_(),
      seq_no_(0),
      parallel_(1),
      max_batch_size_(0)
  {
  }
  ~ObDirectInsertRuntimeOnlyParam() = default;
  bool is_valid() const { return OB_INVALID_ID != task_id_ && OB_INVALID_ID != table_id_ && schema_version_ > 0 && task_cnt_ >= 0; }
  TO_STRING_KV(KP_(exec_ctx),
               K_(task_id),
               K_(table_id),
               K_(schema_version),
               K_(task_cnt),
               K_(need_online_opt_stat_gather),
               KP_(tx_desc),
               K_(trans_id),
               K_(seq_no),
               K_(parallel),
               K_(max_batch_size));
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
  int64_t max_batch_size_;
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

// for sql one slice.
class ObDDLSliceRowIterator : public ObIStoreRowIterator
{
public:
  ObDDLSliceRowIterator(
      sql::ObPxMultiPartSSTableInsertOp *op,
      const common::ObTabletID &tablet_id,
      const bool is_slice_empty,
      const bool is_index_table,
      const int64_t rowkey_cnt,
      const int64_t snapshot_version,
      const ObTabletSliceParam &ddl_slice_param,
      const bool need_idempotent_autoinc_val,
      const int64_t table_all_slice_count,
      const int64_t table_level_slice_idx,
      const int64_t autoinc_range_interval);
  virtual ~ObDDLSliceRowIterator();
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override;
  TO_STRING_KV(K_(tablet_id), K_(current_row), K_(is_slice_empty), K_(rowkey_col_cnt), K_(snapshot_version),
    K_(is_next_row_cached), K_(ddl_slice_param), K_(is_index_table));
private:
  sql::ObPxMultiPartSSTableInsertOp *op_;
  common::ObTabletID tablet_id_; // data_tablet_id rather than lob_meta_tablet_id.
  blocksstable::ObDatumRow current_row_;
  int64_t rowkey_col_cnt_;
  int64_t snapshot_version_;
  ObTabletSliceParam ddl_slice_param_;
  int64_t table_all_slice_count_;
  int64_t table_level_slice_idx_;
  int64_t cur_row_idx_;
  int64_t autoinc_range_interval_;
  bool is_slice_empty_; // without data.
  bool is_next_row_cached_;
  bool need_idempotent_autoinc_val_;
  bool is_index_table_;
  bool has_lob_rowkey_;
};

// for ddl insert row.
class ObDirectLoadMgrAgent;
class ObDDLInsertRowIterator : public ObIDirectLoadRowIterator
{
public:
  ObDDLInsertRowIterator();
  virtual ~ObDDLInsertRowIterator();
  int init(
      const uint64_t source_tenant_id,
      ObDirectLoadMgrAgent &agent,
      ObIStoreRowIterator *slice_row_iter,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t context_id,
      const ObTabletSliceParam &tablet_slice_param,
      const int64_t lob_cols_cnt,
      const int64_t total_slice_cnt,
      const bool is_skip_lob = false);
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    // const bool skip_lob = false;
    return get_next_row(is_skip_lob_, row);
  }
  int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row) override;
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(current_tablet_id), K_(context_id), K_(macro_seq),
      K_(lob_id_generator), K_(lob_id_cache), K_(lob_slice_id), K_(lob_cols_cnt), K_(is_skip_lob), K_(total_slice_cnt));
public:
  int switch_to_new_lob_slice();
  int close_lob_sstable_slice();
  inline int64_t get_lob_slice_id() { return lob_slice_id_; }
  inline share::ObTabletCacheInterval &get_lob_id_cache() { return lob_id_cache_; }
private:
  static const int64_t AUTO_INC_CACHE_SIZE = 5000000; // 500w.
  bool is_inited_;
  uint64_t source_tenant_id_; // recover table ddl task needs it to scan rows.
  ObDirectLoadMgrAgent *ddl_agent_;
  ObIStoreRowIterator *slice_row_iter_;
  share::ObLSID ls_id_;
  common::ObTabletID current_tablet_id_; // data_tablet_id rather than lob_meta_tablet_id.
  int64_t context_id_;
  blocksstable::ObMacroDataSeq macro_seq_;
  ObArenaAllocator lob_allocator_;
  ObDDLSeqGenerator lob_id_generator_;
  share::ObTabletCacheInterval lob_id_cache_;
  int64_t lob_slice_id_;
  int64_t lob_cols_cnt_;
  bool is_skip_lob_;
  int64_t total_slice_cnt_;
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
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) = 0;
  virtual int close() = 0;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) = 0;
  virtual void cancel() = 0; // for exit fill_column_group
  virtual int64_t get_row_count() const { return 0; } // dummy one
  virtual int64_t get_next_block_start_seq() const { return -1; } // invalid block start seq.
  virtual ObDatumRowkey get_compare_key() const { return ObDatumRowkey(); }
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObVectorIndexBaseSliceStore : public ObTabletSliceStore
{
public:
  ObVectorIndexBaseSliceStore()
    : is_inited_(false),
      row_cnt_(0),
      vec_dim_(0),
      cur_row_pos_(0),
      tablet_id_(),
      vec_idx_param_(),
      current_row_()
  {}
  virtual ~ObVectorIndexBaseSliceStore() { reset(); }
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual void cancel() override {}
  virtual int64_t get_row_count() const { return row_cnt_; }

  virtual int init(ObTabletDirectLoadMgr *tablet_direct_load_mgr,
                   const ObString vec_idx_param,
                   const int64_t vec_dim,
                   const ObIArray<ObColumnSchemaItem> &col_array)
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual int get_next_vector_data_row(
      const int64_t rowkey_cnt,
      const int64_t column_cnt,
      const int64_t snapshot_version,
      ObVectorIndexAlgorithmType index_type,
      blocksstable::ObDatumRow *&datum_row)
  {
    return OB_NOT_IMPLEMENT;
  }
  void reset();
  TO_STRING_KV(K_(is_inited), K_(row_cnt), K_(vec_dim), K_(tablet_id), K_(vec_idx_param));
public:
  bool is_inited_;
  int64_t row_cnt_;
  int64_t vec_dim_;
  int64_t cur_row_pos_;
  ObTabletID tablet_id_;
  ObString vec_idx_param_;
  blocksstable::ObDatumRow current_row_;
};

class ObVectorIndexSliceStore : public ObVectorIndexBaseSliceStore
{
public:
  ObVectorIndexSliceStore()
    : ObVectorIndexBaseSliceStore(), vec_allocator_("VecIdxSS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      tmp_allocator_("VecIdxSSAR", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ctx_(), vector_vid_col_idx_(-1),
      vector_col_idx_(-1)
  {}
  virtual ~ObVectorIndexSliceStore() { reset(); }
  virtual int init(ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const ObString vec_idx_param,
      const int64_t vec_dim,
      const ObIArray<ObColumnSchemaItem> &col_array) override;
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  void reset();
  // vector index functions
  int serialize_vector_index(
    ObIAllocator *allocator,
    transaction::ObTxDesc *tx_desc,
    int64_t lob_inrow_threshold,
    ObVectorIndexAlgorithmType &type);
  virtual int get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row) override;
  INHERIT_TO_STRING_KV("ObVectorIndexBaseSliceStore", ObVectorIndexBaseSliceStore,
      K(ctx_), K(vector_vid_col_idx_), K(vector_col_idx_), K(vector_key_col_idx_), K(vector_data_col_idx_));
private:
  static const int64_t OB_VEC_IDX_SNAPSHOT_KEY_LENGTH = 256;
  bool is_vec_idx_col_invalid(const int64_t column_cnt) const;
public:
  ObArenaAllocator vec_allocator_;
  ObArenaAllocator tmp_allocator_;
  ObVecIdxSnapshotDataWriteCtx ctx_;
  int32_t vector_vid_col_idx_;
  int32_t vector_col_idx_;
  int32_t vector_key_col_idx_;
  int32_t vector_data_col_idx_;
};

class ObIvfSliceStore : public ObVectorIndexBaseSliceStore
{
public:
  ObIvfSliceStore()
    : ObVectorIndexBaseSliceStore(),
      tmp_allocator_("IvfSSTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      helper_guard_()
  {}

  virtual ~ObIvfSliceStore() {}
  virtual void reset();
  virtual int build_clusters() = 0;
  virtual int is_empty(bool &empty) = 0;

protected:
  template<typename HelperType>
  int get_spec_ivf_helper(HelperType *&helper);

  ObArenaAllocator vec_allocator_;
  ObArenaAllocator tmp_allocator_;
  ObIvfBuildHelperGuard helper_guard_;
};

template<typename HelperType>
int ObIvfSliceStore::get_spec_ivf_helper(HelperType *&helper)
{
  int ret = OB_SUCCESS;
  helper = nullptr;
  if (OB_NOT_NULL(helper_guard_.get_helper())) {
    helper = reinterpret_cast<HelperType *>(helper_guard_.get_helper());
  }

  if (OB_ISNULL(helper)) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "fail to get spec helper", K(ret), KP(helper_guard_.get_helper()));
  }
  return ret;
}

// for IVF Flat/SQ/PQ calculate cluster center
class ObIvfCenterSliceStore : public ObIvfSliceStore
{
public:
  ObIvfCenterSliceStore()
    : ObIvfSliceStore(),
      center_id_col_idx_(-1),
      center_vector_col_idx_(-1)
  {}

  virtual ~ObIvfCenterSliceStore() { reset(); }
  virtual int init(ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const ObString vec_idx_param,
      const int64_t vec_dim,
      const ObIArray<ObColumnSchemaItem> &col_array) override;
  virtual void reset() override;
  virtual int build_clusters() override;
  // for write: ObDirectLoadSliceWriter::fill_sstable_slice -> get_next_vector_data_row
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int is_empty(bool &empty) override;
  virtual int get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row) override;
public:
  ObArenaAllocator tmp_allocator_;
  int32_t center_id_col_idx_;
  int32_t center_vector_col_idx_;
};

// for IVF SQ8 Meta
class ObIvfSq8MetaSliceStore : public ObIvfSliceStore
{
public:
  ObIvfSq8MetaSliceStore()
    : ObIvfSliceStore(),
      meta_id_col_idx_(-1),
      meta_vector_col_idx_(-1)
  {}

  virtual ~ObIvfSq8MetaSliceStore() { reset(); }
  virtual int init(ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const ObString vec_idx_param,
      const int64_t vec_dim,
      const ObIArray<ObColumnSchemaItem> &col_array) override;
  virtual void reset() override;
  virtual int build_clusters() override;
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row) override;
  virtual int is_empty(bool &empty) override;

private:
  int32_t meta_id_col_idx_;
  int32_t meta_vector_col_idx_;
};

// for IVF-PQ pq cluster center
class ObIvfPqSliceStore : public ObIvfSliceStore
{
public:
  ObIvfPqSliceStore()
    : ObIvfSliceStore(),
      pq_center_id_col_idx_(-1),
      pq_center_vector_col_idx_(-1)
  {}

  virtual ~ObIvfPqSliceStore() { reset(); }
  virtual int init(ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const ObString vec_idx_param,
      const int64_t vec_dim,
      const ObIArray<ObColumnSchemaItem> &col_array) override;
  virtual void reset() override;
  virtual int build_clusters() override;
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row) override;
  virtual int is_empty(bool &empty) override;

private:
  int32_t pq_center_id_col_idx_;
  int32_t pq_center_vector_col_idx_;
};

class ObChunkSliceStore : public ObTabletSliceStore
{
public:
  ObChunkSliceStore() : is_inited_(false), is_canceled_(false), target_store_idx_(-1), row_cnt_(0), arena_allocator_(nullptr), cg_schemas_(), datum_stores_(), rowkey_column_count_(0)
  {
    cg_schemas_.set_attr(ObMemAttr(MTL_ID(), "ChunkSlicStoreC"));
    datum_stores_.set_attr(ObMemAttr(MTL_ID(), "ChunkSlicStoreD"));
  }
  virtual ~ObChunkSliceStore() { reset(); }
  int init(const int64_t rowkey_column_count, const ObStorageSchema *storage_schema, ObArenaAllocator &allocator,
           const ObIArray<ObColumnSchemaItem> &col_schema, const int64_t dir_id, const int64_t parallelism);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override;
  virtual void cancel() override { is_canceled_ = true; }
  void reset();
  virtual int64_t get_row_count() const { return row_cnt_; }
  virtual ObDatumRowkey get_compare_key() const override { return endkey_; }
  TO_STRING_KV(K(is_inited_), K(is_canceled_), K(target_store_idx_), K(row_cnt_), KP(arena_allocator_), K(datum_stores_), K(endkey_), K(rowkey_column_count_), K(cg_schemas_));
private:
  int prepare_datum_stores(const uint64_t tenant_id, const ObStorageSchema *storage_schema, ObIAllocator &allocator,
                           const ObIArray<ObColumnSchemaItem> &col_array, const int64_t dir_id, const int64_t parallelism);
  int64_t calc_chunk_limit(const ObStorageColumnGroupSchema &cg_schema);
public:
  bool is_inited_;
  bool is_canceled_;
  int64_t target_store_idx_;
  int64_t row_cnt_;
  ObArenaAllocator *arena_allocator_;
  ObArray<ObStorageColumnGroupSchema> cg_schemas_;
  ObArray<sql::ObCompactStore *> datum_stores_;
  blocksstable::ObDatumRowkey endkey_;
  int64_t rowkey_column_count_;
};

class ObChunkBatchSliceStore : public ObTabletSliceStore
{
public:
  ObChunkBatchSliceStore()
    : arena_allocator_(nullptr),
      cg_ctxs_(),
      column_count_(0),
      rowkey_column_count_(0),
      row_cnt_(0),
      start_key_(),
      is_canceled_(false),
      is_inited_(false)
  {
    cg_ctxs_.set_attr(ObMemAttr(MTL_ID(), "DL_CK_CG_CTXS"));
  }
  virtual ~ObChunkBatchSliceStore() { reset(); }
  void reset();
  int init(const int64_t rowkey_column_count,
           const ObStorageSchema *storage_schema,
           ObArenaAllocator &allocator,
           const ObIArray<ObColumnSchemaItem> &col_schema,
           const int64_t dir_id,
           const int64_t parallelism,
           const int64_t max_batch_size);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override;
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override;
  virtual void cancel() override { is_canceled_ = true; }
  virtual int64_t get_row_count() const { return row_cnt_; }
  virtual ObDatumRowkey get_compare_key() const override { return start_key_; }
  TO_STRING_KV(KP_(arena_allocator),
               K_(cg_ctxs),
               K_(column_count),
               K_(rowkey_column_count),
               K_(row_cnt),
               K_(start_key),
               K_(is_canceled),
               K_(is_inited));
private:
  int init_start_key();
  int prepare_column_group_ctxs(const uint64_t tenant_id,
                                const ObStorageSchema *storage_schema,
                                ObIAllocator &allocator,
                                const ObIArray<ObColumnSchemaItem> &col_array,
                                const int64_t dir_id,
                                const int64_t parallelism,
                                const int64_t max_batch_size);
private:
  struct ColumnGroupCtx
  {
  public:
    ColumnGroupCtx()
      : allocator_("DL_CK_VECTOR"),
        cg_schema_(),
        store_(),
        vectors_(),
        append_vectors_(),
        brs_()
    {
      allocator_.set_tenant_id(MTL_ID());
      vectors_.set_block_allocator(ModulePageAllocator(allocator_));
      append_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    }
    TO_STRING_KV(K_(cg_schema),
                //  K_(store),
                 K(vectors_.count()),
                 K(append_vectors_.count()),
                 K_(brs));
  public:
    ObArenaAllocator allocator_;
    ObStorageColumnGroupSchema cg_schema_;
    sql::ObTempColumnStore store_;
    ObArray<ObIVector *> vectors_;
    ObArray<ObIVector *> append_vectors_;
    sql::ObBatchRows brs_;
  };
public:
  ObArenaAllocator *arena_allocator_;
  ObArray<ColumnGroupCtx *> cg_ctxs_;
  int64_t column_count_;
  int64_t rowkey_column_count_;
  int64_t row_cnt_;
  blocksstable::ObDatumRowkey start_key_;
  bool is_canceled_;
  bool is_inited_;
};

class ObColumnSliceStore : public ObTabletSliceStore
{
public:
  ObColumnSliceStore();
  virtual ~ObColumnSliceStore();
  int init(ObIAllocator &allocator,
           const ObStorageSchema *storage_schema,
           ObTabletDirectLoadMgr *tablet_direct_load_mgr,
           const blocksstable::ObMacroDataSeq &data_seq,
           const int64_t slice_idx,
           const share::SCN &start_scn,
           const int64_t dir_id,
           const bool is_cs_replica);
  void destroy();
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int close() override;
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override { return OB_ERR_UNEXPECTED; }
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override { return OB_ERR_UNEXPECTED; }
  virtual void cancel() override {}
  TO_STRING_KV(K(is_inited_), K(row_count_), K(dumped_row_count_), K(ls_id_), K(tablet_id_), K(direct_load_type_),
      K(tenant_data_version_), K(snapshot_version_), K(ddl_task_id_), K(parallel_task_count_), K(is_micro_index_clustered_),
      K(tablet_transfer_seq_), K(slice_idx_), K(start_scn_), K(is_cs_replica_), K(start_seqs_), K(datum_stores_), KPC(storage_schema_));
private:
  int check_need_dump(bool &need_dump);
  int dump_macro_block();
private:
  bool is_inited_;
  ObIAllocator *allocator_;
  const ObStorageSchema *storage_schema_; // caller hold this when fill slice
  int64_t row_count_;
  ObArray<sql::ObCompactStore *> datum_stores_;
  int64_t dumped_row_count_;

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDirectLoadType direct_load_type_;
  int64_t tenant_data_version_;
  int64_t snapshot_version_;
  int64_t ddl_task_id_; // for clog wirte speed limit
  int64_t parallel_task_count_; // for gc flag in shared storage mode
  bool is_micro_index_clustered_;
  int64_t tablet_transfer_seq_;

  ObArray<blocksstable::ObMacroDataSeq> start_seqs_;
  int64_t slice_idx_;
  share::SCN start_scn_;
  bool is_cs_replica_;
};

class ObColumnBatchSliceStore : public ObTabletSliceStore
{
public:
  ObColumnBatchSliceStore()
    : arena_allocator_(nullptr),
      cg_ctxs_(),
      column_count_(0),
      rowkey_column_count_(0),
      row_cnt_(0),
      slice_idx_(0),
      merge_slice_idx_(0),
      storage_schema_(nullptr),
      direct_load_type_(DIRECT_LOAD_MAX),
      tenant_data_version_(0),
      snapshot_version_(0),
      ddl_task_id_(0),
      parallel_task_count_(0),
      is_micro_index_clustered_(false),
      tablet_transfer_seq_(share::OB_INVALID_TRANSFER_SEQ),
      is_cs_replica_(false),
      is_inited_(false)
  {
    cg_ctxs_.set_attr(ObMemAttr(MTL_ID(), "DL_CL_CG_CTXS"));
  }
  virtual ~ObColumnBatchSliceStore() { reset(); }
  void reset();
  int init(const int64_t rowkey_column_count,
           const ObStorageSchema *storage_schema,
           ObArenaAllocator &allocator,
           const int64_t slice_idx,
           const int64_t merge_slice_idx,
           ObTabletDirectLoadMgr *tablet_direct_load_mgr,
           const blocksstable::ObMacroDataSeq &data_seq,
           const share::SCN &start_scn,
           const int64_t dir_id,
           const bool is_cs_replica,
           const int64_t max_batch_size);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override;
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual void cancel() override {}
  virtual int64_t get_row_count() const { return row_cnt_; }
  TO_STRING_KV(KP_(arena_allocator),
               K_(cg_ctxs),
               K_(column_count),
               K_(rowkey_column_count),
               K_(row_cnt),
               K_(slice_idx),
               K_(merge_slice_idx),
               K_(ls_id),
               K_(tablet_id),
               K_(direct_load_type),
               K_(tenant_data_version),
               K_(snapshot_version),
               K_(ddl_task_id),
               K_(parallel_task_count),
               K_(is_micro_index_clustered),
               K_(tablet_transfer_seq),
               K_(start_scn),
               K_(is_cs_replica),
               K_(is_inited));
private:
  int prepare_column_group_ctxs(const uint64_t tenant_id,
                                const ObStorageSchema *storage_schema,
                                ObIAllocator &allocator,
                                const ObIArray<ObColumnSchemaItem> &col_array,
                                const blocksstable::ObMacroDataSeq &data_seq,
                                const int64_t dir_id,
                                const int64_t max_batch_size);
  int check_need_dump(bool &need_dump);
  int dump_macro_block();
private:
  struct ColumnGroupCtx
  {
  public:
    ColumnGroupCtx()
      : allocator_("DL_CL_VECTOR"),
        cg_schema_(),
        store_(),
        append_vectors_(),
        brs_(),
        data_seq_(0),
        dumped_row_count_(0)
    {
      allocator_.set_tenant_id(MTL_ID());
      append_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
      datum_rows_.vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    }
    TO_STRING_KV(K_(cg_schema),
                //  K_(store),
                 K(append_vectors_.count()),
                 K_(brs),
                 K_(datum_rows),
                 K_(data_seq),
                 K_(dumped_row_count));
  public:
    ObArenaAllocator allocator_;
    ObStorageColumnGroupSchema cg_schema_;
    sql::ObTempColumnStore store_;
    ObArray<ObIVector *> append_vectors_;
    sql::ObBatchRows brs_;
    blocksstable::ObBatchDatumRows datum_rows_;
    blocksstable::ObMacroDataSeq data_seq_;
    int64_t dumped_row_count_;
  };
public:
  ObArenaAllocator *arena_allocator_;
  ObArray<ColumnGroupCtx *> cg_ctxs_;
  int64_t column_count_;
  int64_t rowkey_column_count_;
  int64_t row_cnt_;
  int64_t slice_idx_;
  int64_t merge_slice_idx_;
  const ObStorageSchema *storage_schema_; // caller hold this when fill slice
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDirectLoadType direct_load_type_;
  int64_t tenant_data_version_;
  int64_t snapshot_version_;
  int64_t ddl_task_id_; // for clog wirte speed limit
  int64_t parallel_task_count_; // for gc flag in shared storage mode
  bool is_micro_index_clustered_;
  int64_t tablet_transfer_seq_;
  share::SCN start_scn_;
  bool is_cs_replica_;
  bool is_inited_;
};

class ObMacroBlockSliceStore: public ObTabletSliceStore
{
public:
  ObMacroBlockSliceStore()
   : is_inited_(false), need_process_cs_replica_(false), ddl_redo_callback_(nullptr), macro_block_writer_(true /* use buffer */) {}
  virtual ~ObMacroBlockSliceStore() {
    if (ddl_redo_callback_ != nullptr) {
      common::ob_delete(ddl_redo_callback_);
    }
  }
  int init(
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &data_seq,
      const share::SCN &start_scn,
      const bool need_process_cs_replica = false);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override;
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override
  {
    return OB_ERR_UNEXPECTED;
  }
  virtual void cancel() override {}
  virtual int64_t get_next_block_start_seq() const override { return macro_block_writer_.get_last_macro_seq(); }
  TO_STRING_KV(K(is_inited_), K_(need_process_cs_replica), K(macro_block_writer_));
private:
  bool is_inited_;
  bool need_process_cs_replica_;
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
};

class ObMultiSliceStore : public ObTabletSliceStore
{
public:
  ObMultiSliceStore();
  virtual ~ObMultiSliceStore();
  int init(
      ObArenaAllocator &allocator,
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &data_seq,
      const int64_t slice_idx,
      const int64_t merge_slice_idx,
      const share::SCN &start_scn,
      const int64_t rowkey_column_count,
      const ObStorageSchema *storage_schema,
      const ObIArray<ObColumnSchemaItem> &col_schema,
      const int64_t dir_id,
      const int64_t parallelism,
      const bool use_batch_store,
      const int64_t max_batch_size);
  virtual int append_row(const blocksstable::ObDatumRow &datum_row) override;
  virtual int append_batch(const blocksstable::ObBatchDatumRows &datum_rows) override;
  virtual int close() override;
  virtual int fill_column_group(const int64_t cg_idx, ObCOSliceWriter *writer, ObInsertMonitor *insert_monitor) override;
  virtual void cancel() override;
  virtual int64_t get_row_count() const override;
  virtual int64_t get_next_block_start_seq() const override;
  virtual ObDatumRowkey get_compare_key() const override;
  void reset();
  TO_STRING_KV(K_(is_inited), KPC_(cs_replica_schema), KPC_(row_slice_store), KPC_(column_slice_store));
private:
  void free_memory(ObArenaAllocator &allocator);
private:
  bool is_inited_;
  ObArenaAllocator *arena_allocator_;
  ObStorageSchema *cs_replica_schema_;
  ObMacroBlockSliceStore *row_slice_store_;
  ObTabletSliceStore *column_slice_store_;
};

class ObTabletDirectLoadMgr;

struct ObInsertMonitor final{
public:
  ObInsertMonitor(int64_t &tmp_scan_row, int64_t &tmp_insert_row, int64_t &cg_insert_row)
    : scanned_row_cnt_(tmp_scan_row), inserted_row_cnt_(tmp_insert_row), inserted_cg_row_cnt_(cg_insert_row)
  {};
  ~ObInsertMonitor();

public:
  int64_t &scanned_row_cnt_;
  int64_t &inserted_row_cnt_;
  int64_t &inserted_cg_row_cnt_;
};

class ObDirectLoadSliceWriter final
{
public:
  enum class ObDirectLoadSliceWriterType: uint8_t
  {
    ROW_STORE_WRITER   = 0,
    COL_STORE_WRITER   = 1,
    COL_REPLICA_WRITER = 2,
    WRITER_TYPE_MAX    = 3
  };
public:
  ObDirectLoadSliceWriter();
  ~ObDirectLoadSliceWriter();
  int init(
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &start_seq,
      const int64_t slice_idx,
      const int64_t merge_slice_idx);
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
  int fill_sstable_slice(
      const share::SCN &start_scn,
      const uint64_t table_id,
      const ObTabletID &curr_tablet_id,
      const ObStorageSchema *storage_schema,
      const blocksstable::ObBatchDatumRows &datum_rows,
      const ObTableSchemaItem &schema_item,
      const ObDirectLoadType &direct_load_type,
      const ObArray<ObColumnSchemaItem> &column_items,
      const int64_t dir_id,
      const int64_t parallelism,
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
      const ObTableSchemaItem &schema_item,
      blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice(
      const uint64_t table_id,
      ObIAllocator &allocator,
      ObIAllocator &iter_allocator,
      const share::SCN &start_scn,
      const ObBatchSliceWriteInfo &info,
      share::ObTabletCacheInterval &pk_interval,
      const ObArray<int64_t> &lob_column_idxs,
      const ObArray<common::ObObjMeta> &col_types,
      const ObTableSchemaItem &schema_item,
      blocksstable::ObBatchDatumRows &datum_rows);
  int close();
  // fill lob meta row into macro block
  int fill_lob_meta_sstable_slice(
      const share::SCN &start_scn,
      const uint64_t table_id,
      const ObTabletID &curr_tablet_id,
      ObIStoreRowIterator *row_iter,
      int64_t &affected_rows);
  int fill_column_group(
      const ObStorageSchema *storage_schema,
      const share::SCN &start_scn,
      ObInsertMonitor *monitor_node = NULL);
  int fill_aggregated_column_group(
      const int64_t cg_idx,
      ObCOSliceWriter *cur_writer);
  int fill_vector_index_data(
    const int64_t snapshot_version,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    const ObTableSchemaItem &schema_item,
    ObInsertMonitor* insert_monitor);
  void set_row_offset(const int64_t row_offset) { row_offset_ = row_offset; }
  int64_t get_row_count() const { return nullptr == slice_store_ ? 0 : slice_store_->get_row_count(); }
  int64_t get_row_offset() const { return row_offset_; }
  blocksstable::ObMacroDataSeq &get_start_seq() { return start_seq_; }
  bool is_empty() const { return 0 == get_row_count(); }
  bool is_row_store_writer() const { return ObDirectLoadSliceWriterType::ROW_STORE_WRITER == writer_type_; }
  bool is_col_store_writer() const { return ObDirectLoadSliceWriterType::COL_STORE_WRITER == writer_type_; }
  bool is_cs_replica_write() const { return ObDirectLoadSliceWriterType::COL_REPLICA_WRITER == writer_type_; }
  bool need_column_store() const { return is_col_store_writer() || is_cs_replica_write(); }
  ObTabletSliceStore *get_slice_store() const { return slice_store_; }
  ObDirectLoadSliceWriterType get_writer_type() const { return writer_type_; }
  void cancel();
  int64_t get_next_block_start_seq() const { return nullptr == slice_store_ ? start_seq_.get_data_seq() /*slice empty*/ : slice_store_->get_next_block_start_seq(); }
  TO_STRING_KV(K(is_inited_), K(writer_type_), K(is_canceled_), K(start_seq_), K(slice_idx_), K(merge_slice_idx_), KPC(slice_store_), K(row_offset_));
private:
  int fill_lob_into_memtable( // for version < 4.3.0.0
      ObIAllocator &allocator,
      const ObBatchSliceWriteInfo &info,
      const common::ObObjMeta &col_type,
      const ObLobStorageParam &lob_storage_param,
      blocksstable::ObStorageDatum &datum);
  int fill_lob_into_macro_block( // for version >= 4.3.0.0
      ObIAllocator &allocator,
      ObIAllocator &iter_allocator,
      const share::SCN &start_scn,
      const ObBatchSliceWriteInfo &info,
      share::ObTabletCacheInterval &pk_interval,
      const common::ObObjMeta &col_type,
      const ObLobStorageParam &lob_storage_param,
      blocksstable::ObStorageDatum &datum);
  int check_null_and_length(
      const bool is_index_table,
      const bool has_lob_rowkey,
      const int64_t rowkey_column_cnt,
      const blocksstable::ObDatumRow &row_val) const;
  int check_null_and_length(
      const bool is_index_table,
      const bool has_lob_rowkey,
      const int64_t rowkey_column_cnt,
      const blocksstable::ObBatchDatumRows &datum_rows);
  int prepare_slice_store_if_need(
      const int64_t schema_rowkey_column_num,
      const bool is_slice_store,
      const int64_t dir_id,
      const int64_t parallelism,
      const ObStorageSchema *storage_schema,
      const share::SCN &start_scn,
      const ObString vec_idx_param,
      const int64_t vec_dim,
      const bool use_vector_store = false,
      const int64_t max_batch_size = 0);
  int prepare_vector_slice_store(
      const ObStorageSchema *storage_schema,
      const ObString vec_idx_param,
      const int64_t vec_dim);
  int report_unique_key_dumplicated(
      const int ret_code,
      const uint64_t table_id,
      const blocksstable::ObDatumRow &datum_row,
      const common::ObTabletID &tablet_id,
      int &report_ret_code);
  int report_unique_key_dumplicated(
      const int ret_code,
      const uint64_t table_id,
      const blocksstable::ObBatchDatumRows &datum_rows,
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
      const transaction::ObTransID trans_id,
      const int64_t seq_no,
      const int64_t timeout_ts,
      const ObLobStorageParam &lob_storage_param,
      const uint64_t src_tenant_id,
      const ObDirectLoadType direct_load_type,
      transaction::ObTxDesc* tx_desc,
      share::ObTabletCacheInterval &pk_interval,
      ObLobMetaRowIterator *&row_iter);
  int mock_chunk_store(const int64_t row_cnt);
  int inner_fill_column_group(
      ObTabletSliceStore *slice_store,
      const ObStorageSchema *storage_schema,
      const share::SCN &start_scn,
      ObInsertMonitor *monitor_node = NULL);
  int inner_fill_vector_index_data(
      ObMacroBlockSliceStore *&macro_block_slice_store,
      ObVectorIndexBaseSliceStore *vec_idx_slice_store,
      const int64_t snapshot_version,
      const ObStorageSchema *storage_schema,
      const SCN &start_scn,
      ObVectorIndexAlgorithmType index_type,
      ObInsertMonitor* insert_monitor);
  int inner_fill_hnsw_vector_index_data(
      ObVectorIndexSliceStore &vec_idx_slice_store,
      const int64_t snapshot_version,
      const ObStorageSchema *storage_schema,
      const SCN &start_scn,
      const int64_t lob_inrow_threshold,
      ObInsertMonitor* insert_monitor);
  int inner_fill_ivf_vector_index_data(
      ObIvfSliceStore &vec_idx_slice_store,
      const int64_t snapshot_version,
      const ObStorageSchema *storage_schema,
      const SCN &start_scn,
      const int64_t lob_inrow_threshold,
      ObInsertMonitor* insert_monitor);
private:
  bool is_inited_;
  ObDirectLoadSliceWriterType writer_type_;
  bool is_canceled_;
  blocksstable::ObMacroDataSeq start_seq_;
  int64_t slice_idx_;
  int64_t merge_slice_idx_;
  ObTabletDirectLoadMgr *tablet_direct_load_mgr_;
  ObTabletSliceStore *slice_store_;
  ObLobMetaWriteIter *meta_write_iter_;
  ObLobMetaRowIterator *row_iterator_;
  common::ObArenaAllocator allocator_;
  common::ObIAllocator *lob_allocator_;
  ObSEArray<int64_t, 256> rowkey_lengths_;
  int64_t row_offset_;
};

class ObCOSliceWriter
{
public:
  ObCOSliceWriter() : is_inited_(false), cg_idx_(-1), cg_schema_(nullptr), data_desc_(),
  index_builder_(true /*use buffer*/), macro_block_writer_(true /*use buffer*/) {}
  ~ObCOSliceWriter() {}
  int init(
      const ObStorageSchema *storage_schema,
      const int64_t cg_idx,
      ObTabletDirectLoadMgr *tablet_direct_load_mgr,
      const blocksstable::ObMacroDataSeq &start_seq,
      const int64_t row_id_offset,
      const share::SCN &start_scn,
      const bool with_cs_replica);
  void reset();
  int append_row(
      const sql::ObChunkDatumStore::StoredRow *stored_row);
  int append_batch(const ObIArray<ObIVector *> &vectors, const int64_t batch_size);
  static int project_cg_row(
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
  blocksstable::ObBatchDatumRows datum_rows_;
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

struct ObTabletDirectLoadBatchSliceKey final
{
public:
  ObTabletDirectLoadBatchSliceKey()
    : tablet_id_(), tid_(GETTID())
  {}
  explicit ObTabletDirectLoadBatchSliceKey(const ObTabletID &tablet_id)
    : tablet_id_(tablet_id), tid_(GETTID())
  {}
  ObTabletDirectLoadBatchSliceKey(const ObTabletDirectLoadBatchSliceKey &other) {
    tablet_id_ = other.tablet_id_;
    tid_ = other.tid_;
  }
  ObTabletDirectLoadBatchSliceKey &operator=(const ObTabletDirectLoadBatchSliceKey &other) {
    tablet_id_ = other.tablet_id_;
    tid_ = other.tid_;
    return *this;
  }
  ~ObTabletDirectLoadBatchSliceKey() = default;
  uint64_t hash() const {
    return tablet_id_.hash() + murmurhash(&tid_, sizeof(tid_), 0);
  }
  int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
  bool operator==(const ObTabletDirectLoadBatchSliceKey &other) const {
        return tablet_id_ == other.tablet_id_ && tid_ == other.tid_; }
  TO_STRING_KV(K_(tablet_id), K_(tid));
public:
  common::ObTabletID tablet_id_;
  int64_t tid_;
};

struct ObTabletDirectLoadSliceGroup final
{
public:
  ObTabletDirectLoadSliceGroup()
    : is_inited_(false), bucket_lock_(), batch_slice_map_(), allocator_()
  {
  }
  ~ObTabletDirectLoadSliceGroup()
  {
    reset();
  }
  int init(const int64_t task_cnt);
  void reset();
  int record_slice_id(const ObTabletDirectLoadBatchSliceKey &key, const int64_t slice_id);
  int get_slice_array(const ObTabletDirectLoadBatchSliceKey &key, ObArray<int64_t> &slice_array);
  int remove_slice_array(const ObTabletDirectLoadBatchSliceKey &key);
  TO_STRING_KV(K_(is_inited));
public:
  bool is_inited_;
  ObBucketLock bucket_lock_;
  hash::ObHashMap<ObTabletDirectLoadBatchSliceKey, ObArray<int64_t/*slices_array_idx*/> *> batch_slice_map_;
  ObConcurrentFIFOAllocator allocator_;
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_OB_DIRECT_LOAD_COMMON_H
