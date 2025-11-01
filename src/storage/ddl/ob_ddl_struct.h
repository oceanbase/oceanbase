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

#ifndef OCEANBASE_STORAGE_OB_DDL_STRUCT_H_
#define OCEANBASE_STORAGE_OB_DDL_STRUCT_H_

#include "lib/container/ob_array.h"
#include "share/ob_ddl_common.h"
#include "share/scn.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_direct_load_type.h"
namespace oceanbase
{
namespace storage
{

class ObMacroMetaStoreManager;
class ObDDLIndependentDag;
struct ObDDLTabletContext;

static const int64_t DDL_FLUSH_MACRO_BLOCK_TIMEOUT = 5 * 1000 * 1000;

static const int64_t SS_DDL_START_SCN_VAL = 100;

enum ObDDLMacroBlockType
{
  DDL_MB_INVALID_TYPE = 0,
  DDL_MB_DATA_TYPE = 1,
  DDL_MB_INDEX_TYPE = 2,
  DDL_MB_SSTABLE_META_TYPE = 3,
  DDL_MB_TABLET_META_TYPE = 4,
  DDL_MB_SS_EMPTY_DATA_TYPE = 5,
};

class ObDDLMacroHandle
{
public:
  ObDDLMacroHandle();
  ObDDLMacroHandle(const ObDDLMacroHandle &other);
  ObDDLMacroHandle &operator=(const ObDDLMacroHandle &other);
  ~ObDDLMacroHandle();
  bool is_valid() const { return block_id_.is_valid(); }
  int set_block_id(const blocksstable::MacroBlockId &block_id);
  int reset_macro_block_ref();
  const blocksstable::MacroBlockId &get_block_id() const { return block_id_; }
  TO_STRING_KV(K_(block_id));
private:
  blocksstable::MacroBlockId block_id_;
};

class ObDDLMacroBlock final
{
public:
  ObDDLMacroBlock();
  ~ObDDLMacroBlock();
  const blocksstable::MacroBlockId &get_block_id() const { return block_handle_.get_block_id(); }
  int deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const;
  int set_data_macro_meta(const blocksstable::MacroBlockId &macro_id,
                          const char* macor_block_buf,
                          const int64_t size,
                          const ObDDLMacroBlockType &block_type,
                          const bool force_set_macro_meta = false);
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  TO_STRING_KV(K_(block_handle),
               K_(logic_id),
               K_(block_type),
               K_(ddl_start_scn),
               K_(scn),
               K_(table_key),
               K_(end_row_id),
               K_(trans_id),
               KPC_(data_macro_meta),
               KP_(buf),
               K_(size),
               K_(merge_slice_idx),
               K_(seq_no));
public:
  ObArenaAllocator allocator_; // used to hold data_macro_meta_
  ObDDLMacroHandle block_handle_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  ObDDLMacroBlockType block_type_;
  share::SCN ddl_start_scn_;
  share::SCN scn_;
  ObITable::TableKey table_key_;
  int64_t end_row_id_;
  transaction::ObTransID trans_id_; // for incremental direct load only
  blocksstable::ObDataMacroBlockMeta *data_macro_meta_;
  const char* buf_; // only used for warm up
  int64_t size_;
  int64_t merge_slice_idx_;
  transaction::ObTxSEQ seq_no_; // for incremental direct load only
};

class ObDDLKV;
class ObDDLKVHandle final
{
public:
  ObDDLKVHandle() : ddl_kv_(nullptr), t3m_(nullptr), allocator_(nullptr) {}
  ObDDLKVHandle(const ObDDLKVHandle &other) : ddl_kv_(nullptr), t3m_(nullptr), allocator_(nullptr) { *this = other; }
  ObDDLKVHandle &operator =(const ObDDLKVHandle &other);
  ~ObDDLKVHandle() { reset(); }
  ObDDLKV* get_obj() const { return ddl_kv_; }
  bool is_valid() const;
  // for full direct load
  int set_obj(ObDDLKV *ddl_kv);
  // for incremental direct load
  int set_obj(ObTableHandleV2 &table_handle);
  void reset();
  DECLARE_TO_STRING;
private:
  ObDDLKV *ddl_kv_;
  ObTenantMetaMemMgr *t3m_;
  common::ObIAllocator *allocator_;
};


class ObTablet;
class ObTabletDirectLoadMgrHandle;
class ObDDLKVPendingGuard final
{
public:
  static int set_macro_block(
    ObTablet *tablet,
    const ObDDLMacroBlock &macro_block,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const ObDirectLoadType direct_load_type);
  static int set_skip_block_scn(ObTablet *tablet,
      const share::SCN &scn,
      const share::SCN &start_scn,
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const ObDirectLoadType direct_load_type);
public:
  ObDDLKVPendingGuard(
    ObTablet *tablet,
    const share::SCN &scn,
    const share::SCN &start_scn,
    const int64_t snapshot_version, // used for shared-storage mode.
    const uint64_t data_format_version, // used for shared-storage mode.
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const ObDirectLoadType direct_load_type,
    const transaction::ObTransID &trans_id = transaction::ObTransID(),
    const transaction::ObTxSEQ &seq_no = transaction::ObTxSEQ(),
    const ObITable::TableType table_type = ObITable::TableType::MAX_TABLE_TYPE);
  ~ObDDLKVPendingGuard();
  int get_ret() const { return ret_; }
  int get_ddl_kv(ObDDLKV *&kv);
  bool can_freeze() { return can_freeze_; }
  TO_STRING_KV(KP(tablet_), K(scn_), K(kv_handle_), K(ret_));
private:
  ObTablet *tablet_;
  share::SCN scn_;
  ObDDLKVHandle kv_handle_;
  int ret_;
  bool can_freeze_;
};

struct ObDDLMacroBlockRedoInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLMacroBlockRedoInfo();
  ~ObDDLMacroBlockRedoInfo() = default;
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  /*
   * For tow conditions:
   *   1. column store table, unnessasery to generate double redo clog.
   *   2. row store table, but unnessasery to process cs replica.
   *     (a) cs replica not exist, may not be created or is creating.
   *     (b) table is not user data table.
   */
  bool is_not_compat_cs_replica() const;
  // If cs replica exist, this redo clog is suitable for F/R replica.
  bool is_cs_replica_row_store() const;
  // If cs replica exist, this redo clog is suitable for C replica.
  bool is_cs_replica_column_store() const;
  void reset();
  TO_STRING_KV(K_(table_key),
               K_(data_buffer),
               K_(block_type),
               K_(logic_id),
               K_(start_scn),
               K_(data_format_version),
               K_(end_row_id),
               K_(type),
               K_(trans_id),
               K_(with_cs_replica),
               K_(macro_block_id),
               K_(parallel_cnt),
               K_(cg_cnt),
               K_(merge_slice_idx),
               K_(seq_no));
public:
  storage::ObITable::TableKey table_key_;
  ObString data_buffer_;
  ObDDLMacroBlockType block_type_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  share::SCN start_scn_;
  uint64_t data_format_version_;
  int64_t end_row_id_;
  /* type is not a relialbe val, only inc major may be write !!!!*/
  storage::ObDirectLoadType type_;
  transaction::ObTransID trans_id_; // for incremental direct load only
  bool with_cs_replica_;
  blocksstable::MacroBlockId macro_block_id_; // for shared storage mode
  // for shared storage gc occupy info
  int64_t parallel_cnt_;
  int64_t cg_cnt_;
  int64_t merge_slice_idx_;
  transaction::ObTxSEQ seq_no_; // for incremental direct load only
};

class ObBaseTabletDirectLoadMgr;
class ObTabletDirectLoadMgr;
class ObTabletFullDirectLoadMgr;
class ObTabletIncDirectLoadMgr;
class ObTabletDirectLoadMgrV3;
class ObTabletDirectLoadMgrHandle final
{
public:
  ObTabletDirectLoadMgrHandle();
  ~ObTabletDirectLoadMgrHandle();
  int set_obj(ObBaseTabletDirectLoadMgr *mgr);
  int assign(const ObTabletDirectLoadMgrHandle &handle);
  ObBaseTabletDirectLoadMgr *get_base_obj();
  const ObBaseTabletDirectLoadMgr *get_base_obj() const;

  ObTabletDirectLoadMgr *get_obj();
  const ObTabletDirectLoadMgr *get_obj() const;
  ObTabletFullDirectLoadMgr *get_full_obj() const;
  ObTabletIncDirectLoadMgr *get_inc_obj() const;
  void reset();
  bool is_valid() const;
  /*
    forbide to copy and move,
    since it would lead to invlaid count value and delete obj
  */
  ObTabletDirectLoadMgrHandle& operator=(const ObTabletDirectLoadMgrHandle&) = delete;
  ObTabletDirectLoadMgrHandle& operator=(ObTabletDirectLoadMgrHandle&&) = delete;
  TO_STRING_KV(KP_(tablet_mgr));
private:
  ObBaseTabletDirectLoadMgr *tablet_mgr_;
  ObIAllocator *allocator;
};

#ifdef OB_BUILD_SHARED_STORAGE
struct ObDDLFinishLogInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDDLFinishLogInfo();
  ~ObDDLFinishLogInfo() = default;
  bool is_valid() const;
  int assign(const ObDDLFinishLogInfo &other);
  void reset();
  TO_STRING_KV(K_(ls_id), K_(table_key), K_(data_buffer), K_(data_format_version));
public:
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  ObString data_buffer_;
  uint64_t data_format_version_;
};
#endif

class ObIDirectLoadRowIterator : public ObIStoreRowIterator
{
public:
  ObIDirectLoadRowIterator() {}
  virtual ~ObIDirectLoadRowIterator() {}
  virtual int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row) = 0;
};

enum ObDDLKVType {
  DDL_KV_INVALID = 0,
  DDL_KV_FULL = 1,
  DDL_KV_INC_MINOR = 2,
  DDL_KV_INC_MAJOR = 3,
  DDL_KV_MAX
};

static inline bool is_valid_ddl_kv(const ObDDLKVType &type)
{
  return (ObDDLKVType::DDL_KV_INVALID < type)
            && (ObDDLKVType::DDL_KV_MAX > type);
}

static inline bool is_full_ddl_kv(const ObDDLKVType &type)
{
  return (ObDDLKVType::DDL_KV_FULL == type);
}

static inline bool is_inc_minor_ddl_kv(const ObDDLKVType &type)
{
  return (ObDDLKVType::DDL_KV_INC_MINOR == type);
}

static inline bool is_inc_major_ddl_kv(const ObDDLKVType &type)
{
  return (ObDDLKVType::DDL_KV_INC_MAJOR == type);
}

static ObDDLKVType convert_direct_load_type_to_ddl_kv_type(const ObDirectLoadType &direct_load_type)
{
  ObDDLKVType ddl_kv_type = ObDDLKVType::DDL_KV_INVALID;
  switch (direct_load_type) {
    case ObDirectLoadType::DIRECT_LOAD_INCREMENTAL:
      ddl_kv_type = ObDDLKVType::DDL_KV_INC_MINOR;
      break;
    case ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR:
      ddl_kv_type = ObDDLKVType::DDL_KV_INC_MAJOR;
      break;
    default:
      ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
      break;
  }
  return ddl_kv_type;
}

struct ObInsertMonitor final {
public:
  ObInsertMonitor(int64_t &tmp_scan_row, int64_t &tmp_insert_row, int64_t &cg_insert_row)
      : scanned_row_cnt_(tmp_scan_row),
        inserted_row_cnt_(tmp_insert_row),
        inserted_cg_row_cnt_(cg_insert_row),
        vec_index_task_thread_pool_cnt_(nullptr),
        vec_index_task_total_cnt_(nullptr),
        vec_index_task_finish_cnt_(nullptr){};
  ~ObInsertMonitor() {}

public:
  int64_t &scanned_row_cnt_;
  int64_t &inserted_row_cnt_;
  int64_t &inserted_cg_row_cnt_;
  int64_t *vec_index_task_thread_pool_cnt_;
  int64_t *vec_index_task_total_cnt_;
  int64_t *vec_index_task_finish_cnt_;
};

struct ObDDLWriteStat final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLWriteStat();
  ~ObDDLWriteStat();
  bool is_valid() const;
  void reset();
  int assign(const ObDDLWriteStat &write_stat);
  bool operator != (const ObDDLWriteStat &other);
  TO_STRING_KV(K_(row_count))

public:
  int64_t row_count_;
};

struct ObDDLTaskParam
{
public:
  ObDDLTaskParam() : tenant_data_version_(0), snapshot_version_(0), schema_version_(0), ddl_task_id_(0), execution_id_(0),
    target_table_id_(0),  is_no_logging_(false), max_batch_size_(0), is_offline_index_rebuild_(false) {}
  void reset()
  {
    tenant_data_version_ = 0;
    snapshot_version_ = 0;
    schema_version_ = 0;
    ddl_task_id_ = 0;
    execution_id_ = 0;
    target_table_id_ = 0;
    is_no_logging_ = false;
    max_batch_size_ = 0;
    is_offline_index_rebuild_ = false;
  }
  bool is_valid() const { return ddl_task_id_ > 0 && execution_id_ >= 0 && tenant_data_version_ > 0 && snapshot_version_ >= 0 && target_table_id_ > 0 && schema_version_ > 0; }
  TO_STRING_KV(K_(ddl_task_id), K_(execution_id), K_(tenant_data_version), K_(snapshot_version), K_(target_table_id), K_(schema_version), K_(is_no_logging), K_(max_batch_size), K_(is_offline_index_rebuild));
public:
  /* necessary param */
  int64_t tenant_data_version_;
  int64_t snapshot_version_;

  /* optional param only used for leader major merge */
  int64_t schema_version_;
  int64_t ddl_task_id_;
  int64_t execution_id_;
  int64_t target_table_id_;
  bool is_no_logging_;
  int64_t max_batch_size_; // for batch rows when load data, from hint named load_batch_size
  bool is_offline_index_rebuild_;
};

struct ObDDLAutoincParam
{
public:
  ObDDLAutoincParam() : need_autoinc_(false), slice_count_(0), slice_idx_(0), autoinc_range_interval_(0) {}
  bool is_valid() const { return !need_autoinc_ || (slice_count_ > 0 && slice_idx_ >= 0 && autoinc_range_interval_ > 0); }
  TO_STRING_KV(K_(need_autoinc), K_(slice_count), K_(slice_idx), K_(autoinc_range_interval));
public:
  bool need_autoinc_;
  int64_t slice_count_;
  int64_t slice_idx_;
  int64_t autoinc_range_interval_;
};

struct ObTableSchemaItem final
{
public:
  ObTableSchemaItem()
    : is_column_store_(false), is_index_table_(false), is_unique_index_(false), has_lob_rowkey_(false),
      is_table_with_clustering_key_(false), is_vec_tablet_rebuild_(false),
      rowkey_column_num_(0), compress_type_(NONE_COMPRESSOR), lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
      vec_idx_param_(), vec_dim_(0), index_type_(INDEX_TYPE_IS_NOT)
  {}
  ~ObTableSchemaItem() { reset(); }
  bool is_skip_lob() const { return is_index_table_ || vec_dim_ > 0; }
  void reset()
  {
    is_column_store_ = false;
    is_index_table_ = false;
    is_unique_index_ = false;
    has_lob_rowkey_ = false;
    is_table_with_clustering_key_ = false;
    is_vec_tablet_rebuild_ = false;
    rowkey_column_num_ = 0;
    compress_type_ = NONE_COMPRESSOR;
    lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
    vec_idx_param_.reset();
    vec_dim_ = 0;
    index_type_ = INDEX_TYPE_IS_NOT;
  }
  TO_STRING_KV(K_(is_column_store), K_(is_index_table), K_(is_unique_index), K_(has_lob_rowkey),
    K_(is_table_with_clustering_key), K_(is_vec_tablet_rebuild),
    K_(rowkey_column_num), K_(compress_type), K_(lob_inrow_threshold), K_(vec_idx_param), K_(vec_dim),
    K_(index_type));

public:
  bool is_column_store_;
  bool is_index_table_;
  bool is_unique_index_;
  bool has_lob_rowkey_;
  bool is_table_with_clustering_key_;
  bool is_vec_tablet_rebuild_;
  int64_t rowkey_column_num_;
  common::ObCompressorType compress_type_;
  int64_t lob_inrow_threshold_;
  ObString vec_idx_param_;
  int64_t vec_dim_;
  ObIndexType index_type_;
};

struct ObColumnSchemaItem final
{
public:
  ObColumnSchemaItem()
    : is_valid_(false), col_type_(), col_accuracy_(), column_flags_(0), is_rowkey_column_(false), is_nullable_(false)
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
    is_rowkey_column_ = false;
    is_nullable_ = false;
  }
  ObColumnSchemaItem &operator=(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    column_flags_ = other.column_flags_;
    is_rowkey_column_ = other.is_rowkey_column_;
    is_nullable_ = other.is_nullable_;
    return *this;
  }
  int assign(const ObColumnSchemaItem &other)
  {
    is_valid_ = other.is_valid_;
    col_type_ = other.col_type_;
    col_accuracy_ = other.col_accuracy_;
    column_flags_ = other.column_flags_;
    is_rowkey_column_ = other.is_rowkey_column_;
    is_nullable_ = other.is_nullable_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(is_valid), K_(col_type), K_(col_accuracy), K_(column_flags), K_(is_rowkey_column), K_(is_nullable));
public:
  bool is_valid_;
  common::ObObjMeta col_type_;
  ObAccuracy col_accuracy_;
  int64_t column_flags_;
  bool is_rowkey_column_;
  bool is_nullable_;
};

// table schema and storage layer column schema for ddl
struct ObDDLTableSchema
{
public:
  static int fill_ddl_table_schema(const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   common::ObArenaAllocator &allocator,
                                   ObDDLTableSchema &ddl_table_schema);
private:
  static int fill_vector_index_schema_item(const uint64_t tenant_id,
                                           ObSchemaGetterGuard &schema_guard,
                                           const ObTableSchema *table_schema,
                                           common::ObArenaAllocator &allocator,
                                           const ObIArray<ObColDesc> &column_descs,
                                           ObDDLTableSchema &ddl_table_schema);

public:
  ObDDLTableSchema() : storage_schema_(nullptr), lob_meta_storage_schema_(nullptr) {}
  TO_STRING_KV(K_(table_id), K_(table_item), KPC_(storage_schema), KPC_(lob_meta_storage_schema), K_(reshape_column_idxs), K_(lob_column_idxs), K_(column_items));
  void reset();
  int assign(const ObDDLTableSchema &other);

public:
  ObTableID table_id_;
  // sql layer table level schema
  ObTableSchemaItem table_item_;

  ObStorageSchema *storage_schema_;
  ObStorageSchema *lob_meta_storage_schema_;

  // column schemas in storage layer
  // column layout: rowkey columns, multiversion columns, other columns
  ObArray<ObColumnSchemaItem> column_items_;
  ObArray<int64_t> reshape_column_idxs_;
  ObArray<int64_t> lob_column_idxs_;
  ObArray<share::schema::ObColDesc> column_descs_;
};

// transaction info for inc direct load
struct ObDirectLoadTxInfo
{
public:
  ObDirectLoadTxInfo() : tx_desc_(nullptr), trans_id_(), seq_no_(0) {}
  void reset()
  {
    tx_desc_ = nullptr;
    trans_id_.reset();
    seq_no_ = 0;
  }
  bool is_valid() const { return nullptr != tx_desc_ && trans_id_.is_valid() && seq_no_ > 0; }
  TO_STRING_KV(K_(trans_id), K_(seq_no), K_(tx_desc));
public:
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTransID trans_id_;
  int64_t seq_no_;
};

struct ObWriteTabletParam
{
public:
  ObWriteTabletParam() :
    tablet_transfer_seq_(0),
    is_micro_index_clustered_(false),
    with_cs_replica_(false),
    storage_schema_(nullptr),
    cs_replica_storage_schema_(nullptr) { }
  void reset()
  {
    tablet_transfer_seq_ = 0;
    is_micro_index_clustered_ = false;
    with_cs_replica_ = false;
    reorganization_scn_.reset();
    storage_schema_ = nullptr;
    cs_replica_storage_schema_ = nullptr;
  }
  TO_STRING_KV(K_(tablet_transfer_seq), K_(is_micro_index_clustered), K_(with_cs_replica), K_(reorganization_scn),
               KP_(storage_schema), KP_(cs_replica_storage_schema));
public:
  int64_t tablet_transfer_seq_; // get from tablet meta
  bool is_micro_index_clustered_; // get from tablet meta
  bool with_cs_replica_; // get from tablet meta
  share::SCN reorganization_scn_; // get from tablet meta
  ObStorageSchema *storage_schema_; // references to ObDDLTableSchema
  ObStorageSchema *cs_replica_storage_schema_;
};

struct ObWriteMacroParam final
{
public:
  ObWriteMacroParam()
    : ls_id_(), tablet_id_(), tenant_data_version_(0), is_no_logging_(false), macro_meta_store_mgr_(nullptr),
      schema_version_(0), slice_idx_(0), slice_count_(0), ddl_thread_count_(0), snapshot_version_(0), direct_load_type_(DIRECT_LOAD_INVALID),
      task_id_(0), is_index_table_(false), tx_info_(), ddl_table_schema_(), tablet_param_(), lob_meta_tablet_param_(),
      cg_idx_(-1), ddl_dag_(nullptr), tablet_context_(nullptr), max_batch_size_(0), start_sequence_(), row_offset_(0),
      is_sorted_table_load_(false)
  {}
  ~ObWriteMacroParam() = default;
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid() && tenant_data_version_ > 0 && schema_version_ > 0
        && slice_idx_ >= 0 && snapshot_version_ > 0 && direct_load_type_ > DIRECT_LOAD_INVALID
        && direct_load_type_ < DIRECT_LOAD_MAX && task_id_ > 0
        && (!is_incremental_major_direct_load(direct_load_type_)
            || (is_incremental_major_direct_load(direct_load_type_) && tx_info_.is_valid()));
  }
  int64_t get_logic_parallel_count() const { return slice_count_ > 0 ? slice_count_ : ddl_thread_count_; }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(lob_meta_tablet_id), K_(tenant_data_version), K_(is_no_logging), KP_(macro_meta_store_mgr),
      K_(schema_version), K_(slice_idx), K_(slice_count), K_(ddl_thread_count), K_(snapshot_version), K_(direct_load_type),
      K_(task_id), K_(is_index_table), K_(ddl_table_schema), K_(tablet_param), K_(lob_meta_tablet_param), KP_(tablet_context),
      K_(is_sorted_table_load));
public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  int64_t tenant_data_version_;
  bool is_no_logging_;
  ObMacroMetaStoreManager *macro_meta_store_mgr_;
  int64_t schema_version_;
  int64_t slice_idx_;
  int64_t slice_count_;
  int64_t ddl_thread_count_;
  int64_t snapshot_version_;
  ObDirectLoadType direct_load_type_;
  int64_t task_id_;
  bool is_index_table_;
  ObDirectLoadTxInfo tx_info_;
  ObDDLTableSchema ddl_table_schema_;
  ObWriteTabletParam tablet_param_;
  ObWriteTabletParam lob_meta_tablet_param_;
  int64_t cg_idx_;
  ObDDLIndependentDag *ddl_dag_;
  ObDDLTabletContext *tablet_context_;
  int64_t max_batch_size_;
  blocksstable::ObMacroDataSeq start_sequence_;
  int64_t row_offset_;
  bool is_sorted_table_load_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif
