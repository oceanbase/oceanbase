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

#ifdef MULTI_VERSION_EXTRA_ROWKEY_DEF
MULTI_VERSION_EXTRA_ROWKEY_DEF(TRANS_VERSION_COL, common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID, \
    &common::ObObjMeta::set_int, &common::ObObjMeta::is_int)
MULTI_VERSION_EXTRA_ROWKEY_DEF(SQL_SEQUENCE_COL, common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID,
    &common::ObObjMeta::set_int, &common::ObObjMeta::is_int)
MULTI_VERSION_EXTRA_ROWKEY_DEF(MAX_EXTRA_ROWKEY, 0, NULL, NULL)
#endif

#ifndef OCEANBASE_STORAGE_OB_I_STORE_H_
#define OCEANBASE_STORAGE_OB_I_STORE_H_
#include "common/ob_store_format.h"
#include "common/ob_tablet_id.h"
#include "common/row/ob_row.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_table_param.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/ob_table_store_stat_mgr.h"
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
namespace memtable
{
class ObMvccRow;
}

namespace transaction
{
class ObPartTransCtx;
struct ObEncryptMetaCache;
}


namespace storage
{
class ObITable;
class ObTableStoreIterator;
class ObLS;
class ObLSHandle;
class ObTablet;
struct ObStoreCtx;

enum class ObStoreAccessType {
  INVALID = 0,
  READ = 1, // old than current stmt
  READ_LATEST = 2, // older than plus current stmt
  MODIFY = 3,
  ROW_LOCK = 4,
  TABLE_LOCK = 5,
};
class ObAccessTypeCheck
{
public:
  static bool is_read_access_type(const ObStoreAccessType access_type)
  {
    return (access_type == ObStoreAccessType::READ ||
            access_type == ObStoreAccessType::READ_LATEST);
  }

  static bool is_write_access_type(const ObStoreAccessType access_type)
  {
    return (access_type == ObStoreAccessType::MODIFY ||
            access_type == ObStoreAccessType::ROW_LOCK ||
            access_type == ObStoreAccessType::TABLE_LOCK);
  }
};

enum ObLockFlag
{
  LF_NONE = 0,
  LF_WRITE = 1,
};

enum ObSSTableStatus
{
  SSTABLE_NOT_INIT = 0,
  SSTABLE_INIT = 1,
  SSTABLE_WRITE_BUILDING = 2,
//  SSTABLE_PRE_READY_FOR_READ = 3, not used any more
  SSTABLE_READY_FOR_READ = 4,
  SSTABLE_READY_FOR_REMOTE_PHYTSICAL_READ = 5,
  SSTABLE_READY_FOR_REMOTE_LOGICAL_READ = 6,
};

struct ObMultiVersionExtraRowkeyIds
{
  enum ObMultiVersionExtraRowkeyIdsEnum
  {
#define MULTI_VERSION_EXTRA_ROWKEY_DEF(name, column_index, set_meta_type_func, is_meta_type_func) name,
#include "storage/ob_i_store.h"
#undef MULTI_VERSION_EXTRA_ROWKEY_DEF
  };
};

typedef void(common::ObObjMeta::*SET_META_TYPE_FUNC)();
typedef bool(common::ObObjMeta::*IS_META_TYPE_FUNC)() const;

struct ObMultiVersionExtraRowkey
{
  int64_t column_index_;
  SET_META_TYPE_FUNC set_meta_type_func_;
  IS_META_TYPE_FUNC is_meta_type_func_;
};

static const ObMultiVersionExtraRowkey OB_MULTI_VERSION_EXTRA_ROWKEY[] = {
#define MULTI_VERSION_EXTRA_ROWKEY_DEF(name, column_index, set_meta_type_func, is_meta_type_func) \
    {column_index, set_meta_type_func, is_meta_type_func},
#include "storage/ob_i_store.h"
#undef MULTI_VERSION_EXTRA_ROWKEY_DEF
};

typedef common::ObSEArray<share::schema::ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> ObColDescArray;
typedef common::ObIArray<share::schema::ObColDesc> ObColDescIArray;

template<typename T>
class ObReallocatedFixedArray final : public common::ObFixedArrayImpl<T, common::ObIAllocator>
{
public:
  int prepare_reallocate(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(capacity_ > 0 && capacity > capacity_)) {
      reset();
    }
    if (OB_FAIL(prepare_allocate(capacity))) {
      OB_LOG(WARN, "fail to prepare allocate", K(ret), K(capacity));
    }
    return ret;
  }
  OB_INLINE int cap() const
  { return capacity_; }
  using common::ObFixedArrayImpl<T, common::ObIAllocator>::reset;
protected:
  using common::ObFixedArrayImpl<T, common::ObIAllocator>::prepare_allocate;
  using common::ObFixedArrayImpl<T, common::ObIAllocator>::capacity_;
};

struct ObMultiVersionRowkeyHelpper
{
public:
  enum MultiVersionRowkeyType
  {
    MVRC_NONE = 0,
    MVRC_OLD_VERSION = 1, // TransVersion:Use Before Version3.0
    MVRC_VERSION_AFTER_3_0 = 2, // TransVersion | SqlSequence[A]:Use After Version3.0
    MVRC_VERSION_MAX = 3,
  };
  static int64_t get_trans_version_col_store_index(
      const int64_t schema_rowkey_col_cnt,
      const bool is_multi_version)
  {
    int64_t index = -1;
    if (is_multi_version) {
      index = schema_rowkey_col_cnt + ObMultiVersionExtraRowkeyIds::TRANS_VERSION_COL;
    }
    return index;
  }
  static int64_t get_sql_sequence_col_store_index(
      const int64_t schema_rowkey_col_cnt,
      const bool is_multi_version)
  {
    int64_t index = -1;
    if (is_multi_version) {
      index = schema_rowkey_col_cnt + ObMultiVersionExtraRowkeyIds::SQL_SEQUENCE_COL;
    }
    return index;
  }

  static int add_extra_rowkey_cols(ObColDescIArray &store_out_cols);
  static int get_extra_rowkey_col_cnt() {
    return ObMultiVersionExtraRowkeyIds::MAX_EXTRA_ROWKEY;
  }
};

class ObFastQueryContext
{
public:
  ObFastQueryContext()
    : timestamp_(-1), memtable_(nullptr), mvcc_row_(nullptr), row_version_(0)
  {}
  ObFastQueryContext(const int64_t timestamp, void *memtable, void *mvcc_row, const int64_t row_version)
    : timestamp_(timestamp), memtable_(memtable), mvcc_row_(mvcc_row), row_version_(row_version)
  {}
  ~ObFastQueryContext() = default;
  bool is_valid() const { return timestamp_ >= 0; }
  int64_t get_timestamp() const { return timestamp_; }
  void *get_memtable() const { return memtable_; }
  void *get_mvcc_row() const { return mvcc_row_; }
  int64_t get_row_version() const { return row_version_; }
  void set_timestamp(const int64_t timestamp) { timestamp_ = timestamp; }
  void set_memtable(void *memtable) { memtable_ = memtable; }
  void set_mvcc_row(void *mvcc_row) { mvcc_row_ = mvcc_row; }
  void set_row_version(const int64_t row_version) { row_version_ = row_version; }
  TO_STRING_KV(K_(timestamp), KP_(memtable), KP_(mvcc_row), K_(row_version));
private:
  int64_t timestamp_;
  void *memtable_;
  void *mvcc_row_;
  int64_t row_version_;
};

enum class ObExistFlag : uint8_t
{
  EXIST        = 0,
  UNKNOWN      = 1,
  NOT_EXIST    = 2,
};

static ObExistFlag extract_exist_flag_from_dml_flag(const blocksstable::ObDmlFlag dml_flag)
{
  ObExistFlag exist_flag = ObExistFlag::UNKNOWN;
  switch (dml_flag) {
  case blocksstable::ObDmlFlag::DF_NOT_EXIST:
    exist_flag = ObExistFlag::UNKNOWN;
    break;
  case blocksstable::ObDmlFlag::DF_LOCK:
    exist_flag = ObExistFlag::EXIST;
    break;
  case blocksstable::ObDmlFlag::DF_UPDATE:
  case blocksstable::ObDmlFlag::DF_INSERT:
    exist_flag = ObExistFlag::EXIST;
    break;
  case blocksstable::ObDmlFlag::DF_DELETE:
    exist_flag = ObExistFlag::NOT_EXIST;
    break;
  default:
    ob_abort();
    break;
  }
  return exist_flag;
}

struct ObStoreRowLockState
{
public:
  ObStoreRowLockState()
    : is_locked_(false),
    trans_version_(share::SCN::min_scn()),
    lock_trans_id_(),
    lock_data_sequence_(),
    lock_dml_flag_(blocksstable::ObDmlFlag::DF_NOT_EXIST),
    is_delayed_cleanout_(false),
    exist_flag_(ObExistFlag::UNKNOWN),
    mvcc_row_(NULL),
    trans_scn_(share::SCN::max_scn()) {}
  inline bool row_exist_decided() const
  {
    return ObExistFlag::EXIST == exist_flag_
      || ObExistFlag::NOT_EXIST == exist_flag_;
  }
  inline bool row_exist() const
  {
    return ObExistFlag::EXIST == exist_flag_;
  }
  inline bool is_lock_decided() const
  {
    return is_locked_ || !trans_version_.is_min();
  }
  inline bool is_locked(const transaction::ObTransID trans_id) const
  {
    return is_locked_ && lock_trans_id_ != trans_id;
  }
  void reset();
  TO_STRING_KV(K_(is_locked),
               K_(trans_version),
               K_(lock_trans_id),
               K_(lock_data_sequence),
               K_(lock_dml_flag),
               K_(is_delayed_cleanout),
               K_(exist_flag),
               KP_(mvcc_row),
               K_(trans_scn));

  bool is_locked_;
  share::SCN trans_version_;
  transaction::ObTransID lock_trans_id_;
  transaction::ObTxSEQ lock_data_sequence_;
  blocksstable::ObDmlFlag lock_dml_flag_;
  bool is_delayed_cleanout_;
  ObExistFlag exist_flag_;
  memtable::ObMvccRow *mvcc_row_;
  share::SCN trans_scn_; // sstable takes end_scn, memtable takes scn_ of ObMvccTransNode
};

struct ObSSTableRowState {
  enum ObSSTableRowStateEnum {
    UNKNOWN_STATE = 0,
    NOT_EXIST,
    IN_ROW_CACHE,
    IN_BLOCK
  };
};

struct ObStoreRow
{
  OB_UNIS_VERSION(1);
public:
  ObStoreRow()
      : flag_(), capacity_(0), scan_index_(0), row_type_flag_(),
      is_get_(false), from_base_(false),
      is_sparse_row_(false), column_ids_(NULL), row_val_(), snapshot_version_(0), group_idx_(0),
      trans_id_(), fast_filter_skipped_(false), last_purge_ts_(0)
  {}
  void reset();
  inline bool is_valid() const;
  int deep_copy(const ObStoreRow &src, char *buf, const int64_t len, int64_t &pos);
  int64_t get_deep_copy_size() const
  {
    return row_val_.get_deep_copy_size();
  }
  TO_YSON_KV(OB_ID(row), row_val_, OB_Y_(capacity));
  int64_t to_string(char *buffer, const int64_t length) const;
  /*
   *multi version row section
   */
  OB_INLINE bool is_ghost_row() const { return row_type_flag_.is_ghost_row(); }
  OB_INLINE bool is_uncommitted_row() const { return row_type_flag_.is_uncommitted_row(); }
  OB_INLINE bool is_compacted_multi_version_row() const { return row_type_flag_.is_compacted_multi_version_row(); }
  OB_INLINE bool is_first_multi_version_row() const { return row_type_flag_.is_first_multi_version_row(); }
  OB_INLINE bool is_last_multi_version_row() const { return row_type_flag_.is_last_multi_version_row(); }
  OB_INLINE bool is_shadow_row() const { return row_type_flag_.is_shadow_row(); }
  OB_INLINE void set_compacted_multi_version_row() { row_type_flag_.set_compacted_multi_version_row(true); }
  OB_INLINE void set_first_multi_version_row() { row_type_flag_.set_first_multi_version_row(true); }
  OB_INLINE void set_last_multi_version_row() { row_type_flag_.set_last_multi_version_row(true); }
  OB_INLINE void set_shadow_row() { row_type_flag_.set_shadow_row(true); }
  OB_INLINE void set_multi_version_flag(const blocksstable::ObMultiVersionRowFlag &multi_version_flag) { row_type_flag_ = multi_version_flag; }
  int32_t get_delta() const
  {
    int32_t delta = 0;
    if (flag_.is_extra_delete()) {
      delta = -1;
    } else if (flag_.is_insert()) {
      delta = 1;
    }
    return delta;
  }

  blocksstable::ObDmlRowFlag flag_;
  int64_t capacity_;
  int64_t scan_index_;
  mutable blocksstable::ObMultiVersionRowFlag row_type_flag_; // will change into multi_version_flag_ in NewRowHeader
  bool is_get_;
  // all cells of this row are from base sstable (major sstable)
  bool from_base_;
  bool is_sparse_row_;
  uint16_t *column_ids_;
  common::ObNewRow row_val_;
  int64_t snapshot_version_;
  int64_t group_idx_;
  transaction::ObTransID trans_id_;
  bool fast_filter_skipped_;
  int64_t last_purge_ts_;
};

struct ObLockRowChecker{
public:
  ObLockRowChecker() = delete;
  ~ObLockRowChecker() = delete;
  static int check_lock_row_valid(
      const blocksstable::ObDatumRow &row,
      const int64_t rowkey_cnt,
      bool is_memtable_iter_row_check);
  static int check_lock_row_valid(
      const blocksstable::ObDatumRow &row,
      const ObITableReadInfo &read_info);
};

template <typename AllocatorT>
int malloc_store_row(
    AllocatorT &allocator,
    const int64_t cell_count,
    ObStoreRow *&row,
    const common::ObRowStoreType row_type = common::FLAT_ROW_STORE)
{
  int ret = common::OB_SUCCESS;
  row = NULL;
  if (cell_count <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(cell_count));
  } else {
    void *ptr = NULL;
    int64_t size = sizeof(ObStoreRow) + sizeof(common::ObObj) * cell_count;
    if (NULL == (ptr = allocator.alloc(size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc ObStoreRow", K(size), K(cell_count));
    } else {
      void *cell_ptr = static_cast<char *>(ptr) + sizeof(ObStoreRow);
      row = new (ptr) ObStoreRow;
      row->row_val_.cells_ = new (cell_ptr) common::ObObj[cell_count]();
      row->flag_.set_flag(blocksstable::ObDmlFlag::DF_NOT_EXIST);
      row->row_val_.count_ = cell_count;
      row->capacity_ = cell_count;
    }
  }

  if (OB_FAIL(ret)) {
    row = NULL;
  }
  return ret;
}

template <typename AllocatorT>
int deep_copy_row(
    ObStoreRow &dest,
    const ObStoreRow &src,
    AllocatorT &allocator)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(dest.capacity_ < src.row_val_.count_)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row is not valid", K(ret), K(dest), K(src));
  } else {
    if (OB_SUCC(ret)) {
      dest.flag_ = src.flag_;
      dest.from_base_ = src.from_base_;
      dest.row_type_flag_ = src.row_type_flag_;
      dest.row_val_.count_ = src.row_val_.count_;
      dest.trans_id_ = src.trans_id_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.row_val_.count_; ++i) {
      if (OB_FAIL(deep_copy_obj(allocator, src.row_val_.cells_[i], dest.row_val_.cells_[i]))) {
        STORAGE_LOG(WARN, "failed to deep copy cell", K(ret), K(i));
      }
    }
  }
  return ret;
}

template <typename AllocatorT>
void free_store_row(AllocatorT &allocator, ObStoreRow *&row)
{
  if (nullptr != row) {
    allocator.free(row);
    row = nullptr;
  }
}

struct ObStoreCtx
{
  ObStoreCtx() { reset(); }
  ~ObStoreCtx() { reset(); }
  ObStoreCtx(const ObStoreCtx&)=delete;
  ObStoreCtx& operator=(const ObStoreCtx&)=delete;
  void reset();
  bool is_valid() const
  {
    return ls_id_.is_valid() && mvcc_acc_ctx_.is_valid();
  }
  bool is_read() const { return mvcc_acc_ctx_.is_read(); }
  bool is_write() const { return mvcc_acc_ctx_.is_write(); }
  bool is_replay() const { return mvcc_acc_ctx_.is_replay(); }
  bool is_read_store_ctx() const { return is_read_store_ctx_; }
  int init_for_read(const share::ObLSID &ls_id,
                    const common::ObTabletID tablet_id,
                    const int64_t timeout,
                    const int64_t lock_timeout_us,
                    const share::SCN &snapshot_version);
  int init_for_read(const storage::ObLSHandle &ls_handle,
                    const int64_t timeout,
                    const int64_t lock_timeout_us,
                    const share::SCN &snapshot_version);
  bool is_uncommitted_data_rollbacked() const;
  void force_print_trace_log();
  TO_STRING_KV(KP(this),
               K_(ls_id),
               KP_(ls),
               K_(branch),
               K_(timeout),
               K_(tablet_id),
               KP_(table_iter),
               K_(table_version),
               K_(mvcc_acc_ctx),
               K_(tablet_stat),
               K_(is_read_store_ctx));
  share::ObLSID ls_id_;
  storage::ObLS *ls_;                              // for performance opt
  int16_t branch_;                                 // parallel write id
  common::ObTabletID tablet_id_;
  mutable ObTableStoreIterator *table_iter_;
  int64_t table_version_;                          // used to update memtable's max_schema_version
  int64_t timeout_;
  memtable::ObMvccAccessCtx mvcc_acc_ctx_;         // all txn relative context
  storage::ObTabletStat tablet_stat_;              // used for collecting query statistics
  bool is_read_store_ctx_;
};


//const double CPU_MERGE_COST = 0.05;
//const double IO_ONCE_COST = 100;
//const double BASE_TEST_ROW_SIZE = 64;
//const double CPU_TUPLE_IN_CACHE_COST = 0.15;
//const double CPU_TUPLE_IN_DISK_COST = 0.3;
//const double CPU_TUPLE_GET_IN_CACHE = 30;
//const double CPU_TUPLE_GET_NOT_IN_CACHE = 35;
//const double CPU_TUPLE_READ_COST = 2;
//const double CPU_TUPLE_COST = 0.05;
//const double CPU_UPS_TUPLE_COST = 0.05;
//const double CPU_PROJECT_COST = 0.05;
//const double UPS_TUPLE_SEL = 1.0 / 5.0;
//const double MERGE_TUPLE_RATIO = 19.0 / 20.0;
//const double INSERT_TUPLE_RATIO = 1.0 / 10.0;
//const double CACHE_MISS_SEL = 6.0 / 10.0;

OB_INLINE bool ObStoreRow::is_valid() const
{
  bool bool_ret = true;
  if (scan_index_ < 0) {
    bool_ret = false;
  } else if (capacity_ < 0) {
    bool_ret = false;
  } else if (flag_.is_valid()) {
    if (flag_.is_delete()
        || flag_.is_not_exist()
        || row_val_.is_valid()) {
      if ((is_sparse_row_ && NULL == column_ids_)
          || (row_type_flag_.is_uncommitted_row() && !trans_id_.is_valid())
          || (!row_type_flag_.is_uncommitted_row() && trans_id_.is_valid())) {
        bool_ret = false;
      } else {
        bool_ret = true;
      }
    } else {
      bool_ret = false;
    }
  } else {
    bool_ret = false;
  }

  return bool_ret;
}

} // storage
} // oceanbase
#endif // OCEANBASE_STORAGE_I_OB_STORE_H_
