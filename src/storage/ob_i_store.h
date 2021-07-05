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
MULTI_VERSION_EXTRA_ROWKEY_DEF(TRANS_VERSION_COL, common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID,
    &common::ObObjMeta::set_int, &common::ObObjMeta::is_int)
MULTI_VERSION_EXTRA_ROWKEY_DEF(
    SQL_SEQUENCE_COL, common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID, &common::ObObjMeta::set_int, &common::ObObjMeta::is_int)
MULTI_VERSION_EXTRA_ROWKEY_DEF(MAX_EXTRA_ROWKEY, 0, NULL, NULL)
#endif

#ifndef OCEANBASE_STORAGE_OB_I_STORE_H_
#define OCEANBASE_STORAGE_OB_I_STORE_H_
#include "share/ob_i_data_access_service.h"
#include "share/schema/ob_table_param.h"
#include "common/row/ob_row.h"
#include "common/ob_store_format.h"
#include "common/ob_store_range.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace memtable {
class ObIMemtableCtx;
}

namespace blocksstable {
class ObRowValueHandle;
class ObMicroBlockBufferHandle;
class ObBlockCacheWorkingSet;
class ObMacroBlockMeta;
class ObLobDataReader;
}  // namespace blocksstable

namespace common {
class ObIOHandle;
}

namespace transaction {
class ObTransStateTableGuard;
}

namespace storage {
typedef common::ObSEArray<int32_t, common::OB_ROW_MAX_COLUMNS_COUNT> ColIdxArray;

class ObITable;
class ObIStoreRowFilter;
class ObSSTable;
class ObPartitionStore;
class ObTablesHandle;
class ObRelativeTable;
class ObRowkeyObjComparer;
class ObSSTableRowkeyHelper;

enum ObDataStoreType {
  ObNullStoreType = 0,
  ObIntStoreType = 1,
  ObNumberStoreType = 2,
  ObCharStoreType = 3,
  ObHexStoreType = 4,
  ObFloatStoreType = 5,
  ObDoubleStoreType = 6,
  ObTimestampStoreType = 7,
  ObBlobStoreType = 8,
  ObTextStoreType = 9,
  ObEnumStoreType = 10,
  ObSetStoreType = 11,
  ObBitStoreType = 12,
  ObTimestampTZStoreType = 13,
  ObRawStoreType = 14,
  ObIntervalYMStoreType = 15,
  ObIntervalDSStoreType = 16,
  ObRowIDStoreType = 17,
  ObExtendStoreType = 31
};

enum ObStoreFlag { ObNormalStoreFlag = 0, ObDeleteStoreFlag = 1, ObNewaddStoreFlag = 2 };

enum ObStoreAttr { STORE_WITHOUT_COLLATION = 0, STORE_WITH_COLLATION = 1 };

const uint8_t STORE_TEXT_STORE_VERSION = 1;

struct ObStoreMeta {
  ObStoreMeta() : type_(0), attr_(0)
  {}
  uint8_t type_ : 5;
  uint8_t attr_ : 3;
};

// type should only be memtable\major_sstable\minor_sstable\multi_version_minor_sstable
enum ObStoreType {
  INVALID_STORE_TYPE = -1,
  MEMSTORE = 0,
  MAJOR_SSSTORE = 1,
  //  MAJOR_FROZEN_MEMSTORE = 2,
  //  MINOR_FROZEN_MEMSTORE = 3,
  MINOR_SSSTORE = 4,
  CREATE_INDEX_MINOR_SSSTORE = 5,
  MULTI_VERSION_MINOR_SSSTORE = 6,
};

enum ObMergeType {
  INVALID_MERGE_TYPE = -1,
  MAJOR_MERGE = 0,
  MINOR_MERGE = 1,
  MINI_MERGE = 2,        // mini merge, only flush memtable
  MINI_MINOR_MERGE = 3,  // mini minor merge, compaction several mini sstable into one larger mini sstable
  RESERVED_MINOR_MERGE = 4,
  RESERVED_MAJOR_MERGE = 5,  // not exists after storage3.1
  HISTORY_MINI_MINOR_MERGE = 6,
  MERGE_TYPE_MAX,
};
const char* merge_type_to_str(const ObMergeType& merge_type);

enum ObMergeLevel {
  MACRO_BLOCK_MERGE_LEVEL = 0,
  MICRO_BLOCK_MERGE_LEVEL = 1,
};

inline bool is_ssstore(enum ObStoreType type)
{
  return MAJOR_SSSTORE == type || MINOR_SSSTORE == type || CREATE_INDEX_MINOR_SSSTORE == type ||
         MULTI_VERSION_MINOR_SSSTORE == type;
}

inline bool is_major_ssstore(enum ObStoreType type)
{
  return MAJOR_SSSTORE == type;
}

inline bool is_multi_version_ssstore(enum ObStoreType type)
{
  return MULTI_VERSION_MINOR_SSSTORE == type;
}

inline bool is_minor_ssstore(enum ObStoreType type)
{
  return MINOR_SSSTORE == type || CREATE_INDEX_MINOR_SSSTORE == type || MULTI_VERSION_MINOR_SSSTORE == type;
}

inline bool is_old_type_minor_ssstore(enum ObStoreType type)
{
  return MINOR_SSSTORE == type;
}

inline bool is_multi_version_minor_ssstore(enum ObStoreType type)
{
  return MULTI_VERSION_MINOR_SSSTORE == type;
}

inline bool is_create_index_minor_sstore(enum ObStoreType type)
{
  return CREATE_INDEX_MINOR_SSSTORE == type;
}

enum ObFollowerMergeLevel {
  SELF_NO_MERGE_LEVEL = 0,
  SELF_MINOR_MERGE_LEVEL = 1,
  SELF_MAJOR_MERGE_LEVEL = 2,
  SELF_ALL_MERGE_LEVEL = 3,
};

inline bool is_follower_d_minor_merge(const int64_t follower_merge_level)
{
  enum ObFollowerMergeLevel merge_level = static_cast<enum ObFollowerMergeLevel>(follower_merge_level);
  return merge_level == SELF_MINOR_MERGE_LEVEL || merge_level == SELF_ALL_MERGE_LEVEL;
}

inline bool is_follower_d_major_merge(const int64_t follower_merge_level)
{
  enum ObFollowerMergeLevel merge_level = static_cast<enum ObFollowerMergeLevel>(follower_merge_level);
  return merge_level == SELF_MAJOR_MERGE_LEVEL || merge_level == SELF_ALL_MERGE_LEVEL;
}

enum ObRowDml {
  T_DML_UNKNOWN = 0,
  T_DML_INSERT,
  T_DML_UPDATE,
  T_DML_DELETE,
  T_DML_REPLACE,
  T_DML_LOCK,
  T_DML_MAX,
};

enum ObLockFlag {
  LF_NONE = 0,
  LF_WRITE = 1,
};

enum ObSSTableStatus {
  SSTABLE_NOT_INIT = 0,
  SSTABLE_INIT = 1,
  SSTABLE_WRITE_BUILDING = 2,
  //  SSTABLE_PRE_READY_FOR_READ = 3, not used any more
  SSTABLE_READY_FOR_READ = 4,
};

struct ObMultiVersionExtraRowkeyIds {
  enum ObMultiVersionExtraRowkeyIdsEnum {
#define MULTI_VERSION_EXTRA_ROWKEY_DEF(name, column_index, set_meta_type_func, is_meta_type_func) name,
#include "storage/ob_i_store.h"
#undef MULTI_VERSION_EXTRA_ROWKEY_DEF
  };
};

typedef void (common::ObObjMeta::*SET_META_TYPE_FUNC)();
typedef bool (common::ObObjMeta::*IS_META_TYPE_FUNC)() const;

struct ObMultiVersionExtraRowkey {
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
struct ObTableAccessParam;
struct ObTableAccessContext;
struct ObTableIterParam;

struct ObMultiVersionRowkeyHelpper {
public:
  enum MultiVersionRowkeyType {
    MVRC_NONE = 0,
    MVRC_OLD_VERSION = 1,        // TransVersion:Use Before Version3.0
    MVRC_VERSION_AFTER_3_0 = 2,  // TransVersion | SqlSequence[A]:Use After Version3.0
    MVRC_VERSION_MAX = 3,
  };
  static int64_t get_multi_version_rowkey_cnt(const int8_t rowkey_type)
  {
    int64_t ret = INT_MAX;
    if (MVRC_NONE == rowkey_type) {
      ret = 0;
    } else if (MVRC_OLD_VERSION == rowkey_type) {
      ret = 1;
    } else if (MVRC_VERSION_AFTER_3_0 == rowkey_type) {
      ret = 2;
    }
    return ret;
  }
  // check whether the extra rowkeys in a multi version macro meta is valid
  static bool is_valid_multi_version_macro_meta(const blocksstable::ObMacroBlockMeta& meta);
  static int64_t get_trans_version_col_store_index(
      const int64_t schema_rowkey_col_cnt, const int64_t multi_version_rowkey_cnt)
  {
    int ret = -1;
    if (multi_version_rowkey_cnt > get_multi_version_rowkey_cnt(MVRC_NONE)) {
      ret = schema_rowkey_col_cnt + ObMultiVersionExtraRowkeyIds::TRANS_VERSION_COL;
    }
    return ret;
  }
  static int64_t get_sql_sequence_col_store_index(
      const int64_t schema_rowkey_col_cnt, const int64_t multi_version_rowkey_cnt)
  {
    int ret = -1;
    if (multi_version_rowkey_cnt > get_multi_version_rowkey_cnt(MVRC_OLD_VERSION)) {
      ret = schema_rowkey_col_cnt + ObMultiVersionExtraRowkeyIds::SQL_SEQUENCE_COL;
    }
    return ret;
  }

  // add extra rowkey columns in param.out_cols_ and param.projector_
  static int convert_multi_version_iter_param(const ObTableIterParam& param, const bool is_get,
      ObColDescArray& store_out_cols, const int64_t multi_version_rowkey_cnt,
      common::ObIArray<int32_t>* store_projector = NULL);
  static int add_extra_rowkey_cols(ObColDescIArray& store_out_cols, int32_t& index,
      const bool need_build_store_projector, const int64_t multi_version_rowkey_cnt,
      common::ObIArray<int32_t>* store_projector = NULL);
  static int get_extra_rowkey_col_cnt()
  {
    return ObMultiVersionExtraRowkeyIds::MAX_EXTRA_ROWKEY;
  }
};

struct ObMultiVersionRowFlag {
#define OB_COMPACTED_MULTI_VERSION_ROW 1
#define OB_LAST_MULTI_VERSION_ROW 1
#define OB_FIRST_MULTI_VERSION_ROW 1
#define OB_UNCOMMITTED_ROW 1
#define OB_MAGIC_ROW 1
#define OB_MULTI_VERSION_ROW_RESERVED 3

  static const uint8_t OBSF_MASK_MULTI_VERSION_COMPACT = (0x1UL << OB_COMPACTED_MULTI_VERSION_ROW) - 1;
  static const uint8_t OBSF_MASK_MULTI_VERSION_LAST = (0x1UL << OB_LAST_MULTI_VERSION_ROW) - 1;
  static const uint8_t OBSF_MASK_MULTI_VERSION_FIRST = (0x1UL << OB_FIRST_MULTI_VERSION_ROW) - 1;
  static const uint8_t OBSF_MASK_MULTI_VERSION_UNCOMMITTED = (0x1UL << OB_UNCOMMITTED_ROW) - 1;
  static const uint8_t OBSF_MASK_MULTI_VERSION_MAGIC = (0x1UL << OB_MAGIC_ROW) - 1;

  union {
    uint8_t flag_;
    struct {
      uint8_t compacted_row_
          : OB_COMPACTED_MULTI_VERSION_ROW;             // 0: multi_version_row(default), 1: compacted_multi_version_row
      uint8_t last_row_ : OB_LAST_MULTI_VERSION_ROW;    // 0: not last row(default), 1: last row
      uint8_t first_row_ : OB_FIRST_MULTI_VERSION_ROW;  // 0: not first row(default), 1: first row
      uint8_t uncommitted_row_ : OB_UNCOMMITTED_ROW;    // 0: committed(default), 1: uncommitted row
      uint8_t magic_row_ : OB_MAGIC_ROW;                // 0: not magic row(default), 1: magic row
      uint8_t reserved_ : OB_MULTI_VERSION_ROW_RESERVED;
    };
  };

  ObMultiVersionRowFlag() : flag_(0)
  {}
  ObMultiVersionRowFlag(uint8_t flag) : flag_(flag)
  {}
  void reset()
  {
    flag_ = 0;
  }
  inline void set_compacted_multi_version_row(const bool is_compacted_multi_version_row)
  {
    compacted_row_ = is_compacted_multi_version_row & OBSF_MASK_MULTI_VERSION_COMPACT;
  }
  inline void set_last_multi_version_row(const bool is_last_multi_version_row)
  {
    last_row_ = is_last_multi_version_row & OBSF_MASK_MULTI_VERSION_LAST;
  }
  inline void set_first_multi_version_row(const bool is_first_multi_version_row)
  {
    first_row_ = is_first_multi_version_row & OBSF_MASK_MULTI_VERSION_FIRST;
  }
  inline void set_uncommitted_row(const bool is_uncommitted_row)
  {
    uncommitted_row_ = is_uncommitted_row & OBSF_MASK_MULTI_VERSION_UNCOMMITTED;
  }
  inline void set_magic_row(const bool is_magic_row)
  {
    magic_row_ = is_magic_row & OBSF_MASK_MULTI_VERSION_MAGIC;
  }
  inline bool is_compacted_multi_version_row() const
  {
    return compacted_row_;
  }
  inline bool is_last_multi_version_row() const
  {
    return last_row_;
  }
  inline bool is_first_multi_version_row() const
  {
    return first_row_;
  }
  inline bool is_uncommitted_row() const
  {
    return uncommitted_row_;
  }
  inline bool is_magic_row() const
  {
    return magic_row_;
  }

  TO_STRING_KV("row_compact", compacted_row_, "row_last", last_row_, "row_first", first_row_, "row_uncommitted",
      uncommitted_row_, "row_magic", magic_row_, "reserved", reserved_, K_(flag));
};

struct ObStoreRowDml {
  static const int64_t FIRST_DML_BITS = 4;
  static const int64_t CUR_DML_BITS = 4;
  static const int64_t STORE_ROW_DML_RESERVED = 24;

  static const uint8_t FIRST_DML_MASK = (1U << FIRST_DML_BITS) - 1;
  static const uint8_t CUR_DML_MASK = (1U << CUR_DML_BITS) - 1;

  union {
    uint8_t dml_val_;
    struct {
      uint8_t first_dml_ : FIRST_DML_BITS;
      uint8_t dml_ : CUR_DML_BITS;
    };
  };

  ObStoreRowDml() : dml_val_(0)
  {}
  ~ObStoreRowDml()
  {
    reset();
  }
  void reset()
  {
    dml_val_ = 0;
  }
  inline void set_first_dml(const ObRowDml dml)
  {
    first_dml_ = dml & FIRST_DML_MASK;
  }
  inline void set_dml(const ObRowDml dml)
  {
    dml_ = dml & CUR_DML_MASK;
  }
  inline ObRowDml get_first_dml() const
  {
    return static_cast<ObRowDml>(first_dml_);
  }
  inline ObRowDml get_dml() const
  {
    return static_cast<ObRowDml>(dml_);
  }

  TO_STRING_KV(K_(first_dml), K_(dml));
};

class ObFastQueryContext {
public:
  ObFastQueryContext() : timestamp_(-1), memtable_(nullptr), mvcc_row_(nullptr), row_version_(0)
  {}
  ObFastQueryContext(const int64_t timestamp, void* memtable, void* mvcc_row, const int64_t row_version)
      : timestamp_(timestamp), memtable_(memtable), mvcc_row_(mvcc_row), row_version_(row_version)
  {}
  ~ObFastQueryContext() = default;
  bool is_valid() const
  {
    return timestamp_ >= 0;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  void* get_memtable() const
  {
    return memtable_;
  }
  void* get_mvcc_row() const
  {
    return mvcc_row_;
  }
  int64_t get_row_version() const
  {
    return row_version_;
  }
  void set_timestamp(const int64_t timestamp)
  {
    timestamp_ = timestamp;
  }
  void set_memtable(void* memtable)
  {
    memtable_ = memtable;
  }
  void set_mvcc_row(void* mvcc_row)
  {
    mvcc_row_ = mvcc_row;
  }
  void set_row_version(const int64_t row_version)
  {
    row_version_ = row_version;
  }
  TO_STRING_KV(K_(timestamp), KP_(memtable), KP_(mvcc_row), K_(row_version));

private:
  int64_t timestamp_;
  void* memtable_;
  void* mvcc_row_;
  int64_t row_version_;
};

struct ObRowPositionFlag {
  ObRowPositionFlag() : flag_(0)
  {}
  inline void reset()
  {
    flag_ = 0;
  }
  inline bool is_micro_first() const
  {
    return micro_first_ | macro_first_;
  }
  inline bool is_macro_first() const
  {
    return macro_first_;
  }
  inline void set_micro_first(const bool is_first)
  {
    micro_first_ = is_first & OBSF_MASK_MICRO_FIRST_ROW;
  }
  inline void set_macro_first(const bool is_first)
  {
    macro_first_ = is_first & OBSF_MASK_MACRO_FIRST_ROW;
  }
  TO_STRING_KV(K_(flag), K_(micro_first), K_(macro_first));

  static const uint8_t OB_MICRO_FIRST_ROW = 1;
  static const uint8_t OB_MACRO_FIRST_ROW = 1;
  static const uint8_t OB_ROW_FLAG_RESERVED = 6;

  static const uint8_t OBSF_MASK_MICRO_FIRST_ROW = (0x1UL << OB_MICRO_FIRST_ROW) - 1;
  static const uint8_t OBSF_MASK_MACRO_FIRST_ROW = (0x1UL << OB_MACRO_FIRST_ROW) - 1;

  union {
    uint8_t flag_;
    struct {
      uint8_t micro_first_ : OB_MICRO_FIRST_ROW;
      uint8_t macro_first_ : OB_MACRO_FIRST_ROW;
      uint8_t flag_reserved_ : OB_ROW_FLAG_RESERVED;
    };
  };
};

struct ObStoreRowLockState {
public:
  ObStoreRowLockState() : is_locked_(false), trans_version_(0), lock_trans_id_()
  {}
  void reset();
  TO_STRING_KV(K_(is_locked), K_(trans_version), K_(lock_trans_id));
  bool is_locked_;
  int64_t trans_version_;
  transaction::ObTransID lock_trans_id_;
};

struct ObStoreRow {
  OB_UNIS_VERSION(1);

public:
  ObStoreRow()
      : flag_(-1),
        capacity_(0),
        scan_index_(0),
        dml_(T_DML_UNKNOWN),
        row_type_flag_(),
        is_get_(false),
        from_base_(false),
        row_pos_flag_(),
        first_dml_(T_DML_UNKNOWN),
        is_sparse_row_(false),
        column_ids_(NULL),
        row_val_(),
        snapshot_version_(0),
        range_array_idx_(0),
        trans_id_ptr_(NULL),
        fast_filter_skipped_(false),
        last_purge_ts_(0)
  {}
  void reset();
  inline bool is_valid() const;
  int deep_copy(const ObStoreRow& src, char* buf, const int64_t len, int64_t& pos);
  int64_t get_deep_copy_size() const
  {
    return row_val_.get_deep_copy_size() + get_column_id_size();
  }
  TO_YSON_KV(OB_ID(row), row_val_, Y_(flag), Y_(dml), Y_(capacity), Y_(first_dml));
  int64_t to_string(char* buffer, const int64_t length) const;
  inline ObRowDml get_first_dml() const
  {
    return static_cast<ObRowDml>(first_dml_);
  }
  inline static ObRowDml get_first_dml(const int8_t dml_val)
  {
    return static_cast<ObRowDml>((dml_val >> DML_BITS) & DML_MASK);
  }
  inline ObRowDml get_dml() const
  {
    return dml_;
  }
  inline static ObRowDml get_dml(const int8_t dml_val)
  {
    return static_cast<ObRowDml>(dml_val & DML_MASK);
  }
  inline void set_first_dml(const ObRowDml dml)
  {
    first_dml_ = static_cast<int8_t>(dml);
  }
  inline void set_dml(const ObRowDml dml)
  {
    dml_ = dml;
  }
  inline transaction::ObTransID* get_trans_id_ptr()
  {
    return trans_id_ptr_;
  }
  int set_trans_id(transaction::ObTransID trans_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(trans_id_ptr_)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "trans id ptr is null", K(trans_id_ptr_), K(trans_id));
    } else {
      *trans_id_ptr_ = trans_id;
    }
    return ret;
  }
  inline int8_t get_dml_val() const
  {
    return static_cast<int8_t>(first_dml_ << DML_BITS) | static_cast<int8_t>(dml_);
  }
  inline void set_row_dml(ObStoreRowDml row_dml)
  {
    set_first_dml(row_dml.get_first_dml());
    set_dml(row_dml.get_dml());
  }
  inline void set_dml_val(int8_t row_dml)
  {
    set_first_dml(get_first_dml(row_dml));
    set_dml(get_dml(row_dml));
  }
  inline bool is_delete() const
  {
    return common::ObActionFlag::OP_DEL_ROW == flag_;
  }
  inline void reset_dml()
  {
    set_dml(T_DML_UNKNOWN);
    set_first_dml(T_DML_UNKNOWN);
  }
  int32_t get_delta() const
  {
    int32_t delta = 0;
    if (T_DML_UNKNOWN != first_dml_ && T_DML_UNKNOWN != dml_) {
      if (T_DML_INSERT == first_dml_ && T_DML_DELETE != dml_) {
        delta = 1;
      } else if (T_DML_INSERT != first_dml_ && T_DML_DELETE == dml_) {
        delta = -1;
      }
    }
    return delta;
  }
  inline int64_t get_column_id_size() const
  {
    int64_t size = 0;
    if (is_sparse_row_) {
      size = sizeof(uint16_t) * row_val_.count_;
    }
    return size;
  }
  inline void reset_sparse_row()
  {
    is_sparse_row_ = false;
    column_ids_ = NULL;
    trans_id_ptr_ = NULL;
  }

  static const int64_t DML_BITS = 4;
  static const int8_t DML_MASK = (1 << DML_BITS) - 1;

  int64_t flag_;  // may be OP_ROW_DOES_NOT_EXIST / OP_ROW_EXIST / OP_DEL_ROW
  int64_t capacity_;
  int64_t scan_index_;
  ObRowDml dml_;
  mutable ObMultiVersionRowFlag row_type_flag_;
  bool is_get_;
  // all cells of this row are from base sstable (major sstable)
  bool from_base_;
  // normal row/ micro first row/ macro first row
  ObRowPositionFlag row_pos_flag_;
  int8_t first_dml_;
  bool is_sparse_row_;
  uint16_t* column_ids_;
  common::ObNewRow row_val_;
  int64_t snapshot_version_;
  ObFastQueryContext fq_ctx_;
  int64_t range_array_idx_;
  transaction::ObTransID* trans_id_ptr_;
  bool fast_filter_skipped_;
  int64_t last_purge_ts_;
};

struct ObMagicRowManager {
public:
  ObMagicRowManager() = delete;
  ~ObMagicRowManager() = delete;
  static int make_magic_row(
      const int64_t sql_sequence_col_idx, const common::ObQueryFlag& query_flag, storage::ObStoreRow& row);
  static int is_magic_row(const storage::ObMultiVersionRowFlag& flag, bool& is_magic_row);
  static const int64_t MAGIC_NUM = INT64_MAX;
};

struct ObWorkRow : public ObStoreRow {
  ObWorkRow()
  {
    reset();
  }
  void reset();
  common::ObObj objs[common::OB_ROW_MAX_COLUMNS_COUNT];
};

#define STORE_ITER_ROW_IN_GAP 1
#define STORE_ITER_ROW_BIG_GAP_HINT 2
#define STORE_ITER_ROW_PARTIAL 8

template <typename AllocatorT>
int malloc_store_row(AllocatorT& allocator, const int64_t cell_count, ObStoreRow*& row,
    const common::ObRowStoreType row_type = common::FLAT_ROW_STORE)
{
  int ret = common::OB_SUCCESS;
  row = NULL;
  if (cell_count <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(cell_count));
  } else {
    int malloc_column_count = common::SPARSE_ROW_STORE == row_type ? common::OB_ROW_MAX_COLUMNS_COUNT : cell_count;
    void* ptr = NULL;
    int64_t size = sizeof(ObStoreRow) + sizeof(common::ObObj) * malloc_column_count;
    if (NULL == (ptr = allocator.alloc(size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc ObStoreRow", K(size), K(malloc_column_count));
    } else {
      void* cell_ptr = static_cast<char*>(ptr) + sizeof(ObStoreRow);
      row = new (ptr) ObStoreRow;
      row->row_val_.cells_ = new (cell_ptr) common::ObObj[malloc_column_count]();
      row->flag_ = common::ObActionFlag::OP_ROW_DOES_NOT_EXIST;
      row->reset_dml();
      row->row_val_.count_ = malloc_column_count;
      row->capacity_ = malloc_column_count;
      if (common::SPARSE_ROW_STORE == row_type) {  // alloc column ids for sparse row
        row->is_sparse_row_ = true;
        if (NULL == (ptr = allocator.alloc(sizeof(uint16_t) * malloc_column_count))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "fail to alloc column ids", K(size), K(malloc_column_count));
        } else {
          row->column_ids_ = (uint16_t*)ptr;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    row = NULL;
  }
  return ret;
}

template <typename AllocatorT>
int deep_copy_row(ObStoreRow& dest, const ObStoreRow& src, AllocatorT& allocator)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(dest.capacity_ < src.row_val_.count_) ||
      (src.is_sparse_row_ && (OB_ISNULL(src.column_ids_) || OB_ISNULL(dest.column_ids_))) ||
      (OB_NOT_NULL(src.trans_id_ptr_) && OB_ISNULL(dest.trans_id_ptr_))) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row is not valid", K(ret), K(dest), K(src));
  } else {
    if (OB_SUCC(ret)) {
      dest.flag_ = src.flag_;
      dest.from_base_ = src.from_base_;
      dest.row_type_flag_ = src.row_type_flag_;
      dest.set_dml_val(src.get_dml_val());
      dest.is_sparse_row_ = src.is_sparse_row_;
      dest.row_val_.count_ = src.row_val_.count_;
      if (OB_NOT_NULL(src.trans_id_ptr_)) {
        *dest.trans_id_ptr_ = *src.trans_id_ptr_;
      } else {
        dest.trans_id_ptr_ = NULL;
      }

      if (src.is_sparse_row_) {
        MEMCPY(dest.column_ids_, src.column_ids_, sizeof(uint16_t) * src.row_val_.count_);
      }
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
void free_store_row(AllocatorT& allocator, ObStoreRow*& row)
{
  if (nullptr != row) {
    allocator.free(row);
    row = nullptr;
  }
}

class ObIStoreRowIterator {
public:
  ObIStoreRowIterator()
  {}
  virtual ~ObIStoreRowIterator()
  {}
  virtual int get_next_row(const ObStoreRow*& row) = 0;
};

class ObStoreRowIterator : public ObIStoreRowIterator {
public:
  ObStoreRowIterator() : type_(0)
  {}
  virtual ~ObStoreRowIterator()
  {}
  virtual int init(
      const ObTableIterParam& param, ObTableAccessContext& context, ObITable* table, const void* query_range)
  {
    UNUSED(param);
    UNUSED(context);
    UNUSED(table);
    UNUSED(query_range);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_next_row_ext(const ObStoreRow*& row, uint8_t& flag)
  {
    int ret = get_next_row(row);
    flag = get_iter_flag();
    return ret;
  }
  virtual uint8_t get_iter_flag()
  {
    return 0;
  }
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
  {
    UNUSED(range_idx);
    UNUSED(gap_key);
    UNUSED(gap_size);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
  {
    UNUSED(range_idx);
    UNUSED(gap_key);
    UNUSED(include_gap_key);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int report_stat()
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_next_row(const ObStoreRow*& row) = 0;
  virtual int get_type() const
  {
    return type_;
  }
  virtual void reuse()
  {}
  virtual bool is_base_sstable_iter() const
  {
    return false;
  }
  virtual int check_fast_skip(const common::ObStoreRowkey& rowkey, const bool iter_del_row)
  {
    UNUSED(rowkey);
    UNUSED(iter_del_row);
    return common::OB_NOT_SUPPORTED;
  }
  virtual bool fast_skip_effective() const
  {
    return false;
  }
  virtual bool fast_skip_checked() const
  {
    return true;
  }
  VIRTUAL_TO_STRING_KV(K_(type));

protected:
  int type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStoreRowIterator);
};

enum ObQRIterType {
  T_INVALID_ITER_TYPE,
  T_MULTI_GET,
  T_MULTI_SCAN,
  T_SINGLE_ROW,
  T_INDEX,
};

class ObQueryRowIterator : public ObIStoreRowIterator {
public:
  ObQueryRowIterator() : type_(T_INVALID_ITER_TYPE)
  {}
  explicit ObQueryRowIterator(const ObQRIterType type) : type_(type)
  {}
  virtual ~ObQueryRowIterator()
  {}

  // get the next ObStoreRow and move the cursor
  // @param row [out], row can be modified outside
  // @return OB_ITER_END if end of iteration
  virtual int get_next_row(ObStoreRow*& row) = 0;
  virtual int get_next_row(const ObStoreRow*& row) override
  {
    return get_next_row(const_cast<ObStoreRow*&>(row));
  }
  virtual void reset() = 0;
  virtual int switch_iterator(const int64_t range_array_idx)
  {
    UNUSED(range_array_idx);
    return common::OB_NOT_SUPPORTED;
  }
  VIRTUAL_TO_STRING_KV(K_(type));

public:
  ObQRIterType get_type() const
  {
    return type_;
  }

protected:
  ObQRIterType type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRowIterator);
};

class ObQueryRowIteratorAdapter : public ObQueryRowIterator {
public:
  ObQueryRowIteratorAdapter(const ObQRIterType type, ObIStoreRowIterator& iter);
  virtual ~ObQueryRowIteratorAdapter();

  int init(common::ObIAllocator& allocator, const int64_t cell_cnt);

  virtual int get_next_row(ObStoreRow*& row) override;
  virtual int get_next_row(const ObStoreRow*& row) override
  {
    return iter_.get_next_row(row);
  }

  virtual void reset() override;

private:
  ObIStoreRowIterator& iter_;
  ObStoreRow row_;
  common::ObIAllocator* allocator_;
  common::ObObj* cells_;
  int64_t cell_cnt_;

  DISALLOW_COPY_AND_ASSIGN(ObQueryRowIteratorAdapter);
};

class ObVPCompactIter : public storage::ObIStoreRowIterator {
public:
  ObVPCompactIter();
  virtual ~ObVPCompactIter();
  int init(const int64_t rowkey_cnt, ObIStoreRowIterator* scan_iter);
  virtual int get_next_row(const storage::ObStoreRow*& row) override;

private:
  ObIStoreRowIterator* scan_iter_;
  int64_t rowkey_cnt_;
  bool is_inited_;
};

struct ObStoreCtx {
  ObStoreCtx()
  {
    reset();
  }
  void reset()
  {
    mem_ctx_ = NULL;
    warm_up_ctx_ = NULL;
    tables_ = NULL;
    tenant_id_ = 0;
    is_sp_trans_ = false;
    isolation_ = transaction::ObTransIsolation::UNKNOWN;
    sql_no_ = -1;
    stmt_min_sql_no_ = -1;
    snapshot_info_.reset();
    trans_table_guard_ = NULL;
    log_ts_ = INT64_MAX;
  }
  int get_snapshot_info(transaction::ObTransSnapInfo& snap_info) const;
  bool is_valid() const
  {
    return (NULL != mem_ctx_);
  }
  int init_trans_ctx_mgr(const common::ObPartitionKey& pg_key);
  bool is_tenant_id_valid() const
  {
    return common::is_valid_tenant_id(tenant_id_);
  }
  TO_STRING_KV(KP_(mem_ctx), KP_(warm_up_ctx), KP_(tables), K_(tenant_id), K_(trans_id), K_(is_sp_trans), K_(isolation),
      K_(sql_no), K_(stmt_min_sql_no), K_(snapshot_info), KP_(trans_table_guard));

  memtable::ObIMemtableCtx* mem_ctx_;
  ObWarmUpCtx* warm_up_ctx_;
  mutable const common::ObIArray<ObITable*>* tables_;

  uint64_t tenant_id_;
  common::ObPartitionKey cur_pkey_;  // for pg transaction, must be pg key; for partition transaction, must be partition
                                     // keyh
  transaction::ObTransID trans_id_;
  bool is_sp_trans_;
  int32_t isolation_;
  int32_t sql_no_;
  int32_t stmt_min_sql_no_;
  int64_t log_ts_;
  transaction::ObTransSnapInfo snapshot_info_;
  transaction::ObTransStateTableGuard* trans_table_guard_;
};

struct ObTableAccessStat {
  ObTableAccessStat();
  ~ObTableAccessStat()
  {}
  void reset();
  inline bool enable_get_row_cache() const
  {
    return row_cache_miss_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM ||
           row_cache_hit_cnt_ > row_cache_miss_cnt_ / 2;
  }
  inline bool enable_put_row_cache() const
  {
    return row_cache_put_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM;
  }
  inline bool enable_put_fuse_row_cache(const int64_t threshold) const
  {
    return fuse_row_cache_put_cnt_ < threshold;
  }
  inline bool enable_get_fuse_row_cache(const int64_t threshold) const
  {
    return fuse_row_cache_miss_cnt_ < threshold || fuse_row_cache_hit_cnt_ > fuse_row_cache_miss_cnt_ / 4;
  }
  inline bool enable_bf_cache() const
  {
    return (bf_access_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM || bf_filter_cnt_ > (bf_access_cnt_ / 8));
  }
  inline bool enable_sstable_bf_cache() const
  {
    return (sstable_bf_access_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM / 5 ||
            sstable_bf_filter_cnt_ > sstable_bf_access_cnt_ / 4);
  }
  TO_STRING_KV(K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(row_cache_put_cnt), K_(bf_filter_cnt),
      K_(bf_access_cnt), K_(empty_read_cnt), K_(block_cache_hit_cnt), K_(block_cache_miss_cnt),
      K_(sstable_bf_filter_cnt), K_(sstable_bf_empty_read_cnt), K_(sstable_bf_access_cnt), K_(fuse_row_cache_hit_cnt),
      K_(fuse_row_cache_miss_cnt), K_(fuse_row_cache_put_cnt), K_(rowkey_prefix));

  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t row_cache_put_cnt_;
  int64_t bf_filter_cnt_;
  int64_t bf_access_cnt_;
  int64_t empty_read_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  int64_t sstable_bf_filter_cnt_;
  int64_t sstable_bf_empty_read_cnt_;
  int64_t sstable_bf_access_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t fuse_row_cache_put_cnt_;
  int64_t rowkey_prefix_;
};

struct ObGetTableParam {
public:
  ObGetTableParam() : partition_store_(NULL), frozen_version_(-1), sample_info_(), tables_handle_(NULL)
  {}
  ~ObGetTableParam() = default;
  bool is_valid() const
  {
    return NULL != partition_store_ || NULL != tables_handle_;
  }
  void reset();
  TO_STRING_KV(KP_(partition_store), K_(frozen_version), K_(sample_info), KP_(tables_handle));
  ObPartitionStore* partition_store_;
  common::ObVersion frozen_version_;
  common::SampleInfo sample_info_;
  const ObTablesHandle* tables_handle_;
};

// Parameters for row iteration like ObITable get, scan, multi_get, multi_scan
// and ObStoreRowIterator::init
struct ObTableIterParam {
public:
  // only use in mini merge
  enum ObIterTransNodeMode {
    OIM_ITER_OVERFLOW_TO_COMPLEMENT = 1,
    OIM_ITER_NON_OVERFLOW_TO_MINI = 2,
    OIM_ITER_FULL = 3,
  };

public:
  ObTableIterParam();
  virtual ~ObTableIterParam();
  void reset();
  bool is_valid() const;
  int has_lob_column_out(const bool is_get, bool& has_lob_column) const;
  bool need_build_column_map(int64_t schema_version) const;
  int get_out_cols(const bool is_get, const ObColDescIArray*& out_cols) const;
  int get_column_map(const bool is_get, const share::schema::ColumnMap*& column_map) const;
  int get_projector(const bool is_get, const common::ObIArray<int32_t>*& projector) const;
  int get_out_cols_param(
      const bool is_get, const common::ObIArray<share::schema::ObColumnParam*>*& out_cols_param) const;
  bool enable_fuse_row_cache() const;
  TO_STRING_KV(K_(table_id), K_(schema_version), K_(rowkey_cnt), KP_(out_cols), KP_(cols_id_map), KP_(projector),
      KP_(full_projector), KP_(out_cols_project), KP_(out_cols_param), KP_(full_out_cols_param),
      K_(is_multi_version_minor_merge), KP_(full_out_cols), KP_(full_cols_id_map), K_(need_scn), K_(iter_mode));

public:
  uint64_t table_id_;
  int64_t schema_version_;
  int64_t rowkey_cnt_;
  const ObColDescIArray* out_cols_;
  const share::schema::ColumnMap* cols_id_map_;
  const common::ObIArray<int32_t>* projector_;
  const common::ObIArray<int32_t>* full_projector_;
  const common::ObIArray<int32_t>* out_cols_project_;
  const common::ObIArray<share::schema::ObColumnParam*>* out_cols_param_;
  const common::ObIArray<share::schema::ObColumnParam*>* full_out_cols_param_;
  // only used in ObMemTable
  bool is_multi_version_minor_merge_;
  const ObColDescIArray* full_out_cols_;
  const share::schema::ColumnMap* full_cols_id_map_;
  bool need_scn_;
  ObIterTransNodeMode iter_mode_;
};

class ObColDescArrayParam final {
public:
  ObColDescArrayParam() : local_cols_(), ref_cols_(nullptr), col_cnt_(0), is_local_(false), is_inited_(false)
  {}
  ~ObColDescArrayParam() = default;

  OB_INLINE bool is_valid() const
  {
    return is_inited_ && (is_local_ || nullptr != ref_cols_);
  }
  int init(const ObColDescIArray* ref = nullptr);
  void reset();
  int assign(const ObColDescIArray& other);
  int push_back(const share::schema::ObColDesc& desc);
  const ObColDescIArray& get_col_descs() const;
  OB_INLINE ObColDescIArray& get_local_col_descs()
  {
    return local_cols_;
  };
  OB_INLINE int64_t count() const
  {
    return static_cast<int64_t>(col_cnt_);
  }

  TO_STRING_KV(K(local_cols_), KPC_(ref_cols), K_(col_cnt), K(is_local_), K(is_inited_));

private:
  ObColDescArray local_cols_;
  const ObColDescIArray* ref_cols_;
  uint16_t col_cnt_;  // max column == 512
  bool is_local_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObColDescArrayParam);
};

enum ObFastAggregationType {
  INVALID_AGG_TYPE = 0,
  AGG_COUNT_ALL,
  AGG_COUNT_COL,
  AGG_MAX,
  AGG_MIN,
};

struct ObFastAggProjectCell {
  ObFastAggProjectCell() : agg_project_(0)
  {}
  ~ObFastAggProjectCell()
  {
    reset();
  }
  void reset()
  {
    agg_project_ = 0;
  }
  inline void set_project_id(const uint32_t id)
  {
    project_id_ = id & OBSF_MASK_AGG_PROJECT;
  }
  inline void set_type(const ObFastAggregationType type)
  {
    type_ = type & OBSF_MASK_AGG_TYPE;
  }
  inline int32_t get_project_id() const
  {
    return project_id_;
  }
  inline ObFastAggregationType get_type() const
  {
    return static_cast<ObFastAggregationType>(type_);
  }
  TO_STRING_KV(K_(agg_project), K_(project_id), K_(type));

  static const uint32_t OB_AGG_PROJECT_ID = 16;
  static const uint32_t OB_AGG_TYPE = 8;
  static const uint32_t OB_AGG_RESERVED = 8;

  static const uint32_t OBSF_MASK_AGG_PROJECT = (0x1UL << OB_AGG_PROJECT_ID) - 1;
  static const uint32_t OBSF_MASK_AGG_TYPE = (0x1UL << OB_AGG_TYPE) - 1;
  union {
    int32_t agg_project_;
    struct {
      int32_t project_id_ : OB_AGG_PROJECT_ID;
      uint32_t type_ : OB_AGG_TYPE;
      uint32_t reserved_ : OB_AGG_RESERVED;
    };
  };
};

struct ObTableAccessParam {
public:
  ObTableAccessParam();
  virtual ~ObTableAccessParam();
  void reset();
  OB_INLINE bool is_valid() const
  {
    return out_col_desc_param_.is_valid() && out_col_desc_param_.count() > 0 && iter_param_.is_valid();
  }
  // used for query
  int init(ObTableScanParam& scan_param, const bool is_mv = false);
  // used for merge
  int init(const uint64_t table_id, const int64_t schema_version, const int64_t rowkey_column_num,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, const bool is_multi_version_merge = false,
      share::schema::ObTableParam* table_param = NULL, const bool is_mv_right_table = false);
  // used for get unique index conflict row
  int init_basic_param(const uint64_t table_id, const int64_t schema_version, const int64_t rowkey_column_num,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, const common::ObIArray<int32_t>* out_cols_index);
  // used for index back when query
  int init_index_back(ObTableScanParam& scan_param);
  // init need_fill_scale_ and search column which need fill scale
  int init_column_scale_info();
  OB_INLINE int64_t get_max_out_col_cnt() const
  {
    return MAX(nullptr == full_out_cols_ ? 0 : full_out_cols_->count(), out_col_desc_param_.count());
  }

  bool is_index_back_index_scan() const
  {
    return NULL != index_back_project_;
  }

public:
  TO_STRING_KV(K_(iter_param), K_(reserve_cell_cnt), K_(out_col_desc_param), KP_(full_out_cols), KP_(out_cols_param),
      KP_(index_back_project), KP_(join_key_project), KP_(right_key_project), KP_(padding_cols), KP_(filters),
      KP_(virtual_column_exprs), KP_(index_projector), K_(projector_size), KP_(output_exprs), KP_(op), KP_(op_filters),
      KP_(row2exprs_projector), KP_(join_key_project), KP_(right_key_project), KP_(fast_agg_project),
      K_(enable_fast_skip), K_(need_fill_scale), K_(col_scale_info));

public:
  // 1. Basic Param for Table Iteration
  ObTableIterParam iter_param_;
  int64_t reserve_cell_cnt_;
  ObColDescArrayParam out_col_desc_param_;
  const ObColDescIArray* full_out_cols_;

  // 2. Adjustment Param for Output Rows
  const common::ObIArray<share::schema::ObColumnParam*>* out_cols_param_;
  const common::ObIArray<int32_t>* index_back_project_;
  const common::ObIArray<int32_t>* padding_cols_;
  const common::ObIArray<common::ObISqlExpression*>* filters_;
  const common::ObColumnExprArray* virtual_column_exprs_;
  int32_t* index_projector_;
  int64_t projector_size_;

  // output for sql static typing engine, NULL for old sql engine scan.
  const sql::ObExprPtrIArray* output_exprs_;
  sql::ObOperator* op_;
  const sql::ObExprPtrIArray* op_filters_;
  ObRow2ExprsProjector* row2exprs_projector_;

  // 3. Multiple Version Param
  // see ObTableParam::join_key_projector_ && ObTableParam::right_key_projector_
  const common::ObIArray<int32_t>* join_key_project_;
  const common::ObIArray<int32_t>* right_key_project_;

  // for fast agg project
  const common::ObIArray<ObFastAggProjectCell>* fast_agg_project_;
  bool enable_fast_skip_;

  bool need_fill_scale_;
  common::ObSEArray<std::pair<int64_t, common::ObScale>, 8, common::ModulePageAllocator, true> col_scale_info_;
};

class ObLobLocatorHelper {
public:
  ObLobLocatorHelper();
  virtual ~ObLobLocatorHelper();
  void reset();
  int init(const share::schema::ObTableParam& table_param, const int64_t snapshot_version);
  int fill_lob_locator(common::ObNewRow& row, bool is_projected_row, const ObTableAccessParam& access_param);
  OB_INLINE bool is_valid() const
  {
    return is_inited_;
  }
  TO_STRING_KV(
      K_(table_id), K_(snapshot_version), K_(rowid_version), KPC(rowid_project_), K_(rowid_objs), K_(is_inited));

private:
  static const int64_t DEFAULT_LOCATOR_OBJ_ARRAY_SIZE = 8;
  int init_rowid_version(const share::schema::ObTableSchema& table_schema);
  int build_rowid_obj(common::ObNewRow& row, common::ObString& rowid_str, bool is_projected_row,
      const common::ObIArray<int32_t>& out_project);
  int build_lob_locator(common::ObObj& lob_obj, const uint64_t column_id, const common::ObString& rowid_str);

private:
  uint64_t table_id_;
  int64_t snapshot_version_;
  int64_t rowid_version_;
  const common::ObIArray<int32_t>* rowid_project_;  // map to projected row
  common::ObSEArray<common::ObObj, DEFAULT_LOCATOR_OBJ_ARRAY_SIZE> rowid_objs_;
  common::ObArenaAllocator locator_allocator_;
  bool is_inited_;
};

struct ObTableAccessContext {
  ObTableAccessContext();
  virtual ~ObTableAccessContext();
  void reset();
  void reuse();
  inline bool is_valid() const
  {
    return is_inited_ && NULL != store_ctx_ && NULL != stmt_allocator_ && NULL != allocator_;
  }
  inline bool need_prewarm() const
  {
    return NULL != store_ctx_ && NULL != store_ctx_->warm_up_ctx_ && !query_flag_.is_prewarm();
  }
  inline bool is_end() const
  {
    return is_end_;
  }
  inline bool enable_get_row_cache() const
  {
    return query_flag_.is_use_row_cache() && access_stat_.enable_get_row_cache() && !need_scn_;
  }
  inline bool enable_put_row_cache() const
  {
    return query_flag_.is_use_row_cache() && access_stat_.enable_put_row_cache() && !need_scn_;
  }
  inline bool enable_bf_cache() const
  {
    return query_flag_.is_use_bloomfilter_cache() && access_stat_.enable_bf_cache() && !need_scn_;
  }
  inline bool enable_sstable_bf_cache() const
  {
    return query_flag_.is_use_bloomfilter_cache() && access_stat_.enable_sstable_bf_cache() && !need_scn_;
  }
  inline bool is_multi_version_read(const int64_t snapshot_version)
  {
    return trans_version_range_.snapshot_version_ < snapshot_version;
  }
  inline bool enable_get_fuse_row_cache(const int64_t threshold) const
  {
    return query_flag_.is_use_fuse_row_cache() && access_stat_.enable_get_fuse_row_cache(threshold) && !need_scn_;
  }
  inline bool enable_put_fuse_row_cache(const int64_t threshold) const
  {
    return query_flag_.is_use_fuse_row_cache() && access_stat_.enable_put_fuse_row_cache(threshold) && !need_scn_;
  }
  // used for query
  int init(ObTableScanParam& scan_param, const ObStoreCtx& ctx, blocksstable::ObBlockCacheWorkingSet& block_cache_ws,
      const common::ObVersionRange& trans_version_range, const ObIStoreRowFilter* row_filter,
      const bool is_index_back = false);
  // used for merge
  int init(const common::ObQueryFlag& query_flag, const ObStoreCtx& ctx, common::ObArenaAllocator& allocator,
      common::ObArenaAllocator& stmt_allocator, blocksstable::ObBlockCacheWorkingSet& block_cache_ws,
      const common::ObVersionRange& trans_version_range);
  // used for exist or simple scan
  int init(const common::ObQueryFlag& query_flag, const ObStoreCtx& ctx, common::ObArenaAllocator& allocator,
      const common::ObVersionRange& trans_version_range);
  TO_STRING_KV(K_(is_inited), K_(timeout), K_(pkey), K_(query_flag), K_(sql_mode), KP_(store_ctx), KP_(expr_ctx),
      KP_(limit_param), KP_(stmt_allocator), KP_(allocator), KP_(stmt_mem), KP_(scan_mem), KP_(table_scan_stat),
      KP_(block_cache_ws), K_(out_cnt), K_(is_end), K_(trans_version_range), KP_(row_filter), K_(merge_log_ts),
      K_(read_out_type), K_(lob_locator_helper));

private:
  int build_lob_locator_helper(ObTableScanParam& scan_param, const common::ObVersionRange& trans_version_range);

public:
  bool is_inited_;
  int64_t timeout_;
  common::ObPartitionKey pkey_;
  common::ObQueryFlag query_flag_;
  ObSQLMode sql_mode_;
  const ObStoreCtx* store_ctx_;
  common::ObExprCtx* expr_ctx_;
  common::ObLimitParam* limit_param_;
  // sql statement level allocator, available before sql execute finish
  common::ObArenaAllocator* stmt_allocator_;
  // storage scan/rescan interface level allocator, will be reclaimed in every scan/rescan call
  common::ObArenaAllocator* allocator_;
  lib::MemoryContext* stmt_mem_;  // sql statement level memory entity, only for query
  lib::MemoryContext* scan_mem_;  // scan/rescan level memory entity, only for query
  common::ObTableScanStatistic* table_scan_stat_;
  blocksstable::ObBlockCacheWorkingSet* block_cache_ws_;
  ObTableAccessStat access_stat_;
  int64_t out_cnt_;
  bool is_end_;
  common::ObVersionRange trans_version_range_;
  const ObIStoreRowFilter* row_filter_;
  bool use_fuse_row_cache_;  // temporary code
  const ObFastQueryContext* fq_ctx_;
  bool need_scn_;
  int16_t fuse_row_cache_hit_rate_;
  int16_t block_cache_hit_rate_;
  bool is_array_binding_;
  const common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true>* range_array_pos_;
  int64_t range_array_cursor_;
  int64_t merge_log_ts_;
  common::ObRowStoreType read_out_type_;
  ObLobLocatorHelper* lob_locator_helper_;
};

struct ObRowsInfo final {
public:
  explicit ObRowsInfo();
  ~ObRowsInfo();
  OB_INLINE bool is_valid() const
  {
    return is_inited_ && exist_helper_.is_valid() && delete_count_ >= 0 && ext_rowkeys_.count() >= delete_count_ &&
           row_count_ >= ext_rowkeys_.count() && OB_NOT_NULL(rows_) && OB_NOT_NULL(scan_mem_);
  }
  int init(const ObRelativeTable& table, const ObStoreCtx& store_ctx,
      const common::ObIArray<share::schema::ObColDesc>& col_descs);
  int init_exist_helper(const ObStoreCtx& store_ctx, const common::ObIArray<share::schema::ObColDesc>& col_descs);
  int check_duplicate(ObStoreRow* rows, const int64_t row_count, const ObRelativeTable& table);
  const common::ObStoreRowkey& get_duplicate_rowkey() const
  {
    return min_key_;
  }
  common::ObStoreRowkey& get_duplicate_rowkey()
  {
    return min_key_;
  }
  const common::ObStoreRowkey& get_min_rowkey() const
  {
    return min_key_;
  }
  int refine_ext_rowkeys(ObSSTable* sstable);
  int clear_found_rowkey(const int64_t rowkey_idx);
  OB_INLINE bool all_rows_found()
  {
    return delete_count_ == ext_rowkeys_.count();
  }
  OB_INLINE int16_t get_max_prefix_length() const
  {
    return max_prefix_length_;
  }
  OB_INLINE const common::ObExtStoreRowkey& get_prefix_rowkey() const
  {
    return prefix_rowkey_;
  }
  TO_STRING_KV(K_(ext_rowkeys), K_(min_key), K_(table_id), K_(row_count), K_(delete_count),
      K_(collation_free_transformed), K_(exist_helper));

public:
  struct ExistHelper final {
    ExistHelper();
    ~ExistHelper();
    int init(const ObStoreCtx& store_ctx, const common::ObIArray<share::schema::ObColDesc>& col_descs,
        uint64_t table_id, int64_t rowkey_column_cnt, common::ObArenaAllocator& allocator);
    OB_INLINE bool is_valid() const
    {
      return is_inited_;
    }
    OB_INLINE const ObStoreCtx& get_store_ctx() const
    {
      return *table_access_context_.store_ctx_;
    }
    OB_INLINE const common::ObIArray<share::schema::ObColDesc>& get_col_desc() const
    {
      return *table_iter_param_.out_cols_;
    }
    TO_STRING_KV(K_(table_iter_param), K_(table_access_context));
    ObTableIterParam table_iter_param_;
    ObTableAccessContext table_access_context_;
    bool is_inited_;
  };

private:
  struct RowsCompare {
    RowsCompare(common::ObIArray<ObRowkeyObjComparer*>* cmp_funcs, common::ObStoreRowkey& dup_key, const bool check_dup,
        int16_t& max_prefix_length, int& ret);
    ~RowsCompare() = default;
    int32_t compare(const common::ObStoreRowkey& left, const common::ObStoreRowkey& right);
    OB_INLINE bool operator()(const common::ObExtStoreRowkey& left, const common::ObExtStoreRowkey& right)
    {
      int32_t cmp_ret = 0;
      if (OB_ISNULL(cmp_funcs_)) {
        cmp_ret = left.get_store_rowkey().compare(right.get_store_rowkey());
      } else {
        cmp_ret = compare(left.get_store_rowkey(), right.get_store_rowkey());
      }
      if (OB_UNLIKELY(0 == cmp_ret && check_dup_)) {
        ret_ = common::OB_ERR_PRIMARY_KEY_DUPLICATE;
        dup_key_ = left.get_store_rowkey();
        STORAGE_LOG(WARN, "Rowkey already exists", K_(dup_key), K_(ret));
      }
      return cmp_ret < 0;
    }
    common::ObIArray<ObRowkeyObjComparer*>* cmp_funcs_;
    common::ObStoreRowkey& dup_key_;
    const bool check_dup_;
    int16_t& max_prefix_length_;
    int& ret_;
  };

private:
  int ensure_space(const int64_t row_count);

public:
  ObStoreRow* rows_;
  int64_t row_count_;
  common::ObArray<common::ObExtStoreRowkey> ext_rowkeys_;
  ExistHelper exist_helper_;
  common::ObExtStoreRowkey prefix_rowkey_;
  uint64_t table_id_;

private:
  common::ObStoreRowkey min_key_;
  lib::MemoryContext* scan_mem_;  // scan/rescan level memory entity, only for query
  int64_t delete_count_;
  bool collation_free_transformed_;
  int16_t rowkey_column_num_;
  int16_t max_prefix_length_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObRowsInfo);
};

const double CPU_MERGE_COST = 0.05;
const double IO_ONCE_COST = 100;
const double BASE_TEST_ROW_SIZE = 64;
const double CPU_TUPLE_IN_CACHE_COST = 0.15;
const double CPU_TUPLE_IN_DISK_COST = 0.3;
const double CPU_TUPLE_GET_IN_CACHE = 30;
const double CPU_TUPLE_GET_NOT_IN_CACHE = 35;
const double CPU_TUPLE_READ_COST = 2;
const double CPU_TUPLE_COST = 0.05;
const double CPU_UPS_TUPLE_COST = 0.05;
const double CPU_PROJECT_COST = 0.05;
const double UPS_TUPLE_SEL = 1.0 / 5.0;
const double MERGE_TUPLE_RATIO = 19.0 / 20.0;
const double INSERT_TUPLE_RATIO = 1.0 / 10.0;
const double CACHE_MISS_SEL = 6.0 / 10.0;

struct ObPartitionEst {
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  TO_STRING_KV(K_(logical_row_count), K_(physical_row_count));

  ObPartitionEst();
  int add(const ObPartitionEst& pe);
  int deep_copy(const ObPartitionEst& src);
  void reset()
  {
    logical_row_count_ = physical_row_count_ = 0;
  }
};

struct ObRowStat {
  int64_t base_row_count_;
  int64_t inc_row_count_;
  int64_t merge_row_count_;
  int64_t result_row_count_;
  int64_t filt_del_count_;

  ObRowStat()
  {
    reset();
  }
  void reset()
  {
    MEMSET(this, 0, sizeof(ObRowStat));
  }
  TO_STRING_KV(K_(base_row_count), K_(inc_row_count), K_(merge_row_count), K_(result_row_count), K_(filt_del_count));
};

OB_INLINE bool ObStoreRow::is_valid() const
{
  bool bool_ret = true;
  if (scan_index_ < 0) {
    bool_ret = false;
  } else if (capacity_ < 0) {
    bool_ret = false;
  } else if (dml_ < 0 || dml_ >= storage::T_DML_MAX) {
    bool_ret = false;
  } else if (first_dml_ < 0 || first_dml_ >= storage::T_DML_MAX) {
    bool_ret = false;
  } else {
    if ((common::ObActionFlag::OP_DEL_ROW == flag_) || common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == flag_ ||
        (common::ObActionFlag::OP_ROW_EXIST == flag_ && row_val_.is_valid())) {
      if ((is_sparse_row_ && NULL == column_ids_) ||
          (row_type_flag_.is_uncommitted_row() && (OB_ISNULL(trans_id_ptr_) || !trans_id_ptr_->is_valid())) ||
          (!row_type_flag_.is_uncommitted_row() && OB_NOT_NULL(trans_id_ptr_))) {
        bool_ret = false;
      } else {
        bool_ret = true;
      }
    } else {
      bool_ret = false;
    }
  }

  return bool_ret;
}

OB_INLINE bool is_major_merge(const ObMergeType& merge_type)
{
  return MAJOR_MERGE == merge_type;
}
OB_INLINE bool is_mini_merge(const ObMergeType& merge_type)
{
  return MINI_MERGE == merge_type;
}
OB_INLINE bool is_mini_minor_merge(const ObMergeType& merge_type)
{
  return MINI_MINOR_MERGE == merge_type || HISTORY_MINI_MINOR_MERGE == merge_type;
}
OB_INLINE bool is_multi_version_minor_merge(const ObMergeType& merge_type)
{
  return MINOR_MERGE == merge_type || MINI_MERGE == merge_type || MINI_MINOR_MERGE == merge_type ||
         HISTORY_MINI_MINOR_MERGE == merge_type;
}
OB_INLINE bool is_history_mini_minor_merge(const ObMergeType& merge_type)
{
  return HISTORY_MINI_MINOR_MERGE == merge_type;
}
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_I_OB_STORE_H_
