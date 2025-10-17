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

#ifndef OB_STORAGE_BLOCKSSTABLE_DATUM_ROW_H
#define OB_STORAGE_BLOCKSSTABLE_DATUM_ROW_H

#include "common/ob_common_types.h"
#include "common/ob_tablet_id.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/meta_mem/ob_fixed_meta_obj_array.h"
#include "storage/tx/ob_trans_define.h"
#include "common/row/ob_row.h"
#include "storage/ob_storage_util.h"
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase
{
namespace share{
namespace schema
{
struct ObColDesc;
}
}
namespace storage
{
struct ObStoreRow;
}
namespace blocksstable
{

struct ObDmlRowFlag;
struct ObDatumRowkey;

enum ObDmlFlag
{
  DF_NOT_EXIST = 0,
  DF_LOCK = 1,
  DF_UPDATE = 2,
  DF_INSERT = 3,
  DF_DELETE = 4,
  DF_MAX = 5,
};

static const char *ObDmlFlagStr[DF_MAX] = {
    "NOT_EXIST",
    "LOCK",
    "UPDATE",
    "INSERT",
    "DELETE"
};

enum ObDmlRowFlagType
{
  DF_TYPE_NORMAL = 0,
  DF_TYPE_INSERT_DELETE = 1,
  DF_TYPE_MAX,
};

static const char *ObDmlTypeStr[DF_TYPE_MAX] = {
    "N",
    "I_D"
};

const char *get_dml_str(ObDmlFlag dml_flag);
void format_dml_str(const int32_t flag, char *str, int len);

struct ObDmlRowFlag
{
  OB_UNIS_VERSION(1);
public:
  ObDmlRowFlag()
   : whole_flag_(0)
  {
  }
  ObDmlRowFlag(const uint8_t flag)
   : whole_flag_(flag)
  {
  }
  ObDmlRowFlag(ObDmlFlag flag)
   : whole_flag_(0)
  {
    set_flag(flag);
  }
  ~ObDmlRowFlag()
  {
  }
  OB_INLINE void reset()
  {
    whole_flag_ = 0;
  }
  OB_INLINE void set_flag(ObDmlFlag row_flag, ObDmlRowFlagType flag_type = DF_TYPE_NORMAL)
  {
    reset();
    if (OB_LIKELY(row_flag >= DF_NOT_EXIST && row_flag < DF_MAX)) {
      flag_ = row_flag;
    }
    if (OB_LIKELY(flag_type >= DF_TYPE_NORMAL && flag_type < DF_TYPE_MAX)) {
      flag_type_ = flag_type;
    }
  }
  OB_INLINE bool is_delete() const
  {
    return DF_DELETE == flag_;
  }
  OB_INLINE bool is_lock() const
  {
    return DF_LOCK == flag_;
  }
  OB_INLINE bool is_not_exist() const
  {
    return DF_NOT_EXIST == flag_;
  }
  OB_INLINE bool is_insert() const
  {
    return DF_INSERT == flag_;
  }
  OB_INLINE bool is_update() const
  {
    return DF_UPDATE == flag_;
  }
  OB_INLINE bool is_exist() const
  {
    return is_valid() && !is_not_exist();
  }
  OB_INLINE bool is_exist_without_delete() const
  {
    return is_exist() && !is_delete();
  }
  OB_INLINE bool is_valid() const
  {
    return (DF_TYPE_NORMAL == flag_type_ && DF_DELETE >= flag_)
        || (DF_TYPE_INSERT_DELETE == flag_type_ && (DF_INSERT == flag_ || DF_DELETE == flag_));
  }
  OB_INLINE bool is_extra_delete() const
  {
    return DF_TYPE_INSERT_DELETE != flag_type_ && DF_DELETE == flag_;
  }
  OB_INLINE bool is_insert_delete() const
  {
    return DF_TYPE_INSERT_DELETE == flag_type_ && DF_DELETE == flag_;
  }
  OB_INLINE bool is_upsert() const
  {
    return DF_TYPE_INSERT_DELETE == flag_type_ && DF_INSERT == flag_;
  }
  OB_INLINE void fuse_flag(const ObDmlRowFlag input_flag)
  {
    if (OB_LIKELY(input_flag.is_valid())) {
      if (DF_INSERT == input_flag.flag_) {
        if (DF_DELETE == flag_) {
          flag_type_ = DF_TYPE_INSERT_DELETE;
        } else {
          flag_ = DF_INSERT;
        }
      } else if (DF_DELETE == input_flag.flag_ && DF_DELETE == flag_) {
        if (flag_type_ == DF_TYPE_INSERT_DELETE) {
          flag_type_ = input_flag.flag_type_;
        } else {
          STORAGE_LOG(DEBUG, "unexpected pure delete row", KPC(this), K(input_flag));
        }
      }
    }
  }
  OB_INLINE uint8_t get_serialize_flag() const // use when Serialize or print
  {
    return whole_flag_;
  }
  OB_INLINE ObDmlFlag get_dml_flag() const { return (ObDmlFlag)flag_; }
  ObDmlRowFlag & operator = (const ObDmlRowFlag &other)
  {
    if (other.is_valid()) {
      whole_flag_ = other.whole_flag_;
    }
    return *this;
  }

  const char *getFlagStr() const
  {
    const char *ret_str = nullptr;
    if (is_valid()) {
      ret_str = ObDmlFlagStr[flag_];
    } else {
      ret_str = "invalid flag";
    }
    return ret_str;
  }
  OB_INLINE void format_str(char *str, int8_t len) const
  { return format_dml_str(whole_flag_, str, len); }
  OB_INLINE int32_t get_delta() const
  {
    int32_t ret_val  = 0;
    if (is_extra_delete()) {
      ret_val = -1;
    } else if (is_insert()) {
      ret_val = 1;
    }
    return ret_val;
  }

  TO_STRING_KV("flag", get_dml_str(ObDmlFlag(flag_)), K_(flag_type)) ;
private:
  bool operator !=(const ObDmlRowFlag &other) const // for unittest
  {
    return flag_ != other.flag_;
  }

  const static uint8_t OB_FLAG_TYPE_MASK = 0x80;
  const static uint8_t OB_FLAG_MASK = 0x7F;
  union
  {
    uint8_t whole_flag_;
    struct {
      uint8_t flag_      : 7;  // store ObDmlFlag
      uint8_t flag_type_ : 1;  // mark is pure_delete or insert_delete
    };
  };
};

static const int8_t MvccFlagCount = 8;
static const char *ObMvccFlagStr[MvccFlagCount] = {
  "",
  "F",
  "U",
  "S",
  "C",
  "G",
  "L",
  "UNKNOWN"
};

void format_mvcc_str(const int32_t flag, char *str, int len);

struct ObMultiVersionRowFlag
{
  OB_UNIS_VERSION(1);
public:
  union
  {
    uint8_t flag_;
    struct
    {
      uint8_t is_first_        : 1;    // 0: not first row(default), 1: first row
      uint8_t is_uncommitted_  : 1;    // 0: committed(default), 1: uncommitted row
      uint8_t is_shadow_       : 1;    // 0: not new compacted shadow row(default), 1: shadow row
      uint8_t is_compacted_    : 1;    // 0: multi_version_row(default), 1: compacted_multi_version_row
      uint8_t is_ghost_        : 1;    // 0: not ghost row(default), 1: ghost row
      uint8_t is_last_         : 1;    // 0: not last row(default), 1: last row
      uint8_t reserved_        : 2;
    };
  };

  ObMultiVersionRowFlag() : flag_(0) {}
  ObMultiVersionRowFlag(uint8_t flag) : flag_(flag) {}
  void reset() { flag_ = 0; }
  inline void set_compacted_multi_version_row(const bool is_compacted_multi_version_row)
  {
    is_compacted_ = is_compacted_multi_version_row;
  }
  inline void set_last_multi_version_row(const bool is_last_multi_version_row)
  {
    is_last_ = is_last_multi_version_row;
  }
  inline void set_first_multi_version_row(const bool is_first_multi_version_row)
  {
    is_first_ = is_first_multi_version_row;
  }
  inline void set_uncommitted_row(const bool is_uncommitted_row)
  {
    is_uncommitted_ = is_uncommitted_row;
  }
  inline void set_ghost_row(const bool is_ghost_row)
  {
    is_ghost_ = is_ghost_row;
  }
  inline void set_shadow_row(const bool is_shadow_row)
  {
    is_shadow_ = is_shadow_row;
  }
  inline bool is_valid() const
  {
    return !is_first_multi_version_row() || is_uncommitted_row() || is_last_multi_version_row() || is_ghost_row() || is_shadow_row();
  }
  inline bool is_compacted_multi_version_row() const { return is_compacted_; }
  inline bool is_last_multi_version_row() const { return is_last_; }
  inline bool is_first_multi_version_row() const { return is_first_; }
  inline bool is_uncommitted_row() const { return is_uncommitted_; }
  inline bool is_ghost_row() const { return is_ghost_; }
  inline bool is_shadow_row() const { return is_shadow_; }
  inline void format_str(char *str, int8_t len) const
  { return format_mvcc_str(flag_, str, len); }

  TO_STRING_KV("first", is_first_,
               "uncommitted", is_uncommitted_,
               "shadow", is_shadow_,
               "compact", is_compacted_,
               "ghost", is_ghost_,
               "last", is_last_,
               "reserved", reserved_,
               K_(flag));
};

struct ObDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObDatumRow(const uint64_t tenant_id = MTL_ID());
  ~ObDatumRow();
  int init(common::ObIAllocator &allocator, const int64_t capacity, char *trans_info_ptr = nullptr);
  int init(const int64_t capacity);
  void reset();
  void reuse();
  int reserve(const int64_t capacity, const bool keep_data = false);
  int deep_copy(const ObDatumRow &src, common::ObIAllocator &allocator);
  int from_store_row(const storage::ObStoreRow &store_row);
  int shallow_copy(const ObDatumRow &other);
  //only for unittest
  bool operator==(const ObDatumRow &other) const;
  bool operator==(const common::ObNewRow &other) const;

  int is_datums_changed(const ObDatumRow &other, bool &is_changed) const;
  int copy_attributes_except_datums(const ObDatumRow &other);
  OB_INLINE int64_t get_capacity() const { return datum_buffer_.get_capacity(); }
  OB_INLINE int64_t get_column_count() const { return count_; }
  OB_INLINE int64_t get_scan_idx() const { return scan_index_; }
  OB_INLINE bool is_valid() const { return nullptr != storage_datums_ && get_capacity() > 0; }
  OB_INLINE bool check_has_nop_col() const
  {
    for (int64_t i = 0; i < get_column_count(); i++) {
      if (storage_datums_[i].is_nop()) {
        return true;
      }
    }
    return false;
  }
  /*
   *multi version row section
   */
  OB_INLINE transaction::ObTransID get_trans_id() const { return trans_id_; }
  OB_INLINE void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
  OB_INLINE bool is_have_uncommited_row() const { return have_uncommited_row_; }
  OB_INLINE void set_have_uncommited_row(const bool have_uncommited_row = true) { have_uncommited_row_ = have_uncommited_row; }
  OB_INLINE bool is_ghost_row() const { return mvcc_row_flag_.is_ghost_row(); }
  OB_INLINE bool is_uncommitted_row() const { return mvcc_row_flag_.is_uncommitted_row(); }
  OB_INLINE bool is_compacted_multi_version_row() const { return mvcc_row_flag_.is_compacted_multi_version_row(); }
  OB_INLINE bool is_first_multi_version_row() const { return mvcc_row_flag_.is_first_multi_version_row(); }
  OB_INLINE bool is_last_multi_version_row() const { return mvcc_row_flag_.is_last_multi_version_row(); }
  OB_INLINE bool is_shadow_row() const { return mvcc_row_flag_.is_shadow_row(); }
  OB_INLINE void set_compacted_multi_version_row() { mvcc_row_flag_.set_compacted_multi_version_row(true); }
  OB_INLINE void set_first_multi_version_row() { mvcc_row_flag_.set_first_multi_version_row(true); }
  OB_INLINE void set_last_multi_version_row() { mvcc_row_flag_.set_last_multi_version_row(true); }
  OB_INLINE void set_shadow_row() { mvcc_row_flag_.set_shadow_row(true); }
  OB_INLINE void set_uncommitted_row() { mvcc_row_flag_.set_uncommitted_row(true); }
  OB_INLINE void set_multi_version_flag(const ObMultiVersionRowFlag &multi_version_flag) { mvcc_row_flag_ = multi_version_flag; }
  /*
   *row estimate section
   */
  OB_INLINE int32_t get_delta() const { return row_flag_.get_delta(); }
  /*
   *delete_insert section
   */
  OB_INLINE bool is_di_delete() const { return !is_delete_filtered_ && (delete_version_ > 0 || row_flag_.is_delete()); }
  OB_INLINE bool is_filtered() const
  {
    return is_insert_filtered_ || (row_flag_.is_delete() && is_delete_filtered_);
  }
  int fuse_delete_insert(const ObDatumRow &former);

  DECLARE_TO_STRING;

public:
  common::ObArenaAllocator local_allocator_;
  uint16_t count_;
  union {
    struct {
      uint32_t have_uncommited_row_: 1;
      uint32_t fast_filter_skipped_: 1;
      // the followings added for delete_insert scan, row must be projected for delete_insert,
      // is_filtered_ means whether or not filtered by the pushdown filter
      uint32_t is_insert_filtered_:  1;
      uint32_t is_delete_filtered_:  1;
      uint32_t reserved_ : 28;
    };
    uint32_t read_flag_;
  };
  ObDmlRowFlag row_flag_;
  ObMultiVersionRowFlag mvcc_row_flag_;
  transaction::ObTransID trans_id_;
  int64_t scan_index_;
  int64_t group_idx_;
  int64_t snapshot_version_;
  // insert_version is meaningfull when the newest insert row is not filtered
  int64_t insert_version_;
  // delete_version is meaningfull when the oldest delete row is not filtered
  int64_t delete_version_;

  ObStorageDatum *storage_datums_;
  // do not need serialize
  ObStorageDatumBuffer datum_buffer_;
  // add by @zimiao ObDatumRow does not care about the free of trans_info_ptr's memory
  // The caller must guarantee the life cycle and release of this memory
  char *trans_info_;
};

class ObNewRowBuilder
{
public:
  ObNewRowBuilder()
    : cols_descs_(nullptr),
      new_row_(),
      obj_buf_()
  {}
  ~ObNewRowBuilder() = default;
  OB_INLINE int init(
      const common::ObIArray<share::schema::ObColDesc> &cols_descs,
      ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    cols_descs_ = &cols_descs;
    if (OB_FAIL(obj_buf_.init(&allocator))) {
      STORAGE_LOG(WARN, "Failed to init ObObjBufArray", K(ret));
    }
    return ret;
  }
  int build(
      const blocksstable::ObDatumRow &datum_row,
      common::ObNewRow *&new_row);
  int build_store_row(
      const blocksstable::ObDatumRow &datum_row,
      storage::ObStoreRow &store_row);
  TO_STRING_KV(KP_(cols_descs), K_(new_row));
private:
  const common::ObIArray<share::schema::ObColDesc> *cols_descs_;
  common::ObNewRow new_row_;
  storage::ObObjBufArray obj_buf_;
};

struct ObConstDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObConstDatumRow() { MEMSET(this, 0, sizeof(ObConstDatumRow)); }
  ObConstDatumRow(ObDatum *datums, uint64_t count, int64_t datum_row_offset)
    : datums_(datums), count_(count), datum_row_offset_(datum_row_offset) {}
  ~ObConstDatumRow() {}
  OB_INLINE int64_t get_column_count() const { return count_; }
  OB_INLINE bool is_valid() const { return nullptr != datums_ && count_ > 0 && datum_row_offset_ >= 0; }
  OB_INLINE const ObDatum &get_datum(const int64_t col_idx) const
  {
    OB_ASSERT(col_idx < count_ && col_idx >= 0);
    return datums_[col_idx];
  }
  int set_datums_ptr(char *datums_ptr);
  TO_STRING_KV(K_(count), "datums_:", ObArrayWrap<ObDatum>(datums_, count_));
  ObDatum *datums_; //The datums ptr may be changed, need to be recalculated by datum_row_offset
  uint64_t count_;
  int64_t datum_row_offset_;
};

struct ObGhostRowUtil {
public:
  ObGhostRowUtil() = delete;
  ~ObGhostRowUtil() = delete;
  static int make_ghost_row(
      const int64_t sql_sequence_col_idx,
      blocksstable::ObDatumRow &row);
  static int is_ghost_row(const blocksstable::ObMultiVersionRowFlag &flag, bool &is_ghost_row);
  static const int64_t GHOST_NUM = INT64_MAX;
};

struct ObShadowRowUtil {
public:
  ObShadowRowUtil() = delete;
  ~ObShadowRowUtil() = delete;
  static int make_shadow_row(
      const int64_t sql_sequence_col_idx,
      blocksstable::ObDatumRow &row);
};

struct ObSqlDatumInfo {
public:
  ObSqlDatumInfo() :
      datum_ptr_(nullptr),
      expr_(nullptr)
  {}
  ObSqlDatumInfo(common::ObDatum* datum_ptr, sql::ObExpr *expr)
      : datum_ptr_(datum_ptr), expr_(expr)
  {}
  ~ObSqlDatumInfo() = default;
  OB_INLINE void reset() { datum_ptr_ = nullptr; expr_ = nullptr; }
  OB_INLINE bool is_valid() const { return datum_ptr_ != nullptr && expr_ != nullptr; }
  OB_INLINE common::ObObjDatumMapType get_obj_datum_map() const { return expr_->obj_datum_map_; }
  TO_STRING_KV(KP_(datum_ptr), KP_(expr));

  common::ObDatum *datum_ptr_;
  const sql::ObExpr *expr_;
};

OB_INLINE int64_t ObStorageDatumWrapper::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (nullptr != buf && buf_len > 0) {
    pos = datum_.storage_to_string(buf, buf_len - 1, for_dump_, hex_length_);
    if (pos >= 0 && pos < buf_len) {
      buf[pos] = '\0';
    }
  } else {
    pos = 0;
    buf[0] = '\0';
  }
  return pos;
}

} // namespace blocksstable
} // namespace oceanbase
#endif
