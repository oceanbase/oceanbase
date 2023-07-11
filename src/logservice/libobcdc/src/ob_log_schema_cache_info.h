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
 *
 * SchemaCacheInfo
 */

#ifndef OCEANBASE_LIBOBCDC_SCHEMA_CACHE_INFO_H__
#define OCEANBASE_LIBOBCDC_SCHEMA_CACHE_INFO_H__

#include "share/ob_errno.h"                               // OB_SUCCESS
#include "share/schema/ob_table_schema.h"                 // ColumnIdxHashArray
#include "common/object/ob_object.h"                      // ObObjMeta
#include "common/ob_accuracy.h"                           // ObAccuracy
#include "lib/container/ob_array_helper.h"

namespace oceanbase
{
namespace datadict
{
class ObDictTableMeta;
}
namespace libobcdc
{
class ObObj2strHelper;
class ObLogSchemaGuard;
class ObCDCUdtSchemaInfo;

// The primary keyless table has one hidden columns.
// column_id=OB_HIDDEN_PK_INCREMENT_COLUMN_ID[1], column_name="__pk_increment"
// Exposed externally using column_id=1 as the output primary key
enum ColumnFlag
{
  NORMAL_COLUMN_FLAG = 0,

  HIDDEN_COLUMN_FLAG = 1,
  DELETE_COLUMN_FLAG =2,
  HEAP_TABLE_PK_INCREMENT_COLUMN_FLAG = 3,
  // Macro definition of INVISIBLE_COLUMN_FLAG already available
  OBLOG_INVISIBLE_COLUMN_FLAG
};
const char *print_column_flag(ColumnFlag column_flag);

class ColumnSchemaInfo
{
public:
  ColumnSchemaInfo();
  virtual ~ColumnSchemaInfo();

public:
  // Common column initialization interface
  // allocator is used to allocate memory for orig_default_value_str, consistent with release_mem
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int init(
      const uint64_t column_id,
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_table_schema,
      const int16_t column_stored_idx,
      const bool is_usr_column,
      const int16_t usr_column_idx,
      const bool is_heap_table_pk_increment_column,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      ObObj2strHelper &obj2str_helper,
      common::ObIAllocator &allocator);
  void destroy();
  void reset();
  // You need to call release_mem in advance before destructuring to free memory
  // 1. release the default memory
  // 2. free extended_type_info memory
  void release_mem(common::ObIAllocator &allocator);

  inline bool is_hidden() const { return HIDDEN_COLUMN_FLAG == column_flag_; }
  inline bool is_delete() const { return DELETE_COLUMN_FLAG == column_flag_; }
  inline bool is_heap_table_pk_increment_column() const
  { return HEAP_TABLE_PK_INCREMENT_COLUMN_FLAG == column_flag_; }
  inline bool is_invisible() const { return OBLOG_INVISIBLE_COLUMN_FLAG == column_flag_; }
  inline bool is_rowkey() const { return is_rowkey_; }

  inline uint64_t get_column_id() const { return column_id_; }
  inline int16_t get_column_stored_idx() const { return column_stored_idx_; }

  inline bool is_usr_column() const { return is_usr_column_; }
  inline int16_t get_usr_column_idx() const { return usr_column_idx_; }

  void set_meta_type(const common::ObObjMeta &meta_type) { meta_type_.set_meta(meta_type); }
  inline common::ObObjMeta get_meta_type() const { return meta_type_; }

  void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  inline const common::ObCollationType &get_collation_type() const { return collation_type_; }

  void set_orig_default_value_str(common::ObString &orig_default_value_str)
  {
    orig_default_value_str_ = &orig_default_value_str;
  }
  inline const common::ObString *get_orig_default_value_str() const { return orig_default_value_str_; }
  // 1. To resolve the memory space, ObArrayHelper is not used directly to store information, get_extended_type_info returns size and an array of pointers directly
  // 2. call ObArrayHelper<ObString>(size, str_ptr, size) directly from the outer layer to construct a temporary array
  inline void get_extended_type_info(int64_t &size, common::ObString *&str_ptr) const
  {
    size = extended_type_info_size_;
    str_ptr = extended_type_info_;
  }
  void get_extended_type_info(common::ObArrayHelper<common::ObString> &str_array) const;

  inline void set_udt_set_id(const uint64_t id) { udt_set_id_ = id; }
  inline uint64_t get_udt_set_id() const { return udt_set_id_; }
  inline bool is_udt_column() const { return udt_set_id_ > 0 && OB_INVALID_ID != udt_set_id_; }

  inline void set_sub_data_type(const uint64_t sub_data_type) { sub_type_ = sub_data_type; }
  inline uint64_t get_sub_data_type() const { return sub_type_; }
  inline bool is_udt_hidden_column() const { return is_udt_column() && is_hidden(); }
  inline bool is_udt_main_column() const { return is_udt_column() && ! is_hidden(); }
  inline bool is_xmltype() const {
    return is_udt_column()
        && (((meta_type_.is_ext() || meta_type_.is_user_defined_sql_type()) && sub_type_ == T_OBJ_XML)
           || meta_type_.is_xml_sql_type());
  }

public:
  TO_STRING_KV(
      K_(column_id),
      K_(column_flag),
      K_(column_stored_idx),
      K_(is_usr_column),
      K_(usr_column_idx),
      K_(meta_type),
      K_(accuracy),
      K_(collation_type),
      K_(orig_default_value_str),
      K_(extended_type_info_size),
      K_(extended_type_info),
      K_(is_rowkey),
      K_(udt_set_id),
      K_(sub_type));

private:
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int get_column_ori_default_value_(
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_table_schema,
      const int16_t column_idx,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      ObObj2strHelper &obj2str_helper,
      common::ObIAllocator &allocator,
      common::ObString *&str);

  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int init_extended_type_info_(
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_table_schema,
      const int16_t column_idx,
      common::ObIAllocator &allocator);

private:
  uint64_t           column_id_;
  int16_t            column_flag_;
  int16_t            column_stored_idx_;     // index of column in ob memtable storage.
  bool               is_usr_column_;  // is column that user cares.
  int16_t            usr_column_idx_; // index of column ignore column that user doesn't care.
  common::ObObjMeta  meta_type_;
  common::ObAccuracy accuracy_;
  common::ObCollationType collation_type_;
  // TODO: There are no multiple versions of the default value, consider maintaining a copy
  common::ObString   *orig_default_value_str_;
  // used for enum and set
  int64_t            extended_type_info_size_;
  common::ObString   *extended_type_info_;
  // The rowkey_info in TableSchema is not accurate because the new no primary key table will change the partition key to the primary key.
  // need to mark if this column was the primary key when the user created it
  bool               is_rowkey_;

  uint64_t           udt_set_id_;
  uint64_t           sub_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ColumnSchemaInfo);
};

// liboblo customer row key info, only mandatory information, size and columnID array
class ObLogRowkeyInfo
{
public:
  ObLogRowkeyInfo();
  ~ObLogRowkeyInfo();
  int init(
      common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &table_schema);
  int init(
      common::ObIAllocator &allocator,
      const datadict::ObDictTableMeta &table_schema);
  void destroy();
  // Before destructuring, you need to call release_mem to release the memory of the silent column_id_array.
  void release_mem(common::ObIAllocator &allocator);

public:
  bool is_valid() const;

  inline int64_t get_size() const { return size_; }
  int set_column_stored_idx(const int16_t rowkey_col_idx, const int16_t column_stored_idx);

  /**
   * get rowkey column id based on index
   * @param[in]  index     column index in RowkeyInfo
   * @param[out] column_id column id
   *
   * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
   */
  int get_column_stored_idx(const int16_t rowkey_index, int16_t &column_stored_index) const;

public:
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  int do_init_(
      common::ObIAllocator &allocator,
      const int64_t size);

private:
  int64_t              size_;
  int16_t              *column_stored_idx_array_; // rowkey column_index -> column_stored_index

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRowkeyInfo);
};

template<class K, class V>
struct GetColumnKey
{
  void operator()(const K &k, const V &v) const
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template<>
struct GetColumnKey<share::schema::ObColumnIdKey, ColumnSchemaInfo *>
{
  share::schema::ObColumnIdKey operator()(const ColumnSchemaInfo *column_schema_info) const
  {
    return share::schema::ObColumnIdKey(column_schema_info->get_column_id());
  }
};

typedef common::hash::ObPointerHashArray<share::schema::ObColumnIdKey, ColumnSchemaInfo *, GetColumnKey> ColumnIdxHashArray;
typedef common::hash::ObHashMap<uint64_t, ObCDCUdtSchemaInfo*> ObCDCUdtSchemaInfoMap;
class TableSchemaInfo
{
public:
  explicit TableSchemaInfo(common::ObIAllocator &allocator);
  virtual ~TableSchemaInfo();

public:
  template<class TABLE_SCHEMA>
  int init(const TABLE_SCHEMA *table_schema);
  void destroy();

  common::ObIAllocator &get_allocator() { return allocator_; }

  inline bool is_heap_table() const { return is_heap_table_; }

  inline uint64_t get_aux_lob_meta_tid() const { return aux_lob_meta_tid_; }

  inline const ObLogRowkeyInfo &get_rowkey_info() const { return rowkey_info_; }

  // Call the function of this API
  // 1. ObLogMetaManager::set_unique_keys_(...)
  // 1. ObLogFormatter::build_row_value_(...)
  inline int64_t get_usr_column_count() const
  {
    return user_column_idx_array_cnt_;
  }
  void set_non_hidden_column_count(const int64_t non_hidden_column_cnt)
  {
    user_column_idx_array_cnt_ = non_hidden_column_cnt;
  }

  inline ColumnSchemaInfo *get_column_schema_array() { return column_schema_array_; }
  inline const ColumnSchemaInfo *get_column_schema_array() const { return column_schema_array_; }

  // init column schema info
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int init_column_schema_info(
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_table_schema,
      const int16_t column_stored_idx,
      const bool is_usr_column,
      const int16_t usr_column_idx,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      ObObj2strHelper &obj2str_helper);

  int get_column_schema_info_of_column_id(
      const uint64_t column_id,
      ColumnSchemaInfo *&column_schema_info) const;

  /// Get column_schema based on column_id
  /// Returns column_schema_info=NULL when it is a non-user/deleted/hidden column
  ///
  /// @param column_idx           [in]   column index
  /// @param is_column_stored_idx [in]   the column_idx is ob_datum column index
  /// @param column_schema_info   [out]  column schema info
  ///
  /// @retval OB_SUCCESS          success
  /// @retval other error code    fail
  int get_column_schema_info(
      const int16_t column_idx,
      const bool is_column_stored_idx,
      ColumnSchemaInfo *&column_schema_info) const;

  int get_column_schema_info_for_rowkey(
      const int16_t rowkey_idx,
      ColumnSchemaInfo *&column_schema_info) const;

  int get_main_column_of_udt(
      const uint64_t udt_set_id,
      ColumnSchemaInfo *&column_schema_info) const;

  int get_udt_schema_info(
      const uint64_t udt_set_id,
      ObCDCUdtSchemaInfo *&schema_info) const;

public:
  TO_STRING_KV(K_(rowkey_info),
      K_(is_heap_table),
      K_(user_column_idx_array),
      K_(user_column_idx_array_cnt),
      K_(column_schema_array),
      K_(column_schema_array_cnt));

private:
  template<class TABLE_SCHEMA>
  int init_rowkey_info_(const TABLE_SCHEMA *table_schema);
  int init_user_column_idx_array_(const int64_t cnt);
  void destroy_user_column_idx_array_();
  int init_column_schema_array_(const int64_t cnt);
  void destroy_column_schema_array_();
  int init_column_id_hash_array_(const int64_t column_cnt);
  inline bool is_valid_column_id_(const uint64_t column_id) const
  {
    return OB_INVALID_ID != column_id
        && (OB_APP_MIN_COLUMN_ID <= column_id
            || (is_heap_table_ && OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id));
  }
  int set_column_schema_info_for_column_id_(
      const uint64_t column_id,
      ColumnSchemaInfo *column_schema_info);

  /// set column_stored_idx - user_column_index mapping
  ///
  /// @param user_column_index   [in]     user column index
  /// @param column_stored_idx   [in]     ob column idx
  ///
  /// @retval OB_SUCCESS          success
  /// @retval other error code    fail
  int set_user_column_idx_(
      const int16_t user_column_index,
      const int16_t column_stored_index);
  inline int64_t get_id_hash_array_mem_size_(const int64_t column_cnt) const
  {
    return common::max(ColumnIdxHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
        column_cnt * 2) * sizeof(void*) + sizeof(ColumnIdxHashArray);
  }

  int add_udt_column_(ColumnSchemaInfo *column_info);
  int init_udt_schema_info_map_();
  int destroy_udt_schema_info_map_();

private:
  bool                 is_inited_;
  common::ObIAllocator &allocator_;

  bool               is_heap_table_;
  uint64_t           aux_lob_meta_tid_;
  ObLogRowkeyInfo    rowkey_info_;

  // column array stores the OBColumnIdx(by memtable datum column order) corresponding to the
  // UserColumnID(may not contain hidden_pk_column and invisible_column by
  // according to user config)
  // NOTE: only column need output to user will be recorded in this array.
  int16_t           *user_column_idx_array_;
  // total count of column that need output to user.
  // will reset after user_column_idx_array_ data setted.
  int64_t            user_column_idx_array_cnt_;

  ColumnSchemaInfo   *column_schema_array_;
  int64_t            column_schema_array_cnt_;
  ColumnIdxHashArray *column_id_hash_arr_; // column_id -> column_stored_idx

  ObCDCUdtSchemaInfoMap *udt_schema_info_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(TableSchemaInfo);
};

} // namespace libobcdc
} // namespace oceanbase
#endif
