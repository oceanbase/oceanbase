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

#ifndef OCEANBASE_LIBOBLOG_SCHEMA_CACHE_INFO_H__
#define OCEANBASE_LIBOBLOG_SCHEMA_CACHE_INFO_H__

#include "share/ob_errno.h"                               // OB_SUCCESS
#include "common/object/ob_object.h"                      // ObObjMeta
#include "common/ob_accuracy.h"                           // ObAccuracy
#include "lib/container/ob_array_helper.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
} // namespace schema
} // namespace share

namespace liboblog
{
class ObObj2strHelper;
class ObLogSchemaGuard;

// The primary keyless table has three hidden columns.
// column_id=OB_HIDDEN_PK_INCREMENT_COLUMN_ID[1], column_name="__pk_increment"
// column_id=OB_HIDDEN_PK_CLUSTER_COLUMN_ID[4], column_name="__pk_cluster_id"
// column_id=OB_HIDDEN_PK_PARTITION_COLUMN_ID[5], column_name="__pk_partition_id"
// Exposed externally using column_id=1 as the output primary key
enum ColumnFlag
{
  NORMAL_COLUMN_FLAG = 0,

  HIDDEN_COLUMN_FLAG = 1,
  DELETE_COLUMN_FLAG =2,
  HIDDEN_PRIMARY_KEY_TABLE_PK_INCREMENT_COLUMN_FLAG = 3,
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
  // Hidden column/invisible column initialisation interface
  int init(ColumnFlag column_flag);
  // Common column initialization interface
  // allocator is used to allocate memory for orig_default_value_str, consistent with release_mem
  int init(const share::schema::ObTableSchema &table_schema,
      const share::schema::ObColumnSchemaV2 &column_table_schema,
      const int64_t column_idx,
      ObObj2strHelper &obj2str_helper,
      common::ObIAllocator &allocator,
      const bool is_hidden_pk_table_pk_increment_column);
  void destroy();
  void reset();
  // You need to call release_mem in advance before destructuring to free memory
  // 1. release the default memory
  // 2. free extended_type_info memory
  void release_mem(common::ObIAllocator &allocator);

  inline bool is_hidden() const { return HIDDEN_COLUMN_FLAG == column_flag_; }
  inline bool is_delete() const { return DELETE_COLUMN_FLAG == column_flag_; }
  inline bool is_hidden_pk_table_pk_increment_column() const
  { return HIDDEN_PRIMARY_KEY_TABLE_PK_INCREMENT_COLUMN_FLAG == column_flag_; }
  inline bool is_invisible() const { return OBLOG_INVISIBLE_COLUMN_FLAG == column_flag_; }
  inline bool is_rowkey() const { return is_rowkey_; }

  void set_column_idx(const int64_t column_idx) { column_idx_ = static_cast<int16_t>(column_idx); }
  inline int64_t get_column_idx() const { return column_idx_; }

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

public:
  TO_STRING_KV(K_(column_flag),
      K_(column_idx),
      K_(meta_type),
      K_(accuracy),
      K_(collation_type),
      K_(orig_default_value_str),
      K_(extended_type_info_size),
      K_(extended_type_info),
      K_(is_rowkey));

private:
  int get_column_ori_default_value_(const share::schema::ObTableSchema &table_schema,
      const share::schema::ObColumnSchemaV2 &column_table_schema,
      const int64_t column_idx,
      ObObj2strHelper &obj2str_helper,
      common::ObIAllocator &allocator,
      common::ObString *&str);

  int init_extended_type_info_(const share::schema::ObTableSchema &table_schema,
      const share::schema::ObColumnSchemaV2 &column_table_schema,
      const int64_t column_idx,
      common::ObIAllocator &allocator);

private:
  int16_t            column_flag_;
  // Record column_idx (does not contain hidden columns)
  int16_t            column_idx_;
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

private:
  DISALLOW_COPY_AND_ASSIGN(ColumnSchemaInfo);
};

// liboblo customer row key info, only mandatory information, size and columnID array
class ObLogRowkeyInfo
{
public:
  ObLogRowkeyInfo();
  ~ObLogRowkeyInfo();
  int init(common::ObIAllocator &allocator,
      const int64_t size,
      const common::ObArray<uint64_t> &column_ids);
  void destroy();
  // Before destructuring, you need to call release_mem to release the memory of the silent column_id_array.
  void release_mem(common::ObIAllocator &allocator);

public:
  bool is_valid() const;

  inline int64_t get_size() const { return size_; }

  /**
   * get rowkey column id based on index
   * @param[in]  index     column index in RowkeyInfo
   * @param[out] column_id column id
   *
   * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
   */
  int get_column_id(const int64_t index, uint64_t &column_id) const;

public:
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int64_t              size_;
  uint64_t             *column_id_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRowkeyInfo);
};

// Non-primary key table outputs hidden primary key externally, column_id=1, column_name="__pk_increment"
// 1. schema perspective, non-user columns (column_id < 16) and hidden columns
// 2. user perspective, user columns and non-hidden columns, so here.
//    is_non_user_column_ = false;
//    is_hidden_column_ = false;
struct ColumnPropertyFlag
{
  ColumnPropertyFlag() { reset(); }
  ~ColumnPropertyFlag() { reset(); }

  void reset()
  {
    is_non_user_column_ = false;
    is_hidden_column_ = false;
    is_delete_column_ = false;
    is_invisible_column_ = false;
  }

  void reset(const bool is_non_user_column,
      const bool is_hidden_column,
      const bool is_delete_column,
      const bool is_invisible_column)
  {
    is_non_user_column_ = is_non_user_column;
    is_hidden_column_ = is_hidden_column;
    is_delete_column_ = is_delete_column;
    is_invisible_column_ = is_invisible_column;
  }

  inline bool is_non_user() const { return is_non_user_column_; }
  inline bool is_hidden() const { return is_hidden_column_; }
  inline bool is_delete() const { return is_delete_column_; }
  inline bool is_invisible() const { return is_invisible_column_; }

  bool is_non_user_column_;
  bool is_hidden_column_;
  bool is_delete_column_;
  bool is_invisible_column_;

  TO_STRING_KV(K_(is_non_user_column),
      K_(is_hidden_column),
      K_(is_delete_column),
      K_(is_invisible_column));
};

class TableSchemaInfo
{
public:
  explicit TableSchemaInfo(common::ObIAllocator &allocator);
  virtual ~TableSchemaInfo();

public:
  int init(const share::schema::ObTableSchema *table_schema);
  void destroy();

  common::ObIAllocator &get_allocator() { return allocator_; }

  inline bool is_hidden_pk_table() const { return is_hidden_pk_table_; }

  inline const ObLogRowkeyInfo &get_rowkey_info() const { return rowkey_info_; }

  inline int64_t get_non_hidden_column_count() const { return user_column_id_array_cnt_; }
  void set_non_hidden_column_count(const int64_t non_hidden_column_cnt)
  {
    user_column_id_array_cnt_ = non_hidden_column_cnt;
  }

  inline uint64_t *get_column_id_array() { return user_column_id_array_; }
  inline const uint64_t *get_column_id_array() const { return user_column_id_array_; }

  inline ColumnSchemaInfo *get_column_schema_array() { return column_schema_array_; }
  inline const ColumnSchemaInfo *get_column_schema_array() const { return column_schema_array_; }

  // init column schema info
  int init_column_schema_info(const share::schema::ObTableSchema &table_schema,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    const int64_t column_idx,
    const bool enable_output_hidden_primary_key,
    ObObj2strHelper &obj2str_helper);

  /// Get column_schema based on column_id
  /// Returns column_schema_info=NULL when it is a non-user/deleted/hidden column
  ///
  /// @param column_id            [in]   column id
  /// @param column_schema_info   [out]  column schema info
  /// @param column_property_flag [out]  Returns the column property identifier, whether it is a non-user/deleted column/hidden column
  ///
  /// @retval OB_SUCCESS          success
  /// @retval other error code    fail
  int get_column_schema_info(const uint64_t column_id,
      const bool enable_output_hidden_primary_key,
      ColumnSchemaInfo *&column_schema_info,
      ColumnPropertyFlag &column_property_flag) const;

  /// get column_id based on index
  ///
  /// @param column_index        [in]     column index
  /// @param column_id           [out]    column id
  ///
  /// @retval OB_SUCCESS          success
  /// @retval other error code    fail
  int get_column_id(const int64_t column_index, uint64_t &column_id) const;

  /// set column_id based on index
  ///
  /// @param column_index        [in]     column index
  /// @param column_id           [in]     column id
  ///
  /// @retval OB_SUCCESS          success
  /// @retval other error code    fail
  int set_column_id(const int64_t column_index, const uint64_t column_id);

public:
  TO_STRING_KV(K_(rowkey_info),
      K_(user_column_id_array),
      K_(user_column_id_array_cnt),
      K_(column_schema_array),
      K_(column_schema_array_cnt));

private:
  int init_rowkey_info_(const share::schema::ObTableSchema *table_schema);
  int init_user_column_id_array_(const int64_t cnt);
  void destroy_user_column_id_array_();
  int init_column_schema_array_(const int64_t cnt);
  void destroy_column_schema_array_();
  // 1. Non-user columns are filtered out directly
  // 2. No primary key tables hide primary keys and do not filter
  int get_column_schema_info_(const uint64_t column_id,
      const bool enable_output_hidden_primary_key,
      ColumnSchemaInfo *&column_schema_info,
      bool &is_non_user_column,
      bool &is_hidden_pk_table_pk_increment_column) const;

private:
  bool                 is_inited_;
  common::ObIAllocator &allocator_;

  bool               is_hidden_pk_table_;
  ObLogRowkeyInfo    rowkey_info_;

  // For tables without primary keys: user_column_id_array_ and column_schema_array_ are stored last at the end of the user column for external primary key output
  // column array stores the columnIdx corresponding to the columnID (does not contain hidden columns)
  uint64_t           *user_column_id_array_;
  int64_t            user_column_id_array_cnt_;

  ColumnSchemaInfo   *column_schema_array_;
  int64_t            column_schema_array_cnt_;

private:
  DISALLOW_COPY_AND_ASSIGN(TableSchemaInfo);
};

} // namespace liboblog
} // namespace oceanbase
#endif
