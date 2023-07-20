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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_schema_cache_info.h"
#include "ob_obj2str_helper.h"                    // ObObj2strHelper
#include "ob_log_utils.h"                         // filter_non_user_column
#include "ob_log_config.h"                        // TCONF
#include "ob_log_meta_data_refresh_mode.h"        // RefreshMode
#include "ob_cdc_udt.h"                           // ObCDCUdtSchemaInfo
#include "src/logservice/data_dictionary/ob_data_dict_struct.h" // ObDictTableMeta/ObDictColumnMeta

#define SCHEMA_STAT_INFO(fmt, args...) LOG_INFO("[SCHEMA_CACHE_STAT] " fmt, args)
#define SCHEMA_STAT_DEBUG(fmt, args...) LOG_DEBUG("[SCHEMA_CACHE_STAT] " fmt, args)

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace libobcdc
{
ColumnSchemaInfo::ColumnSchemaInfo()
    : column_id_(OB_INVALID_ID),
      column_flag_(DELETE_COLUMN_FLAG),
      column_stored_idx_(OB_INVALID_INDEX),
      is_usr_column_(false),
      usr_column_idx_(OB_INVALID_INDEX),
      meta_type_(),
      accuracy_(),
      collation_type_(),
      orig_default_value_str_(NULL),
      extended_type_info_size_(0),
      extended_type_info_(NULL),
      is_rowkey_(false),
      udt_set_id_(0),
      sub_type_(0)
{
  // default column is delete
}

ColumnSchemaInfo::~ColumnSchemaInfo()
{
  destroy();
}

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ColumnSchemaInfo::init(
    const uint64_t column_id,
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_table_schema,
    const int16_t column_stored_idx,
    const bool is_usr_column,
    const int16_t usr_column_idx,
    const bool is_heap_table_pk_increment_column,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  common::ObString *orig_default_value_str = NULL;

  if (OB_UNLIKELY(column_stored_idx < 0 || column_stored_idx > OB_USER_ROW_MAX_COLUMNS_COUNT + OB_APP_MIN_COLUMN_ID)
      || OB_UNLIKELY(is_usr_column && (usr_column_idx < 0 || usr_column_idx > OB_USER_ROW_MAX_COLUMNS_COUNT))) {
    LOG_ERROR("invalid argument", K(column_stored_idx), K(is_usr_column), K(usr_column_idx));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_column_ori_default_value_(table_schema, column_table_schema, column_stored_idx, tz_info_wrap,
            obj2str_helper, allocator, orig_default_value_str))) {
      LOG_ERROR("get_column_ori_default_value_ fail", KR(ret), K(table_schema), K(column_table_schema),
          K(column_stored_idx));
  } else if (OB_ISNULL(orig_default_value_str)) {
    LOG_ERROR("orig_default_value_str is null", K(orig_default_value_str));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(init_extended_type_info_(table_schema, column_table_schema, column_stored_idx, allocator))) {
    LOG_ERROR("init_extended_type_info_ fail", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "column_name", column_table_schema.get_column_name(),
        K(column_stored_idx));
  } else {
    const ObObjMeta &meta_type = column_table_schema.get_meta_type();
    const ObAccuracy &accuracy = column_table_schema.get_accuracy();
    const ObCollationType &collation_type = column_table_schema.get_collation_type();
    column_id_ = column_id;

    if (is_heap_table_pk_increment_column) {
      column_flag_ = HEAP_TABLE_PK_INCREMENT_COLUMN_FLAG;
    } else if (column_table_schema.is_invisible_column()) {
      column_flag_ = OBLOG_INVISIBLE_COLUMN_FLAG;
    } else if (column_table_schema.is_hidden()) {
      column_flag_ = HIDDEN_COLUMN_FLAG;
    } else {
      column_flag_ = NORMAL_COLUMN_FLAG;
    }
    column_stored_idx_ = column_stored_idx;
    is_usr_column_ = is_usr_column;
    if (is_usr_column) {
      usr_column_idx_ = usr_column_idx;
    }
    meta_type_.set_meta(meta_type);
    accuracy_ = accuracy;
    collation_type_ =  collation_type;
    orig_default_value_str_ = orig_default_value_str;
    is_rowkey_ = column_table_schema.is_original_rowkey_column();
    udt_set_id_ = column_table_schema.get_udt_set_id();
    sub_type_ = column_table_schema.get_sub_data_type();
  }

  return ret;
}

void ColumnSchemaInfo::destroy()
{
  reset();
}

void ColumnSchemaInfo::reset()
{
  column_id_ = OB_INVALID_ID;
  column_flag_ = DELETE_COLUMN_FLAG;
  column_stored_idx_ = OB_INVALID_INDEX;
  is_usr_column_ = false;
  usr_column_idx_ = OB_INVALID_INDEX;
  meta_type_.reset();
  accuracy_.reset();
  collation_type_ = ObCollationType::CS_TYPE_INVALID;

  if (NULL != orig_default_value_str_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "orig_default_value_str_ should be null", K(orig_default_value_str_));
    orig_default_value_str_ = NULL;
  }

  extended_type_info_size_ = 0;
  extended_type_info_ = NULL;
  is_rowkey_ = false;
  udt_set_id_ = 0;
  sub_type_ = 0;
}

void ColumnSchemaInfo::get_extended_type_info(common::ObArrayHelper<common::ObString> &str_array) const
{
  str_array.reset();
  str_array.init(extended_type_info_size_, extended_type_info_, extended_type_info_size_);
}

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ColumnSchemaInfo::get_column_ori_default_value_(
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_table_schema,
    const int16_t column_idx,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper,
    common::ObIAllocator &allocator,
    common::ObString *&str)
{
  int ret = OB_SUCCESS;
  str = NULL;

  const ObObj &orig_default_obj = column_table_schema.get_orig_default_value();
  str = static_cast<ObString *>(allocator.alloc(sizeof(ObString)));

  if (OB_ISNULL(str)) {
    LOG_ERROR("allocate memory for ObString fail", K(sizeof(ObString)));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new (str) ObString();

    // Deep copy of string values is required when converting defaults to strings
    // because the memory for the default value is stored in the Schema, which cannot be relied upon
    if (OB_FAIL(obj2str_helper.obj2str(table_schema.get_tenant_id(),
            table_schema.get_table_id(),
            column_table_schema.get_column_id(),
            orig_default_obj, *str, allocator, true,
            column_table_schema.get_extended_type_info(),
            column_table_schema.get_accuracy(),
            column_table_schema.get_collation_type(),
            tz_info_wrap))) {
      LOG_ERROR("obj2str cast orig_default_value fail", KR(ret), K(orig_default_obj), K(*str),
          "tenant_id", table_schema.get_tenant_id(),
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          "column_id", column_table_schema.get_column_id(),
          "column_name", column_table_schema.get_column_name(),
          K(column_idx));
    }
  }

  if (OB_SUCCESS != ret && NULL != str) {
    str->~ObString();
    allocator.free(str);
    str = NULL;
  }

  return ret;
}

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ColumnSchemaInfo::init_extended_type_info_(
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_table_schema,
    const int16_t column_idx,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (! column_table_schema.is_enum_or_set()) {
    // do nothing
  } else {
    // Only enum or set types are cached
    const common::ObIArray<common::ObString> &src_extended_type_info =
      column_table_schema.get_extended_type_info();
    const int64_t alloc_size = src_extended_type_info.count() * static_cast<int64_t>(sizeof(ObString));
    void *buf = NULL;

    if (src_extended_type_info.count() <= 0) {
      // do nothing
    } else if (OB_ISNULL(buf = allocator.alloc(alloc_size))) {
      LOG_ERROR("alloc memory failed", K(alloc_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(extended_type_info_ = static_cast<ObString *>(buf))) {
      LOG_ERROR("extended_type_info_ is null", K(extended_type_info_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      extended_type_info_size_ = src_extended_type_info.count();

      for (int64_t idx = 0; OB_SUCC(ret) && idx < src_extended_type_info.count(); ++idx) {
        ObString &str = extended_type_info_[idx];

        if (OB_FAIL(deep_copy_str(src_extended_type_info.at(idx), str, allocator))) {
          LOG_ERROR("deep_copy_str failed", KR(ret), K(idx), K(str),
              "table_id", table_schema.get_table_id(),
              "table_name", table_schema.get_table_name(),
              "column_id", column_table_schema.get_column_id(),
              "column_name", column_table_schema.get_column_name(),
              K(column_idx));
        } else {
          LOG_INFO("extended_type_info_ get succ", K(idx), K(str),
              "table_id", table_schema.get_table_id(),
              "table_name", table_schema.get_table_name(),
              "column_id", column_table_schema.get_column_id(),
              "column_name", column_table_schema.get_column_name(),
              K(column_idx));
        }
      } // for


      if (OB_SUCCESS != ret && NULL != buf) {
        allocator.free(buf);
      }
    }
  }

  return ret;
}

void ColumnSchemaInfo::release_mem(common::ObIAllocator &allocator)
{
  if (NULL != orig_default_value_str_) {
    allocator.free(orig_default_value_str_);
    orig_default_value_str_ = NULL;
  }

  if (NULL != extended_type_info_) {
    for (int64_t idx = 0; idx < extended_type_info_size_; ++idx) {
      void *ptr = static_cast<void *>(&extended_type_info_[idx]);
      if (NULL != ptr) {
        allocator.free(ptr);
        ptr = NULL;
      }
    }

    allocator.free(static_cast<void *>(extended_type_info_));
    extended_type_info_ = NULL;
    extended_type_info_size_ = 0;
  }
}

ObLogRowkeyInfo::ObLogRowkeyInfo()
    : size_(0),
      column_stored_idx_array_(NULL)
{
}

ObLogRowkeyInfo::~ObLogRowkeyInfo()
{
  destroy();
}

int ObLogRowkeyInfo::init(
    common::ObIAllocator &allocator,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();

  if (OB_FAIL(do_init_(allocator, rowkey_info.get_size()))) {
    LOG_ERROR("init ObLogRowkeyInfo failed", KR(ret), K(rowkey_info), K(table_schema));
  }

  return ret;
}

int ObLogRowkeyInfo::init(
    common::ObIAllocator &allocator,
    const datadict::ObDictTableMeta &table_schema)
{
  int ret = OB_SUCCESS;
  ObRowkeyInfo rowkey_info;

  if (OB_FAIL(table_schema.get_rowkey_info(rowkey_info))) {
    LOG_ERROR("get_rowkey_info from ObDictTableMeta failed", KR(ret), K(table_schema));
  } else if (OB_FAIL(do_init_(allocator, rowkey_info.get_size()))) {
    LOG_ERROR("init ObLogRowkeyInfo failed", KR(ret), K(rowkey_info), K(table_schema));
  }

  return ret;
}

int ObLogRowkeyInfo::do_init_(
    common::ObIAllocator &allocator,
    const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0)) {
    LOG_ERROR("invalid argument", K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    size_ = size;
    int64_t alloc_size = size * sizeof(column_stored_idx_array_[0]);

    if (OB_ISNULL(column_stored_idx_array_ = static_cast<int16_t *>(allocator.alloc(alloc_size)))) {
      LOG_ERROR("allocate memory fail", K(column_stored_idx_array_), K(alloc_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
    }
  }

  return ret;
}

void ObLogRowkeyInfo::destroy()
{
  if (OB_NOT_NULL(column_stored_idx_array_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "column_id_array_ should be null", K(column_stored_idx_array_));
    column_stored_idx_array_ = NULL;
  }
  size_ = 0;
}

void ObLogRowkeyInfo::release_mem(common::ObIAllocator &allocator)
{
  if (NULL != column_stored_idx_array_) {
    allocator.free(column_stored_idx_array_);
    column_stored_idx_array_ = NULL;
  }
}

bool ObLogRowkeyInfo::is_valid() const
{
  bool bool_ret = false;

  bool_ret = (NULL != column_stored_idx_array_ && size_ > 0);

  return bool_ret;
}

int ObLogRowkeyInfo::set_column_stored_idx(const int16_t rowkey_col_idx, const int16_t column_stored_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_col_idx < 0 || rowkey_col_idx >= size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(rowkey_col_idx), K_(size));
  } else {
    column_stored_idx_array_[rowkey_col_idx] = column_stored_idx;
  }

  return ret;
}

int ObLogRowkeyInfo::get_column_stored_idx(const int16_t rowkey_index, int16_t &column_stored_idx) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_index < 0 || rowkey_index >= size_)) {
    LOG_ERROR("invalid argument", K(rowkey_index), K_(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (! is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRowkeyInfo is not valid", KR(ret), K(rowkey_index), K_(size), KP_(column_stored_idx_array));
  } else {
    column_stored_idx = column_stored_idx_array_[rowkey_index];
  }

  return ret;
}

int64_t ObLogRowkeyInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "{size=%ld, ", size_);

    if (OB_NOT_NULL(column_stored_idx_array_)) {
      (void)common::databuff_printf(buf, buf_len, pos, "column_stored_idx:[");

      for (int64_t idx=0; idx < size_ - 1; idx++) {
        (void)common::databuff_printf(buf, buf_len, pos, "%d,", column_stored_idx_array_[idx]);
      }

      (void)common::databuff_printf(buf, buf_len, pos, "%d]}", column_stored_idx_array_[size_ - 1]);
    }
  }

  return pos;
}

TableSchemaInfo::TableSchemaInfo(ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      is_heap_table_(false),
      aux_lob_meta_tid_(OB_INVALID_ID),
      rowkey_info_(),
      user_column_idx_array_(NULL),
      user_column_idx_array_cnt_(0),
      column_schema_array_(NULL),
      column_schema_array_cnt_(0),
      column_id_hash_arr_(nullptr),
      udt_schema_info_map_(nullptr)
{
}

TableSchemaInfo::~TableSchemaInfo()
{
  destroy();
}

template<class TABLE_SCHEMA>
int TableSchemaInfo::init(const TABLE_SCHEMA *table_schema)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("TableSchemaInfo has been initialized", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(table_schema));
  } else {
    // To avoid performance overhead here, do not rely on table schema traversal to get the number of columns that do not contain hidden columns,
    // externally set user_column_idx_array_cnt_ to the number of hidden columns not included
    user_column_idx_array_cnt_ = table_schema->get_column_count();
    column_schema_array_cnt_ = table_schema->get_max_used_column_id() - OB_APP_MIN_COLUMN_ID + 1;
    const bool is_heap_table = table_schema->is_heap_table();
    aux_lob_meta_tid_ = table_schema->get_aux_lob_meta_tid();

    // For table without primary keys:
    // record hidden primary key information at the start column_id=1, column_name="__pk_increment", reserved position
    if (is_heap_table) {
      ++user_column_idx_array_cnt_;
      ++column_schema_array_cnt_;
    }

    // TODO fix me
    // There is a bad case where frequent addition and deletion of columns will lead to serious memory amplification,
    // but online environments generally do not delete columns, so it is safe here

    if (OB_FAIL(init_rowkey_info_(table_schema))) {
      LOG_ERROR("init_rowkey_info_ fail", KR(ret), K(table_schema));
    } else if (OB_FAIL(init_user_column_idx_array_(user_column_idx_array_cnt_))) {
      LOG_ERROR("init_user_column_idx_array_ fail", KR(ret), K(user_column_idx_array_cnt_));
    } else if (OB_FAIL(init_column_schema_array_(column_schema_array_cnt_))) {
      LOG_ERROR("init_column_schema_array_ fail", KR(ret), K(column_schema_array_cnt_));
    } else if (OB_FAIL(init_column_id_hash_array_(column_schema_array_cnt_))) {
      LOG_ERROR("init_column_id_hash_array_ fail", KR(ret), K(column_schema_array_cnt_));
    } else {
      is_inited_ = true;
      is_heap_table_ = is_heap_table;

      LOG_INFO("table_schema_info init succ", "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(),
          K_(is_heap_table),
          K_(aux_lob_meta_tid),
          "version", table_schema->get_schema_version(),
          "user_column_idx_array_cnt", user_column_idx_array_cnt_,
          "max_used_column_id", table_schema->get_max_used_column_id(),
          "column_schema_array_cnt", column_schema_array_cnt_);
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}
template int TableSchemaInfo::init(const share::schema::ObTableSchema *table_schema);
template int TableSchemaInfo::init(const datadict::ObDictTableMeta *table_schema);

void TableSchemaInfo::destroy()
{
  is_inited_ = false;

  is_heap_table_ = false;
  aux_lob_meta_tid_ = OB_INVALID_ID;
  rowkey_info_.release_mem(allocator_);

  if (OB_NOT_NULL(column_id_hash_arr_)) {
    allocator_.free(column_id_hash_arr_);
    column_id_hash_arr_ = NULL;
  }
  destroy_user_column_idx_array_();
  destroy_column_schema_array_();
  destroy_udt_schema_info_map_();
}

template<class TABLE_SCHEMA>
int TableSchemaInfo::init_rowkey_info_(const TABLE_SCHEMA *table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(table_schema));
  } else if (OB_FAIL(rowkey_info_.init(allocator_, *table_schema))) {
    LOG_ERROR("rowkey info init fail", KR(ret), K(table_schema));
  }

  return ret;
}

int TableSchemaInfo::init_user_column_idx_array_(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = cnt * sizeof(user_column_idx_array_[0]);

  if (OB_UNLIKELY(cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(cnt));
  } else if (OB_ISNULL(user_column_idx_array_ = static_cast<int16_t *>(allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory fail", KR(ret), K(user_column_idx_array_), K(alloc_size), K(cnt));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx) {
      user_column_idx_array_[idx] = OB_INVALID_INDEX;
    }
  }

  return ret;
}

void TableSchemaInfo::destroy_user_column_idx_array_()
{
  if (NULL != user_column_idx_array_) {
    allocator_.free(user_column_idx_array_);
    user_column_idx_array_ = NULL;
    user_column_idx_array_cnt_ = 0;
  }
}

int TableSchemaInfo::init_column_schema_array_(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = cnt * sizeof(column_schema_array_[0]);

  if (OB_UNLIKELY(cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(cnt));
  } else if (OB_ISNULL(column_schema_array_ = static_cast<ColumnSchemaInfo *>(allocator_.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory fail", KR(ret), K(column_schema_array_), K(alloc_size), K(cnt));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx) {
      new(column_schema_array_ + idx) ColumnSchemaInfo();
    }
  }

  return ret;
}

void TableSchemaInfo::destroy_column_schema_array_()
{
  if (NULL != column_schema_array_) {
    for (int64_t idx = 0, cnt = column_schema_array_cnt_; idx < cnt; ++idx) {
      column_schema_array_[idx].release_mem(allocator_);
      column_schema_array_[idx].~ColumnSchemaInfo();
    }

    allocator_.free(column_schema_array_);
    column_schema_array_ = NULL;
    column_schema_array_cnt_ = 0;
  }
}

int TableSchemaInfo::init_column_id_hash_array_(const int64_t column_count)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t id_hash_array_size = get_id_hash_array_mem_size_(column_count);

  if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(id_hash_array_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc mem for column_id_hash_arr_ failed", KR(ret), K(id_hash_array_size));
  } else if (OB_ISNULL(column_id_hash_arr_ = new (buf) ColumnIdxHashArray(id_hash_array_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new column_schema_array_ failed", KR(ret));
  }

  return ret;
}

int TableSchemaInfo::set_column_schema_info_for_column_id_(
    const uint64_t column_id,
    ColumnSchemaInfo *column_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_valid_column_id_(column_id))
      || OB_ISNULL(column_schema_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments for set_column_stored_idx_for_column_id_", KR(ret), K(column_id), KPC(column_schema_info), KPC(this));
  } else if (OB_FAIL(column_id_hash_arr_->set_refactored(share::schema::ObColumnIdKey(column_id), column_schema_info))) {
    LOG_ERROR("column_id_hash_arr_ set_refactored  failed", KR(ret), K(column_id), K(column_schema_info), KPC(this));
  }

  return ret;
}

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int TableSchemaInfo::init_column_schema_info(
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_table_schema,
    const int16_t column_stored_idx,
    const bool is_usr_column,
    const int16_t usr_column_idx,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper)
{
  int ret = OB_SUCCESS;
  const uint64_t column_id = column_table_schema.get_column_id();
  const bool is_hidden_column = column_table_schema.is_hidden();
  const bool enable_output_invisible_column = (0 != TCONF.enable_output_invisible_column);
  const bool enable_output_hidden_primary_key = (0 != TCONF.enable_output_hidden_primary_key);
  const bool is_invisible_column = column_table_schema.is_invisible_column();
  const bool is_rowkey_col = column_table_schema.is_rowkey_column();
  const int16_t rowkey_idx = column_table_schema.get_rowkey_position() -1;
  ColumnSchemaInfo *column_schema_info = NULL;
  bool is_heap_table_pk_increment_column = table_schema.is_heap_table()  && (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TableSchemaInfo has not inited", KR(ret));
  } else if (OB_UNLIKELY(column_stored_idx < 0 || (is_usr_column && usr_column_idx < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(column_stored_idx));
  } else if (OB_FAIL(get_column_schema_info(
      column_stored_idx,
      true,/*is_column_stored_idx*/
      column_schema_info))) {
    LOG_ERROR("get_column_schema_info fail", KR(ret), K(column_id), K(column_stored_idx),
        KPC(column_schema_info), K(is_usr_column), K(is_heap_table_pk_increment_column));
  } else if (OB_ISNULL(column_schema_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_schema_info is null", KR(ret), K(column_id), K(column_schema_info));
  } else if (OB_FAIL(column_schema_info->init(
      column_id,
      table_schema,
      column_table_schema,
      column_stored_idx,
      is_usr_column,
      usr_column_idx,
      is_heap_table_pk_increment_column,
      tz_info_wrap,
      obj2str_helper,
      get_allocator()))) {
    LOG_ERROR("column_schema_info init fail", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        K(column_stored_idx), K(is_heap_table_pk_increment_column),
        K(is_usr_column), K(usr_column_idx),
        "meta_type", column_table_schema.get_meta_type(),
        "accuracy", column_table_schema.get_accuracy(),
        K(table_schema), K(column_table_schema));
  } else if (OB_FAIL(set_column_schema_info_for_column_id_(column_id, column_schema_info))) {
    LOG_ERROR("set_column_stored_idx_for_column_id_ failed", KR(ret), K(column_id), K(column_stored_idx), K(column_table_schema), KPC(column_schema_info));
  } else if (is_usr_column && OB_FAIL(set_user_column_idx_(usr_column_idx, column_stored_idx))) {
    LOG_ERROR("set_user_column_id_ fail", KR(ret),K(column_id), K(usr_column_idx), K(column_stored_idx));
  } else if (is_rowkey_col && OB_FAIL(rowkey_info_.set_column_stored_idx(rowkey_idx, column_stored_idx))) {
    LOG_ERROR("set_user_column_id_ fail", KR(ret),K(column_id), K(rowkey_idx), K(column_stored_idx));
  } else if (OB_FAIL(add_udt_column_(column_schema_info))) {
    LOG_ERROR("add_udt_column_ fail", KR(ret),
        K(column_stored_idx), K(is_heap_table_pk_increment_column),
        K(is_usr_column), K(usr_column_idx));
  } else {
    LOG_DEBUG("column_schema_info init success", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        "column_id", column_table_schema.get_column_id(),
        "column_name", column_table_schema.get_column_name(),
        "rowkey_pos", column_table_schema.get_rowkey_position(),
        K(column_stored_idx), K(is_heap_table_pk_increment_column),
        K(is_usr_column), K(usr_column_idx),
        "meta_type", column_table_schema.get_meta_type(),
        "accuracy", column_table_schema.get_accuracy());
  }

  return ret;
}

template int TableSchemaInfo::init_column_schema_info(
    const ObTableSchema &table_schema,
    const ObColumnSchemaV2 &column_table_schema,
    const int16_t column_stored_idx,
    const bool is_usr_column,
    const int16_t usr_column_idx,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper);

 template int TableSchemaInfo::init_column_schema_info(
    const datadict::ObDictTableMeta &table_schema,
    const datadict::ObDictColumnMeta &column_table_schema,
    const int16_t column_stored_idx,
    const bool is_usr_column,
    const int16_t usr_column_idx,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper);

int TableSchemaInfo::get_column_schema_info_of_column_id(
    const uint64_t column_id,
    ColumnSchemaInfo *&column_schema_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TableSchemaInfo is not inited", KR(ret));
  } else if (OB_ISNULL(column_id_hash_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_id_hash_arr_ is not valid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(! is_valid_column_id_(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid column_id", KR(ret), K(column_id), KPC(this));
  } else if (OB_FAIL(column_id_hash_arr_->get_refactored(share::schema::ObColumnIdKey(column_id), column_schema_info))) {
    LOG_ERROR("get_column_stored_idx_of_column_id_ failed", KR(ret), K(column_id), K_(column_id_hash_arr));
  } else if (OB_ISNULL(column_schema_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid column_stored_idx for column_id", KR(ret), K(column_id), KPC(this));
  } else {
    // success
  }

  return ret;
}

int TableSchemaInfo::get_column_schema_info(
    const int16_t column_idx,
    const bool is_column_stored_idx,
    ColumnSchemaInfo *&column_schema_info) const
{
  int ret = OB_SUCCESS;
  int16_t column_stored_idx = column_idx;
  column_schema_info = NULL;

  if (! is_column_stored_idx) {
    if (OB_UNLIKELY(OB_INVALID_INDEX == column_idx
        || column_idx >= user_column_idx_array_cnt_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid user_column_index", KR(ret),
          K(column_idx),K(is_column_stored_idx), KPC(this));
    } else {
      column_stored_idx = user_column_idx_array_[column_idx];
    }
  }

  // range of user columns:
  // OB_APP_MIN_COLUMN_ID: 16
  // OB_MIN_SHADOW_COLUMN_ID: 32767
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TableSchemaInfo has not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_INDEX == column_stored_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(column_idx), K(column_stored_idx), K(is_column_stored_idx), KPC(this));
  } else if (OB_ISNULL(column_schema_info = &column_schema_array_[column_stored_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_schema_info is null", KR(ret), K(column_idx), K(column_stored_idx), K(is_column_stored_idx), KPC(this));
  } else {
  }

  return ret;
}

int TableSchemaInfo::get_column_schema_info_for_rowkey(
    const int16_t rowkey_idx,
    ColumnSchemaInfo *&column_schema_info) const
{
  int ret = OB_SUCCESS;
  int16_t column_stored_idx = OB_INVALID_INDEX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TableSchemaInfo has not inited", KR(ret));
  } else if (OB_UNLIKELY(rowkey_idx < 0 || rowkey_info_.get_size() <= rowkey_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid rowkey_index", KR(ret), K(rowkey_idx), KPC(this));
  } else if (OB_FAIL(rowkey_info_.get_column_stored_idx(rowkey_idx, column_stored_idx))) {
    LOG_ERROR("get column_stored_idx of rowkey_idx failed", KR(ret), K(rowkey_idx), KPC(this));
  } else if (OB_FAIL(get_column_schema_info(column_stored_idx, true, column_schema_info))) {
    LOG_ERROR("get_column_schema_info failed", KR(ret), K(column_stored_idx), K(rowkey_idx), KPC(this));
  }

  return ret;
}

int TableSchemaInfo::set_user_column_idx_(
    const int16_t user_column_index,
    const int16_t column_stored_index)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TableSchemaInfo has not inited", KR(ret));
  } else if (OB_UNLIKELY(user_column_index < 0 || user_column_index >= user_column_idx_array_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(user_column_index), K_(user_column_idx_array_cnt));
  } else {
    user_column_idx_array_[user_column_index] = column_stored_index;
  }

  return ret;
}

int TableSchemaInfo::add_udt_column_(ColumnSchemaInfo *column_info)
{
  int ret = OB_SUCCESS;
  uint64_t udt_set_id = 0;
  ObCDCUdtSchemaInfo *udt_schema_info = nullptr;
  if (OB_ISNULL(column_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add null hidden column info", KR(ret));
  } else if (column_info->is_udt_column()) {
    udt_set_id = column_info->get_udt_set_id();
    if (OB_ISNULL(udt_schema_info_map_) && OB_FAIL(init_udt_schema_info_map_())) {
      LOG_ERROR("udt_schema_info_map_ create fail", KR(ret), K(udt_set_id));
    } else if (OB_FAIL(udt_schema_info_map_->get_refactored(udt_set_id, udt_schema_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // create if not exist
        if (OB_ISNULL(udt_schema_info = OB_NEWx(ObCDCUdtSchemaInfo, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("allocate memory for udt schema info fail", KR(ret), "size", sizeof(ObCDCUdtSchemaInfo));
        } else if (OB_FAIL(udt_schema_info_map_->set_refactored(udt_set_id, udt_schema_info))) {
          if (OB_NOT_NULL(udt_schema_info)) {
            udt_schema_info->~ObCDCUdtSchemaInfo();
            allocator_.free(udt_schema_info);
            udt_schema_info = nullptr;
          }
          LOG_ERROR("set udt_schema_info_map_ fail", KR(ret), K(udt_set_id));
        }
      } else {
        LOG_ERROR("get udt schema info fail", KR(ret), K(udt_set_id));
      }
    }

    // add
    if (OB_SUCC(ret) && OB_NOT_NULL(udt_schema_info)) {
      if (column_info->is_hidden()) {
        if (OB_FAIL(udt_schema_info->add_hidden_column(column_info))) {
          LOG_ERROR("add hidden column info fail", KR(ret), K(column_info));
        }
      } else {
        if (OB_FAIL(udt_schema_info->set_main_column(column_info))) {
          LOG_ERROR("set main column info fail", KR(ret), K(column_info));
        }
      }
    } else {
      LOG_ERROR("add udt column fail", KR(ret), K(udt_set_id), KP(udt_schema_info));
    }
  } // end if is_udt_column
  return ret;
}

int TableSchemaInfo::init_udt_schema_info_map_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(udt_schema_info_map_ = OB_NEWx(ObCDCUdtSchemaInfoMap, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for udt_schema_info_map_ fail", KR(ret), "size", sizeof(ObCDCUdtSchemaInfoMap));
  } else if (OB_FAIL(udt_schema_info_map_->create(4, "UDT"))) {
    LOG_ERROR("udt_schema_info_map_ create fail", KR(ret));
  }
  return ret;
}

int TableSchemaInfo::destroy_udt_schema_info_map_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(udt_schema_info_map_)) {
    LOG_DEBUG("udt_schema_info_map_ not init", KP(udt_schema_info_map_));
  } else {
    for (ObCDCUdtSchemaInfoMap::iterator iter = udt_schema_info_map_->begin(); iter != udt_schema_info_map_->end(); ++iter) {
      uint64_t udt_set_id = iter->first;
      ObCDCUdtSchemaInfo *group_schema = iter->second;
      group_schema->~ObCDCUdtSchemaInfo();
      allocator_.free(group_schema);
    }
    udt_schema_info_map_->~ObCDCUdtSchemaInfoMap();
    allocator_.free(udt_schema_info_map_);
    udt_schema_info_map_ = nullptr;
  }
  return ret;
}

int TableSchemaInfo::get_main_column_of_udt(
    const uint64_t udt_set_id,
    ColumnSchemaInfo *&column_schema_info) const
{
  int ret = OB_SUCCESS;
  ObCDCUdtSchemaInfo *udt_schema_info = nullptr;
  if (OB_ISNULL(udt_schema_info_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("udt_schema_info_map_ is null", KR(ret));
  } else if (OB_FAIL(udt_schema_info_map_->get_refactored(udt_set_id, udt_schema_info))) {
    LOG_ERROR("get udt column schema fail", KR(ret), K(udt_set_id));
  } else if (OB_FAIL(udt_schema_info->get_main_column(column_schema_info))) {
    LOG_ERROR("get main column of udt fail", KR(ret), K(udt_set_id));
  }
  return ret;
}

int TableSchemaInfo::get_udt_schema_info(
    const uint64_t udt_set_id,
    ObCDCUdtSchemaInfo *&schema_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(udt_schema_info_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("udt_schema_info_map_ is null", KR(ret));
  } else if (OB_FAIL(udt_schema_info_map_->get_refactored(udt_set_id, schema_info))) {
    LOG_ERROR("get udt column schema  fail", KR(ret), K(udt_set_id));
  }
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
