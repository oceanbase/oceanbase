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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_schema_cache_info.h"
#include "ob_obj2str_helper.h"                    // ObObj2strHelper
#include "ob_log_utils.h"                         // filter_non_user_column
#include "ob_log_config.h"                        // TCONF

#include "share/schema/ob_table_schema.h"         // ObTableSchema

#define SCHEMA_STAT_INFO(fmt, args...) LOG_INFO("[SCHEMA_CACHE_STAT] " fmt, args)
#define SCHEMA_STAT_DEBUG(fmt, args...) LOG_DEBUG("[SCHEMA_CACHE_STAT] " fmt, args)

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace liboblog
{
ColumnSchemaInfo::ColumnSchemaInfo()
    : column_flag_(DELETE_COLUMN_FLAG),
      column_idx_(0),
      meta_type_(),
      accuracy_(),
      collation_type_(),
      orig_default_value_str_(NULL),
      extended_type_info_size_(0),
      extended_type_info_(NULL),
      is_rowkey_(false)
{
  // default column is delete
}

ColumnSchemaInfo::~ColumnSchemaInfo()
{
  destroy();
}

int ColumnSchemaInfo::init(const share::schema::ObTableSchema &table_schema,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    const int64_t column_idx,
    ObObj2strHelper &obj2str_helper,
    common::ObIAllocator &allocator,
    const bool is_hidden_pk_table_pk_increment_column)
{
  int ret = OB_SUCCESS;
  common::ObString *orig_default_value_str = NULL;

  if (OB_UNLIKELY(column_idx < 0 || column_idx > OB_USER_ROW_MAX_COLUMNS_COUNT)) {
    LOG_ERROR("invalid argument", K(column_idx));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_column_ori_default_value_(table_schema, column_table_schema, column_idx,
            obj2str_helper, allocator, orig_default_value_str))) {
      LOG_ERROR("get_column_ori_default_value_ fail", KR(ret), K(table_schema), K(column_table_schema),
          K(column_idx));
  } else if (OB_ISNULL(orig_default_value_str)) {
    LOG_ERROR("orig_default_value_str is null", K(orig_default_value_str));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(init_extended_type_info_(table_schema, column_table_schema, column_idx, allocator))) {
    LOG_ERROR("init_extended_type_info_ fail", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "column_name", column_table_schema.get_column_name(),
        K(column_idx));
  } else {
    const ObObjMeta &meta_type = column_table_schema.get_meta_type();
    const ObAccuracy &accuracy = column_table_schema.get_accuracy();
    const ObCollationType &collation_type = column_table_schema.get_collation_type();

    if (is_hidden_pk_table_pk_increment_column) {
      column_flag_ = HIDDEN_PRIMARY_KEY_TABLE_PK_INCREMENT_COLUMN_FLAG;
    } else {
      column_flag_ = NORMAL_COLUMN_FLAG;
    }
    column_idx_ = static_cast<int16_t>(column_idx);
    meta_type_.set_meta(meta_type);
    accuracy_ = accuracy;
    collation_type_ =  collation_type;
    orig_default_value_str_ = orig_default_value_str;
    is_rowkey_ = column_table_schema.is_original_rowkey_column();
  }

  return ret;
}

int ColumnSchemaInfo::init(ColumnFlag column_flag)
{
  int ret = OB_SUCCESS;

  reset();
  column_flag_ = column_flag;

  return ret;
}

void ColumnSchemaInfo::destroy()
{
  reset();
}

void ColumnSchemaInfo::reset()
{
  column_flag_ = DELETE_COLUMN_FLAG;
  column_idx_ = 0;
  meta_type_.reset();
  accuracy_.reset();
  collation_type_ = ObCollationType::CS_TYPE_INVALID;

  if (NULL != orig_default_value_str_) {
    LOG_ERROR("orig_default_value_str_ should be null", K(orig_default_value_str_));
    orig_default_value_str_ = NULL;
  }

  extended_type_info_size_ = 0;
  extended_type_info_ = NULL;
  is_rowkey_ = false;
}

void ColumnSchemaInfo::get_extended_type_info(common::ObArrayHelper<common::ObString> &str_array) const
{
  str_array.reset();
  str_array.init(extended_type_info_size_, extended_type_info_, extended_type_info_size_);
}

int ColumnSchemaInfo::get_column_ori_default_value_(const share::schema::ObTableSchema &table_schema,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    const int64_t column_idx,
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
            column_table_schema.get_collation_type()))) {
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

int ColumnSchemaInfo::init_extended_type_info_(const share::schema::ObTableSchema &table_schema,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    const int64_t column_idx,
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
        ObString &str= extended_type_info_[idx];

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
      column_id_array_(NULL)
{
}

ObLogRowkeyInfo::~ObLogRowkeyInfo()
{
  destroy();
}

int ObLogRowkeyInfo::init(common::ObIAllocator &allocator,
    const int64_t size,
    const common::ObArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0)) {
    LOG_ERROR("invalid argument", K(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(size != column_ids.size())) {
    LOG_ERROR("size is not equal to column_ids size", K(size), "column_ids_size", column_ids.size());
    ret = OB_ERR_UNEXPECTED;
  } else {
    size_ = size;
    int64_t alloc_size = size * sizeof(column_id_array_[0]);

    if (OB_ISNULL(column_id_array_ = static_cast<uint64_t *>(allocator.alloc(alloc_size)))) {
      LOG_ERROR("allocate memory fail", K(column_id_array_), K(alloc_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t idx=0, cnt = size_; OB_SUCC(ret) && idx < cnt; idx++) {
        column_id_array_[idx] = column_ids[idx];
      }
    }
  }

  return ret;
}

void ObLogRowkeyInfo::destroy()
{
  if (NULL != column_id_array_) {
    LOG_ERROR("column_id_array_ should be null", K(column_id_array_));
    column_id_array_ = NULL;
  }
  size_ = 0;
}

void ObLogRowkeyInfo::release_mem(common::ObIAllocator &allocator)
{
  if (NULL != column_id_array_) {
    allocator.free(column_id_array_);
    column_id_array_ = NULL;
  }
}

bool ObLogRowkeyInfo::is_valid() const
{
  bool bool_ret = false;

  bool_ret = (NULL != column_id_array_ && size_ > 0);

  return bool_ret;
}

int ObLogRowkeyInfo::get_column_id(const int64_t index, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index < 0 || index >= size_)) {
    LOG_ERROR("invalid argument", K(index), K_(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (! is_valid()) {
    LOG_ERROR("ObLogRowkeyInfo is not valid", K(index), K_(size), KP_(column_id_array));
    ret = OB_NOT_INIT;
  } else {
    column_id = column_id_array_[index];
  }

  return ret;
}

int64_t ObLogRowkeyInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "{size=%ld, ", size_);

    if (NULL != column_id_array_) {
      (void)common::databuff_printf(buf, buf_len, pos, "column_id:[");

      for (int64_t idx=0; idx < size_ - 1; idx++) {
        (void)common::databuff_printf(buf, buf_len, pos, "%ld,", column_id_array_[idx]);
      }

      (void)common::databuff_printf(buf, buf_len, pos, "%ld]}", column_id_array_[size_ - 1]);
    }
  }

  return pos;
}

TableSchemaInfo::TableSchemaInfo(ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      is_hidden_pk_table_(false),
      rowkey_info_(),
      user_column_id_array_(NULL),
      user_column_id_array_cnt_(0),
      column_schema_array_(NULL),
      column_schema_array_cnt_(0)
{
}

TableSchemaInfo::~TableSchemaInfo()
{
  destroy();
}

int TableSchemaInfo::init(const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    LOG_ERROR("TableSchemaInfo has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // To avoid performance overhead here, do not rely on table schema traversal to get the number of columns that do not contain hidden columns,
    // externally set user_column_id_array_cnt_ to the number of hidden columns not included
    user_column_id_array_cnt_ = table_schema->get_column_count();
    column_schema_array_cnt_ = table_schema->get_max_used_column_id() - OB_APP_MIN_COLUMN_ID + 1;
    const bool is_hidden_pk_table = table_schema->is_no_pk_table();

    // No primary key table, record hidden primary key information at the end column_id=1, column_name="__pk_increment", reserved position
    if (is_hidden_pk_table) {
      ++user_column_id_array_cnt_;
      ++column_schema_array_cnt_;
    }

    // TODO fix me
    // There is a bad case where frequent addition and deletion of columns will lead to serious memory amplification, but online environments generally do not delete columns, so it is safe here

    if (OB_FAIL(init_rowkey_info_(table_schema))) {
      LOG_ERROR("init_rowkey_info_ fail", KR(ret), K(table_schema));
    } else if (OB_FAIL(init_user_column_id_array_(user_column_id_array_cnt_))) {
      LOG_ERROR("init_user_column_id_array_ fail", KR(ret), K(user_column_id_array_cnt_));
    } else if (OB_FAIL(init_column_schema_array_(column_schema_array_cnt_))) {
      LOG_ERROR("init_column_schema_array_ fail", KR(ret), K(column_schema_array_cnt_));
    } else {
      is_inited_ = true;
      is_hidden_pk_table_ = is_hidden_pk_table;

      LOG_INFO("table_schema_info init succ", "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(),
          K_(is_hidden_pk_table),
          "version", table_schema->get_schema_version(),
          "user_column_id_array_cnt", user_column_id_array_cnt_,
          "max_used_column_id", table_schema->get_max_used_column_id(),
          "column_schema_array_cnt", column_schema_array_cnt_);
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void TableSchemaInfo::destroy()
{
  is_inited_ = false;

  is_hidden_pk_table_ = false;
  rowkey_info_.release_mem(allocator_);

  destroy_user_column_id_array_();
  destroy_column_schema_array_();
}

int TableSchemaInfo::init_rowkey_info_(const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    ObArray<uint64_t> column_ids;

    if (OB_FAIL(rowkey_info.get_column_ids(column_ids))) {
      LOG_ERROR("rowkey info get_column_ids fail", KR(ret), K(column_ids));
    } else if (OB_FAIL(rowkey_info_.init(allocator_, rowkey_info.get_size(), column_ids))) {
      LOG_ERROR("rowkey info init fail", KR(ret), "size", rowkey_info.get_size(),
          K(column_ids));
    } else {
      // succ
    }
  }

  return ret;
}

int TableSchemaInfo::init_user_column_id_array_(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = cnt * sizeof(user_column_id_array_[0]);

  if (OB_UNLIKELY(cnt <= 0)) {
    LOG_ERROR("invalid argument", K(cnt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(user_column_id_array_ = static_cast<uint64_t *>(allocator_.alloc(alloc_size)))) {
    LOG_ERROR("allocate memory fail", K(user_column_id_array_), K(alloc_size), K(cnt));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t idx = 0; OB_SUCCESS == ret && idx < cnt; ++idx) {
      user_column_id_array_[idx] = OB_INVALID_ID;
    }
  }

  return ret;
}

void TableSchemaInfo::destroy_user_column_id_array_()
{
  if (NULL != user_column_id_array_) {
    allocator_.free(user_column_id_array_);
    user_column_id_array_ = NULL;
    user_column_id_array_cnt_ = 0;
  }
}

int TableSchemaInfo::init_column_schema_array_(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = cnt * sizeof(column_schema_array_[0]);

  if (OB_UNLIKELY(cnt <= 0)) {
    LOG_ERROR("invalid argument", K(cnt));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(column_schema_array_ = static_cast<ColumnSchemaInfo *>(allocator_.alloc(alloc_size)))) {
    LOG_ERROR("allocate memory fail", K(column_schema_array_), K(alloc_size), K(cnt));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t idx = 0; OB_SUCCESS == ret && idx < cnt; ++idx) {
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

int TableSchemaInfo::get_column_schema_info_(const uint64_t column_id,
    const bool enable_output_hidden_primary_key,
    ColumnSchemaInfo *&column_schema_info,
    bool &is_non_user_column,
    bool &is_hidden_pk_table_pk_increment_column) const
{
  int ret = OB_SUCCESS;
  column_schema_info = NULL;
  is_non_user_column = false;
  is_hidden_pk_table_pk_increment_column = false;

  if (OB_UNLIKELY(! is_inited_)) {
    LOG_ERROR("TableSchemaInfo has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    LOG_ERROR("invalid argument", K(column_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(filter_non_user_column(is_hidden_pk_table_, enable_output_hidden_primary_key,
          column_id, is_non_user_column, is_hidden_pk_table_pk_increment_column))) {
    LOG_ERROR("filter_non_user_column fail", KR(ret), K(column_id), K(enable_output_hidden_primary_key),
        K(is_non_user_column), K(is_hidden_pk_table_pk_increment_column));
  } else if (is_non_user_column) {
    // 1. filter out non-user columns directly
    // 2. if enable_output_hidden_primary_key=true, no primary key table hidden primary key support fetch, no filtering here
    // No logs are printed here, external calls are printed
  } else {
    int64_t column_schema_array_idx = -1;

    if (is_hidden_pk_table_pk_increment_column) {
      column_schema_array_idx = column_schema_array_cnt_ - 1;
    } else {
      column_schema_array_idx = column_id - OB_APP_MIN_COLUMN_ID;
    }

    if (OB_UNLIKELY(column_schema_array_idx < 0 || column_schema_array_idx >= column_schema_array_cnt_)) {
      LOG_ERROR("invalid column_schema_array_idx", K(column_id), K(column_schema_array_idx),
          K(column_schema_array_cnt_));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(column_schema_info = &column_schema_array_[column_schema_array_idx])) {
      LOG_ERROR("column_schema_info is null", K(column_id), K(column_schema_info));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // succ
    }
  }

  return ret;
}

int TableSchemaInfo::init_column_schema_info(const share::schema::ObTableSchema &table_schema,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    const int64_t column_idx,
    const bool enable_output_hidden_primary_key,
    ObObj2strHelper &obj2str_helper)
{
  int ret = OB_SUCCESS;
  const uint64_t column_id = column_table_schema.get_column_id();
  const char *column_name = column_table_schema.get_column_name();
  const bool is_hidden_column = column_table_schema.is_hidden();
  const bool enable_output_invisible_column = TCONF.enable_output_invisible_column;
  const bool is_invisible_column = column_table_schema.is_invisible_column();
  ColumnSchemaInfo *column_schema_info = NULL;
  bool is_non_user_column = false;
  bool is_hidden_pk_table_pk_increment_column = false;

  if (OB_UNLIKELY(! is_inited_)) {
    LOG_ERROR("TableSchemaInfo has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(column_idx < 0)) {
    LOG_ERROR("invalid argument", K(column_idx));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_column_schema_info_(column_id, enable_output_hidden_primary_key,
          column_schema_info, is_non_user_column, is_hidden_pk_table_pk_increment_column))) {
    LOG_ERROR("get_column_schema_info_ fail", KR(ret), K(column_id), K(enable_output_hidden_primary_key),
        KPC(column_schema_info), K(is_non_user_column), K(is_hidden_pk_table_pk_increment_column));
  } else if (is_non_user_column) {
    SCHEMA_STAT_INFO("ignore non user column", K(is_non_user_column),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        K(column_idx), K(column_id));
  } else if (OB_ISNULL(column_schema_info)) {
    LOG_ERROR("column_schema_info is null", K(column_id), K(column_schema_info));
    ret = OB_ERR_UNEXPECTED;
  } else if (is_hidden_column && ! is_hidden_pk_table_pk_increment_column) {
    // Hidden columns do not record columnID, columnIdx, meta_type, accuracy information
    // Note: Here the initialization of the hidden column [not the hidden primary key of a non-primary table] is completed
    if (OB_FAIL(column_schema_info->init(ColumnFlag::HIDDEN_COLUMN_FLAG))) {
      LOG_ERROR("column_schema_info init fail", KR(ret),
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          "version", table_schema.get_schema_version(),
          K(column_id), K(column_name), K(is_hidden_column),
          K(is_hidden_pk_table_pk_increment_column),
          K(column_idx));
    }
    LOG_INFO("column_schema_info init hidden column",
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        K(column_id), K(column_name), K(is_hidden_column),
        K(is_hidden_pk_table_pk_increment_column),
        K(column_idx));
  } else if (is_invisible_column && ! enable_output_invisible_column) {
    // invisible columns do not record columnID, columnIdx, meta_type, accuracy information
    if (OB_FAIL(column_schema_info->init(ColumnFlag::OBLOG_INVISIBLE_COLUMN_FLAG))) {
      LOG_ERROR("column_schema_info init fail", KR(ret), K(enable_output_invisible_column),
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          "version", table_schema.get_schema_version(),
          K(column_id), K(column_name), K(is_invisible_column),
          K(column_idx));
    } else {
      LOG_INFO("column_schema_info init invisible column", K(enable_output_invisible_column),
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          "version", table_schema.get_schema_version(),
          K(column_id), K(column_name), K(is_invisible_column),
          K(column_idx));
    }
  } else {
    // Non-hidden columns set column_id and initialize
    if (OB_FAIL(set_column_id(column_idx, column_id))) {
      LOG_ERROR("set_column_id fail", KR(ret), K(column_idx), K(column_id));
    } else if (OB_FAIL(column_schema_info->init(table_schema, column_table_schema, column_idx,
            obj2str_helper, get_allocator(), is_hidden_pk_table_pk_increment_column))) {
      LOG_ERROR("column_schema_info init fail", KR(ret),
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          "version", table_schema.get_schema_version(),
          K(column_idx), K(is_hidden_pk_table_pk_increment_column),
          "meta_type", column_table_schema.get_meta_type(),
          "accuracy", column_table_schema.get_accuracy());
    } else {
      // succ
    }
  }

  return ret;
}

int TableSchemaInfo::get_column_schema_info(const uint64_t column_id,
    const bool enable_output_hidden_primary_key,
    ColumnSchemaInfo *&column_schema_info,
    ColumnPropertyFlag &column_property_flag) const
{
  int ret = OB_SUCCESS;
  column_schema_info = NULL;
  column_property_flag.reset();
  bool is_non_user_column = false;
  bool is_hidden_pk_table_pk_increment_column = false;
  bool is_hidden_column = false;
  bool is_delete_column = false;
  bool is_invisible_column = false;

  // range of user columns:
  // OB_APP_MIN_COLUMN_ID: 16
  // OB_MIN_SHADOW_COLUMN_ID: 32767
  if (OB_UNLIKELY(! is_inited_)) {
    LOG_ERROR("TableSchemaInfo has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    LOG_ERROR("invalid argument", K(column_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_column_schema_info_(column_id, enable_output_hidden_primary_key, column_schema_info,
          is_non_user_column, is_hidden_pk_table_pk_increment_column))) {
    LOG_ERROR("get_column_schema_info_ fail", KR(ret), K(column_id),
        K(enable_output_hidden_primary_key), KPC(column_schema_info),
        K(is_non_user_column), K(is_hidden_pk_table_pk_increment_column));
  } else if (is_non_user_column) {
    // do nothing
  } else if (OB_ISNULL(column_schema_info)) {
    LOG_ERROR("column_schema_info is null", K(column_id), K(column_schema_info));
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (column_schema_info->is_hidden()) {
      is_hidden_column = true;
    }

    if (column_schema_info->is_delete()) {
      is_delete_column = true;
    }

    if (column_schema_info->is_invisible()) {
      is_invisible_column = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_non_user_column || is_hidden_column || is_delete_column || is_invisible_column) {
      column_schema_info = NULL;
    }
    column_property_flag.reset(is_non_user_column, is_hidden_column, is_delete_column, is_invisible_column);
  }

  return ret;
}

int TableSchemaInfo::get_column_id(const int64_t index, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  column_id = OB_INVALID_ID;

  if (OB_UNLIKELY(! is_inited_)) {
    LOG_ERROR("TableSchemaInfo has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(index < 0 || index >= user_column_id_array_cnt_)) {
    LOG_ERROR("invalid argument", K(index), K_(user_column_id_array_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    column_id = user_column_id_array_[index];
  }

  return ret;
}

int TableSchemaInfo::set_column_id(const int64_t index, const uint64_t column_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_inited_)) {
    LOG_ERROR("TableSchemaInfo has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(index < 0 || index >= user_column_id_array_cnt_)) {
    LOG_ERROR("invalid argument", K(index), K_(user_column_id_array_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    user_column_id_array_[index] = column_id;
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
