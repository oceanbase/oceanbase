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

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_schema.h"
#include "storage/column_store/ob_column_store_replica_util.h"

namespace oceanbase
{

using namespace common;
using namespace share::schema;

namespace storage
{

/*
 * ObStorageRowkeyColumnSchema
 * */
ObStorageRowkeyColumnSchema::ObStorageRowkeyColumnSchema()
{
  reset();
}

ObStorageRowkeyColumnSchema::~ObStorageRowkeyColumnSchema()
{
}

void ObStorageRowkeyColumnSchema::reset()
{
  info_ = 0;
  column_idx_ = 0;
  meta_type_.reset();
}

bool ObStorageRowkeyColumnSchema::is_valid() const
{
  return 0 != column_idx_
      && common::ob_is_valid_obj_type(static_cast<ObObjType>(meta_type_.get_type()));
}

OB_SERIALIZE_MEMBER_SIMPLE(
    ObStorageRowkeyColumnSchema,
    info_,
    column_idx_,
    meta_type_);

/*
 * ObStorageColumnSchema
 * */

ObStorageColumnSchema::ObStorageColumnSchema()
{
  reset();
}

ObStorageColumnSchema::~ObStorageColumnSchema()
{
}

void ObStorageColumnSchema::reset()
{
  info_ = 0;
  default_checksum_ = 0;
  meta_type_.reset();
  orig_default_value_.reset();
}

void ObStorageColumnSchema::destroy(ObIAllocator &allocator)
{
  if (orig_default_value_.get_deep_copy_size() > 0) {
    void *ptr = orig_default_value_.get_deep_copy_obj_ptr();
    if (ptr != nullptr) {
      orig_default_value_.reset();
      allocator.free(ptr);
    }
  }
  reset();
}

bool ObStorageColumnSchema::is_valid() const
{
  return common::ob_is_valid_obj_type(static_cast<ObObjType>(meta_type_.get_type()));
}

int ObStorageColumnSchema::construct_column_param(share::schema::ObColumnParam &column_param) const
{
  int ret = OB_SUCCESS;
  column_param.set_meta_type(meta_type_);
  if (orig_default_value_.is_fixed_len_char_type()) {
    blocksstable::ObStorageDatum datum;
    ObObj obj;
    if (OB_FAIL(datum.from_obj(orig_default_value_))) {
      STORAGE_LOG(WARN, "fail to covent datum from obj", K(ret), K(orig_default_value_));
    } else {
      if (OB_FAIL(ObStorageSchema::trim(orig_default_value_.get_collation_type(), datum))) {
        STORAGE_LOG(WARN, "failed to trim datum", K(ret), K_(orig_default_value), K(datum));
      } else if (OB_FAIL(datum.to_obj_enhance(obj, orig_default_value_.get_meta()))) {
        STORAGE_LOG(WARN, "failed to transfer datum to obj", K(ret), K(datum));
      } else if (OB_FAIL(column_param.set_orig_default_value(obj))) {
        STORAGE_LOG(WARN, "fail to set orig default value", K(ret));
      }
    }
  } else if (OB_FAIL(column_param.set_orig_default_value(orig_default_value_))) {
     STORAGE_LOG(WARN, "fail to set orig default value", K(ret));
  }
  return ret;
}

int ObStorageColumnSchema::deep_copy_default_val(ObIAllocator &allocator, const ObObj &default_val)
{
  int ret = OB_SUCCESS;
  if (default_val.get_deep_copy_size() > 0) {
    char *buf = nullptr;
    int64_t pos = 0;
    const int64_t alloc_size = default_val.get_deep_copy_size();
    if (OB_ISNULL(buf = (char *)allocator.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(alloc_size));
    } else if (OB_FAIL(orig_default_value_.deep_copy(default_val, buf, alloc_size, pos))) {
      orig_default_value_.reset();
      allocator.free(buf);
      buf = nullptr;
      STORAGE_LOG(WARN, "failed to deep copy", K(ret), K(default_val), K(pos));
    }
  } else {
    orig_default_value_ = default_val;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObStorageColumnSchema,
    info_,
    default_checksum_,
    meta_type_,
    orig_default_value_);

int ObStorageColumnSchema::legacy_deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  // For schema version before 4_2_0_0
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      info_,
      default_checksum_,
      meta_type_,
      orig_default_value_);
  return ret;
}

int ObStorageColumnSchema::legacy_serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  // For schema version before 4_2_0_0
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      info_,
      default_checksum_,
      meta_type_,
      orig_default_value_);
  return ret;
}

int64_t ObStorageColumnSchema::legacy_serialize_len() const
{
  // For schema version before 4_2_0_0
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      info_,
      default_checksum_,
      meta_type_,
      orig_default_value_);
  return len;
}

/*
 * ObStorageColumnGroupSchema
 */

void ObStorageColumnGroupSchema::destroy(ObIAllocator &allocator)
{
  if (nullptr != column_idxs_) {
    allocator.free(column_idxs_);
  }
  reset();
}
int ObStorageColumnGroupSchema::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected column group schema to serialize", K(ret), K_(version), K_(type), K_(column_cnt));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        version_,
        type_,
        compressor_type_,
        row_store_type_,
        block_size_,
        schema_column_cnt_,
        rowkey_column_cnt_,
        schema_rowkey_column_cnt_);
    if (is_all_column_group()) {
      OB_UNIS_ENCODE(column_cnt_);
    } else {
      OB_UNIS_ENCODE_ARRAY(column_idxs_, column_cnt_);
    }
  }
  return ret;
}

int ObStorageColumnGroupSchema::deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
      version_,
      type_,
      compressor_type_,
      row_store_type_,
      block_size_,
      schema_column_cnt_,
      rowkey_column_cnt_,
      schema_rowkey_column_cnt_,
      column_cnt_);
  if (OB_SUCC(ret)) {
    if (column_cnt_ > 0) {
      if (is_all_column_group()) {
        column_idxs_ = nullptr;
      } else if (OB_ISNULL(column_idxs_ = reinterpret_cast<uint16_t *> (allocator.alloc(sizeof(uint16_t) * column_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(column_cnt_));
      } else {
        OB_UNIS_DECODE_ARRAY(column_idxs_, column_cnt_);
        if (OB_FAIL(ret)) {
          allocator.free(column_idxs_);
          reset();
        }
      }
    } else {
      column_idxs_ = nullptr;
    }
  }

  return ret;
}

int64_t ObStorageColumnGroupSchema::get_serialize_size() const
{
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "Unexpected column group schema to serialize", K_(version), K_(type), K_(column_cnt));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        version_,
        type_,
        compressor_type_,
        row_store_type_,
        block_size_,
        schema_column_cnt_,
        rowkey_column_cnt_,
        schema_rowkey_column_cnt_);
    if (is_all_column_group()) {
      OB_UNIS_ADD_LEN(column_cnt_);
    } else {
      OB_UNIS_ADD_LEN_ARRAY(column_idxs_, column_cnt_);
    }
  }
  return len;
}

int ObStorageColumnGroupSchema::deep_copy(const ObStorageColumnGroupSchema &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  reset();
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy column group schema", K(ret), K(other));
  } else {
    version_ = COLUMN_GRUOP_SCHEMA_VERSION;
    type_ = other.type_;
    compressor_type_ = other.compressor_type_;
    row_store_type_ = other.row_store_type_;
    block_size_ = other.block_size_;
    schema_column_cnt_ = other.schema_column_cnt_;
    rowkey_column_cnt_ = other.rowkey_column_cnt_;
    schema_rowkey_column_cnt_ = other.schema_rowkey_column_cnt_;
    column_cnt_ = other.column_cnt_;
    if (column_cnt_ == 0 ||is_all_column_group()) {
      column_idxs_ = nullptr;
    } else if (OB_ISNULL(column_idxs_ = reinterpret_cast<uint16_t *> (allocator.alloc(sizeof(uint16_t) * column_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(column_cnt_));
    } else {
      MEMCPY(column_idxs_, other.column_idxs_, sizeof(uint16_t) * column_cnt_);
    }
  }

  return ret;
}

int ObStorageColumnGroupSchema::copy_from(ObIArray<ObColDesc> &column_ids,
                                          const int64_t schema_rowkey_cnt,
                                          const ObColumnGroupSchema &cg_schema,
                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!cg_schema.is_valid() || column_ids.empty() || schema_rowkey_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to copy column group schema", K(ret), K(cg_schema), K(column_ids), K(schema_rowkey_cnt));
  } else if (OB_UNLIKELY(is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for copy to valid cg schema", K(ret), K(*this), K(cg_schema));
  } else if (OB_UNLIKELY(cg_schema.get_column_id_count() > column_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for cg schema", K(ret), K(column_ids), K(cg_schema));
  } else if (share::schema::SINGLE_COLUMN_GROUP == cg_schema.get_column_group_type() && cg_schema.get_column_id_count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get unexpected single cg schema", K(ret), K(cg_schema));
  } else {
    const bool is_normal_cg_schema = cg_schema.is_normal_column_group();
    version_ = COLUMN_GRUOP_SCHEMA_VERSION;
    block_size_ = cg_schema.get_block_size();
    type_ = cg_schema.get_column_group_type();
    compressor_type_ = cg_schema.get_compressor_type();
    row_store_type_ = cg_schema.get_row_store_type();
    schema_column_cnt_ = cg_schema.get_column_id_count();
    schema_rowkey_column_cnt_ = is_normal_cg_schema ? 0 : schema_rowkey_cnt;
    rowkey_column_cnt_ = has_multi_version_column() ? schema_rowkey_column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()
                                                        : schema_rowkey_column_cnt_;
    column_cnt_ = has_multi_version_column() ? schema_column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()
                                                : schema_column_cnt_;
    const uint64_t *column_ids_arr = nullptr;
    const int64_t mv_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (is_all_column_group()) {
      column_idxs_ = nullptr;
    } else if (column_cnt_ == 0) {
      if (!is_default_column_group()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected column group schma", K(ret), K(cg_schema));
      } else {
        column_idxs_ = nullptr;
      }
    } else if (OB_UNLIKELY(is_rowkey_column_group() && schema_column_cnt_ != schema_rowkey_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected column cnt for rowkey cg", K(ret));
    } else if (OB_ISNULL(column_idxs_ = reinterpret_cast<uint16_t *> (allocator.alloc(sizeof(uint16_t) * column_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(column_cnt_));
    } else if (OB_ISNULL(column_ids_arr = cg_schema.get_column_ids())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error for null column ids", K(ret), K(cg_schema));
    } else if (is_rowkey_column_group()) {
      //generate storage rowkey column group
      for (int64_t i = 0; i < schema_rowkey_cnt + mv_rowkey_cnt; ++i) {
        column_idxs_[i] = i;
      }
    } else { // deal with normal cg
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
        column_idxs_[i] = UINT16_MAX;
        bool find_column_id = false;
        for (int64_t j = 0; j < column_ids.count(); j++) {
          if (column_ids_arr[i] == column_ids.at(j).col_id_) {
            column_idxs_[i] = j + (j >= schema_rowkey_cnt ? mv_rowkey_cnt : 0);
            find_column_id = true;
            break;
          }
        }
        if (OB_UNLIKELY(!find_column_id)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column id in schema",
              K(ret), K(i), K(column_ids_arr[i]), K(column_ids), K(cg_schema), K(i), K(column_ids_arr[i]));
        }
      }
    }
    if (OB_FAIL(ret)) {
      destroy(allocator);
    }
  }

  return ret;
}

/*
 * ObUpdateCSReplicaSchemaParam
 * */
ObUpdateCSReplicaSchemaParam::ObUpdateCSReplicaSchemaParam()
  : tablet_id_(),
    major_column_cnt_(0),
    update_type_(UpdateType::MAX_TYPE),
    is_inited_(false)
{

}

ObUpdateCSReplicaSchemaParam::~ObUpdateCSReplicaSchemaParam()
{

}

int ObUpdateCSReplicaSchemaParam::init(
    const ObTabletID &tablet_id,
    const int64_t major_column_cnt,
    const UpdateType update_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUpdateCSReplicaSchemaParam has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || update_type >= UpdateType::MAX_TYPE
             || major_column_cnt <= ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tablet_id), K(major_column_cnt), K(update_type));
  } else {
    tablet_id_ = tablet_id;
    major_column_cnt_ = major_column_cnt;
    update_type_ = update_type;
    is_inited_ = true;
  }
  return ret;
}

bool ObUpdateCSReplicaSchemaParam::is_valid() const
{
  return is_inited_
      && tablet_id_.is_valid()
      && major_column_cnt_ > ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()
      && update_type_ >= UpdateType::REFRESH_TABLE_SCHEMA
      && update_type_ < UpdateType::MAX_TYPE;
}

/*
 * ObStorageSchema
 * */

ObStorageSchema::ObStorageSchema()
  : allocator_(nullptr),
    storage_schema_version_(0),
    info_(0),
    table_type_(ObTableType::MAX_TABLE_TYPE),
    table_mode_(),
    index_type_(ObIndexType::INDEX_TYPE_IS_NOT),
    row_store_type_(ObStoreFormat::get_default_row_store_type()),
    schema_version_(OB_INVALID_VERSION),
    column_cnt_(0),
    tablet_size_(OB_DEFAULT_TABLET_SIZE),
    pctfree_(OB_DEFAULT_PCTFREE),
    block_size_(0),
    progressive_merge_round_(0),
    progressive_merge_num_(0),
    master_key_id_(INVALID_ID),
    compressor_type_(ObCompressorType::NONE_COMPRESSOR),
    encryption_(),
    encrypt_key_(),
    rowkey_array_(),
    column_array_(),
    column_group_array_(),
    skip_idx_attr_array_(),
    store_column_cnt_(0),
    has_all_column_group_(false),
    mv_mode_(),
    is_inited_(false)
{
}

ObStorageSchema::~ObStorageSchema()
{
  reset();
}

// move storage_schema_version calculation here
int ObStorageSchema::set_storage_schema_version(const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;
  if (tenant_data_version < DATA_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "tenant data version not supported", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_0_0) {
    storage_schema_version_ = STORAGE_SCHEMA_VERSION_V2;
  } else if (tenant_data_version < DATA_VERSION_4_3_4_0) {
    storage_schema_version_ = STORAGE_SCHEMA_VERSION_V3;
  } else {
    storage_schema_version_ = STORAGE_SCHEMA_VERSION_LATEST;
  }
  return ret;
}

int ObStorageSchema::init(
    common::ObIAllocator &allocator,
    const ObTableSchema &input_schema,
    const lib::Worker::CompatMode compat_mode,
    const bool skip_column_info/* = false*/,
    const uint64_t tenant_data_version/* = DATA_CURRENT_VERSION */,
    const bool generate_cs_replica_cg_array/* = false*/)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!input_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(input_schema), K(skip_column_info));
  } else if (OB_FAIL(copy_from(input_schema))) {
    STORAGE_LOG(WARN, "failed to copy from table schema", K(ret), K(input_schema));
  } else if (FALSE_IT(column_info_simplified_ = skip_column_info)) {
  } else if (OB_UNLIKELY(generate_cs_replica_cg_array && column_info_simplified_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to init storage schema", K(ret));
  } else {
    allocator_ = &allocator;
    rowkey_array_.set_allocator(&allocator);
    column_array_.set_allocator(&allocator);
    column_group_array_.set_allocator(&allocator);
    skip_idx_attr_array_.set_allocator(&allocator);
    if (OB_FAIL(set_storage_schema_version(tenant_data_version))) {
      STORAGE_LOG(WARN, "cal storage schema version failed, not suppert this tenant data version", K(ret), K(tenant_data_version));
    }
    compat_mode_ = static_cast<uint32_t>(compat_mode);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_str(input_schema))) {
    STORAGE_LOG(WARN, "failed to generate string", K(ret), K(input_schema));
  } else if (OB_FAIL(generate_column_array(input_schema))) {
    STORAGE_LOG(WARN, "failed to generate column array", K(ret), K(input_schema));
  } else if (generate_cs_replica_cg_array) {
    if (OB_FAIL(ObStorageSchema::generate_cs_replica_cg_array())) {
      STORAGE_LOG(WARN, "failed to generate_cs_replica_cg_array", K(ret));
    }
  } else if (OB_FAIL(generate_column_group_array(input_schema, allocator))) {
    STORAGE_LOG(WARN, "Failed to generate column group array", K(ret));
  }

  bool is_column_table_schema = false;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!ObStorageSchema::is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "storage schema is invalid", K(ret));
  } else if (OB_FAIL(input_schema.get_is_column_store(is_column_table_schema))) {
    STORAGE_LOG(WARN, "fail to check is column store", K(ret));
  } else {
    is_column_table_schema_ = is_column_table_schema;
    is_cs_replica_compat_ = is_cg_array_generated_in_cs_replica();
    enable_macro_block_bloom_filter_ = input_schema.get_enable_macro_block_bloom_filter();
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObStorageSchema::init(
    common::ObIAllocator &allocator,
    const ObStorageSchema &old_schema,
    const bool skip_column_info/* = false*/,
    const ObStorageSchema *column_group_schema/* = nullptr*/,
    const bool generate_cs_replica_cg_array/* = false*/,
    const ObUpdateCSReplicaSchemaParam *update_param/* = nullptr*/)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!old_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(old_schema), K(skip_column_info));
  } else if (OB_FAIL(copy_from(old_schema))) {
    STORAGE_LOG(WARN, "failed to copy from old schema", K(ret), K(old_schema));
  } else if (FALSE_IT(column_info_simplified_ = (skip_column_info || old_schema.column_info_simplified_))) {
  } else if (OB_UNLIKELY(generate_cs_replica_cg_array && column_group_schema != nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args to init storage schema", K(ret), KPC(column_group_schema));
  } else if (nullptr != update_param) {
    if (OB_UNLIKELY(!update_param->is_valid() || nullptr != column_group_schema)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), K(update_param), KPC(column_group_schema));
    } else if (OB_UNLIKELY(update_param->need_refresh_schema() && !column_info_simplified_
                        || update_param->need_truncate_column_array() && column_info_simplified_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), K(update_param), K(skip_column_info), K(old_schema));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    allocator_ = &allocator;
    rowkey_array_.set_allocator(&allocator);
    column_array_.set_allocator(&allocator);
    column_group_array_.set_allocator(&allocator);
    skip_idx_attr_array_.set_allocator(&allocator);

    storage_schema_version_ = old_schema.storage_schema_version_;
    compat_mode_ = old_schema.compat_mode_;
    compressor_type_ = old_schema.compressor_type_;
    column_cnt_ = old_schema.column_cnt_;
    store_column_cnt_ = old_schema.store_column_cnt_;

    if (OB_FAIL(deep_copy_str(old_schema.encryption_, encryption_))) {
      STORAGE_LOG(WARN, "failed to deep copy encryption", K(ret), K(old_schema));
    } else if (OB_FAIL(deep_copy_str(old_schema.encrypt_key_, encrypt_key_))) {
      STORAGE_LOG(WARN, "failed to deep copy encryption key", K(ret), K(old_schema));
    } else if (OB_FAIL(rowkey_array_.reserve(old_schema.rowkey_array_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve for rowkey array", K(ret), K(old_schema));
    } else if (OB_FAIL(rowkey_array_.assign(old_schema.rowkey_array_))) {
      STORAGE_LOG(WARN, "failed to copy row key array", K(ret), K(old_schema));
    } else if (OB_FAIL(skip_idx_attr_array_.reserve(old_schema.skip_idx_attr_array_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve for skip idx attr array", K(ret), K(old_schema));
    } else if (OB_FAIL(skip_idx_attr_array_.assign(old_schema.skip_idx_attr_array_))) {
      STORAGE_LOG(WARN, "failed to copy skip idx attr array", K(ret), K(old_schema));
    } else if (!column_info_simplified_ && OB_FAIL(deep_copy_column_array(allocator, old_schema, old_schema.column_array_.count()))) {
      STORAGE_LOG(WARN, "failed to deep copy column array", K(ret), K(old_schema));
    } else if (!column_info_simplified_ && generate_cs_replica_cg_array) {
      // skip deep copy if column group schema array need generated from column array
    } else if (NULL != column_group_schema && OB_FAIL(deep_copy_column_group_array(allocator, *column_group_schema))) {
      STORAGE_LOG(WARN, "failed to deep copy column array from column group schema", K(ret), K(old_schema), KPC(column_group_schema));
    } else if (NULL == column_group_schema && OB_FAIL(deep_copy_column_group_array(allocator, old_schema))) {
      STORAGE_LOG(WARN, "failed to deep copy column array", K(ret), K(old_schema));
    }

    if (OB_FAIL(ret)) {
    } else if (nullptr != update_param && OB_FAIL(refactor_storage_schema(allocator, old_schema, *update_param))) {
      STORAGE_LOG(WARN, "failed to rebuild column array", K(ret), K(old_schema), KPC(update_param));
    } else if (!column_info_simplified_ && generate_cs_replica_cg_array && OB_FAIL(ObStorageSchema::generate_cs_replica_cg_array())) {
      STORAGE_LOG(WARN, "failed to generate_cs_replica_cg_array", K(ret));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "storage schema is invalid", K(ret));
    } else {
      is_column_table_schema_ = old_schema.is_column_table_schema_;
      is_cs_replica_compat_ = is_cg_array_generated_in_cs_replica();
      enable_macro_block_bloom_filter_ = old_schema.get_enable_macro_block_bloom_filter();
      is_inited_ = true;
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObStorageSchema::deep_copy_column_array(
    common::ObIAllocator &allocator,
    const ObStorageSchema &src_schema,
    const int64_t copy_array_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(copy_array_cnt <= 0 || copy_array_cnt > src_schema.column_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(copy_array_cnt), K(src_schema.column_array_));
  } else if (OB_FAIL(column_array_.reserve(copy_array_cnt))) {
    STORAGE_LOG(WARN, "failed to reserve for column array", K(ret), K(src_schema));
  }
  for (int i = 0; OB_SUCC(ret) && i < copy_array_cnt; ++i) {
    ObStorageColumnSchema col_schema;
    const ObStorageColumnSchema &src_col_schema = src_schema.column_array_.at(i);
    col_schema.info_ = src_col_schema.info_;
    col_schema.default_checksum_ = src_col_schema.default_checksum_;
    col_schema.meta_type_ = src_col_schema.meta_type_;
    if (OB_FAIL(col_schema.deep_copy_default_val(allocator, src_col_schema.orig_default_value_))) {
      STORAGE_LOG(WARN, "failed to deep copy col schema", K(ret), K(i), K(src_col_schema));
    } else if (OB_FAIL(column_array_.push_back(col_schema))) {
      STORAGE_LOG(WARN, "failed to push back col schema", K(ret), K(i), K(copy_array_cnt),
          K(src_schema.column_array_.count()), K(col_schema));
      col_schema.destroy(allocator);
    }
  }
  return ret;
}

int ObStorageSchema::refactor_storage_schema(
    common::ObIAllocator &allocator,
    const ObStorageSchema &src_schema,
    const ObUpdateCSReplicaSchemaParam &update_param)
{
  int ret = OB_SUCCESS;
  int64_t expected_stored_column_cnt = 0;
  if (OB_UNLIKELY(!update_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(update_param));
  } else if (FALSE_IT(expected_stored_column_cnt = update_param.major_column_cnt_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
  } else if (update_param.need_refresh_schema()) {
    if (OB_UNLIKELY(!src_schema.is_column_info_simplified())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "src schema is not simplified", K(ret), K(src_schema));
    } else if (OB_FAIL(ObCSReplicaUtil::get_full_column_array_from_table_schema(allocator, update_param, src_schema, column_array_))) {
      STORAGE_LOG(WARN, "failed to get full column array from table schema", K(ret), K(update_param));
    } else {
      column_info_simplified_ = false;
      column_cnt_ = column_array_.count();
      store_column_cnt_ = expected_stored_column_cnt;
      STORAGE_LOG(INFO, "rebuild column array from table schema", K(ret), K(update_param), K(src_schema), K_(column_array));
    }
  } else if (update_param.need_truncate_column_array()) {
    const int64_t original_column_cnt = column_array_.count();
    int64_t stored_column_cnt = 0;
    bool finish_truncate = false;
    for (int64_t i = 0; i < original_column_cnt; ++i) {
      if (column_array_.at(i).is_column_stored_in_sstable()) {
        stored_column_cnt++;
        if (stored_column_cnt == expected_stored_column_cnt) {
          for (int64_t j = i + 1; j < original_column_cnt; ++j) {
            column_array_.pop_back();
          }
          finish_truncate = true;
          column_cnt_ = column_array_.count();
          store_column_cnt_ = expected_stored_column_cnt;
          STORAGE_LOG(INFO, "truncate column array for convert co merge", K(ret), K(update_param), K_(column_array));
          break;
        }
      }
    }
    if (!finish_truncate) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to truncate column array", K(ret), K(update_param), K_(column_array));
    } else {
      if (!skip_idx_attr_array_.empty()) {
        /*
         * Remain the skip indexes on old columns(we can not decide whether the skip index is added before or after lastest major compacted).
         * Since adding primary key is offline ddl, we only consider online adding column with skip index here.
         * New column is added to the end of column array, and the skip index array is asc with col_idx(begin with 0),
         * so the skip index with col_idx_ >= column_cnt_ means the it is added after the latest major compacted.
         */
        const int64_t original_skip_idx_count = skip_idx_attr_array_.count();
        for (int64_t i = 0; i < original_skip_idx_count; ++i) {
          if (skip_idx_attr_array_.at(i).col_idx_ >= column_cnt_) {
            for (int64_t j = i; j < original_skip_idx_count; ++j) {
              skip_idx_attr_array_.pop_back();
            }
            STORAGE_LOG(INFO, "finish truncate skip idx array", K(ret), K(update_param), K_(skip_idx_attr_array));
            break;
          }
        }
      }
      if (!column_group_array_.empty()) {
        /*
         * Used for column store tablet split in the future.
         * Similar to truncate skip_idx_attr_array_, the cg of newly added column is added at the end.
         * The column idx calculation takes 2 multi-version column, so use update_param.major_column_cnt_ to truncate.
         */
        const int64_t original_cg_array_count = column_group_array_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < original_cg_array_count; ++i) {
          const ObStorageColumnGroupSchema &column_group = column_group_array_.at(i);
          if (column_group.is_single_column_group()) {
            if (OB_UNLIKELY(column_group.get_column_count() <= 0)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "invalid column group schema", K(ret), K(column_group));
            } else if (column_group.get_column_idx(0) >= update_param.major_column_cnt_) {
              for (int64_t j = i; j < original_cg_array_count; ++j) {
                column_group_array_.pop_back();
              }
              STORAGE_LOG(INFO, "finish truncate cg array", K(ret), K(update_param), K_(column_group_array));
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageSchema::deep_copy_column_group_array(common::ObIAllocator &allocator, const ObStorageSchema &src_schema)
{
  int ret = OB_SUCCESS;

  if (src_schema.get_column_groups().empty()) {
    ObStorageColumnGroupSchema column_group;
    if (OB_FAIL(generate_all_column_group_schema(column_group, row_store_type_))) {
      STORAGE_LOG(WARN, "Failed to mock column group for compat", K(ret));
    } else if (OB_FAIL(column_group_array_.reserve(1))) {
      STORAGE_LOG(WARN, "failed to reserve for column group array", K(ret), K(src_schema));
    } else if (OB_FAIL(add_column_group(column_group))) {
      STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
    }
  } else if (OB_FAIL(column_group_array_.reserve(src_schema.column_group_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve for column group array", K(ret), K(src_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.column_group_array_.count(); i++) {
      ObStorageColumnGroupSchema column_group_schema;
      if (OB_FAIL(column_group_schema.deep_copy(src_schema.column_group_array_.at(i), allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy column group schema", K(ret));
      } else if (OB_FAIL(add_column_group(column_group_schema))) {
        STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group_schema));
        column_group_schema.destroy(allocator);
      }
    }
  }
  return ret;
}

int ObStorageSchema::add_column_group(const ObStorageColumnGroupSchema &column_group)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(column_group_array_.push_back(column_group))) {
    STORAGE_LOG(WARN, "Failed to push back column group", K(ret), K(column_group));
  } else if (column_group.is_all_column_group()) {
    has_all_column_group_ = true;
  }
  return ret;
}

void ObStorageSchema::reset()
{
  storage_schema_version_ = 0;
  info_ = 0;
  table_type_ = MAX_TABLE_TYPE;
  table_mode_.reset();
  mv_mode_.reset();
  index_type_ = INDEX_TYPE_IS_NOT;
  row_store_type_ = ObStoreFormat::get_default_row_store_type();
  schema_version_ = OB_INVALID_VERSION;
  column_cnt_ = 0;
  store_column_cnt_ = 0;
  tablet_size_ = OB_DEFAULT_TABLET_SIZE;
  pctfree_ = OB_DEFAULT_PCTFREE;
  block_size_ = 0;
  progressive_merge_round_ = 0;
  progressive_merge_num_ = 0;
  master_key_id_ = INVALID_ID;
  compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  if (nullptr != allocator_) {
    reset_string(encryption_);
    reset_string(encrypt_key_);

    rowkey_array_.reset();
    for (int i = 0; i < column_array_.count(); ++i) {
      column_array_.at(i).destroy(*allocator_);
    }
    column_array_.reset();
    (void) reset_column_group_array();
    skip_idx_attr_array_.reset();
    has_all_column_group_ = false;
    allocator_ = nullptr;
  }
  is_inited_ = false;
}

void ObStorageSchema::reset_column_group_array()
{
  for (int i = 0; i < column_group_array_.count(); ++i) {
    column_group_array_.at(i).destroy(*allocator_);
  }
  column_group_array_.reset();
}

bool ObStorageSchema::is_valid() const
{
  bool valid_ret = true;
  if (nullptr == allocator_
      || schema_version_ < 0
      || column_cnt_ <= 0
      || tablet_size_ < 0
      || pctfree_ < 0
      || table_type_ >= MAX_TABLE_TYPE
      || !table_mode_.is_valid()
      || !mv_mode_.is_valid()
      || index_type_ >= INDEX_TYPE_MAX
      || !check_column_array_valid(rowkey_array_)
      || !check_column_array_valid(column_array_)
      || !check_column_array_valid(column_group_array_)
      || !check_column_array_valid(skip_idx_attr_array_)) {
    valid_ret = false;
    STORAGE_LOG_RET(WARN, OB_INVALID_ERROR, "invalid", K_(is_inited), KP_(allocator), K_(schema_version), K_(column_cnt),
        K_(tablet_size), K_(pctfree), K_(table_type), K_(table_mode), K_(index_type));
  } else if (!column_info_simplified_ && column_cnt_ != column_array_.count()) {
    valid_ret = false;
    STORAGE_LOG_RET(WARN, OB_INVALID_ERROR, "invalid column count", K(valid_ret), K_(column_info_simplified), K_(column_cnt), K_(column_array));
  } else if (is_view_table()) {
    // no need checking other options for view
  }
  return valid_ret;
}

int ObStorageSchema::assign(common::ObIAllocator &allocator, const ObStorageSchema &other)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_FAIL(init(allocator, other))) {
    STORAGE_LOG(WARN, "failed to init", K(ret), K(other));
  }

  return ret;
}

int ObStorageSchema::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (STORAGE_SCHEMA_VERSION <= storage_schema_version_
      && STORAGE_SCHEMA_VERSION_LATEST >= storage_schema_version_) {
    LST_DO_CODE(OB_UNIS_ENCODE,
        storage_schema_version_,
        info_,
        table_type_,
        table_mode_,
        index_type_,
        row_store_type_,
        schema_version_,
        column_cnt_,
        tablet_size_,
        pctfree_,
        block_size_,
        progressive_merge_round_,
        progressive_merge_num_,
        master_key_id_,
        compressor_type_,
        encryption_,
        encrypt_key_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialize_schema_array(buf, buf_len, pos, rowkey_array_))){
      STORAGE_LOG(WARN, "failed to serialize rowkey columns", K_(rowkey_array));
    } else if (!column_info_simplified_ && OB_FAIL(serialize_column_array(buf, buf_len, pos))){
      STORAGE_LOG(WARN, "failed to serialize columns", K_(column_array));
    } else if (storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V2) {
      if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, store_column_cnt_))) {
        STORAGE_LOG(WARN, "failed to serialize store_column_cnt", K(ret), K(store_column_cnt_));
      } else if (storage_schema_version_ == STORAGE_SCHEMA_VERSION_V2) {
        // v2 do not need column group
      } else if (OB_FAIL(serialize_schema_array(buf, buf_len, pos, column_group_array_))){
        STORAGE_LOG(WARN, "failed to serialize column grups", K_(column_group_array));
      } else if (OB_FAIL(serialize_schema_array(buf, buf_len, pos, skip_idx_attr_array_))){
        STORAGE_LOG(WARN, "failed to serialize skip idx attr array", K_(skip_idx_attr_array));
      }
    }
    if (OB_SUCC(ret) && storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V4) {
      OB_UNIS_ENCODE(mv_mode_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid storage schema version", K(ret), K_(storage_schema_version));
  }

  return ret;
}

int ObStorageSchema::serialize_column_array(char *buf, const int64_t data_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, column_array_.count()))) {
    STORAGE_LOG(WARN, "Fail to encode column count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_array_.count(); ++i) {
    if (STORAGE_SCHEMA_VERSION_V3 > storage_schema_version_) {
      if (OB_FAIL(column_array_.at(i).legacy_serialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Fail to serialize column schema for legacy version", K(ret), K(i), K_(column_array));
      }
    } else if (STORAGE_SCHEMA_VERSION_V3 <= storage_schema_version_) {
      if (OB_FAIL(column_array_.at(i).serialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Fail to serialize column schema", K(ret), K(i), K_(column_array));
      }
    }
  }
  return ret;
}


int ObStorageSchema::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot deserialize inited storage schema", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(data_len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(data_len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else {
    allocator_ = &allocator;
    rowkey_array_.set_allocator(&allocator);
    column_array_.set_allocator(&allocator);
    column_group_array_.set_allocator(&allocator);
    skip_idx_attr_array_.set_allocator(&allocator);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, storage_schema_version_))) {
    STORAGE_LOG(WARN, "failed to deserialize version", K(ret), K(data_len), K(pos));
  } else if (STORAGE_SCHEMA_VERSION <= storage_schema_version_
      && STORAGE_SCHEMA_VERSION_LATEST >= storage_schema_version_) {
    ObString tmp_encryption;
    ObString tmp_encrypt_key;
    LST_DO_CODE(OB_UNIS_DECODE,
        info_,
        table_type_,
        table_mode_,
        index_type_,
        row_store_type_,
        schema_version_,
        column_cnt_,
        tablet_size_,
        pctfree_,
        block_size_,
        progressive_merge_round_,
        progressive_merge_num_,
        master_key_id_,
        compressor_type_,
        tmp_encryption,
        tmp_encrypt_key);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_str(tmp_encryption, encryption_))) {
      STORAGE_LOG(WARN, "failed to deep copy string", K(ret), K(tmp_encryption));
    } else if (OB_FAIL(deep_copy_str(tmp_encrypt_key, encrypt_key_))) {
      STORAGE_LOG(WARN, "failed to deep copy string", K(ret), K(tmp_encrypt_key));
    } else if (OB_FAIL(deserialize_rowkey_column_array(buf, data_len, pos))){
      STORAGE_LOG(WARN, "failed to deserialize rowkey columns", K(ret), K_(rowkey_array));
    } else if (!column_info_simplified_ && OB_FAIL(deserialize_column_array(allocator, buf, data_len, pos))){
      STORAGE_LOG(WARN, "failed to deserialize columns", K(ret), K_(column_array));
    } else if (storage_schema_version_ < STORAGE_SCHEMA_VERSION_V3) {
      if (STORAGE_SCHEMA_VERSION_V2 == storage_schema_version_) {
        if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &store_column_cnt_))) {
          STORAGE_LOG(WARN, "failed to deserialize store_column_cnt", K(ret), K_(store_column_cnt));
        }
      } else {
        store_column_cnt_ = get_store_column_count_by_column_array();
      }
      ObStorageColumnGroupSchema column_group;
      uint64_t compat_version = 0;
      if (FAILEDx(generate_all_column_group_schema(column_group, row_store_type_))) {
        STORAGE_LOG(WARN, "Failed to mock column group for compat", K(ret));
      } else if (OB_FAIL(column_group_array_.reserve(1))) {
        STORAGE_LOG(WARN, "failed to reserve for column group array", K(ret));
      } else if (OB_FAIL(add_column_group(column_group))) {
        STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
      }
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &store_column_cnt_))) {
      STORAGE_LOG(WARN, "failed to deserialize store_column_cnt", K(ret), K_(store_column_cnt));
    } else if (OB_FAIL(deserialize_column_group_array(allocator, buf, data_len, pos))) {
      STORAGE_LOG(WARN, "Failed to deserialize column groups", K(ret));
    } else if (OB_FAIL(deserialize_skip_idx_attr_array(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "failed to deserialize skip idx attr array", K(ret));
    } // TODO(@lixia.yq) need to add compat log for column_group after transfer refresh
    if (OB_SUCC(ret) && storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V4) {
      OB_UNIS_DECODE(mv_mode_);
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid version", K(ret), K_(storage_schema_version));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObStorageSchema::deserialize_rowkey_column_array(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    STORAGE_LOG(WARN, "Fail to decode column count", K(ret));
  } else if (OB_FAIL(rowkey_array_.reserve(count))) {
    STORAGE_LOG(WARN, "Fail to reserve column array", K(ret), K(count));
  } else {
    ObStorageRowkeyColumnSchema column;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      column.reset();
      if (OB_FAIL(column.deserialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN,"Fail to deserialize column", K(ret));
      } else if (OB_FAIL(rowkey_array_.push_back(column))) {
        STORAGE_LOG(WARN, "Fail to add column", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObStorageSchema::get_column_array_serialize_length(
  const common::ObIArray<ObStorageColumnSchema> &array) const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(array.count());
  for (int64_t i = 0; i < array.count(); ++i) {
    if (STORAGE_SCHEMA_VERSION_V3 > storage_schema_version_) {
      len += array.at(i).legacy_serialize_len();
    } else {
      len += array.at(i).get_serialize_size();
    }
  }
  return len;
}

int ObStorageSchema::deserialize_column_array(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    STORAGE_LOG(WARN, "Fail to decode column count", K(ret));
  } else if (OB_FAIL(column_array_.reserve(count))) {
    STORAGE_LOG(WARN, "Fail to reserve column array", K(ret), K(count));
  } else {
    ObStorageColumnSchema column;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      column.reset();
      if (STORAGE_SCHEMA_VERSION_V3 > storage_schema_version_) {
        if (OB_FAIL(column.legacy_deserialize(buf, data_len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize column schema for legacy version", K(ret));
        }
      } else if (STORAGE_SCHEMA_VERSION_V3 <= storage_schema_version_) {
        if (OB_FAIL(column.deserialize(buf, data_len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize column schema", K(ret));
        }
      }

      if (OB_SUCC(ret) && column.orig_default_value_.get_deep_copy_size() > 0) {
        ObStorageColumnSchema deep_copy_column;
        if (OB_FAIL(deep_copy_column.deep_copy_default_val(allocator, column.get_orig_default_value()))) {
          STORAGE_LOG(WARN, "failed to deep copy default val", K(ret), K(column));
        } else {
          column.orig_default_value_ = deep_copy_column.orig_default_value_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_array_.push_back(column))) {
        STORAGE_LOG(WARN, "Fail to add column", K(ret), K(column));
        column.destroy(allocator);
      }
    }
  }
  return ret;
}

int ObStorageSchema::generate_column_group_array(const ObTableSchema &input_schema, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObStorageColumnGroupSchema column_group;
  int64_t column_group_cnt = 0;
  // temporarily we use cnt to decide the row store or column store
  if (OB_FAIL(input_schema.get_store_column_group_count(column_group_cnt))) {
    STORAGE_LOG(WARN, "failed to get column group count", K(ret), K(input_schema));
  } else if (OB_FAIL(column_group_array_.reserve(MAX(column_group_cnt, 1)))) {
    STORAGE_LOG(WARN, "failed to reserve for column group array", K(ret), K(input_schema));
  } else if (column_group_cnt <= 1) {
    if (OB_FAIL(generate_all_column_group_schema(column_group, row_store_type_))) {
      STORAGE_LOG(WARN, "Failed to generate column array group for row store", K(ret));
    } else if (OB_FAIL(add_column_group(column_group))) {
      STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
    }
  } else {
    ObSEArray<ObColDesc, 32> column_ids;
    ObSEArray<const ObColumnGroupSchema *, 8> column_group_metas;
    if (OB_FAIL(input_schema.get_store_column_ids(column_ids))) {
      STORAGE_LOG(WARN, "Failed to get column_ids", K(ret));
    } else if (OB_FAIL(input_schema.get_store_column_groups(column_group_metas))) { // get cg metas without empty default cg
      STORAGE_LOG(WARN, "failed to get column group metas", K(ret), K(input_schema));
    } else {
      const int64_t schema_rowkey_cnt = input_schema.get_rowkey_column_num();
      const ObColumnGroupSchema *cg_schema = nullptr;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < column_group_metas.count(); ++idx) {
        column_group.reset();
        if (OB_ISNULL(cg_schema = column_group_metas.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null cg_schema", K(ret));
        } else if (cg_schema->get_column_group_type() == ALL_COLUMN_GROUP) {
          if (OB_FAIL(generate_all_column_group_schema(column_group, cg_schema->get_row_store_type()))) {
            STORAGE_LOG(WARN, "Failed to generage all column group schema", K(ret));
          }
        } else if (OB_FAIL(column_group.copy_from(column_ids, schema_rowkey_cnt, *cg_schema, allocator))) {
          STORAGE_LOG(WARN, "Failed to copy column group schema", K(ret));
        }

        if (FAILEDx(add_column_group(column_group))) {
          STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
          column_group.destroy(allocator);
        }
      }
    }
  }

  return ret;
}

int ObStorageSchema::generate_all_column_group_schema(ObStorageColumnGroupSchema &column_group, const ObRowStoreType row_store_type) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(store_column_cnt_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to mock column group", K(ret), K_(store_column_cnt), K(column_array_));
  } else {
    // all the row-oriented tables should only consist of one default column group
    column_group.reset();
    column_group.version_ = ObStorageColumnGroupSchema::COLUMN_GRUOP_SCHEMA_VERSION;
    column_group.type_ = ALL_COLUMN_GROUP;
    column_group.schema_column_cnt_ = store_column_cnt_;
    column_group.rowkey_column_cnt_ = get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_group.schema_rowkey_column_cnt_ = get_rowkey_column_num();
    column_group.column_cnt_ = column_group.schema_column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_group.column_idxs_ = nullptr;
    column_group.block_size_ = block_size_;
    column_group.compressor_type_ = compressor_type_;
    column_group.row_store_type_ = row_store_type;
  }

  return ret;
}

int ObStorageSchema::generate_cs_replica_cg_array(common::ObIAllocator &allocator, ObIArray<ObStorageColumnGroupSchema> &cg_schemas) const
{
  int ret = OB_SUCCESS;
  int schema_rowkey_column_cnt = get_rowkey_column_num();
  cg_schemas.reset();
  ObStorageColumnGroupSchema column_group;
  uint16_t normal_column_start_idx = schema_rowkey_column_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  /*
   * column idx in ObStorageColumnGroupSchema means the offset in column_ids(row keys in the front and according to the definition order).
   * rowkey_array_ init with the definition order, so need sort to decide the column idx.
   *
   * For example, if create a table with (v1, k1, v2, k2, v3, k3, primary key(k3, k1, k2))
   *                                [v1, k1, v2, k2, v3, k3]
   * the column id from v1 to k3 is [16, 17, 18, 19, 20, 21]
   *
   *                                [k3, k1, k2, tid, sql, v1, v2, v3]
   *          array idx is          [ 0,  1,  2,   3,   4,  5,  6,  7]
   * but the column_ids is          [21, 17, 19,   7,   8, 16, 18, 20]
   *
   * so the column group is         [rowkey cg0,      cg1(v1), cg2(k1), cg3(v2), cg4(k2), cg5(v3), cg6(k3)]
   *       with column_idxs         [[0, 1, 2, 3, 4],     [5],     [1],     [6],     [2],     [7],     [0]]
   */

  if (FAILEDx(cg_schemas.reserve(store_column_cnt_ + 1))) {
    STORAGE_LOG(WARN, "failed to reserve for column group array", K(ret), K_(store_column_cnt));
  } else if (OB_FAIL(generate_rowkey_column_group_schema(column_group, ObRowStoreType::CS_ENCODING_ROW_STORE, allocator))) {
    STORAGE_LOG(WARN, "failed to generate_rowkey_column_group_schema", K(ret));
  } else if (OB_FAIL(cg_schemas.push_back(column_group))) {
    STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
    column_group.destroy(allocator);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_array_.count(); i++) {
    const ObStorageColumnSchema &column = column_array_.at(i);
    uint16_t column_idx = UINT16_MAX;
    if (OB_UNLIKELY(!column.is_column_stored_in_sstable())) {
    } else {
      if (column.is_rowkey_column()) {
        const uint32_t column_id = i + OB_APP_MIN_COLUMN_ID;
        for (int16_t j = 0; j < rowkey_array_.count(); j++) {
          if (rowkey_array_.at(j).column_idx_ == column_id) {
            column_idx = j;
            break;
          }
        }
        if (OB_UNLIKELY(column_idx == UINT16_MAX)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "failed to find column idx in rowkey array", K(ret), K_(rowkey_array), K(column_id));
        }
      } else {
        column_idx = normal_column_start_idx++;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_single_column_group_schema(column_group, ObRowStoreType::CS_ENCODING_ROW_STORE, column_idx, allocator))) {
        STORAGE_LOG(WARN, "failed to generate_single_column_group_schema", K(ret), K(i));
      } else if (OB_FAIL(cg_schemas.push_back(column_group))) {
        STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
        column_group.destroy(allocator);
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < cg_schemas.count(); i++) {
      cg_schemas.at(i).destroy(allocator);
    }
  }

  return ret;
}

int ObStorageSchema::generate_cs_replica_cg_array()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_info_simplified_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column info is simplified", K(ret), KPC(this));
  } else if (OB_FAIL(generate_cs_replica_cg_array(*allocator_, column_group_array_))) {
    STORAGE_LOG(WARN, "Failed to generate column store cg array", K(ret), KPC(this));
  } else {
    is_cs_replica_compat_ = is_cg_array_generated_in_cs_replica();
    STORAGE_LOG(INFO, "[CS-Replica] Success to generate cs replica cg array", K(ret), KPC(this));
  }
  return ret;
}

int ObStorageSchema::generate_single_column_group_schema(ObStorageColumnGroupSchema &column_group, const ObRowStoreType row_store_type, const uint16_t column_idx, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  column_group.reset();
  column_group.version_ = ObStorageColumnGroupSchema::COLUMN_GRUOP_SCHEMA_VERSION;
  column_group.type_ = SINGLE_COLUMN_GROUP;
  column_group.schema_column_cnt_ = 1;
  column_group.rowkey_column_cnt_ = 0;
  column_group.schema_rowkey_column_cnt_ = column_group.rowkey_column_cnt_;
  column_group.column_cnt_ = column_group.schema_column_cnt_;

  uint16_t *column_idxs = nullptr;
  if (OB_ISNULL(column_idxs = reinterpret_cast<uint16_t *> (allocator_->alloc(sizeof(uint16_t) * column_group.column_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(column_cnt_));
  } else {
    column_idxs[0] = column_idx;
    column_group.column_idxs_ = column_idxs;
    column_group.block_size_ = block_size_;
    column_group.compressor_type_ = compressor_type_;
    column_group.row_store_type_ = row_store_type;
  }

  return ret;
}

int ObStorageSchema::generate_rowkey_column_group_schema(ObStorageColumnGroupSchema &column_group, const ObRowStoreType row_store_type, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  column_group.reset();
  column_group.version_ = ObStorageColumnGroupSchema::COLUMN_GRUOP_SCHEMA_VERSION;
  column_group.type_ = ROWKEY_COLUMN_GROUP;
  column_group.schema_column_cnt_ = get_rowkey_column_num();
  column_group.rowkey_column_cnt_ = get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  column_group.schema_rowkey_column_cnt_ = column_group.schema_column_cnt_;
  column_group.column_cnt_ = column_group.rowkey_column_cnt_;

  uint16_t *column_idxs = nullptr;
  if (OB_ISNULL(column_idxs = reinterpret_cast<uint16_t *> (allocator_->alloc(sizeof(uint16_t) * column_group.column_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(column_cnt_));
  } else {
    for (int64_t i = 0; i < column_group.column_cnt_; ++i) {
      column_idxs[i] = i;
    }
    column_group.column_idxs_ = column_idxs;
    column_group.block_size_ = block_size_;
    column_group.compressor_type_ = compressor_type_;
    column_group.row_store_type_ = row_store_type;
  }

  return ret;
}

int ObStorageSchema::mock_row_store_cg(ObStorageColumnGroupSchema &mocked_row_store_cg) const
{
  // if cache mocked_row_store_cg in storage schema, cached value will become invalid when ddl happen, so re-build every time
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_all_column_group_schema(mocked_row_store_cg, row_store_type_))) {
    STORAGE_LOG(WARN, "fail to mock row store cg schema", K(ret));
  }
  return ret;
}

int ObStorageSchema::transform_from_row_to_columnar()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (!is_row_store()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "only row store schema can be transformed", K(ret), KPC(this));
  } else {
    is_inited_ = false;
    (void) reset_column_group_array();
    has_all_column_group_ = false;
    if (OB_FAIL(ObStorageSchema::generate_cs_replica_cg_array())) {
      STORAGE_LOG(WARN, "failed to generate_cs_replica_cg_array", K(ret));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid storage schema", K(ret), KPC(this));
    } else {
      is_inited_ = true;
    }
  }
  STORAGE_LOG(INFO, "[CS-Replica] finish transform row store storage schema", K(ret), KPC(this));
  return ret;
}

/*
 * base_cg of column store schema can only be ROWKEY_CG OR ALL_CG
 * "with column group(all columns, each column)" -> ALL_CG + each_cg
 * "with column group(each column)" -> ROWKEY_CG + each_cg
*/
int ObStorageSchema::get_base_rowkey_column_group_index(int32_t &cg_idx) const
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = column_group_array_.count();
  cg_idx = OB_INVALID_INDEX; // -1
  if (OB_UNLIKELY(1 >= column_group_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "No column group exist", K(ret), KPC(this));
  } else if (OB_UNLIKELY(column_group_cnt > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(ERROR, "column group count is overflow", K(column_group_cnt));
  } else {
    for (int32_t i = 0; (OB_INVALID_INDEX == cg_idx) && i < column_group_cnt; i++) {
      if (column_group_array_.at(i).is_rowkey_column_group() || column_group_array_.at(i).is_all_column_group()) {
         cg_idx = i;
      }
    }
    if (OB_INVALID_INDEX == cg_idx) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to find base/rowkey cg", K(ret), K(column_group_array_));
    }
  }
  return ret;
}

int ObStorageSchema::get_column_group_index(
    const uint64_t &column_id,
    const int32_t &column_idx,
    int32_t &cg_idx) const
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = column_group_array_.count();
  const bool is_multi_version_col = OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id
                                 || OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id;
  cg_idx = OB_INVALID_INDEX; // -1
  if (OB_UNLIKELY(1 >= column_group_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "No column group exist", K(ret), KPC(this));
  } else if (OB_UNLIKELY(column_group_cnt > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(ERROR, "column group count is overflow", K(column_group_cnt));
  } else if (column_id < OB_END_RESERVED_COLUMN_ID_NUM &&
      common::OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID != column_id &&
      common::OB_HIDDEN_SESSION_ID_COLUMN_ID != column_id &&
      common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != column_id) { // this has its own column group now
    if (is_multi_version_col) {
      if (OB_FAIL(get_base_rowkey_column_group_index(cg_idx))) {
        STORAGE_LOG(WARN, "Fail to get base/rowkey column group index", K(ret), K(column_id));
      }
    } else {
      // TODO: check the following
      // TODO: after check, also see ObTableSchema::get_column_group_index
      // common::OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id
      // common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == column_id
      cg_idx = OB_INVALID_INDEX;
    }
  } else {
    const uint16_t *column_idxs = nullptr;
    for (int32_t i = 0; OB_SUCC(ret) && (OB_INVALID_INDEX == cg_idx) && i < column_group_cnt; i++) {
      const ObStorageColumnGroupSchema &cg_schema = column_group_array_.at(i);
      if (!cg_schema.is_single_column_group()) { // now only support single cg
      } else if (OB_ISNULL(column_idxs = cg_schema.column_idxs_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid empty cg", K(ret), K(cg_schema));
      } else if (column_idxs[0] == column_idx) {
        cg_idx = i;
      }
    }
    if (OB_SUCC(ret) && (OB_INVALID_INDEX == cg_idx)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to find column group", K(ret), K(column_id), K(column_idx), K(column_group_array_));
    }
  }

  return ret;
}

bool ObStorageSchema::is_cg_array_generated_in_cs_replica() const
{
  bool bret = false;
  if (column_group_array_.count() <= 1 || is_column_table_schema_) {
    // row store or column store table schema after v435 (all/rowkey cg is placed in the front of cg array)
  } else {
    bret = column_group_array_.at(0).is_rowkey_column_group(); // cs replica will set rowkey cg in the front of cg array
  }
  return bret;
}

int ObStorageSchema::deserialize_column_group_array(ObIAllocator &allocator,
                                                    const char *buf,
                                                    const int64_t data_len,
                                                    int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    STORAGE_LOG(WARN, "Fail to decode column count", K(ret));
  } else if (OB_FAIL(column_group_array_.reserve(count))) {
    STORAGE_LOG(WARN, "Fail to reserve column array", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObStorageColumnGroupSchema column_group;
      if (OB_FAIL(column_group.deserialize(allocator, buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Failed to deserialize column group", K(ret));
      } else if (OB_FAIL(add_column_group(column_group))) {
        STORAGE_LOG(WARN, "failed to add column group", K(ret), K(column_group));
        column_group.destroy(allocator);
      }
    }
  }

  return ret;
}

int ObStorageSchema::deserialize_skip_idx_attr_array(const char *buf,
                                                     const int64_t data_len,
                                                     int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    STORAGE_LOG(WARN, "Fail to decode column count", K(ret));
  } else if (OB_FAIL(skip_idx_attr_array_.reserve(count))) {
    STORAGE_LOG(WARN, "Fail to reserve skip idx array", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObSkipIndexAttrWithId skip_attr_with_id;
      if (OB_FAIL(skip_attr_with_id.deserialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Failed to deserialize skip idx attr with col id", K(ret));
      } else if (OB_FAIL(skip_idx_attr_array_.push_back(skip_attr_with_id))) {
        STORAGE_LOG(WARN, "Fail to add skip attr with col id", K(ret), K(skip_attr_with_id));
      }
    }
  }
  return ret;
}

int64_t ObStorageSchema::get_serialize_size() const
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      storage_schema_version_,
      info_,
      table_type_,
      table_mode_,
      index_type_,
      row_store_type_,
      schema_version_,
      column_cnt_,
      tablet_size_,
      pctfree_,
      block_size_,
      progressive_merge_round_,
      progressive_merge_num_,
      master_key_id_,
      compressor_type_,
      encryption_,
      encrypt_key_);
  len += get_array_serialize_length(rowkey_array_);
  //get columms size
  if (!column_info_simplified_) {
    len += get_column_array_serialize_length(column_array_);
  }
  if (storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V2) {
    len += serialization::encoded_length_i64(store_column_cnt_);
  }
  if (storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V3) {
    len += get_array_serialize_length(column_group_array_);
    len += get_array_serialize_length(skip_idx_attr_array_);
  }
  if (storage_schema_version_ >= STORAGE_SCHEMA_VERSION_V4) {
    OB_UNIS_ADD_LEN(mv_mode_);
  }

  return len;
}

int ObStorageSchema::generate_str(const ObTableSchema &input_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_str(input_schema.get_encryption_str(), encryption_))) {
    STORAGE_LOG(WARN, "failed to deep copy string", K(ret), K(*this));
  } else if (OB_FAIL(deep_copy_str(input_schema.get_encrypt_key(), encrypt_key_))) {
    STORAGE_LOG(WARN, "failed to deep copy string", K(ret), K(*this));
  }
  return ret;
}

int ObStorageSchema::generate_column_array(const ObTableSchema &input_schema)
{
  int ret = OB_SUCCESS;
  // build column schema map
  common::hash::ObHashMap<uint64_t, uint64_t> tmp_map; // column_id -> index

  if (OB_FAIL(tmp_map.create(input_schema.get_column_count(), "StorageSchema"))) {
    STORAGE_LOG(WARN, "failed to create map", K(ret));
  } else if (OB_FAIL(input_schema.check_column_array_sorted_by_column_id(true/*skip_rowkey*/))) {
    STORAGE_LOG(WARN, "invalid schema", K(ret), K(input_schema));
  }

  ObTableSchema::const_column_iterator iter = input_schema.column_begin();
  ObColumnSchemaV2 *col = NULL;
  ObStorageColumnSchema col_schema;
  if (FAILEDx(column_array_.reserve(input_schema.get_column_count()))) {
    STORAGE_LOG(WARN, "Fail to reserve column array", K(ret));
  }
  int64_t col_idx = 0;
  int64_t col_cnt_in_sstable = 0;
  int64_t has_skip_index_cnt = 0;
  blocksstable::ObStorageDatum datum;
  ObSEArray<share::schema::ObSkipIndexAttrWithId, 16> tmp_skip_array;
  for ( ; OB_SUCC(ret) && iter != input_schema.column_end(); iter++) {
    if (OB_ISNULL(col = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The column is NULL", K(col));
    } else if (FALSE_IT(col_cnt_in_sstable += col->is_column_stored_in_sstable())) {
      // only record stored column count here
    } else if (!column_info_simplified_) {
      col_schema.reset();
      col_schema.is_rowkey_column_ = col->is_rowkey_column();
      col_schema.is_column_stored_in_sstable_ = col->is_column_stored_in_sstable();
      col_schema.is_generated_column_ = col->is_generated_column();
      ObObjMeta meta_type = col->get_meta_type();
      if (meta_type.is_decimal_int()) {
        meta_type.set_stored_precision(col->get_accuracy().get_precision());
        meta_type.set_scale(col->get_accuracy().get_scale());
      } else if (ob_is_real_type(meta_type.get_type())) {
        meta_type.set_scale(col->get_accuracy().get_scale());
      }
      col_schema.meta_type_ = meta_type;
      if (OB_FAIL(datum.from_obj_enhance(col->get_orig_default_value()))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
      } else if (is_lob_storage(col->get_data_type()) && !datum.has_lob_header()
              && OB_FAIL(ObLobManager::fill_lob_header(*allocator_, datum))) {
        STORAGE_LOG(WARN, "failed to fill lob header", K(ret), K(datum));
      } else {
        col_schema.default_checksum_ = datum.checksum(0);
#ifdef ERRSIM
        int64_t error_code = OB_E(EventTable::EN_COMPACTION_WITH_ZERO_DEFAULT_COLUMN_CHECKSUM) OB_SUCCESS;
        int64_t errsim_data_version = static_cast<int>(DATA_VERSION_4_3_4_0);
        if (-errsim_data_version == error_code && is_lob_storage(col->get_data_type())) {
          col_schema.default_checksum_ = 0;
          STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_WITH_ZERO_DEFAULT_COLUMN_CHECKSUM set zero default checksum", K(error_code), K(col_schema));
        }
#endif
      }
      if (FAILEDx(col_schema.deep_copy_default_val(*allocator_, col->get_orig_default_value()))) {
        STORAGE_LOG(WARN, "failed to deep copy", K(ret), K(col->get_orig_default_value()));
      } else if (OB_FAIL(column_array_.push_back(col_schema))) {
        STORAGE_LOG(WARN, "Fail to push into column array", K(ret), K(col_schema));
        col_schema.destroy(*allocator_);
      }
    }
    const share::schema::ObSkipIndexColumnAttr &skip_idx_attr = col->get_skip_index_attr();
    ObSkipIndexAttrWithId skip_attr_with_id;
    skip_attr_with_id.col_idx_ = col_idx;
    skip_attr_with_id.skip_idx_attr_ = skip_idx_attr;
    if (FAILEDx(tmp_map.set_refactored(col->get_column_id(), col_idx))) {
      STORAGE_LOG(WARN, "failed to set column map", K(ret), "col_id", col->get_column_id(), K(col_idx));
    } else if (skip_idx_attr.has_skip_index() && col->is_column_stored_in_sstable() &&
        OB_FAIL(tmp_skip_array.push_back(skip_attr_with_id))) {
      STORAGE_LOG(WARN, "fail to push into skip idx attr with col id array",
          K(ret), K(col_idx), K(skip_attr_with_id));
    } else {
      col_idx++;
    }
  } // end of for

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(skip_idx_attr_array_.assign(tmp_skip_array))){
    STORAGE_LOG(WARN, "Fail to reserve skip idx attr with col id array", K(ret), K(tmp_skip_array));
  } else {
    store_column_cnt_ = is_storage_index_table() ? input_schema.get_column_count() : col_cnt_in_sstable;
  }
  // add rowkey columns
  ObStorageRowkeyColumnSchema rowkey_schema;
  const ObColumnSchemaV2 *rowkey_col_schema = nullptr;
  const common::ObRowkeyInfo &rowkey_info = input_schema.get_rowkey_info();
  const ObRowkeyColumn *rowkey_column = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rowkey_array_.reserve(rowkey_info.get_size()))) {
    STORAGE_LOG(WARN, "Fail to reserve rowkey column array", K(ret));
  }
  uint64_t find_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The rowkey column is NULL", K(i));
    } else if (OB_FAIL(tmp_map.get_refactored(rowkey_column->column_id_, find_idx))) {
      STORAGE_LOG(WARN, "failed to get column idx from tmp_map", K(ret), "column_id", rowkey_column->column_id_);
    } else if (OB_ISNULL(rowkey_col_schema =
                           input_schema.get_column_schema(rowkey_column->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to get rowkey column schema", K(ret));
    } else {
      rowkey_schema.reset();
      rowkey_schema.column_idx_ = common::OB_APP_MIN_COLUMN_ID + find_idx;
      ObObjMeta meta_type = rowkey_column->type_;
      if (meta_type.is_decimal_int()) {
        meta_type.set_stored_precision(rowkey_col_schema->get_accuracy().get_precision());
        meta_type.set_scale(rowkey_col_schema->get_accuracy().get_scale());
      } else if (ob_is_real_type(meta_type.get_type())) {
        meta_type.set_scale(rowkey_col_schema->get_accuracy().get_scale());
      }
      rowkey_schema.meta_type_ = meta_type;
      rowkey_schema.order_ = rowkey_column->order_;
      if (OB_FAIL(rowkey_array_.push_back(rowkey_schema))) {
        STORAGE_LOG(WARN, "Fail to add rowkey column id to rowkey array", K(ret));
      }
    }
  }

  if (tmp_map.created()) {
    tmp_map.destroy();
  }
  return ret;
}

int ObStorageSchema::get_column_ids_without_rowkey(
    common::ObIArray<share::schema::ObColDesc> &column_ids,
    const bool no_virtual) const
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  if (column_info_simplified_) {
    // fake column ids
    for (int64_t i = rowkey_array_.count(); OB_SUCC(ret) && i < store_column_cnt_; i++) {
      col_desc.col_id_ = common::OB_APP_MIN_COLUMN_ID + i;
      //for non-rowkey, col_desc.col_order_ is not meaningful
      if (OB_FAIL(column_ids.push_back(col_desc))) {
        STORAGE_LOG(WARN, "Fail to add column id to column_ids", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
      const ObStorageColumnSchema &column = column_array_[i];
      if (!column.is_rowkey_column_                    // column is not a rowkey column(rowkey column is already added in step1 get_rowkey_column_ids)
         && (column.is_column_stored_in_sstable_       // current column is not virtual
              || !no_virtual)) {                       // could have virtual column
        col_desc.col_id_ = common::OB_APP_MIN_COLUMN_ID + i;
        col_desc.col_type_ = column.meta_type_;
        //for non-rowkey, col_desc.col_order_ is not meaningful
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          STORAGE_LOG(WARN, "Fail to add column id to column_ids", K(ret));
        }
      }
    } // end of for
  }
  return ret;
}

int ObStorageSchema::get_rowkey_column_ids(common::ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else {
    //add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_array_.count(); ++i) {
      col_desc.col_id_ = rowkey_array_[i].column_idx_;
      col_desc.col_type_ = rowkey_array_[i].meta_type_;
      col_desc.col_order_ = (ObOrderType)rowkey_array_[i].order_;
      if (OB_FAIL(column_ids.push_back(col_desc))) {
        STORAGE_LOG(WARN, "Fail to add rowkey column id to column_ids", K(ret));
      }
    }
  }
  return ret;
}

int ObStorageSchema::get_skip_index_col_attr(
    ObIArray<share::schema::ObSkipIndexColumnAttr> &skip_idx_attrs) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else {
    skip_idx_attrs.reset();
    // add rowkey columns
    share::schema::ObSkipIndexColumnAttr rowkey_skip_idx_attr;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_array_.count(); ++i) {
      rowkey_skip_idx_attr.reset();
      int64_t rowkey_col_idx = rowkey_array_[i].column_idx_ - common::OB_APP_MIN_COLUMN_ID;
      for (int64_t skip_col_id = 0; OB_SUCC(ret) && skip_col_id < skip_idx_attr_array_.count(); ++skip_col_id) {
        if (rowkey_col_idx == skip_idx_attr_array_.at(skip_col_id).col_idx_) {
          rowkey_skip_idx_attr = skip_idx_attr_array_.at(skip_col_id).skip_idx_attr_;
          break;
        }
      }
      if (FAILEDx(skip_idx_attrs.push_back(rowkey_skip_idx_attr))) {
        STORAGE_LOG(WARN, "fail to append rowkey skip index attr to array",
            K(ret), K(i), K(rowkey_skip_idx_attr));
      }
    }
    // add dummy idx for stored multi-version columns
    if (OB_SUCC(ret)) {
      ObSkipIndexColumnAttr dummy_multi_version_col_attr;
      if (OB_FAIL(skip_idx_attrs.push_back(dummy_multi_version_col_attr))) {
        STORAGE_LOG(WARN, "failed to push dummy multi version column skip index attr", K(ret));
      } else if (OB_FAIL(skip_idx_attrs.push_back(dummy_multi_version_col_attr))) {
        STORAGE_LOG(WARN, "failed to push dummy multi version column skip index attr", K(ret));
      }
    }
    // add non-rowkey columns

    share::schema::ObSkipIndexColumnAttr no_rowkey_skip_idx_attr;
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_cnt_; ++col_idx) {
      no_rowkey_skip_idx_attr.reset();
      bool is_rowkey = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_array_.count(); ++j) {
        int64_t rowkey_col_idx = rowkey_array_[j].column_idx_ - common::OB_APP_MIN_COLUMN_ID;
        if (rowkey_col_idx == col_idx) {
          is_rowkey = true;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_rowkey) {
        //skip
      } else {
        for (int64_t skip_col_id = 0; OB_SUCC(ret) && skip_col_id < skip_idx_attr_array_.count(); ++skip_col_id) {
          if (col_idx == skip_idx_attr_array_.at(skip_col_id).col_idx_) {
            no_rowkey_skip_idx_attr = skip_idx_attr_array_.at(skip_col_id).skip_idx_attr_;
            break;
          }
        }
        if (FAILEDx(skip_idx_attrs.push_back(no_rowkey_skip_idx_attr))) {
          STORAGE_LOG(WARN, "fail to append no rowkey skip index attr to array",
              K(ret), K(col_idx), K(rowkey_skip_idx_attr));
        }
      }
    }
  }
  return ret;
}

int ObStorageSchema::get_encryption_id(int64_t &encrypt_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(share::ObEncryptionUtil::parse_encryption_id(encryption_, encrypt_id))) {
    STORAGE_LOG(WARN, "failed to parse_encryption_id", K(ret), K(encryption_));
  }
  return ret;
}

bool ObStorageSchema::need_encrypt() const {
  bool ret = false;
  if (0 != encryption_.length() && 0 != encryption_.case_compare("none")) {
    ret = true;
  }
  return ret;
}

int ObStorageSchema::get_stored_column_count_in_sstable(int64_t &column_count) const
{
  int ret = OB_SUCCESS;
  column_count = 0;
  if (OB_FAIL(get_store_column_count(column_count, true/*full_col*/))) {
    STORAGE_LOG(WARN, "fail to get store column count", K(ret), K(column_count));
  } else {
    column_count += storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }
  return ret;
}

int ObStorageSchema::get_store_column_count(int64_t &column_count, const bool full_col) const
{
  UNUSED(full_col);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (is_storage_index_table()) {
    column_count = column_cnt_;
  } else {
    column_count = store_column_cnt_;
  }
  return ret;
}

// will call in deserialize for compat to init store_column_cnt_
int64_t ObStorageSchema::get_store_column_count_by_column_array()
{
  int64_t column_count = 0;
  if (is_storage_index_table()) {
    column_count = column_cnt_;
  } else {
    column_count = 0;
    for (int64_t i = 0; i < column_cnt_; ++i) {
      if (column_array_[i].is_column_stored_in_sstable_) { // current column is not virtual
        ++column_count;
      }
    }
  }
  return column_count;
}

int ObStorageSchema::init_column_meta_array(
    common::ObIArray<blocksstable::ObSSTableColumnMeta> &meta_array) const
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> columns;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (column_info_simplified_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support get multi version column desc array when column simplified", K(ret), KPC(this));
  } else if (OB_FAIL(get_multi_version_column_descs(columns))) {
    STORAGE_LOG(WARN, "fail to get store column ids", K(ret));
  } else {
    // build column schema map
    common::hash::ObHashMap<uint64_t, uint64_t> tmp_map; // column_id -> index
    if (OB_FAIL(tmp_map.create(column_array_.count(), "StorageSchema"))) {
      STORAGE_LOG(WARN, "failed to create map", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < column_array_.count(); ++i) {
      if (OB_FAIL(tmp_map.set_refactored(common::OB_APP_MIN_COLUMN_ID + i, i))) {
        STORAGE_LOG(WARN, "failed to set column map", K(ret), K(i), K(column_array_.at(i)));
      }
    }
    blocksstable::ObSSTableColumnMeta col_meta;
    uint64_t idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const uint64_t column_idx = columns.at(i).col_id_;
      col_meta.column_id_ = column_idx;
      col_meta.column_checksum_ = 0;
      if (OB_FAIL(tmp_map.get_refactored(column_idx, idx))) {
        // if it's multi version extra rowkey, no problem
        if (column_idx == OB_HIDDEN_TRANS_VERSION_COLUMN_ID ||
            column_idx == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
          col_meta.column_default_checksum_ = 0;
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to get column schema", K(ret), K(i), K(columns.at(i)));
        }
      } else if (idx >= column_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "idx is invalid", K(ret), K(idx), K(*this));
      } else {
        const ObStorageColumnSchema &col_schema = column_array_.at(idx);
        if (!col_schema.is_column_stored_in_sstable_ && !is_storage_index_table()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "virtual generated column should be filtered already", K(ret), K(col_schema));
        } else {
          col_meta.column_default_checksum_ = col_schema.default_checksum_;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(meta_array.push_back(col_meta))) {
        STORAGE_LOG(WARN, "Fail to push column meta", K(ret));
      }
    } // end for
    if (tmp_map.created()) {
      tmp_map.destroy();
    }
  }

  return ret;
}

int ObStorageSchema::get_orig_default_row(
    const common::ObIArray<ObColDesc> &column_ids,
    bool need_trim,
    blocksstable::ObDatumRow &default_row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!default_row.is_valid() || default_row.count_ != column_ids.count()
      || column_ids.count() > column_cnt_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(column_cnt_), K(default_row), K(column_ids.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (column_ids.at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID ||
        column_ids.at(i).col_id_ == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
      default_row.storage_datums_[i].set_int(0);
    } else {
      const ObStorageColumnSchema *col_schema = nullptr;
      if (OB_ISNULL(col_schema = get_column_schema(column_ids.at(i).col_id_))) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "column id not found", K(ret), K(column_ids.at(i)));
      } else if (OB_FAIL(default_row.storage_datums_[i].from_obj_enhance(col_schema->get_orig_default_value()))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
      } else if (need_trim && col_schema->get_orig_default_value().is_fixed_len_char_type()) {
        if (OB_FAIL(trim(col_schema->get_orig_default_value().get_collation_type(), default_row.storage_datums_[i]))) {
          STORAGE_LOG(WARN, "Failed to trim default value", K(ret), KPC(col_schema), K(default_row));
        }
      }
    }
  }
  return ret;
}

int ObStorageSchema::trim(const ObCollationType type, blocksstable::ObStorageDatum &storage_datum)
{
  int ret = OB_SUCCESS;
  ObString space_pattern = ObCharsetUtils::get_const_str(type, ' ');
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(type) || (0 == space_pattern.length()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid collation type", K(ret), K(type), K(space_pattern));
  } else {
    const char *str = storage_datum.ptr_;
    int32_t len = storage_datum.len_;
    for (; len >= space_pattern.length(); len -= space_pattern.length()) {
      if (0 != MEMCMP(str + len - space_pattern.length(),
            space_pattern.ptr(),
            space_pattern.length())) {
        break;
      }
    }
    storage_datum.len_ = len;
  }
  return ret;
}

const ObStorageColumnSchema *ObStorageSchema::get_column_schema(const int64_t column_idx) const
{
  const ObStorageColumnSchema *found_col = nullptr;
  for (int64_t j = 0; j < column_cnt_; ++j) {
    const ObStorageColumnSchema &column = column_array_[j];
    if (common::OB_APP_MIN_COLUMN_ID + j == column_idx) {
      found_col = &column;
      break;
    }
  }
  return found_col;
}

int ObStorageSchema::get_multi_version_column_descs(common::ObIArray<ObColDesc> &column_descs) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (column_info_simplified_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support get multi version column desc array when column simplified", K(ret), KPC(this));
  } else if (OB_FAIL(get_mulit_version_rowkey_column_ids(column_descs))) { // add rowkey columns
    STORAGE_LOG(WARN, "Fail to get rowkey column descs", K(ret));
  } else if (OB_FAIL(get_column_ids_without_rowkey(column_descs, !is_storage_index_table()))) { //add other columns
    STORAGE_LOG(WARN, "Fail to get column descs without rowkey", K(ret));
  }
  return ret;
}

int ObStorageSchema::copy_from(const share::schema::ObMergeSchema &input_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(input_schema.get_mv_mode_struct(mv_mode_))) {
    STORAGE_LOG(WARN, "Fail to get mv mode struct", K(ret));
  } else {
    is_use_bloomfilter_ = input_schema.is_use_bloomfilter();
    table_type_ = input_schema.get_table_type();
    table_mode_ = input_schema.get_table_mode_struct();
    index_type_ = input_schema.get_index_type();
    row_store_type_ = input_schema.get_row_store_type();
    schema_version_ = input_schema.get_schema_version();
    column_cnt_ = input_schema.get_column_count();
    tablet_size_ = input_schema.get_tablet_size();
    pctfree_ = input_schema.get_pctfree();
    block_size_ = input_schema.get_block_size();
    progressive_merge_round_ = input_schema.get_progressive_merge_round();
    progressive_merge_num_ = input_schema.get_progressive_merge_num();
    master_key_id_ = input_schema.get_master_key_id();
    compressor_type_ = input_schema.get_compressor_type();
  }

  return ret;
}

int ObStorageSchema::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (src.length() > 0) {
    int64_t len = src.length() + 1;
    if (OB_ISNULL(src.ptr())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "The src is NULL", K(ret), K(src));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret), K(src.length()));
    } else {
      MEMCPY(buf, src.ptr(), len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
    }
  } else {
    dest.reset();
  }
  return ret;
}

void ObStorageSchema::reset_string(ObString &str)
{
  if (NULL != str.ptr() && 0 != str.length()) {
    allocator_->free(str.ptr());
  }
  str.reset();
}

void ObStorageSchema::update_column_cnt(const int64_t input_col_cnt)
{
  column_cnt_ = MAX(column_cnt_, input_col_cnt);
  store_column_cnt_ = MAX(store_column_cnt_, input_col_cnt);
  if (column_cnt_ != column_array_.count()) {
    column_info_simplified_ = true;
    STORAGE_LOG(INFO, "update column cnt", K(column_cnt_), K(store_column_cnt_), K(column_cnt_), K(column_array_.count()));
  }
}

int ObCreateTabletSchema::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObStorageSchema));
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              index_status_,
              truncate_version_);
  return ret;
}

int ObCreateTabletSchema::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageSchema::deserialize(allocator, buf, data_len, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
                table_id_,
                index_status_,
                truncate_version_);
  }
  return ret;
}

int64_t ObCreateTabletSchema::get_serialize_size() const
{
  int64_t len = ObStorageSchema::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              index_status_,
              truncate_version_);
  return len;
}

int ObCreateTabletSchema::init(
    common::ObIAllocator &allocator,
    const share::schema::ObTableSchema &input_schema,
    const lib::Worker::CompatMode compat_mode,
    const bool skip_column_info,
    const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageSchema::init(allocator, input_schema, compat_mode, skip_column_info, tenant_data_version))) {
    STORAGE_LOG(WARN, "failed to init", K(ret), KPC(this));
  } else {
    table_id_ = input_schema.get_table_id();
    index_status_ = input_schema.get_index_status();
    truncate_version_ = input_schema.get_truncate_version();
  }
  return ret;
}

int ObCreateTabletSchema::init(
    common::ObIAllocator &allocator,
    const ObCreateTabletSchema &old_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageSchema::init(allocator, old_schema))) {
    STORAGE_LOG(WARN, "failed to init", K(ret), KPC(this));
  } else {
    table_id_ = old_schema.get_table_id();
    index_status_ = old_schema.get_index_status();
    truncate_version_ = old_schema.get_truncate_version();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
