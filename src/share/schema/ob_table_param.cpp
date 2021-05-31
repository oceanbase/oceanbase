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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_table_param.h"
#include "ob_table_schema.h"
#include "ob_multi_version_schema_service.h"
#include "lib/ob_errno.h"
#include "observer/ob_server.h"
#include "sql/ob_sql_mock_schema_utils.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
int ColumnHashMap::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket_num", K(ret), K(bucket_num));
  } else {
    HashNode** buckets = static_cast<HashNode**>(allocator_.alloc(bucket_num * sizeof(HashNode*)));
    if (NULL == buckets) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buckets failed", K(ret));
    } else {
      MEMSET(buckets, 0, bucket_num * sizeof(HashNode*));
      buckets_ = buckets;
      bucket_num_ = bucket_num;
      is_inited_ = true;
    }
  }
  return ret;
}

int ColumnHashMap::clear()
{
  if (is_inited_) {
    if (NULL != buckets_) {
      allocator_.free(buckets_);
      buckets_ = NULL;
    }
    bucket_num_ = 0;
    is_inited_ = false;
  }
  return OB_SUCCESS;
}

int ColumnHashMap::set(const uint64_t key, const int32_t value)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HashNode*& bucket = buckets_[key % bucket_num_];
    HashNode* dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", K(key), K(ret));
    } else if (NULL != dst_node) {
      ret = OB_HASH_EXIST;
      LOG_WARN("key already exists", K(key), K(ret));
    } else {
      HashNode* new_node = static_cast<HashNode*>(allocator_.alloc(sizeof(HashNode)));
      if (NULL == new_node) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc new node failed", K(ret));
      } else {
        new_node->key_ = key;
        new_node->value_ = value;
        new_node->next_ = bucket;
        bucket = new_node;
      }
    }
  }
  return ret;
}

int ColumnHashMap::get(const uint64_t key, int32_t& value) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HashNode*& bucket = buckets_[key % bucket_num_];
    HashNode* dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", K(key), K(ret));
    } else if (NULL == dst_node) {
      ret = OB_HASH_NOT_EXIST;
      LOG_WARN("key not exists", K(key), K(ret));
    } else {
      value = dst_node->value_;
    }
  }
  return ret;
}

int ColumnHashMap::find_node(const uint64_t key, HashNode* head, HashNode*& node) const
{
  int ret = OB_SUCCESS;
  node = NULL;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == head) {
    // do-nothing
  } else {
    HashNode* cur_node = head;
    while (NULL != cur_node && NULL == node) {
      if (cur_node->key_ == key) {
        node = cur_node;
      } else {
        cur_node = cur_node->next_;
      }
    }
  }

  return ret;
}

int ColumnMap::init(const common::ObIArray<ObColumnParam*>& column_params)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    ObArray<uint64_t> column_ids;
    for (int64_t i = 0; i < column_params.count() && OB_SUCC(ret); ++i) {
      ObColumnParam* column = nullptr;
      if (OB_ISNULL(column = column_params.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(column), K(ret));
      } else if (OB_FAIL(column_ids.push_back(column->get_column_id()))) {
        LOG_WARN("push column id fail", K(ret), K(*column));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init(column_ids))) {
        LOG_WARN("init failed", K(ret));
      }
    }
  }
  return ret;
}

// Caller must guarantee that item in column_descs is unique.
int ColumnMap::init(const common::ObIArray<ObColDesc>& column_descs)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    ObArray<uint64_t> column_ids;
    for (int64_t i = 0; i < column_descs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(column_ids.push_back(column_descs.at(i).col_id_))) {
        LOG_WARN("push column id fail", K(ret), K(column_descs.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init(column_ids))) {
        LOG_WARN("init failed", K(ret));
      }
    }
  }
  return ret;
}

int ColumnMap::init(const common::ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    uint64_t max_column_id = OB_INVALID_ID;
    uint64_t max_shadow_column_id = OB_INVALID_ID;
    ObArray<int32_t> non_shadow_columns;
    ObArray<int32_t> shadow_columns;
    for (int32_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const uint64_t column_id = column_ids.at(i);
      if (OB_INVALID_ID == column_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column_id", K(column_id), K(ret));
      } else {
        if (!IS_SHADOW_COLUMN(column_id)) {
          if (OB_INVALID_ID == max_column_id || column_id > max_column_id) {
            max_column_id = column_id;
          }
          ret = non_shadow_columns.push_back(i);
        } else {
          if (OB_INVALID_ID == max_shadow_column_id || column_id > max_shadow_column_id) {
            max_shadow_column_id = column_id;
          }
          ret = shadow_columns.push_back(i);
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_ = max_column_id != OB_INVALID_ID;
      has_shadow_ = max_shadow_column_id != OB_INVALID_ID;
      if (has_) {
        use_array_ = max_column_id <= MAX_COLUMN_ID_USING_ARRAY;
        if (OB_FAIL(create(use_array_,
                max_column_id - COLUMN_ID_OFFSET + 1,
                COLUMN_ID_OFFSET,
                column_ids,
                non_shadow_columns,
                array_,
                map_))) {
          LOG_WARN("create failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (has_shadow_) {
          shadow_use_array_ = max_shadow_column_id <= MAX_SHADOW_COLUMN_ID_USING_ARRAY;
          if (OB_FAIL(create(shadow_use_array_,
                  max_shadow_column_id - SHADOW_COLUMN_ID_OFFSET + 1,
                  SHADOW_COLUMN_ID_OFFSET,
                  column_ids,
                  shadow_columns,
                  shadow_array_,
                  shadow_map_))) {
            LOG_WARN("create failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ColumnMap::create(const bool use_array, const int64_t array_size, const int64_t offset,
    const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<int32_t>& column_indexes, ColumnArray& array,
    ColumnHashMap& map)
{
  int ret = OB_SUCCESS;

  if (use_array) {
    if (OB_FAIL(array.init(array_size))) {
      LOG_WARN("init array failed", K(array_size), K(ret));
    } else if (OB_FAIL(array.prepare_allocate(array_size))) {
      LOG_WARN("init array failed", K(array_size), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
        array.at(i) = OB_INVALID_INDEX;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_indexes.count(); ++i) {
        const int32_t idx = column_indexes.at(i);
        const uint64_t column_id = column_ids.at(idx);
        int64_t array_idx = column_id - offset;
        if (!(array_idx >= 0 && array_idx < array_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(array_idx), K(array_size), K(ret));
        } else {
          array.at(array_idx) = idx;
        }
      }
    }
  } else {
    if (OB_FAIL(map.init(MAX_ARRAY_SIZE))) {
      LOG_WARN("init map failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_indexes.count(); ++i) {
        const int32_t idx = column_indexes.at(i);
        const uint64_t column_id = column_ids.at(idx);
        if (OB_FAIL(map.set(column_id, idx))) {
          LOG_WARN("set failed", K(column_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ColumnMap::clear()
{
  array_.reset();
  shadow_array_.reset();
  map_.clear();
  shadow_map_.clear();
  use_array_ = false;
  shadow_use_array_ = false;
  has_ = false;
  has_shadow_ = false;
  is_inited_ = false;
  return OB_SUCCESS;
}

int ColumnMap::get(const uint64_t column_id, int32_t& proj) const
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_id", K(column_id), K(ret));
  } else {
#define GET_FROM_ARRAY_OR_MAP(use_array, array, map, offset) \
  if (use_array) {                                           \
    int64_t idx = column_id - offset;                        \
    if (idx < 0) {                                           \
      ret = OB_ERR_UNEXPECTED;                               \
      LOG_WARN("unexpected idx", K(idx), K(ret));            \
    } else if (idx >= array.count()) {                       \
      proj = OB_INVALID_INDEX;                               \
    } else {                                                 \
      proj = array.at(idx);                                  \
    }                                                        \
  } else {                                                   \
    if (OB_FAIL(map.get(column_id, proj))) {                 \
      if (OB_HASH_NOT_EXIST != ret) {                        \
        LOG_WARN("get failed", K(column_id), K(ret));        \
      } else {                                               \
        proj = OB_INVALID_INDEX;                             \
        ret = OB_SUCCESS;                                    \
      }                                                      \
    }                                                        \
  }
    if (!IS_SHADOW_COLUMN(column_id)) {
      if (!has_) {
        proj = OB_INVALID_INDEX;
      } else {
        GET_FROM_ARRAY_OR_MAP(use_array_, array_, map_, COLUMN_ID_OFFSET);
      }
    } else {
      if (!has_shadow_) {
        proj = OB_INVALID_INDEX;
      } else {
        GET_FROM_ARRAY_OR_MAP(shadow_use_array_, shadow_array_, shadow_map_, SHADOW_COLUMN_ID_OFFSET);
      }
    }
  }

  return ret;
}

ObColumnParam::ObColumnParam(ObIAllocator& allocator) : allocator_(allocator)
{
  reset();
}

ObColumnParam::~ObColumnParam()
{}

void ObColumnParam::reset()
{
  column_id_ = OB_INVALID_ID;
  meta_type_.reset();
  order_ = ObOrderType::ASC;
  accuracy_.reset();
  orig_default_value_.reset();
  cur_default_value_.reset();
  is_nullable_ = false;
}

int ObColumnParam::deep_copy_obj(const ObObj& src, ObObj& dest)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (size > 0) {
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    }
  } else {
    dest = src;
  }

  return ret;
}

int32_t ObColumnParam::get_data_length() const
{
  return ObColumnSchemaV2::get_data_length(accuracy_, meta_type_);
}

OB_DEF_SERIALIZE(ObColumnParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(
      OB_UNIS_ENCODE, column_id_, meta_type_, accuracy_, orig_default_value_, cur_default_value_, order_, is_nullable_);
  return ret;
}

OB_DEF_DESERIALIZE(ObColumnParam)
{
  int ret = OB_SUCCESS;
  ObObj orig_default_value;
  ObObj cur_default_value;

  LST_DO_CODE(OB_UNIS_DECODE, column_id_, meta_type_, accuracy_, orig_default_value, cur_default_value, order_);

  // compatibility code
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, is_nullable_))) {
        LOG_WARN("failed to decode index_schema_version_", K(ret));
      }
    } else {
      is_nullable_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_obj(orig_default_value, orig_default_value_))) {
      LOG_WARN("Fail to deep copy orig_default_value, ", K(ret), K_(orig_default_value));
    } else if (OB_FAIL(deep_copy_obj(cur_default_value, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K_(cur_default_value));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObColumnParam)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      column_id_,
      meta_type_,
      accuracy_,
      orig_default_value_,
      cur_default_value_,
      order_,
      is_nullable_);
  return len;
}

int ObColumnParam::assign(const ObColumnParam& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    column_id_ = other.column_id_;
    meta_type_ = other.meta_type_;
    order_ = other.order_;
    accuracy_ = other.accuracy_;
    is_nullable_ = other.is_nullable_;
    if (OB_FAIL(deep_copy_obj(other.cur_default_value_, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(cur_default_value_));
    } else if (OB_FAIL(deep_copy_obj(other.orig_default_value_, orig_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(orig_default_value_));
    }
  }
  return ret;
}

ObTableParam::ObTableParam(ObIAllocator& allocator)
    : allocator_(allocator),
      cols_(allocator),
      col_map_(allocator),
      projector_(allocator),
      output_projector_(allocator),
      index_cols_(allocator),
      index_col_map_(allocator),
      index_projector_(allocator),
      index_output_projector_(allocator),
      index_back_projector_(allocator),
      pad_col_projector_(allocator),
      join_key_projector_(allocator),
      right_key_projector_(allocator),
      full_cols_(allocator),
      full_projector_(allocator),
      full_col_map_(allocator),
      col_descs_(allocator),
      index_col_descs_(allocator),
      full_col_descs_(allocator),
      rowid_projector_(allocator)
{
  reset();
}

ObTableParam::~ObTableParam()
{}

void ObTableParam::reset()
{
  table_id_ = OB_INVALID_ID;
  index_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  main_table_rowkey_cnt_ = 0;
  index_table_rowkey_cnt_ = 0;
  cols_.reset();
  col_map_.clear();
  projector_.reset();
  output_projector_.reset();
  index_cols_.reset();
  index_col_map_.clear();
  index_projector_.reset();
  index_output_projector_.reset();
  index_back_projector_.reset();
  pad_col_projector_.reset();
  join_key_projector_.reset();
  right_key_projector_.reset();
  index_schema_version_ = OB_INVALID_VERSION;
  full_cols_.reset();
  full_projector_.reset();
  full_col_map_.clear();
  col_descs_.reset();
  index_col_descs_.reset();
  full_col_descs_.reset();
  use_lob_locator_ = false;
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_projector_.reset();
}

OB_DEF_SERIALIZE(ObTableParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
      table_id_,
      index_id_,
      schema_version_,
      projector_,
      output_projector_,
      index_projector_,
      index_output_projector_,
      index_back_projector_,
      pad_col_projector_,
      main_table_rowkey_cnt_,
      index_table_rowkey_cnt_,
      join_key_projector_,
      right_key_projector_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_columns(cols_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize columns", K(ret));
    } else if (OB_FAIL(serialize_columns(index_cols_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize columns", K(ret));
    }
  }
  OB_UNIS_ENCODE(index_schema_version_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_columns(full_cols_, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize columns", K(ret));
    } else if (OB_FAIL(full_projector_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize projector", K(ret));
    }
  }
  OB_UNIS_ENCODE(use_lob_locator_);
  OB_UNIS_ENCODE(rowid_version_);
  OB_UNIS_ENCODE(rowid_projector_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
      table_id_,
      index_id_,
      schema_version_,
      projector_,
      output_projector_,
      index_projector_,
      index_output_projector_,
      index_back_projector_,
      pad_col_projector_,
      main_table_rowkey_cnt_,
      index_table_rowkey_cnt_,
      join_key_projector_,
      right_key_projector_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deserialize_columns(buf, data_len, pos, cols_, allocator_))) {
      LOG_WARN("failed to deserialize columns", K(ret));
    } else if (OB_FAIL(deserialize_columns(buf, data_len, pos, index_cols_, allocator_))) {
      LOG_WARN("failed to deserialize columns", K(ret));
    } else if (OB_FAIL(create_column_map(cols_, col_map_))) {
      LOG_WARN("failed to create column map", K(ret));
    } else if (OB_FAIL(create_column_map(index_cols_, index_col_map_))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }

  // compatibility code, set index_schema_version to invalid_version if pos >= data_len
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, index_schema_version_))) {
        LOG_WARN("failed to decode index_schema_version_", K(ret));
      }
    } else {
      index_schema_version_ = OB_INVALID_VERSION;
    }
  }

  // compatibility code, reset if pos >= data_len
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(deserialize_columns(buf, data_len, pos, full_cols_, allocator_))) {
        LOG_WARN("fail to deserialize columns", K(ret));
      } else if (OB_FAIL(full_projector_.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize projector", K(ret));
      } else if (OB_FAIL(create_column_map(full_cols_, full_col_map_))) {
        LOG_WARN("fail to create column map", K(ret));
      }
    } else {
      full_cols_.reset();
      full_projector_.reset();
      full_col_map_.clear();
    }
  }

  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, use_lob_locator_))) {
        LOG_WARN("failed to decode use lob locator", K(ret));
      }
    } else {
      use_lob_locator_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(construct_storage_param())) {
      LOG_WARN("failed to construct storage param", K(ret));
    }
  }
  OB_UNIS_DECODE(rowid_version_);
  OB_UNIS_DECODE(rowid_projector_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableParam)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      table_id_,
      index_id_,
      schema_version_,
      projector_,
      output_projector_,
      index_projector_,
      index_output_projector_,
      index_back_projector_,
      pad_col_projector_,
      main_table_rowkey_cnt_,
      index_table_rowkey_cnt_,
      join_key_projector_,
      right_key_projector_);

  if (OB_SUCC(ret)) {
    int64_t size1 = 0;
    int64_t size2 = 0;
    if (OB_FAIL(get_columns_serialize_size(cols_, size1))) {
      LOG_WARN("failed to get columns serialize size", K(ret));
    } else if (OB_FAIL(get_columns_serialize_size(index_cols_, size2))) {
      LOG_WARN("failed to get columns serialize size", K(ret));
    } else {
      len += (size1 + size2);
    }
  }
  OB_UNIS_ADD_LEN(index_schema_version_);

  if (OB_SUCC(ret)) {
    int64_t full_col_size = 0;
    if (OB_FAIL(get_columns_serialize_size(full_cols_, full_col_size))) {
      LOG_WARN("fail to get column serialize size", K(ret));
    } else {
      len += full_col_size;
      len += full_projector_.get_serialize_size();
    }
  }
  OB_UNIS_ADD_LEN(use_lob_locator_);
  OB_UNIS_ADD_LEN(rowid_version_);
  OB_UNIS_ADD_LEN(rowid_projector_);
  return len;
}

int ObTableParam::get_columns_serialize_size(const Columns& columns, int64_t& size)
{
  int ret = OB_SUCCESS;
  size = 0;

  size += serialization::encoded_length_vi64(columns.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (OB_ISNULL(columns.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else {
      size += columns.at(i)->get_serialize_size();
    }
  }
  return ret;
}

int ObTableParam::serialize_columns(const Columns& columns, char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, columns.count()))) {
    LOG_WARN("Fail to encode column count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (OB_ISNULL(columns.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else if (OB_FAIL(columns.at(i)->serialize(buf, data_len, pos))) {
      LOG_WARN("Fail to serialize column", K(ret));
    }
  }
  return ret;
}

int ObTableParam::deserialize_columns(
    const char* buf, const int64_t data_len, int64_t& pos, Columns& columns, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObColumnParam** column = NULL;
  int64_t column_cnt = 0;
  void* tmp_ptr = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_cnt))) {
    LOG_WARN("Fail to decode column count", K(ret));
  } else if (column_cnt > 0) {
    if (NULL == (tmp_ptr = allocator.alloc(column_cnt * sizeof(ObColumnParam*)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc", K(ret), K(column_cnt));
    } else if (FALSE_IT(column = static_cast<ObColumnParam**>(tmp_ptr))) {
      // not reach
    } else {
      ObArray<ObColumnParam*> tmp_columns;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        ObColumnParam*& cur_column = column[i];
        cur_column = nullptr;
        if (OB_FAIL(alloc_column(allocator, cur_column))) {
          LOG_WARN("Fail to alloc", K(ret), K(i));
        } else if (OB_FAIL(cur_column->deserialize(buf, data_len, pos))) {
          LOG_WARN("Fail to deserialize column", K(ret));
        } else if (OB_FAIL(tmp_columns.push_back(cur_column))) {
          LOG_WARN("Fail to add column", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(columns.assign(tmp_columns))) {
          LOG_WARN("Fail to add columns", K(ret));
        }
      }
    }
  }
  return ret;
}

// Since version 2.2, all the columns information from both main table and index table will be
// retrieved in order to cache the query result in the storage row cache.
// table_schame can be either main table or index table.
// This method must be called after construct_vertitical_partition.
int ObTableParam::construct_full_columns_and_projector(const ObTableSchema& table_schema,
    common::ObIArray<ObColumnParam*>& full_cols, Projector& full_projector, ColumnMap& full_col_map)
{
  int ret = OB_SUCCESS;
  static const int64_t COMMON_COLUMN_NUM = 16;
  ObSEArray<ObColumnParam*, COMMON_COLUMN_NUM> tmp_cols;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_projector;
  // rowkey columns front, other columns behind
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> column_ids_no_virtual;
  full_cols.reuse();
  const bool no_virtual = !table_schema.is_index_table();

  if (OB_FAIL(table_schema.get_column_ids(column_ids_no_virtual, no_virtual))) {
    LOG_WARN("fail to get column ids", K(ret));
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < column_ids_no_virtual.count(); ++i) {
    const ObColumnSchemaV2* column_schema = NULL;
    const int64_t column_id = column_ids_no_virtual.at(i).col_id_;
    ObColumnParam* column = NULL;
    if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
    } else if (OB_FAIL(alloc_column(allocator_, column))) {
      LOG_WARN("fail to allocate column", K(ret), K(i));
    } else if (OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
      LOG_WARN("fail to convert column schema to param", K(ret), K(*column_schema), K(i));
    } else if (OB_FAIL(tmp_cols.push_back(column))) {
      LOG_WARN("fail to push back column param", K(ret));
    }
  }

  // projector
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < tmp_cols.count(); ++i) {
      if (OB_FAIL(tmp_projector.push_back(i))) {
        STORAGE_LOG(WARN, "fail to push back projector", K(ret));
      }
    }
  }

  // assign final result
  if (OB_SUCC(ret)) {
    if (tmp_cols.count() > 0) {
      if (OB_FAIL(full_cols.assign(tmp_cols))) {
        LOG_WARN("fail to assign columns", K(ret));
      } else if (OB_FAIL(full_projector.assign(tmp_projector))) {
        LOG_WARN("fail to assign projector", K(ret));
      } else if (OB_FAIL(create_column_map(full_cols, full_col_map))) {
        LOG_WARN("fail to create column map", K(ret));
      }
    }
  }
  return ret;
}

int ObTableParam::construct_columns_and_projector(const ObTableSchema& table_schema,
    const common::ObIArray<uint64_t>& output_column_ids, common::ObIArray<ObColumnParam*>& cols, ColumnMap& col_map,
    common::ObIArray<int32_t>& projector, common::ObIArray<int32_t>& output_projector)
{
  int ret = OB_SUCCESS;
  static const int64_t COMMON_COLUMN_NUM = 16;
  ObSEArray<ObColumnParam*, COMMON_COLUMN_NUM> tmp_cols;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_projector;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_output_projector;

  // rowkey columns front, other columns behind
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> column_ids_no_virtual;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> index_column_ids;
  int32_t rowkey_count = 0;
  if (OB_FAIL(table_schema.get_column_ids(column_ids_no_virtual, true))) {
    LOG_WARN("get column ids no virtual failed", K(ret));
  }

  // column array
  if (OB_SUCC(ret)) {
    const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
    rowkey_count = rowkey_info.get_size();
    // add rowkey columns
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      const ObRowkeyColumn* rowkey_column = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      ObColumnParam* column = NULL;
      if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
      } else if (OB_FAIL(alloc_column(allocator_, column))) {
        LOG_WARN("alloc column failed", K(ret), K(i));
      } else if (OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
        LOG_WARN("convert failed", K(ret), K(*column_schema), K(i));
      } else {
        ret = tmp_cols.push_back(column);
      }
    }
    // add other columns
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      const uint64_t column_id = output_column_ids.at(i);
      const ObColumnSchemaV2* column_schema = NULL;
      ObColumnParam* column = NULL;
      if (OB_UNLIKELY(common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id) ||
          OB_UNLIKELY(common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id)) {
        if (OB_FAIL(alloc_column(allocator_, column))) {
          LOG_WARN("alloc column failed", K(ret), K(i));
        } else {
          ObObjMeta meta_type;
          meta_type.set_int();
          column->set_column_id(column_id);
          column->set_meta_type(meta_type);
          ret = tmp_cols.push_back(column);
        }
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL", K(table_schema.get_table_id()), K(column_id), K(i));
      } else if (OB_UNLIKELY(column_schema->get_data_type() == ObLobType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpecte ObLobType column to init table scan", K(ret), KPC(column_schema));
      } else if (!column_schema->is_rowkey_column()) {
        if (OB_FAIL(alloc_column(allocator_, column))) {
          LOG_WARN("alloc column failed", K(ret), K(i));
        } else if (OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
          LOG_WARN("convert failed", K(*column_schema), K(ret), K(i));
        } else {
          ret = tmp_cols.push_back(column);
        }
      }
    }
  }

  // projector
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < tmp_cols.count(); ++i) {
      const ObColumnParam* column = tmp_cols.at(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL", K(ret), K(i));
      } else {
        int32_t idx = OB_INVALID_INDEX;
        if (common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column->get_column_id()) {
          idx = storage::ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(rowkey_count,
              storage::ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(
                  storage::ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0));
        } else if (common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column->get_column_id()) {
          idx = storage::ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(rowkey_count,
              storage::ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(
                  storage::ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0));
        } else {
          for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < column_ids_no_virtual.count(); ++j) {
            if (column->get_column_id() == column_ids_no_virtual.at(j).col_id_) {
              idx = j;
            }
          }
        }
        if (OB_SUCC(ret)) {
          ret = tmp_projector.push_back(idx);
        }
      }
    }
  }

  // output projector
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      int32_t idx = OB_INVALID_INDEX;
      for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < tmp_cols.count(); ++j) {
        const ObColumnParam* column = tmp_cols.at(j);
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is NULL", K(ret), K(j));
        } else if (output_column_ids.at(i) == column->get_column_id()) {
          idx = j;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_INVALID_INDEX == idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret));
        } else {
          ret = tmp_output_projector.push_back(idx);
        }
      }
    }
  }

  // assign
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cols.assign(tmp_cols))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(projector.assign(tmp_projector))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(output_projector.assign(tmp_output_projector))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(create_column_map(cols, col_map))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }

  return ret;
}

int ObTableParam::filter_common_columns(
    const ObIArray<const ObColumnSchemaV2*>& columns, ObIArray<const ObColumnSchemaV2*>& new_columns)
{
  int ret = OB_SUCCESS;
  new_columns.reset();

  for (int64_t i = 0; i < columns.count() && OB_SUCC(ret); ++i) {
    const ObColumnSchemaV2* column = columns.at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else {
      bool is_exists = false;
      for (int64_t j = 0; j < new_columns.count() && OB_SUCC(ret) && !is_exists; ++j) {
        const ObColumnSchemaV2* new_column = new_columns.at(j);
        if (OB_ISNULL(new_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(j));
        } else {
          is_exists = column->get_column_id() == new_column->get_column_id();
        }
      }
      if (OB_SUCC(ret) && !is_exists) {
        ret = new_columns.push_back(column);
      }
    }
  }
  return ret;
}

int ObTableParam::construct_columns_and_projector_for_index(const ObTableSchema& table_schema,
    const ObTableSchema& index_schema, const common::ObIArray<uint64_t>& output_column_ids, Columns& cols,
    ColumnMap& col_map, Projector& projector, Projector& output_projector)
{
  int ret = OB_SUCCESS;

  ObArray<ObColumnParam*> tmp_cols;
  ObArray<int32_t> tmp_projector;
  ObArray<int32_t> tmp_output_projector;
  bool has_rowid_column = false;
  const ObColumnSchemaV2* rowid_column_schema = NULL;

  // rowkey columns front, other columns behind
  ObArray<ObColDesc> column_ids;
  if (OB_FAIL(index_schema.get_column_ids(column_ids))) {
    LOG_WARN("get column ids failed", K(ret));
  } else if (OB_FAIL(index_schema.has_column(OB_HIDDEN_ROWID_COLUMN_ID, has_rowid_column))) {
    LOG_WARN("fail to judge if has rowid column", K(ret));
  } else if (has_rowid_column &&
             OB_ISNULL(rowid_column_schema = index_schema.get_column_schema(OB_HIDDEN_ROWID_COLUMN_ID))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The rowid column schema is NULL", K(ret), K(index_schema));
  }

  // column array
  if (OB_SUCC(ret)) {
    ObArray<const ObColumnSchemaV2*> column_schemas_all;
    // add rowkey of index_table
    {
      const ObRowkeyInfo& rowkey_info = index_schema.get_rowkey_info();
      for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        const ObRowkeyColumn* rowkey_column = NULL;
        const ObColumnSchemaV2* column_schema = NULL;
        if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
        } else if (OB_ISNULL(column_schema = index_schema.get_column_schema(rowkey_column->column_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
        } else {
          ret = column_schemas_all.push_back(column_schema);
        }
      }
    }
    // add rowkey of data_table
    {
      const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
      for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        const ObRowkeyColumn* rowkey_column = NULL;
        const ObColumnSchemaV2* column_schema = NULL;
        if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
        } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_column->column_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
        } else {
          ret = column_schemas_all.push_back(column_schema);
        }
      }
    }
    // add rowid column of index_table
    if (OB_SUCC(ret) && has_rowid_column && OB_FAIL(column_schemas_all.push_back(rowid_column_schema))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to push back rowid column schema", K(ret));
    }

    // add output columns
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      const uint64_t column_id = output_column_ids.at(i);
      const ObColumnSchemaV2* column_schema = NULL;
      if (NULL == (column_schema = index_schema.get_column_schema(column_id))) {
        // do-nothing
      } else {
        ret = column_schemas_all.push_back(column_schema);
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<const ObColumnSchemaV2*> column_schemas;
      if (OB_FAIL(filter_common_columns(column_schemas_all, column_schemas))) {
        LOG_WARN("filter common columns failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
        const ObColumnSchemaV2* column_schema = column_schemas.at(i);
        ObColumnParam* column = NULL;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(i));
        } else if (OB_FAIL(alloc_column(allocator_, column))) {
          LOG_WARN("alloc column failed", K(ret), K(i));
        } else if (OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
          LOG_WARN("convert failed", K(*column_schema), K(ret), K(i));
        } else {
          ret = tmp_cols.push_back(column);
        }
      }
    }
  }

  // projector
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < tmp_cols.count(); ++i) {
      const ObColumnParam* column = tmp_cols.at(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i));
      }
      int32_t idx = OB_INVALID_INDEX;
      for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < column_ids.count(); ++j) {
        if (column_ids.at(j).col_id_ == column->get_column_id()) {
          idx = j;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_INVALID_INDEX == idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret));
        } else {
          ret = tmp_projector.push_back(idx);
        }
      }
    }
  }

  // output projector
  if (OB_SUCC(ret)) {
    ObArray<const ObColumnSchemaV2*> column_schemas_all;
    // add rowkey of data_table
    const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      const ObRowkeyColumn* rowkey_column = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
      } else {
        ret = column_schemas_all.push_back(column_schema);
      }
    }
    // When index back, if exist row id, it must be put after main table row key columns.
    if (OB_SUCC(ret) && has_rowid_column && OB_FAIL(column_schemas_all.push_back(rowid_column_schema))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to push back rowid column schema", K(ret));
    }

    // add output columns
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      const uint64_t column_id = output_column_ids.at(i);
      const ObColumnSchemaV2* column_schema = NULL;
      if (NULL == (column_schema = index_schema.get_column_schema(column_id))) {
        // do-nothing
      } else {
        ret = column_schemas_all.push_back(column_schema);
      }
    }
    ObArray<const ObColumnSchemaV2*> column_schemas;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filter_common_columns(column_schemas_all, column_schemas))) {
        LOG_WARN("filter common columns failed", K(ret));
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
      const ObColumnSchemaV2* column_schema = column_schemas.at(i);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i));
      }
      int32_t idx = OB_INVALID_INDEX;
      for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < tmp_cols.count(); ++j) {
        const ObColumnParam* column = tmp_cols.at(j);
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is NULL", K(ret), K(j));
        } else if (column_schema->get_column_id() == column->get_column_id()) {
          idx = j;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_INVALID_INDEX == idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret));
        } else {
          ret = tmp_output_projector.push_back(idx);
        }
      }
    }
  }

  // assign
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cols.assign(tmp_cols))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(projector.assign(tmp_projector))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(output_projector.assign(tmp_output_projector))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(create_column_map(cols, col_map))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }

  return ret;
}

int ObTableParam::construct_pad_projector(
    const Columns& dst_columns, const Projector& dst_output_projector, Projector& pad_projector)
{
  int ret = OB_SUCCESS;

  ObArray<int32_t> pad_col_projector;
  for (int32_t i = 0; OB_SUCC(ret) && i < dst_output_projector.count(); ++i) {
    const ObColumnParam* column = dst_columns.at(dst_output_projector.at(i));
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else if (column->get_meta_type().is_char() || column->get_meta_type().is_nchar()) {
      ret = pad_col_projector.push_back(i);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pad_projector.assign(pad_col_projector))) {
      LOG_WARN("assign failed", K(ret));
    }
  }
  return ret;
}

int ObTableParam::construct_storage_param()
{
  int ret = OB_SUCCESS;
  const int64_t column_count = cols_.count();
  const int64_t index_column_count = index_cols_.count();
  const int64_t full_column_count = full_cols_.count();
  const int64_t max_column_count = MAX3(column_count, index_column_count, full_column_count);
  ObSEArray<ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> tmp_col_descs;
  ObSEArray<ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> tmp_index_col_descs;
  ObSEArray<ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> tmp_full_col_descs;
  const share::schema::ObColumnParam* col = nullptr;
  share::schema::ObColDesc desc;
  for (int32_t i = 0; OB_SUCC(ret) && i < max_column_count; ++i) {
    if (i < column_count) {
      col = cols_.at(i);
      desc.col_id_ = col->get_column_id();
      desc.col_type_ = col->get_meta_type();
      desc.col_order_ = col->get_column_order();
      if (OB_FAIL(tmp_col_descs.push_back(desc))) {
        LOG_WARN("add output columns failed", K(ret));
      }
    }

    if (i < index_column_count) {
      col = index_cols_.at(i);
      desc.col_id_ = col->get_column_id();
      desc.col_type_ = col->get_meta_type();
      desc.col_order_ = col->get_column_order();
      if (OB_FAIL(tmp_index_col_descs.push_back(desc))) {
        LOG_WARN("add index output columns failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && i < full_column_count) {
      col = full_cols_.at(i);
      desc.col_id_ = col->get_column_id();
      desc.col_type_ = col->get_meta_type();
      desc.col_order_ = col->get_column_order();
      if (OB_FAIL(tmp_full_col_descs.push_back(desc))) {
        LOG_WARN("add full output columns failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(col_descs_.assign(tmp_col_descs))) {
      LOG_WARN("fail to assign column description", K(ret));
    } else if (OB_FAIL(index_col_descs_.assign(tmp_index_col_descs))) {
      LOG_WARN("fail to assign index column description", K(ret));
    } else if (OB_FAIL(full_col_descs_.assign(tmp_full_col_descs))) {
      LOG_WARN("fail to assign full column description", K(ret));
    }
  }
  return ret;
}

int ObTableParam::convert(const ObTableSchema& table_schema, const ObTableSchema& index_schema,
    const ObIArray<uint64_t>& output_column_ids, const bool index_back)
{
  int ret = OB_SUCCESS;
  if (!index_back) {
    // if mocked rowid index is used
    // we keep the index_id_, but use table schema to generate the col infos
    // because eventually, we use primary key to do table scan
    table_id_ = index_schema.get_table_id();
    const ObTableSchema& table_schema_to_use =
        ObSQLMockSchemaUtils::is_mock_index(table_id_) ? table_schema : index_schema;
    schema_version_ = table_schema_to_use.get_schema_version();
    main_table_rowkey_cnt_ = table_schema_to_use.get_rowkey_column_num();
    if (OB_FAIL(construct_columns_and_projector(
            table_schema_to_use, output_column_ids, cols_, col_map_, projector_, output_projector_))) {
      LOG_WARN("construct failed", K(ret));
    } else if (OB_FAIL(construct_full_columns_and_projector(
                   table_schema_to_use, full_cols_, full_projector_, full_col_map_))) {
      LOG_WARN("fail to construct full columns", K(ret));
    } else if (OB_FAIL(construct_pad_projector(cols_, output_projector_, pad_col_projector_))) {
      LOG_WARN("Fail to construct pad projector, ", K(ret));
    } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 && lib::is_oracle_mode() &&
               !is_sys_table(table_schema_to_use.get_table_id()) &&
               OB_FAIL(construct_lob_locator_param(table_schema_to_use,
                   cols_,
                   output_projector_,
                   use_lob_locator_,
                   rowid_version_,
                   rowid_projector_))) {
      LOG_WARN("fail to construct rowid dep column projector", K(ret));
    } else if (OB_FAIL(construct_storage_param())) {
      LOG_WARN("Fail to construct storage param, ", K(ret));
    } else {
      LOG_DEBUG("construct columns", K(cols_), K(output_column_ids), K(full_cols_));
    }
  } else {
    table_id_ = table_schema.get_table_id();
    index_id_ = index_schema.get_table_id();
    schema_version_ = table_schema.get_schema_version();
    index_schema_version_ = index_schema.get_schema_version();
    main_table_rowkey_cnt_ = table_schema.get_rowkey_column_num();
    index_table_rowkey_cnt_ = index_schema.get_rowkey_column_num();

    if (OB_FAIL(construct_columns_and_projector_for_index(table_schema,
            index_schema,
            output_column_ids,
            index_cols_,
            index_col_map_,
            index_projector_,
            index_output_projector_))) {
      LOG_WARN("construct failed", K(ret));
    } else if (OB_FAIL(construct_columns_and_projector(
                   table_schema, output_column_ids, cols_, col_map_, projector_, output_projector_))) {
      LOG_WARN("construct failed", K(ret));
    } else if (OB_FAIL(
                   construct_full_columns_and_projector(table_schema, full_cols_, full_projector_, full_col_map_))) {
      LOG_WARN("fail to construct full columns", K(ret));
    } else if (OB_FAIL(construct_pad_projector(cols_, output_projector_, pad_col_projector_))) {
      LOG_WARN("Fail to construct pad projector, ", K(ret));
    } else if (OB_FAIL(construct_storage_param())) {
      LOG_WARN("Fail to construct storage param, ", K(ret));
    } else {
      LOG_DEBUG("construct columns", K(index_cols_), K(cols_), K(output_column_ids), K(full_cols_));
      ObArray<int32_t> index_back_projector;
      // index back projector
      for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
        int32_t idx = OB_INVALID_INDEX;
        for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < index_output_projector_.count(); ++j) {
          int32_t pos = index_output_projector_.at(j);
          const ObColumnParam* column = NULL;
          if (!(pos >= 0 && pos < index_cols_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected pos", K(ret), K(pos), K(index_cols_.count()));
          } else if (OB_ISNULL(column = index_cols_.at(pos))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret));
          } else if (output_column_ids.at(i) == column->get_column_id()) {
            idx = j;
          }
        }
        if (OB_SUCC(ret)) {
          ret = index_back_projector.push_back(idx);
        }
      }
      // assign
      if (OB_SUCC(ret)) {
        if (OB_FAIL(index_back_projector_.assign(index_back_projector))) {
          LOG_WARN("assign failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 && lib::is_oracle_mode() &&
          !is_sys_table(table_schema.get_table_id()) &&
          OB_FAIL(construct_lob_locator_param(
              table_schema, cols_, output_projector_, use_lob_locator_, rowid_version_, rowid_projector_))) {
        LOG_WARN("fail to construct rowid dep column projector", K(ret));
      }
    }
  }

  return ret;
}

int ObTableParam::construct_lob_locator_param(const ObTableSchema& table_schema, const Columns& storage_project_columns,
    const Projector& access_projector, bool& use_lob_locator, int64_t& rowid_version, Projector& rowid_projector)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnParam* col_param = nullptr;
  use_lob_locator = false;
  if (!(table_schema.is_sys_table() || table_schema.is_sys_view() || table_schema.is_vir_table())) {
    for (int64_t i = 0; OB_SUCC(ret) && !use_lob_locator && i < access_projector.count(); i++) {
      int32_t idx = access_projector.at(i);
      if (OB_ISNULL(col_param = storage_project_columns.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null col param", K(ret), K(idx), K(storage_project_columns));
      } else {
        use_lob_locator = col_param->get_meta_type().get_type() == ObLongTextType;
      }
    }
  }

  // Virtual table may not contain primary key columns, i.e. TENANT_VIRTUAL_SESSION_VARIABLE.
  // When access such virtual table, get_column_ids_serialize_to_rowid may return failure because
  // of the null rowkey info. So here skip the lob locator.
  if (use_lob_locator && 0 == table_schema.get_rowkey_info().get_size()) {
    use_lob_locator = false;
  }

  // generate rowid_projector
  if (use_lob_locator && OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> rowid_col_ids;
    int64_t rowkey_col_cnt = 0;
    if (OB_FAIL(table_schema.get_column_ids_serialize_to_rowid(rowid_col_ids, rowkey_col_cnt))) {
      LOG_WARN("Failed to get columns needed by rowid", K(ret));
    } else if (OB_FAIL(rowid_projector.init(rowid_col_ids.count()))) {
      LOG_WARN("Failed to init rowid projector", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_col_ids.count(); i++) {
        bool exist = false;
        for (int64_t j = 0; OB_SUCC(ret) && !exist && j < access_projector.count(); j++) {
          int32_t idx = access_projector.at(j);
          if (OB_ISNULL(col_param = storage_project_columns.at(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected null col param", K(ret), K(j), K(storage_project_columns));
          } else if (rowid_col_ids.at(i) == col_param->get_column_id()) {
            if (OB_FAIL(rowid_projector.push_back(j))) {
              LOG_WARN("Failed to push back rowid project", K(ret));
            } else {
              exist = true;
            }
          }
        }  // access projector end
        if (!exist) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("column which rowid dependent is not exist", K(rowid_col_ids.at(i)), K(rowid_col_ids), K(ret));
        }
      }  // rowid col ids end
    }
  }
  // generate rowid_version_
  if (OB_SUCC(ret)) {
    if (table_schema.is_old_no_pk_table()) {
      rowid_version = ObURowIDData::INVALID_ROWID_VERSION;
    } else if (table_schema.is_new_no_pk_table()) {
      rowid_version = ObURowIDData::NO_PK_ROWID_VERSION;
    } else {
      rowid_version = ObURowIDData::PK_ROWID_VERSION;
    }
  }
  LOG_TRACE("construct lob locator param", K(use_lob_locator), K(rowid_projector), K(rowid_version));

  return ret;
}

int ObTableParam::convert_join_mv_rparam(
    const ObTableSchema& mv_schema, const ObTableSchema& right_schema, const common::ObIArray<uint64_t>& mv_column_ids)
{
  int ret = OB_SUCCESS;
  if (!mv_schema.is_materialized_view() || mv_column_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mv_schema), K(mv_column_ids));
  }
  table_id_ = right_schema.get_table_id();
  schema_version_ = right_schema.get_schema_version();
  main_table_rowkey_cnt_ = right_schema.get_rowkey_column_num();

  ObArray<ObColumnParam*> cols;
  ObArray<uint64_t> tmp_cols;

  ObArray<int32_t> projector;
  ObArray<int32_t> output_projector;
  ObArray<int32_t> join_key_projector;
  ObArray<int32_t> right_key_projector;
  ObArray<ObColDesc> column_ids;

  const ObRowkeyInfo& pkinfo = right_schema.get_rowkey_info();
  // add pk columns
  for (int64_t i = 0; OB_SUCC(ret) && i < pkinfo.get_size(); ++i) {
    const ObRowkeyColumn* pk_col = pkinfo.get_column(i);
    if (NULL == pk_col) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get rowkey column failed", K(ret), K(pkinfo), K(i));
    } else if (OB_FAIL(tmp_cols.push_back(pk_col->column_id_))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }

  // add other columns (pk columns may be added again)
  FOREACH_CNT_X(cid, mv_column_ids, OB_SUCC(ret))
  {
    uint64_t org_tid = 0;
    uint64_t org_cid = 0;
    if (OB_FAIL(mv_schema.convert_to_depend_table_column(*cid, org_tid, org_cid))) {
      LOG_WARN("convert column id to origin table column id failed", K(ret));
    } else if (org_tid == right_schema.get_table_id()) {
      if (OB_FAIL(tmp_cols.push_back(org_cid))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }

  FOREACH_CNT_X(cid, tmp_cols, OB_SUCC(ret))
  {
    const ObColumnSchemaV2* col = right_schema.get_column_schema(*cid);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL column schema", K(ret), K(right_schema), "cid", *cid);
    } else {
      if (cols.count() < pkinfo.get_size() || !col->is_rowkey_column()) {
        ObColumnParam* cp = nullptr;
        if (OB_FAIL(alloc_column(allocator_, cp))) {
          LOG_WARN("alloc column parameter failed", K(ret));
        } else if (OB_FAIL(convert_column_schema_to_param(*col, *cp))) {
          LOG_WARN("convert to column parameter failed", K(ret), "col", *col);
        } else if (OB_FAIL(cols.push_back(cp))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }

  // projector && output projector
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(right_schema.get_column_ids(column_ids))) {
    LOG_WARN("get table columns failed", K(ret), K(right_schema));
  } else {
    FOREACH_CNT_X(col, cols, OB_SUCC(ret))
    {
      int64_t idx = OB_INVALID_INDEX;
      for (int64_t i = 0; i < column_ids.count(); i++) {
        if ((*col)->get_column_id() == column_ids.at(i).col_id_) {
          idx = i;
          break;
        }
      }
      if (OB_INVALID_INDEX == idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column not found", K(ret), "col", *(col));
      } else if (OB_FAIL(projector.push_back(static_cast<int32_t>(idx)))) {
        LOG_WARN("array push back failed", K(ret));
      }

      idx = OB_INVALID_INDEX;
      for (int64_t i = 0; OB_SUCC(ret) && i < mv_column_ids.count(); ++i) {
        uint64_t org_tid = 0;
        uint64_t org_cid = 0;
        if (OB_FAIL(mv_schema.convert_to_depend_table_column(mv_column_ids.at(i), org_tid, org_cid))) {
          LOG_WARN("convert column id to origin table column id failed", K(ret));
        } else if (org_tid == right_schema.get_table_id() && org_cid == (*col)->get_column_id()) {
          idx = i;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        // if %col not exist in %mv_column_ids, we push OB_INVALID_INDEX to %output_projector
        if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(idx)))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }

  // join key projector
  for (int64_t i = 0; OB_SUCC(ret) && i < main_table_rowkey_cnt_ && i < cols.count(); ++i) {
    int64_t join_cid = OB_INVALID_ID;
    if (mv_schema.get_join_conds().count() % 2 != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid join condition count", K(ret), "join_cnd_cnt", mv_schema.get_join_conds().count());
    } else {
      for (int64_t j = 0; j < mv_schema.get_join_conds().count(); j += 2) {
        const std::pair<uint64_t, uint64_t>& t1 = mv_schema.get_join_conds().at(j);
        const std::pair<uint64_t, uint64_t>& t2 = mv_schema.get_join_conds().at(j + 1);
        if (t1.first == right_schema.get_table_id() && t1.second == cols.at(i)->get_column_id()) {
          join_cid = t2.second;
        } else if (t2.first == right_schema.get_table_id() && t2.second == cols.at(i)->get_column_id()) {
          join_cid = t1.second;
        }
      }
      if (OB_INVALID_ID == join_cid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join column not found", K(ret), K(mv_schema), K(right_schema));
      }
    }
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t j = 0; OB_SUCC(ret) && j < mv_column_ids.count(); ++j) {
      if (mv_column_ids.at(j) == join_cid) {
        idx = j;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_INDEX == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right table primary key not found in MV output join keys", K(ret), K(i), K(mv_column_ids));
    } else if (OB_FAIL(join_key_projector.push_back(static_cast<int32_t>(idx)))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }

  // right table pk projector
  for (int64_t i = 0; OB_SUCC(ret) && i < main_table_rowkey_cnt_ && i < cols.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t j = 0; OB_SUCC(ret) && j < mv_column_ids.count(); ++j) {
      uint64_t org_tid = 0;
      uint64_t org_cid = 0;
      if (OB_FAIL(mv_schema.convert_to_depend_table_column(mv_column_ids.at(j), org_tid, org_cid))) {
        LOG_WARN("convert column id to origin table column id failed", K(ret));
      } else if (org_tid == right_schema.get_table_id() && org_cid == cols.at(i)->get_column_id()) {
        idx = j;
        break;
      }
    }
    if (OB_INVALID_INDEX == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right table primary key not found in MV output columns", K(ret), K(i), K(mv_column_ids));
    } else if (OB_FAIL(right_key_projector.push_back(static_cast<int32_t>(idx)))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cols_.assign(cols))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(projector_.assign(projector))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(output_projector_.assign(output_projector))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(join_key_projector_.assign(join_key_projector))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(right_key_projector_.assign(right_key_projector))) {
    LOG_WARN("array assign failed", K(ret));
  } else if (OB_FAIL(create_column_map(cols_, col_map_))) {
    LOG_WARN("create column map failed", K(ret));
  }
  return ret;
}

int ObTableParam::convert_schema_param(
    const share::schema::ObTableSchemaParam& schema_param, const common::ObIArray<uint64_t>& output_column_ids)
{
  int ret = OB_SUCCESS;
  if (!schema_param.is_valid() || 0 == output_column_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_param), K(output_column_ids));
  } else if (schema_param.get_rowkey_column_num() != output_column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output column ids should be row key columns", K(ret), K(schema_param), K(output_column_ids));
  } else {
    const int64_t COMMON_COLUMN_NUM = 16;
    const ObColumnParam* src_col = NULL;
    ObSEArray<ObColumnParam*, COMMON_COLUMN_NUM> tmp_cols;
    ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_projector;
    ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_output_projector;

    table_id_ = schema_param.get_table_id();
    schema_version_ = schema_param.get_schema_version();
    main_table_rowkey_cnt_ = schema_param.get_rowkey_column_num();
    for (int32_t i = 0; OB_SUCC(ret) && i < schema_param.get_rowkey_column_num(); ++i) {
      ObColumnParam* dst_col = nullptr;
      if (NULL == (src_col = schema_param.get_rowkey_column_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column param is NULL", K(ret), K(i));
      } else if (src_col->get_column_id() != output_column_ids.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row key column id not match", K(ret), K(i), K(*src_col), K(output_column_ids));
      } else if (OB_FAIL(alloc_column(allocator_, dst_col))) {
        LOG_WARN("alloc column failed", K(ret), K(i));
      } else if (OB_FAIL(dst_col->assign(*src_col))) {
        LOG_WARN("assign column failed", K(ret), K(i));
      } else if (OB_FAIL(tmp_cols.push_back(dst_col))) {
        LOG_WARN("push back column failed", K(ret), K(i));
      } else if (OB_FAIL(tmp_projector.push_back(i))) {
        LOG_WARN("push back projector failed", K(ret), K(i));
      } else if (OB_FAIL(tmp_output_projector.push_back(i))) {
        LOG_WARN("push back output projector failed", K(ret), K(i));
      }
    }

    // assign
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cols_.assign(tmp_cols))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(projector_.assign(tmp_projector))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(output_projector_.assign(tmp_output_projector))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(create_column_map(cols_, col_map_))) {
        LOG_WARN("failed to create column map", K(ret));
      }
    }
  }
  return ret;
}

int ObTableParam::alloc_column(ObIAllocator& allocator, ObColumnParam*& col_ptr)
{
  int ret = OB_SUCCESS;
  void* tmp_ptr = nullptr;
  if (OB_ISNULL(tmp_ptr = allocator.alloc(sizeof(ObColumnParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    col_ptr = new (tmp_ptr) ObColumnParam(allocator);
  }
  return ret;
}

int ObTableParam::create_column_map(const common::ObIArray<ObColumnParam*>& cols, ColumnMap& col_map)
{
  int ret = OB_SUCCESS;

  if (!col_map.is_inited()) {
    if (OB_FAIL(col_map.init(cols))) {
      LOG_WARN("init map failed", K(ret));
    }
  }

  return ret;
}

int ObTableParam::convert_column_schema_to_param(const ObColumnSchemaV2& column_schema, ObColumnParam& column_param)
{
  int ret = OB_SUCCESS;

  column_param.set_column_id(column_schema.get_column_id());
  column_param.set_meta_type(column_schema.get_meta_type());
  column_param.set_column_order(column_schema.get_order_in_rowkey());
  column_param.set_accuracy(column_schema.get_accuracy());
  column_param.set_nullable(column_schema.is_nullable());
  if (column_schema.is_generated_column() || OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_schema.get_column_id()) {
    ObObj nop_obj;
    nop_obj.set_nop_value();
    ret = column_param.set_orig_default_value(nop_obj);
    if (OB_SUCC(ret)) {
      ret = column_param.set_cur_default_value(nop_obj);
    }
  } else {
    ret = column_param.set_orig_default_value(column_schema.get_orig_default_value());
    if (OB_SUCC(ret)) {
      ret = column_param.set_cur_default_value(column_schema.get_cur_default_value());
    }
  }
  return ret;
}

int64_t ObTableParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
      K_(index_id),
      K_(schema_version),
      K_(index_schema_version),
      K_(main_table_rowkey_cnt),
      K_(index_table_rowkey_cnt),
      "column_array",
      ObArrayWrap<ObColumnParam*>(0 == cols_.count() ? NULL : &cols_.at(0), cols_.count()),
      K_(projector),
      K_(output_projector),
      "index_column_array",
      ObArrayWrap<ObColumnParam*>(0 == index_cols_.count() ? NULL : &index_cols_.at(0), index_cols_.count()),
      K_(index_projector),
      K_(index_output_projector),
      K_(index_back_projector),
      K_(pad_col_projector),
      K_(full_cols),
      K_(full_projector),
      K_(use_lob_locator),
      K_(rowid_version),
      K_(rowid_projector));
  J_OBJ_END();

  return pos;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
