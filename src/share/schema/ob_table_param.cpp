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
#include "lib/ob_errno.h"
#include "ob_multi_version_schema_service.h"
#include "ob_table_param.h"
#include "ob_table_schema.h"
#include "observer/ob_server.h"
#include "storage/ob_storage_schema.h"
#include "storage/access/ob_table_read_info.h"
#include "share/ob_lob_access_utils.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
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
    HashNode **buckets =
      static_cast<HashNode **>(allocator_.alloc(bucket_num * sizeof(HashNode *)));
    if (NULL == buckets) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buckets failed", K(ret));
    } else {
      MEMSET(buckets, 0, bucket_num * sizeof(HashNode *));
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
    HashNode *&bucket = buckets_[key % bucket_num_];
    HashNode *dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", K(key), K(ret));
    } else if (NULL != dst_node) {
      ret = OB_HASH_EXIST;
      LOG_WARN("key already exists", K(key), K(ret));
    } else {
      HashNode *new_node =
        static_cast<HashNode *>(allocator_.alloc(sizeof(HashNode)));
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

int ColumnHashMap::get(const uint64_t key, int32_t &value) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HashNode *&bucket = buckets_[key % bucket_num_];
    HashNode *dst_node = NULL;
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

int ColumnHashMap::find_node(const uint64_t key, HashNode *head, HashNode *&node) const
{
  int ret = OB_SUCCESS;
  node = NULL;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == head) {
    // do-nothing
  } else {
    HashNode *cur_node = head;
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

int ColumnMap::init(const common::ObIArray<ObColumnParam *> &column_params)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    ObSEArray<uint64_t, 10> column_ids;
    for (int64_t i = 0; i < column_params.count() && OB_SUCC(ret); ++i) {
      ObColumnParam *column = nullptr;
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
int ColumnMap::init(const common::ObIArray<ObColDesc> &column_descs)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    ObSEArray<uint64_t, 10> column_ids;
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

int ColumnMap::init(const common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    uint64_t max_column_id = OB_INVALID_ID;
    uint64_t max_shadow_column_id = OB_INVALID_ID;
    ObSEArray<int32_t, 10> non_shadow_columns;
    ObSEArray<int32_t, 10> shadow_columns;
    for (int32_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const uint64_t column_id = column_ids.at(i);
      if (OB_INVALID_ID == column_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column_id", K(column_id), K(ret));
      } else {
        if (!IS_SHADOW_COLUMN(column_id)) {
          if (OB_INVALID_ID == max_column_id
                     || column_id > max_column_id) {
            max_column_id = column_id;
          }
          ret = non_shadow_columns.push_back(i);
        } else {
          if (OB_INVALID_ID == max_shadow_column_id
                     || column_id > max_shadow_column_id) {
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

int ColumnMap::create(const bool use_array,
                      const int64_t array_size,
                      const int64_t offset,
                      const common::ObIArray<uint64_t> &column_ids,
                      const common::ObIArray<int32_t> &column_indexes,
                      ColumnArray &array,
                      ColumnHashMap &map)
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

int ColumnMap::get(const uint64_t column_id, int32_t &proj) const
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
  if (use_array) {                                    \
    int64_t idx = column_id - offset;                 \
    if (idx < 0) {                                    \
      ret = OB_ERR_UNEXPECTED;                        \
      LOG_WARN("unexpected idx", K(idx), K(ret));     \
    } else if (idx >= array.count()) {                \
      proj = OB_INVALID_INDEX;                        \
    } else {                                          \
      proj = array.at(idx);                           \
    }                                                 \
  } else {                                            \
    if (OB_FAIL(map.get(column_id, proj))) {          \
      if (OB_HASH_NOT_EXIST != ret) {                 \
        LOG_WARN("get failed", K(column_id), K(ret)); \
      } else {                                        \
        proj = OB_INVALID_INDEX;                      \
        ret = OB_SUCCESS;                             \
      }                                               \
    }                                                 \
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

ObColumnParam::ObColumnParam(ObIAllocator &allocator)
    : allocator_(allocator)
{
  reset();
}

ObColumnParam::~ObColumnParam()
{
}

void ObColumnParam::reset()
{
  column_id_ = OB_INVALID_ID;
  meta_type_.reset();
  order_ = ObOrderType::ASC;
  accuracy_.reset();
  orig_default_value_.reset();
  cur_default_value_.reset();
  is_nullable_for_write_ = false;
  is_nullable_for_read_ = false;
  is_gen_col_ = false;
  is_virtual_gen_col_ = false;
  is_gen_col_udf_expr_ = false;
  is_hidden_ = false;
}

void ObColumnParam::destroy()
{
  if (orig_default_value_.need_deep_copy()) {
    allocator_.free(orig_default_value_.get_deep_copy_obj_ptr());
    orig_default_value_.reset();
  }
  if (cur_default_value_.need_deep_copy()) {
    allocator_.free(cur_default_value_.get_deep_copy_obj_ptr());
    cur_default_value_.reset();
  }
  ObColumnParam::reset();
}

int ObColumnParam::deep_copy_obj(const ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (size > 0) {
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
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

  LST_DO_CODE(OB_UNIS_ENCODE,
              column_id_,
              meta_type_,
              accuracy_,
              orig_default_value_,
              cur_default_value_,
              order_,
              is_nullable_for_write_,
              is_gen_col_,
              is_virtual_gen_col_,
              is_gen_col_udf_expr_,
              is_nullable_for_read_,
              is_hidden_);
  return ret;
}

OB_DEF_DESERIALIZE(ObColumnParam)
{
  int ret = OB_SUCCESS;
  ObObj orig_default_value;
  ObObj cur_default_value;

  LST_DO_CODE(OB_UNIS_DECODE,
              column_id_,
              meta_type_,
              accuracy_,
              orig_default_value,
              cur_default_value,
              order_);

  // compatibility code
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, is_nullable_for_write_))) {
        LOG_WARN("failed to decode index_schema_version_", K(ret));
      }
    } else {
      is_nullable_for_write_ = false;
    }
  }
  OB_UNIS_DECODE(is_gen_col_);
  OB_UNIS_DECODE(is_virtual_gen_col_);
  OB_UNIS_DECODE(is_gen_col_udf_expr_);
  OB_UNIS_DECODE(is_nullable_for_read_);
  OB_UNIS_DECODE(is_hidden_);

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
              is_nullable_for_write_,
              is_nullable_for_read_,
              is_gen_col_,
              is_virtual_gen_col_,
              is_gen_col_udf_expr_,
              is_hidden_);
  return len;
}

int ObColumnParam::assign(const ObColumnParam &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    column_id_ = other.column_id_;
    meta_type_ = other.meta_type_;
    order_ = other.order_;
    accuracy_ = other.accuracy_;
    is_nullable_for_write_ = other.is_nullable_for_write_;
    is_nullable_for_read_ = other.is_nullable_for_read_;
    is_gen_col_ = other.is_gen_col_;
    is_virtual_gen_col_ = other.is_virtual_gen_col_;
    is_gen_col_udf_expr_= other.is_gen_col_udf_expr_;
    is_hidden_ = other.is_hidden_;
    if (OB_FAIL(deep_copy_obj(other.cur_default_value_, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(cur_default_value_));
    } else if (OB_FAIL(deep_copy_obj(other.orig_default_value_, orig_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(orig_default_value_));
    }
  }
  return ret;
}

void ObColDesc::reset()
{
  col_id_ = storage::ObStorageSchema::INVALID_ID;
  col_type_.reset();
  col_order_ = common::ObOrderType::ASC;
}

DEFINE_SERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(col_id_);
  OB_UNIS_ENCODE(col_type_);
  OB_UNIS_ENCODE(col_order_);
  return ret;
}

DEFINE_DESERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(col_id_);
  OB_UNIS_DECODE(col_type_);
  OB_UNIS_DECODE(col_order_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObColDesc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(col_id_);
  OB_UNIS_ADD_LEN(col_type_);
  OB_UNIS_ADD_LEN(col_order_);
  return len;
}

/************************************* ObTableParam **********************************/
ObTableParam::ObTableParam(ObIAllocator &allocator)
  : allocator_(allocator),
    output_projector_(allocator),
    aggregate_projector_(allocator),
    output_sel_mask_(allocator),
    pad_col_projector_(allocator),
    main_read_info_(),
    has_virtual_column_(false),
    use_lob_locator_(false),
    rowid_version_(ObURowIDData::INVALID_ROWID_VERSION),
    rowid_projector_(allocator),
    enable_lob_locator_v2_(false),
    is_spatial_index_(false)
{
  reset();
}

ObTableParam::~ObTableParam()
{
}

void ObTableParam::reset()
{
  table_id_ = OB_INVALID_ID;
  output_projector_.reset();
  aggregate_projector_.reset();
  output_sel_mask_.reset();
  pad_col_projector_.reset();
  has_virtual_column_ = false;
  use_lob_locator_ = false;
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_projector_.reset();
  main_read_info_.reset();
  enable_lob_locator_v2_ = false;
  is_spatial_index_ = false;
}

OB_DEF_SERIALIZE(ObTableParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              output_projector_,
              aggregate_projector_,
              output_sel_mask_,
              pad_col_projector_,
              has_virtual_column_,
              use_lob_locator_,
              rowid_version_,
              rowid_projector_,
              main_read_info_,
              enable_lob_locator_v2_,
              is_spatial_index_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
              table_id_,
              output_projector_,
              aggregate_projector_,
              output_sel_mask_,
              pad_col_projector_,
              has_virtual_column_,
              use_lob_locator_,
              rowid_version_,
              rowid_projector_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(main_read_info_.deserialize(allocator_, buf, data_len, pos))) {
      LOG_WARN("Fail to deserialize read info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                enable_lob_locator_v2_,
                is_spatial_index_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableParam)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              output_projector_,
              aggregate_projector_,
              output_sel_mask_,
              pad_col_projector_,
              has_virtual_column_,
              use_lob_locator_,
              rowid_version_,
              rowid_projector_,
              main_read_info_,
              enable_lob_locator_v2_,
              is_spatial_index_);
  return len;
}

int ObTableParam::get_columns_serialize_size(const Columns &columns, int64_t &size)
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

int ObTableParam::serialize_columns(const Columns &columns, char *buf, const int64_t data_len,
                                    int64_t &pos)
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

int ObTableParam::deserialize_columns(const char *buf, const int64_t data_len,
                                      int64_t &pos, Columns &columns, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObColumnParam **column = NULL;
  int64_t column_cnt = 0;
  void *tmp_ptr  = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_cnt))) {
    LOG_WARN("Fail to decode column count", K(ret));
  } else if (column_cnt > 0) {
    if (NULL == (tmp_ptr = allocator.alloc(column_cnt * sizeof(ObColumnParam *)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc", K(ret), K(column_cnt));
    } else if (FALSE_IT(column = static_cast<ObColumnParam **>(tmp_ptr))) {
      // not reach
    } else {
      ObArray<ObColumnParam *> tmp_columns;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        ObColumnParam *&cur_column = column[i];
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

int ObTableParam::construct_columns_and_projector(
    const ObTableSchema &table_schema,
    const common::ObIArray<uint64_t> & output_column_ids,
    const common::ObIArray<uint64_t> *tsc_out_cols,
    const bool force_mysql_mode)
{
  int ret = OB_SUCCESS;
  static const int64_t COMMON_COLUMN_NUM = 16;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> tmp_access_cols_desc;
  ObSEArray<ObColumnParam *, COMMON_COLUMN_NUM> tmp_access_cols_param;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_access_cols_index;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_output_projector;
  ObSEArray<bool, COMMON_COLUMN_NUM> tmp_output_sel_mask;
  share::schema::ObColDesc tmp_col_desc;

  // rowkey columns front, other columns behind
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> column_ids_no_virtual;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> column_ids;
  if (OB_FAIL(table_schema.get_column_ids(column_ids_no_virtual, true))) {
    LOG_WARN("get column ids no virtual failed", K(ret));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids, false))) {
    LOG_WARN("get column ids failed", K(ret));
  }

  // column array
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  int64_t rowkey_count = rowkey_info.get_size();
  if (OB_SUCC(ret)) {
    //add rowkey columns
    for (int32_t i = 0; OB_SUCC(ret) && i < rowkey_count; ++i) {
      const ObRowkeyColumn *rowkey_column = NULL;
      const ObColumnSchemaV2 *column_schema = NULL;
      ObColumnParam *column = NULL;
      if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL", K(ret), K(i), K(rowkey_info));
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column schema is NULL", K(ret), K(i), K(table_schema));
      } else if (OB_FAIL(alloc_column(allocator_, column))) {
        LOG_WARN("alloc column failed", K(ret), K(i));
      } else if(OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
        LOG_WARN("convert failed", K(ret), K(*column_schema), K(i));
      } else if (OB_FAIL(tmp_access_cols_param.push_back(column))) {
        LOG_WARN("fail to push_back tmp_access_cols_param", K(ret));
      } else if (OB_FAIL(tmp_access_cols_index.push_back(i))) {
        LOG_WARN("fail to push_back tmp_access_cols_index", K(ret));
      } else {
        tmp_col_desc.col_id_ = static_cast<uint32_t>(column->get_column_id());
        tmp_col_desc.col_type_ = column->get_meta_type();
        tmp_col_desc.col_order_ = column->get_column_order();
        if (tmp_col_desc.col_type_.is_lob_storage() && (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0)) {
          tmp_col_desc.col_type_.set_has_lob_header();
        }
        if (OB_FAIL(tmp_access_cols_desc.push_back(tmp_col_desc))) {
          LOG_WARN("fail to push_back tmp_col_desc", K(ret));
        }
      }
    }

    //add other columns
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      const uint64_t column_id = output_column_ids.at(i);
      const ObColumnSchemaV2 *column_schema = NULL;
      ObColumnParam *column = NULL;
      int32_t col_index = OB_INVALID_INDEX;
      int32_t mem_col_index = OB_INVALID_INDEX;
      if (OB_FAIL(alloc_column(allocator_, column))) {
        LOG_WARN("alloc column failed", K(ret), K(i));
      } else if (OB_UNLIKELY(common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id) ||
                 common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id ||
                 common::OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id ||
                 common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == column_id) {
        ObObjMeta meta_type;
        if (common::OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id) {
          meta_type.set_urowid();
        } else {
          meta_type.set_int();
        }
        column->set_column_id(column_id);
        column->set_meta_type(meta_type);
        col_index = -1;
        mem_col_index = -1;
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL", K(ret), K(table_schema.get_table_id()), K(column_id), K(i));
      } else if (OB_UNLIKELY(column_schema->get_data_type() == ObLobType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected ObLobType column to init table scan", K(ret), KPC(column_schema));
      } else if (column_schema->is_rowkey_column()) {
        // continue if is rowkeycolumn
        continue;
      } else {
        if(OB_FAIL(convert_column_schema_to_param(*column_schema, *column))) {
          LOG_WARN("convert failed", K(*column_schema), K(ret), K(i));
        } else {
          int32_t idx = OB_INVALID_INDEX;
          for (int32_t j = 0; OB_INVALID_INDEX == idx && j < column_ids_no_virtual.count(); ++j) {
            if (column_id == column_ids_no_virtual.at(j).col_id_) {
              idx = j;
            }
          }
          col_index = idx;
        }
      }

      if (OB_SUCC(ret)) {
        has_virtual_column_ = column_ids_no_virtual.count() != column_ids.count();
        tmp_col_desc.col_id_ = static_cast<uint32_t>(column->get_column_id());
        tmp_col_desc.col_type_ = column->get_meta_type();
        tmp_col_desc.col_order_ = column->get_column_order();
        if (tmp_col_desc.col_type_.is_lob_storage() && (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0)) {
          tmp_col_desc.col_type_.set_has_lob_header();
        }
        if (OB_FAIL(tmp_access_cols_param.push_back(column))) {
          LOG_WARN("fail to push_back tmp_access_cols_param", K(ret));
        } else if (OB_FAIL(tmp_access_cols_desc.push_back(tmp_col_desc))) {
          LOG_WARN("fail to push_back tmp_access_cols_desc", K(ret));
        } else if (OB_FAIL(tmp_access_cols_index.push_back(col_index))) {
          LOG_WARN("fail to push_back tmp_access_cols_index", K(ret));
        }
      }
    }
  }

  // output projector
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); ++i) {
      int32_t idx = OB_INVALID_INDEX;
      for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < tmp_access_cols_param.count(); ++j) {
        const ObColumnParam *column = tmp_access_cols_param.at(j);
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

  // table scan output columns mask
  if (OB_SUCC(ret)) {
    if (NULL != tsc_out_cols) {
      int32_t output_count = 0;
      for(int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); i++) {
        bool found = false;
        uint64_t column_id = output_column_ids.at(i);
        for(int32_t j = 0; !found && j < tsc_out_cols->count(); j++) {
          found = tsc_out_cols->at(j) == column_id;
        }
        if (found) {
          output_count++;
        }
        if (OB_FAIL(tmp_output_sel_mask.push_back(found))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && 0 == output_count && 0 < tmp_output_sel_mask.count()) {
        // make sure one output expr at least
        tmp_output_sel_mask.at(0) = true;
      }
    } else {
      bool found = true;
      for(int32_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); i++) {
        if (OB_FAIL(tmp_output_sel_mask.push_back(found))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }

  // assign
  if (OB_SUCC(ret)) {
    if (OB_FAIL(main_read_info_.init(allocator_,
                                     table_schema.get_column_count(),
                                     rowkey_count,
                                     force_mysql_mode ? false : lib::is_oracle_mode(),
                                     tmp_access_cols_desc,
                                     &tmp_access_cols_index,
                                     &tmp_access_cols_param))) {
      LOG_WARN("fail to init main read info", K(ret));
    } else if (OB_FAIL(output_projector_.assign(tmp_output_projector))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(output_sel_mask_.assign(tmp_output_sel_mask))) {
      LOG_WARN("assign failed", K(ret));
    }
  }
  LOG_DEBUG("Generated main read info", K_(main_read_info));
  return ret;
}

int ObTableParam::filter_common_columns(const ObIArray<const ObColumnSchemaV2 *> &columns,
                                        ObIArray<const ObColumnSchemaV2 *> &new_columns)
{
  int ret = OB_SUCCESS;
  new_columns.reset();

  for (int64_t i = 0; i < columns.count() && OB_SUCC(ret); ++i) {
    const ObColumnSchemaV2 *column = columns.at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else {
      bool is_exists = false;
      for (int64_t j = 0; j < new_columns.count() && OB_SUCC(ret) && !is_exists; ++j) {
        const ObColumnSchemaV2 *new_column = new_columns.at(j);
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

int ObTableParam::construct_pad_projector(
    const ObIArray<ObColumnParam *> &dst_columns,
    const Projector &dst_output_projector,
    Projector &pad_projector)
{
  int ret = OB_SUCCESS;

  ObArray<int32_t> pad_col_projector;
  for (int32_t i = 0; OB_SUCC(ret) && i < dst_output_projector.count(); ++i) {
    const ObColumnParam *column = dst_columns.at(dst_output_projector.at(i));
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else if (column->get_meta_type().is_char()
               || column->get_meta_type().is_nchar()) {
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

int ObTableParam::convert(const ObTableSchema &table_schema,
                          const ObIArray<uint64_t> &access_column_ids,
                          const common::ObIArray<uint64_t> *tsc_out_cols,
                          const bool force_mysql_mode)
{
  int ret = OB_SUCCESS;
    // if mocked rowid index is used
    // because eventually, we use primary key to do table scan
  table_id_ = table_schema.get_table_id();
  bool is_oracle_mode = false;
  if (OB_FAIL(construct_columns_and_projector(table_schema, access_column_ids, tsc_out_cols, force_mysql_mode))) {
    LOG_WARN("construct failed", K(ret));
  } else if (OB_FAIL(construct_pad_projector(*main_read_info_.get_columns(), output_projector_, pad_col_projector_))) {
    LOG_WARN("Fail to construct pad projector, ", K(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if ((enable_lob_locator_v2_ || is_oracle_mode)
             && OB_FAIL(construct_lob_locator_param(table_schema,
                                                    *main_read_info_.get_columns(),
                                                    output_projector_,
                                                    use_lob_locator_,
                                                    rowid_version_,
                                                    rowid_projector_,
                                                    enable_lob_locator_v2_))) {
    LOG_WARN("fail to construct rowid dep column projector", K(ret));
  } else {
    LOG_DEBUG("construct columns", K(table_id_), K(access_column_ids), K_(main_read_info));
  }

  return ret;
}

int ObTableParam::convert_agg(const ObIArray<uint64_t> &output_column_ids,
                              const common::ObIArray<uint64_t> &aggregate_column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggregate_projector_.init(aggregate_column_ids.count()))) {
    LOG_WARN("failed to init aggregate projector", K(ret), K(aggregate_column_ids.count()));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < aggregate_column_ids.count(); ++i) {
      int32_t idx = OB_INVALID_INDEX;
      if (OB_COUNT_AGG_PD_COLUMN_ID == aggregate_column_ids.at(i)) {
        // count(*/CONST)
        if (OB_FAIL(aggregate_projector_.push_back(OB_COUNT_AGG_PD_COLUMN_ID))) {
          LOG_WARN("failed to push aggregate projector", K(ret), K(i));
        }
      } else {
        for (int32_t j = 0; OB_SUCC(ret) && j < output_column_ids.count(); ++j) {
          if (aggregate_column_ids.at(i) == output_column_ids.at(j)) {
            if (j > output_projector_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected index", K(ret), K(j), K(output_column_ids.count()), K(output_projector_.count()));
            } else {
              idx = output_projector_.at(j);
            }
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_INVALID_INDEX == idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected index", K(ret), K(aggregate_column_ids), K(aggregate_column_ids), K(i));
          } else if (OB_FAIL(aggregate_projector_.push_back(idx))) {
            LOG_WARN("failed to push aggregate projector", K(ret), K(i), K(idx));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableParam::construct_lob_locator_param(const ObTableSchema &table_schema,
                                              const ObIArray<ObColumnParam *> &storage_project_columns,
                                              const Projector &access_projector,
                                              bool &use_lob_locator,
                                              int64_t &rowid_version,
                                              Projector &rowid_projector,
                                              bool is_use_lob_locator_v2)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnParam *col_param = nullptr;
  use_lob_locator = false;
  bool has_row_id = true;
  if (is_use_lob_locator_v2) {
    for (int64_t i = 0; OB_SUCC(ret) && !use_lob_locator && i < access_projector.count(); i++) {
      int32_t idx = access_projector.at(i);
      if (OB_ISNULL(col_param = storage_project_columns.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null col param", K(ret), K(idx), K(storage_project_columns));
      } else {
        ObObjType type = col_param->get_meta_type().get_type();
        use_lob_locator = is_lob_storage(type);
      }
    }
    // Virtual table may not contain primary key columns, i.e. TENANT_VIRTUAL_SESSION_VARIABLE.
    // When access such virtual table, get_column_ids_serialize_to_rowid may return failure because
    // of the null rowkey info. So here skip the rowkey.
    if (table_schema.is_sys_table()
        || table_schema.is_sys_view()
        || table_schema.is_vir_table()
        || (table_schema.get_rowkey_info().get_size() == 0)
        || lib::is_mysql_mode()) {
      has_row_id = false; // need lob locator without rowid
      rowid_version = ObURowIDData::INVALID_ROWID_VERSION;
    }
  } else {
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
  }

  // generate rowid_projector
  if (use_lob_locator && has_row_id && OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> rowid_col_ids;
    // The lob type generates column no need partition info in rowkey table.
    if (OB_FAIL(table_schema.get_rowkey_column_ids(rowid_col_ids))) {
      LOG_WARN("Failed to get rowkey column ids", K(ret));
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
          } else if (rowid_col_ids.at(i) == col_param->get_column_id())  {
            if (OB_FAIL(rowid_projector.push_back(static_cast<int32_t>(j)))) {
              LOG_WARN("Failed to push back rowid project", K(ret));
            } else {
              exist = true;
            }
          }
        } // access projector end
        if (!exist) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("column which rowid dependent is not exist",
                   K(rowid_col_ids.at(i)), K(rowid_col_ids), K(ret));
        }
        if (table_schema.is_heap_table()) {
          rowid_version = table_schema.is_extended_rowid_mode() ? ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION : ObURowIDData::HEAP_TABLE_ROWID_VERSION;
        } else {
          rowid_version = common::ObURowIDData::LOB_NO_PK_ROWID_VERSION;
        }
      } // rowid col ids end
    }
    LOG_TRACE("construct lob locator param", K(use_lob_locator), K(rowid_projector),
              K(rowid_col_ids), K(rowid_version));
  }

  return ret;
}

int ObTableParam::alloc_column(ObIAllocator &allocator, ObColumnParam *& col_ptr)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = nullptr;
  if (OB_ISNULL(tmp_ptr = allocator.alloc(sizeof(ObColumnParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    col_ptr = new (tmp_ptr) ObColumnParam(allocator);
  }
  return ret;
}

int ObTableParam::convert_column_schema_to_param(const ObColumnSchemaV2 &column_schema,
                                                 ObColumnParam &column_param)
{
  int ret = OB_SUCCESS;
  column_param.set_column_id(column_schema.get_column_id());
  column_param.set_meta_type(column_schema.get_meta_type());
  column_param.set_column_order(column_schema.get_order_in_rowkey());
  column_param.set_accuracy(column_schema.get_accuracy());
  column_param.set_nullable_for_write(!column_schema.is_not_null_for_write());
  column_param.set_nullable_for_read(!column_schema.is_not_null_for_read());
  column_param.set_gen_col_flag(column_schema.is_generated_column(),
                                column_schema.is_virtual_generated_column());
  column_param.set_gen_col_udf_expr(column_schema.is_generated_column_using_udf());
  column_param.set_is_hidden(column_schema.is_hidden());
  LOG_DEBUG("convert_column_schema_to_param", K(column_schema), K(column_param), K(lbt()));
  if (column_schema.is_generated_column()) {
    ObObj nop_obj;
    nop_obj.set_nop_value();
    ret = column_param.set_orig_default_value(nop_obj);
    if (OB_SUCC(ret)) {
      ret = column_param.set_cur_default_value(nop_obj);
    }
  } else if (column_schema.is_identity_column()) {
    // Identity colunm's orig_default_value and cur_default_val are used to store sequence id
    // and desc table, it does not have the same semantics as normal default. so here we set
    // its default value as null to avoid type mismatch.
    ObObj null_obj;
    null_obj.set_null();
    ret = column_param.set_orig_default_value(null_obj);
    if (OB_SUCC(ret)) {
      ret = column_param.set_cur_default_value(null_obj);
    }
  } else {
    ret = column_param.set_orig_default_value(column_schema.get_orig_default_value());
    if (OB_SUCC(ret)) {
      ret = column_param.set_cur_default_value(column_schema.get_cur_default_value());
    }
  }
  return ret;
}

int64_t ObTableParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
       K_(output_projector),
       K_(aggregate_projector),
       K_(output_sel_mask),
       K_(pad_col_projector),
       K_(main_read_info),
       K_(use_lob_locator),
       K_(rowid_version),
       K_(rowid_projector),
       K_(enable_lob_locator_v2));
  J_OBJ_END();

  return pos;
}
} //namespace schema
} //namespace share
} //namespace oceanbase
