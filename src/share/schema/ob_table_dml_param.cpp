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
#include "ob_table_dml_param.h"
#include "share/schema/ob_column_schema.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
// ------ ObTableSchemaParam ------ //
ObTableSchemaParam::ObTableSchemaParam(ObIAllocator &allocator)
  : allocator_(allocator),
    table_id_(OB_INVALID_ID),
    schema_version_(OB_INVALID_VERSION),
    table_type_(MAX_TABLE_TYPE),
    index_type_(INDEX_TYPE_IS_NOT),
    index_status_(INDEX_STATUS_NOT_FOUND),
    shadow_rowkey_column_num_(0),
    doc_id_col_id_(OB_INVALID_ID),
    fulltext_col_id_(OB_INVALID_ID),
    spatial_geo_col_id_(OB_INVALID_ID),
    spatial_cellid_col_id_(OB_INVALID_ID),
    spatial_mbr_col_id_(OB_INVALID_ID),
    index_name_(),
    fts_parser_name_(),
    columns_(allocator),
    col_map_(allocator),
    pk_name_(),
    read_param_version_(0),
    read_info_(),
    cg_read_infos_(),
    lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    multivalue_col_id_(OB_INVALID_ID),
    multivalue_arr_col_id_(OB_INVALID_ID),
    data_table_rowkey_column_num_(0)
{
}

ObTableSchemaParam::~ObTableSchemaParam()
{
  reset();
}

void ObTableSchemaParam::reset()
{
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_type_ = MAX_TABLE_TYPE;
  index_type_ = INDEX_TYPE_IS_NOT;
  index_status_ = INDEX_STATUS_NOT_FOUND;
  shadow_rowkey_column_num_ = 0;
  doc_id_col_id_ = OB_INVALID_ID;
  fulltext_col_id_ = OB_INVALID_ID;
  spatial_geo_col_id_ = OB_INVALID_ID;
  spatial_cellid_col_id_ = OB_INVALID_ID;
  spatial_mbr_col_id_ = OB_INVALID_ID;
  index_name_.reset();
  fts_parser_name_.reset();
  columns_.reset();
  col_map_.clear();
  pk_name_.reset();
  read_info_.reset();
  cg_read_infos_.reset();
  read_param_version_ = 0;
  lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
  multivalue_col_id_ = OB_INVALID_ID;
  multivalue_arr_col_id_ = OB_INVALID_ID;
  data_table_rowkey_column_num_ =0 ;
}

int ObTableSchemaParam::convert(const ObTableSchema *schema)
{
  int ret = OB_SUCCESS;
  static const int64_t COMMON_COLUMN_NUM = 16;
  ObSEArray<ObColumnParam *, COMMON_COLUMN_NUM> tmp_cols;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> all_column_ids;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> tmp_col_descs;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_cols_index;
  ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_cg_idxs;
  bool use_cs = false;
  int32_t cg_idx = 0;

  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is NULL", K(ret), KP(schema));
  } else {
    table_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    table_type_ = schema->get_table_type();
    lob_inrow_threshold_ = schema->get_lob_inrow_threshold();
    if (OB_FAIL(schema->get_is_row_store(use_cs))) {
      LOG_WARN("fail to get is row store", K(ret));
    } else {
      use_cs = !use_cs;
    }
  }

  if (OB_SUCC(ret) && schema->is_user_table() && !schema->is_heap_table()) {
    ObString tmp_pk_name;
    if (OB_FAIL(schema->get_pk_constraint_name(tmp_pk_name))) {
      LOG_WARN("get pk name from schema failed", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_pk_name, pk_name_))) {
      LOG_WARN("fail to copy pk name", K(ret), K(pk_name_));
    }
  }

  if(OB_SUCC(ret) && schema->is_index_table()) {
    index_type_ = schema->get_index_type();
    index_status_ = schema->get_index_status();
    shadow_rowkey_column_num_ = schema->get_shadow_rowkey_column_num();
    ObString tmp_name;

    if (schema->is_spatial_index()) {
      if (OB_FAIL(schema->get_spatial_geo_column_id(spatial_geo_col_id_))) {
        LOG_WARN("fail to get spatial geo column id", K(ret), K(schema->get_index_info()));
      } else if (OB_FAIL(schema->get_index_info().get_spatial_cellid_col_id(spatial_cellid_col_id_))) {
        LOG_WARN("fail to get spatial cellid column id", K(ret), K(schema->get_index_info()));
      } else if (OB_FAIL(schema->get_index_info().get_spatial_mbr_col_id(spatial_mbr_col_id_))) {
        LOG_WARN("fail to get spatial mbr column id", K(ret), K(schema->get_index_info()));
      }
    } else if (schema->is_fts_index_aux() || schema->is_fts_doc_word_aux()) {
      if (OB_FAIL(schema->get_fulltext_column_ids(doc_id_col_id_, fulltext_col_id_))) {
        LOG_WARN("fail to get fulltext column ids", K(ret));
      } else if (OB_UNLIKELY(doc_id_col_id_ <= OB_APP_MIN_COLUMN_ID || OB_INVALID_ID == doc_id_col_id_
                        || fulltext_col_id_ <= OB_APP_MIN_COLUMN_ID || OB_INVALID_ID == fulltext_col_id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid doc id or fulltext column id", K(ret), K(doc_id_col_id_), K(fulltext_col_id_));
      } else if (OB_FAIL(ob_write_string(allocator_, schema->get_parser_name_str(), fts_parser_name_))) {
        LOG_WARN("fail to copy fts parser name", K(ret), K(schema->get_parser_name_str()));
      }
    } else if (schema->is_multivalue_index_aux()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema->get_column_count(); ++i) {
        const ObColumnSchemaV2 *column_schema = schema->get_column_schema_by_idx(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(schema));
        } else if (column_schema->is_doc_id_column()) {
          doc_id_col_id_ = column_schema->get_column_id();
        } else if (column_schema->is_multivalue_generated_column()) {
          multivalue_col_id_ = column_schema->get_column_id();
        } else if (column_schema->is_multivalue_generated_array_column()) {
          multivalue_arr_col_id_ = column_schema->get_column_id();
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(schema->get_index_name(tmp_name))) {
      LOG_WARN("fail to get index name", K(ret), K(schema->get_index_info()));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, index_name_))) {
      LOG_WARN("fail to copy index name", K(ret), K(tmp_name));
    }
  }

  if (OB_SUCC(ret) && schema->is_mlog_table()) {
    index_type_ = schema->get_index_type();
    index_status_ = schema->get_index_status();
    ObString tmp_name;
    if (OB_FAIL(schema->get_mlog_name(tmp_name))) {
      LOG_WARN("fail to get materialized view log name", KR(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, index_name_))) {
      LOG_WARN("fail to copy materialized view log name", KR(ret), K(tmp_name));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(schema->get_column_ids(all_column_ids, false))) {
    LOG_WARN("fail to get column ids", K(ret));
  }
  int32_t virtual_cols_cnt = 0;
  int64_t schema_rowkey_cnt = schema->get_rowkey_column_num();
  for (int32_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); ++i) {
    int32_t col_index = OB_INVALID_INDEX;
    const uint64_t column_id = all_column_ids.at(i).col_id_;
    const ObColumnSchemaV2 *column_schema = NULL;
    ObColumnParam *column = NULL;
    if (OB_ISNULL(column_schema = schema->get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is NULL", K(schema->get_table_id()), K(column_id), K(i));
    } else if (OB_FAIL(ObTableParam::alloc_column(allocator_, column))) {
      LOG_WARN("alloc column failed", K(ret), K(i));
    } else if(OB_FAIL(ObTableParam::convert_column_schema_to_param(*column_schema, *column))) {
      LOG_WARN("convert failed", K(*column_schema), K(ret), K(i));
    } else if (OB_FAIL(tmp_cols.push_back(column))) {
      LOG_WARN("store tmp column param failed", K(ret));
    } else if (OB_FAIL(tmp_col_descs.push_back(all_column_ids.at(i)))) {
      LOG_WARN("store tmp column desc failed", K(ret));
    } else if (i < schema_rowkey_cnt) {
      col_index = i;
    } else if (column_schema->is_virtual_generated_column()) {
      col_index = -1;
      virtual_cols_cnt++;
    } else {
      col_index = i - virtual_cols_cnt;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_cols_index.push_back(col_index))) {
        LOG_WARN("fail to push_back col_index", K(ret));
      } else if (use_cs && OB_FAIL(schema->get_column_group_index(*column, cg_idx))) {
        LOG_WARN("Fail to get column group index", K(ret));
      } else if (use_cs && OB_FAIL(tmp_cg_idxs.push_back(cg_idx))) {
        LOG_WARN("Fail to push back cg idx", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(columns_.assign(tmp_cols))) {
      LOG_WARN("fail to assign columns", K(ret));
    } else if (OB_FAIL(read_info_.init(
                allocator_,
                schema->get_column_count(),
                schema_rowkey_cnt,
                lib::is_oracle_mode(),
                tmp_col_descs,
                &tmp_cols_index,
                &tmp_cols,
                use_cs ? &tmp_cg_idxs : nullptr))) {
      LOG_WARN("Fail to init read info", K(ret));
    } else if (!col_map_.is_inited()) {
      if (OB_FAIL(col_map_.init(tmp_cols))) {
        LOG_WARN("failed to create column map", K(ret), K_(columns));
      }
    }
  }
  LOG_DEBUG("Generated read info", K_(read_info));
  read_param_version_ = storage::ObCGReadInfo::MIX_READ_INFO_LOCAL_CACHE;
  if (OB_SUCC(ret) && use_cs && tmp_cg_idxs.count() <= storage::ObCGReadInfo::get_local_max_cg_cnt()) {
    // construct cg read infos
    void *tmp_ptr  = nullptr;
    int64_t cg_cnt = tmp_cg_idxs.count();
    ObArray<storage::ObTableReadInfo *> tmp_read_infos;
    if (OB_UNLIKELY(tmp_col_descs.count() != cg_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected not equal col count", K(ret), K(cg_cnt), K(tmp_col_descs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cg_cnt; i++) {
        storage::ObTableReadInfo *cur_read_info = nullptr;
        if (0 > tmp_cg_idxs.at(i)) {
        } else if (OB_ISNULL(tmp_ptr = allocator_.alloc(sizeof(storage::ObTableReadInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret));
        } else if (FALSE_IT(cur_read_info = new (tmp_ptr) storage::ObTableReadInfo())) {
        } else if (OB_FAIL(storage::ObTenantCGReadInfoMgr::construct_cg_read_info(allocator_,
                                                                                  read_info_.is_oracle_mode(),
                                                                                  tmp_col_descs.at(i),
                                                                                  tmp_cols.at(i),
                                                                                  *cur_read_info))) {
          LOG_WARN("Fail to init cg read info", K(ret));
        }
        if (OB_SUCC(ret) && OB_FAIL(tmp_read_infos.push_back(cur_read_info))) {
          LOG_WARN("Fail to push back read info", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(cg_read_infos_.init_and_assign(tmp_read_infos, allocator_))) {
        LOG_WARN("Fail to add read infos", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchemaParam::is_rowkey_column(const uint64_t column_id, bool &is_rowkey) const
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  if (OB_FAIL(col_map_.get(column_id, idx)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get idx from column map fail", K(ret), K(column_id));
  } else {
    is_rowkey = (idx >= 0 && idx < read_info_.get_schema_rowkey_count());
  }
  return ret;
}

int ObTableSchemaParam::is_column_nullable_for_write(const uint64_t column_id,
                                                     bool &is_nullable_for_write) const
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  if (OB_FAIL(col_map_.get(column_id, idx)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get idx from column map fail", K(ret), K(column_id));
  } else if (idx < 0) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("column not exist", K(ret), K(column_id));
  } else {
    is_nullable_for_write = columns_.at(idx)->is_nullable_for_write();
  }
  return ret;
}

const ObColumnParam * ObTableSchemaParam::get_column(const uint64_t column_id) const
{
  int ret = OB_SUCCESS;
  const ObColumnParam * ptr = NULL;
  int32_t idx = -1;
  if (OB_FAIL(col_map_.get(column_id, idx)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get idx from column map fail", K(ret), K(column_id));
  } else if (idx < 0) {
    // do nothing
  } else {
    ptr = get_column_by_idx(idx);
  }
  return ptr;
}

const ObColumnParam * ObTableSchemaParam::get_column_by_idx(const int64_t idx) const
{
  const ObColumnParam * ptr = NULL;
  if (idx < 0 || idx >= columns_.count()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "idx out of range", K(idx), K(columns_.count()), K(lbt()));
  } else {
    ptr = columns_.at(idx);
  }
  return ptr;
}

ObColumnParam * ObTableSchemaParam::get_column_by_idx(const int64_t idx)
{
  ObColumnParam * ptr = NULL;
  if (idx < 0 || idx >= columns_.count()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "idx out of range", K(idx), K(columns_.count()), K(lbt()));
  } else {
    ptr = columns_.at(idx);
  }
  return ptr;
}

const ObColumnParam * ObTableSchemaParam::get_rowkey_column_by_idx(const int64_t idx) const
{
  const ObColumnParam * ptr = NULL;
  if (idx < 0 || idx >= read_info_.get_schema_rowkey_count()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "idx out of range", K(idx), K(read_info_.get_schema_rowkey_count()));
  } else {
    ptr = columns_.at(idx);
  }
  return ptr;
}

int ObTableSchemaParam::get_rowkey_column_ids(ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("param not inited", K(ret), K(*this));
  } else {
    const ObColumnParam *param = NULL;
    ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < read_info_.get_schema_rowkey_count(); ++i) {
      if (OB_ISNULL(param = columns_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column param is NULL", K(ret), K(i));
      } else {
        col_desc.col_id_ = static_cast<uint32_t>(param->get_column_id());
        col_desc.col_order_ = param->get_column_order();
        col_desc.col_type_ = param->get_meta_type();
        col_desc.col_type_.set_scale(param->get_accuracy().get_scale());
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add rowkey column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSchemaParam::get_rowkey_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("param not inited", K(ret), K(*this));
  } else {
    const ObColumnParam *param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < read_info_.get_schema_rowkey_count(); ++i) {
      if (OB_ISNULL(param = columns_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column param is NULL", K(ret), K(i));
      } else if (OB_FAIL(column_ids.push_back(param->get_column_id()))) {
        LOG_WARN("Fail to add rowkey column id to column_ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchemaParam::get_index_name(common::ObString &index_name) const
{
  int ret = OB_SUCCESS;
  if (!is_index_table()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table is not index table", K(ret), K(table_id_), K(table_type_));
  } else {
    index_name.assign_ptr(index_name_.ptr(), index_name_.length());
  }
  return ret;
}

const ObString &ObTableSchemaParam::get_pk_name() const
{
  return pk_name_;
}

bool ObTableSchemaParam::is_depend_column(uint64_t column_id) const
{
  bool is_depend = false;
  int32_t idx = 0;
  if (is_materialized_view() &&
      column_id > OB_MIN_MV_COLUMN_ID &&
      column_id < OB_MIN_SHADOW_COLUMN_ID &&
      OB_SUCCESS == col_map_.get(column_id, idx)) {
    is_depend = true;
  }
  return is_depend;
}

int64_t ObTableSchemaParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
       K_(schema_version),
       K_(table_type),
       K_(index_type),
       K_(index_status),
       K_(shadow_rowkey_column_num),
       K_(doc_id_col_id),
       K_(fulltext_col_id),
       K_(index_name),
       K_(fts_parser_name),
       K_(pk_name),
       K_(columns),
       K_(read_info),
       K_(lob_inrow_threshold));
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObTableSchemaParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              schema_version_,
              table_type_,
              index_type_,
              index_status_,
              shadow_rowkey_column_num_,
              fulltext_col_id_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize index name", K(ret));
    } else if (OB_FAIL(ObTableParam::serialize_columns(columns_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize columns", K(ret));
    }
  }
  OB_UNIS_ENCODE(read_info_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pk_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize pk name", K(ret));
    }
  }
  OB_UNIS_ENCODE(spatial_geo_col_id_);
  OB_UNIS_ENCODE(spatial_cellid_col_id_);
  OB_UNIS_ENCODE(spatial_mbr_col_id_);
  OB_UNIS_ENCODE(lob_inrow_threshold_);
  OB_UNIS_ENCODE(read_param_version_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cg_read_infos_.count()))) {
      LOG_WARN("Fail to encode column count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_read_infos_.count(); ++i) {
      if (nullptr != cg_read_infos_.at(i) && OB_FAIL(cg_read_infos_.at(i)->serialize(buf, buf_len, pos))) {
        LOG_WARN("Fail to serialize column", K(ret));
      }
    }
  }
  OB_UNIS_ENCODE(multivalue_col_id_);
  OB_UNIS_ENCODE(multivalue_arr_col_id_);
  OB_UNIS_ENCODE(data_table_rowkey_column_num_);
  OB_UNIS_ENCODE(doc_id_col_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fts_parser_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize fts parser name", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableSchemaParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              table_id_,
              schema_version_,
              table_type_,
              index_type_,
              index_status_,
              shadow_rowkey_column_num_,
              fulltext_col_id_);

  if (OB_SUCC(ret)) {
    ObString tmp_name;
    if (OB_FAIL(tmp_name.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize index name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, index_name_))) {
      LOG_WARN("failed to copy index name", K(ret), K(tmp_name));
    } else if (OB_FAIL(ObTableParam::deserialize_columns(buf, data_len, pos, columns_, allocator_))) {
      LOG_WARN("failed to deserialize columns", K(ret));
    } else if (!col_map_.is_inited()) {
      if (OB_FAIL(col_map_.init(columns_))) {
        LOG_WARN("failed to create column map", K(ret), K_(columns));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(read_info_.deserialize(allocator_, buf, data_len, pos))) {
    LOG_WARN("Fail to deserialize read_info", K(ret));
  }
  // for compatibility: at least need two bytes to deserialize an ObString
  const int64_t MINIMAL_NEEDED_SIZE = 2;
  if (OB_SUCC(ret) && (data_len - pos) > MINIMAL_NEEDED_SIZE) {
     ObString tmp_name;
     if (OB_FAIL(tmp_name.deserialize(buf, data_len, pos))) {
       LOG_WARN("failed to deserialize pk name", K(ret), K(data_len), K(pos));
     } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, pk_name_))) {
       LOG_WARN("failed to copy pk name", K(ret), K(tmp_name));
     }
  }
  OB_UNIS_DECODE(spatial_geo_col_id_);
  OB_UNIS_DECODE(spatial_cellid_col_id_);
  OB_UNIS_DECODE(spatial_mbr_col_id_);
  OB_UNIS_DECODE(lob_inrow_threshold_);
  OB_UNIS_DECODE(read_param_version_);
  if (OB_SUCC(ret) && pos < data_len) {
    int64_t cg_read_info_cnt = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &cg_read_info_cnt))) {
      LOG_WARN("Fail to decode cg read info count", K(ret));
    } else if (cg_read_info_cnt > 0) {
      void *tmp_ptr  = nullptr;
      const common::ObIArray<int32_t> *access_cgs = read_info_.get_cg_idxs();
      if (OB_UNLIKELY(nullptr == access_cgs || access_cgs->count() != cg_read_info_cnt ||
                      storage::ObCGReadInfo::MIX_READ_INFO_LOCAL_CACHE != read_param_version_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cg read count", K(ret), KPC(access_cgs), K(cg_read_info_cnt), K_(read_param_version));
      } else {
        ObArray<storage::ObTableReadInfo *> tmp_read_infos;
        for (int64_t i = 0; OB_SUCC(ret) && i < cg_read_info_cnt; ++i) {
          storage::ObTableReadInfo *cur_read_info = nullptr;
          if (0 > access_cgs->at(i)) {
          } else if (OB_ISNULL(tmp_ptr = allocator_.alloc(sizeof(storage::ObTableReadInfo)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed", K(ret));
          } else if (FALSE_IT(cur_read_info = new (tmp_ptr) storage::ObTableReadInfo())) {
          } else if (OB_FAIL(cur_read_info->deserialize(allocator_, buf, data_len, pos))) {
            LOG_WARN("Fail to deserialize read info", K(ret));
          }

          if (OB_SUCC(ret) && OB_FAIL(tmp_read_infos.push_back(cur_read_info))) {
            LOG_WARN("Fail to add read info", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(cg_read_infos_.init_and_assign(tmp_read_infos, allocator_))) {
          LOG_WARN("Fail to add read infos", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && pos == data_len) {
    // Here is to solve the compatibility problem and correct the `index_type_` and `index_status_`.
    //
    // Before version 4.3.1.0, the default value of `index_type_` and `index_sattus_` was max. In
    // version 4.3.1.0, the full-text search and json multi-value indexes were introduced. If the
    // RPC request from older version observer is received, the `index_type_` will be mistaken for
    // a valid json multi-valued index.
    //
    // Therefore, if there are still unresolved fields here, it means that it is a new version
    // observer. It is necessary to re-assign the initial values to the `index_type_` and
    // `index_status_` to avoid misjudgment as a valid index.


    // ATTENTION!!!
    // The front-end version is currently only 4.3.0.x, and its value of max index type is 23.
    if (23 == index_type_) {
      index_type_ = INDEX_TYPE_IS_NOT;
    }
    // ATTENTION!!!
    // The front-end version is currently only 4.3.0.x, and its value of max index status is 8.
    if (8 == index_status_) {
      index_status_ = INDEX_STATUS_NOT_FOUND;
    }
  }

  OB_UNIS_DECODE(multivalue_col_id_);
  OB_UNIS_DECODE(multivalue_arr_col_id_);
  OB_UNIS_DECODE(data_table_rowkey_column_num_);
  OB_UNIS_DECODE(doc_id_col_id_)

  if (OB_SUCC(ret) && pos < data_len) {
    ObString tmp_name;
    if (OB_FAIL(tmp_name.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize fts parser name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, fts_parser_name_))) {
      LOG_WARN("fail to copy fts parser name", K(ret), K(tmp_name));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSchemaParam)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              schema_version_,
              table_type_,
              index_type_,
              index_status_,
              shadow_rowkey_column_num_,
              fulltext_col_id_);
  len += index_name_.get_serialize_size();

  if (OB_SUCC(ret)) {
    int64_t columns_size = 0;
    if (OB_FAIL(ObTableParam::get_columns_serialize_size(columns_, columns_size))) {
      LOG_WARN("failed to get columns serialize size", K(ret));
    } else {
      len += columns_size;
    }
  }
  OB_UNIS_ADD_LEN(read_info_);
  len += pk_name_.get_serialize_size();
  OB_UNIS_ADD_LEN(spatial_geo_col_id_);
  OB_UNIS_ADD_LEN(spatial_cellid_col_id_);
  OB_UNIS_ADD_LEN(spatial_mbr_col_id_);
  OB_UNIS_ADD_LEN(lob_inrow_threshold_);
  OB_UNIS_ADD_LEN(read_param_version_);
  if (OB_SUCC(ret)) {
    len += serialization::encoded_length_vi64(cg_read_infos_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_read_infos_.count(); ++i) {
      if (nullptr != cg_read_infos_.at(i)) {
        len += cg_read_infos_.at(i)->get_serialize_size();
      }
    }
  }

  OB_UNIS_ADD_LEN(multivalue_col_id_);
  OB_UNIS_ADD_LEN(multivalue_arr_col_id_);
  OB_UNIS_ADD_LEN(data_table_rowkey_column_num_);
  OB_UNIS_ADD_LEN(doc_id_col_id_);
  len += fts_parser_name_.get_serialize_size();
  return len;
}

int ObTableSchemaParam::has_udf_column(bool &has_udf) const
{
  int ret = OB_SUCCESS;
  has_udf = false;
  const ObColumnParam *param = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count() && !has_udf; ++i) {
    if (OB_ISNULL(param = columns_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column failed", K(ret), KP(param));
    } else {
      has_udf = param->is_gen_col_udf_expr();
    }
  }
  return ret;
}

// ------ ObTableDMLParam ------ //
ObTableDMLParam::ObTableDMLParam(common::ObIAllocator &allocator)
  : allocator_(allocator),
    tenant_schema_version_(OB_INVALID_VERSION),
    data_table_(allocator),
    col_descs_(allocator),
    col_map_(allocator)
{
}

ObTableDMLParam::~ObTableDMLParam()
{
  reset();
}

void ObTableDMLParam::reset()
{
  tenant_schema_version_ = OB_INVALID_VERSION;
  data_table_.reset();
}

int ObTableDMLParam::convert(const ObTableSchema *table_schema,
                             const int64_t tenant_schema_version,
                             const common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *schema = NULL;
  if (OB_ISNULL(table_schema) || OB_INVALID_VERSION == tenant_schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_schema_version), KP(table_schema));
  } else if (OB_FAIL(data_table_.convert(table_schema))) {
    LOG_WARN("convert data table fail", K(ret));
  } else if (OB_FAIL(prepare_storage_param(column_ids))) {
    LOG_WARN("prepare storage param fail", K(ret));
  } else {
    tenant_schema_version_ = tenant_schema_version;
  }
  return ret;
}

int64_t ObTableDMLParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_schema_version),
       K_(data_table),
       K_(col_descs),
       K_(col_map)
       );
  J_OBJ_END();

  return pos;
}

OB_DEF_SERIALIZE(ObTableDMLParam)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, tenant_schema_version_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(data_table_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize data table", K(ret), K(data_table_));
    }
  }
  OB_UNIS_ENCODE(col_descs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableDMLParam)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || data_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, tenant_schema_version_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(data_table_.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize data table", K(ret));
    }
  }
  OB_UNIS_DECODE(col_descs_);
  if (OB_SUCC(ret) && !col_descs_.empty()) {
    if (OB_FAIL(col_map_.init(col_descs_))) {
      LOG_WARN("init col map failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableDMLParam)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t size = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_schema_version_);
  len += data_table_.get_serialize_size();
  OB_UNIS_ADD_LEN(col_descs_);
  return len;
}

int ObTableDMLParam::prepare_storage_param(const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = column_ids.count();
  if (col_cnt <= 0 || col_cnt > UINT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_ids", K(ret), K(col_cnt));
  } else {
    const ObColumnParam *col_param = nullptr;
    uint64_t column_id = OB_INVALID_ID;
    ObColDesc col_desc;
    col_descs_.set_capacity(static_cast<uint32_t>(col_cnt));
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      column_id = column_ids.at(i);
      if (nullptr == (col_param = data_table_.get_column(column_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get column param fail", K(ret), K(column_id), KP(col_param));
      } else {
        col_desc.col_id_ = static_cast<uint32_t>(column_id);
        col_desc.col_type_ = col_param->get_meta_type();
        col_desc.col_type_.set_scale(col_param->get_accuracy().get_scale());
        col_desc.col_order_ = col_param->get_column_order();
        if (OB_FAIL(col_descs_.push_back(col_desc))) {
          LOG_WARN("fail to push back column description", K(ret), K(col_desc));
        }
      }
    }

    // assign
    if (OB_SUCC(ret)) {
      if (OB_FAIL(col_map_.init(col_descs_))) {
        LOG_WARN("fail to init column map", K(ret));
      }
    }
  }
  return ret;
}

}//namespace oceanbase
}//namespace share
}//namespace schema
