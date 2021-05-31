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

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
// ------ ObTableSchemaParam ------ //
ObTableSchemaParam::ObTableSchemaParam(ObIAllocator& allocator)
    : allocator_(allocator),
      table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      table_type_(MAX_TABLE_TYPE),
      index_type_(INDEX_TYPE_MAX),
      index_status_(INDEX_STATUS_MAX),
      rowkey_column_num_(0),
      shadow_rowkey_column_num_(0),
      fulltext_col_id_(OB_INVALID_ID),
      index_name_(),
      columns_(allocator),
      col_map_(allocator),
      is_dropped_schema_(false),
      pk_name_()
{}

ObTableSchemaParam::~ObTableSchemaParam()
{
  reset();
}

void ObTableSchemaParam::reset()
{
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_type_ = MAX_TABLE_TYPE;
  index_type_ = INDEX_TYPE_MAX;
  index_status_ = INDEX_STATUS_MAX;
  rowkey_column_num_ = 0;
  shadow_rowkey_column_num_ = 0;
  fulltext_col_id_ = OB_INVALID_ID;
  index_name_.reset();
  columns_.reset();
  col_map_.clear();
  is_dropped_schema_ = false;
  pk_name_.reset();
}

int ObTableSchemaParam::convert(const ObTableSchema* schema, const ObIArray<uint64_t>& col_ids)
{
  int ret = OB_SUCCESS;
  static const int64_t COMMON_COLUMN_NUM = 16;
  ObSEArray<ObColumnParam*, COMMON_COLUMN_NUM> tmp_cols;
  ObSEArray<ObColDesc, COMMON_COLUMN_NUM> column_ids_no_virtual;

  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is NULL", K(ret), KP(schema));
  } else {
    table_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    table_type_ = schema->get_table_type();
    rowkey_column_num_ = schema->get_rowkey_column_num();
  }

  if (OB_SUCC(ret) && schema->is_user_table() && !schema->is_no_pk_table()) {
    ObString tmp_pk_name;
    if (OB_FAIL(schema->get_pk_constraint_name(tmp_pk_name))) {
      LOG_WARN("get pk name from schema failed", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_pk_name, pk_name_))) {
      LOG_WARN("fail to copy pk name", K(ret), K(pk_name_));
    }
  }

  if (OB_SUCC(ret) && schema->is_index_table()) {
    index_type_ = schema->get_index_type();
    index_status_ = schema->get_index_status();
    is_dropped_schema_ = schema->is_dropped_schema();
    shadow_rowkey_column_num_ = schema->get_shadow_rowkey_column_num();
    ObString tmp_name;
    if (OB_FAIL(schema->get_index_info().get_fulltext_column(fulltext_col_id_))) {
      LOG_WARN("fail to get fulltext column id", K(ret), K(schema->get_index_info()));
    } else if (OB_FAIL(schema->get_index_name(tmp_name))) {
      LOG_WARN("fail to get index name", K(ret), K(schema->get_index_info()));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, index_name_))) {
      LOG_WARN("fail to copy index name", K(ret), K(tmp_name));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema->get_column_ids(column_ids_no_virtual, false))) {
      LOG_WARN("fail to get column ids", K(ret));
    }

    for (int32_t i = 0; OB_SUCC(ret) && i < column_ids_no_virtual.count(); ++i) {
      const uint64_t column_id = column_ids_no_virtual.at(i).col_id_;
      const ObColumnSchemaV2* column_schema = NULL;
      ObColumnParam* column = NULL;
      if (OB_ISNULL(column_schema = schema->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL", K(schema->get_table_id()), K(column_id), K(i));
      } else if (column_schema->is_rowid_pseudo_column() && !has_exist_in_array(col_ids, column_id)) {
        // ignore rowid, because storage donot store rowid column
        // eg: create table t1(c1 int);
        //     delete from t1;
        // table dml param only need c1
        //     delete from t1 where rowid = 'xxx';
        // table dml param also need rowid column
      } else {
        if (OB_FAIL(ObTableParam::alloc_column(allocator_, column))) {
          LOG_WARN("alloc column failed", K(ret), K(i));
        } else if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*column_schema, *column))) {
          LOG_WARN("convert failed", K(*column_schema), K(ret), K(i));
        } else {
          ret = tmp_cols.push_back(column);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(columns_.assign(tmp_cols))) {
      LOG_WARN("assign failed", K(ret), K(tmp_cols.count()));
    } else if (OB_FAIL(ObTableParam::create_column_map(columns_, col_map_))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }
  return ret;
}

int ObTableSchemaParam::is_rowkey_column(const uint64_t column_id, bool& is_rowkey) const
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  if (OB_FAIL(col_map_.get(column_id, idx)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get idx from column map fail", K(ret), K(column_id));
  } else {
    is_rowkey = (idx >= 0 && idx < rowkey_column_num_);
  }
  return ret;
}

int ObTableSchemaParam::is_column_nullable(const uint64_t column_id, bool& is_nullable) const
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  if (OB_FAIL(col_map_.get(column_id, idx)) && OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get idx from column map fail", K(ret), K(column_id));
  } else if (idx < 0) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("column not exist", K(ret), K(column_id));
  } else {
    is_nullable = columns_.at(idx)->is_nullable();
  }
  return ret;
}

const ObColumnParam* ObTableSchemaParam::get_column(const uint64_t column_id) const
{
  int ret = OB_SUCCESS;
  const ObColumnParam* ptr = NULL;
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

const ObColumnParam* ObTableSchemaParam::get_column_by_idx(const int64_t idx) const
{
  const ObColumnParam* ptr = NULL;
  if (idx < 0 || idx >= columns_.count()) {
    LOG_WARN("idx out of range", K(idx), K(columns_.count()), K(lbt()));
  } else {
    ptr = columns_.at(idx);
  }
  return ptr;
}

const ObColumnParam* ObTableSchemaParam::get_rowkey_column_by_idx(const int64_t idx) const
{
  const ObColumnParam* ptr = NULL;
  if (idx < 0 || idx >= rowkey_column_num_) {
    LOG_WARN("idx out of range", K(idx), K(rowkey_column_num_));
  } else {
    ptr = columns_.at(idx);
  }
  return ptr;
}

int ObTableSchemaParam::get_rowkey_column_ids(ObIArray<ObColDesc>& column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("param not inited", K(ret), K(*this));
  } else {
    const ObColumnParam* param = NULL;
    ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_num_; ++i) {
      if (OB_ISNULL(param = columns_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column param is NULL", K(ret), K(i));
      } else {
        col_desc.col_id_ = param->get_column_id();
        col_desc.col_order_ = param->get_column_order();
        col_desc.col_type_ = param->get_meta_type();
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add rowkey column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSchemaParam::get_index_name(common::ObString& index_name) const
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

const ObString& ObTableSchemaParam::get_pk_name() const
{
  return pk_name_;
}

bool ObTableSchemaParam::is_depend_column(uint64_t column_id) const
{
  bool is_depend = false;
  int32_t idx = 0;
  if (is_materialized_view() && column_id > OB_MIN_MV_COLUMN_ID && column_id < OB_MIN_SHADOW_COLUMN_ID &&
      OB_SUCCESS == col_map_.get(column_id, idx)) {
    is_depend = true;
  }
  return is_depend;
}

int64_t ObTableSchemaParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
      K_(schema_version),
      K_(table_type),
      K_(index_type),
      K_(index_status),
      K_(rowkey_column_num),
      K_(shadow_rowkey_column_num),
      K_(fulltext_col_id),
      K_(index_name),
      K_(pk_name),
      "columns",
      ObArrayWrap<ObColumnParam*>(0 == columns_.count() ? NULL : &columns_.at(0), columns_.count()));
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
      rowkey_column_num_,
      shadow_rowkey_column_num_,
      fulltext_col_id_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize index name", K(ret));
    } else if (OB_FAIL(ObTableParam::serialize_columns(columns_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize columns", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, is_dropped_schema_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pk_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize pk name", K(ret));
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
      rowkey_column_num_,
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
    } else if (OB_FAIL(ObTableParam::create_column_map(columns_, col_map_))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, is_dropped_schema_);
  // for compatibility: at least need two bytes to deserialize an ObString
  const int64_t MINIMAL_NEEDED_SIZE = 2;
  if (OB_SUCC(ret) && (data_len - pos) > MINIMAL_NEEDED_SIZE) {
    ObString tmp_name;
    if (OB_FAIL(tmp_name.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize pk name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, pk_name_))) {
      LOG_WARN("failed to copy pk name", K(ret), K(tmp_name));
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
      rowkey_column_num_,
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
  LST_DO_CODE(OB_UNIS_ADD_LEN, is_dropped_schema_);
  len += pk_name_.get_serialize_size();
  return len;
}

// ------ ObTableDMLParam ------ //
ObTableDMLParam::ObTableDMLParam(common::ObIAllocator& allocator)
    : allocator_(allocator),
      tenant_schema_version_(OB_INVALID_VERSION),
      data_table_(allocator),
      index_tables_(allocator),
      col_descs_(allocator),
      col_map_(allocator)
{}

ObTableDMLParam::~ObTableDMLParam()
{
  reset();
}

void ObTableDMLParam::reset()
{
  tenant_schema_version_ = OB_INVALID_VERSION;
  data_table_.reset();
  index_tables_.reset();
  col_descs_.reset();
  col_map_.clear();
}

int ObTableDMLParam::convert(const ObTableSchema* table_schema,
    const common::ObIArray<const ObTableSchema*>& index_schemas, const int64_t tenant_schema_version,
    const common::ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* schema = NULL;
  ObTableSchemaParam* tmp = NULL;
  ObSEArray<ObTableSchemaParam*, OB_MAX_INDEX_PER_TABLE> tmp_index;
  if (OB_ISNULL(table_schema) || OB_INVALID_VERSION == tenant_schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_schema_version), KP(table_schema));
  } else if (OB_FAIL(data_table_.convert(table_schema, column_ids))) {
    LOG_WARN("convert data table fail", K(ret));
  } else if (OB_FAIL(prepare_storage_param(column_ids))) {
    LOG_WARN("prepare storage param fail", K(ret));
  } else {
    tenant_schema_version_ = tenant_schema_version;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      schema = index_schemas.at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema is NULL", K(ret), K(i));
      } else if (OB_ISNULL(tmp = alloc_schema())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc", K(ret), K(i));
      } else if (OB_FAIL(tmp->convert(schema, column_ids))) {
        LOG_WARN("convert index table fail", K(ret), K(i));
      } else if (OB_FAIL(tmp_index.push_back(tmp))) {
        LOG_WARN("push back index table fail", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_tables_.assign(tmp_index))) {
      LOG_WARN("assign index tables fail", K(ret), K(tmp_index.count()));
    }
  }
  return ret;
}

int64_t ObTableDMLParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_schema_version),
      K_(data_table),
      K_(col_descs),
      K_(col_map),
      "index_tables",
      ObArrayWrap<ObTableSchemaParam*>(
          0 == index_tables_.count() ? NULL : &index_tables_.at(0), index_tables_.count()));
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
    } else if (OB_FAIL(serialize_schemas(index_tables_, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize index tables", K(ret));
    }
  }
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
    } else if (OB_FAIL(deserialize_schemas(buf, data_len, pos, index_tables_))) {
      LOG_WARN("fail to deserialize index tables", K(ret));
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
  if (OB_FAIL(get_schemas_serialize_size(index_tables_, size))) {
    LOG_WARN("failed to get index tables serialize size", K(ret));
  } else {
    len += size;
  }
  return len;
}

int ObTableDMLParam::serialize_schemas(
    const TableSchemas& schemas, char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, schemas.count()))) {
    LOG_WARN("Fail to encode schema count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    if (OB_ISNULL(schemas.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else if (OB_FAIL(schemas.at(i)->serialize(buf, buf_len, pos))) {
      LOG_WARN("Fail to serialize schema", K(ret));
    }
  }
  return ret;
}

int ObTableDMLParam::deserialize_schemas(const char* buf, const int64_t data_len, int64_t& pos, TableSchemas& schemas)
{
  int ret = OB_SUCCESS;
  ObTableSchemaParam** schema = NULL;
  int64_t schema_cnt = 0;
  void* tmp_ptr = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &schema_cnt))) {
    LOG_WARN("Fail to decode schema count", K(ret));
  } else if (schema_cnt > 0) {
    if (NULL == (tmp_ptr = allocator_.alloc(schema_cnt * sizeof(ObTableSchemaParam*)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc", K(ret), K(schema_cnt));
    } else if (FALSE_IT(schema = static_cast<ObTableSchemaParam**>(tmp_ptr))) {
      // not reach
    } else {
      ObArray<ObTableSchemaParam*> tmp_schemas;
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_cnt; ++i) {
        ObTableSchemaParam*& cur_schema = schema[i];
        if (OB_ISNULL(cur_schema = alloc_schema())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to alloc", K(ret), K(i));
        } else if (OB_FAIL(cur_schema->deserialize(buf, data_len, pos))) {
          LOG_WARN("Fail to deserialize schema", K(ret));
        } else if (OB_FAIL(tmp_schemas.push_back(cur_schema))) {
          LOG_WARN("Fail to add schema", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schemas.assign(tmp_schemas))) {
          LOG_WARN("Fail to add schemas", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableDMLParam::get_schemas_serialize_size(const TableSchemas& schemas, int64_t& size) const
{
  int ret = OB_SUCCESS;
  size = 0;

  size += serialization::encoded_length_vi64(schemas.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    if (OB_ISNULL(schemas.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(i));
    } else {
      size += schemas.at(i)->get_serialize_size();
    }
  }
  return ret;
}

ObTableSchemaParam* ObTableDMLParam::alloc_schema()
{
  ObTableSchemaParam* schema = NULL;
  void* tmp_ptr = allocator_.alloc(sizeof(ObTableSchemaParam));
  if (NULL == tmp_ptr) {
    LOG_WARN("alloc failed");
  } else {
    schema = new (tmp_ptr) ObTableSchemaParam(allocator_);
  }
  return schema;
}

int ObTableDMLParam::prepare_storage_param(const ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = column_ids.count();
  if (col_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_ids", K(ret), K(col_cnt));
  } else {
    ObSEArray<ObColDesc, common::OB_DEFAULT_COL_DEC_NUM> tmp_col_descs;
    const ObColumnParam* col_param = nullptr;
    uint64_t column_id = OB_INVALID_ID;
    ObColDesc col_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      column_id = column_ids.at(i);
      if (nullptr == (col_param = data_table_.get_column(column_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get column param fail", K(ret), K(column_id), KP(col_param));
      } else {
        col_desc.col_id_ = column_id;
        col_desc.col_type_ = col_param->get_meta_type();
        col_desc.col_order_ = col_param->get_column_order();
        if (OB_FAIL(tmp_col_descs.push_back(col_desc))) {
          LOG_WARN("fail to push back column description", K(ret), K(col_desc));
        }
      }
    }

    // assign
    if (OB_SUCC(ret)) {
      if (OB_FAIL(col_descs_.assign(tmp_col_descs))) {
        LOG_WARN("fail to assign column description array", K(ret));
      } else if (OB_FAIL(col_map_.init(col_descs_))) {
        LOG_WARN("fail to init column map", K(ret));
      }
    }
  }
  return ret;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
