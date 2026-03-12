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

#define USING_LOG_PREFIX COMMON
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "lib/udt/ob_array_utils.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;

namespace share
{

int ObSearchIndexBuilderUtil::generate_search_index_name(
    obrpc::ObCreateIndexArg &arg,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  share::schema::ObIndexType type = arg.index_type_;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (!is_search_data_index(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    MEMSET(name_buf, 0, OB_MAX_TABLE_NAME_LENGTH);
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(name_buf, OB_MAX_TABLE_NAME_LENGTH, pos,
                                "%.*s_search_data",
                                arg.index_name_.length(),
                                arg.index_name_.ptr()))) {
      LOG_WARN("failed to format search data index name", K(ret));
    } else {
      arg.index_name_.assign_ptr(name_buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::append_search_index_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg search_index_arg;
  if (OB_ISNULL(allocator) ||
      !is_search_def_index(index_arg.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(search_index_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to search arg", K(ret));
  } else if (FALSE_IT(search_index_arg.index_type_ = INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL)) {
  } else if (OB_FAIL(generate_search_index_name(search_index_arg, allocator))) {
    LOG_WARN("failed to generate search index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(search_index_arg))) {
    LOG_WARN("failed to push back search arg", K(ret));
  }
  return ret;
}

int ObSearchIndexBuilderUtil::set_search_index_table_columns(
    const obrpc::ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() || !share::schema::is_search_data_index(arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for search index", K(ret), K(data_schema), K(arg.index_type_));
  } else {
    int64_t rowkey_pos = 0;
    const int64_t tenant_id = data_schema.get_tenant_id();
    struct ColumnDef {
      const char *name;
      ObObjType type;
      int64_t length;
      bool is_binary;
      bool is_rowkey;
    } columns[] = {
      {"COL_IDX",    ObIntType,     0,                          false, true},
      {"PATH",       ObVarcharType, SEARCH_INDEX_PATH_LENGTH,   true, true},
      {"VALUE",      ObVarcharType, SEARCH_INDEX_VALUE_LENGTH,  true,  true},
      {"DOC_ID",     ObUInt64Type,  0,                          false, true},
    };
    const int64_t column_cnt = static_cast<int64_t>(sizeof(columns) / sizeof(columns[0]));
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      const ColumnDef &c = columns[i];
      if (OB_FAIL(add_search_index_column(data_schema,
                                          index_schema,
                                          tenant_id,
                                          c.name,
                                          c.type,
                                          c.length,
                                          c.is_binary,
                                          c.is_rowkey,
                                          rowkey_pos))) {
        LOG_WARN("failed to add search column", K(ret), K(i), K(c.name));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(rowkey_pos);
      index_schema.set_index_column_num(rowkey_pos);
      if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort columns", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::check_single_layer_array_for_search_index(
    const share::schema::ObColumnSchemaV2 &column_schema,
    ObIAllocator *allocator,
    bool &is_supported)
{
  int ret = OB_SUCCESS;
  is_supported = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (!column_schema.is_collection()) {
    // not collection column, ignore
    is_supported = false;
  } else {
    const ObIArray<ObString> &type_infos = column_schema.get_extended_type_info();
    ObSqlCollectionInfo *coll_info = nullptr;
    if (OB_FAIL(ObArrayUtil::get_collection_info(type_infos, *allocator, coll_info))) {
      LOG_WARN("failed to get collection info", K(ret), K(type_infos));
    } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection info or meta is null", K(ret), KP(coll_info));
    } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_ARRAY_TYPE) {
      is_supported = false;
    } else {
      uint32_t depth = 0;
      (void) coll_info->collection_meta_->get_basic_meta(depth);
      is_supported = (depth == 1);
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::add_search_index_column(
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema,
    const int64_t tenant_id,
    const char *name,
    ObObjType type,
    const int64_t length,
    const bool is_binary,
    const bool is_rowkey,
    int64_t &rowkey_pos)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 col;
  col.set_tenant_id(tenant_id);
  col.set_column_name(name);
  col.set_data_type(type);
  col.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[type]);
  if (type == ObVarcharType) {
    col.set_data_length(length);
    if (is_binary) {
      col.set_collation_type(CS_TYPE_BINARY);
      col.set_charset_type(ObCharset::charset_type_by_coll(data_schema.get_collation_type()));
    } else {
      col.set_collation_type(data_schema.get_collation_type());
    }
  } else {
    col.set_collation_type(CS_TYPE_BINARY);
  }
  col.set_rowkey_position(is_rowkey ? (++rowkey_pos) : 0);
  col.set_index_position(is_rowkey ? rowkey_pos : 0);
  col.set_order_in_rowkey(common::ObOrderType::ASC);
  col.set_prev_column_id(UINT64_MAX);
  col.set_next_column_id(UINT64_MAX);
  col.set_tbl_part_key_pos(0);
  col.set_table_id(OB_INVALID_ID);
  col.set_is_hidden(false);

  uint64_t new_col_id = index_schema.get_max_used_column_id() + 1;
  col.set_column_id(new_col_id);
  if (OB_FAIL(index_schema.add_column(col))) {
    LOG_WARN("add search column failed", K(ret), K(col));
  }
  return ret;
}

int ObSearchIndexBuilderUtil::get_dropping_search_data_index_invisiable_index_schema(
    const uint64_t tenant_id,
    const ObTableSchema &index_table_schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas)
{
  int ret = OB_SUCCESS;
  if (index_table_schema.is_search_def_index()) {
    const ObIArray<share::schema::ObAuxTableMetaInfo> &indexs = index_table_schema.get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < indexs.count(); ++i) {
      const ObTableSchema *data_index_schema = NULL;
      const share::schema::ObAuxTableMetaInfo &info = indexs.at(i);
      if (!share::schema::is_search_data_index(info.index_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect data search index", KR(ret), K(info.index_type_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                       info.table_id_,
                                                       data_index_schema))) {
        LOG_WARN("fail to get search data index schema", K(ret),
                  "data_index_table_id", info.table_id_);
      } else if (OB_ISNULL(data_index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, data search index schema is nullptr", K(ret), K(info));
      } else if (OB_FAIL(new_aux_schemas.push_back(*data_index_schema))) {
        LOG_WARN("fail to push doc rowkey table schema", K(ret), KPC(data_index_schema));
      }
    }
  }
  return ret;
}

int ObSearchIndexBuilderUtil::get_search_index_column_name(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  col_names.reset();
  if (!index_table_schema.is_search_index()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not a search def index", K(ret), K(index_table_schema));
  } else {
    // Search index uses the original create index column names
    // stored in the index_schema's index_columns
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_schema.get_column_count(); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(index_table_schema));
      } else if (col_schema->get_index_position() > 0) {
        // This is an index column, get the original column name from data table
        const ObColumnSchemaV2 *data_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(data_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data column not found", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
        } else if (OB_FAIL(col_names.push_back(data_col_schema->get_column_name_str()))) {
          LOG_WARN("fail to push back col names", K(ret), K(data_col_schema->get_column_name_str()));
        }
      }
    }
  }
  return ret;
}

} // namespace share
}//end namespace oceanbase
