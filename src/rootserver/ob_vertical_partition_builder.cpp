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

#define USING_LOG_PREFIX RS
#include "ob_vertical_partition_builder.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_index_builder_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{

ObVertialPartitionBuilder::ObVertialPartitionBuilder(ObDDLService &ddl_service)
  : ddl_service_(ddl_service)
{
}

ObVertialPartitionBuilder::~ObVertialPartitionBuilder()
{
}

int ObVertialPartitionBuilder::generate_aux_vp_table_schema(
    ObSchemaService *schema_service,
    const obrpc::ObCreateVertialPartitionArg &vp_arg,
    const int64_t frozen_version,
    share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_vp_table_schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited() || NULL == schema_service) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (frozen_version <= 0 || !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(frozen_version), K(data_schema), K(ret));
  } else {
    uint64_t new_table_id = OB_INVALID_ID;
    const int64_t buf_size = 64;
    char buf[buf_size];
    MEMSET(buf, 0, buf_size);
    int64_t pos = 0;
    if (OB_FAIL(generate_schema(vp_arg, frozen_version, data_schema, aux_vp_table_schema))) {
      LOG_WARN("generate_schema for aux vp table failed", K(vp_arg), K(frozen_version), K(data_schema), K(ret));
    } else if (OB_FAIL(schema_service->fetch_new_table_id(data_schema.get_tenant_id(), new_table_id))) {
      LOG_WARN("failed to fetch_new_table_id", "tenant_id", data_schema.get_tenant_id(), K(ret));
    } else if (OB_FAIL(generate_vp_table_name(new_table_id, buf, buf_size, pos))) {
      LOG_WARN("failed to generate_vp_table_name", K(ret), K(new_table_id));
    } else {
      ObString aux_vp_table_name(pos, buf);
      aux_vp_table_schema.set_table_id(new_table_id);
      aux_vp_table_schema.set_data_table_id(data_schema.get_table_id());
      aux_vp_table_schema.set_table_type(AUX_VERTIAL_PARTITION_TABLE);
      if (OB_FAIL(aux_vp_table_schema.set_table_name(aux_vp_table_name))) {
        LOG_WARN("set_table_name failed", K(aux_vp_table_name), K(ret));
      }
    }
  }
  return ret;
}

int ObVertialPartitionBuilder::generate_schema(
    const obrpc::ObCreateVertialPartitionArg &vp_arg,
    const int64_t frozen_version,
    share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_vp_table_schema)
{
  int ret = OB_SUCCESS;
  if (vp_arg.vertical_partition_columns_.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("vertical partition columns can't be empty", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vp_arg.vertical_partition_columns_.count(); ++i) {
    ObColumnSchemaV2 *data_column = NULL;
    const ObString vp_column_name(vp_arg.vertical_partition_columns_.at(i));
    if (NULL == (data_column = data_schema.get_column_schema(vp_column_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, vp_column_name.length(), vp_column_name.ptr());
      LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
               "database_id", data_schema.get_database_id(),
               "table_name", data_schema.get_table_name(),
               "column name", vp_column_name, K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_basic_infos(frozen_version, data_schema, aux_vp_table_schema))) {
      LOG_WARN("set_basic_infos failed", K(vp_arg), K(frozen_version), K(data_schema), K(ret));
    } else if (OB_FAIL(set_aux_vp_table_columns(vp_arg, data_schema, aux_vp_table_schema))) {
      LOG_WARN("set_index_table_columns failed", K(vp_arg), K(data_schema), K(ret));
    }
  }

  return ret;
}

int ObVertialPartitionBuilder::set_basic_infos(
    const int64_t frozen_version,
    const share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_vp_table_schema)
{
  int ret = OB_SUCCESS;
  aux_vp_table_schema.set_table_type(AUX_VERTIAL_PARTITION_TABLE);
  aux_vp_table_schema.set_data_table_id(data_schema.get_table_id());

  // priority same with data table schema
  aux_vp_table_schema.set_tenant_id(data_schema.get_tenant_id());
  aux_vp_table_schema.set_database_id(data_schema.get_database_id());
  aux_vp_table_schema.set_tablegroup_id(OB_INVALID_ID);
  aux_vp_table_schema.set_load_type(data_schema.get_load_type());
  aux_vp_table_schema.set_def_type(data_schema.get_def_type());
  aux_vp_table_schema.set_charset_type(data_schema.get_charset_type());
  aux_vp_table_schema.set_collation_type(data_schema.get_collation_type());
  aux_vp_table_schema.set_row_store_type(data_schema.get_row_store_type());
  aux_vp_table_schema.set_store_format(data_schema.get_store_format());

  if (data_schema.get_max_used_column_id() > aux_vp_table_schema.get_max_used_column_id()) {
    aux_vp_table_schema.set_max_used_column_id(data_schema.get_max_used_column_id());
  }
  aux_vp_table_schema.set_autoinc_column_id(0);
  aux_vp_table_schema.set_progressive_merge_num(data_schema.get_progressive_merge_num());
  aux_vp_table_schema.set_table_mode(data_schema.get_table_mode());
  aux_vp_table_schema.set_block_size(data_schema.get_block_size());
  aux_vp_table_schema.set_pctfree(data_schema.get_pctfree());
  aux_vp_table_schema.set_storage_format_version(data_schema.get_storage_format_version());
  aux_vp_table_schema.set_progressive_merge_round(data_schema.get_progressive_merge_round());
  if (OB_FAIL(aux_vp_table_schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
    LOG_WARN("set_compress_func_name failed", K(data_schema));
  }

  return ret;
}

int ObVertialPartitionBuilder::set_aux_vp_table_columns(
    const obrpc::ObCreateVertialPartitionArg &vp_arg,
    share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_vp_table_schema)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObRowDesc, row_desc) {
    // index columns
    // attention: must add data table pk column first
    const ObColumnSchemaV2 *data_column = NULL;
    const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      data_column = NULL;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("get_column_id failed", "index", i, K(ret));
      } else if (NULL == (data_column = data_schema.get_column_schema(column_id))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", K(column_id), K(ret));
      } else {
        ObColumnSchemaV2 column_schema = *data_column;
        // 在处理主分区表时，将主表的垂直分区列原地标记为PRIMARY_VP_COLUMN_FLAG
        // 将主表的rowkey copy进入副表时，应该将该标记清除
        column_schema.del_column_flag(PRIMARY_VP_COLUMN_FLAG);
        column_schema.set_is_hidden(true);
        if (OB_FAIL(ObIndexBuilderUtil::add_column(&column_schema, false /*is_index_column*/, true /*is_rowkey*/,
            data_column->get_order_in_rowkey(), row_desc, aux_vp_table_schema, false /* is_hidden */, false /* is_specified_storing_col */))) {
          LOG_WARN("add column failed", K(column_schema), K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < vp_arg.vertical_partition_columns_.count(); ++i) {
      data_column = NULL;
      ObString column_name(vp_arg.vertical_partition_columns_.at(i));
      if (NULL == (data_column = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", K(column_name), K(ret));
      } else {
        ObColumnSchemaV2 column_schema = *data_column;
        if (column_schema.is_rowkey_column()) {
          // 无需添加该列，因为第一步已经添加该列，但仍需要给该列打标
          ObColumnSchemaV2 *rowkey_column = NULL;
          if (NULL == (rowkey_column = aux_vp_table_schema.get_column_schema(column_name))) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            LOG_WARN("get_column_schema failed", K(column_name), K(ret));
          } else {
            rowkey_column->set_column_flags(AUX_VP_COLUMN_FLAG);
            rowkey_column->set_is_hidden(false);
          }
        } else if (FALSE_IT(column_schema.set_column_flags(AUX_VP_COLUMN_FLAG))) {
          // 副表列上都标记AUX_VP_COLUMN_FLAG
        } else if (OB_FAIL(ObIndexBuilderUtil::add_column(
            &column_schema,
            false /*is_index_column*/,
            false /*is_rowkey*/,
            column_schema.get_order_in_rowkey(),
            row_desc,
            aux_vp_table_schema,
            false /* is_hidden */, 
            false /* is_specified_storing_col */))) {
          LOG_WARN("add column failed", K(column_schema), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObVertialPartitionBuilder::set_primary_vp_table_options(
    const obrpc::ObCreateVertialPartitionArg &vp_arg,
    share::schema::ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *data_column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < vp_arg.vertical_partition_columns_.count(); ++i) {
    data_column = NULL;
    ObString column_name(vp_arg.vertical_partition_columns_.at(i));
    if (NULL == (data_column = data_schema.get_column_schema(column_name))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("get_column_schema failed", "table_id", data_schema.get_table_id(),
          K(column_name), K(ret));
    } else {
      data_column->set_column_flags(PRIMARY_VP_COLUMN_FLAG);
    }
  }

  return ret;
}

int ObVertialPartitionBuilder::generate_vp_table_name(
    const uint64_t new_table_id,
    char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  // ObString aux_vp_table_name构成：
  // __AUX_VP_<table_id>_
  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size), K(pos));
  } else if ((pos = snprintf(buf, buf_size, "__AUX_VP_%lu_", new_table_id)) >= buf_size || pos < 0) {
    ret = common::OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not large enough", K(ret), K(buf_size), K(new_table_id));
  }

  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
