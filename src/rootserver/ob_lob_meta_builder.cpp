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
#include "ob_lob_meta_builder.h"
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

ObLobMetaBuilder::ObLobMetaBuilder(ObDDLService &ddl_service)
  : ddl_service_(ddl_service)
{
}

ObLobMetaBuilder::~ObLobMetaBuilder()
{
}

int ObLobMetaBuilder::generate_aux_lob_meta_schema(
    ObSchemaService *schema_service,
    const share::schema::ObTableSchema &data_schema,
    const uint64_t specified_table_id,
    share::schema::ObTableSchema &aux_lob_meta_schema,
    bool need_generate_id)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited() || NULL == schema_service) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    uint64_t new_table_id = specified_table_id;
    const int64_t buf_size = 64;
    char buf[buf_size];
    MEMSET(buf, 0, buf_size);
    int64_t pos = 0;
    if (OB_FAIL(generate_schema(data_schema, aux_lob_meta_schema))) {
      LOG_WARN("generate_schema for aux vp table failed", K(data_schema), K(ret));
    } else if (OB_INVALID_ID == new_table_id
               && OB_FAIL(schema_service->fetch_new_table_id(data_schema.get_tenant_id(), new_table_id))) {
      LOG_WARN("failed to fetch_new_table_id", "tenant_id", data_schema.get_tenant_id(), K(ret));
    } else if (OB_FAIL(generate_lob_meta_table_name(new_table_id, buf, buf_size, pos))) {
      LOG_WARN("failed to generate_lob_meta_table_name", K(ret), K(new_table_id));
    } else {
      ObString aux_lob_meta_table_name(pos, buf);
      aux_lob_meta_schema.set_table_id(new_table_id);
      aux_lob_meta_schema.set_table_type(AUX_LOB_META);
      if (OB_FAIL(aux_lob_meta_schema.set_table_name(aux_lob_meta_table_name))) {
        LOG_WARN("set_table_name failed", K(aux_lob_meta_table_name), K(ret));
      } else {
        // column
        int64_t column_count = aux_lob_meta_schema.get_column_count();
        for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
          ObColumnSchemaV2 *column = NULL;
          if (NULL == (column = aux_lob_meta_schema.get_column_schema_by_idx(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column is null", K(ret));
          } else {
            column->set_tenant_id(aux_lob_meta_schema.get_tenant_id());
            column->set_table_id(aux_lob_meta_schema.get_table_id());
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (data_schema.get_part_level() > 0 && 
               OB_FAIL(aux_lob_meta_schema.assign_partition_schema(data_schema))) {
      LOG_WARN("fail to assign partition schema", K(aux_lob_meta_schema), K(ret));
    } else if (need_generate_id) {
      if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(aux_lob_meta_schema))) {
        LOG_WARN("fail to fetch new object id", K(aux_lob_meta_schema), K(ret));
      } else if (OB_FAIL(ddl_service_.generate_tablet_id(aux_lob_meta_schema))) {
        LOG_WARN("fail to fetch new tablet id", K(aux_lob_meta_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObLobMetaBuilder::generate_schema(
    const share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_lob_meta_schema)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // reuse inner lob table create schema
    if (OB_FAIL(ObInnerTableSchema::all_column_aux_lob_meta_schema(aux_lob_meta_schema))) {
      LOG_WARN("get lob meta schema failed", K(data_schema), K(ret));
    } else if (OB_FAIL(set_basic_infos(data_schema, aux_lob_meta_schema))) {
      LOG_WARN("set_basic_infos failed", K(data_schema), K(ret));
    }
  }

  return ret;
}

int ObLobMetaBuilder::set_basic_infos(
    const share::schema::ObTableSchema &data_schema,
    share::schema::ObTableSchema &aux_lob_meta_schema)
{
  int ret = OB_SUCCESS;
  aux_lob_meta_schema.set_table_type(AUX_LOB_META);
  aux_lob_meta_schema.set_data_table_id(data_schema.get_table_id());

  // priority same with data table schema
  aux_lob_meta_schema.set_tenant_id(data_schema.get_tenant_id());
  aux_lob_meta_schema.set_database_id(data_schema.get_database_id());
  if (is_inner_table(data_schema.get_table_id())) {
    aux_lob_meta_schema.set_tablegroup_id(data_schema.get_tablegroup_id());
  } else {
    aux_lob_meta_schema.set_tablegroup_id(OB_INVALID_ID);
  }
  aux_lob_meta_schema.set_load_type(data_schema.get_load_type());
  aux_lob_meta_schema.set_def_type(data_schema.get_def_type());
  aux_lob_meta_schema.set_charset_type(data_schema.get_charset_type());
  aux_lob_meta_schema.set_collation_type(data_schema.get_collation_type());
  aux_lob_meta_schema.set_row_store_type(data_schema.get_row_store_type());
  aux_lob_meta_schema.set_store_format(data_schema.get_store_format());
  aux_lob_meta_schema.set_part_level(data_schema.get_part_level());
  if (data_schema.get_max_used_column_id() > aux_lob_meta_schema.get_max_used_column_id()) {
    aux_lob_meta_schema.set_max_used_column_id(data_schema.get_max_used_column_id());
  }
  aux_lob_meta_schema.set_autoinc_column_id(0);
  aux_lob_meta_schema.set_progressive_merge_num(data_schema.get_progressive_merge_num());
  // keep the initial value for table_mode, do not get table mode from data_schema which may be hidden table
  // aux_lob_meta_schema.set_table_mode(data_schema.get_table_mode());
  aux_lob_meta_schema.set_block_size(data_schema.get_block_size());
  aux_lob_meta_schema.set_pctfree(data_schema.get_pctfree());
  aux_lob_meta_schema.set_storage_format_version(data_schema.get_storage_format_version());
  aux_lob_meta_schema.set_progressive_merge_round(data_schema.get_progressive_merge_round());
  aux_lob_meta_schema.set_duplicate_scope(data_schema.get_duplicate_scope());
  if (OB_FAIL(aux_lob_meta_schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
    LOG_WARN("set_compress_func_name failed", K(data_schema));
  }
  return ret;
}

int ObLobMetaBuilder::generate_lob_meta_table_name(
    const uint64_t new_table_id,
    char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  // ObString aux_lob_meta_table_name构成：
  // __AUX_LOB_META_<table_id>_
  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size), K(pos));
  } else if ((pos = snprintf(buf, buf_size, "__AUX_LOB_META_%lu_", new_table_id)) >= buf_size || pos < 0) {
    ret = common::OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not large enough", K(ret), K(buf_size), K(new_table_id));
  }

  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
