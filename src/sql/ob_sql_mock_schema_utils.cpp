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

#define USING_LOG_PREFIX SQL
#include "ob_sql_mock_schema_utils.h"

#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_param.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/rc/context.h"
#include "lib/string/ob_sql_string.h"
#include "lib/thread_local/ob_tsi_factory.h"

#define GET_MOCKED_TBLS(mocked_tables)                                                                            \
  ObSQLMockedTables* mocked_tables = GET_TSI_MULT(ObSQLMockedTables, ObSQLMockedTables::MOCKED_TABLE_IDENTIFIER); \
  OB_ASSERT(NULL != mocked_tables)

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::lib;

const char* const ROWID_IDX_TABLE_NAME = "__idx__rowid_index";

uint64_t ObSQLMockSchemaUtils::get_rowid_index_table_id(const uint64_t table_id)
{
  return (extract_pure_id(table_id) | ObSQLMockedTables::ROWID_INDEX_MASK);
}

void ObSQLMockSchemaUtils::pop_mock_table()
{
  GET_MOCKED_TBLS(mocked_tables);
  mocked_tables->pop_mock_table_and_index();
}
const ObIArray<uint64_t>& ObSQLMockSchemaUtils::get_all_mocked_tables()
{
  GET_MOCKED_TBLS(mocked_tables);
  return mocked_tables->get_table_ids();
}

int ObSQLMockSchemaUtils::add_mock_index(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  GET_MOCKED_TBLS(mocked_tables);
  const uint64_t mocked_rowid_idx_id = get_rowid_index_table_id(table_id);
  ObSQLMockedTables::ObMockedRowIDIndexInfo mocked_idx_info(table_id, mocked_rowid_idx_id);
  ret = mocked_tables->add_index(mocked_idx_info);

  LOG_TRACE("add mock index", K(mocked_idx_info));
  return ret;
}

int ObSQLMockSchemaUtils::add_mock_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  GET_MOCKED_TBLS(mocked_tables);
  LOG_TRACE("add mock table", K(table_id));
  if (OB_FAIL(mocked_tables->add_table(table_id))) {
    LOG_WARN("failed to mock rowid column", K(ret));
  } else if (OB_FAIL(add_mock_index(table_id))) {
    LOG_WARN("failed to mock rowid index", K(ret));
  }
  return ret;
}

bool ObSQLMockSchemaUtils::is_mock_table(const uint64_t table_id)
{
  GET_MOCKED_TBLS(mocked_tables);
  bool is_found = false;
  for (int i = 0; !is_found && i < mocked_tables->get_table_ids().count(); i++) {
    if (table_id == mocked_tables->get_table_ids().at(i)) {
      is_found = true;
    }
  }  // for end
  return is_found;
}

bool ObSQLMockSchemaUtils::is_mock_index(const uint64_t index_id, uint64_t& base_table_id)
{
  GET_MOCKED_TBLS(mocked_tables);
  bool is_found = false;
  base_table_id = OB_INVALID_ID;
  for (int i = 0; !is_found && i < mocked_tables->get_index_ids().count(); i++) {
    if (index_id == mocked_tables->get_index_ids().at(i).mocked_table_id_) {
      is_found = true;
      base_table_id = mocked_tables->get_index_ids().at(i).org_table_id_;
    }
  }  // for end
  return is_found;
}

bool ObSQLMockSchemaUtils::is_mock_index(const uint64_t index_id)
{
  uint64_t unused_base_table_id = OB_INVALID_ID;
  bool ret_bool = is_mock_index(index_id, unused_base_table_id);
  if (ret_bool) {
    OB_ASSERT(OB_INVALID_ID != unused_base_table_id);  // debug
  }
  return ret_bool;
}

bool ObSQLMockSchemaUtils::contain_mock_index(const uint64_t table_id)
{
  bool is_contain = false;
  GET_MOCKED_TBLS(mocked_tables);
  for (int i = 0; !is_contain && i < mocked_tables->get_index_ids().count(); i++) {
    if (table_id == mocked_tables->get_index_ids().at(i).org_table_id_) {
      is_contain = true;
    }
  }  // for end
  return is_contain;
}

const char* ObSQLMockSchemaUtils::get_rowid_index_name()
{
  const static char* ret_idx_name = "ROWID_INDEX";
  return ret_idx_name;
}

int ObSQLMockSchemaUtils::reset_mock_table()
{
  int ret = OB_SUCCESS;
  GET_MOCKED_TBLS(mocked_tables);
  mocked_tables->reset();
  return ret;
}

uint64_t ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(const uint64_t mocked_index_id)
{
  GET_MOCKED_TBLS(mocked_tables);
  uint64_t real_table_id = OB_INVALID_ID;
  for (int i = 0; OB_INVALID_ID == real_table_id && i < mocked_tables->get_index_ids().count(); i++) {
    if (mocked_index_id == mocked_tables->get_index_ids().at(i).mocked_table_id_) {
      real_table_id = mocked_tables->get_index_ids().at(i).org_table_id_;
    }
  }  // for end;
  return real_table_id;
}

int ObSQLMockSchemaUtils::prepare_mocked_schemas(const common::ObIArray<uint64_t>& mock_rowid_tables)
{
  int ret = OB_SUCCESS;
  // prepare mocked schemas
  for (int i = 0; OB_SUCC(ret) && i < mock_rowid_tables.count(); i++) {
    if (OB_FAIL(add_mock_table(mock_rowid_tables.at(i)))) {
      LOG_WARN("failed to mock rowid column", K(ret));
    }
  }  // for end
  return ret;
}

ObSQLMockSchemaGuard::ObSQLMockSchemaGuard()
{
  ObSQLMockSchemaUtils::reset_mock_table();
}

ObSQLMockSchemaGuard::~ObSQLMockSchemaGuard()
{
  ObSQLMockSchemaUtils::reset_mock_table();
}

int ObSQLMockSchemaUtils::mock_rowid_index(
    const ObTableSchema* base_table_schema, const ObTableSchema*& mocked_rowid_index_schema)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != base_table_schema);
  ObIAllocator& allocator = THIS_WORKER.get_sql_arena_allocator();
  const uint64_t tenant_id = base_table_schema->get_tenant_id();
  ObTableSchema rowid_idx_schema;
  ObTableSchema* mocked_schema = NULL;
  if (OB_ISNULL(base_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema", K(ret));
  } else {
    rowid_idx_schema.set_tenant_id(tenant_id);
    rowid_idx_schema.set_table_id(get_rowid_index_table_id(base_table_schema->get_table_id()));
    rowid_idx_schema.set_database_id(base_table_schema->get_database_id());
    rowid_idx_schema.set_tablespace_id(base_table_schema->get_tablespace_id());
    rowid_idx_schema.set_tablegroup_id(OB_INVALID_ID);
    rowid_idx_schema.set_table_type(USER_INDEX);
    rowid_idx_schema.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
    rowid_idx_schema.set_index_using_type(USING_BTREE);
    rowid_idx_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
    rowid_idx_schema.set_def_type(TABLE_DEF_TYPE_USER);
    rowid_idx_schema.set_rowkey_column_num(base_table_schema->get_rowkey_column_num() + 1);
    rowid_idx_schema.set_index_column_num(1);
    rowid_idx_schema.set_replica_num(-1);
    rowid_idx_schema.set_autoinc_column_id(0);
    rowid_idx_schema.set_auto_increment(0);
    rowid_idx_schema.set_read_only(true);
    rowid_idx_schema.set_rowkey_split_pos(0);
    rowid_idx_schema.set_max_used_column_id(base_table_schema->get_max_used_column_id());
    rowid_idx_schema.set_part_level(PARTITION_LEVEL_ZERO);
    rowid_idx_schema.set_create_mem_version(base_table_schema->get_create_mem_version());
    rowid_idx_schema.set_schema_version(base_table_schema->get_schema_version());
    rowid_idx_schema.set_block_size(OB_DEFAULT_SSTABLE_BLOCK_SIZE);
    rowid_idx_schema.set_progressive_merge_num(base_table_schema->get_progressive_merge_num());
    rowid_idx_schema.set_table_name(ROWID_IDX_TABLE_NAME);
    rowid_idx_schema.set_compress_func_name(ObString::make_string("none"));
    rowid_idx_schema.set_is_use_bloomfilter(false);
    rowid_idx_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    rowid_idx_schema.set_charset_type(ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_BIN));
    rowid_idx_schema.set_data_table_id(base_table_schema->get_table_id());
    rowid_idx_schema.set_index_status(INDEX_STATUS_AVAILABLE);
    rowid_idx_schema.set_partition_status(PARTITION_STATUS_ACTIVE);
    rowid_idx_schema.set_partition_schema_version(base_table_schema->get_partition_schema_version());
    rowid_idx_schema.set_comment(ObString::make_string(""));
    rowid_idx_schema.set_locality(base_table_schema->get_locality());
    rowid_idx_schema.set_previous_locality(base_table_schema->get_previous_locality());
    rowid_idx_schema.set_parser_name(base_table_schema->get_parser_name());
    rowid_idx_schema.set_tablet_size(base_table_schema->get_tablet_size());
    rowid_idx_schema.set_pctfree(base_table_schema->get_pctfree());
    // TODO set rest infomation

    const ObColumnSchemaV2* col_schema = NULL;
    const ObRowkeyInfo& rowkey_info = base_table_schema->get_rowkey_info();
    uint64_t cur_rowkey_pos = 1;
    ObArray<uint64_t> rowkey_column_ids;
    ObArray<ObColDesc> normal_column_descs;

    if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, rowid_idx_schema, mocked_schema))) {
      LOG_WARN("failed ti allocate schema", K(ret));
    } else if (OB_FAIL(rowkey_info.get_column_ids(rowkey_column_ids))) {
      LOG_WARN("failed to get column ids", K(ret));
    } else if (OB_FAIL(base_table_schema->get_column_ids_without_rowkey(normal_column_descs))) {
      LOG_WARN("failed to get all normal column descs", K(ret));
    } else {
      if (OB_ISNULL(col_schema = base_table_schema->get_column_schema(OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected null column schema", K(ret));
      } else {
        ObColumnSchemaV2 tmp_column = *col_schema;
        tmp_column.set_rowkey_position(cur_rowkey_pos++);
        tmp_column.set_index_position(1);
        tmp_column.set_table_id(mocked_schema->get_table_id());
        if (OB_FAIL(mocked_schema->add_column(tmp_column))) {
          LOG_WARN("failed to add column schema", K(ret));
        }
      }
      for (int i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); i++) {
        if (OB_ISNULL(col_schema = base_table_schema->get_column_schema(rowkey_column_ids.at(i)))) {
          LOG_WARN("failed to get column schema", K(ret));
        } else {
          ObColumnSchemaV2 tmp_column = *col_schema;
          tmp_column.set_rowkey_position(cur_rowkey_pos++);
          tmp_column.set_index_position(0);
          tmp_column.set_table_id(mocked_schema->get_table_id());
          if (OB_FAIL(mocked_schema->add_column(tmp_column))) {
            LOG_WARN("failed to add column schema", K(ret));
          }
        }
      }  // for end

      for (int i = 0; OB_SUCC(ret) && i < normal_column_descs.count(); i++) {
        if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == normal_column_descs.at(i).col_id_) {
          // do nothing
        } else if (OB_ISNULL(col_schema = base_table_schema->get_column_schema(normal_column_descs.at(i).col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret));
        } else {
          ObColumnSchemaV2 tmp_column = *col_schema;
          tmp_column.set_index_position(0);
          tmp_column.set_table_id(mocked_schema->get_table_id());
          if (OB_FAIL(mocked_schema->add_column(tmp_column))) {
            LOG_WARN("failed to add column schema", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (mocked_schema != NULL) {
      mocked_schema->~ObTableSchema();
      allocator.free(mocked_schema);
    }
  } else {
    mocked_rowid_index_schema = mocked_schema;
    LOG_TRACE("get mocked rowid index", K(*mocked_schema));
  }
  return ret;
}

int ObSQLMockSchemaUtils::mock_rowid_column(share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  bool is_rowid_exists = false;
  if (NULL != table_schema.get_column_schema(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)) {
    is_rowid_exists = true;
  }
  if (is_virtual_table(table_schema.get_table_id()) || share::is_mysql_mode() || table_schema.is_index_table() ||
      is_rowid_exists) {
    LOG_DEBUG("do not mock rowid column",
        K(table_schema.get_table_name_str()),
        K(share::is_mysql_mode()),
        K(table_schema.is_index_table()));
  } else {
    ObColumnSchemaV2 rowid_schema;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = extract_pure_id(table_schema.get_table_id());
    const int64_t schema_version = table_schema.get_schema_version();
    ObObj null_obj;
    null_obj.set_null();
    rowid_schema.set_tenant_id(tenant_id);
    if (OB_INVALID_ID == table_id || 0 == table_id || OB_SYS_TENANT_ID == tenant_id) {
      rowid_schema.set_table_id(table_id);
    } else {
      rowid_schema.set_table_id(combine_id(tenant_id, table_id));
    }
    rowid_schema.set_column_id(OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID);
    rowid_schema.set_rowkey_position(0);
    rowid_schema.set_index_position(0);
    rowid_schema.set_tbl_part_key_pos(0);
    rowid_schema.set_data_type(ObURowIDType);
    rowid_schema.set_data_length(OB_MAX_USER_ROW_KEY_LENGTH);
    rowid_schema.set_data_precision(-1);
    rowid_schema.set_zero_fill(false);
    rowid_schema.set_nullable(true);
    rowid_schema.set_autoincrement(false);
    rowid_schema.set_is_hidden(true);
    rowid_schema.set_on_update_current_timestamp(false);
    rowid_schema.set_order_in_rowkey(ASC);  // what this column means?
    rowid_schema.set_collation_type(CS_TYPE_BINARY);
    rowid_schema.set_charset_type(ObCharset::charset_type_by_coll(CS_TYPE_BINARY));
    rowid_schema.set_schema_version(schema_version);
    rowid_schema.set_column_name(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
    rowid_schema.set_comment("");
    rowid_schema.set_column_flags(0);
    rowid_schema.set_orig_default_value(null_obj);
    rowid_schema.set_cur_default_value(null_obj);
    rowid_schema.set_orig_default_value_v2(null_obj);
    rowid_schema.set_cur_default_value_v2(null_obj);

    if (OB_FAIL(table_schema.add_column(rowid_schema))) {
      LOG_ERROR("failed to add rowid column", K(ret));
    } else {
      LOG_TRACE(
          "mocked rowid column", K(table_schema.get_column_schema(OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID)), K(table_schema));
    }
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase
