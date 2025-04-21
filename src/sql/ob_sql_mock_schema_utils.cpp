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
#include "lib/allocator/ob_allocator.h"
#include "lib/rc/context.h"
#include "lib/string/ob_sql_string.h"
#include "lib/thread_local/ob_tsi_factory.h"

#define GET_MOCKED_TBLS(mocked_tables)                                \
  ObSQLMockedTables *mocked_tables = GET_TSI_MULT(                    \
      ObSQLMockedTables, ObSQLMockedTables::MOCKED_TABLE_IDENTIFIER);

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::lib;

int ObSQLMockSchemaUtils::add_mock_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  GET_MOCKED_TBLS(mocked_tables);
  if (OB_ISNULL(mocked_tables)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory for mocked_tables", K(ret));
  } else if (OB_FAIL(mocked_tables->add_table(table_id))) {
    LOG_WARN("failed to mock rowid column", K(ret));
  }
  return ret;
}

bool ObSQLMockSchemaUtils::is_mock_table(const uint64_t table_id)
{
  GET_MOCKED_TBLS(mocked_tables);
  bool is_found = false;
  if (mocked_tables != nullptr) {
    for (int i = 0; !is_found && i < mocked_tables->get_table_ids().count(); i++) {
      if (table_id == mocked_tables->get_table_ids().at(i)) {
        is_found = true;
      }
    } // for end
  }
  return is_found;
}

int ObSQLMockSchemaUtils::reset_mock_table()
{
  int ret = OB_SUCCESS;
  GET_MOCKED_TBLS(mocked_tables);
  if (OB_ISNULL(mocked_tables)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory for mocked_tables", K(ret));
  } else {
    mocked_tables->reset();
  }
  return ret;
}

int ObSQLMockSchemaUtils::prepare_mocked_schemas(
                                          const common::ObIArray<uint64_t> &mock_rowid_tables)
{
  int ret = OB_SUCCESS;
  // prepare mocked schemas
  for (int i = 0; OB_SUCC(ret) && i < mock_rowid_tables.count(); i++) {
    if (OB_FAIL(add_mock_table(mock_rowid_tables.at(i)))) {
      LOG_WARN("failed to mock rowid column", K(ret));
    }
  } // for end
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

int ObSQLMockSchemaUtils::mock_pseudo_columns_schema(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (is_virtual_table(table_schema.get_table_id())
      || table_schema.is_index_table()) {
    LOG_TRACE("do not mock rowid column", K(table_schema.get_table_name_str()),
                                          K(lib::is_mysql_mode()),
                                          K(table_schema.is_index_table()));
  } else {
    ObString part_id_column_name(OB_PART_ID_PSEUDO_COLUMN_NAME);
    ObString subpart_id_column_name(OB_SUBPART_ID_PSEUDO_COLUMN_NAME);
    ObString part_name_column_name(OB_PART_NAME_PSEUDO_COLUMN_NAME);
    ObString subpart_name_column_name(OB_SUBPART_NAME_PSEUDO_COLUMN_NAME);
    ObString part_index_column_name(OB_PART_INDEX_PSEUDO_COLUMN_NAME);
    ObString subpart_index_column_name(OB_SUBPART_INDEX_PSEUDO_COLUMN_NAME);

    if (OB_FAIL(mock_partid_column(table_schema, part_id_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(part_id_column_name));
    } else if ( OB_FAIL(mock_partid_column(table_schema, subpart_id_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(subpart_id_column_name));
    } else if ( OB_FAIL(mock_partid_column(table_schema, part_name_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(part_name_column_name));
    } else if ( OB_FAIL(mock_partid_column(table_schema, subpart_name_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(subpart_name_column_name));
    } else if ( OB_FAIL(mock_partid_column(table_schema, part_index_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(part_index_column_name));
    } else if ( OB_FAIL(mock_partid_column(table_schema, subpart_index_column_name))) {
      LOG_WARN("mock partid column failed", K(ret), K(subpart_index_column_name));
    }
  }
  return ret;
}

int ObSQLMockSchemaUtils::mock_partid_column(share::schema::ObTableSchema &table_schema,
                                            ObString column_name)
{
  int ret = OB_SUCCESS;
  bool is_rowid_exists = false;
  if (NULL != table_schema.get_column_schema(column_name)) {
    is_rowid_exists = true;
  }
  if (is_rowid_exists) {
    LOG_TRACE("do not mock rowid column", K(is_rowid_exists));
  } else {
    ObColumnSchemaV2 partid_schema;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const int64_t schema_version = table_schema.get_schema_version();
    ObObj null_obj;
    null_obj.set_null();
    partid_schema.set_tenant_id(tenant_id);
    partid_schema.set_table_id(table_id);
    partid_schema.set_column_id(table_schema.get_column_count() + OB_APP_MIN_COLUMN_ID);
    partid_schema.set_rowkey_position(0);
    partid_schema.set_index_position(0);
    partid_schema.set_tbl_part_key_pos(0);
    if (column_name.case_compare(OB_PART_NAME_PSEUDO_COLUMN_NAME) == 0 ||
        column_name.case_compare(OB_SUBPART_NAME_PSEUDO_COLUMN_NAME) == 0) {
      partid_schema.set_data_type(ObVarcharType);
      partid_schema.set_data_length(OB_MAX_COLUMN_NAME_LENGTH);
    } else {
      if (lib::is_oracle_mode()) {
        partid_schema.set_data_type(ObNumberType);
        partid_schema.set_data_scale(0);
        partid_schema.set_data_precision(20);
      } else {
        partid_schema.set_data_type(ObIntType);
        partid_schema.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
        partid_schema.set_data_precision(20);
      }
    }
    partid_schema.set_zero_fill(false);
    partid_schema.set_nullable(true);
    partid_schema.set_autoincrement(false);
    partid_schema.set_is_hidden(false);
    partid_schema.set_on_update_current_timestamp(false);
    partid_schema.set_order_in_rowkey(ObOrderType::ASC); // what this column means?
    partid_schema.set_collation_type(CS_TYPE_BINARY);
    partid_schema.set_charset_type(ObCharset::charset_type_by_coll(CS_TYPE_BINARY));
    partid_schema.set_schema_version(schema_version);
    partid_schema.set_column_name(column_name);
    partid_schema.set_comment("");
    partid_schema.set_column_flags(0);
    partid_schema.set_orig_default_value(null_obj);
    partid_schema.set_cur_default_value(null_obj, false);
    partid_schema.set_orig_default_value_v2(null_obj);
    partid_schema.set_cur_default_value_v2(null_obj);
    partid_schema.set_column_flags(MOCK_COLUMN_FLAG); // mark it is mocked column

    if (OB_FAIL(table_schema.add_column(partid_schema))) {
      LOG_ERROR("failed to add rowid column", K(ret));
    } else {
      LOG_TRACE("mocked rowid column",
                 K(table_schema.get_column_schema(column_name)),
                 K(table_schema));
    }
  }
  return ret;
}

int ObSQLMockSchemaUtils::try_mock_partid(const share::schema::ObTableSchema *org_table,
                                          const share::schema::ObTableSchema *&final_table)
{
  int ret = OB_SUCCESS;
  final_table = org_table;
  if (OB_ISNULL(org_table)) {
    // do nothing
  } else if (!sql::ObSQLMockSchemaUtils::is_mock_table(org_table->get_table_id())) {
    // do nothing
  } else {
    // mock schema请求一定在工作线程上执行，后台线程不会有is_mock_table一定是false
    // 所以可以拿到上下文allocator
    ObIAllocator &allocator = THIS_WORKER.get_sql_arena_allocator();
    ObTableSchema *tmp_table = NULL;
    if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, *org_table, tmp_table))) {
      LOG_WARN("failed to alloc schema", K(ret));
    } else if (OB_FAIL(sql::ObSQLMockSchemaUtils::mock_pseudo_columns_schema(*tmp_table))) {
      LOG_WARN("failed to mock rowid column", K(ret));
    } else {
      final_table = tmp_table;
      LOG_TRACE("mocked rowid column", K(*final_table));
    }
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase