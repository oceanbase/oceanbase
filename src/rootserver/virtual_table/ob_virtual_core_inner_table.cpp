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

#include "ob_virtual_core_inner_table.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_core_table_proxy.h"
#include "observer/omt/ob_tenant_timezone_mgr.h" //OTTZ_MGR
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver
{
ObVritualCoreInnerTable::ObVritualCoreInnerTable()
  : inited_(false), sql_proxy_(NULL),
    table_name_(NULL), table_id_(OB_INVALID_ID),
    schema_guard_(NULL)
{
}

ObVritualCoreInnerTable::~ObVritualCoreInnerTable()
{
}

int ObVritualCoreInnerTable::init(
    ObMySQLProxy &sql_proxy, const char *table_name,
    const uint64_t table_id, ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!sql_proxy.is_inited() || NULL == table_name
      || OB_INVALID_ID == table_id || NULL == schema_guard) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "sql_proxy_inited", sql_proxy.is_inited(),
        KP(table_name), KT(table_id), KP(schema_guard), K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    table_name_ = table_name;
    table_id_ = table_id;
    schema_guard_ = schema_guard;
    inited_ = true;
  }
  return ret;
}

int ObVritualCoreInnerTable::inner_open()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret), K_(effective_tenant_id));
  } else if (OB_FAIL(schema_guard_->get_table_schema(
             effective_tenant_id_, table_id_, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(table_id));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("get_table_schema failed", KT_(table_id), KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      const ObSimpleTenantSchema *tenant = NULL;
      if (!is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id) {
        // user tenant can see its own data
      } else if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id, tenant))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant) || !tenant->is_normal()) {
        // skip
      } else {
        ObCoreTableProxy core_table(table_name_, *sql_proxy_, tenant_id);
        if (OB_FAIL(core_table.load())) {
          LOG_WARN("core_table load failed", KR(ret), K(tenant_id));
        } else {
          ObArray<Column> columns;
          while (OB_SUCC(ret) && OB_SUCC(core_table.next())) {
            columns.reuse();
            if (OB_FAIL(get_full_row(tenant_id, table_schema, core_table, columns))) {
              LOG_WARN("get_full_row failed", K(tenant_id), K(table_schema), KR(ret));
            } else if (OB_FAIL(project_row(columns, cur_row_))) {
              LOG_WARN("project_row failed", K(tenant_id), K(columns), KR(ret));
            } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
              LOG_WARN("add_row failed", K(tenant_id), K_(cur_row), KR(ret));
            }
          } // end while
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    } // end for
  }
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}
int ObVritualCoreInnerTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get_next_row failed", KR(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObVritualCoreInnerTable::get_full_row(
    const uint64_t tenant_id,
    const ObTableSchema *table,
    const ObCoreTableProxy &core_table,
    ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    // core_table doesn't need to check
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else {
    const ObColumnSchemaV2 *column_schema = NULL;
    const char *column_name = NULL;
    ObString str_column;
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      if (NULL == (column_schema = table->get_column_schema(output_column_ids_.at(i)))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("column id not exist", "column_id", output_column_ids_.at(i), K(ret));
      } else if (NULL == (column_name = column_schema->get_column_name())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is null", K(column_schema), K(ret));
      } else {
        int inner_ret = OB_SUCCESS;
        if (ObVarcharType == column_schema->get_data_type()
            || ObLongTextType == column_schema->get_data_type()) {
          str_column.reset();
          if (OB_SUCCESS == (inner_ret = core_table.get_varchar(
              column_name, str_column))) {
            if (ObVarcharType == column_schema->get_data_type()) {
              ADD_COLUMN(set_varchar, table, column_name, str_column, columns);
            } else if (ObLongTextType == column_schema->get_data_type()) {
              ADD_TEXT_COLUMN(ObLongTextType, table, column_name, str_column, columns);
            }
          } else if (OB_ERR_NULL_VALUE == inner_ret) {
            ADD_NULL_COLUMN(table, column_name, columns);
          } else {
            ret = inner_ret;
            LOG_WARN("get_varchar failed", K(column_name), K(ret));
          }
        } else if (ObIntType == column_schema->get_data_type()
            || ObTinyIntType == column_schema->get_data_type()
            || ObUInt64Type == column_schema->get_data_type()) {
          int64_t int_column = 0;
          if (OB_SUCCESS == (inner_ret = core_table.get_int(
              column_name, int_column))) {
            if (0 == column_schema->get_column_name_str().case_compare("tenant_id")) {
              int_column = tenant_id;
            }
            if (ObIntType == column_schema->get_data_type()) {
              ADD_COLUMN(set_int, table, column_name, int_column, columns);
            } else if (ObTinyIntType == column_schema->get_data_type()) {
              ADD_COLUMN(set_tinyint, table, column_name, static_cast<int8_t>(int_column), columns);
            } else {
              ADD_COLUMN(set_uint64, table, column_name, int_column, columns);
            }
          } else if (OB_ERR_NULL_VALUE == inner_ret) {
            ADD_NULL_COLUMN(table, column_name, columns);
          } else {
            ret = inner_ret;
            LOG_WARN("get_int failed", K(column_name), K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data type is not expected", "data_type",
              column_schema->get_data_type(), K(ret));
        }
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
