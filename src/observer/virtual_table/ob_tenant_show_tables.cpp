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

#include "observer/virtual_table/ob_tenant_show_tables.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObTenantShowTables::ObTenantShowTables()
    : ObVirtualTableIterator(),
      tenant_id_(OB_INVALID_ID),
      database_id_(OB_INVALID_ID),
      table_schemas_(),
      table_schema_idx_(0)
{
}

ObTenantShowTables::~ObTenantShowTables()
{
}

int ObTenantShowTables::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_guard_
                  || OB_INVALID_ID == tenant_id_
                  || NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K(ret), K(schema_guard_), K(tenant_id_), K(allocator_));
  } else {
    //get database_id
    ObRowkey start_key;
    ObRowkey end_key;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_id(database_id_) && i < key_ranges_.count(); ++i) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
      if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
        if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                   && ObIntType == start_key_obj_ptr[0].get_type()) {
          database_id_ = start_key_obj_ptr[0].get_int();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_valid_id(database_id_))) {
        // FIXME(tingshuai.yts):暂时定为显示该错误信息，只有直接查询该虚拟表才可能出现
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "select a table which is used for show clause");
      } else {
        if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_,
                                                                 database_id_,
                                                                 table_schemas_))) {
          SERVER_LOG(WARN, "fail to get table schemas in database", K(ret), K(tenant_id_), K(database_id_));
        }
      }
    }
  }
  return ret;
}

void ObTenantShowTables::reset()
{
  session_ = NULL;
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  table_schemas_.reset();
  table_schema_idx_ = 0;
  ObVirtualTableIterator::reset();
}

int ObTenantShowTables::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret), K(cur_row_));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObTenantShowTables::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  ObString database_name;
  if (OB_UNLIKELY(NULL == allocator_
                  || NULL == schema_guard_
                  || NULL == session_
                  || NULL == (cells = cur_row_.cells_)
                  || !is_valid_id(tenant_id_)
                  || !is_valid_id(database_id_))) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member dosen't init", K(ret), K(allocator_), K(schema_guard_),
               K(session_), K(cells), K(tenant_id_), K(database_id_));
  } else {
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, database_id_, db_schema))) {
      SERVER_LOG(WARN, "Failed to get database schema", K(ret), K_(tenant_id), K_(database_id));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "db_schema should not be null", K(ret), K_(tenant_id), K_(database_id));
    } else {
      database_name = db_schema->get_database_name_str();
      bool is_allow = false;
      ObSessionPrivInfo priv_info;
      do {
        is_allow = true;
        if (OB_UNLIKELY(table_schema_idx_ < 0)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid table_schema_idx_", K(ret), K(table_schema_idx_));
        } else if (table_schema_idx_ >= table_schemas_.count()) {
          ret = OB_ITER_END;
          table_schema_idx_ = 0;
        } else {
          if (OB_ISNULL(table_schema = table_schemas_.at(table_schema_idx_))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "table schema is NULL", K(ret), K(table_schema_idx_), K(tenant_id_), K(database_id_));
          } else {
            uint64_t cell_idx = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              uint64_t col_id = output_column_ids_.at(j);
              switch(col_id) {
                case DATABASE_ID: {
                  cells[cell_idx].set_int(database_id_);
                  break;
                }
                case TABLE_NAME: {
                  cells[cell_idx].set_varchar(table_schema->get_table_name_str());
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case TABLE_TYPE: {
                  if (is_mysql_database_id(table_schema->get_database_id())) {
                    cells[cell_idx].set_varchar(table_schema->is_user_view() ?
                    ObString::make_string("VIEW") : ObString::make_string("BASE TABLE"));
                  } else {
                    cells[cell_idx].set_varchar(ObString::make_string(ob_mysql_table_type_str(table_schema->get_table_type())));
                  }
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                             K(j), K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                cell_idx++;
              }
            }
            if (OB_SUCC(ret)) {
              // skip table that is neither normal table, system table nor view table
              if ((!table_schema->is_user_table()
                  && !table_schema->is_sys_table()
                  && !table_schema->is_view_table()
                  && !table_schema->is_external_table()
                  && !is_information_schema_database_id(table_schema->get_database_id())
                  && !is_mysql_database_id(table_schema->get_database_id()))
                  || table_schema->is_ctas_tmp_table()
                  || table_schema->is_user_hidden_table()) {
                is_allow = false;
              } else {
                priv_info.reset();
                session_->get_session_priv_info(priv_info);
                if (OB_FAIL(schema_guard_->check_table_show(priv_info, database_name,
                                                            table_schema->get_table_name_str(), is_allow))) {
                  SERVER_LOG(WARN, "check show table priv failed", K(ret));
                }
              }
            }
          }
          table_schema_idx_++;
        }
      } while(!is_allow && OB_SUCCESS == ret);
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
