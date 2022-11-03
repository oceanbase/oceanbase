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

#include "ob_show_database_status.h"

#include "lib/string/ob_string.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace observer
{

ObShowDatabaseStatus::ObShowDatabaseStatus()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObShowDatabaseStatus::~ObShowDatabaseStatus()
{
  reset();
}

void ObShowDatabaseStatus::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}


int ObShowDatabaseStatus::add_database_status(const ObAddr &server_addr,
                                              const ObDatabaseSchema &database_schema,
                                              ObObj *cells,
                                              const int64_t col_count)
{
  int ret = OB_SUCCESS;
  char ip_buf[common::OB_IP_STR_BUFF];
  int64_t cell_idx = 0;
  if (OB_ISNULL(cells) || col_count > reserved_column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells is null or col_count is error", K(col_count), K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
    int64_t col_id = output_column_ids_.at(j);
    switch(col_id) {
      case DATABASE_NAME: {
        cells[cell_idx].set_varchar(database_schema.get_database_name());//db_name
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case HOST: {
        if (false == server_addr.ip_to_string(ip_buf, common::OB_IP_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "failed to convert ip to string", K(server_addr), K(ret));
        } else {
          cells[cell_idx].set_varchar(ObString::make_string(ip_buf));//host
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case READ_ONLY: {
        if (database_schema.is_read_only()) {
          cells[cell_idx].set_int(1);
        } else {
          cells[cell_idx].set_int(0);
        }
        break;
      }
      case PORT: {
        cells[cell_idx].set_int(server_addr.get_port());//port
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
    if (OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    }
  }
  return ret;
}

int ObShowDatabaseStatus::add_all_database_status()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  if (0 > col_count || col_count > DATABASE_STATUS_COLUMN_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    ObSArray<const ObDatabaseSchema *> database_schemas;
    if (OB_ISNULL(schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "schema manager should not be null", K(ret));
    } else if (OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_,
                                                                     database_schemas))) {
      SERVER_LOG(WARN, "failed to get database schema of tenant", K_(tenant_id));
    } else {
      ObServer &server = ObServer::get_instance();
      const ObAddr server_ip = server.get_self();
      const bool is_oracle_mode = lib::is_oracle_mode();
      for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas.count(); ++i) {
        const ObDatabaseSchema* database_schema = database_schemas.at(i);
        const ObString database_name(database_schema->get_database_name());
        if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "database not exist", K(ret));
        } else if (database_schema->is_in_recyclebin() || database_schema->is_hidden()) {
          continue;
        } else if (is_oracle_mode && is_oceanbase_sys_database_id(database_schema->get_database_id())) {
          // To skip for oceanbase in Oracle mode
          continue;
        } else if (OB_FAIL(add_database_status(server_ip, *database_schema, cells, col_count))) {
          SERVER_LOG(WARN, "failed to add table constraint of database schema!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShowDatabaseStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema manager is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K_(tenant_id), K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(add_all_database_status())) {
        SERVER_LOG(WARN, "failed to add all database status!", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
