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

#include "ob_show_tenant_status.h"

#include "lib/string/ob_string.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace observer
{

ObShowTenantStatus::ObShowTenantStatus()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObShowTenantStatus::~ObShowTenantStatus()
{
  reset();
}

void ObShowTenantStatus::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}


int ObShowTenantStatus::add_tenant_status(const ObAddr &server_addr,
                                          const ObTenantSchema &tenant_schema,
                                          const ObSysVariableSchema &sys_variable_schema,
                                          ObObj *cells,
                                          const int64_t col_count)
{
  int ret = OB_SUCCESS;
  char ip_buf[common::OB_IP_STR_BUFF];
  if (OB_ISNULL(cells) || col_count > reserved_column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells is null or col_count error", K(col_count), K(ret));
  }
  int64_t cell_idx = 0;
  for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
    uint64_t col_id = output_column_ids_.at(j);
    switch(col_id) {
      case TENANT_NAME: {
        cells[cell_idx].set_varchar(tenant_schema.get_tenant_name_str());
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
        if (sys_variable_schema.is_read_only()) {
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

int ObShowTenantStatus::add_all_tenant_status()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = NULL;
  if (0 > col_count || col_count > TENANT_STATUS_COLUMN_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)){
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema manager should not be null", K(ret));
  } else {
    const ObTenantSchema *tenant_schema = NULL;
    const ObSysVariableSchema *sys_variable_schema = NULL;
    if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id_, tenant_schema))) {
      SERVER_LOG(WARN, "get_tenant_info failed", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      SERVER_LOG(WARN, "tenant not exist!", K_(tenant_id));
    } else if (OB_FAIL(schema_guard_->get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
      SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(sys_variable_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "sys variable schema is null", K(ret));
    } else {
      ObServer &server = ObServer::get_instance();
      const ObAddr server_ip = server.get_self();
      if (OB_FAIL(add_tenant_status(server_ip, *tenant_schema, *sys_variable_schema,
                                    cells, output_column_ids_.count()))) {
        SERVER_LOG(WARN, "failed to add table constraint of Tenant schema!", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  return ret;
}

int ObShowTenantStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_)) {
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
      if (OB_FAIL(add_all_tenant_status())) {
        SERVER_LOG(WARN, "failed to add all tenant status!", K(ret));
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
