/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_kv_connection.h"
#include "observer/ob_server.h"
using namespace oceanbase::common;
using namespace oceanbase::table;

namespace oceanbase
{
namespace observer
{

ObAllVirtualKvConnection::ObAllVirtualKvConnection()
  : ObVirtualTableScannerIterator(),
    connection_mgr_(NULL),
    fill_scanner_()
{
}

ObAllVirtualKvConnection::~ObAllVirtualKvConnection()
{
  reset();
}

void ObAllVirtualKvConnection::reset()
{
  connection_mgr_ = NULL;
  fill_scanner_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualKvConnection::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connection_mgr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "connection mgr is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(fill_scanner_.init(allocator_,
                                     &scanner_,
                                     &cur_row_,
                                     output_column_ids_,
                                     effective_tenant_id_))) {
        SERVER_LOG(WARN, "init fill_scanner fail", K(ret));
      } else if (OB_FAIL(connection_mgr_->for_each_connection(fill_scanner_))) {
        SERVER_LOG(WARN, "fill connection scanner fail", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

int ObAllVirtualKvConnection::FillScanner::operator()(hash::HashMapPair<ObAddr, ObTableConnection> &entry)
{
  int ret = OB_SUCCESS;
  ObTableConnection &conn = entry.second;
  uint64_t tenant_id = conn.get_tenant_id();
  if (OB_UNLIKELY(NULL == scanner_
                  || NULL == allocator_
                  || NULL == cur_row_
                  || NULL == cur_row_->cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "some parameters is NULL",
               K(ret),
               K(scanner_),
               K(allocator_),
               K(cur_row_));
  } else if (!is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id) {
    // do nothing
  } else if (OB_UNLIKELY(cur_row_->count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "cells count is less than output column count",
                   K(ret),
                   K(cur_row_->count_),
                   K(output_column_ids_.count()));
  } else {
    uint64_t cell_idx = 0;
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case SVR_IP : {
          cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(svr_ip_));
          cur_row_->cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cur_row_->cells_[cell_idx].set_int(svr_port_);
          break;
        }
        case CLIENT_IP: {
          if (!conn.get_addr().ip_to_string(client_ip_, OB_IP_STR_BUFF)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to get ip string", K(ret), K(conn.get_addr()));
          } else {
            cur_row_->cells_[cell_idx].set_varchar(ObString::make_string(client_ip_));
            cur_row_->cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case CLIENT_PORT: {
          cur_row_->cells_[cell_idx].set_int(conn.get_addr().get_port());
          break;
        }
        case TENANT_ID: {
          cur_row_->cells_[cell_idx].set_int(conn.get_tenant_id());
          break;
        }
        case USER_ID: {
          cur_row_->cells_[cell_idx].set_int(conn.get_user_id());
          break;
        }
        case DATABASE_ID: {
          cur_row_->cells_[cell_idx].set_int(conn.get_database_id());
          break;
        }
        case FIRST_ACTIVE_TIME: {
          cur_row_->cells_[cell_idx].set_timestamp(conn.get_first_active_time());
          break;
        }
        case LAST_ACTIVE_TIME: {
          cur_row_->cells_[cell_idx].set_timestamp(conn.get_last_active_time());
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                      K(i), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
    if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_->add_row(*cur_row_)))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
    }
  }
  return ret;
}

void ObAllVirtualKvConnection::FillScanner::reset()
{
  allocator_ = NULL;
  scanner_ = NULL;
  cur_row_ = NULL;
  output_column_ids_.reset();
}

int ObAllVirtualKvConnection::FillScanner::init(ObIAllocator *allocator,
                                                common::ObScanner *scanner,
                                                common::ObNewRow *cur_row,
                                                const ObIArray<uint64_t> &column_ids,
                                                uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator
                  || NULL == scanner
                  || NULL == cur_row)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "some parameter is NULL", K(ret), K(allocator), K(scanner), K(cur_row));
  } else if (OB_FAIL(output_column_ids_.assign(column_ids))) {
    SERVER_LOG(WARN, "fail to assign output column ids", K(ret), K(column_ids));
  } else if (!ObServer::get_instance().get_self().ip_to_string(svr_ip_, common::OB_IP_STR_BUFF)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get ip string", K(ret), K(ObServer::get_instance().get_self()));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "tenant id is invalid", K(ret));
  } else {
    allocator_ = allocator;
    scanner_ = scanner;
    cur_row_ = cur_row;
    svr_port_ = ObServer::get_instance().get_self().get_port();
    effective_tenant_id_ = tenant_id;
  }
  return ret;
}


} //namespace observer
} //namespace oceanbase