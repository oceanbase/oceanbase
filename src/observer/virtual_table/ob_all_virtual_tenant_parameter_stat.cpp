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

#include "ob_all_virtual_tenant_parameter_stat.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase {
using namespace common;

namespace observer {

ObAllVirtualTenantParameterStat::ObAllVirtualTenantParameterStat()
    : ObAllVirtualTenantParameterStat(OB_INVALID_TENANT_ID)
{}

ObAllVirtualTenantParameterStat::ObAllVirtualTenantParameterStat(uint64_t tenant_id)
    : ObVirtualTableIterator(),
      exec_tenant_id_(tenant_id),
      sys_iter_(),
      tenant_config_(),
      tenant_iter_(),
      show_seed_(false),
      seed_config_(OB_INVALID_TENANT_ID)
{}

ObAllVirtualTenantParameterStat::~ObAllVirtualTenantParameterStat()
{
  reset();
}

void ObAllVirtualTenantParameterStat::set_exec_tenant(uint64_t tenant_id)
{
  exec_tenant_id_ = tenant_id;
}

void ObAllVirtualTenantParameterStat::set_show_seed(bool show_seed)
{
  show_seed_ = show_seed;
}

int ObAllVirtualTenantParameterStat::inner_open()
{
  int ret = OB_SUCCESS;
  sys_iter_ = GCONF.get_container().begin();
  if (show_seed_) {
    ret = update_seed();
  } else if (OB_UNLIKELY(nullptr == GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", K(ret));
  } else if (GCTX.omt_->has_tenant(exec_tenant_id_)) {
    tenant_config_.set_config(TENANT_CONF(exec_tenant_id_));
    if (tenant_config_.is_valid()) {
      tenant_iter_ = tenant_config_->get_container().begin();
    } else {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "failed to get tenant config", K(exec_tenant_id_), K(ret));
    }
  }
  return ret;
}

void ObAllVirtualTenantParameterStat::reset()
{
  sys_iter_ = GCONF.get_container().begin();
  if (OB_UNLIKELY(nullptr == GCTX.omt_)) {
    SERVER_LOG(WARN, "unexpected null of omt");
  } else if (GCTX.omt_->has_tenant(exec_tenant_id_)) {
    tenant_config_.set_config(TENANT_CONF(exec_tenant_id_));
    if (tenant_config_.is_valid()) {
      tenant_iter_ = tenant_config_->get_container().begin();
    } else {
      SERVER_LOG(ERROR, "failed to get tenant config", K(exec_tenant_id_));
    }
  }
  show_seed_ = false;
}

int ObAllVirtualTenantParameterStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (show_seed_) {
    ret = inner_seed_get_next_row(row);
  } else {
    if (OB_SUCC(inner_sys_get_next_row(row))) {
    } else if (OB_ITER_END == ret) {
      ret = inner_tenant_get_next_row(row);
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::inner_sys_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const ObConfigContainer& container = GCONF.get_container();
  if (sys_iter_ == container.end()) {
    ret = OB_ITER_END;
  } else {
    ObObj* cells = cur_row_.cells_;
    ObString ipstr;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
      SERVER_LOG(ERROR, "get server ip failed", K(ret));
    } else {
      while (sys_iter_ != container.end() && (sys_iter_->second->invisible())) {
        ++sys_iter_;
      }  // while
      if (sys_iter_ == container.end()) {
        ret = OB_ITER_END;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_TYPE: {
            cells[i].set_varchar("observer");
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_IP: {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case NAME: {
            cells[i].set_varchar(sys_iter_->first.str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DATA_TYPE: {
            cells[i].set_null();
            break;
          }
          case VALUE: {
            cells[i].set_varchar(sys_iter_->second->str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case INFO: {
            cells[i].set_varchar(sys_iter_->second->info());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SECTION: {
            cells[i].set_varchar(sys_iter_->second->section());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SCOPE: {
            cells[i].set_varchar(sys_iter_->second->scope());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SOURCE: {
            cells[i].set_varchar(sys_iter_->second->source());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case EDIT_LEVEL: {
            cells[i].set_varchar(sys_iter_->second->edit_level());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }  // end for
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++sys_iter_;
      }
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::inner_tenant_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", K(ret));
  } else if (!GCTX.omt_->has_tenant(exec_tenant_id_) || tenant_iter_ == tenant_config_->get_container().end()) {
    ret = OB_ITER_END;
  } else {
    ObObj* cells = cur_row_.cells_;
    ObString ipstr;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
      SERVER_LOG(ERROR, "get server ip failed", K(ret));
    } else {
      while (tenant_iter_ != tenant_config_->get_container().end() && (tenant_iter_->second->invisible())) {
        ++tenant_iter_;
      }  // while
      if (tenant_iter_ == tenant_config_->get_container().end()) {
        ret = OB_ITER_END;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_TYPE: {
            cells[i].set_varchar("observer");
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_IP: {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case NAME: {
            cells[i].set_varchar(tenant_iter_->first.str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DATA_TYPE: {
            cells[i].set_null();
            break;
          }
          case VALUE: {
            cells[i].set_varchar(tenant_iter_->second->str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case INFO: {
            cells[i].set_varchar(tenant_iter_->second->info());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SECTION: {
            cells[i].set_varchar(tenant_iter_->second->section());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SCOPE: {
            cells[i].set_varchar(tenant_iter_->second->scope());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SOURCE: {
            cells[i].set_varchar(tenant_iter_->second->source());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case EDIT_LEVEL: {
            cells[i].set_varchar(tenant_iter_->second->edit_level());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }  // end for
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++tenant_iter_;
      }
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::update_seed()
{
  int ret = OB_SUCCESS;
  const static char* from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                                 "data_type, value, info, section, scope, source, edit_level "
                                 "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(GCTX.sql_proxy_, false);
  SMART_VAR(ObMySQLProxy::MySQLResult, result)
  {
    if (OB_FAIL(seed_config_.init(&(OTC_MGR)))) {
      SERVER_LOG(WARN, "seed config init failed", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, from_seed))) {
      SERVER_LOG(WARN, "read config from __all_seed_parameter failed", K(from_seed), K(ret));
    } else if (OB_FAIL(seed_config_.update_local(ObSystemConfig::INIT_VERSION, result, false))) {
      SERVER_LOG(WARN, "update seed config failed", K(ret));
    } else {
      tenant_iter_ = seed_config_.get_container().begin();
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::inner_seed_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (tenant_iter_ == seed_config_.get_container().end()) {
    ret = OB_ITER_END;
  } else {
    ObObj* cells = cur_row_.cells_;
    ObString ipstr;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
      SERVER_LOG(ERROR, "get server ip failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_TYPE: {
            cells[i].set_varchar("observer");
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_IP: {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case NAME: {
            cells[i].set_varchar(tenant_iter_->first.str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DATA_TYPE: {
            cells[i].set_null();
            break;
          }
          case VALUE: {
            cells[i].set_varchar(tenant_iter_->second->str());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case INFO: {
            cells[i].set_varchar(tenant_iter_->second->info());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SECTION: {
            cells[i].set_varchar(tenant_iter_->second->section());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SCOPE: {
            cells[i].set_varchar(tenant_iter_->second->scope());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SOURCE: {
            cells[i].set_varchar(tenant_iter_->second->source());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case EDIT_LEVEL: {
            cells[i].set_varchar(tenant_iter_->second->edit_level());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }  // end for
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++tenant_iter_;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
