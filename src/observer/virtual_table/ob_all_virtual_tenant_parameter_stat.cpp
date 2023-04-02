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
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObAllVirtualTenantParameterStat::ObAllVirtualTenantParameterStat() :
    inited_(false),
    show_seed_(false),
    sys_iter_(),
    tenant_iter_(),
    cur_tenant_idx_(-1),
    tenant_id_list_(),
    tenant_config_(),
    seed_config_()
{
}

ObAllVirtualTenantParameterStat::~ObAllVirtualTenantParameterStat()
{
  reset();
}

int ObAllVirtualTenantParameterStat::init(const bool show_seed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", KR(ret), K(GCTX.omt_));
  } else if (FALSE_IT(show_seed_ = show_seed)) {
  } else if (show_seed_) {
    ret = update_seed();
  } else {
    if (is_sys_tenant(effective_tenant_id_)) {
      // sys tenant show all local tenant parameter info
      if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_list_))) {
        SERVER_LOG(WARN, "get_mtl_tenant_ids fail", KR(ret), K(effective_tenant_id_));
      } else {
        SERVER_LOG(INFO, "sys tenant show all local tenant parameter", K(effective_tenant_id_),
            K(tenant_id_list_));
      }
    } else if (GCTX.omt_->has_tenant(effective_tenant_id_)) {
      if (OB_FAIL(tenant_id_list_.push_back(effective_tenant_id_))) {
        SERVER_LOG(WARN, "push back tenant id list fail", KR(ret), K(effective_tenant_id_),
            K(tenant_id_list_));
      } else {
        SERVER_LOG(INFO, "user tenant only show self tenant parameter", K(effective_tenant_id_),
            K(tenant_id_list_));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      sys_iter_ = GCONF.get_container().begin();

      // -1 means: no tenant has been handled
      cur_tenant_idx_ = -1;
      tenant_config_.set_config(NULL);
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}


int ObAllVirtualTenantParameterStat::inner_open()
{
  return OB_SUCCESS;
}

void ObAllVirtualTenantParameterStat::reset()
{
  ObVirtualTableIterator::reset();
  inited_ = false;
  show_seed_ = false;
  tenant_id_list_.reset();
  cur_tenant_idx_ = -1;
  sys_iter_ = GCONF.get_container().begin();
  tenant_config_.set_config(NULL);
}

int ObAllVirtualTenantParameterStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(inited_), KR(ret));
  } else if (show_seed_) {
    ret = inner_seed_get_next_row(row);
  } else {
    if (OB_SUCC(inner_sys_get_next_row(row))) {
    } else if (OB_ITER_END == ret) {
      ret = inner_tenant_get_next_row(row);
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::inner_sys_get_next_row(common::ObNewRow *&row)
{
  /*cluster parameter does not belong to any tenant*/
  return fill_row_(row, sys_iter_, GCONF.get_container(), NULL);
}

int ObAllVirtualTenantParameterStat::inner_tenant_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(inited_), KR(ret));
  } else if (OB_UNLIKELY(nullptr == GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", K(ret));
  } else if (cur_tenant_idx_ >= tenant_id_list_.count()) {
    ret = OB_ITER_END;
  } else {
    if (cur_tenant_idx_ < 0 // first come-in
        // current tenant is over
        || (tenant_config_.is_valid() && tenant_iter_ == tenant_config_->get_container().end())) {
      // find next valid tenant
      while (OB_SUCC(ret) && ++cur_tenant_idx_ < tenant_id_list_.count()) {
        uint64_t tenant_id = tenant_id_list_.at(cur_tenant_idx_);

        // init tenant config
        tenant_config_.set_config(TENANT_CONF(tenant_id));
        if (tenant_config_.is_valid()) {
          tenant_iter_ = tenant_config_->get_container().begin();
          break;
        } else {
          // tenant config has not been loaded, just skip it
          SERVER_LOG(WARN, "tenant config is not ready", K(tenant_id), K(cur_tenant_idx_));
        }
      }

      if (cur_tenant_idx_ >= tenant_id_list_.count()) {
        ret = OB_ITER_END;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cur_tenant_idx_ < 0
        || !tenant_config_.is_valid()
        || cur_tenant_idx_ >= tenant_id_list_.count()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur_tenant_idx_ and tenant_config_ is not valid, unexpected", KR(ret),
          K(cur_tenant_idx_), K(tenant_id_list_));
    } else {
      const uint64_t tenant_id = tenant_id_list_.at(cur_tenant_idx_);
      if (OB_FAIL(fill_row_(row,
          tenant_iter_,
          tenant_config_->get_container(),
          &tenant_id))) {
        SERVER_LOG(WARN, "fill row fail", KR(ret), K(tenant_id), K(tenant_config_->get_tenant_id()),
            K(cur_tenant_idx_), K(tenant_id_list_));
      }
    }
  }
  return ret;
}

int ObAllVirtualTenantParameterStat::update_seed()
{
  int ret = OB_SUCCESS;
  const static char *from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                     "data_type, value, info, section, scope, source, edit_level "
                     "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(GCTX.sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
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

int ObAllVirtualTenantParameterStat::inner_seed_get_next_row(common::ObNewRow *&row)
{
  return fill_row_(row, tenant_iter_, seed_config_.get_container(), NULL);
}

int ObAllVirtualTenantParameterStat::fill_row_(common::ObNewRow *&row,
    CfgIter &iter,
    const ObConfigContainer &cfg_container,
    const uint64_t *tenant_id_ptr /*NULL means not to output tenant_id*/)
{
  int ret = OB_SUCCESS;
  if (iter == cfg_container.end()) {
    ret = OB_ITER_END;
  } else {
    ObObj *cells = cur_row_.cells_;
    ObString ipstr;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", KR(ret));
    } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
      SERVER_LOG(ERROR, "get server ip failed", KR(ret));
    } else {
      if (iter == cfg_container.end()) {
        ret = OB_ITER_END;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_TYPE: {
            cells[i].set_varchar("observer");
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_IP: {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SERVER_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case NAME: {
            cells[i].set_varchar(iter->first.str());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DATA_TYPE: {
            cells[i].set_null();
            break;
          }
          case VALUE: {
            if (0 == ObString("compatible").case_compare(iter->first.str())
                && !iter->second->value_updated()) {
              // `compatible` is used for tenant compatibility,
              // default value should not be used when `compatible` is not loaded yet.
              cells[i].set_varchar("0.0.0.0");
            } else {
              cells[i].set_varchar(iter->second->str());
            }
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case INFO: {
            cells[i].set_varchar(iter->second->info());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SECTION: {
            cells[i].set_varchar(iter->second->section());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SCOPE: {
            cells[i].set_varchar(iter->second->scope());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SOURCE: {
            cells[i].set_varchar(iter->second->source());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case EDIT_LEVEL: {
            cells[i].set_varchar(iter->second->edit_level());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TENANT_ID: {
            if (NULL == tenant_id_ptr) {
              cells[i].set_null();
            } else {
              cells[i].set_int(*tenant_id_ptr);
            }
            break;
          }
          default : {
            // skip unknown column for version compatibility
            cells[i].set_null();
            SERVER_LOG(WARN, "unknown column id", K(col_id), K(i), K(ret),
                K(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME),
                K(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TNAME));
            break;
          }
        }
      } // end for
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++iter;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

