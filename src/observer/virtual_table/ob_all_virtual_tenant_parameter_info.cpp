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

#include "ob_all_virtual_tenant_parameter_info.h"
#include "lib/container/ob_array_iterator.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace common;

namespace observer
{

ObAllVirtualTenantParameterInfo::ObAllVirtualTenantParameterInfo()
  : all_config_(), config_iter_()
{
}

ObAllVirtualTenantParameterInfo::~ObAllVirtualTenantParameterInfo()
{
  reset();
}

int ObAllVirtualTenantParameterInfo::inner_open()
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = GCTX.self_addr();
  if (OB_FAIL(OTC_MGR.get_all_tenant_config_info(all_config_))) {
    SERVER_LOG(WARN, "fail to get all tenant config info", K(ret));
  } else if (!addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip to string failed", K(ret));
  } else {
    config_iter_ = all_config_.begin();
  }
  return ret;
}

void ObAllVirtualTenantParameterInfo::reset()
{
  config_iter_ = all_config_.begin();
}

int ObAllVirtualTenantParameterInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (config_iter_ == all_config_.end()) {
    ret = OB_ITER_END;
  } else {
    ObObj *cells = cur_row_.cells_;
    if (OB_UNLIKELY(nullptr == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case TENANT_ID : {
          cells[i].set_int(config_iter_->tenant_id_);
          cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
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
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case SERVER_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
        case NAME: {
            cells[i].set_varchar(config_iter_->name_.ptr());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case DATA_TYPE: {
            cells[i].set_null();
            break;
          }
        case VALUE: {
            cells[i].set_varchar(config_iter_->value_.ptr());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case INFO: {
            cells[i].set_varchar(config_iter_->info_.ptr());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case SECTION: {
            cells[i].set_varchar(config_iter_->section_.ptr());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case SCOPE: {
            cells[i].set_varchar(config_iter_->scope_.ptr());
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case SOURCE: {
          cells[i].set_varchar(config_iter_->source_.ptr());
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
          }
        case EDIT_LEVEL: {
          cells[i].set_varchar(config_iter_->edit_level_.ptr());
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
          }
        default : {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        } // switch col_id
      } // for columns
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++config_iter_;
      }
    } // else
  } // else
  return ret;
}

} // namespace observer
} // namespace oceanbase

