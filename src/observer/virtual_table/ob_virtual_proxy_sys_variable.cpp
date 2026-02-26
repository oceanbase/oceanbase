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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_virtual_proxy_sys_variable.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{
ObVirtualProxySysVariable::ObVirtualProxySysVariable()
  : is_inited_(false),
    table_schema_(NULL),
    tenant_info_(NULL),
    config_(NULL),
    sys_variable_schema_(NULL)
{
}

ObVirtualProxySysVariable::~ObVirtualProxySysVariable()
{
}

void ObVirtualProxySysVariable::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  table_schema_ = NULL;
  tenant_info_ = NULL;
  config_ = NULL;
  sys_variable_schema_ = NULL;
}

int ObVirtualProxySysVariable::init(ObMultiVersionSchemaService &schema_service, ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID,
      OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID, table_schema_))) {
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema_ is NULL", KP_(table_schema), KR(ret));
  } else if (OB_FAIL(schema_guard_->get_tenant_info(OB_SYS_TENANT_ID, tenant_info_))) {
    LOG_WARN("tenant_info_ is NULL", KP_(tenant_info), KR(ret));
  } else if (OB_ISNULL(tenant_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_info_ is null");
  } else if (OB_FAIL(schema_guard_->get_sys_variable_schema(OB_SYS_TENANT_ID, sys_variable_schema_))) {
    LOG_WARN("get sys variable schema failed", KR(ret));
  } else if (OB_ISNULL(sys_variable_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    config_ = config;

    is_inited_ = true;
  }
  return ret;
}

int ObVirtualProxySysVariable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is null" , KR(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited" , KR(ret));
  } else if (!start_to_read_) {
    if (OB_FAIL(get_all_sys_variable())) {
      LOG_WARN("fill scanner fail", KR(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }

  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }

  return ret;
}

int ObVirtualProxySysVariable::get_all_sys_variable()
{
  int ret = OB_SUCCESS;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(tenant_info_)
             || OB_ISNULL(sys_variable_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info or sys_variable schema is null");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema_->get_sysvar_count(); ++i) {
      if (sys_variable_schema_->get_sysvar_schema(i) != NULL) {
        const share::schema::ObSysVarSchema *sysvar_schema = sys_variable_schema_->get_sysvar_schema(i);
        uint64_t cell_idx = 0;
        const int64_t col_count = output_column_ids_.count();
        for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
          uint64_t col_id = output_column_ids_.at(j);
          switch (col_id) {
            case NAME: {
              cur_row_.cells_[cell_idx].set_varchar(sysvar_schema->get_name());
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
              break;
            }
            case TENANT_ID: {
              cur_row_.cells_[cell_idx].set_int(sysvar_schema->get_tenant_id());
              break;
            }
            case DATA_TYPE: {
              cur_row_.cells_[cell_idx].set_int(sysvar_schema->get_data_type());
              break;
            }
            case VALUE: {
              cur_row_.cells_[cell_idx].set_varchar(sysvar_schema->get_value());
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
              break;
            }
            case FLAGS: {
              cur_row_.cells_[cell_idx].set_int(sysvar_schema->get_flags());
              break;
            }
            case MODIFIED_TIME: {
              cur_row_.cells_[cell_idx].set_int(
                  sysvar_schema->get_schema_version());
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid column id", KR(ret), K(cell_idx), K(i),
                         K(output_column_ids_), K(col_id));
              break;
            }
          }
          if (OB_SUCC(ret)) {
            cell_idx++;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(scanner_.add_row(cur_row_))) {
            LOG_WARN("fail to add row", KR(ret), K(cur_row_));
          }
        }
      }
    }
  }
  return ret;
}

}//end namespace observer
}//end namespace oceanbase
