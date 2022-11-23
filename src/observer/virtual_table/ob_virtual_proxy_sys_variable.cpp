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
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"

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
    is_queried_(false),
    table_schema_(NULL),
    tenant_info_(NULL),
    var_state_(),
    idx_(-1),
    var_states_(),
    config_(NULL),
    sys_variable_schema_(NULL)
{
}

ObVirtualProxySysVariable::~ObVirtualProxySysVariable()
{
}

int ObVirtualProxySysVariable::init(ObMultiVersionSchemaService &schema_service, ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID,
      OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID, table_schema_))) {
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema_ is NULL", KP_(table_schema), K(ret));
  } else if (OB_FAIL(schema_guard_->get_tenant_info(OB_SYS_TENANT_ID, tenant_info_))) {
    SERVER_LOG(WARN, "tenant_info_ is NULL", KP_(tenant_info), K(ret));
  } else if (OB_ISNULL(tenant_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tenant_info_ is null");
  } else if (OB_FAIL(schema_guard_->get_sys_variable_schema(OB_SYS_TENANT_ID, sys_variable_schema_))) {
    SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    is_queried_ = false;
    var_state_.reset();
    idx_ = -1;
    var_states_.reset();
    config_ = config;

    is_inited_ = true;
  }
  return ret;
}

ObVirtualProxySysVariable::ObVarStateInfo::ObVarStateInfo()
{
  reset();
}

void ObVirtualProxySysVariable::ObVarStateInfo::reset()
{
  data_type_ = 0;
  flags_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  name_buf_[0] = '\0';
  name_len_ = 0;
  value_buf_[0] = '\0';
  value_len_ = 0;
  gmt_modified_ = OB_INVALID_TIMESTAMP;
}

bool ObVirtualProxySysVariable::ObVarStateInfo::is_valid() const
{
  return data_type_ > 0
    && flags_ > 0
    && tenant_id_ > 0
    && name_len_ > 0
    && static_cast<int64_t>(strlen(name_buf_)) == name_len_
    && gmt_modified_ != OB_INVALID_TIMESTAMP;
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
    start_to_read_ = true;
  }

  if (OB_SUCC(ret)) {
    ObArray<Column> columns;
    if (OB_FAIL(get_next_sys_variable())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get sys variable info", KR(ret));
      }
    } else if (OB_FAIL(get_full_row(table_schema_, var_state_, columns))) {
      LOG_WARN("failed to get full row", "table_schema", *table_schema_, K(var_state_), KR(ret));
    } else if (OB_FAIL(project_row(columns, cur_row_))) {
      LOG_WARN("failed to project row", KR(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObVirtualProxySysVariable::get_full_row(const share::schema::ObTableSchema *table,
                                           const ObVarStateInfo &var_state,
                                           ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited" , KR(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", KR(ret));
  } else if (!var_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid var_state", K(var_state), KR(ret));
  } else {
    ADD_COLUMN(set_int, table, "data_type", var_state.data_type_, columns);
    ADD_COLUMN(set_int, table, "flags", var_state.flags_, columns);
    ADD_COLUMN(set_int, table, "tenant_id", var_state.tenant_id_, columns);
    ADD_COLUMN(set_varchar, table, "name",
               ObString(var_state.name_len_, var_state.name_buf_), columns);
    ADD_COLUMN(set_varchar, table, "value",
               ObString(var_state.value_len_, var_state.value_buf_), columns);
    ADD_COLUMN(set_int, table, "modified_time", var_state.gmt_modified_, columns);
  }
  return ret;
}


int ObVirtualProxySysVariable::get_next_sys_variable()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited" , KR(ret));
  } else if (is_queried_ && (idx_ == var_states_.count())) {
    ret = OB_ITER_END;
  } else {
    if (!is_queried_) {
      if (OB_FAIL(get_all_sys_variable())) {
        LOG_WARN("failed to get all sys variable", KR(ret));
      } else {
        idx_ = 0;
        is_queried_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (idx_ < 0 || idx_ >= var_states_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invaild idx_", K_(idx), KR(ret));
      } else {
        var_state_ = var_states_[idx_];
        ++idx_;
      }
    }
  }
  return ret;
}

int ObVirtualProxySysVariable::get_all_sys_variable()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited" , KR(ret));
  } else if (OB_ISNULL(tenant_info_)
             || OB_ISNULL(sys_variable_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info or sys_variable schema is null");
  } else {
    ObVarStateInfo *var_state = OB_NEW(ObVarStateInfo, ObModIds::OB_PROXY_DEFAULT_SYS_VARIABLE);
    if (OB_ISNULL(var_state)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("malloc ObVarStateInfo failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema_->get_sysvar_count(); ++i) {
      if (sys_variable_schema_->get_sysvar_schema(i) != NULL) {
        var_state->reset();
        const ObString &name = sys_variable_schema_->get_sysvar_schema(i)->get_name();
        const ObString &value = sys_variable_schema_->get_sysvar_schema(i)->get_value();
        var_state->data_type_ = sys_variable_schema_->get_sysvar_schema(i)->get_data_type();
        var_state->flags_ = sys_variable_schema_->get_sysvar_schema(i)->get_flags();
        var_state->tenant_id_ = sys_variable_schema_->get_sysvar_schema(i)->get_tenant_id();
        var_state->gmt_modified_ = sys_variable_schema_->get_sysvar_schema(i)->get_schema_version();
        ObString name_buffer;
        ObString value_buffer;
        name_buffer.assign_buffer(var_state->name_buf_, sizeof(var_state->name_buf_));
        value_buffer.assign_buffer(var_state->value_buf_, sizeof(var_state->value_buf_));
        if (OB_UNLIKELY((var_state->name_len_ = name_buffer.write(name.ptr(), name.length())) < name.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("write sysvar name failed", K(name));
        } else if (OB_UNLIKELY(var_state->name_len_ >= sizeof(var_state->name_buf_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("name length is invalid", K_(var_state->name_len));
        } else if (FALSE_IT(var_state->name_buf_[var_state->name_len_] = '\0')) {
          //do nothing
        } else if (OB_UNLIKELY((var_state->value_len_ = value_buffer.write(value.ptr(), value.length())) < value.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("write sysvar value failed", K(value));
        } else if (OB_FAIL(var_states_.push_back(*var_state))) {
          LOG_WARN("failed to push back", K(var_state), KR(ret));
        }
      }
    }
    if (var_state != NULL) {
      ob_delete(var_state);
      var_state = NULL;
    }
  }
  return ret;
}

}//end namespace observer
}//end namespace oceanbase
