/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_sys_variable_history.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/system_variable/ob_system_variable_init.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "common/object/ob_obj_type.h"
#include "lib/string/ob_string.h"
#include "lib/utility/utility.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{

namespace
{
int convert_string_to_obj(const ObObjType expected_type,
                          const ObString &str_val,
                          ObObj &in_obj,
                          bool &conv_error)
{
  int ret = OB_SUCCESS;
  conv_error = false;
  if (ObIntType == expected_type) {
    int64_t int_val = 0;
    char buf[32];
    if (str_val.length() >= static_cast<ObString::obstr_size_t>(sizeof(buf))) {
      LOG_WARN("numeric string too long for int conversion", K(str_val), K(str_val.length()));
      conv_error = true;
    } else {
      const int64_t len = str_val.length();
      if (len > 0) {
        MEMCPY(buf, str_val.ptr(), len);
      }
      buf[len] = '\0';
      char *endptr = NULL;
      if (OB_FAIL(ob_strtoll(buf, endptr, int_val))) {
        LOG_WARN("failed to convert string to int", K(ret), K(str_val));
        conv_error = true;
      } else if (buf == endptr || OB_ISNULL(endptr) || '\0' != *endptr) {
        LOG_WARN("invalid int string format", K(str_val), K(buf));
        conv_error = true;
      } else {
        in_obj.set_int(int_val);
      }
    }
  } else if (ObUInt64Type == expected_type) {
    uint64_t uint_val = 0;
    char buf[32];
    if (str_val.length() >= static_cast<ObString::obstr_size_t>(sizeof(buf))) {
      LOG_WARN("numeric string too long for uint64 conversion", K(str_val), K(str_val.length()));
      conv_error = true;
    } else {
      const int64_t len = str_val.length();
      if (len > 0) {
        MEMCPY(buf, str_val.ptr(), len);
      }
      buf[len] = '\0';
      char *endptr = NULL;
      if (OB_FAIL(ob_strtoull(buf, endptr, uint_val))) {
        LOG_WARN("failed to convert string to uint64", K(ret), K(str_val));
        conv_error = true;
      } else if (buf == endptr || OB_ISNULL(endptr) || '\0' != *endptr) {
        LOG_WARN("invalid uint64 string format", K(str_val), K(buf));
        conv_error = true;
      } else {
        in_obj.set_uint64(uint_val);
      }
    }
  } else {
    in_obj.set_varchar(str_val);
    in_obj.set_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}
} // namespace

int ObAllVirtualSysVariableHistory::raw_sys_var_value_to_display(ObIAllocator &allocator,
                                                                const sql::ObSQLSessionInfo *session,
                                                                const ObString &var_name,
                                                                const ObString &raw_value,
                                                                ObString &out_display,
                                                                bool &result_is_null)
{
  int ret = OB_SUCCESS;
  result_is_null = false;
  out_display.reset();
  if (OB_ISNULL(session)) {
    LOG_WARN("session is null, fail to convert sys var value to display", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObSysVarClassType var_id = SYS_VAR_INVALID;
    ObObjType expected_type = ObVarcharType;
    ObBasicSysVar::ToObjFunc to_select_obj_func = nullptr;
    ObBasicSysVar *temp_sys_var = nullptr;
    bool found = false;
    for (int64_t i = 0; i < ObSysVariables::get_amount() && !found; i++) {
      const ObString name = ObSysVariables::get_name(i);
      if (!name.empty() && var_name.case_compare_equal(name)) {
        var_id = ObSysVariables::get_sys_var_id(i);
        expected_type = ObSysVariables::get_type(i);
        found = true;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown sys variable name, cannot convert value to display", K(ret), K(var_name), K(raw_value));
    } else if (OB_FAIL(ObSysVarFactory::create_sys_var(allocator, var_id, temp_sys_var))) {
      LOG_WARN("failed to create sys var", K(ret), K(var_id), K(var_name));
    } else if (OB_ISNULL(temp_sys_var)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys var is null", K(ret), K(var_id), K(var_name));
    } else if (OB_ISNULL(to_select_obj_func = temp_sys_var->to_select_obj_)) {
      if (OB_FAIL(ob_write_string(allocator, raw_value, out_display))) {
        LOG_WARN("write string failed", K(ret), K(var_name));
      }
    } else if (raw_value.empty()) {
      out_display.reset();
    } else {
      ObObj in_obj;
      bool conv_error = false;
      if (OB_FAIL(convert_string_to_obj(expected_type, raw_value, in_obj, conv_error))) {
        LOG_WARN("failed to convert string to obj", K(ret), K(var_name), K(raw_value));
      } else if (conv_error) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sys variable value for expected type",
                 K(ret), K(var_name), K(raw_value), K(expected_type));
      } else {
        temp_sys_var->set_value(in_obj);
        ObObj result_obj;
        if (OB_FAIL(to_select_obj_func(allocator, *session, *temp_sys_var, result_obj))) {
          LOG_WARN("failed to convert to select obj", K(ret), K(var_name), K(raw_value));
        } else if (result_obj.is_null()) {
          result_is_null = true;
        } else {
          ObString tmp_str;
          if (OB_FAIL(result_obj.get_string(tmp_str))) {
            LOG_WARN("failed to get string from result obj", K(ret), K(var_name), K(result_obj));
          } else if (OB_FAIL(ob_write_string(allocator, tmp_str, out_display))) {
            LOG_WARN("write string failed", K(ret), K(var_name));
          }
        }
      }
    }
  }
  return ret;
}

ObAllVirtualSysVariableHistory::ObAllVirtualSysVariableHistory()
  : ObVirtualTableIterator(),
    sql_proxy_(nullptr),
    row_alloc_("SysVarHist"),
    rows_(),
    row_idx_(0)
{
}

int ObAllVirtualSysVariableHistory::init(common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
  }
  return ret;
}

bool ObAllVirtualSysVariableHistory::is_tenant_in_range(
    uint64_t tenant_id, const ObNewRange &r)
{
  bool in_range = !(r.start_key_.is_max_row() && r.end_key_.is_min_row());
  const ObObj &start = r.start_key_.get_obj_ptr()[0];
  const ObObj &end   = r.end_key_.get_obj_ptr()[0];
  if (in_range && r.start_key_.is_valid() && !start.is_ext()
      && tenant_id < static_cast<uint64_t>(start.get_int())) {
    in_range = false;
  }
  if (in_range && r.end_key_.is_valid() && !end.is_ext()
      && tenant_id > static_cast<uint64_t>(end.get_int())) {
    in_range = false;
  }
  return in_range;
}

int ObAllVirtualSysVariableHistory::inner_open()
{
  int ret = OB_SUCCESS;
  rows_.reset();
  row_alloc_.reset();
  row_idx_ = 0;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(sql_proxy_), KP(schema_guard_));
  } else {
    ObArray<uint64_t> all_tenants;
    if (OB_FAIL(schema_guard_->get_available_tenant_ids(all_tenants))) {
      LOG_WARN("get available tenant ids failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.count(); ++i) {
      const uint64_t tid = all_tenants.at(i);
      // filter by key_ranges: tenant_id is the first rowkey column
      bool in_range = key_ranges_.empty();
      for (int64_t j = 0; !in_range && j < key_ranges_.count(); ++j) {
        if (is_tenant_in_range(tid, key_ranges_.at(j))) {
          in_range = true;
        }
      }
      if (!in_range) {
        continue;
      }
      if (!is_sys_tenant(effective_tenant_id_) && tid != effective_tenant_id_) {
        continue;
      }
      bool exist = false;
      if (OB_FAIL(schema_guard_->check_table_exist(
              tid, OB_ALL_SYS_VARIABLE_HISTORY_TID, exist))) {
        LOG_WARN("check table exist failed", K(ret), K(tid));
      } else if (!exist) {
        LOG_TRACE("__all_sys_variable_history not exist in tenant, skip", K(tid));
      } else if (OB_FAIL(fetch_rows_for_tenant(tid))) {
        LOG_WARN("fetch rows for tenant failed", K(ret), K(tid));
      }
    }
  }
  return ret;
}

int ObAllVirtualSysVariableHistory::fetch_rows_for_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client(sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObSqlString sql;
    if (OB_FAIL(sql.assign(
          "SELECT zone, name, schema_version,"
          " TIME_TO_USEC(gmt_create) AS gmt_create,"
          " TIME_TO_USEC(gmt_modified) AS gmt_modified,"
          " is_deleted, data_type, value, info, flags, min_val, max_val"
          " FROM __all_sys_variable_history"))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql), K(tenant_id));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null mysql result", K(ret));
      }
      while (OB_SUCC(ret)) {
        int next_ret = result->next();
        if (OB_ITER_END == next_ret) {
          break;
        } else if (OB_FAIL(next_ret)) {
          LOG_WARN("result next failed", K(ret));
          break;
        }
        SysVarRow row;
        row.tenant_id_ = static_cast<int64_t>(tenant_id);
        ObString zone, name, value, info, min_val, max_val;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "zone",       zone);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "name",       name);
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", row.schema_version_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "gmt_create",     row.gmt_create_,     int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "gmt_modified",   row.gmt_modified_,   int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "is_deleted",     row.is_deleted_,     int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "data_type",      row.data_type_,      int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "flags",          row.flags_,          int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "info",       info);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "min_val",    min_val);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "max_val",    max_val);
        // value is nullable: read separately
        if (OB_SUCC(ret)) {
          int tmp_ret = result->get_varchar("value", value);
          if (OB_ERR_NULL_VALUE == tmp_ret) {
            row.value_is_null_ = true;
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("get value column failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(row_alloc_, zone,    row.zone_))
              || OB_FAIL(ob_write_string(row_alloc_, name, row.name_))
              || OB_FAIL(ob_write_string(row_alloc_, info, row.info_))
              || OB_FAIL(ob_write_string(row_alloc_, min_val, row.min_val_))
              || OB_FAIL(ob_write_string(row_alloc_, max_val, row.max_val_))) {
            LOG_WARN("deep copy string field failed", K(ret));
          } else if (!row.value_is_null_
                     && OB_FAIL(ob_write_string(row_alloc_, value, row.value_))) {
            LOG_WARN("deep copy value failed", K(ret));
          } else if (OB_FAIL(rows_.push_back(row))) {
            LOG_WARN("push back row failed", K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAllVirtualSysVariableHistory::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (row_idx_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fill_cur_row(rows_.at(row_idx_++)))) {
    LOG_WARN("fill cur row failed", K(ret), K(row_idx_ - 1));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualSysVariableHistory::fill_cur_row(const SysVarRow &r)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null cells in cur_row_", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    const uint64_t cid = output_column_ids_.at(i);
    switch (cid) {
      case TENANT_ID:
        cells[i].set_int(r.tenant_id_);
        break;
      case ZONE:
        cells[i].set_varchar(r.zone_);
        cells[i].set_collation_type(ObCharset::get_system_collation());
        break;
      case NAME:
        cells[i].set_varchar(r.name_);
        cells[i].set_collation_type(ObCharset::get_system_collation());
        break;
      case SCHEMA_VERSION:
        cells[i].set_int(r.schema_version_);
        break;
      case GMT_CREATE:
        cells[i].set_timestamp(r.gmt_create_);
        break;
      case GMT_MODIFIED:
        cells[i].set_timestamp(r.gmt_modified_);
        break;
      case IS_DELETED:
        cells[i].set_int(r.is_deleted_);
        break;
      case DATA_TYPE:
        cells[i].set_int(r.data_type_);
        break;
      case VALUE: {
        if (r.value_is_null_) {
          cells[i].set_null();
        } else {
          ObString display;
          bool result_is_null = false;
          if (OB_FAIL(raw_sys_var_value_to_display(
                  *allocator_, session_, r.name_, r.value_, display, result_is_null))) {
            LOG_WARN("convert sys var value failed", K(ret), K(r.name_));
            cells[i].set_null();
          } else if (result_is_null) {
            cells[i].set_null();
          } else {
            cells[i].set_varchar(display);
            cells[i].set_collation_type(ObCharset::get_system_collation());
          }
        }
        break;
      }
      case INFO:
        cells[i].set_varchar(r.info_);
        cells[i].set_collation_type(ObCharset::get_system_collation());
        break;
      case FLAGS:
        cells[i].set_int(r.flags_);
        break;
      case MIN_VAL:
        cells[i].set_varchar(r.min_val_);
        cells[i].set_collation_type(ObCharset::get_system_collation());
        break;
      case MAX_VAL:
        cells[i].set_varchar(r.max_val_);
        cells[i].set_collation_type(ObCharset::get_system_collation());
        break;
      default:
        cells[i].set_null();
        LOG_WARN("unknown column id, set null", K(cid));
        break;
    }
  }
  return ret;
}

int ObAllVirtualSysVariableHistory::inner_close()
{
  rows_.reset();
  row_alloc_.reset();
  row_idx_ = 0;
  return OB_SUCCESS;
}

} // namespace observer
} // namespace oceanbase
