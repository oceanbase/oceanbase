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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "lib/container/ob_array_serialization.h"
#include "ob_error_info.h"
#include "ob_schema_getter_guard.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ObErrorInfo::ObErrorInfo()
{
  reset();
}

ObErrorInfo::ObErrorInfo(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObErrorInfo::ObErrorInfo(const ObErrorInfo &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObErrorInfo::~ObErrorInfo()
{
}

ObErrorInfo &ObErrorInfo::operator =(const ObErrorInfo &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    database_id_ = src_schema.database_id_;
    obj_id_ = src_schema.obj_id_;
    obj_seq_ = src_schema.obj_seq_;
    line_ = src_schema.line_;
    position_ = src_schema.position_;
    text_length_ = src_schema.text_length_;
    property_ = src_schema.property_;
    error_number_ = src_schema.error_number_;
    schema_version_ = src_schema.schema_version_;
    error_status_ = src_schema.error_status_;
    if (OB_FAIL(deep_copy_str(src_schema.text_, text_))) {
      LOG_WARN("deep copy error text failed", K(ret), K_(src_schema.text));
    }
    error_ret_ = ret;
  }
  return *this;
}

int ObErrorInfo::assign(const ObErrorInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObErrorInfo::is_user_field_valid() const
{
  bool ret = false;
  if (ObSchema::is_valid()) {
    ret = (OB_INVALID_ID != tenant_id_);
  }
  return ret;
}

bool ObErrorInfo::is_valid() const
{
  bool ret = false;
  if (ObSchema::is_valid()) {
    if (is_user_field_valid()) {
      ret = (OB_INVALID_ID != database_id_)
          && (OB_INVALID_VERSION != schema_version_);
    } else {}
  } else {}
  return ret;
}

int ObErrorInfo::collect_error_info(const IObErrorInfo *info, 
                                    const ObWarningBuffer *buf, 
                                    bool fill_info,
                                    const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error info is null", K(ret), K(lbt()));
  } else {
    if (fill_info) {
      (set_obj_id(info->get_object_id()));
      (set_database_id(info->get_database_id()));
      (set_tenant_id(info->get_tenant_id()));
      (set_schema_version(info->get_schema_version()));
      (set_obj_type(static_cast<uint64_t>(obj_type == ObObjectType::INVALID ? info->get_object_type() : obj_type)));
      if (NULL != buf) {
        (set_error_number(buf->get_err_code()));
        set_line((const_cast<ObWarningBuffer *>(buf))->get_error_line());
        set_position((const_cast<ObWarningBuffer *>(buf))->get_error_column());
        set_error_status(ERROR_STATUS_HAS_ERROR);
        ObString err_txt(buf->get_err_msg());
        if (err_txt.empty()) {
          ObString err_unknown("unknown error message. the system doesn't declare this msg.");
          (set_text_length(err_unknown.length()));
          OZ (set_text(err_unknown));
        } else {
          (set_text_length(err_txt.length()));
          OZ (set_text(err_txt));
        }
      } else {
        set_error_status(ERROR_STATUS_NO_ERROR);
      }
      set_obj_seq(0);
    } else {
      set_error_number(0);
      set_obj_seq(0);
      set_error_status(ERROR_STATUS_NO_ERROR);
    } 
  }
  return ret;
}

int ObErrorInfo::collect_error_info(const IObErrorInfo *info,
                                    const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collect error info failed", K(ret));
  } else {
    const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
    if (OB_NOT_NULL(warnings_buf)) {
      uint16_t wcnt = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
      if (OB_FAIL(collect_error_info(info, warnings_buf, wcnt > 0, obj_type))) {
        LOG_WARN("failed to fill error info", K(ret), K(*this));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObErrorInfo::add_error(common::ObISQLClient & sql_client, 
                                 bool is_replace, 
                                 bool only_history)
{
  int ret = OB_SUCCESS;
  const ObErrorInfo & error_info = *this;
  const uint64_t tenant_id = error_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(gen_error_dml(exec_tenant_id, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      if (is_replace) {
        if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_ERROR_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_ERROR_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObErrorInfo::gen_error_dml(const uint64_t exec_tenant_id,
                                     ObDMLSqlSplicer &dml)
{
  UNUSED(exec_tenant_id);
  int ret = OB_SUCCESS;

  const ObErrorInfo &error_info = *this;
  if (OB_FAIL(dml.add_pk_column("tenant_id", error_info.extract_tenant_id()))
      || OB_FAIL(dml.add_pk_column("obj_id", error_info.extract_obj_id()))
      || OB_FAIL(dml.add_pk_column("obj_type", error_info.get_obj_type()))
      || OB_FAIL(dml.add_pk_column("obj_seq", error_info.get_obj_seq()))
      || OB_FAIL(dml.add_column("line", error_info.get_line()))
      || OB_FAIL(dml.add_column("position", error_info.get_position()))
      || OB_FAIL(dml.add_column("text_length", error_info.get_text_length()))
      || OB_FAIL(dml.add_column("property", error_info.get_property()))
      || OB_FAIL(dml.add_column("error_number", error_info.get_error_number()))
      || OB_FAIL(dml.add_column("text", ObHexEscapeSqlStr(error_info.get_text())))
      || OB_FAIL(dml.add_column("schema_version", error_info.get_schema_version()))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObErrorInfo::update_error_info(const IObErrorInfo *info, const ObObjectType obj_type) {
  int ret = OB_SUCCESS;
  
  if (NULL == info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update update error info failed due to null info", K(ret));
  } else {
    (set_obj_id(info->get_object_id()));
    (set_obj_type(static_cast<uint64_t>(obj_type == ObObjectType::INVALID ? info->get_object_type() : obj_type)));
    (set_database_id(info->get_database_id()));
    (set_tenant_id(info->get_tenant_id()));
    (set_schema_version(info->get_schema_version()));
  }
  return ret;
}

uint64_t ObErrorInfo::extract_tenant_id() const
{
  const uint64_t tenant_id = get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  return ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, get_tenant_id());
}

uint64_t ObErrorInfo::extract_obj_id() const
{
  const uint64_t tenant_id = get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  return ObSchemaUtils::get_extract_schema_id(exec_tenant_id, get_obj_id());
}

int ObErrorInfo::get_error_info_from_table(ObISQLClient &sql_client, ObErrorInfo *old_err_info)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  if (false == is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error info is invalid", K(ret));
  } else if (
    sql.assign_fmt("SELECT * FROM %s WHERE obj_id = %ld AND tenant_id = %ld \
                                                        AND obj_seq = %ld AND obj_type = %ld",
             OB_ALL_TENANT_ERROR_TNAME,
             extract_obj_id(),
             extract_tenant_id(),
             get_obj_seq(),
             get_obj_type())) {
    LOG_WARN("assign select object sequence failed.", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_client.read(res, get_tenant_id(), sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (NULL != result) {
          uint64_t line = 0;
          old_err_info->set_tenant_id(get_tenant_id());
          old_err_info->set_obj_id(get_obj_id());
          old_err_info->set_obj_seq(get_obj_seq());
          old_err_info->set_obj_type(get_obj_type());
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "line", line, uint64_t);
          old_err_info->set_line(line);
        } else {
          old_err_info->set_tenant_id(OB_INVALID_ID);
          old_err_info->set_obj_id(OB_INVALID_ID);
          old_err_info->set_obj_seq(OB_INVALID_ID);
          old_err_info->set_obj_type(get_obj_type());
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObErrorInfo::del_error(common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  bool with_snap_shot = true;
  common::ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(sql_proxy, get_tenant_id(), with_snap_shot))) {
    LOG_WARN("fail start trans", K(ret));
  } else {
    if (OB_FAIL(del_error(trans))) {
      LOG_WARN("fail to delete error info");
    } else {
      }
    }
    if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        bool is_commit = (OB_SUCCESS == ret);
        if (OB_SUCCESS != (temp_ret = trans.end(is_commit))) {
          LOG_WARN("trans end failed", "is_commit", is_commit, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
    }
  return ret;
}

int ObErrorInfo::del_error(ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObErrorInfo &error_info = *this;
  const uint64_t tenant_id = error_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (ERROR_STATUS_NO_ERROR != error_info.get_error_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete error info unexpected.", K(ret), K(error_info));
  } else if (OB_FAIL(sql.assign_fmt("delete FROM %s WHERE obj_id = %ld \
                                                  AND tenant_id = %ld  \
                                                  AND obj_seq = %ld \
                                                  AND obj_type = %ld", 
             OB_ALL_TENANT_ERROR_TNAME, 
             error_info.extract_obj_id(),
             error_info.extract_tenant_id(),
             error_info.get_obj_seq(),
             error_info.get_obj_type()))) {
    LOG_WARN("delete from __all_error table failed.", K(ret));
  } else {
    if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute query failed", K(ret), K(sql));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObErrorInfo::get_error_obj_seq(common::ObISQLClient &sql_client,
                                         bool &exist)
{
  int ret = OB_SUCCESS;
  ObErrorInfo &error_info = *this;
  ObSqlString sql;
  if (false == error_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error info is invalid", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT obj_id, obj_seq FROM %s WHERE obj_id = %ld  \
                                                                  AND tenant_id= %ld \
                                                                  AND obj_seq = %ld\
                                                                  AND obj_type = %ld",
             OB_ALL_TENANT_ERROR_TNAME,
             error_info.extract_obj_id(),
             error_info.extract_tenant_id(),
             error_info.get_obj_seq(),
             error_info.get_obj_type()))) {
    // do nothing
    LOG_WARN("assign select object sequence failed.", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_client.read(res, error_info.get_tenant_id(), sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (NULL != result && OB_SUCCESS == (ret = result->next())) {
          exist = true;
        } else {
          exist = false;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObErrorInfo::handle_error_info(ObMySQLTransaction &trans, const IObErrorInfo *info,
                                   const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  ObErrorInfo &error_info = *this;
  if (OB_NOT_NULL(info) && OB_FAIL(update_error_info(info, obj_type))) {
    LOG_WARN("update error info failed.", K(ret));
  } else if (error_info.is_valid()) {
    bool exist = false;
    bool only_history = false;
    if (OB_FAIL(get_error_obj_seq(trans, exist))) {
      LOG_WARN("get error info sequence failed", K(ret), K(error_info));
    } else {
      if (ERROR_STATUS_HAS_ERROR == error_info.get_error_status()) {
        if (error_info.is_valid()) {
          if (OB_FAIL(add_error(trans, exist, only_history))) {
            LOG_WARN("insert error info failed.", K(ret));
          } else {
            // do nothing
          }
        }
      } else if (ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
        if (exist) {
          if (OB_FAIL(del_error(trans))) {
            LOG_WARN("delete error info failed", K(ret));
          } else {
            //do nothing
          }
        }
      } else {
        error_info.reset();
      }
    } 
  } else {
    // do nothing
  }
  return ret;
}

int ObErrorInfo::handle_error_info(const IObErrorInfo *info,
                                   const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(collect_error_info(info, obj_type))) {
    LOG_WARN("collect error info failed", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, get_tenant_id(), true))) {
    LOG_WARN("fail start trans", K(ret));
  } else if (OB_FAIL(handle_error_info(trans, info, obj_type))) {
    LOG_WARN("handle error info failed.", K(ret));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", K(ret), K(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObErrorInfo::delete_error(const IObErrorInfo *info,
                              const ObObjectType obj_type)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = nullptr;
  if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    // do nothing
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(collect_error_info(info, NULL, true, obj_type))) {
      LOG_WARN("collect error info failed.", K(ret), K(info), K(*this));
    } else {
      if (OB_FAIL(del_error(sql_proxy))) {
        LOG_WARN("delete error info failed", K(ret), K(*this));
      }
    }
  }
  return ret;  
}

void ObErrorInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  obj_id_ = OB_INVALID_ID;
  obj_type_ = 0;  //obobjtype::invalid
  obj_seq_ = OB_INVALID_ID;
  line_ = 0;
  position_ = 0;
  text_length_ = 0;
  property_ = ERROR_INFO_ERROR;
  error_number_ = 0;
  schema_version_ = OB_INVALID_VERSION;
  error_status_ = ERROR_STATUS_NO_ERROR;
  reset_string(text_);
}

int64_t ObErrorInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObErrorInfo));
  len += text_.length() + 1;
  return len;
}

OB_DEF_SERIALIZE(ObErrorInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              obj_id_,
              obj_type_,
              obj_seq_,
              line_,
              position_,
              text_length_,
              text_,
              property_,
              error_number_,
              database_id_,
              schema_version_,
              error_status_);
  return ret;
}

OB_DEF_DESERIALIZE(ObErrorInfo)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              obj_id_,
              obj_type_,
              obj_seq_,
              line_,
              position_,
              text_length_,
              text_,
              property_,
              error_number_,
              database_id_,
              schema_version_,
              error_status_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObErrorInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              obj_id_,
              obj_type_,
              obj_seq_,
              line_,
              position_,
              text_length_,
              text_,
              property_,
              error_number_,
              database_id_,
              schema_version_,
              error_status_);
  return len;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
