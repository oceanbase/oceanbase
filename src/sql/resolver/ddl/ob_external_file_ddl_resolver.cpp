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

#define USING_LOG_PREFIX SQL_RESV

#include "ob_ddl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{

int ObDDLResolver::resolve_external_file_format(const ParseNode *format_node,
                                                ObResolverParams &params,
                                                ObExternalFileFormat& format,
                                                ObString &format_str)
{
  int ret = OB_SUCCESS;
  bool has_file_format = false;
  if (OB_FAIL(format.csv_format_.init_format(ObDataInFileStruct(),
                                            OB_MAX_COLUMN_NUMBER,
                                            CS_TYPE_UTF8MB4_BIN))) {
    LOG_WARN("failed to init csv format", K(ret));
  }
  // resolve file type and encoding type
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(format_node) || OB_ISNULL(params.allocator_)
        || (T_EXTERNAL_FILE_FORMAT != format_node->type_ && T_EXTERNAL_PROPERTIES != format_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format node", K(ret), K(format_node->type_), KP(params.allocator_));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(format.plugin_format_.init(*params.allocator_))) {
    LOG_WARN("failed to init plugin format/properties", K(ret));
  }

  ObResolverUtils::FileFormatContext ff_ctx;
  for (int i = 0; OB_SUCC(ret) && i < format_node->num_child_; ++i) {
    if (OB_ISNULL(format_node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL ptr", K(ret), K(format_node->num_child_));
    } else if (T_EXTERNAL_FILE_FORMAT_TYPE == format_node->children_[i]->type_
                || T_CHARSET == format_node->children_[i]->type_) {
      if (OB_FAIL(ObResolverUtils::resolve_file_format(format_node->children_[i], format, params, ff_ctx))) {
        LOG_WARN("fail to resolve file format", K(ret));
      }
      has_file_format |= (T_EXTERNAL_FILE_FORMAT_TYPE == format_node->children_[i]->type_);
    }
  }
  if (OB_SUCC(ret) && !has_file_format) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "format");
  }
  // resolve other format value
  ff_ctx.reset();
  for (int i = 0; OB_SUCC(ret) && i < format_node->num_child_; ++i) {
    if (OB_ISNULL(format_node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL ptr", K(ret), K(format_node->num_child_));
    } else if (OB_FAIL(ObResolverUtils::resolve_file_format(format_node->children_[i], format, params, ff_ctx))) {
      LOG_WARN("fail to resolve file format", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_valid = true;
    if (ObExternalFileFormat::ODPS_FORMAT == format.format_type_ && OB_FAIL(format.odps_format_.encrypt())) {
      LOG_WARN("failed to encrypt odps format", K(ret));
    } else if (OB_FAIL(ObDDLResolver::check_format_valid(format, is_valid))) {
      LOG_WARN("check format valid failed", K(ret));
    } else if (!is_valid) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("file format is not valid", K(ret));
    } else if (OB_FAIL(format.to_string_with_alloc(format_str, *params.allocator_))) {
      LOG_WARN("failed to convert format to string", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_external_file_pattern(const ParseNode *option_node,
                                                bool is_external_table,
                                                common::ObIAllocator &allocator,
                                                const ObSQLSessionInfo *session_info,
                                                ObString &pattern)
{
  int ret = OB_SUCCESS;
  if (!is_external_table) {
    ret = OB_NOT_SUPPORTED;
    ObSqlString err_msg;
    err_msg.append_fmt("Using PATTERN as a CREATE TABLE option");
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
    LOG_WARN("using PATTERN as a table option is support in external table only", K(ret));
  } else if (option_node->num_child_ != 1 || OB_ISNULL(option_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child num", K(option_node->num_child_));
  } else if (0 == option_node->children_[0]->str_len_) {
    ObSqlString err_msg;
    err_msg.append_fmt("empty regular expression");
    ret = OB_ERR_REGEXP_ERROR;
    LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, err_msg.ptr());
    LOG_WARN("empty regular expression", K(ret));
  } else {
    pattern = ObString(option_node->children_[0]->str_len_,
                      option_node->children_[0]->str_value_);
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator,
                                                            session_info->get_dtc_params(),
                                                            pattern))) {
      LOG_WARN("failed to convert pattern to utf8", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_external_file_location(ObResolverParams &params,
                                                  ObTableSchema &table_schema,
                                                  common::ObString table_location)
{
  int ret = OB_SUCCESS;
  ObString resolved_table_location;
  ObString resolved_access_info;
  if (!table_schema.is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "location option");
  } else if (OB_FAIL(resolve_external_file_location(params.allocator_,
                                                    table_location,
                                                    resolved_table_location,
                                                    resolved_access_info))) {
    LOG_WARN("failed to resolve external file location", K(ret));
  } else if (OB_FAIL(table_schema.set_external_file_location(resolved_table_location))) {
    LOG_WARN("failed to set external file location", K(ret));
  } else if (OB_FAIL(table_schema.set_external_file_location_access_info(resolved_access_info))) {
    LOG_WARN("failed to set external file location access info", K(ret));
  }

  return ret;
}

int ObDDLResolver::resolve_external_file_location(ObIAllocator *allocator,
                                                  const common::ObString &table_location,
                                                  common::ObString &resolved_table_location,
                                                  common::ObString &resolved_access_info)
{
  int ret = OB_SUCCESS;

  ObString url = table_location;
  uint64_t data_version = 0;
  const bool is_hdfs_type = url.prefix_match(OB_HDFS_PREFIX);

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (is_hdfs_type && data_version < DATA_VERSION_4_3_5_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("failed to support hdfs feature when data version is lower", K(ret), K(data_version));
  } else if (url.prefix_match(OB_COS_PREFIX) && data_version > DATA_VERSION_4_3_5_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create external table on cos");
    LOG_WARN("create external table on cos is no longer supported");
  }
  ObHDFSStorageInfo hdfs_storage_info;
  ObBackupStorageInfo backup_storage_info;
  ObObjectStorageInfo *storage_info
      = is_hdfs_type ? static_cast<ObObjectStorageInfo *>(&hdfs_storage_info)
                     : static_cast<ObObjectStorageInfo *>(&backup_storage_info);
  char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  ObString path = url.split_on('?');
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (path.empty()) {
    // url like: oss://ak:sk@host/bucket/...
    ObSqlString tmp_location;
    ObSqlString prefix;

    if (OB_FAIL(resolve_file_prefix(url, prefix, storage_info->device_type_, allocator))) {
      LOG_WARN("failed to resolve file prefix", K(ret));
    } else if (OB_FAIL(tmp_location.append(prefix.string()))) {
      LOG_WARN("failed to append prefix", K(ret));
    } else {
      url = url.trim_space_only();
    }

    if (OB_SUCC(ret)) {
      if (OB_STORAGE_FILE != storage_info->device_type_
          && OB_STORAGE_HDFS != storage_info->device_type_ /* hdfs with simple auth*/) {
        if (OB_FAIL(ObSQLUtils::split_remote_object_storage_url(url, storage_info))) {
          LOG_WARN("failed to split remote object storage url", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_location.append(url))) {
        LOG_WARN("failed to append url", K(ret));
      } else if (OB_FAIL(storage_info->get_storage_info_str(storage_info_buf,
                                                            sizeof(storage_info_buf)))) {
        LOG_WARN("failed to get storage info str", K(ret));
      } else {
        OZ(ob_write_string(*allocator, tmp_location.string(), resolved_table_location));
        OZ(ob_write_string(*allocator, storage_info_buf, resolved_access_info));
      }
    }
  } else {
    // url like: oss://bucket/...?host=xxxx&access_id=xxx&access_key=xxx
    ObString uri_cstr;
    ObString storage_info_cstr;
    ObArenaAllocator tmp_allocator;
    if (OB_FAIL(ob_write_string(tmp_allocator, path, uri_cstr, true))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(ob_write_string(tmp_allocator, url, storage_info_cstr, true))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(storage_info->set(uri_cstr.ptr(), storage_info_cstr.ptr()))) {
      LOG_WARN("failed to set storage info", K(ret));
    } else if (OB_FAIL(storage_info->get_storage_info_str(storage_info_buf,
                                                          sizeof(storage_info_buf)))) {
      LOG_WARN("failed to get storage info str", K(ret));
    } else {
      OZ(ob_write_string(*allocator, path, resolved_table_location));
      OZ(ob_write_string(*allocator, storage_info_buf, resolved_access_info));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_external_file_location_object(ObResolverParams &params,
                                                         ObTableSchema &table_schema,
                                                         common::ObString location_obj,
                                                         common::ObString sub_path)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!table_schema.is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "location option");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("failed to support location feature when data version is lower", K(ret), K(data_version));
  } else {
    const uint64_t tenant_id = params.session_info_->get_effective_tenant_id();
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObLocationSchema *schema_ptr = NULL;
    if (OB_ISNULL(params.schema_checker_)
        || NULL == (schema_guard = params.schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker or schema guard is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_location_schema_by_name(tenant_id, location_obj, schema_ptr))) {
      LOG_WARN("failed to get schema by location name", K(ret), K(tenant_id), K(location_obj));
    } else if (OB_ISNULL(schema_ptr)) {
      ret = OB_LOCATION_OBJ_NOT_EXIST;
      LOG_WARN("location object does't exist", K(ret), K(tenant_id), K(location_obj));
    } else {
      table_schema.set_external_location_id(schema_ptr->get_location_id());
      OZ (table_schema.set_external_sub_path(sub_path));
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        ObPackedObjPriv packed_priv = 0;
        OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_READ, packed_priv));
        ObOraNeedPriv need_priv;
        need_priv.grantee_id_ = params.session_info_->get_user_id();
        need_priv.obj_id_ = schema_ptr->get_location_id();
        need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
        need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::LOCATION);
        need_priv.obj_privs_ = packed_priv;
        need_priv.check_flag_ = CHECK_FLAG_NORMAL;
        need_priv.owner_id_ = OB_ORA_SYS_USER_ID;
        ObStmtOraNeedPrivs need_privs;
        need_privs.need_privs_.set_allocator(params.allocator_);
        need_privs.need_privs_.set_capacity(10);
        need_privs.need_privs_.push_back(need_priv);
        OZ(params.schema_checker_->check_ora_priv(
        params.session_info_->get_effective_tenant_id(),
        params.session_info_->get_priv_user_id(),
        need_privs,
        params.session_info_->get_enable_role_array()),
      tenant_id, params.session_info_->get_user_id());
      }
    }
  }
  return ret;
}

}
}
