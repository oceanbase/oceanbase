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

#define USING_LOG_PREFIX SHARE
#include "share/object_storage/ob_zone_storage_table_operation.h"
#include "observer/ob_server.h"

using namespace oceanbase;
using namespace share;
using namespace common;

//*********************ObStorageInfoOperator*****************

int ObStorageInfoOperator::insert_storage(common::ObISQLClient &proxy,
                                          const ObBackupDest &storage_dest,
                                          const ObStorageUsedType::TYPE &used_for,
                                          const ObZone &zone,
                                          const ObZoneStorageState::STATE op_type,
                                          const uint64_t storage_id, const uint64_t op_id,
                                          const int64_t max_iops, const int64_t max_bandwidth)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      OB_INVALID_ID == storage_id || OB_INVALID_ID == op_id ||
      !ObZoneStorageState::is_valid(op_type) || zone.is_empty() ||
      max_iops < 0 || max_bandwidth < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_id), K(op_id), K(op_type), K(storage_dest),
             K(used_for), K(zone), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(
               authorization, sizeof(authorization)))) {
    LOG_WARN("fail to get authorization", KR(ret), K(storage_dest));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_STORAGE_ZONE, zone.ptr())) ||
             OB_FAIL(
               dml.add_pk_column(OB_STR_STORAGE_DEST_PATH, storage_dest.get_root_path().ptr())) ||
             OB_FAIL(dml.add_pk_column(OB_STR_STORAGE_DEST_ENDPOINT,
                                       storage_dest.get_storage_info()->endpoint_)) ||
             OB_FAIL(
               dml.add_pk_column(OB_STR_STORAGE_USEDFOR, ObStorageUsedType::get_str(used_for))) ||
             OB_FAIL(dml.add_uint64_column(OB_STR_STORAGE_ID, storage_id)) ||
             OB_FAIL(dml.add_column(OB_STR_STORAGE_DEST_AUTHORIZATION, authorization)) ||
             OB_FAIL(dml.add_column(OB_STR_STORAGE_STATE, ObZoneStorageState::get_str(op_type))) ||
             OB_FAIL(dml.add_column(OB_STR_STORAGE_EXTENSION, storage_dest.get_storage_info()->extension_)) ||
             OB_FAIL(dml.add_uint64_column(OB_STR_STORAGE_OP_ID, op_id) ||
             OB_FAIL(dml.add_uint64_column(OB_STR_STORAGE_MAX_IOPS, max_iops)) ||
             OB_FAIL(dml.add_uint64_column(OB_STR_STORAGE_MAX_BANDWIDTH, max_bandwidth)))) {
    LOG_WARN("fail to fill zone storage", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_ZONE_STORAGE_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", KR(ret));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("succ insert zone storage table", K(sql), K(storage_id));
  }
  return ret;
}

int ObStorageOperationOperator::insert_storage_operation(common::ObISQLClient &proxy,
                                                         const uint64_t storage_id,
                                                         const uint64_t op_id,
                                                         const uint64_t sub_op_id,
                                                         const ObZone &zone,
                                                         const ObZoneStorageState::STATE op_type,
                                                         const char *op_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  if (OB_INVALID_ID == storage_id || OB_INVALID_ID == op_id || OB_INVALID_ID == sub_op_id ||
      !ObZoneStorageState::is_valid(op_type) || OB_ISNULL(op_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_id), K(op_id), K(sub_op_id));
  } else if (OB_FAIL(dml.add_uint64_pk_column(OB_STR_STORAGE_ID, storage_id)) ||
             OB_FAIL(dml.add_uint64_pk_column(OB_STR_STORAGE_OP_ID, op_id)) ||
             OB_FAIL(dml.add_uint64_pk_column(OB_STR_STORAGE_SUB_OP_ID, sub_op_id)) ||
             OB_FAIL(dml.add_pk_column(OB_STR_STORAGE_ZONE, zone.ptr())) ||
             OB_FAIL(
               dml.add_column(OB_STR_STORAGE_OP_TYPE, ObZoneStorageState::get_str(op_type))) ||
             OB_FAIL(dml.add_column(OB_STR_STORAGE_OP_INFO, op_info))) {
    LOG_WARN("fail to fill zone storage operation", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_ZONE_STORAGE_OPERATION_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", KR(ret));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("succ insert zone storage operation table", K(sql), K(storage_id));
  }

  return ret;
}

int ObStorageInfoOperator::remove_storage_info(common::ObISQLClient &proxy,
                                               const common::ObZone &zone,
                                               const ObString &storage_path,
                                               const ObStorageUsedType::TYPE used_for)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone), K(used_for));
  } else if (OB_FAIL(parse_storage_path(storage_path.ptr(), path, sizeof(path), endpoint,
                                        sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE %s = '%s' "
                                    "AND %s = '%s' AND %s = '%s' AND %s = '%s'",
                                    OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_ZONE, zone.ptr(),
                                    OB_STR_STORAGE_DEST_PATH, path, OB_STR_STORAGE_DEST_ENDPOINT,
                                    endpoint, OB_STR_STORAGE_USEDFOR,
                                    ObStorageUsedType::get_str(used_for)))) {
    LOG_WARN("failed to assign sql", KR(ret), K(zone), K(storage_path), K(used_for));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret));
  } else {
    LOG_INFO("succ delete zone storage info", K(sql), K(storage_path));
  }
  return ret;
}

int ObStorageInfoOperator::remove_storage_info(common::ObISQLClient &proxy,
                                               const common::ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(zone));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE %s = '%s' ", OB_ALL_ZONE_STORAGE_TNAME,
                                    OB_STR_STORAGE_ZONE, zone.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(zone));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret));
  } else {
    LOG_INFO("succ delete zone storage info", K(sql), K(zone));
  }
  return ret;
}

int ObStorageInfoOperator::get_storage_state(common::ObISQLClient &proxy,
                                             const common::ObZone &zone,
                                             const ObBackupDest &storage_dest,
                                             const ObStorageUsedType::TYPE used_for,
                                             ObZoneStorageState::STATE &state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  state = ObZoneStorageState::MAX;
  if (!ObStorageUsedType::is_valid(used_for) && !storage_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(used_for), K(storage_dest));
  } else if (OB_FAIL(sql.assign_fmt(
               "SELECT %s FROM %s WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s'",
               OB_STR_STORAGE_STATE, OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_ZONE, zone.ptr(),
               OB_STR_STORAGE_DEST_PATH, storage_dest.get_root_path().ptr(),
               OB_STR_STORAGE_DEST_ENDPOINT, storage_dest.get_storage_info()->endpoint_,
               OB_STR_STORAGE_USEDFOR, ObStorageUsedType::get_str(used_for)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
      } else if (OB_SUCC(result->next())) {
        char state_str[OB_MAX_STORAGE_STATE_LENGTH] = {0};
        int tmp_str_len = 0;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_STATE, state_str,
                                   OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
        state = ObZoneStorageState::get_state(state_str);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", KR(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", KR(ret));
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::get_storage_state(common::ObISQLClient &proxy,
                                             const common::ObZone &zone,
                                             const ObString &storage_path,
                                             const ObStorageUsedType::TYPE used_for,
                                             ObZoneStorageState::STATE &state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  state = ObZoneStorageState::MAX;
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone), K(used_for));
  } else if (OB_FAIL(parse_storage_path(storage_path.ptr(), path, sizeof(path), endpoint,
                                        sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s = '%s' AND %s = '%s' AND "
                                    "%s = '%s' AND %s = '%s'",
                                    OB_STR_STORAGE_STATE, OB_ALL_ZONE_STORAGE_TNAME,
                                    OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH, path,
                                    OB_STR_STORAGE_DEST_ENDPOINT, endpoint, OB_STR_STORAGE_USEDFOR,
                                    ObStorageUsedType::get_str(used_for)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
      } else if (OB_SUCC(result->next())) {
        char state_str[OB_MAX_STORAGE_STATE_LENGTH] = {0};
        int tmp_str_len = 0;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_STATE, state_str,
                                   OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
        state = ObZoneStorageState::get_state(state_str);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", KR(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", KR(ret));
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::get_storage_id(common::ObISQLClient &proxy, const common::ObZone &zone,
                                          const ObString &storage_path,
                                          const ObStorageUsedType::TYPE used_for,
                                          uint64_t &storage_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  storage_id = OB_INVALID_ID;
  ObString state;
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone), K(used_for));
  } else if (OB_FAIL(parse_storage_path(storage_path.ptr(), path, sizeof(path), endpoint,
                                        sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s = '%s' AND %s = '%s' AND "
                                    "%s = '%s' AND %s = '%s'",
                                    OB_STR_STORAGE_ID, OB_ALL_ZONE_STORAGE_TNAME,
                                    OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH, path,
                                    OB_STR_STORAGE_DEST_ENDPOINT, endpoint, OB_STR_STORAGE_USEDFOR,
                                    ObStorageUsedType::get_str(used_for)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
      } else if (OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_ID, storage_id, uint64_t);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", KR(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", KR(ret));
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::parse_storage_path(const char *storage_path, char *root_path,
                                              int64_t path_len, char *endpoint,
                                              int64_t endpoint_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  common::ObStorageType type;
  ObString storage_path_str(storage_path);
  if (OB_ISNULL(storage_path) || OB_ISNULL(root_path) || OB_ISNULL(endpoint)) {
    ret = OB_INVALID_STORAGE_DEST;
    LOG_WARN("invalid args", KR(ret), KP(storage_path));
  } else if (OB_FAIL(get_storage_type_from_path(storage_path_str, type))) {
    LOG_WARN("failed to get storage type", KR(ret));
  } else if (OB_STORAGE_OSS != type && OB_STORAGE_S3 != type && OB_STORAGE_COS != type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot support this storage type", KR(ret), K(type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cannot support this storage type, it is");
  } else {
    while (storage_path[pos] != '\0') {
      if (storage_path[pos] == '?') {
        break;
      }
      ++pos;
    }
    int64_t left_count = strlen(storage_path) - pos;
    if (pos >= path_len || left_count >= endpoint_len) {
      ret = OB_INVALID_STORAGE_DEST;
      LOG_ERROR("storage dest is too long, cannot work", KR(ret), K(pos), K(storage_path),
                K(left_count));
    } else {
      MEMCPY(root_path, storage_path, pos);
      root_path[pos] = '\0';
      ++pos;
      if (0 != left_count) {
        MEMCPY(endpoint, storage_path + pos, left_count);
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::select_for_update(common::ObMySQLTransaction &trans,
                                             const common::ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(zone));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_string.assign_fmt("SELECT name FROM %s WHERE %s = '%s' FOR UPDATE",
                                        OB_ALL_ZONE_TNAME, OB_STR_STORAGE_ZONE, zone.ptr()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(zone));
      } else if (OB_FAIL(trans.read(res, sql_string.ptr()))) {
        LOG_WARN("update status of ddl task record failed", KR(ret), K(sql_string));
      } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to get next row", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::zone_storage_dest_exist(common::ObISQLClient &proxy, const ObZone &zone,
                                                   const ObBackupDest &storage_dest,
                                                   const ObStorageUsedType::TYPE used_for,
                                                   bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) || zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), K(used_for), K(zone));
  } else if (OB_FAIL(sql.assign_fmt(
               "SELECT * FROM %s "
               "WHERE %s = '%s' AND %s = '%s' AND %s = '%s' AND %s = '%s'",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH,
               storage_dest.get_root_path().ptr(), OB_STR_STORAGE_DEST_ENDPOINT,
               storage_dest.get_storage_info()->endpoint_, OB_STR_STORAGE_USEDFOR,
               ObStorageUsedType::get_str(used_for)))) {
    LOG_WARN("fail to assign sql", KR(ret), K(zone));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_exist = false;
        } else {
          LOG_WARN("failed to get next", KR(ret));
        }
      } else {
        is_exist = true;
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::get_zone_storage_table_dest(
  common::ObISQLClient &proxy, ObArray<share::ObStorageDestAttr> &dest_attrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_string.assign_fmt(
          "SELECT %s,%s,%s,%s,%s FROM %s", OB_STR_STORAGE_DEST_PATH, OB_STR_STORAGE_DEST_ENDPOINT,
          OB_STR_STORAGE_DEST_AUTHORIZATION, OB_STR_STORAGE_DEST_ENCRYPT_INFO,
          OB_STR_STORAGE_DEST_EXTENSION, OB_ALL_ZONE_STORAGE_TNAME))) {
      LOG_WARN("assign sql string failed", KR(ret));
    } else if (OB_FAIL(proxy.read(res, sql_string.ptr()))) {
      LOG_WARN("query zone storage record failed", KR(ret), K(sql_string));
    } else if (OB_ISNULL((result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), KP(result));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          int tmp_str_len = 0;
          share::ObStorageDestAttr dest_attr;
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_PATH, dest_attr.path_,
                                     OB_MAX_BACKUP_DEST_LENGTH, tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_ENDPOINT, dest_attr.endpoint_,
                                     OB_MAX_BACKUP_ENDPOINT_LENGTH, tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_AUTHORIZATION,
                                     dest_attr.authorization_, OB_MAX_BACKUP_AUTHORIZATION_LENGTH,
                                     tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_EXTENSION, dest_attr.extension_,
                                     OB_MAX_BACKUP_EXTENSION_LENGTH, tmp_str_len);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to extract field", KR(ret), K(sql));
          } else if (OB_FAIL(dest_attrs.push_back(dest_attr))) {
            LOG_WARN("fail to push back element into array", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::get_zone_storage_table_info(
  common::ObISQLClient &proxy, ObArray<share::ObZoneStorageTableInfo> &storage_table_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s", OB_ALL_ZONE_STORAGE_TNAME))) {
      LOG_WARN("assign sql string failed", KR(ret));
    } else if (OB_FAIL(proxy.read(res, sql_string.ptr()))) {
      LOG_WARN("query zone storage record failed", KR(ret), K(sql_string));
    } else if (OB_ISNULL((result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), KP(result));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          ObString zone;
          char state[OB_MAX_STORAGE_STATE_LENGTH] = {0};
          char used_for[OB_MAX_STORAGE_USED_FOR_LENGTH] = {0};
          uint64_t storage_id = OB_INVALID_ID;
          int tmp_str_len = 0;
          share::ObZoneStorageTableInfo storage_table_info;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, OB_STR_STORAGE_ZONE, zone);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_PATH,
                                     storage_table_info.dest_attr_.path_,
                                     OB_MAX_BACKUP_DEST_LENGTH, tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_ENDPOINT,
                                     storage_table_info.dest_attr_.endpoint_,
                                     OB_MAX_BACKUP_ENDPOINT_LENGTH, tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_USEDFOR, used_for,
                                     OB_MAX_STORAGE_USED_FOR_LENGTH, tmp_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_ID, storage_id, uint64_t);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_AUTHORIZATION,
                                     storage_table_info.dest_attr_.authorization_,
                                     OB_MAX_BACKUP_AUTHORIZATION_LENGTH, tmp_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_STATE, state,
                                     OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_ID, storage_table_info.op_id_,
                                  uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_MAX_IOPS, storage_table_info.max_iops_,
                                  uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_MAX_BANDWIDTH, storage_table_info.max_bandwidth_,
                                  uint64_t);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_EXTENSION,
                                     storage_table_info.dest_attr_.extension_,
                                     OB_MAX_BACKUP_EXTENSION_LENGTH, tmp_str_len);
          storage_table_info.state_ = share::ObZoneStorageState::get_state(state);
          storage_table_info.used_for_ = share::ObStorageUsedType::get_type(used_for);
          storage_table_info.storage_id_ = storage_id;
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to extract field", KR(ret), K(sql));
          } else if (OB_FAIL(storage_table_info.zone_.assign(zone))) {
            LOG_WARN("failed to assign zone", KR(ret), K(zone));
          } else if (OB_UNLIKELY(!storage_table_info.is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("storage table info is invalid", KR(ret), K(storage_table_info));
          } else if (OB_FAIL(storage_table_infos.push_back(storage_table_info))) {
            LOG_WARN("fail to push back element into array", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::fetch_new_storage_id(ObMySQLProxy &sql_proxy, uint64_t &new_storage_id)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_storage_id = OB_INVALID_ID;
  ObMaxIdFetcher id_fetcher(sql_proxy);
  if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_STORAGE_ID_TYPE,
                                          tmp_storage_id, 1L /*storage start id*/))) {
    LOG_WARN("fetch_new_max_id failed", KR(ret), "id_type", OB_MAX_USED_STORAGE_ID_TYPE);
  } else {
    new_storage_id = tmp_storage_id;
  }
  return ret;
}

int ObStorageInfoOperator::fetch_new_storage_op_id(ObMySQLProxy &sql_proxy,
                                                   uint64_t &new_storage_op_id)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_storage_op_id = OB_INVALID_ID;
  ObMaxIdFetcher id_fetcher(sql_proxy);
  if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_STORAGE_OP_ID_TYPE,
                                          tmp_storage_op_id, 1L /*storage op start id*/))) {
    LOG_WARN("fetch_new_max_id failed", KR(ret), "id_type", OB_MAX_USED_STORAGE_OP_ID_TYPE);
  } else {
    new_storage_op_id = tmp_storage_op_id;
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_authorization(common::ObISQLClient &proxy,
                                                        const common::ObZone &zone,
                                                        const ObBackupDest &storage_dest,
                                                        const uint64_t op_id,
                                                        const ObStorageUsedType::TYPE used_for)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(
               authorization, sizeof(authorization)))) {
    LOG_WARN("fail to get authorization", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = '%s' WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu", OB_ALL_ZONE_STORAGE_TNAME,
               OB_STR_STORAGE_DEST_AUTHORIZATION, authorization, OB_STR_STORAGE_ZONE, zone.ptr(),
               OB_STR_STORAGE_DEST_PATH, storage_dest.get_root_path().ptr(), OB_STR_STORAGE_USEDFOR,
               ObStorageUsedType::get_str(used_for), OB_STR_STORAGE_DEST_ENDPOINT,
               storage_dest.get_storage_info()->endpoint_, OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_dest));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage authorization in zone storage table", K(sql), K(storage_dest));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_iops(common::ObISQLClient &proxy,
                                               const common::ObZone &zone,
                                               const share::ObBackupDest &storage_dest,
                                               const uint64_t op_id,
                                               const ObStorageUsedType::TYPE used_for,
                                               const int64_t max_iops)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      max_iops < 0 || OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest), K(max_iops), K(op_id), K(used_for));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = %ld WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_MAX_IOPS, max_iops,
               OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH,
               storage_dest.get_root_path().ptr(), OB_STR_STORAGE_USEDFOR,
               ObStorageUsedType::get_str(used_for), OB_STR_STORAGE_DEST_ENDPOINT,
               storage_dest.get_storage_info()->endpoint_, OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_dest));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage max_iops in zone storage table", K(sql), K(max_iops));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_bandwidth(common::ObISQLClient &proxy,
                                                    const common::ObZone &zone,
                                                    const share::ObBackupDest &storage_dest,
                                                    const uint64_t op_id,
                                                    const ObStorageUsedType::TYPE used_for,
                                                    const int64_t max_bandwidth)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      max_bandwidth < 0 || OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest), K(max_bandwidth), K(op_id), K(used_for));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = %ld WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_MAX_BANDWIDTH, max_bandwidth,
               OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH,
               storage_dest.get_root_path().ptr(), OB_STR_STORAGE_USEDFOR,
               ObStorageUsedType::get_str(used_for), OB_STR_STORAGE_DEST_ENDPOINT,
               storage_dest.get_storage_info()->endpoint_, OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_dest));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage max_bandwidth in zone storage table", K(sql), K(max_bandwidth));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_state(common::ObISQLClient &proxy,
                                                const common::ObZone &zone,
                                                const ObBackupDest &storage_dest,
                                                const ObStorageUsedType::TYPE used_for,
                                                const uint64_t op_id,
                                                const ObZoneStorageState::STATE state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      !ObZoneStorageState::is_valid(state) || OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = '%s' WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_STATE, ObZoneStorageState::get_str(state),
               OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH,
               storage_dest.get_root_path().ptr(), OB_STR_STORAGE_USEDFOR,
               ObStorageUsedType::get_str(used_for), OB_STR_STORAGE_DEST_ENDPOINT,
               storage_dest.get_storage_info()->endpoint_, OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_dest));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage state in zone storage table", K(sql), K(storage_dest));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_state(common::ObISQLClient &proxy,
                                                const common::ObZone &zone,
                                                const ObString &storage_path,
                                                const ObStorageUsedType::TYPE used_for,
                                                const uint64_t op_id,
                                                const ObZoneStorageState::STATE state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for) ||
      OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone), K(used_for));
  } else if (OB_FAIL(parse_storage_path(storage_path.ptr(), path, sizeof(path), endpoint,
                                        sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = '%s' WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_STATE, ObZoneStorageState::get_str(state),
               OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH, path,
               OB_STR_STORAGE_USEDFOR, ObStorageUsedType::get_str(used_for),
               OB_STR_STORAGE_DEST_ENDPOINT, endpoint, OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_path));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage state in zone storage table", K(sql), K(storage_path));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_op_id(common::ObISQLClient &proxy,
                                                const common::ObZone &zone,
                                                const ObBackupDest &storage_dest,
                                                const ObStorageUsedType::TYPE used_for,
                                                const uint64_t old_op_id, const uint64_t op_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  if (!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for) ||
      OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest), K(used_for), K(op_id));
  } else if (OB_FAIL(sql.assign_fmt(
               "UPDATE %s SET %s = %lu WHERE %s = '%s' AND %s = '%s' AND "
               "%s = '%s' AND %s = '%s' AND %s = %lu",
               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_OP_ID, op_id, OB_STR_STORAGE_ZONE,
               zone.ptr(), OB_STR_STORAGE_DEST_PATH, storage_dest.get_root_path().ptr(),
               OB_STR_STORAGE_USEDFOR, ObStorageUsedType::get_str(used_for),
               OB_STR_STORAGE_DEST_ENDPOINT, storage_dest.get_storage_info()->endpoint_,
               OB_STR_STORAGE_OP_ID, old_op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_dest));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage op id in zone storage table", K(sql), K(storage_dest));
  }
  return ret;
}

int ObStorageInfoOperator::update_storage_op_id(common::ObISQLClient &proxy,
                                                const common::ObZone &zone,
                                                const ObString &storage_path,
                                                const ObStorageUsedType::TYPE used_for,
                                                const uint64_t old_op_id, const uint64_t op_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t now = common::ObTimeUtil::current_time();
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for) ||
      OB_INVALID_ID == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone), K(used_for), K(op_id));
  } else if (OB_FAIL(parse_storage_path(storage_path.ptr(), path, sizeof(path), endpoint,
                                        sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET %s = %lu WHERE %s = '%s' AND %s = '%s' AND "
                                    "%s = '%s' AND %s = '%s' AND %s = %lu",
                                    OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_OP_ID, op_id,
                                    OB_STR_STORAGE_ZONE, zone.ptr(), OB_STR_STORAGE_DEST_PATH, path,
                                    OB_STR_STORAGE_USEDFOR, ObStorageUsedType::get_str(used_for),
                                    OB_STR_STORAGE_DEST_ENDPOINT, endpoint, OB_STR_STORAGE_OP_ID,
                                    old_op_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(zone), K(used_for), K(storage_path));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", KR(ret), K(affected_rows));
  } else {
    LOG_INFO("update storage op id in zone storage table", K(sql), K(storage_path));
  }
  return ret;
}

int ObStorageInfoOperator::get_ordered_zone_storage_infos(
  ObISQLClient &proxy, const ObZone &zone, ObIArray<ObZoneStorageTableInfo> &storage_table_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE %s = '%s' ORDER BY %s ASC",
                               OB_ALL_ZONE_STORAGE_TNAME, OB_STR_STORAGE_ZONE, zone.ptr(),
                               OB_STR_STORAGE_OP_ID))) {
      LOG_WARN("assign sql string failed", KR(ret));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("query zone storage record failed", KR(ret), K(sql));
    } else if (OB_ISNULL((result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), KP(result));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          ObZoneStorageTableInfo storage_table_info;
          if (OB_FAIL(extract_storage_table_info(*result, storage_table_info))) {
            LOG_WARN("failed to extract field", KR(ret), K(sql));
          } else if (OB_UNLIKELY(!storage_table_info.is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("storage table info is invalid", KR(ret), K(storage_table_info));
          } else if (OB_FAIL(storage_table_infos.push_back(storage_table_info))) {
            LOG_WARN("fail to push back element into array", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageInfoOperator::extract_storage_table_info(const sqlclient::ObMySQLResult &result,
    share::ObZoneStorageTableInfo &storage_table_info)
{
  int ret = OB_SUCCESS;
  char state[OB_MAX_STORAGE_STATE_LENGTH] = {0};
  char used_for[OB_MAX_STORAGE_USED_FOR_LENGTH] = {0};
  int tmp_str_len = 0;
  ObString zone;
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_STORAGE_ZONE, zone);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_DEST_PATH,
      storage_table_info.dest_attr_.path_,
      OB_MAX_BACKUP_DEST_LENGTH, tmp_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_DEST_ENDPOINT,
      storage_table_info.dest_attr_.endpoint_,
      OB_MAX_BACKUP_ENDPOINT_LENGTH, tmp_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_USEDFOR, used_for,
      OB_MAX_STORAGE_USED_FOR_LENGTH, tmp_str_len);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_STORAGE_ID, storage_table_info.storage_id_, uint64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_DEST_AUTHORIZATION,
      storage_table_info.dest_attr_.authorization_,
      OB_MAX_BACKUP_AUTHORIZATION_LENGTH, tmp_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_STATE, state,
      OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STORAGE_DEST_EXTENSION,
      storage_table_info.dest_attr_.extension_,
      OB_MAX_BACKUP_EXTENSION_LENGTH, tmp_str_len);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_STORAGE_OP_ID, storage_table_info.op_id_,
      uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_STORAGE_MAX_IOPS, storage_table_info.max_iops_,
      uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_STORAGE_MAX_BANDWIDTH, storage_table_info.max_bandwidth_,
      uint64_t);
  storage_table_info.used_for_ = share::ObStorageUsedType::get_type(used_for);
  storage_table_info.state_ = share::ObZoneStorageState::get_state(state);
  if (FAILEDx(storage_table_info.zone_.assign(zone))) {
    LOG_WARN("failed to assign zone", KR(ret), K(zone));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to extract field", KR(ret));
  }
  return ret;
}

int ObStorageInfoOperator::get_ordered_zone_storage_infos_with_sub_op_id( ObISQLClient &proxy,
    const ObZone &zone, ObIArray<ObZoneStorageTableInfo> &storage_table_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT A.*, MAX(B.%s) AS %s FROM %s AS A, %s AS B "
            "WHERE A.%s = B.%s AND A.%s = '%s' "
            "GROUP BY A.%s "
            "ORDER BY %s ASC, %s ASC",
            OB_STR_STORAGE_SUB_OP_ID, OB_STR_STORAGE_SUB_OP_ID, // sub_op_id
            OB_ALL_ZONE_STORAGE_TNAME, OB_ALL_ZONE_STORAGE_OPERATION_TNAME, // the two table name
            OB_STR_STORAGE_OP_ID, OB_STR_STORAGE_OP_ID, // where op_id
            OB_STR_STORAGE_ZONE, zone.ptr(), // where zone
            OB_STR_STORAGE_OP_ID, // group by
            OB_STR_STORAGE_OP_ID, OB_STR_STORAGE_SUB_OP_ID))) { // order by
      LOG_WARN("assign sql string failed", KR(ret));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("query zone storage record failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), KP(result));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          share::ObZoneStorageTableInfo storage_table_info;
          if (OB_FAIL(extract_storage_table_info(*result, storage_table_info))) {
            LOG_WARN("failed to extract_storage_table_info", KR(ret), K(sql));
          }
          EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_SUB_OP_ID,
              storage_table_info.sub_op_id_, uint64_t);
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(!storage_table_info.is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("storage table info is invalid", KR(ret), K(storage_table_info));
          } else if (OB_FAIL(storage_table_infos.push_back(storage_table_info))) {
            LOG_WARN("fail to push back element into array", KR(ret));
          }
        }
      }
    }
  }
  return ret;

}

int ObStorageInfoOperator::get_zone_storage_info(common::ObISQLClient &proxy, const uint64_t op_id,
                                                 ObZoneStorageTableInfo &zone_storage_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (UINT64_MAX == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op_id", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE %s = %lu", OB_ALL_ZONE_STORAGE_TNAME,
                                    OB_STR_STORAGE_OP_ID, op_id))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result must not be NULL", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no exist row", KR(ret), K(sql));
        } else {
          LOG_WARN("fail to get next row", KR(ret), K(sql));
        }
      } else {
        ObString zone;
        char state[OB_MAX_STORAGE_STATE_LENGTH] = {0};
        char used_for[OB_MAX_STORAGE_USED_FOR_LENGTH] = {0};
        int tmp_str_len = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, OB_STR_STORAGE_ZONE, zone);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_PATH,
                                   zone_storage_info.dest_attr_.path_, OB_MAX_BACKUP_DEST_LENGTH,
                                   tmp_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_ENDPOINT,
                                   zone_storage_info.dest_attr_.endpoint_,
                                   OB_MAX_BACKUP_ENDPOINT_LENGTH, tmp_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_USEDFOR, used_for,
                                   OB_MAX_STORAGE_USED_FOR_LENGTH, tmp_str_len);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_ID, zone_storage_info.storage_id_,
                                uint64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_AUTHORIZATION,
                                   zone_storage_info.dest_attr_.authorization_,
                                   OB_MAX_BACKUP_AUTHORIZATION_LENGTH, tmp_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_STATE, state,
                                   OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_DEST_EXTENSION,
                                   zone_storage_info.dest_attr_.extension_,
                                   OB_MAX_BACKUP_EXTENSION_LENGTH, tmp_str_len);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_ID, zone_storage_info.op_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_MAX_IOPS, zone_storage_info.max_iops_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_MAX_BANDWIDTH, zone_storage_info.max_bandwidth_, uint64_t);
        zone_storage_info.used_for_ = share::ObStorageUsedType::get_type(used_for);
        zone_storage_info.state_ = share::ObZoneStorageState::get_state(state);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extract field", KR(ret), K(sql));
        } else if (OB_FAIL(zone_storage_info.zone_.assign(zone))) {
          LOG_WARN("failed to assign zone", KR(ret), K(zone));
        } else if (OB_UNLIKELY(!zone_storage_info.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("storage table info is invalid", KR(ret), K(zone_storage_info));
        }
      }
    }
  }
  return ret;
}

int ObStorageOperationOperator::get_max_sub_op_info(
  ObISQLClient &proxy, const uint64_t op_id, ObZoneStorageOperationTableInfo &storage_op_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (UINT64_MAX == op_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op_id", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT %s, %s, %s FROM %s WHERE %s = %lu ORDER BY %s"
                                    " DESC LIMIT 1",
                                    OB_STR_STORAGE_OP_ID, OB_STR_STORAGE_SUB_OP_ID,
                                    OB_STR_STORAGE_OP_TYPE, OB_ALL_ZONE_STORAGE_OPERATION_TNAME,
                                    OB_STR_STORAGE_OP_ID, op_id, OB_STR_STORAGE_SUB_OP_ID))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result must not be NULL", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no exist row", KR(ret), K(sql));
        } else {
          LOG_WARN("fail to get next row", KR(ret), K(sql));
        }
      } else {
        char tmp_op_type[OB_MAX_STORAGE_STATE_LENGTH] = {0};
        int tmp_str_len = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_ID, storage_op_info.op_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_SUB_OP_ID, storage_op_info.sub_op_id_,
                                uint64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_TYPE, tmp_op_type,
                                   OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
        if (OB_SUCC(ret)) {
          storage_op_info.op_type_ = share::ObZoneStorageState::get_state(tmp_op_type);
        } else {
          LOG_WARN("failed to extract field", KR(ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObStorageOperationOperator::get_min_op_info_greater_than(
  ObISQLClient &proxy, const common::ObZone &zone, const uint64_t last_op_id,
  const uint64_t last_sub_op_id, ObZoneStorageOperationTableInfo &storage_op_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if ((UINT64_MAX == last_op_id) || (UINT64_MAX == last_sub_op_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op_id", KR(ret), K(last_op_id), K(last_sub_op_id));
  } else if (OB_FAIL(sql.assign_fmt(
               "SELECT %s, %s, %s FROM %s WHERE zone = '%s' AND"
               " sub_op_id > %lu ORDER BY sub_op_id ASC LIMIT 1",
               OB_STR_STORAGE_OP_ID, OB_STR_STORAGE_SUB_OP_ID, OB_STR_STORAGE_OP_TYPE,
               OB_ALL_ZONE_STORAGE_OPERATION_TNAME, zone.ptr(), last_sub_op_id))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result must not be NULL", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no exist row", KR(ret), K(sql));
        } else {
          LOG_WARN("fail to get next row", KR(ret), K(sql));
        }
      } else {
        char tmp_op_type[OB_MAX_STORAGE_STATE_LENGTH] = {0};
        int tmp_str_len = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_ID, storage_op_info.op_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_STORAGE_SUB_OP_ID, storage_op_info.sub_op_id_,
                                uint64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STORAGE_OP_TYPE, tmp_op_type,
                                   OB_MAX_STORAGE_STATE_LENGTH, tmp_str_len);
        if (OB_SUCC(ret)) {
          storage_op_info.op_type_ = share::ObZoneStorageState::get_state(tmp_op_type);
        } else {
          LOG_WARN("failed to extract field", KR(ret), K(sql));
        }
      }
    }
  }
  return ret;
}
