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

#include "share/ob_gts_table_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase {
using namespace common;
namespace share {
ObGtsTableOperator::ObGtsTableOperator()
{
  is_inited_ = false;
  proxy_ = NULL;
}

ObGtsTableOperator::~ObGtsTableOperator()
{
  is_inited_ = false;
  proxy_ = NULL;
}

int ObGtsTableOperator::init(common::ObMySQLProxy* proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else {
    proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObGtsTableOperator::insert_tenant_gts(const uint64_t tenant_id, const uint64_t gts_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) || OB_FAIL(dml.add_column("gts_id", gts_id)) ||
        OB_FAIL(dml.add_column("orig_gts_id", 0 /* deem 0 as invalid gts id*/)) ||
        OB_FAIL(dml.add_column("gts_invalid_ts", OB_INVALID_TIMESTAMP))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_TENANT_GTS_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec insert", K(ret));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected rows unexpected", K(ret));
    }
  }
  return ret;
}

int ObGtsTableOperator::erase_tenant_gts(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_GTS_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec insert", K(ret));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected rows unexpected", K(ret));
    }
  }
  return ret;
}

int ObGtsTableOperator::insert_gts_instance(const common::ObGtsInfo& gts_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (!gts_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(gts_info));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    ObString gts_name(strlen(gts_info.gts_name_.ptr()), gts_info.gts_name_.ptr());
    ObString region_name(strlen(gts_info.region_.ptr()), gts_info.region_.ptr());
    char standby_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    const int64_t standby_port = gts_info.standby_.get_port();
    char member_list_str[MAX_MEMBER_LIST_LENGTH] = "";
    ObPartitionReplica::MemberList member_list;
    if (!gts_info.standby_.ip_to_string(standby_ip, sizeof(standby_ip))) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "fail to convert server ip to string", K(ret), "addr", gts_info.standby_);
    } else if (OB_FAIL(convert_member_list_(gts_info.member_list_, member_list))) {
      SHARE_LOG(WARN, "fail to conver member list", K(ret));
    } else if (OB_FAIL(ObPartitionReplica::member_list2text(member_list, member_list_str, MAX_MEMBER_LIST_LENGTH))) {
      SHARE_LOG(WARN, "fail to exec member list to text", K(ret));
    } else if (OB_FAIL(dml.add_pk_column("gts_id", gts_info.gts_id_)) ||
               OB_FAIL(dml.add_column("gts_name", gts_name)) || OB_FAIL(dml.add_column("region", region_name)) ||
               OB_FAIL(dml.add_column("epoch_id", gts_info.epoch_id_)) ||
               OB_FAIL(dml.add_column("member_list", member_list_str)) ||
               OB_FAIL(dml.add_column("standby_ip", standby_ip)) ||
               OB_FAIL(dml.add_column("standby_port", standby_port)) ||
               OB_FAIL(dml.add_column("heartbeat_ts", gts_info.heartbeat_ts_))) {
      SHARE_LOG(WARN, "fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_GTS_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec insert", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected rows unexpected", K(ret));
    }
  }
  return ret;
}

int ObGtsTableOperator::try_update_standby(
    const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby, bool& do_update)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char standby_ip[MAX_IP_ADDR_LENGTH];
  memset(standby_ip, 0, MAX_IP_ADDR_LENGTH);
  const int64_t standby_port = new_standby.get_port();
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (!new_standby.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(new_standby));
  } else if (!new_standby.ip_to_string(standby_ip, sizeof(standby_ip))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "fail to covert server ip to string", K(ret), K(new_standby));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET standby_ip = '%s', standby_port = '%ld', gmt_create = now(6) "
                                    "WHERE gts_id = %lu and epoch_id = %ld",
                 OB_ALL_GTS_TNAME,
                 standby_ip,
                 standby_port,
                 gts_info.gts_id_,
                 gts_info.epoch_id_))) {
    SHARE_LOG(WARN, "fail to assign fmt", K(ret));
  } else if (OB_UNLIKELY(nullptr == proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "proxy_ ptr is null", K(ret));
  } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
    SHARE_LOG(WARN, "fail to write proxy", K(ret));
  } else if (1 == affected_rows) {
    do_update = true;
  } else if (affected_rows <= 0) {
    do_update = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "unexpected affected rows", K(ret), K(affected_rows));
  }
  return ret;
}

int ObGtsTableOperator::get_gts_info(const uint64_t gts_id, common::ObGtsInfo& gts_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(0 == gts_id || OB_INVALID_ID == gts_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(gts_id));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SELECT * from %s where gts_id = %lu", OB_ALL_GTS_TNAME, gts_id))) {
      SHARE_LOG(WARN, "fail to append fmt", K(ret));
    } else {
      ObSQLClientRetryWeak sql_client_retry_weak(proxy_, false);
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        common::sqlclient::ObMySQLResult* result = NULL;
        if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
          SHARE_LOG(WARN, "execute sql failed", K(ret), K(sql));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "result is not expected to be NULL", K(ret));
        } else if (OB_FAIL(result->next())) {
          SHARE_LOG(WARN, "get next result failed", K(ret));
        } else if (OB_FAIL(read_gts_info_(*result, gts_info))) {
          SHARE_LOG(WARN, "fail to read gts info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGtsTableOperator::get_gts_infos(common::ObIArray<common::ObGtsInfo>& gts_infos) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      SHARE_LOG(WARN, "fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s", OB_ALL_GTS_TNAME))) {
      SHARE_LOG(WARN, "append_fmt failed", K(ret));
    } else if (OB_FAIL(read_gts_infos_(sql, gts_infos))) {
      SHARE_LOG(WARN, "read_gts_infos_ failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObGtsTableOperator::get_gts_tenant_infos(common::ObIArray<common::ObGtsTenantInfo>& gts_tenant_infos) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      SHARE_LOG(WARN, "fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt(
                   "select __all_gts.gts_id as gts_id, tenant_id, member_list from __all_gts , __all_tenant_gts where "
                   "__all_gts.gts_id = __all_tenant_gts.gts_id order by gts_id"))) {
      SHARE_LOG(WARN, "append_fmt failed", K(ret));
    } else if (OB_FAIL(read_gts_tenant_infos_(sql, gts_tenant_infos))) {
      SHARE_LOG(WARN, "read_gts_tenant_infos_ failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObGtsTableOperator::get_gts_standby(const uint64_t gts_id, common::ObAddr& standby) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (!is_valid_gts_id(gts_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(gts_id));
  } else if (OB_FAIL(sql.append_fmt("select standby_ip, standby_port from __all_gts where gts_id = %lu", gts_id))) {
    SHARE_LOG(WARN, "append_fmt failed", K(ret), K(gts_id));
  } else if (OB_FAIL(read_gts_standby_(sql, standby))) {
    SHARE_LOG(WARN, "read_gts_standby_ failed", K(ret), K(sql));
  } else {
    // do nothing
  }
  SHARE_LOG(INFO, "get_gts_standby finished", K(ret), K(gts_id), K(standby), K(sql));
  return ret;
}

int ObGtsTableOperator::update_gts_member_list(const uint64_t gts_id, const int64_t curr_epoch_id,
    const int64_t new_epoch_id, const common::ObMemberList& member_list, const common::ObAddr& standby)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  ObPartitionReplica::MemberList replica_member_list;
  char str_member_list[MAX_MEMBER_LIST_LENGTH];
  memset(str_member_list, 0, MAX_MEMBER_LIST_LENGTH);
  char standby_ip[MAX_IP_ADDR_LENGTH];
  memset(standby_ip, 0, MAX_IP_ADDR_LENGTH);
  const int64_t standby_port = standby.get_port();
  int64_t affected_rows = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (!is_valid_gts_id(gts_id) || curr_epoch_id <= 0 || new_epoch_id <= 0 || curr_epoch_id >= new_epoch_id ||
             member_list.get_member_number() != 2 || !standby.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(
        WARN, "invalid arguments", K(ret), K(gts_id), K(curr_epoch_id), K(new_epoch_id), K(member_list), K(standby));
  } else if (!standby.ip_to_string(standby_ip, sizeof(standby_ip))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "fail to convert server ip to string", K(ret), K(standby));
  } else if (OB_FAIL(convert_member_list_(member_list, replica_member_list))) {
    SHARE_LOG(WARN, "convert_member_list_ failed", K(ret));
  } else if (OB_FAIL(
                 ObPartitionReplica::member_list2text(replica_member_list, str_member_list, MAX_MEMBER_LIST_LENGTH))) {
    SHARE_LOG(WARN, "member_list2text failed", K(ret));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE __all_gts SET epoch_id = %ld, member_list = '%s', gmt_modified = now(6) "
                                "WHERE gts_id = %lu AND epoch_id = %ld AND standby_ip = '%s' AND standby_port = %ld",
                     new_epoch_id,
                     str_member_list,
                     gts_id,
                     curr_epoch_id,
                     standby_ip,
                     standby_port))) {
    SHARE_LOG(WARN, "assign_fmt failed", K(ret));
  } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
    SHARE_LOG(WARN, "execute_sql failed", K(ret));
  } else if (affected_rows != 1) {
    ret = OB_GTS_UPDATE_FAILED;
  } else {
    // do nothing
  }

  SHARE_LOG(INFO,
      "update_gts_member_list",
      K(ret),
      K(gts_id),
      K(curr_epoch_id),
      K(new_epoch_id),
      K(member_list),
      K(standby),
      K(sql),
      K(affected_rows));
  return ret;
}

int ObGtsTableOperator::update_gts_member_list_and_standby(const uint64_t gts_id, const int64_t curr_epoch_id,
    const int64_t new_epoch_id, const common::ObMemberList& member_list, const common::ObAddr& standby)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  ObPartitionReplica::MemberList replica_member_list;
  char str_member_list[MAX_MEMBER_LIST_LENGTH];
  memset(str_member_list, 0, MAX_MEMBER_LIST_LENGTH);
  char standby_ip[MAX_IP_ADDR_LENGTH];
  memset(standby_ip, 0, MAX_IP_ADDR_LENGTH);
  const int64_t standby_port = standby.get_port();

  common::ObAddr dummy_standby;
  char dummy_standby_ip[MAX_IP_ADDR_LENGTH];
  memset(dummy_standby_ip, 0, MAX_IP_ADDR_LENGTH);
  const int64_t dummy_standby_port = dummy_standby.get_port();

  int64_t affected_rows = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (!is_valid_gts_id(gts_id) || curr_epoch_id <= 0 || new_epoch_id <= 0 || curr_epoch_id >= new_epoch_id ||
             member_list.get_member_number() != 3 || !standby.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(
        WARN, "invalid arguments", K(ret), K(gts_id), K(curr_epoch_id), K(new_epoch_id), K(member_list), K(standby));
  } else if (!standby.ip_to_string(standby_ip, sizeof(standby_ip))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "fail to convert server ip to string", K(ret), K(standby));
  } else if (!dummy_standby.ip_to_string(dummy_standby_ip, sizeof(dummy_standby_ip))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "fail to convert server ip to string", K(ret), K(dummy_standby));
  } else if (OB_FAIL(convert_member_list_(member_list, replica_member_list))) {
    SHARE_LOG(WARN, "convert_member_list_ failed", K(ret));
  } else if (OB_FAIL(
                 ObPartitionReplica::member_list2text(replica_member_list, str_member_list, MAX_MEMBER_LIST_LENGTH))) {
    SHARE_LOG(WARN, "member_list2text failed", K(ret));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE __all_gts SET epoch_id = %ld, member_list = '%s', standby_ip = '%s', "
                                "standby_port = %ld, gmt_modified = now(6) "
                                "WHERE gts_id = %lu AND epoch_id = %ld AND standby_ip = '%s' AND standby_port = %ld",
                     new_epoch_id,
                     str_member_list,
                     dummy_standby_ip,
                     dummy_standby_port,
                     gts_id,
                     curr_epoch_id,
                     standby_ip,
                     standby_port))) {
    SHARE_LOG(WARN, "assign_fmt failed", K(ret));
  } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
    SHARE_LOG(WARN, "execute_sql failed", K(ret));
  } else if (affected_rows != 1) {
    ret = OB_GTS_UPDATE_FAILED;
  } else {
    // do nothing
  }

  SHARE_LOG(INFO,
      "update_gts_member_list_and_standby finished",
      K(ret),
      K(gts_id),
      K(curr_epoch_id),
      K(new_epoch_id),
      K(member_list),
      K(standby),
      K(sql),
      K(affected_rows));
  return ret;
}

int ObGtsTableOperator::read_gts_infos_(const ObSqlString& sql, common::ObIArray<common::ObGtsInfo>& gts_infos) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid sql", K(sql), K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(proxy_, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SHARE_LOG(WARN, "execute sql failed", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "result is not expected to be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          ObGtsInfo item;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              SHARE_LOG(WARN, "get next result failed", K(ret));
            }
          } else if (OB_FAIL(read_gts_info_(*result, item))) {
            SHARE_LOG(WARN, "read_gts_info_ failed", K(ret));
          } else if (OB_FAIL(gts_infos.push_back(item))) {
            SHARE_LOG(WARN, "push_back failed", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObGtsTableOperator::read_gts_info_(const common::sqlclient::ObMySQLResult& result, ObGtsInfo& gts_info) const
{
  int ret = OB_SUCCESS;
  gts_info.reset();
  int64_t tmp_real_str_len = 0;
  char str_member_list[MAX_MEMBER_LIST_LENGTH];
  memset(str_member_list, 0, MAX_MEMBER_LIST_LENGTH);
  char standby_ip[MAX_IP_ADDR_LENGTH];
  memset(standby_ip, 0, MAX_IP_ADDR_LENGTH);
  int32_t standby_port = 0;
  ObPartitionReplica::MemberList orig_member_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "gts_id", gts_info.gts_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "gts_name", gts_info.gts_name_.ptr(), MAX_GTS_NAME_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "region", gts_info.region_.ptr(), MAX_REGION_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "epoch_id", gts_info.epoch_id_, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "member_list", str_member_list, MAX_MEMBER_LIST_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "standby_ip", standby_ip, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "standby_port", standby_port, int32_t);
    EXTRACT_INT_FIELD_MYSQL(result, "heartbeat_ts", gts_info.heartbeat_ts_, int64_t);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPartitionReplica::text2member_list(str_member_list, orig_member_list))) {
        SHARE_LOG(WARN, "text2member_list failed", K(ret));
      } else if (OB_FAIL(convert_member_list_(orig_member_list, gts_info.member_list_))) {
        SHARE_LOG(WARN, "convert_member_list_ failed", K(ret));
      } else {
        gts_info.standby_.set_ip_addr(standby_ip, standby_port);
      }
    }
    (void)tmp_real_str_len;
  }
  return ret;
}

int ObGtsTableOperator::read_gts_tenant_infos_(
    const common::ObSqlString& sql, common::ObIArray<common::ObGtsTenantInfo>& gts_tenant_infos) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid sql", K(sql), K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(proxy_, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SHARE_LOG(WARN, "execute sql failed", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "result is not expected to be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          ObGtsTenantInfo item;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              SHARE_LOG(WARN, "get next result failed", K(ret));
            }
          } else if (OB_FAIL(read_gts_tenant_info_(*result, item))) {
            SHARE_LOG(WARN, "read_gts_tenant_info_ failed", K(ret));
          } else if (OB_FAIL(gts_tenant_infos.push_back(item))) {
            SHARE_LOG(WARN, "push_back failed", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObGtsTableOperator::read_gts_tenant_info_(
    const common::sqlclient::ObMySQLResult& result, common::ObGtsTenantInfo& gts_tenant_info) const
{
  int ret = OB_SUCCESS;
  gts_tenant_info.reset();
  int64_t tmp_real_str_len = 0;
  char str_member_list[MAX_MEMBER_LIST_LENGTH];
  memset(str_member_list, 0, MAX_MEMBER_LIST_LENGTH);
  ObPartitionReplica::MemberList orig_member_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "gts_id", gts_tenant_info.gts_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", gts_tenant_info.tenant_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "member_list", str_member_list, MAX_MEMBER_LIST_LENGTH, tmp_real_str_len);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPartitionReplica::text2member_list(str_member_list, orig_member_list))) {
        SHARE_LOG(WARN, "text2member_list failed", K(ret));
      } else if (OB_FAIL(convert_member_list_(orig_member_list, gts_tenant_info.member_list_))) {
        SHARE_LOG(WARN, "convert_member_list_ failed", K(ret));
      }
    }
    (void)tmp_real_str_len;
  }
  return ret;
}

int ObGtsTableOperator::read_gts_standby_(const common::ObSqlString& sql, common::ObAddr& standby) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid sql", K(sql), K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(proxy_, false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SHARE_LOG(WARN, "execute sql failed", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "result is not expected to be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              SHARE_LOG(WARN, "get next result failed", K(ret));
            }
          } else if (OB_FAIL(read_gts_standby_(*result, standby))) {
            SHARE_LOG(WARN, "read_gts_standby_ failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObGtsTableOperator::read_gts_standby_(const common::sqlclient::ObMySQLResult& result, common::ObAddr& standby) const
{
  int ret = OB_SUCCESS;
  standby.reset();
  int64_t tmp_real_str_len = 0;
  char standby_ip[MAX_IP_ADDR_LENGTH];
  memset(standby_ip, 0, MAX_IP_ADDR_LENGTH);
  int32_t standby_port = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    EXTRACT_STRBUF_FIELD_MYSQL(result, "standby_ip", standby_ip, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "standby_port", standby_port, int32_t);
    if (OB_SUCC(ret)) {
      standby.set_ip_addr(standby_ip, standby_port);
    }
    (void)tmp_real_str_len;
  }
  return ret;
}

int ObGtsTableOperator::convert_member_list_(
    const ObPartitionReplica::MemberList& orig_member_list, common::ObMemberList& curr_member_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_member_list.count(); i++) {
    const ObMember member = ObMember(orig_member_list[i].server_, orig_member_list[i].timestamp_);
    if (OB_FAIL(curr_member_list.add_member(member))) {
      SHARE_LOG(WARN, "curr_member_list add_member failed", K(ret));
    }
  }
  return ret;
}
int ObGtsTableOperator::convert_member_list_(
    const common::ObMemberList& orig_member_list, ObPartitionReplica::MemberList& curr_member_list)
{
  int ret = OB_SUCCESS;
  curr_member_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_member_list.get_member_number(); ++i) {
    ObMember member;
    if (OB_FAIL(orig_member_list.get_member_by_index(i, member))) {
      SHARE_LOG(WARN, "fail to get member by index", K(ret));
    } else if (OB_FAIL(curr_member_list.push_back(
                   ObPartitionReplica::Member(member.get_server(), member.get_timestamp())))) {
      SHARE_LOG(WARN, "fail to push back", K(ret));
    }
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase
