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


#include "ob_service_name_proxy.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_tenant_event_def.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_service_epoch_proxy.h"

using namespace oceanbase::tenant_event;
using namespace oceanbase::common::sqlclient;
namespace oceanbase
{
namespace share
{
static const char *SERVICE_STATUS_STR[] = {
    "INVALID SERVICE STATUS",
    "STARTED",
    "STOPPING",
    "STOPPED",
};
OB_SERIALIZE_MEMBER(ObServiceNameID,id_);
OB_SERIALIZE_MEMBER(ObServiceNameString,str_);
int ObServiceNameString::init(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_service_name(str))) {
    LOG_WARN("fail to execute check_service_name", KR(ret), K(str));
  } else if (OB_FAIL(str_.assign(str))) {
    LOG_WARN("fail to assign str_", KR(ret), K(str));
  }
  return ret;
}
int ObServiceNameString::assign(const ObServiceNameString &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(str_.assign(other.str_))) {
      LOG_WARN("fail to assign str_", KR(ret), K(other));
    }
  }
  return ret;
}
bool ObServiceNameString::equal_to(const ObServiceNameString &service_name_string) const
{
  return 0 == str_.str().case_compare(service_name_string.str_.str());
}
int ObServiceNameString::check_service_name(const ObString &service_name_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(service_name_str.empty() || service_name_str.length() > OB_SERVICE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid service_name", KR(ret), K(service_name_str), K(service_name_str.length()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "service_name, whose length should be 1 - 64");
  } else if (!isalpha(service_name_str[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid service_name", KR(ret), K(service_name_str));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "service_name, which should be start with letter");
  } else {
    for (int i = 1; i < service_name_str.length() && OB_SUCC(ret); i++)
    {
      const char cur_char = service_name_str[i];
      if (!isalpha(cur_char) && !isdigit(cur_char) && cur_char != '_' ) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid service_name", KR(ret), K(service_name_str));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "service_name, which can only include letter, digit and underscore");
      }
    }
  }
  return ret;
}

static const char *SERVICE_OP_STR[] = {
    "INVALID SERVICE OPERATION",
    "CREATE SERVICE",
    "DELETE SERVICE",
    "START SERVICE",
    "STOP SERVICE",
};
const char *ObServiceNameArg::service_op_to_str(const ObServiceNameArg::ObServiceOp &service_op)
{
  STATIC_ASSERT(ARRAYSIZEOF(SERVICE_OP_STR) == MAX_SERVICE_OP, "array size mismatch");
  ObServiceOp returned_service_op = INVALID_SERVICE_OP;
  if (OB_UNLIKELY(service_op >= MAX_SERVICE_OP
                  || service_op < INVALID_SERVICE_OP)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown service op", K(service_op));
  } else {
    returned_service_op = service_op;
  }
  return SERVICE_OP_STR[returned_service_op];
}
int ObServiceNameArg::init(const ObServiceOp op, const uint64_t target_tenant_id, const ObString &service_name_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_service_op(op)
      || !is_valid_tenant_id(target_tenant_id)
      || service_name_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init", KR(ret), K(op), K(target_tenant_id), K(service_name_str));
  } else if (OB_FAIL(service_name_str_.init(service_name_str))) {
    LOG_WARN("fail to init service_name_str_", KR(ret), K(service_name_str));
  } else {
    op_ = op;
    target_tenant_id_ = target_tenant_id;
  }
  return ret;
}
bool ObServiceNameArg::is_valid() const
{
  return is_valid_service_op(op_) && is_valid_tenant_id(target_tenant_id_) && service_name_str_.is_valid();
}
bool ObServiceNameArg::is_valid_service_op(ObServiceOp op)
{
  return op > INVALID_SERVICE_OP && op < MAX_SERVICE_OP;
}
int ObServiceNameArg::assign(const ObServiceNameArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(service_name_str_.assign(other.service_name_str_))) {
      LOG_WARN("fail to assign service_name_str_", KR(ret), K(other));
    } else {
      op_ = other.op_;
      target_tenant_id_ = other.target_tenant_id_;
    }
  }
  return ret;
}
void ObServiceNameArg::reset()
{
  op_ = INVALID_SERVICE_OP;
  target_tenant_id_ = OB_INVALID_TENANT_ID;
  service_name_str_.reset();
}
const char *ObServiceName::service_status_to_str(const ObServiceName::ObServiceStatus &service_status)
{
  STATIC_ASSERT(ARRAYSIZEOF(SERVICE_STATUS_STR) == MAX_SERVICE_STATUS, "array size mismatch");
  ObServiceStatus returned_service_status = INVALID_SERVICE_STATUS;
  if (OB_UNLIKELY(service_status >= MAX_SERVICE_STATUS
                  || service_status < INVALID_SERVICE_STATUS)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown service status", K(service_status));
  } else {
    returned_service_status = service_status;
  }
  return SERVICE_STATUS_STR[returned_service_status];
}
ObServiceName::ObServiceStatus ObServiceName::str_to_service_status(const ObString &service_status_str)
{
  ObServiceStatus service_status = INVALID_SERVICE_STATUS;
  bool is_found = false;
  for (int i = INVALID_SERVICE_STATUS; i < MAX_SERVICE_STATUS && !is_found; i++)
  {
    if (0 == service_status_str.case_compare(SERVICE_STATUS_STR[i])) {
      service_status = static_cast<ObServiceStatus>(i);
      is_found = true;
    }
  }
  return service_status;
}
bool ObServiceName::is_valid_service_status(const ObServiceName::ObServiceStatus &service_status)
{
  return service_status > INVALID_SERVICE_STATUS && service_status < MAX_SERVICE_STATUS;
}
OB_SERIALIZE_MEMBER(ObServiceName, tenant_id_, service_name_id_, service_name_str_, service_status_);
int ObServiceName::init(
    const uint64_t tenant_id,
    const uint64_t service_name_id,
    const ObString &service_name_str,
    const ObString &service_status_str)
{
  int ret = OB_SUCCESS;
  ObServiceStatus service_status = str_to_service_status(service_status_str);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ObServiceNameID::is_valid_service_name_id(service_name_id)
      || !is_valid_service_status(service_status)
      || service_name_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_id), K(service_name_str),
        K(service_status_str), K(service_status));
  } else if (OB_FAIL(service_name_str_.init(service_name_str))) {
    LOG_WARN("fail to init service_name_str_", KR(ret), K(service_name_str));
  } else {
    tenant_id_ = tenant_id;
    service_name_id_ = service_name_id;
    service_status_ = service_status;
  }
  return ret;
}
bool ObServiceName::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && service_name_id_.is_valid()
      && !service_name_str_.is_empty() && is_valid_service_status(service_status_);
}
int ObServiceName::assign(const ObServiceName &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(service_name_str_.assign(other.service_name_str_))) {
      LOG_WARN("fail to assign service_name_str_", KR(ret), K(other));
    } else {
      service_name_id_ = other.service_name_id_;
      service_status_ = other.service_status_;
      tenant_id_ = other.tenant_id_;
    }
  }
  return ret;
}
void ObServiceName::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  service_name_id_.reset();
  service_name_str_.reset();
  service_status_ = INVALID_SERVICE_STATUS;
}
int ObServiceNameProxy::select_all_service_names_with_epoch(
    const int64_t tenant_id,
    int64_t &epoch,
    ObIArray<ObServiceName> &all_service_names)
{
  int ret = OB_SUCCESS;
  const bool EXTRACT_EPOCH = true;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(sql.assign_fmt("SELECT s.*, e.value as epoch FROM %s AS s "
      "RIGHT JOIN %s AS e ON s.tenant_id = e.tenant_id WHERE e.tenant_id = %lu and e.name='%s' ORDER BY s.gmt_create",
      OB_ALL_SERVICE_TNAME, OB_ALL_SERVICE_EPOCH_TNAME, tenant_id, ObServiceEpochProxy::SERVICE_NAME_EPOCH))) {
    // join the two tables to avoid add a row lock on __all_service_epoch
    // otherwise there might be conflicts and too many retries in tenant_info_loader thread
    // when the number of observers is large
    // use right join instead of join, otherwise we cannot see service_name being deleted in the cache
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql));
  } else if (OB_FAIL(select_service_name_sql_helper_(*GCTX.sql_proxy_, tenant_id, EXTRACT_EPOCH,
      sql, epoch, all_service_names))) {
    LOG_WARN("fail to execute select_service_name_sql_helper_", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}
int ObServiceNameProxy::select_all_service_names_(
    common::ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    ObIArray<ObServiceName> &all_service_names)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const bool NOT_EXTRACT_EPOCH = false;
  int64_t unused_epoch = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE TENANT_ID = %lu ORDER BY gmt_create",
      OB_ALL_SERVICE_TNAME, tenant_id))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql));
  } else if (OB_FAIL(select_service_name_sql_helper_(sql_proxy, tenant_id, NOT_EXTRACT_EPOCH,
      sql, unused_epoch, all_service_names))) {
    LOG_WARN("fail to execute select_service_name_sql_helper_", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObServiceNameProxy::select_service_name_sql_helper_(
    common::ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const bool extract_epoch,
    ObSqlString &sql,
    int64_t &epoch,
    ObIArray<ObServiceName> &all_service_names)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t QUERY_TIMEOUT = 5 * GCONF.rpc_timeout; // 10s
  all_service_names.reset();
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, QUERY_TIMEOUT))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx), K(QUERY_TIMEOUT));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else {
        ObServiceName service_name;
        while (OB_SUCC(ret)) {
          service_name.reset();
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(build_service_name_(*result, service_name))) {
            LOG_WARN("fail to build server status", KR(ret));
          } else if (service_name.is_valid() && OB_FAIL(all_service_names.push_back(service_name))) {
            LOG_WARN("fail to build service_name", KR(ret));
          } else if (extract_epoch) {
            // epoch can only be extracted when __all_service_epoch table is joined
            EXTRACT_INT_FIELD_MYSQL(*result, "epoch", epoch, int64_t);
          }
        }
      }
    }
  }
  return ret;
}
int ObServiceNameProxy::select_service_name(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str,
    ObServiceName &service_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t QUERY_TIMEOUT = 5 * GCONF.rpc_timeout; // 10s
  service_name.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, QUERY_TIMEOUT))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx), K(QUERY_TIMEOUT));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE TENANT_ID = %lu AND SERVICE_NAME = '%s'",
      OB_ALL_SERVICE_TNAME, tenant_id, service_name_str.ptr()))) {
    LOG_WARN("sql assign_fmt failed", KR(ret), K(sql), K(tenant_id), K(service_name_str));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(sql), KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SERVICE_NAME_NOT_FOUND;
        }
        LOG_WARN("fail to get next result", KR(ret));
      } else if (OB_FAIL(build_service_name_(*result, service_name))) {
        LOG_WARN("fail to build server status", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!service_name.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select invalid service_name", KR(ret), K(tenant_id), K(service_name_str), K(service_name));
  }
  return ret;
}

int ObServiceNameProxy::insert_service_name(
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str,
    int64_t &epoch,
    ObArray<ObServiceName> &all_service_names)
{
  // insert when so_status is normal
  int ret = OB_SUCCESS;
  int64_t service_name_num = INT64_MAX;
  ObMySQLTransaction trans;
  ObSqlString sql;
  const char * service_status_str = ObServiceName::service_status_to_str(ObServiceName::STARTED);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !service_name_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans_start_and_precheck_(trans, tenant_id, epoch))) {
    LOG_WARN("fail to execute trans_start_and_precheck_", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_service_name_num(trans, tenant_id, service_name_num))) {
    LOG_WARN("fail to get the tenant's service_name_num", KR(ret), K(tenant_id));
  } else if (SERVICE_NAME_MAX_NUM <= service_name_num) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("The number of service_name for the tenant exceeds the limit", KR(ret), K(service_name_num));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The number of service_name for the tenant exceeds the limit, service name related command is");
  } else {
    ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    uint64_t new_service_name_id = OB_INVALID_ID;
    uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        meta_tenant_id,
        OB_MAX_USED_SERVICE_NAME_ID_TYPE,
        new_service_name_id,
        0 /* initial */))) {
      LOG_WARN("fail to fetch new service_name id", KR(ret), K(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
        "(tenant_id, service_name_id, service_name, service_status) value (%lu, %lu, '%s', '%s')",
        OB_ALL_SERVICE_TNAME, tenant_id, new_service_name_id, service_name_str.ptr(), service_status_str))) {
      LOG_WARN("fail to insert service_name", KR(ret), K(tenant_id), K(new_service_name_id),
          K(service_name_str), K(service_status_str));
    }
  }
  (void) write_and_end_trans_(ret, trans, tenant_id, sql, all_service_names);
  return ret;
}

int ObServiceNameProxy::update_service_status(
    const ObServiceName &service_name,
    const ObServiceName::ObServiceStatus &new_status,
    int64_t &epoch,
    ObArray<ObServiceName> &all_service_names)
{
  // update when so_status is normal
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  const char * old_service_status_str = ObServiceName::service_status_to_str(service_name.get_service_status());
  const char * new_service_status_str = ObServiceName::service_status_to_str(new_status);
  const uint64_t service_name_id = service_name.get_service_name_id().id();
  const uint64_t tenant_id = service_name.get_tenant_id();
  if (OB_UNLIKELY(!service_name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(service_name));
  } else if (service_name.get_service_status() == new_status) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the new service status is the same with the old one", KR(ret), K(service_name), K(new_status));
  } else if (OB_FAIL(trans_start_and_precheck_(trans, tenant_id, epoch))) {
    LOG_WARN("fail to execute trans_start_and_precheck_", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET service_status = '%s' "
      "WHERE tenant_id = %lu AND service_name_id = '%lu' AND service_status = '%s'",
      OB_ALL_SERVICE_TNAME, new_service_status_str, tenant_id, service_name_id, old_service_status_str))) {
    LOG_WARN("fail to insert service_name", KR(ret), K(tenant_id), K(service_name), K(new_service_status_str));
  }
  (void) write_and_end_trans_(ret, trans, tenant_id, sql, all_service_names);
  return ret;
}

int ObServiceNameProxy::delete_service_name(const ObServiceName &service_name)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t epoch = 0;
  ObArray<ObServiceName> all_service_names;
  const char * stopped_status_str = ObServiceName::service_status_to_str(ObServiceName::STOPPED);
  const uint64_t tenant_id = service_name.get_tenant_id();
  const uint64_t service_name_id = service_name.get_service_name_id().id();
  if (OB_UNLIKELY(!service_name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid service_name", KR(ret), K(service_name));
  } else if (OB_FAIL(trans_start_and_precheck_(trans, tenant_id, epoch))) {
    LOG_WARN("fail to execute trans_start_and_precheck_", KR(ret), K(tenant_id));
  }  else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND service_name_id = '%lu' "
      "AND service_status = '%s'",
      OB_ALL_SERVICE_TNAME, service_name.get_tenant_id(), service_name_id, stopped_status_str))) {
    LOG_WARN("fail to insert service_name", KR(ret), K(service_name));
  }
  (void) write_and_end_trans_(ret, trans, tenant_id, sql, all_service_names);
  return ret;
}

int ObServiceNameProxy::check_is_service_name_enabled(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  uint64_t tenant_data_version = 0;
  uint64_t meta_tenant_data_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("fail to get the tenant's min data version", KR(ret), K(tenant_id));
  } else if (!((tenant_data_version >= MOCK_DATA_VERSION_4_2_1_9 && tenant_data_version < DATA_VERSION_4_2_2_0)
      || (tenant_data_version >= MOCK_DATA_VERSION_4_2_4_0 && tenant_data_version < DATA_VERSION_4_3_0_0)
      || tenant_data_version >= DATA_VERSION_4_3_3_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant_data_version should be [4.2.1.9, 4.2.2.0) or [4.2.4.0, 4.3.0.0) "
        "or [4.3.3.0, +infinity)", KR(ret), K(tenant_data_version));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(meta_tenant_id, meta_tenant_data_version))) {
    LOG_WARN("fail to get the meta tenant's min data version", KR(ret), K(meta_tenant_id));
  } else if (!((meta_tenant_data_version >= MOCK_DATA_VERSION_4_2_1_9 && meta_tenant_data_version < DATA_VERSION_4_2_2_0)
      || (meta_tenant_data_version >= MOCK_DATA_VERSION_4_2_4_0 && meta_tenant_data_version < DATA_VERSION_4_3_0_0)
      || meta_tenant_data_version >= DATA_VERSION_4_3_3_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("meta_tenant_data_version should be [4.2.1.9, 4.2.2.0) or [4.2.4.0, 4.3.0.0) "
        "or [4.3.3.0, +infinity)", KR(ret), K(meta_tenant_data_version));
  }
  return ret;
}

int ObServiceNameProxy::trans_start_and_precheck_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    int64_t &epoch)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, exec_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true /* for_update */, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant's switchover status is not normal, service name related command is");
  } else if (OB_FAIL(ObServiceEpochProxy::get_service_epoch(trans, tenant_id, ObServiceEpochProxy::SERVICE_NAME_EPOCH, epoch))) {
    LOG_WARN("fail to get service epoch", KR(ret), K(tenant_id));
  } else if (FALSE_IT(epoch += 1)) {
  } else if (OB_FAIL(ObServiceEpochProxy::update_service_epoch(trans, tenant_id,
      ObServiceEpochProxy::SERVICE_NAME_EPOCH, epoch, affected_rows))) {
    LOG_WARN("fail to get service epoch", KR(ret), K(tenant_id));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows), K(tenant_id));
  }
  return ret;
}

void ObServiceNameProxy::write_and_end_trans_(
    int &ret,
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    ObArray<ObServiceName> &all_service_names)
{
  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
    } else if (OB_FAIL(select_all_service_names_(trans, tenant_id, all_service_names))) {
      LOG_WARN("fail to execute select_all_service_names_", KR(ret), K(tenant_id));
    }
  }
  if (OB_UNLIKELY(!trans.is_started())) {
    LOG_WARN("the transaction is not started");
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit the transaction", KR(ret), KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
}

int ObServiceNameProxy::build_service_name_(
    const common::sqlclient::ObMySQLResult &res,
    ObServiceName &service_name)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t service_name_id = ObServiceNameID::INVALID_SERVICE_NAME_ID;
  ObString service_name_str;
  ObString service_status_str;
  service_name.reset();
  EXTRACT_INT_FIELD_MYSQL(res, "service_name_id", service_name_id, uint64_t);
  if (OB_ERR_NULL_VALUE == ret) {
    ret = OB_SUCCESS; // __all_service table is empty, overwrite ret
  } else {
    EXTRACT_VARCHAR_FIELD_MYSQL(res, "service_name", service_name_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(res, "service_status", service_status_str);
    EXTRACT_INT_FIELD_MYSQL(res, "tenant_id", tenant_id, uint64_t);
    if (FAILEDx(service_name.init(tenant_id, service_name_id, service_name_str, service_status_str))) {
      LOG_WARN("fail to init service_name", KR(ret), K(tenant_id), K(service_name_id), K(service_name_str),
          K(service_status_str));
    } else if (OB_UNLIKELY(!service_name.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("build invalid service_name", KR(ret), K(service_name));
    }
  }
  return ret;
}

int ObServiceNameProxy::get_tenant_service_name_num(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    int64_t &service_name_num)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  service_name_num = INT64_MAX;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id = %lu", OB_ALL_SERVICE_TNAME, tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", service_name_num, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to extract count", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
        }
      }
    }
  }
  return ret;
}
}
}