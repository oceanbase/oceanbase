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

#include "share/ob_service_epoch_proxy.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_force_print_log.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{
int ObServiceEpochProxy::init_service_epoch(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const int64_t freeze_service_epoch,
    const int64_t arbitration_service_epoch,
    const int64_t server_zone_op_service_epoch,
    const int64_t heartbeat_service_epoch)
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  // sys/meta tenant initialized freeze_service_epoch
  } else if (is_sys_tenant(tenant_id)) {
    if (OB_FAIL(insert_service_epoch(
        sql_proxy,
        tenant_id,
        FREEZE_SERVICE_EPOCH,
        freeze_service_epoch))) {
      LOG_WARN("fail to init freeze_service_epoch", KR(ret), K(tenant_id), K(freeze_service_epoch));
    } else if (OB_FAIL(ObServiceEpochProxy::insert_service_epoch(
        sql_proxy,
        tenant_id,
        ARBITRATION_SERVICE_EPOCH,
        arbitration_service_epoch))) {
      LOG_WARN("fail to init arb service epoch", KR(ret), K(tenant_id), K(arbitration_service_epoch));
    } else if (OB_FAIL(insert_service_epoch(
        sql_proxy,
        tenant_id,
        SERVER_ZONE_OP_SERVICE_EPOCH,
        server_zone_op_service_epoch))) {
      LOG_WARN("fail to init server_zone_op_service_epoch", KR(ret), K(tenant_id), K(server_zone_op_service_epoch));
    } else if (OB_FAIL(insert_service_epoch(
        sql_proxy,
        tenant_id,
        HEARTBEAT_SERVICE_EPOCH,
        heartbeat_service_epoch))) {
      LOG_WARN("fail to init heartbeat_service_epoch", KR(ret), K(tenant_id), K(heartbeat_service_epoch));
    } else {}
  } else if (is_meta_tenant(tenant_id)) {
    // user tenant initialized freeze_service_epoch
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    if (OB_FAIL(insert_service_epoch(
        sql_proxy,
        user_tenant_id,
        FREEZE_SERVICE_EPOCH,
        freeze_service_epoch))) {
      LOG_WARN("fail to init freeze_service_epoch", KR(ret), K(user_tenant_id), K(freeze_service_epoch));
    } else if (OB_FAIL(insert_service_epoch(
        sql_proxy,
        tenant_id,
        FREEZE_SERVICE_EPOCH,
        freeze_service_epoch))) {
      LOG_WARN("fail to init freeze_service_epoch", KR(ret), K(tenant_id), K(freeze_service_epoch));
    } else if (OB_FAIL(ObServiceEpochProxy::insert_service_epoch(
        sql_proxy,
        user_tenant_id,
        ARBITRATION_SERVICE_EPOCH,
        arbitration_service_epoch))) {
      LOG_WARN("fail to init arb service epoch", KR(ret), K(user_tenant_id), K(arbitration_service_epoch));
    } else if (OB_FAIL(ObServiceEpochProxy::insert_service_epoch(
        sql_proxy,
        tenant_id,
        ARBITRATION_SERVICE_EPOCH,
        arbitration_service_epoch))) {
      LOG_WARN("fail to init arb service epoch", KR(ret), K(user_tenant_id), K(arbitration_service_epoch));
    } else {}
  } else {}
  return ret;
}

int ObServiceEpochProxy::insert_service_epoch(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const char *name,
    const int64_t epoch_value)
{
  int ret = OB_SUCCESS;

  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(name));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        || OB_FAIL(dml.add_pk_column("name", name))
        || OB_FAIL(dml.add_column("value", epoch_value))) {
      LOG_WARN("fail to add column", KR(ret), K(name), K(epoch_value));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_SERVICE_EPOCH_TNAME, sql))) {
      LOG_WARN("fail to splice batch insert update sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    } else {
      LOG_INFO("succ to insert service epoch", K(tenant_id), K(name), K(epoch_value), K(affected_rows));
    }
  }

  return ret;
}

int ObServiceEpochProxy::update_service_epoch(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const char *name,
    const int64_t epoch_value,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(name) || (epoch_value < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(name), K(epoch_value));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET value = %ld WHERE tenant_id = '%lu' AND name = '%s' "
        "AND value < %ld", OB_ALL_SERVICE_EPOCH_TNAME, epoch_value, tenant_id, name, epoch_value))) {
      LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(name), K(epoch_value));
    } else if (OB_FAIL(sql_proxy.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    } else {
      FLOG_INFO("succ to update service epoch", K(tenant_id), K(name), K(epoch_value), K(affected_rows));
    }
  }
  return ret;
}

int ObServiceEpochProxy::get_service_epoch(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const char *name,
    int64_t &epoch_value)
{
  return inner_get_service_epoch_(sql_proxy, tenant_id, false, name, epoch_value);
}

int ObServiceEpochProxy::select_service_epoch_for_update(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const char *name,
    int64_t &epoch_value)
{
  return inner_get_service_epoch_(sql_proxy, tenant_id, true, name, epoch_value);
}

int ObServiceEpochProxy::check_service_epoch(
    common::ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const char *name,
    const int64_t expected_epoch,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  int64_t persistent_epoch = -1;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (expected_epoch < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(expected_epoch));
  } else if (OB_FAIL(ObServiceEpochProxy::get_service_epoch(sql_proxy, tenant_id, name, persistent_epoch))) {
    LOG_WARN("fail to get service_epoch", KR(ret), K(tenant_id));
  } else if (persistent_epoch != expected_epoch) {
    is_match = false;
    LOG_WARN("service_epoch mismatch", K(tenant_id), K(expected_epoch), K(persistent_epoch));
  }
  return ret;
}

int ObServiceEpochProxy::inner_get_service_epoch_(
    ObISQLClient &sql_proxy,
    const int64_t tenant_id,
    const bool is_for_update,
    const char *name,
    int64_t &epoch_value)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(name));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND name = '%s' %s", 
          OB_ALL_SERVICE_EPOCH_TNAME, tenant_id, name, (is_for_update ? "FOR UPDATE" : "")))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(name));
      } else if (OB_FAIL(sql_proxy.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "value", epoch_value, int64_t);
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ret)) {
        //nothing todo
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  return ret;
}

int ObServiceEpochProxy::check_service_epoch_with_trans(
    ObMySQLTransaction &trans,
    const int64_t tenant_id,
    const char *name,
    const int64_t expected_epoch,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  int64_t persistent_epoch = -1;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (expected_epoch < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(expected_epoch));
  } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(trans, tenant_id,
                                            name, persistent_epoch))) {
    LOG_WARN("fail to select service_epoch for update", KR(ret), K(tenant_id));
  } else if (persistent_epoch != expected_epoch) {
    is_match = false;
  }
  return ret;
}

int ObServiceEpochProxy::check_and_update_service_epoch(
    ObMySQLTransaction &trans,
    const int64_t tenant_id,
    const char * const name,
    const int64_t service_epoch)
{
  int ret = OB_SUCCESS;
  int64_t persistent_service_epoch = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || palf::INVALID_PROPOSAL_ID == service_epoch)
      || OB_ISNULL(name)) {
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(service_epoch), KP(name));
  } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(
      trans,
      tenant_id,
      name,
      persistent_service_epoch ))) {
    // check and update service epoch
    LOG_WARN("fail to get heartbeat service epoch from inner table", KR(ret), K(tenant_id), K(name));
  } else if (OB_UNLIKELY(service_epoch < persistent_service_epoch)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("the service_epoch is smaller than the service epoch in __all_service_epoch table, "
        "the service cannot be provided", KR(ret), K(tenant_id), K(name),
        K(service_epoch), K(persistent_service_epoch));
  } else if (service_epoch > persistent_service_epoch) {
    int64_t affected_rows = 0;
    if (OB_FAIL(ObServiceEpochProxy::update_service_epoch(
        trans,
        tenant_id,
        name,
        service_epoch,
        affected_rows))) {
      LOG_WARN("fail to update the service epoch", KR(ret), K(tenant_id), K(name),
          K(service_epoch), K(persistent_service_epoch), K(affected_rows));
    } else if (1 != affected_rows) {
      ret = OB_NEED_RETRY;
      LOG_WARN("fail to update service epoch, affected_rows is expected to be one", KR(ret),
          K(tenant_id), K(name), K(service_epoch), K(persistent_service_epoch), K(affected_rows));
    }
  } else {}
  FLOG_INFO("check and update service epoch", KR(ret), K(tenant_id), K(name),
      K(service_epoch), K(persistent_service_epoch));
  return ret;
}

} // end namespace share
} // end namespace oceanbase
