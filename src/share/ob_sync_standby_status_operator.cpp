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

#include "share/ob_sync_standby_status_operator.h"
#include "share/ob_tenant_info_proxy.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection.h"
namespace oceanbase
{
namespace share
{
OB_SERIALIZE_MEMBER(ObSyncStandbyStatusAttr, cluster_id_, tenant_id_, protection_stat_);
ObSyncStandbyStatusAttr::ObSyncStandbyStatusAttr()
    : cluster_id_(OB_INVALID_CLUSTER_ID), tenant_id_(OB_INVALID_TENANT_ID),
      protection_stat_() {}

ObSyncStandbyStatusAttr::~ObSyncStandbyStatusAttr()
{
}

void ObSyncStandbyStatusAttr::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  protection_stat_.reset();
}

int ObSyncStandbyStatusAttr::init(const uint64_t cluster_id, const uint64_t tenant_id,
                                  const ObProtectionStat &protection_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!protection_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cluster_id), K(tenant_id), K(protection_stat));
  } else {
    cluster_id_ = cluster_id;
    tenant_id_ = tenant_id;
    protection_stat_ = protection_stat;
  }
  return ret;
}

bool ObSyncStandbyStatusAttr::is_valid() const
{
  bool bret = true;
  if (!protection_stat_.is_valid()) {
    bret = false;
  } else if (cluster_id_ == OB_INVALID_CLUSTER_ID || !is_valid_tenant_id(tenant_id_)) {
    if (protection_stat_.get_protection_mode().is_sync_mode()) {
      bret = false;
    }
  }
  return bret;
}

int ObSyncStandbyStatusOperator::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id_) || !is_user_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObSyncStandbyStatusOperator::read_sync_standby_status(
  ObISQLClient &sql_client,
  const bool for_update,
  ObProtectionStat &protection_stat)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
    "SELECT * FROM %s where 1=1 %s",
      OB_ALL_SYNC_STANDBY_STATUS_TNAME, (for_update ? "FOR UPDATE" : "")))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id_), K(for_update));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id_));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next", KR(ret));
      } else if (OB_FAIL(protection_stat.init(result))) {
        LOG_WARN("failed to init protection stat", KR(ret));
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("too many rows in __all_sync_standby_status", KR(ret), KR(tmp_ret),
          K(tenant_id_), K(sql));
      }
    }
  }
  return ret;
}

int ObSyncStandbyStatusOperator::update_sync_standby_status_in_trans(
  common::ObMySQLTransaction &trans,
  const ObProtectionStat &previous_protection_stat,
  const ObSyncStandbyStatusAttr &attr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObProtectionStat protection_stat;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
    "UPDATE %s SET protection_mode = '%s', protection_level = '%s', switchover_epoch = %ld "
    "WHERE switchover_epoch = %ld",
      OB_ALL_SYNC_STANDBY_STATUS_TNAME, attr.get_protection_stat().get_protection_mode().to_str(),
      attr.get_protection_stat().get_protection_level().to_str(),
      attr.get_protection_stat().get_switchover_epoch(),
      previous_protection_stat.get_switchover_epoch()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id_), K(attr));
  } else if (OB_FAIL(trans.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id_));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  } else if (OB_FAIL(write_sync_standby_status_log_(trans, attr))) {
    LOG_WARN("failed to write sync standby status log", KR(ret), K(attr));
  }
  return ret;
}

int ObSyncStandbyStatusOperator::update_sync_standby_status(
  const ObProtectionStat &previous_protection_stat,
  const ObSyncStandbyStatusAttr &attr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(update_sync_standby_status_in_trans(trans, previous_protection_stat, attr))) {
    LOG_WARN("failed to update sync standby status", KR(ret), K(previous_protection_stat), K(attr));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit transaction", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObSyncStandbyStatusOperator::write_sync_standby_status_log_(
  common::ObMySQLTransaction &trans,
  const ObSyncStandbyStatusAttr &attr)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  if (OB_UNLIKELY(!attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(attr));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection or trans service is null", KR(ret), KP(inner_conn));
  } else {
    ObArenaAllocator allocator("SyncStandbyStat");
    const int64_t length = attr.get_serialize_size();
    char *buf = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret), K(length));
    } else if (OB_FAIL(attr.serialize(buf, length, pos))) {
      LOG_WARN("failed to serialize", KR(ret), K(attr), K(length), K(pos));
    } else if (OB_UNLIKELY(pos > length)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialize error", KR(ret), K(pos), K(length), K(attr));
    } else if (OB_FAIL(inner_conn->register_multi_data_source(
      tenant_id_, SYS_LS, transaction::ObTxDataSourceType::SYNC_STANDBY_STATUS, buf, length))) {
      LOG_WARN("failed to register tx data", KR(ret), K(tenant_id_));
    }
    LOG_INFO("write sync standby status log finished", KR(ret), K(attr));
  }
  return ret;
}

int ObSyncStandbyStatusOperator::init_sync_standby_status(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSyncStandbyStatusAttr attr;
  ObProtectionStat protection_stat;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(protection_stat.init(ObProtectionMode(ObProtectionMode::MAXIMUM_PERFORMANCE_MODE),
     ObProtectionLevel(ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL),
      ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH))) {
    LOG_WARN("failed to init protection stat", KR(ret));
  } else if (OB_FAIL(attr.init(OB_INVALID_CLUSTER_ID, OB_INVALID_TENANT_ID, protection_stat))) {
    LOG_WARN("failed to init attr", KR(ret), K(protection_stat));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", OB_INVALID_TENANT_ID))) {
    LOG_WARN("failed to add pk column tenant_id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(dml.add_column("protection_mode", protection_stat.get_protection_mode().to_str()))) {
    LOG_WARN("failed to add column protection_mode", KR(ret), K(protection_stat));
  } else if (OB_FAIL(dml.add_column("protection_level", protection_stat.get_protection_level().to_str()))) {
    LOG_WARN("failed to add column protection_level", KR(ret), K(protection_stat));
  } else if (OB_FAIL(dml.add_column("switchover_epoch", protection_stat.get_switchover_epoch()))) {
    LOG_WARN("failed to add column switchover_epoch", KR(ret), K(protection_stat));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_SYNC_STANDBY_STATUS_TNAME, sql))) {
    LOG_WARN("failed to splice insert sql", KR(ret), K(sql));
  } else if (OB_FAIL(trans.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id_), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(tenant_id_), K(sql));
  } else if (OB_FAIL(write_sync_standby_status_log_(trans, attr))) {
    LOG_WARN("failed to write sync standby status log", KR(ret), K(attr));
  }
  return ret;
}
} // namespace share
} // namespace oceanbase