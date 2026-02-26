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
#include "ob_tenant_snapshot_table_operator.h"
#include "lib/string/ob_hex_utils_base.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObCreateSnapshotJob::init(const int64_t create_active_ts,
                              const int64_t create_expire_ts,
                              const int64_t paxos_replica_num,
                              const ObArbitrationServiceStatus &arbitration_service_status,
                              const ObTenantSnapJobItem& job_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == create_active_ts
                  || OB_INVALID_TIMESTAMP == create_expire_ts
                  || OB_INVALID_COUNT == paxos_replica_num
                  || !arbitration_service_status.is_valid()
                  || !job_item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_active_ts),
                                 K(create_expire_ts), K(paxos_replica_num),
                                 K(arbitration_service_status), K(job_item));
  } else if (OB_FAIL(job_item_.assign(job_item))) {
    LOG_WARN("fail to assign create job item", KR(ret), K(job_item));
  } else {
    create_active_ts_ = create_active_ts;
    create_expire_ts_ = create_expire_ts;
    paxos_replica_num_ = paxos_replica_num;
    arbitration_service_status_ = arbitration_service_status;
  }
  return ret;
}

void ObCreateSnapshotJob::reset()
{
  job_item_.reset();
  create_active_ts_ = 0;
  create_expire_ts_ = INT64_MAX;
  paxos_replica_num_ = 0;
  arbitration_service_status_.reset();
}

int ObCreateSnapshotJob::assign(const ObCreateSnapshotJob& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign to itself", KR(ret), K(other));
  } else if (OB_FAIL(job_item_.assign(other.job_item_))) {
    LOG_WARN("fail to assign job", KR(ret), K(other));
  } else {
    create_active_ts_ = other.create_active_ts_;
    create_expire_ts_ = other.create_expire_ts_;
    paxos_replica_num_ = other.paxos_replica_num_;
    arbitration_service_status_ = other.arbitration_service_status_;
  }

  return ret;
}

int ObDeleteSnapshotJob::init(const ObTenantSnapJobItem& job_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job_item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_item));
  } else if (OB_FAIL(job_item_.assign(job_item))) {
    LOG_WARN("fail to assign job", KR(ret), K(job_item));
  }

  return ret;
}

void ObDeleteSnapshotJob::reset()
{
  job_item_.reset();
}

int ObDeleteSnapshotJob::assign(const ObDeleteSnapshotJob& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign to itself", KR(ret), K(other));
  } else if (OB_FAIL(job_item_.assign(other.job_item_))) {
    LOG_WARN("fail to assign job", KR(ret), K(other));
  }

  return ret;
}

ObTenantSnapItem::ObTenantSnapItem() :
  tenant_id_(OB_INVALID_TENANT_ID),
  tenant_snapshot_id_(),
  status_(ObTenantSnapStatus::MAX),
  snapshot_scn_(),
  clog_start_scn_(),
  type_(ObTenantSnapType::MAX),
  create_time_(OB_INVALID_TIMESTAMP),
  data_version_(0),
  owner_job_id_(OB_INVALID_ID)
{
  snapshot_name_[0] = '\0';
}

int ObTenantSnapItem::init(const uint64_t tenant_id,
                           const ObTenantSnapshotID &snapshot_id,
                           const ObString &snapshot_name,
                           const ObTenantSnapStatus &status,
                           const SCN &snapshot_scn,
                           const SCN &clog_start_scn,
                           const ObTenantSnapType &type,
                           const int64_t create_time,
                           const uint64_t data_version,
                           const int64_t owner_job_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !snapshot_id.is_valid()
                  || snapshot_name.empty()
                  || ObTenantSnapStatus::MAX == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_id), K(snapshot_name), K(status));
  } else if (OB_FAIL(databuff_printf(snapshot_name_, sizeof(snapshot_name_), pos, "%.*s",
                          snapshot_name.length(), snapshot_name.ptr()))) {
    LOG_WARN("snapshot_name assign failed", K(snapshot_name));
  } else {
    tenant_id_ = tenant_id;
    tenant_snapshot_id_ = snapshot_id;
    status_ = status;
    snapshot_scn_ = snapshot_scn;
    clog_start_scn_ = clog_start_scn;
    type_ = type;
    create_time_ = create_time;
    data_version_ = data_version;
    owner_job_id_ = owner_job_id;
  }
  return ret;
}


int ObTenantSnapItem::assign(const ObTenantSnapItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(snapshot_name_, sizeof(snapshot_name_), "%s", other.snapshot_name_))) {
    LOG_WARN("snapshot_name assign failed", K(other.snapshot_name_));
  } else {
    tenant_id_ = other.tenant_id_;
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
    status_ = other.status_;
    snapshot_scn_ = other.snapshot_scn_;
    clog_start_scn_ = other.clog_start_scn_;
    type_ = other.type_;
    create_time_ = other.create_time_;
    data_version_ = other.data_version_;
    owner_job_id_ = other.owner_job_id_;
  }
  return ret;
}

void ObTenantSnapItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_snapshot_id_.reset();
  snapshot_name_[0] = '\0';
  status_ = ObTenantSnapStatus::MAX;
  snapshot_scn_.reset();
  clog_start_scn_.reset();
  type_ = ObTenantSnapType::MAX;
  create_time_ = OB_INVALID_TIMESTAMP;
  data_version_ = 0;
  owner_job_id_ = OB_INVALID_ID;
}

int ObTenantSnapLSItem::init(const uint64_t tenant_id,
                             const ObTenantSnapshotID &tenant_snapshot_id,
                             const ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_snapshot_id.is_valid()
                  || !ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_snapshot_id), K(ls_attr));
  } else if (OB_FAIL(ls_attr_.assign(ls_attr))) {
    LOG_WARN("assign failed", KR(ret), K(ls_attr));
  } else {
    tenant_id_ = tenant_id;
    tenant_snapshot_id_ = tenant_snapshot_id;
  }
  return ret;
}

int ObTenantSnapLSItem::assign(const ObTenantSnapLSItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_attr_.assign(other.ls_attr_))) {
    LOG_WARN("assign failed", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
  }
  return ret;
}

void ObTenantSnapLSItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_snapshot_id_.reset();
  ls_attr_.reset();
}

ObTenantSnapLSReplicaSimpleItem::ObTenantSnapLSReplicaSimpleItem() :
  tenant_id_(OB_INVALID_TENANT_ID),
  tenant_snapshot_id_(),
  ls_id_(),
  addr_(),
  status_(ObLSSnapStatus::MAX),
  zone_(),
  unit_id_(OB_INVALID_ID),
  begin_interval_scn_(),
  end_interval_scn_()
{
}

int ObTenantSnapLSReplicaSimpleItem::init(const uint64_t tenant_id,
                                          const ObTenantSnapshotID &tenant_snapshot_id,
                                          const ObLSID &ls_id,
                                          const common::ObAddr &addr,
                                          const ObLSSnapStatus &status,
                                          const ObZone &zone,
                                          const uint64_t unit_id,
                                          const SCN &begin_interval_scn,
                                          const SCN &end_interval_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_snapshot_id.is_valid()
                  || !ls_id.is_valid())
                  || !addr.is_valid()
                  || ObLSSnapStatus::MAX == status
                  || zone.is_empty()
                  || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_snapshot_id), K(ls_id),
                                      K(addr), K(status), K(zone), K(unit_id));
  } else if (OB_FAIL(zone_.assign(zone))) {
    LOG_WARN("zone assign failed", K(zone));
  } else {
    tenant_id_ = tenant_id;
    tenant_snapshot_id_ = tenant_snapshot_id;
    ls_id_ = ls_id;
    addr_ = addr;
    status_ = status;
    unit_id_ = unit_id;
    begin_interval_scn_ = begin_interval_scn;
    end_interval_scn_ = end_interval_scn;
  }
  return ret;
}

int ObTenantSnapLSReplicaSimpleItem::assign(const ObTenantSnapLSReplicaSimpleItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("zone assign failed", K(other.zone_));
  } else {
    tenant_id_ = other.tenant_id_;
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
    ls_id_ = other.ls_id_;
    addr_ = other.addr_;
    status_ = other.status_;
    unit_id_ = other.unit_id_;
    begin_interval_scn_ = other.begin_interval_scn_;
    end_interval_scn_ = other.end_interval_scn_;
  }
  return ret;
}

void ObTenantSnapLSReplicaSimpleItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_snapshot_id_.reset();
  ls_id_.reset();
  addr_.reset();
  status_ = ObLSSnapStatus::MAX;
  zone_.reset();
  unit_id_ = OB_INVALID_ID;
  begin_interval_scn_.reset();
  end_interval_scn_.reset();
}

int ObTenantSnapLSReplicaItem::assign(const ObTenantSnapLSReplicaItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(s_.assign(other.s_))) {
    LOG_WARN("simple item assign failed", K(other.s_));
  } else {
    ls_meta_package_ = other.ls_meta_package_;
  }
  return ret;
}

int ObTenantSnapJobItem::assign(const ObTenantSnapJobItem &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign to itself", KR(ret), K(other));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
    operation_ = other.operation_;
    trace_id_ = other.trace_id_;
    majority_succ_time_ = other.majority_succ_time_;
  }
  return ret;
}

bool ObTenantSnapJobItem::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID &&
         tenant_snapshot_id_.is_valid() &&
         operation_ != ObTenantSnapOperation::MAX &&
         trace_id_.is_valid();
}

void ObTenantSnapJobItem::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_snapshot_id_.reset();
  operation_ = ObTenantSnapOperation::MAX;
  trace_id_.reset();
  majority_succ_time_ = OB_INVALID_TIMESTAMP;
}

const char* ObTenantSnapshotTableOperator::TENANT_SNAP_STATUS_ARRAY[] =
{
  "CREATING",
  "DECIDED",
  "NORMAL",
  "CLONING",
  "DELETING",
  "FAILED",
};

const char* ObTenantSnapshotTableOperator::LS_SNAP_STATUS_ARRAY[] =
{
  "CREATING",
  "NORMAL",
  "CLONING",
  "FAILED",
};

const char* ObTenantSnapshotTableOperator::TENANT_SNAP_TYPE_ARRAY[] =
{
  "AUTO",
  "MANUAL",
};

const char* ObTenantSnapshotTableOperator::TENANT_SNAP_OPERATION_ARRAY[] =
{
  "CREATE",
  "DELETE",
};

ObTenantSnapStatus ObTenantSnapshotTableOperator::str_to_tenant_snap_status(const ObString &status_str)
{
  ObTenantSnapStatus ret_status = ObTenantSnapStatus::MAX;
  if (status_str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_SNAP_STATUS_ARRAY); i++) {
      if (0 == status_str.case_compare(TENANT_SNAP_STATUS_ARRAY[i])) {
        ret_status = static_cast<ObTenantSnapStatus>(i);
        break;
      }
    }
  }
  return ret_status;
}

const char* ObTenantSnapshotTableOperator::tenant_snap_status_to_str(const ObTenantSnapStatus &status)
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_SNAP_STATUS_ARRAY) == static_cast<int64_t>(ObTenantSnapStatus::MAX),
                "type string array size mismatch with enum tenant snapshot status count");
  const char* str = INVALID_STR;
  if (status >= ObTenantSnapStatus::CREATING && status < ObTenantSnapStatus::MAX) {
    str = TENANT_SNAP_STATUS_ARRAY[static_cast<int64_t>(status)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid tenant snapshot status", K(status));
  }
  return str;
}

ObLSSnapStatus ObTenantSnapshotTableOperator::str_to_ls_snap_status(const ObString &status_str)
{
  ObLSSnapStatus ret_status = ObLSSnapStatus::MAX;
  if (!status_str.empty()) {
    for (int64_t i = 0; i < ARRAYSIZEOF(LS_SNAP_STATUS_ARRAY); i++) {
      if (0 == status_str.case_compare(LS_SNAP_STATUS_ARRAY[i])) {
        ret_status = static_cast<ObLSSnapStatus>(i);
        break;
      }
    }
  }
  return ret_status;
}

const char* ObTenantSnapshotTableOperator::ls_snap_status_to_str(const ObLSSnapStatus &status)
{
  STATIC_ASSERT(ARRAYSIZEOF(LS_SNAP_STATUS_ARRAY) == static_cast<int64_t>(ObLSSnapStatus::MAX),
                "type string array size mismatch with enum ls snapshot status count");
  const char* str = INVALID_STR;
  if (status >= ObLSSnapStatus::CREATING && status < ObLSSnapStatus::MAX) {
    str = LS_SNAP_STATUS_ARRAY[static_cast<int64_t>(status)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ls snapshot status", K(status));
  }
  return str;
}

ObTenantSnapType ObTenantSnapshotTableOperator::str_to_type(const ObString &type_str)
{
  ObTenantSnapType ret_type = ObTenantSnapType::MAX;
  if (type_str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_SNAP_TYPE_ARRAY); i++) {
      if (0 == type_str.case_compare(TENANT_SNAP_TYPE_ARRAY[i])) {
        ret_type = static_cast<ObTenantSnapType>(i);
        break;
      }
    }
  }
  return ret_type;
}

const char* ObTenantSnapshotTableOperator::type_to_str(const ObTenantSnapType &type)
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_SNAP_TYPE_ARRAY) == static_cast<int64_t>(ObTenantSnapType::MAX),
                "type string array size mismatch with enum tenant snapshot type count");
  const char* str = INVALID_STR;
  if (type >= ObTenantSnapType::AUTO && type < ObTenantSnapType::MAX) {
    str = TENANT_SNAP_TYPE_ARRAY[static_cast<int64_t>(type)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid tenant snapshot type", K(type));
  }
  return str;
}

ObTenantSnapOperation ObTenantSnapshotTableOperator::str_to_operation(const ObString &str)
{
  ObTenantSnapOperation ret_type = ObTenantSnapOperation::MAX;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_SNAP_OPERATION_ARRAY); i++) {
      if (0 == str.case_compare(TENANT_SNAP_OPERATION_ARRAY[i])) {
        ret_type = static_cast<ObTenantSnapOperation>(i);
        break;
      }
    }
  }
  return ret_type;
}

const char* ObTenantSnapshotTableOperator::operation_to_str(const ObTenantSnapOperation &operation)
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_SNAP_OPERATION_ARRAY) == static_cast<int64_t>(ObTenantSnapOperation::MAX),
                "operation string array size mismatch with enum tenant snapshot operation count");
  const char* str = INVALID_STR;
  if (operation >= ObTenantSnapOperation::CREATE && operation < ObTenantSnapOperation::MAX) {
    str = TENANT_SNAP_OPERATION_ARRAY[static_cast<int64_t>(operation)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid tenant snapshot operation", K(operation));
  }
  return str;
}

ObTenantSnapshotTableOperator::ObTenantSnapshotTableOperator() :
  is_inited_(false),
  user_tenant_id_(OB_INVALID_TENANT_ID),
  proxy_(NULL)
{}

int ObTenantSnapshotTableOperator::init(const uint64_t user_tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("snapshot table operator init twice", KR(ret));
  } else if (!is_user_tenant(user_tenant_id) || OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(proxy));
  } else {
    user_tenant_id_ = user_tenant_id;
    proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantSnapshotTableOperator::insert_tenant_snap_item(const ObTenantSnapItem& item)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.get_tenant_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", item.get_tenant_snapshot_id().id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("snapshot_name", item.get_snapshot_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("status", tenant_snap_status_to_str(item.get_status())))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_uint64_column("snapshot_scn", item.get_snapshot_scn().get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_uint64_column("clog_start_scn", item.get_clog_start_scn().get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("type", type_to_str(item.get_type())))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_time_column("create_time", item.get_create_time()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_uint64_column("data_version", item.get_data_version()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("owner_job_id", item.get_owner_job_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice insert sql failed", KR(ret), K(item));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret || OB_ERR_DUPLICATED_UNIQUE_KEY == ret) {
      ret = OB_TENANT_SNAPSHOT_EXIST;
    }
    LOG_WARN("exec sql failed", KR(ret), K(item), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}


int ObTenantSnapshotTableOperator::get_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                        const bool need_lock,
                                                        ObTenantSnapItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld",
                        OB_ALL_TENANT_SNAPSHOT_TNAME, user_tenant_id_, tenant_snapshot_id.id()))) {
        LOG_WARN("assign sql failed", KR(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" FOR UPDATE "))) {  /*lock row*/
        LOG_WARN("assign sql failed", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_SNAPSHOT_NOT_EXIST;
        } else {
          LOG_WARN("next failed", KR(ret));
        }
      } else if (OB_FAIL(fill_item_from_result_(result, item))) {
        LOG_WARN("fill item from result failed", KR(ret));
      }
    }
  }
  return ret;
}

//for sys tenant to call
int ObTenantSnapshotTableOperator::get_tenant_snap_item(const ObString &snapshot_name,
                                                        const bool need_lock,
                                                        ObTenantSnapItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_name));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND snapshot_name = '%.*s'",
                                  OB_ALL_TENANT_SNAPSHOT_TNAME, user_tenant_id_,
                                  snapshot_name.length(), snapshot_name.ptr()))) {
        LOG_WARN("assign sql failed", KR(ret));
      } else if (need_lock && OB_FAIL(sql.append_fmt(" FOR UPDATE "))) {  /*lock row*/
        LOG_WARN("assign sql failed", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_SNAPSHOT_NOT_EXIST;
        } else {
          LOG_WARN("next failed", KR(ret));
        }
      } else if (OB_FAIL(fill_item_from_result_(result, item))) {
        LOG_WARN("fill item from result failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_all_user_tenant_snap_items(ObArray<ObTenantSnapItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND snapshot_id != %ld",
                                 OB_ALL_TENANT_SNAPSHOT_TNAME, user_tenant_id_, GLOBAL_STATE_ID))) {
        LOG_WARN("assign sql failed", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        ObTenantSnapItem item;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          item.reset();
          if (OB_FAIL(fill_item_from_result_(result, item))) {
            LOG_WARN("fill item from result failed", KR(ret));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("push back item failed", KR(ret), K(item));
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next result", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

//Note: if new_snapshot_scn is invalid, we will not update column "snapshot_scn"
//      if new_create_time is invalid(OB_INVALID_TIMESTAMP), we will not update column "create_time"
int ObTenantSnapshotTableOperator::update_special_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                   const ObTenantSnapStatus &old_status,
                                                                   const ObTenantSnapStatus &new_status,
                                                                   const int64_t owner_job_id,
                                                                   const SCN &new_snapshot_scn,
                                                                   const int64_t new_create_time)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(ObTenantSnapshotID(GLOBAL_STATE_ID) != tenant_snapshot_id
                         || ObTenantSnapStatus::MAX == old_status
                         || ObTenantSnapStatus::MAX == new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(old_status), K(new_status));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("status", tenant_snap_status_to_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (new_snapshot_scn.is_valid() &&
             OB_FAIL(dml.add_uint64_column("snapshot_scn", new_snapshot_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (new_create_time != OB_INVALID_TIMESTAMP &&
             OB_FAIL(dml.add_time_column("create_time", new_create_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("owner_job_id", owner_job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%s'",
                            "status", tenant_snap_status_to_str(old_status)))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

//if clog_start_scn is not valid, we don't update the column
//if snapshot_scn is not valid, we don't update the column
int ObTenantSnapshotTableOperator::update_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                           const ObTenantSnapStatus &old_status,
                                                           const ObTenantSnapStatus &new_status,
                                                           const SCN &new_snapshot_scn,
                                                           const SCN &new_clog_start_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || ObTenantSnapStatus::MAX == old_status
                         || ObTenantSnapStatus::MAX == new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(old_status), K(new_status));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_column("status", tenant_snap_status_to_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret), K(new_status));
  } else if (new_snapshot_scn.is_valid() &&
             OB_FAIL(dml.add_uint64_column("snapshot_scn", new_snapshot_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (new_clog_start_scn.is_valid() &&
             OB_FAIL(dml.add_uint64_column("clog_start_scn", new_clog_start_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%s'",
                            "status", tenant_snap_status_to_str(old_status)))) {
    LOG_WARN("add extra_condition failed", KR(ret), K(old_status));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret), K(tenant_snapshot_id), K(old_status), K(new_status));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::update_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                           const ObTenantSnapStatus &old_status_1,
                                                           const ObTenantSnapStatus &old_status_2,
                                                           const ObTenantSnapStatus &new_status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || ObTenantSnapStatus::MAX == old_status_1
                         || ObTenantSnapStatus::MAX == old_status_2
                         || ObTenantSnapStatus::MAX == new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        KR(ret), K(tenant_snapshot_id), K(old_status_1), K(old_status_2), K(new_status));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_column("status", tenant_snap_status_to_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret), K(new_status));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("(status = '%s' OR status = '%s')",
                            tenant_snap_status_to_str(old_status_1),
                            tenant_snap_status_to_str(old_status_2)))) {
    LOG_WARN("add extra_condition failed", KR(ret), K(old_status_1), K(old_status_2));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice update sql failed",
        KR(ret), K(tenant_snapshot_id), K(old_status_1), K(old_status_2), K(new_status));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::update_tenant_snap_item(const ObString &snapshot_name,
                                                           const ObTenantSnapStatus &old_status,
                                                           const ObTenantSnapStatus &new_status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(snapshot_name.empty()
                         || ObTenantSnapStatus::MAX == old_status
                         || ObTenantSnapStatus::MAX == new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_name), K(old_status), K(new_status));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("status", tenant_snap_status_to_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%.*s'",
                          "snapshot_name", snapshot_name.length(), snapshot_name.ptr()))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().append_fmt(" AND %s = '%s'",
                            "status", tenant_snap_status_to_str(old_status)))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(user_tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::delete_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_TENANT_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("splice delete sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  } else if (!is_zero_row(affected_rows)
             && !is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::insert_tenant_snap_ls_items(const ObArray<ObTenantSnapLSItem> &items)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObLSFlagStr flag_str;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      flag_str.reset();
      const ObTenantSnapLSItem &item = items.at(i);
      if (OB_UNLIKELY(!item.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(item));
      } else if (OB_FAIL(item.get_ls_attr().get_ls_flag().flag_to_str(flag_str))) {
        LOG_WARN("fail to convert flag to string", KR(ret), K(item));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", item.get_tenant_id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("snapshot_id", item.get_tenant_snapshot_id().id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("ls_id", item.get_ls_attr().get_ls_id().id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("ls_group_id", item.get_ls_attr().get_ls_group_id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("status", ls_status_to_str(item.get_ls_attr().get_ls_status())))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("flag", flag_str.ptr()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_uint64_column("create_scn",
                          item.get_ls_attr().get_create_scn().get_val_for_inner_table_field()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(user_tenant_id_));
      }
    }
  }

  if (FAILEDx(dml.splice_batch_insert_sql(OB_ALL_TENANT_SNAPSHOT_LS_TNAME, sql))) {
    LOG_WARN("fail to generate sql", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  } else if (affected_rows != items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows not match", KR(ret), K(user_tenant_id_), K(affected_rows), K(items.count()));
  }

  return ret;
}

int ObTenantSnapshotTableOperator::delete_tenant_snap_ls_items(const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("fail to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("fail to add column", KR(ret));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_TENANT_SNAPSHOT_LS_TNAME, sql))) {
    LOG_WARN("failed to splice delete sql", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_ls_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                                            ObArray<ObTenantSnapLSItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  items.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld",
                        OB_ALL_TENANT_SNAPSHOT_LS_TNAME, user_tenant_id_, tenant_snapshot_id.id()))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObTenantSnapLSItem item;
          if (OB_FAIL(fill_item_from_result_(result, item))) {
            LOG_WARN("fill tenant snapshot ls item failed", KR(ret));
          } else if (OB_FAIL(items.push_back(item))) {
            LOG_WARN("array push back failed", KR(ret), K(item));
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next result", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_ls_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                           const ObLSID &ls_id,
                                                           ObTenantSnapLSItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld AND ls_id = %ld",
                        OB_ALL_TENANT_SNAPSHOT_LS_TNAME, user_tenant_id_, tenant_snapshot_id.id(), ls_id.id()))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("next failed", KR(ret));
        }
      } else if (OB_FAIL(fill_item_from_result_(result, item))) {
        LOG_WARN("fill item from result failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_related_addrs(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                 ObArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  addrs.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT svr_ip, svr_port FROM %s "
                                 "WHERE tenant_id = %lu AND snapshot_id = %ld",
                                 OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                 user_tenant_id_, tenant_snapshot_id.id()))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObAddr addr;
          ObString svr_ip;
          int32_t svr_port = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int32_t);
          if (OB_SUCC(ret)) {
            if (!(addr.set_ip_addr(svr_ip, svr_port))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("set addr failed", KR(ret), K(svr_ip), K(svr_port));
            } else if (OB_FAIL(addrs.push_back(addr))) {
              LOG_WARN("array push back failed", KR(ret), K(addr));
            }
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next result", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_proper_ls_meta_package(const ObTenantSnapshotID &tenant_snapshot_id,
                                                              const ObLSID &ls_id,
                                                              ObLSMetaPackage& ls_meta_package)
{
  int ret = OB_SUCCESS;
  ls_meta_package.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %ld "
                                 "AND snapshot_id = %ld AND ls_id = %ld AND status = 'NORMAL'"
                                 "ORDER BY begin_interval_scn, svr_ip, svr_port limit 1",
                                 OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                 user_tenant_id_, tenant_snapshot_id.id(), ls_id.id()))) {
        LOG_WARN("fail to assign get_proper_ls_meta_package sql",
            KR(ret), K(user_tenant_id_), K(tenant_snapshot_id), K(ls_id));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("next failed", KR(ret));
        }
      } else {
        ObString ls_meta_package_str;
        ObArenaAllocator allocator;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ls_meta_package", ls_meta_package_str);
        if (FAILEDx(decode_hex_string_to_package_(ls_meta_package_str, allocator, ls_meta_package))) {
          LOG_WARN("fail to decode", KR(ret), K(ls_meta_package_str));
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_ls_meta_package(const ObTenantSnapshotID &tenant_snapshot_id,
                                                       const ObLSID &ls_id,
                                                       const ObAddr& addr,
                                                       ObLSMetaPackage& ls_meta_package)
{
  int ret = OB_SUCCESS;
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};

  ls_meta_package.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator is not init", KR(ret));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant snapshot id is not valid", KR(ret), K(tenant_snapshot_id), K(ls_id), K(addr));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is not valid", KR(ret), K(tenant_snapshot_id), K(ls_id), K(addr));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("addr is not valid", KR(ret), K(tenant_snapshot_id), K(ls_id), K(addr));
  } else if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip to string failed", KR(ret), K(addr));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT ls_meta_package "
                                 "FROM %s WHERE tenant_id = %ld AND snapshot_id = %ld AND ls_id = %ld "
                                 "AND svr_ip = '%s' AND svr_port = %ld",
                                 OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                 user_tenant_id_, tenant_snapshot_id.id(), ls_id.id(),
                                 ip_buf, (int64_t)addr.get_port()))) {
        LOG_WARN("fail to assign get_ls_meta_package sql",
            KR(ret), K(user_tenant_id_), K(tenant_snapshot_id), K(ls_id), K(ip_buf), K(addr));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("fail to execute get_ls_meta_package sql",
            KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is unexpected nullptr",
            KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to exec result next",
              KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
        }
      } else {
        ObString ls_meta_package_str;
        ObArenaAllocator allocator;
        char *unhex_buf = NULL;
        int64_t unhex_buf_len = 0;
        int64_t pos = 0;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "ls_meta_package", ls_meta_package_str);
        if (FAILEDx(decode_hex_string_to_package_(ls_meta_package_str, allocator, ls_meta_package))) {
          LOG_WARN("fail to decode_hex_string_to_package_",
              KR(ret), K(ls_meta_package_str), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
        }
      }
    }
  }
  return ret;
}

//get all simple_items by tenant_snapshot_id
int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                           ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  simple_items.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, status, "
                                    "zone, unit_id, begin_interval_scn, end_interval_scn "
                                    "FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_, tenant_snapshot_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(sql, simple_items))) {
    LOG_WARN("failed to get snap ls replica simple items", KR(ret), K(sql));
  }
  return ret;
}

//get all simple_items by tenant_snapshot_id and addr
int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                           const common::ObAddr& addr,
                                                                           ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  simple_items.reset();
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(addr));
  } else if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip to string failed", KR(ret), K(addr));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, status, "
                                    "zone, unit_id, begin_interval_scn, end_interval_scn "
                                    "FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld "
                                    "AND svr_ip = '%s' AND svr_port = %d",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_, tenant_snapshot_id.id(),
                                    ip_buf, addr.get_port()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(sql, simple_items))) {
    LOG_WARN("failed to get snap ls replica simple items", KR(ret), K(sql));
  }
  return ret;
}

//get all simple_items by tenant_snapshot_id and ls_id
int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                           const ObLSID &ls_id,
                                                                           ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  simple_items.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, status, "
                                    "zone, unit_id, begin_interval_scn, end_interval_scn "
                                    "FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld and ls_id = %ld",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_, tenant_snapshot_id.id(), ls_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(sql, simple_items))) {
    LOG_WARN("failed to get snap ls replica simple items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                           const ObLSID &ls_id,
                                                                           const ObLSSnapStatus &status,
                                                                           ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  simple_items.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !ls_id.is_valid()
                         || ObLSSnapStatus::MAX == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(ls_id), K(status));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, status, "
                                    "zone, unit_id, begin_interval_scn, end_interval_scn "
                                    "FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld "
                                    "and ls_id = %ld and status = '%s'",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_, tenant_snapshot_id.id(), ls_id.id(), ls_snap_status_to_str(status)))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_snapshot_id), K(ls_id), K(status));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(sql, simple_items))) {
    LOG_WARN("failed to get snap ls replica simple items", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                          const ObLSID &ls_id,
                                                                          const ObAddr& addr,
                                                                          const bool need_lock,
                                                                          ObTenantSnapLSReplicaSimpleItem &simple_item)
{
  int ret = OB_SUCCESS;
  simple_item.reset();
  ObSqlString sql;
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};
  ObArray<ObTenantSnapLSReplicaSimpleItem> simple_items;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid()
                         || !ls_id.is_valid()
                         || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(ls_id), K(addr));
  } else if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip to string failed", KR(ret), K(addr));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, status, "
                                    "zone, unit_id, begin_interval_scn, end_interval_scn "
                                    "FROM %s WHERE tenant_id = %lu AND snapshot_id = %ld AND ls_id = %ld "
                                    "AND svr_ip = '%s' AND svr_port = %d",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_, tenant_snapshot_id.id(), ls_id.id(),
                                    ip_buf, addr.get_port()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" FOR UPDATE "))) {  /*lock row*/
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(sql, simple_items))) {
    LOG_WARN("failed to get snap ls replica simple items", KR(ret), K(sql));
  } else {
    if (simple_items.count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("item not exist", KR(ret), K(sql), K(simple_items));
    } else if (simple_items.count() > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of simple_items is not as expected", KR(ret), K(sql), K(simple_items));
    } else if (OB_FAIL(simple_item.assign(simple_items.at(0)))) {
      LOG_WARN("failed to assign", KR(ret), K(sql), K(simple_items));
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_ls_replica_simple_items_(const ObSqlString &sql,
                                                                            ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else {
      ObTenantSnapLSReplicaSimpleItem simple_item;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        simple_item.reset();
        if (OB_FAIL(fill_item_from_result_(result, simple_item))) {
          LOG_WARN("fill simple item failed", KR(ret));
        } else if (OB_FAIL(simple_items.push_back(simple_item))) {
          LOG_WARN("failed push back item", KR(ret), K(simple_item));
        }
      }
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next result", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::fill_item_from_result_(const ObMySQLResult *result,
                                                          ObTenantSnapItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObTenantSnapshotID tenant_snapshot_id;
    ObString snapshot_name;
    ObString status_str;
    uint64_t snapshot_scn_val = OB_INVALID_SCN_VAL;
    SCN snapshot_scn;
    uint64_t clog_start_scn_val = OB_INVALID_SCN_VAL;
    SCN clog_start_scn;
    ObString type_str;
    int64_t create_time = OB_INVALID_TIMESTAMP;
    uint64_t data_version = 0;
    int64_t owner_job_id;

    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_id", tenant_snapshot_id, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "snapshot_name", snapshot_name);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_UINT_FIELD_MYSQL(*result, "snapshot_scn", snapshot_scn_val, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "clog_start_scn", clog_start_scn_val, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "type", type_str);
    EXTRACT_TIMESTAMP_FIELD_MYSQL(*result, "create_time", create_time);
    EXTRACT_UINT_FIELD_MYSQL(*result, "data_version", data_version, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "owner_job_id", owner_job_id, int64_t);
    if (OB_SUCC(ret)) {
      if (OB_INVALID_SCN_VAL != snapshot_scn_val &&
          OB_FAIL(snapshot_scn.convert_for_inner_table_field(snapshot_scn_val))) {
        LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(snapshot_scn_val));
      } else if (OB_INVALID_SCN_VAL != clog_start_scn_val &&
                OB_FAIL(clog_start_scn.convert_for_inner_table_field(clog_start_scn_val))) {
        LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(clog_start_scn_val));
      } else if (OB_FAIL(item.init(tenant_id, tenant_snapshot_id, snapshot_name,
                                   str_to_tenant_snap_status(status_str), snapshot_scn, clog_start_scn,
                                   str_to_type(type_str), create_time, data_version, owner_job_id))) {
        LOG_WARN("fail to init item", KR(ret), K(tenant_id), K(tenant_snapshot_id), K(snapshot_name),
                              K(str_to_tenant_snap_status(status_str)), K(snapshot_scn), K(clog_start_scn),
                              K(str_to_type(type_str)), K(create_time), K(data_version), K(owner_job_id));
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::fill_item_from_result_(const ObMySQLResult *result,
                                                          ObTenantSnapLSItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObTenantSnapshotID tenant_snapshot_id;
    ObLSID ls_id;
    uint64_t ls_group_id = OB_INVALID_ID;
    ObString status_str;
    ObLSStatus status;
    ObString flag_str;
    ObLSFlag flag;
    uint64_t create_scn_val = OB_INVALID_SCN_VAL;
    SCN create_scn;
    ObLSOperationType type;
    ObLSAttr ls_attr;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_id", tenant_snapshot_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "flag", flag_str);
    EXTRACT_UINT_FIELD_MYSQL(*result, "create_scn", create_scn_val, uint64_t);
    if (OB_SUCC(ret)) {
      status = str_to_ls_status(status_str);
      type = ObLSAttrOperator::get_ls_operation_by_status(status);
      if (OB_FAIL(flag.str_to_flag(flag_str))) {
        LOG_WARN("failed to convert flag", KR(ret), K(flag_str));
      } else if (OB_INVALID_SCN_VAL == create_scn_val) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected create_scn_val", KR(ret), K(create_scn_val));
      } else if (OB_FAIL(create_scn.convert_for_inner_table_field(create_scn_val))) {
        LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(create_scn_val));
      } else if (OB_FAIL(ls_attr.init(ls_id, ls_group_id, flag, status, type, create_scn))) {
          LOG_WARN("failed to init ls attr", KR(ret), K(ls_group_id),
                  K(ls_id), K(status_str), K(status), K(type), K(create_scn));
      } else if (OB_FAIL(item.init(tenant_id, tenant_snapshot_id, ls_attr))) {
        LOG_WARN("failed to init item", KR(ret), K(tenant_id), K(tenant_snapshot_id), K(ls_attr));
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::fill_item_from_result_(const ObMySQLResult *result,
                                                          ObTenantSnapLSReplicaSimpleItem &simple_item)
{
  int ret = OB_SUCCESS;
  simple_item.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObTenantSnapshotID tenant_snapshot_id;
    ObLSID ls_id;
    ObString svr_ip;
    int64_t svr_port = 0;
    ObAddr addr;
    ObString status_str;
    ObString zone_str;
    ObZone zone;
    uint64_t unit_id = OB_INVALID_ID;
    uint64_t begin_interval_scn_val = OB_INVALID_SCN_VAL;
    SCN begin_interval_scn;
    uint64_t end_interval_scn_val = OB_INVALID_SCN_VAL;
    SCN end_interval_scn;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_id", tenant_snapshot_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
    EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "zone", zone_str);
    EXTRACT_INT_FIELD_MYSQL(*result, "unit_id", unit_id, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "begin_interval_scn", begin_interval_scn_val, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "end_interval_scn", end_interval_scn_val, uint64_t);
    if (OB_SUCC(ret)) {
      if (OB_INVALID_SCN_VAL != begin_interval_scn_val &&
          OB_FAIL(begin_interval_scn.convert_for_inner_table_field(begin_interval_scn_val))) {
        LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(begin_interval_scn_val));
      } else if (OB_INVALID_SCN_VAL != end_interval_scn_val &&
                OB_FAIL(end_interval_scn.convert_for_inner_table_field(end_interval_scn_val))) {
        LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(end_interval_scn_val));
      } else if (OB_UNLIKELY(!addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set_ip_addr", KR(ret), K(svr_ip), K(svr_port));
      } else if (OB_FAIL(zone.assign(zone_str))) {
        LOG_WARN("fail to assign zone", KR(ret), K(zone_str));
      } else if (OB_FAIL(simple_item.init(tenant_id, tenant_snapshot_id, ls_id, addr, str_to_ls_snap_status(status_str),
                                          zone, unit_id, begin_interval_scn, end_interval_scn))) {
        LOG_WARN("fail to init simple_item", KR(ret), K(tenant_id), K(tenant_snapshot_id), K(ls_id), K(addr),
                K(str_to_ls_snap_status(status_str)), K(zone), K(unit_id), K(begin_interval_scn), K(end_interval_scn));
      }
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::fill_item_from_result_(const ObMySQLResult *result,
                                                          ObTenantSnapJobItem &item)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObTenantSnapshotID tenant_snapshot_id;
  int64_t job_start_time = OB_INVALID_TIMESTAMP;
  int64_t majority_succ_time = OB_INVALID_TIMESTAMP;
  ObString operation_str;

  int64_t real_length = 0;
  common::ObCurTraceId::TraceId trace_id;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};

  EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_id", tenant_snapshot_id, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "operation", operation_str);
  EXTRACT_TIMESTAMP_FIELD_MYSQL(*result, "job_start_time", job_start_time);
  EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id_buf, sizeof(trace_id_buf), real_length);
  EXTRACT_TIMESTAMP_FIELD_MYSQL(*result, "majority_succ_time", majority_succ_time);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trace_id.parse_from_buf(trace_id_buf))) {
      LOG_WARN("fail to parse trace id from buf", KR(ret), K(trace_id_buf));
    } else {
      item.set_tenant_id(tenant_id);
      item.set_tenant_snapshot_id(tenant_snapshot_id);
      item.set_operation(str_to_operation(operation_str));
      item.set_job_start_time(job_start_time);
      item.set_trace_id(trace_id);
      item.set_majority_succ_time(majority_succ_time);
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::delete_tenant_snap_ls_replica_items(const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("fail to add column", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("fail to add column", KR(ret));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME, sql))) {
    LOG_WARN("failed to splice delete sql", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::update_tenant_snap_ls_replica_item(const ObTenantSnapLSReplicaSimpleItem &simple_item,
                                                                      const ObLSMetaPackage* ls_meta_package)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else {
    ObString hex_meta_package;
    ObArenaAllocator allocator;
    if (!simple_item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(simple_item));
    } else if (NULL != ls_meta_package && !ls_meta_package->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KPC(ls_meta_package));
    } else if (!simple_item.get_addr().ip_to_string(ip_buf, sizeof(ip_buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ip to string failed", KR(ret), K(simple_item.get_addr()));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", simple_item.get_tenant_id()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("snapshot_id", simple_item.get_tenant_snapshot_id().id()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("ls_id", simple_item.get_ls_id().id()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("svr_ip", ip_buf))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("svr_port", simple_item.get_addr().get_port()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_column("status", ls_snap_status_to_str(simple_item.get_status())))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_column("zone", simple_item.get_zone().ptr()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_column("unit_id", simple_item.get_unit_id()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_uint64_column("begin_interval_scn",
                        simple_item.get_begin_interval_scn().get_val_for_inner_table_field()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(dml.add_uint64_column("end_interval_scn",
                        simple_item.get_end_interval_scn().get_val_for_inner_table_field()))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (NULL != ls_meta_package) {
      if (OB_FAIL(encode_package_to_hex_string_(*ls_meta_package, allocator, hex_meta_package))) {
        LOG_WARN("encode package failed", KR(ret), KPC(ls_meta_package));
      } else if (OB_FAIL(dml.add_column("ls_meta_package", hex_meta_package))) {
        LOG_WARN("add column failed", KR(ret));
      }
    }
  }

  if (FAILEDx(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME, sql))) {
    LOG_WARN("fail to generate sql", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows not match", KR(ret), K(user_tenant_id_), K(affected_rows), K(simple_item));
  }

  return ret;
}

int ObTenantSnapshotTableOperator::archive_tenant_snap_ls_replica_history(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant snapshot id", KR(ret), K(snapshot_id));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s SELECT * FROM %s WHERE "
                                    "tenant_id = %ld AND "
                                    "snapshot_id = %ld",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME,
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id_,
                                    snapshot_id.id()
                                    ))) {
    LOG_WARN("fail to build sql", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
  }

  return ret;
}

// lazy eliminate
int ObTenantSnapshotTableOperator::eliminate_tenant_snap_ls_replica_history()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t replica_num = 0;
  int64_t affected_rows = 0;
  static const int64_t REPLICA_NUM_LIMIT = 1000;
  static const int64_t ELIMINATED_NUM = 200;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT COUNT(1) AS REPLICA_NUM FROM %s ",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME))) {
    LOG_WARN("fail to build sql", KR(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(user_tenant_id_), K(sql));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("next failed", KR(ret), K(user_tenant_id_), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "REPLICA_NUM", replica_num, int64_t);
      }
    }
  }

  if (OB_SUCC(ret) && replica_num > REPLICA_NUM_LIMIT) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE snapshot_id IN "
                               "(SELECT snapshot_id FROM %s "
                               "ORDER BY gmt_create ASC "
                               "LIMIT %ld)",
                               OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME,
                               OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME,
                               ELIMINATED_NUM))) {
      LOG_WARN("fail to build sql", KR(ret));
    } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
    } else if (is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not match", KR(ret), K(user_tenant_id_), K(affected_rows), K(sql));
    }
  }

  return ret;
}

int ObTenantSnapshotTableOperator::insert_tenant_snap_ls_replica_simple_items(const ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_items.count(); i++) {
      MEMSET(ip_buf, '\0', sizeof(ip_buf));
      const ObTenantSnapLSReplicaSimpleItem &simple_item = simple_items.at(i);
      if (!simple_item.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(simple_item));
      } else if (!simple_item.get_addr().ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ip to string failed", KR(ret), K(simple_item.get_addr()));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", simple_item.get_tenant_id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("snapshot_id", simple_item.get_tenant_snapshot_id().id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("ls_id", simple_item.get_ls_id().id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("svr_ip", ip_buf))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("svr_port", simple_item.get_addr().get_port()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("status", ls_snap_status_to_str(simple_item.get_status())))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("zone", simple_item.get_zone().ptr()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("unit_id", simple_item.get_unit_id()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_uint64_column("begin_interval_scn",
                          simple_item.get_begin_interval_scn().get_val_for_inner_table_field()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_uint64_column("end_interval_scn",
                          simple_item.get_end_interval_scn().get_val_for_inner_table_field()))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(user_tenant_id_));
      }
    }
    if (FAILEDx(dml.splice_batch_insert_sql(OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME, sql))) {
      LOG_WARN("fail to generate sql", KR(ret), K(user_tenant_id_));
    } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(gen_meta_tenant_id(user_tenant_id_)), K(sql));
    } else if (affected_rows != simple_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not match", KR(ret), K(user_tenant_id_), K(affected_rows), K(simple_items.count()));
    }
  }
  return ret;
}

int ObTenantSnapshotTableOperator::get_tenant_snap_job_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                                            const ObTenantSnapOperation operation,
                                                            ObTenantSnapJobItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %ld "
                                 "AND snapshot_id = %ld AND operation = '%s'",
                                 OB_ALL_TENANT_SNAPSHOT_JOB_TNAME,
                                 user_tenant_id_,
                                 tenant_snapshot_id.id(),
                                 operation_to_str(operation)))) {
        LOG_WARN("failed to assign sql",
            KR(ret), K(user_tenant_id_), K(tenant_snapshot_id), K(operation));
      } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(user_tenant_id_), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("next failed", KR(ret), K(sql));
      } else if (OB_FAIL(fill_item_from_result_(result, item))) {
        LOG_WARN("fill item from result failed", KR(ret), K(sql));
      }
    }
  }

  return ret;
}

int ObTenantSnapshotTableOperator::insert_tenant_snap_job_item(const ObTenantSnapJobItem &item)
{
  int ret = OB_SUCCESS;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};

  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t job_start_time = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", item.get_tenant_snapshot_id().id()))) {
    LOG_WARN("add column failed", KR(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("operation", operation_to_str(item.get_operation())))) {
    LOG_WARN("add column failed", KR(ret), K(item));
  } else if (OB_FAIL(dml.add_time_column("job_start_time", job_start_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (FALSE_IT(item.get_trace_id().to_string(trace_id_buf, sizeof(trace_id_buf)))) {
  } else if (OB_FAIL(dml.add_column("trace_id", trace_id_buf))) {
    LOG_WARN("add column failed", KR(ret), K(trace_id_buf), K(item));
  } else if (OB_FAIL(dml.add_raw_time_column("majority_succ_time", item.get_majority_succ_time()))) {
    LOG_WARN("add column failed", KR(ret), K(item));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_SNAPSHOT_JOB_TNAME, sql))) {
    LOG_WARN("splice insert sql failed", KR(ret), K(item));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(item), K(sql), K_(user_tenant_id));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantSnapshotTableOperator::update_tenant_snap_job_majority_succ_time(
                                                      const ObTenantSnapshotID &tenant_snapshot_id,
                                                      const int64_t majority_succ_time)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid() || 0 >= majority_succ_time)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), K(majority_succ_time));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("operation", operation_to_str(ObTenantSnapOperation::CREATE)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_raw_time_column("majority_succ_time", majority_succ_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_SNAPSHOT_JOB_TNAME, sql))) {
    LOG_WARN("splice insert sql failed", KR(ret), K(tenant_snapshot_id), K(majority_succ_time));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K_(user_tenant_id));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantSnapshotTableOperator::delete_tenant_snap_job_item(
                                                    const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant snapshot table operator not init", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", user_tenant_id_))) {
    LOG_WARN("add column failed", KR(ret), K(user_tenant_id_), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_id", tenant_snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret), K(user_tenant_id_), K(tenant_snapshot_id));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_TENANT_SNAPSHOT_JOB_TNAME, sql))) {
    LOG_WARN("splice delete sql failed", KR(ret), K(sql), K(user_tenant_id_), K(tenant_snapshot_id));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(user_tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(user_tenant_id_), K(tenant_snapshot_id));
  } else if (!is_zero_row(affected_rows)
      && !is_single_row(affected_rows)
      && !is_double_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantSnapshotTableOperator::encode_package_to_hex_string_(const ObLSMetaPackage& ls_meta_package,
                                                                 ObArenaAllocator& allocator,
                                                                 ObString& hex_str)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  int64_t buf_len = ls_meta_package.get_serialize_size();
  int64_t pos = 0;
  int64_t need_len = 0;

  if (!ls_meta_package.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_meta_package));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
  } else if (OB_FAIL(ls_meta_package.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret));
  } else if (OB_FAIL(ObHexUtilsBase::hex(hex_str, allocator, buf, buf_len))) {
    LOG_WARN("fail to hex", KR(ret));
  }

  return ret;
}

int ObTenantSnapshotTableOperator::decode_hex_string_to_package_(const ObString& hex_str,
                                                                 ObArenaAllocator& allocator,
                                                                 ObLSMetaPackage& ls_meta_package)
{
  int ret = OB_SUCCESS;
  char *unhex_buf = nullptr;
  int64_t unhex_buf_len = 0;
  int64_t pos = 0;

  if (hex_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hex_str));
  } else if (OB_FAIL(ObHexUtilsBase::unhex(hex_str, allocator, unhex_buf, unhex_buf_len))) {
    LOG_WARN("fail to unhex", KR(ret), K(hex_str));
  } else if (OB_FAIL(ls_meta_package.deserialize(unhex_buf, unhex_buf_len, pos))) {
    LOG_WARN("deserialize ls meta package failed", KR(ret), K(hex_str));
  }

  return ret;
}
