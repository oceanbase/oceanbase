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

#define USING_LOG_PREFIX RS
#include "ob_restore_point_service.h"
#include "share/ob_snapshot_table_proxy.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "storage/transaction/ob_ts_mgr.h"

namespace oceanbase {
namespace rootserver {

ObRestorePointService::ObRestorePointService() : is_inited_(false), ddl_service_(NULL), freeze_info_mgr_(NULL)
{}

ObRestorePointService::~ObRestorePointService()
{}

int ObRestorePointService::init(rootserver::ObDDLService& ddl_service, rootserver::ObFreezeInfoManager& freeze_info_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore point service init twice", K(ret));
  } else {
    ddl_service_ = &ddl_service;
    freeze_info_mgr_ = &freeze_info_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObRestorePointService::create_restore_point(const uint64_t tenant_id, const char* name)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;
  int64_t snapshot_count = 0;
  int64_t retry_cnt = 0;
  share::ObSnapshotInfo tmp_info;
  int get_snapshot_ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  int64_t schema_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore point service do not init", K(ret));
  } else if (OB_ISNULL(name) || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create recovery point get invalid argument", K(ret), KP(name));
  } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().get_snapshot_count(ddl_service_->get_sql_proxy(),
                 tenant_id,
                 share::ObSnapShotType::SNAPSHOT_FOR_RESTORE_POINT,
                 snapshot_count))) {
    LOG_WARN("fail to get snapshot count", K(ret), K(tenant_id), K(name));
  } else if (snapshot_count >= MAX_RESTORE_POINT) {
    ret = OB_ERR_RESTORE_POINT_TOO_MANY;
    LOG_INFO("too many restore points", K(tenant_id), K(snapshot_count));
  } else if (FALSE_IT(get_snapshot_ret = ddl_service_->get_snapshot_mgr().get_snapshot(ddl_service_->get_sql_proxy(),
                          tenant_id,
                          share::ObSnapShotType::SNAPSHOT_FOR_RESTORE_POINT,
                          name,
                          tmp_info))) {
  } else if (OB_SUCCESS == get_snapshot_ret) {
    ret = OB_ERR_RESTORE_POINT_EXIST;
    char tmp_name[OB_MAX_RESERVED_POINT_NAME_LENGTH + 2];
    convert_name(name, tmp_name, OB_MAX_RESERVED_POINT_NAME_LENGTH + 2);
    LOG_USER_ERROR(OB_ERR_RESTORE_POINT_EXIST, tmp_name);
  } else if (OB_ITER_END != get_snapshot_ret) {
    ret = get_snapshot_ret;
    LOG_WARN("failed to get snapshot", K(ret), K(name), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to get schema version", K(ret), K(tenant_id));
  } else {
    const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
    transaction::MonotonicTs ts;
    ret = OB_SNAPSHOT_DISCARDED;
    while (OB_SNAPSHOT_DISCARDED == ret || OB_EAGAIN == ret) {
      if (THIS_WORKER.get_timeout_remain() < 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("create restore point timeout", K(ret), K(tenant_id), K(name));
      } else if (OB_FAIL(OB_TS_MGR.get_local_trans_version(tenant_id, stc, NULL, snapshot_version, ts))) {
        LOG_WARN("failed to get publish version", K(ret), K(tenant_id));
      } else {
        if (retry_cnt > RETRY_CNT) {
          snapshot_version = ObTimeUtility::current_time();
        }
        share::ObSnapshotInfo info;
        info.snapshot_type_ = share::ObSnapShotType::SNAPSHOT_FOR_RESTORE_POINT;
        info.snapshot_ts_ = snapshot_version;
        info.schema_version_ = schema_version;
        info.tenant_id_ = tenant_id;
        info.table_id_ = 0;
        info.comment_ = name;
        ObMySQLTransaction trans;
        common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
        if (OB_FAIL(trans.start(&proxy))) {
          LOG_WARN("fail to start trans", K(ret));
        } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().acquire_snapshot(trans, info))) {
          if (OB_SNAPSHOT_DISCARDED != ret) {
            LOG_WARN("fail to acquire snapshot", K(ret), K(info));
          } else {
            retry_cnt++;
          }
        }
        if (trans.is_started()) {
          bool is_commit = (ret == OB_SUCCESS);
          int tmp_ret = trans.end(is_commit);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to end trans", K(ret), K(is_commit));
            if (OB_SUCC(ret)) {
              ret = tmp_ret;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && retry_cnt > RETRY_CNT) {
      if (OB_FAIL(OB_TS_MGR.wait_gts_elapse(tenant_id, snapshot_version))) {
        LOG_WARN("failed to wait gts elapse", K(ret), K(tenant_id), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObRestorePointService::create_backup_point(
    const uint64_t tenant_id, const char* name, const int64_t snapshot_version, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  share::ObSnapshotInfo tmp_info;
  int64_t snapshot_gc_ts = 0;
  int get_snapshot_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore point service do not init", K(ret));
  } else if (OB_ISNULL(name) || OB_INVALID_ID == tenant_id || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create recovery point get invalid argument", K(ret), KP(name), K(snapshot_version));
  } else if (FALSE_IT(get_snapshot_ret = ddl_service_->get_snapshot_mgr().get_snapshot(ddl_service_->get_sql_proxy(),
                          tenant_id,
                          share::ObSnapShotType::SNAPSHOT_FOR_BACKUP_POINT,
                          snapshot_version,
                          tmp_info))) {
  } else if (OB_SUCCESS == get_snapshot_ret) {
    ret = OB_ERR_BACKUP_POINT_EXIST;
    char tmp_name[OB_MAX_RESERVED_POINT_NAME_LENGTH + 2];
    convert_name(name, tmp_name, OB_MAX_RESERVED_POINT_NAME_LENGTH + 2);
    LOG_USER_ERROR(OB_ERR_BACKUP_POINT_EXIST, tmp_name);
  } else if (OB_ITER_END != get_snapshot_ret) {
    ret = get_snapshot_ret;
    LOG_WARN("failed to get snapshot", K(ret), K(name), K(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_->get_latest_snapshot_gc_ts(snapshot_gc_ts))) {
    LOG_WARN("failed to get latest snapshot gc ts", K(ret));
  } else if (snapshot_gc_ts >= snapshot_version) {
    ret = OB_BACKUP_CAN_NOT_START;
    LOG_WARN("can not create backup point", K(ret), K(snapshot_gc_ts), K(snapshot_version));
  } else {
    share::ObSnapshotInfo info;
    info.snapshot_type_ = share::SNAPSHOT_FOR_BACKUP_POINT;
    info.snapshot_ts_ = snapshot_version;
    info.schema_version_ = schema_version;
    info.tenant_id_ = tenant_id;
    info.table_id_ = 0;
    info.comment_ = name;

    ObMySQLTransaction trans;
    common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(&proxy))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().acquire_snapshot(trans, info))) {
      LOG_WARN("fail to acquire snapshot", K(ret), K(info));
    }
    if (trans.is_started()) {
      bool is_commit = (ret == OB_SUCCESS);
      int tmp_ret = trans.end(is_commit);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to end trans", K(ret), K(is_commit));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObRestorePointService::drop_restore_point(const uint64_t tenant_id, const char* name)
{
  int ret = OB_SUCCESS;
  share::ObSnapshotInfo tmp_info;
  int get_snapshot_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore point service do not init", K(ret));
  } else if (NULL == name || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(name));
  } else if (FALSE_IT(
                 get_snapshot_ret = ddl_service_->get_snapshot_mgr().get_snapshot(
                     ddl_service_->get_sql_proxy(), tenant_id, share::SNAPSHOT_FOR_RESTORE_POINT, name, tmp_info))) {
  } else if (OB_ITER_END == get_snapshot_ret) {
    ret = OB_ERR_RESTORE_POINT_NOT_EXIST;
    LOG_WARN("entry not exist", K(ret), K(tmp_info));
    char tmp_name[OB_MAX_RESERVED_POINT_NAME_LENGTH + 2];
    convert_name(name, tmp_name, OB_MAX_RESERVED_POINT_NAME_LENGTH + 2);
    LOG_USER_ERROR(OB_ERR_RESTORE_POINT_NOT_EXIST, tmp_name);
  } else if (OB_SUCCESS != get_snapshot_ret) {
    ret = get_snapshot_ret;
    LOG_WARN("failed to get snapshot", K(ret), K(name), K(tenant_id));
  }

  ObMySQLTransaction trans;
  common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&proxy))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().release_snapshot(trans, tmp_info))) {
    LOG_WARN("fail to release snapshot", K(ret), K(tmp_info));
  }
  if (trans.is_started()) {
    bool is_commit = (ret == OB_SUCCESS);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end trans", K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObRestorePointService::drop_backup_point(const uint64_t tenant_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  share::ObSnapshotInfo tmp_info;
  int get_snapshot_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore point service do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(snapshot_version));
  } else if (FALSE_IT(get_snapshot_ret = ddl_service_->get_snapshot_mgr().get_snapshot(ddl_service_->get_sql_proxy(),
                          tenant_id,
                          share::SNAPSHOT_FOR_BACKUP_POINT,
                          snapshot_version,
                          tmp_info))) {
  } else if (OB_ITER_END == get_snapshot_ret) {
    ret = OB_ERR_BACKUP_POINT_NOT_EXIST;
    LOG_WARN("entry not exist", K(ret), K(tmp_info), K(snapshot_version));
  } else if (OB_SUCCESS != get_snapshot_ret) {
    ret = get_snapshot_ret;
    LOG_WARN("failed to get snapshot", K(ret), K(snapshot_version), K(tenant_id));
  }

  ObMySQLTransaction trans;
  common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(&proxy))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().release_snapshot(trans, tmp_info))) {
    LOG_WARN("fail to release snapshot", K(ret), K(tmp_info));
  }
  if (trans.is_started()) {
    bool is_commit = (ret == OB_SUCCESS);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end trans", K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

void ObRestorePointService::convert_name(const char* name, char* tmp_name, const int64_t length)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t name_length = strlen(name);
  memset(tmp_name, 0, length);
  if (name_length + 2 > length) {
    tmp_ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("name size overflow", K(tmp_ret), K(name_length), K(length));
  } else if (OB_SUCCESS != (tmp_ret = databuff_printf(tmp_name, length, "\'%s\'", name))) {
    LOG_ERROR("failed to create name", K(tmp_ret), K(name), K(tmp_name), K(length));
  }
}

}  // namespace rootserver
}  // namespace oceanbase
