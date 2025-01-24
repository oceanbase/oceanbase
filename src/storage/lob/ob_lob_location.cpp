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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_location.h"
#include "observer/ob_server.h"
#include "sql/das/ob_das_utils.h"

namespace oceanbase
{
namespace storage
{

int ObLobLocationUtil::get_ls_leader(ObLobAccessParam& param, const uint64_t tenant_id,
    const share::ObLSID &ls_id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else {
    uint32_t renew_count = 0;
    const uint32_t max_renew_count = 10;
    const int64_t retry_us = 200 * 1000;
    do {
      if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader))) {
        if (OB_LS_LOCATION_NOT_EXIST == ret && renew_count++ < max_renew_count) {  // retry ten times
          LOG_WARN("failed to get location and force renew", K(ret), K(tenant_id), K(ls_id), K(cluster_id));
          if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
            LOG_WARN("failed to nonblock renew from location cache", K(tmp_ret), K(ls_id), K(cluster_id));
          } else if (ObTimeUtility::current_time() > param.timeout_) {
            renew_count = max_renew_count;
          } else {
            usleep(retry_us);
          }
        }
      } else {
        LOG_DEBUG("get ls leader", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
      }
    } while (OB_LS_LOCATION_NOT_EXIST == ret && renew_count < max_renew_count);

    if (OB_SUCC(ret) && !leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(ls_id), K(leader), K(cluster_id));
    }
  }
  return ret;
}

int ObLobLocationUtil::is_remote(ObLobAccessParam& param, bool& is_remote, common::ObAddr& dst_addr)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  uint64_t tenant_id = param.tenant_id_;
  const ObAddr &self_addr = MYADDR;
  if (lob_locator == nullptr) {
    is_remote = false;
  } else if (!lob_locator->is_persist_lob()) {
    is_remote = false;
  } else if (param.from_rpc_ == true) {
    is_remote = false;
  } else {
    bool has_retry_info = false;
    ObMemLobExternHeader *extern_header = nullptr;
    if (OB_SUCC(lob_locator->get_extern_header(extern_header))) {
      has_retry_info = extern_header->flags_.has_retry_info_;
    }
    if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_2_2_0) {
      // compat old version
      has_retry_info = false;
    }
    if (has_retry_info) {
      ObMemLobRetryInfo *retry_info = nullptr;
      if (OB_FAIL(lob_locator->get_retry_info(retry_info))) {
        LOG_WARN("fail to get retry info", K(ret), KPC(lob_locator));
      } else {
        dst_addr = retry_info->addr_;
      }
    } else {
      if (OB_FAIL(get_ls_leader(param, tenant_id, param.ls_id_, dst_addr))) {
        LOG_WARN("failed to get ls leader", K(ret), K(tenant_id), K(param.ls_id_));
      }
    }
    if (OB_SUCC(ret)) {
      // lob from other tenant also should read by rpc
      is_remote = (dst_addr != self_addr) || (tenant_id != MTL_ID());
      if (param.from_rpc_ == true && is_remote) {
        ret = OB_NOT_MASTER;
        LOG_WARN("call from rpc, but remote again", K(ret), K(dst_addr), K(self_addr), K(tenant_id), K(MTL_ID()));
      }
    }
  }
  return ret;
}


int ObLobLocationUtil::lob_check_tablet_not_exist(ObLobAccessParam &param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  bool tablet_exist = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", KR(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(param.tenant_id_, schema_guard))) {
    // tenant could be deleted
    LOG_WARN("get tenant schema guard fail", KR(ret), K(param.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param.tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    //table could be dropped
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist, fast fail das task", K(table_id));
  } else if (OB_FAIL(table_schema->check_if_tablet_exists(param.tablet_id_, tablet_exist))) {
    LOG_WARN("check if tablet exists failed", K(ret), K(param), K(table_id));
  } else if (!tablet_exist) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("partition not exist, maybe dropped by DDL", K(ret), K(param), K(table_id));
  }
  return ret;
}

int ObLobLocationUtil::lob_refresh_location(ObLobAccessParam &param, int last_err, int retry_cnt)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 *lob_locator = param.lob_locator_;
  const ObAddr &self_addr = MYADDR;
  ObMemLobExternHeader *extern_header = NULL;
  bool has_retry_info = false;
  if (OB_NOT_NULL(lob_locator) && OB_SUCC(lob_locator->get_extern_header(extern_header))) {
    has_retry_info = extern_header->flags_.has_retry_info_;
  }
  if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_2_2_0) {
    // compat old version
    has_retry_info = false;
  }
  if (param.tenant_id_ != MTL_ID()) { // query over tenant id
    has_retry_info = false;
  }

  if (!has_retry_info) {
    // do check remote
    if (OB_FAIL(ObLobLocationUtil::get_ls_leader(param))) {
      LOG_WARN("fail to do check is remote", K(ret));
    }
  } else if (OB_FAIL(ObDASUtils::wait_das_retry(retry_cnt))) {
    LOG_WARN("wait das retry failed", K(ret), K(last_err), K(retry_cnt));
  } else {
    // do location refresh
    ObArenaAllocator tmp_allocator("LobRefLoc", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
    sql::ObDASLocationRouter router(tmp_allocator);
    router.set_last_errno(last_err);
    sql::ObDASTableLocMeta loc_meta(tmp_allocator);
    loc_meta.ref_table_id_ = extern_header->table_id_;
    sql::ObDASTabletLoc tablet_loc;
    ObMemLobRetryInfo *retry_info = nullptr;
    ObMemLobLocationInfo *location_info = nullptr;
    if (last_err == OB_TABLET_NOT_EXIST && OB_FAIL(ObLobLocationUtil::lob_check_tablet_not_exist(param, extern_header->table_id_))) {
      LOG_WARN("fail to check tablet not exist", K(ret), K(extern_header->table_id_), K(last_err), K(retry_cnt));
    } else if (OB_FAIL(lob_locator->get_retry_info(retry_info))) {
      LOG_WARN("fail to get retry info", K(ret), KPC(lob_locator), K(last_err), K(retry_cnt));
    } else if (OB_FAIL(lob_locator->get_location_info(location_info))) {
      LOG_WARN("failed to get location info", K(ret), KPC(lob_locator), K(last_err), K(retry_cnt));
    } else if (OB_FALSE_IT(loc_meta.select_leader_ = retry_info->is_select_leader_)) {
       // use main tablet id to get location, for lob meta tablet is same location as main tablet
    } else if (OB_FAIL(router.get_tablet_loc(loc_meta, param.tablet_id_, tablet_loc))) {
      LOG_WARN("fail to refresh location", K(ret), K(last_err), K(retry_cnt));
    } else if (param.tablet_id_ != tablet_loc.tablet_id_ || location_info->tablet_id_ != tablet_loc.tablet_id_.id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet id is changed", K(ret), K(tablet_loc), K(param), KPC(location_info));
    } else {
      param.addr_ = tablet_loc.server_;
      if (tablet_loc.ls_id_ != param.ls_id_) {
        LOG_INFO("[LOB RETRY] lob retry find tablet ls id is changed",
                 K(param.tablet_id_), K(param.ls_id_), K(tablet_loc.ls_id_), K(last_err), K(retry_cnt));
        param.ls_id_ = tablet_loc.ls_id_;
        location_info->ls_id_ = tablet_loc.ls_id_.id();
      }
    }
  }
  LOG_TRACE("[LOB RETRY] after do fresh location", K(ret), K(last_err), K(retry_cnt), K(has_retry_info), K(param));
  return ret;
}


int ObLobLocationUtil::get_ls_leader(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = param.tenant_id_;
  const int64_t cluster_id = GCONF.cluster_id;
  const share::ObLSID &ls_id = param.ls_id_;
  ObAddr leader_addr;

  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", K(ret), K(param));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(param));
  } else {
    uint32_t renew_count = 0;
    const uint32_t max_renew_count = 10;
    const int64_t retry_us = 200 * 1000;
    do {
      if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader_addr))) {
        if (OB_LS_LOCATION_NOT_EXIST == ret && renew_count++ < max_renew_count) {  // retry ten times
          LOG_WARN("failed to get location and force renew", K(ret), K(tenant_id), K(ls_id), K(cluster_id), K(renew_count));
          if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
            LOG_WARN("failed to nonblock renew from location cache", K(tmp_ret), K(ls_id), K(cluster_id));
          } else if (ObTimeUtility::current_time() > param.timeout_) {
            renew_count = max_renew_count;
            LOG_WARN("renew timeout", K(ret), K(tmp_ret), K(param));
          } else {
            usleep(retry_us);
          }
        }
      } else {
        LOG_TRACE("[LOB] get ls leader", K(tenant_id), K(ls_id), K(leader_addr), K(cluster_id), K(renew_count));
      }
    } while (OB_LS_LOCATION_NOT_EXIST == ret && renew_count < max_renew_count);


    if (OB_FAIL(ret)) {
    } else if (!leader_addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(ls_id), K(leader_addr), K(cluster_id));
    } else {
      param.addr_ = leader_addr;
    }
  }
  return ret;
}


}  // end namespace storage
}  // end namespace oceanbase