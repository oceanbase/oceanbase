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

#include "ob_major_mv_merge_info.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "rootserver/mview/ob_collect_mv_merge_info_task.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObMajorMVMergeInfo::ObMajorMVMergeInfo()
  : major_mv_merge_scn_(share::ObScnRange::MIN_SCN),
    major_mv_merge_scn_safe_calc_(share::ObScnRange::MIN_SCN),
    major_mv_merge_scn_publish_(share::ObScnRange::MIN_SCN)
{
}

void ObMajorMVMergeInfo::reset()
{
  major_mv_merge_scn_ = share::ObScnRange::MIN_SCN;
  major_mv_merge_scn_safe_calc_ = share::ObScnRange::MIN_SCN;
  major_mv_merge_scn_publish_ = share::ObScnRange::MIN_SCN;
}

bool ObMajorMVMergeInfo::is_valid() const
{
  return major_mv_merge_scn_.is_valid()
      && major_mv_merge_scn_safe_calc_.is_valid()
      && major_mv_merge_scn_publish_.is_valid()
      && major_mv_merge_scn_ <= major_mv_merge_scn_safe_calc_
      && major_mv_merge_scn_safe_calc_ <= major_mv_merge_scn_publish_;
}

void ObMajorMVMergeInfo::operator=(const ObMajorMVMergeInfo &other)
{
  major_mv_merge_scn_ = other.major_mv_merge_scn_;
  major_mv_merge_scn_safe_calc_ = other.major_mv_merge_scn_safe_calc_;
  major_mv_merge_scn_publish_ = other.major_mv_merge_scn_publish_;
}

int ObMajorMVMergeInfo::init(const share::SCN &major_mv_merge_scn,
                             const share::SCN &major_mv_merge_scn_safe_calc,
                             const share::SCN &major_mv_merge_scn_publish)
{
  int ret = OB_SUCCESS;

  if (!major_mv_merge_scn.is_valid()
      || !major_mv_merge_scn_safe_calc.is_valid()
      || !major_mv_merge_scn_publish.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(major_mv_merge_scn), K(major_mv_merge_scn_safe_calc), K(major_mv_merge_scn_publish));
  } else {
    major_mv_merge_scn_ = major_mv_merge_scn;
    major_mv_merge_scn_safe_calc_ = major_mv_merge_scn_safe_calc;
    major_mv_merge_scn_publish_ = major_mv_merge_scn_publish;
  }

  return ret;
}
OB_SERIALIZE_MEMBER(ObMajorMVMergeInfo, major_mv_merge_scn_, major_mv_merge_scn_safe_calc_, major_mv_merge_scn_publish_);

ObUpdateMergeScnArg::ObUpdateMergeScnArg()
  : ls_id_(),
    merge_scn_(share::ObScnRange::MIN_SCN)
{
}

void ObUpdateMergeScnArg::reset()
{
  merge_scn_ = share::ObScnRange::MIN_SCN;
  ls_id_.reset();
}

bool ObUpdateMergeScnArg::is_valid() const
{
  return merge_scn_.is_valid() && ls_id_.is_valid();
}

int ObUpdateMergeScnArg::init(const share::ObLSID &ls_id, const share::SCN &merge_scn)
{
  int ret = OB_SUCCESS;
  if (!merge_scn.is_valid()||
      !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_id), K(merge_scn));
  } else {
    ls_id_ = ls_id;
    merge_scn_ = merge_scn;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUpdateMergeScnArg, ls_id_, merge_scn_);

#define SET_MERGE_SCN(set_func) \
  ObUpdateMergeScnArg arg; \
  ObLSHandle ls_handle; \
  ObLS *ls = NULL; \
  int64_t pos = 0; \
  if (OB_FAIL(ret)) { \
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) { \
    ret = OB_INVALID_ARGUMENT; \
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len)); \
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) { \
    LOG_WARN("failed to deserialize", KR(ret)); \
  } else if (OB_UNLIKELY(!arg.is_valid())) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("arg is invalid", KR(ret), K(arg)); \
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) { \
    LOG_WARN("failed to get ls", KR(ret), K(arg)); \
  } else if (OB_UNLIKELY(NULL == (ls = ls_handle.get_ls()))) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("ls should not be NULL", KR(ret), K(arg), KPC(ls)); \
  } else if (OB_FAIL(ls->set_func(arg.merge_scn_))) { \
    LOG_WARN("failed to "#set_func, KR(ret), K(arg), KPC(ls)); \
  } \
  LOG_INFO(#set_func" finish", KR(ret), K(arg));


int ObMVPublishSCNHelper::on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  SET_MERGE_SCN(set_major_mv_merge_scn_publish);
  return ret;
}


int ObMVPublishSCNHelper::on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();
  if (is_invalid_tenant(tenant_role)) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant role cache is invalid", KR(ret));
  } else if (is_standby_tenant(tenant_role)) {
    // ret = OB_NOT_SUPPORTED;
    LOG_INFO("new mview skip in standy tenant", KR(ret));
  } else {
    SET_MERGE_SCN(set_major_mv_merge_scn_publish);
  }
  return ret;
}

int ObMVNoticeSafeHelper::on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  SET_MERGE_SCN(set_major_mv_merge_scn_safe_calc);
  return ret;
}

int ObMVNoticeSafeHelper::on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();
  if (is_invalid_tenant(tenant_role)) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant role cache is invalid", KR(ret));
  } else if (is_standby_tenant(tenant_role)) {
    LOG_INFO("new mview skip in standy tenant", KR(ret));
  } else {
    SET_MERGE_SCN(set_major_mv_merge_scn_safe_calc);
  }
  return ret;
}


int ObMVMergeSCNHelper::on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  SET_MERGE_SCN(set_major_mv_merge_scn);
  return ret;
}

int ObMVMergeSCNHelper::on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();
  if (is_invalid_tenant(tenant_role)) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant role cache is invalid", KR(ret));
  } else if (is_standby_tenant(tenant_role)) {
    LOG_INFO("new mview skip in standy tenant", KR(ret));
  } else {
    SET_MERGE_SCN(set_major_mv_merge_scn);
  }
  return ret;
}

int ObMVCheckReplicaHelper::get_and_update_merge_info(
      const share::ObLSID &ls_id,
      ObMajorMVMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  bool skip_update = false;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is not valid", KR(ret), K(ls_id));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(NULL == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), KPC(ls));
  } else if (FALSE_IT(info = ls->get_ls_meta().get_major_mv_merge_info())) {
  } else if (!info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info is invalid", KR(ret), K(info));
  } else if (!info.need_update_major_mv_merge_scn()) {
    STORAGE_LOG(INFO, "no need update", KR(ret), K(info), KPC(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    STORAGE_LOG(WARN, "failed to build ls tablet iter", KR(ret), KPC(ls));
  } else {
    share::SCN min_major_mv_merge_scn;
    min_major_mv_merge_scn.set_max();
    while (OB_SUCC(ret) && !skip_update) {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = NULL;
      ObStorageSchema *storage_schema = nullptr;
      ObArenaAllocator allocator;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get tablet", KR(ret), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), K(tablet_handle));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet is NULL", KR(ret));
      } else if (tablet->is_ls_inner_tablet() ||
                 tablet->is_empty_shell()) {
        // skip ls inner tablet or empty shell
      } else if (OB_FAIL(tablet->load_storage_schema(allocator, storage_schema))) {
        LOG_WARN("load storage schema failed", K(ret), K(ls_id), KPC(tablet));
      } else if (OB_ISNULL(storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is NULL", K(ret), K(ls_id), KPC(tablet));
      } else if (storage_schema->is_mv_major_refresh()) {
        const int64_t snapshot = tablet->get_last_major_snapshot_version();
        if (0 == snapshot || 1 == snapshot) {
          skip_update = true;
          LOG_INFO("snapshot is invalid, skip update", K(ret), K(info), K(snapshot), K(ls_id), KPC(tablet));
        } else if (min_major_mv_merge_scn.get_val_for_gts() > snapshot
            && OB_FAIL(min_major_mv_merge_scn.convert_for_gts(snapshot))) {
          LOG_WARN("failed to convert_for_gts", K(ret), K(info), K(snapshot), KPC(ls));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (skip_update) {
    } else if (min_major_mv_merge_scn < info.major_mv_merge_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("min_major_mv_merge_scn is less info.major_mv_merge_scn", K(ret), K(info), K(min_major_mv_merge_scn));
    }
    // there is no new mv tablet
    else if (min_major_mv_merge_scn.is_max()) {
      if (OB_FAIL(ls->set_major_mv_merge_scn(info.major_mv_merge_scn_safe_calc_))) {
        LOG_WARN("failed to set_major_mv_merge_scn", K(ret), K(info), K(min_major_mv_merge_scn), KPC(ls));
      } else {
        info.major_mv_merge_scn_ = info.major_mv_merge_scn_safe_calc_;
      }
    } else if (min_major_mv_merge_scn >= info.major_mv_merge_scn_safe_calc_) {
      if (OB_FAIL(ls->set_major_mv_merge_scn(info.major_mv_merge_scn_safe_calc_))) {
        LOG_WARN("failed to set_major_mv_merge_scn", K(ret), K(info), K(min_major_mv_merge_scn), KPC(ls));
      } else {
        info.major_mv_merge_scn_ = info.major_mv_merge_scn_safe_calc_;
      }
    }
  }
  return ret;
}

int ObMVCheckReplicaHelper::get_merge_info(
      const share::ObLSID &ls_id,
      ObMajorMVMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is not valid", KR(ret), K(ls_id));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(NULL == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), KPC(ls));
  } else if (FALSE_IT(info = ls->get_ls_meta().get_major_mv_merge_info())) {
  } else if (!info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info is invalid", KR(ret), K(info));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_RECONFIG_CHECK_FAILED);

int ObMVCheckReplicaHelper::check_can_add_member(
      const common::ObAddr &server,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const uint64_t rpc_timeout)
{
  int ret = OB_SUCCESS;

  ObMajorMVMergeInfo leader_merge_info;
  ObMajorMVMergeInfo member_merge_info;
  uint64_t data_version = 0;
  if (!server.is_valid() || tenant_id == OB_INVALID_TENANT_ID || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tenant_id), K(server));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_4_0)) {
    // skip ls reconfig check
  } else if (ls_id.is_sys_ls()) {
    // do nothing
  } else if (OB_FAIL(rootserver::ObCollectMvMergeInfoTask::sync_get_ls_member_merge_info(server,
                                                                                  tenant_id,
                                                                                  ls_id,
                                                                                  member_merge_info,
                                                                                  rpc_timeout,
                                                                                  false/*check_leader*/))) {
    LOG_WARN("sync get ls member merge info failed", K(ret),
             K(server), K(ls_id), K(tenant_id));
  } else if (OB_FAIL(get_merge_info(ls_id, leader_merge_info))) {
    LOG_WARN("get and update local merge info failed", K(ret), K(ls_id));
  } else if (leader_merge_info.major_mv_merge_scn_ > member_merge_info.major_mv_merge_scn_) {
    ret = NEW_MV_MAJOR_VERSION_NOT_MATCH;
    LOG_WARN("ls mv merge scn is small than leader", K(ret),
             K(leader_merge_info), K(ls_id), K(member_merge_info));
  } else {
    LOG_INFO("ls reconfig check success",  K(ret),
              K(leader_merge_info), K(ls_id), K(member_merge_info));
  }
  if (NEW_MV_MAJOR_VERSION_NOT_MATCH == ret) {
    if (OB_FAIL(rootserver::ObCollectMvMergeInfoTask::sync_get_ls_member_merge_info(server,
                                                                                  tenant_id,
                                                                                  ls_id,
                                                                                  member_merge_info,
                                                                                  rpc_timeout,
                                                                                  false/*check_leader*/,
                                                                                  true/*need_update*/))) {
      LOG_WARN("sync get ls member merge info failed", K(ret),
              K(server), K(ls_id), K(tenant_id));
    } else if (leader_merge_info.major_mv_merge_scn_ > member_merge_info.major_mv_merge_scn_) {
      ret = NEW_MV_MAJOR_VERSION_NOT_MATCH;
      LOG_WARN("ls mv merge scn is small than leader", K(ret),
              K(leader_merge_info), K(ls_id), K(member_merge_info));
    }
    LOG_INFO("push dest mv merge scn to avoid failed", K(ret), K(tenant_id), K(server),
             K(ls_id), K(member_merge_info), K(leader_merge_info));
  }
  if (OB_UNLIKELY(ERRSIM_RECONFIG_CHECK_FAILED == NEW_MV_MAJOR_VERSION_NOT_MATCH) && !ls_id.is_sys_ls()) {
    ret = NEW_MV_MAJOR_VERSION_NOT_MATCH;
    LOG_INFO("error sim to check failed",  K(ret),
             K(leader_merge_info), K(ls_id), K(member_merge_info));
  }
  return ret;
}

}
}
