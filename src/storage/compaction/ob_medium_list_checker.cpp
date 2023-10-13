//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"

namespace oceanbase
{
namespace compaction
{

int ObMediumListChecker::validate_medium_info_list(
    const ObExtraMediumInfo &extra_info,
    const MediumInfoArray *medium_info_array,
    const int64_t last_major_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t next_medium_info_idx = 0;
  if (OB_FAIL(check_extra_info(extra_info, last_major_snapshot))) {
    LOG_WARN("failed to check extra info", KR(ret), K(last_major_snapshot), K(extra_info));
  } else if (nullptr == medium_info_array || medium_info_array->empty()) {
    // do nothing
  } else if (OB_FAIL(filter_finish_medium_info(*medium_info_array, last_major_snapshot, next_medium_info_idx))) {
    LOG_WARN("failed to filter finish medium info", KR(ret), K(last_major_snapshot), K(next_medium_info_idx));
  } else if (next_medium_info_idx >= medium_info_array->count()) {
    // do nothing
  } else if (OB_FAIL(check_continue(*medium_info_array, next_medium_info_idx))) {
    LOG_WARN("failed to check medium list continue", KR(ret), K(last_major_snapshot), KPC(medium_info_array));
  } else if (OB_FAIL(check_next_schedule_medium(medium_info_array->at(next_medium_info_idx), last_major_snapshot, false/*force_check*/))) {
    LOG_WARN("failed to check next schedule medium info", KR(ret), K(last_major_snapshot), KPC(medium_info_array), K(next_medium_info_idx));
  }
  return ret;
}

int ObMediumListChecker::check_continue(
  const MediumInfoArray &medium_info_array,
    const int64_t start_check_idx)
{
  int ret = OB_SUCCESS;
  const ObMediumCompactionInfo *first_info = nullptr;
  if (medium_info_array.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(start_check_idx >= medium_info_array.count()
      || nullptr == (first_info = medium_info_array.at(start_check_idx)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_check_idx), K(medium_info_array), KPC(first_info));
  } else if (!first_info->from_cur_cluster()) {
    // not from current cluster, maybe standby cluster,
    // no need to check medium info continuity
  } else {
    int64_t prev_medium_snapshot = first_info->medium_snapshot_;
    const ObMediumCompactionInfo *info = nullptr;
    for (int64_t idx = start_check_idx + 1; OB_SUCC(ret) && idx < medium_info_array.count(); ++idx) {
      info = medium_info_array.at(idx);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info ist null", K(ret), KPC(info), K(idx), K(medium_info_array));
      } else if (OB_UNLIKELY(prev_medium_snapshot != info->last_medium_snapshot_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info list is not continuous", K(ret), K(prev_medium_snapshot),
            "last_medium_snapshot", info->last_medium_snapshot_,
            K(medium_info_array));
      } else {
        prev_medium_snapshot = info->medium_snapshot_;
      }
    } // end of for
  }
  return ret;
}

int ObMediumListChecker::filter_finish_medium_info(
    const MediumInfoArray &medium_info_array,
    const int64_t last_major_snapshot,
    int64_t &next_medium_info_idx)
{
  int ret = OB_SUCCESS;
  next_medium_info_idx = 0;

  const ObMediumCompactionInfo *info = nullptr;
  int64_t idx = 0;
  for ( ; OB_SUCC(ret) && idx < medium_info_array.count(); ++idx) {
    info = medium_info_array.at(idx);
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("medium info ist null", K(ret), KPC(info), K(idx), K(medium_info_array));
    } else if (info->medium_snapshot_ > last_major_snapshot) {
      break;
    }
  } // end of for
  if (OB_SUCC(ret)) {
    next_medium_info_idx = idx;
  }
  return ret;
}

int ObMediumListChecker::check_extra_info(
  const ObExtraMediumInfo &extra_info,
  const int64_t last_major_snapshot)
{
  int ret = OB_SUCCESS;
  if (last_major_snapshot > 0 && extra_info.last_medium_scn_ > 0) {
    if (OB_UNLIKELY(extra_info.last_medium_scn_ != last_major_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("medium list is invalid for last major sstable", K(ret), K(extra_info), K(last_major_snapshot));
    }
  }
  return ret;
}

int ObMediumListChecker::check_next_schedule_medium(
    const ObMediumCompactionInfo *next_medium_info,
    const int64_t last_major_snapshot,
    const bool force_check)
{
  int ret = OB_SUCCESS;
  if (nullptr != next_medium_info
      && last_major_snapshot > 0
      && ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2 == next_medium_info->medium_compat_version_
      && next_medium_info->medium_snapshot_ > last_major_snapshot) {
    if (next_medium_info->from_cur_cluster()) { // same cluster_id & same tenant_id
      if (OB_UNLIKELY(next_medium_info->last_medium_snapshot_ != last_major_snapshot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("last medium snapshot in medium info is not equal to last "
                 "major sstable, medium info may lost",
                 KR(ret), KPC(next_medium_info), K(last_major_snapshot));
      }
    } else if (next_medium_info->is_major_compaction()) { // check next freeze info in inner_table & medium_info
      ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
      if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(
              get_freeze_info_behind_snapshot_version,
              last_major_snapshot, freeze_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get freeze info", K(ret), K(last_major_snapshot));
        } else if (force_check) {
          ret = OB_EAGAIN;
          LOG_WARN("next freeze info is not exist yet, need to check after refresh freeze info",
                   KR(ret), KPC(next_medium_info), K(last_major_snapshot));
        } else { // if force_check = false, not return errno; check next time
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(freeze_info.freeze_version < next_medium_info->medium_snapshot_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("next major medium info may lost",
          KR(ret), "freeze_version", freeze_info.freeze_version, KPC(next_medium_info), K(last_major_snapshot));
      }
    } else {
      // medium info from same cluster_id, can't make sure all medium info exists, so not check medium & last_major_snapshot
      // like primary-standby relations: cluster1(A) -> cluster2(B) -> cluster1(C), medium info is dropped by B for different cluster_id
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
