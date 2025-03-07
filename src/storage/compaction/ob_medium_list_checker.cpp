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
ERRSIM_POINT_DEF(EN_SKIP_CHECK_MEDIUM_LIST);

int ObMediumListChecker::validate_medium_info_list(
    const ObExtraMediumInfo &extra_info,
    const MediumInfoArray *medium_info_array,
    const int64_t last_major_snapshot)
{
  int ret = OB_SUCCESS;
  bool skip_validate = false;
#ifdef ERRSIM
  ret = EN_SKIP_CHECK_MEDIUM_LIST ? : OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    skip_validate = true;
    FLOG_INFO("EN_SKIP_CHECK_MEDIUM_LIST, skip check medium info list",
              K(ret), K(last_major_snapshot), K(extra_info), KPC(medium_info_array));
  }
#endif

  if (skip_validate) {
    // do nothing
  } else if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(inner_check_medium_list_for_ss(extra_info, medium_info_array, last_major_snapshot))) {
      LOG_WARN("failed to inner check medium info list for ss", K(ret));
    }
#endif
  } else if (OB_FAIL(inner_check_medium_list(extra_info, medium_info_array, last_major_snapshot))) {
    LOG_WARN("failed to inner check medium info list", K(ret));
  }
  return ret;
}

int ObMediumListChecker::inner_check_medium_list(
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
  } else if (OB_FAIL(check_next_schedule_medium(*medium_info_array->at(next_medium_info_idx), last_major_snapshot, false/*force_check*/))) {
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
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot,
    const bool force_check)
{
  int ret = OB_SUCCESS;
  if (last_major_snapshot > 0 &&
      ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2 <= next_medium_info.medium_compat_version_ &&
      next_medium_info.medium_snapshot_ > last_major_snapshot) {
    if (next_medium_info.from_cur_cluster()) { // same cluster_id & same tenant_id
      if (OB_UNLIKELY(next_medium_info.last_medium_snapshot_ != last_major_snapshot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("last medium snapshot in medium info is not equal to last "
                 "major sstable, medium info may lost",
                 KR(ret), K(next_medium_info), K(last_major_snapshot));
      }
    } else if (next_medium_info.is_major_compaction()) { // check next freeze info in inner_table & medium_info
      share::ObFreezeInfo freeze_info;

      if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version,
                                           last_major_snapshot,
                                           freeze_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get freeze info", K(ret), K(last_major_snapshot));
        } else if (force_check) {
          ret = OB_EAGAIN;
          LOG_WARN("next freeze info is not exist yet, need to check after refresh freeze info",
                   KR(ret), K(next_medium_info), K(last_major_snapshot));
        } else { // if force_check = false, not return errno; check next time
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(freeze_info.frozen_scn_.get_val_for_tx() < next_medium_info.medium_snapshot_)) {
        if (next_medium_info.is_skip_tenant_major_) {
          // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
          FLOG_INFO("medium info have marked skip tenant major, may have schema issue when schedule tenant major", KR(ret), K(next_medium_info), K(freeze_info));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("next major medium info may lost",
            KR(ret), "freeze_version", freeze_info.frozen_scn_.get_val_for_tx(), K(next_medium_info), K(last_major_snapshot));
        }
      }
    } else {
      // medium info from same cluster_id, can't make sure all medium info exists, so not check medium & last_major_snapshot
      // like primary-standby relations: cluster1(A) -> cluster2(B) -> cluster1(C), medium info is dropped by B for different cluster_id
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMediumListChecker::check_next_schedule_medium_for_ss(
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot)
{
  int ret = OB_SUCCESS;
  if (last_major_snapshot > 0 &&
      ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2 <= next_medium_info.medium_compat_version_ &&
      next_medium_info.medium_snapshot_ > last_major_snapshot) {
    if (OB_UNLIKELY(!next_medium_info.is_major_compaction())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get unexpected medium compaction info in share storage mode", K(ret));
    } else if (!next_medium_info.cluster_id_equal()) {
      // do nothing
    } else if (-1 == next_medium_info.last_medium_snapshot_
            && ObAdaptiveMergePolicy::DURING_DDL == next_medium_info.medium_merge_reason_) {
      // ignore the medium info submitted when tablet has no major
    } else if (OB_UNLIKELY(next_medium_info.last_medium_snapshot_ != last_major_snapshot)) {
      // v1(major sstable), v2(skip merge, generate new major), v3(medium_info, last_medium_snapshot_ = v2)
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last medium snapshot in medium info is not equal to last major sstable, need refresh",
               KR(ret), K(next_medium_info), K(last_major_snapshot));
    }
  }
  return ret;
}

int ObMediumListChecker::inner_check_medium_list_for_ss(
    const ObExtraMediumInfo &extra_info,
    const MediumInfoArray *medium_info_array,
    const int64_t last_major_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t next_medium_info_idx = 0;

  if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected storage mode", K(ret));
  } else if (OB_FAIL(check_extra_info(extra_info, last_major_snapshot))) {
    LOG_WARN("failed to check extra info", KR(ret), K(last_major_snapshot), K(extra_info));
  } else if (nullptr == medium_info_array || medium_info_array->empty() || 0 == last_major_snapshot) {
    // do nothing
  } else if (OB_FAIL(filter_finish_medium_info(*medium_info_array, last_major_snapshot, next_medium_info_idx))) {
    LOG_WARN("failed to filter finish medium info", KR(ret), K(last_major_snapshot), K(next_medium_info_idx));
  } else if (next_medium_info_idx >= medium_info_array->count()) {
    // do nothing
  } else if (OB_FAIL(check_continue_for_ss(*medium_info_array, next_medium_info_idx))) {
    LOG_WARN("failed to check medium list continue", KR(ret), K(last_major_snapshot), KPC(medium_info_array));
  } else if (OB_FAIL(check_next_schedule_medium_for_ss(*medium_info_array->at(next_medium_info_idx), last_major_snapshot))) {
    LOG_WARN("failed to check next schedule medium info for ss", KR(ret), K(last_major_snapshot), KPC(medium_info_array), K(next_medium_info_idx));
  }
  return ret;
}

int ObMediumListChecker::check_continue_for_ss(
    const MediumInfoArray &medium_info_array,
    const int64_t start_check_idx)
{
  int ret = OB_SUCCESS;
  const ObMediumCompactionInfo *first_info = nullptr;

  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected storage mode", K(ret));
  } else if (medium_info_array.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(start_check_idx >= medium_info_array.count()
      || nullptr == (first_info = medium_info_array.at(start_check_idx)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_check_idx), K(medium_info_array), KPC(first_info));
  } else if (!first_info->from_cur_cluster()) {
    // not from current cluster, maybe standby cluster,
    // no need to check medium info continuity
  } else {
    int64_t prev_medium_snapshot = INT64_MAX;
    for (int64_t idx = start_check_idx; OB_SUCC(ret) && idx < medium_info_array.count(); ++idx) {
      const ObMediumCompactionInfo *info = medium_info_array.at(idx);
      if (OB_ISNULL(info = medium_info_array.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info is unexpected null", K(ret), KPC(info), K(idx), K(medium_info_array));
      } else if (ObAdaptiveMergePolicy::DURING_DDL == info->medium_merge_reason_) {
        /* DURING_DDL clog should be filtered, consider the following tablet:
         * last major was created by ddl kv, last major snapshot = 50, and the medium list is:
         * (DURING_DDL, 100, -1), (DURING_DDL, 200, 50), (NO_INC_DATA, 300, 50), (NO_INC_DATA, 400, 200)
         * we should check continue with the 3rd and the 4th medium info.
         */
      } else if (OB_UNLIKELY(INT64_MAX != prev_medium_snapshot && prev_medium_snapshot != info->last_medium_snapshot_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info list is not continuous", K(ret), K(prev_medium_snapshot),
                 "last_medium_snapshot", info->last_medium_snapshot_, K(medium_info_array));
      } else {
        prev_medium_snapshot = info->medium_snapshot_;
      }
    }
  }
  return ret;
}


#endif

} // namespace compaction
} // namespace oceanbase
