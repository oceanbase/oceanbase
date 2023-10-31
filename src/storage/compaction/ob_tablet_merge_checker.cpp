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

#include "storage/compaction/ob_tablet_merge_checker.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE_COMPACTION

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace compaction
{
int ObTabletMergeChecker::check_need_merge(const ObMergeType merge_type, const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  bool need_merge = true;

  if (OB_UNLIKELY(merge_type <= ObMergeType::INVALID_MERGE_TYPE
      || merge_type >= ObMergeType::MERGE_TYPE_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge type is invalid", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (!is_minor_merge(merge_type)
      && !is_mini_merge(merge_type)
      && !is_major_merge(merge_type)
      && !is_medium_merge(merge_type)) {
    need_merge = true;
  } else {
    const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
    const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
    bool is_empty_shell = tablet.is_empty_shell();
    if (is_minor_merge(merge_type) || is_mini_merge(merge_type)) {
      need_merge = !is_empty_shell;
    } else if (is_major_merge(merge_type) || is_medium_merge(merge_type)) {
      need_merge = tablet.is_data_complete();
    }

    if (OB_FAIL(ret)) {
    } else if (!need_merge) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("tablet has no need to merge", K(ret), K(ls_id), K(tablet_id),
          "merge_type", merge_type_to_str(merge_type), K(is_empty_shell));
    }
  }

  return ret;
}

int ObTabletMergeChecker::check_could_merge_for_medium(
  const ObTablet &tablet,
  bool &could_schedule_merge)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  bool committed_flag = false;
  could_schedule_merge = true;
  if (OB_FAIL(tablet.ObITabletMdsInterface::get_latest_tablet_status(user_data, committed_flag))) {
    LOG_WARN("failed to get tablet status", K(ret), K(tablet), K(user_data));
  } else if (ObTabletStatus::TRANSFER_OUT == user_data.tablet_status_
    || ObTabletStatus::TRANSFER_OUT_DELETED == user_data.tablet_status_) {
    could_schedule_merge = false;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("tablet status is TRANSFER_OUT or TRANSFER_OUT_DELETED, merging is not allowed", K(user_data), K(tablet));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
