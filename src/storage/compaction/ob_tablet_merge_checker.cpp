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
} // namespace compaction
} // namespace oceanbase
