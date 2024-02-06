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

#ifndef STORAGE_COMPACTION_OB_TABLET_MERGE_CHECKER_H_
#define STORAGE_COMPACTION_OB_TABLET_MERGE_CHECKER_H_

#include "storage/compaction/ob_compaction_util.h"
#include "stdint.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObLS;
}

namespace compaction
{
class ObTabletMergeChecker
{
public:
  static int check_need_merge(const compaction::ObMergeType merge_type, const storage::ObTablet &tablet);
  static int check_could_merge_for_medium(
    const storage::ObTablet &tablet,
    bool &could_schedule_merge);
  static int check_ls_state(storage::ObLS &ls, bool &need_merge);
  static int check_ls_state_in_major(storage::ObLS &ls, bool &need_merge);
private:
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
};
} // namespace compaction
} // namespace oceanbase

#endif // STORAGE_COMPACTION_OB_TABLET_MERGE_CHECKER_H_
