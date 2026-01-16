/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #define private public
 #define protected public

#include "lib/ob_errno.h"
#include "storage/high_availability/ob_tablet_copy_dependency_mgr.h"

namespace oceanbase
{
namespace unittest
{
struct TabletDependencyMgrUtils
{
  static int remove_tablets_from_dep_mgr(ObTabletCopyDependencyMgr &dep_mgr, const ObIArray<ObLogicTabletID> &tablet_ids);
  static void update_and_check_expected_tablet_count(ObTabletCopyDependencyMgr &dep_mgr,
      const ObIArray<ObLogicTabletID> &tablet_ids, int64_t &expected_tablet_count);
};

}; // namespace storage
}; // namespace oceanbase