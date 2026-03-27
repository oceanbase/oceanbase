/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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