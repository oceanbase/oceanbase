/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "observer/table_load/backup/ob_table_load_physical_backup_table.h"
#include "observer/table_load/backup/ob_table_load_logical_backup_table.h"

namespace oceanbase
{
namespace observer
{
using namespace table_load_backup;

int ObTableLoadBackupTable::get_table(const ObTableLoadBackupVersion &backup_version,
                                      ObTableLoadBackupTable *&table,
                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  table = nullptr;
  if (OB_UNLIKELY(backup_version <= ObTableLoadBackupVersion::INVALID ||
                  backup_version >= ObTableLoadBackupVersion::MAX_VERSION)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(backup_version));
  } else {
    switch (backup_version) {
      case ObTableLoadBackupVersion::V_1_4:
      case ObTableLoadBackupVersion::V_2_X_LOG:
        if (OB_ISNULL(table = OB_NEWx(ObTableLoadLogicalBackupTable, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        }
        break;
      case ObTableLoadBackupVersion::V_2_X_PHY:
      case ObTableLoadBackupVersion::V_3_X: {
        if (OB_ISNULL(table = OB_NEWx(ObTableLoadPhysicalBackupTable, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not support version", KR(ret), K(backup_version));
        break;
      }
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
