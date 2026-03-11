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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_MACRO_BLOCK_COLLECTOR_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_MACRO_BLOCK_COLLECTOR_H_

#ifdef OB_BUILD_SHARED_STORAGE
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/high_availability/ob_ss_ha_macro_copy_task_generator.h"

namespace oceanbase
{
namespace rootserver
{

// TODO(haoyuanke.hyk): implement this collector later
// Macro block information collector: collect macro block info needed for backup from SSLOG table
class ObMacroBlockCollector final
{
public:
  ObMacroBlockCollector() = default;
  ~ObMacroBlockCollector() = default;

  // Collect macro blocks using ObSSHAMacroCopyTaskGenerator
  static int collect_macro_blocks_with_generator(
      const ObSSHAMacroTaskType &task_type,
      const uint64_t tenant_id,
      const int64_t job_id,
      const share::ObBackupDest &backup_dest,
      const share::SCN &snapshot_scn,
      const common::ObIArray<share::ObLSID> &ls_id_array,
      share::ObHATransEndHelper *trans_end_helper,
      int64_t &total_task_cnt,
      int64_t &total_macro_cnt,
      int64_t &total_bytes);

  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockCollector);
};

} // namespace rootserver
} // namespace oceanbase
#endif

#endif // OCEANBASE_ROOTSERVER_BACKUP_OB_MACRO_BLOCK_COLLECTOR_H_
