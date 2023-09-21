// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEABASE_STORAGE_HA_UTILS_H_
#define OCEABASE_STORAGE_HA_UTILS_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "ob_storage_ha_struct.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{

class ObStorageHAUtils
{
public:
  static int get_ls_leader(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader_addr);
  static int check_tablet_replica_validity(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObAddr &addr, const common::ObTabletID &tablet_id, common::ObISQLClient &sql_client);
  static int report_ls_meta_table(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const storage::ObMigrationStatus &migration_status);
  static int get_server_version(uint64_t &server_version);
  static int check_server_version(const uint64_t server_version);
  static int check_ls_deleted(
      const share::ObLSID &ls_id,
      bool &is_deleted);

  // When the src_ls of the transfer does not exist, it is necessary to check whether the dest_ls can be rebuilt
  static int check_transfer_ls_can_rebuild(
      const share::SCN replay_scn,
      bool &need_rebuild);
  static int get_readable_scn_with_retry(share::SCN &readable_scn);
  static int64_t get_rpc_timeout();
  static int check_is_primary_tenant(const uint64_t tenant_id, bool &is_primary_tenant);
  static int check_disk_space();

private:
  static int check_merge_error_(const uint64_t tenant_id, common::ObISQLClient &sql_client);
  static int fetch_src_tablet_meta_info_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const common::ObAddr &src_addr, common::ObISQLClient &sql_client,
    share::SCN &compaction_scn);
  static int check_tablet_replica_checksum_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const share::SCN &compaction_scn, common::ObISQLClient &sql_client);
  static int get_readable_scn_(share::SCN &readable_scn);
};

struct ObTransferUtils
{
  static bool is_need_retry_error(const int err);
  static int block_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int kill_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int unblock_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts_scn);
  static int get_gts(const uint64_t tenant_id, share::SCN &gts);
  static void set_transfer_module();
  static void clear_transfer_module();
};

} // end namespace storage
} // end namespace oceanbase

#endif
