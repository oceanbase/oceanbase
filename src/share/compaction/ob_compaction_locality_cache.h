//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_
#define OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_

#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_table_operator.h"
#include "deps/oblib/src/lib/net/ob_addr.h"
#include "deps/oblib/src/common/ob_zone.h"
#include "rootserver/freeze/ob_major_merge_info_manager.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{

class ObCompactionLocalityCache
{
public:
  ObCompactionLocalityCache();
  ~ObCompactionLocalityCache();
  int init(const uint64_t tenant_id, rootserver::ObMajorMergeInfoManager *merge_info_mgr = nullptr);
  void destroy();
  bool empty() const { return ls_infos_map_.empty(); }
  int refresh_ls_locality(const bool force_refresh);
  int get_ls_info(const share::ObLSID &ls_id, share::ObLSInfo &ls_info);
  TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  const int64_t CHECK_LS_LOCALITY_INTERVAL = 5 * 60 * 1000 * 1000L; // 5 mins
  int get_zone_list_from_inner_table(ObIArray<common::ObZone> &zone_list);
  int str2zone_list(
      const char *str,
      ObIArray<common::ObZone> &zone_list);
  int refresh_by_zone(
      const share::ObLSInfo &ls_info,
      const ObIArray<common::ObZone> &zone_list);
  bool replica_in_zone_list(
      const ObLSReplica &ls_replica,
      const ObIArray<common::ObZone> &zone_list);
  int inner_refresh_ls_locality();
  int build_member_list_set(
    const ObLSReplica &tmp_replica,
    ObIArray<common::ObAddr> &member_list_set);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  rootserver::ObMajorMergeInfoManager *merge_info_mgr_;
  common::hash::ObHashMap<share::ObLSID, share::ObLSInfo> ls_infos_map_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_COMPACTION_LOCALITY_CACHE_H_
