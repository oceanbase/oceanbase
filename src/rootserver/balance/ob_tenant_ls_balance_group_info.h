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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_LS_BALANCE_GROUP_INFO_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_LS_BALANCE_GROUP_INFO_H

#include "share/ob_ls_id.h"                 //ObLSID
#include "lib/hash/ob_hashmap.h"            //ObHashMap
#include "lib/ob_define.h"                  // OB_INVALID_TENANT_ID
#include "lib/allocator/page_arena.h"       // ObArenaAllocator

#include "ob_all_balance_group_builder.h"   // ObAllBalanceGroupBuilder
#include "ob_ls_balance_group_info.h"       // ObLSBalanceGroupInfo

namespace oceanbase
{
namespace rootserver
{

// Tenant All LS Balance Group Info
//
// Build current balance group info on every LS.
//
// NOTE: if partitions are not balanced, partitions of same partition group may locate on different LS
class ObTenantLSBalanceGroupInfo final : public ObAllBalanceGroupBuilder::NewPartitionCallback
{
public:
  ObTenantLSBalanceGroupInfo() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID), ls_bg_map_() {}
  ~ObTenantLSBalanceGroupInfo() { destroy(); }

  int init(const uint64_t tenant_id);
  void destroy();

  // build All LS Balance Group Info
  int build(const char *mod,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service);

  int get(const share::ObLSID &ls_id, ObLSBalanceGroupInfo *&ls_bg_info) const
  {
    return ls_bg_map_.get_refactored(ls_id, ls_bg_info);
  }

public:
  // for ObAllBalanceGroupBuilder
  // Handle new partition when building balance group
  virtual int on_new_partition(
      const ObBalanceGroup &bg,
      const common::ObObjectID table_id,
      const common::ObObjectID part_object_id,
      const common::ObTabletID tablet_id,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const int64_t tablet_size,
      const bool in_new_partition_group,
      const uint64_t part_group_uid);

  TO_STRING_KV(K_(inited), K_(tenant_id), "valid_ls_count", ls_bg_map_.size());

private:
  int create_new_ls_bg_info_(const share::ObLSID ls_id,
      ObLSBalanceGroupInfo *&ls_bg_info);

private:
  static const int64_t MAP_BUCKET_NUM = 100;

  bool                      inited_;
  uint64_t                  tenant_id_;
  common::ObArenaAllocator  alloc_;

  // map for all balance groups on tenant every LS
  // If LS is empty, it does not exist in this map
  common::hash::ObHashMap<share::ObLSID, ObLSBalanceGroupInfo *> ls_bg_map_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_LS_BALANCE_GROUP_INFO_H */
