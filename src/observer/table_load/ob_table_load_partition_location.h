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

#pragma once

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "share/ob_ls_id.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace share
{
class ObLSLocation;
}  // namespace share
namespace storage
{
class ObTabletHandle;
}  // namespace storage
namespace observer
{

class ObTableLoadPartitionLocation
{
public:
  struct PartitionLocationInfo
  {
    table::ObTableLoadLSIdAndPartitionId partition_id_;
    common::ObAddr leader_addr_;
    TO_STRING_KV(K_(partition_id), K_(leader_addr));
  };
  struct LeaderInfo
  {
    common::ObAddr addr_;
    table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> partition_id_array_;
    TO_STRING_KV(K_(addr), K_(partition_id_array));
  };
  struct LeaderInfoForSort
  {
    common::ObAddr addr_;
    common::ObIArray<table::ObTableLoadLSIdAndPartitionId> *partition_id_array_ptr_;
    TO_STRING_KV(K_(addr), KP_(partition_id_array_ptr));
  };
public:
  ObTableLoadPartitionLocation()
    : allocator_("TLD_PL"),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    tablet_ids_.set_tenant_id(MTL_ID());
  }
  int init(uint64_t tenant_id,
           const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids);
  int get_leader(common::ObTabletID tablet_id, PartitionLocationInfo &info) const;
  int get_all_leader(table::ObTableLoadArray<common::ObAddr> &addr_array) const;
  int get_all_leader_info(table::ObTableLoadArray<LeaderInfo> &info_array) const;
  void reset() {
    tablet_ids_.reset();
    partition_map_.destroy();
    all_leader_addr_array_.reset();
    all_leader_info_array_.reset();
    allocator_.reset();
    is_inited_ = false;
  }
  int check_tablet_has_same_leader(const ObTableLoadPartitionLocation &other, bool &result);

public:
  // 通过tablet_id获取
  static int fetch_ls_id(uint64_t tenant_id, const common::ObTabletID &tablet_id,
                         share::ObLSID &ls_id);
  static int fetch_ls_location(uint64_t tenant_id, const common::ObTabletID &tablet_id,
                               share::ObLSLocation &ls_location, share::ObLSID &ls_id);
  static int fetch_location_leader(uint64_t tenant_id, const common::ObTabletID &tablet_id,
                                   PartitionLocationInfo &info);
  static int fetch_tablet_handle(uint64_t tenant_id, const share::ObLSID &ls_id,
                                 const common::ObTabletID &tablet_id,
                                 storage::ObTabletHandle &tablet_handle);
  static int fetch_tablet_handle(uint64_t tenant_id, const common::ObTabletID &tablet_id,
                                 storage::ObTabletHandle &tablet_handle);
private:
  int init_all_partition_location(
    uint64_t tenant_id, const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids);
  int init_all_leader_info();
  int fetch_ls_locations(
    uint64_t tenant_id,
    const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids);
 private:
  common::ObArenaAllocator allocator_;
  common::ObArray<common::ObTabletID> tablet_ids_; //保证遍历partition_map_的时候顺序不变
  common::hash::ObHashMap<common::ObTabletID, PartitionLocationInfo> partition_map_;
  table::ObTableLoadArray<common::ObAddr> all_leader_addr_array_;
  table::ObTableLoadArray<LeaderInfo> all_leader_info_array_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
