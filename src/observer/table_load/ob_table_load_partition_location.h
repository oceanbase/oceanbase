// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

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
  ObTableLoadPartitionLocation() : is_inited_(false) {}
  int init(uint64_t tenant_id,
           const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids,
           common::ObIAllocator &allocator);
  int get_leader(common::ObTabletID tablet_id, PartitionLocationInfo &info) const;
  int get_all_leader(table::ObTableLoadArray<common::ObAddr> &addr_array) const;
  int get_all_leader_info(table::ObTableLoadArray<LeaderInfo> &info_array) const;
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
    uint64_t tenant_id, const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids,
    common::ObIAllocator &allocator);
  int init_all_leader_info(common::ObIAllocator &allocator);
  int fetch_ls_locations(
    uint64_t tenant_id,
    const table::ObTableLoadArray<table::ObTableLoadPartitionId> &partition_ids);
 private:
  common::ObArray<common::ObTabletID> tablet_ids_; //保证遍历partition_map_的时候顺序不变
  common::hash::ObHashMap<common::ObTabletID, PartitionLocationInfo> partition_map_;
  table::ObTableLoadArray<common::ObAddr> all_leader_addr_array_;
  table::ObTableLoadArray<LeaderInfo> all_leader_info_array_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
