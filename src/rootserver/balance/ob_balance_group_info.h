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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H
#define OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H

#include "lib/container/ob_array.h"           //ObArray
#include "lib/ob_define.h"                    // OB_MALLOC_NORMAL_BLOCK_SIZE
#include "lib/allocator/ob_allocator.h"       // ObIAllocator
#include "share/transfer/ob_transfer_info.h"  // ObTransferPartInfo, ObTransferPartList
#include "ob_balance_group_define.h"          //ObBalanceGroupID
#include "lib/allocator/page_arena.h"         // ModulePageAllocator

namespace oceanbase
{
namespace rootserver
{

// A group of partitions that should be distributed on the same LS and transfered together
class ObTransferPartGroup
{
public:
  ObTransferPartGroup() :
      data_size_(0),
      part_list_("PartGroup") {}

  ObTransferPartGroup(common::ObIAllocator &alloc) :
      data_size_(0),
      part_list_(alloc, "PartGroup") {}

  ~ObTransferPartGroup() {
    data_size_ = 0;
    part_list_.reset();
  }

  int64_t get_data_size() const { return data_size_; }
  const share::ObTransferPartList &get_part_list() const { return part_list_; }
  int64_t count() const { return part_list_.count(); }

  // add new partition into partition group
  int add_part(const share::ObTransferPartInfo &part, int64_t data_size);

  TO_STRING_KV(K_(data_size), K_(part_list));
private:
  int64_t data_size_;
  share::ObTransferPartList part_list_;
};

// Balance Group Partition Info
//
// A group of Partition Groups (ObTransferPartGroup) that should be evenly distributed on all LS.
class ObBalanceGroupInfo final
{
public:
  explicit ObBalanceGroupInfo(const ObBalanceGroupID &id, common::ObIAllocator &alloc) :
      id_(id),
      last_part_group_uid_(OB_INVALID_ID),
      alloc_(alloc),
      part_groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PartGroupArray"))
  {
  }

  ~ObBalanceGroupInfo();

  bool is_valid() { return id_.is_valid(); }
  const ObBalanceGroupID &id() const { return id_; }
  const common::ObArray<ObTransferPartGroup *> get_part_groups() const { return part_groups_; }
  int64_t get_part_group_count() const { return part_groups_.count(); }

  // append partition at the newest partition group. create new partition group if needed
  //
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  // @param [in] part_group_uid               partition group unique id
  //
  // @return OB_SUCCESS         success
  // @return OB_ENTRY_EXIST     no partition group found
  // @return other              fail
  int append_part(share::ObTransferPartInfo &part,
      const int64_t data_size,
      const uint64_t part_group_uid);

  // pop partition groups from back of array, and push back into part list
  //
  // @param [in] part_group_count           partition group count that need be popped
  // @param [in/out] part_list              push popped part into the part list
  // @param [out] popped_part_count         popped partition count
  int pop_back(const int64_t part_group_count,
      share::ObTransferPartList &part,
      int64_t &popped_part_count);

  TO_STRING_KV(K_(id), "part_group_count", part_groups_.count());

private:
  int create_new_part_group_if_needed_(const uint64_t part_group_uid);

private:
  ObBalanceGroupID id_;
  int64_t last_part_group_uid_; // unique id of the last part group in part_groups_
  ObIAllocator &alloc_; // allocator for ObTransferPartGroup
  // Partition Group Array
  common::ObArray<ObTransferPartGroup *> part_groups_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H */
