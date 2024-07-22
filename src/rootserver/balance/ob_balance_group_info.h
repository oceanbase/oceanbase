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
#include "lib/hash/ob_hashmap.h"              // ObHashMap

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

typedef common::ObArray<ObTransferPartGroup *> ObPartGroupBucket;

class ObBalanceGroupUnit
{
public:
  explicit ObBalanceGroupUnit(common::ObIAllocator &alloc) :
      inited_(false),
      alloc_(alloc),
      part_group_buckets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PGBuckets")),
      part_group_cnt_(0) {}
  ~ObBalanceGroupUnit();
  int init(const int64_t bucket_num);
  bool is_valid() const { return inited_; }
  int64_t get_part_group_count() const { return part_group_cnt_; }
  int append_part_group(const uint64_t part_group_uid, ObTransferPartGroup *const part_group);
  int append_part_group_into_bucket(
      const int64_t bucket_idx,
      ObTransferPartGroup *const part_group);
  int remove_part_group(const int64_t bucket_idx, const int64_t pg_idx);
  int transfer_out(ObBalanceGroupUnit &dst_unit, ObTransferPartGroup *&part_group);
  int get_largest_part_group(
      int64_t &bucket_idx,
      int64_t &pg_idx,
      ObTransferPartGroup *&part_group) const;
  int get_smallest_part_group(
      int64_t &bucket_idx,
      int64_t &pg_idx,
      ObTransferPartGroup *&part_group) const;

  TO_STRING_KV("part_group_count", part_group_cnt_, K_(part_group_buckets));

private:
  int get_transfer_out_bucket_(const ObBalanceGroupUnit &dst_unit, int64_t &bucket_idx) const;

private:
  bool inited_;
  ObIAllocator &alloc_;
  common::ObArray<ObPartGroupBucket> part_group_buckets_;
  int64_t part_group_cnt_;
};

// Balance Group Partition Info
//
// A group of Partition Groups (ObTransferPartGroup) that should be evenly distributed on all LS.
class ObBalanceGroupInfo final
{
public:
  explicit ObBalanceGroupInfo(common::ObIAllocator &alloc) :
      inited_(false),
      bg_id_(),
      last_part_group_uid_(OB_INVALID_ID),
      last_part_group_(NULL),
      alloc_(alloc),
      bg_units_(),
      part_group_cnt_(0),
      bucket_num_(0)
  {
  }

  ~ObBalanceGroupInfo();

  int init(const ObBalanceGroupID &bg_id, const share::ObLSID &ls_id, const int64_t ls_num);
  bool is_valid() const { return inited_; }
  const ObBalanceGroupID& get_bg_id() const { return bg_id_; }
  const share::ObLSID& get_ls_id() const { return ls_id_; }
  int64_t get_part_group_count() const { return part_group_cnt_; }

  // append partition at the newest partition group. create new partition group if needed
  //
  // @param [in] bg_unit_id                   the balance group unit which the new partition group
  //                                          belongs to
  // @param [in] part_group_uid               partition group unique id
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  //
  // @return OB_SUCCESS             success
  // @return OB_ENTRY_NOT_EXIST     no partition group found
  // @return other                  fail
  int append_part(
      const ObObjectID &bg_unit_id,
      const uint64_t part_group_uid,
      const share::ObTransferPartInfo &part,
      const int64_t data_size);

  // transfer out partition groups
  //
  // @param [in] part_group_count           partition group count that need be removed
  // @param [in/out] dst_bg_info            push transfered out part group into dst_bg_info
  // @param [in/out] part_list              push transfered out part into the part list
  // @param [out] removed_part_count        removed partition count
  int transfer_out(
      const int64_t part_group_count,
      ObBalanceGroupInfo &dst_bg_info,
      share::ObTransferPartList &part_list,
      int64_t &removed_part_count);

  class ObPartGroupIndex {
  public:
    ObPartGroupIndex() :
        inited_(false),
        bg_unit_id_(OB_INVALID_ID),
        bucket_idx_(OB_INVALID_INDEX),
        pg_idx_(OB_INVALID_INDEX) {}

    int init(const ObObjectID &bg_unit_id, const int64_t bucket_idx, const int64_t pg_idx);

    void reset()  {
      bg_unit_id_ = OB_INVALID_ID;
      bucket_idx_ = OB_INVALID_INDEX;
      pg_idx_ = OB_INVALID_INDEX;
      inited_ = false;
    }

    bool is_valid() const { return inited_; }

    const ObObjectID& bg_unit_id() const { return bg_unit_id_; }
    int64_t bucket_idx() const { return bucket_idx_; }
    int64_t pg_idx() const { return pg_idx_; }

    TO_STRING_KV(K_(bg_unit_id), K_(bucket_idx), K_(pg_idx));

  private:
    bool inited_;
    ObObjectID bg_unit_id_;
    int64_t bucket_idx_;
    int64_t pg_idx_;
  };

  int get_largest_part_group(ObPartGroupIndex &pg_index, ObTransferPartGroup *&part_group) const;
  int get_smallest_part_group(ObPartGroupIndex &pg_index, ObTransferPartGroup *&part_group) const;
  int transfer_out(ObBalanceGroupInfo &dest_bg_info, ObTransferPartGroup *&part_group);
  int remove_part_group(const ObPartGroupIndex &pg_index);
  int append_part_group(
      const ObObjectID &bg_unit_id,
      const int64_t bucket_idx,
      ObTransferPartGroup *const part_group);
  TO_STRING_KV(K_(bg_id), K_(ls_id), "part_group_count", part_group_cnt_, K_(bucket_num));

private:
  int create_new_part_group_if_needed_(const uint64_t part_group_uid, const ObObjectID &bg_unit_id);
  int get_or_create_bg_unit_(const ObObjectID &bg_unit_id, ObBalanceGroupUnit *&bg_unit);
  int get_transfer_out_unit_(const ObBalanceGroupInfo &dst_bg, ObObjectID &bg_unit_id) const;

private:
  const int64_t MAP_BUCKET_NUM = 100;
  bool inited_;
  ObBalanceGroupID bg_id_;
  share::ObLSID ls_id_;
  int64_t last_part_group_uid_; // unique id of the last part group in part_groups_
  ObTransferPartGroup *last_part_group_;
  ObIAllocator &alloc_; // allocator for ObTransferPartGroup
  // Balance Group Units
  hash::ObHashMap<ObObjectID, ObBalanceGroupUnit*> bg_units_;
  int64_t part_group_cnt_;
  int64_t bucket_num_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H */
