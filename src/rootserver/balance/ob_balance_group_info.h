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

#include "lib/ob_define.h"                    // OB_MALLOC_NORMAL_BLOCK_SIZE
#include "lib/allocator/ob_allocator.h"       // ObIAllocator
#include "share/transfer/ob_transfer_info.h"  // ObTransferPartInfo, ObTransferPartList
#include "ob_balance_group_define.h"          // ObBalanceGroupID
#include "ob_part_group_container.h"          // ObIPartGroupContainer

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
  explicit ObBalanceGroupInfo(common::ObIAllocator &alloc) :
      inited_(false),
      bg_id_(),
      last_part_group_uid_(OB_INVALID_ID),
      last_part_group_(nullptr),
      alloc_(alloc),
      pg_container_(nullptr) {}

  ~ObBalanceGroupInfo();
  // for partition balance: ls_num is the number of LS during partition balance
  // for LS balance: ls_num is the number of LS after LS balance
  int init(
      const ObBalanceGroupID &bg_id,
      const share::ObLSID &ls_id,
      const int64_t balanced_ls_num,
      const ObPartDistributionMode &part_distribution_mode);
  bool is_valid() const { return inited_; }
  const ObBalanceGroupID& get_bg_id() const { return bg_id_; }
  const share::ObLSID& get_ls_id() const { return ls_id_; }
  int64_t get_part_group_count() const { return nullptr == pg_container_ ? 0 : pg_container_->count(); }

  // append partition at the newest partition group. create new partition group if needed
  //
  // @param [in] table_schema                 the table schema of the table which the partition
  //                                          belongs to
  // @param [in] part_group_uid               partition group unique id
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  //
  // @return OB_SUCCESS             success
  // @return OB_ENTRY_NOT_EXIST     no partition group found
  // @return other                  fail
  int append_part(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid,
      const share::ObTransferPartInfo &part,
      const int64_t data_size);
  int get_largest_part_group(ObIPartGroupInfo *&pg_info) const;
  int get_smallest_part_group(ObIPartGroupInfo *&pg_info) const;
  int transfer_out(ObBalanceGroupInfo &dest_bg_info, ObIPartGroupInfo *&pg_info);
  int remove_part_group(const ObIPartGroupInfo &pg_info);
  int append_part_group(const ObIPartGroupInfo &pg_info);

  TO_STRING_KV(K_(bg_id), K_(ls_id), "part_group_count", get_part_group_count(), KPC_(pg_container));

private:
  int create_part_group_container_(
      const ObBalanceGroupID &bg_id,
      const int64_t balanced_ls_num,
      const ObPartDistributionMode &part_distribution_mode);
  int create_new_part_group_if_needed_(const share::schema::ObSimpleTableSchemaV2 &table_schema,
                                      const uint64_t part_group_uid);

private:
  bool inited_;
  ObBalanceGroupID bg_id_;
  share::ObLSID ls_id_;
  int64_t last_part_group_uid_; // unique id of the last part group in part_groups_
  ObTransferPartGroup *last_part_group_;
  ObIAllocator &alloc_; // allocator for ObTransferPartGroup
  ObIPartGroupContainer *pg_container_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H */
