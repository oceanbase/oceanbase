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
#include "ob_balance_group_define.h"          // ObBalanceGroupID
#include "ob_part_group_container.h"          // ObPartGroupContainer

namespace oceanbase
{
namespace rootserver
{
// Balance Group Partition Info
//
// A group of Partition Groups (ObPartGroupInfo) that should be evenly distributed on all LS.
class ObBalanceGroupInfo final
{
public:
  explicit ObBalanceGroupInfo(common::ObIAllocator &alloc)
      : inited_(false),
        bg_id_(),
        ls_id_(),
        last_part_group_(nullptr),
        pg_container_(nullptr),
        alloc_(alloc) {}

  ~ObBalanceGroupInfo();

  // for partition balance: ls_num is the number of LS during partition balance
  // for LS balance: ls_num is the number of LS after LS balance
  int init(
      const ObBalanceGroupID &bg_id,
      const share::ObLSID &ls_id,
      const int64_t balanced_ls_num);
  bool is_valid() const { return inited_ && OB_NOT_NULL(pg_container_); }
  const ObBalanceGroupID& get_bg_id() const { return bg_id_; }
  const share::ObLSID& get_ls_id() const { return ls_id_; }
  int64_t get_part_groups_count() const { return nullptr == pg_container_ ? 0 : pg_container_->get_part_group_count(); }
  int64_t get_part_groups_data_size() const { return nullptr == pg_container_ ? 0 : pg_container_->get_data_size();}
  int64_t get_part_groups_weight() const { return nullptr == pg_container_ ? 0 : pg_container_->get_balance_weight(); }

  // append partition at the newest partition group. create new partition group if needed
  //
  // @param [in] table_schema                 the table schema of the table which the partition belongs to
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  // @param [in] part_group_uid               partition group unique id
  // @param [in] balance_weight               balance weight of the partition
  //
  // @return OB_SUCCESS         success
  // @return OB_ENTRY_EXIST     no partition group found
  // @return other              fail
  int append_part(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const share::ObTransferPartInfo &part,
      const int64_t data_size,
      const uint64_t part_group_uid,
      const int64_t balance_weight);

  int transfer_out_by_round_robin(
      ObBalanceGroupInfo &dest_bg_info,
      ObPartGroupInfo *&part_group);
  int transfer_out_by_balance_weight(
      const int64_t balance_weight,
      ObBalanceGroupInfo &dest_bg_info,
      ObPartGroupInfo *&part_group);
  int swap_largest_for_smallest_pg(ObBalanceGroupInfo &dest_bg_info);
  int swap_for_smallest_pg(ObPartGroupInfo *const inner_pg, ObBalanceGroupInfo &dest_bg_info);
  int get_largest_part_group(ObPartGroupInfo *&part_group) const;
  int get_smallest_part_group(ObPartGroupInfo *&part_group) const;
  int get_balance_weight_array(ObIArray<int64_t> &weight_arr);
  int split_out_weighted_bg_info(ObBalanceGroupInfo &weighted_bg_info);

  TO_STRING_KV(K_(inited), K_(bg_id), K_(ls_id), "part_groups_count", get_part_groups_count(),
      "part_groups_data_size", get_part_groups_data_size(), "part_groups_balance_weight", get_part_groups_weight());

public:
  // less by weight
  static bool weight_cmp(const ObBalanceGroupInfo *left, const ObBalanceGroupInfo *right)
  {
    bool bret = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(right)) {
      if (left->get_part_groups_weight() < right->get_part_groups_weight()) {
        bret = true;
      } else if (left->get_part_groups_weight() == right->get_part_groups_weight()) {
        if (left->get_ls_id() > right->get_ls_id()) {
          bret = true;
        }
      }
    }
    return bret;
  }

  // less by count
  static bool cnt_cmp(const ObBalanceGroupInfo *left, const ObBalanceGroupInfo *right)
  {
    bool bret = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(right)) {
      if (left->get_part_groups_count() < right->get_part_groups_count()) {
        bret = true;
      } else if (left->get_part_groups_count() == right->get_part_groups_count()) {
        if (left->get_ls_id() > right->get_ls_id()) {
          bret = true;
        }
      }
    }
    return bret;
  }

private:
  int create_new_part_group_if_needed_(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid);
  int create_part_group_container_(
      const ObBalanceGroupID &bg_id,
      const int64_t balanced_ls_num);
  int inner_transfer_out_(
      const int64_t balance_weight,
      ObBalanceGroupInfo &dest_bg_info,
      ObPartGroupInfo *&part_group);
  int inner_swap_for_smallest_pg_(
      ObPartGroupInfo *const inner_pg,
      ObBalanceGroupInfo &dest_bg_info);

private:
  bool inited_;
  ObBalanceGroupID bg_id_;
  share::ObLSID ls_id_;
  ObPartGroupInfo *last_part_group_;
  ObPartGroupContainer *pg_container_;
  ObIAllocator &alloc_; // allocator for ObPartGroupInfo and ObPartGroupContainer
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H */
