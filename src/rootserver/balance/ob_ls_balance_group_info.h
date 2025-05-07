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

#ifndef OCEANBASE_ROOTSERVER_OB_LS_BALANCE_GROUP_INFO_H
#define OCEANBASE_ROOTSERVER_OB_LS_BALANCE_GROUP_INFO_H

#include "share/transfer/ob_transfer_info.h"  //ObTransferPartList, ObTransferPartInfo
#include "share/ob_ls_id.h"                   //ObLSID
#include "lib/hash/ob_hashmap.h"              //ObHashMap
#include "ob_balance_group_define.h"          //ObBalanceGroupID
#include "ob_balance_group_info.h"            //ObBalanceGroupInfo

namespace oceanbase
{
namespace rootserver
{

// LS Balance Statistic Info
class ObLSBalanceGroupInfo final
{
public:
  ObLSBalanceGroupInfo(common::ObIAllocator &alloc) :
      inited_(false),
      ls_id_(),
      alloc_(alloc),
      bg_map_(),
      orig_part_group_cnt_map_(),
      balanced_ls_num_(0) {}
  ~ObLSBalanceGroupInfo() { destroy(); }

  int init(const share::ObLSID &ls_id, const int64_t balanced_ls_num);
  void destroy();
  bool is_inited() const { return inited_; }

  // append partition at the newest partition group in target balance group.
  // create new partition group in balance group if needed.
  //
  // NOTE: if balance group not exist, it will create a new balance group automatically
  //
  // @param [in] bg_id                        target balance group id
  // @param [in] table_schema                 table schema of the table which the partition belongs to
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  // @param [in] part_group_uid               target partition group unique id
  // @param [in] balance_weight               balance weight of the partition
  //
  // @return OB_SUCCESS         success
  // @return OB_ENTRY_EXIST     no partition group found
  // @return other              fail
  int append_part_into_balance_group(
      const ObBalanceGroupID &bg_id,
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      share::ObTransferPartInfo &part,
      const int64_t data_size,
      const uint64_t part_group_uid,
      const int64_t balance_weight);

  ////////////////////////////////////////////////
  // Transfer out partition groups by specified factor
  //
  // NOTE: This function can be called only if all partitions are added.
  int transfer_out_by_factor(
      ObLSBalanceGroupInfo &dst_ls_bg_info,
      const float factor,
      share::ObTransferPartList &part_list);

  TO_STRING_KV(K_(inited), K_(ls_id), "balance_group_count", bg_map_.size(), K_(balanced_ls_num));

private:
  int get_or_create_(const ObBalanceGroupID &bg_id, ObBalanceGroupInfo *&bg);

private:
  static const int64_t MAP_BUCKET_NUM = 4096;

  bool                      inited_;
  share::ObLSID             ls_id_;
  common::ObIAllocator      &alloc_;
  // map for all balance groups on this LS
  common::hash::ObHashMap<ObBalanceGroupID, ObBalanceGroupInfo *> bg_map_;
  // map for all balance groups' original partition group count
  // This original count will be maintained during adding partitions into balance group.
  // When all partitions are added, the original count will not change anymore.
  common::hash::ObHashMap<ObBalanceGroupID, int64_t> orig_part_group_cnt_map_;
  // the number of LS after LS balance
  int64_t balanced_ls_num_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_BALANCE_GROUP_INFO_H */
