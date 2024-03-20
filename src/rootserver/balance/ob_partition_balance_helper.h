/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_BALANCE_HELPER_H
#define OCEANBASE_ROOTSERVER_OB_PARTITION_BALANCE_HELPER_H

#include "lib/hash/ob_hashmap.h" // ObHashMap
#include "share/balance/ob_balance_job_table_operator.h" // ObBalanceJob
#include "share/balance/ob_balance_task_table_operator.h" // ObBalanceTask
#include "share/transfer/ob_transfer_info.h" // ObTransferTaskKey
#include "share/ls/ob_ls_status_operator.h" // ObLSStatusInfoIArray

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace rootserver
{
class ObTransferPartGroup;

class ObLSDesc
{
public:
  ObLSDesc(share::ObLSID ls_id, uint64_t ls_group_id)
      : ls_id_(ls_id), ls_group_id_(ls_group_id), partgroup_cnt_(0), data_size_(0) {}
  ~ObLSDesc() {}
  share::ObLSID get_ls_id() const { return ls_id_; }
  uint64_t get_partgroup_cnt() const { return partgroup_cnt_; }
  uint64_t get_data_size() const { return data_size_; }
  uint64_t get_ls_group_id() const { return ls_group_id_; }
  void add_data_size(int64_t size) { data_size_ += size; }
  void add_partgroup(int64_t count, int64_t size) { partgroup_cnt_ += count; add_data_size(size); }
  TO_STRING_KV(K_(ls_id), K_(partgroup_cnt), K_(data_size));
private:
  share::ObLSID ls_id_;
  uint64_t ls_group_id_;
  uint64_t partgroup_cnt_;
  uint64_t data_size_;
};

typedef common::hash::ObHashMap<share::ObLSID, uint64_t> ObLSGroupIDMap;

// Record the partitions to be transferred and generate the corresponding balance job and tasks.
class ObPartTransferJobGenerator
{
public:
  ObPartTransferJobGenerator();
  virtual ~ObPartTransferJobGenerator() {}
  int init(
      const uint64_t tenant_id,
      const int64_t primary_zone_num,
      const int64_t unit_group_num,
      common::ObMySQLProxy *sql_proxy);
  int prepare_ls(const share::ObLSStatusInfoIArray &ls_stat_array);
  void reset();
  int add_need_transfer_part(
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferPartInfo &part_info);

  share::ObBalanceJob& get_balance_job() { return balance_job_; }
  common::ObArray<share::ObBalanceTask>& get_balance_tasks() { return balance_tasks_; }
  bool need_gen_job() const
  {
    return !dup_to_normal_part_map_.empty()
        || !normal_to_dup_part_map_.empty()
        || !dup_to_dup_part_map_.empty()
        || !normal_to_normal_part_map_.empty();
  }

  int gen_balance_job_and_tasks(
      const share::ObBalanceJobType &job_type,
      const ObString &balance_strategy);

  TO_STRING_KV(K_(tenant_id), K_(primary_zone_num), K_(unit_group_num),
      K_(dup_ls_ids), K_(balance_job), K_(balance_tasks));

private:
  int check_inner_stat_() const;
  int add_need_transfer_part_(
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferPartInfo &part_info,
      share::ObTransferPartMap &map);
  // gen_transfer_tasks_xxxx only append balance tasks
  int gen_transfer_tasks_from_dup_ls_to_normal_ls_(); // use dup_to_normal_part_map_
  int gen_transfer_tasks_from_normal_ls_to_dup_ls_(); // use normal_to_dup_part_map_
  int gen_transfer_tasks_between_dup_ls_(); // use dup_to_dup_part_map_
  int gen_transfer_tasks_between_normal_ls_(); // use normal_to_normal_part_map_
  int choose_dup_ls_transfer_ls_group_id_(
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      uint64_t &ls_group_id);

private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t primary_zone_num_; // for balance job
  int64_t unit_group_num_; // for balance job
  common::ObMySQLProxy *sql_proxy_;
  share::ObBalanceJob balance_job_;
  common::ObArray<share::ObBalanceTask> balance_tasks_;
  common::ObSEArray<share::ObLSID, 1> dup_ls_ids_; // dup_ls_ids can be empty
  ObLSGroupIDMap ls_group_id_map_;
  share::ObTransferPartMap dup_to_normal_part_map_; // dup ls -> normal ls
  share::ObTransferPartMap normal_to_dup_part_map_; // normal ls -> dup ls
  share::ObTransferPartMap dup_to_dup_part_map_; // tranfer between dup ls
  share::ObTransferPartMap normal_to_normal_part_map_; // transfer between normal ls
};

} // end rootserver
} // end oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_PARTITION_BALANCE_HELPER_H
