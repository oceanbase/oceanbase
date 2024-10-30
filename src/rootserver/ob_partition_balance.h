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

#ifndef OB_OCEANBASE_BALANCE_PARTITION_JOB_H_
#define OB_OCEANBASE_BALANCE_PARTITION_JOB_H_

#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/transfer/ob_transfer_info.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ls/ob_ls_operator.h"
#include "share/balance/ob_balance_task_table_operator.h" //ObBalanceTask
#include "share/balance/ob_balance_job_table_operator.h" //ObBalanceJob

#include "balance/ob_balance_group_info.h"            // ObTransferPartGroup
#include "balance/ob_all_balance_group_builder.h"     // ObAllBalanceGroupBuilder
#include "balance/ob_partition_balance_helper.h"      // ObPartTransferJobGenerator

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

typedef hash::ObHashMap<share::ObLSID, ObLSDesc*> ObLSDescMap;

// Partition Balance implement
class ObPartitionBalance final : public ObAllBalanceGroupBuilder::NewPartitionCallback
{
public:
  ObPartitionBalance() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID), dup_ls_id_(), sql_proxy_(nullptr),
                         allocator_("PART_BALANCE", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
                         bg_builder_(),
                         ls_desc_array_(), ls_desc_map_(),
                         bg_map_(),
                         bg_ls_stat_operator_(),
                         task_mode_(GEN_BG_STAT),
                         job_generator_(),
                         part_distribution_mode_(ObPartDistributionMode::INVALID)
  {}
  ~ObPartitionBalance() {
    destroy();
  }
  enum TaskMode {
    GEN_BG_STAT,
    GEN_TRANSFER_TASK
  };

  int init(
      uint64_t tenant_id,
      schema::ObMultiVersionSchemaService *schema_service,
      common::ObMySQLProxy *sql_proxy,
      const int64_t primary_zone_num,
      const int64_t unit_group_num,
      TaskMode mode = GEN_BG_STAT,
      const ObPartDistributionMode &part_distribution_mode = ObPartDistributionMode::ROUND_ROBIN);
  void destroy();
  int process(const ObBalanceJobID &job_id = ObBalanceJobID(), const int64_t timeout = 0);
  bool is_inited() const { return inited_; }

  ObBalanceJob &get_balance_job() { return job_generator_.get_balance_job(); }
  ObArray<ObBalanceTask> &get_balance_task() { return job_generator_.get_balance_tasks(); }

  // For ObAllBalanceGroupBuilder::NewPartitionCallback
  // handle new partition of every balance group
  int on_new_partition(
      const ObBalanceGroup &bg_in,
      const schema::ObSimpleTableSchemaV2 &table_schema,
      const ObObjectID part_object_id,
      const ObLSID &src_ls_id,
      const ObLSID &dest_ls_id,
      const int64_t tablet_size,
      const bool in_new_partition_group,
      const uint64_t part_group_uid);

  static const int64_t PART_BALANCE_THRESHOLD_SIZE =  50 * 1024L * 1024L * 1024L; // 50GB

private:
  int prepare_balance_group_();
  int save_balance_group_stat_();
  // balance group inner balance
  int process_balance_partition_inner_();
  // balance group extend balance
  int process_balance_partition_extend_();
  // ls disk balance
  int process_balance_partition_disk_();

  int prepare_ls_();
  int add_part_to_bg_map_(
      const ObLSID &ls_id,
      ObBalanceGroup &bg,
      const schema::ObSimpleTableSchemaV2 &table_schema,
      const uint64_t part_group_uid,
      const ObTransferPartInfo &part_info,
      const int64_t tablet_size);
  int add_transfer_task_(
      const ObLSID &src_ls_id,
      const ObLSID &dest_ls_id,
      ObTransferPartGroup *part_group,
      bool modify_ls_desc = true);
  int update_ls_desc_(const ObLSID &ls_id, int64_t cnt, int64_t size);
  int try_swap_part_group_(ObLSDesc &src_ls,
                          ObLSDesc &dest_ls,
                          int64_t part_group_min_size,
                          int64_t &swap_cnt);
  bool check_ls_need_swap_(uint64_t ls_more_size, uint64_t ls_less_size);

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSID dup_ls_id_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;

  ObAllBalanceGroupBuilder bg_builder_;

  // ls array to assign part
  ObArray<ObLSDesc*> ls_desc_array_;
  ObLSDescMap ls_desc_map_;

  // partition distribute in balance group and ls
  hash::ObHashMap<ObBalanceGroup, ObArray<ObBalanceGroupInfo *>> bg_map_;

  ObBalanceGroupLSStatOperator bg_ls_stat_operator_;
  TaskMode task_mode_;
  // record the partitions to be transferred and generate the corresponding balance job and tasks
  ObPartTransferJobGenerator job_generator_;
  ObPartDistributionMode part_distribution_mode_;
};

} // end rootserver
} // end oceanbase
#endif
