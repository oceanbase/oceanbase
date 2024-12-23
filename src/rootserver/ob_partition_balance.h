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
                         bg_builder_(), cur_part_group_(nullptr),
                         ls_desc_array_(), ls_desc_map_(),
                         bg_map_(),
                         weighted_bg_map_(),
                         bg_ls_stat_operator_(),
                         task_mode_(GEN_BG_STAT),
                         job_generator_()
  {}
  ~ObPartitionBalance() {
    destroy();
  }
  enum TaskMode {
    GEN_BG_STAT,
    GEN_TRANSFER_TASK
  };

  int init(uint64_t tenant_id, schema::ObMultiVersionSchemaService *schema_service, common::ObMySQLProxy *sql_proxy,
      const int64_t primary_zone_num, const int64_t unit_group_num,
      TaskMode mode = GEN_BG_STAT);
  void destroy();
  int process(const ObBalanceJobID &job_id = ObBalanceJobID(), const int64_t timeout = 0);
  bool is_inited() const { return inited_; }

  ObBalanceJob &get_balance_job() { return job_generator_.get_balance_job(); }
  ObArray<ObBalanceTask> &get_balance_task() { return job_generator_.get_balance_tasks(); }

  // For ObAllBalanceGroupBuilder::NewPartitionCallback
  // handle new partition of every balance group
  int on_new_partition(
      const ObBalanceGroup &bg,
      const ObObjectID table_id,
      const ObObjectID part_object_id,
      const ObTabletID tablet_id,
      const ObLSID &src_ls_id,
      const ObLSID &dest_ls_id,
      const int64_t tablet_size,
      const bool in_new_partition_group,
      const uint64_t part_group_uid,
      const int64_t balance_weight);

  class ObLSPartGroupDesc
  {
  public:
    ObLSPartGroupDesc(ObLSID ls_id, ObIAllocator &alloc) :
        ls_id_(ls_id),
        alloc_(alloc),
        part_groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "LSPartGroupDesc")) {}
    ~ObLSPartGroupDesc() {
      ls_id_.reset();
      for (int64_t i = 0; i < part_groups_.count(); i++) {
        if (OB_NOT_NULL(part_groups_.at(i))) {
          part_groups_.at(i)->~ObTransferPartGroup();
          alloc_.free(part_groups_.at(i));
          part_groups_.at(i) = NULL;
        }
      }
      part_groups_.reset();
    }
    ObLSID get_ls_id() const { return ls_id_; }
    ObArray<ObTransferPartGroup *> &get_part_groups() { return part_groups_; }
    int64_t get_part_groups_cnt() const { return part_groups_.count(); }
    int64_t get_part_groups_weight() const
    {
      int64_t weight = 0;
      ARRAY_FOREACH_NORET(part_groups_, idx) {
        if (OB_NOT_NULL(part_groups_.at(idx))) {
          weight += part_groups_.at(idx)->get_weight();
        }
      }
      return weight;
    }
    int64_t get_unweighted_part_groups_cnt() const
    {
      int64_t count = 0;
      ARRAY_FOREACH_NORET(part_groups_, idx) {
        if (OB_NOT_NULL(part_groups_.at(idx))) {
          if (part_groups_.at(idx)->get_weight() == 0) {
            ++count;
          }
        }
      }
      return count;
    }
    int add_new_part_group(ObTransferPartGroup *&part_gourp);
    int remove_part_group(const int64_t idx);

    // less by weight
    static bool weight_cmp(const ObLSPartGroupDesc *left, const ObLSPartGroupDesc *right)
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
    static bool cnt_cmp(const ObLSPartGroupDesc *left, const ObLSPartGroupDesc *right)
    {
      bool bret = false;
      if (OB_NOT_NULL(left) && OB_NOT_NULL(right)) {
        if (left->get_part_groups_cnt() < right->get_part_groups_cnt()) {
          bret = true;
        } else if (left->get_part_groups_cnt() == right->get_part_groups_cnt()) {
          if (left->get_ls_id() > right->get_ls_id()) {
            bret = true;
          }
        }
      }
      return bret;
    }

    TO_STRING_KV(K_(ls_id), K_(part_groups));
  private:
    ObLSID ls_id_;
    ObIAllocator &alloc_;
    ObArray<ObTransferPartGroup *> part_groups_;
  };
  typedef hash::ObHashMap<ObBalanceGroup, ObArray<ObLSPartGroupDesc *>> ObBalanceGroupMap;

  static const int64_t PART_BALANCE_THRESHOLD_SIZE =  50 * 1024L * 1024L * 1024L; // 50GB

private:
  int prepare_balance_group_();
  int split_out_weighted_bg_map_();
  int save_balance_group_stat_();
  int process_weight_balance_intragroup_();
  // balance group inner balance
  int process_balance_partition_inner_();
  // balance group extend balance
  int process_balance_partition_extend_();
  // ls disk balance
  int process_balance_partition_disk_();

  int prepare_ls_();
  int add_new_pg_to_bg_map_(
      const ObLSID &ls_id,
      ObBalanceGroup &bg,
      ObTransferPartGroup *&part_group,
      ObBalanceGroupMap &bg_map);
  int add_transfer_task_(
      const ObLSID &src_ls_id,
      const ObLSID &dest_ls_id,
      ObTransferPartGroup *part_group,
      bool modify_ls_desc = true);
  int update_ls_desc_(
      const ObLSID &ls_id,
      const int64_t cnt,
      const int64_t size,
      const int64_t balance_weight);
  int try_swap_part_group_(ObLSDesc &src_ls, ObLSDesc &dest_ls, int64_t part_group_min_size ,int64_t &swap_cnt);
  int try_swap_part_group_in_bg_(
      ObBalanceGroupMap::iterator &iter,
      ObLSDesc &src_ls,
      ObLSDesc &dest_ls,
      int64_t part_group_min_size,
      int64_t &swap_cnt);
  int get_table_schemas_in_tablegroup_(int64_t tablegroup_id,
                                      ObArray<const schema::ObSimpleTableSchemaV2*> &table_schemas,
                                      int &max_part_level);
  bool check_ls_need_swap_(uint64_t ls_more_size, uint64_t ls_less_size);
  bool is_bg_with_balance_weight_(const ObArray<ObLSPartGroupDesc *> &ls_pg_desc_arr);
  int get_ls_balance_weight_avg_(
      const ObArray<ObLSPartGroupDesc *> &ls_pg_desc_arr,
      const int64_t begin_idx,
      const int64_t end_idx,
      double &weight_avg);
  int try_move_weighted_part_group_(
      ObArray<ObLSPartGroupDesc *> &sorted_ls_pg_desc_arr,
      int64_t &transfer_cnt);
  int try_move_weighted_pg_to_dest_ls_(
      const double avg,
      ObLSPartGroupDesc *src_ls,
      ObLSPartGroupDesc *dest_ls,
      int64_t &transfer_cnt);
  int try_swap_weighted_part_group_(
      ObArray<ObLSPartGroupDesc *> &sorted_ls_pg_desc_arr,
      int64_t &transfer_cnt);
  int try_swap_weighted_pg_between_ls_(
      const double avg,
      ObLSPartGroupDesc *src_ls,
      ObLSPartGroupDesc *dest_ls,
      int64_t &transfer_cnt);

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSID dup_ls_id_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;

  ObAllBalanceGroupBuilder bg_builder_;
  ObTransferPartGroup *cur_part_group_;

  // ls array to assign part
  ObArray<ObLSDesc*> ls_desc_array_;
  ObLSDescMap ls_desc_map_;

  // partition distribute in balance group and ls
  // record all balance group in bg_map when building and then split the weighted balance group out
  ObBalanceGroupMap bg_map_;
  ObBalanceGroupMap weighted_bg_map_;

  ObBalanceGroupLSStatOperator bg_ls_stat_operator_;
  TaskMode task_mode_;
  // record the partitions to be transferred and generate the corresponding balance job and tasks
  ObPartTransferJobGenerator job_generator_;
};

} // end rootserver
} // end oceanbase
#endif
