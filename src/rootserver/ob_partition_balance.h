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

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

// Partition Balance implment
class ObPartitionBalance final : public ObAllBalanceGroupBuilder::NewPartitionCallback
{
public:
  ObPartitionBalance() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(nullptr),
                         allocator_("PART_BALANCE", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
                         bg_builder_(), cur_part_group_(nullptr),
                         ls_desc_array_(), ls_desc_map_(),
                         bg_map_(),
                         transfer_logical_tasks_(),
                         bg_ls_stat_operator_(),
                         balance_job_(),
                         balance_tasks_(),
                         task_mode_(GEN_BG_STAT),
                         primary_zone_num_(-1),
                         unit_group_num_(-1)
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
  int process();

  ObBalanceJob &get_balance_job() { return balance_job_; }
  ObArray<ObBalanceTask> &get_balance_task() { return balance_tasks_; }


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
      const uint64_t part_group_uid);

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
    int add_new_part_group(ObTransferPartGroup *&part_gourp);
    TO_STRING_KV(K_(ls_id), K_(part_groups));
  private:
    ObLSID ls_id_;
    ObIAllocator &alloc_;
    ObArray<ObTransferPartGroup *> part_groups_;
  };

  class ObLSDesc
  {
  public:
    ObLSDesc(ObLSID ls_id, uint64_t ls_group_id) : ls_id_(ls_id), ls_group_id_(ls_group_id), partgroup_cnt_(0), data_size_(0) {}
    ~ObLSDesc() {
      ls_id_.reset();
      ls_group_id_ = OB_INVALID_ID;
      partgroup_cnt_ = 0;
      data_size_ = 0;
    }
    ObLSID get_ls_id() const { return ls_id_; }
    uint64_t get_partgroup_cnt() const { return partgroup_cnt_; }
    uint64_t get_data_size() const { return data_size_; }
    uint64_t get_ls_group_id() const { return ls_group_id_; }
    void add_partgroup(int64_t count, int64_t size) { partgroup_cnt_ += count; add_data_size(size); }
    void add_data_size(int64_t size) { data_size_ += size; }
    TO_STRING_KV(K_(ls_id), K_(partgroup_cnt), K_(data_size));
  private:
    ObLSID ls_id_;
    uint64_t ls_group_id_;
    uint64_t partgroup_cnt_;
    uint64_t data_size_;
  };

  class ObTransferTaskKey
  {
  public:
    ObTransferTaskKey(const ObLSID &src_ls_id, const ObLSID &dest_ls_id) :
      src_ls_id_(src_ls_id), dest_ls_id_(dest_ls_id) {}
    ObTransferTaskKey(const ObTransferTaskKey &other) {
      src_ls_id_ = other.src_ls_id_;
      dest_ls_id_ = other.dest_ls_id_;
    }
    ObTransferTaskKey() {}
    int hash(uint64_t &res) const {
      res = 0;
      res = murmurhash(&src_ls_id_, sizeof(src_ls_id_), res);
      res = murmurhash(&dest_ls_id_, sizeof(dest_ls_id_), res);
      return OB_SUCCESS;
    }
    bool operator ==(const ObTransferTaskKey &other) const {
      return src_ls_id_ == other.src_ls_id_ && dest_ls_id_ == other.dest_ls_id_;
    }
    bool operator !=(const ObTransferTaskKey &other) const { return !(operator ==(other)); }
    ObLSID get_src_ls_id() const { return src_ls_id_; }
    ObLSID get_dest_ls_id() const { return dest_ls_id_; }
    TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id));
  private:
    ObLSID src_ls_id_;
    ObLSID dest_ls_id_;
  };

  static const int64_t PART_BALANCE_THRESHOLD_SIZE =  50 * 1024L * 1024L * 1024L; // 50GB

private:
  int prepare_balance_group_();
  int save_balance_group_stat_();
  int construct_balance_group_();
  int construct_bg_for_partlevel_zero_(const schema::ObSimpleTableSchemaV2 &table_schema);
  int construct_bg_for_partlevel_one_(const schema::ObSimpleTableSchemaV2 &table_schema);
  int construct_bg_for_partlevel_two_(const schema::ObSimpleTableSchemaV2 &table_schema);
  int construct_bg_for_tablegroup_sharding_none_(const schema::ObSimpleTablegroupSchema &tablegroup_schema, ObArray<const schema::ObSimpleTableSchemaV2*> &table_schemas);
  int construct_bg_for_tablegroup_sharding_partition_(const schema::ObSimpleTablegroupSchema &tablegroup_schema, ObArray<const schema::ObSimpleTableSchemaV2*> &table_schemas);
  int construct_bg_for_tablegroup_sharding_subpart_(const schema::ObSimpleTablegroupSchema &tablegroup_schema, ObArray<const schema::ObSimpleTableSchemaV2*> &table_schemas);
  int add_tablet_to_bg_(ObTransferPartGroup *&dest_part_group, ObLSID &dest_ls_id, ObTabletID tablet_id, ObBalanceGroupID &bg_id, ObObjectID table_id, ObObjectID part_id);
  // balance group inner balance
  int process_balance_partition_inner_();
  // balance group extend balance
  int process_balance_partition_extend_();
  // ls disk balance
  int process_balance_partition_disk_();
  int generate_balance_job_from_logical_task_();

  int prepare_ls_();
  int add_new_pg_to_bg_map_(const ObLSID &ls_id, ObBalanceGroup &bg, ObTransferPartGroup *&part_group);
  int add_transfer_task_(const ObLSID &src_ls_id, const ObLSID &dest_ls_id, ObTransferPartGroup *part_group, bool modify_ls_desc = true);
  int update_ls_desc_(const ObLSID &ls_id, int64_t cnt, int64_t size);
  int try_swap_part_group_(ObLSDesc &src_ls, ObLSDesc &dest_ls, int64_t part_group_min_size ,int64_t &swap_cnt);
  int get_table_schemas_in_tablegroup_(int64_t tablegroup_id,
                                      ObArray<const schema::ObSimpleTableSchemaV2*> &table_schemas,
                                      int &max_part_level);
  bool check_ls_need_swap_(uint64_t ls_more_size, uint64_t ls_less_size);
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;

  ObAllBalanceGroupBuilder bg_builder_;
  ObTransferPartGroup *cur_part_group_;

  // ls array to assign part
  ObArray<ObLSDesc*> ls_desc_array_;
  hash::ObHashMap<ObLSID, ObLSDesc*> ls_desc_map_;

  // partition distribute in balance group and ls
  hash::ObHashMap<ObBalanceGroup, ObArray<ObLSPartGroupDesc *>> bg_map_;

  // logical transfer task
  hash::ObHashMap<ObTransferTaskKey, ObTransferPartList> transfer_logical_tasks_;

  ObBalanceGroupLSStatOperator bg_ls_stat_operator_;

  // generate result: balance job and balance task
  ObBalanceJob balance_job_;
  ObArray<ObBalanceTask> balance_tasks_;

  TaskMode task_mode_;
  //for generate balance job
  int64_t primary_zone_num_;
  int64_t unit_group_num_;
};

class ObPartitionHelper
{
public:
  class ObPartInfo {
  public:
    ObPartInfo() {}
    int init(ObTabletID tablet_id, ObObjectID part_id) {
      int ret = OB_SUCCESS;
      tablet_id_ = tablet_id;
      part_id_ = part_id;
      return ret;
    }
    ObTabletID get_tablet_id() { return tablet_id_; }
    ObObjectID get_part_id() { return part_id_; }
    TO_STRING_KV(K_(tablet_id), K_(part_id));
  private:
    ObTabletID tablet_id_;
    ObObjectID part_id_;
  };
  static int get_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, ObPartInfo &part_info);
  static int get_sub_part_num(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t &sub_part_num);
  static int get_sub_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t sub_part_idx, ObPartInfo &part_info);
  static int check_partition_option(const schema::ObSimpleTableSchemaV2 &t1, const schema::ObSimpleTableSchemaV2 &t2, bool is_subpart, bool &is_matched);
  static int check_partition_match(const schema::ObSimpleTableSchemaV2 &t1, const schema::ObSimpleTableSchemaV2 &t2, bool &match);
};

} // end rootserver
} // end oceanbase
#endif
