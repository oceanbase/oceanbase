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

#ifndef OCEANBASE_ROOTSERVER_OB_LS_BALANCE_HELPER_H
#define OCEANBASE_ROOTSERVER_OB_LS_BALANCE_HELPER_H
#include "lib/container/ob_array.h" //ObArray
#include "share/unit/ob_unit_info.h" //ObSimpleUnitGroup
#include "share/balance/ob_balance_task_table_operator.h" //ObBalanceTask
#include "share/balance/ob_balance_job_table_operator.h" //ObBalanceJob
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_operator.h"//ObLSAttr
#include "share/transfer/ob_transfer_info.h"//ObPartList
#include "share/ob_balance_define.h"  // ObBalanceTaskID, ObBalanceJobID, ObBalanceStrategy
#include "rootserver/ob_balance_group_ls_stat_operator.h"//ObBalanceGroupID
#include "rootserver/balance/ob_tenant_ls_balance_group_info.h" //ObTenantLSBalanceGroupInfo

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
struct ObBalanceJob;
}
namespace rootserver
{
struct ObUnitGroupBalanceInfo
{
public:
  ObUnitGroupBalanceInfo() { reset(); }
  ObUnitGroupBalanceInfo(const share::ObSimpleUnitGroup &unit_group,
                      const int64_t target_ls_count) :
                      target_ls_count_(target_ls_count), unit_group_(unit_group),
                      redundant_ls_array_(), normal_ls_array_() {}
  ~ObUnitGroupBalanceInfo() {}

  int64_t get_lack_ls_count() const
  {
    int64_t count = 0;
    if (unit_group_.is_active()) {
      count = target_ls_count_ - normal_ls_array_.count();
    }
    return count;
  }
  int64_t get_redundant_ls_count() const
  {
    return redundant_ls_array_.count();
  }
  int64_t get_normal_ls_count() const
  {
    return normal_ls_array_.count();
  }
  uint64_t get_unit_group_id() const
  {
    return unit_group_.get_unit_group_id();
  }
  const share::ObLSStatusInfoArray & get_redundant_ls_array() const
  {
    return redundant_ls_array_;
  }
  const share::ObLSStatusInfoArray & get_normal_ls_array() const
  {
    return normal_ls_array_;
  }
  bool is_active_unit_group() const
  {
    return unit_group_.is_active();
  }
  int64_t get_all_ls_count() const
  {
    return redundant_ls_array_.count() + normal_ls_array_.count();
  }
  int remove_redundant_ls(const int64_t &index);
  void reset();
  int add_ls_status_info(const share::ObLSStatusInfo &ls_info);
  int get_and_remove_ls_status(share::ObLSStatusInfo &ls_info);
  TO_STRING_KV(K_(target_ls_count), K_(unit_group),
               K_(redundant_ls_array), K_(normal_ls_array));
private:
  int64_t target_ls_count_;
  share::ObSimpleUnitGroup unit_group_;
  share::ObLSStatusInfoArray redundant_ls_array_;// ls need merge to other normal ls
  share::ObLSStatusInfoArray normal_ls_array_;//normal ls need keep
};
typedef ObArray<ObUnitGroupBalanceInfo> ObUnitGroupBalanceInfoArray;

//for split or transfer
struct ObSplitLSParam
{
public:
  ObSplitLSParam(const share::ObLSStatusInfo *ls_info, const double current_factor)
      : info_(ls_info), current_factor_(current_factor) {}
  ObSplitLSParam() : info_(NULL), current_factor_(OB_FLOAT_EPSINON) {}
  bool is_valid() const
  {
    return OB_NOT_NULL(info_) && current_factor_ > OB_FLOAT_EPSINON;
  }
  double get_current_factor() const
  {
    return current_factor_;
  }
  const share::ObLSStatusInfo * get_ls_info() const
  {
    return info_;
  }
  const share::ObLSID get_ls_id() const {
    return (NULL == info_) ? share::ObLSID() : info_->ls_id_;
  }
  //Each time you split, you need to ensure that there is
  //enough factor on the source side, and then allocate the rest
  double reduce_factor_for_dest(const double need_factor, const double target_factor)
  {
    double can_split = 0;
    if (need_factor <= 0 || target_factor <= 0 || current_factor_ <= target_factor) {
    } else {
      can_split = std::min(current_factor_ - target_factor, target_factor);
      can_split = std::min(can_split, need_factor);
      current_factor_ -= can_split;
    }
    return can_split;
  }
  double reduce_enough_factor(const double need_factor)
  {
    double can_split = 0;
    if (need_factor <= 0) {
    } else {
      can_split = std::min(need_factor, current_factor_);
      current_factor_ -= can_split;
    }
    return can_split;
  }
  void reduce_all()
  {
    current_factor_ = 0;
  }
  TO_STRING_KV(KPC_(info), K_(current_factor));
private:
  const share::ObLSStatusInfo *info_;
  double current_factor_;
};
typedef ObArray<ObSplitLSParam> ObSplitLSParamArray;


class ObLSBalanceTaskHelper
{
/////////////////// static functions ///////////////////
public:
  static int add_ls_alter_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObLSID &src_ls_id,
      const share::ObBalanceStrategy &balance_strategy,
      common::ObIArray<share::ObBalanceTask> &task_array);
  static int add_ls_transfer_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferPartList &part_list,
      const share::ObBalanceStrategy &balance_strategy,
      common::ObIArray<share::ObBalanceTask> &task_array);
  static int add_ls_split_task(
      common::ObMySQLProxy *sql_proxy,
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObLSID &src_ls_id,
      const share::ObTransferPartList &part_list,
      const share::ObBalanceStrategy &balance_strategy,
      share::ObLSID &new_ls_id,
      common::ObIArray<share::ObBalanceTask> &task_array);
  static int add_ls_merge_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObBalanceStrategy &balance_strategy,
      common::ObIArray<share::ObBalanceTask> &task_array);
  static int add_create_ls_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObBalanceStrategy &balance_strategy,
      common::ObMySQLProxy *sql_proxy,
      common::ObIArray<share::ObBalanceTask> &task_array);
  static int choose_ls_group_id_for_transfer_between_dup_ls(
      const uint64_t src_ls_group_id,
      const uint64_t dest_ls_group_id,
      const uint64_t other_ls_group_id,
      uint64_t &chosen_ls_group_id);
/////////////////// static functions end ///////////////////

public:
  ObLSBalanceTaskHelper ();
  ~ObLSBalanceTaskHelper() {}
  int init(
      const uint64_t tenant_id,
      const share::ObLSStatusInfoArray &status_array,
      const ObIArray<share::ObSimpleUnitGroup> &unit_group_array,
      const int64_t primary_zone_num,
      ObMySQLProxy *sql_proxy);
  //check need ls balance
  int check_need_ls_balance(bool &need_balance);
  //generate ls balance job and task
  int generate_ls_balance_task();
  share::ObBalanceJob& get_balance_job()
  {
    return job_;
  }
  ObArray<share::ObBalanceTask>& get_balance_tasks()
  {
    return task_array_;
  }
private:
  int generate_balance_job_();
  int generate_balance_job_strategy_();
  int generate_alter_task_();
  int generate_migrate_task_();
  int generate_expand_task_();
  int generate_shrink_task_();
  int generate_factor_task_();
  /* description: get index of the unit_group_id in unit_group_balance_array
   * param[in] unit_group_id : unit_group_id
   * param[out] index : index of unit_group in unit_group_balance_array
   * return:
   * OB_SUCCESS : find the valid index
   * OB_ENTRY_NOT_EXIST: the unit_group not exist
   * OTHER : failed
   */
  int find_unit_group_balance_index(const uint64_t unit_group_id, int64_t &index);
  int construct_expand_dest_param_(
      const int64_t lack_ls_count,
      ObSplitLSParamArray &src_ls,
      ObIArray<ObSplitLSParamArray> &dest_array);
  int construct_shrink_src_param_(
      const int64_t target_count,
      ObSplitLSParamArray &src_ls,
      ObIArray<ObSplitLSParamArray> &dest_array);
  int generate_balance_task_for_expand_(
      const ObSplitLSParamArray &dest_split_param,
      const uint64_t ls_group_id);
  // target_ls_id: the ls which the splitted part groups will be located eventually
  // 1. for ls expand:
  //    param[out] target_ls_id: the first splitted dest_ls_id, other splitted dest_ls_id
  //                             is merged into target_ls_id
  //    for example: when ls_num 2 -> 3, generate ls_split(1001, 1003), ls_split(1002, 1004),
  //                 ls_merge(1004, 1003). target_ls_id will be 1003
  // 2. for ls shrink:
  //    param[in] target_ls_id: the normal ls which the splitted part groups will be located eventually
  //    for example: when ls_num 3 -> 2, generate ls_split(1003, 1004)
  //                 target_ls_id is 1001
  int generate_ls_split_task_(
      const ObSplitLSParamArray &dest_split_param,
      share::ObLSID &target_ls_id,
      int64_t &task_begin_index);
  int prepare_ls_partition_info_();
  int add_ls_part_info(
      const share::ObLSID &ls_id,
      const share::ObTransferPartInfo &part_info,
      const ObBalanceGroupID &bg_id);
  int construct_ls_part_info_(
      const ObSplitLSParam &src_ls,
      const share::ObLSID &dest_ls_id,
      share::ObTransferPartList &part_list);
  int generate_ls_alter_task_(const share::ObLSStatusInfo &ls_status_info, ObUnitGroupBalanceInfo &dest_unit_group);
  int generate_task_for_shrink_(const ObSplitLSParamArray &src_split_param, const share::ObLSStatusInfo &ls_status_info);
  int generate_transfer_task_(const ObSplitLSParam &param, const share::ObLSStatusInfo &ls_status_info);
  //for task
  int construct_ls_alter_task_(const share::ObLSID &ls_id, const uint64_t ls_group_id);
  int construct_ls_merge_task_(
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const uint64_t ls_group_id);
  bool has_redundant_dup_ls_() const { return dup_ls_stat_array_.count() > 1; }
  int generate_task_for_dup_ls_shrink_();
  int check_need_modify_ls_group_(const ObUnitGroupBalanceInfo &balance_info, bool &need_modify);
  //generate for scale_out_factor
  int generate_migrate_task_for_deleting_unit_(ObUnitGroupBalanceInfo &balance_info);
  int generate_expand_task_for_factor_(const ObUnitGroupBalanceInfo &balance_info);
  int generate_shrink_task_for_factor_(const ObUnitGroupBalanceInfo &balance_info);
  int check_ls_count_balanced_between_normal_unitgroup_(bool &need_balance);
  int get_min_max_ls_unitgroup_(int64_t &min_index, int64_t &max_index);
  //尽可能补齐缺口
  int generate_migrate_task_while_enable_transfer_();
  //把日志流均匀打散在各个unit_group中
  int generate_migrate_task_while_disable_transfer_();

private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t primary_zone_num_;
  int64_t ls_scale_out_factor_;
  bool enable_transfer_;
  share::ObBalanceStrategy balance_strategy_;
  ObUnitGroupBalanceInfoArray unit_group_balance_array_;
  ObMySQLProxy *sql_proxy_;
  share::ObBalanceJob job_;
  ObArray<share::ObBalanceTask> task_array_;
  ObTenantLSBalanceGroupInfo tenant_ls_bg_info_;
  ObArray<share::ObLSStatusInfo> dup_ls_stat_array_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_BALANCE_HELPER_H */
