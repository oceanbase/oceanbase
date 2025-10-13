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
#include "lib/container/ob_array.h" //common::ObArray
#include "lib/hash/ob_hashmap.h"//ObHashMap
#include "lib/container/ob_bit_set.h"//obitset
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"//ObArenaAllocator
#include "share/unit/ob_unit_info.h" //ObUnit
#include "share/balance/ob_balance_task_table_operator.h" //ObBalanceTask
#include "share/balance/ob_balance_job_table_operator.h" //ObBalanceJob
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_operator.h"//ObLSAttr
#include "share/transfer/ob_transfer_info.h"//ObPartList
#include "share/ob_balance_define.h"  // ObBalanceTaskID, ObBalanceJobID, ObBalanceStrategy
#include "share/ob_unit_table_operator.h"   // ObUnitUGOp
#include "share/ob_share_util.h"//ObMatrix
#include "rootserver/ob_balance_group_ls_stat_operator.h"//ObBalanceGroupID
#include "rootserver/ob_server_balancer.h"//Matrix
#include "rootserver/balance/ob_tenant_ls_balance_group_info.h"//ObTenantLSBalanceGroupInfo

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
struct ObBalanceJob;
struct ObUnit;
}
namespace rootserver
{
struct ObUnitGroupStat;
struct ObZoneLSStat;
struct ObLSGroupStat
{
  ObLSGroupStat() { reset(); }
  ~ObLSGroupStat() {}
  void reset()
  {
    lg_id_ = OB_INVALID_ID;
    current_unit_list_.reset();
    ls_info_set_.reset();
    ug_ids_.reset();
    target_unit_list_.reset();
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != lg_id_
           && !current_unit_list_.empty();
  }
  int init(const uint64_t lg_id, const ObUnitIDList &unit_list);
  int assign(const ObLSGroupStat &other);
  int add_ls_status(const int64_t ls_info_index);
  int64_t ls_count_in_group() const
  {
    return ls_info_set_.num_members();
  }
  bool need_change_unit_list() const
  {
    return current_unit_list_ != target_unit_list_;
  }
  int find_and_remove_ls(int64_t &ls_index);
  //ls_info_set打印出来特别不友好，移除打印
  TO_STRING_KV(K_(lg_id), K_(current_unit_list), K(ug_ids_),
      K(target_unit_list_), "ls_count", ls_count_in_group());
public:
  uint64_t lg_id_;
  ObUnitIDList current_unit_list_;
  ObFixedBitSet<OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET> ls_info_set_;
  ObSEArray<uint64_t, 2> ug_ids_;
  ObUnitIDList target_unit_list_;
};

struct ObUnitLSStat
{
public:
 ObUnitLSStat() { reset(); }

 ~ObUnitLSStat() {}
  void reset()
  {
    unit_ = NULL;
    unit_id_ = OB_INVALID_ID;
    is_sys_standalone_ = false;
    ls_group_info_.reset();
    zone_stat_ = NULL;
    unit_group_stat_ = NULL;
  }
  DECLARE_TO_STRING;
  bool is_valid() const
  {
    return OB_NOT_NULL(unit_) && unit_->is_valid();
  }
  int init(share::ObUnit &unit);
  int add_ls_group(ObLSGroupStat &ls_group_info);
  int assign(const ObUnitLSStat &other);
  bool is_deleting() const
  {
    return ObUnit::Status::UNIT_STATUS_DELETING == unit_->status_;
  }
  bool is_valid_for_normal_ls() const
  {
    return !is_deleting() && !is_sys_standalone_;
  }
  int64_t get_ls_group_count() const
  {
    return ls_group_info_.count();
  }
  bool is_gts_standalone_applicable() const
  {
    return common::ObReplicaTypeCheck::gts_standalone_applicable(unit_->replica_type_);
  }
public:
  ObUnit* unit_;
  //only for foreach
  uint64_t unit_id_;
  bool is_sys_standalone_;
  ObSEArray<ObLSGroupStat*, 3> ls_group_info_;
  ObZoneLSStat* zone_stat_;
  ObUnitGroupStat* unit_group_stat_;
};

//两组异构的unit_group是不会有unit_id和unit_groupid级别的交叉的
struct ObUnitGroupStat
{
  ObUnitGroupStat() { reset(); }
  ~ObUnitGroupStat() {}
  void reset()
  {
    ug_id_ = OB_INVALID_ID;
    unit_info_.reset();
    ls_group_info_.reset();
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != ug_id_
           && !unit_info_.empty();
  }
  //TODO后续考虑gts独占的也不进入unit_group
  //这个应该默认就是返回true的
  bool is_valid_add_ls() const
  {
    return is_valid() && OB_NOT_NULL(unit_info_.at(0))
           && unit_info_.at(0)->is_valid_for_normal_ls();
  }
  int init(ObUnitLSStat &unit_info);
  int add_unit(ObUnitLSStat &unit);
  int assign(const ObUnitGroupStat& other);
  int get_unit_info_by_zone(const ObZone &zone, const ObUnitLSStat* &unit_info) const;
  //日志流初始化的时候，取的是第一个unit上所有的日志流组信息
  int construct_ls_group_info();
  //在对unit group对齐的时候，或者其他的均衡操作，需要增加ls_group信息
  int try_add_ls_group(ObLSGroupStat* lg_info);
  int64_t get_ls_group_count() const
  {
    return ls_group_info_.count();
  }
  int get_zone_array(ObIArray<ObZone> &zone_array) const;
  int get_zone_info_array(ObIArray<ObZoneLSStat*> &zone_info_array);

  DECLARE_TO_STRING;
public:
  uint64_t ug_id_;
  ObSEArray<ObUnitLSStat*, 7> unit_info_;
  ObSEArray<ObLSGroupStat*, 3> ls_group_info_;
};

typedef ObSEArray<ObUnitGroupStat*, 20> ObUGArray;
typedef ObSEArray<ObUGArray, 2> ObHeteroUGArray;

struct ObZoneLSStat
{
public:
  ObZoneLSStat() { reset();}
  ObZoneLSStat(const ObZone &zone, bool in_locality, common::ObReplicaType replica_type = common::ObReplicaType::REPLICA_TYPE_FULL) :
    zone_(zone), is_balance_(false), is_in_locality_(in_locality),
    replica_type_(replica_type),
    gts_unit_(NULL), valid_unit_array_(), deleting_unit_array_() {}
  ~ObZoneLSStat() {}
  void reset()
  {
    zone_.reset();
    is_balance_ = false;
    is_in_locality_ = false;
    replica_type_ = common::ObReplicaType::REPLICA_TYPE_FULL;
    gts_unit_ = NULL;
    valid_unit_array_.reset();
    deleting_unit_array_.reset();
  }
  DECLARE_TO_STRING;
  bool is_valid() const
  {
    return !zone_.is_empty();
  }
  int assign(const ObZoneLSStat &other);
  int add_unit_ls_info(ObUnitLSStat &zone_ls_stat);
  int set_unit_gts_standalone(ObUnitLSStat &unit_stat);
  //由于deleting状态的unit不计入unit_group中，所以直接使用ug_array的大小即可
  int64_t get_valid_unit_num() const
  {
    return valid_unit_array_.count();
  }
  int64_t get_pool_unit_num() const
  {
    // including all units except for deleting units
    return valid_unit_array_.count() + (NULL == gts_unit_ ? 0 : 1);
  }
  int get_unit_info_by_ug_id(const uint64_t ug_id, ObUnitLSStat* &unit_info);
  int get_min_unit_valid_for_normal_ls(ObUnitLSStat* &unit_info) const;
  int get_max_unit_valid_for_normal_ls(ObUnitLSStat* &unit_info) const;
  int get_ug_array(ObUGArray &ug_array);
  int set_is_balance();
  bool is_all_unit_active() const;
  int calculate_variance_score(double &variance_score) const;
  // 判断在开启gts_standalone时，该zone是否适用gts_standalone
  bool is_gts_standalone_applicable() const
  {
    return common::ObReplicaTypeCheck::gts_standalone_applicable(replica_type_) && is_in_locality_;
  }
  bool is_need_to_align_to_ug() const
  {
    return common::ObReplicaTypeCheck::need_to_align_to_ug(replica_type_) && is_in_locality_;
  }
public:
  common::ObZone zone_;
  bool is_balance_;
  bool is_in_locality_;
  common::ObReplicaType replica_type_;
  ObUnitLSStat* gts_unit_;//可能为空
  ObSEArray<ObUnitLSStat*, 10> valid_unit_array_;
  ObSEArray<ObUnitLSStat*, 5> deleting_unit_array_;//deleting
};

class ChooseZoneCmp
{
public:
  ChooseZoneCmp() : ret_(OB_SUCCESS) {}
  ~ChooseZoneCmp() {}
public:
  bool operator()(const ObZoneLSStat *left, const ObZoneLSStat *right);
  int get_ret() const
  {
    return ret_;
  }
private:
  int ret_;
};

class ChooseUGCmp
{
public:
  ChooseUGCmp() : ret_(OB_SUCCESS) {}
  ~ChooseUGCmp() {}
public:
  bool operator()(const ObUnitLSStat *left, const ObUnitLSStat *right);
  int get_ret() const
  {
    return ret_;
  }
private:
  int ret_;
};

class ChooseSysLSUnitCmp
{
public:
  ChooseSysLSUnitCmp(const bool is_determine_gts_unit, const ObLSInfo &sys_ls_info, const ObUnitLSStat *inherit_unit = nullptr) :
      is_determine_gts_unit_(is_determine_gts_unit), sys_ls_info_(sys_ls_info), inherit_unit_(inherit_unit), ret_(OB_SUCCESS) {}
  ~ChooseSysLSUnitCmp() {}
  bool operator()(const ObUnitLSStat *left, const ObUnitLSStat *right);
  int try_compare_by_sys_ls_replica(
      const ObUnitLSStat *left, const ObUnitLSStat *right, bool &bret, bool &priority_decided);
  int get_ret() const { return ret_; }
private:
  const bool is_determine_gts_unit_;
  const ObLSInfo &sys_ls_info_;
  const ObUnitLSStat *inherit_unit_;
  int ret_;
};

class ObTenantLSBalanceInfo
{
public:
  ObTenantLSBalanceInfo(ObIAllocator &allocator):
    is_inited_(false), tenant_id_(), tenant_role_(), job_desc_(), sys_ls_info_(), sys_ls_target_unit_list_(),
    normal_ls_info_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "LSInfo")),
    duplicate_ls_info_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "DupLSInfo")),
    unit_info_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "UnitInfo")),
    ls_group_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "LGInfo")),
    unit_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "UnitArray")),
    ug_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "UGArray")),
    zone_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ZoneArray"))
  {}
  ~ObTenantLSBalanceInfo() {}
  void reset()
  {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    tenant_role_.reset();
    job_desc_.reset();
    sys_ls_info_.reset();
    sys_ls_target_unit_list_.reset();
    normal_ls_info_.reset();
    duplicate_ls_info_.reset();
    unit_info_.reset();
    ls_group_array_.reset();
    unit_array_.reset();
    ug_array_.reset();
    zone_array_.reset();
  }

  int init_tenant_ls_balance_info(const uint64_t tenant_id,
      const share::ObLSStatusInfoArray &status_array,
      const ObBalanceJobDesc &job_desc, const ObArray<ObUnit> &unit_array,
      const ObTenantRole &tenant_role);
  int get_unit_num_lcm(int64_t &unit_num) const;
  int get_target_ls_count(int64_t &target_num) const;
  const common::ObArray<ObUnit> &get_unit_info() { return unit_info_; }
  friend class ObLSBalanceStrategy;
  friend class ObDupLSBalance;
  friend class ObLSGroupCountBalance;
  friend class ObUnitListBalance;
  friend class ObLSGroupLocationBalance;
  friend class ObLSCountBalance;
  friend class ObUnitGroupBalance;
  friend class ObLSGroupCountBalance;

  int check_inner_stat() const;
  //参考locality
  int get_valid_unit_list(share::ObUnitIDList &unit_list);
  TO_STRING_KV(K_(tenant_id), K_(tenant_role), K_(job_desc), K(sys_ls_info_),
                 K(sys_ls_target_unit_list_), K(normal_ls_info_), K(duplicate_ls_info_),
                 K(unit_info_), K(ls_group_array_), K(ug_array_),
                 K(unit_array_), K(zone_array_));
  int split_hetero_zone_to_homo_unit_group(ObHeteroUGArray &hetero_ug_array);
  int split_hetero_zone_to_homo_zone_stat(ObArray<ObArray<ObZoneLSStat*>> &hetero_zone_array);
  int get_ug_locality_unit_list(ObUnitGroupStat &ug_info, share::ObUnitIDList &unit_list);
  int get_zone_info(const ObZone &zone, ObZoneLSStat* &zone_info);
  int get_unit_zone_info(const uint64_t unit_id, ObZoneLSStat* &zone_info);
  int get_ls_group_info(const uint64_t ls_group_id, ObLSGroupStat* &lg_info);
  int get_unit_ls_info(const uint64_t unit_id, ObUnitLSStat* &unit_ls_info);
  int get_ls_unit_list(const share::ObLSStatusInfo &ls_info,
      ObArray<ObUnit*> &unit_list);
  int get_unit_group_info(const uint64_t ug_id, ObUnitGroupStat* &ug_info);
  int check_unit_list_valid(const share::ObUnitIDList &unit_list, bool &is_valid);
  int get_min_unit_group(common::ObIArray<ObUnitGroupStat*> &ug_array,
      ObUnitGroupStat* &ug_info);
  int get_max_unit_group(common::ObIArray<ObUnitGroupStat*> &ug_array,
      ObUnitGroupStat* &ug_info);
  int get_next_ls_status_info(
      const int64_t start_pos,
      ObLSGroupStat &lg_stat,
      int64_t &pos,
      ObLSStatusInfo *&status_info);
  int get_ls_status_in_lg(
      ObLSGroupStat &lg_stat,
      ObArray<ObLSStatusInfo*> &ls_status_array);
  int get_all_ls_group(ObArray<ObLSGroupStat*> &ls_group_array);
  bool is_primary() const
  {
    return tenant_role_.is_primary();
  }

private:
  int build_ls_group_array_();
  int build_unit_ls_array_();
  int get_or_create_unit_group_info_(ObUnitLSStat &unit_ls_info, ObUnitGroupStat* &ug_info);
  int create_new_zone_info_(ObZone &zone, const common::ObReplicaType replica_type);
  int build_zone_ls_info_();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObTenantRole tenant_role_;
  ObBalanceJobDesc job_desc_;
  //初始值
  share::ObLSStatusInfo sys_ls_info_;
  ObUnitIDList sys_ls_target_unit_list_;
  //正常可以在均衡的日志流
  common::ObArray<share::ObLSStatusInfo> normal_ls_info_;
  common::ObArray<share::ObLSStatusInfo> duplicate_ls_info_;
  common::ObArray<ObUnit> unit_info_;
  //构造值
  common::ObArray<ObLSGroupStat> ls_group_array_;
  common::ObArray<ObUnitLSStat> unit_array_;
  common::ObArray<ObUnitGroupStat> ug_array_;
  common::ObArray<ObZoneLSStat> zone_array_;
};

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
typedef common::ObArray<ObSplitLSParam> ObSplitLSParamArray;

class ObLSBalanceStrategy
{
public:
  ObLSBalanceStrategy(ObTenantLSBalanceInfo *tenant_ls_balance_info,
                      ObMySQLProxy *sql_proxy,
                      const share::ObBalanceJobID &specified_job_id,
                      share::ObBalanceJob *job,
                      common::ObArray<share::ObBalanceTask> *task_array,
                      common::ObArray<share::ObLSGroupUnitListOp>* lg_op_array,
                      common::ObArray<share::ObUnitUGOp>* unit_ug_op_array):
  tenant_info_(tenant_ls_balance_info), sql_proxy_(sql_proxy), specified_job_id_(specified_job_id),
  job_(job), task_array_(task_array), lg_op_array_(lg_op_array), unit_ug_op_array_(unit_ug_op_array), tenant_ls_bg_info_() {}
  ~ObLSBalanceStrategy() {}
  //only_job_strategy : 只检查是否需要balance
  virtual int balance(const bool only_job_strategy) = 0;
  int check_inner_stat() const;
  int generate_balance_job(const share::ObBalanceStrategy &balance_strategy, const bool only_job_strategy);
  int construct_ls_part_info(const ObSplitLSParam &src_ls,
      const share::ObLSID &dest_ls_id,
      share::ObTransferPartList &part_list);
  int try_construct_job_without_task(const share::ObBalanceStrategy &balance_strategy, const bool only_job_strategy);
  int construct_src_split_param_array(
      ObLSGroupStat &lg_array,
      ObSplitLSParamArray &src_ls);
  int construct_expand_dest_param(
      const int64_t lack_ls_count,
      ObSplitLSParamArray &src_ls,
      ObIArray<ObSplitLSParamArray> &dest_array);
  int construct_shrink_src_param(
      const int64_t target_count,
      ObSplitLSParamArray &src_ls,
      ObIArray<ObSplitLSParamArray> &dest_split_array);
  int filter_unit_list_in_locality_(const ObUnitIDList &unit_list, ObUnitIDList &tgt_unit_list, ObArray<ObZone> &tgt_zone_list);
  //把日志流组的unit_list转换成unit_group_id
  int convert_unit_list_to_unit_stat_(const ObUnitIDList &unit_list,
                                  ObArray<ObUnitLSStat*> &unit_stat_array);
  int reorganize_sys_ls_unit_list_();
  int do_reorganize_sys_ls_unit_list_(const bool need_remove_deleting_unit);
  int determine_zone_sys_ls_unit_(const ObLSInfo &sys_ls_info, ObZoneLSStat &zone_stat, ObUnitLSStat *&determined_unit);
  int get_valid_unit_by_inherit_(ObZoneLSStat &zone_stat,
                      const ObUnitIDList &valid_unit_list, ObUnitLSStat* &unit);

protected:
  ObTenantLSBalanceInfo *tenant_info_;
  ObMySQLProxy *sql_proxy_;
  share::ObBalanceJobID specified_job_id_;
  share::ObBalanceJob *job_;
  common::ObArray<share::ObBalanceTask> *task_array_;
  common::ObArray<share::ObLSGroupUnitListOp> *lg_op_array_;
  common::ObArray<share::ObUnitUGOp> *unit_ug_op_array_;
  ObTenantLSBalanceGroupInfo tenant_ls_bg_info_;
};

//广播日志流均衡
class ObDupLSBalance : public ObLSBalanceStrategy
{
public:
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObDupLSBalance() {}
  int balance(const bool only_job_strategy);
private:
  int check_dup_ls_need_balance_(bool &need_balance);
  int generate_shrink_task_();
  int get_ls_group_(uint64_t &ls_group_id);
};
//修改__all_unit表，根据日志流组的实际分布
class ObUnitGroupBalance : public ObLSBalanceStrategy
{
public:
  // if zone unit_num larger than this value, do not execute balance due to complexity
  const int64_t MAX_SUPPORTED_UNIT_NUM = 10;
  class MaxWeightMatchHelper
  {
  public:
    MaxWeightMatchHelper() : is_inited_(false), lx_(), ly_(), vis_x_(), vis_y_(), match_() {}
    ~MaxWeightMatchHelper() {}
    int init(const ObMatrix<int64_t> *weight);
    int do_match_by_KM_algorithm();
    const ObArray<int64_t> &get_match_result() const { return match_; }
  private:
    static constexpr int64_t LOG_MSG_LEN = 128;
    int reset_all_();
    int reset_visited_();
    int check_inner_stat_();
    int search_path_(const int64_t u, bool &found, char *buf, int64_t &buf_pos);
    bool is_inited_;
    // weight_[i][j] is the weight of edge between x[i] and y[j]
    const ObMatrix<int64_t> *weight_;
    int64_t node_n_;
    // labels of each side
    ObArray<int64_t> lx_;
    ObArray<int64_t> ly_;
    // visited flag of nodes
    ObArray<bool> vis_x_;
    ObArray<bool> vis_y_;
    // match result, match[i] is index of x matched by y[i]
    ObArray<int64_t> match_;
  };
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObUnitGroupBalance() {}
  int balance(const bool only_job_strategy);
private:
  int try_determine_and_set_gts_units_();
  int reorganize_homo_zones_ug_(const common::ObIArray<ObZoneLSStat*> &homo_zone_stats);
  int reorganize_zone_ug_to_ref_zone_(const ObZoneLSStat &ref_zone, const ObZoneLSStat &adjust_zone);
  int stat_ls_count_matrix_(const ObZoneLSStat &ref_zone, const ObZoneLSStat &adjust_zone, ObMatrix<int64_t> &ls_count_matrix, bool &all_ls_in_unit_group);
  int compute_max_weight_match_(const ObMatrix<int64_t> &ls_count_matrix, ObArray<int64_t> &match_indices);
  int compute_update_units_(const ObZoneLSStat &ref_zone, const ObZoneLSStat &adjust_zone, const ObArray<int64_t> &match_indices);
};

//修改日志流组的unit_list，保证日志流组都在恰当的unit_group上
class ObLSGroupLocationBalance : public ObLSBalanceStrategy
{
public:
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObLSGroupLocationBalance() {}
  int balance(const bool only_job_strategy);
private:
  int choose_unit_group_from_ug_array_(const ObArray<ObUnitLSStat*> &unit_stat_array,
                                       ObUGArray &ug_array,
                                       uint64_t &unit_group_id);
  int get_unit_info_in_ug_array_(const ObArray<ObUnitLSStat*> &unit_stat_array,
                                 const ObUGArray &ug_array,
                                 common::hash::ObHashMap<uint64_t, int64_t> &ug_map,
                                 ObArray<const ObUnitLSStat*> &unit_in_ug);
  int reorganize_new_unit_list_(const ObArray<uint64_t> &unit_group_array,
                                ObArray<ObUnitLSStat*> &unit_stat_array,
                                ObLSGroupStat &ls_group);
  int try_fix_ug_ls_group_(ObLSGroupStat &ls_group,
                           const ObArray<uint64_t> &unit_group_array,
                           ObHeteroUGArray &hetero_ug);
  int get_min_ug_in_ug_array_(ObUGArray &ug_array, uint64_t &unit_group_id);
  int get_min_ug_in_unit_list_(const ObArray<const ObUnitLSStat*> &unit_stat_array, uint64_t &unit_group_id);
};

//日志流组的个数均衡
class ObLSGroupMatrixCell
{
public:
  ObLSGroupMatrixCell() : inner_ls_group_stat_array_(), target_lg_cnt_(-1) {}
  ~ObLSGroupMatrixCell() {}
  void reset()
  {
    inner_ls_group_stat_array_.reset();
    target_lg_cnt_ = -1;
  }

  bool is_empty() const { return 0 == inner_ls_group_stat_array_.count(); }
  int64_t get_ls_group_count() const
  {
    return inner_ls_group_stat_array_.count();
  }
  int get_ls_count(int64_t &ls_count) const;

  int64_t get_target_lg_cnt() const
  {
    return target_lg_cnt_;
  }
  int64_t get_expand_shrink_cnt() const
  {
    return fabs(target_lg_cnt_ - get_ls_group_count()) ;
  }
  void inc_expand_cnt()
  {
    target_lg_cnt_++;
  }
  void inc_shrink_cnt()
  {
    target_lg_cnt_--;
  }
  ObArray<ObLSGroupStat*> &get_ls_groups()
  {
    return inner_ls_group_stat_array_;
  }

  int assign(const ObLSGroupMatrixCell &other);
  int init_ls_groups(ObIArray<ObLSGroupStat*> &ls_groups_to_add);
  int add_ls_groups(ObIArray<ObLSGroupStat*> &ls_groups_to_add);

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int inner_add_ls_group_(ObLSGroupStat* &ls_groups_to_add);
  ObArray<ObLSGroupStat*> inner_ls_group_stat_array_;
  int64_t target_lg_cnt_;
};

typedef ObMatrix<ObLSGroupMatrixCell> LSGroupMatrix;

class ObLSGroupCountBalance : public ObLSBalanceStrategy
{
public:
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObLSGroupCountBalance() {}
  int balance(const bool only_job_strategy);
private:
  int check_can_balance_ls_group_(bool &need_balance,
      int64_t &target_lg_count) const;
  int construct_ls_group_matrix_to_do_balance_(
      LSGroupMatrix &ls_group_matrix);
  int construct_ls_group_matrix_with_homo_deployed_(
      const ObHeteroUGArray &hetero_ug_array,
      LSGroupMatrix &ls_group_matrix);
  int construct_ls_group_matrix_with_hetero_deployed_(
      const ObHeteroUGArray &hetero_ug_array,
      LSGroupMatrix &ls_group_matrix);
  int choose_row_and_columns_for_matrix_(
      const ObHeteroUGArray &hetero_ug_array,
      ObUGArray &row_ug_array,
      ObUGArray &column_ug_array);
  int find_most_available_zone_(
      const ObUGArray &ug_array,
      ObZoneLSStat*& most_available_zone);
  int build_matrix_cell_(
      ObUnitGroupStat &row_ug_stat,
      ObUnitGroupStat &column_ug_stat,
      ObLSGroupMatrixCell &ls_group_matrix_cell);
  int build_matrix_for_single_parent_ls_group_(
      bool is_row_array,
      ObUGArray &ug_array,
      LSGroupMatrix &lg_matrix);
  int build_matrix_for_orphans_ls_group_(
      LSGroupMatrix &lg_matrix);
  int balance_each_unit_group_by_row_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt);
  int expand_ls_group_cnt_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt, const int64_t curr_lg_cnt);
  int set_cell_expand_lg_cnt_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt, const int64_t curr_lg_cnt);
  int expand_empty_row_by_create_(LSGroupMatrix &lg_matrix, const int64_t row_index, const int64_t target_lg_cnt);
  int construct_expand_lg_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt, const int64_t curr_lg_cnt,
      ObArray<ObLSGroupStat> &expand_lg);
  int construct_expand_task_for_cell_emtpy_lg_(ObIArray<ObLSGroupStat*> &curr_lg_array);
  int get_cell_expand_lg_(ObLSGroupMatrixCell &cell,
    ObIArray<ObLSGroupStat*> &curr_lg_array,
    ObArray<ObLSGroupStat> &expand_lg_array);
  int balance_ls_between_lgs_in_cell_(ObArray<ObLSGroupStat*> &curr_lg_array);
  int generate_alter_task_for_each_lg_(ObArray<ObLSGroupStat*> &curr_lg_array);
  int construct_ls_expand_task(const uint64_t ls_group_id,
      const ObSplitLSParamArray &dest_split_param);
  int shrink_ls_group_cnt_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt, const int64_t curr_lg_cnt);
  int get_cell_with_max_lg_in_column_(LSGroupMatrix &lg_matrix, const int64_t row_index, ObLSGroupMatrixCell* &cell);
  int construct_shrink_task_(LSGroupMatrix &lg_matrix, const int64_t row_index);
  int complete_missing_LS_in_lg_(ObArray<ObLSGroupStat*> &target_lg,
      ObArray<ObLSGroupStat*> &shrink_lg);
  int construct_lg_shrink_task_(ObArray<ObLSGroupStat*> &target_lg,
                                ObArray<ObLSGroupStat*> &shrink_lg);
  int construct_lg_shrink_transfer_task_(ObArray<ObLSGroupStat*> &target_lg,
                                         ObArray<ObLSGroupStat*> &shrink_lg);
  int generate_shrink_lg_disable_transfer_(ObArray<ObLSGroupStat*> &target_lg,
      ObArray<ObLSGroupStat*> &shrink_lg);
  int construct_ls_shrink_task_(const ObSplitLSParamArray &src_split_param,
                                const ObLSStatusInfo &ls_status_info);
};

//日志流组内均衡
class ObLSCountBalance : public ObLSBalanceStrategy
{
public:
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObLSCountBalance() {}
  int balance(const bool only_job_strategy);
private:
  enum BalanceOp {
    NO_OP,
    TRANSFER,
    MATCH_LS_COUNT,
  };
  int get_op_and_generate_job_(BalanceOp &op, const bool only_job_strategy);
  int generate_expand_task_(ObLSGroupStat &lg_stat);
  int generate_shrink_task_(ObLSGroupStat &lg_stat);
  int generate_shrink_task_for_each_ls_(
    const ObSplitLSParamArray &src_split_param,
    const ObLSStatusInfo &ls_status_info);
  int generate_ls_split_task_(const ObSplitLSParamArray &dest_split_param);
  int generate_transfer_task_(const ObSplitLSParam &param, const ObLSStatusInfo &ls_status_info);
};

//unit_list均衡，包括对齐locality
class ObUnitListBalance : public ObLSBalanceStrategy
{
public:
  using ObLSBalanceStrategy::ObLSBalanceStrategy;
  ~ObUnitListBalance() {}
  int balance(const bool only_job_strategy);
private:
  int reorganize_unit_list_by_locality_(ObLSGroupStat &ls_group);
  int construct_lg_unit_group_ids_(ObHeteroUGArray &hetero_ug, ObLSGroupStat &ls_group);
  int balance_ls_unit_group_(ObUGArray &ug_array);
  int balance_ls_unit_for_L_(ObZoneLSStat &zone_stat);
  int construct_lg_unit_list_();
};

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
  static int check_unit_array_same(
      const ObIArray<ObUnit> &left_arr,
      const ObIArray<ObUnit> &right_arr);

/////////////////// static functions end ///////////////////

public:
  ObLSBalanceTaskHelper (ObIAllocator &allocator);
  ~ObLSBalanceTaskHelper() {}
  int init(const uint64_t tenant_id, const share::ObLSStatusInfoArray &status_array,
           const ObBalanceJobDesc &job_desc, const ObArray<ObUnit> &unit_array,
           const ObTenantRole &tenant_role,
           ObMySQLProxy *sql_proxy);
  //check need ls balance
  int check_need_ls_balance(bool &need_balance);
  //generate ls balance job and task
  //generate new job_id if specified_job_id is not specified
  int generate_ls_balance_task(
      const bool only_job_strategy,
      const ObBalanceJobID &specified_job_id = ObBalanceJobID());
  int execute_job_without_task();

  bool need_ls_balance() const
  {
    return job_.is_valid();
  }
  share::ObBalanceJob& get_balance_job()
  {
    return job_;
  }
  common::ObArray<share::ObBalanceTask>& get_balance_tasks()
  {
    return task_array_;
  }
  common::ObArray<share::ObLSGroupUnitListOp>& get_ls_group_op()
  {
    return lg_op_array_;
  }
  common::ObArray<share::ObUnitUGOp>& get_unit_ug_op()
  {
    return unit_ug_op_array_;
  }
  const common::ObArray<ObUnit> &get_unit_array()
  {
    return tenant_info_.get_unit_info();
  }

  TO_STRING_KV(K_(inited), K_(job), K_(task_array), K_(lg_op_array), K_(unit_ug_op_array), K_(tenant_info));

private:
  struct ObUnitIdCmp
  {
    bool operator()(const ObUnit &lhs, const ObUnit &rhs) const
    {
      return lhs.unit_id_ < rhs.unit_id_;
    }
  };
  int update_ls_group_unit_list_in_table_();
  int update_unit_ug_id_in_table_();
  int lock_balance_job_(
      const ObBalanceJob &balance_job,
      ObMySQLTransaction &job_trans);
private:
  bool inited_;
  ObMySQLProxy *sql_proxy_;
  share::ObBalanceJob job_;
  common::ObArray<share::ObBalanceTask> task_array_;
  common::ObArray<share::ObLSGroupUnitListOp> lg_op_array_;
  common::ObArray<share::ObUnitUGOp> unit_ug_op_array_;
  ObTenantLSBalanceInfo tenant_info_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_BALANCE_HELPER_H */
