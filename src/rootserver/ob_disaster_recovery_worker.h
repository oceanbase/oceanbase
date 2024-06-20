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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_WORKER_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_WORKER_H_

#include "share/ob_define.h"
#include "share/ob_unit_getter.h"
#include "ob_unit_stat_manager.h"
#include "ob_root_utils.h"
#include "ob_disaster_recovery_info.h"
#include "lib/thread/ob_async_task_queue.h"
#include "ob_disaster_recovery_task.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
}
namespace share
{
class ObLSInfo;
class ObLSTableOperator;
class ObLSReplica;
}
namespace rootserver
{
class ObUnitManager;
class ObZoneManager;
class ObDRTaskMgr;
class DRLSInfo;
class ObDstReplica;
struct DRServerStatInfo;
struct DRUnitStatInfo;
struct ObDRTaskKey;

class ObLSReplicaTaskDisplayInfo
{
public:
  ObLSReplicaTaskDisplayInfo();
  explicit ObLSReplicaTaskDisplayInfo(const ObLSReplicaTaskDisplayInfo &other) { assign(other); }
  ~ObLSReplicaTaskDisplayInfo();
  void reset();

  int init(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const ObDRTaskType &task_type,
      const ObDRTaskPriority &task_priority,
      const common::ObAddr &target_server,
      const common::ObReplicaType &target_replica_type,
      const int64_t &target_replica_paxos_replica_number,
      const common::ObAddr &source_server,
      const common::ObReplicaType &source_replica_type,
      const int64_t &source_replica_paxos_replica_number,
      const common::ObAddr &execute_server,
      const ObString &comment);
  inline bool is_valid() const;

  int assign(const ObLSReplicaTaskDisplayInfo &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  inline const uint64_t &get_tenant_id() const { return tenant_id_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline const ObDRTaskType &get_task_type() const { return task_type_; }
  inline const ObDRTaskPriority &get_task_priority() const { return task_priority_; }
  inline const common::ObAddr &get_target_server() const { return target_server_; }
  inline const common::ObReplicaType &get_target_replica_type() const { return target_replica_type_; }
  inline const int64_t &get_target_replica_paxos_replica_number() const { return target_replica_paxos_replica_number_; }
  inline const common::ObAddr &get_source_server() const { return source_server_; }
  inline const common::ObReplicaType &get_source_replica_type() const { return source_replica_type_; }
  inline const int64_t &get_source_replica_paxos_replica_number() const { return source_replica_paxos_replica_number_; }
  inline const common::ObAddr &get_execute_server() const { return execute_server_; }
  inline const ObSqlString &get_comment() const { return comment_; }

private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObDRTaskType task_type_;
  ObDRTaskPriority task_priority_;
  common::ObAddr target_server_;
  common::ObReplicaType target_replica_type_;
  int64_t target_replica_paxos_replica_number_;
  common::ObAddr source_server_;
  common::ObReplicaType source_replica_type_;
  int64_t source_replica_paxos_replica_number_;
  common::ObAddr execute_server_;
  ObSqlString comment_;
};

class ObDRWorker : public share::ObCheckStopProvider
{
public:
  typedef common::ObIArray<ObLSReplicaTaskDisplayInfo> LSReplicaTaskDisplayInfoArray;
public:
  ObDRWorker(volatile bool &stop);
  virtual ~ObDRWorker();
public:
  int init(
      common::ObAddr &self_addr,
      common::ObServerConfig &cfg,
      ObZoneManager &zone_mgr,
      ObDRTaskMgr &task_mgr,
      share::ObLSTableOperator &lst_operator,
      share::schema::ObMultiVersionSchemaService &schema_service,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      common::ObMySQLProxy &sql_proxy);
  int try_disaster_recovery();
  int try_tenant_disaster_recovery(
      const uint64_t tenant_id,
      const bool only_for_display,
      int64_t &acc_dr_task);
  static int check_tenant_locality_match(
      const uint64_t tenant_id,
      ObZoneManager &zone_mgr,
      bool &locality_is_matched);

  inline int64_t get_display_task_count_() const { return display_tasks_.count(); }
  int get_task_plan_display(
      common::ObSArray<ObLSReplicaTaskDisplayInfo> &task_plan);

private:
  struct TaskCountStatistic
  {
  public:
    TaskCountStatistic()
      : remain_task_cnt_(-1), // -1 means initial status
        total_task_one_round_(0) {}
    void reset() { remain_task_cnt_ = -1; total_task_one_round_ = 0; }
    void accumulate_task(const int64_t acc) { total_task_one_round_ += acc; }
    int64_t get_total_task_one_round() const { return total_task_one_round_; }
    void set_remain_task_cnt(const int64_t r) { remain_task_cnt_ = r; }
    int64_t get_remain_task_cnt() const { return remain_task_cnt_; }
    TO_STRING_KV(K_(remain_task_cnt),
                 K_(total_task_one_round));
  public:
    int64_t remain_task_cnt_;
    int64_t total_task_one_round_;
  };

  enum MemberChangeType
  {
    MEMBER_CHANGE_ADD = 0,
    MEMBER_CHANGE_NOP,
    MEMBER_CHANGE_SUB,
  };

  static int generate_disaster_recovery_paxos_replica_number(
      const DRLSInfo &dr_ls_info,
      const int64_t curr_paxos_replica_number,
      const int64_t locality_paxos_replica_number,
      const MemberChangeType member_change_type,
      int64_t &new_paxos_replica_number,
      bool &found);
  enum LATaskType
  {
    RemovePaxos = 0,
    RemoveNonPaxos,
    AddReplica,
    TypeTransform,
    ModifyPaxosReplicaNumber,
  };

  enum class LATaskPrio : int64_t
  {
    LA_P_ADD_FULL = 1,
    LA_P_READONLY_TO_FULL,
    LA_P_ADD_LOGONLY,
    LA_P_ADD_ENCRYPTION,
    LA_P_FULL_TO_LOGONLY,
    LA_P_ADD_READONLY,
    LA_P_REMOVE_NON_PAXOS,
    LA_P_FULL_TO_READONLY,
    LA_P_REMOVE_PAXOS,
    LA_P_MODIFY_PAXOS_REPLICA_NUMBER,
    LA_P_MAX,
  };

  struct LATask
  {
  public:
    LATask() {}
    virtual ~LATask() {}
  public:
    virtual LATaskType get_task_type() const = 0;
    virtual LATaskPrio get_task_priority() const = 0;
    virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  };

  struct LATaskCmp
  {
  public:
    LATaskCmp(common::ObArray<LATask *> &task_array)
      : task_array_(task_array),
        ret_(common::OB_SUCCESS) {}
    int execute_sort();
    bool operator()(const LATask *left, const LATask *right);
  public:
    common::ObArray<LATask *> &task_array_;
    int ret_;
  };

  struct RemoveReplicaLATask : public LATask
  {
  public:
    RemoveReplicaLATask()
      : LATask(),
        remove_server_(),
        replica_type_(REPLICA_TYPE_MAX),
        memstore_percent_(100),
        member_time_us_(-1),
        orig_paxos_replica_number_(0),
        paxos_replica_number_(0) {}
    virtual ~RemoveReplicaLATask() {}
  public:
    virtual LATaskType get_task_type() const override { return ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)
                                                               ? RemovePaxos
                                                               : RemoveNonPaxos; }
    virtual LATaskPrio get_task_priority() const override {
      LATaskPrio priority = ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)
                          ? LATaskPrio::LA_P_REMOVE_PAXOS
                          : LATaskPrio::LA_P_REMOVE_NON_PAXOS;
      return priority;
    }
    VIRTUAL_TO_STRING_KV("task_type", get_task_type(),
                         K_(remove_server),
                         K_(replica_type),
                         K_(memstore_percent),
                         K_(member_time_us),
                         K_(orig_paxos_replica_number),
                         K_(paxos_replica_number));
  public:
    common::ObAddr remove_server_;
    ObReplicaType replica_type_;
    int64_t memstore_percent_;
    int64_t member_time_us_;
    int64_t orig_paxos_replica_number_;
    int64_t paxos_replica_number_;
  };

  struct AddReplicaLATask : public LATask
  {
  public:
    AddReplicaLATask()
      : LATask(),
        zone_(),
        dst_server_(),
        unit_id_(OB_INVALID_ID),
        unit_group_id_(OB_INVALID_ID),
        replica_type_(REPLICA_TYPE_MAX),
        memstore_percent_(100),
        member_time_us_(-1),
        orig_paxos_replica_number_(0),
        paxos_replica_number_(0) {}
    virtual ~AddReplicaLATask() {}
  public:
    virtual LATaskType get_task_type() const override{ return AddReplica; }
    virtual LATaskPrio get_task_priority() const override {
      LATaskPrio priority = LATaskPrio::LA_P_MAX;
      if (common::REPLICA_TYPE_FULL == replica_type_) {
        priority = LATaskPrio::LA_P_ADD_FULL;
      } else if (common::REPLICA_TYPE_LOGONLY == replica_type_) {
        priority = LATaskPrio::LA_P_ADD_LOGONLY;
      } else if (common::REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type_) {
        priority = LATaskPrio::LA_P_ADD_ENCRYPTION;
      } else if (common::REPLICA_TYPE_READONLY == replica_type_) {
        priority = LATaskPrio::LA_P_ADD_READONLY;
      } else {} // default priority value
      return priority;
    }

    VIRTUAL_TO_STRING_KV("task_type", get_task_type(),
                         K(zone_),
                         K(dst_server_),
                         K(unit_id_),
                         K(unit_group_id_),
                         K(replica_type_),
                         K(memstore_percent_),
                         K(member_time_us_),
                         K(orig_paxos_replica_number_),
                         K(paxos_replica_number_));
  public:
    common::ObZone zone_;
    common::ObAddr dst_server_;
    uint64_t unit_id_;
    uint64_t unit_group_id_;
    ObReplicaType replica_type_;
    int64_t memstore_percent_;
    int64_t member_time_us_;
    int64_t orig_paxos_replica_number_;
    int64_t paxos_replica_number_;
  };

  struct TypeTransformLATask : public LATask
  {
  public:
    TypeTransformLATask()
      : LATask(),
        zone_(),
        dst_server_(),
        unit_id_(OB_INVALID_ID),
        unit_group_id_(OB_INVALID_ID),
        src_replica_type_(REPLICA_TYPE_MAX),
        src_memstore_percent_(100),
        src_member_time_us_(-1),
        dst_replica_type_(REPLICA_TYPE_MAX),
        dst_memstore_percent_(100),
        dst_member_time_us_(-1),
        orig_paxos_replica_number_(0),
        paxos_replica_number_(0) {}
    virtual ~TypeTransformLATask() {}
  public:
    virtual LATaskType get_task_type() const override { return TypeTransform; }
    virtual LATaskPrio get_task_priority() const override {
      LATaskPrio priority = LATaskPrio::LA_P_MAX;
      if (common::REPLICA_TYPE_FULL == dst_replica_type_
          && common::REPLICA_TYPE_READONLY == src_replica_type_) {
        priority = LATaskPrio::LA_P_READONLY_TO_FULL;
      } else if (common::REPLICA_TYPE_LOGONLY == dst_replica_type_
          && common::REPLICA_TYPE_FULL == src_replica_type_) {
        priority = LATaskPrio::LA_P_FULL_TO_LOGONLY;
      } else if (common::REPLICA_TYPE_READONLY == dst_replica_type_
          && common::REPLICA_TYPE_FULL == src_replica_type_) {
        priority = LATaskPrio::LA_P_FULL_TO_READONLY;
      } else {} // default priority value
      return priority;
    }
    VIRTUAL_TO_STRING_KV("task_type", get_task_type(),
                         K(zone_),
                         K(dst_server_),
                         K(unit_id_),
                         K(unit_group_id_),
                         K(src_replica_type_),
                         K(src_memstore_percent_),
                         K(src_member_time_us_),
                         K(dst_replica_type_),
                         K(dst_memstore_percent_),
                         K(dst_member_time_us_),
                         K(orig_paxos_replica_number_),
                         K(paxos_replica_number_));
  public:
    common::ObZone zone_;
    common::ObAddr dst_server_;
    uint64_t unit_id_;
    uint64_t unit_group_id_;
    ObReplicaType src_replica_type_;
    int64_t src_memstore_percent_;
    int64_t src_member_time_us_;
    ObReplicaType dst_replica_type_;
    int64_t dst_memstore_percent_;
    int64_t dst_member_time_us_;
    int64_t orig_paxos_replica_number_;
    int64_t paxos_replica_number_;
  };

  struct ModifyPaxosReplicaNumberLATask : public LATask
  {
  public:
    ModifyPaxosReplicaNumberLATask()
      : orig_paxos_replica_number_(0),
        paxos_replica_number_(0) {}
    virtual ~ModifyPaxosReplicaNumberLATask() {}
  public:
    virtual LATaskType get_task_type() const override { return ModifyPaxosReplicaNumber; }
    virtual LATaskPrio get_task_priority() const override {
      LATaskPrio priority = LATaskPrio::LA_P_MODIFY_PAXOS_REPLICA_NUMBER;
      return priority;
    }

    VIRTUAL_TO_STRING_KV("task_type", get_task_type(),
                         K(orig_paxos_replica_number_),
                         K(paxos_replica_number_));
  public:
    int64_t orig_paxos_replica_number_;
    int64_t paxos_replica_number_;
  };

  struct ReplicaDesc
  {
  public:
    ReplicaDesc(const ObReplicaType replica_type,
                const int64_t memstore_percent,
                const int64_t replica_num)
      : replica_type_(replica_type),
        memstore_percent_(memstore_percent),
        replica_num_(replica_num) {}
    ReplicaDesc()
      : replica_type_(REPLICA_TYPE_MAX),
        memstore_percent_(100),
        replica_num_(0) {}
    TO_STRING_KV(K(replica_type_),
                 K(memstore_percent_),
                 K(replica_num_));
    int64_t cast(const common::ObReplicaType replica_type) {
      int64_t ret_val = 0;
      if (REPLICA_TYPE_READONLY == replica_type) {
        ret_val = 1;
      } else if (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type) {
        ret_val = 2;
      } else if (REPLICA_TYPE_LOGONLY == replica_type) {
        ret_val = 3;
      } else if (REPLICA_TYPE_FULL == replica_type) {
        ret_val = 4;
      } else {
        ret_val = 0; // invalid type, put it at the beginning
      }
      return ret_val;
    }
    bool operator<(const ReplicaDesc &that) {
      return cast(this->replica_type_) < cast(that.replica_type_);
    }
  public:
    ObReplicaType replica_type_;
    int64_t memstore_percent_;
    int64_t replica_num_;
  };

  struct ReplicaStatDesc
  {
  public:
    ReplicaStatDesc(share::ObLSReplica *replica,
                    DRServerStatInfo *server_stat_info,
                    DRUnitStatInfo *unit_stat_info,
                    DRUnitStatInfo *unit_in_group_stat_info)
      : replica_(replica),
        server_stat_info_(server_stat_info),
        unit_stat_info_(unit_stat_info),
        unit_in_group_stat_info_(unit_in_group_stat_info) {}
    ReplicaStatDesc()
      : replica_(nullptr),
        server_stat_info_(nullptr),
        unit_stat_info_(nullptr),
        unit_in_group_stat_info_(nullptr) {}
  public:
    bool is_valid() const {
      return nullptr != replica_
             && nullptr != server_stat_info_
             && nullptr != unit_stat_info_
             && nullptr != unit_in_group_stat_info_;
    }
    int64_t cast(const common::ObReplicaType replica_type) {
      int64_t ret_val = 0;
      if (REPLICA_TYPE_READONLY == replica_type) {
        ret_val = 1;
      } else if (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type) {
        ret_val = 2;
      } else if (REPLICA_TYPE_LOGONLY == replica_type) {
        ret_val = 3;
      } else if (REPLICA_TYPE_FULL == replica_type) {
        ret_val = 4;
      } else {
        ret_val = 0; // invalid type, put it at the beginning
      }
      return ret_val;
    }
    bool operator<(const ReplicaStatDesc &that) {
      bool bool_ret = true;
      if (nullptr == this->replica_ && nullptr != that.replica_) {
        bool_ret = true;
      } else if (nullptr != this->replica_ && nullptr == that.replica_) {
        bool_ret = false;
      } else if (nullptr == this->replica_ && nullptr == that.replica_) {
        bool_ret = true;
      } else {
        bool_ret = cast(this->replica_->get_replica_type())
                   < cast(that.replica_->get_replica_type());
      }
      return bool_ret;
    }
    TO_STRING_KV(KPC(replica_),
                 KPC(server_stat_info_),
                 KPC(unit_stat_info_),
                 KPC(unit_in_group_stat_info_));
  public:
    share::ObLSReplica *replica_;
    DRServerStatInfo *server_stat_info_;
    DRUnitStatInfo *unit_stat_info_;
    DRUnitStatInfo *unit_in_group_stat_info_;
  };

  struct ReplicaDescArray : public common::ObSEArrayImpl<ReplicaDesc, 7>
  {
  public:
    ReplicaDescArray() : common::ObSEArrayImpl<ReplicaDesc, 7>(),
                         is_readonly_all_server_(false),
                         readonly_memstore_percent_(100) {}
  public:
    bool is_readonly_all_server_;
    int64_t readonly_memstore_percent_;
  };


  class UnitProvider
  {
  public:
    UnitProvider()
      : inited_(false),
        tenant_id_(OB_INVALID_ID),
        unit_set_() {}
    int init(
        const uint64_t tenant_id,
        DRLSInfo &dr_ls_info);
    int allocate_unit(
        const common::ObZone &zone,
        const uint64_t unit_group_id,
        share::ObUnit &unit);
    int init_unit_set(
        DRLSInfo &dr_ls_info);

  private:
    int inner_get_valid_unit_(
        const common::ObZone &zone,
        const common::ObArray<share::ObUnit> &unit_array,
        share::ObUnit &output_unit,
        const bool &force_get,
        bool &found);
  private:
    bool inited_;
    uint64_t tenant_id_;
    share::ObUnitTableOperator unit_operator_;
    common::hash::ObHashSet<int64_t> unit_set_;
  };

  typedef common::hash::ObHashMap<
          common::ObZone,
          ReplicaDescArray *,
          common::hash::NoPthreadDefendMode> LocalityMap;
  typedef common::ObArray<ReplicaStatDesc> ReplicaStatMap;

  class LocalityAlignment
  {
  public:
    LocalityAlignment(ObZoneManager *zone_mgr, DRLSInfo &dr_ls_info);
    virtual ~LocalityAlignment();
    int build();
    int get_next_locality_alignment_task(
        const LATask *&task);
    int64_t get_task_array_cnt() const { return task_array_.count(); }
  private:
    int generate_paxos_replica_number();
    int build_locality_stat_map();
    int locate_zone_locality(
        const common::ObZone &zone,
        ReplicaDescArray *&replica_desc_array);
    int build_replica_stat_map();
    int prepare_generate_locality_task();
    int do_generate_locality_task();
    int do_generate_locality_task_from_full_replica(
        ReplicaStatDesc &replica_stat_desc,
        share::ObLSReplica &replica,
        const int64_t index);
    int do_generate_locality_task_from_logonly_replica(
        ReplicaStatDesc &replica_stat_desc,
        share::ObLSReplica &replica,
        const int64_t index);
    int do_generate_locality_task_from_encryption_logonly_replica(
        ReplicaStatDesc &replica_stat_desc,
        share::ObLSReplica &replica,
        const int64_t index);

    int try_generate_type_transform_task_for_readonly_replica_(
        ReplicaDescArray &zone_replica_desc_in_locality,
        ReplicaStatDesc &replica_stat_desc,
        const int64_t index,
        bool &task_generated);
    int try_generate_remove_readonly_task_for_duplicate_log_stream_(
        ReplicaStatDesc &replica_stat_desc,
        share::ObLSReplica &replica,
        const int64_t index);
    int do_generate_locality_task_from_readonly_replica(
        ReplicaStatDesc &replica_stat_desc,
        share::ObLSReplica &replica,
        const int64_t index);

    int try_generate_locality_task_from_locality_map();
    int try_generate_locality_task_from_paxos_replica_number();
    void print_locality_information();
    int generate_locality_task();
    int try_remove_match(
        ReplicaStatDesc &replica_stat_desc,
        const int64_t index);
    // generate specific task
    int generate_remove_replica_task(
        ReplicaStatDesc &replica_stat_desc);
    int generate_type_transform_task(
        ReplicaStatDesc &replica_stat_desc,
        const ObReplicaType dst_replica_type,
        const int64_t dst_memstore_percent);
    int generate_add_replica_task(
        const common::ObZone &zone,
        ReplicaDesc &replica_desc);
    int generate_modify_paxos_replica_number_task();
    // private func for get_next_locality_alignment_task
    int try_get_readonly_all_server_locality_alignment_task(
        UnitProvider &unit_provider,
        const LATask *&task);
    int try_get_normal_locality_alignment_task(
        UnitProvider &unit_provider,
        const LATask *&task);
    int try_review_remove_replica_task(
        UnitProvider &unit_provider,
        LATask *my_task,
        const LATask *&output_task,
        bool &found);
    int try_review_add_replica_task(
        UnitProvider &unit_provider,
        LATask *my_task,
        const LATask *&output_task,
        bool &found);
    int try_review_type_transform_task(
        UnitProvider &unit_provider,
        LATask *my_task,
        const LATask *&output_task,
        bool &found);
    int try_review_modify_paxos_replica_number_task(
        UnitProvider &unit_provider,
        LATask *my_task,
        const LATask *&output_task,
        bool &found);
  private:
    static const int64_t LOCALITY_MAP_BUCKET_NUM = 100;
    static const int64_t UNIT_SET_BUCKET_NUM = 5000;
  private:
    int64_t task_idx_;
    AddReplicaLATask add_replica_task_;
    ObZoneManager *zone_mgr_;
    DRLSInfo &dr_ls_info_;
    common::ObArray<LATask *> task_array_;
    int64_t curr_paxos_replica_number_;
    int64_t locality_paxos_replica_number_;
    LocalityMap locality_map_;
    ReplicaStatMap replica_stat_map_;
    UnitProvider unit_provider_;
    common::ObArenaAllocator allocator_;
  };
private:
  int check_stop() const;

  static int check_ls_locality_match_(
      DRLSInfo &dr_ls_info,
      ObZoneManager &zone_mgr,
      bool &locality_is_matched);

  int start();

  void statistic_remain_dr_task();

  void statistic_total_dr_task(const int64_t task_cnt);

  int try_ls_disaster_recovery(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task,
      DRLSInfo &dr_ls_info_with_flag);

  int check_has_leader_while_remove_replica(
      const common::ObAddr &server,
      DRLSInfo &dr_ls_info,
      bool &has_leader);

private:
  void reset_task_plans_() { display_tasks_.reset(); }

  int check_task_already_exist(
      const ObDRTaskKey &task_key,
      const DRLSInfo &dr_ls_info,
      const int64_t &priority,
      bool &task_exist);

  int check_whether_the_tenant_role_can_exec_dr_(const uint64_t tenant_id);

  int try_remove_permanent_offline_replicas(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int do_single_replica_permanent_offline_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      DRLSInfo &dr_ls_info,
      const bool only_for_display,
      const ObReplicaType &replica_type,
      const ObMember &member_to_remove,
      int64_t &acc_dr_task);

  int check_ls_only_in_member_list_or_with_flag_(
      const DRLSInfo &dr_ls_info);

  int check_can_generate_task(
      const int64_t acc_dr_task,
      const bool need_check_has_leader_while_remove_replica,
      const bool is_high_priority_task,
      const ObAddr &server_addr,
      DRLSInfo &dr_ls_info,
      ObDRTaskKey &task_key,
      bool &can_generate);

  int construct_extra_infos_to_build_remove_replica_task(
      const DRLSInfo &dr_ls_info,
      share::ObTaskId &task_id,
      int64_t &new_paxos_replica_number,
      int64_t &old_paxos_replica_number,
      common::ObAddr &leader_addr,
      const ObReplicaType &replica_type);

  int generate_remove_permanent_offline_replicas_and_push_into_task_manager(
      const ObDRTaskKey task_key,
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const common::ObAddr &leader_addr,
      const ObReplicaMember &remove_member,
      const int64_t &old_paxos_replica_number,
      const int64_t &new_paxos_replica_number,
      int64_t &acc_dr_task,
      const ObReplicaType &replica_type);

  int try_replicate_to_unit(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int check_need_generate_replicate_to_unit(
      const int64_t index,
      DRLSInfo &dr_ls_info,
      share::ObLSReplica *&ls_replica,
      DRServerStatInfo *&server_stat_info,
      DRUnitStatInfo *&unit_stat_info,
      DRUnitStatInfo *&unit_in_group_stat_info,
      bool &need_generate);

  int generate_migrate_ls_task(
      const bool only_for_display,
      const char* task_comment,
      const share::ObLSReplica &ls_replica,
      const DRServerStatInfo &server_stat_info,
      const DRUnitStatInfo &unit_stat_info,
      const DRUnitStatInfo &unit_in_group_stat_info,
      const ObReplicaMember &dst_member,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int construct_extra_infos_to_build_migrate_task(
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      const DRUnitStatInfo &unit_stat_info,
      const DRUnitStatInfo &unit_in_group_stat_info,
      const ObReplicaMember &dst_member,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      share::ObTaskId &task_id,
      int64_t &data_size,
      ObDstReplica &dst_replica,
      int64_t &old_paxos_replica_number);

  int generate_replicate_to_unit_and_push_into_task_manager(
      const ObDRTaskKey task_key,
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t &data_size,
      const ObDstReplica &dst_replica,
      const ObReplicaMember &src_member,
      const ObReplicaMember &data_source,
      const int64_t &old_paxos_replica_number,
      const char* task_comment,
      int64_t &acc_dr_task);

  int try_locality_alignment(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int try_shrink_resource_pools(
      const bool &only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int try_cancel_unit_migration(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int check_need_generate_cancel_unit_migration_task(
      const int64_t index,
      DRLSInfo &dr_ls_info,
      share::ObLSReplica *&ls_replica,
      DRServerStatInfo *&server_stat_info,
      DRUnitStatInfo *&unit_stat_info,
      DRUnitStatInfo *&unit_in_group_stat_info,
      bool &is_paxos_replica_related,
      bool &need_generate);

  int construct_extra_info_to_build_cancael_migration_task(
      const bool &is_paxos_replica_related,
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      share::ObTaskId &task_id,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      common::ObAddr &leader_addr,
      int64_t &old_paxos_replica_number,
      int64_t &new_paxos_replica_number);

  int generate_cancel_unit_migration_task(
      const bool &is_paxos_replica_related,
      const ObDRTaskKey &task_key,
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const common::ObAddr &leader_addr,
      const ObReplicaMember &remove_member,
      const int64_t &old_paxos_replica_number,
      const int64_t &new_paxos_replica_number,
      int64_t &acc_dr_task);

  int try_migrate_to_unit(
      const bool only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  int check_need_generate_migrate_to_unit_task(
      const int64_t index,
      DRLSInfo &dr_ls_info,
      share::ObLSReplica *&ls_replica,
      DRServerStatInfo *&server_stat_info,
      DRUnitStatInfo *&unit_stat_info,
      DRUnitStatInfo *&unit_in_group_stat_info,
      bool &need_generate,
      bool &is_unit_in_group_related);

  int construct_extra_infos_for_generate_migrate_to_unit_task(
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      const DRUnitStatInfo &unit_stat_info,
      const DRUnitStatInfo &unit_in_group_stat_info,
      const ObReplicaMember &dst_member,
      const bool &is_unit_in_group_related,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      share::ObTaskId &task_id,
      int64_t &data_size,
      ObDstReplica &dst_replica,
      int64_t &old_paxos_replica_number);

  int generate_migrate_to_unit_task(
      const ObDRTaskKey task_key,
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t &data_size,
      const ObDstReplica &dst_replica,
      const ObReplicaMember &src_member,
      const ObReplicaMember &data_source,
      const int64_t &old_paxos_replica_number,
      const bool is_unit_in_group_related,
      int64_t &acc_dr_task);

  int generate_task_key(
      const DRLSInfo &dr_ls_info,
      ObDRTaskKey &task_key) const;

  int add_display_info(const ObLSReplicaTaskDisplayInfo &display_info);

  int record_task_plan_for_locality_alignment(
    DRLSInfo &dr_ls_info,
    const LATask *task);

  int try_generate_locality_alignment_task(
      DRLSInfo &dr_ls_info,
      const LATask *task,
      int64_t &acc_dr_task);

  int try_generate_remove_replica_locality_alignment_task(
      DRLSInfo &dr_ls_info,
      const ObDRTaskKey &task_key,
      const LATask *task,
      int64_t &acc_dr_task);

  int try_generate_add_replica_locality_alignment_task(
      DRLSInfo &dr_ls_info,
      const ObDRTaskKey &task_key,
      const LATask *task,
      int64_t &acc_dr_task);

  int try_generate_type_transform_locality_alignment_task(
      DRLSInfo &dr_ls_info,
      const ObDRTaskKey &task_key,
      const LATask *task,
      int64_t &acc_dr_task);

  int try_generate_modify_paxos_replica_number_locality_alignment_task(
      DRLSInfo &dr_ls_info,
      const ObDRTaskKey &task_key,
      const LATask *task,
      int64_t &acc_dr_task);

  // If unit is deleting and a R-replica of duplicate log stream is on it,
  // we have to remove this replica from learner_list directly
  // @params[in]  ls_replica, the replica to remove
  // @params[in]  only_for_display, whether just to display this task
  // @params[in]  dr_ls_info, disaster recovery infos of this log stream
  // @params[out] acc_dr_task, accumulated disaster recovery task count
  int try_remove_readonly_replica_for_deleting_unit_(
      const share::ObLSReplica &ls_replica,
      const bool &only_for_display,
      DRLSInfo &dr_ls_info,
      int64_t &acc_dr_task);

  // If unit is delting and a replica is on it,
  // we have to migrate this replica to another unit
  // @params[in]  unit_provider, allocate a valid unit to do migration
  // @params[in]  dr_ls_info, disaster recovery infos of this log stream
  // @params[in]  ls_replica, the replica to migrate
  // @params[in]  ls_status_info, status info of this log stream
  // @params[in]  server_stat_info, server info of this replica
  // @params[in]  unit_stat_info, unit info of this replica
  // @params[in]  unit_in_group_stat_info, unit group info of this log stream
  // @params[in]  only_for_display, whether just to display this task
  // @params[out] acc_dr_task, accumulated disaster recovery task count
  int try_migrate_replica_for_deleting_unit_(
      ObDRWorker::UnitProvider &unit_provider,
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      const share::ObLSStatusInfo &ls_status_info,
      const DRServerStatInfo &server_stat_info,
      const DRUnitStatInfo &unit_stat_info,
      const DRUnitStatInfo &unit_in_group_stat_info,
      const bool &only_for_display,
      int64_t &acc_dr_task);

  // If unit is deleting and a F-replica of duplicate log stream is on it,
  // we have to type transform another valid R-replica to F-replica
  // @params[in]  dr_ls_info, disaster recovery infos of this log stream
  // @params[in]  ls_replica, the replica to do type transform
  // @params[in]  only_for_display, whether just to display this task
  // @params[out] acc_dr_task, accumulated disaster recovery task count
  int try_type_transform_for_deleting_unit_(
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      const bool &only_for_display,
      int64_t &acc_dr_task);

  // When need to type transform a R-replica to F-replica,
  // use this function to get a valid R-replica
  // @params[in]  dr_ls_info, disaster recovery infos of this log stream
  // @params[in]  exclude_replica, excluded replica
  // @params[in]  target_zone, which zone to scan
  // @params[out] replica, the expected valid R-replica
  // @params[out] unit_id, which unit does this replica belongs to
  // @params[out] unit_group_id, which unit group does this replica belongs to
  // @params[out] find_a_valid_readonly_replica, whether find a valid replica
  int find_valid_readonly_replica_(
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &exclude_replica,
      const ObZone &target_zone,
      share::ObLSReplica &replica,
      uint64_t &unit_id,
      uint64_t &unit_group_id,
      bool &find_a_valid_readonly_replica);

  // construct extra infos to build a type transform task
  // @params[in]  dr_ls_info, disaster recovery infos of this log stream
  // @params[in]  ls_replica, which replica to do type transform
  // @params[in]  dst_member, dest replica
  // @params[in]  target_unit_id, dest replica belongs to whcih unit
  // @params[in]  target_unit_group_id, dest replica belongs to which unit group
  // @params[out] task_id, the unique task key
  // @params[out] tenant_id, which tenant's task
  // @params[out] ls_id, which log stream's task
  // @params[out] leader_addr, leader replica address
  // @params[out] data_size, data_size of this replica
  // @params[out] dst_replica, dest replica infos
  // @params[out] old_paxos_replica_number, previous number of F-replica count
  // @params[out] new_paxos_replica_number, new number of F-replica count
  int construct_extra_info_to_build_type_transform_task_(
      DRLSInfo &dr_ls_info,
      const share::ObLSReplica &ls_replica,
      const ObReplicaMember &dst_member,
      const uint64_t &target_unit_id,
      const uint64_t &target_unit_group_id,
      share::ObTaskId &task_id,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      common::ObAddr &leader_addr,
      int64_t &data_size,
      ObDstReplica &dst_replica,
      int64_t &old_paxos_replica_number,
      int64_t &new_paxos_replica_number);

  // generate a type transform and push into task manager
  // @params[in]  task_key, the key of this task
  // @params[in]  tenant_id, which tenant's task
  // @params[in]  ls_id, which log stream's task
  // @params[in]  task_id, the id of this task
  // @params[in]  data_size, data_size of this replica
  // @params[in]  dst_replica, dest replica
  // @params[in]  src_member, source member
  // @params[in]  data_source, data_source of this task
  // @params[in]  old_paxos_replica_number, previous number of F-replica count
  // @params[in]  new_paxos_replica_number, new number of F-replica count
  // @params[out] acc_dr_task, accumulated disaster recovery task count
  int generate_type_transform_task_(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t data_size,
      const ObDstReplica &dst_replica,
      const ObReplicaMember &src_member,
      const ObReplicaMember &data_source,
      const int64_t old_paxos_replica_number,
      const int64_t new_paxos_replica_number,
      int64_t &acc_dr_task);

private:
  volatile bool &stop_;
  bool inited_;
  bool dr_task_mgr_is_loaded_;
  common::ObAddr self_addr_;
  common::ObServerConfig *config_;
  ObZoneManager *zone_mgr_;
  ObDRTaskMgr *disaster_recovery_task_mgr_;
  share::ObLSTableOperator *lst_operator_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  TaskCountStatistic task_count_statistic_;
  common::ObSArray<ObLSReplicaTaskDisplayInfo> display_tasks_;
  common::SpinRWLock display_tasks_rwlock_;  // to protect display_tasks_
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_
