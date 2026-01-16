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

#ifndef OCEABASE_STORAGE_HA_SRC_PROVIDER_
#define OCEABASE_STORAGE_HA_SRC_PROVIDER_

#include "lib/ob_define.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "common/ob_learner_list.h"
#include "ob_storage_ha_utils.h"

namespace oceanbase {
namespace storage {
struct ObMigrationChooseSrcHelperInitParam;

class ObStorageHAMemberUtils final
{
public:
  static int get_addr_array(
      const common::ObIArray<common::ObMember> &member_list,
      common::ObIArray<common::ObAddr> &addr_array);
  static int get_member_by_addr(
      const common::ObAddr &addr,
      const common::ObIArray<common::ObMember> &member_list,
      common::ObMember &member);
};

class ObStorageHAGetMemberHelper
{
public:
  ObStorageHAGetMemberHelper();
  virtual ~ObStorageHAGetMemberHelper();
  int init(storage::ObStorageRpc *storage_rpc);
  int get_ls_member_list(const uint64_t tenant_id, const share::ObLSID &ls_id,
      common::ObMemberList &member_list);
  int get_ls_member_list_and_learner_list(
      const uint64_t tenant_id, const share::ObLSID &ls_id, const bool need_learner_list,
      common::ObAddr &leader_addr, common::GlobalLearnerList &learner_list,
      common::ObMemberList &member_list);
  virtual int get_ls_leader(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &addr);
  virtual int get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  virtual bool check_tenant_primary();
  // According to the replica type, determine whether to get learner list.
  // F replica get member list, R replica get member list and learner list.
  int get_member_list_by_replica_type(
      const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObReplicaMember &dst,
      ObLSMemberListInfo &info, bool &is_first_c_replica);

private:
  int fetch_ls_member_list_and_learner_list_(const uint64_t tenant_id, const share::ObLSID &ls_id, const bool need_learner_list,
      common::ObAddr &addr, common::GlobalLearnerList &learner_list, common::ObMemberList &member_list);
  virtual int get_ls_member_list_and_learner_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const bool need_learner_list, common::ObAddr &leader_addr,
      common::GlobalLearnerList &learner_list, common::ObMemberList &member_list);
  int filter_dest_replica_(
      const common::ObReplicaMember &dst,
      common::GlobalLearnerList &learner_list);
  // check whether the dst is the first C replica in the learner list
  int check_is_first_c_replica_(
      const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list,
      const bool &need_learner_list,
      bool &is_first_c_replica);
private:
  bool is_inited_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAGetMemberHelper);
};

class ObMigrationChooseSourcePolicy
{
public:
  enum TYPE : uint8_t
  {
    ZONE = 0,
    IDC = 1,
    REGION = 2,
    DIFFERENT_REGION = 3, // cannot set manually
    // above policies are used for choosing source by location
    CHECKPOINT = 4,
    RECOMMEND = 5,
    LOG_ONLY = 6,
    MAX_POLICY
  };

  static const int64_t LOCATION_POLICY_COUNT = DIFFERENT_REGION + 1;
public:
  ObMigrationChooseSourcePolicy(): type_(MAX_POLICY) {}
  ~ObMigrationChooseSourcePolicy() = default;
  explicit ObMigrationChooseSourcePolicy(const TYPE &type);
  operator TYPE() const;
  ObMigrationChooseSourcePolicy &operator=(const TYPE &type);
  bool operator==(const TYPE &type);
  bool operator!=(const TYPE &type);

  void reset();
  bool is_valid() const;
  static const char *get_str(const TYPE &policy);

  TO_STRING_KV("val", static_cast<uint8_t>(type_),
               "str", get_str(*this));
private:
  TYPE type_;
  static constexpr const char *type_str_array_[] = {
    "zone",
    "idc",
    "region",
    "different_region",
    "checkpoint",
    "recommend",
    "log_only",
  };
};

class ObMigrationSourceValidationResult
{
public:
  enum TYPE : uint8_t
  {
    CAN_BECOME_SOURCE = 0,
    INVALID_REPLICA_TYPE = 1,
    CLOG_CHECKPOINT = 2, // source ls clog ckpt scn < dest clog ckpt scn
    PARENT_CLOG_CHECKPOINT = 3, // source ls clog ckpt scn < dest parent clog ckpt scn
    DISK_ERROR = 4,
    VERSION_INCOMPATIBLE = 5,
    SOURCE_IN_MIGRATION = 6,
    SOURCE_RESTORE_FAILED = 7,
    OTHER_ERROR = 8,
    MAX_RESULT
  };
public:
  ObMigrationSourceValidationResult(): type_(MAX_RESULT) {}
  ~ObMigrationSourceValidationResult() = default;
  explicit ObMigrationSourceValidationResult(const TYPE &type);
  operator TYPE() const;
  ObMigrationSourceValidationResult &operator=(const TYPE &type);
  bool operator==(const TYPE &type);
  bool operator!=(const TYPE &type);

  void reset();
  bool is_valid() const;
  static const char *get_str(const TYPE &result);

  TO_STRING_KV("val", static_cast<uint8_t>(type_),
               "str", get_str(*this));
private:
  TYPE type_;
  static constexpr const char *type_str_array_[] = {
    "valid",
    "invalid_replica_type",
    "clog_ckpt",
    "parent_clog_ckpt",
    "disk_error",
    "version_incompatible",
    "in_migration",
    "restore_failed",
    "other_error",
  };
};

class ObMigrationChooseSourceInfo
{
public:
  struct ChooseSourceInfo
  {
    public:
      ChooseSourceInfo();
      ~ChooseSourceInfo();

      // customize to_string
      int64_t to_string(char *buf, const int64_t buf_len) const;
    public:
      ObMigrationSourceValidationResult validation_result_;
      common::ObAddr addr_;
      share::SCN clog_checkpoint_scn_;
      ObMigrationStatus migration_status_;
      ObLSRestoreStatus restore_status_;
      int64_t err_code_;
  };
public:
  ObMigrationChooseSourceInfo();
  ~ObMigrationChooseSourceInfo();

  void set_parent_info(const common::ObAddr &parent_addr, const share::SCN &palf_parent_checkpoint_scn) {
    parent_addr_ = parent_addr;
    palf_parent_checkpoint_scn_ = palf_parent_checkpoint_scn;
  }
  void set_clog_checkpoint_scn(const share::SCN &clog_checkpoint_scn) {
    clog_checkpoint_scn_ = clog_checkpoint_scn;
  }
  void set_policy_info(
    const int64_t result,
    const ObMigrationChooseSourcePolicy &policy_type,
    const ObMigrationChooseSourcePolicy &chosen_policy_type,
    const bool &use_c_replica_policy,
    const bool &is_first_c_replica) {
      result_ = result;
      policy_type_ = policy_type;
      chosen_policy_type_ = chosen_policy_type;
      use_c_replica_policy_ = use_c_replica_policy;
      is_first_c_replica_ = is_first_c_replica;
  }
  void set_advance_src_addr(const common::ObAddr &advance_src_addr);
  int add_choose_source_info(
    const common::ObAddr &addr,
    const ObMigrationSourceValidationResult &failed_reason,
    const obrpc::ObFetchLSMetaInfoResp &ls_info,
    const int64_t err_code);
  int get_checkpoint_failed_source_infos(common::ObIArray<ChooseSourceInfo> &infos);
  const common::ObIArray<ChooseSourceInfo> &get_source_infos() const { return source_infos_; }

  // customize to_string, won't print source_infos_ here
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t result_;
  ObMigrationChooseSourcePolicy policy_type_;
  ObMigrationChooseSourcePolicy chosen_policy_type_;
  bool use_c_replica_policy_;
  bool is_first_c_replica_;
  common::ObAddr parent_addr_;
  share::SCN clog_checkpoint_scn_;
  share::SCN palf_parent_checkpoint_scn_;
  bool trigger_advance_;
  common::ObAddr advance_src_addr_;
  common::ObArray<ChooseSourceInfo> source_infos_;

  DISALLOW_COPY_AND_ASSIGN(ObMigrationChooseSourceInfo);
};

class ObStorageHASrcProvider
{
public:
  ObStorageHASrcProvider();
  virtual ~ObStorageHASrcProvider();
  int init(const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);
  virtual int choose_ob_src(
      const ObMigrationOpArg &arg, common::ObAddr &chosen_src_addr);

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  ObMigrationOpType::TYPE get_migration_op_type() const { return type_; }
  ObMigrationChooseSourcePolicy get_policy_type() const { return policy_type_; }
  storage::ObStorageRpc *get_storage_rpc() const { return storage_rpc_; }
  const share::SCN &get_local_clog_checkpoint_scn() const { return local_clog_checkpoint_scn_; }
  const share::SCN &get_palf_parent_checkpoint_scn() const { return palf_parent_checkpoint_scn_; }
  bool is_first_c_replica() const { return is_first_c_replica_; }
  const ObMigrationChooseSourceInfo &get_choose_source_info() const { return choose_source_info_; }
  int check_tenant_primary(bool &is_primary);
  int record_choose_source_policy_info(const int64_t result);
  int advance_src_ls_checkpoint(const ObMigrationOpArg &arg);
protected:
  /*
   * The validity assessment of replicas includes:
   * server_version: dest server_version >= src server_version
   * restore_status: if restore_status of ls is fail, migration needs to wait.
   * migration_status: OB_MIGRATION_STATUS_NONE
   * replica type:
   * 1. F replica could serve as the source of F/R/C replica
   * 2. R replica could only serve as the source of R/C
   * 3. C replica could only serve as the source of C replica
   * clog_checkpoint: source checkpoint scn must be greater than or equal than palf_parent_checkpoint_scn_ and local_clog_checkpoint_scn_
   *
   * If must_choose_c_replica is true, the source must be C replica.
   */
  int check_replica_validity(
      const common::ObMember &member, const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list, const bool &must_choose_c_replica,
      obrpc::ObFetchLSMetaInfoResp &ls_info);

  virtual int inner_choose_ob_src(
      const common::ObAddr &leader_addr, const common::GlobalLearnerList &learner_list,
      const common::ObIArray<common::ObMember> &member_list, const ObMigrationOpArg &arg, const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) = 0;
  virtual int choose_src_to_advance_checkpoint(
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) = 0;
protected:
  bool is_inited_;
  ObLSMemberListInfo member_list_info_;
  bool is_first_c_replica_;
  ObMigrationChooseSourcePolicy chosen_policy_type_; // the policy type finally chosen after checking
  bool use_c_replica_policy_; // true if dst is c replica and there already exists c replica in the cluster
private:
  int fetch_ls_meta_info_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObAddr &member_addr,
      obrpc::ObFetchLSMetaInfoResp &ls_meta_info);
  int check_replica_type_(
      const common::ObMember &member,
      const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list,
      const bool &must_choose_c_replica,
      bool &is_replica_type_valid);
  int init_palf_parent_checkpoint_scn_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const share::SCN &local_clog_checkpoint_scn, const common::ObReplicaType replica_type, const ObMigrationOpType::TYPE op_type);

  int get_palf_parent_checkpoint_scn_from_rpc_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObReplicaType replica_type, const ObMigrationOpType::TYPE op_type, share::SCN &parent_checkpoint_scn);
  int get_palf_parent_addr_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObReplicaType replica_type, const ObMigrationOpType::TYPE op_type, common::ObAddr &parent_addr);
  int check_replica_type_for_normal_replica_(
      const common::ObMember &member,
      const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list,
      bool &is_replica_type_valid);
  int check_replica_type_for_c_replica_(
      const common::ObMember &member,
      const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list,
      bool &is_replica_type_valid);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObMigrationOpType::TYPE type_;
  share::SCN local_clog_checkpoint_scn_;
  share::SCN palf_parent_checkpoint_scn_;
  ObStorageHAGetMemberHelper *member_helper_;
  storage::ObStorageRpc *storage_rpc_;
  ObMigrationChooseSourcePolicy policy_type_; // the policy type chosen by the user
  ObMigrationChooseSourceInfo choose_source_info_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHASrcProvider);
};

class ObMigrationSrcByLocationProvider : public ObStorageHASrcProvider
{
public:
  ObMigrationSrcByLocationProvider();
  virtual ~ObMigrationSrcByLocationProvider();
  int init(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);
protected:
  virtual int inner_choose_ob_src(
      const common::ObAddr &leader_addr, const common::GlobalLearnerList &learner_list,
      const common::ObIArray<common::ObMember> &member_list, const ObMigrationOpArg &arg, const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
  virtual int choose_src_to_advance_checkpoint(
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
private:
  /*
   * Find source from the sorted addr list.
   * Will only choose the src in [start_index, end_index]
   * The chosen source must be valid (see ObStorageHASrcProvider::check_replica_validity).
   */
  int find_src_in_sorted_addr_list_(
      const common::ObIArray<common::ObAddr> &sorted_addr_list,
      const int64_t start_index,
      const int64_t end_index,
      const common::GlobalLearnerList &learner_list,
      const common::ObAddr &leader_addr,
      const common::ObReplicaMember &dst,
      const bool &must_choose_c_replica,
      const common::ObIArray<common::ObMember> &member_list,
      common::ObAddr &chosen_src_addr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrationSrcByLocationProvider);
};

class ObMigrationSrcByCheckpointProvider : public ObStorageHASrcProvider
{
public:
  ObMigrationSrcByCheckpointProvider();
  virtual ~ObMigrationSrcByCheckpointProvider();
  int init(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);
protected:
  virtual int inner_choose_ob_src(
      const common::ObAddr &leader_addr, const common::GlobalLearnerList &learner_list,
      const common::ObIArray<common::ObMember> &member_list, const ObMigrationOpArg &arg, const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
  virtual int choose_src_to_advance_checkpoint(
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationSrcByCheckpointProvider);
};

class ObRSRecommendSrcProvider : public ObStorageHASrcProvider
{
public:
  ObRSRecommendSrcProvider();
  virtual ~ObRSRecommendSrcProvider();
  int init(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);

  // overwrite choose_ob_src, if use specified policy, only need to call inner_choose_ob_src once (no need to retry when is_first_c_replica)
  virtual int choose_ob_src(
      const ObMigrationOpArg &arg, common::ObAddr &chosen_src_addr) override;
protected:
  virtual int inner_choose_ob_src(
      const common::ObAddr &leader_addr, const common::GlobalLearnerList &learner_list,
      const common::ObIArray<common::ObMember> &member_list, const ObMigrationOpArg &arg, const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
  virtual int choose_src_to_advance_checkpoint(
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
private:
  int check_replica_validity_(const int64_t cluster_id, const common::ObIArray<common::ObMember> &member_list,
      const common::ObAddr &addr, const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list, const bool &must_choose_c_replica);
  DISALLOW_COPY_AND_ASSIGN(ObRSRecommendSrcProvider);
};

class ObMigrationLogOnlyProvider : public ObStorageHASrcProvider
{
public:
  ObMigrationLogOnlyProvider();
  virtual ~ObMigrationLogOnlyProvider();
  int init(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);
  virtual int choose_ob_src(
      const ObMigrationOpArg &arg, common::ObAddr &chosen_src_addr) override;
protected:
  virtual int inner_choose_ob_src(
      const common::ObAddr &leader_addr, const common::GlobalLearnerList &learner_list,
      const common::ObIArray<common::ObMember> &member_list, const ObMigrationOpArg &arg, const bool &must_choose_c_replica,
      common::ObAddr &choosen_src_addr) override;
  virtual int choose_src_to_advance_checkpoint(
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr) override;
private:
  int check_replica_validity_(const int64_t cluster_id, const common::ObIArray<common::ObMember> &member_list,
      const common::ObAddr &addr, const common::ObReplicaMember &dst,
      const common::GlobalLearnerList &learner_list);
  DISALLOW_COPY_AND_ASSIGN(ObMigrationLogOnlyProvider);
};


class ObStorageHAChooseSrcHelper final
{
public:
  ObStorageHAChooseSrcHelper();
  ~ObStorageHAChooseSrcHelper();
  int init(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);
  int get_available_src(const ObMigrationOpArg &arg, ObStorageHASrcInfo &src_info);
  static int get_policy_type(
      const ObMigrationOpArg &arg,
      const uint64_t tenant_id,
      const bool enable_choose_source_policy,
      const char *policy_str,
      const common::GlobalLearnerList &learner_list,
      ObMigrationChooseSourcePolicy &policy,
      bool &use_c_replica_policy);
  /*
   * Divide (sort) the addr_list by zone, idc, region, different region.
   * The layout of the sorted_addr_list is as follows:
   * |<---- same zone ----->|<------ same idc ------>|<----- same region ---->|<-- different region -->|
   * {addr[0], ...,  addr[p], addr[p+1], ..., addr[q], addr[q+1], ..., addr[s], addr[s+1], ..., addr[t]}
   *                      |                        |                        |
   *                zone_end_index            idc_end_index         region_end_index
   */
  static int divide_addr_list_by_locality(
      const common::ObIArray<common::ObAddr> &addr_list,
      const common::ObReplicaMember &dst,
      common::ObIArray<common::ObAddr> &sorted_addr_list,
      int64_t &zone_end_index,
      int64_t &idc_end_index,
      int64_t &region_end_index);

  static int choose_src_to_advance_checkpoint_by_location(
      const ObLSMemberListInfo &member_list_info,
      const common::ObIArray<ObMigrationChooseSourceInfo::ChooseSourceInfo> &failed_src_infos,
      const ObMigrationOpArg &arg,
      const bool &must_choose_c_replica,
      common::ObAddr &chosen_src_addr);
private:
  template <typename ProviderType>
  int init_provider_(
      const ObMigrationChooseSrcHelperInitParam &param,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHAGetMemberHelper *member_helper);

  void errsim_test_(const ObMigrationOpArg &arg, ObStorageHASrcInfo &src_info);
  ObStorageHASrcProvider * get_provider() const { return provider_; }
  static int check_c_replica_migration_policy_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObReplicaMember &dst, const common::GlobalLearnerList &learner_list, bool &use_c_replica_policy);
  static int check_exist_c_replica_(const uint64_t tenant_id,
      const share::ObLSID &ls_id, const common::GlobalLearnerList &learner_list, bool &exist_c_replica);
  static int get_server_geography_info_(
    const common::ObAddr &addr, common::ObRegion &region, common::ObIDC &idc, common::ObZone &zone);
private:
  ObStorageHASrcProvider *provider_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAChooseSrcHelper);
};

struct ObMigrationChooseSrcHelperInitParam final
{
public:
  ObMigrationChooseSrcHelperInitParam();
  ~ObMigrationChooseSrcHelperInitParam() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObMigrationChooseSrcHelperInitParam &param);

  TO_STRING_KV(
    K_(tenant_id), K_(ls_id), K_(local_clog_checkpoint_scn), K_(arg), K_(info), K_(policy), K_(use_c_replica_policy), K_(is_first_c_replica));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN local_clog_checkpoint_scn_;
  ObMigrationOpArg arg_;
  ObLSMemberListInfo info_;
  ObMigrationChooseSourcePolicy policy_;
  bool use_c_replica_policy_;
  bool is_first_c_replica_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrationChooseSrcHelperInitParam);
};

template<typename ProviderType>
int ObStorageHAChooseSrcHelper::init_provider_(
    const ObMigrationChooseSrcHelperInitParam &param,
    storage::ObStorageRpc *storage_rpc,
    ObStorageHAGetMemberHelper *member_helper)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ProviderType *provider = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ProviderType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(provider = (new (buf) ProviderType()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "provider is nullptr", K(ret));
  } else if (OB_FAIL(provider->init(param, storage_rpc, member_helper))) {
    STORAGE_LOG(WARN, "failed to init src by location provider", K(ret), K(param), KP(storage_rpc), KP(member_helper));
  } else {
    provider_ = provider;
    provider = nullptr;
  }

  if (OB_NOT_NULL(provider)) {
    provider->~ProviderType();
    provider = nullptr;
  }
  buf = nullptr;
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
#endif
