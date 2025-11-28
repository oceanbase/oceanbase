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

#ifndef OCEANBASE_SHARE_OB_BALANCE_JOB_OPERATOR_H_
#define OCEANBASE_SHARE_OB_BALANCE_JOB_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ob_common_id.h"// ObCommonID
#include "share/ob_balance_define.h"  // ObBalanceJobID
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "lib/string/ob_sql_string.h"//ObSqlString
#include "share/ob_display_list.h"//ObDisplayList

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObString;
namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;
class ObBalanceJobStatus
{

public:
  static const int64_t BALANCE_JOB_STATUS_INVALID = -1;
  static const int64_t BALANCE_JOB_STATUS_DOING = 0;
  static const int64_t BALANCE_JOB_STATUS_CANCELING = 1;
  static const int64_t BALANCE_JOB_STATUS_COMPLETED = 2;
  static const int64_t BALANCE_JOB_STATUS_CANCELED = 3;
  static const int64_t BALANCE_JOB_STATUS_SUSPEND = 4;
  static const int64_t BALANCE_JOB_STATUS_MAX = 5;
  ObBalanceJobStatus(const int64_t value = BALANCE_JOB_STATUS_INVALID) : val_(value) {}
  ObBalanceJobStatus(const ObString &str);
  ~ObBalanceJobStatus() {reset(); }

public:
  void reset() { val_ = BALANCE_JOB_STATUS_INVALID; }
  bool is_valid() const { return val_ != BALANCE_JOB_STATUS_INVALID; }
  const char* to_str() const;

  // assignment
  ObBalanceJobStatus &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObBalanceJobStatus &other) const { return val_ == other.val_; }
  bool operator != (const ObBalanceJobStatus &other) const { return val_ != other.val_; }

  // ObBalanceJobStatus attribute interface
  bool is_doing() const { return BALANCE_JOB_STATUS_DOING == val_; }
  bool is_canceling() const { return BALANCE_JOB_STATUS_CANCELING == val_; }
  bool is_success() const { return BALANCE_JOB_STATUS_COMPLETED == val_; }
  bool is_canceled() const { return BALANCE_JOB_STATUS_CANCELED == val_; }
  bool is_suspend() const { return BALANCE_JOB_STATUS_SUSPEND == val_; }

  TO_STRING_KV(K_(val), "job_status", to_str());
private:
  int64_t val_;
};
class ObBalanceJobType
{
public:
  static const int64_t BALANCE_JOB_INVALID = -1;
  //ls balance for expand or shrink
  static const int64_t BALANCE_JOB_LS = 0;
  //partition balance
  static const int64_t BALANCE_JOB_PARTITION = 1;
  static const int64_t BALANCE_JOB_TRANSFER_PARTITION = 2;
  static const int64_t BALANCE_JOB_MAX = 3;
  ObBalanceJobType(const int64_t value = BALANCE_JOB_INVALID) : val_(value) {}
  ObBalanceJobType(const ObString &str);
public:
  void reset() { val_ = BALANCE_JOB_INVALID; }
  bool is_valid() const { return val_ != BALANCE_JOB_INVALID; }
  const char* to_str() const;
  TO_STRING_KV(K_(val), "job_type", to_str());

  // assignment
  ObBalanceJobType &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObBalanceJobType &other) const { return val_ == other.val_; }
  bool operator != (const ObBalanceJobType &other) const { return val_ != other.val_; }

  // ObBalanceJobStatus attribute interface
  bool is_balance_ls() const { return BALANCE_JOB_LS == val_; }
  bool is_balance_partition() const { return BALANCE_JOB_PARTITION == val_; }
  bool is_transfer_partition() const { return BALANCE_JOB_TRANSFER_PARTITION == val_; }
private:
  int64_t val_;
};

struct ObDisplayZoneUnitCnt final : public ObDisplayType
{
public:
  ObDisplayZoneUnitCnt() : zone_(), unit_cnt_(0), replica_type_(common::ObReplicaType::REPLICA_TYPE_FULL) {}
  ObDisplayZoneUnitCnt(
    const ObZone &zone, int64_t unit_cnt, common::ObReplicaType replica_type = common::ObReplicaType::REPLICA_TYPE_FULL) :
  zone_(zone), unit_cnt_(unit_cnt), replica_type_(replica_type) {}
  ~ObDisplayZoneUnitCnt() {}
  void reset()
  {
    zone_.reset();
    unit_cnt_ = 0;
    replica_type_ = common::ObReplicaType::REPLICA_TYPE_FULL;
  }
  bool is_valid() const
  {
    return !zone_.is_empty() && unit_cnt_ > 0 && ObReplicaTypeCheck::is_replica_type_valid(replica_type_);
  }
  int64_t max_display_str_len() const
  {
    return MAX_ZONE_LENGTH + MAX_REPLICA_TYPE_LENGTH + 17 + 2;
  }
  int parse_from_display_str(const common::ObString &str);
  int to_display_str(char *buf, const int64_t len, int64_t &pos) const;
  int64_t get_unit_cnt() const
  {
    return unit_cnt_;
  }
  const ObZone& get_zone() const
  {
    return zone_;
  }
  common::ObReplicaType get_replica_type() const
  {
    return replica_type_;
  }
  bool operator==(const ObDisplayZoneUnitCnt &other) const
  {
    return zone_ == other.zone_ && unit_cnt_ == other.unit_cnt_ && replica_type_ == other.replica_type_;
  }
  bool operator!=(const ObDisplayZoneUnitCnt &other) const { return !(other == *this); }
  bool operator<(const ObDisplayZoneUnitCnt &other) const
  {
    return (zone_ == other.zone_) ? (unit_cnt_ < other.unit_cnt_) : (zone_ < other.zone_);
  }

  TO_STRING_KV(K_(zone), K_(unit_cnt), K_(replica_type));
private:
  ObZone zone_;
  int64_t unit_cnt_;
  // replica type of zone units
  common::ObReplicaType replica_type_;
};

typedef ObDisplayList<ObDisplayZoneUnitCnt, 9> ObZoneUnitCntList;

struct ObBalanceJobDesc
{
public:
  ObBalanceJobDesc() { reset(); }
  ~ObBalanceJobDesc() {}
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    job_id_.reset();
    primary_zone_num_ = 0;
    ls_scale_out_factor_ = 0;
    enable_rebalance_ = false;
    enable_transfer_ = false;
    enable_gts_standalone_ = false;
    zone_unit_num_list_.reset();
  }
  int init(
      const uint64_t tenant_id,
      const ObBalanceJobID &job_id,
      const ObZoneUnitCntList &zone_list,
      const int64_t primary_zone_num,
      const int64_t ls_scale_out_factor,
      const bool enable_rebalance,
      const bool enable_transfer,
      const bool enable_gts_standalone);
  int init_without_job(
      const uint64_t tenant_id,
      const ObZoneUnitCntList &zone_list,
      const int64_t primary_zone_num,
      const int64_t ls_scale_out_factor,
      const bool enable_rebalance,
      const bool enable_transfer,
      const bool enable_gts_standalone);
  int get_unit_lcm_count(int64_t &lcm_count) const;
  int assign(const ObBalanceJobDesc &other);
  bool is_valid() const
  { // job_id is invalid most of the time
    return is_valid_tenant_id(tenant_id_)
        && primary_zone_num_ > 0
        && ls_scale_out_factor_ >= 1
        && !zone_unit_num_list_.empty();
  }
  int get_zone_unit_num(const ObZone &zone, int64_t &unit_num) const;
  int check_zone_in_locality(const ObZone &zone, bool &in_locality) const;
  int compare(const ObBalanceJobDesc &other, bool &is_same, ObSqlString &diff_str);
  const ObZoneUnitCntList &get_zone_unit_num_list() const { return zone_unit_num_list_; }
  int64_t get_ls_cnt_in_group() const { return primary_zone_num_ * ls_scale_out_factor_; }

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(primary_zone_num), K_(ls_scale_out_factor),
      K_(enable_rebalance), K_(enable_transfer), K_(enable_gts_standalone), K_(zone_unit_num_list));
private:
#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(ObBalanceJobID, job_id)
  Property_declare_var(int64_t, primary_zone_num)
  Property_declare_var(int64_t, ls_scale_out_factor)
  Property_declare_var(bool, enable_rebalance)
  Property_declare_var(bool, enable_transfer)
  Property_declare_var(bool, enable_gts_standalone)
#undef Property_declare_var
private:
  ObZoneUnitCntList zone_unit_num_list_;
};

struct ObBalanceJob
{
public:
  ObBalanceJob() { reset(); }
  ~ObBalanceJob() {}
  void reset();
  int init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceJobType job_type,
           const ObBalanceJobStatus job_status,
           const ObString &comment,
           const ObBalanceStrategy &balance_strategy,
           const int64_t max_end_time = OB_INVALID_TIMESTAMP);
  bool is_valid() const;
  bool is_timeout() const;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(job_type), K_(job_status),
      K_(comment), K_(balance_strategy), K_(max_end_time));
#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(ObBalanceJobID, job_id)
  Property_declare_var(ObBalanceJobType, job_type)
  Property_declare_var(ObBalanceJobStatus, job_status)
  Property_declare_var(int64_t, max_end_time)
#undef Property_declare_var
public:
  const ObSqlString& get_comment() const
  {
    return comment_;
  }
  const ObBalanceStrategy& get_balance_strategy() const
  {
    return balance_strategy_;
  }
private:
  ObSqlString comment_;
  ObBalanceStrategy balance_strategy_;
};

class ObBalanceJobTableOperator
{
public:
  /**
   * @description: insert new job to __all_balance_job
   * @param[in] job : a valid balance job include tenant_id
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   */
  static int insert_new_job(const ObBalanceJob &job,
                     ObISQLClient &client);
  /**
   * @description: get job from __all_balance_job, only one job
   * @param[in] tenant_id : user_tenant_id
   * @param[in] for_update : whether lock the job
   * @param[in] client : sql client or trans
   * @param[out] job : get a valid balance job
   * @param[out] start_time: the job gmt_create
   * @param[out] finish_time: the job gmt_modify
   * @return :
   * OB_SUCCESS : get a valid job
   * OB_ENTRY_NOT_EXIST : __all_balance_job is empty
   * OTHER : fail
   */
  static int get_balance_job(const uint64_t tenant_id,
                      const bool for_update,
                      ObISQLClient &client,
                      ObBalanceJob &job,
                      int64_t &start_time,
                      int64_t &finish_time);
  /**
   * @description: update job status of __all_balance_job, only one job
   * @param[in] tenant_id : user_tenant_id
   * @param[in] old_job_status : current job status
   * @param[in] new_job_status : new job status
   * @param[in] update_comment : weather to update comment
   * @param[in] comment : reason to change job status(only for abort);
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS : update job status success
   * OB_STATE_NOT_MATCH : current job status not match, can not update
   * OTHER : fail
   */
  static int update_job_status(const uint64_t tenant_id,
                               const ObBalanceJobID job_id,
                               const ObBalanceJobStatus old_job_status,
                               const ObBalanceJobStatus new_job_status,
                               bool update_comment, const common::ObString &new_comment,
                               ObISQLClient &client);
  /**
   * @description: delete job from of __all_balance_job and insert to job to __all_balance_job_history
   * @param[in] tenant_id : user_tenant_id
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS : clean job success
   * OB_OP_NOT_ALLOW : job is in progress or init
   * OB_STATE_NOT_MATCH /OB_ENTRY_NOT_EXIST : current job has beed clean
   * OTHER : fail
   */
  static int clean_job(const uint64_t tenant_id,
                       const ObBalanceJobID job_id,
                       ObMySQLProxy &client);
  static int fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceJob &job);

  /**
   * @description: update balance_strategy of __all_balance_job
   * @param[in] tenant_id : user_tenant_id
   * @param[in] job_status : current job status
   * @param[in] old_strategy : current job balance strategy
   * @param[in] new_strategy : new job balance strategy
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS: update job balance strategy successfully
   * OB_STATE_NOT_MATCH: current job balance strategy not match
   * Other: failed
   */
  static int update_job_balance_strategy(
      const uint64_t tenant_id,
      const ObBalanceJobID job_id,
      const ObBalanceJobStatus job_status,
      const ObBalanceStrategy &old_strategy,
      const ObBalanceStrategy &new_strategy,
      ObISQLClient &client);
private:
  static int construct_get_balance_job_sql_(
      const uint64_t tenant_id,
      const bool for_update,
      ObSqlString &sql);
};

class ObBalanceJobDescOperator
{
public:
  /**
   * @description: insert new job to __all_balance_job_description
   * @param[in] tenant_id : user tenant_id
   * @param[in] job_id : balance job id
   * @param[in] job_desc : a valid balance job desc
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   */
  static int insert_balance_job_desc(
      const uint64_t tenant_id,
      const ObBalanceJobID &job_id,
      const ObBalanceJobDesc &job_desc,
      ObISQLClient &client);
  /**
   * @description: get job_desc from __all_balance_job_description by job_id
   * @param[in] tenant_id : user tenant_id
   * @param[in] job_id : balance job id
   * @param[in] client : sql client or trans
   * @param[out] job_desc : get a valid balance job
   * @return :
   * OB_SUCCESS : get a valid job
   * OB_ENTRY_NOT_EXIST : job not found
   * OTHER : failed
   */
  static int get_balance_job_desc(
      const uint64_t tenant_id,
      const ObBalanceJobID &job_id,
      ObISQLClient &client,
      ObBalanceJobDesc &job_desc);
private:
  static int construct_parameter_list_str_(
      const ObBalanceJobDesc &job_desc,
      ObIAllocator &allocator,
      ObString &parameter_list_str);
  static int parse_parameter_from_str_(
      const ObString &parameter_list_str,
      int64_t &ls_scale_out_factor,
      bool &enable_rebalance,
      bool &enable_transfer,
      bool &enable_gts_standalone);
};

}
}

#endif /* !OCEANBASE_SHARE_OB_BALANCE_JOB_OPERATOR_H_ */
