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
  static const int64_t BALANCE_JOB_STATUS_MAX = 4;
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
  static const int64_t BALANCE_JOB_MAX = 2;
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
private:
  int64_t val_;
};

const static char * const LS_BALANCE_BY_MIGRATE = "LS balance by migrate";
const static char * const LS_BALANCE_BY_ALTER = "LS balance by alter";
const static char * const LS_BALANCE_BY_EXPAND = "LS balance by expand";
const static char * const LS_BALANCE_BY_SHRINK = "LS balance by shrink";

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
           const int64_t primary_zone_num,
           const int64_t unit_group_num,
           const ObString &comment,
           const ObString &balance_strategy);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(job_type), K_(job_status),
               K_(primary_zone_num), K_(unit_group_num), K_(comment), K_(balance_strategy));
#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(ObBalanceJobID, job_id)
  Property_declare_var(int64_t, primary_zone_num)
  Property_declare_var(int64_t, unit_group_num)
  Property_declare_var(ObBalanceJobType, job_type)
  Property_declare_var(ObBalanceJobStatus, job_status)
#undef Property_declare_var
public:
  const ObSqlString& get_comment() const
  {
    return comment_;
  }
  const ObSqlString& get_balance_strategy() const
  {
    return balance_strategy_;
  }
private:
  ObSqlString comment_;
  ObSqlString balance_strategy_;
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
};
}
}

#endif /* !OCEANBASE_SHARE_OB_BALANCE_JOB_OPERATOR_H_ */
