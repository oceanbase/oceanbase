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

#ifndef OCEANBASE_SHARE_OB_TRANSFER_PARTITION_TASK_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TRANSFER_PARTITION_TASK_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ob_common_id.h"// ObCommonID
#include "share/ob_balance_define.h"  // ObTransferPartitionTaskID
#include "share/transfer/ob_transfer_info.h"//ObTransferPartInfo
#include "lib/string/ob_sql_string.h"//ObSqlString

namespace oceanbase
{

namespace common
{
class ObMySQLTransaction;
class ObString;
namespace sqltrans
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;
class ObTransferPartitionTaskStatus
{
public:
  static const int64_t TRP_TASK_STATUS_INVALID = -1;
  static const int64_t TRP_TASK_STATUS_WAITING = 0;
  static const int64_t TRP_TASK_STATUS_INIT = 1;
  static const int64_t TRP_TASK_STATUS_DOING = 2;
  static const int64_t TRP_TASK_STATUS_COMPLETED = 3;
  static const int64_t TRP_TASK_STATUS_FAILED = 4;
  static const int64_t TRP_TASK_STATUS_CANCELED = 5;
  static const int64_t TRP_TASK_STATUS_MAX = 6;
  ObTransferPartitionTaskStatus(const int64_t value = TRP_TASK_STATUS_INVALID) : val_(value) {}
  ObTransferPartitionTaskStatus(const ObString &str);
  ~ObTransferPartitionTaskStatus() {reset(); }

public:
  void reset() { val_ = TRP_TASK_STATUS_INVALID; }
  bool is_valid() const { return val_ > TRP_TASK_STATUS_INVALID
                                 && val_ < TRP_TASK_STATUS_MAX; }
  const char* to_str() const;

  // assignment
  ObTransferPartitionTaskStatus &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObTransferPartitionTaskStatus &other) const { return val_ == other.val_; }
  bool operator != (const ObTransferPartitionTaskStatus &other) const { return val_ != other.val_; }
#define IS_TRANSFER_PARTITION_TASK(TRANSFER_PARTITION_TASK, TRANSFER_PARTITION)\
  bool is_##TRANSFER_PARTITION()const { return TRANSFER_PARTITION_TASK == val_;}
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_INVALID, invalid)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_WAITING, waiting)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_INIT, init)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_DOING, doing)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_COMPLETED, completed)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_FAILED, failed)
  IS_TRANSFER_PARTITION_TASK(TRP_TASK_STATUS_CANCELED, canceled)
#undef IS_TRANSFER_PARTITION_TASK
  bool is_finish_status() const
  {
    return is_completed() || is_failed() || is_canceled();
  }
  TO_STRING_KV(K_(val), "job_status", to_str());
private:
  int64_t val_;
};

struct ObTransferPartitionTask
{
public:
  ObTransferPartitionTask() { reset(); }
  ~ObTransferPartitionTask() {}
  void reset();
  //set status_ to waiting, reset balance_job_id_, reset transfer_task_id_
  int simple_init(const uint64_t tenant_id,
           const ObTransferPartInfo &part_info,
           const ObLSID &dest_ls,
           const ObTransferPartitionTaskID &task_id);
  int init(const uint64_t tenant_id,
           const ObTransferPartInfo &part_info,
           const ObLSID &dest_ls,
           const ObTransferPartitionTaskID &task_id,
           const ObBalanceJobID &balance_job_id,
           const ObTransferTaskID &transfer_task_id,
           const ObTransferPartitionTaskStatus &task_status,
           const ObString &comment);
  bool is_valid() const;
  int assign(const ObTransferPartitionTask &other);
  TO_STRING_KV(K_(tenant_id), K_(part_info), K_(dest_ls), K_(task_id),
               K_(balance_job_id), K_(transfer_task_id), K_(task_status), K_(comment));

#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(ObLSID, dest_ls)
  Property_declare_var(ObTransferPartitionTaskID, task_id)
  Property_declare_var(ObBalanceJobID, balance_job_id)
  Property_declare_var(ObTransferTaskID, transfer_task_id)
  Property_declare_var(ObTransferPartitionTaskStatus, task_status)

#undef Property_declare_var
public:
  const ObSqlString& get_comment() const
  {
    return comment_;
  }
  const ObTransferPartInfo& get_part_info() const
  {
    return part_info_;
  }
private:
  ObTransferPartInfo part_info_;
  ObSqlString comment_;
};

class ObTransferPartitionTaskTableOperator
{
public:
  /**
   * for add task by sql
   * @description: insert new task to __all_transfer_partition_task
   * @param[in] task : a valid transfer partition task include tenant_id
   * @param[in] trans: trans
   * @return OB_SUCCESS if success, otherwise failed
   */
  static int insert_new_task(
      const uint64_t tenant_id,
      const ObTransferPartInfo &part_info,
      const ObLSID &dest_ls,
      ObMySQLTransaction &trans);
  /* 生成任务时，需要获取所有的transfer partition任务，不关心是否有并发的新插入的任务。
   * @description: get all transfer partition task from __all_transfer_partition_task
                   and smaller than max_task_id
   * @param[in] tenant_id : user_tenant_id
   * @param[in] max_task_id : max_task_id
   * @param[in] for_update
   * @param[in] trans: trans
   * @param[out] task_array: transfer_partition_task
   * */
  static int load_all_wait_task_in_part_info_order(const uint64_t tenant_id,
      const bool for_update,
      const ObTransferPartitionTaskID &max_task_id,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client);
  /*
   * @description: get all transfer partition task from __all_transfer_partition_task
   * @param[in] tenant_id : user_tenant_id
   * @param[in] trans: sql trans or trans
   * @param[out] task_array: transfer_partition_task
   * */
  static int load_all_task(const uint64_t tenant_id,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client);
  /*
   * @description:获取某个balance_job的所有的任务
   * @param[in] tenant_id : user_tenant_id
   * @param[in] job_id : the corelative balance job id
   * @param[in] trans: sql trans or trans
   * @param[out] task_array: transfer_partition_task
   * */
  static int load_all_balance_job_task(const uint64_t tenant_id,
      const share::ObBalanceJobID &job_id,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client);

  /*
   * 构造任务时，需要保证任务之间的偏序关系，后插进来的任务task_id是小的，可能会把多个任务合并成一个balance_job
   * 所以不能一个个的改，需要改一批，利用了偏序关系
   * 后续如果有需求的话，可以按照批来做，例如1024个任务做一批
   * @description: set all task smaller than max_task_id from waiting to schedule and set balance job id
   * @param[in] tenant_id : user_tenant_id
   * @param[in] max_task_id : max_task_id
   * @param[in] job_id : the corelative balance job id
   * @param[in] task_count: for double check the task smller than max_task_id and affected_rows
   * @param[in] trans: trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int set_all_tasks_schedule(const uint64_t tenant_id,
      const ObTransferPartitionTaskID &max_task_id,
      const ObBalanceJobID &job_id,
      const int64_t &task_count,
      ObMySQLTransaction &trans);
  /*
   * 关闭enable_transfer的时候，当前的balance_job需要取消掉，和balance_job关联的transfer partition任务需要回到waiting状态
   * @description: balance job maybe cancel, Disassociate balance_job and transfer_partition_task,
   *               rollback task from doing to waiting, and clean balance_job and transfer_task_id
   *  @param[in] tenant_id : user_tenant_id
   *  @param[in] job_id : the corelative balance job id
   *  @param[in] trans: must in trans
   * */
  static int rollback_all_to_waitting(const uint64_t tenant_id,
                           const ObBalanceJobID &job_id,
                           ObMySQLTransaction &trans);
  /*
   * 当一个transfer任务开始时，需要标记这一批任务transfer task
   * @description: set task corelative transfer task ID
   * @param[in] tenant_id : user_tenant_id
   * @param[in] job_id : the corelative balance job id
   * @param[in] part_list : part_info start transfer
   * @param[in] transfer_task_id : the corelative transfer task id
   * @param[in] trans: must in trans
   * */
  static int start_transfer_task(const uint64_t tenant_id,
                         const ObBalanceJobID &job_id,
                         const ObTransferPartList &part_list,
                         const ObTransferTaskID &transfer_task_id,
                         ObMySQLTransaction &trans);

  /*
   * 任务在执行时发现分区不在需要失败掉,调用点在finish_task_from_init和生成balance_job的过程中
   * transfer任务在执行成功的时候，需要结束掉，调用点在finish_task里面
   * canceled这个状态这一次不做
   * @description: task maybe CANCELED, FAILED, COMPLETE
   *               and insert into __all_transfer_partition_task_history
   * @param[in] tenant_id : user_tenant_id
   * @param[in] part_list : part_info need finish
   * @param[in] status : must be CANCELED, FAILED, COMPLETE, must be finish
   * @param[in] max_task_id: max_task_id
   * @param[in] comment : task comment
   * @param[in] trans: must in trans
   * */
  static int finish_task(const uint64_t tenant_id,
                         const ObTransferPartList &part_list,
                         const ObTransferPartitionTaskID &max_task_id,
                         const ObTransferPartitionTaskStatus &status,
                         const ObString &comment,
                         ObMySQLTransaction &trans);
  /*
   * transfer任务在结束时可能会存在一批not_exist的分区，但是这一部分分区可能
   * 不是真的not_exist，可能是由于前置统计的源端日志流不正确导致的，需要回滚掉
   * 这部分任务的状态到waiting状态，重新生成任务
   * @param[in] tenant_id : user_tenant_id
   * @param[in] job_id : the corelative balance job id
   * @param[in] part_list : part_info start transfer
   * @param[in] transfer_task_id : the corelative transfer task id
   * @param[in] comment : reason of rollback
   * @param[in] trans: must in trans
   * */

  static int rollback_from_doing_to_waiting(const uint64_t tenant_id,
                         const ObBalanceJobID &job_id,
                         const ObTransferPartList &part_list,
                         const ObString &comment,
                         ObMySQLTransaction &trans);

  /*
  * 获取一批part_list的任务信息，part_list可能不存在表里。
  * @description: get dest_ls of part_list
   * @param[in] tenant_id : user_tenant_id
   * @param[in] part_list : part_info
   * @param[in] job_id : the corelative balance job id
   * @param[out] dest_ls : dest_ls of part_info
   * @param[in] trans: must in trans
  */
  static int load_part_list_task(const uint64_t tenant_id,
                         const ObBalanceJobID &job_id,
                         const ObTransferPartList &part_list,
                         ObIArray<ObTransferPartitionTask> &task_array,
                         ObMySQLTransaction &trans);
  /*
   * 获取指定分区的transfer partition task
   * @description: get transfer partition task of part
   * @param[in] tenant_id : user_tenant_id
   * @param[in] part_info : table_id and part_object_id
   * @param[out] task : transfer partition task
   * @param[in] sql_client: trans or sql_client
   * return OB_SUCCESS if success,
   *        OB_ENTRY_NOT_EXIST if task not exist
   *        otherwise failed
   * */
  static int get_transfer_partition_task(const uint64_t tenant_id,
      const ObTransferPartInfo &part_info,
      ObTransferPartitionTask &task,
      ObISQLClient &sql_client);
private:
  static int fill_dml_splicer_(share::ObDMLSqlSplicer &dml,
                              const ObTransferPartitionTask &task);
  static int fill_cell_(const uint64_t tenant_id,
                       sqlclient::ObMySQLResult *result,
                       ObTransferPartitionTask &task);
  static int append_sql_with_part_list_(const ObTransferPartList &part_list,
                                        ObSqlString &sql);
  static int get_tasks_(const uint64_t tenant_id,
      const ObSqlString &sql,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client);
  static int fetch_new_task_id_(const uint64_t tenant_id,
      ObMySQLTransaction &trans,
      ObTransferPartitionTaskID &task_id);

};
}
}

#endif /* !OCEANBASE_SHARE_OB_TRANSFER_PARTITION_TASK_OPERATOR_H_ */
