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

#ifndef OCEANBASE_SHARE_OB_BALANCE_TASK_OPERATOR_H_
#define OCEANBASE_SHARE_OB_BALANCE_TASK_OPERATOR_H_

#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "lib/string/ob_sql_string.h"//ObSqlString
#include "lib/allocator/ob_allocator.h"//ObIAllocator
#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ob_display_list.h"  // ObDisplayList
#include "share/transfer/ob_transfer_info.h"//ObTransferPartList
#include "share/ob_balance_define.h" // ObBalanceJobID, ObBalanceTaskID, ObTransferTaskID

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObString;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace storage
{
namespace mds
{
class BufferCtx;
}
}
namespace share
{
class ObBalanceJobStatus;
class ObDMLSqlSplicer;
#define IS_BALANCE_TASK(BALANCE_TASK, BALANCE)\
  bool is_##BALANCE()const { return BALANCE_TASK == val_;}
class ObBalanceTaskStatus
{
public:
  static const int64_t BALANCE_TASK_STATUS_INVALID = -1;
  static const int64_t BALANCE_TASK_STATUS_INIT = 0;
  static const int64_t BALANCE_TASK_STATUS_CREATE_LS = 1;
  static const int64_t BALANCE_TASK_STATUS_ALTER_LS = 2;
  static const int64_t BALANCE_TASK_STATUS_SET_LS_MERGING = 3;
  static const int64_t BALANCE_TASK_STATUS_TRANSFER = 4;
  static const int64_t BALANCE_TASK_STATUS_DROP_LS = 5;
  static const int64_t BALANCE_TASK_STATUS_COMPLETED = 6;
  static const int64_t BALANCE_TASK_STATUS_CANCELED = 7;
  static const int64_t BALANCE_TASK_STATUS_MAX = 8;
  ObBalanceTaskStatus(const int64_t value = BALANCE_TASK_STATUS_INVALID) : val_(value) {};
  ObBalanceTaskStatus(const ObString &str);
  ~ObBalanceTaskStatus() {reset(); }

public:
  void reset() { val_ = BALANCE_TASK_STATUS_INVALID; }
  bool is_valid() const { return val_ != BALANCE_TASK_STATUS_INVALID; }
  const char* to_str() const;

  // assignment
  ObBalanceTaskStatus &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObBalanceTaskStatus &other) const { return val_ == other.val_; }
  bool operator != (const ObBalanceTaskStatus &other) const { return val_ != other.val_; }

  // ObBalanceTaskStatus attribute interface
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_INIT, init)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_CREATE_LS, create_ls)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_ALTER_LS, alter_ls);
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_DROP_LS, drop_ls)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_SET_LS_MERGING, set_merge_ls)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_TRANSFER, transfer)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_COMPLETED, completed)
  IS_BALANCE_TASK(BALANCE_TASK_STATUS_CANCELED, canceled)

  // COMPLETED / CANCELED
  bool is_finish_status() const { return (is_completed() || is_canceled()); }

  TO_STRING_KV(K_(val), "task_status", to_str());
private:
  int64_t val_;
};
class ObBalanceTaskType
{
public:
  static const int64_t BALANCE_TASK_INVALID = -1;
  static const int64_t BALANCE_TASK_SPLIT = 0;
  static const int64_t BALANCE_TASK_ALTER = 1;
  static const int64_t BALANCE_TASK_MERGE = 2;
  static const int64_t BALANCE_TASK_TRANSFER = 3;
  static const int64_t BALANCE_TASK_MAX = 4;


  ObBalanceTaskType(const int64_t value = BALANCE_TASK_INVALID) : val_(value) {}
  ObBalanceTaskType(const ObString &str);
public:
  void reset() { val_ = BALANCE_TASK_INVALID; }
  bool is_valid() const { return val_ != BALANCE_TASK_INVALID; }
  const char* to_str() const;
  TO_STRING_KV(K_(val), "task_type", to_str());

  IS_BALANCE_TASK(BALANCE_TASK_SPLIT, split_task)
  IS_BALANCE_TASK(BALANCE_TASK_ALTER, alter_task)
  IS_BALANCE_TASK(BALANCE_TASK_MERGE, merge_task)
  IS_BALANCE_TASK(BALANCE_TASK_TRANSFER, transfer_task)
  // assignment
  ObBalanceTaskType &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObBalanceTaskType &other) const { return val_ == other.val_; }
  bool operator != (const ObBalanceTaskType &other) const { return val_ != other.val_; }

private:
  int64_t val_;
};

typedef ObDisplayList<ObBalanceTaskID> ObBalanceTaskIDList;

class ObBalanceTaskMDSHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      storage::mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      storage::mds::BufferCtx &ctx);
};

struct ObBalanceTask
{
public:
  ObBalanceTask() { reset(); }
  ~ObBalanceTask() {}
  void reset();
  int init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObTransferPartList &part_list,
           const ObTransferPartList &finished_part_list,
           const ObBalanceTaskIDList &parent_list,
           const ObBalanceTaskIDList &child_list,
           const ObString &comment);
  int init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObString &part_list_str,
           const ObString &finished_part_list_str,
           const ObString &parent_list_str,
           const ObString &child_list_str,
           const ObString &comment);
  int simple_init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferPartList &part_list);

  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(balance_task_id), K_(task_status),
               K_(src_ls_id), K_(dest_ls_id),
               K_(task_type), K_(ls_group_id),
               K_(current_transfer_task_id), K_(parent_list), K_(child_list),
               K_(part_list), K_(finished_part_list), K_(comment));
private:
int init_(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObString &comment);
public:
  ObBalanceTaskStatus get_next_status(const ObBalanceJobStatus &job_status) const;
#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(ObLSID, src_ls_id)
  Property_declare_var(ObLSID, dest_ls_id)
  Property_declare_var(ObBalanceJobID, job_id)
  Property_declare_var(ObBalanceTaskID, balance_task_id)
  Property_declare_var(ObBalanceTaskType, task_type)
  Property_declare_var(ObBalanceTaskStatus, task_status)
  Property_declare_var(uint64_t, ls_group_id)
  Property_declare_var(ObTransferTaskID, current_transfer_task_id)

#undef Property_declare_var
  int assign(const ObBalanceTask &other);
public:
  const ObBalanceTaskIDList& get_parent_task_list() const { return parent_list_; }
  const ObBalanceTaskIDList& get_child_task_list() const { return child_list_; }
  const ObTransferPartList& get_part_list() const {return part_list_;}
  const ObTransferPartList& get_finished_part_list() const {return finished_part_list_;}
  ObBalanceTaskIDList& get_parent_task_list() { return parent_list_; }
  ObBalanceTaskIDList& get_child_task_list() { return child_list_; }
  const ObSqlString& get_comment() const { return comment_; }

private:
  ObTransferPartList part_list_;
  ObTransferPartList finished_part_list_;
  ObBalanceTaskIDList parent_list_;
  ObBalanceTaskIDList child_list_;
  ObSqlString comment_;
};
typedef ObArray<ObBalanceTask> ObBalanceTaskArray;
typedef ObIArray<ObBalanceTask> ObBalanceTaskIArray;
class ObBalanceTaskTableOperator
{
public:
  /**
   * @description: insert new task to __all_balance_task
   * @param[in] task : a valid balance task include tenant_id
   * @param[in] client : sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   */
  static int insert_new_task(const ObBalanceTask &task,
                     ObISQLClient &client);
  /**
   * @description: get task from __all_balance_task by balance_task_id
   * @param[in] tenant_id : user_tenant_id
   * @param[in] balance_task_id : target balance task id
   * @param[in] for_update: whether lock the task
   * @param[in] client : sql client or trans
   * @param[out] task : get a valid balance task
   * @param[out] start_time : task gmt_create in table
   * @param[out] finish_time : task current gmt_modify
   * @return :
   * OB_SUCCESS : get the target balance task
   * OB_ENTRY_NOT_EXIST : the task not exist
   * OTHER : fail
   */
  static int get_balance_task(const uint64_t tenant_id,
                      const ObBalanceTaskID balance_task_id,
                      const bool for_update,
                      ObISQLClient &client,
                      ObBalanceTask &task,
                      int64_t &start_time,
                      int64_t &finish_time);
  /**
   * @description: update task status of __all_balance_task
   * @param[in] balance_task : target balance task id and tenant_id, and old_task_status
   * @param[in] new_task_status : new task status
   * @param[in] trans : must update in trans
   * @return :
   * OB_SUCCESS : update task status success
   * OB_STATE_NOT_MATCH : current task status not match, can not update
   * OTHER : fail
   */
  static int update_task_status(const ObBalanceTask &balance_task,
                                const ObBalanceTaskStatus new_task_status,
                                ObMySQLTransaction &trans);
  /**
   * @description: update comment of balance task
   * @param[in] tenant_id : user_tenant_id
   * @param[in] new_comment : new comment
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS : update comment success
   * OB_STATE_NOT_MATCH : task not exist
   * OTHER : fail
   */
  static int update_task_comment(const uint64_t tenant_id,
                               const ObBalanceTaskID task_id,
                               const common::ObString &new_comment,
                               ObISQLClient &client);
    /**
   * @description: old status must be tranfer and current transfer task must be empty
   * @param[in] tenant_id : user_tenant_id
   * @param[in] balance_task_id : target balance task id
   * @param[in] tranfer_task_id : tranfer_task_id start by the task
   * @param[in]: client: sql client or trans
   * @return :
   * OB_SUCCESS : start the transfer task
   * OB_STATE_NOT_MATCH : already has transfer task, can not start now
   * OTHER : fail
   */
  static int start_transfer_task(const uint64_t tenant_id,
                                 const ObBalanceTaskID balance_task_id,
                                 const ObTransferTaskID transfer_task_id,
                                 ObISQLClient &client);
  /**
   * @description: finish transfer task, set current_transfer_task and finish_part_list
   * @param[in] balance_task : a valid balance task include tenant_id
   * @param[in] tranfer_task_id : tranfer_task_id start by the task
   * @param[in] transfer_finished_part_list: finished partitions of the transfer task
   * @param[in] client: sql client or trans
   * @param[out] to_do_part_list: to_do_part_list = balance_part_list - transfer_finished_part_list
   * @param[out] all_part_transferred: whether all parts of the balance task have been transferred
   * @return :
   * OB_SUCCESS : finish the transfer task
   * OB_STATE_NOT_MATCH : the transfer_task maybe finished
   * OTHER : fail
   */
  static int finish_transfer_task(const ObBalanceTask &balance_task,
                                  const ObTransferTaskID transfer_task_id,
                                  const ObTransferPartList &transfer_finished_part_list,
                                  ObISQLClient &client,
                                  ObTransferPartList &to_do_part_list,
                                  bool &all_part_transferred);
  /**
   * @description: delete task from of __all_balance_task and insert to task to __all_balance_task_history
   * @param[in] tenant_id : user_tenant_id
   * @param[in] balance_task_id : target balance task id
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS : clean task
   * OB_ENTRY_NOT_EXIST : the balance task already has been clean
   * OB_STATE_NOT_MATCH : the balance task is not finished, status not match
   * OTHER : fail
   */
  static int clean_task(const uint64_t tenant_id,
                        const ObBalanceTaskID balance_task_id,
                        common::ObMySQLTransaction &trans);


   /**
   * @description: clean parent info of all child tasks after parent task is completed
   *
   * @param[in] tenant_id           user_tenant_id
   * @param[in] parent_task_id      target parent task id which is complete
   * @param[in] balance_job_id      balance job id
   * @param[in] trans               transaction client
   *
   * @return :
   * - OB_SUCCESS   succeed to clean parent info of all child tasks
   * - OTHER        fail
   */
  static int clean_parent_info(
      const uint64_t tenant_id,
      const ObBalanceTaskID parent_task_id,
      const ObBalanceJobID balance_job_id,
      common::ObMySQLTransaction &trans);

   /**
   * @description: after process one task, set child_task can process
   * @param[in] tenant_id : user_tenant_id
   * @param[in] balance_task_id : target balance task id
   * @param[in] parent_task_id : remove parent_task_id from the balance_task_id
   * @param[in] client: sql client or trans
   * @return :
   * OB_SUCCESS : clean task
   * OB_STATE_NOT_MATCH : the parent task has beed remove from the balance task
   * OTHER : fail
   */
  static int remove_parent_task(const uint64_t tenant_id,
                               const ObBalanceTaskID balance_task_id,
                               const ObBalanceTaskID parent_task_id,
                               ObISQLClient &client);
  static int fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceTask &task);
  /*
   * @description: load all task which parent_list is empty
   * @param[in] tenant_id : user_tenant_id
   * @param[out] balance_task_array : get all task which parent_list is empty
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int load_can_execute_task(const uint64_t tenant_id,
                                   ObBalanceTaskIArray &task_array,
                                   ObISQLClient &client);
  /*
   * @description: load all task
   * @param[in] tenant_id : user_tenant_id
   * @param[out] balance_task_array : get all task
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int load_task(const uint64_t tenant_id,
                                   ObBalanceTaskIArray &task_array,
                                   ObISQLClient &client);

  /*
   * @description: load all task of the balance job that parent list not empty
   * @param[in] job_id : balance_job_id
   * @param[out] balance_task_array : get all task of the job
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int get_job_cannot_execute_task(const uint64_t tenant_id,
                              const ObBalanceJobID balance_job_id,
                              ObBalanceTaskIArray &task_array,
                              ObISQLClient &client);
  static int read_tasks_(const uint64_t tenant_id, ObISQLClient &client,
                         const ObSqlString &sql, ObBalanceTaskIArray &task_array);
  static int fill_cell(const uint64_t tenant_id, sqlclient::ObMySQLResult*result, ObBalanceTask &task);
  /*
   * @description: for check job can finish, get task cnt of the job, if is zero, job can finish
   * @param[in] tenant_id : user_tenant_id
   * @param[in] job_id : balance_job_id
   * @param[out] task_cnt : task count of the job
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int get_job_task_cnt(const uint64_t tenant_id, const ObBalanceJobID job_id,
  int64_t &task_cnt, ObISQLClient &client);
  /*
   * @description: update ls part list before set to transfer
   * @param[in] tenant_id : user_tenant_id
   * @param[in] balance_task_id : balance_task_id
   * @param[in] part_list : all part list of src ls
   * @param[in] trans: update in trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int update_task_part_list(const uint64_t tenant_id,
                               const ObBalanceTaskID balance_task_id,
                               const ObTransferPartList &part_list,
                               common::ObMySQLTransaction &trans);

  /**
   * @description: get merge task dest_ls according to src_ls
   *
   * @param[in] sql_client          sql client or trans
   * @param[in] tenant_id           user_tenant_id
   * @param[in] src_ls              source ls id
   * @param[out] dest_ls            destination ls id
   *
   * @return :
   * - OB_SUCCESS   succeed to get dest_ls by src_ls
   * - OB_ENTRY_NOT_EXIST  task not found according to src_ls
   * - OTHER        failed
   */
  static int get_merge_task_dest_ls_by_src_ls(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObLSID &src_ls,
      ObLSID &dest_ls);
  /*
   * @description: load all task which has transfer task
   * @param[in] tenant_id : user_tenant_id
   * @param[out] balance_task_array : get all task which has transfer task
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   * */
  static int load_need_transfer_task(const uint64_t tenant_id,
                                       ObBalanceTaskIArray &task_array,
                                       ObISQLClient &client);

};
#undef IS_BALANCE_TASK
}
}

#endif /* !OCEANBASE_SHARE_OB_BALANCE_TASK_OPERATOR_H_ */
