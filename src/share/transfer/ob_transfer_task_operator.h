/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_TRANSFER_TASK_OPERATOR
#define OCEANBASE_SHARE_OB_TRANSFER_TASK_OPERATOR

#include "share/transfer/ob_transfer_info.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;
class ObTabletLSCache;

// operator for __all_transfer_task
class ObTransferTaskOperator final
{
public:
  ObTransferTaskOperator() {};
  virtual ~ObTransferTaskOperator() {};

  /*
   * get a transfer task by task_id
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] for_update: select for update
   * @param [out] task:      transfer task
   * @param [in] group_id:   rpc queue id
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int get(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const bool for_update,
      ObTransferTask &task,
      const int32_t group_id);

  /*
   * get a transfer task by task_id with create_time and finish_time
   *
   * @param [in] sql_proxy:    sql client
   * @param [in] tenant_id:    target tenant_id
   * @param [in] task_id:      target task_id
   * @param [in] for_update:   select for update
   * @param [out] task:        transfer task
   * @param [out] create_time: transfer task create timestamp
   * @param [out] finish_time: transfer task finish timestamp
   * @return
   * - OB_ENTRY_NOT_EXIST:     not found
   * - OB_SUCCESS:             successful
   * - other:                  failed
   */
  static int get_task_with_time(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const bool for_update,
      ObTransferTask &task,
      int64_t &create_time,
      int64_t &finish_time);

  /*
   * get transfer tasks by status
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] status:     transfer status
   * @param [out] tasks:     transfer tasks array
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int get_by_status(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferStatus &status,
      common::ObIArray<ObTransferTask> &tasks);

  /*
   * get all task status
   *
   * @param [in] sql_proxy:    sql client
   * @param [in] tenant_id:    target tenant_id
   * @param [out] task_status: array of task status
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int get_all_task_status(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      common::ObIArray<ObTransferTask::TaskStatus> &task_status);

  /*
   * get transfer task by src ls (there is no more than 1 transfer task on a ls)
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] src_ls:    src ls_id
   * @param [out] task:      transfer task
   * @param [in] group_id:   rpc queue id
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_ERR_UNEXPECTED:    more than 1 transfer task on a ls
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int get_by_src_ls(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObLSID &src_ls,
      ObTransferTask &task,
      const int32_t group_id);

  /*
   * get transfer task by dest ls (there is no more than 1 transfer task on a ls)
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] dest_ls:    destination ls_id
   * @param [out] task:      transfer task
   * @param [in] group_id:   rpc queue id
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_ERR_UNEXPECTED:    more than 1 transfer task on a ls
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int get_by_dest_ls(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObLSID &dest_ls,
      ObTransferTask &task,
      const int32_t group_id);

  /*
   * insert task
   *
   * @param [in] sql_proxy: sql client
   * @param [in] tenant_id: target tenant_id
   * @param [in] task:      transfer task
   * @return
   * - OB_ENTRY_EXIST:      duplicate insert
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int insert(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTask &task);

  /*
   * update transfer task from INIT to START
   *
   * @param [in] sql_proxy:                   sql client
   * @param [in] tenant_id:                   target tenant_id
   * @param [in] task_id:                     target task_id
   * @param [in] old_status:                  old task status
   * @param [in] new_part_list:               part_list of start status transfer task
   * @param [in] new_not_exist_part_list:     partitions that not exist
   * @param [in] new_lock_conflict_part_list: partitions that are in conflict when adding table lock or online ddl lock
   * @param [in] new_table_lock_tablet_list:  tablet list which add table lock successfully
   * @param [in] new_tablet_list:             new tablet_list
   * @param [in] new_status:                  new task status
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int update_to_start_status(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferStatus &old_status,
      const ObTransferPartList &new_part_list,
      const ObTransferPartList &new_not_exist_part_list,
      const ObTransferPartList &new_lock_conflict_part_list,
      const ObDisplayTabletList &new_table_lock_tablet_list,
      const ObTransferTabletList &new_tablet_list,
      const ObTransferStatus &new_status,
      const transaction::tablelock::ObTableLockOwnerID &table_lock_owner_id);

  /*
   * finish task and update status to CANCELED/FAILED/COMPLETED
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] old_status: old task status
   * @param [in] new_status: new task status
   * @param [in] result:     return code for the transfer process
   * @param [in] comment:    information for task finish
   * @param [in] group_id:   rpc queue id
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int finish_task(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferStatus &old_status,
      const ObTransferStatus &new_status,
      const int result,
      const ObTransferTaskComment &comment,
      const int32_t group_id);

  /*
   * finish task from INIT status to COMPLETED when part_list is all unreachable
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] old_status: old task status
   * @param [in] new_part_list:               part_list of the finished task
   * @param [in] new_not_exist_part_list:     partitions that not exist
   * @param [in] new_lock_conflict_part_list: partitions that are in conflict when adding table lock or online ddl lock
   * @param [in] new_status: new task status
   * @param [in] result:     return code for the transfer process
   * @param [in] comment:    information for task finish
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int finish_task_from_init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferStatus &old_status,
      const ObTransferPartList &new_part_list,
      const ObTransferPartList &new_not_exist_part_list,
      const ObTransferPartList &new_lock_conflict_part_list,
      const ObTransferStatus &new_status,
      const int result,
      const ObTransferTaskComment &comment);

  /*
   * remove task
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @return
   * - OB_ENTRY_NOT_EXIST:   not found
   * - OB_SUCCESS:           successful
   * - other:                failed
   */
  static int remove(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id);

  /*
   * update task status and result
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] old_status: old task status
   * @param [in] new_status: new task status
   * @param [in] result:     task result
   * @param [in] group_id:   rpc queue id
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int update_status_and_result(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferStatus &old_status,
      const ObTransferStatus &new_status,
      const int result,
      const int32_t group_id);


  /*
   * update start_scn
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] old_status: old task status
   * @param [in] start_scn:  start scn
   * @param [in] group_id:   rpc queue id, default is 0
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int update_start_scn(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferStatus &old_status,
      const share::SCN &start_scn,
      const int32_t group_id);

  /*
   * update finish_scn
   *
   * @param [in] sql_proxy:  sql client
   * @param [in] tenant_id:  target tenant_id
   * @param [in] task_id:    target task_id
   * @param [in] old_status: old task status
   * @param [in] finish_scn: finish scn
   * @param [in] group_id:   rpc queue id, default is 0
   * @return
   * - OB_STATE_NOT_MATCH:  task not found or task status mismatch
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int update_finish_scn(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID,
      const ObTransferStatus &old_status,
      const share::SCN &finish_scn,
      const int32_t group_id);

  /*
   * record transfer task in __all_transfer_task_history
   *
   * @param [in] sql_proxy:   sql client
   * @param [in] tenant_id:   target tenant_id
   * @param [in] task:        transfer task
   * @param [in] create_time: transfer task create timestamp
   * @param [in] finish_time: transfer task finish timestamp
   * @return
   * - OB_ENTRY_EXIST:        duplicate insert
   * - OB_SUCCESS:            successful
   * - other:                 failed
   */
  static int insert_history(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTask &task,
      const int64_t create_time,
      const int64_t finish_time);

  /*
   * get transfer task from __all_transfer_task_history
   *
   * @param [in] sql_proxy: sql client
   * @param [in] tenant_id: target tenant_id
   * @param [in] task_id:   transfer task id
   * @param [out] task:     transfer task recorded in __all_transfer_task_history
   * @param [out] create_time: create time of the transfer task
   * @param [out] finish_time: finish time of the transfer task
   * @return
   * - OB_ENTRY_NOT_EXIST:  not found
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int get_history_task(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      ObTransferTask &task,
      int64_t &create_time,
      int64_t &finish_time);

  /*
   * get max transfer task id from __all_transfer_task_history
   *
   * @param [in] sql_proxy:     sql client
   * @param [in] tenant_id:     target tenant_id
   * @param [out] max_task_id:  max transfer task id
   *                            return INVALID_ID(-1) when history is empty
   * @return
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int get_max_task_id_from_history(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      ObTransferTaskID &max_task_id);

  /*
   * get last transfer task from __all_transfer_task_history by balance_task_id
   *
   * @param [in] sql_proxy:       sql client
   * @param [in] tenant_id:       target tenant_id
   * @param [in] balance_task_id: target balance task id
   * @param [out] last_task:      transfer task with max transfer_id in history
   * @param [out] finish_time:    finish time of the last task
   * @return
   * - OB_SUCCESS:          successful
   * - OB_ENTRY_NOT_EXIST:  no history
   * - other:               failed
   */
  static int get_last_task_by_balance_task_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObBalanceTaskID &balance_task_id,
      ObTransferTask &last_task,
      int64_t &finish_time);

  /*
   * update comment in __all_transfer_task according to task_id
   *
   * @param [in] sql_proxy: sql client
   * @param [in] tenant_id: target tenant_id
   * @param [in] task_id:   transfer task id
   * @param [in] comment:   comment for the task
   * @return
   * - OB_SUCCESS:          successful
   * - other:               failed
   */
  static int update_comment(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTransferTaskID task_id,
      const ObTransferTaskComment &comment);

  /*
   * generate new task_id for transfer task
   *
   * should be protected by trans to ensure task_id is unique
   */
  static int generate_transfer_task_id(
             common::ObMySQLTransaction &trans,
             const uint64_t tenant_id,
             ObTransferTaskID &new_task_id);

  /*-----For auto refresh tablet location----*/

  static int fetch_initial_base_task_id(
             common::ObISQLClient &sql_proxy,
             const uint64_t tenant_id,
             ObTransferTaskID &base_task_id);

  static int fetch_inc_task_infos(
             common::ObISQLClient &sql_proxy,
             const uint64_t tenant_id,
             const ObTransferTaskID &base_task_id,
             common::ObIArray<ObTransferRefreshInfo> &inc_task_infos);

  static int batch_get_tablet_ls_cache(
             common::ObISQLClient &sql_proxy,
             const uint64_t tenant_id,
             const common::ObIArray<ObTransferTaskID> &task_ids,
             common::ObIArray<ObTabletLSCache> &tablet_ls_caches);
  /*-----------------------------------------*/

private:
  static int get_by_ls_id_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const bool is_src_ls,
      const int32_t group_id,
      ObTransferTask &task);
  static int construct_transfer_tasks_(
      const uint64_t tenant_id,
      common::sqlclient::ObMySQLResult &res,
      common::ObIArray<ObTransferTask> &tasks);
  static int construct_transfer_task_(
      const uint64_t tenant_id,
      common::sqlclient::ObMySQLResult &res,
      ObTransferTask &task);
  static int parse_sql_result_(
      const uint64_t tenant_id,
      common::sqlclient::ObMySQLResult &res,
      const bool with_time,
      ObTransferTask &task,
      int64_t &create_time,
      int64_t &finish_time);
  static int construct_task_status_(
      common::sqlclient::ObMySQLResult &res,
      common::ObIArray<ObTransferTask::TaskStatus> &task_status);
  static int fill_dml_splicer_(
      ObDMLSqlSplicer &dml_splicer,
      common::ObArenaAllocator &allocator,
      const ObTransferTask &task);
  static int convert_data_version_(
      const uint64_t tenant_id,
      const bool is_history,
      const bool column_not_exist,
      const ObString &data_version_str,
      uint64_t &data_version);
};

} // end namespace share
} // end namespace oceanbase
#endif
