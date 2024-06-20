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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_LOCK_UTILS_H
#define OCEANBASE_STORAGE_TABLELOCK_OB_LOCK_UTILS_H

#include "share/ob_ls_id.h" // ObLSID
#include "share/inner_table/ob_inner_table_schema.h"
#include "storage/tablelock/ob_table_lock_common.h" // ObTableLockMode

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}

namespace transaction
{
namespace tablelock
{
class ObInnerTableLockUtil
{
public:
  // only inner tables in the white list can be locked
  // please think carefully about circular dependencies before adding inner table into the white list
  static bool in_inner_table_lock_white_list(const uint64_t inner_table_id)
  {
    bool b_ret = share::OB_ALL_BALANCE_JOB_TID == inner_table_id
              || share::OB_ALL_RECOVER_TABLE_JOB_TID == inner_table_id
              || share::OB_ALL_LS_REPLICA_TASK_TID == inner_table_id
              || share::OB_ALL_TRANSFER_PARTITION_TASK_TID == inner_table_id;
    return b_ret;
  }
  /*
   * lock inner table in trans with internal_sql_execute_timeout
   *
   * @param[in] trans:           ObMySQLTransaction
   * @param[in] tenant_id:       tenant_id of the inner table
   * @param[in] inner_table_id:  inner table id which you want to lock
   * @param[in] lock_mode:       table lock mode
   * @param[in] is_from_sql:     is from sql table_lock can retry
   * @return
   * - OB_SUCCESS:               lock inner table successfully
   * - OB_TRY_LOCK_ROW_CONFLICT: lock conflict
   * - other:                    lock failed
   */
  static int lock_inner_table_in_trans(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t inner_table_id,
      const ObTableLockMode &lock_mode,
      const bool is_from_sql);
};

class ObLSObjLockUtil
{
public:
  /*
   * lock ls in trans with internal_sql_execute_timeout
   *
   * @param[in] trans:           ObMySQLTransaction
   * @param[in] tenant_id:       tenant_id of the ls
   * @param[in] ls_id:           target log stream(ls) id
   * @param[in] lock_mode:       obj lock mode
   * @return
   * - OB_SUCCESS:               lock ls successfully
   * - OB_TRY_LOCK_ROW_CONFLICT: lock conflict
   * - other:                    lock failed
   */
  static int lock_ls_in_trans(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObTableLockMode &lock_mode);
};

} // end namespace tablelock
} // end namespace transaction
} // end namespace oceanbase
#endif
