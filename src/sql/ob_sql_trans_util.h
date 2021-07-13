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

#ifndef OCEANBASE_SQL_TRANS_UTIL_
#define OCEANBASE_SQL_TRANS_UTIL_

#include "share/ob_define.h"
#include "sql/ob_sql_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/transaction/ob_trans_define.h"
namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
class TransResult {
  OB_UNIS_VERSION(1);

public:
  TransResult() : is_incomplete_(false), lock_(), max_sql_no_(0), trans_desc_(NULL)
  {}

  void reset();
  const common::ObPartitionArray& get_total_partitions()
  {
    return total_partitions_;
  }
  const common::ObPartitionArray& get_response_partitions()
  {
    return response_partitions_;
  }
  const transaction::ObPartitionEpochArray& get_part_epoch_list()
  {
    return part_epoch_list_;
  }
  int merge_result(const TransResult& other);
  int merge_total_partitions(const common::ObPartitionArray& partitions);
  int merge_response_partitions(const common::ObPartitionArray& partitions);
  int merge_part_epoch_list(const transaction::ObPartitionEpochArray& part_epoch_list);
  void set_incomplete()
  {
    is_incomplete_ = true;
  }
  bool is_incomplete() const
  {
    return is_incomplete_;
  }
  int set_max_sql_no(int64_t sql_no);
  int64_t get_max_sql_no() const
  {
    return max_sql_no_;
  }
  void set_trans_desc(transaction::ObTransDesc* trans_desc)
  {
    trans_desc_ = trans_desc;
  }
  int assign(const TransResult& other);
  void clear_stmt_result();
  TO_STRING_KV(K_(total_partitions), K_(part_epoch_list), K_(response_partitions), K_(max_sql_no));

private:
  // collect all partitions for foreign key.
  common::ObPartitionArray total_partitions_;
  // discard_partitions_ indicates that the transaction in the partition is enabled,
  // but the data of partition is not read or written
  // record the partition info of the task that was actually executed
  // response_partitions_ indicates that the partition must be read or written
  // response_partitions_ + discard_partitions_ record the partition info of the task that
  // has been start_participants
  common::ObPartitionArray response_partitions_;
  // part_epoch_list_ record the epoch id of response_partitions_
  // when start_participants executed in the leader replica
  transaction::ObPartitionEpochArray part_epoch_list_;
  // indicate the partitions array is complete or not.
  bool is_incomplete_;
  common::ObSpinLock lock_;

  // The transaction layer requires max_sql_no to increase monotonically,
  // so it is transmitted across nodes with the help of the sql layer TransResult mechanism.
  // But the transaction layer needs to pass from the remote TransDesc object to the local TransDesc object,
  // and the existing mechanism of sql can only be from
  // The remote TransResult object is passed to the local TransResult object,
  // so here is a new TransDesc object
  // Pointer to facilitate access to the TransDesc object during transmission.
  // ps: This pointer is not set for all TransResult objects,
  // such as the two objects of the scheduling thread
  // Not in the same session object, it is not convenient to set.
  int64_t max_sql_no_;
  transaction::ObTransDesc* trans_desc_;
};

class ObSqlTransUtil {
public:
  /* Determine whether a statement should start a transaction remotely */
  static bool is_remote_trans(bool ac, bool in_trans, ObPhyPlanType ptype)
  {
    return true == ac && false == in_trans && OB_PHY_PLAN_REMOTE == ptype;
  }

  /* Determine whether the transaction can be automatically opened */
  static bool plan_can_start_trans(bool ac, bool in_trans)
  {
    UNUSED(ac);
    return false == in_trans;
  }

  /* Determine whether the current transaction can be automatically ended */
  static bool plan_can_end_trans(bool ac, bool in_trans)
  {
    return false == in_trans && true == ac;
  }

  /* Determine whether cmd can automatically end the previous transaction */
  static bool cmd_need_new_trans(bool ac, bool in_trans)
  {
    UNUSED(ac);
    return true == in_trans;
  }

private:
  ObSqlTransUtil(){};
  ~ObSqlTransUtil(){};
  DISALLOW_COPY_AND_ASSIGN(ObSqlTransUtil);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_TRANS_UTIL_ */
//// end of header file
