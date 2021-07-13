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

#ifndef OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_

#include "lib/allocator/ob_allocator.h"
#include "sql/executor/ob_job.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/table/ob_mv_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}
namespace sql {
class ObJob;
class ObJobConf;
class ObTransmit;
class ObTaskInfo;
class ObPhysicalPlanCtx;
class ObTaskExecutorCtx;
class ObTableScan;
class ObPhyOperator;
class ObTableModify;

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>
class ObTaskSpliter {
public:
  enum TaskSplitType {
    INVALID_SPLIT = 0,
    LOCAL_IDENTITY_SPLIT = 1,   // root(local) transmit task splitter
    REMOTE_IDENTITY_SPLIT = 2,  // remote transmit task splitter
    PARTITION_RANGE_SPLIT = 3,  // distributed transmit task splitter
    INTERM_SPLIT = 4,
    INSERT_SPLIT = 5,
    INTRA_PARTITION_SPLIT = 6,  // split single partition into multiple individual ranges
    DISTRIBUTED_SPLIT = 7,
    DETERMINATE_TASK_SPLIT = 8  // tasks are splited already
  };

public:
  ObTaskSpliter();
  virtual ~ObTaskSpliter();
  int init(ObPhysicalPlanCtx* plan_ctx, ObExecContext* exec_ctx, ObJob& job, common::ObIAllocator& allocator);
  virtual int get_next_task(ObTaskInfo*& task) = 0;
  virtual TaskSplitType get_type() const = 0;
  VIRTUAL_TO_STRING_KV(K_(server));

  static int find_scan_ops(common::ObIArray<const ObTableScan*>& scan_ops, const ObPhyOperator& op);
  static int find_scan_ops(common::ObIArray<const ObTableScanSpec*>& scan_ops, const ObOpSpec& op);

  static int find_insert_ops(common::ObIArray<const ObTableModify*>& insert_ops, const ObPhyOperator& op);
  static int find_insert_ops(common::ObIArray<const ObTableModifySpec*>& insert_ops, const ObOpSpec& op);

  static int find_all_table_location_keys(
      common::ObIArray<TableLocationKey>& table_location_keys, const ObPhyOperator& op);
  static int find_all_table_location_keys(common::ObIArray<TableLocationKey>& table_location_keys, const ObOpSpec& op);
  static int check_dml_exist(bool& exist, const ObPhyOperator& op);

  bool is_inited() const
  {
    return NULL != job_;
  }

protected:
  int create_task_info(ObTaskInfo*& task);

  template <bool NEW_ENG>
  static int find_scan_ops_inner(common::ObIArray<const ENG_OP::TSC*>& scan_ops, const ENG_OP::Root& op);

  template <bool NEW_ENG>
  static int find_insert_ops_inner(common::ObIArray<const ENG_OP::TableModify*>& insert_ops, const ENG_OP::Root& op);

  template <bool NEW_ENG>
  static int find_all_table_location_keys_inner(
      common::ObIArray<TableLocationKey>& table_location_keys, const ENG_OP::Root& op);

protected:
  common::ObAddr server_;
  ObPhysicalPlanCtx* plan_ctx_;
  ObExecContext* exec_ctx_;
  common::ObIAllocator* allocator_;
  ObJob* job_;
  ObJobConf* job_conf_;
  common::ObSEArray<ObTaskInfo*, 16> task_store_;
};

#undef ENG_OP

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_SPLITER_ */
//// end of header file
