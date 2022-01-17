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

#ifndef DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_DML_PLAN_MGR_H_
#define DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_DML_PLAN_MGR_H_
#include "sql/executor/ob_job.h"
#include "lib/container/ob_fixed_array.h"
namespace oceanbase {
namespace sql {
class ObExecContext;
struct ObTableDMLCtx;

class ObMultiDMLPlanMgr {
  struct PartValuesInfo {
  public:
    PartValuesInfo()
        : datum_store_(NULL),
          row_store_(NULL),
          part_key_ref_id_(common::OB_INVALID_ID),
          value_ref_id_(common::OB_INVALID_ID)
    {}
    TO_STRING_KV(K_(part_key), KPC_(row_store), K_(part_key_ref_id), K_(value_ref_id));

    common::ObPartitionKey part_key_;
    ObChunkDatumStore* datum_store_;
    common::ObRowStore* row_store_;
    uint64_t part_key_ref_id_;
    uint64_t value_ref_id_;
  };

  class ServerOpInfo;
  typedef common::ObArrayWrap<PartValuesInfo> PartSubPlanArray;
  typedef common::ObSEArray<PartSubPlanArray, 4> IndexSubPlanInfo;
  typedef common::ObArrayWrap<IndexSubPlanInfo> TableSubPlanInfo;

public:
  ObMultiDMLPlanMgr(common::ObIAllocator& allocator)
      : allocator_(allocator),
        exec_ctx_(NULL),
        table_dml_ctxs_(NULL),
        table_subplan_array_(),
        mini_task_infos_(allocator),
        table_need_first_(false)
  {}
  ~ObMultiDMLPlanMgr()
  {
    reset();
  }
  void reset();
  int init(ObExecContext* exec_ctx, common::ObIArrayWrap<ObTableDMLCtx>* dml_table_ctxs, const ObPhysicalPlan* my_plan,
      const ObPhyOperator* subplan_root, const ObOpSpec* se_subplan_root);
  int get_runner_server(uint64_t table_id, uint64_t index_tid, int64_t part_id, common::ObAddr& runner_server);
  int add_part_row(int64_t table_idx, int64_t index_idx, int64_t part_idx, int64_t op, const common::ObNewRow& row);
  int add_part_row(
      int64_t table_idx, int64_t index_idx, int64_t part_idx, int64_t op, const common::ObIArray<ObExpr*>& row);
  void release_part_row_store();
  int build_multi_part_dml_task();
  inline common::ObIArray<ObTaskInfo*>& get_mini_task_infos()
  {
    return mini_task_infos_;
  }
  inline const ObMiniJob& get_subplan_job() const
  {
    return subplan_job_;
  }
  inline bool table_need_first() const
  {
    return table_need_first_;
  }

private:
  int get_or_create_part_subplan_info(int64_t table_idx, int64_t index_idx, int64_t part_idx, int64_t op,
      PartValuesInfo*& part_subplan_info, bool is_static_engine = false);
  int get_server_op_info(
      common::ObIArray<ServerOpInfo>& server_ops, const common::ObAddr& runner_server, ServerOpInfo*& server_op);
  int generate_all_task_info(const common::ObAddr& table_runner_server, common::ObIArray<ServerOpInfo>& server_ops);
  int generate_mini_task_info(
      const ServerOpInfo table_op, const ObJobID& ob_job_id, uint64_t task_id, ObTaskInfo& task_info);
  int allocate_mini_task_info(ObTaskInfo*& task_info);

private:
  common::ObIAllocator& allocator_;
  ObExecContext* exec_ctx_;
  common::ObIArrayWrap<ObTableDMLCtx>* table_dml_ctxs_;
  common::ObArrayWrap<TableSubPlanInfo> table_subplan_array_;
  common::ObFixedArray<ObTaskInfo*, common::ObIAllocator> mini_task_infos_;
  bool table_need_first_;
  ObMiniJob subplan_job_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_DML_PLAN_MGR_H_ */
