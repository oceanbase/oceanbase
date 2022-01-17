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

#ifndef OB_TABLE_LOOK_UP_H_
#define OB_TABLE_LOOK_UP_H_
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/ob_phy_table_location.h"
#include "share/schema/ob_schema_struct.h"
#include "common/ob_range.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace sql {

typedef struct _ObLookupInfo {
  OB_UNIS_VERSION_V(1);

public:
  _ObLookupInfo()
      : ref_table_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        partition_num_(0),
        partition_cnt_(0),
        schema_version_(-1),
        is_old_no_pk_table_(false)
  {}
  virtual ~_ObLookupInfo()
  {}
  int assign(const _ObLookupInfo& other);
  void reset();
  TO_STRING_KV(
      K_(table_id), K_(ref_table_id), K_(partition_num), K_(partition_cnt), K_(schema_version), K_(is_old_no_pk_table));
  // real table id
  uint64_t ref_table_id_;
  // for get phy table location
  uint64_t table_id_;
  // partition num
  int64_t partition_num_;
  // for init partition key
  int64_t partition_cnt_;
  // used for the optimization to do query range pruning
  int64_t schema_version_;
  bool is_old_no_pk_table_;
} ObLookupInfo;

class ObTableLookup : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);
  class ObTableLookupCtx;

private:
  enum LookupState { INDEX_SCAN, DISTRIBUTED_LOOKUP, OUTPUT_ROWS, EXECUTION_FINISHED };

public:
  explicit ObTableLookup(common::ObIAllocator& alloc);
  virtual ~ObTableLookup();

  ObLookupInfo& get_lookup_info()
  {
    return lookup_info_;
  }
  inline void set_remote_plan(ObPhyOperator* remote_table_scan)
  {
    remote_table_scan_ = remote_table_scan;
  };
  inline ObPhyOperator* get_remote_plan() const
  {
    return remote_table_scan_;
  };
  ObTableLocation& get_part_id_getter()
  {
    return table_location_;
  }

protected:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   *         pure virtual function
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief reopen the operator, with new params
   * if any operator needs rescan, code generator must have told 'him' that
   * where should 'he' get params, e.g. op_id
   */
  virtual int rescan(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;

private:
  int process_row(ObExecContext& ctx, const common::ObNewRow* row, int64_t& part_row_cnt) const;
  int wait_all_task(ObTableLookupCtx& dml_ctx, ObPhysicalPlanCtx* plan_ctx) const;

private:
  /*
   *  default row count
   * */
  const static int64_t DEFAULT_BATCH_ROW_COUNT = 1024l * 1024;
  /*
   *  default max row count in single partition
   * */
  const static int64_t DEFAULT_PARTITION_BATCH_ROW_COUNT = 1000L;
  ObPhyOperator* remote_table_scan_;
  ObLookupInfo lookup_info_;
  ObTableLocation table_location_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif
