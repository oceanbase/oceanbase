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

#ifndef OB_MONITORING_DUMP_H_
#define OB_MONITORING_DUMP_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {

namespace sql {

class ObMonitoringDump : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION(1);
  class ObMonitoringDumpCtx;

public:
  explicit ObMonitoringDump(common::ObIAllocator& alloc) : ObSingleChildPhyOperator(alloc), flags_(0), dst_op_id_(-1)
  {}
  virtual ~ObMonitoringDump() = default;

  void set_flags(uint64_t flags)
  {
    flags_ = flags;
  }
  void set_dst_op_id(uint64_t dst_op_id)
  {
    dst_op_id_ = dst_op_id;
  }

private:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const override;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const override;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const override;
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;

  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

private:
  uint64_t flags_;
  uint64_t dst_op_id_;
  DISALLOW_COPY_AND_ASSIGN(ObMonitoringDump);
};

}  // namespace sql
}  // namespace oceanbase

#endif
