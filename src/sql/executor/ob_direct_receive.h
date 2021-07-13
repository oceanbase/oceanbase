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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_
#define OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_

#include "sql/executor/ob_receive.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_phy_operator.h"
#include "share/ob_scanner.h"
namespace oceanbase {
namespace common {
class ObNewRow;
}
namespace sql {
class ObExecContext;
class ObTaskInfo;

class ObDirectReceiveInput : public ObReceiveInput {
  OB_UNIS_VERSION_V(1);

public:
  ObDirectReceiveInput();
  virtual ~ObDirectReceiveInput();
  virtual void reset() override;
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  virtual ObPhyOperatorType get_phy_op_type() const;

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObDirectReceiveInput);
};

class ObDirectReceive : public ObReceive {
private:
  class ObDirectReceiveCtx : public ObPhyOperatorCtx {
    friend class ObDirectReceive;

  public:
    explicit ObDirectReceiveCtx(ObExecContext& ctx);
    virtual ~ObDirectReceiveCtx();
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
    /* variables */
    common::ObScanner* scanner_;
    common::ObScanner::Iterator scanner_iter_;
    bool all_data_empty_;
    bool cur_data_empty_;
    bool first_request_received_;
    int64_t found_rows_;

  private:
    /* functions */
    /* variables */
    DISALLOW_COPY_AND_ASSIGN(ObDirectReceiveCtx);
  };

public:
  explicit ObDirectReceive(common::ObIAllocator& alloc);
  virtual ~ObDirectReceive();

  virtual int rescan(ObExecContext& ctx) const;

private:
  /* functions */
  int setup_next_scanner(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  int get_next_row_from_cur_scanner(ObDirectReceiveCtx& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;

private:
  /* macros */
  DISALLOW_COPY_AND_ASSIGN(ObDirectReceive);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_ */
//// end of header file
