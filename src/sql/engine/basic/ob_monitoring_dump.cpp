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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_monitoring_dump.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {

OB_SERIALIZE_MEMBER((ObMonitoringDump, ObSingleChildPhyOperator), flags_, dst_op_id_);

class ObMonitoringDump::ObMonitoringDumpCtx : public ObPhyOperatorCtx {
public:
  explicit ObMonitoringDumpCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx),
        op_name_(),
        tracefile_identifier_(),
        open_time_(0),
        close_time_(0),
        rows_(0),
        first_row_time_(0),
        last_row_time_(0),
        first_row_fetched_(false)
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }
  ObString op_name_;
  ObString tracefile_identifier_;
  uint64_t open_time_;
  uint64_t close_time_;
  uint64_t rows_;
  uint64_t first_row_time_;
  uint64_t last_row_time_;
  bool first_row_fetched_;

private:
  friend class ObMonitoringDump;
};

int ObMonitoringDump::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  ObSQLSessionInfo* my_session = nullptr;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMonitoringDumpCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("Create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx) || OB_ISNULL(child_op_) || OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Op_ctx or child_op_ is null", K(ret));
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret), "#columns", get_column_count());
  } else {
    ObObj val;
    ObMonitoringDumpCtx* md_ctx = static_cast<ObMonitoringDumpCtx*>(op_ctx);
    const char* name = get_phy_op_name(child_op_->get_type());
    md_ctx->op_name_.assign_ptr(name, strlen(name));
    if (OB_FAIL(my_session->get_sys_variable(SYS_VAR_TRACEFILE_IDENTIFIER, val))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (OB_FAIL(val.get_string(md_ctx->tracefile_identifier_))) {
      LOG_WARN("Failed to get tracefile identifier", K(ret));
    } else {
      md_ctx->open_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObMonitoringDump::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("Failed to init operator context", K(ret));
  }
  return ret;
}

int ObMonitoringDump::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMonitoringDumpCtx* md_ctx = nullptr;
  if (OB_ISNULL(md_ctx = GET_PHY_OPERATOR_CTX(ObMonitoringDumpCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (flags_ & OB_MONITOR_STAT) {
    const ObString& OPERATOR_NAME = md_ctx->op_name_;
    const ObString& TRACEFILE_IDENTIFIER = md_ctx->tracefile_identifier_;
    uint64_t ROWS_COUNT = md_ctx->rows_;
    uint64_t CLOSE_TIME = ObTimeUtility::current_time();
    uint64_t OPEN_TIME = md_ctx->open_time_;
    uint64_t FIRST_ROW_TIME = md_ctx->first_row_time_;
    uint64_t LAST_ROW_TIME = md_ctx->last_row_time_;
    const uint64_t OP_ID = dst_op_id_;
    if (TRACEFILE_IDENTIFIER.length() > 0) {
      LOG_INFO("",
          K(TRACEFILE_IDENTIFIER),
          K(OPERATOR_NAME),
          K(ROWS_COUNT),
          K(OPEN_TIME),
          K(CLOSE_TIME),
          K(FIRST_ROW_TIME),
          K(LAST_ROW_TIME),
          K(OP_ID));
    } else {
      LOG_INFO("",
          K(OPERATOR_NAME),
          K(ROWS_COUNT),
          K(OPEN_TIME),
          K(CLOSE_TIME),
          K(FIRST_ROW_TIME),
          K(LAST_ROW_TIME),
          K(OP_ID));
    }
  }
  return ret;
}

int ObMonitoringDump::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMonitoringDumpCtx* md_ctx = nullptr;
  const uint64_t OP_ID = dst_op_id_;
  const common::ObNewRow* tmp_row;
  if (OB_ISNULL(md_ctx = GET_PHY_OPERATOR_CTX(ObMonitoringDumpCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, tmp_row))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("OB_ITER_END");
      md_ctx->last_row_time_ = ObTimeUtility::current_time();
    } else {
      LOG_WARN("Failed to get next row", K(ret));
    }
  } else {
    md_ctx->cur_row_.assign(tmp_row->cells_, tmp_row->count_);
    row = &md_ctx->cur_row_;
    const ObString& OPERATOR_NAME = md_ctx->op_name_;
    const ObString& TRACEFILE_IDENTIFIER = md_ctx->tracefile_identifier_;
    md_ctx->rows_++;
    if (flags_ & OB_MONITOR_TRACING) {
      if (TRACEFILE_IDENTIFIER.length() > 0) {
        LOG_INFO("", K(TRACEFILE_IDENTIFIER), K(OPERATOR_NAME), KPC(row), K(OP_ID));
      } else {
        LOG_INFO("", K(OPERATOR_NAME), KPC(row), K(OP_ID));
      }
    }
    if (!md_ctx->first_row_fetched_) {
      md_ctx->first_row_fetched_ = true;
      md_ctx->first_row_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int64_t ObMonitoringDump::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  UNUSED(buf);
  UNUSED(buf_len);
  return pos;
}

}  // namespace sql
}  // namespace oceanbase
