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

#ifndef OCEANBASE_SQL_OB_LOG_LINK_H
#define OCEANBASE_SQL_OB_LOG_LINK_H

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {

typedef common::ObIArray<common::ObString> ObStringIArray;

class ObLogLink : public ObLogicalOperator {
public:
  ObLogLink(ObLogPlan& plan);
  virtual ~ObLogLink()
  {}
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

  int gen_link_stmt_fmt();
  int assign_param_infos(const common::ObIArray<ObParamPosIdx>& param_infos);
  int assign_stmt_fmt(const char* stmt_fmt_buf, int32_t stmt_fmt_len);
  inline const common::ObIArray<ObParamPosIdx>& get_param_infos() const
  {
    return param_infos_;
  }
  inline const char* get_stmt_fmt_buf() const
  {
    return stmt_fmt_buf_;
  }
  inline int32_t get_stmt_fmt_len() const
  {
    return stmt_fmt_len_;
  }

private:
  int gen_link_stmt_fmt_buf();
  int gen_link_stmt_param_infos();

private:
  common::ObIAllocator& allocator_;
  ObLinkStmt link_stmt_;
  char* stmt_fmt_buf_;
  int32_t stmt_fmt_buf_len_;
  int32_t stmt_fmt_len_;
  common::ObSEArray<ObParamPosIdx, 16> param_infos_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_LINK_H
