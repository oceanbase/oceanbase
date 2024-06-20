/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_LOG_LINK_DML_H
#define OCEANBASE_SQL_OB_LOG_LINK_DML_H

#include "sql/optimizer/ob_log_link.h"
namespace oceanbase
{
namespace sql
{

typedef common::ObIArray<common::ObString> ObStringIArray;

class ObLogLinkDml : public ObLogLink
{
public:
  ObLogLinkDml(ObLogPlan &plan);
  virtual ~ObLogLinkDml() {}
  virtual int compute_op_ordering() override;
  virtual int get_explain_name_internal(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  inline void set_dml_type(stmt::StmtType type) { dml_type_ = type; }
  inline ObIArray<int64_t> &get_related_dblink_ids() { return related_dblink_ids_; }
private:
  virtual bool print_flashback_query() const override { return false; };
private:
  stmt::StmtType dml_type_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> related_dblink_ids_; // all dblinks related in this link dml sql
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_LINK_DML_H