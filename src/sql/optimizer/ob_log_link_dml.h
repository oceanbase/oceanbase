/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  stmt::StmtType dml_type_;
  ObSqlArray<int64_t> related_dblink_ids_; // all dblinks related in this link dml sql
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_LINK_DML_H