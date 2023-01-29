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
private:
  stmt::StmtType dml_type_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_LINK_DML_H