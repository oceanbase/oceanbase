#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_link_dml.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

ObLogLinkDml::ObLogLinkDml(ObLogPlan &plan)
  : ObLogLink(plan),
    dml_type_(stmt::StmtType::T_SELECT)
{}

int ObLogLinkDml::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  get_op_ordering().reset();
  is_local_order_ = false;
  return ret;
}

int ObLogLinkDml::get_explain_name_internal(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  switch (dml_type_) {
      case stmt::StmtType::T_INSERT:
        ret = BUF_PRINTF("%s", "LINK INSERT");
        break;
      case stmt::StmtType::T_UPDATE:
        ret = BUF_PRINTF("%s", "LINK UPDATE");
        break;
      case stmt::StmtType::T_DELETE:
        ret = BUF_PRINTF("%s", "LINK DELETE");
        break;
      case stmt::StmtType::T_MERGE:
        ret = BUF_PRINTF("%s", "LINK MERGE INTO");
        break;
      default:
        ret = BUF_PRINTF("%s", get_name());
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
