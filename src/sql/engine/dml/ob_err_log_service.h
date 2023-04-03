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

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_ERR_LOG_SERVICE_H_
#define DEV_SRC_SQL_ENGINE_DML_OB_ERR_LOG_SERVICE_H_
#include "sql/engine/dml/ob_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{

enum ObErrLogType
{
  OB_ERR_LOG_INSERT = 0,
  OB_ERR_LOG_UPDATE,
  OB_ERR_LOG_DELETE,
};

class ObTableModifyOp;
class ObErrLogService
{
public:
  ObErrLogService(ObEvalCtx &eval_ctx):
    eval_ctx_(eval_ctx)
  {
  }
  ~ObErrLogService() {};

  int gen_insert_sql_str(ObIAllocator &alloc,
                         int first_err_ret,
                         const ObErrLogCtDef &err_log_ct_def,
                         ObString &dynamic_column_name,
                         ObString &dynamic_column_value,
                         char *&sql_str,
                         ObDASOpType type);
  int catch_err_and_gen_sql(ObIAllocator &alloc,
                            const ObSQLSessionInfo *session,
                            ObString &dynamic_column_name,
                            ObString &dynamic_column_value,
                            const ObErrLogCtDef &err_log_ct_def);
  const ObObjPrintParams get_obj_print_params(const ObSQLSessionInfo *session) { return CREATE_OBJ_PRINT_PARAM(session); }
  int insert_err_log_record(const ObSQLSessionInfo *session,
                            const ObErrLogCtDef &err_log_ct_def,
                            ObErrLogRtDef &err_log_rt_def,
                            ObDASOpType type);
  int execute_write(uint64_t tenant_id, char *sql_str);

private:
  ObEvalCtx &eval_ctx_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_ */
