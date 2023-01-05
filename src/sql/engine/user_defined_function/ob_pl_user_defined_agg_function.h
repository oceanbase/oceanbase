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

#ifndef OB_PL_USER_DEFINED_AGG_FUNCTION_H
#define OB_PL_USER_DEFINED_AGG_FUNCTION_H

#include "common/object/ob_object.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_udf.h"
#include "share/schema/ob_routine_info.h"

namespace oceanbase
{
namespace sql
{

class ObPlAggUdfFunction
{
  public:
  ObPlAggUdfFunction()
   : session_info_(NULL),
     exec_ctx_(NULL),
     allocator_(NULL),
     type_id_(0),
     params_type_(),
     result_type_()
    {
    }

  int init(ObSQLSessionInfo *session_info,
           ObIAllocator *allocator,
           ObExecContext *exec_ctx,
           uint64_t type_id,
           const common::ObIArray<ObExprResType> &params_type,
           const ObExprResType result_type,
           ObObjParam &pl_obj);

  int call_pl_engine_exectue_udf(ParamStore& udf_params,
                                 const share::schema::ObRoutineInfo *routine_info,
                                 ObObj &result);

  int build_in_params_store(ObObjParam &pl_obj,
                            bool is_out_param,
                            const ObObj *obj_params,
                            int64_t param_num,
                            ObIArray<ObUDFParamDesc> &params_desc,
                            ObIArray<ObExprResType> &params_type,
                            ParamStore *&udf_params);

  int process_init_pl_agg_udf(ObObjParam &pl_obj);

  int process_calc_pl_agg_udf(ObObjParam &pl_obj,
                              const ObObj *objs_stack,
                              int64_t param_num);

  int process_merge_pl_agg_udf(ObObjParam &pl_obj,
                               ObObjParam &pl_obj2);

  int process_get_pl_agg_udf_result(ObObjParam &pl_obj,
                                    ObObj &result);

  int get_package_udf_id(const ObString &routine_name,
                         const share::schema::ObRoutineInfo *&routine_info);

  int pick_routine(ObSEArray<const ObIRoutineInfo *, 4> &routine_infos,
                    const ObIRoutineInfo *&routine_info,
                    ObIArray<ObExprResType> &param_type);

  int get_package_routine_info(const ObString &routine_name,
                               const share::schema::ObRoutineInfo *&routine_info,
                               ObIArray<ObExprResType> &param_type);

  int check_types(const ObObj *obj_params,
                  int64_t param_num,
                  common::ObIArray<ObExprResType> &params_type);

  int process_obj_params(ObObj *obj_params,
                         int64_t param_num);

  int check_params_validty(const ObObj *obj_params,
                           int64_t param_num,
                           bool &is_null_params);

  TO_STRING_KV(K_(type_id),
               K_(params_type),
               K_(result_type));

  private:
  ObSQLSessionInfo *session_info_;
  ObExecContext *exec_ctx_;
  ObIAllocator *allocator_;
  uint64_t type_id_;
  common::ObSEArray<ObExprResType, 5> params_type_;
  ObExprResType result_type_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /*#endif OB_PL_USER_DEFINED_AGG_FUNCTION_H */