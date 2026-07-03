/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_AI_FUNC_H_
#define OB_AI_FUNC_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_json_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf_utils.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/ai_service/ob_ai_func_provider.h"

namespace oceanbase
{
namespace common
{

struct ObAIFuncExprInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAIFuncExprInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        name_(), type_(), model_()
  {
  }
  virtual ~ObAIFuncExprInfo() {}
  void reset()
  {
    name_.reset();
    type_ = share::EndpointType::MAX_TYPE;
    model_.reset();
  }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
  int init(ObIAllocator &allocator, const ObString &model_id, share::schema::ObSchemaGetterGuard &schema_guard);
  common::ObString name_;
  share::EndpointType::TYPE type_;
  common::ObString model_;
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_H_ */