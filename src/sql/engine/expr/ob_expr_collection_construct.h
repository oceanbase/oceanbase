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

#ifndef OCEANBASE_SQL_OB_EXPR_COLLECTION_CONSTRUCT_H_
#define OCEANBASE_SQL_OB_EXPR_COLLECTION_CONSTRUCT_H_

#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace sql
{

class ObExprCollectionConstruct : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprCollectionConstruct(common::ObIAllocator &alloc);
  virtual ~ObExprCollectionConstruct();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual inline void reset() {
    elem_type_.reset();
    ObFuncExprOperator::reset();
  }

  inline void set_type(pl::ObPLType type) { type_ = type; }
  inline void set_not_null(bool not_null) { not_null_ = not_null; }
  inline void set_elem_type(const common::ObDataType &elem_type) { elem_type_ = elem_type; }
  inline void set_capacity(int64_t capacity) { capacity_ = capacity; }
  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_collection_construct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int check_match(const ObObj &element_obj, pl::ObElemDesc &desc, pl::ObPLINS &ns);

  struct ExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
        : ObIExprExtraInfo(alloc, type),
        type_(pl::ObPLType::PL_INVALID_TYPE),
        not_null_(false),
        elem_type_(),
        capacity_(common::OB_INVALID_SIZE),
        udt_id_(common::OB_INVALID_ID)
    {
    }
    virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

    pl::ObPLType type_;
    bool not_null_;
    common::ObDataType elem_type_;
    int64_t capacity_;
    uint64_t udt_id_;
  };

private:
  pl::ObPLType type_;
  bool not_null_;
  common::ObDataType elem_type_;
  int64_t capacity_;
  uint64_t udt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCollectionConstruct);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_USER_DEFINED_FUNC_H_
