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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_EXPR_VALUES_OP_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_EXPR_VALUES_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/dml/ob_err_log_service.h"

namespace oceanbase
{
namespace sql
{
class ObPhyOpSeriCtx;
class ObErrLogService;

class ObExprValuesSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObExprValuesSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      values_(alloc),
      column_names_(alloc),
      is_strict_json_desc_(alloc),
      str_values_array_(alloc),
      err_log_ct_def_(alloc),
      contain_ab_param_(0),
      ins_values_batch_opt_(false),
      array_group_idx_(-1)
  { }
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K(array_group_idx_));
  int64_t get_value_count() const { return values_.count(); }
  int64_t get_is_strict_json_desc_count() const { return is_strict_json_desc_.count(); }
  virtual int serialize(char *buf,
                        int64_t buf_len,
                        int64_t &pos,
                        ObPhyOpSeriCtx &seri_ctx) const override;
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx &seri_ctx) const override;
  int64_t get_serialize_size_(const ObPhyOpSeriCtx &seri_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValuesSpec);
public:
  common::ObFixedArray<ObExpr *, common::ObIAllocator> values_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
  common::ObFixedArray<bool, common::ObIAllocator> is_strict_json_desc_;
  common::ObFixedArray<ObStrValues, common::ObIAllocator> str_values_array_;
  ObErrLogCtDef err_log_ct_def_;
  int64_t contain_ab_param_;
  bool ins_values_batch_opt_;
  int64_t array_group_idx_;
};

class ObExprValuesOp : public ObOperator
{
public:
  ObExprValuesOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_rescan() override;

  virtual int switch_iterator() override;

  virtual int inner_get_next_row() override;

  virtual int inner_close() override;

  virtual void destroy() override { ObOperator::destroy(); }
private:
  int calc_next_row();
  void update_src_meta(ObDatumMeta &src_meta, const ObObjMeta &src_obj_meta, const ObAccuracy &src_obj_acc);
  int get_real_batch_obj_type(ObDatumMeta &src_meta,
                              ObObjMeta &src_obj_meta,
                              ObExpr *src_expr,
                              int64_t group_idx);
  int eval_values_op_dynamic_cast_to_lob(ObExpr &real_src_expr,
                                         ObObjMeta &src_obj_meta,
                                         ObExpr *dst_expr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprValuesOp);

private:
  int64_t node_idx_;
  int64_t vector_index_;
  ObDatumCaster datum_caster_;
  common::ObCastMode cm_;
  ObErrLogService err_log_service_;
  ObErrLogRtDef err_log_rt_def_;
  bool has_sequence_;
  int64_t real_value_cnt_;
  int64_t param_idx_;
  int64_t param_cnt_;
};

} // end namespace sql
} // end namespace oceanbase
#endif
