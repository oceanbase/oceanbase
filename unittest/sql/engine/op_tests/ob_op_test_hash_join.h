/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_HASH_JOIN_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_HASH_JOIN_H_

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX SQL
#include "sql/engine/join/ob_join_vec_op.h"
#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/join/ob_hash_join_op.h"
#include "unittest/sql/engine/op_tests/ob_op_test_join_base.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

namespace oceanbase
{
namespace sql
{

class HashJoinTestSpec : public JoinSpecBuilder<HashJoinTestSpec>
{
public:
  HashJoinTestSpec() : is_naaj_(false), is_sna_(false) {}
  ~HashJoinTestSpec() = default;

  HashJoinTestSpec& enable_naaj(bool enable = true)
  {
    is_naaj_ = enable;
    return *this;
  }

  HashJoinTestSpec& enable_sna(bool enable = true)
  {
    is_sna_ = enable;
    return *this;
  }

  ObOpSpec *create_spec(common::ObIAllocator &alloc,
                        MockDataSourceSpec *left_mock_spec,
                        MockDataSourceSpec *right_mock_spec,
                        const ExprFixedArray &output_exprs,
                        bool use_rich_format)
  {
    if (use_rich_format) {
      return create_spec_impl<ObHashJoinVecSpec, ObHashJoinVecOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_VEC_HASH_JOIN);
    } else {
      return create_spec_impl<ObHashJoinSpec, ObHashJoinOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_HASH_JOIN);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec,
                        ObOperator *left_op, ObOperator *right_op)
  {
    if (spec.type_ == PHY_VEC_HASH_JOIN) {
      return default_create_join_op<ObHashJoinVecOp>(ctx, spec, left_op, right_op);
    } else {
      return default_create_join_op<ObHashJoinOp>(ctx, spec, left_op, right_op);
    }
  }

  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    int ret = OB_SUCCESS;
    if (spec.type_ == PHY_VEC_HASH_JOIN) {
      void *mem = ctx.get_allocator().alloc(sizeof(ObHashJoinVecInput));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashJoinVecInput failed", K(ret));
        return nullptr;
      }
      return new (mem) ObHashJoinVecInput(ctx, spec);
    } else {
      void *mem = ctx.get_allocator().alloc(sizeof(ObHashJoinInput));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashJoinInput failed", K(ret));
        return nullptr;
      }
      return new (mem) ObHashJoinInput(ctx, spec);
    }
  }

private:
  bool is_naaj_;
  bool is_sna_;

  // ===== SFINAE helpers for 2.0-only fields =====
  // ObHashJoinVecSpec (2.0) has: build_keys_, probe_keys_, build_key_proj_,
  //   probe_key_proj_, build_rows_output_, join_conds_, equal_join_conds_, all_join_keys_
  // ObHashJoinSpec (1.0) has: equal_join_conds_, all_join_keys_, is_ns_equal_cond_
  //   but NOT: build_keys_, probe_keys_, build_key_proj_, probe_key_proj_,
  //   build_rows_output_, join_conds_

  template <typename T>
  auto set_build_rows_output(T *spec, const ExprFixedArray &right_out, int)
    -> decltype(spec->build_rows_output_.assign(right_out), void())
  {
    spec->build_rows_output_.assign(right_out);
  }
  template <typename T>
  void set_build_rows_output(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto set_all_join_keys(T *spec, const ExprFixedArray &keys, int)
    -> decltype(spec->all_join_keys_.assign(keys), void())
  {
    spec->all_join_keys_.assign(keys);
  }
  template <typename T>
  void set_all_join_keys(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto set_equal_join_conds(T *spec, const ExprFixedArray &conds, int)
    -> decltype(spec->equal_join_conds_.assign(conds), void())
  {
    spec->equal_join_conds_.assign(conds);
  }
  template <typename T>
  void set_equal_join_conds(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto set_join_conds(T *spec, const ExprFixedArray &conds, int)
    -> decltype(spec->join_conds_.assign(conds), void())
  {
    spec->join_conds_.assign(conds);
  }
  template <typename T>
  void set_join_conds(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto set_build_keys(T *spec, const ExprFixedArray &keys, int)
    -> decltype(spec->build_keys_.assign(keys), void())
  {
    spec->build_keys_.assign(keys);
  }
  template <typename T>
  void set_build_keys(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto set_probe_keys(T *spec, const ExprFixedArray &keys, int)
    -> decltype(spec->probe_keys_.assign(keys), void())
  {
    spec->probe_keys_.assign(keys);
  }
  template <typename T>
  void set_probe_keys(T *, const ExprFixedArray &, long) {}

  template <typename T>
  auto init_build_key_proj(T *spec, int64_t cnt, int)
    -> decltype(spec->build_key_proj_.init(cnt), int())
  {
    return spec->build_key_proj_.init(cnt);
  }
  template <typename T>
  int init_build_key_proj(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto push_build_key_proj(T *spec, int64_t idx, int)
    -> decltype(spec->build_key_proj_.push_back(idx), int())
  {
    return spec->build_key_proj_.push_back(idx);
  }
  template <typename T>
  int push_build_key_proj(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto init_probe_key_proj(T *spec, int64_t cnt, int)
    -> decltype(spec->probe_key_proj_.init(cnt), int())
  {
    return spec->probe_key_proj_.init(cnt);
  }
  template <typename T>
  int init_probe_key_proj(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto push_probe_key_proj(T *spec, int64_t idx, int)
    -> decltype(spec->probe_key_proj_.push_back(idx), int())
  {
    return spec->probe_key_proj_.push_back(idx);
  }
  template <typename T>
  int push_probe_key_proj(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto init_build_rows_output(T *spec, int64_t cnt, int)
    -> decltype(spec->build_rows_output_.init(cnt), int())
  {
    return spec->build_rows_output_.init(cnt);
  }
  template <typename T>
  int init_build_rows_output(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto push_build_rows_output(T *spec, ObExpr *expr, int)
    -> decltype(spec->build_rows_output_.push_back(expr), int())
  {
    return spec->build_rows_output_.push_back(expr);
  }
  template <typename T>
  int push_build_rows_output(T *, ObExpr *, long) { return OB_SUCCESS; }

  // ===== SFINAE helpers for all_hash_funcs_ (1.0 only) =====
  // ObHashJoinSpec (1.0) has all_hash_funcs_, ObHashJoinVecSpec (2.0) does not
  template <typename T>
  auto init_all_hash_funcs(T *spec, int64_t cnt, int)
    -> decltype(spec->all_hash_funcs_.init(cnt), int())
  {
    return spec->all_hash_funcs_.init(cnt);
  }
  template <typename T>
  int init_all_hash_funcs(T *, int64_t, long) { return OB_SUCCESS; }

  template <typename T>
  auto push_all_hash_funcs(T *spec, const common::ObHashFunc &func, int)
    -> decltype(spec->all_hash_funcs_.push_back(func), int())
  {
    return spec->all_hash_funcs_.push_back(func);
  }
  template <typename T>
  int push_all_hash_funcs(T *, const common::ObHashFunc &, long) { return OB_SUCCESS; }

  template <typename T>
  auto append_all_hash_funcs(T *spec, const common::ObIArray<common::ObHashFunc> &funcs, int)
    -> decltype(append(spec->all_hash_funcs_, funcs), int())
  {
    return append(spec->all_hash_funcs_, funcs);
  }
  template <typename T>
  int append_all_hash_funcs(T *, const common::ObIArray<common::ObHashFunc> &, long) { return OB_SUCCESS; }

  ObExpr *find_rt_expr(ObRawExpr *raw)
  {
    if (OB_ISNULL(raw)) return nullptr;
    return ObStaticEngineExprCG::get_rt_expr(*raw);
  }

  bool is_expr_from_table(ObExpr *expr, uint64_t table_id)
  {
    if (OB_ISNULL(expr) || OB_ISNULL(resolved_stmt_)) {
      return false;
    }
    if (T_REF_COLUMN == expr->type_) {
      const common::ObIArray<ColumnItem> &col_items = resolved_stmt_->get_column_items();
      for (int64_t i = 0; i < col_items.count(); ++i) {
        const ColumnItem &col = col_items.at(i);
        if (OB_NOT_NULL(col.expr_)) {
          ObExpr *col_expr = ObStaticEngineExprCG::get_rt_expr(*col.expr_);
          if (col_expr == expr) {
            return col.table_id_ == table_id;
          }
        }
      }
    }
    for (uint32_t i = 0; i < expr->arg_cnt_ && OB_NOT_NULL(expr->args_); ++i) {
      if (is_expr_from_table(expr->args_[i], table_id)) {
        return true;
      }
    }
    return false;
  }

  static bool has_exist_in_array(const ExprFixedArray &arr, ObExpr *expr, int64_t *idx = nullptr)
  {
    for (int64_t i = 0; i < arr.count(); ++i) {
      if (arr.at(i) == expr) {
        if (idx != nullptr) *idx = i;
        return true;
      }
    }
    return false;
  }

  template <typename SpecType, typename OpType>
  SpecType *create_spec_impl(common::ObIAllocator &alloc,
                             MockDataSourceSpec *left_mock_spec,
                             MockDataSourceSpec *right_mock_spec,
                             const ExprFixedArray &output_exprs,
                             bool use_rich_format,
                             ObPhyOperatorType op_type)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc hash join spec failed", K(ret));
      return nullptr;
    }
    SpecType *hj_spec = new (mem) SpecType(alloc, op_type);

    hj_spec->plan_ = left_mock_spec->plan_;
    hj_spec->max_batch_size_ = left_mock_spec->max_batch_size_;
    hj_spec->use_rich_format_ = use_rich_format;
    hj_spec->output_ = output_exprs;
    hj_spec->join_type_ = join_type_;
    hj_spec->is_naaj_ = is_naaj_;
    hj_spec->is_sna_ = is_sna_;
    hj_spec->is_shared_ht_ = false;
    hj_spec->can_prob_opt_ = false;

    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *) * 2);
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = left_mock_spec;
    children[1] = right_mock_spec;
    if (OB_FAIL(hj_spec->set_children_pointer(children, 2))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Extract equal conditions from ON clause
    common::ObSEArray<ObRawExpr *, 8> equal_join_conds;
    const common::ObIArray<JoinedTable *> &joined_tables = resolved_stmt_->get_joined_tables();
    for (int64_t i = 0; i < joined_tables.count(); ++i) {
      JoinedTable *jt = joined_tables.at(i);
      if (OB_NOT_NULL(jt)) {
        const common::ObIArray<ObRawExpr *> &join_conds = jt->get_join_conditions();
        for (int64_t j = 0; j < join_conds.count(); ++j) {
          ObRawExpr *raw_expr = join_conds.at(j);
          if (OB_NOT_NULL(raw_expr)) {
            ObItemType expr_type = raw_expr->get_expr_type();
            if (T_OP_EQ == expr_type || T_OP_NSEQ == expr_type) {
              if (OB_FAIL(equal_join_conds.push_back(raw_expr))) {
                LOG_WARN("push equal join cond failed", K(ret));
                return nullptr;
              }
            }
          }
        }
      }
    }

    int64_t eq_cnt = equal_join_conds.count();

    // Initialize is_ns_equal_cond_ (exists in both 1.0 and 2.0)
    if (OB_SUCC(ret) && eq_cnt > 0) {
      if (OB_FAIL(hj_spec->is_ns_equal_cond_.init(eq_cnt))) {
        LOG_WARN("init is_ns_equal_cond failed", K(ret));
        return nullptr;
      }
    }

    // Initialize 2.0-only fields via SFINAE
    if (OB_SUCC(ret) && eq_cnt > 0) {
      if (OB_FAIL(init_build_key_proj(hj_spec, eq_cnt, 0))) {
        LOG_WARN("init build_key_proj failed", K(ret));
        return nullptr;
      }
      if (OB_FAIL(init_probe_key_proj(hj_spec, eq_cnt, 0))) {
        LOG_WARN("init probe_key_proj failed", K(ret));
        return nullptr;
      }
    }

    // Initialize build_rows_output_ with left child output (2.0 only, via SFINAE)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_build_rows_output(hj_spec, left_mock_spec->output_.count() + eq_cnt, 0))) {
        LOG_WARN("init build_rows_output failed", K(ret));
        return nullptr;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < left_mock_spec->output_.count(); ++i) {
        if (OB_FAIL(push_build_rows_output(hj_spec, left_mock_spec->output_.at(i), 0))) {
          LOG_WARN("push left output to build_rows_output failed", K(ret), K(i));
          return nullptr;
        }
      }
    }

    // Build keys and probe keys (local arrays for both 1.0 and 2.0)
    ExprFixedArray build_keys(alloc);  // from left table (build side)
    ExprFixedArray probe_keys(alloc);  // from right table (probe side)
    if (eq_cnt > 0) {
      if (OB_FAIL(build_keys.init(eq_cnt)) || OB_FAIL(probe_keys.init(eq_cnt))) {
        LOG_WARN("init keys failed", K(ret));
        return nullptr;
      }
      if (OB_FAIL(build_keys.prepare_allocate(eq_cnt)) || OB_FAIL(probe_keys.prepare_allocate(eq_cnt))) {
        LOG_WARN("prepare_allocate keys failed", K(ret));
        return nullptr;
      }
    }

    int64_t build_key_not_in_output_idx = left_mock_spec->output_.count();
    int64_t probe_key_not_in_output_idx = right_mock_spec->output_.count();

    for (int64_t i = 0; OB_SUCC(ret) && i < eq_cnt; ++i) {
      ObRawExpr *raw_expr = equal_join_conds.at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw_expr is null", K(ret));
        break;
      }

      ObExpr *rt_expr = find_rt_expr(raw_expr);
      if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_rt_expr returned null", K(ret));
        break;
      }

      if (rt_expr->arg_cnt_ != 2 || OB_ISNULL(rt_expr->args_) ||
          OB_ISNULL(rt_expr->args_[0]) || OB_ISNULL(rt_expr->args_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid equal condition expr", K(ret));
        break;
      }

      // Determine which arg comes from which table
      bool is_left_arg_from_left = is_expr_from_table(rt_expr->args_[0], left_resolved_table_id_);
      bool is_right_arg_from_right = is_expr_from_table(rt_expr->args_[1], right_resolved_table_id_);

      // In OceanBase: build_keys = left side (from left table), probe_keys = right side (from right table)
      ObExpr *build_rt = nullptr;  // from left table (build side)
      ObExpr *probe_rt = nullptr;  // from right table (probe side)
      if (is_left_arg_from_left && is_right_arg_from_right) {
        build_rt = rt_expr->args_[0];
        probe_rt = rt_expr->args_[1];
      } else {
        build_rt = rt_expr->args_[1];
        probe_rt = rt_expr->args_[0];
      }

      build_keys.at(i) = build_rt;
      probe_keys.at(i) = probe_rt;

      // Set build_key_proj_ and probe_key_proj_ (2.0 only, via SFINAE)
      int64_t idx = 0;
      if (has_exist_in_array(left_mock_spec->output_, build_rt, &idx)) {
        if (OB_FAIL(push_build_key_proj(hj_spec, idx, 0))) {
          LOG_WARN("push build_key_proj failed", K(ret), K(i));
          break;
        }
      } else {
        if (OB_FAIL(push_build_rows_output(hj_spec, build_rt, 0))) {
          LOG_WARN("push build key to build_rows_output failed", K(ret), K(i));
          break;
        }
        if (OB_FAIL(push_build_key_proj(hj_spec, build_key_not_in_output_idx++, 0))) {
          LOG_WARN("push build_key_proj failed", K(ret), K(i));
          break;
        }
      }

      if (has_exist_in_array(right_mock_spec->output_, probe_rt, &idx)) {
        if (OB_FAIL(push_probe_key_proj(hj_spec, idx, 0))) {
          LOG_WARN("push probe_key_proj failed", K(ret), K(i));
          break;
        }
      } else {
        if (OB_FAIL(push_probe_key_proj(hj_spec, probe_key_not_in_output_idx++, 0))) {
          LOG_WARN("push probe_key_proj failed", K(ret), K(i));
          break;
        }
      }

      // Set is_ns_equal_cond_ (exists in both 1.0 and 2.0)
      bool is_ns = (raw_expr->get_expr_type() == T_OP_NSEQ);
      if (OB_FAIL(hj_spec->is_ns_equal_cond_.push_back(is_ns))) {
        LOG_WARN("push is_ns_equal_cond failed", K(ret), K(i));
        break;
      }
    }

    // Set all_join_keys_ (exists in both 1.0 and 2.0)
    if (OB_SUCC(ret) && eq_cnt > 0) {
      ExprFixedArray all_keys(alloc);
      if (OB_FAIL(all_keys.init(eq_cnt * 2)) || OB_FAIL(all_keys.prepare_allocate(eq_cnt * 2))) {
        LOG_WARN("init all_keys failed", K(ret));
        return nullptr;
      }
      for (int64_t i = 0; i < eq_cnt; ++i) {
        all_keys.at(i) = build_keys.at(i);
      }
      for (int64_t i = 0; i < eq_cnt; ++i) {
        all_keys.at(eq_cnt + i) = probe_keys.at(i);
      }
      set_all_join_keys(hj_spec, all_keys, 0);
    }

    // Set all_hash_funcs_ (1.0 only, via SFINAE)
    // Production CG: init(2*eq_cnt), push left hash funcs, then append right hash funcs
    if (OB_SUCC(ret) && eq_cnt > 0) {
      if (OB_FAIL(init_all_hash_funcs(hj_spec, 2 * eq_cnt, 0))) {
        LOG_WARN("init all_hash_funcs failed", K(ret));
        return nullptr;
      }
      common::ObSEArray<common::ObHashFunc, 8> right_hash_funcs;
      for (int64_t i = 0; OB_SUCC(ret) && i < eq_cnt; ++i) {
        ObExpr *build_rt = build_keys.at(i);
        ObExpr *probe_rt = probe_keys.at(i);
        common::ObHashFunc left_hash_func;
        common::ObHashFunc right_hash_func;
        if (OB_ISNULL(build_rt) || OB_ISNULL(probe_rt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build or probe key is null", K(ret), K(i));
          break;
        }
        // Use murmur_hash_v2 (cluster version >= 4.1.0.0)
        left_hash_func.hash_func_ = build_rt->basic_funcs_->murmur_hash_v2_;
        right_hash_func.hash_func_ = probe_rt->basic_funcs_->murmur_hash_v2_;
        if (OB_ISNULL(left_hash_func.hash_func_) || OB_ISNULL(right_hash_func.hash_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hash func is null, check datatype", K(ret), K(i));
          break;
        }
        if (OB_FAIL(push_all_hash_funcs(hj_spec, left_hash_func, 0))) {
          LOG_WARN("push left hash func failed", K(ret), K(i));
          break;
        }
        if (OB_FAIL(right_hash_funcs.push_back(right_hash_func))) {
          LOG_WARN("push right hash func failed", K(ret), K(i));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append_all_hash_funcs(hj_spec, right_hash_funcs, 0))) {
          LOG_WARN("append right hash funcs failed", K(ret));
          return nullptr;
        }
      }
    }

    // Set equal_join_conds_ (exists in both 1.0 and 2.0)
    if (OB_SUCC(ret) && eq_cnt > 0) {
      ExprFixedArray eq_conds_arr(alloc);
      if (OB_FAIL(eq_conds_arr.init(eq_cnt)) || OB_FAIL(eq_conds_arr.prepare_allocate(eq_cnt))) {
        LOG_WARN("init eq_conds failed", K(ret));
        return nullptr;
      }
      for (int64_t i = 0; i < eq_cnt; ++i) {
        ObExpr *rt = find_rt_expr(equal_join_conds.at(i));
        if (OB_ISNULL(rt)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        eq_conds_arr.at(i) = rt;
      }
      if (OB_SUCC(ret)) {
        set_equal_join_conds(hj_spec, eq_conds_arr, 0);
        // join_conds_ is 2.0 only (via SFINAE)
        set_join_conds(hj_spec, eq_conds_arr, 0);
      }
    }

    // Set build_keys_ and probe_keys_ (2.0 only, via SFINAE)
    if (OB_SUCC(ret) && eq_cnt > 0) {
      set_build_keys(hj_spec, build_keys, 0);
      set_probe_keys(hj_spec, probe_keys, 0);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("create hash join spec failed", K(ret));
      return nullptr;
    }
    return hj_spec;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_HASH_JOIN_H_
