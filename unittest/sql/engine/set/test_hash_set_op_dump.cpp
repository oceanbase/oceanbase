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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>

#define private public
#define protected public

#include "sql/engine/set/ob_hash_union_op.h"
#include "sql/engine/set/ob_hash_intersect_op.h"
#include "sql/engine/set/ob_hash_except_op.h"
#include "sql/engine/set/ob_merge_union_op.h"
#include "sql/engine/set/ob_merge_intersect_op.h"
#include "sql/engine/set/ob_merge_except_op.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "sql/engine/table/ob_fake_table.h"
#include "set_data_op_generator.h"
#include "sql/ob_sql_init.h"
#include "share/ob_cluster_version.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace omt;

ObExecContext exec_ctx_;
ObArenaAllocator eval_res_;
ObArenaAllocator eval_tmp_;
ObEvalCtx op_eval_ctx_ = ObEvalCtx(exec_ctx_, eval_res_ , eval_tmp_);

template <ObObjType type, typename T>
struct DatumHashCalculator
{
  static uint64_t calc_datum_hash(const ObDatum &datum, const uint64_t seed)
  {
    uint64_t res = seed;
    if (datum.is_null()) {
      int null_type = ObNullType;
      res = T::hash(&null_type, sizeof(null_type), res);
    } else {
      res = ObjHashCalculator<type, T, ObDatum>::calc_hash_value(datum, seed);
    }
    return res;
  }
};

class MockSqlExpression : public ObSqlExpression
{
public:
  MockSqlExpression(ObIAllocator &alloc): ObSqlExpression(alloc)
  {
    set_item_count(10);
  }
};

#define TEST_SET_DUMP_GET_HASH_AREA_SIZE() (get_hash_area_size())
#define TEST_SET_DUMP_SET_HASH_AREA_SIZE(size) (set_hash_area_size(size))

class ObHashSetDumpTest:
  public blocksstable::TestDataFilePrepare, public ::testing::WithParamInterface<ObJoinType>
{
public:
  typedef std::function<int64_t(const int64_t, const int64_t)> IdxCntFunc;
  enum TestAlgo
  {
    UNION = 0,
    INTERSECT,
    EXCEPT
  };
protected:
  struct SetPlan
  {
    explicit SetPlan(ObExecContext &exec_ctx, const ObOpSpec &left_spec, const ObOpSpec &right_spec, ObOpInput *input)
        : exec_ctx_(exec_ctx), set_op_(nullptr), left_(exec_ctx, left_spec, input, ObString("LEFT_OP")), right_(exec_ctx, right_spec, input, ObString("RIHGT_OP")), expr_(exec_ctx.get_allocator())
    {


    }

    int setup_plan(ObOperator *set_op);

    ObSQLSessionInfo session_;
    ObPhysicalPlan plan_;
    ObExecContext &exec_ctx_;
    ObOperator *set_op_;
    SetDataGeneratorOp left_;
    SetDataGeneratorOp right_;
    MockSqlExpression expr_;
  };
public:
  ObHashSetDumpTest()
      : blocksstable::TestDataFilePrepare("TestDiskIR", 8<<20, 5000), spec1_(exec_ctx_.get_allocator(), PHY_HASH_INTERSECT),
        spec2_(exec_ctx_.get_allocator(), PHY_MERGE_INTERSECT), spec3_(exec_ctx_.get_allocator(), PHY_HASH_EXCEPT),
        spec4_(exec_ctx_.get_allocator(), PHY_MERGE_EXCEPT), spec5_(exec_ctx_.get_allocator(), PHY_TABLE_SCAN), spec6_(exec_ctx_.get_allocator(), PHY_TABLE_SCAN),
        hash_intersect_(exec_ctx_, spec1_, nullptr), merge_intersect_(exec_ctx_, spec2_, nullptr),
        hash_except_(exec_ctx_, spec3_, nullptr), merge_except_(exec_ctx_, spec4_, nullptr),
        hash_set_op_(nullptr), merge_set_op_(nullptr),
        hash_plan_(exec_ctx_, spec5_, spec6_,  nullptr), merge_plan_(exec_ctx_, spec5_, spec6_, nullptr)

  {
  }

  int init_tenant_mgr();
  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObTmpFileManager::get_instance().init());
    CHUNK_MGR.set_limit(128L * 1024L * 1024L * 1024L);
    GCONF.enable_sql_operator_dump.set_value("True");
    uint64_t cluster_version = CLUSTER_VERSION_3000;
    common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
    EXPECT_EQ(cluster_version, common::ObClusterVersion::get_instance().get_cluster_version());
    LOG_INFO("set cluster version", K(cluster_version),
      K(common::ObClusterVersion::get_instance().get_cluster_version()));
  }
  virtual void TearDown() override
  {
    blocksstable::ObTmpFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    destroy_tenant_mgr();
  }

  void destroy_tenant_mgr()
  {
    ObTenantManager::get_instance().destroy();
  }

  int64_t get_hash_area_size()
  {
    int64_t hash_area_size = 0;
    int ret = OB_SUCCESS;
    ret = ObSqlWorkareaUtil::get_workarea_size(HASH_WORK_AREA, OB_SYS_TENANT_ID, hash_area_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get hash area size", K(ret), K(hash_area_size));
    }
    return hash_area_size;
  }

  void set_hash_area_size(int64_t size)
  {
    int ret = OB_SUCCESS;
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_hash_area_size = size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: config is invalid", K(tenant_id));
    }
    // ASSERT_EQ(OB_SUCCESS, ret);
  }

  void setup_plan(SetPlan &plan, bool hash_algo, ObHashSetDumpTest::TestAlgo algo);
  void setup_test(TestAlgo algo, int32_t string_size,
      int64_t left_row_count, bool left_reverse, IdxCntFunc left_func,
      int64_t right_row_count, bool right_reverse, IdxCntFunc right_func);

  // iterate hash join result and verify result with merge join.
  void run_test(int64_t print_row_cnt = 0);
  int generate_hash_set_spec(ObHashSetSpec &spec)
  {
    int ret = OB_SUCCESS;
    int pos = 0;

    //spec.set_exprs_.set_allocator(&exec_ctx_.get_allocator());
    //spec.sort_cmp_funs_.set_allocator(&exec_ctx_.get_allocator());
    //spec.sort_collations_.set_allocator(&exec_ctx_.get_allocator());
    //spec.hash_funcs_.set_allocator(&exec_ctx_.get_allocator());
    spec.set_exprs_.destroy();
    spec.cost_ = 1;
    spec.rows_ = 5000;
    spec.width_ = 20;
    if (OB_FAIL(spec.output_.init(3))) {
      LOG_WARN("failed to init output_", K(ret));
      return ret;
    }
    int pos1 = 512;
    for (int i = 0; i < 3 ; ++i) {
      ObExpr *expr = static_cast<ObExpr *> (alloc_.alloc(sizeof(ObExpr)));
      if (OB_FAIL(spec.output_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
        return ret;
      }
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos1;
      pos1 += sizeof(ObExprDatum);
      ObExprDatum *expr_datum = &expr->locate_expr_datum(op_eval_ctx_);
      new (expr_datum) ObExprDatum;
      pos1 += sizeof(int64_t);
    }
    if (OB_FAIL(spec.set_exprs_.init(3))) {
      LOG_WARN("failed to get output exprs", K(ret), K(spec.set_exprs_.capacity_));
      return ret;
    }
    for (int i = 0; i  < 3; ++i) {
      ObExpr *expr = static_cast<ObExpr *>(exec_ctx_.get_allocator().alloc(sizeof(ObExpr)));//新建expr， 设置expr对应的datum， 在对应位置放置datum
      if (OB_FAIL(spec.set_exprs_.push_back(expr))) {
        return ret;
      }
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      pos += sizeof(ObExprDatum);
      ObExprDatum *expr_datum = &expr->locate_expr_datum(op_eval_ctx_);
      new (expr_datum)(ObExprDatum);
      expr_datum->ptr_ = op_eval_ctx_.frames_[0] + pos;
      pos += sizeof(int64_t);//？？
    }
    if (OB_FAIL(spec.sort_collations_.init(1))) {
      LOG_WARN("failed to init sort collations", K(ret));
    } else if (OB_FAIL(spec.sort_cmp_funs_.init(spec.set_exprs_.count()))) {
      LOG_WARN("failed to compare function", K(ret));
    } else if (OB_FAIL(spec.hash_funcs_.init(spec.set_exprs_.count()))) {
      LOG_WARN("failed to compare function", K(ret));
    } else {
      // 初始化compare func和hash func
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(0,
          CS_TYPE_BINARY,
          is_ascending,
          (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back sort collation", K(ret));
      }
      for (int64_t i = 0; i < spec.set_exprs_.count() && OB_SUCC(ret); ++i) {
        ObSortCmpFunc cmp_func;
        ObObjType tmp_type;
        if (0 == i) {
          tmp_type = ObIntType;
        } else if (1 == i) {
          tmp_type = ObNullType;
        } else {
          tmp_type = ObVarcharType;
        }
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(tmp_type,
                                                                tmp_type,
                                                                field_collation.null_pos_,
                                                                field_collation.cs_type_,
                                                                SCALE_UNKNOWN_YET,
                                                                lib::is_oracle_mode(),
                                                                false);
        ObHashFunc hash_func;
        if (0 == i) {
          ObExpr *func_expr = static_cast<ObExpr *> (alloc_.alloc(sizeof(ObExpr)));
          func_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObIntType,
                                                      CS_TYPE_UTF8MB4_GENERAL_CI);
          hash_func.hash_func_ = func_expr->basic_funcs_->default_hash_;
        } else if (1 ==i) {
          ObExpr *func_expr = static_cast<ObExpr *> (alloc_.alloc(sizeof(ObExpr)));
          func_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObNullType,
                                                      CS_TYPE_UTF8MB4_GENERAL_CI);
          hash_func.hash_func_ = func_expr->basic_funcs_->default_hash_;
        } else {
          ObExpr *func_expr = static_cast<ObExpr *> (alloc_.alloc(sizeof(ObExpr)));
          func_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(ObVarcharType,
                                                      CS_TYPE_UTF8MB4_GENERAL_CI);
          hash_func.hash_func_ = func_expr->basic_funcs_->default_hash_;
        }
        if (OB_FAIL(spec.sort_cmp_funs_.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort function", K(ret));
        } else if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
          LOG_WARN("failed to push back hash funcs", K(ret));
        }
      }
    }
    return ret;
  }

  int generate_merge_set_spec(ObMergeSetSpec &spec)
  {
    int ret = OB_SUCCESS;
    int pos = 0;

    //spec.set_exprs_.set_allocator(&alloc_);
    //spec.sort_cmp_funs_.set_allocator(&alloc_);
    //spec.sort_collations_.set_allocator(&alloc_);
    spec.set_exprs_.destroy();
    spec.cost_ = 1;
    spec.rows_ = 5000;
    spec.width_ = 20;
    if (OB_FAIL(spec.output_.init(3))) {
      LOG_WARN("failed to init output_", K(ret));
      return ret;
    }
    int pos1 = 512;
    for (int i = 0; i < 3 ; ++i) {
      ObExpr *expr = static_cast<ObExpr *> (alloc_.alloc(sizeof(ObExpr)));
      if (OB_FAIL(spec.output_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
        return ret;
      }
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos1;
      pos1 += sizeof(ObExprDatum);
      ObExprDatum *expr_datum = &expr->locate_expr_datum(op_eval_ctx_);
      new (expr_datum) ObExprDatum;
      pos1 += sizeof(int64_t);
    }
    if (OB_FAIL(spec.set_exprs_.init(3))) {
      LOG_WARN("failed to get output exprs", K(ret), K(spec.set_exprs_.capacity_));
      return ret;
    }
    for (int i = 0; i  < 3; ++i) {
      ObExpr *expr = static_cast<ObExpr *>(alloc_.alloc(sizeof(ObExpr)));//新建expr， 设置expr对应的datum， 在对应位置放置datum
      if (OB_FAIL(spec.set_exprs_.push_back(expr))) {
        return ret;
      }
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      pos += sizeof(ObExprDatum);
      ObExprDatum *expr_datum = &expr->locate_expr_datum(op_eval_ctx_);
      new (expr_datum)(ObExprDatum);
      expr_datum->ptr_ = op_eval_ctx_.frames_[0] + pos;
      pos += sizeof(int64_t);//？？
    }
    if (OB_FAIL(spec.sort_collations_.init(1))) {
      LOG_WARN("failed to init sort collations", K(ret));
    } else if (OB_FAIL(spec.sort_cmp_funs_.init(1))) {
      LOG_WARN("failed to compare function", K(ret));
    } else {
      LOG_WARN("init funcs");
      // 初始化compare func和hash func
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(0,
          CS_TYPE_BINARY,
          is_ascending,
          (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back sort collation", K(ret));
      }
      ObSortCmpFunc cmp_func;
      ObObjType tmp_type = ObIntType;
      cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(tmp_type,
                                                              tmp_type,
                                                              field_collation.null_pos_,
                                                              field_collation.cs_type_,
                                                              SCALE_UNKNOWN_YET,
                                                              lib::is_oracle_mode(),
                                                              false);
      if (OB_FAIL(spec.sort_cmp_funs_.push_back(cmp_func))) {
        LOG_WARN("failed to push back sort function", K(ret));
      }
    }
    return ret;
  }
protected:
  ObArenaAllocator alloc_;

  ObHashIntersectSpec spec1_;
  ObMergeIntersectSpec spec2_;
  ObHashExceptSpec spec3_;
  ObMergeExceptSpec spec4_;
  SetDataGeneratorSpec spec5_;
  SetDataGeneratorSpec spec6_;

  ObHashIntersectOp hash_intersect_;
  ObMergeIntersectOp merge_intersect_;

  ObHashExceptOp hash_except_;
  ObMergeExceptOp merge_except_;

  ObOperator *hash_set_op_;
  ObOperator *merge_set_op_;

  SetPlan hash_plan_;
  SetPlan merge_plan_;



};

int ObHashSetDumpTest::SetPlan::setup_plan(ObOperator *set_op)
{
  int ret = OB_SUCCESS;
  left_.init_expr();
  right_.init_expr();

  SetDataGeneratorSpec &left_spec = const_cast<SetDataGeneratorSpec &> (left_.get_spec());
  SetDataGeneratorSpec &right_spec = const_cast<SetDataGeneratorSpec &> (right_.get_spec());
  set_op_ = set_op;
  ObOpSpec &set_op_spec = const_cast<ObOpSpec &> (set_op_->get_spec());
  left_spec.id_ = 0;
  right_spec.id_ = 1;
  set_op_spec.id_ = 2;
  //LOG_WARN("value:", K(left_spec), K(right_spec));

  //set_op_->set_column_count(SetDataGenerator::CELL_CNT * 2); 没有的参数

  left_spec.plan_=&plan_;
  right_spec.plan_=&plan_;
  set_op_spec.plan_=&plan_;
  set_op_->ctx_.set_my_session(&session_);
  set_op_->ctx_.init_phy_op(3);
  set_op_->ctx_.create_physical_plan_ctx();

  ObOperator **children_ptr = static_cast<ObOperator **>(exec_ctx_.get_allocator().alloc(sizeof(ObOperator *) * 2));
  ObOpSpec **spec_children_ptr = static_cast<ObOpSpec **>(exec_ctx_.get_allocator().alloc(sizeof(ObOpSpec *) * 2));
  if (OB_ISNULL(children_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alloc for children_ptr failed ", K(ret));
  } else if (OB_ISNULL(spec_children_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alloc for spec_children_ptr falied", K(ret));
  } else {
    children_ptr[0] = static_cast<ObOperator *>(&left_);
    children_ptr[1] = static_cast<ObOperator *>(&right_);
    spec_children_ptr[0] = static_cast<ObOpSpec *>(&left_spec);
    spec_children_ptr[1] = static_cast<ObOpSpec *>(&right_spec);
  }
  if (OB_FAIL(set_op_->set_children_pointer(children_ptr, 2))) {
    LOG_WARN("failed to set children for set op", K(ret));
  } else if (OB_FAIL(set_op_spec.set_children_pointer(spec_children_ptr, 2))) {
    LOG_WARN("failed to set spec children for set op", K(ret));
  }

  //set_op_->set_distinct(true);  没有的参数

  // setup context
  ObString tenant_name("test");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_.test_init(0, 0, 0, NULL))) {
  } else if (OB_FAIL(ObPreProcessSysVars::init_sys_var())) {
  } else if (OB_FAIL(session_.load_default_sys_variable(false, true))) {
  } else if (OB_FAIL(session_.init_tenant(tenant_name, OB_SYS_TENANT_ID))) {
  } else if (FALSE_IT(exec_ctx_.set_my_session(&session_))) {
  } else if (OB_FAIL(exec_ctx_.init_phy_op(3))) {
  } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
  }
  LOG_WARN("value:", K(left_spec), K(right_spec), K(set_op_spec));
  return ret;
}

void ObHashSetDumpTest::setup_plan(SetPlan &plan, bool hash_algo, ObHashSetDumpTest::TestAlgo algo)
{
  int ret = OB_SUCCESS;
  ObMergeSetOp *merge_op = nullptr;
  ObHashSetOp *hash_op = nullptr;
  switch (algo)
  {
  case INTERSECT:
    if (hash_algo) {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&hash_intersect_));
      hash_set_op_ = &hash_intersect_;
      hash_op = &hash_intersect_;
      ObHashSetSpec &hash_spec = static_cast<ObHashSetSpec &>(const_cast<ObOpSpec &> (hash_set_op_->get_spec()));
      if (OB_FAIL(generate_hash_set_spec(hash_spec))) {
        LOG_WARN("failed to generate hash_set_spec", K(ret));
      } else {
        LOG_WARN("success to generate hash_set_spec", K(ret));
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&merge_intersect_));
      merge_op = &merge_intersect_;
      merge_set_op_ = &merge_intersect_;
      ObMergeSetSpec &merge_spec = static_cast<ObMergeSetSpec &>(const_cast<ObOpSpec &> (merge_set_op_->get_spec()));
      if (OB_FAIL(generate_merge_set_spec(merge_spec))) {
        LOG_WARN("failed to generate merge_set_spec", K(ret));
      } else {
        LOG_WARN("success to generate merge_set_spec", K(ret));
      }
    }
    break;
  case EXCEPT:
    if (hash_algo) {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&hash_except_));
      hash_set_op_ = &hash_except_;
      hash_op = &hash_except_;
      ObHashSetSpec &hash_spec = static_cast<ObHashSetSpec &>(const_cast<ObOpSpec &> (hash_set_op_->get_spec()));
      generate_hash_set_spec(hash_spec);
    } else {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&merge_except_));
      merge_op = &merge_except_;
      merge_set_op_ = &merge_except_;
      ObMergeSetSpec &merge_spec = static_cast<ObMergeSetSpec &>(const_cast<ObOpSpec &> (merge_set_op_->get_spec()));
      generate_merge_set_spec(merge_spec);
    }
    break;
  default:
    break;
  }

}

void ObHashSetDumpTest::setup_test(TestAlgo algo, int32_t string_size,
    int64_t left_row_count, bool left_reverse, IdxCntFunc left_func,
    int64_t right_row_count, bool right_reverse, IdxCntFunc right_func)
{
  SetPlan *plans[] = { &hash_plan_, &merge_plan_ };
  for (int i = 0; i < 2; i++) {
    auto &plan = *plans[i];

    setup_plan(plan, i == 0 ? true : false, algo);

    plan.left_.row_cnt_ = left_row_count;
    plan.right_.row_cnt_ = right_row_count;
    plan.left_.string_size_ = string_size;
    plan.right_.string_size_ = string_size;
    UNUSED(left_reverse);
    UNUSED(left_func);
    UNUSED(right_reverse);
    UNUSED(right_func);
    /*if (&plan != &merge_plan_) {
      plan.left_.reverse_ = left_reverse;
      plan.right_.reverse_ = right_reverse;
    }*/
    //plan.left_.idx_cnt_func_ = left_func;
    //plan.right_.idx_cnt_func_ = right_func;

    ASSERT_EQ(OB_SUCCESS, plan.left_.test_init());
    ASSERT_EQ(OB_SUCCESS, plan.right_.test_init());
  }
}

void ObHashSetDumpTest::run_test(int64_t print_row_cnt)
{
  UNUSED(print_row_cnt);
  ObArenaAllocator alloc;
  typedef ObArray<int64_t *> ResArray;
  int64_t res_cell_cnt = 1;
  auto fun = [&](SetPlan &plan, ResArray &res)->void
  {
    ASSERT_EQ(OB_SUCCESS, plan.set_op_->open());
    int ret = OB_SUCCESS;
    const ObNewRow *row = NULL;
    int64_t cnt = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(plan.set_op_->get_next_row())) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        if (cnt < print_row_cnt) {
          LOG_INFO("join res", K(*row));
        }
        auto r = static_cast<int64_t *>(alloc.alloc(sizeof(int64_t) * res_cell_cnt));
        ASSERT_TRUE(NULL != r);

        for (int64_t i = 0; i < res_cell_cnt; i++) {
          auto &c = plan.set_op_->get_spec().output_[0];
          ObDatum *dtm = nullptr;
          c->eval(op_eval_ctx_, dtm);
          r[i] = dtm->get_int();
        }
        ASSERT_EQ(OB_SUCCESS, res.push_back(r));
      }
      cnt++;
    }
  };

  auto pfunc = [&](int64_t *r)
  {
    ObSqlString s;
    for (int64_t i = 0; i < res_cell_cnt; i++) {
      s.append_fmt("%ld, ", r[i]);
    }
    LOG_INFO("RES:", K(s.ptr()));
  };

  ResArray hash_res;
  fun(hash_plan_, hash_res);
  ASSERT_FALSE(HasFatalFailure());
  ResArray merge_res;
  fun(merge_plan_, merge_res);
  LOG_WARN("bp4");
  ASSERT_FALSE(HasFatalFailure());

  ASSERT_EQ(hash_res.count(), merge_res.count());

  auto sort_cmp = [&](int64_t *l, int64_t *r)
  {
    for (int64_t i = 0; i < res_cell_cnt; i++) {
      if (l[i] != r[i]) {
        return l[i] < r[i];
      }
    }
    return false;
  };
  LOG_WARN("bp5", K(hash_res.count()));
  std::sort(&hash_res.at(0), &hash_res.at(0) + hash_res.count(), sort_cmp);
  LOG_WARN("bp6", K(merge_res.count()));
  std::sort(&merge_res.at(0), &merge_res.at(0) + merge_res.count(), sort_cmp);
  for (int64_t i = 0; i < hash_res.count(); i++) {
    if (sort_cmp(hash_res.at(i), merge_res.at(i))
        || sort_cmp(merge_res.at(i), hash_res.at(i))) {
      pfunc(hash_res.at(i));
      pfunc(merge_res.at(i));
      ASSERT_FALSE(true);
    }
  }

  hash_set_op_->close();
  merge_set_op_->close();
  hash_plan_.~SetPlan();
  merge_plan_.~SetPlan();
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObTmpFileManager::get_instance().files_.map_.size());
}

int ObHashSetDumpTest::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObTenantManager &tm = ObTenantManager::get_instance();
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  uint64_t cluster_version = CLUSTER_VERSION_3000;
  common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
  EXPECT_EQ(cluster_version, common::ObClusterVersion::get_instance().get_cluster_version());
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.init(self, rpc_proxy, rs_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.set_tenant_mem_limit(tenant_id,
      4L * 1024L * 1024L * 1024L, 8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 256LL << 30;
  const int64_t llmt = 256LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.set_tenant_mem_limit(OB_SERVER_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  auto ctx_allocator =
    lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
          OB_SERVER_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ctx_allocator->set_limit(8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}


/*TEST_F(ObHashSetDumpTest, test_except)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 512,
    1000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    1000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/

/*TEST_F(ObHashSetDumpTest, test_intersect_small)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 512,
    2000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    2000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_intersect_large)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 512,
    20000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    20000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_intersect_dump)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 512,
    200000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    200000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/

/*TEST_F(ObHashSetDumpTest, test_intersect_dump_large)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 512,
    1000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    1000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/

/*TEST_F(ObHashSetDumpTest, test_except)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      2000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      2000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_except_large)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      100000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      100000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/



TEST_F(ObHashSetDumpTest, test_except_dump2)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      1000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      1000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

/*TEST_F(ObHashSetDumpTest, test_except_dump1)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      500000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_except_dump2)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      500000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/

/*TEST_F(ObHashSetDumpTest, test_dump_intersect)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_dump_except)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}*/

// farm时间比较长，暂时skip掉
// TEST_F(ObHashSetDumpTest, Size20M_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, Size20M_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, Size20M_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

} // end sql
} // end oceanbase

int main(int argc, char **argv)
{

  exec_ctx_.eval_ctx_ = &op_eval_ctx_;
  op_eval_ctx_.frames_ = static_cast<char **>(exec_ctx_.get_allocator().alloc(sizeof(void *) * 2));
  op_eval_ctx_.frames_[0] = (char *)exec_ctx_.get_allocator().alloc(4096);
  oceanbase::sql::init_sql_factories();
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
