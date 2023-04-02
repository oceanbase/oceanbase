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

#include <gtest/gtest.h>
#include "../optimizer/ob_mock_part_mgr.h"
#define protected public
#define private public
#include "sql/engine/test_engine_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_in.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "share/ob_tenant_mgr.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace test;
using namespace oceanbase::observer;
using namespace oceanbase::share;

class ObExecContextTest: public ::testing::Test
{
public:
  ObExecContextTest();
  virtual ~ObExecContextTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExecContextTest(const ObExecContextTest &other);
  ObExecContextTest& operator=(const ObExecContextTest &other);
private:
  // data members
};
ObExecContextTest::ObExecContextTest()
{
}

ObExecContextTest::~ObExecContextTest()
{
}

void ObExecContextTest::SetUp()
{
}

void ObExecContextTest::TearDown()
{
}

class ObPhyOperatorCtxFake : public ObPhyOperator::ObPhyOperatorCtx
{
public:
  ObPhyOperatorCtxFake(ObExecContext &ctx);
  ~ObPhyOperatorCtxFake();
  virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  int phy_op_calc();
  void set_phy_op_ctx_a(int a);
  void set_phy_op_ctx_b(int b);
private:
  int phy_op_ctx_a_;
  int phy_op_ctx_b_;
};

ObPhyOperatorCtxFake::ObPhyOperatorCtxFake(ObExecContext &ctx)
  : ObPhyOperatorCtx(ctx), phy_op_ctx_a_(0), phy_op_ctx_b_(0)
{
}

ObPhyOperatorCtxFake::~ObPhyOperatorCtxFake()
{
}

void ObPhyOperatorCtxFake::set_phy_op_ctx_a(int a)
{
  phy_op_ctx_a_ = a;
}

void ObPhyOperatorCtxFake::set_phy_op_ctx_b(int b)
{
  phy_op_ctx_b_ = b;
}

int ObPhyOperatorCtxFake::phy_op_calc()
{
  return phy_op_ctx_a_ + phy_op_ctx_b_;
}

class ObRootTransmitInput : public ObIPhyOperatorInput
{
public:
  ObRootTransmitInput(ObExecContext &ctx)
      : input_param_a_(0)
  {
    UNUSED(ctx);
  }
  virtual void reset() override
  {
    input_param_a_ = 0;
  }
  void set_input_param_a(int64_t a)
  {
    input_param_a_ = a;
  }

  int64_t get_input_param_a() const
  {
    return input_param_a_;
  }
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_IMPLEMENT;
  }
  int deserialize(const char *buf, int64_t data_len, int64_t &pos)
  {
    UNUSED(buf);
    UNUSED(data_len);
    UNUSED(pos);
    return OB_NOT_IMPLEMENT;
  }
  int64_t get_serialize_size() const
  {
    return 0;
  }
  int init(ObExecContext &ctx, ObTaskInfo &task_info, ObPhyOperator &op)
  {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(op);
    return OB_NOT_IMPLEMENT;
  }
  ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_ROOT_TRANSMIT;
  }
private:
  int64_t input_param_a_;
};

const uint64_t phy_op_id = 0;
//test create physical operator normal
TEST_F(ObExecContextTest, create_phy_op_test)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObExecContext exec_context;
  ObIAllocator *allocator = NULL;
  ObPhyOperatorCtxFake *phy_op_ctx = NULL;
  ObRootTransmitInput *input_param = NULL;
  int64_t phy_op_size = 64;

  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  allocator = &exec_context.get_allocator();
  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(phy_op_size));
  for (int i = 0; i < phy_op_size; i++)
  {
    ret = CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                  exec_context,
                                  phy_op_id + i,
                                  static_cast<ObPhyOperatorType>(0),
                                  phy_op_ctx);
    ASSERT_FALSE(NULL == phy_op_ctx);
    phy_op_ctx->set_phy_op_ctx_a(i);
    phy_op_ctx->set_phy_op_ctx_b(i + 1);
    ASSERT_EQ(i * 2 + 1, phy_op_ctx->phy_op_calc());

    //create input param
    ret = CREATE_PHY_OP_INPUT(ObRootTransmitInput,
                            exec_context,
                            phy_op_id + i,
                            PHY_ROOT_TRANSMIT,
                            input_param);
    ASSERT_FALSE(NULL == input_param);
    input_param->set_input_param_a(i);
    ASSERT_EQ(i, input_param->get_input_param_a());
  }

  for (int i = 0; i < phy_op_size; i++)
  {
    phy_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake, exec_context, phy_op_id + i);
    ASSERT_FALSE(NULL == phy_op_ctx);
    ASSERT_EQ(i * 2 + 1, phy_op_ctx->phy_op_calc());
    input_param = GET_PHY_OP_INPUT(ObRootTransmitInput, exec_context, phy_op_id + i);
    ASSERT_FALSE(NULL == input_param);
    ASSERT_EQ(i, input_param->get_input_param_a());
  }

  ptr = allocator->alloc(10);
  ASSERT_FALSE(NULL == ptr);
  allocator->free(ptr);
  UNUSED(ret);
}

//excepted create physical operator fail, create the same physical operator twice
TEST_F(ObExecContextTest, create_phy_op_fail)
{
  ObExecContext exec_context;
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  int64_t phy_op_size = 64;
  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(phy_op_size));

  ObPhyOperatorCtxFake *phy_op_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                                exec_context,
                                                phy_op_id,
                                                static_cast<ObPhyOperatorType>(0),
                                                phy_op_ctx));
  ASSERT_FALSE(NULL == phy_op_ctx);
  phy_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake, exec_context, phy_op_id);
  ASSERT_FALSE(NULL == phy_op_ctx);

  //create phy_op_ctx twice
  ASSERT_EQ(OB_INIT_TWICE, CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                                   exec_context,
                                                   phy_op_id,
                                                   static_cast<ObPhyOperatorType>(0),
                                                   phy_op_ctx));
  ASSERT_TRUE(NULL == phy_op_ctx);

  //test create input param failed
  ObRootTransmitInput *input_param = NULL;
  ASSERT_EQ(OB_SUCCESS, CREATE_PHY_OP_INPUT(ObRootTransmitInput,
                                            exec_context,
                                            phy_op_id,
                                            PHY_ROOT_TRANSMIT,
                                            input_param));
  ASSERT_FALSE(NULL == input_param);
  input_param = GET_PHY_OP_INPUT(ObRootTransmitInput, exec_context, phy_op_id);
  ASSERT_FALSE(NULL == input_param);
  //create input_param twice
  ASSERT_EQ(OB_INIT_TWICE, CREATE_PHY_OP_INPUT(ObRootTransmitInput,
                                            exec_context,
                                            phy_op_id,
                                            PHY_ROOT_TRANSMIT,
                                            input_param));
  ASSERT_TRUE(NULL == input_param);
}

TEST_F(ObExecContextTest, create_physical_plan_ctx)
{
  ObExecContext exec_context;
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(64));
  //create normal
  ASSERT_EQ(OB_SUCCESS, exec_context.create_physical_plan_ctx());
  ASSERT_FALSE(NULL == exec_context.get_physical_plan_ctx());
  //get plan ctx normal
  plan_ctx = exec_context.get_physical_plan_ctx();
  ASSERT_FALSE(NULL == plan_ctx);
}

TEST_F(ObExecContextTest, create_physical_plan_ctx_fail)
{
  ObExecContext exec_context;
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  ObPhysicalPlanCtx *plan_ctx = NULL;

  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(64));
  //get physical plan but not be created
  plan_ctx = exec_context.get_physical_plan_ctx();
  ASSERT_TRUE(NULL == plan_ctx);
  //create physical plan twice
  ASSERT_EQ(OB_SUCCESS, exec_context.create_physical_plan_ctx());
  ASSERT_FALSE(NULL == exec_context.get_physical_plan_ctx());
  ASSERT_EQ(OB_INIT_TWICE, exec_context.create_physical_plan_ctx());
}

TEST_F(ObExecContextTest, invalid_argument)
{
  ObExecContext exec_context;
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  int64_t phy_op_size = 64;
  uint64_t phy_op_id = 100;

  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(phy_op_size));
  //phy_op_id larger than phy_op_size
  ObPhyOperatorCtxFake *phy_op_ctx = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT, CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                                         exec_context,
                                                         phy_op_id,
                                                         static_cast<ObPhyOperatorType>(0),
                                                         phy_op_ctx));
  ASSERT_TRUE(NULL == phy_op_ctx);
  //OB_INVALID_ID phy_op_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                                         exec_context,
                                                         OB_INVALID_ID,
                                                         static_cast<ObPhyOperatorType>(0),
                                                         phy_op_ctx));
  ASSERT_EQ(NULL, phy_op_ctx);
  phy_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake, exec_context, phy_op_id);
  ASSERT_TRUE(NULL == phy_op_ctx);
}

TEST_F(ObExecContextTest, serialization)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_context;
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  ObPhyOperatorCtxFake *phy_op_ctx = NULL;
  ObRootTransmitInput *input_param = NULL;
  int64_t phy_op_size = 64;
  ObSql sql_engine;

  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, exec_context.init_phy_op(phy_op_size));
  ASSERT_EQ(OB_SUCCESS, create_test_session(exec_context));
  auto my_session = exec_context.get_my_session();
  ASSERT_FALSE(NULL == my_session);
  ObPhysicalPlan cur_phy_plan;
  ASSERT_EQ(OB_SUCCESS, my_session->set_cur_phy_plan(&cur_phy_plan));
  ASSERT_EQ(OB_SUCCESS, exec_context.create_physical_plan_ctx());
  ASSERT_EQ(OB_SUCCESS, my_session->load_default_sys_variable(false, true));
  GCTX.sql_engine_ = &sql_engine;

  for (int i = 0; i < phy_op_size; i++)
  {
    ret = CREATE_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake,
                                  exec_context,
                                  phy_op_id + i,
                                  static_cast<ObPhyOperatorType>(0),
                                  phy_op_ctx);
    ASSERT_FALSE(NULL == phy_op_ctx);
    phy_op_ctx->set_phy_op_ctx_a(i);
    phy_op_ctx->set_phy_op_ctx_b(i + 1);
    ASSERT_EQ(i * 2 + 1, phy_op_ctx->phy_op_calc());

    //create input param
    ret = CREATE_PHY_OP_INPUT(ObRootTransmitInput,
                            exec_context,
                            phy_op_id + i,
                            PHY_ROOT_TRANSMIT,
                            input_param);
    ASSERT_FALSE(NULL == input_param);
    input_param->set_input_param_a(i);
    ASSERT_EQ(i, input_param->get_input_param_a());
  }

  for (int i = 0; i < phy_op_size; i++)
  {
    phy_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtxFake, exec_context, phy_op_id + i);
    ASSERT_FALSE(NULL == phy_op_ctx);
    ASSERT_EQ(i * 2 + 1, phy_op_ctx->phy_op_calc());
    input_param = GET_PHY_OP_INPUT(ObRootTransmitInput, exec_context, phy_op_id + i);
    ASSERT_FALSE(NULL == input_param);
    ASSERT_EQ(i, input_param->get_input_param_a());
  }

  int64_t pos = 0;
  const int64_t MAX_SERIALIZE_BUF_LEN = 10240;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, exec_context.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, exec_context.get_serialize_size());
  UNUSED(ret);
}

int main(int argc, char **argv)
{
  system("rm -rf test_exec_context.log");
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_exec_context.log", true);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
