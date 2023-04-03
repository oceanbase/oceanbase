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

#define USING_LOG_PREFIX PL
#include <gtest/gtest.h>
#include "test_mock_pl_stmt.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "observer/ob_server.h"
#include "test_pl_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::pl;
using namespace oceanbase::share;
using namespace observer;
using namespace llvm;
using namespace test;

#define CG_AND_EXECUTE \
  ObPLFunction func(allocator_); \
  ObPLCodeGenerator cg(func.get_allocator(), func.get_expressions(), func.get_helper(), func.get_di_helper()); \
  ObPLExecState pl(exec_ctx.get_allocator(), exec_ctx, func, result, status, recursion_ctx, false); \
  ObPhysicalPlanCtx *phy_plan_ctx = exec_ctx.get_physical_plan_ctx(); \
  ObExprOperatorCtx **expr_op_ctx_store = exec_ctx.get_expr_op_ctx_store(); \
  uint64_t expr_op_size = exec_ctx.get_expr_op_size(); \
  exec_ctx.set_physical_plan_ctx(&pl.get_physical_plan_ctx()); \
  if (OB_FAIL(recursion_ctx.init(&exec_ctx))) { \
    LOG_WARN("fail to init recursion context", K(ret));\
  }\
  if (OB_SUCC(ret) && func.get_expr_op_size() > 0)  { \
    exec_ctx.set_expr_op_ctx_store(NULL); \
    exec_ctx.set_expr_op_size(0); \
    if (OB_FAIL(exec_ctx.init_expr_op(func.get_expr_op_size()))) { \
      SQL_LOG(WARN, "failed to init expr op", K(ret)); \
    } \
  } \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(pl.init(&params))) { \
      LOG_WARN("failed to init pl", K(ret)); \
    } else if (OB_FAIL(pl.execute())) { \
      LOG_WARN("failed to execute pl", K(ret)); \
    } else { \
      for (int64_t i = 0; OB_SUCC(ret) && i < func.get_arg_count(); ++i) { \
        if (func.get_out_args().has_member(i)) { \
          params.at(i) = pl.get_params().at(i); \
        } \
      } \
    } \
  } \
  if (OB_SUCC(ret)) { \
    exec_ctx.set_physical_plan_ctx(phy_plan_ctx); \
    if (func.get_expr_op_size() > 0) { \
      exec_ctx.get_allocator().free(exec_ctx.get_expr_op_ctx_store()); \
      exec_ctx.set_expr_op_ctx_store(expr_op_ctx_store); \
      exec_ctx.set_expr_op_size(expr_op_size); \
    } \
  }

#define DEFINE_PL_EXEC_CTX                                \
  sql::ObExecContext exec_ctx(allocator_);                \
  exec_ctx.set_my_session(&session_info_);                \
  exec_ctx.set_sql_proxy(OBSERVER.get_gctx().sql_proxy_); \
  common::ObArray<common::ObObjParam> params;             \
  ObObj result;                                           \
  int status = OB_SUCCESS;                                \
  ObPLExecRecursionCtx recursion_ctx;                     \
  ObPLExecCtx ctx(&allocator_, &exec_ctx, &params, &result, &status, &recursion_ctx);

class ObPLBasicTest : public TestPLUtils, public ::testing::Test
{
public:
  ObPLBasicTest() {}
  virtual ~ObPLBasicTest() {}
  virtual void SetUp() { init(); }
  virtual void TearDown() {}
  virtual void init() { TestPLUtils::init(); }

public:
  // data members

};

TEST_F(ObPLBasicTest, test_declare)
{
  int ret = OB_SUCCESS;

  //构造FunctionAST
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObNullType);
  func_ast.set_ret_type(res_type); //无返回值

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareVarStmt *var_stmt = static_cast<ObPLDeclareVarStmt*>(stmt_service.make_stmt(PL_VAR, block));
  var_stmt->add_index(0);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_label_table(&func_ast.get_label_table());
  block->get_namespace().set_type_table(&func_ast.get_user_type_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  block->get_namespace().set_cursor_table(&func_ast.get_cursor_table());
  block->get_namespace().set_exprs(&func_ast.get_exprs());
//  ObPLExternalNS external_ns(NULL);
  block->get_namespace().set_external_ns(NULL/*&external_ns*/);
  func_ast.set_body(block);

  //构造符号表
  ObPLDataType var1;
  ObDataType obj_type;
  ObObjMeta obj_meta;
  obj_meta.set_int();
  obj_type.set_meta_type(obj_meta);
  var1.set_data_type(obj_type);
  block->get_namespace().add_symbol(ObString("a"), var1);

  //构造执行环境
  DEFINE_PL_EXEC_CTX

  CG_AND_EXECUTE
}

TEST_F(ObPLBasicTest, test_assign)
{
  int ret = OB_SUCCESS;
  //构造表达式
  ObRawExprFactory expr_factory(allocator_);
  ObRawExpr *expr = NULL;
  ObConstRawExpr *left_const = NULL;
  ObConstRawExpr *right_const = NULL;
  ObRawExpr *left_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, left_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (FALSE_IT(left_expr = static_cast<ObRawExpr*>(left_const))) {
  } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, 0, left_expr))) { //symbol table idx: 0
    LOG_WARN("failed to create param expr", K(ret));
  } else if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObInt32Type, 100, right_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (ObRawExprUtils::create_double_op_expr(expr_factory, NULL, T_OP_ADD, expr, left_expr, right_const)) {
    LOG_WARN("failed to create double expr", K(ret));
  } else { /*do nothing*/ }

  ObConstRawExpr *into_const = NULL;
  ObRawExpr *into_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, into_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (FALSE_IT(into_expr = static_cast<ObRawExpr*>(into_const))) {
  } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, 1, into_expr))) { //symbol table idx: 1
    LOG_WARN("failed to create param expr", K(ret));
  } else { /*do nothing*/ }

  ObConstRawExpr *default_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, default_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  }

  ObConstRawExpr *return_const = NULL;
  ObRawExpr *return_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, return_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (FALSE_IT(return_expr = static_cast<ObRawExpr*>(return_const))) {
  } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, 1, return_expr))) { //symbol table idx: 2
    LOG_WARN("failed to create param expr", K(ret));
  } else { /*do nothing*/ }

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObIntType);
  func_ast.set_ret_type(res_type);
  ObDataType arg1;
  arg1.set_int();
  func_ast.add_argument(ObString("test_func_arg1"), arg1);
  func_ast.add_expr(expr); //expr table idx: 0
  func_ast.add_expr(return_expr); //expr table idx: 1
  func_ast.add_expr(default_expr); //expr table idx: 2
  func_ast.add_expr(into_expr); //expr table idx: 3

  //构造变量
  ObPLDataType pl_arg1;
  pl_arg1.set_data_type(arg1);
  ObPLDataType var1;
  var1.set_data_type(arg1);

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareVarStmt *var_stmt = static_cast<ObPLDeclareVarStmt*>(stmt_service.make_stmt(PL_VAR, block));
  ObPLAssignStmt *assign_stmt = static_cast<ObPLAssignStmt*>(stmt_service.make_stmt(PL_ASSIGN, block));
  ObPLReturnStmt *return_stmt = static_cast<ObPLReturnStmt*>(stmt_service.make_stmt(PL_RETURN, block));
  var_stmt->add_index(1);
  assign_stmt->set_into(3);
  assign_stmt->set_value(0);
  return_stmt->set_ret(1);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_label_table(&func_ast.get_label_table());
  block->get_namespace().set_type_table(&func_ast.get_user_type_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  block->get_namespace().set_cursor_table(&func_ast.get_cursor_table());
  block->get_namespace().set_exprs(&func_ast.get_exprs());
//  ObPLExternalNS external_ns(NULL);
  block->get_namespace().set_external_ns(NULL/*&external_ns*/);
  func_ast.set_body(block);


  //构造符号表
  block->get_namespace().add_symbol(ObString("test_func_arg1"), pl_arg1, NULL);
  ObObj value;
  value.set_int(42); //声明变量默认值
  default_expr->set_value(value);
  block->get_namespace().add_symbol(ObString("a"), var1, default_expr);

  //构造执行环境
  DEFINE_PL_EXEC_CTX
  ObObjParam param;
  param.set_int(22); //输入参数实际值
  params.push_back(param);
  param.set_int(42); //声明变量实际值
  params.push_back(param);

  CG_AND_EXECUTE
}

TEST_F(ObPLBasicTest, test_return)
{
  int ret = OB_SUCCESS;
  //构造表达式
  ObRawExprFactory expr_factory(allocator_);
  ObConstRawExpr *return_const = NULL;
  ObRawExpr *return_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 77, return_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (FALSE_IT(return_expr = static_cast<ObRawExpr*>(return_const))) {
  } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, 0, return_expr))) {
    LOG_WARN("failed to create param expr", K(ret));
  } else { /*do nothing*/ }

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObIntType);
  func_ast.set_ret_type(res_type);
  func_ast.add_expr(return_expr); //expr table idx: 0

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLReturnStmt *return_stmt = static_cast<ObPLReturnStmt*>(stmt_service.make_stmt(PL_RETURN, block));
  return_stmt->set_ret(0);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_label_table(&func_ast.get_label_table());
  block->get_namespace().set_type_table(&func_ast.get_user_type_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  block->get_namespace().set_cursor_table(&func_ast.get_cursor_table());
  block->get_namespace().set_exprs(&func_ast.get_exprs());
//  ObPLExternalNS external_ns(NULL);
  block->get_namespace().set_external_ns(NULL/*&external_ns*/);
  func_ast.set_body(block);

  //构造执行环境
  DEFINE_PL_EXEC_CTX
  ObObjParam param;
  param.set_int(22); //输入参数实际值
  params.push_back(param);

  CG_AND_EXECUTE
}

#if 0
TEST_F(ObPLBasicTest, test_sql)
{
  int ret = OB_SUCCESS;
  //构造表达式
  ObRawExprFactory expr_factory(allocator_);
  ObConstRawExpr *return_const = NULL;
  ObRawExpr *return_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, return_const)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (FALSE_IT(return_expr = static_cast<ObRawExpr*>(return_const))) {
  } else if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, 0, return_expr))) {
    LOG_WARN("failed to create param expr", K(ret));
  } else { /*do nothing*/ }

  ObConstRawExpr *default_expr = NULL;
  if (ObRawExprUtils::build_const_int_expr(expr_factory, common::ObIntType, 0, default_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  }

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObIntType);
  func_ast.set_ret_type(res_type);
  func_ast.add_expr(return_expr); //expr table idx: 0

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareVarStmt *var_stmt = static_cast<ObPLDeclareVarStmt*>(stmt_service.make_stmt(PL_VAR, block));
  ObPLSqlStmt *sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, block));
  ObPLReturnStmt *return_stmt = static_cast<ObPLReturnStmt*>(stmt_service.make_stmt(PL_RETURN, block));
  var_stmt->add_index(0);
  sql_stmt->set_sql(ObString("select 99 from dual"));
  sql_stmt->add_into(0);
  return_stmt->set_ret(0);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_label_table(&func_ast.get_label_table());
  block->get_namespace().set_type_table(&func_ast.get_user_type_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  block->get_namespace().set_cursor_table(&func_ast.get_cursor_table());
    block->get_namespace().set_exprs(&func_ast.get_exprs());
//  ObPLExternalNS external_ns(NULL);
  block->get_namespace().set_external_ns(NULL/*&external_ns*/);
  func_ast.set_body(block);

  //构造符号表
  ObDataType var1;
  var1.set_int();
  ObPLDataType pl_var1;
  pl_var1.set_data_type(var1);
  ObObj value;
  value.set_int(42); //声明变量默认值
  default_expr->set_value(value);
  block->get_namespace().add_symbol(ObString("a"), pl_var1, default_expr);

  //构造执行环境
  DEFINE_PL_EXEC_CTX
  ObObjParam param;
  param.set_int(42); //声明变量实际值
  params.push_back(param);

  CG_AND_EXECUTE
}
#endif

TEST_F(ObPLBasicTest, test_eh_basic)
{
  int ret = OB_SUCCESS;

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("p"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObNullType);
  func_ast.set_ret_type(res_type); //无返回值

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareHandlerStmt *cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, block));
  ObPLDeclareHandlerStmt::DeclareHandler dh;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc = new(desc)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  ObPLConditionValue value;
  value.type_ = ERROR_CODE;
  value.error_code_ = 7777;
  desc->add_condition(value);
//  ObPLExternalNS external_ns(NULL);
  ObPLStmtBlock *exception_block = stmt_service.make_block(&block->get_namespace(),
                                                           &func_ast.get_symbol_table(),
                                                           &func_ast.get_label_table(),
                                                           &func_ast.get_condition_table(),
                                                           &func_ast.get_cursor_table(),
                                                           &func_ast.get_exprs(),
                                                           NULL/*&external_ns*/);
  ObPLDeclareVarStmt *var_stmt = static_cast<ObPLDeclareVarStmt*>(stmt_service.make_stmt(PL_VAR, exception_block));
  var_stmt->add_index(0);
  desc->set_body(exception_block);
  dh.set_desc(desc);
  cond_stmt->add_handler(dh);
  ObPLSignalStmt *signal_stmt = static_cast<ObPLSignalStmt*>(stmt_service.make_stmt(PL_SIGNAL, block));
  signal_stmt->set_cond_type(ERROR_CODE);
  signal_stmt->set_error_code(7777);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  func_ast.set_body(block);

  ObDataType var1;
  var1.set_int();
  ObPLDataType pl_var1;
  pl_var1.set_data_type(var1);
  block->get_namespace().add_symbol(ObString("a"), pl_var1);

  //构造执行环境
  DEFINE_PL_EXEC_CTX

  CG_AND_EXECUTE
}

TEST_F(ObPLBasicTest, test_eh_1)
{
  int ret = OB_SUCCESS;

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObNullType);
  func_ast.set_ret_type(res_type); //无返回值

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareHandlerStmt *cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, block));
  ObPLDeclareHandlerStmt::DeclareHandler dh;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc = new(desc)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  ObPLConditionValue value;
  value.type_ = SQL_EXCEPTION;
  value.error_code_ = 11111111;
  value.sql_state_ = "biu~~~";
  value.str_len_ = 6;
  desc->add_condition(value);
  value.type_ = SQL_STATE;
  value.error_code_ = 987654321;
  value.sql_state_ = "dangdangdangdang";
  value.str_len_ = 16;
  desc->add_condition(value);
//  ObPLExternalNS external_ns(NULL);
  ObPLStmtBlock *exception_block = stmt_service.make_block(&block->get_namespace(),
                                                           &func_ast.get_symbol_table(),
                                                           &func_ast.get_label_table(),
                                                           &func_ast.get_condition_table(),
                                                           &func_ast.get_cursor_table(),
                                                           &func_ast.get_exprs(),
                                                           NULL/*&external_ns*/);
  ObPLSqlStmt *sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, exception_block));
  sql_stmt->set_sql(ObString("select 88 from dual"));
  desc->set_body(exception_block);
  dh.set_desc(desc);
  cond_stmt->add_handler(dh);
  ObPLSignalStmt *signal_stmt = static_cast<ObPLSignalStmt*>(stmt_service.make_stmt(PL_SIGNAL, block));
  signal_stmt->set_cond_type(SQL_STATE);
  signal_stmt->set_error_code(987654321);
  signal_stmt->set_sql_state("dangdangdangdang");
  signal_stmt->set_str_len(16);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  func_ast.set_body(block);

  //构造执行环境
  DEFINE_PL_EXEC_CTX

  CG_AND_EXECUTE
}

TEST_F(ObPLBasicTest, test_eh_2)
{
  int ret = OB_SUCCESS;

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObNullType);
  func_ast.set_ret_type(res_type); //无返回值

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareHandlerStmt *cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, block));
  ObPLDeclareHandlerStmt::DeclareHandler dh1;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc1 = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc1 = new(desc1)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  ObPLConditionValue value;
  value.type_ = SQL_WARNING;
  value.error_code_ = 11111111;
  value.sql_state_ = "biu~~~";
  value.str_len_ = 6;
  desc1->add_condition(value);
//  ObPLExternalNS external_ns(NULL);
  ObPLStmtBlock *exception_block = stmt_service.make_block(&block->get_namespace(),
                                                           &func_ast.get_symbol_table(),
                                                           &func_ast.get_label_table(),
                                                           &func_ast.get_condition_table(),
                                                           &func_ast.get_cursor_table(),
                                                           &func_ast.get_exprs(),
                                                           NULL/*&external_ns*/);
  ObPLSqlStmt *sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, exception_block));
  sql_stmt->set_sql(ObString("select 99 from dual"));
  desc1->set_body(exception_block);
  dh1.set_desc(desc1);
  cond_stmt->add_handler(dh1);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  func_ast.set_body(block);
  ObPLStmtBlock *subblock = stmt_service.make_block(&block->get_namespace(),
                                                    &func_ast.get_symbol_table(),
                                                    &func_ast.get_label_table(),
                                                    &func_ast.get_condition_table(),
                                                    &func_ast.get_cursor_table(),
                                                    &func_ast.get_exprs(),
                                                    NULL/*&external_ns*/);
  subblock->set_block(block);
  block->add_stmt(subblock);
  ObPLDeclareHandlerStmt::DeclareHandler dh2;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc2 = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc2 = new(desc2)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, subblock));
  value.type_ = NOT_FOUND;
  value.error_code_ = 22222222;
  value.sql_state_ = "biu~biu~~~";
  value.str_len_ = 10;
  desc2->add_condition(value);
  value.type_ = SQL_STATE;
  value.error_code_ = 33333333;
  value.sql_state_ = "biu~biu~biu~~~";
  value.str_len_ = 14;
  desc2->add_condition(value);
  value.type_ = SQL_EXCEPTION;
  value.error_code_ = 44444444;
  value.sql_state_ = "biu~biu~biu~biu~~~";
  value.str_len_ = 18;
  desc2->add_condition(value);
  exception_block = stmt_service.make_block(&block->get_namespace(),
                                            &func_ast.get_symbol_table(),
                                            &func_ast.get_label_table(),
                                            &func_ast.get_condition_table(),
                                            &func_ast.get_cursor_table(),
                                            &func_ast.get_exprs(),
                                            NULL/*&external_ns*/);
  sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, exception_block));
  sql_stmt->set_sql(ObString("select 88 from dual"));
  desc2->set_body(exception_block);
  dh2.set_desc(desc2);
  cond_stmt->add_handler(dh2);
  ObPLSignalStmt *signal_stmt = static_cast<ObPLSignalStmt*>(stmt_service.make_stmt(PL_SIGNAL, subblock));
  signal_stmt->set_cond_type(SQL_STATE);
  signal_stmt->set_error_code(987654321);
  signal_stmt->set_sql_state("dangdangdangdang");
  signal_stmt->set_str_len(16);

  //构造执行环境
  DEFINE_PL_EXEC_CTX

  CG_AND_EXECUTE
}

TEST_F(ObPLBasicTest, test_eh_3)
{
  int ret = OB_SUCCESS;

  //构造Function
  ObPLFunctionAST func_ast(allocator_);
  func_ast.set_name(ObString("test_func"));
  ObDataType res_type;
  res_type.set_obj_type(common::ObNullType);
  func_ast.set_ret_type(res_type); //无返回值

  //构造Stmt
  TestPLStmtMockService stmt_service(allocator_);
  ObPLStmtBlock *block = stmt_service.make_block(NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  ObPLDeclareHandlerStmt *cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, block));
  ObPLDeclareHandlerStmt::DeclareHandler dh1;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc1 = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc1 = new(desc1)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  ObPLConditionValue value;
  value.type_ = SQL_STATE;
  value.error_code_ = 0;
  value.sql_state_ = "42S02";
  value.str_len_ = 5;
  desc1->add_condition(value);
//  ObPLExternalNS external_ns(NULL);
  ObPLStmtBlock *exception_block = stmt_service.make_block(&block->get_namespace(),
                                                           &func_ast.get_symbol_table(),
                                                           &func_ast.get_label_table(),
                                                           &func_ast.get_condition_table(),
                                                           &func_ast.get_cursor_table(),
                                                           &func_ast.get_exprs(),
                                                           NULL/*&external_ns*/);
  ObPLSqlStmt *sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, exception_block));
  sql_stmt->set_sql(ObString("select 99 from dual"));
  desc1->set_body(exception_block);
  dh1.set_desc(desc1);
  cond_stmt->add_handler(dh1);
  block->get_namespace().set_symbol_table(&func_ast.get_symbol_table());
  block->get_namespace().set_condition_table(&func_ast.get_condition_table());
  func_ast.set_body(block);
  ObPLStmtBlock *subblock = stmt_service.make_block(&block->get_namespace(),
                                                    &func_ast.get_symbol_table(),
                                                    &func_ast.get_label_table(),
                                                    &func_ast.get_condition_table(),
                                                    &func_ast.get_cursor_table(),
                                                    &func_ast.get_exprs(),
                                                    NULL/*&external_ns*/);
  subblock->set_block(block);
  block->add_stmt(subblock);
  ObPLDeclareHandlerStmt::DeclareHandler dh2;
  ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc2 = static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
  desc2 = new(desc2)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc();
  cond_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt_service.make_stmt(PL_HANDLER, subblock));
  value.type_ = ERROR_CODE;
  value.error_code_ = 1051;
  value.sql_state_ = NULL;
  desc2->add_condition(value);
  exception_block = stmt_service.make_block(&block->get_namespace(),
                                            &func_ast.get_symbol_table(),
                                            &func_ast.get_label_table(),
                                            &func_ast.get_condition_table(),
                                            &func_ast.get_cursor_table(),
                                            &func_ast.get_exprs(),
                                            NULL/*&external_ns*/);
  sql_stmt = static_cast<ObPLSqlStmt*>(stmt_service.make_stmt(PL_SQL, exception_block));
  sql_stmt->set_sql(ObString("select 88 from dual"));
  desc2->set_body(exception_block);
  dh2.set_desc(desc2);
  cond_stmt->add_handler(dh2);
  ObPLSignalStmt *signal_stmt = static_cast<ObPLSignalStmt*>(stmt_service.make_stmt(PL_SIGNAL, subblock));
  signal_stmt->set_cond_type(SQL_STATE);
  signal_stmt->set_error_code(0);
  signal_stmt->set_sql_state("42S02");
  signal_stmt->set_str_len(5);

  //构造执行环境
  DEFINE_PL_EXEC_CTX

  CG_AND_EXECUTE
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  ObScannerPool::build_instance();
  ObIntermResultPool::build_instance();
  ObServerOptions opts;
  opts.cluster_id_ = 1;
  opts.rs_list_ = "127.0.0.1:1";
  opts.zone_ = "test1";
  opts.data_dir_ = "/tmp";
  opts.mysql_port_ = 23000 + getpid() % 1000;
  opts.rpc_port_ = 24000 + getpid() % 1000;
  system("mkdir -p /tmp/sstable /tmp/clog /tmp/slog");
  GCONF.datafile_size = 41943040;
//  OBSERVER.init(opts);
  int ret = 0;//RUN_ALL_TESTS();
  OBSERVER.destroy();
  return ret;
}

