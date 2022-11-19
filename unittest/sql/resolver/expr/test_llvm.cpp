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
#include "sql/test_sql_utils.h"
#include "lib/utility/ob_test_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_print_visitor.h"
#include "sql/ob_sql_init.h"
#include "lib/json/ob_json_print_utils.h"
#include <fstream>

// files needed by LLVM
#ifdef ID
#undef ID
#endif
#include "llvm/DerivedTypes.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/JIT.h"
#include "llvm/LLVMContext.h"
#include "llvm/Module.h"
#include "llvm/PassManager.h"
#include "llvm/Analysis/Verifier.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/Target/TargetData.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Support/IRBuilder.h"
#include "llvm/Support/TargetSelect.h"
#include <cstdio>
#include <string>
#include <map>
#include <vector>

using namespace llvm;
using namespace oceanbase::common;
using namespace oceanbase::sql;

static FunctionPassManager *TheFPM;
static Module *TheModule;
////static std::map<std::string, Value*> NamedValues;
static IRBuilder<> Builder(getGlobalContext());
static ExecutionEngine *TheExecutionEngine;

static int64_t get_usec()
{
  struct timeval time_val;
  gettimeofday(&time_val, NULL);
  return time_val.tv_sec*1000000 + time_val.tv_usec;
}

class ExprAST;
class TestLLVM: public ::testing::Test
{
public:
  TestLLVM();
  virtual ~TestLLVM();
  virtual void SetUp();
  virtual void TearDown();
private:

  DISALLOW_COPY_AND_ASSIGN(TestLLVM);
protected:
  // function members
  int resolve(const char* expr, const char *&json_expr);
  int convert(ObRawExpr *raw_expr, ExprAST *&ast);
};


TestLLVM::TestLLVM()
{
}

TestLLVM::~TestLLVM()
{
}

void TestLLVM::SetUp()
{
}

void TestLLVM::TearDown()
{
}

/// ExprAST - Base class for all expression nodes.
class ExprAST {
public:
  virtual ~ExprAST() {}
  virtual Value *Codegen() = 0;
};

/// NumberExprAST - Expression class for numeric literals like "1.0".
class NumberExprAST : public ExprAST {
  double Val;
public:
  NumberExprAST(double val) : Val(val) {}
  virtual Value *Codegen();
};

Value *NumberExprAST::Codegen() {
  return ConstantFP::get(getGlobalContext(), APFloat(Val));
}

ExprAST *Error(const char *Str) { fprintf(stderr, "Error: %s\n", Str);return 0;}
Value *ErrorV(const char *Str) { Error(Str); return 0; }

///// VariableExprAST - Expression class for referencing a variable, like "a".
//class VariableExprAST : public ExprAST {
//  std::string Name;
//public:
//  VariableExprAST(const std::string &name) : Name(name) {}
//  virtual Value *Codegen();
//};


/// BinaryExprAST - Expression class for a binary operator.
class BinaryExprAST : public ExprAST {
  char Op;    // ObItemType
  ExprAST *LHS, *RHS;
public:
  BinaryExprAST(char op, ExprAST *lhs, ExprAST *rhs)
    : Op(op), LHS(lhs), RHS(rhs) {}
  void add_child_ast(ExprAST *child);
  virtual Value *Codegen();
};

Value *BinaryExprAST::Codegen() {
  Value *L = LHS->Codegen();
  Value *R = RHS->Codegen();
  if (L == 0 || R == 0) return 0;
//  printf("binary op %d\n", Op);
  switch (Op) {
  case T_OP_ADD: return Builder.CreateFAdd(L, R, "addtmp");
  case T_OP_MINUS: return Builder.CreateFSub(L, R, "subtmp");
  case T_OP_MUL: return Builder.CreateFMul(L, R, "multmp");
  case T_OP_LT:
    L = Builder.CreateFCmpULT(L, R, "cmptmp");
    // Convert bool 0/1 to double 0.0 or 1.0
    return Builder.CreateUIToFP(L, Type::getDoubleTy(getGlobalContext()),
                                "booltmp");
  default: return ErrorV("invalid binary operator");
  }
}

void BinaryExprAST::add_child_ast(ExprAST *child)
{
  if (LHS) {
    RHS = child;
  } else {
    LHS = child;
  }
}


/// PrototypeAST - This class represents the "prototype" for a function,
/// which captures its name, and its argument names (thus implicitly the number
/// of arguments the function takes).
class PrototypeAST {
  std::string Name;
  std::vector<std::string> Args;
  std::map<std::string, Value*> NamedValues;
public:
  PrototypeAST(const std::string &name, const std::vector<std::string> &args)
    : Name(name), Args(args) {}

  Function *Codegen();
};

Function *PrototypeAST::Codegen() {
  // Make the function type:  double(double,double) etc.
  std::vector<Type*> Doubles(Args.size(),
                             Type::getDoubleTy(getGlobalContext()));
  FunctionType *FT = FunctionType::get(Type::getDoubleTy(getGlobalContext()),
                                       Doubles, false);

  Function *F = Function::Create(FT, Function::ExternalLinkage, Name, TheModule);

  // If F conflicted, there was already something named 'Name'.  If it has a
  // body, don't allow redefinition or reextern.
  if (F->getName() != Name) {
    // Delete the one we just made and get the existing one.
    F->eraseFromParent();
    F = TheModule->getFunction(Name);

    // If F already has a body, reject this.
    if (!F->empty()) {
      printf("redefinition of function");
      return 0;
    }

    // If F took a different number of args, reject.
    if (F->arg_size() != Args.size()) {
      printf("redefinition of function with different # args");
      return 0;
    }
  }

  // Set names for all arguments.
  unsigned Idx = 0;
  for (Function::arg_iterator AI = F->arg_begin(); Idx != Args.size();
       ++AI, ++Idx) {
    AI->setName(Args[Idx]);

    // Add arguments to variable symbol table.
    NamedValues[Args[Idx]] = AI;
  }

  return F;
}

/// FunctionAST - This class represents a function definition itself.
class FunctionAST {
  PrototypeAST *Proto;
  ExprAST *Body;
public:
  FunctionAST(PrototypeAST *proto, ExprAST *body)
    : Proto(proto), Body(body) {}

  Function *Codegen();
};

Function *FunctionAST::Codegen() {
//  NamedValues.clear();

  Function *TheFunction = Proto->Codegen();
  if (TheFunction == 0)
    return 0;

  // Create a new basic block to start insertion into.
  BasicBlock *BB = BasicBlock::Create(getGlobalContext(), "entry", TheFunction);
  Builder.SetInsertPoint(BB);

  if (Value *RetVal = Body->Codegen()) {
    // Finish off the function.
    Builder.CreateRet(RetVal);

    // Validate the generated code, checking for consistency.
    verifyFunction(*TheFunction);

    // Optimize the function.
    TheFPM->run(*TheFunction);

    return TheFunction;
  }

  // Error reading body, remove function.
  TheFunction->eraseFromParent();
  return 0;
}

int TestLLVM::resolve(const char* expr, const char *&json_expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> expr_store;
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  const char* expr_str = expr;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObRawExprFactory expr_factory(allocator);
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ctx.is_extract_param_type_ = false;
  ObRawExpr *raw_expr = NULL;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  if (OB_FAIL(ObRawExprUtils::make_raw_expr_from_str(expr_str,
                                                     strlen(expr_str),
                                                     ctx,
                                                     raw_expr,
                                                     columns,
                                                     sys_vars,
                                                     &sub_query_info,
                                                     aggr_exprs,
                                                     win_exprs,
                                                     udf_info)))
  {
    printf("error");
  }

  _OB_LOG(DEBUG, "================================================================");
  _OB_LOG(DEBUG, "%s", expr);
  _OB_LOG(DEBUG, "%s", CSJ(raw_expr));
  if (OB_FAIL(raw_expr->extract_info())) {
    printf("error");
  }
  //OK(raw_expr->deduce_type());
  json_expr = CSJ(raw_expr);


  ExprAST *ast = NULL;
  if (OB_FAIL(convert(raw_expr, ast))) {
    printf("fail to convert");
  } else {
    InitializeNativeTarget();
    LLVMContext &Context = getGlobalContext();
    // Make the module, which holds all the code.
    TheModule = new Module("my cool jit", Context);

    // Create the JIT.  This takes ownership of the module.
    std::string ErrStr;
    TheExecutionEngine = EngineBuilder(TheModule).setErrorStr(&ErrStr).create();
    if (!TheExecutionEngine) {
      printf("Could not create ExecutionEngine\n");
      std::cout << "ErrStr : " << ErrStr << std::endl;
      OB_ASSERT(0);
    } else {


      FunctionPassManager OurFPM(TheModule);

      // Set up the optimizer pipeline.  Start with registering info about how the
      // target lays out data structures.
      OurFPM.add(new TargetData(*TheExecutionEngine->getTargetData()));
      // Provide basic AliasAnalysis support for GVN.
      OurFPM.add(createBasicAliasAnalysisPass());
      // Do simple "peephole" optimizations and bit-twiddling optzns.
      OurFPM.add(createInsstructionCombiningPass());
      // Reassociate expressions.
      OurFPM.add(createReassociatePass());
      // Eliminate Common SubExpressions.
      OurFPM.add(createGVNPass());
      // Simplify the control flow graph (deleting unreachable blocks, etc).
      OurFPM.add(createCFGSimplificationPass());

      OurFPM.doInitialization();

      // Set the global so the code gen can use this.
      TheFPM = &OurFPM;


      PrototypeAST *Proto = new PrototypeAST("", std::vector<std::string>());
      FunctionAST *func = new FunctionAST(Proto, ast);

      if (!func) {
      } else if (Function *LF = func->Codegen()) {
        // JIT the function, returning a function pointer.
        int64_t i, j, m, n;
        i = get_usec();
        void *FPtr = TheExecutionEngine->getPointerToFunction(LF);
        j = get_usec();
        printf("JIT in %ld us\n", j - i);
        // Cast it to the right type (takes no arguments, returns a double) so we
        // can call it as a native function.
        double (*FP)() = (double (*)())(intptr_t)FPtr;
        m = get_usec();
        for (int64_t k = 0; k < 10000000; k++) {
          FP();
        }
        n = get_usec();
        printf("Evaluated to %f\n in %ld us\n", FP(), n-m);
      }
    }
  }

  return ret;
}

int TestLLVM::convert(ObRawExpr *raw_expr, ExprAST *&ast)
{
  int ret = OB_SUCCESS;
  switch (raw_expr->get_expr_class()) {
  case ObRawExpr::EXPR_CONST: {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*> (raw_expr);
    ast = new NumberExprAST(static_cast<double> (const_expr->get_value().get_int()));
  }
    break;
  case ObRawExpr::EXPR_OPERATOR: {
    ast = new BinaryExprAST(raw_expr->get_expr_type(), NULL, NULL);
  }
    break;
  default:
    OB_ASSERT(0);
    break;
  }

  if (raw_expr->get_expr_class() == ObRawExpr::EXPR_CONST) {
    // do nothing
  } else {
    ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *> (raw_expr);
    BinaryExprAST *binary_ast = static_cast<BinaryExprAST *> (ast);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); i++) {
      ExprAST *new_ast = NULL;
      if (OB_FAIL(convert(op_expr->get_param_exprs().at(i), new_ast))) {
        printf("fail to convert");
      } else {
        binary_ast->add_child_ast(new_ast);
      }
    }
  }

  return ret;
}

TEST_F(TestLLVM, all)
{
  const char* json_expr = NULL;
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = resolve("1+2", json_expr))) {
    printf("fail to resolve\n");
  } else if (OB_SUCCESS != (ret = resolve("2-1", json_expr))) {
    printf("fail to resolve\n");
  } else if (OB_SUCCESS != (ret = resolve("1345435+ 1232132", json_expr))) {
    printf("fail to resolve\n");
  } else if (OB_SUCCESS != (ret = resolve("6/3", json_expr))) {
  }
//  static const char* test_file = "./expr/test_raw_expr_resolver.test";
//  static const char* tmp_file = "./expr/test_raw_expr_resolver.tmp";
//  static const char* result_file = "./expr/test_raw_expr_resolver.result";
//
//  std::ifstream if_tests(test_file);
//  ASSERT_TRUE(if_tests.is_open());
//  std::string line;
//  const char* json_expr = NULL;
//  std::ofstream of_result(tmp_file);
//  ASSERT_TRUE(of_result.is_open());
//  int64_t case_id = 0;
//  while (std::getline(if_tests, line)) {
//    of_result << '[' << case_id++ << "] " << line << std::endl;
//    resolve(line.c_str(), json_expr);
//    of_result << json_expr << std::endl;
//  }
//  of_result.close();
//  // verify results
//  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
//  std::ifstream if_result(tmp_file);
//  ASSERT_TRUE(if_result.is_open());
//  std::istream_iterator<std::string> it_result(if_result);
//  std::ifstream if_expected(result_file);
//  ASSERT_TRUE(if_expected.is_open());
//  std::istream_iterator<std::string> it_expected(if_expected);
//  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
//  std::remove(tmp_file);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  init_sql_factories();
  return RUN_ALL_TESTS();
}
