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
#include "test_pl_utils.h"
#include "pl/parser/ob_pl_parser.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_code_generator.h"
#include "pl/ob_pl_package.h"
#include "observer/ob_server.h"
#include "sql/resolver/ob_resolver_utils.h"

using namespace oceanbase::pl;

#define CLUSTER_VERSION_140 (oceanbase::common::cal_version(1, 4, 0, 0))
#define BUF_LEN 102400 // 100K
namespace test
{

void TestPLUtils::resolve_test(const char* test_file,
                               const char* result_file,
                               const char* tmp_file)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) {
      resolve_pl(total_line.c_str(), of_result);
      total_line = "";
    } else {
      std::size_t begin = line.find_first_not_of('\t');
      if (line.at(begin) == '#') continue;
      std::size_t end = line.find_last_not_of('\t');
      std::string exact_line = line.substr(begin, end - begin + 1);
      total_line += exact_line;
      total_line += " ";
    }
  }
  if_tests.close();
  of_result.close();
  std::cout << "diff -u " << tmp_file << " " << result_file << std::endl;
  is_equal_content(tmp_file, result_file);
}

void TestPLUtils::compile_test(const char* test_file,
                               const char* result_file,
                               const char* tmp_file)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) {
      compile_pl(total_line.c_str(), of_result);
      total_line = "";
    } else {
      std::size_t begin = line.find_first_not_of('\t');
      if (line.at(begin) == '#') continue;
      std::size_t end = line.find_last_not_of('\t');
      std::string exact_line = line.substr(begin, end - begin + 1);
      total_line += exact_line;
      total_line += " ";
    }
  }
  if_tests.close();
  of_result.close();
  std::cout << "diff -u " << tmp_file << " " << result_file << std::endl;
//  is_equal_content(tmp_file, result_file);
  std::remove(tmp_file);
}

void TestPLUtils::resolve_pl(const char* pl_str, std::ofstream &of_result)
{
  of_result << "***************   Case "<< ++case_id_ << "   ***************" << std::endl;
  of_result << std::endl;
  ObString sql = ObString::make_string(pl_str);
  of_result << "SQL: " << pl_str << std::endl;
  LOG_INFO("Case query", K_(case_id), K(pl_str));
  ObPLFunctionAST func(allocator_);
  sql::ObRawExprFactory expr_factory(allocator_);
  OK(do_resolve(pl_str, expr_factory, func));
  of_result << std::endl;
  char buf[BUF_LEN];
  func.to_string(buf, BUF_LEN);
  //printf("%s\n", buf);
  of_result << buf << std::endl;
  of_result << "*************** Case "<< case_id_ << "(end)  ************** " << std::endl;
  of_result << std::endl;

  // deconstruct
  expr_factory_.destory();
}

void TestPLUtils::compile_pl(const char* pl_str, std::ofstream &of_result)
{
  of_result << "***************   Case "<< ++case_id_ << "   ***************" << std::endl;
  of_result << std::endl;
  ObString sql = ObString::make_string(pl_str);
  of_result << "SQL: " << std::endl << pl_str << std::endl;
  LOG_INFO("Case query", K_(case_id), K(pl_str));
  FILE *tmp = freopen("tmp", "w+", stderr);
  ObPLFunction func(allocator_);
  OK(do_compile(pl_str, func));
  fclose(tmp);
  char buf[BUF_LEN];
  func.to_string(buf, BUF_LEN);
  of_result << "FUNCTION:" << std::endl << buf << std::endl;
  std::ifstream in("tmp");
  std::ostringstream stream;
  char ch;
  while (!in.eof()) { in.get(ch); stream.put(ch); }
  of_result << ">>>>>>>>>>>>>>>> IR Code <<<<<<<<<<<<<<<<" <<std::endl << stream.str() <<std::endl;
  of_result << "*************** Case "<< case_id_ << "(end)  ************** " << std::endl;
  of_result << std::endl;

  // deconstruct
  expr_factory_.destory();
}

int TestPLUtils::do_resolve(const char* pl_str, sql::ObRawExprFactory &expr_factory, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ParseResult parse_result;
  ObStmtNodeTree *pl_tree = NULL;
  ObSchemaGetterGuard schema_guard;
  ObPLPackageGuard package_guard;
  if (OB_FAIL(OBSERVER.schema_service_.get_schema_guard(schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else {
    ObPLParser parser(allocator_);
    ObPLResolver resolver(allocator_, session_info_, schema_guard, package_guard, OBSERVER.sql_proxy_, expr_factory, NULL, false);
    if (OB_FAIL(parser.parse(pl_str, pl_str, parse_result))) {
      LOG_WARN("failed to parse pl", K(pl_str), K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_)
        || T_STMT_LIST != parse_result.result_tree_->type_
        || OB_ISNULL(parse_result.result_tree_->children_[0])
        || T_SP_CREATE != parse_result.result_tree_->children_[0]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid pl parse result", K(pl_str), K(ret));
    } else {
      pl_tree = parse_result.result_tree_->children_[0];
      ObStmtNodeTree *name = pl_tree->children_[0];
      if (OB_ISNULL(name)) {
        LOG_WARN("Invalid pl", K(name), K(pl_str), K(ret));
      } else if (T_SP_NAME != name->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid pl name", K(name->type_), K(pl_str), K(ret));
      } else if (OB_ISNULL(name->children_[1])) {
        LOG_WARN("Invalid pl", K(name->children_[1]), K(pl_str), K(ret));
      } else if (T_IDENT != name->children_[1]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid pl name", K(name->children_[1]->type_), K(pl_str), K(ret));
      } else {
        func.set_name(ObString(name->children_[1]->str_len_, name->children_[1]->str_value_));
      }

      if (OB_SUCC(ret)) {
        ObStmtNodeTree *params = pl_tree->children_[1];
        if (NULL == params) {
          //skip
        } else if (T_SP_PARAM_LIST != params->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid pl params", K(params->type_), K(pl_str), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < params->num_child_; ++i) {
            ObStmtNodeTree *param = params->children_[i];
            if (OB_ISNULL(param)) {
              LOG_WARN("Invalid pl", K(param), K(pl_str), K(ret));
            } else if (T_SP_PARAM != param->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Invalid pl param", K(param->type_), K(i), K(pl_str), K(ret));
            } else {
              ObStmtNodeTree *param_name = param->children_[0];
              if (OB_ISNULL(param_name)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("name node is NULL", K(param_name), K(ret));
              } else if (T_IDENT != param_name->type_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid name node", K(i), K(param_name->type_), K(ret));
              } else { /*do nothing*/ }

              if (OB_SUCC(ret)) {
                ObStmtNodeTree *param_type = param->children_[1];
                ObDataType data_type;
                ObString ident_name;
                if (OB_ISNULL(param_type)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("name node is NULL", K(param_type), K(ret));
                } else if (OB_FAIL(ObResolverUtils::resolve_data_type(*param_type, ident_name, data_type))) {
                  LOG_WARN("failed to resolve type", K(param_type), K(ret));
                } else {
                  ObObjParam value;
                  if (OB_FAIL(func.add_argument(ObString(param_name->str_len_, param_name->str_value_),
                                                data_type,
                                                NULL))) {
                    LOG_WARN("failed to add argument", K(param_name->str_value_), K(value), K(ret));
                  }
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObDataType res_type;
        res_type.set_obj_type(common::ObNullType);
        func.set_ret_type(res_type); //无返回值
      }

      if (OB_SUCC(ret)) {
        ObStmtNodeTree *body = pl_tree->children_[3]; //获取body
        if (OB_ISNULL(body)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pl body is NULL", K(pl_tree), K(ret));
        } else if (OB_FAIL(resolver.init(func))) {
          LOG_WARN("failed to init resolver", K(body), K(ret));
        } else if (OB_FAIL(resolver.resolve(body, func))) {
          LOG_WARN("failed to analyze pl body", K(body), K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int TestPLUtils::do_compile(const char *pl_str, ObPLFunction &func)
{
  int ret = OB_SUCCESS;
  ObPLFunctionAST func_ast(allocator_);
  if (OB_FAIL(do_resolve(pl_str, func_ast.get_expr_factory(), func_ast))) {
    LOG_WARN("failed to do resolve", K(pl_str), K(func_ast), K(ret));
  } else {
#ifdef USE_MCJIT
    ObPLCodeGenerator cg(allocator_, &session_info_);
#else
    ObPLCodeGenerator cg(func.get_allocator(), func.get_expressions(), func.get_helper(), func.get_di_helper());
#endif
    if (OB_FAIL(cg.init(func_ast, false))) {
      LOG_WARN("failed to init code generator", K(ret));
    } else if (cg.generate(func_ast, func)) {
      LOG_WARN("failed to code generate for stmt", K(func_ast), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

void TestPLUtils::init()
{
  TestSqlUtils::init();
  static const int64_t PS_BUCKET_NUM = 64;
  session_info_.ps_session_info_map_.create(hash::cal_next_prime(PS_BUCKET_NUM),
                                                 ObModIds::OB_HASH_BUCKET_PS_SESSION_INFO,
                                                 ObModIds::OB_HASH_NODE_PS_SESSION_INFO);

  session_info_.is_inited_ = true;
}

void TestPLUtils::destroy()
{
  TestSqlUtils::destroy();
}

}
