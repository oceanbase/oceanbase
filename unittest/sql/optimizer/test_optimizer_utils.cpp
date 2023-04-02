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

#define USING_LOG_PREFIX SQL_OPT
#include "test_optimizer_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "sql/engine/ob_physical_plan.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"

namespace test
{
using namespace pl;
TestOptimizerUtils::TestOptimizerUtils()
    : case_id_(0), optctx_(NULL), is_json_(false), explain_type_(EXPLAIN_EXTENDED_NOADDR)
{
  memset(schema_file_path_, '\0', 128);
  memcpy(schema_file_path_, "./test_optimizer_schema.sql", strlen("./test_optimizer_schema.sql"));
}

TestOptimizerUtils::~TestOptimizerUtils()
{
}

void TestOptimizerUtils::init()
{
  TestSqlUtils::init();
  //OK(stat_manager_.init(&cs_, &ts_));
  OK(opt_stat_manager_.init(&opt_stat_, NULL, NULL));
}
void TestOptimizerUtils::SetUp()
{
  init();
}

int TestOptimizerUtils::generate_logical_plan(ObResultSet &result, //ObIAllocator &mempool,
                                              ObString &query,
                                              const char *query_str,
                                              ObLogPlan *&logical_plan,
                                              bool &is_select,
                                              bool parameterized)
{
  //TODO 直接调用ob_sql.cpp的接口
    UNUSED(result);
  int ret = OB_SUCCESS;

  /**
   *  Init variable
   */
//  ObSQLSessionInfo session_info;
//  session_info_.set_tenant_id(1);
//  session_info_.set_database_id(combine_id(1,1));

//    ObParser parser(mempool);
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;

  //ObResultPlan result_plan;
  int64_t time_1;
  int64_t time_2;
  int64_t time_2_5;
  int64_t time_3_0;
  int64_t time_3;
  int64_t time_4;
  int64_t memory_parser = 0;
  int64_t memory_resolver = 0;
  int64_t memory_transformer = 0;
  int64_t memory_optimizer = 0;
  int64_t memory_total = 0;

  THIS_WORKER.set_timeout_ts(get_usec() + 100000000);
  /**
   *  2. set resolver context
   */
  ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
  param_store.reuse();
  ObSchemaChecker schema_checker;
  ObStmt *basic_stmt = NULL;
  ObPhysicalPlan *phy_plan = NULL;
  session_info_.set_user_session();
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_FAIL(MockCacheObjectFactory::alloc(
              phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    //  result_plan.schema_checker_ = &schema_checker;
    expr_factory_.destory();
    stmt_factory_.destory();
    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_  = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info_;
    resolver_ctx.database_id_ = 1024;
    resolver_ctx.param_list_ = &param_store;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory_;
    resolver_ctx.stmt_factory_ = &stmt_factory_;
    resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
    //stat_manager_.set_schema_checker(&schema_checker);

    /**
     *  3. set resolver context
     */
    LOG_DEBUG("begin parse sql");
    ObAddr addr;
    LOG_DEBUG("setting local address to 1.1.1.1");
    addr.set_ip_addr("1.1.1.1", 8888);
    memory_total  = allocator_.total();
    memory_parser = allocator_.total();
    time_1 = get_usec();
    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = parser.parse(query, parse_result)))
    {
      LOG_WARN("failed to parse");
    } else {
      if (parameterized) {
        SqlInfo not_param_info;
        ParseNode *root = parse_result.result_tree_->children_[0];
        ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                        ObWrapperAllocator(&allocator_));
        bool is_transform_outline = false;
        if (T_SELECT == root->type_
            || T_INSERT == root->type_
            || T_UPDATE == root->type_
            || T_DELETE == root->type_) {
          if OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator_,
                                                                  session_info_,
                                                                  NULL,
                                                                  parse_result.result_tree_,
                                                                  not_param_info,
                                                                  param_store,
                                                                  NULL,
                                                                  fixed_param_store,
                                                                  is_transform_outline)) {
            LOG_WARN("failed to parameterized", K(ret));
          }
        }
      }
      memory_parser = allocator_.total() - memory_parser;
      memory_resolver = allocator_.total();

      resolver_ctx.query_ctx_->question_marks_count_ = param_store.count();
      ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
      if (OB_SUCC(ret)) {
        pctx->set_original_param_cnt(pctx->get_param_store().count());
        if (OB_FAIL(pctx->init_datum_param_store())) {
          LOG_WARN("fail to init datum param store", K(ret));
        }
      }
      ObResolver resolver(resolver_ctx);
      if ((time_2 = get_usec())
          && OB_SUCCESS != (ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                                   *parse_result.result_tree_->children_[0],
                                                   basic_stmt))) {
        LOG_WARN("failed to resolve");
      } else {
        memory_resolver = allocator_.total() - memory_resolver;
        memory_transformer = allocator_.total();

        time_2_5 = get_usec();
        if (basic_stmt->get_stmt_type() == stmt::T_USE_DATABASE) {
          do_use_database(basic_stmt);
          is_select = false;
        } else {
          ObDMLStmt *stmt = NULL;
          if (NULL == (stmt = dynamic_cast<ObDMLStmt *>(basic_stmt))) {
            _LOG_WARN("Generate stmt error, query=%.*s", parse_result.input_sql_len_,
                      parse_result.input_sql_);
            ret = OB_ERROR;
          } else {
            stmt->get_query_ctx()->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
            LOG_DEBUG("Generate stmt success", "query", stmt->get_query_ctx()->get_sql_stmt());
          }
          if (OB_SUCC(ret)) {
            // transform
            time_3_0 = get_usec();
            ObSchemaChecker schema_checker;
            if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
              LOG_WARN("fail to init schema_checker", K(ret));
            } else {
              ObTransformerCtx ctx;
              ctx.allocator_ = &allocator_;
              ctx.session_info_ = &session_info_;
              ctx.schema_checker_ = &schema_checker;
              ctx.exec_ctx_ = &exec_ctx_;
              ctx.expr_factory_ = &expr_factory_;
              ctx.stmt_factory_ = &stmt_factory_;
              //ctx.stat_mgr_ = &stat_manager_;
              ctx.sql_schema_guard_ = &sql_schema_guard_;
              ctx.self_addr_ = &addr;
              ctx.merged_version_ = OB_MERGED_VERSION_INIT;

              ctx.phy_plan_ = phy_plan;

              ObTransformerImpl transformer(&ctx);
              uint64_t dummy_types;
              ObDMLStmt *sql_stmt = dynamic_cast<ObDMLStmt *>(stmt);
              if (stmt->is_explain_stmt()) {
                sql_stmt = static_cast<ObExplainStmt*>(stmt)->get_explain_query_stmt();
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(transformer.transform(sql_stmt))) {
                  LOG_WARN("Failed to transform statement", K(ret));
                } else {
                  stmt = sql_stmt;
                }
              }
              memory_transformer = allocator_.total() - memory_transformer;
              memory_optimizer = allocator_.total();
              if (OB_SUCC(ret)) {
                time_3 = get_usec();
                ObOptimizerContext *ctx_ptr =
                    static_cast<ObOptimizerContext *>(allocator_.alloc(sizeof(ObOptimizerContext)));
                exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
                optctx_ = new(ctx_ptr) ObOptimizerContext(&session_info_,
                    &exec_ctx_,
                    //schema_mgr_, // schema manager
                    &sql_schema_guard_,
                    //&stat_manager_, // statistics manager
                    NULL, // statistics manager
                    static_cast<ObIAllocator &>(allocator_),
                    &param_store,
                    addr,
                    NULL,
                    stmt->get_query_ctx()->get_global_hint(),
                    expr_factory_,
                    sql_stmt,
                    false,
                    stmt_factory_.get_query_ctx());
                // optctx_->set_opt_stat_manager(&opt_stat_manager_);
                optctx_->set_opt_stat_manager(NULL);
                optctx_->disable_batch_rpc();
                ObTableLocation table_location;
                ret = optctx_->get_table_location_list().push_back(table_location);
                ObOptimizer optimizer(*optctx_);
                if (OB_SUCCESS != (ret = optimizer.optimize(*stmt, logical_plan))) {
                  LOG_WARN("failed to optimize", "SQL", *stmt);
                } else {
                  time_4 = get_usec();
                  memory_optimizer = allocator_.total() - memory_optimizer;
                  memory_total = allocator_.total() - memory_total;
                  LOG_INFO("[SQL MEM USAGE TOTAL]", "parser", memory_parser, "resolver", memory_resolver,
                          "optimizer", memory_optimizer, "total", memory_total, K(query));
                  LOG_DEBUG("succ to generate logical plan");
                  // std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
                  // std::cout << "SQL: " << query_str << std::endl;
                  // std::cout << "parse:     " << time_2 - time_1 << std::endl;
                  // std::cout << "resolve:   " << time_2_5 - time_2 << std::endl;
                  // std::cout << "transform: " << time_3 - time_3_0 << std::endl;
                  // std::cout << "optimize:  " << time_4 - time_3 << std::endl;
                  // std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
                  UNUSED(query_str);
                  UNUSED(time_1);
                  UNUSED(time_2);
                  UNUSED(time_2_5);
                  UNUSED(time_3);
                  UNUSED(time_3_0);
                  UNUSED(time_4);
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

void TestOptimizerUtils::compile_sql(const char* query_str, std::ofstream &of_result) {
  ObResultSet result(session_info_, allocator_);
  of_result << "***************   Case "<< ++case_id_ << "   ***************" << std::endl;
  of_result << std::endl;
  //ObArenaAllocator mempool(ObModIds::OB_SQL_COMPILE, OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObLogPlan *logical_plan;
  ObString sql = ObString::make_string(query_str);
  of_result << "SQL: " << query_str << std::endl;
  LOG_INFO("Case query", K_(case_id), K(query_str));
  bool is_select = true;
  OK(generate_logical_plan(result, sql, query_str, logical_plan, is_select));
  if (is_select) {
    of_result << std::endl;
    char buf[BUF_LEN];
    logical_plan->to_string(buf, BUF_LEN, explain_type_);
    //printf("%s\n", buf);
    of_result << buf << std::endl;

    if (is_json_) {
      // print plan as json
      explain_plan_json(logical_plan, of_result);
    }

    of_result << "*************** Case "<< case_id_ << "(end)  ************** " << std::endl;
    of_result << std::endl;

    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
  //  log_plan_factory_.destroy();
  }
}

void TestOptimizerUtils::explain_plan_json(ObLogPlan *logical_plan, std::ofstream &of_result)
{
  of_result << "start to print plan in json format" << std::endl;
  char output_buf[OB_MAX_LOG_BUFFER_SIZE];
  char buf[OB_MAX_LOG_BUFFER_SIZE];
  int64_t pos = 0;

  output_buf[pos++] = '\n';

  // // 1. generate json
  // Value *val = NULL;
  // logical_plan->get_plan_root()->to_json(output_buf, OB_MAX_LOG_BUFFER_SIZE, pos, val);
  // Tidy tidy(val);
  // LOG_DEBUG("succ to generate json object", K(pos));

  // // 2. from json to string
  // pos = tidy.to_string(buf, OB_MAX_LOG_BUFFER_SIZE);
  // LOG_DEBUG("succ to print json to string", K(pos));

  if (pos < OB_MAX_LOG_BUFFER_SIZE -2) {
    output_buf[pos + 1] = '\n';
    _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos + 2), buf);
  }
}

#define T(query)                                      \
do {                                                \
  compile_sql(query, of_result);                    \
} while(0)

#define T_FAIL(query)                                                        \
do {                                                                         \
  ObLogPlan *logical_plan;                                                   \
  ObResultSet result(session_info_, allocator_);                             \
  ObString sql = ObString::make_string(query);                               \
  bool is_select = true;\
  LOG_INFO("Case query", K(sql));                                            \
  ASSERT_TRUE(OB_SUCCESS != generate_logical_plan(result, sql, query, logical_plan, is_select)); \
} while(0)

#define T_FAIL_AS(query, ret_val)                                                        \
do {                                                                         \
   ObLogPlan *logical_plan;                                                   \
   ObResultSet result(session_info_, allocator_);                              \
   ObString sql = ObString::make_string((query));                               \
   bool is_select = true;\
   int actual_ret = generate_logical_plan(result, sql, (query), logical_plan, is_select); \
   LOG_INFO("actual ret is", K(actual_ret)); \
   ASSERT_TRUE((ret_val) == actual_ret); \
} while(0)


void TestOptimizerUtils::run_test(const char* test_file,
                             const char* result_file,
                             const char* tmp_file,
                             bool default_stat_est)
{
  if (default_stat_est) {
  //  stat_manager_.set_default_stat(true);
  }
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      T(total_line.c_str());
      total_line = "";
    }
  }
  if_tests.close();
  of_result.close();
  formalize_tmp_file(tmp_file);
  TestSqlUtils::is_equal_content(tmp_file, result_file);
}

void TestOptimizerUtils::init_histogram(
    common::ObIAllocator &allocator,
    const ObHistType type,
    const double sample_size,
    const double density,
    const common::ObIArray<int64_t> &repeat_count,
    const common::ObIArray<int64_t> &value,
    const common::ObIArray<int64_t> &num_elements,
    ObHistogram &hist)
{
  // tuple 0: repeat_count
  // tuple 1: value
  // tuple 2: num_elements
  hist.set_type(type);
  hist.set_sample_size(sample_size);
  hist.set_density(density);
  int64_t bucket_cnt = 0;
  hist.prepare_allocate_buckets(allocator, repeat_count.count());
  for (int64_t i = 0; i < repeat_count.count(); i++) {
    hist.get(i).endpoint_num_ = repeat_count.at(i);
    hist.get(i).endpoint_repeat_count_ = num_elements.at(i);
    hist.get(i).endpoint_value_.set_int(value.at(i));
  }
  hist.set_bucket_cnt(repeat_count.count());
}

void TestOptimizerUtils::run_fail_test(const char *test_file)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      T_FAIL(total_line.c_str());
      total_line = "";
    }
    expr_factory_.destory();
    stmt_factory_.destory();
  }
}

void TestOptimizerUtils::run_fail_test_ret_as(const char *test_file, int ret_val)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      T_FAIL_AS(total_line.c_str(), (ret_val));
      total_line = "";
    }
    expr_factory_.destory();
    stmt_factory_.destory();
  }
}

void TestOptimizerUtils::formalize_tmp_file(const char *tmp_file)
{

  std::string cmd_string = "./remove_pointer.py ";
  cmd_string += tmp_file;
  if (0 != system(cmd_string.c_str())) {
    LOG_ERROR_RET(OB_ERR_SYS, "fail formalize tmp file", K(cmd_string.c_str()));
  }
}

int MockCacheObjectFactory::alloc(ObPhysicalPlan *&plan,
                                uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *cache_obj = NULL;
  if (OB_FAIL(alloc(cache_obj, ObLibCacheNameSpace::NS_CRSR, tenant_id))) {
    LOG_WARN("alloc physical plan failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_obj) ||
             OB_UNLIKELY(ObLibCacheNameSpace::NS_CRSR != cache_obj->get_ns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache object is invalid", KPC(cache_obj));
  } else {
    plan = static_cast<ObPhysicalPlan*>(cache_obj);
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    MockCacheObjectFactory::free(cache_obj);
    cache_obj = NULL;
  }
  return ret;
}

int MockCacheObjectFactory::alloc(ObPLFunction *&func,
                                ObLibCacheNameSpace ns,
                                uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *cache_obj = NULL;
  if (OB_FAIL(alloc(cache_obj, ns, tenant_id))) {
    LOG_WARN("alloc cache object failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_obj) ||
             OB_UNLIKELY(ObLibCacheNameSpace::NS_PRCR != cache_obj->get_ns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache object is invalid", KPC(cache_obj));
  } else {
    func = static_cast<ObPLFunction*>(cache_obj);
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    MockCacheObjectFactory::free(cache_obj);
    cache_obj = NULL;
  }
  return ret;
}

int MockCacheObjectFactory::alloc(ObPLPackage *&package,
                                uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *cache_obj = NULL;
  if (OB_FAIL(alloc(cache_obj, ObLibCacheNameSpace::NS_PKG, tenant_id))) {
    LOG_WARN("alloc cache object failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_obj) ||
             OB_UNLIKELY(ObLibCacheNameSpace::NS_PKG != cache_obj->get_ns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache object is invalid", KPC(cache_obj));
  } else {
    package = static_cast<ObPLPackage*>(cache_obj);
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    MockCacheObjectFactory::free(cache_obj);
    cache_obj = NULL;
  }
  return ret;
}

int MockCacheObjectFactory::alloc(ObILibCacheObject *&cache_obj,
                                ObLibCacheNameSpace ns, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id;
  if (ObLibCacheNameSpace::NS_PRCR == ns
    || ObLibCacheNameSpace::NS_PKG == ns
    || ObLibCacheNameSpace::NS_ANON == ns) {
    mem_attr.label_ = ObNewModIds::OB_SQL_PHY_PL_OBJ;
  } else {
    mem_attr.label_ = ObNewModIds::OB_SQL_PHY_PLAN;
  }
  mem_attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
  lib::MemoryContext entity = NULL;

  if (OB_UNLIKELY(ns < ObLibCacheNameSpace::NS_CRSR) || OB_UNLIKELY(ns >= ObLibCacheNameSpace::NS_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("namespace is invalid", K(ns));
  } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(entity,
                                                 lib::ContextParam().set_mem_attr(mem_attr)))) {
    LOG_WARN("create entity failed", K(ret), K(mem_attr));
  } else if (NULL == entity) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL memory entity", K(ret));
  }

  if (OB_SUCC(ret)) {
    WITH_CONTEXT(entity) {
      switch (ns) {
        case ObLibCacheNameSpace::NS_CRSR:
          if (NULL != (buf = entity->get_arena_allocator().alloc(sizeof(ObPhysicalPlan)))) {
            cache_obj = new(buf) ObPhysicalPlan(entity);
          }
          break;
        case ObLibCacheNameSpace::NS_ANON:
        case ObLibCacheNameSpace::NS_PRCR:
          if (NULL != (buf = entity->get_arena_allocator().alloc(sizeof(pl::ObPLFunction)))) {
            cache_obj = new(buf) pl::ObPLFunction(entity);
          }
          break;
        case ObLibCacheNameSpace::NS_PKG:
          if (NULL != (buf = entity->get_arena_allocator().alloc(sizeof(pl::ObPLPackage)))) {
            cache_obj = new(buf) pl::ObPLPackage(entity);
          }
          break;
        default:
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "fail to alloc cache obj", K(ret), KP(buf), K(ns));
    } else {
      cache_obj->inc_ref_count(PLAN_GEN_HANDLE);
      cache_obj->set_tenant_id(tenant_id);
    }
  }

  if (OB_FAIL(ret) && NULL != entity) {
    DESTROY_CONTEXT(entity);
    entity = NULL;
  }
  return ret;
}

void MockCacheObjectFactory::free(ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    //nothing to do
  } else {
    lib::MemoryContext entity = cache_obj->get_mem_context();
    int64_t ref_count = cache_obj->dec_ref_count(PLAN_GEN_HANDLE);
    if (ref_count > 0) {
      //nothing todo
    } else if (ref_count == 0) {
      BACKTRACE(DEBUG, true, "remove cache_obj, ref_count=%ld, cache_obj=%p", ref_count, cache_obj);
      inner_free(cache_obj);
    } else {
      LOG_ERROR("invalid plan ref count", K(ref_count), KP(cache_obj));
    }
  }
}

void MockCacheObjectFactory::inner_free(ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;

  lib::MemoryContext entity = cache_obj->get_mem_context();
  WITH_CONTEXT(entity) { cache_obj->~ObILibCacheObject(); }
  cache_obj = NULL;
  DESTROY_CONTEXT(entity);
}

}//end of namespace test
