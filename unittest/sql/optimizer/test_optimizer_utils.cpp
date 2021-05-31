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
#include "sql/rewrite/ob_transform_simplify.h"
#include "sql/rewrite/ob_transform_project_pruning.h"
#include "sql/engine/ob_physical_plan.h"

namespace test {
TestOptimizerUtils::TestOptimizerUtils()
    : case_id_(0), optctx_(NULL), is_json_(false), explain_type_(EXPLAIN_EXTENDED_NOADDR)
{
  exec_ctx_.get_task_executor_ctx()->set_partition_service(&partition_service_);
  memset(schema_file_path_, '\0', 128);
  memcpy(schema_file_path_, "./test_optimizer_schema.sql", strlen("./test_optimizer_schema.sql"));
}

TestOptimizerUtils::~TestOptimizerUtils()
{}

void TestOptimizerUtils::init()
{
  TestSqlUtils::init();
  OK(stat_manager_.init(&cs_, &ts_));
  OK(opt_stat_manager_.init(&opt_stat_, NULL, NULL));
}
void TestOptimizerUtils::SetUp()
{
  init();
}

int TestOptimizerUtils::generate_logical_plan(ObResultSet& result,  // ObIAllocator &mempool,
    ObString& query, const char* query_str, ObLogPlan*& logical_plan, bool& is_select, bool parameterized)
{
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

  // ObResultPlan result_plan;
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
  ParamStore& param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
  param_store.reuse();
  ObSchemaChecker schema_checker;
  ObStmt* basic_stmt = NULL;
  ObPhysicalPlan* phy_plan = NULL;
  ObCodeGeneratorImpl code_generator(exec_ctx_.get_min_cluster_version());
  session_info_.set_user_session();
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_FAIL(MockCacheObjectFactory::alloc(phy_plan, session_info_.get_effective_tenant_id()))) {
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate phy plan", K(ret));
  } else {
    //  result_plan.schema_checker_ = &schema_checker;
    expr_factory_.destory();
    stmt_factory_.destory();
    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_ = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info_;
    resolver_ctx.database_id_ = 1024;
    resolver_ctx.param_list_ = &param_store;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory_;
    resolver_ctx.stmt_factory_ = &stmt_factory_;
    resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
    stat_manager_.set_schema_checker(&schema_checker);

    /**
     *  3. set resolver context
     */
    LOG_DEBUG("begin parse sql");
    ObAddr addr;
    LOG_DEBUG("setting local address to 1.1.1.1");
    addr.set_ip_addr("1.1.1.1", 8888);
    memory_total = allocator_.total();
    memory_parser = allocator_.total();
    time_1 = get_usec();
    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = parser.parse(query, parse_result))) {
      LOG_WARN("failed to parse");
    } else {
      if (parameterized) {
        SqlInfo not_param_info;
        ParseNode* root = parse_result.result_tree_->children_[0];
        ObMaxConcurrentParam::FixParamStore fixed_param_store(
            OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&allocator_));
        bool is_transform_outline = false;
        if (T_SELECT == root->type_ || T_INSERT == root->type_ || T_UPDATE == root->type_ || T_DELETE == root->type_) {
          if OB_FAIL (ObSqlParameterization::transform_syntax_tree(allocator_,
                          session_info_,
                          NULL,
                          0,
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
      ObResolver resolver(resolver_ctx);
      if ((time_2 = get_usec()) &&
          OB_SUCCESS != (ret = resolver.resolve(
                             ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], basic_stmt))) {
        LOG_WARN("failed to resolve");
      } else {
        memory_resolver = allocator_.total() - memory_resolver;
        memory_transformer = allocator_.total();

        time_2_5 = get_usec();
        if (basic_stmt->get_stmt_type() == stmt::T_USE_DATABASE) {
          do_use_database(basic_stmt);
          is_select = false;
        } else {
          ObDMLStmt* stmt = NULL;
          if (NULL == (stmt = dynamic_cast<ObDMLStmt*>(basic_stmt))) {
            _LOG_WARN("Generate stmt error, query=%.*s", parse_result.input_sql_len_, parse_result.input_sql_);
            ret = OB_ERROR;
          } else if (OB_FAIL(
                         ObSql::calc_pre_calculable_exprs(stmt->get_calculable_exprs(), exec_ctx_, *stmt, *phy_plan))) {
            LOG_WARN("Failed to deal pre calculable exprs", K(ret));
          } else {
            stmt->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
            LOG_DEBUG("Generate stmt success", "query", stmt->get_sql_stmt());
          }
          if (OB_SUCC(ret)) {
            ret = get_hidden_column_value(resolver_ctx, param_store);
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObSql::replace_stmt_bool_filter(exec_ctx_, stmt))) {
              LOG_WARN("Failed to replace stmt bool filer", K(ret));
            }
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
              ctx.partition_location_cache_ = &part_cache_;
              ctx.stat_mgr_ = &stat_manager_;
              ctx.partition_service_ = &partition_service_;
              ctx.sql_schema_guard_ = &sql_schema_guard_;
              ctx.self_addr_ = &addr;
              ctx.merged_version_ = OB_MERGED_VERSION_INIT;

              ctx.phy_plan_ = phy_plan;

              ObTransformerImpl transformer(&ctx);
              uint64_t dummy_types;
              ObDMLStmt* sql_stmt = dynamic_cast<ObDMLStmt*>(stmt);
              if (sql_stmt->is_select_stmt()) {
                ObTransformProjectPruning project_pruning(&ctx);
                if (OB_FAIL(project_pruning.transform(sql_stmt, dummy_types))) {
                  LOG_WARN("Failed to project pruning transform", K(ret));
                }
              } else {
              }
              if (OB_SUCC(ret)) {
                if (NULL != sql_stmt && sql_stmt->is_select_stmt()) {
                  if (OB_FAIL(transformer.transform(sql_stmt))) {
                    LOG_WARN("Failed to transform statement", K(ret));
                  }
                }
              }
              if (OB_SUCC(ret)) {
                stmt = sql_stmt;
                if (OB_FAIL(stmt->distribute_hint_in_query_ctx(&allocator_))) {
                  LOG_WARN("Failed to distribute hint in query context", K(ret));
                } else if (OB_FAIL(stmt->check_and_convert_hint(*ctx.session_info_))) {
                  LOG_WARN("Failed to check and convert hint", K(ret));
                }
              }
              memory_transformer = allocator_.total() - memory_transformer;
              memory_optimizer = allocator_.total();
              if (OB_SUCC(ret)) {
                time_3 = get_usec();
                ObOptimizerContext* ctx_ptr =
                    static_cast<ObOptimizerContext*>(allocator_.alloc(sizeof(ObOptimizerContext)));
                ObQueryHint hint = sql_stmt->get_stmt_hint().get_query_hint();
                exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
                optctx_ = new (ctx_ptr) ObOptimizerContext(&session_info_,
                    &exec_ctx_,
                    // schema_mgr_, // schema manager
                    &sql_schema_guard_,
                    &stat_manager_,  // statistics manager
                    NULL,            // statistics manager
                    &partition_service_,
                    static_cast<ObIAllocator&>(allocator_),
                    &part_cache_,
                    &param_store,
                    addr,
                    NULL,
                    OB_MERGED_VERSION_INIT,
                    hint,
                    expr_factory_,
                    sql_stmt);
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
                  LOG_INFO("[SQL MEM USAGE TOTAL]",
                      "parser",
                      memory_parser,
                      "resolver",
                      memory_resolver,
                      "optimizer",
                      memory_optimizer,
                      "total",
                      memory_total,
                      K(query));
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

void TestOptimizerUtils::compile_sql(const char* query_str, std::ofstream& of_result)
{
  ObResultSet result(session_info_);
  of_result << "***************   Case " << ++case_id_ << "   ***************" << std::endl;
  of_result << std::endl;
  // ObArenaAllocator mempool(ObModIds::OB_SQL_COMPILE, OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObLogPlan* logical_plan;
  ObString sql = ObString::make_string(query_str);
  of_result << "SQL: " << query_str << std::endl;
  LOG_INFO("Case query", K_(case_id), K(query_str));
  bool is_select = true;
  OK(generate_logical_plan(result, sql, query_str, logical_plan, is_select));
  if (is_select) {
    of_result << std::endl;
    char buf[BUF_LEN];
    logical_plan->to_string(buf, BUF_LEN, explain_type_);
    // printf("%s\n", buf);
    of_result << buf << std::endl;

    if (is_json_) {
      // print plan as json
      explain_plan_json(logical_plan, of_result);
    }

    of_result << "*************** Case " << case_id_ << "(end)  ************** " << std::endl;
    of_result << std::endl;

    // deconstruct
    expr_factory_.destory();
    stmt_factory_.destory();
    //  log_plan_factory_.destroy();
  }
}

void TestOptimizerUtils::explain_plan_json(ObLogPlan* logical_plan, std::ofstream& of_result)
{
  of_result << "start to print plan in json format" << std::endl;
  char output_buf[OB_MAX_LOG_BUFFER_SIZE];
  char buf[OB_MAX_LOG_BUFFER_SIZE];
  int64_t pos = 0;

  output_buf[pos++] = '\n';

  // 1. generate json
  Value* val = NULL;
  logical_plan->get_plan_root()->to_json(output_buf, OB_MAX_LOG_BUFFER_SIZE, pos, val);
  Tidy tidy(val);
  LOG_DEBUG("succ to generate json object", K(pos));

  // 2. from json to string
  pos = tidy.to_string(buf, OB_MAX_LOG_BUFFER_SIZE);
  LOG_DEBUG("succ to print json to string", K(pos));

  if (pos < OB_MAX_LOG_BUFFER_SIZE - 2) {
    output_buf[pos + 1] = '\n';
    _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos + 2), buf);
  }
}

#define T(query)                   \
  do {                             \
    compile_sql(query, of_result); \
  } while (0)

#define T_FAIL(query)                                                                              \
  do {                                                                                             \
    ObLogPlan* logical_plan;                                                                       \
    ObResultSet result(session_info_);                                                             \
    ObString sql = ObString::make_string(query);                                                   \
    bool is_select = true;                                                                         \
    LOG_INFO("Case query", K(sql));                                                                \
    ASSERT_TRUE(OB_SUCCESS != generate_logical_plan(result, sql, query, logical_plan, is_select)); \
  } while (0)

#define T_FAIL_AS(query, ret_val)                                                          \
  do {                                                                                     \
    ObLogPlan* logical_plan;                                                               \
    ObResultSet result(session_info_);                                                     \
    ObString sql = ObString::make_string((query));                                         \
    bool is_select = true;                                                                 \
    int actual_ret = generate_logical_plan(result, sql, (query), logical_plan, is_select); \
    LOG_INFO("actual ret is", K(actual_ret));                                              \
    ASSERT_TRUE((ret_val) == actual_ret);                                                  \
  } while (0)

void TestOptimizerUtils::run_test(
    const char* test_file, const char* result_file, const char* tmp_file, bool default_stat_est)
{
  if (default_stat_est) {
    stat_manager_.set_default_stat(true);
    partition_service_.partition_.get_storage(1).set_default_stat(true);
  }
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0)
      continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#')
      continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';')
      continue;
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

void TestOptimizerUtils::init_histogram(const ObHistogram::Type type, const double sample_size, const double density,
    const common::ObIArray<int64_t>& repeat_count, const common::ObIArray<int64_t>& value,
    const common::ObIArray<int64_t>& num_elements, ObHistogram& hist)
{
  // tuple 0: repeat_count
  // tuple 1: value
  // tuple 2: num_elements
  hist.set_type(type);
  hist.set_sample_size(sample_size);
  hist.set_density(density);
  int64_t bucket_cnt = 0;
  for (int64_t i = 0; i < repeat_count.count(); i++) {
    ObHistogram::Bucket* bucket = static_cast<ObHistogram::Bucket*>(allocator_.alloc(sizeof(ObHistogram::Bucket)));
    bucket = new (bucket) ObHistogram::Bucket(repeat_count.at(i), num_elements.at(i));
    hist.get_buckets().push_back(bucket);
    hist.get_buckets().at(hist.get_buckets().count() - 1)->endpoint_value_.set_int(value.at(i));
    bucket_cnt += num_elements.at(i);
  }
  hist.set_bucket_cnt(bucket_cnt);
}

void TestOptimizerUtils::run_fail_test(const char* test_file)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0)
      continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#')
      continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';')
      continue;
    else {
      T_FAIL(total_line.c_str());
      total_line = "";
    }
    expr_factory_.destory();
    stmt_factory_.destory();
  }
}

void TestOptimizerUtils::run_fail_test_ret_as(const char* test_file, int ret_val)
{
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  std::string total_line;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0)
      continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#')
      continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';')
      continue;
    else {
      T_FAIL_AS(total_line.c_str(), (ret_val));
      total_line = "";
    }
    expr_factory_.destory();
    stmt_factory_.destory();
  }
}

void TestOptimizerUtils::formalize_tmp_file(const char* tmp_file)
{

  std::string cmd_string = "./remove_pointer.py ";
  cmd_string += tmp_file;
  if (0 != system(cmd_string.c_str())) {
    LOG_ERROR("fail formalize tmp file", K(cmd_string.c_str()));
  }
}

int MockCacheObjectFactory::alloc(ObPhysicalPlan*& plan, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCacheObject* cache_obj = NULL;
  if (OB_FAIL(alloc(cache_obj, T_CO_SQL_CRSR, tenant_id))) {
    LOG_WARN("alloc physical plan failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_obj) || OB_UNLIKELY(!cache_obj->is_sql_crsr())) {
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

int MockCacheObjectFactory::alloc(ObCacheObject*& cache_obj, ObCacheObjType co_type, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id;
  if (T_CO_PRCR == co_type || T_CO_PKG == co_type || T_CO_ANON == co_type) {
    ret = OB_NOT_SUPPORTED;
  } else {
    mem_attr.label_ = ObNewModIds::OB_SQL_PHY_PLAN;
  }
  mem_attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
  MemoryContext* entity = NULL;

  if (OB_UNLIKELY(co_type < T_CO_SQL_CRSR) || OB_UNLIKELY(co_type >= T_CO_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("co_type is invalid", K(co_type));
  } else if (OB_FAIL(ROOT_CONTEXT.CREATE_CONTEXT(entity, lib::ContextParam().set_mem_attr(mem_attr)))) {
    LOG_WARN("create entity failed", K(ret), K(mem_attr));
  } else if (NULL == entity) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL memory entity", K(ret));
  }

  if (OB_SUCC(ret)) {
    WITH_CONTEXT(entity)
    {
      switch (co_type) {
        case T_CO_SQL_CRSR:
          if (NULL != (buf = entity->get_arena_allocator().alloc(sizeof(ObPhysicalPlan)))) {
            cache_obj = new (buf) ObPhysicalPlan(*entity);
          }
          break;
        case T_CO_ANON:
        case T_CO_PRCR:
        case T_CO_PKG:
          ret = OB_NOT_SUPPORTED;
          break;
        default:
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "fail to alloc cache obj", K(ret), KP(buf), K(co_type));
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

void MockCacheObjectFactory::free(ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    // nothing to do
  } else {
    MemoryContext& entity = cache_obj->get_mem_context();
    int64_t ref_count = cache_obj->dec_ref_count(PLAN_GEN_HANDLE);
    if (ref_count > 0) {
      // nothing todo
    } else if (ref_count == 0) {
      BACKTRACE(DEBUG, true, "remove cache_obj, ref_count=%ld, cache_obj=%p", ref_count, cache_obj);
      inner_free(cache_obj);
    } else {
      LOG_ERROR("invalid plan ref count", K(ref_count), KP(cache_obj));
    }
  }
}

void MockCacheObjectFactory::inner_free(ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;

  MemoryContext& entity = cache_obj->get_mem_context();
  WITH_CONTEXT(&entity)
  {
    cache_obj->~ObCacheObject();
  }
  cache_obj = NULL;
  DESTROY_CONTEXT(&entity);
}

}  // end of namespace test
