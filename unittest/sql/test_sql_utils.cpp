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
#include "test_sql_utils.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_cluster_version.h"
#include "observer/ob_server.h"
#include "sql/ob_sql_utils.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "share/ob_tenant_mgr.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "optimizer/ob_mock_opt_stat_manager.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_cache.h"
#define CLUSTER_VERSION_2100 (oceanbase::common::cal_version(2, 1, 0, 0))
#define CLUSTER_VERSION_2200 (oceanbase::common::cal_version(2, 2, 0, 0))
using namespace oceanbase::observer;
//c funcs
namespace test
{

CmdLineParam clp;
bool comparisonFunc(const char *c1, const char *c2)
{
  return strcmp(c1, c2) < 0;
}

void load_sql_file(const char* file_name)
{
  if (file_name != NULL){
    if ( strcmp(".", file_name) != 0
         && strcmp("..", file_name) != 0
         && strncmp("test_resolver", file_name, strlen("test_resolver")) == 0 ) {
      snprintf(clp.file_names[clp.file_count++],
               strlen(file_name) - 3,  //strlen("test")-1
               "%s",
               file_name);
      _OB_LOG(INFO, "add file %s to cmd", clp.file_names[clp.file_count-1]);
      clp.file_names_vector.push_back(clp.file_names[clp.file_count-1]);
    }
  }
}

void load_all_sql_files(const char* directory_name)
{
  DIR *dp = NULL;
  if((dp  = opendir(directory_name)) == NULL) {
     _OB_LOG_RET(ERROR, OB_ERR_SYS, "error open file, %s", directory_name);
      return;
  }
  struct dirent *dirp = NULL;
  clp.file_count = 0;
  while ((dirp = readdir(dp)) != NULL) {
      load_sql_file(dirp->d_name);
  }
  std::sort(clp.file_names_vector.begin(), clp.file_names_vector.end(), comparisonFunc);
  for (std::vector<const char*>::iterator iter = clp.file_names_vector.begin(); iter != clp.file_names_vector.end(); ++iter){
    _OB_LOG(INFO, "sorted %s", *iter);
  }
  closedir(dp);
}

void print_help_msg (const char* exe_name)
{
  //TODO(yaoying.yyy)
  const char* msg = "Put you file test_resolver_xxx.test in the sql sub directory.\n\
 Then add the xxx to the command line param like ./test_resolver -c xxx,\n\
 It will resolve the sql in ./sql/test_resolver_xxx.test and print the result to ./result/test_resolver_xxx.tmp\n\
 If you don't config any param, it will resolver all the file in ./sql directory! \
 ./test_resolver -i can help to input sql from the command!";

  fprintf (stderr, "%s", msg);
  fprintf (stderr, "\nUsage: %s  [-c clause_type]\n\n", exe_name);
}

void parse_cmd_line_param(int argc, char *argv[], CmdLineParam &clp)
{
  if (1 == argc){
    load_all_sql_files("./sql");
  }else{
    int opt = 0;
    const char* opt_string = "hc:idrs:";
    struct option longopts[] =
    {
      { "help", 0, NULL, 'h' },        //help message
      { "clause_type", 0, NULL, 'c' }, //use in ./test_resolver -c select  // will run the test in sql/test_resolver_select.test
      { "input", 0, NULL, 'i'},        // ./test_resolver -i will help to quick test a sql in command line
      { "detail", 0, NULL, 'd'},       // ./test_resolver -id will print the detail info in json format in test_resolver.schema
      { "record", 0, NULL, 'r'},       // ./test_resolver -r will remove tmp file to result file
      { "sql_mode", 0, NULL, 's' },
      { 0, 0, 0, 0 }
    };

    memset(&clp, 0, sizeof(clp));
    //clp.reset();
    while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
      _OB_LOG(DEBUG, "opt=%d,optarg=%s\n", opt, optarg);
      switch (opt) {
      case 'h': {
        print_help_msg("test_resolver");
        exit(0);
      }
        //add test_resolver_xxx.test
      case 'c': {
        char tmp_file_name[256];
        snprintf(tmp_file_name,
                 strlen("test_resolver_") + strlen(optarg) + 7,
                 "test_resolver_%s.test",
                 optarg);
        _OB_LOG(INFO, "%s", tmp_file_name);
        load_sql_file(tmp_file_name);
        break;
      }
      case 'i': {
        clp.test_input_from_cmd = true;
        break;
      }
      case 'd': {
        clp.print_schema_detail_info = true;
        break;
      }
      case 'r': {
        clp.record_test_result = true;
        break;
      }
      case 's': {
        if (casesame_cstr(optarg, "oracle")) {
          clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
        }
        break;
      }
      default: {
        //TODO(yaoying.yyy)
        print_help_msg("test_resolver");
        load_all_sql_files("./sql");
        break;
        //exit(1);
      }
      }
    }
  }
}

TestSqlUtils::TestSqlUtils()
    : //next_user_table_id_(OB_MIN_USER_TABLE_ID),
      next_user_table_id_map_(),
      sys_user_id_(OB_SYS_USER_ID),
      next_user_id_(OB_MIN_USER_OBJECT_ID),
      sys_database_id_(OB_SYS_DATABASE_ID),
      next_user_database_id_(OB_MIN_USER_OBJECT_ID),
      sys_tenant_id_(OB_SYS_TENANT_ID),
      schema_version_(2),
      //next_user_tenant_id_(OB_USER_TENANT_ID),
      allocator_(ObModIds::TEST),
      expr_factory_(allocator_),
      stmt_factory_(allocator_),
      log_plan_factory_(allocator_),
      sql_ctx_(),
      exec_ctx_(allocator_),
      param_list_( (ObWrapperAllocator(allocator_)) )
{
    memset(schema_file_path_, '\0', 128);
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    (oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200));
    ObServer &observer = ObServer::get_instance();
    int ret = OB_SUCCESS;
    if (OB_FAIL(observer.init_tz_info_mgr())) {
      LOG_ERROR("init tz_info_mgr fail", K(ret));
    } else if (OB_FAIL(observer.init_global_context())) {
      LOG_ERROR("init global context fail", K(ret));
    }
}

void TestSqlUtils::init()
{
  int64_t ret = OB_SUCCESS;
  //common::ObSessionDIBuffer::get_instance().init(OB_MAX_SERVER_SESSION_CNT, 4);
  //common::ObDITenantCache::get_instance().init(100000, 4);
  schema_service_ = new MockSchemaService();
  ASSERT_TRUE(schema_service_);
  ObVirtualTenantManager::get_instance().init();
  ObVirtualTenantManager::get_instance().add_tenant(sys_tenant_id_);
  ObVirtualTenantManager::get_instance().set_tenant_mem_limit(sys_tenant_id_, 1024L * 1024L * 1024L, 1024L * 1024L * 1024L);
  if (OB_SUCCESS != (ret = ObPreProcessSysVars::init_sys_var())) {
    _OB_LOG(WARN, "PreProcessing system value init failed, ret=%ld", ret);
    ASSERT_TRUE(0);
  } else if (OB_FAIL(schema_service_->init())) {
    _OB_LOG(WARN, "schema_service_ init fail, ret=%ld", ret);
    ASSERT_TRUE(0);
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard_, schema_version_))) {
    _OB_LOG(WARN, "schema_guard init fail, ret=%ld", ret);
    ASSERT_TRUE(0);
  } else {
    sql_schema_guard_.set_schema_guard(&schema_guard_);
    ObString tenant("sql_test");
    ASSERT_TRUE(OB_SUCCESS == session_info_.init_tenant(tenant, sys_tenant_id_));

    ObArenaAllocator *allocator = NULL;
    uint32_t version = 0;
    if (OB_SUCCESS != (ret = session_info_.test_init(version, 0, 0, allocator)) ){
      _OB_LOG(ERROR, "%s", "init session_info error!");
      ASSERT_TRUE(0);
    } else {
      exec_ctx_.set_my_session(&session_info_);
    }

    if (OB_SUCC(ret)) {
      exec_ctx_.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_VERSION_2200);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
      OB_LOG(WARN, "Create plan ctx error", K(ret));
      ASSERT_TRUE(0);
    } else if (OB_SUCCESS != (ret = next_user_table_id_map_.create(16, "HashBucAltTabMa"))) {
      _OB_LOG(WARN, "create user table id map failed, ret=%ld", ret);
      ASSERT_TRUE(0);
    } else {
      OK(session_info_.load_default_sys_variable(true, true));
      //OK(session_info_.load_sys_variable(sql_mode, type, value, ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE));
      const uint64_t tenant_id = 1;
      ASSERT_TRUE(OB_SUCCESS == session_info_.init_tenant(tenant, tenant_id));
      session_info_.set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID);
      session_info_.set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT | OB_PRIV_BOOTSTRAP);
      session_info_.set_default_database(OB_SYS_DATABASE_NAME, CS_TYPE_UTF8MB4_GENERAL_CI);
      ObObj obj;
      obj.set_int(1);
      ASSERT_TRUE(OB_SUCCESS == session_info_.update_sys_variable_by_name(OB_SV_ENABLE_AGGREGATION_PUSHDOWN, obj));
      create_system_db();
      create_system_table();
      // create schema
      load_schema_from_file(schema_file_path_);
      // prevent interference between test schema and the following DDL test cases
      next_user_table_id_map_.set_refactored(sys_tenant_id_, OB_MIN_USER_OBJECT_ID + 100, 1 /*replace*/);

      //close the recyclebin
      ObObj obj2;
      obj2.set_bool(false);
      ASSERT_TRUE(OB_SUCCESS == session_info_.update_sys_variable_by_name(OB_SV_RECYCLEBIN, obj2));
      int64_t default_collation = 45;  // utf8mb4_general_ci
      ASSERT_TRUE(OB_SUCCESS == session_info_.update_sys_variable(SYS_VAR_COLLATION_CONNECTION, default_collation));
      ObAddr addr;
      ObPlanCache* pc = new ObPlanCache();
      ObPsCache* ps = new ObPsCache();
      ObPCMemPctConf pc_mem_conf;
      if (OB_FAIL(pc->init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
        LOG_WARN("failed to init request manager", K(ret));
      } else if (OB_FAIL(ps->init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
        LOG_WARN("failed to init request manager", K(ret));
      } else if (OB_FAIL(session_info_.get_pc_mem_conf(pc_mem_conf))) {
        _OB_LOG(WARN,"fail to get pc mem conf, ret=%ld", ret);
        ASSERT_TRUE(0);
      } else {
        session_info_.set_plan_cache(pc);
        session_info_.set_ps_cache(ps);
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestSqlUtils::destroy()
{
  ObPlanCache* pc = session_info_.get_plan_cache();
  ObPsCache* ps = session_info_.get_ps_cache();
  sys_user_id_ = OB_SYS_USER_ID;
  next_user_id_ = OB_MIN_USER_OBJECT_ID;
  sys_database_id_ = OB_SYS_DATABASE_ID;
  next_user_database_id_= OB_MIN_USER_OBJECT_ID;
  sys_tenant_id_ = OB_SYS_TENANT_ID;
  next_user_table_id_map_.destroy();
  session_info_.~ObSQLSessionInfo();
  new (&session_info_) ObSQLSessionInfo();
  stmt_factory_.destory();
  expr_factory_.destory();
  exec_ctx_.~ObExecContext();
  new (&exec_ctx_) ObExecContext(allocator_);
  schema_guard_.~ObSchemaGetterGuard();
  new (&schema_guard_) ObSchemaGetterGuard();
  // destroy
  if (NULL != schema_service_) {
    delete schema_service_;
  }
  if (NULL != ps) {
    ps->destroy();
    delete ps;
  }
  if (NULL != pc) {
    pc->destroy();
    delete pc;
  }
  ObKVGlobalCache::get_instance().destroy();
}

void TestSqlUtils::load_schema_from_file(const char *file_path) {
  if (file_path != NULL && strncmp(file_path, "", 1) != 0){
    _OB_LOG(INFO, "file_path=%s", file_path);
    std::ifstream if_schema(file_path);
    ASSERT_TRUE(if_schema.is_open());
    std::string line;
    while (std::getline(if_schema, line)) {
      ObStmt *stmt = NULL;
      ASSERT_NO_FATAL_FAILURE(do_load_sql(line.c_str(), stmt, clp.print_schema_detail_info, JSON_FORMAT));
      stmt_factory_.destory();
      expr_factory_.destory();
    }
  }
}

void TestSqlUtils::do_load_sql(
    const char *query_str,
    ObStmt *&stmt,
    bool is_print,
    enum ParserResultFormat format,
    int64_t expect_error,
    int64_t case_line)
{
  //ObStmt *stmt = NULL;
  ObString real_query = ObString::make_string(query_str).trim();
  if (real_query.length() > 0 && *real_query.ptr() != '#') {
    //ignore empty query and comment
    _OB_LOG(INFO, "query_str: %s", query_str);
    ASSERT_NO_FATAL_FAILURE(do_resolve(query_str, stmt,is_print, format, expect_error, true, true, case_line));
    //ASSERT_FALSE(HasFatalFailure()) << "query_str: " << query_str << std::endl;
    if (!stmt) {
      // expect error case
      _OB_LOG_RET(WARN, OB_ERROR, "fail to resolve query_str: %s", query_str);
    } else if (OB_SUCCESS != expect_error) {
    } else {
      if (stmt->get_stmt_type() == stmt::T_CREATE_TABLE) {
        do_create_table(stmt);
      } else if (stmt->get_stmt_type() == stmt::T_CREATE_INDEX) {
        do_create_index(stmt);
      } else if (stmt->get_stmt_type() == stmt::T_CREATE_DATABASE) {
        do_create_database(stmt);
      } else if (stmt->get_stmt_type() == stmt::T_USE_DATABASE) {
        do_use_database(stmt);
      } else if (stmt->get_stmt_type() == stmt::T_CREATE_USER) {
        do_create_user(stmt);
      } else if (stmt->get_stmt_type() == stmt::T_DROP_TABLE) {
        do_drop_table(stmt);
      }
    }
  }
}


void TestSqlUtils::do_resolve(
    const char* query_str,
    ObStmt *&stmt,
    bool is_print,
    enum ParserResultFormat format,
    int64_t expect_error,
    bool parameterized,
    bool need_replace_param_expr,
    int64_t case_line)
{
  UNUSED(need_replace_param_expr);
  ObSQLMode mode = lib::is_oracle_mode() ? (SMO_ORACLE | DEFAULT_ORACLE_MODE) : SMO_DEFAULT;
  ObParser parser(allocator_, mode);
  ObString query = ObString::make_string(query_str);
  ParseResult parse_result;
  ObArenaAllocator tmp_alloc;
  OK(parser.parse(query, parse_result));
  if (true){
    if (JSON_FORMAT == format) {
      _OB_LOG(INFO, "%s", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    } else{
      _OB_LOG(INFO, "%s", CSJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)));
    }
  }
  ParseNode *root = parse_result.result_tree_->children_[0];
  ParamStore param_store ( (ObWrapperAllocator(tmp_alloc)) );
  ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&allocator_));
  bool is_transform_outline = false;
  if (parameterized) {
    SqlInfo not_param_info;
    if (T_SELECT == root->type_
        || T_INSERT == root->type_
        || T_UPDATE == root->type_
        || T_DELETE == root->type_) {
      OK(ObSqlParameterization::transform_syntax_tree(allocator_,
                                                      session_info_,
                                                      NULL,
                                                      parse_result.result_tree_,
                                                      not_param_info,
                                                      param_store,
                                                      NULL,
                                                      fixed_param_store,
                                                      is_transform_outline));
    }
  }
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  schema_checker.init(schema_guard_);
  //schema_checker.init(*schema_mgr_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_  = &allocator_;
  resolver_ctx.schema_checker_ = &schema_checker;
  resolver_ctx.session_info_ = &session_info_;
  resolver_ctx.param_list_ = &param_store;
  resolver_ctx.database_id_ = 1024;
  resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
  resolver_ctx.expr_factory_ = &expr_factory_;
  resolver_ctx.stmt_factory_ = &stmt_factory_;
  resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
  resolver_ctx.query_ctx_->question_marks_count_ = param_store.count();
  ObResolver resolver(resolver_ctx);
  ret = resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt);
  if (OB_SUCC(ret)) {
    get_hidden_column_value(resolver_ctx, param_list_);
  }
  _OB_LOG(INFO, "expect = %ld, actual = %d", expect_error, -ret);
  if (OB_SUCCESS == ret && stmt->get_stmt_type() == stmt::T_CREATE_TABLE) {
    uint64_t database_id = OB_INVALID_ID;
    ObCreateTableStmt *create_table_stmt = dynamic_cast<ObCreateTableStmt*>(stmt);
    share::schema::ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    if (table_schema.get_part_array() == NULL) {
      int64_t part_num = table_schema.get_first_part_num();
      if (part_num >= 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
          char *name = new char[10];
          if (name !=  NULL) {
            snprintf(name, 10, "p%d", static_cast<int32_t>(i));
            ObPartition *part = new ObPartition;
            ObString name_string(name);
            part->set_part_name(name_string);
            part->set_part_id(i);
            part->set_part_idx(i);
            if (OB_FAIL(table_schema.add_partition(*part))) {
              _OB_LOG(WARN, "add partition to table schema failed, ret %d", ret);
            }
          }
        }
      }
    }
    common::ObString database_name = create_table_stmt->get_create_table_arg().db_name_;
    OK(schema_guard_.get_database_id(table_schema.get_tenant_id(), database_name, database_id));
    OB_ASSERT(OB_INVALID_ID != database_id);
    uint64_t table_id = OB_INVALID_ID;
    OK(schema_guard_.get_table_id(table_schema.get_tenant_id(), database_id, table_schema.get_table_name(), false, ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
    if (OB_INVALID_ID != table_id && !create_table_stmt->get_create_table_arg().if_not_exist_) {
      ret = OB_ERR_TABLE_EXIST;
    }
  }
  if (ret != -expect_error && case_line > 0) {
    fprintf(stderr, "sql unittest case failed at line:%ld\n", case_line);
  }
  ASSERT_EQ(expect_error, -ret);
  if (is_print){
    _OB_LOG(INFO, "%s", CSJ(*stmt));
  }
  ret = OB_SUCCESS;
  parser.free_result(parse_result);
}

int TestSqlUtils::create_system_table()
{
  int ret = OB_SUCCESS;
  // ObString sys_database_name(OB_SYS_DATABASE_NAME);
  // session_info_.set_database_name(sys_database_name);
  //array,each type is a pointer to function pointer
  typedef int (*schema_init_func)(ObTableSchema &table_schema);
  const schema_init_func *creator_ptr_array[] = { core_table_schema_creators,
    sys_table_schema_creators,
    virtual_table_schema_creators,
    NULL };
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(schema_guard_.get_tenant_info(sys_tenant_id_, tenant_schema))) {
    _OB_LOG(WARN, "get tenant info fail, ret %d", ret);
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_SCHEMA_ERROR;
    _OB_LOG(WARN, "tenant schema is null, ret %d", ret);
  }
  for (const schema_init_func **creator_ptr_ptr = creator_ptr_array;
       OB_SUCCESS == ret && NULL != *creator_ptr_ptr; ++creator_ptr_ptr) {
    for (const schema_init_func *creator_ptr = *creator_ptr_ptr;
         OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
      ObTableSchema table_schema;
      if (OB_SUCCESS != (ret = (*creator_ptr)(table_schema))) {
        _OB_LOG(WARN, "create table schema fialed, ret %d", ret);
        ret = OB_SCHEMA_ERROR;
      } else {
        table_schema.set_database_id(table_schema.get_database_id());
        table_schema.set_table_id(table_schema.get_table_id());
        if (OB_FAIL(add_table_schema(table_schema))) {
          _OB_LOG(WARN, "add table schema fail, ret %d", ret);
        }
      }
      _OB_LOG(INFO, "do_create_table table_name=[%s], table_id=[%lu], tenant_id=[%lu], database_id=[%lu]",
              table_schema.get_table_name(),
              table_schema.get_table_id(),
              table_schema.get_tenant_id(),
              table_schema.get_database_id());
    }
  }

  for (int i = 0; OB_SUCC(ret) && NULL != information_schema_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    const schema_init_func creator_ptr = information_schema_table_schema_creators[i];
    if (OB_SUCCESS != (ret = creator_ptr(table_schema))) {
      _OB_LOG(WARN, "create table schema failed !, ret=%d", ret);
    } else {
      table_schema.set_database_id(next_user_database_id_ + 1);
      table_schema.set_table_id(table_schema.get_table_id());
      if (OB_FAIL(add_table_schema(table_schema))) {
        _OB_LOG(WARN, "add table schema fail, ret %d", ret);
      }
    }
    _OB_LOG(INFO, "do_create_table table_name=[%s], table_id=[%lu], tenant_id=[%lu], database_id=[%lu]",
            table_schema.get_table_name(),
            table_schema.get_table_id(),
            table_schema.get_tenant_id(),
            table_schema.get_database_id());
  }
  return ret;
}
void TestSqlUtils::do_create_table(const char *query_str)
{

  ObStmt *stmt = NULL;
  _OB_LOG(INFO, "query_str: %s", query_str);
  do_resolve(query_str, stmt, false, JSON_FORMAT);
  ASSERT_TRUE(NULL != stmt);
  do_create_table(stmt);
  stmt_factory_.destory();
  expr_factory_.destory();
}

void TestSqlUtils::do_create_table(ObStmt *&stmt)
{
  // add the created table schema
  ObCreateTableStmt *create_table_stmt = dynamic_cast<ObCreateTableStmt*>(stmt);
  ObSEArray<ObColDesc, 16> col_ids;
  uint64_t database_id = OB_INVALID_ID;
  OK(ObPartitionExecutorUtils::calc_values_exprs(exec_ctx_, *create_table_stmt));
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, table_schema.assign(create_table_stmt->get_create_table_arg().schema_));
  _OB_LOG(INFO, "table_schema=%s", CSJ(table_schema));
  common::ObString database_name = create_table_stmt->get_create_table_arg().db_name_;

  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  OK(schema_guard_.get_database_id(table_schema.get_tenant_id(), database_name, database_id));
  OB_ASSERT(OB_INVALID_ID != database_id);
  uint64_t table_id = OB_INVALID_ID;
  OK(schema_guard_.get_table_id(table_schema.get_tenant_id(), database_id, table_schema.get_table_name(), false, ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
  if (OB_INVALID_ID != table_id) {
    _OB_LOG(INFO, "guard have table %s", table_schema.get_table_name());
  } else {
    //combine the database_id and tenant_id
    table_schema.set_database_id(database_id);
    //get the next_table_id of this tenant and database
    uint64_t next_table_id = get_next_table_id(table_schema.get_tenant_id());
    table_schema.set_table_id(next_table_id);
    //table_schema.set_data_table_id( combine_id(next_user_tenant_id_, next_table_id));

    ASSERT_EQ(OB_SUCCESS, table_schema.get_column_ids(col_ids));
    for (int64_t i = 0; i < col_ids.count(); ++i) {
      const ObColumnSchemaV2 *col = table_schema.get_column_schema(col_ids.at(i).col_id_);
      const_cast<ObColumnSchemaV2*>(col)->set_table_id(table_schema.get_table_id());
    }
    OK(add_table_schema(table_schema));
    _OB_LOG(INFO, "do_create_table table_name=[%s], table_id=[%lu], tenant_id=[%lu], database_id=[%lu]",
           table_schema.get_table_name(),
            table_schema.get_table_id(),
            table_schema.get_tenant_id(),
            table_schema.get_tenant_id());
  }
}

int TestSqlUtils::add_table_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable= NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_tenant_info(table_schema.get_tenant_id(), tenant_schema))) {
      OB_LOG(WARN, "get tenant info failed", K(table_schema), K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "tenant schema is null", K(ret));
    } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(table_schema.get_tenant_id(), sys_variable))) {
      OB_LOG(WARN, "get sys variable failed", K(sys_variable), K(ret));
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "sys variable schema is null", K(ret));
    } else {
      ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
      if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX)   {
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "invalid tenant mod, ret %d", ret);
      } else {
        table_schema.set_name_case_mode(local_mode);
        table_schema.set_schema_version(schema_version_);
        if (OB_FAIL(schema_service_->add_table_schema(table_schema, schema_version_++))) {
          _OB_LOG(WARN, "add table schema fail, ret %d", ret);
        }
      }
    }
  }
  return ret;
}

void TestSqlUtils::do_drop_table(ObStmt *&stmt)
{
  // add the created table schema
  ObDropTableStmt *drop_table_stmt = dynamic_cast<ObDropTableStmt*>(stmt);
  OB_ASSERT(NULL != drop_table_stmt);
  uint64_t tenant_id = drop_table_stmt->get_drop_table_arg().tenant_id_;
  bool if_exist = drop_table_stmt->get_drop_table_arg().if_exist_;
  ObSArray<ObTableItem> &tables = drop_table_stmt->get_drop_table_arg().tables_;
  for (int64_t i = 0; i < tables.count(); ++i) {
    ObString &database_name = tables.at(i).database_name_;
    ObString &table_name = tables.at(i).table_name_;
    const ObTableSchema *table_schema = NULL;
    OK(schema_guard_.get_table_schema(tenant_id, database_name, table_name, false,table_schema));
    ASSERT_TRUE(table_schema || if_exist);
    if (table_schema) {
      OK(drop_table_schema(*table_schema));
    }
  }
}
int TestSqlUtils::drop_table_schema(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable = NULL;
  if (OB_FAIL(schema_guard_.get_tenant_info(sys_tenant_id_, tenant_schema))) {
    OB_LOG(WARN, "get tenant info failed", K_(sys_tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    OB_LOG(WARN, "tenant schema is null", K(ret));
  } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(table_schema.get_tenant_id(), sys_variable))) {
    OB_LOG(WARN, "get sys variable failed", K(sys_variable), K(ret));
  } else if (OB_ISNULL(sys_variable)) {
    ret = OB_TENANT_NOT_EXIST;
    OB_LOG(WARN, "sys variable schema is null", K(ret));
  } else {
    ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
    if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX)   {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "invalid tenant mod, ret %d", ret);
    } else {
      schema_version_++;
      if (OB_FAIL(schema_service_->drop_table_schema(table_schema.get_tenant_id(), table_schema.get_table_id()))) {
        _OB_LOG(WARN, "drop table schema fail, ret %d", ret);
      }
    }
  }
  return ret;
}
void TestSqlUtils::do_create_index(ObStmt *&stmt)
{
  //add the create index schema
  ObCreateIndexStmt *crt_idx_stmt = dynamic_cast<ObCreateIndexStmt*>(stmt);
  OB_ASSERT(NULL != crt_idx_stmt);
  generate_index_schema(*crt_idx_stmt);
}

void TestSqlUtils::do_create_database(ObStmt *&stmt)
{
  ObCreateDatabaseStmt *create_database_stmt = dynamic_cast<ObCreateDatabaseStmt *>(stmt);
  OB_ASSERT(NULL != create_database_stmt);
  share::schema::ObDatabaseSchema database_schema = create_database_stmt->get_create_database_arg().database_schema_;
  _OB_LOG(INFO, "database_schema=%s", CSJ(database_schema));
  database_schema.set_tenant_id(sys_tenant_id_);
  database_schema.set_database_id(next_user_database_id_++);
  OK(add_database_schema(database_schema));
}

int TestSqlUtils::add_database_schema(ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_tenant_info(database_schema.get_tenant_id(), tenant_schema))) {
      OB_LOG(WARN, "get tenant info failed", K(database_schema), K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "tenant schema is null", K(ret));
    } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(database_schema.get_tenant_id(), sys_variable))) {
      OB_LOG(WARN, "get sys variable failed", K(sys_variable), K(ret));
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "sys variable schema is null", K(ret));
    } else {
      ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
      if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX)   {
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "invalid tenant mod, ret %d", ret);
      } else {
        database_schema.set_name_case_mode(local_mode);
        database_schema.set_schema_version(schema_version_);
        if (OB_FAIL(schema_service_->add_database_schema(database_schema,
           schema_version_++))) {
          _OB_LOG(WARN, "add database schema fail, ret %d", ret);
        }
      }
    }
  }
  return ret;

}
void TestSqlUtils::create_system_db()
{
  share::schema::ObDatabaseSchema database_schema;
  database_schema.set_tenant_id(sys_tenant_id_);
  database_schema.set_database_id(OB_SYS_DATABASE_ID);
  database_schema.set_database_name("oceanbase");
  database_schema.set_charset_type(CHARSET_UTF8MB4);
  database_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  _OB_LOG(INFO, "sys_database_schema=%s", CSJ(database_schema));
  OK(add_database_schema(database_schema));
}

void TestSqlUtils::do_use_database(ObStmt *&stmt)
{
  ObUseDatabaseStmt *use_database_stmt = dynamic_cast<ObUseDatabaseStmt *>(stmt);
  OB_ASSERT(NULL != use_database_stmt);
  session_info_.set_default_database(use_database_stmt->get_db_name(), CS_TYPE_UTF8MB4_GENERAL_CI);
  _OB_LOG(INFO, "%s", CSJ(*use_database_stmt));
}

void TestSqlUtils::do_create_user(ObStmt *&stmt){
  OB_ASSERT(stmt::T_CREATE_USER == stmt->get_stmt_type());
  ObCreateUserStmt *create_user_stmt = static_cast<ObCreateUserStmt*>(stmt);
  ObUserInfo user_info;
  const ObStrings& users = create_user_stmt->get_users();
  ObString user_name;
  ObString host_name;
  ObString pwd;
  int64_t ret = OB_SUCCESS;
  for (int64_t i = 0; i < users.count() - 4; i += 3) {
    if (OB_SUCCESS != (ret = users.get_string(i, user_name))) {
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else if (OB_SUCCESS != (ret = users.get_string(i + 1, host_name))) {
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else if (OB_SUCCESS != (ret = users.get_string(i + 2, pwd))) {
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else {
      ObUserInfo user_info;
      user_info.set_user_id(next_user_id_++);
      user_info.set_user_name(user_name);
      user_info.set_host(host_name);
      user_info.set_passwd(pwd);
      user_info.set_tenant_id(create_user_stmt->get_tenant_id());
      user_info.set_schema_version(schema_version_);
      OK(schema_service_->add_user_schema(user_info,
          schema_version_++));
    }
  }
}

uint64_t TestSqlUtils::get_next_table_id(const uint64_t user_tenant_id)
{
  uint64_t next_table_id = OB_INVALID_ID;
  if (OB_HASH_NOT_EXIST == next_user_table_id_map_.get_refactored(user_tenant_id, next_table_id )){
    next_table_id = OB_MIN_USER_OBJECT_ID + 1;
    OB_ASSERT(OB_SUCCESS == next_user_table_id_map_.set_refactored(user_tenant_id, next_table_id));
    _OB_LOG(INFO, "tenant_id = [%lu] not exist, set next_table_id = [%lu]", user_tenant_id, next_table_id);
  } else {
    ++next_table_id;
    OB_ASSERT(OB_SUCCESS == next_user_table_id_map_.set_refactored(user_tenant_id, next_table_id, 1 /* replace */));
    _OB_LOG(INFO, "tenant_id = [%lu] exist, set new next_table_id = [%lu]", user_tenant_id, next_table_id);
  }
  return next_table_id;
}


void TestSqlUtils::generate_index_column_schema(ObCreateIndexStmt &stmt,
                                                ObTableSchema &index_schema)
{
  int64_t index_rowkey_num = 0;
  uint64_t max_column_id = 0;
  const ObTableSchema *table_schema = NULL;
  ObCreateIndexArg &index_arg = stmt.get_create_index_arg();

  OK(schema_guard_.get_table_schema(index_arg.tenant_id_,
                                               index_arg.database_name_,
                                               index_arg.table_name_,
                                               false,
                                               table_schema));
  ASSERT_FALSE(NULL == table_schema);
  for (int64_t i = 0; i < index_arg.index_columns_.count(); ++i) {
    ObColumnSchemaV2 index_column;
    const ObColumnSchemaV2 *col = table_schema->get_column_schema(index_arg.index_columns_[i].column_name_);
    ASSERT_FALSE(NULL == col);
    index_column = *col;
    ++index_rowkey_num;
    index_column.set_rowkey_position(index_rowkey_num);
    index_column.set_index_position(index_rowkey_num);
    if (col->get_column_id() > max_column_id) {
      max_column_id = col->get_column_id();
    }
    index_schema.set_tenant_id(1);
    ASSERT_EQ(OB_SUCCESS, index_schema.add_column(index_column));
  }
  //add primary key
  const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
  for (int64_t i = 0; i < rowkey_info.get_size(); ++i) {
    uint64_t column_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, rowkey_info.get_column_id(i, column_id));
    if (NULL == index_schema.get_column_schema(column_id)) {
      ++index_rowkey_num;
      const ObColumnSchemaV2 *col = table_schema->get_column_schema(column_id);
      ASSERT_FALSE(NULL == col);
      ObColumnSchemaV2 index_column;
      index_column = *col;
      index_column.set_rowkey_position(index_rowkey_num);
      if (col->get_column_id() > max_column_id) {
        max_column_id = col->get_column_id();
      }
      ASSERT_EQ(OB_SUCCESS, index_schema.add_column(index_column));
    }
  }
  //add storing column
  for (int64_t i = 0; i < index_arg.store_columns_.count(); ++i) {
    const ObColumnSchemaV2 *col = table_schema->get_column_schema(index_arg.store_columns_[i]);
    OB_ASSERT(col);
    if (col->get_column_id() > max_column_id) {
      max_column_id = col->get_column_id();
    }
    ASSERT_EQ(OB_SUCCESS, index_schema.add_column(*col));
  }
  index_schema.set_rowkey_column_num(index_rowkey_num);
  index_schema.set_max_used_column_id(max_column_id);
}

void TestSqlUtils::generate_index_schema(ObCreateIndexStmt &stmt)
{
  ObTableSchema index_schema;
  ObCreateIndexArg &index_arg = stmt.get_create_index_arg();
  const ObTableSchema *data_table_schema = NULL;
  OK(schema_guard_.get_table_schema(index_arg.tenant_id_, index_arg.database_name_,
      index_arg.table_name_, false,data_table_schema));
  OB_ASSERT(data_table_schema);
  generate_index_column_schema(stmt, index_schema);
  ObString index_table_name;
  OK(ObTableSchema::build_index_table_name(allocator_, data_table_schema->get_table_id(), index_arg.index_name_,index_table_name));
  ASSERT_EQ(OB_SUCCESS, index_schema.set_table_name(index_table_name));
  index_schema.set_block_size(index_arg.index_option_.block_size_);
  index_schema.set_is_use_bloomfilter(index_arg.index_option_.use_bloom_filter_);
  index_schema.set_progressive_merge_num(index_arg.index_option_.progressive_merge_num_);
  index_schema.set_data_table_id(data_table_schema->get_table_id());
  ASSERT_EQ(OB_SUCCESS, index_schema.set_compress_func_name(index_arg.index_option_.compress_method_));
  ASSERT_EQ(OB_SUCCESS, index_schema.set_comment(index_arg.index_option_.comment_));
  index_schema.set_table_type(USER_INDEX);
  index_schema.set_index_type(index_arg.index_type_);
  index_schema.set_tenant_id(sys_tenant_id_);
  index_schema.set_tablegroup_id(0);
  index_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  _OB_LOG(INFO, "origin index_schema database id is %ld", index_schema.get_database_id() );
  //combine the database_id and tenant_id
  uint64_t next_index_tid = get_next_table_id(index_schema.get_tenant_id());
  index_schema.set_table_id(next_index_tid);
  index_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  ASSERT_TRUE(NULL != data_table_schema);
  //database id is same as data_table schema
  index_schema.set_database_id(data_table_schema->get_database_id());
  OK(add_table_schema(index_schema));
  if (data_table_schema != NULL){
    ObTableSchema table_schema;
    OK(table_schema.assign(*data_table_schema));
    OK(table_schema.add_simple_index_info(ObAuxTableMetaInfo(
       index_schema.get_table_id(), USER_TABLE, INDEX_TYPE_NORMAL_LOCAL)));
    OK(add_table_schema(table_schema));
    OK(schema_service_->get_schema_guard(schema_guard_, schema_version_));
  }else{
    _OB_LOG_RET(ERROR, OB_ERROR, "no data table found for tid=%lu", data_table_schema->get_table_id());
  }
  _OB_LOG(DEBUG, "index_schema: %s", to_cstring(index_schema));
}

int TestSqlUtils::get_hidden_column_value(
    ObResolverParams &resolver_ctx,
    ParamStore &params)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = resolver_ctx.query_ctx_;
  ObIAllocator *allocator = resolver_ctx.allocator_;
  ObQueryCtx::CalculableItems &calc_items = query_ctx->calculable_items_;
  for (int64_t i = 0; OB_SUCC(ret) && i < calc_items.count(); i++) {
    ObRawExpr *expr = calc_items.at(i).expr_;
    ObObjParam result;
    if (OB_SUCCESS != (ret = ObSQLUtils::calc_calculable_expr(
                resolver_ctx.session_info_,
                expr, result, allocator, params))) {
      SQL_LOG(WARN, "Get calculabel expr value error", K(ret));
    } else if (OB_SUCCESS != (ret = params.push_back(result))) {
      SQL_LOG(WARN, "Add result to params error", K(ret));
    } else { }
  }
  return ret;
}

void TestSqlUtils::is_equal_content(const char* tmp_file, const char* result_file)
{
  std::ifstream if_test(tmp_file);
  if_test.is_open();
  EXPECT_EQ(true, if_test.is_open());
  std::istream_iterator<std::string> it_test(if_test);
  std::ifstream if_expected(result_file);
  if_expected.is_open();
  EXPECT_EQ(true, if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  bool is_equal = std::equal(it_test, std::istream_iterator<std::string>(), it_expected);
  _OB_LOG(INFO, "result file is %s, expect file is %s, is_equal:%d", tmp_file, result_file, is_equal);
  if (is_equal) {
    std::remove(tmp_file);
  } else if (clp.record_test_result) {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "emacs -q %s %s\n", result_file, tmp_file);
    fprintf(stdout, "diff -u %s %s\n", result_file, tmp_file);
    fprintf(stdout, "mv %s %s\n", tmp_file, result_file);
    std::rename(tmp_file,result_file);
  } else {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "diff -u %s %s\n", tmp_file, result_file);
  }
  EXPECT_EQ(true, is_equal);
}

int TestSqlUtils::parse_row_from_json(const ObString &json_str, ObString &table_name, ObIArray<ObSEArray<ObObj, 3> > &row_array)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;
  json::Value *root = NULL;
  if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), root))) {
    LOG_WARN("parse json failed", K(ret), K(json_str));
  } else if (NULL == root) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else {
    if (json::JT_OBJECT != root->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error json format", K(ret), K(json_str), "root", *root);
    } else {
      ObObj value;
      DLIST_FOREACH(it, root->get_object()) {
        if (it->name_.case_compare("table_name") == 0) {
          if (NULL == it->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_STRING != it->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected table_name type", K(ret), "type", it->value_->get_type());
          } else {
            table_name = it->value_->get_string();
          }
        } else if (it->name_.case_compare("row") == 0) {
          if (NULL == it->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_ARRAY != it->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected row type", K(ret), "type", it->value_->get_type());
          } else if (OB_FAIL(parse_json_array(*it->value_, row_array))) {
            LOG_WARN("parse json array failed", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected json name", K(it->name_));
        }
      }
    }
  }
  return ret;
}

int TestSqlUtils::parse_json_array(json::Value &value, ObIArray<ObSEArray<ObObj, 3> > &row_array)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ObSEArray<ObObj, 3> row;
  bool is_row = false;
  DLIST_FOREACH(array_it, value.get_array()) {
    if (json::JT_NUMBER == array_it->get_type()) {
      obj.reset();
      obj.set_int(array_it->get_number());
      if (OB_FAIL(row.push_back(obj))) {
        LOG_WARN("store obj value to row store failed", K(ret));
      }
    } else if (json::JT_STRING == array_it->get_type()) {
      obj.reset();
      obj.set_varchar(array_it->get_string());
      obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      if (OB_FAIL(row.push_back(obj))) {
        LOG_WARN("store row value to row store failed", K(ret));
      }
    } else if (json::JT_NULL == array_it->get_type()) {
      obj.reset();
      obj.set_null();
      if (OB_FAIL(row.push_back(obj))) {
        LOG_WARN("store row value to row store failed", K(ret));
      }
    } else if (json::JT_ARRAY == array_it->get_type()) {
      //The elements of array are still array
      is_row = true;
      if (OB_FAIL(parse_json_array(*array_it, row_array))) {
        LOG_WARN("parse json array failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row obj type", K(array_it->get_type()));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_row && row.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array and single value in array not allowed", K(is_row), K(row.count()));
    } else if (row.count() > 0) {
      if (OB_FAIL(row_array.push_back(row))) {
        LOG_WARN("store row to row array failed", K(ret), K(is_row));
      }
    }
  }
  return ret;
}
}
