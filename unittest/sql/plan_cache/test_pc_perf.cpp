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

#define private public
#include "rpc/frame/ob_req_transport.h"
#include "observer/ob_server.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "test_sql.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace share;
using namespace observer;
using namespace rpc::frame;
static test::TestSQL *test_sql = NULL;
ObSql sql_engine;
ObRsMgr rs_mgr;
ObAddr addr;

std::vector<string> test_sql_array;
bool g_stop = false;
int64_t g_end_time = 0;

struct Config {
  int thread_num;
  const char *schema_file;
  const char *query_file;
  Config();

  bool is_valid();
};

bool Config::is_valid() {
  return thread_num != 0 && schema_file != NULL && query_file != NULL;
}

Config::Config() {
  thread_num = 0;
  schema_file = NULL;
  query_file = NULL;
}

struct ThreadArgs {
  const Config *config;
  int64_t exec_count; //out param_;
  int64_t pc_total_time;
  int64_t parse_total_time;
  int64_t select_count;
  int64_t update_count;
  int64_t insert_count;
  int64_t delete_count;

  ThreadArgs();
};

ThreadArgs::ThreadArgs() {
  config = NULL;
  exec_count = 0;
  pc_total_time = 0;
  parse_total_time = 0;
  select_count = 0;
  update_count = 0;
  insert_count = 0;
  delete_count = 0;
}

class ObTestPlanCachePerformance : public test::TestOptimizerUtils
{
  public:
    int test(const char * query_str);
    int init_engine(const char *schema_file);
    virtual void SetUp();
    virtual void TearDown();
    void TestBody() {}
  public:
    int64_t test_times_; //执行test的次数
    int64_t pc_time_;     //plan cache总时间
    int64_t parse_time_;  //parse 总时间
    int64_t select_hit_count_;
    int64_t update_hit_count_;
    int64_t insert_hit_count_;
    int64_t delete_hit_count_;
};

void ObTestPlanCachePerformance::SetUp()
{
}

void ObTestPlanCachePerformance::TearDown()
{
}

TEST_F(ObTestPlanCachePerformance, basic_test)
{
}

void init_pc(const char *schema_file)
{
  if (NULL == test_sql) {
    test_sql = new test::TestSQL(ObString(schema_file));
    test_sql->init();
  }
}

int ObTestPlanCachePerformance::init_engine(const char *schema_file) {
  int ret = OB_SUCCESS;
  test_times_ = 0;
  pc_time_ = 0;
  parse_time_ = 0;
  select_hit_count_ = 0;
  update_hit_count_ = 0;
  insert_hit_count_ = 0;
  delete_hit_count_ = 0;
  init_pc(schema_file);
  init();
  if (OB_FAIL(sql_engine.init(&stat_manager_,
                              (ObReqTransport*)1,
                              &partition_service_,
                              NULL, // no virtual data access
                              &part_cache_,
                              addr,
                              rs_mgr))) {
    SQL_PC_LOG(ERROR, "fail to init sql engine");
  }
  return ret;
}

int load_sql(const char *test_file, std::vector<string> &sql_array)
{
  int ret = OB_SUCCESS;
  std::ifstream if_tests(test_file);
  if (!if_tests.is_open()) {
    SQL_PC_LOG(ERROR, "maybe reach max file open");
    ret = OB_ERROR;
  }
  std::string line;
  std::string total_line;
  ;
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
      sql_array.push_back(total_line);
      total_line = "";
    }
  }
  if_tests.close();
  return ret;
}

void *run(void * arg) {
  ThreadArgs *thread_args = (ThreadArgs *)(arg);

  ObTestPlanCachePerformance p;
  p.init();

  while (!g_stop) {
    for (int i = 0; i < (int)test_sql_array.size(); i ++) {
      p.test(test_sql_array[i].c_str());
    }
  }
  thread_args->exec_count = p.test_times_;
  thread_args->pc_total_time = p.pc_time_;
  thread_args->parse_total_time = p.parse_time_;
  thread_args->select_count = p.select_hit_count_;
  thread_args->update_count = p.update_hit_count_;
  thread_args->insert_count = p.insert_hit_count_;
  thread_args->delete_count = p.delete_hit_count_;
  return NULL;
}

void run_all(const Config &config) {
  load_sql(config.query_file, test_sql_array);
  pthread_t t_array[config.thread_num];
  ThreadArgs args[config.thread_num];
  for (int i = 0; i < config.thread_num; i ++) {
    args[i].config = &config;
    pthread_create(&t_array[i], NULL, run, &(args[i]));
  }

  int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();

  void *ret = NULL;
  for (int i = 0; i < config.thread_num; i ++) {
    pthread_join(t_array[i], &ret);
  }

  int64_t timeu = g_end_time - start_time;

  int64_t total = 0;
  int64_t pc_total_time = 0;
  int64_t parse_total_time = 0;
  int64_t select_hit_total = 0;
  int64_t update_hit_total = 0;
  int64_t insert_hit_total= 0;
  int64_t delete_hit_total = 0;
  for (int i = 0; i < config.thread_num; i ++) {
    total += args[i].exec_count;
    pc_total_time += args[i].pc_total_time;
    parse_total_time += args[i].parse_total_time;
    select_hit_total += args[i].select_count;
    update_hit_total += args[i].update_count;
    insert_hit_total += args[i].insert_count;
    delete_hit_total += args[i].delete_count;
  }
  _SQL_PC_LOG(ERROR, "===");
  _SQL_PC_LOG(ERROR, "===");
  _SQL_PC_LOG(ERROR, "===");
  _SQL_PC_LOG(ERROR, "total_cnt = %ld, agv_timeu = %ld, QPS = %f", total, timeu, ((double)total / (double)timeu) * 1000000.0);
  _SQL_PC_LOG(ERROR, "avg_parse_time = %ld, avg_pc_time = %ld", parse_total_time/total, pc_total_time/total);
  _SQL_PC_LOG(ERROR, "hit_total = %ld, select_hit = %ld, update_hit = %ld, insert_hit = %ld, delete_hit = %ld",
                     select_hit_total + update_hit_total + insert_hit_total + delete_hit_total,
                     select_hit_total, update_hit_total, insert_hit_total, delete_hit_total);
  _SQL_PC_LOG(ERROR, "===");
  _SQL_PC_LOG(ERROR, "===");
  _SQL_PC_LOG(ERROR, "===");
}

int ObTestPlanCachePerformance::test(const char * query_str) {
  int ret = OB_SUCCESS;
  ObSqlCtx context;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObResultSet result(session_info_, allocator);
  int64_t t1 = 0, t2 =0, t3 = 0;
  test_times_ ++;

  t1 = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    context.schema_manager_ = test_sql->get_schema_manager();
    context.session_info_ = &session_info_;
    context.partition_location_cache_ = &part_cache_;
    session_info_.set_plan_cache(sql_engine.get_plan_cache(OB_SYS_TENANT_ID));
  }

  t2 = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    if OB_FAIL(sql_engine.handle_text_query(query_str, context, result)) {
      SQL_PC_LOG(WARN, "fail to handle text query", K(ret));
    }
  }
  t3 = ::oceanbase::common::ObTimeUtility::current_time();
  parse_time_ += t2-t1;
  pc_time_ += t3-t2;

  return ret;
}

void print_usage() {
  fprintf(stderr, "Usage: %s -t thread_num -s schema_file -q query_file [-l log_level]\n", "test_performance");
  exit(0);
}

void my_signal(int sig) {
  if (sig == SIGINT) {
    g_stop = true;
    g_end_time = ::oceanbase::common::ObTimeUtility::current_time();
  }
}

int main(int argc, char **argv)
{
  Config config;
  const char *log_level = "INFO";
  if (SIG_ERR == signal(SIGINT, my_signal)) {
    exit(-1);
  }
  int c = 0;
  while(-1 != (c = getopt(argc, argv, "t:s:q:l:"))) {
    switch (c) {
    case 't':
      config.thread_num = atoi(optarg);
      break;
    case 's':
      config.schema_file = optarg;
      break;
    case 'q':
      config.query_file = optarg;
      break;
    case 'l':
      log_level = optarg;
      break;
    default:
      print_usage();
    }
  }
  if (!config.is_valid()) {
    print_usage();
  }
  OB_LOGGER.set_log_level(log_level);
  ::oceanbase::sql::init_sql_factories();
  ObTestPlanCachePerformance p;
  p.init_engine(config.schema_file);
  run_all(config);
  return 0;
}

