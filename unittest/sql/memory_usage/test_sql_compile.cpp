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
#include <iterator>
#include <string>
#define private public
#define protected public
#include "sql/rewrite/ob_transformer_impl.h"
#include "observer/mysql/obmp_base.h"
#undef protected
#undef private
#include "sql/ob_sql_init.h"
#include "../test_sql_utils.h"
#include "../optimizer/ob_mock_opt_stat_manager.h"

using namespace oceanbase::obrpc;
namespace test
{
const char* SQL_DIR = "sql";
const char* RESULT_DIR ="result";
const char* SCHEMA_DIR ="schema";
class TestSQLCompile: public TestSqlUtils, public ::testing::Test
{
public:
  TestSQLCompile();
  virtual ~TestSQLCompile(){}
  virtual void SetUp();
  virtual void TearDown();
  void test_sql_compile(const char *file_name);
  int transform(ObDMLStmt *&stmt, MemoryUsage &memory_usage);
  int optimize(ObDMLStmt *&stmt, ObLogPlan *&logical_plan, MemoryUsage &memory_usage);
private:
  int parse_memory_usage_from_result(const char* file_path, ObIArray<MemoryUsage>& memory_usages);
  int is_memory_usage_same(const MemoryUsage& current, const MemoryUsage& baseline,
                          const int64_t mem_diff_threshold,
                          bool &is_equal, bool &is_same);
  void is_equal_memory_usage(const char* tmp_file, const char* result_file, ObIArray<MemoryUsage>& tmp_usages);
  int parse_table_line(const std::string& line, int64_t& parse_mem, int64_t& resolve_mem, int64_t& transform_mem, int64_t& optimize_mem, int64_t& total_mem);
  int64_t parse_table_field(const std::string& field);
  ::test::MockOptStatManager opt_stat_manager_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSQLCompile);
};

TestSQLCompile::TestSQLCompile()
{
}

void TestSQLCompile::SetUp()
{
  init();
}

void TestSQLCompile::TearDown()
{
  destroy();
}

void TestSQLCompile::test_sql_compile(const char *file_name_prefix)
{
  int ret = OB_SUCCESS;
  const char *postfix[] = {"test","tmp","result","schema"};
  int64_t sql_postfix_len = strlen(postfix[0]);
  int64_t tmp_postfix_len = strlen(postfix[1]);
  int64_t result_postfix_len = strlen(postfix[2]);
  int64_t schema_postfix_len = strlen(postfix[3]);
  char file_name[4][FILE_PATH_LEN];
  //construct the file name ./sql/test_sql_compile_xxx.test  sql file
  //construct the file name ./result/test_sql_compile_xxx.tmp  tmp result file
  //construct the file name ./result/test_sql_compile_xxx.result  correct result file
  //construct the file name ./schema/test_sql_compile_xxx.schema  schema file
  int64_t sql_file_len = strlen(file_name_prefix);
  snprintf(file_name[0],
           strlen(SQL_DIR) + sql_file_len + sql_postfix_len + 4,
           "./%s/%s%s",
           SQL_DIR,
           file_name_prefix,
           postfix[0]);
  snprintf(file_name[1],
            strlen(RESULT_DIR) + sql_file_len + tmp_postfix_len + 4,
            "./%s/%s%s",
            RESULT_DIR,
            file_name_prefix,
            postfix[1]);
  snprintf(file_name[2],
            strlen(RESULT_DIR) + sql_file_len + result_postfix_len + 4,
            "./%s/%s%s",
            RESULT_DIR,
            file_name_prefix,
            postfix[2]);
  snprintf(file_name[3],
            strlen(SCHEMA_DIR) + sql_file_len + schema_postfix_len + 4,
            "./%s/%s%s",
            SCHEMA_DIR,
            file_name_prefix,
            postfix[3]);
  _OB_LOG(INFO, "%s\t%s\t%s\t%s",file_name_prefix,file_name[0], file_name[1], file_name[2], file_name[3]);
  memcpy(schema_file_path_, file_name[3], strlen(file_name[3]) + 1);
  reset_schema();
  init_schema();

  std::ifstream if_sql(file_name[0]);
  ASSERT_TRUE(if_sql.is_open()) << file_name[0];
  fprintf(stdout, "CASE FILE: %s\n", file_name[0]);
  std::ofstream of_tmp(file_name[1]);
  ASSERT_TRUE(of_tmp.is_open()) << file_name[1];

  ObSQLMode mode = SMO_DEFAULT;
  ObArenaAllocator parser_allocator;
  ObSEArray<ObString, 4> queries;
  ObParser parser(parser_allocator, mode);
  ObMPParseStat parse_stat;

  if_sql.seekg(0, std::ios::end);
  size_t file_size = if_sql.tellg();
  if_sql.seekg(0, std::ios::beg);
  char* buffer = static_cast<char*>(parser_allocator.alloc(file_size + 1));
  ASSERT_TRUE(buffer != NULL);
  MEMSET(buffer, 0, file_size + 1);
  if_sql.read(buffer, file_size);
  size_t bytes_read = if_sql.gcount();
  ASSERT_EQ(bytes_read, file_size);
  OK(parser.split_multiple_stmt(ObString(bytes_read, buffer), queries, parse_stat));

  ObStmt *stmt = NULL;
  int64_t case_id = 0;
  reset_sql_ctx();
  ObSEArray<MemoryUsage, 4> memory_usages;
  for (int64_t j = 0; j < queries.count(); ++j) {
    case_id = j + 1;
    ObString query = queries.at(j).trim();
    stmt = NULL;
    of_tmp << "***************   Case "<< case_id << "   ***************" << std::endl;
    of_tmp.write(query.ptr(), query.length());
    of_tmp << std::endl;
    LOG_INFO("process query", K(case_id), K(query));
    char identifier_name[FILE_PATH_LEN];
    snprintf(identifier_name, FILE_PATH_LEN, "%.*s_%d",
                              strlen(file_name_prefix) - 1, file_name_prefix,
                              case_id);

    ContextParam param;
    param.set_mem_attr(1001, "compile", ObCtxIds::WORK_AREA).set_page_size(OB_MALLOC_REQ_NORMAL_BLOCK_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      init_sql_ctx();
      MemoryUsage memory_usage;
      ObString identifier(identifier_name);
      OK(session_info_.get_optimizer_tracer().enable_mem_perf(identifier));
      observer::ObMemPerfCallback mpcb(session_info_.get_optimizer_tracer());
      ObSQLSessionInfo *session_info = &session_info_;
      BEGIN_MEM_PERF(session_info);
      {
        lib::ObMallocCallbackGuard mp_guard(mpcb);
        int64_t max_mem_used = 0;
        observer::ObProcessMallocCallback pmcb(0, max_mem_used);
        lib::ObMallocCallbackGuard guard(pmcb);
        ObMemPerfGuard mem_perf_guard("hard_parse");
        ASSERT_NO_FATAL_FAILURE(do_load_sql(query, stmt, TREE_FORMAT, 0, case_id, &memory_usage));
        // of_tmp << CSJ(*stmt) << std::endl;
        ObDMLStmt *dml_stmt = NULL;
        ObLogPlan *logical_plan = NULL;
        LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
        if (OB_ISNULL(stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt is null", K(ret));
        } else if (OB_UNLIKELY(!stmt->is_dml_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt is not dml stmt", K(ret));
        } else if (FALSE_IT(dml_stmt = static_cast<ObDMLStmt *>(stmt))) {
        } else if (OB_FAIL(transform(dml_stmt, memory_usage))) {
          LOG_WARN("failed to transform", K(ret));
        } else if (OB_FAIL(optimize(dml_stmt, logical_plan, memory_usage))) {
          LOG_WARN("failed to optimize", K(ret));
        } else {
          memory_usage.total_mem_used_ = pmcb.get_cur_used();
          memory_usage.total_mem_max_used_ = pmcb.get_max_used();
          of_tmp << "memory usage: parser = " << memory_usage.parse_mem_used_ <<
                    ", resolve = " << memory_usage.resolve_mem_used_ <<
                    ", transform = " << memory_usage.transform_mem_used_ <<
                    ", optimize = " << memory_usage.optimize_mem_used_<<
                    ", total = " << memory_usage.total_mem_used_ << std::endl;
          if (OB_FAIL(memory_usages.push_back(memory_usage))) {
            LOG_WARN("failed to push memory usage", K(ret));
          }
        }
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      END_MEM_PERF(session_info);
      reset_sql_ctx();
    }
  }
  of_tmp.imbue(std::locale("en_US.UTF-8"));
  of_tmp << std::endl;
  of_tmp << "memory usage" << std::endl;
  of_tmp << "| query | parser | resolve | transform | optimize | total |" << std::endl;
  of_tmp << "| --- | --- | --- | --- | --- | --- |" << std::endl;
  for (int64_t j = 0; j < memory_usages.count(); ++j) {
    of_tmp << "| " << j + 1 <<
              " | " << memory_usages[j].parse_mem_used_ <<
              " | " << memory_usages[j].resolve_mem_used_ <<
              " | " << memory_usages[j].transform_mem_used_ <<
              " | " << memory_usages[j].optimize_mem_used_ <<
              " | " << memory_usages[j].total_mem_used_ <<
              " |" << std::endl;
  }

  of_tmp << std::endl;
  of_tmp << "max memory usage" << std::endl;
  of_tmp << "| query | parser | resolve | transform | optimize | total |" << std::endl;
  of_tmp << "| --- | --- | --- | --- | --- | --- |" << std::endl;
  for (int64_t j = 0; j < memory_usages.count(); ++j) {
    of_tmp << "| " << j + 1 <<
              " | " << memory_usages[j].parse_mem_max_used_ <<
              " | " << memory_usages[j].resolve_mem_max_used_ <<
              " | " << memory_usages[j].transform_mem_max_used_ <<
              " | " << memory_usages[j].optimize_mem_max_used_ <<
              " | " << memory_usages[j].total_mem_max_used_ <<
              " |" << std::endl;
  }
  of_tmp.close();
  if_sql.close();
  _OB_LOG(INFO, "test %s finished!, total %ld case", file_name_prefix, case_id);
  fprintf(stdout, "FINISHED %s TOTAL %ld\n", file_name_prefix, case_id);
  ASSERT_NO_FATAL_FAILURE(is_equal_memory_usage(file_name[1], file_name[2], memory_usages));
}

int TestSQLCompile::transform(ObDMLStmt *&stmt, MemoryUsage &memory_usage)
{
  int ret = OB_SUCCESS;

  ObTransformerCtx trans_ctx;
  ObSchemaChecker schema_checker;
  ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx is null", K(ret));
  } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_))) {
    LOG_WARN("failed to init schema_checker", K(ret));
  } else {
    ObMemPerfGuard mem_perf_guard("rewrite");
    int64_t max_mem_used = 0;
    observer::ObProcessMallocCallback pmcb(0, max_mem_used);
    lib::ObMallocCallbackGuard guard(pmcb);

    trans_ctx.allocator_ = &allocator_;
    trans_ctx.schema_checker_ = &schema_checker;
    trans_ctx.session_info_ = &session_info_;
    trans_ctx.exec_ctx_ = &exec_ctx_;
    trans_ctx.expr_factory_ = &expr_factory_;
    trans_ctx.stmt_factory_ = &stmt_factory_;
    trans_ctx.opt_stat_mgr_ = &opt_stat_manager_;
    trans_ctx.sql_schema_guard_ = &query_ctx->sql_schema_guard_;
    trans_ctx.self_addr_ = &local_addr_;
    ObTransformerImpl transformer(&trans_ctx);
    if (OB_FAIL(transformer.transform(stmt))) {
      LOG_WARN("failed to transform", K(ret));
    } else {
      memory_usage.transform_mem_used_= pmcb.get_cur_used();
      memory_usage.transform_mem_max_used_= pmcb.get_max_used();
    }
  }
  return ret;
}

int TestSQLCompile::optimize(ObDMLStmt *&stmt, ObLogPlan *&logical_plan, MemoryUsage &memory_usage)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
  ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();
  if (OB_ISNULL(pctx) || OB_ISNULL(stmt) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(pctx), K(stmt));
  } else {
    ObMemPerfGuard mem_perf_guard("optimize");
    int64_t max_mem_used = 0;
    observer::ObProcessMallocCallback pmcb(0, max_mem_used);
    lib::ObMallocCallbackGuard guard(pmcb);
    ObOptimizerContext optctx(&session_info_,
                              &exec_ctx_,
                              &query_ctx->sql_schema_guard_,
                              &opt_stat_manager_,
                              allocator_,
                              &pctx->get_param_store(),
                              local_addr_,
                              NULL, /*srv_proxy*/
                              stmt->get_query_ctx()->get_global_hint(),
                              expr_factory_,
                              stmt,
                              false, /*is_ps_protocol*/
                              stmt_factory_.get_query_ctx());
    ObOptimizer optimizer(optctx);
    if (OB_FAIL(optimizer.optimize(*stmt, logical_plan))) {
      LOG_WARN("Failed to optimize logical plan", K(ret));
    } else {
      memory_usage.optimize_mem_used_= pmcb.get_cur_used();
      memory_usage.optimize_mem_max_used_= pmcb.get_max_used();
    }
  }
  return ret;
}

int TestSQLCompile::parse_memory_usage_from_result(const char* file_path, ObIArray<MemoryUsage>& memory_usages)
{
  int ret = OB_SUCCESS;
  std::ifstream file(file_path);
  if (!file.is_open()) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("failed to open file", K(ret), K(file_path));
    return ret;
  }

  std::vector<std::string> lines;
  std::string line;
  bool is_memory_usage = false;
  bool is_max_memory_usage = false;
  int64_t idx = 0;
  while (OB_SUCC(ret) && std::getline(file, line)) {
    if ("memory usage" == line) {
      is_memory_usage = true;
      idx = 0;
    } else if ("max memory usage" == line) {
      is_max_memory_usage = true;
      idx = 0;
    } else if (line.empty()) {
      is_memory_usage = false;
      is_max_memory_usage = false;
      idx = 0;
    } else if (line.find("query")!= std::string::npos || line.find("---") != std::string::npos) {
      // do nothing
    } else if (is_memory_usage) {
      MemoryUsage usage;
      if (OB_FAIL(parse_table_line(line, usage.parse_mem_used_, usage.resolve_mem_used_,
                                   usage.transform_mem_used_, usage.optimize_mem_used_,
                                   usage.total_mem_used_))) {
        LOG_WARN("failed to parse table line", K(ret), "line", line.c_str());
      } else if (OB_FAIL(memory_usages.push_back(usage))) {
        LOG_WARN("failed to push memory usage", K(ret));
      }
    } else if (is_max_memory_usage) {
      if (idx >= memory_usages.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max memory usage table has more rows than memory usage table", K(ret), K(idx), K(memory_usages.count()));
      } else {
        MemoryUsage &usage = memory_usages.at(idx);
        if (OB_FAIL(parse_table_line(line, usage.parse_mem_max_used_, usage.resolve_mem_max_used_,
                                     usage.transform_mem_max_used_, usage.optimize_mem_max_used_,
                                     usage.total_mem_max_used_))) {
          LOG_WARN("failed to parse table line", K(ret), "line", line.c_str());
        } else {
          ++idx;
        }
      }
    }
  }
  file.close();
  return ret;
}

// considering the memory usage will be greater than the result file occasionally
// if the current memory usage satisfies the following condition, it is considered as same:
// current >= baseline && current <= baseline + mem_diff_threshold
int TestSQLCompile::is_memory_usage_same(const MemoryUsage& current, const MemoryUsage& baseline,
                                         const int64_t mem_diff_threshold,
                                         bool &is_equal, bool &is_same)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  is_same = true;
  int64_t mem_diff = 0;
#define CMP_MEM_USAGE(attr)                               \
  if (is_same) {                                          \
    mem_diff = current.attr##_ - baseline.attr##_;        \
    if (mem_diff != 0) {                                  \
      is_equal = false;                                   \
    }                                                     \
    if (mem_diff < 0 || mem_diff > mem_diff_threshold) {  \
      is_same = false;                                    \
    }                                                     \
  }
  CMP_MEM_USAGE(total_mem_used)
  CMP_MEM_USAGE(parse_mem_used)
  CMP_MEM_USAGE(resolve_mem_used)
  CMP_MEM_USAGE(transform_mem_used)
  CMP_MEM_USAGE(optimize_mem_used)
  CMP_MEM_USAGE(total_mem_max_used)
  CMP_MEM_USAGE(parse_mem_max_used)
  CMP_MEM_USAGE(resolve_mem_max_used)
  CMP_MEM_USAGE(transform_mem_max_used)
  CMP_MEM_USAGE(optimize_mem_max_used)
#undef CMP_MEM_USAGE
  return ret;
}

void TestSQLCompile::is_equal_memory_usage(const char* tmp_file, const char* result_file, ObIArray<MemoryUsage>& tmp_usages)
{
  int ret = OB_SUCCESS;
  std::ifstream if_tmp(tmp_file, std::ios::binary);
  std::ifstream if_result(result_file, std::ios::binary);
  EXPECT_EQ(true, if_tmp.is_open());
  EXPECT_EQ(true, if_result.is_open());

  std::istreambuf_iterator<char> it_tmp(if_tmp);
  std::istreambuf_iterator<char> it_result(if_result);
  bool is_exactly_equal = std::equal(it_tmp, std::istreambuf_iterator<char>(), it_result);
  if_tmp.close();
  if_result.close();

  _OB_LOG(INFO, "tmp file is %s, result file is %s, is_exactly_equal:%d",
          tmp_file, result_file, is_exactly_equal);

  if (is_exactly_equal) {
    std::remove(tmp_file);
  } else {
    ObSEArray<MemoryUsage, 4> result_usages;
    const int64_t MEM_DIFF_THRESHOLD = 3 * 1024; // 3KB
    const int64_t CASE_DIFF_THRESHOLD = 2; // 2 cases

    if (OB_FAIL(parse_memory_usage_from_result(result_file, result_usages))) {
      LOG_WARN("failed to parse result file", K(ret), K(result_file));
    } else if (OB_UNLIKELY(tmp_usages.count() != result_usages.count())) {
      fprintf(stdout, "The case count is mismatched with result file, you can choose to\n");
      fprintf(stdout, "sh accept_result.sh\n");
      EXPECT_EQ(true, false);
      return;
    }

    int64_t equal_cnt = 0;
    int64_t same_cnt = 0;
    int64_t mismatch_cnt = 0;
    for (int64_t i = 0; i < tmp_usages.count(); ++i) {
      bool is_equal = false;
      bool is_same = false;
      if (OB_FAIL(is_memory_usage_same(tmp_usages.at(i), result_usages.at(i), MEM_DIFF_THRESHOLD, is_equal, is_same))) {
        LOG_WARN("failed to check if memory usage is same", K(ret), K(tmp_usages.at(i)), K(result_usages.at(i)));
      } else if (is_equal) {
        ++equal_cnt;
      } else {
        if (is_same) {
          ++same_cnt;
          fprintf(stdout, "case %ld is similar to result file\n", i + 1);
        } else {
          ++mismatch_cnt;
          fprintf(stdout, "case %ld is completely different from result file\n", i + 1);
        }
        fprintf(stdout, "memory usage\n");
        fprintf(stdout, "-| %ld | %ld | %ld | %ld | %ld | %ld |\n", i + 1,
                        result_usages.at(i).parse_mem_used_, result_usages.at(i).resolve_mem_used_,
                        result_usages.at(i).transform_mem_used_, result_usages.at(i).optimize_mem_used_,
                        result_usages.at(i).total_mem_used_);
        fprintf(stdout, "+| %ld | %ld | %ld | %ld | %ld | %ld |\n", i + 1,
                        tmp_usages.at(i).parse_mem_used_, tmp_usages.at(i).resolve_mem_used_,
                        tmp_usages.at(i).transform_mem_used_, tmp_usages.at(i).optimize_mem_used_,
                        tmp_usages.at(i).total_mem_used_);
        fprintf(stdout, "max memory usage\n");
        fprintf(stdout, "-| %ld | %ld | %ld | %ld | %ld | %ld |\n", i + 1,
                        result_usages.at(i).parse_mem_max_used_, result_usages.at(i).resolve_mem_max_used_,
                        result_usages.at(i).transform_mem_max_used_, result_usages.at(i).optimize_mem_max_used_,
                        result_usages.at(i).total_mem_max_used_);
        fprintf(stdout, "+| %ld | %ld | %ld | %ld | %ld | %ld |\n", i + 1,
                        tmp_usages.at(i).parse_mem_max_used_, tmp_usages.at(i).resolve_mem_max_used_,
                        tmp_usages.at(i).transform_mem_max_used_, tmp_usages.at(i).optimize_mem_max_used_,
                        tmp_usages.at(i).total_mem_max_used_);
      }
    }

    fprintf(stdout, "Some case results are different, equal_cnt: %ld, same_cnt: %ld, mismatch_cnt: %ld\n", equal_cnt, same_cnt, mismatch_cnt);
    if (0 == mismatch_cnt && same_cnt <= CASE_DIFF_THRESHOLD) {
      fprintf(stdout, "The difference between current and result file can be ignored,\n");
      fprintf(stdout, "you can still choose to\n");
      fprintf(stdout, "sh accept_result.sh\n");
    } else {
      fprintf(stdout, "The result files mismatched, you can choose to\n");
      fprintf(stdout, "sh accept_result.sh\n");
      EXPECT_EQ(true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

int TestSQLCompile::parse_table_line(const std::string& line, int64_t& parse_mem, int64_t& resolve_mem, int64_t& transform_mem, int64_t& optimize_mem, int64_t& total_mem)
{
  int ret = OB_SUCCESS;
  std::vector<std::string> tokens;
  std::stringstream ss(line);
  std::string token;

  while (std::getline(ss, token, '|')) {
    size_t start = token.find_first_not_of(" \t");
    size_t end = token.find_last_not_of(" \t");
    if (start != std::string::npos && end != std::string::npos) {
      tokens.push_back(token.substr(start, end - start + 1));
    }
  }

  // format: | query_id | parser | resolve | transform | optimize | total |
  if (tokens.size() != 6) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table line", K(ret), "line", line.c_str());
  } else {
    parse_mem = parse_table_field(tokens[1]);
    resolve_mem = parse_table_field(tokens[2]);
    transform_mem = parse_table_field(tokens[3]);
    optimize_mem = parse_table_field(tokens[4]);
    total_mem = parse_table_field(tokens[5]);
  }
  return ret;
}

int64_t TestSQLCompile::parse_table_field(const std::string& field)
{
  std::string temp = field;
  temp.erase(std::remove(temp.begin(), temp.end(), ','), temp.end());
  return std::atoll(temp.c_str());
}

TEST_F(TestSQLCompile, memory_usage_tpcds)
{
  int ret = OB_SUCCESS;
  const char *file_name_prefix = "test_sql_compile_tpcds.";
  test_sql_compile(file_name_prefix);
}

TEST_F(TestSQLCompile, memory_usage_tpch)
{
  int ret = OB_SUCCESS;
  const char *file_name_prefix = "test_sql_compile_tpch.";
  test_sql_compile(file_name_prefix);
}

TEST_F(TestSQLCompile, memory_usage_others)
{
  int ret = OB_SUCCESS;
  const char *tpcx_prefix = "test_sql_compile_tpc";
  for(int32_t i = 0; i < clp.file_count; ++i){
    const char *file_name_prefix = clp.file_names_vector[i];
    if (file_name_prefix != NULL && strncmp(file_name_prefix, tpcx_prefix, strlen(tpcx_prefix)) == 0) {
      continue;
    }
    test_sql_compile(file_name_prefix);
  }
}

TEST_F(TestSQLCompile, parse_table_line)
{
  int ret = OB_SUCCESS;
  for(int32_t i = 0; i < clp.file_count; ++i){
    const char *file_name_prefix = clp.file_names_vector[i];
    // const char *file_name_prefix = "test_sql_compile_many_partition.";
    char file_name[FILE_PATH_LEN];
    ObArenaAllocator allocator(ObModIds::TEST);
    ObSqlArray<MemoryUsage> memory_usages(allocator);
    snprintf(file_name, FILE_PATH_LEN, "result/%sresult", file_name_prefix);
    if (OB_FAIL(parse_memory_usage_from_result(file_name, memory_usages))) {
      LOG_WARN("failed to parse memory usage from result file", K(ret), K(file_name));
    } else {
      LOG_INFO("memory usage", "file_name", file_name_prefix, K(memory_usages));
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

}//end of namespace test

int main(int argc, char **argv)
{
  system("rm -rf test_sql_compile.log* log");
  system("mkdir log");
  freopen("test_sql_compile.log.stderr", "w+", stderr);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_sql_compile.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  test::parse_cmd_line_param(argc, argv, test::clp);
  std::cout<<"clp:record_test_result:::"<<test::clp.record_test_result<<std::endl;
  return RUN_ALL_TESTS();
}
