/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_admin_uncompress_plan_executor.h"
#include "src/sql/resolver/dml/ob_hint.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase
{
namespace tools
{

int ObAdminUncompressPlanExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_cmd(argc, argv))) {
    LOG_WARN("failed to parse cmd");
  } else if (OB_FAIL(uncompress_plan())) {
    LOG_WARN("uncompress plan failed");
  }
  return ret;
}

int ObAdminUncompressPlanExecutor::get_compressed_plan(ObArenaAllocator &allocator,
                                                       const std::string &hex_str,
                                                       char *&compressed_str)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(hex_str.size() / 2 + 1);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for string buffer");
  } else {
    compressed_str = (char *)buf;
    char hex_word[3] = "\0";
    for (int32_t pos = 0; pos < hex_str.length(); pos += 2) {
      MEMCPY(hex_word, hex_str.c_str() + pos, 2);
      compressed_str[pos / 2] = (char)(int)strtol(hex_word, nullptr, 16);
    }
  }
  return ret;
}

int ObAdminUncompressPlanExecutor::uncompress_plan()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObLogicalPlanRawData logical_plan;
  logical_plan.uncompress_len_ = uncompress_len_;
  logical_plan.logical_plan_len_ = compressed_len_;
  if (plan_str_.size() % 2 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("length of plan string must be even", K(plan_str_.size()));
  } else if (logical_plan.logical_plan_len_ * 2 != plan_str_.size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unmatched logical_plan_len and length of plan string",
              K(logical_plan.logical_plan_len_), K(plan_str_.size()));
  } else if (OB_FAIL(get_compressed_plan(allocator, plan_str_, logical_plan.logical_plan_))) {
    LOG_WARN("failed to get_logical_plan");
  } else {
    PlanText out_plan_text;
    ObExplainDisplayOpt option;
    option.with_tree_line_ = true;
    ObSqlPlan sql_plan(allocator);
    ExplainType type = EXPLAIN_EXTENDED;
    ObSEArray<ObSqlPlanItem *, 4> plan_items;
    if (OB_FAIL(logical_plan.uncompress_logical_plan(allocator, plan_items))) {
      LOG_WARN("failed to uncompress logical plan", K(ret));
    } else if (OB_FAIL(sql_plan.format_sql_plan(plan_items, type, option, out_plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    }
    std::string result(out_plan_text.buf_, out_plan_text.pos_);
    std::cout << result << std::endl;
  }
  return ret;
}

int ObAdminUncompressPlanExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "hu:c:p:";
  struct option long_opts[] =
    {{"help", 0, NULL, 'h' },
     {"uncompress_len", 1, NULL, 'u'},
     {"compressed_len", 1, NULL, 'c'},
     {"plan_str", 1, NULL, 'p' },
     {NULL, 0, NULL, 0}};

  while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        exit(1);
      }
      case 'u': {
        uncompress_len_ = (int64_t)strtol(optarg, nullptr, 10);
        break;
      }
      case 'c': {
        compressed_len_ = (int64_t)strtol(optarg, nullptr, 10);
        break;
      }
      case 'p': {
        plan_str_ = optarg;
        break;
      }
      default: {
        print_usage();
        exit(1);
      }
    }
  }
  return ret;
}

void ObAdminUncompressPlanExecutor::print_usage()
{
  printf("\n");
  printf("Usage: uncompress_plan command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-u,--uncompress_len", "the uncompressed length of logical plan");
  printf(HELP_FMT, "-c,--compressed_len", "the compressed length of logical plan");
  printf(HELP_FMT, "-p,--plan_str", "the hex format of compressed logical plan");

  printf("samples:\n");
  std::string sample("\t./ob_admin uncompress_plan -u $uncompress_len -c $logical_plan_len -p $hex_plan_str \n");
  std::cout << sample << std::endl;
}

} // namespace sql
} // namespace oceanbase
