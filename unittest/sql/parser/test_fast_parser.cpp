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

#include "sql/parser/ob_parser.h"
#include "sql/parser/ob_fast_parser.h"
#include <gtest/gtest.h>
#include "lib/worker.h"
#include "lib/allocator/page_arena.h"
#include <fstream>
#include <iterator>
#include <vector>
#include <string>
#include <iostream>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test {
class TestFastParser
{
public:
  TestFastParser();
  ~TestFastParser();
  int load_sql(const std::string file_path, std::vector<std::string> &sql_array);
  int parse(const ObString &sql);
  bool compare_parser_result(
       const ParseResult &parse_result, 
       const char *no_param_sql_ptr, 
       const int64_t no_param_sql_len,
       ParamList *p_list,
       const int64_t param_num);
    
private:
  ObArenaAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestFastParser);
};

TestFastParser::TestFastParser()
  : allocator_(ObModIds::TEST)
{

}

TestFastParser::~TestFastParser()
{

}


int TestFastParser::load_sql(const std::string file_path, std::vector<std::string> &sql_array)
{
  int ret = OB_SUCCESS;
  std::ifstream in(file_path);
  if (!in.is_open()) {
    ret = OB_ERROR;
    SQL_PC_LOG(ERROR, "failed to open file");
  }
  std::string line;
  while (std::getline(in, line)) {
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string sql = line.substr(begin, end - begin + 1);
    sql_array.push_back(sql);
  }
  in.close();
  return ret;
}

bool TestFastParser::compare_parser_result(
     const ParseResult &parse_result, 
     const char *no_param_sql_ptr, 
     const int64_t no_param_sql_len,
     ParamList *p_list,
     const int64_t param_num)
{
  int ret = OB_SUCCESS;
  int64_t cmp_param_num = parse_result.param_node_num_;
  char *cmp_no_param_sql_ptr = parse_result.no_param_sql_;
  int64_t cmp_no_param_sql_len = parse_result.no_param_sql_len_;
  ParamList *cmp_p_list = parse_result.param_nodes_;
  if (param_num != cmp_param_num) {
    SQL_PC_LOG(ERROR, "param_num diff", K(param_num), K(cmp_param_num),
    K(ObString(no_param_sql_len, no_param_sql_ptr)),
    K(ObString(cmp_no_param_sql_len, cmp_no_param_sql_ptr)));
    return false;
  } else if (cmp_no_param_sql_len != no_param_sql_len) {
    SQL_PC_LOG(ERROR, "param sql len diff", K(no_param_sql_len), K(cmp_no_param_sql_len),
    K(ObString(no_param_sql_len, no_param_sql_ptr)),
    K(ObString(cmp_no_param_sql_len, cmp_no_param_sql_ptr)));
    return false;
  } else {
    if (0 != (strncmp(no_param_sql_ptr, cmp_no_param_sql_ptr, no_param_sql_len))) {
      SQL_PC_LOG(ERROR, "no_param_sql diff",
              K(ObString(no_param_sql_len, no_param_sql_ptr)),
              K(ObString(cmp_no_param_sql_len, cmp_no_param_sql_ptr)));
      return false;
    } else {
      ParamList *fp_p_list = p_list;
      for (int64_t i = 0; i < param_num && NULL != fp_p_list; i++) {
        if (fp_p_list->node_->type_ != cmp_p_list->node_->type_) {
          SQL_PC_LOG(ERROR, "type diff", K(fp_p_list->node_->type_), K(cmp_p_list->node_->type_), K(i),
          K(fp_p_list->node_->value_), K(cmp_p_list->node_->value_));
          return false;
        } else if (fp_p_list->node_->num_child_ != cmp_p_list->node_->num_child_) {
          SQL_PC_LOG(ERROR, "num child diff", K(fp_p_list->node_->num_child_), K(cmp_p_list->node_->num_child_), K(i));
          return false;
        } else if (fp_p_list->node_->is_neg_ != cmp_p_list->node_->is_neg_) {
          SQL_PC_LOG(ERROR, "neg diff", K(fp_p_list->node_->is_neg_), K(cmp_p_list->node_->is_neg_), K(i));
          return false;
        } else if (fp_p_list->node_->is_hidden_const_ != cmp_p_list->node_->is_hidden_const_) {
          SQL_PC_LOG(ERROR, "hidden const diff", K(fp_p_list->node_->is_hidden_const_), K(cmp_p_list->node_->is_hidden_const_), K(i));
          return false;
        } else if (fp_p_list->node_->is_tree_not_param_ != cmp_p_list->node_->is_tree_not_param_) {
          SQL_PC_LOG(ERROR, "not param diff", K(fp_p_list->node_->is_tree_not_param_), K(cmp_p_list->node_->is_tree_not_param_), K(i));
          return false;
        } else if (fp_p_list->node_->length_semantics_ != cmp_p_list->node_->length_semantics_) {
          SQL_PC_LOG(ERROR, "length semantics diff", K(fp_p_list->node_->length_semantics_), K(cmp_p_list->node_->length_semantics_), K(i));
          return false;
        } else if (fp_p_list->node_->is_val_paramed_item_idx_ != cmp_p_list->node_->is_val_paramed_item_idx_) {
          SQL_PC_LOG(ERROR, "val paramed item idx diff", K(fp_p_list->node_->is_val_paramed_item_idx_), K(cmp_p_list->node_->is_val_paramed_item_idx_), K(i));
          return false;
        } else if (fp_p_list->node_->is_copy_raw_text_ != cmp_p_list->node_->is_copy_raw_text_) {
          SQL_PC_LOG(ERROR, "copy row text diff", K(fp_p_list->node_->is_copy_raw_text_), K(cmp_p_list->node_->is_copy_raw_text_), K(i));
          return false;
        } else if (fp_p_list->node_->is_column_varchar_ != cmp_p_list->node_->is_column_varchar_) {
          SQL_PC_LOG(ERROR, "column varchar diff", K(fp_p_list->node_->is_column_varchar_), K(cmp_p_list->node_->is_column_varchar_), K(i));
          return false;
        } else if (fp_p_list->node_->is_assigned_from_child_ != cmp_p_list->node_->is_assigned_from_child_) {
          SQL_PC_LOG(ERROR, "assigned from child diff", K(fp_p_list->node_->is_assigned_from_child_), K(cmp_p_list->node_->is_assigned_from_child_), K(i));
          return false;
        } else if (fp_p_list->node_->is_trans_from_minus_ != cmp_p_list->node_->is_trans_from_minus_) {
          SQL_PC_LOG(ERROR, "is trans from minus diff", K(fp_p_list->node_->is_trans_from_minus_), K(cmp_p_list->node_->is_trans_from_minus_), K(i));
          return false;
        } else if (fp_p_list->node_->is_num_must_be_pos_ != cmp_p_list->node_->is_num_must_be_pos_) {
          SQL_PC_LOG(ERROR, "is num must be pos diff", K(fp_p_list->node_->is_num_must_be_pos_), K(cmp_p_list->node_->is_num_must_be_pos_), K(i));
          return false;
        } else if (fp_p_list->node_->value_ != cmp_p_list->node_->value_) {
          SQL_PC_LOG(ERROR, "value diff", K(fp_p_list->node_->value_), K(cmp_p_list->node_->value_), K(i));
          return false;
        } else if (fp_p_list->node_->pos_ != cmp_p_list->node_->pos_) {
          SQL_PC_LOG(ERROR, "pos diff", K(fp_p_list->node_->pos_), K(cmp_p_list->node_->pos_), K(i));
          return false;
        } else if (fp_p_list->node_->text_len_ != cmp_p_list->node_->text_len_) {
          SQL_PC_LOG(ERROR, "text len diff", K(fp_p_list->node_->text_len_), K(cmp_p_list->node_->text_len_), K(i));
          return false;
        } else if (0 != strncmp(fp_p_list->node_->raw_text_,
                                cmp_p_list->node_->raw_text_,
                                fp_p_list->node_->text_len_)) {
          SQL_PC_LOG(ERROR, "raw_text diff", K(i), K(ObString(fp_p_list->node_->text_len_, fp_p_list->node_->raw_text_)),
          K(ObString(fp_p_list->node_->text_len_, cmp_p_list->node_->raw_text_)));
          return false;
        } else if (fp_p_list->node_->str_len_ != cmp_p_list->node_->str_len_) {
          SQL_PC_LOG(ERROR, "str len diff", K(fp_p_list->node_->str_len_), K(cmp_p_list->node_->str_len_), K(i));
          return false;
        } else if (0 != strncmp(fp_p_list->node_->str_value_,
                                cmp_p_list->node_->str_value_,
                                fp_p_list->node_->str_len_)) {
          SQL_PC_LOG(ERROR, "str value diff", K(i), K(ObString(fp_p_list->node_->str_len_, fp_p_list->node_->str_value_)),
          K(ObString(fp_p_list->node_->str_len_, cmp_p_list->node_->str_value_)));
          return false;
        } else if (fp_p_list->node_->raw_param_idx_ != cmp_p_list->node_->raw_param_idx_) {
          SQL_PC_LOG(ERROR, "raw param idx diff",
                  K(fp_p_list->node_->raw_param_idx_), K(cmp_p_list->node_->raw_param_idx_), K(i));
          return false;
        } else {
          fp_p_list = fp_p_list->next_;
          cmp_p_list = cmp_p_list->next_;
        }
      }
    }
  }
  return true;
}

int TestFastParser::parse(const ObString &sql)
{
  ObSQLMode mode = SMO_DEFAULT;
  ParseResult parse_result;
  ObCharsets4Parser charsets4parser;
  if (lib::is_oracle_mode()) {
    parse_result.sql_mode_ = (DEFAULT_ORACLE_MODE | SMO_ORACLE);
    mode = (DEFAULT_ORACLE_MODE | SMO_ORACLE);
  } else {
    parse_result.sql_mode_ = DEFAULT_MYSQL_MODE;
    mode = DEFAULT_MYSQL_MODE;
  }
  ObParser parser(allocator_, mode, charsets4parser);
  MEMSET(&parse_result, 0, sizeof(parse_result));
  int ret1 = parser.parse(sql, parse_result, FP_MODE);
  int64_t param_num = 0;
  char *no_param_sql_ptr = NULL;
  int64_t no_param_sql_len = 0;
  ParamList *p_list = NULL; 
  FPContext fp_ctx(charsets4parser);
  fp_ctx.enable_batched_multi_stmt_ = false;
  fp_ctx.is_udr_mode_ = false;
  int ret2 = ObFastParser::parse(sql, fp_ctx, allocator_,
    no_param_sql_ptr, no_param_sql_len, p_list, param_num);
  if ((OB_SUCCESS == ret1) != (OB_SUCCESS == ret2)) {
    SQL_PC_LOG_RET(ERROR, OB_ERROR, "parser results are not equal", K(ret1), K(ret2), K(sql));
    return OB_ERROR;
  }
  if (OB_SUCCESS == ret1) {
    bool is_same = compare_parser_result(parse_result, no_param_sql_ptr,
                                         no_param_sql_len, p_list, param_num);
    return is_same ? OB_SUCCESS : OB_ERROR;
  }
  return OB_SUCCESS;
}

void run()
{
  int ret = OB_SUCCESS;
  const std::string file_path = "test_fast_parser.sql";
  std::vector<std::string> sql_array;
  TestFastParser fast_parser;
  fast_parser.load_sql(file_path, sql_array);
  for (uint32_t i = 0; i < sql_array.size(); i++) {
    ObString sql = ObString::make_string(sql_array.at(i).c_str());
    if (OB_FAIL(fast_parser.parse(sql))) {
      SQL_PC_LOG(ERROR, "parser failed", K(sql));
    }
  }
}
}

int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);
  OB_LOGGER.set_log_level("ERROR");
  OB_LOGGER.set_file_name("test_fast_parser.log", false);
  set_compat_mode(lib::Worker::CompatMode::MYSQL);
  ::test::run();
  set_compat_mode(lib::Worker::CompatMode::ORACLE);
  ::test::run();
  return 0;
}
