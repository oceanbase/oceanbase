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
#include <iostream>
#include <string.h>
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "sql/parser/ob_parser.h"
#include "sql/ob_sql_mode_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/outline/ob_pattern_matcher.h"

namespace oceanbase
{
using namespace common;
using namespace std;
using namespace sql;
namespace share
{
namespace schema
{
static const int64_t BUF_SIZE = 1024*10;

void serialize_obstring(const ObString &raw, ObIAllocator &allocator, ObString &serialized)
{
  int64_t size = raw.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, raw.serialize(buf, size, pos));
  serialized.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos));
}

void deserialize_obstring(const ObString &serialized, ObString &raw)
{
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, raw.deserialize(serialized.ptr(), serialized.length(), pos));
}

void judge_outline_info_equal(const ObOutlineInfo &info_l, const ObOutlineInfo &info_r)
{
   EXPECT_EQ(info_l.get_name_str() , info_r.get_name_str());
   EXPECT_EQ(info_l.get_signature_str() , info_r.get_signature_str());
   EXPECT_EQ(info_l.get_outline_content_str() , info_r.get_outline_content_str());
   EXPECT_EQ(info_l.get_sql_text_str() , info_r.get_sql_text_str());
   EXPECT_EQ(info_l.get_owner_str() , info_r.get_owner_str());
   EXPECT_EQ(info_l.get_version_str() , info_r.get_version_str());
   EXPECT_EQ(info_l.get_tenant_id() , info_r.get_tenant_id());
   EXPECT_EQ(info_l.get_database_id() , info_r.get_database_id());
   EXPECT_EQ(info_l.get_outline_id() , info_r.get_outline_id());
   EXPECT_EQ(info_l.get_schema_version() , info_r.get_schema_version());

   EXPECT_EQ(info_l.get_outline_params_wrapper().get_outline_params().count(),
             info_r.get_outline_params_wrapper().get_outline_params().count());
   for (int64_t i = 0; i < info_l.get_outline_params_wrapper().get_outline_params().count(); i++) {
     const ObMaxConcurrentParam *param_l = info_l.get_outline_params_wrapper().get_outline_params().at(i);
     const ObMaxConcurrentParam *param_r = info_r.get_outline_params_wrapper().get_outline_params().at(i);
     bool is_same = false;
     EXPECT_EQ(OB_SUCCESS, param_l->same_param_as(*param_r, is_same));
     EXPECT_EQ(true, is_same);
   }
}

void init_outline_info(ObOutlineInfo &outline_info)
{
  ObArenaAllocator allocator;
  outline_info.set_name("outline_name");
  outline_info.set_signature("signature");
  outline_info.set_outline_content("outline_content");
  outline_info.set_sql_text("sql_text");
  outline_info.set_owner("owner_name");
  outline_info.set_owner_id(1);
  outline_info.set_version("version");
  outline_info.set_tenant_id(1);
  outline_info.set_database_id(1);
  outline_info.set_outline_id(1);
  outline_info.set_schema_version(1);
  EXPECT_EQ(true, outline_info.is_valid());

  //add param
  ObFixedParam fixed_param;
  fixed_param.offset_ = 90;
  fixed_param.value_.set_int(80);
  ObMaxConcurrentParam param(&allocator);
  param.concurrent_num_= 100;
  EXPECT_EQ(OB_SUCCESS, param.fixed_param_store_.push_back(fixed_param));
  EXPECT_EQ(OB_SUCCESS,  outline_info.add_param(param));
}

TEST(ObSchemaStructTest, test_deep_copy_outline_info)
{
  //create outline_info
  ObOutlineInfo outline_info;
  init_outline_info(outline_info);

  //copy outline_info
  ObOutlineInfo *new_outline = NULL;
  const int64_t size = outline_info.get_convert_size();
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ObDataBuffer data_buf(buf + sizeof(ObOutlineInfo), size - sizeof(ObOutlineInfo));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  new_outline = new (buf) ObOutlineInfo(&data_buf);
  ASSERT_NE(static_cast<ObOutlineInfo *>(NULL), new_outline);
  *new_outline = outline_info;
  EXPECT_EQ(true, new_outline->is_valid());

  //judge equal
  judge_outline_info_equal(*new_outline, outline_info);
}

TEST(ObSchemaStructTest, test_serialize_outline_info)
{
  //create outline_info
  ObOutlineInfo outline_info;
  init_outline_info(outline_info);

  //serialize outline_info
  ObOutlineInfo new_outline;
  const int64_t serialize_size = outline_info.get_serialize_size();
  char buf[serialize_size];
  int64_t buf_len = serialize_size, new_buf_len = 0;
  int64_t pos = 0, new_pos = 0;

  EXPECT_EQ(OB_SUCCESS, outline_info.serialize(buf, buf_len, pos));
  new_buf_len = pos;
  EXPECT_EQ(OB_SUCCESS, new_outline.deserialize(buf, new_buf_len, new_pos));

  //judge equal
  judge_outline_info_equal(new_outline, outline_info);
}

TEST(ObSchemaStructTest, test_gen_limit_sql)
{
  ObString visible_signature("select  '?' from t1 where c1 > ? and c2 = ? order by 1");
  ObString limit_sql;
  ObArenaAllocator allocator;
  ObMaxConcurrentParam param(&allocator);
  ObFixedParam fixed_param1;
  ObSQLSessionInfo session;
  fixed_param1.offset_ = 0;
  fixed_param1.value_.set_int(1);
  EXPECT_EQ(OB_SUCCESS, param.fixed_param_store_.push_back(fixed_param1));
  ObFixedParam fixed_param2;
  fixed_param2.offset_ = 1;
  fixed_param2.value_.set_int(2);
  EXPECT_EQ(OB_SUCCESS, param.fixed_param_store_.push_back(fixed_param2));

  //test
  EXPECT_EQ(OB_SUCCESS, ObOutlineInfo::gen_limit_sql(visible_signature,
                                                     &param,
                                                     session,
                                                     allocator,
                                                     limit_sql));
  ObString result("select  '?' from t1 where c1 > 1 and c2 = 2 order by 1");
  cout<<result.ptr()<<endl;
  cout<<limit_sql.ptr()<<endl;
  EXPECT_EQ(0, strncmp(result.ptr(), limit_sql.ptr(), limit_sql.length()));

  //test
  EXPECT_EQ(OB_SUCCESS, ObOutlineInfo::gen_limit_sql(visible_signature,
                                                     &param,
                                                     session,
                                                     allocator,
                                                     limit_sql));
  cout<<result.ptr()<<endl;
  cout<<limit_sql.ptr()<<endl;
  EXPECT_EQ(0, strncmp(result.ptr(), limit_sql.ptr(), limit_sql.length()));

  //test
  EXPECT_EQ(OB_SUCCESS, ObOutlineInfo::gen_limit_sql(visible_signature,
                                                     &param,
                                                     session,
                                                     allocator,
                                                     limit_sql));
  cout<<result.ptr()<<endl;
  cout<<limit_sql.ptr()<<endl;
  EXPECT_EQ(0, strncmp(result.ptr(), limit_sql.ptr(), limit_sql.length()));

}

TEST(ObSchemaStructTest, test_question_makr_pos)
{
  ObArenaAllocator allocator;
  ObSQLMode sql_mode = 0;
  ObString sql("select * from t1 where c1 > ? and c2 = ? and c3 = '?'");
  ObParser parser(allocator, sql_mode);
  ParseResult parse_result;
  EXPECT_EQ(OB_SUCCESS, parser.parse(sql, parse_result, FP_MODE));
  EXPECT_EQ(3, parse_result.param_node_num_);
}

TEST(ObSchemaStructTest, test_simple_outline_template_metadata)
{
  ObSimpleOutlineSchema non_template;
  ObArenaAllocator allocator;
  non_template.set_tenant_id(1);
  non_template.set_outline_id(1);
  non_template.set_schema_version(1);
  non_template.set_database_id(1);
  ASSERT_EQ(OB_SUCCESS, non_template.set_name(ObString::make_string("ntl")));

  ObString serialized_plain_sig;
  serialize_obstring(ObString::make_string("select * from t1 where c1 = ?"), allocator, serialized_plain_sig);
  ASSERT_EQ(OB_SUCCESS, non_template.set_signature(serialized_plain_sig));
  ASSERT_EQ(OB_SUCCESS, non_template.init_template_metadata(ObString()));
  EXPECT_FALSE(non_template.is_template());
  EXPECT_TRUE(non_template.get_pattern_rules_str().empty());

  ObSimpleOutlineSchema templ;
  templ.set_tenant_id(1);
  templ.set_outline_id(2);
  templ.set_schema_version(1);
  templ.set_database_id(1);
  ASSERT_EQ(OB_SUCCESS, templ.set_name(ObString::make_string("tpl")));

  // Clean signature — no #PR hack needed anymore
  ObString serialized_template_sig;
  serialize_obstring(ObString::make_string("select * from * where c1 = ?"), allocator, serialized_template_sig);
  ASSERT_EQ(OB_SUCCESS, templ.set_signature(serialized_template_sig));
  ASSERT_EQ(OB_SUCCESS, templ.init_template_metadata(ObString::make_string("[]")));
  EXPECT_TRUE(templ.is_template());
  EXPECT_EQ(ObString::make_string("[]"), templ.get_pattern_rules_str());
}

TEST(ObSchemaStructTest, test_outline_mgr_binding_map_only_for_template)
{
  ObOutlineMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  ObArenaAllocator allocator;
  bool has_template = true;
  ObArray<const ObSimpleOutlineSchema *> candidates;
  const ObSimpleOutlineSchema *exact_match = NULL;

  // Non-template outline
  ObSimpleOutlineSchema non_template;
  non_template.set_tenant_id(1);
  non_template.set_outline_id(10);
  non_template.set_schema_version(1);
  non_template.set_database_id(1);
  ASSERT_EQ(OB_SUCCESS, non_template.set_name(ObString::make_string("ntl")));
  ObString serialized_plain_sig;
  serialize_obstring(ObString::make_string("select * from t1 where c1 = ?"), allocator, serialized_plain_sig);
  ASSERT_EQ(OB_SUCCESS, non_template.set_signature(serialized_plain_sig));
  ASSERT_EQ(OB_SUCCESS, non_template.init_template_metadata(ObString()));

  ASSERT_EQ(OB_SUCCESS, mgr.add_outline(non_template));
  ASSERT_EQ(OB_SUCCESS, mgr.has_template_outline(1, has_template));
  EXPECT_FALSE(has_template);
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_infos_with_signature(
      1, 1, non_template.get_signature_str(), false, candidates));
  EXPECT_EQ(0, candidates.count());

  // Template outline — clean signature, no #PR hack
  ObSimpleOutlineSchema templ;
  templ.set_tenant_id(1);
  templ.set_outline_id(11);
  templ.set_schema_version(1);
  templ.set_database_id(1);
  ASSERT_EQ(OB_SUCCESS, templ.set_name(ObString::make_string("tpl")));
  ObString serialized_template_sig;
  serialize_obstring(ObString::make_string("select * from * where c1 = ?"), allocator, serialized_template_sig);
  ASSERT_EQ(OB_SUCCESS, templ.set_signature(serialized_template_sig));
  ASSERT_EQ(OB_SUCCESS, templ.init_template_metadata(ObString::make_string("[]")));

  ASSERT_EQ(OB_SUCCESS, mgr.add_outline(templ));
  ASSERT_EQ(OB_SUCCESS, mgr.has_template_outline(1, has_template));
  EXPECT_TRUE(has_template);

  // Dedup lookup with matching pattern_rules should find the outline
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_schema_with_signature(
      1, 1, templ.get_signature_str(), false, exact_match,
      ObString::make_string("[]")));
  ASSERT_NE(static_cast<const ObSimpleOutlineSchema *>(NULL), exact_match);
  EXPECT_EQ(templ.get_outline_id(), exact_match->get_outline_id());

  // Dedup lookup with empty pattern_rules should NOT find the template outline
  exact_match = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_schema_with_signature(
      1, 1, templ.get_signature_str(), false, exact_match));
  EXPECT_EQ(static_cast<const ObSimpleOutlineSchema *>(NULL), exact_match);

  // Binding match (1:N) should find it by signature alone
  candidates.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_infos_with_signature(
      1, 1, templ.get_signature_str(), false, candidates));
  ASSERT_EQ(1, candidates.count());
  ASSERT_NE(static_cast<const ObSimpleOutlineSchema *>(NULL), candidates.at(0));
  EXPECT_EQ(templ.get_outline_id(), candidates.at(0)->get_outline_id());

  // Second template with same signature but different pattern_rules
  ObSimpleOutlineSchema templ2;
  templ2.set_tenant_id(1);
  templ2.set_outline_id(12);
  templ2.set_schema_version(1);
  templ2.set_database_id(1);
  ASSERT_EQ(OB_SUCCESS, templ2.set_name(ObString::make_string("tpl2")));
  ASSERT_EQ(OB_SUCCESS, templ2.set_signature(templ.get_signature_str()));
  ASSERT_EQ(OB_SUCCESS, templ2.init_template_metadata(ObString::make_string("[{\"x\":1}]")));

  ASSERT_EQ(OB_SUCCESS, mgr.add_outline(templ2));

  // Both templates in binding_match_map (1:N)
  candidates.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_infos_with_signature(
      1, 1, templ.get_signature_str(), false, candidates));
  ASSERT_EQ(2, candidates.count());

  // Dedup: each findable with its own pattern_rules
  exact_match = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_schema_with_signature(
      1, 1, templ.get_signature_str(), false, exact_match,
      ObString::make_string("[]")));
  ASSERT_NE(static_cast<const ObSimpleOutlineSchema *>(NULL), exact_match);
  EXPECT_EQ(11u, exact_match->get_outline_id());

  exact_match = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr.get_outline_schema_with_signature(
      1, 1, templ.get_signature_str(), false, exact_match,
      ObString::make_string("[{\"x\":1}]")));
  ASSERT_NE(static_cast<const ObSimpleOutlineSchema *>(NULL), exact_match);
  EXPECT_EQ(12u, exact_match->get_outline_id());
}

TEST(ObSchemaStructTest, test_pattern_parser_strips_spaces)
{
  sql::ObPatternVarInfo var_info;

  // Case 1: No spaces (baseline)
  ASSERT_EQ(OB_SUCCESS, sql::ObPatternMatcher::parse_pattern(
      ObString::make_string("orders_${S:[a-z]+}"), var_info));
  EXPECT_TRUE(var_info.has_var_);
  EXPECT_EQ(ObString::make_string("orders_"), var_info.prefix_);
  EXPECT_EQ(ObString::make_string("S"), var_info.var_name_);
  EXPECT_EQ(ObString::make_string("[a-z]+"), var_info.var_regex_);
  EXPECT_TRUE(var_info.suffix_.empty());

  // Case 2: Spaces around var_name and regex
  var_info.reset();
  ASSERT_EQ(OB_SUCCESS, sql::ObPatternMatcher::parse_pattern(
      ObString::make_string("orders_${ S : [a-z]+ }"), var_info));
  EXPECT_TRUE(var_info.has_var_);
  EXPECT_EQ(ObString::make_string("orders_"), var_info.prefix_);
  EXPECT_EQ(ObString::make_string("S"), var_info.var_name_);
  EXPECT_EQ(ObString::make_string("[a-z]+"), var_info.var_regex_);

  // Case 3: Spaces in reference variable (no regex)
  var_info.reset();
  ASSERT_EQ(OB_SUCCESS, sql::ObPatternMatcher::parse_pattern(
      ObString::make_string("products_${ S }"), var_info));
  EXPECT_TRUE(var_info.has_var_);
  EXPECT_EQ(ObString::make_string("products_"), var_info.prefix_);
  EXPECT_EQ(ObString::make_string("S"), var_info.var_name_);
  EXPECT_TRUE(var_info.var_regex_.empty());

  // Case 4: Only leading spaces
  var_info.reset();
  ASSERT_EQ(OB_SUCCESS, sql::ObPatternMatcher::parse_pattern(
      ObString::make_string("t_${  X:[0-9]+}"), var_info));
  EXPECT_TRUE(var_info.has_var_);
  EXPECT_EQ(ObString::make_string("X"), var_info.var_name_);
  EXPECT_EQ(ObString::make_string("[0-9]+"), var_info.var_regex_);
}

}//schema
}//share
}//oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_outline_info.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_outline_info.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
