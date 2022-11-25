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
#include "share/schema/ob_schema_struct.h"

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
  char *buf = static_cast<char *>(ob_malloc(size));
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
  ObSQLModeManager mod_mgr;
  ObString sql("select * from t1 where c1 > ? and c2 = ? and c3 = '?'");
  ObParser parser(allocator, mod_mgr.get_sql_mode());
  ParseResult parse_result;
  EXPECT_EQ(OB_SUCCESS, parser.parse(sql, parse_result, FP_MODE));
  EXPECT_EQ(3, parse_result.param_node_num_);
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
