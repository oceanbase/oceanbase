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
#include "lib/string/ob_sql_string.h"
#define private public
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_bin.h"
#undef private

using namespace oceanbase::common;

class TestJsonPath : public ::testing::Test {
public:
  TestJsonPath()
  {}
  ~TestJsonPath()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestJsonPath);
};

TEST_F(TestJsonPath, test_get_cnt)
{
  ObJsonPath path(NULL);
  ASSERT_EQ(0, path.path_node_cnt());
}


TEST_F(TestJsonPath, test_is_terminator)
{
  char ch[] = "[. *ibc%^&";
  for(int i = 0; i < sizeof(ch); ++i)
  {
    if (i <= 3) {
      ASSERT_EQ(true, ObJsonPathUtil::is_terminator(ch[i]));
    } else {
      ASSERT_EQ(false, ObJsonPathUtil::is_terminator(ch[i]));
    }
  }
}


TEST_F(TestJsonPath, test_create_basic_node)
{
  // **
  ObJsonPathBasicNode temp1;
  ASSERT_EQ(0, temp1.init(JPN_ELLIPSIS));
  ASSERT_EQ(JPN_ELLIPSIS, temp1.get_node_type());
  ASSERT_EQ(true, temp1.get_node_content().is_had_wildcard_);
  // .*
  ObJsonPathBasicNode temp2;
  ASSERT_EQ(0, temp2.init(JPN_MEMBER_WILDCARD));
  ASSERT_EQ(JPN_MEMBER_WILDCARD, temp2.get_node_type());
  ASSERT_EQ(true, temp2.get_node_content().is_had_wildcard_);
  // [*]
  ObJsonPathBasicNode temp3;
  ASSERT_EQ(0, temp3.init(JPN_ARRAY_CELL_WILDCARD));
  ASSERT_EQ(JPN_ARRAY_CELL_WILDCARD, temp3.get_node_type());
  ASSERT_EQ(true, temp3.get_node_content().is_had_wildcard_);
  // [1]
  ObJsonPathBasicNode temp4(1,false);
  ASSERT_EQ(JPN_ARRAY_CELL, temp4.get_node_type());
  ASSERT_EQ(1, temp4.get_node_content().array_cell_.index_);
  ASSERT_EQ(false, temp4.get_node_content().array_cell_.is_index_from_end_);
  // [last-3 to 6]
  ObJsonPathBasicNode temp5(3, true, 6, false);
  ASSERT_EQ(JPN_ARRAY_RANGE, temp5.get_node_type());
  ASSERT_EQ(3, temp5.get_node_content().array_range_.first_index_);
  ASSERT_EQ(true, temp5.get_node_content().array_range_.is_first_index_from_end_);
  ASSERT_EQ(6, temp5.get_node_content().array_range_.last_index_);
  ASSERT_EQ(false, temp5.get_node_content().array_range_.is_last_index_from_end_);
  // .keyname
  ObString kn("keyname");
  ObJsonPathBasicNode temp6(kn);
  ASSERT_EQ(JPN_MEMBER, temp6.get_node_type());
  std::cout<<temp6.get_node_content().member_.object_name_<<std::endl;

  ObJsonPathNode *fa1 = &temp1;
  ASSERT_EQ(JPN_ELLIPSIS, (static_cast<ObJsonPathBasicNode *> (fa1))->get_node_type());
  ASSERT_EQ(true, fa1->get_node_content().is_had_wildcard_);
}

// test append func
TEST_F(TestJsonPath, test_append)
{
  // append **
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(&allocator);
  ObJsonPathBasicNode temp1;
  ASSERT_EQ(0, temp1.init(JPN_ELLIPSIS));
  ObJsonPathNode *fa1 = &temp1;
  ret = test_path.append(fa1);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_ELLIPSIS, test_path.path_nodes_[0]->get_node_type());
  ASSERT_EQ(true, test_path.path_nodes_[0]->get_node_content().is_had_wildcard_);

  // append .*
  ObJsonPathBasicNode temp2;
  ASSERT_EQ(0, temp2.init(JPN_MEMBER_WILDCARD));
  ObJsonPathNode *fa2 = &temp2;
  ret = test_path.append(fa2);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_MEMBER_WILDCARD, test_path.path_nodes_[1]->get_node_type());
  ASSERT_EQ(true, test_path.path_nodes_[1]->get_node_content().is_had_wildcard_);

  // append [*]
  ObJsonPathBasicNode temp3;
  ASSERT_EQ(0, temp3.init(JPN_ARRAY_CELL_WILDCARD));
  ObJsonPathNode *fa3 = &temp3;
  ret = test_path.append(fa3);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_ARRAY_CELL_WILDCARD, test_path.path_nodes_[2]->get_node_type());
  ASSERT_EQ(true, test_path.path_nodes_[2]->get_node_content().is_had_wildcard_);

  // append array_cell
  ObJsonPathBasicNode temp4(1,false);
  ObJsonPathNode *fa4 = &temp4;
  ret = test_path.append(fa4);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_ARRAY_CELL, test_path.path_nodes_[3]->get_node_type());
  ASSERT_EQ(1, test_path.path_nodes_[3]->get_node_content().array_cell_.index_);
  ASSERT_EQ(false, test_path.path_nodes_[3]->get_node_content().array_cell_.is_index_from_end_);

  // [last-3 to 6]
  ObJsonPathBasicNode temp5(3, true, 6, false);
  ObJsonPathNode *fa5 = &temp5;
  ret = test_path.append(fa5);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_ARRAY_RANGE, test_path.path_nodes_[4]->get_node_type());
  ASSERT_EQ(3, test_path.path_nodes_[4]->get_node_content().array_range_.first_index_);
  ASSERT_EQ(true, test_path.path_nodes_[4]->get_node_content().array_range_.is_first_index_from_end_);
  ASSERT_EQ(6, test_path.path_nodes_[4]->get_node_content().array_range_.last_index_);
  ASSERT_EQ(false, test_path.path_nodes_[4]->get_node_content().array_range_.is_last_index_from_end_);

  // .keyname
  ObString kn("keyname");
  ObJsonPathBasicNode temp6(kn);
  ObJsonPathNode *fa6 = &temp6;
  ret = test_path.append(fa6);
  ASSERT_EQ(OB_SUCCESS,ret);
  ASSERT_EQ(JPN_MEMBER, test_path.path_nodes_[5]->get_node_type());
  std::cout<<"6: "<<test_path.path_nodes_[5]->get_node_content().member_.object_name_<<std::endl;
}

// test parse_array_index() func, to get array_index
TEST_F(TestJsonPath, test_parse_array_index)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[last-10]", &allocator);
  test_path.index_ = 2;
  uint64_t array_index = 0;
  bool from_end = false;
  ret = test_path.parse_array_index(array_index, from_end);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(10, array_index);
  ASSERT_EQ(true, from_end);
}

// test parse_array_node() func, to get array_node
TEST_F(TestJsonPath, test_parse_array_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path1("$[  *   ]", &allocator);
  test_path1.index_ = 1;
  
  ret = test_path1.parse_array_node();
  ASSERT_EQ(1, test_path1.path_node_cnt());
  ASSERT_EQ(ret, OB_SUCCESS);
  // ASSERT_EQ(JPN_ARRAY_CELL_WILDCARD, test_path1.path_nodes_[0]->node_type);
  //ASSERT_EQ(10, test_path1.path_nodes_[0]->path_node_content_.array_cell_.index_);
  //ASSERT_EQ(false, test_path1.path_nodes_[0]->path_node_content_.array_cell_.index_);
}

// test parse array_cell_node
TEST_F(TestJsonPath, test_array_cell_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[10]", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());
  ASSERT_EQ(JPN_ARRAY_CELL,test_path.path_nodes_[0]->get_node_type());
  ASSERT_EQ(10,test_path.path_nodes_[0]->get_node_content().array_cell_.index_);
  ASSERT_EQ(false,test_path.path_nodes_[0]->get_node_content().array_cell_.is_index_from_end_);

}

// test parse array_range_node
TEST_F(TestJsonPath, test_array_range_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[1 to 10]", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());
  ASSERT_EQ(JPN_ARRAY_RANGE,test_path.path_nodes_[0]->get_node_type());
  ASSERT_EQ(1,test_path.path_nodes_[0]->get_node_content().array_range_.first_index_);
  ASSERT_EQ(false,test_path.path_nodes_[0]->get_node_content().array_range_.is_first_index_from_end_);
  ASSERT_EQ(10,test_path.path_nodes_[0]->get_node_content().array_range_.last_index_);
  ASSERT_EQ(false,test_path.path_nodes_[0]->get_node_content().array_range_.is_last_index_from_end_);
}

// test parse array_range_wildcard_node
TEST_F(TestJsonPath, test_array_wildcard_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[*]", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());
  ASSERT_EQ(JPN_ARRAY_CELL_WILDCARD,test_path.path_nodes_[0]->get_node_type());
}

// test parse member_wildcard_node
TEST_F(TestJsonPath, test_member_wildcard_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$.*", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());
  ASSERT_EQ(JPN_MEMBER_WILDCARD,test_path.path_nodes_[0]->get_node_type());
}

// test parse member_node
TEST_F(TestJsonPath, test_member_node)
{
  int ret = OB_SUCCESS;
  ObString str_orgin("$.name");
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(str_orgin, &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  std::cout<<"test"<<std::endl;
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());
  ASSERT_EQ(JPN_MEMBER,test_path.path_nodes_[0]->get_node_type());
  ObString str(test_path.path_nodes_[0]->get_node_content().member_.object_name_);
  ASSERT_TRUE(str.case_compare("name") == 0);
  std::cout<<test_path.path_nodes_[0]->get_node_content().member_.object_name_<<std::endl;
}

// test parse ellipsis_node
TEST_F(TestJsonPath, test_ellipsis_node)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$**[10]", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(2, test_path.path_node_cnt());
  ASSERT_EQ(JPN_ELLIPSIS,test_path.path_nodes_[0]->get_node_type());
  ASSERT_EQ(JPN_ARRAY_CELL,test_path.path_nodes_[1]->get_node_type());
  ASSERT_EQ(10,test_path.path_nodes_[1]->get_node_content().array_cell_.index_);
  ASSERT_EQ(false,test_path.path_nodes_[1]->get_node_content().array_cell_.is_index_from_end_);
}

// test parse path expression
TEST_F(TestJsonPath, test_parse_path)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path("$[last-10 to last-1]", &allocator);
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // only one node
  ASSERT_EQ(1, test_path.path_node_cnt());

  if (OB_SUCC(ret)) {
    for(int i=0; i<test_path.path_node_cnt(); ++i)
    {

      if (i==0) {
        // node type is wildcard
        // ASSERT_EQ(JPN_MEMBER_WILDCARD,test_path.path_nodes_[i]->node_type);
        std::cout<<i<<std::endl;
        //ASSERT_EQ(JPN_ARRAY_CELL_WILDCARD,test_path.path_nodes_[i]->node_type);
        /*
        ASSERT_EQ(JPN_MEMBER,test_path.path_nodes_[i]->node_type);
        std::cout<<test_path.path_nodes_[0]->get_node_content().member_.object_name_<<std::endl;
        ASSERT_EQ(JPN_ARRAY_CELL,test_path.path_nodes_[i]->node_type);
        ASSERT_EQ(10,test_path.path_nodes_[i]->get_node_content().array_cell_.index_);
        ASSERT_EQ(true,test_path.path_nodes_[i]->get_node_content().array_cell_.is_index_from_end_);
        */
        ASSERT_EQ(10,test_path.path_nodes_[i]->get_node_content().array_range_.first_index_);
        ASSERT_EQ(true,test_path.path_nodes_[i]->get_node_content().array_range_.is_first_index_from_end_);
        ASSERT_EQ(1,test_path.path_nodes_[i]->get_node_content().array_range_.last_index_);
        ASSERT_EQ(true,test_path.path_nodes_[i]->get_node_content().array_range_.is_last_index_from_end_);
        
      }
    }
  } else {
    std::cout<<"fail\n";
  }
}


// test tostring
// test array_cell node to string
TEST_F(TestJsonPath, test_array_cell_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPathBasicNode node(10, true);
  node.node_to_string(str);
  std::cout<<str.ptr()<<std::endl;
}

// test array_range node to string
TEST_F(TestJsonPath, test_array_range_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPathBasicNode node(5, true, 1, true);
  node.node_to_string(str);
  std::cout<<"test\n";
  std::cout<<str.ptr()<<std::endl;
}

// test array_wildcard node to string
TEST_F(TestJsonPath, test_array_wildcard_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPathBasicNode node;
  ASSERT_EQ(0, node.init(JPN_ARRAY_CELL_WILDCARD));
  node.node_to_string(str);
  std::cout<<"test\n";
  std::cout<<str.ptr()<<std::endl;
}

// test member_wildcard node to string
TEST_F(TestJsonPath, test_member_wildcard_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPathBasicNode node;
  ASSERT_EQ(0, node.init(JPN_MEMBER_WILDCARD));
  node.node_to_string(str);
  std::cout<<"test\n";
  std::cout<<str.ptr()<<std::endl;
}

// test member node to string
TEST_F(TestJsonPath, test_member_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObString kn("keyname");
  ObJsonPathBasicNode node(kn);
  node.node_to_string(str);
  std::cout<<"test\n";
  std::cout<<str.ptr()<<std::endl;
}

// test ellipsis node to string
TEST_F(TestJsonPath, test_ellipsis_node_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPathBasicNode node;
  ASSERT_EQ(0, node.init(JPN_ELLIPSIS));
  node.node_to_string(str);
  std::cout<<"test\n";
  std::cout<<str.ptr()<<std::endl;
}

// test ObJsonPath::to_string()
TEST_F(TestJsonPath, test_path_to_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  ObJsonPath test_path(&allocator);
  ObJsonPathBasicNode node;
  ASSERT_EQ(0, node.init(JPN_ARRAY_CELL_WILDCARD));
  ObJsonPathNode* fa = &node;
  test_path.append(fa);
  test_path.to_string(str);
  std::cout<<"test1\n";
  std::cout<<str.ptr()<<std::endl;
  std::cout<<"test2\n";
}

// test bad path
// record bad_index
TEST_F(TestJsonPath, test_bad_path)
{
  int ret = OB_SUCCESS;
  ObString str = "$\"abcd\"";
  // int num = 5;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonPath test_path(str, &allocator);
  ret = test_path.parse_path();
  std::cout<<"str:"<<str.ptr()<<std::endl;
  std::cout<<"ret:"<<ret<<std::endl;
  ASSERT_EQ(true, OB_FAIL(ret));
  // ASSERT_EQ(num,test_path.bad_index_);
  std::cout<<test_path.bad_index_<<std::endl;
}

TEST_F(TestJsonPath, test_random)
{
  int dice = 0;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer str(&allocator);
  str.append("$");
  int node_num = 99;
  for (int i=0; i<node_num; ++i) {
    dice = rand() % 5;
    switch (dice) {
      case 0:
      // add JPN_MEMBER
        str.append(".keyname");
        break;

      case 1:
      // add JPN_MEMBER_WILDCARD
        str.append(".*");
        break;
        
      case 2:
      // JPN_ARRAY_CELL
        str.append("[last-5]");
        break;
        
      case 3:
      // JPN_ARRAY_RANGE
        str.append("[10 to last-1]");
        break;
        
      case 4:
      // JPN_ARRAY_CELL_WILDCARD
        str.append("[*]");
        break;
        
      case 5:
      // JPN_ELLIPSIS
        str.append("**");
        break;
        
      default:
        break;
    }
  }
  // to avoid last node is **
  str.append("[1]");

  int ret = OB_SUCCESS;
  ObString str_origin(str.ptr());
  std::cout<<str_origin.ptr()<<std::endl;
  
  ObJsonPath test_path(str_origin, &allocator);
  ret = test_path.parse_path();
  std::cout<<"count:"<<test_path.path_node_cnt()<<std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer str2(&allocator);
  ret = test_path.to_string(str2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString str3(str2.ptr());
  ASSERT_EQ(str_origin, str3);
  std::cout<<str2.ptr()<<std::endl;
  
}
// test good path
// include parse and to_string
TEST_F(TestJsonPath, test_good_path)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  // use to parse 
  ObString str0 = "$.\"abc d\"";
  //ObString str0 = "$.abc.\"\".def";
  //.keyname[last-10 to last-1][5].a
  // ObString str0 = "$[9999999999999].keyname";
  std::cout<<str0.ptr()<<std::endl;
  // use to compare to to_string
  ObString str1 = "$.\"abc d\"";
  // ObString str1 = "$[9999999999999].keyname";
  ObJsonPath test_path(str0, &allocator);
  // parse
  ret = test_path.parse_path();
  ASSERT_EQ(OB_SUCCESS, ret);
  // to_string
  ObJsonBuffer str2(&allocator);
  ret = test_path.to_string(str2);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  for(int i=0; i<test_path.path_node_cnt(); ++i)
  {
    std::cout<<"type:"<<test_path.path_nodes_[i]->get_node_type()<<std::endl;
    if (i==0) {
      std::cout<<"content:"<<test_path.path_nodes_[i]->get_node_content().member_.object_name_<<std::endl;
    }
    if (i==1) {
      std::cout<<"content:"<<test_path.path_nodes_[i]->get_node_content().array_cell_.index_<<std::endl;
      std::cout<<"content:"<<test_path.path_nodes_[i]->get_node_content().array_cell_.is_index_from_end_<<std::endl;
    }
  }
  
  // verify equality
  ObString str3(str2.ptr());
  std::cout<<str2.ptr()<<std::endl;
  ASSERT_EQ(str1, str3);
  std::cout<<"end test"<<std::endl;
}

TEST_F(TestJsonPath, test_pathcache_funcion) {
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  
  ObJsonPathCache path_cache(&allocator);
  ObString ok_path1 = "$.\"abc d\"";

  // initial state verify
  ASSERT_EQ(path_cache.get_allocator(), &allocator);
  ASSERT_EQ(path_cache.path_stat_at(0), ObPathParseStat::UNINITIALIZED);

  // test a nornal json path
  ObJsonPath* json_path = NULL;
  int ok_path1_idx = path_cache.size();

  // parse a normal path
  ret = path_cache.find_and_add_cache(json_path, ok_path1, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(path_cache.path_stat_at(ok_path1_idx), ObPathParseStat::OK_NOT_NULL);

  // can read again
  ObJsonPath* read_ok_path1 = path_cache.path_at(ok_path1_idx);
  ASSERT_STREQ(json_path->get_path_string().ptr(), read_ok_path1->get_path_string().ptr());

  // cache stratety works, iff the path string unchanged, the path needn't parse again
  ret = path_cache.find_and_add_cache(json_path, ok_path1, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(json_path, read_ok_path1);

  ObString ok_path2 = "  $.\"  abc d \"  ";
  // some spaces do not has any effect upon the path cache
  ret = path_cache.find_and_add_cache(json_path, ok_path2, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(json_path, read_ok_path1);
  ObJsonPath* read_ok_path2 = path_cache.path_at(ok_path1_idx);
  ASSERT_EQ(read_ok_path2, read_ok_path1);
}

TEST_F(TestJsonPath, test_pathcache_exprire) {
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  
  ObJsonPathCache path_cache(&allocator);
  ObString ok_path1 = "$.\"abc d\"";

  // test a nornal json path
  ObJsonPath* json_path = NULL;
  int ok_path1_idx = path_cache.size();

  // parse a normal path
  ret = path_cache.find_and_add_cache(json_path, ok_path1, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(path_cache.path_stat_at(ok_path1_idx), ObPathParseStat::OK_NOT_NULL);

  ObJsonPath* read_ok_path1 = path_cache.path_at(ok_path1_idx);

  ObString ok_path2 = "  $.\"efs d \"";
  // some spaces do not has any effect upon the path cache
  ret = path_cache.find_and_add_cache(json_path, ok_path2, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);

  // json path string differs, cache invalid
  ASSERT_NE(json_path, read_ok_path1);
  ObJsonPath* read_ok_path2 = path_cache.path_at(ok_path1_idx);
  ASSERT_EQ(read_ok_path2, json_path);
}

TEST_F(TestJsonPath, test_pathcache_reset) {
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  
  ObJsonPathCache path_cache(&allocator);
  ObString ok_path1 = "$.\"abc d\"";

  // test a nornal json path
  ObJsonPath* json_path = NULL;
  int ok_path1_idx = path_cache.size();

  // parse a normal path
  ret = path_cache.find_and_add_cache(json_path, ok_path1, ok_path1_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(path_cache.path_stat_at(ok_path1_idx), ObPathParseStat::OK_NOT_NULL);

  path_cache.reset();
  ASSERT_EQ(path_cache.path_stat_at(ok_path1_idx), ObPathParseStat::UNINITIALIZED);
  ASSERT_EQ(path_cache.size(), 0);
}


int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_path.log");
  OB_LOGGER.set_file_name("test_json_path.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}
