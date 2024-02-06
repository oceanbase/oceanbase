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
#define private public
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/timezone/ob_timezone_info.h"
#undef private

#include <sys/time.h>    
using namespace std;
namespace oceanbase {
namespace common {

#define YEAR_MAX_YEAR 2155
#define YEAR_MIN_YEAR 1901
#define YEAR_BASE_YEAR 1900

class TestJsonTree : public ::testing::Test {
public:
  TestJsonTree()
  {}
  ~TestJsonTree()
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
  DISALLOW_COPY_AND_ASSIGN(TestJsonTree);
};

// ObJsonParser::parse_json_text 单元函数测试
TEST_F(TestJsonTree, test_parse_json_text)
{
  const char *syntaxerr = NULL;
  uint64_t err_offset = 0;
  ObJsonNode *j_node = NULL;
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");

  // 1. 入参检查测试
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(NULL, j_text.ptr(), j_text.length(), 
                                               syntaxerr, &err_offset, j_node));
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(&allocator, NULL, j_text.length(), 
                                               syntaxerr, &err_offset, j_node));
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(&allocator, j_text.ptr(), 0, 
                                               syntaxerr, &err_offset, j_node));
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(), j_text.length(), 
                                        syntaxerr, NULL, j_node));
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(), j_text.length(), 
                                        syntaxerr, &err_offset, j_node));                                             
  common::ObString j_sp_text("ab' as json)n)mit 1oot@mysql");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ObJsonParser::parse_json_text(&allocator,
      j_sp_text.ptr(), j_sp_text.length(), syntaxerr, &err_offset, j_node)); 
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ObJsonParser::parse_json_text(&allocator,
      j_sp_text.ptr(), 2, syntaxerr, &err_offset, j_node)); // 只解析ab

  // 2. 路径覆盖测试,有些异常分支不一定能覆盖到
  // 2.1 成功解析，解析结果不为NULL
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(), j_text.length(), 
                                        syntaxerr, &err_offset, j_node));
  ASSERT_TRUE(j_node != NULL);                                      
  // 2.2 成功解析，解析结果为NULL, syntaxerr不为NULL(这个路径应该是rapidjson出错了，比较难构造用例，不测试)
  // 2.3 解析出错，吐出语法错误内容
  char buf[256] = {0};
  syntaxerr = buf;
  j_node = NULL;
  common::ObString j_err_text("{ \"greeting\" error \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ObJsonParser::parse_json_text(&allocator,
      j_err_text.ptr(), j_err_text.length(), syntaxerr, &err_offset, j_node));
  ASSERT_EQ(NULL, j_node);
  std::cout << "syntaxerr:" << syntaxerr << std::endl;
  std::cout << "err_offset:" << err_offset << std::endl;
  ASSERT_EQ(13, err_offset);
  ASSERT_STREQ("Missing a colon after a name of object member.", syntaxerr);

  common::ObString j_err_text1("}");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ObJsonParser::parse_json_text(&allocator,
      j_err_text1.ptr(), j_err_text1.length(), syntaxerr, &err_offset, j_node));                                                                                                                                                              
}

// add_double_quote 单元函数测试
TEST_F(TestJsonTree, test_add_double_quote)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString str("hello");
  ObJsonBuffer j_buf(&allocator);

  // 1. 入参检查测试
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonBaseUtil::add_double_quote(j_buf, NULL, str.length()));
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), 0));

  // 2. 路径覆盖测试,有些异常分支不一定能覆盖到
  // 2.1 源字符串不带转义字符
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  cout << j_buf.ptr() << endl;
  // 2.2 源字符串带转义字符（测试escape_character函数的所有转义字符）
  str.reset();
  str.assign_ptr("\b\t\n\f\r\"\\", strlen("\b\t\n\f\r\"\\"));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  cout << "quote: " << j_buf.ptr() << endl;
  str.reset();
  str.assign_ptr("\a", strlen("\a"));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  str.reset();
  cout << "quote: " << j_buf.ptr() << endl;
  str.assign_ptr("\0", strlen("\0"));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  str.reset();
  str.assign_ptr("\?", strlen("\?"));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  cout << "quote: " << j_buf.ptr() << endl;
  str.reset();
  str.assign_ptr("\v", strlen("\v"));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseUtil::add_double_quote(j_buf, str.ptr(), str.length()));
  cout << "quote: " << j_buf.ptr() << endl;
}

// ObJsonObject 单元测试
TEST_F(TestJsonTree, test_ObJsonObject)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject obj(&allocator);

  /* 1. ObJsonObject::add测试 */
  // 参数检查测试
  ObString j_str("hello");
  ASSERT_EQ(OB_INVALID_ARGUMENT, obj.add(j_str, NULL));

  // 路径覆盖测试
  ObString k1("k1");
  ObString k2("k2");
  ObString k3("k2");
  ObJsonInt j_int1(1);
  ObJsonInt j_int2(2);
  ObJsonInt j_int3(3);
  ASSERT_EQ(OB_SUCCESS, obj.add(k1, &j_int1));
  ASSERT_EQ(1, obj.element_count());
  ObJsonNode *v1 = obj.get_value(k1);
  ASSERT_TRUE(NULL != v1);
  ObJsonInt *v1_int = static_cast<ObJsonInt *>(v1);
  ASSERT_EQ(1, v1_int->value());
  ASSERT_EQ(OB_SUCCESS, obj.add(k2, &j_int2));
  ASSERT_EQ(2, obj.element_count());
  ObJsonNode *v2 = obj.get_value(k2);
  ASSERT_TRUE(NULL != v2);
  ObJsonInt *v2_int = static_cast<ObJsonInt *>(v2);
  ASSERT_EQ(2, v2_int->value());
  ASSERT_EQ(OB_SUCCESS, obj.add(k3, &j_int3)); // 覆盖k2
  ASSERT_EQ(2, obj.element_count());
  ObJsonNode *v3 = obj.get_value(k3);
  ASSERT_TRUE(NULL != v3);
  ObJsonInt *v3_int = static_cast<ObJsonInt *>(v3);
  ASSERT_EQ(3, v3_int->value());

  /* 2. ObJsonObject::replace测试 */
  // 参数检查测试
  ObJsonInt j_int(1);
  ASSERT_EQ(OB_INVALID_ARGUMENT, obj.replace(NULL, &j_int));
  ASSERT_EQ(OB_INVALID_ARGUMENT, obj.replace(&j_int, NULL));

  // 路径覆盖测试
  ASSERT_EQ(OB_SEARCH_NOT_FOUND, obj.replace(&j_int, &j_int1));
  ASSERT_EQ(OB_SUCCESS, obj.replace(&j_int1, &j_int2));
  v1 = obj.get_value(k1);
  ASSERT_TRUE(NULL != v1);
  v1_int = static_cast<ObJsonInt *>(v1);
  ASSERT_EQ(2, v1_int->value());

  /* 3. ObJsonObject::get测试 */
  // 参数检查测试
  ASSERT_EQ(NULL, obj.get_value(ULLONG_MAX));

  // 路径覆盖测试
  v1 = obj.get_value(0);
  ASSERT_TRUE(NULL != v1);
  v1_int = static_cast<ObJsonInt *>(v1);
  ASSERT_EQ(2, v1_int->value());

  /* 3. ObJsonObject::remove测试 */
  // 路径覆盖测试
  ASSERT_EQ(OB_SUCCESS, obj.remove(k3));
  ASSERT_EQ(1, obj.element_count());
}

// ObJsonArray 单元测试
TEST_F(TestJsonTree, test_ObJsonArray)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray j_arr(&allocator);

  /* 1. ObJsonArray::append测试 */
  // 参数检查测试
  ASSERT_EQ(OB_INVALID_ARGUMENT, j_arr.append(NULL));

  // 路径覆盖测试
  ObJsonInt j_int0(0);
  ASSERT_EQ(OB_SUCCESS, j_arr.append(&j_int0));
  ASSERT_EQ(1, j_arr.element_count());
  ObJsonNode *node = j_arr[0];
  ObJsonInt *v = static_cast<ObJsonInt *>(node);
  ASSERT_EQ(0, v->value());

  /* 2. ObJsonArray::insert测试 */
  // 参数检查测试
  ASSERT_EQ(OB_INVALID_ARGUMENT, j_arr.insert(0, NULL));

  // 路径覆盖测试
  ObJsonInt j_int1(1);
  ASSERT_EQ(OB_SUCCESS, j_arr.insert(1, &j_int1));
  ASSERT_EQ(2, j_arr.element_count());
  node = j_arr[1];
  v = static_cast<ObJsonInt *>(node);
  ASSERT_EQ(1, v->value());
  ObJsonInt j_int2(2);
  ASSERT_EQ(OB_SUCCESS, j_arr.insert(10, &j_int2));
  ASSERT_EQ(3, j_arr.element_count());
  node = j_arr[2];
  v = static_cast<ObJsonInt *>(node);
  ASSERT_EQ(2, v->value());

  /* 3. ObJsonArray::replace测试 */
  // 参数检查测试
  ASSERT_EQ(OB_INVALID_ARGUMENT, j_arr.replace(NULL, &j_int0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, j_arr.replace(&j_int0, NULL));
  ASSERT_EQ(OB_INVALID_ARGUMENT, j_arr.replace(NULL, NULL));

  // 路径覆盖测试
  ObJsonInt j_not_found(10);
  ASSERT_EQ(OB_SEARCH_NOT_FOUND, j_arr.replace(&j_not_found, &j_int0));
  ASSERT_EQ(OB_SUCCESS, j_arr.replace(&j_int0, &j_not_found));
  node = j_arr[0];
  v = static_cast<ObJsonInt *>(node);
  ASSERT_EQ(10, v->value());

  /* 4. ObJsonArray::remove测试 */
  // 参数检查测试
  ASSERT_EQ(OB_ERROR_OUT_OF_RANGE, j_arr.remove(100));
  ObJsonNode *null_ptr = NULL;

  // 路径覆盖测试
  ASSERT_EQ(OB_SUCCESS, j_arr.remove(2));
  ASSERT_EQ(2, j_arr.element_count());
}

TEST_F(TestJsonTree, test_rapijson)
{
  // correct json text
  common::ObString json_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\", \
      \"json_text\" : \"test!\" }");
  common::ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, json_text.ptr(),
      json_text.length(), syntaxerr, NULL, json_tree));
  ASSERT_TRUE(json_tree != NULL);
  
  // null allocator
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(NULL, json_text.ptr(),
      json_text.length(), syntaxerr, NULL, json_tree));
  // null json text
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(&allocator, NULL,
      json_text.length(), syntaxerr, NULL, json_tree));
  // json text length=0
  ASSERT_EQ(OB_ERR_NULL_VALUE, ObJsonParser::parse_json_text(&allocator, json_text.ptr(),
      0, syntaxerr, NULL, json_tree));

  // wrong json text
  const char syntaxerr_info[100] = {0};
  const char *err_info_ptr = syntaxerr_info;
  uint64_t wr_info_offest = 0;
  common::ObString wrong_json_text("{ \"greeting\" , \"Hello!\", \"farewell\" : \"bye-bye!\", \
      \"json_text\" : \"test!\" }");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ObJsonParser::parse_json_text(&allocator,
      wrong_json_text.ptr(), wrong_json_text.length(), err_info_ptr, &wr_info_offest, json_tree));
  std::cout << err_info_ptr << std::endl;
  std::cout << wr_info_offest << std::endl;
  ASSERT_STREQ(err_info_ptr, "Missing a colon after a name of object member.");
  ASSERT_EQ(13, wr_info_offest);
}

TEST_F(TestJsonTree, test_json_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject obj(&allocator);
  ASSERT_EQ(NULL, obj.get_parent());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, obj.json_type());

  ObJsonString v1("v1", 2), v2("v2", 2), v3("v3", 2), v4("v4", 2);
  ObString k1("key1"), k2("key2"), k3("key3");
  ASSERT_EQ(OB_SUCCESS, obj.add(k1, &v1));
  ASSERT_EQ(OB_SUCCESS, obj.add(k2, &v2));
  ASSERT_EQ(OB_SUCCESS, obj.add(k3, &v3));
  ASSERT_EQ(OB_SUCCESS, obj.add(k2, &v4));
  ASSERT_EQ(OB_INVALID_ARGUMENT, obj.add(k2, NULL));
  obj.sort();
  const ObJsonString *node1 = static_cast<const ObJsonString*>(obj.get_value(k1));
  const ObJsonString *node2 = static_cast<const ObJsonString*>(obj.get_value(k2));
  const ObJsonString *node3 = static_cast<const ObJsonString*>(obj.get_value(k3));
  ASSERT_STREQ("v1", node1->value().ptr());
  ASSERT_STREQ("v4", node2->value().ptr()); // v4 覆盖了 v2
  ASSERT_STREQ("v3", node3->value().ptr());
  ASSERT_EQ(OB_SUCCESS, obj.remove(k1));
  node1 = static_cast<const ObJsonString*>(obj.get_value(k1));
  ASSERT_EQ(NULL, node1);
  ASSERT_EQ(OB_SUCCESS, obj.remove(k1));
  obj.clear();
  ASSERT_EQ(0, obj.element_count());
}

TEST_F(TestJsonTree, test_array_consume_int)
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObJsonArray target(&alloc), src(&alloc);
  int val = 0, val_initial = 0;

  // src array add some elements
  ObJsonInt sr_val1(++val);
  ASSERT_EQ(src.append(&sr_val1), OB_SUCCESS);
  ObJsonInt sr_val2(++val);
  ASSERT_EQ(src.append(&sr_val2), OB_SUCCESS);
  ObJsonInt sr_val3(++val);
  ASSERT_EQ(src.append(&sr_val3), OB_SUCCESS);
  ObJsonInt sr_val4(++val);
  ASSERT_EQ(src.append(&sr_val4), OB_SUCCESS);

  // src elements moved to target 
  ASSERT_EQ(target.consume(&alloc, &src), false);
  ASSERT_EQ(target.element_count(), 4);
  ASSERT_EQ(src.element_count(), 0);

  for (int i = 0; i < target.element_count(); ++i) {
    // check value is correct
    ASSERT_EQ((static_cast<ObJsonInt*>(target[i]))->value(), ++val_initial);
  }

  target.clear();
  src.clear();
  
  ASSERT_EQ(target.element_count(), 0);
  ASSERT_EQ(src.element_count(), 0);

  val = 0, val_initial = 0;

  // src array add some elements
  ASSERT_EQ(target.append(&sr_val1), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val2), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val3), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val4), OB_SUCCESS);
  ASSERT_EQ(target.element_count(), 4);

  // src is empty, nothing changed
  ASSERT_EQ(target.consume(&alloc, &src), false);
  for (int i = 0; i < target.element_count(); ++i) {
    // check value is correct
    ASSERT_EQ((static_cast<ObJsonInt*>(target[i]))->value(), ++val_initial);
  }

  val = val_initial = 0;
  ASSERT_EQ(src.append(&sr_val1), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val2), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val3), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val4), OB_SUCCESS);
  ASSERT_EQ(src.element_count(), 4);

  ASSERT_EQ(target.consume(&alloc, &src), false);

  ASSERT_EQ(target.element_count(), 8);
  ASSERT_EQ(src.element_count(), 0);
}

TEST_F(TestJsonTree, test_array_consume_string)
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObJsonArray target(&alloc), src(&alloc);

  ObString str_val1("sr_val1");
  ObJsonString sr_val1(str_val1.ptr(), str_val1.length());
  ASSERT_EQ(src.append(&sr_val1), OB_SUCCESS);

  ObString str_val2("sr_val2");
  ObJsonString sr_val2(str_val2.ptr(), str_val1.length());
  ASSERT_EQ(src.append(&sr_val2), OB_SUCCESS);

  ObString str_val3("sr_val3");
  ObJsonString sr_val3(str_val3.ptr(), str_val1.length());
  ASSERT_EQ(src.append(&sr_val3), OB_SUCCESS);

  ObString str_val4("sr_val4");
  ObJsonString sr_val4(str_val4.ptr(), str_val1.length());
  ASSERT_EQ(src.append(&sr_val4), OB_SUCCESS);

  // src elements moved to target 
  ASSERT_EQ(target.consume(&alloc, &src), false);
  ASSERT_EQ(target.element_count(), 4);
  ASSERT_EQ(src.element_count(), 0);

  ASSERT_STREQ((static_cast<ObJsonString*>(target[0]))->value().ptr(), "sr_val1");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[1]))->value().ptr(), "sr_val2");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[2]))->value().ptr(), "sr_val3");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[3]))->value().ptr(), "sr_val4");

  target.clear();
  src.clear();
  
  ASSERT_EQ(target.element_count(), 0);
  ASSERT_EQ(src.element_count(), 0);

  // src array add some elements
  ASSERT_EQ(target.append(&sr_val1), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val2), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val3), OB_SUCCESS);
  ASSERT_EQ(target.append(&sr_val4), OB_SUCCESS);
  ASSERT_EQ(target.element_count(), 4);

  // src is empty, nothing changed
  ASSERT_EQ(target.consume(&alloc, &src), false);

  ASSERT_EQ(src.append(&sr_val1), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val2), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val3), OB_SUCCESS);
  ASSERT_EQ(src.append(&sr_val4), OB_SUCCESS);
  ASSERT_EQ(src.element_count(), 4);

  ASSERT_EQ(target.consume(&alloc, &src), false);

  ASSERT_STREQ((static_cast<ObJsonString*>(target[4]))->value().ptr(), "sr_val1");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[5]))->value().ptr(), "sr_val2");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[6]))->value().ptr(), "sr_val3");
  ASSERT_STREQ((static_cast<ObJsonString*>(target[7]))->value().ptr(), "sr_val4");

  ASSERT_EQ(target.element_count(), 8);
  ASSERT_EQ(src.element_count(), 0);
}

TEST_F(TestJsonTree, test_object_consume) 
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObJsonObject target(&alloc), src(&alloc);
  
  ObString k1("k1");
  ObString k2("k2");
  ObString k3("k3");
  ObJsonInt j_int1(1);
  ObJsonInt j_int2(2);
  ObJsonInt j_int3(3);
  ASSERT_EQ(OB_SUCCESS, src.add(k1, &j_int1));
  ASSERT_EQ(OB_SUCCESS, src.add(k2, &j_int2));
  ASSERT_EQ(OB_SUCCESS, src.add(k3, &j_int3));
  
  ASSERT_EQ(target.consume(&alloc, &src), OB_SUCCESS);
  ASSERT_EQ(target.element_count(), 3);
  ASSERT_EQ(src.element_count(), 0);

  ASSERT_EQ(OB_SUCCESS, src.add(k1, &j_int1));
  ASSERT_EQ(OB_SUCCESS, src.add(k2, &j_int2));
  ASSERT_EQ(OB_SUCCESS, src.add(k3, &j_int3));

  ASSERT_EQ(target.consume(&alloc, &src), OB_SUCCESS);
  ASSERT_EQ(target.element_count(), 3);
  ASSERT_EQ(src.element_count(), 0);
}

TEST_F(TestJsonTree, test_merge_patch) 
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObJsonObject patch_obj(&alloc), target_obj(&alloc), sub_obj(&alloc);
  ObJsonArray patch_arr(&alloc);
  
  ObString str_key1("str_key1");
  ObString str_key2("str_key2");
  ObString str_key3("str_key3");

  ObJsonString json_str1(str_key1.ptr(), str_key1.length());
  ObJsonString json_str2(str_key2.ptr(), str_key2.length());
  ObJsonString json_str3(str_key3.ptr(), str_key3.length());

  ObJsonInt json_int1(1);
  ObJsonInt json_int2(2);
  ObJsonInt json_int3(3);
  ObJsonNull json_null;

  ASSERT_EQ(sub_obj.add(str_key1, &json_str1), OB_SUCCESS);
  ASSERT_EQ(sub_obj.add(str_key2, &json_str2), OB_SUCCESS);
  ASSERT_EQ(sub_obj.add(str_key3, &json_str3), OB_SUCCESS);

  ASSERT_EQ(target_obj.add(str_key1, &sub_obj), OB_SUCCESS);
  ASSERT_EQ(target_obj.add(str_key2, &json_str2), OB_SUCCESS);
  ASSERT_EQ(target_obj.add(str_key3, &json_str3), OB_SUCCESS);
  
  ASSERT_EQ(patch_obj.add(str_key1, &json_str1), OB_SUCCESS);
  // target_obj, str_key1 is overwritten
  ASSERT_EQ(target_obj.merge_patch(&alloc, &patch_obj), false);
  ASSERT_EQ(target_obj.element_count(), 3);
  ASSERT_EQ(target_obj.get_value(str_key1)->json_type(), ObJsonNodeType::J_STRING);
  ASSERT_STREQ((static_cast<ObJsonString*>(target_obj.get_value(str_key1)))->value().ptr(),
      json_str1.value().ptr());

  // patch null value, ignore
  patch_obj.clear();
  ASSERT_EQ(patch_obj.add(str_key1, &json_null), OB_SUCCESS);
  ASSERT_EQ(target_obj.merge_patch(&alloc, &patch_obj), false);
  ASSERT_EQ(target_obj.element_count(), 2);

  // clear all
  patch_obj.clear();
  target_obj.clear();

  // patch has member which target_obj not exist
  ASSERT_EQ(target_obj.add(str_key2, &json_str2), OB_SUCCESS);
  ASSERT_EQ(target_obj.add(str_key3, &json_str3), OB_SUCCESS);

  ASSERT_EQ(patch_obj.add(str_key1, &json_str1), OB_SUCCESS);
  ASSERT_EQ(target_obj.merge_patch(&alloc, &patch_obj), false);
  ASSERT_EQ(target_obj.element_count(), 3);

  // patch has the same key object
  ASSERT_NE(target_obj.get_value(str_key1), static_cast<ObJsonNode*>(NULL));
  patch_obj.clear();
  ASSERT_EQ(target_obj.add(str_key1, &sub_obj), OB_SUCCESS);
  ASSERT_EQ(target_obj.merge_patch(&alloc, &patch_obj), false);
  ASSERT_EQ(target_obj.get_value(str_key1), &sub_obj);
  ObJsonObject* temp_object = static_cast<ObJsonObject*>(target_obj.get_value(str_key1));
  ASSERT_EQ(temp_object->element_count(), 3);
}

TEST_F(TestJsonTree, test_merge_tree_bugfix)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *result = NULL;

  ObJsonObject obj1(&allocator);
  ObJsonObject obj2(&allocator);
  ObIJsonBase *left_obj = &obj1;
  ObIJsonBase *right_obj = &obj2;

  ObString k1("a");
  ObString k2("b");

  ObJsonInt v1(2);
  ObJsonInt v2(3);

  ASSERT_EQ(OB_SUCCESS, obj1.add(k1, &v1));
  ASSERT_EQ(OB_SUCCESS, obj2.add(k2, &v2));

  ASSERT_EQ(OB_SUCCESS, left_obj->merge_tree(&allocator, right_obj, result));
  ASSERT_TRUE(result != NULL);

  ASSERT_EQ(2, result->element_count());
}

TEST_F(TestJsonTree, test_json_cast_to_int)
{
  int64_t i = 0;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonUint my_uint(0);
  ObJsonInt my_int(0);
  ObJsonString my_str(buf, strlen(buf));
  ObJsonBoolean my_bool(true);
  ObJsonDouble my_double(0.0);
  // uint -> int （10）
  j_base = &my_uint;
  my_uint.set_value(10);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(10, i);

  // uint -> int (LLONG_MAX)
  my_uint.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MAX, i);

  // int -> int (LLONG_MIN)
  j_base = &my_int;
  my_int.set_value(LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MIN, i);

  // int -> int (0)
  my_int.set_value(0);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(0, i);

  // int -> int (LLONG_MAX)
  my_int.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MAX, i);

  // string -> int ("abc")
  j_base = &my_str;
  ASSERT_EQ(strlen("abc"), sprintf(buf, "abc"));
  my_str.set_value(buf, strlen(buf));
  ASSERT_EQ(OB_ERR_DATA_TRUNCATED, j_base->to_int(i));

  // string -> int ("LLONG_MIN")
  j_base = &my_str;
  int length = sprintf(buf, "%lld", LLONG_MIN);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MIN, i);

  // string -> int ("LLONG_MAX")
  j_base = &my_str;
  length = sprintf(buf, "%lld", LLONG_MAX);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MAX, i);

  // string -> int ("0")
  j_base = &my_str;
  length = sprintf(buf, "%d", 0);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(0, i);

  // boolean -> int (true)
  j_base = &my_bool;
  my_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(true, i);

  // boolean -> int (false)
  j_base = &my_bool;
  my_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(false, i);
  
  // number -> int (LLONG_MIN)
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  allocator.free();
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal my_num(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MIN, i);

  // number -> int (0)
  allocator.free();
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(0, i);

  // number -> int (LLONG_MAX)
  allocator.free();
  length = sprintf(buf, "%lld", LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MAX, i);

  // double -> int (LLONG_MIN - 1)
  double d = static_cast<double>(LLONG_MIN);
  my_double.set_value(d - 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MIN, i);

  // double -> int (LLONG_MAX + 1)
  d = static_cast<double>(LLONG_MAX);
  my_double.set_value(d + 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(LLONG_MAX, i);

  // double -> int (3.14159)
  my_double.set_value(3.14159);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_int(i));
  ASSERT_EQ(3, i);
}

TEST_F(TestJsonTree, test_json_cast_to_uint)
{
  uint64_t ui = 0;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonUint my_uint(0);
  ObJsonInt my_int(0);
  ObJsonString my_str(buf, strlen(buf));
  ObJsonBoolean my_bool(true);
  ObJsonDouble my_double(0.0);

  // uint -> uint （10）
  j_base = &my_uint;
  my_uint.set_value(10);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(10, ui);

  // uint -> uint (ULLONG_MAX)
  my_uint.set_value(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(ULLONG_MAX, ui);

  // int -> uint (LLONG_MIN)
  j_base = &my_int;
  my_int.set_value(LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(LLONG_MAX + 1, ui);

  // int -> uint (0)
  my_int.set_value(0);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(0, ui);

  // int -> uint (LLONG_MAX)
  my_int.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(LLONG_MAX, ui);

  // string -> int ("abc")
  j_base = &my_str;
  ASSERT_EQ(strlen("abc"), sprintf(buf, "abc"));
  my_str.set_value(buf, strlen(buf));
  ASSERT_EQ(OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD, j_base->to_uint(ui));

  // string -> uint ("LLONG_MIN")
  j_base = &my_str;
  int length = sprintf(buf, "%lld", LLONG_MIN);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(LLONG_MAX + 1, ui);

  // string -> uint ("ULLONG_MAX")
  j_base = &my_str;
  length = sprintf(buf, "%llu", ULLONG_MAX);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(ULLONG_MAX, ui);

  // string -> uint ("0")
  j_base = &my_str;
  length = sprintf(buf, "%d", 0);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(0, ui);

  // boolean -> uint (true)
  j_base = &my_bool;
  my_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(true, ui);

  // boolean -> uint (false)
  j_base = &my_bool;
  my_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(false, ui);
  
  // number -> uint (LLONG_MIN)
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  allocator.free();
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal my_num(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(LLONG_MAX + 1, ui);

  // number -> uint (0)
  allocator.free();
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(0, ui);

  // number -> uint (ULLONG_MAX)
  allocator.free();
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(ULLONG_MAX, ui);

  // double -> uint (LLONG_MIN)
  double d = static_cast<double>(LLONG_MIN);
  my_double.set_value(d - 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(LLONG_MAX + 1, ui);

  // double -> uint (ULLONG_MAX + 1)
  d = static_cast<double>(ULLONG_MAX);
  my_double.set_value(d + 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(ULLONG_MAX, ui);

  // double -> uint (3.14159)
  my_double.set_value(3.14159);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_uint(ui));
  ASSERT_EQ(3, ui);
}

TEST_F(TestJsonTree, test_json_cast_to_double)
{
  double dou = 0.0;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonUint my_uint(0);
  ObJsonInt my_int(0);
  ObJsonString my_str(buf, strlen(buf));
  ObJsonBoolean my_bool(true);
  ObJsonDouble my_double(0.0);

  // uint -> double （10）
  j_base = &my_uint;
  my_uint.set_value(10);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(10, dou);

  // uint -> double (ULLONG_MAX)
  my_uint.set_value(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(ULLONG_MAX, dou);

  // int -> double (LLONG_MIN)
  j_base = &my_int;
  my_int.set_value(LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(LLONG_MIN, dou);

  // int -> double (0)
  my_int.set_value(0);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(0, dou);

  // int -> double (LLONG_MAX)
  my_int.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(LLONG_MAX, dou);

  // string -> double ("abc")
  j_base = &my_str;
  ASSERT_EQ(strlen("abc"), sprintf(buf, "abc"));
  my_str.set_value(buf, strlen(buf));
  ASSERT_EQ(OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD, j_base->to_double(dou));

  // string -> double ("LLONG_MIN")
  j_base = &my_str;
  int length = sprintf(buf, "%lld", LLONG_MIN);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(LLONG_MIN, dou);

  // string -> double ("ULLONG_MAX")
  j_base = &my_str;
  length = sprintf(buf, "%llu", ULLONG_MAX);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(ULLONG_MAX, dou);

  // string -> double ("0")
  j_base = &my_str;
  length = sprintf(buf, "%d", 0);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(0, dou);

  // boolean -> double (true)
  j_base = &my_bool;
  my_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(true, dou);

  // boolean -> double (false)
  j_base = &my_bool;
  my_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(false, dou);
  
  // number -> double (LLONG_MIN)
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  allocator.free();
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal my_num(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(LLONG_MAX + 1, dou);

  // number -> double (0)
  allocator.free();
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(0, dou);

  // number -> double (ULLONG_MAX)
  allocator.free();
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(ULLONG_MAX, dou);

  // double -> double (DBL_MIN)
  my_double.set_value(DBL_MIN);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(DBL_MIN, dou);

  // double -> double (DBL_MAX)
  my_double.set_value(DBL_MAX);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(DBL_MAX, dou);

  // double -> double (3.14159)
  my_double.set_value(3.14159);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_double(dou));
  ASSERT_EQ(3.14159, dou);
}

TEST_F(TestJsonTree, test_json_cast_to_number)
{
  int64_t to_int = 0;
  int64_t pos = 0;
  number::ObNumber nmb;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonUint my_uint(0);
  ObJsonInt my_int(0);
  ObJsonString my_str(buf, strlen(buf));
  ObJsonBoolean my_bool(true);
  ObJsonDouble my_double(0.0);

  // number -> number (LLONG_MIN)
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  allocator.free();
  int length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal my_num(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(LLONG_MIN, to_int);

  // number -> number (0)
  allocator.free();
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(0, to_int);

  // number -> number (ULLONG_MAX)
  allocator.free();
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(0, nmb.compare(num));

  // uint -> number （10）
  j_base = &my_uint;
  my_uint.set_value(10);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(10, to_int);

  // uint -> number (ULLONG_MAX)
  my_uint.set_value(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, nmb.format(buf, sizeof(buf), pos, -1));
  char *endptr = NULL;
  int err = 0;
  double dou = ObCharset::strntod(buf, pos, &endptr, &err);
  ASSERT_EQ(ULLONG_MAX, dou);

  // int -> number (LLONG_MIN)
  j_base = &my_int;
  my_int.set_value(LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(LLONG_MIN, to_int);

  // int -> number (0)
  my_int.set_value(0);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(0, to_int);

  // int -> number (LLONG_MAX)
  my_int.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(LLONG_MAX, to_int);

  // string -> number ("abc")
  MEMSET(buf, 0, sizeof(buf));
  j_base = &my_str;
  ASSERT_EQ(strlen("abc"), sprintf(buf, "abc"));
  my_str.set_value(buf, strlen(buf));
  ASSERT_EQ(OB_INVALID_NUMERIC, j_base->to_number(&alloc, nmb));

  // string -> number ("LLONG_MIN")
  MEMSET(buf, 0, sizeof(buf));
  j_base = &my_str;
  length = sprintf(buf, "%lld", LLONG_MIN);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(LLONG_MIN, to_int);

  // string -> number ("ULLONG_MAX")
  MEMSET(buf, 0, sizeof(buf));
  j_base = &my_str;
  length = sprintf(buf, "%llu", ULLONG_MAX);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, nmb.format(buf, sizeof(buf), pos, -1));
  uint64_t ui = ObCharset::strntoullrnd(buf, sizeof(buf), true, &endptr, &err);
  ASSERT_EQ(ULLONG_MAX, ui);

  // string -> number ("0")
  MEMSET(buf, 0, sizeof(buf));
  j_base = &my_str;
  length = sprintf(buf, "%d", 0);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(0, to_int);

  // boolean -> number (true)
  j_base = &my_bool;
  my_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(true, to_int);

  // boolean -> number (false)
  j_base = &my_bool;
  my_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  ASSERT_EQ(OB_SUCCESS, nmb.cast_to_int64(to_int));
  ASSERT_EQ(false, to_int);

  // double -> number (-123.45678)
  MEMSET(buf, 0, sizeof(buf));
  my_double.set_value(-123.45678);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, nmb.format(buf, sizeof(buf), pos, -1));
  dou = ObCharset::strntod(buf, pos, &endptr, &err);
  ASSERT_EQ(-123.45678, dou);

  // double -> number (123.45678)
  MEMSET(buf, 0, sizeof(buf));
  my_double.set_value(123.45678);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, nmb.format(buf, sizeof(buf), pos, -1));
  dou = ObCharset::strntod(buf, pos, &endptr, &err);
  ASSERT_EQ(123.45678, dou);

  // double -> number (DBL_MIN)
  my_double.set_value(DBL_MIN);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_number(&alloc, nmb));

  // double -> number (DBL_MAN)
  my_double.set_value(DBL_MAX);
  j_base = &my_double;
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, j_base->to_number(&alloc, nmb));
}

static bool ob_time_eq(const ObTime& ans, int64_t year, int64_t month, int64_t day,
    int64_t hour, int64_t minute, int64_t second, int64_t usecond)
{
  bool ret = (ans.parts_[DT_YEAR] == year || -1 == year) && (ans.parts_[DT_MON] == month || -1 == month) &&
             (ans.parts_[DT_MDAY] == day || -1 == day) && (ans.parts_[DT_HOUR] == hour || -1 == hour) &&
             (ans.parts_[DT_MIN] == minute || -1 == minute) && (ans.parts_[DT_SEC] == second || -1 == second) &&
             (ans.parts_[DT_USEC] == usecond || -1 == usecond);
  if (!ret) {
    printf("%04u-%02u-%02u %02u:%02u:%02u.%06u\n",
        ans.parts_[DT_YEAR],
        ans.parts_[DT_MON],
        ans.parts_[DT_MDAY],
        ans.parts_[DT_HOUR],
        ans.parts_[DT_MIN],
        ans.parts_[DT_SEC],
        ans.parts_[DT_USEC]);
  }
  return ret;
}

TEST_F(TestJsonTree, test_json_cast_to_datetime)
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  int64_t datetime = 0;
  int32_t date = 0;

  // datetime -> datetime
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time1));
  ASSERT_TRUE(ob_time_eq(ob_time1, 2015, 4, 15, 9, 22, 7, 0));
  ObJsonDatetime j_datetime(ob_time1, ObDateTimeType);
  j_base = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_base->to_datetime(datetime));
  ASSERT_EQ(1429089727 * USECS_PER_SEC, datetime);

  // date -> datetime
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(0, ob_time2));
  EXPECT_TRUE(ob_time_eq(ob_time2, 1970, 1, 1, 0, 0, 0, 0));
  j_datetime.set_value(ob_time2);
  j_datetime.set_field_type(ObDateType);
  j_base = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_base->to_datetime(datetime));
  ASSERT_EQ(0, datetime);

  ObTime ob_time3;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time3));
  EXPECT_TRUE(ob_time_eq(ob_time3, 2015, 4, 15, 0, 0, 0, 0));
  j_datetime.set_value(ob_time3);
  j_base = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_base->to_datetime(datetime));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(datetime, NULL, date));
  ASSERT_EQ(16540, date);

  // timestamp -> datetime
  ObTime ob_time4;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data, NULL, ob_time4));
  j_datetime.set_value(ob_time4);
  j_datetime.set_field_type(ObTimestampType);
  j_base = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_base->to_datetime(datetime));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(datetime, NULL, date));
  ASSERT_EQ(16540, date);
  
  // string -> datetime
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 11:12:00.1234567");
  ObJsonString j_str(buf, strlen(buf));
  j_base = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_base->to_datetime(datetime));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(datetime, NULL, date));
  ASSERT_EQ(16540, date);
}

TEST_F(TestJsonTree, test_json_cast_to_date)
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  int32_t date = 0;

  // datetime -> date
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time1));
  ASSERT_TRUE(ob_time_eq(ob_time1, 2015, 4, 15, 9, 22, 7, 0));
  ObJsonDatetime j_date(ob_time1, ObDateTimeType);
  j_base = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_base->to_date(date));
  ASSERT_EQ(16540, date);

  // date -> date
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(0, ob_time2));
  EXPECT_TRUE(ob_time_eq(ob_time2, 1970, 1, 1, 0, 0, 0, 0));
  j_date.set_value(ob_time2);
  j_date.set_field_type(ObDateType);
  j_base = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_base->to_date(date));
  ASSERT_EQ(0, date);

  ObTime ob_time3;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time3));
  EXPECT_TRUE(ob_time_eq(ob_time3, 2015, 4, 15, 0, 0, 0, 0));
  j_date.set_value(ob_time3);
  j_base = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_base->to_date(date));
  ASSERT_EQ(16540, date);

  // timestamp -> date
  ObTime ob_time4;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data, NULL, ob_time4));
  j_date.set_value(ob_time4);
  j_date.set_field_type(ObTimestampType);
  j_base = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_base->to_date(date));
  ASSERT_EQ(16540, date);
  
  // string -> date
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 11:12:00.1234567");
  ObJsonString j_str(buf, strlen(buf));
  j_base = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_base->to_date(date));
  ASSERT_EQ(16540, date);
}

TEST_F(TestJsonTree, test_json_cast_to_time)
{
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  int64_t time = 0;

  // time -> time
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time1));
  ASSERT_TRUE(ob_time_eq(ob_time1, 0, 0, 0, 12, 5, 9, 1234));
  ObJsonDatetime j_time(ob_time1, ObTimeType);
  j_base = &j_time;
  ASSERT_EQ(OB_SUCCESS, j_base->to_time(time));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, time);
  
  // string -> time
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "12:05:09.001234");
  ObJsonString j_str(buf, strlen(buf));
  j_base = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_base->to_time(time));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, time);
}

TEST_F(TestJsonTree, test_json_cast_to_bit)
{
  uint64_t bit = 0;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  common::ObArenaAllocator alloc(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonUint my_uint(0);
  ObJsonInt my_int(0);
  ObJsonString my_str(buf, strlen(buf));
  ObJsonBoolean my_bool(true);
  ObJsonDouble my_double(0.0);

  // uint -> bit （10）
  j_base = &my_uint;
  my_uint.set_value(10);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(10, bit);

  // uint -> bit (ULLONG_MAX)
  my_uint.set_value(ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(ULLONG_MAX, bit);

  // int -> bit (LLONG_MIN)
  j_base = &my_int;
  my_int.set_value(LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(LLONG_MAX + 1, bit);

  // int -> bit (0)
  my_int.set_value(0);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(0, bit);

  // int -> bit (LLONG_MAX)
  my_int.set_value(LLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(LLONG_MAX, bit);

  // string -> bit ("a")
  j_base = &my_str;
  ASSERT_EQ(strlen("a"), sprintf(buf, "a"));
  my_str.set_value(buf, strlen(buf));
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(97, bit);

  // string -> bit ("LLONG_MIN")
  j_base = &my_str;
  int length = sprintf(buf, "%lld", LLONG_MIN);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, j_base->to_bit(bit));

  // string -> bit ("ULLONG_MAX")
  j_base = &my_str;
  length = sprintf(buf, "%llu", ULLONG_MAX);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, j_base->to_bit(bit));

  // string -> bit ("0")
  j_base = &my_str;
  length = sprintf(buf, "%d", 0);
  my_str.set_value(buf, length);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(48, bit);

  // boolean -> bit (true)
  j_base = &my_bool;
  my_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(true, bit);

  // boolean -> bit (false)
  j_base = &my_bool;
  my_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(false, bit);
  
  // number -> bit (LLONG_MIN)
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  allocator.free();
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal my_num(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(LLONG_MAX + 1, bit);

  // number -> bit (0)
  allocator.free();
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(0, bit);

  // number -> bit (ULLONG_MAX)
  allocator.free();
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  my_num.set_value(num);
  j_base = &my_num;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(ULLONG_MAX, bit);

  // double -> bit (LLONG_MIN)
  double d = static_cast<double>(LLONG_MIN);
  my_double.set_value(d - 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(LLONG_MAX + 1, bit);

  // double -> bit (ULLONG_MAX + 1)
  d = static_cast<double>(ULLONG_MAX);
  my_double.set_value(d + 1);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(ULLONG_MAX, bit);

  // double -> bit (3.14159)
  my_double.set_value(3.14159);
  j_base = &my_double;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(3, bit);

  // datetime -> bit
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time1));
  ASSERT_TRUE(ob_time_eq(ob_time1, 2015, 4, 15, 9, 22, 7, 0));
  ObJsonDatetime j_datetime(ob_time1, ObDateTimeType);
  j_base = &j_datetime;
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, j_base->to_bit(bit));

  // date -> bit
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(0, ob_time2));
  EXPECT_TRUE(ob_time_eq(ob_time2, 1970, 1, 1, 0, 0, 0, 0));
  ObJsonDatetime j_date(ob_time2, ObDateType);
  j_base = &j_date;
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, j_base->to_bit(bit));

  ObTime ob_time3;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time3));
  EXPECT_TRUE(ob_time_eq(ob_time3, 2015, 4, 15, 0, 0, 0, 0));
  j_date.set_value(ob_time3);
  j_base = &j_date;
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, j_base->to_bit(bit));

  // timestamp -> bit
  MEMSET(buf, 0, sizeof(buf));
  ObTime ob_time4;
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data, NULL, ob_time4));
  ObJsonDatetime j_timestamp(ob_time4, ObTimestampType);
  j_base = &j_timestamp;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(0, bit);
  
  // time -> bit
  ObTime ob_time5;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time5));
  ASSERT_TRUE(ob_time_eq(ob_time5, 0, 0, 0, 12, 5, 9, 1234));
  ObJsonDatetime j_time(ob_time5, ObTimeType);
  j_base = &j_time;
  ASSERT_EQ(OB_SUCCESS, j_base->to_bit(bit));
  ASSERT_EQ(3544959835419848761, bit);
}

TEST_F(TestJsonTree, test_get_serialize_size)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  uint64_t err_offset = 0;
  uint64_t serialize_size = 0;
  ObJsonNode *j_node = NULL;

  // object
  common::ObString j_obj_text("{ \"k1\" : 1, \"k2\" : \"a\" }");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_obj_text.ptr(), j_obj_text.length(), 
                                        syntaxerr, &err_offset, j_node));
  serialize_size = j_node->get_serialize_size();
  cout << "============ object serialize_size:" << serialize_size << endl;
  ASSERT_EQ(19, serialize_size);
  
  // array
  common::ObString j_arr_text("[1, \"abc\", 1.2, null, 0, [123, 1, 2]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_arr_text.ptr(), j_arr_text.length(), 
                                        syntaxerr, &err_offset, j_node));
  serialize_size = j_node->get_serialize_size();
  cout << "============ array serialize_size:" << serialize_size << endl;
  ASSERT_EQ(44, serialize_size);

  // int
  ObJsonInt j_int(1);
  j_node = &j_int;
  serialize_size = j_node->get_serialize_size();
  cout << "============ int(1) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(1, serialize_size);
  j_int.set_value(LLONG_MAX);
  serialize_size = j_node->get_serialize_size();
  cout << "============ int(LLONG_MAX) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(9, serialize_size);
  j_int.set_value(LLONG_MIN);
  serialize_size = j_node->get_serialize_size();
  cout << "============ int(LLONG_MAX) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(10, serialize_size);

  // uint
  ObJsonUint j_uint(1);
  j_node = &j_uint;
  serialize_size = j_node->get_serialize_size();
  cout << "============ uint(1) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(1, serialize_size);
  j_uint.set_value(ULLONG_MAX);
  serialize_size = j_node->get_serialize_size();
  cout << "============ uint(ULLONG_MAX) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(10, serialize_size);

  // null
  ObJsonNull j_null;
  j_node = &j_null;
  serialize_size = j_node->get_serialize_size();
  cout << "============ null serialize_size:" << serialize_size << endl;
  ASSERT_EQ(1, serialize_size);

  // decimal
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  number::ObNumber num;
  sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num);
  j_node = &j_dec;
  serialize_size = j_node->get_serialize_size();
  cout << "============ decimal(LLONG_MIN) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(20, serialize_size);
  MEMSET(buf, 0, sizeof(buf));
  sprintf(buf, "%d", 1);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  j_dec.set_value(num);
  serialize_size = j_node->get_serialize_size();
  cout << "============ decimal(1) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(12, serialize_size);
  MEMSET(buf, 0, sizeof(buf));
  sprintf(buf, "%lld", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  j_dec.set_value(num);
  serialize_size = j_node->get_serialize_size();
  cout << "============ decimal(ULLONG_MAX) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(12, serialize_size);

  // double
  ObJsonDouble j_double(1.0);
  j_node = &j_double;
  serialize_size = j_node->get_serialize_size();
  cout << "============ double(1.0) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(8, serialize_size);
  j_double.set_value(DBL_MAX);
  serialize_size = j_node->get_serialize_size();
  cout << "============ double(DBL_MAX) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(8, serialize_size);
  j_double.set_value(DBL_MIN);
  serialize_size = j_node->get_serialize_size();
  cout << "============ double(DBL_MIN) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(8, serialize_size);

  // string
  ObJsonString j_str("a", strlen("a"));
  j_node = &j_str;
  serialize_size = j_node->get_serialize_size();
  cout << "============ string(a) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(2, serialize_size);
  j_str.set_value("abcdefghijklmnobqrst", strlen("abcdefghijklmnobqrst"));
  serialize_size = j_node->get_serialize_size();
  cout << "============ string(abcdefghijklmnobqrst) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(21, serialize_size);

  // bool
  ObJsonBoolean j_bool(true);
  j_node = &j_bool;
  serialize_size = j_node->get_serialize_size();
  cout << "============ bool(true) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(1, serialize_size);
  j_bool.set_value(false);
  serialize_size = j_node->get_serialize_size();
  cout << "============ bool(false) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(1, serialize_size);

  // date
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time1));
  EXPECT_TRUE(ob_time_eq(ob_time1, 2015, 4, 15, 0, 0, 0, 0));
  ObJsonDatetime j_date(ob_time1, ObDateType);
  j_node = &j_date;
  serialize_size = j_node->get_serialize_size();
  cout << "============ date(16540) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(4, serialize_size);

  // time
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time2));
  ASSERT_TRUE(ob_time_eq(ob_time2, 0, 0, 0, 12, 5, 9, 1234));
  ObJsonDatetime j_time(ob_time2, ObTimeType);
  j_node = &j_time;
  serialize_size = j_node->get_serialize_size();
  cout << "============ time(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(8, serialize_size);

  // datetime
  ObTime ob_time3;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time3));
  ASSERT_TRUE(ob_time_eq(ob_time3, 2015, 4, 15, 9, 22, 7, 0));
  ObJsonDatetime j_datetime(ob_time3, ObDateTimeType);
  j_node = &j_datetime;
  serialize_size = j_node->get_serialize_size();
  cout << "============ datetime(1429089727 * USECS_PER_SEC) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(8, serialize_size);

  // opaque
  ObString str("10101011010101010110101010101");
  ObJsonOpaque j_opaque(str, ObBitType);
  j_node = &j_opaque;
  serialize_size = j_node->get_serialize_size();
  cout << "============ opaque(10101011010101010110101010101) serialize_size:" << serialize_size << endl;
  ASSERT_EQ(39, serialize_size);
}

TEST_F(TestJsonTree, test_get_serialize_size_array_one_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  uint64_t serialize_size = 0;
  ObIJsonBase *j_base = NULL;
  void *buf = NULL;
  common::ObString json_val("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");

  // array
  ObJsonArray j_arr(&allocator);
  j_base = &j_arr;
  int64 repeat_num = 10000;
  int64_t times = 0;
  uint64_t last_size = 0;
  uint64_t new_size = 0;
  while(times < repeat_num) {
    buf = allocator.alloc(sizeof(ObJsonString));
    ObJsonString *j_str = (ObJsonString *)new(buf) ObJsonString(json_val.ptr(), json_val.length());
    ObIJsonBase *tmp_base = j_str;
    ASSERT_EQ(OB_SUCCESS, j_base->array_append(tmp_base));
    new_size = j_arr.get_serialize_size();
    ASSERT_GT(new_size, last_size);
    last_size = new_size;
    times++;
  }
}

TEST_F(TestJsonTree, test_get_serialize_size_array_multi_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  uint64_t serialize_size = 0;
  ObIJsonBase *j_base = NULL;
  void *buf = NULL;
  common::ObString json_val("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");

  // array
  ObJsonArray j_arr_root(&allocator);
  j_base = &j_arr_root;
  int64 repeat_num = 10000;
  int64_t times = 0;
  uint64_t last_size = 0;
  uint64_t new_size = 0;
  ObIJsonBase *last_j_base = j_base;
  while(times < repeat_num) {
    // new_array
    buf = allocator.alloc(sizeof(ObJsonArray));
    ObJsonArray *j_arr = (ObJsonArray *)new(buf) ObJsonArray(&allocator);
    ObIJsonBase *tmp_arr_base = j_arr;
    // new_string
    buf = allocator.alloc(sizeof(ObJsonString));
    ObJsonString *j_str = (ObJsonString *)new(buf) ObJsonString(json_val.ptr(), json_val.length());
    ObIJsonBase *tmp_str_base = j_str;
    ASSERT_EQ(OB_SUCCESS, tmp_arr_base->array_append(tmp_str_base)); // [j_str]
    ASSERT_EQ(OB_SUCCESS, last_j_base->array_append(tmp_arr_base));
    last_j_base = tmp_arr_base;
    new_size = j_arr_root.get_serialize_size();
    // cout << "root_serialize_size:" << new_size << endl;
    ASSERT_GT(new_size, last_size);
    last_size = new_size;
    times++;
  }
}

TEST_F(TestJsonTree, test_get_serialize_size_object_one_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  uint64_t serialize_size = 0;
  ObIJsonBase *j_base = NULL;
  char *buf = NULL;
  char key_str[10] = "key";

  // object
  ObJsonObject j_obj(&allocator);
  j_base = &j_obj;
  int64 repeat_num = 10000;
  int64_t times = 0;
  uint64_t last_size = 0;
  uint64_t new_size = 0;
  while(times < repeat_num) {
    // key
    buf = reinterpret_cast<char *>(allocator.alloc(10));
    MEMCPY(buf, key_str, strlen(key_str));
    buf[strlen(key_str)] = '\0';
    ObString key(strlen(key_str), buf);

    // value
    buf = reinterpret_cast<char *>(allocator.alloc(sizeof(ObJsonInt)));
    ObJsonInt *j_int = (ObJsonInt *)new(buf) ObJsonInt(1);
    ObIJsonBase *j_tmp_base = j_int;
    ASSERT_EQ(OB_SUCCESS, j_base->object_add(key, j_tmp_base));
    new_size = j_obj.get_serialize_size();
    // cout << "root_serialize_size:" << new_size << endl;
    ASSERT_GT(new_size, last_size);
    last_size = new_size;
    times++;
  }
}

TEST_F(TestJsonTree, test_get_serialize_size_object_multi_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  uint64_t serialize_size = 0;
  ObIJsonBase *j_base = NULL;
  char *buf = NULL;
  char key_str[10] = "key";

  // object
  ObJsonObject j_obj(&allocator);
  j_base = &j_obj;
  int64 repeat_num = 10000;
  int64_t times = 0;
  uint64_t last_size = 0;
  uint64_t new_size = 0;
  ObIJsonBase *last_j_base = j_base;
  while(times < repeat_num) {
    // new_object
    buf = reinterpret_cast<char *>(allocator.alloc(sizeof(ObJsonObject)));
    ObJsonObject *j_new_obj = (ObJsonObject *)new(buf) ObJsonObject(&allocator);
    ObIJsonBase* tmp_obj_base = j_new_obj;
    // new_key
    buf = reinterpret_cast<char *>(allocator.alloc(10));
    MEMCPY(buf, key_str, strlen(key_str));
    buf[strlen(key_str)] = '\0';
    ObString key(strlen(key_str), buf);
    // new_int
    buf = reinterpret_cast<char *>(allocator.alloc(sizeof(ObJsonInt)));
    ObJsonInt *j_int = (ObJsonInt *)new(buf) ObJsonInt(1);
    ObIJsonBase *tmp_int_base = j_int;
    ASSERT_EQ(OB_SUCCESS, tmp_obj_base->object_add(key, tmp_int_base));
    ASSERT_EQ(OB_SUCCESS, last_j_base->object_add(key, tmp_obj_base));
    last_j_base = tmp_obj_base;
    new_size = j_obj.get_serialize_size();
    // cout << "root_serialize_size:" << new_size << endl;
    ASSERT_GT(new_size, last_size);
    last_size = new_size;
    times++;
  }
}

TEST_F(TestJsonTree, test_clone_node_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  uint64_t err_offset = 0;
  uint64_t serialize_size = 0;
  ObJsonNode *j_node = NULL;
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  
  // array
  common::ObString j_arr_text("[1, \"abc\", 1.2, null, 0, [123, 1, 2]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_arr_text.ptr(), j_arr_text.length(), 
                                        syntaxerr, &err_offset, j_node));

  j_base = j_node;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "array_without_quote: " << j_buf.ptr() << endl;
  ASSERT_EQ(j_node->json_type(), ObJsonNodeType::J_ARRAY);

  ObJsonBuffer new_node_buf(&allocator);
  new_node_buf.reset();
  ObJsonNode *new_node = j_node->clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(new_node_buf, false));
  cout << "array_without_quote: " << new_node_buf.ptr() << endl;
  ASSERT_EQ(new_node->json_type(), ObJsonNodeType::J_ARRAY);
  ASSERT_STREQ(new_node_buf.ptr(), j_buf.ptr());
}

static int create_object(common::ObIAllocator *allocator, const char *key_ptr, 
                         uint64_t length, ObJsonNode *value, ObJsonObject *&j_obj)
{
  int ret = OB_SUCCESS;
  void *obj_buf = allocator->alloc(sizeof(ObJsonObject));
  void *key_buf = allocator->alloc(sizeof(ObString));
  void *key_val = allocator->alloc(sizeof(char) * length);
  ObJsonObject *obj = new (obj_buf) ObJsonObject(allocator);
  MEMCPY(key_val, key_ptr, length);
  ObString *key = new (key_buf) ObString(length, (char *)key_val);

  if (value == NULL) {
    void *val_buf = allocator->alloc(sizeof(ObJsonInt));
    value = new (val_buf) ObJsonInt(1);
  }

  if (OB_FAIL(obj->add(*key, value))) {
    ret = OB_ERR_UNEXPECTED;
  } else{
    j_obj = obj;
  }

  return ret;
}

TEST_F(TestJsonTree, test_clone_node_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonObject *last_obj = NULL;
  ObJsonObject *new_obj = NULL;
  char key_buf[10] = {0};

  // 1. 深度测试
  // 99个对象嵌套
  for (uint32_t i = 0; i < 100; i++) {
    sprintf(key_buf, "key%d", i);
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, create_object(&allocator, key_buf, strlen(key_buf), NULL, new_obj));
    } else {
      ASSERT_EQ(OB_SUCCESS, create_object(&allocator, key_buf, strlen(key_buf), last_obj, new_obj));
    }
    last_obj = new_obj;
  }
  j_base = new_obj;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "object_without_quote: " << j_buf.ptr() << endl;

  ObJsonBuffer new_node_buf(&allocator);
  new_node_buf.reset();
  ObJsonNode *new_node = new_obj->clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(new_node_buf, false));
  cout << "object_without_quote: " << new_node_buf.ptr() << endl;
  ASSERT_STREQ(new_node_buf.ptr(), j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（boolean类型）
TEST_F(TestJsonTree, test_clone_node_boolean)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonBoolean j_bool(true);
  j_base = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "boolean_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("true", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_bool.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "boolean_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("true", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（decimal类型）
TEST_F(TestJsonTree, test_clone_node_decimal)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  number::ObNumber num;
  int length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_decimal(num);

  j_base = &j_decimal;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "date_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("-9223372036854775808", j_buf.ptr());
  
  j_buf.reset();
  ObJsonNode *new_node = j_decimal.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "date_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("-9223372036854775808", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（double类型）
TEST_F(TestJsonTree, test_clone_node_double)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonDouble j_double(DBL_MAX);

  j_base = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "double_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("1.7976931348623157e308", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_double.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "double_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("1.7976931348623157e308", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（null类型）
TEST_F(TestJsonTree, test_clone_node_null)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonNull j_null;

  j_base = &j_null;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "null_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("null", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_null.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "null_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("null", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（opaque类型）
TEST_F(TestJsonTree, test_clone_node_opaque)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObString str("opaque");
  ObJsonOpaque j_opaque(str, ObLongTextType);

  j_base = &j_opaque;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "opaque_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("base64:type251:b3BhcXVl", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_opaque.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "opaque_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("base64:type251:b3BhcXVl", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（string类型）
TEST_F(TestJsonTree, test_clone_node_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObString str("string");
  ObJsonString j_string(str.ptr(), str.length());

  j_base = &j_string;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "string_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("string", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_string.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "string_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("string", j_buf.ptr());
}

TEST_F(TestJsonTree, test_clone_node_int)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonInt j_int(LONGLONG_MIN);

  j_base = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "int_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("-9223372036854775808", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_int.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "int_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("-9223372036854775808", j_buf.ptr());
}

// ObJsonWrapper::print 单元测试（uint类型）
TEST_F(TestJsonTree, test_clone_node_uint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);
  ObJsonUint j_uint(ULLONG_MAX);

  j_base = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "uint_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("18446744073709551615", j_buf.ptr());

  j_buf.reset();
  ObJsonNode *new_node = j_uint.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "uint_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("18446744073709551615", j_buf.ptr());
}

// ObRapidJsonHandler::seeing_value 单元测试
TEST_F(TestJsonTree, test_clone_node_datetime)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base = NULL;
  ObJsonBuffer j_buf(&allocator);

  // datetime
  ObTime ob_time3;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time3));
  ASSERT_TRUE(ob_time_eq(ob_time3, 2015, 4, 15, 9, 22, 7, 0));
  ObJsonDatetime j_datetime(ob_time3, ObDateTimeType);

  j_base = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "datetime_without_quote: " << j_buf.ptr() << endl;

  j_buf.reset();
  ObJsonNode *new_node = j_datetime.clone(&allocator);
  ObIJsonBase *j_new_base = new_node;

  ASSERT_EQ(OB_SUCCESS, j_new_base->print(j_buf, false));
  cout << "datetime_without_quote: " << j_buf.ptr() << endl;
  ASSERT_STREQ("2015-04-15 09:22:07.000000", j_buf.ptr());

}

TEST_F(TestJsonTree, oracle_sub_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  /* contruct type */
  // int olong
  ObJsonOInt o_int(123);
  ObJsonOLong o_long(1234567890);
  size_t MAX_BUF_SIZE = 256;

  // odecimal
  char dec_buf[MAX_BUF_SIZE];
  memset(dec_buf, 0, MAX_BUF_SIZE);
  number::ObNumber num;
  size_t length = sprintf(dec_buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(dec_buf, allocator));
  ObJsonDecimal o_dec(num);

  // odouble ofloat
  ObJsonODouble o_double(0.0);
  ObJsonOFloat o_float(23.23);

  // interval ds
  const char* interval_ds = "interval ds 123";
  char* buf1 = (char*)allocator.alloc(MAX_BUF_SIZE);
  memset(buf1, 0, MAX_BUF_SIZE);
  memcpy(buf1, interval_ds, strlen(interval_ds));
  ObJsonOInterval o_intervalds(buf1, strlen(interval_ds), ObIntervalDSType);

  // interval ym
  const char* interval_ym = "interval ym 123";
  char* buf2 = (char*)allocator.alloc(MAX_BUF_SIZE);
  memset(buf2, 0, MAX_BUF_SIZE);
  memcpy(buf2, interval_ds, strlen(interval_ds));
  ObJsonOInterval o_intervalym(buf2, strlen(interval_ds), ObIntervalDSType);

  // binary
  const char* binary = "binary string";
  char* buf3 = (char*)allocator.alloc(MAX_BUF_SIZE);
  memset(buf3, 0, MAX_BUF_SIZE);
  memcpy(buf3, binary, strlen(binary));
  ObJsonORawString o_binary(buf3, strlen(binary), ObJsonNodeType::J_OBINARY);

  // odate, oracledate, otimestamp, otimestamptz
  ObTime ob_time;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC, NULL, ob_time));
  ObJsonDatetime j_odate(ObJsonNodeType::J_ODATE, ob_time);
  ObJsonDatetime j_oracledate(ObJsonNodeType::J_ORACLEDATE, ob_time);
  ObJsonDatetime j_otimestamp(ObJsonNodeType::J_OTIMESTAMP, ob_time);
  ObJsonDatetime j_otimestamptz(ObJsonNodeType::J_OTIMESTAMPTZ, ob_time);

  /* test print */
  // print
  ObJsonBuffer j_buf(&allocator);
  ObIJsonBase *j_base = nullptr;

  j_base = (ObIJsonBase*)&o_int;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "oint = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_long;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "olong = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_dec;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "odecimal = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_float;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "ofloat = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_binary;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "obinary = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_intervalds;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "ointervalds = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&o_intervalym;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "ointervalmy = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&j_odate;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "odate = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&j_oracledate;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "oracledate = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&j_otimestamp;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "otimestamp = " << j_buf.ptr() << endl;

  j_buf.reuse();
  j_base = (ObIJsonBase*)&j_otimestamptz;
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << "otimestamptz = " << j_buf.ptr() << endl;

  /* clone */
  ObJsonNode *p_oint = o_int.clone(&allocator);
  ASSERT_NE(nullptr, p_oint);

  ObJsonNode *p_olong = o_long.clone(&allocator);
  ASSERT_NE(nullptr, p_olong);

  ObJsonNode *p_ofloat = o_float.clone(&allocator);
  ASSERT_NE(nullptr, p_ofloat);

  ObJsonNode *p_odoube = o_double.clone(&allocator);
  ASSERT_NE(nullptr, p_odoube);

  ObJsonNode *p_ointervalds = o_intervalds.clone(&allocator);
  ASSERT_NE(nullptr, p_ointervalds);

  ObJsonNode *p_ointervalym = o_intervalym.clone(&allocator);
  ASSERT_NE(nullptr, p_ointervalym);

  ObJsonNode *p_obinary = o_binary.clone(&allocator);
  ASSERT_NE(nullptr, p_obinary);

  ObJsonNode *p_odate = j_odate.clone(&allocator);
  ASSERT_NE(nullptr, p_odate);

  ObJsonNode *p_oracledate = j_oracledate.clone(&allocator);
  ASSERT_NE(nullptr, p_oracledate);

  ObJsonNode *p_otimestamp = j_otimestamp.clone(&allocator);
  ASSERT_NE(nullptr, p_otimestamp);

  ObJsonNode *p_otimestamptz = j_otimestamptz.clone(&allocator);
  ASSERT_NE(nullptr, p_otimestamptz);

  double d_value;
  float f_value;
  uint64_t u_value;
  int64_t i_value;

  ASSERT_EQ(OB_SUCCESS, o_int.to_double(d_value));
  ASSERT_EQ(OB_SUCCESS, o_int.to_uint(u_value));

  ASSERT_EQ(OB_SUCCESS, o_float.to_uint(u_value));
  ASSERT_EQ(OB_SUCCESS, o_float.to_double(d_value));
  ASSERT_EQ(OB_SUCCESS, o_float.to_int(i_value));
}

TEST_F(TestJsonTree, test_sort)
{
  // correct json text
  common::ObString json_text("{ \"a\" : \"value1\", \"a\" : \"value2\", \
      \"b\" : \"value3\",  \"b\" : \"value4\" }");
  common::ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, json_text.ptr(),
      json_text.length(), syntaxerr, NULL, json_tree));
  ASSERT_TRUE(json_tree != NULL);

  ObJsonBuffer j_buf(&allocator);
  ASSERT_EQ(json_tree->print(j_buf, false), 0);

  std::string tmp_res(j_buf.ptr());
  std::string result("{\"a\": \"value2\", \"b\": \"value4\"}");
  ASSERT_EQ(result, tmp_res);
}

TEST_F(TestJsonTree, test_big_json)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer j_buf(&allocator);
  ASSERT_EQ(j_buf.reserve(1024 * 1024), 0);
  ASSERT_EQ(j_buf.append("{"), 0);


  static char origin[] = "0123456789abcdef";
  char key_buffer[33] = {0};
  char value_buffer[16] = {0};
  int idx = 0;

  for (int64_t pos = 0; pos < 50000; ++pos) {
    for (int i = 0; i < 32; ++i) {
      idx = ObRandom::rand(0, 15);
      key_buffer[i] = origin[idx];
    }

    ASSERT_EQ(j_buf.append("\""), 0);
    ASSERT_EQ(j_buf.append(key_buffer, 32), 0);
    ASSERT_EQ(j_buf.append("\""), 0);

    ASSERT_EQ(j_buf.append(": "), 0);
    snprintf(value_buffer, 16, "%ld", pos);
    ASSERT_EQ(j_buf.append(value_buffer), 0);

    ASSERT_EQ(j_buf.append(", "), 0);
  }

  j_buf.set_length(j_buf.length() - 2);
  ASSERT_EQ(j_buf.append("}"), 0);

  // correct json text
  common::ObString json_text(j_buf.length(), j_buf.ptr());

  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;

  struct timeval time_start, time_end;
  gettimeofday(&time_start, nullptr);
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, json_text.ptr(),
      json_text.length(), syntaxerr, NULL, json_tree));
  ASSERT_TRUE(json_tree != NULL);

  gettimeofday(&time_end, nullptr);

  cout << "time start : " << " sec = " << time_start.tv_sec << ", usec = " << time_start.tv_usec << endl;
  cout << "time  end  : " << " sec = " << time_end.tv_sec << ", usec = " << time_end.tv_usec << endl;

}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  /*
  system("rm -f test_json_tree.log");
  OB_LOGGER.set_file_name("test_json_tree.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}