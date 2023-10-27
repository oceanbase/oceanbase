/**
 * Copyright (c) 2023 OceanBase
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
#include "test_xml_utils.h"
#include "lib/xml/ob_xpath.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/xml/ob_xml_parser.h"
#include "libxml2/libxml/xmlmemory.h"
#include "lib/xml/ob_xml_util.h"
#undef private

using namespace oceanbase::common;

using namespace ObXmlTestUtils;
class TestXPathFilter : public ::testing::Test {
public:
  TestXPathFilter()
  {}
  ~TestXPathFilter()
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
  DISALLOW_COPY_AND_ASSIGN(TestXPathFilter);
};
# define PREDICATE_TEST_COUNT 100
# define PREDICATE_FALSE_TEST_COUNT 100

ObString func_false_enter[PREDICATE_TEST_COUNT] = {
  "/bookstore/book/title[text() = \"aaa\"]",
  "/bookstore/book/title[text() = \"bbb\"]",
};

ObString func_false_enter_parse_result[PREDICATE_TEST_COUNT] = {
  "/bookstore/book/title[text() = \"aaa\"]",
  "/bookstore/book/title[text() = \"bbb\"]",
};

TEST_F(TestXPathFilter, test_false_function)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book category=\"a\"><title lang=\"x\">abc</title> <author>def</author><year month=\"1\">1</year><price>false</price></book><book category=\"b\"><title lang=\"y\">def</title><author>xyz</author><year month=\"2\">2005</year><price>30.00</price></book><book category=\"c\"><title lang=\"en\">xyz</title><author>abc</author><year>2005</year><price>29.99</price></book><book category=\"WEB\"><title lang=\"en\">Learning XML</title><year>2003</year><sell></sell></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < PREDICATE_TEST_COUNT; i++) {
    if (func_false_enter[i].empty()) break;
    ObString enter_xpath = func_false_enter[i];
    std::cout << enter_xpath.ptr() << std::endl;
    ObString default_ns;
    ObPathVarObject pass(allocator);
    ObDatum data;
    data.set_string("ns2");
    ret = pass.add("h", &data);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObPathExprIter pathiter(&allocator);
    pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
    ret = pathiter.open();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObJsonBuffer buf(&allocator);
    ret = pathiter.path_node_->node_to_string(buf);
    std::cout << buf.ptr() << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString parse_result(buf.ptr());
    ASSERT_EQ(func_false_enter_parse_result[i], parse_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}
ObString func_enter[PREDICATE_TEST_COUNT] = {
  "/bookstore/book/title[text() = \"abc\"]",
  "/bookstore/book/title[text() = \"def\"]",
};

ObString func_enter_parse_result[PREDICATE_TEST_COUNT] = {
  "/bookstore/book/title[text() = \"abc\"]",
  "/bookstore/book/title[text() = \"def\"]",
};

ObString func_predicate_result[PREDICATE_TEST_COUNT] = {
  "<title lang=\"x\">abc</title>",
  "<title lang=\"y\">def</title>",
};

TEST_F(TestXPathFilter, test_function)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book category=\"a\"><title lang=\"x\">abc</title> <author>def</author><year month=\"1\">1</year><price>false</price></book><book category=\"b\"><title lang=\"y\">def</title><author>xyz</author><year month=\"2\">2005</year><price>30.00</price></book><book category=\"c\"><title lang=\"en\">xyz</title><author>abc</author><year>2005</year><price>29.99</price></book><book category=\"WEB\"><title lang=\"en\">Learning XML</title><year>2003</year><sell></sell></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < PREDICATE_TEST_COUNT; i++) {
    if (func_enter[i].empty()) break;
    ObString enter_xpath = func_enter[i];
    std::cout << enter_xpath.ptr() << std::endl;
    ObString default_ns;
    ObPathVarObject pass(allocator);
    ObDatum data;
    data.set_string("ns2");
    ret = pass.add("h", &data);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObPathExprIter pathiter(&allocator);
    pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
    ret = pathiter.open();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObJsonBuffer buf(&allocator);
    ret = pathiter.path_node_->node_to_string(buf);
    std::cout << buf.ptr() << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString parse_result(buf.ptr());
    ASSERT_EQ(func_enter_parse_result[i], parse_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObXmlTreeTextWriter writer(&allocator);
    ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
    writer.visit(xnode);
    ObString s = writer.get_xml_text();
    std::cout << "Test NO." << i << " : " << s.ptr() << std::endl;
    ASSERT_EQ(s, func_predicate_result[i]);
  }
}

ObString complex_result[2] = {"<a>a<b><c>c<e>e</e><g>g</g></c><f>f</f></b></a>", "<a>xyz</a>"};
TEST_F(TestXPathFilter, test_complex_in_predicate)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObString xml_text("<tmp><a>a<b><c>c<e>e</e><g>g</g></c><f>f</f></b></a><a>xyz</a></tmp>");
  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  //ObString enter_xpath = "/tmp/a[/tmp/a = \"acegf\"]";
  ObString enter_xpath = "/tmp/a[/tmp/a = \"xyz\"]";
  std::cout << enter_xpath.ptr() << std::endl;
  ObString default_ns;
    ObPathVarObject pass(allocator);
  ObDatum data;
  data.set_string("ns2");
  ret = pass.add("h", &data);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPathExprIter pathiter(&allocator);
  pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
  ret = pathiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObJsonBuffer buf(&allocator);
  ret = pathiter.path_node_->node_to_string(buf);
  std::cout << buf.ptr() << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString parse_result(buf.ptr());
  ASSERT_EQ(enter_xpath, parse_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  int i = 0;
  while (OB_SUCC(ret)) {
    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ObXmlTreeTextWriter writer(&allocator);
      ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
      writer.visit(xnode);
      ObString s = writer.get_xml_text();
      ASSERT_EQ(s, complex_result[i]);
      std::cout<< i << ": " << s.ptr() << std::endl;
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  }
}

/*
<bookstore>
  <book category="">
    <title lang="x">abc</title>
    <author>def</author>
    <year month="1">1</year>
    <price>false</price>
  </book>
  <book category="b">
    <title lang="y">def</title>
    <author>xyz</author>
    <year month="2">2005</year>
    <price>30.00</price>
  </book>
  <book category="c">
    <title lang="en">xyz</title>
    <author>abc</author>
    <year>2005</year>
    <price>29.99</price>
  </book>
  <book category="WEB">
    <title lang="en">Learning XML</title>
    <year>2003</year>
    <sell></sell>
  </book>
</bookstore>
*/

ObString enter_predicate[PREDICATE_TEST_COUNT] = {
  // node-set
  // "/bookstore/book/sell[0 | /bookstore/book]",
  // "/bookstore/book/sell[/bookstore/book | 0]",
  // "/bookstore/book/sell[/a | /bookstore]",
  "/bookstore/book/sell[/bookstore/book/@category = \"\"]",
  "/bookstore/book/sell[@abced = \"\"]",

  "/bookstore/book/sell[/bookstore/book/sell = \"\"]",
  "/bookstore/book/sell[(3 > 5) = /bookstore/book/abc]",
  "/bookstore/book/sell[/bookstore/book/abc = (3 > 5)]",
  "/bookstore/book/sell[/bookstore/book/year < 2]",
  "/bookstore/book/sell[/bookstore/book/year = 2003]",
  "/bookstore/book/sell[/bookstore/book/author > \"x\"]",
  "/bookstore/book/sell[/bookstore/book/author < \"x\"]",
  "/bookstore/book/sell[/bookstore/book/author = (3 > 1)]",
  "/bookstore/book/sell[/bookstore/book/title = /bookstore/book/author]",

  // union
  // "/bookstore/book/sell[1 | 0]",
  // "/bookstore/book/sell[\"abc\" | 0]",
  // "/bookstore/book/sell[1 > 0 | 0]",

  // logic compare
  "/bookstore/book/sell[(0 < 2) and \"abc\"]",
  "/bookstore/book/sell[(0 < 2) and \"x\"]",
  "/bookstore/book/sell[\"\" or 1 > -1]",

  "/bookstore/book/sell[1 or 1 > 0]",
  "/bookstore/book/sell[0 or 1 > 0]",
  "/bookstore/book/sell[2 and 1 > 0]",
  "/bookstore/book/sell[(0 < 2) and -1]",
  "/bookstore/book/sell[(0 < 2) and 3]",
  "/bookstore/book/sell[(1 > 2) or (0.12 < 9)]",
  "/bookstore/book/sell[(1 < 2) and (3 < 9)]",

  // bool e double
  "/bookstore/book/sell[(1 < 2) = -123]",
  "/bookstore/book/sell[(1 < 2) = 123]",

  // bool e string
  "/bookstore/book/sell[(1 < 2) = \"true\"]",
  "/bookstore/book/sell[(1 < 2) = \"1\"]",
  "/bookstore/book/sell[(1 > 2) != \"true\"]",

  // double e bool
  "/bookstore/book/sell[0.0 != (1 < 2)]",
  "/bookstore/book/sell[-1 = (1 < 2)]",
  "/bookstore/book/sell[31 = (1 < 2)]",

  // double e string
  "/bookstore/book/sell[31 = \"31.0\"]",

  // string e bool
  "/bookstore/book/sell[\"\" != (3 > 1)]",
  "/bookstore/book/sell[\"\" = (0 > 1)]",
  "/bookstore/book/sell[\"abc\" = (3 > 2)]",

  // string e string
  "/bookstore/book/sell[\"\" = \"\"]",
  "/bookstore/book/sell[\"abc\" = \"abc\"]",
  "/bookstore/book/sell[\"abc\" != \"bc\"]",

  // string e double
  "/bookstore/book/sell[\"2\" = 2]",

  // compare

  // bool c double
  "/bookstore/book/sell[(1 < 2) >= 1.0]",
  "/bookstore/book/sell[(1 > 2) >= -1]",
  "/bookstore/book/sell[(1 < 2) < 3.3]",

  // string c bool
  "/bookstore/book/sell[\"3\" >= (2 > 1)]",

  // string c double
  "/bookstore/book/sell[\"22.1\" >= 1]",
  "/bookstore/book/sell[\"2\" >= 1]",

  // string c string
  "/bookstore/book/sell[\"abc\" <= \"efg\"]",
  "/bookstore/book/sell[\"abc\" < \"xyz\"]",

  // bool c bool
  "/bookstore/book/sell[(1 > 2) < (1 < 2)]",
  "/bookstore/book/sell[(1 > 2) >= (3 < 2)]",

  // double c double
  "/bookstore/book/sell[1 < 2]",
  "/bookstore/book/sell[1 <= 2]",
  "/bookstore/book/sell[189 <= 2222]",
};
ObString expect_predicate_parse_result[PREDICATE_TEST_COUNT] = {
  // "/bookstore/book/sell[0 | /bookstore/book]",
  // "/bookstore/book/sell[/bookstore/book | 0]",
  // "/bookstore/book/sell[/a | /bookstore]",
  "/bookstore/book/sell[/bookstore/book/@category = \"\"]",
  "/bookstore/book/sell[@abced = \"\"]",

  "/bookstore/book/sell[/bookstore/book/sell = \"\"]",
  "/bookstore/book/sell[3 > 5 = /bookstore/book/abc]",
  "/bookstore/book/sell[/bookstore/book/abc = 3 > 5]",
  "/bookstore/book/sell[/bookstore/book/year < 2]",
  "/bookstore/book/sell[/bookstore/book/year = 2003]",
  "/bookstore/book/sell[/bookstore/book/author > \"x\"]",
  "/bookstore/book/sell[/bookstore/book/author < \"x\"]",
  "/bookstore/book/sell[/bookstore/book/author = 3 > 1]",
  "/bookstore/book/sell[/bookstore/book/title = /bookstore/book/author]",

  // "/bookstore/book/sell[1 | 0]",
  // "/bookstore/book/sell[\"abc\" | 0]",
  // "/bookstore/book/sell[1 > 0 | 0]",

  "/bookstore/book/sell[0 < 2 and \"abc\"]",
  "/bookstore/book/sell[0 < 2 and \"x\"]",
  "/bookstore/book/sell[\"\" or 1 > -1]",

  "/bookstore/book/sell[1 or 1 > 0]",
  "/bookstore/book/sell[0 or 1 > 0]",
  "/bookstore/book/sell[2 and 1 > 0]",
  "/bookstore/book/sell[0 < 2 and -1]",
  "/bookstore/book/sell[0 < 2 and 3]",
  "/bookstore/book/sell[1 > 2 or 0.12 < 9]",
  "/bookstore/book/sell[1 < 2 and 3 < 9]",

  "/bookstore/book/sell[1 < 2 = -123]",
  "/bookstore/book/sell[1 < 2 = 123]",

  "/bookstore/book/sell[1 < 2 = \"true\"]",
  "/bookstore/book/sell[1 < 2 = \"1\"]",
  "/bookstore/book/sell[1 > 2 != \"true\"]",

  "/bookstore/book/sell[0 != 1 < 2]",
  "/bookstore/book/sell[-1 = 1 < 2]",
  "/bookstore/book/sell[31 = 1 < 2]",

  "/bookstore/book/sell[31 = \"31.0\"]",

  "/bookstore/book/sell[\"\" != 3 > 1]",
  "/bookstore/book/sell[\"\" = 0 > 1]",
  "/bookstore/book/sell[\"abc\" = 3 > 2]",

  "/bookstore/book/sell[\"\" = \"\"]",
  "/bookstore/book/sell[\"abc\" = \"abc\"]",
  "/bookstore/book/sell[\"abc\" != \"bc\"]",

  "/bookstore/book/sell[\"2\" = 2]",

  "/bookstore/book/sell[1 < 2 >= 1]",
  "/bookstore/book/sell[1 > 2 >= -1]",
  "/bookstore/book/sell[1 < 2 < 3.3]",

  "/bookstore/book/sell[\"3\" >= 2 > 1]",

  "/bookstore/book/sell[\"22.1\" >= 1]",
  "/bookstore/book/sell[\"2\" >= 1]",

  "/bookstore/book/sell[\"abc\" <= \"efg\"]",
  "/bookstore/book/sell[\"abc\" < \"xyz\"]",

  "/bookstore/book/sell[1 > 2 < 1 < 2]",
  "/bookstore/book/sell[1 > 2 >= 3 < 2]",

  "/bookstore/book/sell[1 < 2]",
  "/bookstore/book/sell[1 <= 2]",
  "/bookstore/book/sell[189 <= 2222]",
};

TEST_F(TestXPathFilter, test_in_predicate)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book category=\"\"><title lang=\"x\">abc</title> <author>def</author><year month=\"1\">1</year><price>false</price></book><book category=\"b\"><title lang=\"y\">def</title><author>xyz</author><year month=\"2\">2005</year><price>30.00</price></book><book category=\"c\"><title lang=\"en\">xyz</title><author>abc</author><year>2005</year><price>29.99</price></book><book category=\"WEB\"><title lang=\"en\">Learning XML</title><year>2003</year><sell></sell></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < PREDICATE_TEST_COUNT; i++) {
    if (enter_predicate[i].empty()) break;
    ObString enter_xpath = enter_predicate[i];
    std::cout << enter_xpath.ptr() << std::endl;
    ObString default_ns;
    ObPathVarObject pass(allocator);
    ObDatum data;
    data.set_string("ns2");
    ret = pass.add("h", &data);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObPathExprIter pathiter(&allocator);
    pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
    ret = pathiter.open();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObJsonBuffer buf(&allocator);
    ret = pathiter.path_node_->node_to_string(buf);
    std::cout << buf.ptr() << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString parse_result(buf.ptr());
    ASSERT_EQ(expect_predicate_parse_result[i], parse_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObXmlTreeTextWriter writer(&allocator);
    ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
    writer.visit(xnode);
    ObString s = writer.get_xml_text();
    ObString predicate_ans("<sell></sell>");
    std::cout << "Test NO." << i << " : " << s.ptr() << std::endl;
    ASSERT_EQ(s, predicate_ans);
  }
}
ObString enter_false_predicate[PREDICATE_FALSE_TEST_COUNT] = {
  // node-set
  // "/bookstore/book/sell[/a | 0]",
  // "/bookstore/book/sell[/a | 1]",

  "/bookstore/book/sell[/bookstore/book/abc != (3 > 3)]",
  "/bookstore/book/sell[/bookstore/book/title < 2]",
  "/bookstore/book/sell[/bookstore/book/year = \"abc\"]",
  "/bookstore/book/sell[/bookstore/book/year > 10086]",
  "/bookstore/book/sell[/bookstore/book/year = (3 > 3)]",
  "/bookstore/book/sell[/bookstore/book = \"xyz\"]",
  "/bookstore/book/sell[/bookstore/book/abc = \"\"]",
  "/bookstore/book/sell[/bookstore/book/abc > \"\"]",
  "/bookstore/book/sell[/abc = /xyz]",

  // union
  // "/bookstore/book/sell[0 | 1]",
  // "/bookstore/book/sell[\"\" | 0]",
  // "/bookstore/book/sell[\"\" | 1]",
  // "/bookstore/book/sell[\"\" | 3 > 2]",

  // logic compare
  "/bookstore/book/sell[(20 < 2) and (3 < 9)]",
  "/bookstore/book/sell[(20 < 2) or (19 < 9)]",
  "/bookstore/book/sell[1 and 1 > 2]",
  "/bookstore/book/sell[0 < 2 and 0]",
  "/bookstore/book/sell[4 < 2 or 0]",
  "/bookstore/book/sell[0 or -2 > -1]",
  "/bookstore/book/sell[\"\" and 1 > -1]",
  "/bookstore/book/sell[\"\" or 1 > 2]",

  // compare
  "/bookstore/book/sell[(1 < 2) = 0]",
  "/bookstore/book/sell[(1 < 2) != 123]",
  "/bookstore/book/sell[0 = (1 < 2)]",
  "/bookstore/book/sell[100 = \"101\"]",
  "/bookstore/book/sell[31 != \"31.0\"]",
  "/bookstore/book/sell[\"\" = (3 > 1)]",
  "/bookstore/book/sell[\"\" >= \"\"]",
  "/bookstore/book/sell[\"\" != \"\"]",
  "/bookstore/book/sell[\"abc\" < \"\"]",
  "/bookstore/book/sell[\"\" <= \"efg\"]",
  "/bookstore/book/sell[\"abc\" <= 1]",
  "/bookstore/book/sell[\"abc\" = \"efg\"]",
  "/bookstore/book/sell[\"abc\" = \"a bc\"]",
  "/bookstore/book/sell[\"2\" <= 1]",

  "/bookstore/book/sell[(1 < 2) >= 1.1]",
  "/bookstore/book/sell[(1 > 2) <= -1]",
  "/bookstore/book/sell[\"abc\" <= (2 > 1)]",
  "/bookstore/book/sell[\"abc\" >= (2 > 1)]",
  "/bookstore/book/sell[1 and \"\"]",
  "/bookstore/book/sell[(1 > 2) > (1 < 2)]",
  "/bookstore/book/sell[(1 > 2) != (3 < 2)]",
  "/bookstore/book/sell[1.1 > 2]",
  "/bookstore/book/sell[189.23 > 222.2]",
  "/bookstore/book/sell[1 != 1.0]",
};
ObString expect_false_predicate_parse_result[PREDICATE_FALSE_TEST_COUNT] = {
  // "/bookstore/book/sell[/a | 0]",
  // "/bookstore/book/sell[/a | 1]",

  "/bookstore/book/sell[/bookstore/book/abc != 3 > 3]",
  "/bookstore/book/sell[/bookstore/book/title < 2]",
  "/bookstore/book/sell[/bookstore/book/year = \"abc\"]",
  "/bookstore/book/sell[/bookstore/book/year > 10086]",
  "/bookstore/book/sell[/bookstore/book/year = 3 > 3]",
  "/bookstore/book/sell[/bookstore/book = \"xyz\"]",
  "/bookstore/book/sell[/bookstore/book/abc = \"\"]",
  "/bookstore/book/sell[/bookstore/book/abc > \"\"]",
  "/bookstore/book/sell[/abc = /xyz]",

  // "/bookstore/book/sell[0 | 1]",
  // "/bookstore/book/sell[\"\" | 0]",
  // "/bookstore/book/sell[\"\" | 1]",
  // "/bookstore/book/sell[\"\" | 3 > 2]",

  "/bookstore/book/sell[20 < 2 and 3 < 9]",
  "/bookstore/book/sell[20 < 2 or 19 < 9]",
  "/bookstore/book/sell[1 and 1 > 2]",
  "/bookstore/book/sell[0 < 2 and 0]",
  "/bookstore/book/sell[4 < 2 or 0]",
  "/bookstore/book/sell[0 or -2 > -1]",
  "/bookstore/book/sell[\"\" and 1 > -1]",
  "/bookstore/book/sell[\"\" or 1 > 2]",

  "/bookstore/book/sell[1 < 2 = 0]",
  "/bookstore/book/sell[1 < 2 != 123]",
  "/bookstore/book/sell[0 = 1 < 2]",
  "/bookstore/book/sell[100 = \"101\"]",
  "/bookstore/book/sell[31 != \"31.0\"]",
  "/bookstore/book/sell[\"\" = 3 > 1]",
  "/bookstore/book/sell[\"\" >= \"\"]",
  "/bookstore/book/sell[\"\" != \"\"]",
  "/bookstore/book/sell[\"abc\" < \"\"]",
  "/bookstore/book/sell[\"\" <= \"efg\"]",
  "/bookstore/book/sell[\"abc\" <= 1]",
  "/bookstore/book/sell[\"abc\" = \"efg\"]",
  "/bookstore/book/sell[\"abc\" = \"a bc\"]",
  "/bookstore/book/sell[\"2\" <= 1]",

  "/bookstore/book/sell[1 < 2 >= 1.1]",
  "/bookstore/book/sell[1 > 2 <= -1]",
  "/bookstore/book/sell[\"abc\" <= 2 > 1]",
  "/bookstore/book/sell[\"abc\" >= 2 > 1]",
  "/bookstore/book/sell[1 and \"\"]",
  "/bookstore/book/sell[1 > 2 > 1 < 2]",
  "/bookstore/book/sell[1 > 2 != 3 < 2]",
  "/bookstore/book/sell[1.1 > 2]",
  "/bookstore/book/sell[189.23 > 222.2]",
  "/bookstore/book/sell[1 != 1]",
};

TEST_F(TestXPathFilter, test_false_in_predicate)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<bookstore><book category=\"a\"><title lang=\"x\">abc</title> <author>def</author><year month=\"1\">1</year><price>false</price></book><book category=\"b\"><title lang=\"y\">def</title><author>xyz</author><year month=\"2\">2005</year><price>30.00</price></book><book category=\"c\"><title lang=\"en\">xyz</title><author>abc</author><year>2005</year><price>29.99</price></book><book category=\"WEB\"><title lang=\"en\">Learning XML</title><year>2003</year><sell></sell></book></bookstore>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < PREDICATE_FALSE_TEST_COUNT; i++) {
    if (enter_false_predicate[i].empty()) break;
    INIT_SUCC(ret);
    ObString enter_xpath = enter_false_predicate[i];
    std::cout << "TEST: " << i << std::endl;
    std::cout << enter_xpath.ptr() << std::endl;
    ObString default_ns;
    ObPathVarObject pass(allocator);
    ObDatum data;
    data.set_string("ns2");
    ret = pass.add("h", &data);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObPathExprIter pathiter(&allocator);
    pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
    ret = pathiter.open();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObJsonBuffer buf(&allocator);
    ret = pathiter.path_node_->node_to_string(buf);
    std::cout << buf.ptr() << std::endl;
    std::cout << expect_false_predicate_parse_result[i].ptr() << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString parse_result(buf.ptr());
    ASSERT_EQ(expect_false_predicate_parse_result[i], parse_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

# define COMPARE_TEST_COUNT 7
ObString enter_compare[COMPARE_TEST_COUNT] = {
  "1 = 2", "0=0", "1 < 2", "1 <= 2", "2 != 3",
  // "1 + 2", "25 + 100 = 125", "2 +  10 < 1",
  "\"abc\" = \"abc\"", "\"abc\" != \"y\"",
  //"1.1 + 2.2 = 3.3"
};
ObString expect_parse_result[COMPARE_TEST_COUNT] = {
  "1 = 2", "0 = 0", "1 < 2", "1 <= 2", "2 != 3",
  //"1 + 2", "25 + 100 = 125", "2 + 10 < 1",
  "\"abc\" = \"abc\"", "\"abc\" != \"y\"",
  //"1.1 + 2.2 = 3.3"
};
ObString compare_ans[COMPARE_TEST_COUNT] = {
  "false", "true", "true", "true", "true",
  "true", "true",
  //"true"
};
TEST_F(TestXPathFilter, test_equal_compare)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString xml_text("<root>root</root>");
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 1; i < COMPARE_TEST_COUNT; i++) {
    ObString enter_xpath = enter_compare[i];
    std::cout << enter_xpath.ptr() << std::endl;
    ObString default_ns;
    ObPathVarObject pass(allocator);
    ObDatum data;
    data.set_string("ns2");
    ret = pass.add("h", &data);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObPathExprIter pathiter(&allocator);
    pathiter.init(ctx, enter_xpath, default_ns, doc, &pass);
    ret = pathiter.open();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObJsonBuffer buf(&allocator);
    ret = pathiter.path_node_->node_to_string(buf);
    std::cout << buf.ptr() << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString parse_result(buf.ptr());
    ASSERT_EQ(expect_parse_result[i], parse_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObIMulModeBase* res;
    ret = pathiter.get_next_node(res);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObXmlTreeTextWriter writer(&allocator);
    ObXmlNode* xnode = static_cast<ObXmlNode*>(res);
    writer.visit(xnode);
    ObString s = writer.get_xml_text();
    std::cout << "Test NO." << i << " : " << s.ptr() << std::endl;
    std::cout << "compare_ans[" << i  << "] : " << compare_ans[i].ptr() << std::endl;
    ASSERT_EQ(s, compare_ans[i]);
  }
}
int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  // system("rm -f test_xpath_filter.log");
  // OB_LOGGER.set_file_name("test_xpath_filter.log");
  // OB_LOGGER.set_log_level("INFO");

  return RUN_ALL_TESTS();
}