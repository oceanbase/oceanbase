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
 * This file contains interface support for the binary aggregate test.
 */

#include <gtest/gtest.h>

#define private public
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/xml/ob_multi_mode_bin.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_tree_base.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_binary_aggregate.h"
#undef private

#include <sys/time.h>
#include <chrono>
using namespace std;

namespace oceanbase {
namespace  common{

class TestBinaryAgg : public ::testing::Test {
public:
  TestBinaryAgg()
  {}
  ~TestBinaryAgg()
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
  DISALLOW_COPY_AND_ASSIGN(TestBinaryAgg);
};

static void get_xml_document_1(ObMulModeMemCtx* ctx, ObXmlDocument*& handle)
{
  int ret = 0;
  common::ObString xml_text(
  "<shiporder orderid=\"889923\">\n"
  "  <orderperson>John Smith</orderperson>\n"
  "  <shipto>\n"
  "    <name>Ola Nordmann</name>\n"
  "    <address>Langgt 23</address>\n"
  "    <city>4000 Stavanger</city>\n"
  "    <country>Norway</country>\n"
  "  </shipto>\n"
  "</shiporder>\n"
  );

  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  handle = doc;
}

static void get_xml_document_2(ObMulModeMemCtx* ctx, ObXmlDocument*& handle)
{
  int ret = 0;
  common::ObString xml_text(
  "<abcd orderid=\"889923\">\n"
  "  <orderperson>John Smith</orderperson>\n"
  "</abcd>\n"
  );

  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  handle = doc;
}

#define PREDICATE_TEST_COUNT 20
ObString xml_text_array[PREDICATE_TEST_COUNT] = {
  // 0
  "<shipord>\n"
  "  <orderperson>John Smith</orderperson>\n"
  "</shipord>\n",

  // 1
  "<a>\n"
  "  <person1>carrot1 Smith</person1>\n"
  "  <person2>carrot2 Smith</person2>\n"
  "</a>\n",

  // 2
  "<titleeee lang=\"en\">Everyday Italian</titleeee>",

  // 3
  "<book><price>29.99</price></book>",

  // 4
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?><cdefg/>",

  // 5
  "<year>2005</year><price>30.00</price>",

  // 6
  "<case/>abcd<for/>",

  // 7
  "<abdes>a<abdes",

  // 8
  "<Thisisatestcase,ifyoureadalltheinstructions,nowyouarefree",

  // 9
  "<abc",

  // 10
  "xyz",

  // 11
  "carrot"
};

ObMulModeNodeType xml_test_type[PREDICATE_TEST_COUNT] = {
  ObMulModeNodeType::M_DOCUMENT, // 0
  ObMulModeNodeType::M_DOCUMENT, // 1
  ObMulModeNodeType::M_DOCUMENT, // 2
  ObMulModeNodeType::M_DOCUMENT, // 3
  ObMulModeNodeType::M_DOCUMENT, // 4
  ObMulModeNodeType::M_CONTENT, // 5
  ObMulModeNodeType::M_CONTENT, // 6
  ObMulModeNodeType::M_UNPARSED, // 7
  ObMulModeNodeType::M_UNPARSED, // 8
  ObMulModeNodeType::M_UNPARSED, // 9
  ObMulModeNodeType::M_UNPARSED, // 10
  ObMulModeNodeType::M_UNPARSED, // 11
};

static void get_xml_document(ObMulModeMemCtx* ctx, ObXmlDocument*& handle, int i)
{
  int ret = 0;
  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text_array[i], doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  handle = doc;
}

TEST_F(TestBinaryAgg, serialize_xml_bin)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc1 = nullptr;
  ObXmlDocument* doc2 = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObArray<ObIMulModeBase*> tree_array;

  get_xml_document_1(ctx, doc1);
  ASSERT_EQ(OB_SUCCESS, ret);
  tree_array.push_back(doc1);

  get_xml_document_2(ctx, doc2);
  ASSERT_EQ(OB_SUCCESS, ret);
  tree_array.push_back(doc2);

  ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));

  {
    for (int i = 0; i < tree_array.count(); i++) {
      ObXmlBin bin(ctx);
      ObIMulModeBase* tree = tree_array.at(i);
      ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

      ASSERT_EQ(bin_agg.append_key_and_value(&bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();
    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    base->print_document(str_buf, CS_TYPE_UTF8MB4_GENERAL_CI, 0, 0);
    ASSERT_EQ(str_buf.string(), "<shiporder orderid=\"889923\"><orderperson>John Smith</orderperson><shipto><name>Ola Nordmann</name><address>Langgt 23</address><city>4000 Stavanger</city><country>Norway</country></shipto></shiporder><abcd orderid=\"889923\"><orderperson>John Smith</orderperson></abcd>");
    cout << str_buf.ptr() << endl;
  }

}

#define TEST_ROW_COUNT 10000

static void get_multi_node_document(ObMulModeMemCtx* ctx, ObXmlDocument*& handle)
{
  int ret = 0;
  // common::ObString xml_text(
  //   "<bookstore><book>book0</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book1</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book2</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book3</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book4</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book5</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book6</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book7</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book8</book><title>Everyday</title><author>carrot</author><year>2023</year><price>99.99</price> <book>book9</book><title>Everyday</title></bookstore>"
  // );
  common::ObString xml_text(
    "<tag>tagi_value</tag>"
  );
  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  handle = doc;
}

TEST_F(TestBinaryAgg, xmlagg_performance_test)
{
  int ret = 0;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_multi_node_document(ctx, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlBin bin(ctx);
  ASSERT_EQ(bin.parse_tree(doc), OB_SUCCESS);

  std::chrono::milliseconds ms_start = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch());

  int start_time = ms_start.count();
  cout << start_time << endl;

  ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));

  for (int i = 0; i < TEST_ROW_COUNT; i++) {
    ASSERT_EQ(bin_agg.append_key_and_value(&bin), OB_SUCCESS);
  }

  ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

  std::chrono::milliseconds ms_end = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch());

  int end_time = ms_end.count();
  cout << end_time << endl;
  cout << "total used: " << end_time - start_time << endl;

}

TEST_F(TestBinaryAgg, xmlagg_tree_test)
{
  int ret = 0;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_multi_node_document(ctx, doc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObXmlBin bin(ctx);
  ASSERT_EQ(bin.parse_tree(doc), OB_SUCCESS);
  ObString input_str;
  ASSERT_EQ(OB_SUCCESS, doc->get_raw_binary(input_str, ctx->allocator_));

  std::chrono::milliseconds ms_start = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch());

  int start_time = ms_start.count();
  cout << start_time << endl;

  ObXmlDocument *content = NULL;
  content = OB_NEWx(ObXmlDocument, ctx->allocator_, ObMulModeNodeType::M_CONTENT, ctx);
  ASSERT_EQ(OB_SUCCESS, content->alter_member_sort_policy(false));

  for (int i = 0; i < TEST_ROW_COUNT; i++) {
    ObIMulModeBase *base;
    ASSERT_EQ(OB_SUCCESS, ObMulModeFactory::get_xml_base(ctx, input_str, ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base));
    ObXmlDocument *input_doc = static_cast<ObXmlDocument*>(base);
    ASSERT_EQ(OB_SUCCESS, content->add_element(input_doc->at(0)));
  }

  ObString binary_str;
  ASSERT_EQ(OB_SUCCESS, content->get_raw_binary(binary_str, ctx->allocator_));


  std::chrono::milliseconds ms_end = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch());

  int end_time = ms_end.count();
  cout << end_time << endl;
  cout << "total used: " << end_time - start_time << endl;

}

TEST_F(TestBinaryAgg, mulit_bin)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObArray<ObIMulModeBase*> tree_array;
  for (int i = 0; i < 5; i++) {
    ObXmlDocument* doc = nullptr;
    get_xml_document(ctx, doc, i);
    ASSERT_EQ(OB_SUCCESS, ret);
    tree_array.push_back(doc);
  }

  {
    cout << "TEST CASE: 1" << endl;
    ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT), true);

    for (int i = 0; i < 5; i++) {
      ObXmlBin bin(ctx);
      ObIMulModeBase* tree = tree_array.at(i);
      cout << "Add XML: " << i << endl;
      ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

      ASSERT_EQ(bin_agg.append_key_and_value(&bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    ParamPrint param_list;
    base->print_content(str_buf, false, false, 0, param_list);
    ASSERT_EQ(str_buf.string(), "<shipord><orderperson>John Smith</orderperson></shipord><a><person1>carrot1 Smith</person1><person2>carrot2 Smith</person2></a><titleeee lang=\"en\">Everyday Italian</titleeee><book><price>29.99</price></book><cdefg></cdefg>");
    cout << str_buf.ptr() << endl;
  }
  {
    cout << "TEST CASE: 2" << endl;
    ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT), true);

    int doc_array[5] = {0, 2, 4, 5, 6};
    for (int i = 0; i < 5; i++) {
      cout << "Add XML: " << i << endl;
      ObIMulModeBase *input_base;
      ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, xml_text_array[doc_array[i]], ObNodeMemType::TREE_TYPE, ObNodeMemType::BINARY_TYPE, input_base, xml_test_type[doc_array[i]]), OB_SUCCESS);

      ObXmlBin *bin = static_cast<ObXmlBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    ParamPrint param_list;
    base->print_content(str_buf, false, false, 0, param_list);
    ASSERT_EQ(str_buf.string(), "<shipord><orderperson>John Smith</orderperson></shipord><titleeee lang=\"en\">Everyday Italian</titleeee><cdefg></cdefg><year>2005</year><price>30.00</price><case></case>abcd<for></for>");
    cout << str_buf.ptr() << endl;
  }

  {
    cout << "TEST CASE: 3" << endl;
    ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT), true);

    int doc_array[2] = {7, 8};
    for (int i = 0; i < 2; i++) {
      cout << "Add XML: " << i << endl;
      ObIMulModeBase *input_base;
      ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, xml_text_array[doc_array[i]], ObNodeMemType::TREE_TYPE, ObNodeMemType::BINARY_TYPE, input_base, xml_test_type[doc_array[i]]), OB_SUCCESS);

      ObXmlBin *bin = static_cast<ObXmlBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    ParamPrint param_list;
    base->print_content(str_buf, false, false, 0, param_list);
    ASSERT_EQ(str_buf.string(), "<abdes>a<abdes<Thisisatestcase,ifyoureadalltheinstructions,nowyouarefree");
    cout << str_buf.ptr() << endl;
  }

  {
    cout << "TEST CASE: 4" << endl;
    ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT), true);
    int doc_array[5] = {7, 6, 8, 9, 5};
    for (int i = 0; i < 5; i++) {
      cout << "Add XML: " << i << endl;
      ObIMulModeBase *input_base;
      ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, xml_text_array[doc_array[i]], ObNodeMemType::TREE_TYPE, ObNodeMemType::BINARY_TYPE, input_base, xml_test_type[doc_array[i]]), OB_SUCCESS);

      ObXmlBin *bin = static_cast<ObXmlBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    ParamPrint param_list;
    base->print_content(str_buf, false, false, 0, param_list);
    ASSERT_EQ(str_buf.string(), "<abdes>a<abdes<case></case>abcd<for></for><Thisisatestcase,ifyoureadalltheinstructions,nowyouarefree<abc<year>2005</year><price>30.00</price>");
    cout << str_buf.ptr() << endl;
  }

  {
    cout << "TEST CASE: 5" << endl;
    ObBinAggSerializer bin_agg(ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT), true);
    int doc_array[12] = {0, 7, 8, 1, 2, 3, 9, 4, 5, 6, 10, 11};
    for (int i = 0; i < 12; i++) {
      cout << "Add XML: " << i << endl;
      ObIMulModeBase *input_base;
      ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, xml_text_array[doc_array[i]], ObNodeMemType::TREE_TYPE, ObNodeMemType::BINARY_TYPE, input_base, xml_test_type[doc_array[i]]), OB_SUCCESS);

      ObXmlBin *bin = static_cast<ObXmlBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIMulModeBase *base;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx, buffer->string(), ObNodeMemType::BINARY_TYPE, ObNodeMemType::TREE_TYPE, base), OB_SUCCESS);

    ObStringBuffer str_buf(ctx->allocator_);
    ParamPrint param_list;
    base->print_content(str_buf, false, false, 0, param_list);
    ASSERT_EQ(str_buf.string(), "<shipord><orderperson>John Smith</orderperson></shipord><abdes>a<abdes<Thisisatestcase,ifyoureadalltheinstructions,nowyouarefree<a><person1>carrot1 Smith</person1><person2>carrot2 Smith</person2></a><titleeee lang=\"en\">Everyday Italian</titleeee><book><price>29.99</price></book><abc<cdefg></cdefg><year>2005</year><price>30.00</price><case></case>abcd<for></for>xyzcarrot");
    cout << str_buf.ptr() << endl;
  }
}

ObString json_text_array[PREDICATE_TEST_COUNT] = {
  "{\"a\":100, \"b\":200, \"c\":300}", // 0
  "{ \"item\" : \"canvas\"}", // 1
  "{\"c\":2, \"a\":0, \"b\":1}", // 2
  "\"not object\"", // 3
  "\"123\"", // 4
  "333", // 5
  "[\"book\", \"read\"]", // 6
};

ObString json_key_array[PREDICATE_TEST_COUNT] = {
  "abcd", // 0
  "efg", // 1
  "carrot", // 2
  "newkey", // 3
  "xyz", // 4
  "yxz", // 5
  "", // 6
};

TEST_F(TestBinaryAgg, json_agg)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  {
    cout << "TEST JSON CASE: 1" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_OBJECT));
    ObStringBuffer value(&allocator);
    int json_array[2] = {0, 1};
    for (int i = 0; i < 2; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(json_key_array[json_array[i]], value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    ASSERT_EQ(buf.string(), "{\"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}, \"efg\": {\"item\": \"canvas\"}}");
  }

  {
    cout << "TEST JSON CASE: 2" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_OBJECT));
    int json_array[3] = {1, 2, 0};
     ObStringBuffer value(&allocator);
    for (int i = 0; i < 3; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(json_key_array[json_array[i]], value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    ASSERT_EQ(buf.string(), "{\"efg\": {\"item\": \"canvas\"}, \"carrot\": {\"a\": 0, \"b\": 1, \"c\": 2}, \"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}}");
  }

  {
    cout << "TEST JSON CASE: 3" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_OBJECT));
    int json_array[5] = {0, 1, 2, 3, 4};
     ObStringBuffer value(&allocator);
    for (int i = 0; i < 5; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(json_key_array[json_array[i]], value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    ASSERT_EQ(buf.string(), "{\"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}, \"efg\": {\"item\": \"canvas\"}, \"carrot\": {\"a\": 0, \"b\": 1, \"c\": 2}, \"newkey\": \"not object\", \"xyz\": \"123\"}");
  }

  {
    cout << "TEST JSON CASE: 4" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_OBJECT));
    int json_array[3] = {3, 5, 4};
    ObStringBuffer value(&allocator);
    for (int i = 0; i < 3; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(json_key_array[json_array[i]], value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    ASSERT_EQ(buf.string(), "{\"newkey\": \"not object\", \"yxz\": 333, \"xyz\": \"123\"}");
  }

  {
    cout << "TEST JSON CASE: 5" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_OBJECT));
    int json_array[5] = {0, 1, 4, 5, 6};
     ObStringBuffer value(&allocator);
    for (int i = 0; i < 5; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(json_key_array[json_array[i]], value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    ASSERT_EQ(buf.string(), "{\"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}, \"efg\": {\"item\": \"canvas\"}, \"xyz\": \"123\", \"yxz\": 333, \"\": [\"book\", \"read\"]}");
  }

  // json_query with wrapper
  {
    cout << "TEST JSON CASE: 6" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_ARRAY));
    int json_array[8] = {0, 0, 0, 6};
    ObStringBuffer value(&allocator);
    ObString input_null;
    for (int i = 0; i < 4; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(input_null, value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    //ASSERT_EQ(buf.string(), "{\"\": [\"book\", \"read\"], \"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}, \"efg\": {\"item\": \"canvas\"}, \"xyz\": \"123\", \"yxz\": 333}");
  }

  {
    cout << "TEST JSON CASE: 7" << endl;
    ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_ARRAY));
    int json_array[8] = {0, 0, 0, 0, 6};
    ObStringBuffer value(&allocator);
    ObString input_null;
    for (int i = 0; i < 5; i++) {
      cout << "Add JSON: " << i << endl;
      ObIJsonBase *input_base = NULL;
      ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, json_text_array[json_array[i]], ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, input_base), OB_SUCCESS);
      ObJsonBin *bin = static_cast<ObJsonBin*>(input_base);
      ASSERT_EQ(bin_agg.append_key_and_value(input_null, value, bin), OB_SUCCESS);
    }

    ASSERT_EQ(bin_agg.serialize(), OB_SUCCESS);

    ObIJsonBase *base = NULL;
    ObStringBuffer *buffer = bin_agg.get_buffer();

    ASSERT_EQ(ObJsonBaseFactory::get_json_base(&allocator, buffer->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, base), OB_SUCCESS);

    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(base->print(buf, true), OB_SUCCESS);
    cout << buf.ptr() << endl;
    //ASSERT_EQ(buf.string(), "{\"\": [\"book\", \"read\"], \"abcd\": {\"a\": 100, \"b\": 200, \"c\": 300}, \"efg\": {\"item\": \"canvas\"}, \"xyz\": \"123\", \"yxz\": 333}");
  }

}

};
};

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
