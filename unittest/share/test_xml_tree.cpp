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
 * This file contains interface support for the json tree abstraction.
 */

#include <gtest/gtest.h>

#define private public
#include "test_xml_utils.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/timezone/ob_timezone_info.h"

#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#undef private

#include <sys/time.h>
using namespace std;

namespace oceanbase {
namespace  common{

using namespace ObXmlTestUtils;
class TestXmlNodeBase : public ::testing::Test {
public:
  TestXmlNodeBase()
  {}
  ~TestXmlNodeBase()
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
  DISALLOW_COPY_AND_ASSIGN(TestXmlNodeBase);
};

TEST_F(TestXmlNodeBase, test_xml_node_init)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  // element
  ObXmlElement element1(ObMulModeNodeType::M_ELEMENT, ctx);
  // document
  ObXmlDocument document(ObMulModeNodeType::M_DOCUMENT, ctx);
  // content
  ObXmlDocument content(ObMulModeNodeType::M_CONTENT, ctx);
  // attribute
  ObXmlAttribute attr(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  // namespace
  ObXmlAttribute ns(ObMulModeNodeType::M_NAMESPACE, ctx);
  // PI
  ObXmlAttribute pi(ObMulModeNodeType::M_INSTRUCT, ctx);
  // CData
  ObXmlText cdata(ObMulModeNodeType::M_CDATA, ctx);
  // comment
  ObXmlText comment(ObMulModeNodeType::M_COMMENT, ctx);
  // text
  ObXmlText text(ObMulModeNodeType::M_TEXT, ctx);
  element1.init();
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, element1.type());
  ObString emelent1= "emelent1";
  element1.set_key(emelent1);
  ASSERT_EQ(element1.get_key(), "emelent1");
  ASSERT_EQ(element1.get_key(), "emelent1");
  ObString cdata1= "cdata1";
  cdata.set_text(cdata1);
  ASSERT_EQ(cdata.get_text(), "cdata1");
  ObString key1= "key1";
  ObString value1= "value1";
  pi.set_key(key1);
  pi.set_value(value1);
  ObString n1= "n1";
  ObString ns1= "namespace1";
  ns.set_key(n1);
  ns.set_value(ns1);
  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr.set_key(atr1);
  attr.set_value(atr_value1);
  attr.set_ns(&ns);
  ObString val_ns;
  int ret = attr.get_ns()->get_value(val_ns);
  ASSERT_EQ(val_ns, "namespace1");

}

TEST_F(TestXmlNodeBase, test_xml_node_element_add_child)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  // element1
  ObXmlElement element1(ObMulModeNodeType::M_ELEMENT, ctx);
  // element2
  ObXmlElement element2(ObMulModeNodeType::M_ELEMENT, ctx);
  // document
  ObXmlDocument document(ObMulModeNodeType::M_DOCUMENT, ctx);
  // content
  ObXmlDocument content(ObMulModeNodeType::M_CONTENT, ctx);
  // attribute
  ObXmlAttribute attr1(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  ObXmlAttribute attr2(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  // namespace
  ObXmlAttribute ns(ObMulModeNodeType::M_NAMESPACE, ctx);
  // PI
  ObXmlAttribute pi(ObMulModeNodeType::M_INSTRUCT, ctx);
  // CData
  ObXmlText cdata(ObMulModeNodeType::M_CDATA, ctx);
  // comment
  ObXmlText comment(ObMulModeNodeType::M_COMMENT, ctx);
  // text
  ObXmlText text(ObMulModeNodeType::M_TEXT, ctx);
  element1.init();
  element2.init();
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, element1.type());
  ObString emelent1= "emelent1";
  ObString emelent2= "emelent2";
  element1.set_key(emelent1);
  element2.set_key(emelent2);
  ASSERT_EQ(element1.get_key(), "emelent1");
  ObString cdata1= "cdata1";
  cdata.set_text(cdata1);
  ASSERT_EQ(cdata.get_text(), "cdata1");
  ObString key1= "key1";
  ObString value1= "value1";
  pi.set_key(key1);
  pi.set_value(value1);
  ObString n1= "n1";
  ObString ns1= "namespace1";
  ns.set_key(n1);
  ns.set_value(ns1);

  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr1.set_key(atr1);
  attr1.set_prefix(n1);
  attr1.set_value(atr_value1);
  attr1.set_ns(&ns);
  ObString atr2= "sttr2_name";
  ObString atr_value2= "sttr2_value";
  attr2.set_key(atr2);
  attr2.set_value(atr_value2);
  attr2.set_ns(&ns);
  ObString val_ns;
  int ret = attr1.get_ns()->get_value(val_ns);
  ASSERT_EQ(val_ns, "namespace1");

  element1.add_element(&element2);
  element1.add_element(&element2);
  element1.add_attribute(&attr1);
  element2.add_attribute(&attr1);
  element1.set_ns(&ns);
  element1.set_prefix(ns.get_key());
  ASSERT_EQ(element1.size(), 2);
  ASSERT_EQ(element1.attribute_size(), 1);
  element1.update_attribute(&attr2, 0);
  // ASSERT_EQ(element1.get_attribute().at(0).get_value(), "sttr2_value");
  ObString key_res1;
  int ret2 = element1.get_ns()->get_key(key_res1);
  ASSERT_EQ(key_res1, element1.get_prefix());

  ObString version1= "version1";
  ObString encod= "utf";
  document.set_version(version1);
  document.set_encoding(encod);
  document.set_standalone(1);
  document.set_has_xml_decl(0);
  ASSERT_EQ(document.get_version(), "version1");

  comment.set_text(cdata1);
  int res = cdata.compare(cdata1, res);
  ASSERT_EQ(res, 0);
}

TEST_F(TestXmlNodeBase, test_xml_node_element_ns_valid)
{
  // insert element/attribute with invalid ns
  ObArenaAllocator allocator(ObModIds::TEST);

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  // element1
  ObXmlElement element1(ObMulModeNodeType::M_ELEMENT, ctx);
  // element2
  ObXmlElement element2(ObMulModeNodeType::M_ELEMENT, ctx);
  // attribute
  ObXmlAttribute attr1(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  ObXmlAttribute attr2(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  // namespace
  ObXmlAttribute ns(ObMulModeNodeType::M_NAMESPACE, ctx);
  ObXmlAttribute ns2(ObMulModeNodeType::M_NAMESPACE,ctx);
  ObString emelent1= "emelent1";
  ObString emelent2= "emelent2";
  bool need_ns_check = true;

  element1.init();
  element2.init();
  element1.set_key(emelent1);
  element2.set_key(emelent2);

  ObString n1= "n1";
  ObString ns1= "namespace1";
  ns.set_key(n1);
  ns.set_value(ns1);
  ObString n2= "n2";
  ObString nstr2= "namespace2";
  ns2.set_key(n2);
  ns2.set_value(nstr2);
  ObString prefix = "n2";
  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr1.set_key(atr1);
  attr1.set_prefix(prefix);
  attr1.set_value(atr_value1);
  attr1.set_ns(&ns);
  ObString atr2= "sttr2_name";
  ObString atr_value2= "sttr2_value";
  attr2.set_key(atr2);
  attr2.set_value(atr_value2);
  attr2.set_ns(&ns);
  element1.add_attribute(&ns);

  element2.set_prefix(prefix);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(&element1);
  ObString s = writer.get_xml_text();
  int ret = element1.add_element(&element2, need_ns_check);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  writer.reuse();
  writer.visit(&element1);
  ObString s1 = writer.get_xml_text();
  ret = element1.add_attribute(&attr1, need_ns_check);
  writer.reuse();
  writer.visit(&element1);
  ObString s2 = writer.get_xml_text();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ret = element1.add_attribute(&ns2, need_ns_check);
  writer.reuse();
  writer.visit(&element1);
  ObString s3 = writer.get_xml_text();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = element1.add_element(&element2, need_ns_check);
  writer.reuse();
  writer.visit(&element1);
  ObString s4 = writer.get_xml_text();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = element1.remove_namespace(1, need_ns_check);
  writer.reuse();
  writer.visit(&element1);
  ObString s5 = writer.get_xml_text();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ret = element1.update_attribute(&ns2, 0);
  writer.reuse();
  writer.visit(&element1);
  ObString s6 = writer.get_xml_text();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = element1.remove_namespace(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, element1.size());

}

TEST_F(TestXmlNodeBase, test_path_interface)
{
  int ret = 0;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString element3_str = "element3";

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  // element1
  ObXmlElement element1(ObMulModeNodeType::M_ELEMENT, ctx);
  // element3
  ObXmlElement element3(ObMulModeNodeType::M_ELEMENT, ctx, element3_str);
  // document
  ObXmlDocument document(ObMulModeNodeType::M_DOCUMENT, ctx);
  // content
  ObXmlDocument content(ObMulModeNodeType::M_CONTENT, ctx);
  // attribute
  ObXmlAttribute attr1(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  ObXmlAttribute attr2(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  // namespace
  ObXmlAttribute ns(ObMulModeNodeType::M_NAMESPACE, ctx);
  // PI
  ObXmlAttribute pi(ObMulModeNodeType::M_INSTRUCT, ctx);
  // CData
  ObXmlText cdata(ObMulModeNodeType::M_CDATA, ctx);
  // comment
  ObXmlText comment(ObMulModeNodeType::M_COMMENT, ctx);
  // text
  ObXmlText text(ObMulModeNodeType::M_TEXT, ctx);
  element1.init();
  ASSERT_EQ(true, ObXmlUtil::is_node(element1.type()));
  ASSERT_EQ(false, ObXmlUtil::is_text(element1.type()));
  ASSERT_EQ(true, ObXmlUtil::is_element(element1.type()));
  ASSERT_EQ(element3_str, element3.get_key());
  ObString emelent1= "emelent1";
  element1.set_key(emelent1);
  ASSERT_EQ(true, element1.is_element(emelent1));
  ObString cdata1= "cdata1";
  cdata.set_text(cdata1);
  ASSERT_EQ(true, ObXmlUtil::is_text(cdata.type()));
  ObString key1= "key1";
  ObString value1= "value1";
  pi.set_key(key1);
  pi.set_value(value1);
  ASSERT_EQ(true, ObXmlUtil::is_pi(pi.type()));
  ASSERT_EQ(true, pi.is_pi(key1));
  ObString n1= "n1";
  ObString ns1= "namespace1";
  ns.set_key(n1);
  ns.set_value(ns1);
  ASSERT_EQ(false, ObXmlUtil::is_node(ns.type()));
  ASSERT_EQ(true, ObXmlUtil::is_comment(comment.type()));

  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr1.set_key(atr1);
  attr1.set_prefix(n1);
  attr1.set_value(atr_value1);
  attr1.set_ns(&ns);
  ObString val_ns;
  ret = attr1.get_ns()->get_value(val_ns);
  ASSERT_EQ(val_ns, "namespace1");

  element1.add_attribute(&attr1);
  element1.add_attribute(&ns);
  element1.set_ns(&ns);
  element1.set_prefix(ns.get_key());
  ObArray<ObIMulModeBase*> attr_list;
  element1.get_attribute_list(attr_list);
  ASSERT_EQ(1, attr_list.size());
  ASSERT_EQ(&attr1, attr_list.at(0));
  attr_list.remove(0);
  element1.get_namespace_list(attr_list);
  ASSERT_EQ(1, attr_list.size());
  ASSERT_EQ(&ns, attr_list.at(0));



  ASSERT_EQ(true, element1.has_attribute(ns1, atr1));
  ASSERT_EQ(false, element1.has_attribute(ns1, n1));
  ASSERT_EQ(&attr1, element1.get_attribute_by_name(ns1, atr1));
  ASSERT_EQ(true, element1.is_invalid_namespace(&ns));

  ObString n3= "n3";
  ObString ns3= "namespace3";
  ret = element1.add_attr_by_str(n3, ns3);
  attr_list.remove(0);
  element1.get_namespace_list(attr_list);
  ASSERT_EQ(2, attr_list.size());

}

TEST_F(TestXmlNodeBase, test_simple_print_document)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
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
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<shiporder orderid=\"889923\">"
  "<orderperson>John Smith</orderperson>"
  "<shipto>"
  "<name>Ola Nordmann</name>"
  "<address>Langgt 23</address>"
  "<city>4000 Stavanger</city>"
  "<country>Norway</country>"
  "</shipto>"
  "</shiporder>"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->print_document(buf_str, type, ObXmlFormatType::NO_FORMAT);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));
}

TEST_F(TestXmlNodeBase, test_simple_print_document_with_pretty)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<?pi name target  ?>"
  "<shiporder orderid=\"889923\">\n"
  "  <orderperson>John Smith</orderperson>\n"
  "  <shipto>\n"
  "    <name>Ola Nordmann</name>\n"
  "    <address>Langgt 23</address>\n"
  "    <city>4000 Stavanger</city>\n"
  "    <country>Norway</country>\n"
  " <![CDATA[xyz123abc]]> \n"
  "  </shipto>\n"
  "</shiporder>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<?pi name target  ?>\n"
  "<shiporder orderid=\"889923\">\n"
  "  <orderperson>John Smith</orderperson>\n"
  "  <shipto>\n"
  "    <name>Ola Nordmann</name>\n"
  "    <address>Langgt 23</address>\n"
  "    <city>4000 Stavanger</city>\n"
  "    <country>Norway</country><![CDATA[xyz123abc]]></shipto>\n"
  "</shiporder>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->print_document(buf_str, type, ObXmlFormatType::WITH_FORMAT, 2);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));
}

TEST_F(TestXmlNodeBase, test_print_content_with_pretty)
{
  int ret = 0;
  common::ObString xml_text(
  "123456"
  "<contact>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  "</address>"
  "</contact>"
  );
  common::ObString serialize_text(
  "123456\n"
  "<contact>\n"
  "    <address category=\"residence\">\n"
  "        <name>Tanmay Patil</name>\n"
  "    </address>\n"
  "</contact>\n"
  );
  common::ObString serialize_text_with_encoding(
  "<?xml encoding=\"utf-8\"?>\n"
  "123456\n"
  "<contact>\n"
  "    <address category=\"residence\">\n"
  "        <name>Tanmay Patil</name>\n"
  "    </address>\n"
  "</contact>\n"
  );
  common::ObString serialize_text_with_version(
  "<?xml version=\"4.0.0\"?>\n"
  "123456\n"
  "<contact>\n"
  "    <address category=\"residence\">\n"
  "        <name>Tanmay Patil</name>\n"
  "    </address>\n"
  "</contact>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  ParamPrint param_list;
  param_list.version = "4.0.0";
  param_list.encode = "utf-8";
  param_list.indent = 4;
  content->print_content(buf_str, false, false, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));
  buf_str.reuse();
  content->print_content(buf_str, true, false, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text_with_encoding.ptr(), serialize_text_with_encoding.length()), std::string(buf_str.ptr(), buf_str.length()));
  buf_str.reuse();
  content->print_content(buf_str, false, true, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text_with_version.ptr(), serialize_text_with_version.length()), std::string(buf_str.ptr(), buf_str.length()));
}

TEST_F(TestXmlNodeBase, test_print_content)
{
  int ret = 0;
  common::ObString xml_text(
  "123456"
  "<contact>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  common::ObString ser_text(
  "123456"
  "<contact>"
  "<address category=\"residence\">"
  "<name>Tanmay Patil</name>"
  "</address>"
  "</contact>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  ParamPrint param_list;
  param_list.version = "";
  param_list.encode = "";
  param_list.indent = 2;
  content->print_content(buf_str, false, false, ObXmlFormatType::NO_FORMAT, param_list);
  ASSERT_EQ(std::string(ser_text.ptr(), ser_text.length()), std::string(buf_str.ptr(), buf_str.length()));
}

TEST_F(TestXmlNodeBase, test_print_xml_node)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
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
  common::ObString serialize_text(
  "<shiporder orderid=\"889923\">"
  "<orderperson>John Smith</orderperson>"
  "<shipto>"
  "<name>Ola Nordmann</name>"
  "<address>Langgt 23</address>"
  "<city>4000 Stavanger</city>"
  "<country>Norway</country>"
  "</shipto>"
  "</shiporder>"
  );

  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->at(0)->print(buf_str, ObXmlFormatType::NO_FORMAT);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}
TEST_F(TestXmlNodeBase, test_print_xml_node_with_pretty)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
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
  common::ObString serialize_text(
  "<shiporder orderid=\"889923\">\n"
  "   <orderperson>John Smith</orderperson>\n"
  "   <shipto>\n"
  "      <name>Ola Nordmann</name>\n"
  "      <address>Langgt 23</address>\n"
  "      <city>4000 Stavanger</city>\n"
  "      <country>Norway</country>\n"
  "   </shipto>\n"
  "</shiporder>"
  );

  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->at(0)->print(buf_str, ObXmlFormatType::WITH_FORMAT, 0, 3);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}

TEST_F(TestXmlNodeBase, test_simple_print_document_mysqltest)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text("<?xml version=\"1.0\"?><a/>");
  common::ObString serialize_text("<?xml version=\"1.0\"?>\n<a></a>");

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->print_document(buf_str, type, ObXmlFormatType::NO_FORMAT);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}

TEST_F(TestXmlNodeBase, test_xml_node_element_add_well_from)
{
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  // element1
  ObXmlElement element1(ObMulModeNodeType::M_ELEMENT, ctx);
  // element2
  ObXmlElement element2(ObMulModeNodeType::M_ELEMENT, ctx);
  // content
  ObXmlDocument content(ObMulModeNodeType::M_CONTENT, ctx);
  // attribute
  ObXmlAttribute attr1(ObMulModeNodeType::M_ATTRIBUTE, ctx);
  // PI
  ObXmlAttribute pi(ObMulModeNodeType::M_INSTRUCT, ctx);
  // CData
  ObXmlText cdata(ObMulModeNodeType::M_CDATA, ctx);
  // comment
  ObXmlText comment(ObMulModeNodeType::M_COMMENT, ctx);
  // text
  ObXmlText text(ObMulModeNodeType::M_TEXT, ctx);
  element1.init();
  element2.init();
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, element1.type());
  ObString emelent1= "emelent1";
  ObString emelent2= "emelent2";
  ASSERT_EQ(content.attribute_size(), 0);
  element1.set_key(emelent1);
  element2.set_key(emelent2);
  ASSERT_EQ(element1.get_key(), "emelent1");
  ObString cdata1= "cdata1";
  cdata.set_text(cdata1);
  ASSERT_EQ(cdata.get_text(), "cdata1");
  ObString key1= "key1";
  ObString value1= "value1";
  pi.set_key(key1);
  pi.set_value(value1);

  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr1.set_key(atr1);
  attr1.set_value(atr_value1);


  element1.add_element(&element2);
  element1.add_element(&element2);
  element1.add_attribute(&attr1);
  element2.add_attribute(&attr1);
  ASSERT_EQ(element1.size(), 2);
  ASSERT_EQ(element1.attribute_size(), 1);

  comment.set_text(cdata1);
  int res = cdata.compare(cdata1, res);
  ASSERT_EQ(res, 0);

  content.add_element(&element1);
  content.add_element(&element2);
  ObString str1 = "<dasdsa";
  content.append_unparse_text(str1);
  ASSERT_EQ(3, content.size());
  ObString str2 = " a=\"dasa\" />";
  content.append_unparse_text(str2);
  ASSERT_EQ(3, content.size());
  content.add_element(&comment);
  content.append_unparse_text(str2);
  ASSERT_EQ(5, content.size());
  ObStringBuffer str_res(&allocator);
  content.print_document(str_res, type, ObXmlFormatType::NO_FORMAT);
  cout<< "str_res :" <<endl;
  ObString str_res_ = str_res.string();
  cout<< str_res_ <<endl;

}

} // common
} // oceanbase


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