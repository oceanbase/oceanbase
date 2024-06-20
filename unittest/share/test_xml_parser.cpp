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
#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <mutex>
#define private public
#include "test_xml_utils.h"
#include "lib/xml/ob_xpath.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "libxml2/libxml/xmlmemory.h"
#include "lib/xml/ob_xml_util.h"
#undef private

namespace oceanbase {
namespace common {


using namespace ObXmlTestUtils;
#define OB_XML_CHECK_XML_ATTRIBUTE(attr, attr_type, prefix, name, value) \
do {\
  ASSERT_EQ(attr_type, attr->type());\
  ObXmlAttribute* _attr_node = dynamic_cast<ObXmlAttribute*>(attr);\
  ObString attr##_prefix= _attr_node->get_prefix();\
  ASSERT_EQ(prefix, std::string(attr##_prefix.ptr(), attr##_prefix.length()));\
  ObString attr##_name = _attr_node->get_key();\
  ASSERT_EQ(std::string(name), std::string(attr##_name.ptr(), attr##_name.length()));\
  ObString attr##_value;\
  _attr_node->get_value(attr##_value);\
  ASSERT_EQ(std::string(value), std::string(attr##_value.ptr(), attr##_value.length()));\
}while(0);

#define OB_XML_CHECK_XML_TEXT(node, node_type, value) \
do {\
  ASSERT_EQ(node_type, node->type());\
  ObXmlText* _text_node = dynamic_cast<ObXmlText*>(node);\
  ObString _text_value;\
  _text_node->get_value(_text_value);\
  ASSERT_EQ(value, std::string(_text_value.ptr(), _text_value.length()));\
}while(0);

namespace ObXmlTestMemoryUsage {

// class ObXmlTestArenaAllocator : public ObArenaAllocator {
// public:

//   ObXmlTestArenaAllocator(const lib::ObLabel &label): ObArenaAllocator(label) {}
//   virtual ~ObXmlTestArenaAllocator() {}

//   virtual void *alloc(const int64_t sz) {
//     if (sz > 600)
//       std::cout << "alloc size " << sz << std::endl;
//     return arena_.alloc_aligned(sz);
//   }
// };

static std::mutex mut;
static int mem_counter = 0;
static std::unordered_map<void*, int> alloc_size;

static void alloc_memory(void* ptr, int size) {
  std::lock_guard<std::mutex> guard(mut);
  //std::cout << "alloc_memory size " << size << " old_mem_counter " << mem_counter << " new_mem_counter " << (mem_counter+size)<< std::endl;
  mem_counter += size;
  alloc_size[ptr] = size;
}

static void free_memory(void* ptr) {
  std::lock_guard<std::mutex> guard(mut);
  int size = alloc_size[ptr];
  //std::cout << "free_memory size " << size << " old_mem_counter " << mem_counter << " new_mem_counter " << (mem_counter-size)<< std::endl;
  mem_counter -= size;
  alloc_size.erase(ptr);
}
void* test_malloc (size_t size) {
  void* ptr = malloc(size);
  alloc_memory(ptr, size);
  return ptr;
}

void* test_realloc (void  *ptr, size_t size) {
  void* nptr = realloc(ptr, size);
  free_memory(ptr);
  alloc_memory(nptr, size);
  return nptr;
}

void test_free (void  *ptr) {
  free(ptr);
  free_memory(ptr);
}

char* test_strdup (const char *str) {
  int str_len = str == nullptr  ? 0 : STRLEN(str);
  char* dup_str = nullptr;
  if(str_len > 0) {
    if(OB_ISNULL(dup_str = static_cast<char*>(test_malloc(str_len+1)))) {
      std::cout << "test_malloc failed" << std::endl;
    } else {
      MEMCPY(dup_str, str, str_len);
      dup_str[str_len] = 0;
    }
  }
  return dup_str;
}
};

class TestXmlParser : public ::testing::Test {
public:
  static constexpr const char* separator = "\n";
  TestXmlParser()
  {}
  ~TestXmlParser()
  {}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

  static void SetUpTestCase()
  {
  xmlMemSetup(oceanbase::common::ObXmlTestMemoryUsage::test_free,
              oceanbase::common::ObXmlTestMemoryUsage::test_malloc,
              oceanbase::common::ObXmlTestMemoryUsage::test_realloc,
              oceanbase::common::ObXmlTestMemoryUsage::test_strdup);
  }

  static void TearDownTestCase()
  {
    xmlCleanupParser();
    ASSERT_EQ(0, ObXmlTestMemoryUsage::mem_counter);
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestXmlParser);

};

TEST_F(TestXmlParser, test_simple_parser_document)
{
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
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<shiporder orderid=\"889923\">"
  "<orderperson>John Smith</orderperson>"
  "<shipto>"
  "<name>Ola Nordmann</name>"
  "<address>Langgt 23</address>"
  "<city>4000 Stavanger</city>"
  "<country>Norway</country>"
  "</shipto>"
  "</shiporder>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_simple_parser_document_with_blank)
{
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_invalid_document)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "orderid=\"889923\">\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_gbk_decl_document)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"gbk\"?>\n"
  "<obtime>国庆佳节</obtime>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"gbk\"?>\n"
  "<obtime>国庆佳节</obtime>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}
TEST_F(TestXmlParser, test_simple_chinese)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\"?>\n<obtime>国庆佳节</obtime>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_empty_encoding_decl)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\"?>\n<obtime encoding=\"\">国庆佳节</obtime>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, not_allow_inner_dtd_content)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<!DOCTYPE note [\n"
  "<!ENTITY nbsp \"&#xA0;\">\n"
  "<!ENTITY writer \"Writer: Donald Duck.\">\n"
  "<!ENTITY copyright \"Copyright: W3Schools.\">"
  "]>\n"
  "<note>\n"
  "<to>Tove</to>\n"
  "<from>Jani</from>\n"
  "<heading>Reminder</heading>\n"
  "<body>Don't forget me this weekend!</body>\n"
  "<footer>&writer;&nbsp;&copyright;</footer>\n"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, not_allow_custom_entity_ref)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<to>Tove</to>\n"
  "<from>Jani</from>\n"
  "<heading>Reminder</heading>\n"
  "<body>Don't forget me this weekend!</body>\n"
  "<footer>&writer;&nbsp;&copyright;</footer>\n"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_predefined_entity_ref)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&lt;</lt>\n"
  "<gt>&gt;</gt>\n"
  "<amp>&amp;</amp>\n"
  "<apos>&apos;</apos>\n"
  "<quot>&quot;</quot>\n"
  "</note>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>"
  "<lt>&lt;</lt>"
  "<gt>&gt;</gt>"
  "<amp>&amp;</amp>"
  "<apos>&apos;</apos>"
  "<quot>&quot;</quot>"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_predefined_entity_ref_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&lt;</lt>\n"
  "<gt>&gt;</gt>\n"
  "<amp>&amp;</amp>\n"
  "<apos>&apos;</apos>\n"
  "<quot>&quot;</quot>\n"
  "</note>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&lt;</lt>\n"
  "<gt>&gt;</gt>\n"
  "<amp>&amp;</amp>\n"
  "<apos>&apos;</apos>\n"
  "<quot>&quot;</quot>\n"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_char_entity_ref)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&#x0d;</lt>\n"
  "</note>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>"
  "<lt>&#x0d;</lt>"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_char_entity_ref_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&#x0d;</lt>\n"
  "</note>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  "<note>\n"
  "<lt>&#x0d;</lt>\n"
  "</note>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_comment)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<class_list>\n"
  "<!--Menu in the restaurant \n"
  "Type of dish -->\n"
  "<!--Calculator example -->\n"
  "<student>\n"
  "    <!-- this is student name -->\n"
  "    <name><!-- stuend name is --> Tanmay </name>\n"
  "    <grade>A</grade>\n"
  "</student>\n"
  "</class_list>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<class_list>"
  "<!--Menu in the restaurant \n"
  "Type of dish -->"
  "<!--Calculator example -->"
  "<student>"
  "<!-- this is student name -->"
  "<name><!-- stuend name is --> Tanmay </name>"
  "<grade>A</grade>"
  "</student>"
  "</class_list>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_comment_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<class_list>\n"
  "<!--Menu in the restaurant \n"
  "Type of dish -->\n"
  "<!--Calculator example -->\n"
  "<student>\n"
  "    <!-- this is student name -->\n"
  "    <name><!-- stuend name is --> Tanmay </name>\n"
  "    <grade>A</grade>\n"
  "</student>\n"
  "</class_list>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<!--Students grades are uploaded by months-->\n"
  "<class_list>\n"
  "<!--Menu in the restaurant \n"
  "Type of dish -->\n"
  "<!--Calculator example -->\n"
  "<student>\n"
  "    <!-- this is student name -->\n"
  "    <name><!-- stuend name is --> Tanmay </name>\n"
  "    <grade>A</grade>\n"
  "</student>\n"
  "</class_list>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);

  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_cdata_section)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\"?>\n"
  "<html xmlns=\"http://www.w3.org/1999/xhtml\">\n"
  "<head>\n"
  "<title>Displaying the Time</title>\n"
  "\n"
  "<script type=\"text/javascript\">\n"
  "<![CDATA[\n"
  "var currentTime = new Date()\n"
  "var hours = currentTime.getHours()\n"
  "var minutes = currentTime.getMinutes()\n"
  "\n"
  "var suffix = \"AM\";\n"
  "if (hours >= 12) {\n"
  "suffix = \"PM\";\n"
  "hours = hours - 12;\n"
  "}\n"
  "if (hours == 0) {\n"
  "hours = 12;\n"
  "}\n"
  "\n"
  "if (minutes < 10)\n"
  "minutes = \"0\" + minutes\n"
  "\n"
  "document.write(\"<b>\" + hours + \":\" + minutes + \" \" + suffix + \"</b>\")\n"
  "]]>\n"
  "</script>\n"
  "\n"
  "</head>\n"
  "<body>\n"
  "<h1>Displaying the Time</h1>\n"
  "</body>\n"
  "</html>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\"?>\n"
  "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
  "<head>"
  "<title>Displaying the Time</title>"
  "<script type=\"text/javascript\">"
  "<![CDATA[\n"
  "var currentTime = new Date()\n"
  "var hours = currentTime.getHours()\n"
  "var minutes = currentTime.getMinutes()\n"
  "\n"
  "var suffix = \"AM\";\n"
  "if (hours >= 12) {\n"
  "suffix = \"PM\";\n"
  "hours = hours - 12;\n"
  "}\n"
  "if (hours == 0) {\n"
  "hours = 12;\n"
  "}\n"
  "\n"
  "if (minutes < 10)\n"
  "minutes = \"0\" + minutes\n"
  "\n"
  "document.write(\"<b>\" + hours + \":\" + minutes + \" \" + suffix + \"</b>\")\n"
  "]]>"
  "</script>"
  "</head>"
  "<body>"
  "<h1>Displaying the Time</h1>"
  "</body>"
  "</html>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_cdata_section_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\"?>\n"
  "<html xmlns=\"http://www.w3.org/1999/xhtml\">\n"
  "<head>\n"
  "<title>Displaying the Time</title>\n"
  "\n"
  "<script type=\"text/javascript\">\n"
  "<![CDATA[\n"
  "var currentTime = new Date()\n"
  "var hours = currentTime.getHours()\n"
  "var minutes = currentTime.getMinutes()\n"
  "\n"
  "var suffix = \"AM\";\n"
  "if (hours >= 12) {\n"
  "suffix = \"PM\";\n"
  "hours = hours - 12;\n"
  "}\n"
  "if (hours == 0) {\n"
  "hours = 12;\n"
  "}\n"
  "\n"
  "if (minutes < 10)\n"
  "minutes = \"0\" + minutes\n"
  "\n"
  "document.write(\"<b>\" + hours + \":\" + minutes + \" \" + suffix + \"</b>\")\n"
  "]]>\n"
  "</script>\n"
  "\n"
  "</head>\n"
  "<body>\n"
  "<h1>Displaying the Time</h1>\n"
  "</body>\n"
  "</html>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_simple_namespace)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<root xmlns:h=\"http://www.w3.org/TR/html4/\" "
  "xmlns:f=\"http://www.w3.org/1999/xhtml\">\n"
  "\n"
  "<h:table>\n"
  "<h:tr>\n"
  "  <h:td>Apples</h:td>\n"
  "  <h:td>Bananas</h:td>\n"
  "</h:tr>\n"
  "</h:table>\n"
  "\n"
  "<f:table>\n"
  "<f:name>African Coffee Table</f:name>\n"
  "<f:width>80</f:width>\n"
  "<f:length>120</f:length>\n"
  "</f:table>\n"
  "</root>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<root xmlns:h=\"http://www.w3.org/TR/html4/\" "
  "xmlns:f=\"http://www.w3.org/1999/xhtml\">"
  "<h:table>"
  "<h:tr>"
  "<h:td>Apples</h:td>"
  "<h:td>Bananas</h:td>"
  "</h:tr>"
  "</h:table>"
  "<f:table>"
  "<f:name>African Coffee Table</f:name>"
  "<f:width>80</f:width>"
  "<f:length>120</f:length>"
  "</f:table>"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, with_simple_namespace_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<root xmlns:h=\"http://www.w3.org/TR/html4/\" "
  "xmlns:f=\"http://www.w3.org/1999/xhtml\">\n"
  "\n"
  "<h:table>\n"
  "<h:tr>\n"
  "  <h:td>Apples</h:td>\n"
  "  <h:td>Bananas</h:td>\n"
  "</h:tr>\n"
  "</h:table>\n"
  "\n"
  "<f:table>\n"
  "<f:name>African Coffee Table</f:name>\n"
  "<f:width>80</f:width>\n"
  "<f:length>120</f:length>\n"
  "</f:table>\n"
  "</root>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
  "<root xmlns:h=\"http://www.w3.org/TR/html4/\" "
  "xmlns:f=\"http://www.w3.org/1999/xhtml\">\n"
  "\n"
  "<h:table>\n"
  "<h:tr>\n"
  "  <h:td>Apples</h:td>\n"
  "  <h:td>Bananas</h:td>\n"
  "</h:tr>\n"
  "</h:table>\n"
  "\n"
  "<f:table>\n"
  "<f:name>African Coffee Table</f:name>\n"
  "<f:width>80</f:width>\n"
  "<f:length>120</f:length>\n"
  "</f:table>\n"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content)
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
  common::ObString serialize_text(
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
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_with_blank)
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
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  content = parser.document();
  ASSERT_NE(nullptr, content);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_with_pi)
{
  int ret = 0;
  common::ObString xml_text(
  "123456"
  "<contact>"
  "<?pi name target  ?>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  common::ObString serialize_text(
  "123456"
  "<contact>"
  "<?pi name target  ?>"
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
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_with_pi_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "123456"
  "<contact>"
  "<?pi name target  ?>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  content = parser.document();
  ASSERT_NE(nullptr, content);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_parse_content_with_xml_decl_at_begin)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml encoding=\"utf-8\"?>"
  "<contact>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_content_with_xml_decl_at_middle)
{
  int ret = 0;
  common::ObString xml_text(
  "<contact>"
  "<?xml encoding=\"utf-8\"?>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_content_with_xml_decl_at_end)
{
  int ret = 0;
  common::ObString xml_text(
  "<contact>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  "<?xml encoding=\"utf-8\"?>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_content_with_upper_xml_decl_at_middle)
{
  int ret = 0;
  common::ObString xml_text(
  "<contact>"
  "<?XML encoding=\"utf-8\"?>"
  "<address category=\"residence\">"
  "    <name>Tanmay Patil</name>"
  " </address>"
  "</contact>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_content_with_predefined_entity)
{
  int ret = 0;
  common::ObString xml_text(
  "<sayhello word='&apos;Hi&apos;' />"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  content = parser.document();
  ASSERT_NE(nullptr, content);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<sayhello word=\"&apos;Hi&apos;\"/>"), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_with_process_instruction)
{
  int ret = 0;
  common::ObString xml_text(
  "<?welcome to pg = 10 of tutorials point?>"
  "<?welcome?>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_predined_entity)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome>xyz&lt;abc</welcome>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  content = parser.document();
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<welcome>xyz&lt;abc</welcome>"), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_text_sep)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome>xyz&lt;abc</welcome>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  content = parser.document();
  ASSERT_NE(nullptr, content);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<welcome>xyz&lt;abc</welcome>\n"), std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "xyz&lt;abc")
}

TEST_F(TestXmlParser, test_parse_text_sep_v2)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome> xyz abc  </welcome>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<welcome> xyz abc  </welcome>\n"), std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, " xyz abc  ")
}


TEST_F(TestXmlParser, test_parse_text_sep_v3)
{
  int ret = 0;
  common::ObString xml_text(
  8,
  "<x>y</x>1"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<x>y</x>\n"), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_parse_blank_start)
{
  int ret = 0;
  common::ObString xml_text(
  "   <x>y</x>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<x>y</x>\n"), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_blank_start)
{
  int ret = 0;
  common::ObString xml_text(
  "<x>y</x>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<x>y</x>"), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_parse_text_no_sep)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome>xyz123abc</welcome>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<welcome>xyz123abc</welcome>\n"), std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "xyz123abc")

}

TEST_F(TestXmlParser, test_parse_text_huge_content)
{
  int ret = 0;

  std::string huge_text;
  for(int i=0 ; i<5000; ++i){
    huge_text.append(std::to_string(i)).append(" ");
  }
  std::string xml_str = "<content>" + huge_text + "</content>\n";

  common::ObString xml_text(xml_str.length(), xml_str.c_str());
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(xml_str, std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, huge_text)
}

TEST_F(TestXmlParser, test_parse_cdata_sep)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome><![CDATA[xyz&lt;abc]]></welcome>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_CDATA, "xyz&lt;abc")
}

TEST_F(TestXmlParser, test_parse_cdata_no_sep)
{
  int ret = 0;
  common::ObString xml_text(
  "<welcome><![CDATA[xyz123abc]]></welcome>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_CDATA, "xyz123abc")

}

TEST_F(TestXmlParser, test_parse_cdata_huge_content)
{
  int ret = 0;

  std::string huge_text;
  for(int i=0 ; i<5000; ++i){
    huge_text.append(std::to_string(i)).append(" ");
  }
  std::string xml_str = "<content><![CDATA[" + huge_text + "]]></content>\n";

  common::ObString xml_text(xml_str.length(), xml_str.c_str());
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(xml_str, std::string(s.ptr(), s.length()));

  ASSERT_EQ(1, content->size());
  ObXmlNode* root_node = content->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_CDATA, huge_text)

}

TEST_F(TestXmlParser, test_parse_content_with_ns_attr)
{
  int ret = 0;
  common::ObString xml_text(
  "<BOOKS>"
  "<bk:BOOK xmlns:bk=\"urn:example.microsoft.com:BookInfo\" "
  "xmlns:money=\"urn:Finance:Money\">\n"
  "  <bk:TITLE>Creepy Crawlies</bk:TITLE>\n"
  "  <bk:PRICE money:currency=\"US Dollar\">22.95</bk:PRICE>\n"
  "</bk:BOOK>\n"
  "</BOOKS>\n"
  );
  common::ObString serialize_text(
  "<BOOKS>"
  "<bk:BOOK xmlns:bk=\"urn:example.microsoft.com:BookInfo\" "
  "xmlns:money=\"urn:Finance:Money\">"
  "<bk:TITLE>Creepy Crawlies</bk:TITLE>"
  "<bk:PRICE money:currency=\"US Dollar\">22.95</bk:PRICE>"
  "</bk:BOOK>"
  "</BOOKS>"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, content);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_content_with_ns_attr_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<BOOKS>"
  "<bk:BOOK xmlns:bk=\"urn:example.microsoft.com:BookInfo\" "
  "xmlns:money=\"urn:Finance:Money\">\n"
  "  <bk:TITLE>Creepy Crawlies</bk:TITLE>\n"
  "  <bk:PRICE money:currency=\"US Dollar\">22.95</bk:PRICE>\n"
  "</bk:BOOK>\n"
  "</BOOKS>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* content = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  content = parser.document();
  ASSERT_NE(nullptr, content);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(content);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_namespace_order_default)
{
  int ret = 0;
  common::ObString xml_text(
  "<root>"
  "<h:table h:color=\"red\" xmlns:h=\"http://www.w3.org/TR/html4/\">"
  "  <h:tr>"
  "    <h:td>Apples</h:td>"
  "    <h:td>Bananas</h:td>"
  "	</h:tr>"
  "</h:table>"
  "<f:table xmlns:f=\"http://www.w3.org/1999/xhtml\" f:color=\"blue\">"
  "  <f:name>African Coffee Table</f:name>"
  "  <f:width>80</f:width>"
  "  <f:length>120</f:length>"
  "</f:table>"
	"<table>"
  "	<a>aaa</a>"
  "	<b>bbb</b>"
	"</table>"
  "</root>\n"
  );
  common::ObString serialize_text(
  "<root>"
  "<h:table h:color=\"red\" xmlns:h=\"http://www.w3.org/TR/html4/\">"
  "<h:tr>"
  "<h:td>Apples</h:td>"
  "<h:td>Bananas</h:td>"
  "</h:tr>"
  "</h:table>"
  "<f:table xmlns:f=\"http://www.w3.org/1999/xhtml\" f:color=\"blue\">"
  "<f:name>African Coffee Table</f:name>"
  "<f:width>80</f:width>"
  "<f:length>120</f:length>"
  "</f:table>"
	"<table>"
  "<a>aaa</a>"
  "<b>bbb</b>"
	"</table>"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_namespace_order_default_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "<root>"
  "<h:table h:color=\"red\" xmlns:h=\"http://www.w3.org/TR/html4/\">"
  "  <h:tr>"
  "    <h:td>Apples</h:td>"
  "    <h:td>Bananas</h:td>"
  "	</h:tr>"
  "</h:table>"
  "<f:table xmlns:f=\"http://www.w3.org/1999/xhtml\" f:color=\"blue\">"
  "  <f:name>African Coffee Table</f:name>"
  "  <f:width>80</f:width>"
  "  <f:length>120</f:length>"
  "</f:table>"
	"<table>"
  "	<a>aaa</a>"
  "	<b>bbb</b>"
	"</table>"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_parse_elemet_name_start_colon)
{
  int ret = 0;
  common::ObString xml_text(
  "<root>"
  "<:table xmlns=\"http://www.w3.org/TR/html4/\">"
  "</:table>"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_attr_name_start_colon)
{
  int ret = 0;
  common::ObString xml_text(
  "<root>"
  "<table :color=\"red\">"
  "</table>"
  "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_parse_empty_attribute)
{
  int ret = 0;
  common::ObString xml_text(
    "<root>"
    "	<a attr1=\"\" attr2=\"xyz\">aaa</a>"
    "	<b>bbb</b>"
    "</root>\n"
  );
  common::ObString serialize_text(
    "<root>"
    "<a attr1=\"\" attr2=\"xyz\">aaa</a>"
    "<b>bbb</b>"
    "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_parse_empty_attribute_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
    "<root>"
    "	<a attr1=\"\" attr2=\"xyz\">aaa</a>"
    "	<b>bbb</b>"
    "</root>\n"
  );
  common::ObString serialize_text(
    "<root>"
    "	<a attr1=\"\" attr2=\"xyz\">aaa</a>"
    "	<b>bbb</b>"
    "</root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_parse_default_ns)
{
  int ret = 0;
  common::ObString xml_text(
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "  <tr>"
  "    <td>Apples</td>"
  "    <td>Bananas</td>"
  "	</tr>"
  "</table>"
  );
  common::ObString serialize_text(
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "<tr>"
  "<td>Apples</td>"
  "<td>Bananas</td>"
  "</tr>"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}





TEST_F(TestXmlParser, test_empty_version)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_empty_version_with_encoding)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml encoding=\"utf-8\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_version_is_1)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_version_is_1_2)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.2\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_version_is_2_0)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"2.0\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_without_encoding)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_encoding_empty)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  //ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_encoding_not_support)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utt-8\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  //ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_standalone_with_yes)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" standalone=\"yes\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" standalone=\"yes\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_standalone_with_no)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" standalone=\"no\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" standalone=\"no\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_standalone_with_empty)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" standalone=\"\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" standalone=\"\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_standalone_with_n)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" standalone=\"n\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" standalone=\"n\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_standalone_with_other)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" standalone=\"other\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" standalone=\"other\"?>\n"
  "<table color=\"red\" xmlns=\"http://www.w3.org/TR/html4/\">"
  "</table>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_empty_content)
{
  int ret = 0;
  common::ObString xml_text(
  "     "
  );
  common::ObString serialize_text(
  "     "
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, doc);
  ASSERT_NE(nullptr, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestXmlParser, test_empty_content_with_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "     "
  );
  common::ObString serialize_text(
  "     "
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_blank_text)
{
  int ret = 0;
  common::ObString xml_text(
  "      <a>     <b> </b></a>"
  );
  common::ObString serialize_text(
  "<a><b> </b></a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_blank_text_v2)
{
  int ret = 0;
  common::ObString xml_text(
  "<a>    <b>        abc          </b></a>"
  );
  common::ObString serialize_text(
  "<a><b>        abc          </b></a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_blank_text_v3)
{
  int ret = 0;
  common::ObString xml_text(
  "<a>    <b>    张三       </b></a>"
  );
  common::ObString serialize_text(
  "<a><b>    张三       </b></a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_blank_text_not_ignore)
{
  int ret = 0;
  common::ObString xml_text(
  "      <a>     <b> </b></a>"
  );
  common::ObString serialize_text(
  "<a>     <b> </b></a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_ignore_space();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_unicode_char_ref)
{
  int ret = 0;
  common::ObString xml_text(
  "<lt>&#x01f60d;</lt>\n"
  );
  common::ObString serialize_text(
  "<lt>&#x01f60d;</lt>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_empty_tag)
{
  int ret = 0;
  common::ObString xml_text(
  "<root><a1></a1><a2/></root>\n"
  );
  common::ObString serialize_text(
  "<root><a1></a1><a2/></root>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_empty_doc)
{
  int ret = 0;
  common::ObString xml_text(
  ""
  );
  common::ObString serialize_text(
  ""
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_blank_doc)
{
  int ret = 0;
  common::ObString xml_text(
  "   "
  );
  common::ObString serialize_text(
  "   "
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_document_syntax_check)
{
  int ret = 0;
  common::ObString xml_text(
  "<a><b>123</b></a>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_only_syntax_check();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  // todo free
  // doc = parser.document();
  // ASSERT_NE(nullptr, doc);
}

TEST_F(TestXmlParser, test_content_syntax_check)
{
  int ret = 0;
  common::ObString xml_text(
  "<a><b>123</b></a>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_only_syntax_check();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  // todo free
  // doc = parser.document();
  // ASSERT_NE(nullptr, doc);
}

TEST_F(TestXmlParser, test_ns_elem_simple)
{
  int ret = 0;
  common::ObString xml_text(
  "<t:a xmlns:t=\"test.xsd\" t:color=\"red\">123</t:a>"
  );
  common::ObString serialize_text(
  "<t:a xmlns:t=\"test.xsd\" t:color=\"red\">123</t:a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_ns_attr_simple)
{
  int ret = 0;
  common::ObString xml_text(
  "<a xmlns:t=\"test.xsd\" t:color=\"red\">123</a>"
  );

  common::ObString serialize_text(
  "<a xmlns:t=\"test.xsd\" t:color=\"red\">123</a>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());
  ASSERT_EQ(2, root->attribute_size());

  // check root attrs
  ObXmlAttribute* attr1 = NULL;
  ret = root->get_attribute(attr1, 0);
  OB_XML_CHECK_XML_ATTRIBUTE(attr1, ObMulModeNodeType::M_NAMESPACE, "xmlns", "t", "test.xsd");

  ObXmlAttribute* attr2 = NULL;
  ret = root->get_attribute(attr2, 1);
  OB_XML_CHECK_XML_ATTRIBUTE(attr2, ObMulModeNodeType::M_ATTRIBUTE, "t", "color", "red");

  // check namepsace
  ObXmlAttribute* attr2_ns = dynamic_cast<ObXmlAttribute*>(attr2)->get_ns();
  ASSERT_EQ(attr1, attr2_ns);

  ObXmlAttribute* attr1_ns = dynamic_cast<ObXmlAttribute*>(attr1)->get_ns();
  ASSERT_EQ(nullptr, attr1_ns);

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "123")

}

TEST_F(TestXmlParser, test_ns_attr_before_simple)
{
  int ret = 0;
  common::ObString xml_text(
  "<a t:color=\"red\" xmlns:t=\"test.xsd\">123</a>"
  );

  common::ObString serialize_text(
  "<a t:color=\"red\" xmlns:t=\"test.xsd\">123</a>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());
  ASSERT_EQ(2, root->attribute_size());

  // check root attrs
  ObXmlAttribute* attr1 = NULL;
  ret = root->get_attribute(attr1, 0);
  OB_XML_CHECK_XML_ATTRIBUTE(attr1, ObMulModeNodeType::M_ATTRIBUTE, "t", "color", "red");

  ObXmlAttribute* attr2 = NULL;
  ret = root->get_attribute(attr2, 1);
  OB_XML_CHECK_XML_ATTRIBUTE(attr2, ObMulModeNodeType::M_NAMESPACE, "xmlns", "t", "test.xsd");

  // check namepsace
  ObXmlAttribute* attr1_ns = dynamic_cast<ObXmlAttribute*>(attr1)->get_ns();
  ASSERT_EQ(attr2, attr1_ns);

  ObXmlAttribute* attr2_ns = dynamic_cast<ObXmlAttribute*>(attr2)->get_ns();
  ASSERT_EQ(nullptr, attr2_ns);

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "123")
}

TEST_F(TestXmlParser, test_ns_default_elem)
{
  int ret = 0;
  common::ObString xml_text(
  "<a color=\"red\" xmlns=\"test.xsd\">123</a>"
  );

  common::ObString serialize_text(
  "<a color=\"red\" xmlns=\"test.xsd\">123</a>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(2, root->attribute_size());
  ASSERT_EQ(1, root->size());

  // check root attrs
  ObXmlAttribute* attr1 = NULL;
  ret = root->get_attribute(attr1, 0);
  OB_XML_CHECK_XML_ATTRIBUTE(attr1, ObMulModeNodeType::M_ATTRIBUTE, "", "color", "red");

  ObXmlAttribute* attr2 = NULL;
  ret = root->get_attribute(attr2, 1);
  OB_XML_CHECK_XML_ATTRIBUTE(attr2, ObMulModeNodeType::M_NAMESPACE, "", "xmlns", "test.xsd");

  // check namepsace
  ObXmlAttribute* attr1_ns = attr1->get_ns();
  ASSERT_EQ(nullptr, attr1_ns);

  ObXmlAttribute* attr2_ns = attr2->get_ns();
  ASSERT_EQ(nullptr, attr2_ns);

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "123")
}


TEST_F(TestXmlParser, test_ns_multi)
{
  int ret = 0;
  common::ObString xml_text(
  "<n1:a n2:color=\"red\" xmlns=\"test.xsd\" xmlns:n1=\"n1.xsd\" xmlns:n2=\"n2.xsd\">"
  "<n1:name> xyz123 </n1:name>"
  "<age>30</age>"
  "<n2:job>se</n2:job>"
  "<other xmlns:n3=\"n3.xsd\">"
  "<n3:addr>shenzhen</n3:addr>"
  "</other>"
  "<n2:country xmlns:n4=\"n4.xsd\">"
  "<n4:addr>shenzhen</n4:addr>"
  "</n2:country>"
  "</n1:a>"
  );

  common::ObString serialize_text(
  "<n1:a n2:color=\"red\" xmlns=\"test.xsd\" xmlns:n1=\"n1.xsd\" xmlns:n2=\"n2.xsd\">"
  "<n1:name> xyz123 </n1:name>"
  "<age>30</age>"
  "<n2:job>se</n2:job>"
  "<other xmlns:n3=\"n3.xsd\">"
  "<n3:addr>shenzhen</n3:addr>"
  "</other>"
  "<n2:country xmlns:n4=\"n4.xsd\">"
  "<n4:addr>shenzhen</n4:addr>"
  "</n2:country>"
  "</n1:a>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_ns_multi_override)
{
  int ret = 0;
  common::ObString xml_text(
  "<n1:a n2:color=\"red\" xmlns=\"test.xsd\" xmlns:n1=\"n1.xsd\" xmlns:n2=\"n2.xsd\">"
  "<n1:name> xyz123 </n1:name>"
  "<age>30</age>"
  "<n2:job>se</n2:job>"
  "<other xmlns:n3=\"n3.xsd\">"
  "<n3:addr>shenzhen</n3:addr>"
  "</other>"
  "<n1:country xmlns:n1=\"n4.xsd\">"
  "<n1:addr>shenzhen</n1:addr>"
  "</n1:country>"
  "</n1:a>"
  );

  common::ObString serialize_text(
  "<n1:a n2:color=\"red\" xmlns=\"test.xsd\" xmlns:n1=\"n1.xsd\" xmlns:n2=\"n2.xsd\">"
  "<n1:name> xyz123 </n1:name>"
  "<age>30</age>"
  "<n2:job>se</n2:job>"
  "<other xmlns:n3=\"n3.xsd\">"
  "<n3:addr>shenzhen</n3:addr>"
  "</other>"
  "<n1:country xmlns:n1=\"n4.xsd\">"
  "<n1:addr>shenzhen</n1:addr>"
  "</n1:country>"
  "</n1:a>\n"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}


TEST_F(TestXmlParser, test_doc_start_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "   <a>          </a>   "
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ASSERT_EQ("a", std::string(root->get_key().ptr(), root->get_key().length()));

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "          ")
}

TEST_F(TestXmlParser, test_content_start_blank)
{
  int ret = 0;
  common::ObString xml_text(
  "   <a>          </a>   "
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObXmlTreeTextWriter writer(&allocator);
  writer.visit(doc);
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string("<a>          </a>"), std::string(s.ptr(), s.length()));

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ASSERT_EQ("a", std::string(root->get_key().ptr(), root->get_key().length()));

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "          ")
}


TEST_F(TestXmlParser, test_non_c_str_content_parse)
{
  int ret = 0;

  char str[15] = "<a>456</a>";
  str[10] = 1;
  str[11] = 2;
  str[12] = 3;
  str[14] = 3;

  common::ObString xml_text(
  10,
  str
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check doc children
  ASSERT_EQ(1, doc->size());
  ObXmlNode* root_node = doc->at(0);
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, root_node->type());
  ObXmlElement* root = dynamic_cast<ObXmlElement*>(root_node);
  ASSERT_EQ(1, root->size());

  ASSERT_EQ("a", std::string(root->get_key().ptr(), root->get_key().length()));

  // check root text
  ObXmlNode* text_node = root->at(0);
  OB_XML_CHECK_XML_TEXT(text_node, ObMulModeNodeType::M_TEXT, "456")
}

TEST_F(TestXmlParser, test_content_not_allow_with_xml_decl)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<a>123</a>"
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<b>456</b>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<a>123</a>"
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<b>456</b>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, parser.parse_content(xml_text));
}

TEST_F(TestXmlParser, test_content_with_xml_decl)
{
  int ret = 0;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<a>123</a>"
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<b>456</b>"
  );
  common::ObString serialize_text(
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<a>123</a>"
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
  "<b>456</b>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_content_allow_xml_decl();
  ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_attr_escape)
{
  int ret = 0;
  common::ObString xml_text(
  "<a attr=\"&lt;&gt;&amp;&quot;&apos;\">123</a>"
  );
  common::ObString serialize_text(
  "<a attr=\"&lt;&gt;&amp;&quot;&apos;\">123</a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  parser.set_not_entity_replace();
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_attr_not_escape)
{
  int ret = 0;
  common::ObString xml_text(
  "<a attr=\"&lt;&gt;&amp;&quot;&apos;\">123</a>"
  );
  common::ObString serialize_text(
  "<a attr=\"<>&\"'\">123</a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

TEST_F(TestXmlParser, test_cons_mem_usage)
{
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  // element1
  ObXmlElement *element1 = OB_NEWx(ObXmlElement, &allocator, ObMulModeNodeType::M_ELEMENT, ctx);
  element1->init();
  // element2
  ObXmlElement *element2 = OB_NEWx(ObXmlElement, &allocator, ObMulModeNodeType::M_ELEMENT, ctx);
  element2->init();
  // content
  ObXmlDocument *doc = OB_NEWx(ObXmlDocument, &allocator, ObMulModeNodeType::M_CONTENT, ctx);

  // attribute
  ObXmlAttribute *attr1 = OB_NEWx(ObXmlAttribute, &allocator, ObMulModeNodeType::M_ATTRIBUTE, ctx);

  // text
  ObXmlText *text1 = OB_NEWx(ObXmlText, &allocator, ObMulModeNodeType::M_TEXT, ctx);
  ObXmlText *text2 = OB_NEWx(ObXmlText, &allocator, ObMulModeNodeType::M_TEXT, ctx);

  ObString emelent1= "emelent1";
  ObString emelent2= "emelent2";
  element1->set_xml_key(emelent1);
  element2->set_xml_key(emelent2);

  ObString atr1= "sttr1_name";
  ObString atr_value1= "sttr1_value";
  attr1->set_xml_key(atr1);
  attr1->set_value(atr_value1);

  element1->add_element(text1);
  element1->add_attribute(attr1);
  element2->add_element(text2);
  doc->add_element(element1);
  doc->add_element(element2);

  std::cout << "used " << allocator.used() << std::endl;
  std::cout << "total " << allocator.total() << std::endl;
}

TEST_F(TestXmlParser, test_mem_usage)
{
  int ret = 0;
  common::ObString xml_text(
  "<profile>"
  "<name>John</name>"
  "<age>30</age>"
  "<job>se</job>"
  "</profile>"
  );
  //ObXmlTestMemoryUsage::ObXmlTest
  ObArenaAllocator allocator(ObModIds::TEST);
  for(int i=0; i<50000; ++i) {
    ObMulModeMemCtx* ctx = nullptr;
    ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
    ObXmlDocument* doc = nullptr;
    ObXmlParser parser(ctx);
    ASSERT_EQ(OB_SUCCESS, parser.parse_content(xml_text));
    doc = parser.document();
    ASSERT_NE(nullptr, doc);
  }

  std::cout << "ObIMulModeBase " << sizeof(ObIMulModeBase) << std::endl;
  std::cout << "ObLibContainerNode " << sizeof(ObLibContainerNode) << std::endl;
  std::cout << "ObLibTreeNodeBase " << sizeof(ObLibTreeNodeBase) << std::endl;
  std::cout << "ObArray " << sizeof(ObArray<ObLibContainerNode::iterator>) << std::endl;
  std::cout << "ObLibTreeNodeVector " << sizeof(ObLibTreeNodeVector) << std::endl;
  std::cout << "LibTreeModuleArena " << sizeof(LibTreeModuleArena) << std::endl;
  std::cout << "ModulePageAllocator " << sizeof(ModulePageAllocator) << std::endl;

  std::cout << "ObXmlNode " << sizeof(ObXmlNode) << std::endl;
  std::cout << "ObXmlDocument " << sizeof(ObXmlDocument) << std::endl;
  std::cout << "ObXmlElement " << sizeof(ObXmlElement) << std::endl;
  std::cout << "ObXmlText " << sizeof(ObXmlText) << std::endl;
  std::cout << "ObXmlAttribute " << sizeof(ObXmlAttribute) << std::endl;

  std::cout << "ObPathNode " << sizeof(ObPathNode) << std::endl;
  std::cout << "ObPathLocationNode " << sizeof(ObPathLocationNode) << std::endl;
  std::cout << "ObPathFilterNode " << sizeof(ObPathFilterNode) << std::endl;
  std::cout << "ObPathFuncNode " << sizeof(ObPathFuncNode) << std::endl;
  std::cout << "ObPathArgNode " << sizeof(ObPathArgNode) << std::endl;

  std::cout << "used " << allocator.used() << std::endl;
  std::cout << "total " << allocator.total() << std::endl;
}

TEST_F(TestXmlParser, test_xml_bin_to_binary)
{
  int ret = 0;
  common::ObString xml_text(
  "<root>"
  "<a>123</a>"
  "<b>456</b>"
  "</root>"
  );
  common::ObString serialize_text(
  "<root>"
  "<a>123</a>"
  "<b>456</b>"
  "</root>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObArenaAllocator tmp_allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&tmp_allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  ASSERT_NE(nullptr, (doc = parser.document()));
  doc->set_xml_type(M_CONTENT);

  ObString bin;
  ObString text;
  ASSERT_EQ(OB_SUCCESS, doc->get_raw_binary(bin, &tmp_allocator));
  ASSERT_EQ(OB_SUCCESS, ObXmlUtil::xml_bin_to_text(allocator, bin, text));
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(text.ptr(), text.length()));

  std::cout << "tmp used " << tmp_allocator.used() << std::endl;
  std::cout << "tmp total " << tmp_allocator.total() << std::endl;
  std::cout << "used " << allocator.used() << std::endl;
  std::cout << "total " << allocator.total() << std::endl;
  std::cout << "total " << text.length() << std::endl;
}

TEST_F(TestXmlParser, test_endtags_content)
{
  int ret = 0;
  common::ObString xml_text(
  "<customerName> Acme Enterprises</customerName>"
  "<itemNo>32987457</itemNo>"
  "</aseOrder>"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_content_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
}

TEST_F(TestXmlParser, test_revert_escape)
{
  int ret = 0;
  common::ObString text_1(
  "&lt;heading&gt;Reminder&lt;/heading&gt;"
  "&lt;a/&gt;&amp;:&apos;&apos;&apos; &quot; &apos;"
  );
  common::ObString text_2("<heading>Reminder</heading><a/>&:''' \" '");
  common::ObString text_3("abdasdjkkjlasdopqweoionk");
  common::ObString res;
  ObArenaAllocator allocator(ObModIds::TEST);
  ret = ObXmlUtil::revert_escape_character(allocator, text_1, res);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(std::string(res.ptr(), res.length()), std::string(text_2.ptr(), text_2.length()));
  ret = ObXmlUtil::revert_escape_character(allocator, text_3, res);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(std::string(res.ptr(), res.length()), std::string(text_3.ptr(), text_3.length()));
}

TEST_F(TestXmlParser, test_attr_and_text_no_escape)
{
  int ret = 0;
  common::ObString xml_text(
  "<a attr=\"&lt;&gt;&amp;&quot;&apos;\">&lt;&gt;&amp;&quot;&apos;</a>"
  );
  common::ObString serialize_text(
  "<a attr=\"<>&\"'\"><>&\"'</a>\n"
  );
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlParser parser(ctx);
  ASSERT_EQ(OB_SUCCESS, parser.parse_document(xml_text));
  doc = parser.document();
  ASSERT_NE(nullptr, doc);
  ObXmlTreeTextWriter writer(&allocator);
  ASSERT_EQ(OB_SUCCESS, writer.visit(doc));
  ObString s = writer.get_xml_text();
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(s.ptr(), s.length()));
}

// class TestMemoryXmlParser : public ::testing::Test {
// public:
//   TestMemoryXmlParser()
//   {}
//   ~TestXmlParser()
//   {}
//   virtual void SetUp()
//   {
//   }
//   virtual void TearDown()
//   {
//   }

//   static void SetUpTestCase()
//   {
//     xmlDefaultBufferSize = 1024;
//     xmlMemSetup(ObXmlTestMemoryUsage::test_free,
//                 ObXmlTestMemoryUsage::test_malloc,
//                 ObXmlTestMemoryUsage::test_realloc,
//                 ObXmlTestMemoryUsage::test_strdup);
//   }

//   static void TearDownTestCase()
//   {
//     ASSERT_EQ(0, ObXmlTestMemoryUsage::mem_counter);
//   }

// private:
//   // disallow copy
//   DISALLOW_COPY_AND_ASSIGN(TestXmlParser);

// };


} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}