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
#define protected public

#include "lib/xml/ob_multi_mode_bin.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#undef private

#include <sys/time.h>
using namespace std;

namespace oceanbase {
namespace  common{

class TestXmlBin : public ::testing::Test {
public:
  TestXmlBin()
  {}
  ~TestXmlBin()
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
  DISALLOW_COPY_AND_ASSIGN(TestXmlBin);
};


static void get_xml_document_1(ObMulModeMemCtx* ctx, ObXmlDocument*& handle)
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

  ObXmlDocument* doc = nullptr;
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  handle = doc;
}


TEST_F(TestXmlBin, serialize_bin_header)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_xml_document_1(ctx, doc);
  ASSERT_EQ(OB_SUCCESS, ret);

  {
    ObStringBuffer buffer(&allocator);
    ObMulBinHeaderSerializer serializer1(&buffer,
                                        doc->type(),
                                        doc->get_serialize_size(),
                                        doc->size());

    // serialize meta info check
    ASSERT_EQ(serializer1.get_obj_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer1.get_entry_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer1.get_count_var_size(), sizeof(uint8_t));

    ASSERT_EQ(serializer1.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer1.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer1.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(serializer1.header_size(), 5);


    ASSERT_EQ(serializer1.count_, doc->size());
    ASSERT_EQ(doc->type(), M_DOCUMENT);
    ASSERT_EQ(serializer1.start(), buffer.length());


    ASSERT_EQ(serializer1.obj_var_offset_, 3);
    ASSERT_EQ(serializer1.count_var_offset_, 2);

    ASSERT_EQ(OB_SUCCESS, serializer1.serialize());

    // deserialize
    ObMulBinHeaderSerializer serializer2(buffer.ptr(), buffer.length());
    ASSERT_EQ(serializer2.deserialize(), 0);

    ASSERT_EQ(serializer2.get_obj_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer2.get_entry_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer2.get_count_var_size(), sizeof(uint8_t));
    ASSERT_EQ(serializer2.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer2.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer2.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(doc->type(), M_DOCUMENT);
    ASSERT_EQ(serializer2.obj_var_offset_, 3);
    ASSERT_EQ(serializer2.count_var_offset_, 2);
    ASSERT_EQ(serializer2.count_, doc->size());

    ASSERT_EQ(serializer2.total_, doc->get_serialize_size());
    ASSERT_EQ(serializer2.total_, serializer1.total_);
  }


  {
    ObStringBuffer buffer(&allocator);
    int64_t count = 128;
    int64_t total = 127;
    ObMulBinHeaderSerializer serializer1(&buffer, M_DOCUMENT, total, count);

    // serialize meta info check
    ASSERT_EQ(serializer1.get_obj_var_size(), sizeof(uint8_t));
    ASSERT_EQ(serializer1.get_entry_var_size(), sizeof(uint8_t));
    ASSERT_EQ(serializer1.get_count_var_size(), sizeof(uint16_t));

    ASSERT_EQ(serializer1.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(serializer1.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(serializer1.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);

    ASSERT_EQ(serializer1.count_, count);
    ASSERT_EQ(serializer1.type(), M_DOCUMENT);
    ASSERT_EQ(serializer1.start(), buffer.length());


    ASSERT_EQ(serializer1.obj_var_offset_, 4);
    ASSERT_EQ(serializer1.count_var_offset_, 2);
    ASSERT_EQ(5, serializer1.header_size());

    ASSERT_EQ(OB_SUCCESS, serializer1.serialize());

    // deserialize
    ObMulBinHeaderSerializer serializer2(buffer.ptr(), buffer.length());
    ASSERT_EQ(serializer2.deserialize(), 0);

    ASSERT_EQ(serializer2.get_obj_var_size(), sizeof(uint8_t));
    ASSERT_EQ(serializer2.get_entry_var_size(), sizeof(uint8_t));
    ASSERT_EQ(serializer2.get_count_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer2.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(serializer2.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT8);
    ASSERT_EQ(serializer2.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer2.type(), M_DOCUMENT);
    ASSERT_EQ(serializer2.obj_var_offset_, 4);
    ASSERT_EQ(serializer2.count_var_offset_, 2);
    ASSERT_EQ(serializer2.count_, count);
    ASSERT_EQ(serializer2.total_, total);
  }

  {
    ObStringBuffer buffer(&allocator);
    int64_t count = 1100;
    int64_t total = 66536;
    ObMulBinHeaderSerializer serializer1(&buffer, M_DOCUMENT, total, count);

    // serialize meta info check
    ASSERT_EQ(serializer1.get_obj_var_size(), sizeof(uint32_t));
    ASSERT_EQ(serializer1.get_entry_var_size(), sizeof(uint32_t));
    ASSERT_EQ(serializer1.get_count_var_size(), sizeof(uint16_t));

    ASSERT_EQ(serializer1.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT32);
    ASSERT_EQ(serializer1.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT32);
    ASSERT_EQ(serializer1.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);

    ASSERT_EQ(serializer1.count_, count);
    ASSERT_EQ(serializer1.type(), M_DOCUMENT);
    ASSERT_EQ(serializer1.start(), buffer.length());


    ASSERT_EQ(serializer1.obj_var_offset_, 4);
    ASSERT_EQ(serializer1.count_var_offset_, 2);

    ASSERT_EQ(OB_SUCCESS, serializer1.serialize());
    ASSERT_EQ(8, serializer1.header_size());

    // deserialize
    ObMulBinHeaderSerializer serializer2(buffer.ptr(), buffer.length());
    ASSERT_EQ(serializer2.deserialize(), 0);

    ASSERT_EQ(serializer2.get_obj_var_size(), sizeof(uint32_t));
    ASSERT_EQ(serializer2.get_entry_var_size(), sizeof(uint32_t));
    ASSERT_EQ(serializer2.get_count_var_size(), sizeof(uint16_t));
    ASSERT_EQ(serializer2.get_obj_var_size_type(), ObMulModeBinLenSize::MBL_UINT32);
    ASSERT_EQ(serializer2.get_entry_var_size_type(), ObMulModeBinLenSize::MBL_UINT32);
    ASSERT_EQ(serializer2.get_count_var_size_type(), ObMulModeBinLenSize::MBL_UINT16);
    ASSERT_EQ(serializer2.type(), M_DOCUMENT);
    ASSERT_EQ(serializer2.obj_var_offset_, 4);
    ASSERT_EQ(serializer2.count_var_offset_, 2);
    ASSERT_EQ(serializer2.count_, count);
    ASSERT_EQ(serializer2.total_, total);
  }
}

TEST_F(TestXmlBin, serialize_element_header)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_xml_document_1(ctx, doc);
  ASSERT_EQ(doc->size(), 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObXmlElement* elem = static_cast<ObXmlElement*>(doc->at(0));
  ObString prefix("prefix-string");

  {
    elem->set_prefix(prefix);
    ObStringBuffer buffer(&allocator);
    ObXmlElementSerializer serializer1(elem, &buffer);

    ASSERT_EQ(serializer1.child_count_, 2);
    ASSERT_EQ(serializer1.attr_count_, 1);
    ASSERT_EQ(serializer1.header_.header_size(), 5);
    ASSERT_EQ(serializer1.ele_header_.header_size(), 16);
    ASSERT_EQ(serializer1.key_entry_start_, 21);
    ASSERT_EQ(serializer1.key_entry_size_, 2);
    ASSERT_EQ(serializer1.value_entry_start_, 39);
    ASSERT_EQ(serializer1.value_entry_size_, 2);

    ASSERT_EQ(serializer1.serialize(0), 0);

    ObXmlElementSerializer serializer2(buffer.ptr(), buffer.length(), ctx);
    ObIMulModeBase* handle;
    ASSERT_EQ(serializer2.deserialize(handle), 0);
    ASSERT_EQ(serializer2.child_count_, 2);
    ASSERT_EQ(serializer2.attr_count_, 1);
    ASSERT_EQ(serializer2.header_.header_size(), 5);
    ASSERT_EQ(serializer2.ele_header_.header_size(), 16);

    ObXmlElement *res = static_cast<ObXmlElement*>(handle);
    ASSERT_EQ(res->get_prefix().length(), prefix.length());
    ASSERT_EQ(0, res->get_prefix().compare(prefix));
    ASSERT_EQ(res->attributes_size(), 1);
    ASSERT_EQ(res->size(), 2);
  }
}

TEST_F(TestXmlBin, serialize_document_header)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_xml_document_1(ctx, doc);
  ASSERT_EQ(doc->size(), 1);

  {
    ObStringBuffer buffer(&allocator);
    doc->set_standalone(1);
    ObXmlDocBinHeader doc_header1(doc->get_standalone(),
                                  doc->get_version(),
                                  doc->get_encoding(),
                                  doc->is_empty());
    ASSERT_EQ(doc_header1.serialize(buffer), 0);
    ASSERT_EQ(doc_header1.header_size(), buffer.length());
    ASSERT_EQ(doc_header1.is_standalone_, 1);
    ASSERT_EQ(doc_header1.is_version_, 1);
    ASSERT_EQ(doc_header1.is_encoding_, 1);

    ObXmlDocBinHeader doc_header2;
    ASSERT_EQ(doc_header2.deserialize(buffer.ptr(), buffer.length()), 0);

    ASSERT_EQ(doc_header2.is_standalone_, 1);
    ASSERT_EQ(doc_header2.is_version_, 1);
    ASSERT_EQ(doc_header2.is_encoding_, 1);
  }
}

TEST_F(TestXmlBin, serialize_text)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_xml_document_1(ctx, doc);
  ASSERT_EQ(doc->size(), 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObXmlElement* elem = static_cast<ObXmlElement*>(doc->at(0));
  elem = static_cast<ObXmlElement*>(elem->at(0));
  ASSERT_EQ(elem->get_key().compare("orderperson"), 0);

  {
    ObStringBuffer buffer(&allocator);
    ObXmlText* text = static_cast<ObXmlText*>(elem->at(0));
    ObString value;
    ASSERT_EQ(text->get_value(value), 0);
    ASSERT_EQ(value.compare("John Smith"), 0);

    ObXmlTextSerializer serializer1(text, buffer);
    ASSERT_EQ(serializer1.header_size(), 1);
    ASSERT_EQ(serializer1.serialize(), 0);

    ObXmlTextSerializer serializer2(buffer.ptr(), buffer.length(), ctx);
    ObIMulModeBase* handle;
    ASSERT_EQ(serializer2.deserialize(handle), 0);

    ObXmlText* tmp_text = static_cast<ObXmlText*>(handle);

    ObString tmp_value;
    ASSERT_EQ(tmp_text->get_value(tmp_value), 0);
    ASSERT_EQ(tmp_value.compare("John Smith"), 0);
  }
}

TEST_F(TestXmlBin, serialize_attribute)
{
  int ret = 0;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  get_xml_document_1(ctx, doc);
  ASSERT_EQ(doc->size(), 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObXmlElement* elem = static_cast<ObXmlElement*>(doc->at(0));

  ObXmlAttribute* attr = nullptr;

  ret = elem->get_attribute(attr, 0);
  ASSERT_EQ(ret, 0);

  ObString prefix("prefix-string");

  {
    attr->set_prefix(prefix);
    ObStringBuffer buffer(&allocator);

    ObXmlAttributeSerializer serializer1(attr, buffer);
    ASSERT_EQ(serializer1.header_.header_size(), 16);
    ASSERT_EQ(serializer1.serialize(), 0);

    ASSERT_EQ(serializer1.header_.is_prefix_, 1);
    ASSERT_EQ(serializer1.header_.prefix_len_, 13);
    ASSERT_EQ(serializer1.header_.prefix_len_size_, 1);
    ASSERT_EQ(buffer.length(), 16 + 1 + 6);


    ObXmlAttributeSerializer serializer2(buffer.ptr(), buffer.length(), ctx);
    ObIMulModeBase* handle;
    ASSERT_EQ(serializer2.deserialize(handle), 0);

    ObXmlAttribute* res = static_cast<ObXmlAttribute*>(handle);
    ASSERT_EQ(res->get_prefix().compare(prefix), 0);
    ASSERT_EQ(res->get_value().compare("889923"), 0);
  }
}

TEST_F(TestXmlBin, serialize_base)
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buffer(&allocator);

  ObXmlElementSerializer serializer(doc, &buffer);
  ret = serializer.serialize(0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObIMulModeBase* handle = nullptr;
  ObXmlElementSerializer deserializer(buffer.ptr(), buffer.length(), ctx);
  ret = deserializer.deserialize(handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  {
    ObXmlDocument* tmp_doc = static_cast<ObXmlDocument*>(handle);
    ObXmlElement* tmp_elem = static_cast<ObXmlElement*>(tmp_doc->at(0));
    ASSERT_EQ(tmp_doc->get_version().compare("1.0"), 0);
    ASSERT_EQ(tmp_doc->get_encoding().compare("UTF-8"), 0);

    ASSERT_EQ(tmp_elem->get_key().compare("shiporder"), 0);

    ObXmlAttribute* tmp_attr = nullptr;

    ret = tmp_elem->get_attribute(tmp_attr, 0);
    ASSERT_EQ(ret, 0);

    ASSERT_EQ(tmp_attr->get_key().compare("orderid"), 0);
    ASSERT_EQ(tmp_attr->get_value().compare("889923"), 0);

    ObXmlAttribute* tmp_elem1 = static_cast<ObXmlAttribute*>(tmp_elem->at(0));
    ASSERT_EQ(tmp_elem1->get_key().compare("orderperson"), 0);

    ObXmlAttribute* tmp_elem2 = static_cast<ObXmlAttribute*>(tmp_elem->at(1));
    ASSERT_EQ(tmp_elem2->get_key().compare("shipto"), 0);


  }
}




} // common
} // oceanbase


int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  /*
  system("rm -f test_xml_bin.log");
  OB_LOGGER.set_file_name("test_xml_bin.log");
  OB_LOGGER.set_log_level("INFO");
  */
  return RUN_ALL_TESTS();
}