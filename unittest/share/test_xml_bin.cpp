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
#include "lib/xml/ob_tree_base.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_path_parser.h"
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
    ASSERT_EQ(serializer1.ele_header_.header_size(), 15);
    cout << "index start = " << serializer1.index_start_
          << " type start = " << (int)serializer1.index_entry_size_ << endl;
    ASSERT_EQ(serializer1.index_start_, 20);
    ASSERT_EQ(serializer1.index_entry_size_, 1);
    ASSERT_EQ(serializer1.key_entry_start_, 23);
    ASSERT_EQ(serializer1.key_entry_size_, 2);
    ASSERT_EQ(serializer1.value_entry_start_, 35);
    ASSERT_EQ(serializer1.value_entry_size_, 2);
    ASSERT_EQ(serializer1.key_start_, 44);

    ASSERT_EQ(serializer1.serialize(0), 0);

    ObXmlElementSerializer serializer2(buffer.ptr(), buffer.length(), ctx);
    ObIMulModeBase* handle;
    ASSERT_EQ(serializer2.deserialize(handle), 0);
    ASSERT_EQ(serializer2.child_count_, 2);
    ASSERT_EQ(serializer2.attr_count_, 1);
    ASSERT_EQ(serializer2.header_.header_size(), 5);
    ASSERT_EQ(serializer2.ele_header_.header_size(), 15);

    ObXmlElement *res = static_cast<ObXmlElement*>(handle);
    ASSERT_EQ(res->get_prefix().length(), prefix.length());
    ASSERT_EQ(0, res->get_prefix().compare(prefix));
    ASSERT_EQ(res->attribute_size(), 1);
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
    ObXmlDocBinHeader doc_header1(doc->get_version(),
                                  doc->get_encoding(),
                                  doc->get_encoding_flag(),
                                  doc->get_standalone(),
                                  doc->has_xml_decl());
    ASSERT_EQ(doc_header1.serialize(buffer), 0);
    ASSERT_EQ(doc_header1.header_size(), buffer.length());
    ASSERT_EQ(doc_header1.is_version_, 1);
    ASSERT_EQ(doc_header1.is_encoding_, 1);

    ObXmlDocBinHeader doc_header2;
    ASSERT_EQ(doc_header2.deserialize(buffer.ptr(), buffer.length()), 0);

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

TEST_F(TestXmlBin, parse_meta)
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;

  ObStringBuffer buffer(&allocator);
  ObXmlElementSerializer serializer(tree, &buffer);

  ASSERT_EQ(serializer.serialize(0), OB_SUCCESS);

  ObXmlBinMetaParser meta_parser(buffer.ptr(), buffer.length());

  ASSERT_EQ(meta_parser.parser(), OB_SUCCESS);

  ASSERT_EQ(serializer.header_.entry_var_size_, meta_parser.value_entry_size_);
  ASSERT_EQ(serializer.header_.count_, meta_parser.count_);
  ASSERT_EQ(serializer.key_entry_start_, meta_parser.key_entry_);

}


TEST_F(TestXmlBin, set_at)
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);
  doc->set_standalone(2);

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);


  ObXmlBin rbin(ctx);
  ret = rbin.parse(bin.buffer_.ptr(), bin.buffer_.length());
  ASSERT_EQ(ret, OB_SUCCESS);

  ObString version = rbin.get_version();
  ObString encoding = rbin.get_encoding();

  ASSERT_EQ(std::string("1.0"), std::string(version.ptr(), version.length()));
  ASSERT_EQ(std::string("UTF-8"), std::string(encoding.ptr(), encoding.length()));
  ASSERT_EQ(2, rbin.get_standalone());
  ASSERT_EQ(0, rbin.get_is_empty());
  ASSERT_EQ(0, rbin.get_unparse());

  ret = rbin.set_at(0);
  ASSERT_EQ(ret, OB_SUCCESS);

  ObXmlBin bin_ele_entry = rbin;

  ObString ele_key;
  ASSERT_EQ(rbin.get_key(ele_key), 0);
  ASSERT_EQ(std::string("shiporder"), std::string(ele_key.ptr(), ele_key.length()));
  ASSERT_EQ(rbin.count(), 2);
  ASSERT_EQ(rbin.get_child_start(), 1);

  ObString key_str;
  ObString value_str;

  ret = rbin.set_at(0);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(rbin.get_key(key_str), OB_SUCCESS);
  ASSERT_EQ(rbin.get_value(value_str), OB_SUCCESS);

  ASSERT_EQ(std::string("orderid"), std::string(key_str.ptr(), key_str.length()));
  ASSERT_EQ(std::string("889923"), std::string(value_str.ptr(), value_str.length()));

  rbin = bin_ele_entry;

  ret = rbin.set_at(1);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(rbin.get_key(key_str), OB_SUCCESS);
  ASSERT_EQ(std::string("orderperson"), std::string(key_str.ptr(), key_str.length()));

  {
    ObXmlBin tmp(rbin);
    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_value(value_str), OB_SUCCESS);
    ASSERT_EQ(std::string("John Smith"), std::string(value_str.ptr(), value_str.length()));
  }


  ret = bin_ele_entry.set_at(2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(bin_ele_entry.get_key(key_str), OB_SUCCESS);
  ASSERT_EQ(std::string("shipto"), std::string(key_str.ptr(), key_str.length()));
  ASSERT_EQ(bin_ele_entry.size(), 4);
  ASSERT_EQ(bin_ele_entry.count(), 4);
  ASSERT_EQ(bin_ele_entry.get_child_start(), 0);

  {
    ObXmlBin tmp(bin_ele_entry);

    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_key(key_str), OB_SUCCESS);
    ASSERT_EQ(std::string("name"), std::string(key_str.ptr(), key_str.length()));

    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_value(value_str), OB_SUCCESS);
    ASSERT_EQ(std::string("Ola Nordmann"), std::string(value_str.ptr(), value_str.length()));


    tmp = bin_ele_entry;
    ret = tmp.set_at(1);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_key(key_str), OB_SUCCESS);
    ASSERT_EQ(std::string("address"), std::string(key_str.ptr(), key_str.length()));

    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_value(value_str), OB_SUCCESS);
    ASSERT_EQ(std::string("Langgt 23"), std::string(value_str.ptr(), value_str.length()));

    tmp = bin_ele_entry;
    ret = tmp.set_at(2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_key(key_str), OB_SUCCESS);
    ASSERT_EQ(std::string("city"), std::string(key_str.ptr(), key_str.length()));

    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_value(value_str), OB_SUCCESS);
    ASSERT_EQ(std::string("4000 Stavanger"), std::string(value_str.ptr(), value_str.length()));

    tmp = bin_ele_entry;
    ret = tmp.set_at(3);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_key(key_str), OB_SUCCESS);
    ASSERT_EQ(std::string("country"), std::string(key_str.ptr(), key_str.length()));

    ret = tmp.set_at(0);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(tmp.get_value(value_str), OB_SUCCESS);
    ASSERT_EQ(std::string("Norway"), std::string(value_str.ptr(), value_str.length()));
  }
}

TEST_F(TestXmlBin, iterator_base)
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

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStringBuffer buf_str(&allocator);

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  ObXmlBin rbin(ctx);
  ret = rbin.parse(bin.buffer_.ptr(), bin.buffer_.length());
  ASSERT_EQ(ret, OB_SUCCESS);

  ObXmlBin::iterator iter = rbin.begin();
  ObXmlBin::iterator end = rbin.end();

  ASSERT_EQ(iter.is_valid(), true);
  ASSERT_EQ(iter != end, true);
  ASSERT_EQ(iter <= end, true);
  ASSERT_EQ(iter < end, true);

  ObXmlBin* root_ele = *iter;
  ASSERT_EQ(root_ele->parse(), OB_SUCCESS);
  ASSERT_EQ(root_ele->size(), 2);

  {
    ObXmlBin* tmp = *iter;
    ASSERT_NE(nullptr, tmp);
    ASSERT_EQ(tmp->type(), M_ELEMENT);

    iter++;
    ASSERT_EQ(iter == end, true);
    ASSERT_EQ(iter.end(), true);
    ASSERT_EQ(iter < end, false);
  }

  ObString root_key;
  ASSERT_EQ(root_ele->get_key(root_key), OB_SUCCESS);
  ASSERT_EQ(std::string(root_key.ptr(), root_key.length()), std::string("shiporder"));

  ObXmlBin::iterator iter1 = root_ele->begin();
  ObXmlBin::iterator end1 = root_ele->end();
  {
    ASSERT_EQ(iter1.is_valid(), true);
    ASSERT_EQ(iter1.end(), false);
    ASSERT_EQ(iter1 < end1, true);
  }

  ASSERT_EQ(iter1 < end1, true);
  ObXmlBin* child1 = *iter1;
  ASSERT_NE(child1, nullptr);

  ASSERT_EQ(child1->type(), M_ATTRIBUTE);

  ObString attr_key1;
  ASSERT_EQ(child1->get_key(attr_key1), 0);
  ObString attr_val1;
  ASSERT_EQ(child1->get_value(attr_val1), 0);

  ASSERT_EQ(std::string(attr_key1.ptr(), attr_key1.length()), std::string("orderid"));
  ASSERT_EQ(std::string(attr_val1.ptr(), attr_val1.length()), std::string("889923"));

  ASSERT_EQ(iter1 != end1, true);
  ASSERT_EQ(iter1.is_valid(), true);
  ASSERT_EQ(iter1 < end1, true);

  ++iter1;
  ObXmlBin* child2 = *iter1;
  ASSERT_NE(child2, nullptr);

  ObString ele_key1;
  ASSERT_EQ(child2->get_key(ele_key1), 0);

  ASSERT_EQ(std::string(ele_key1.ptr(), ele_key1.length()), std::string("orderperson"));
  ASSERT_EQ(child2->type(), M_ELEMENT);
  ASSERT_EQ(child2->size(), 1);

  {
    ObXmlBin::iterator sub_iter1 = child2->begin();
    ObXmlBin::iterator sub_end1 = child2->end();

    ASSERT_EQ(sub_iter1 != sub_end1, true);
    ASSERT_EQ(sub_iter1.is_valid(), true);
    ASSERT_EQ(sub_iter1 < sub_end1, true);

    ObXmlBin* text1 = *sub_iter1;
    ASSERT_NE(text1, nullptr);

    ObString text_val;
    ASSERT_EQ(text1->get_value(text_val), OB_SUCCESS);
    ASSERT_EQ(text1->type(), M_TEXT);
    ASSERT_EQ(std::string(text_val.ptr(), text_val.length()), std::string("John Smith"));
  }

  ++iter1;
  ASSERT_EQ(iter1 != end1, true);
  ASSERT_EQ(iter1.is_valid(), true);
  ASSERT_EQ(iter1 < end1, true);

  ObXmlBin* child3 = *iter1;
  ASSERT_NE(child3, nullptr);

  ObString ele_key3;
  ASSERT_EQ(child3->get_key(ele_key3), 0);

  ASSERT_EQ(std::string(ele_key3.ptr(), ele_key3.length()), std::string("shipto"));
  ASSERT_EQ(child3->type(), M_ELEMENT);
  ASSERT_EQ(child3->size(), 4);

  {
    ObXmlBin::iterator sub_iter2 = child3->begin();
    ObXmlBin::iterator sub_end2 = child3->end();

    ASSERT_EQ(sub_iter2 != sub_end2, true);
    ASSERT_EQ(sub_iter2.is_valid(), true);
    ASSERT_EQ(sub_iter2 < sub_end2, true);

    ObXmlBin* sub_ele1 = *sub_iter2;
    ASSERT_NE(sub_ele1, nullptr);

    ObString ele_key1;
    ASSERT_EQ(sub_ele1->get_key(ele_key1), OB_SUCCESS);
    ASSERT_EQ(sub_ele1->type(), M_ELEMENT);
    ASSERT_EQ(std::string(ele_key1.ptr(), ele_key1.length()), std::string("name"));

    ObXmlBin::iterator tmp_iter = sub_ele1->begin();
    ObString text_val;
    ASSERT_EQ(tmp_iter->get_value(text_val), 0);
    ASSERT_EQ(std::string(text_val.ptr(), text_val.length()), std::string("Ola Nordmann"));

    sub_iter2++;
    ASSERT_EQ(sub_iter2 != sub_end2, true);
    ASSERT_EQ(sub_iter2.is_valid(), true);
    ASSERT_EQ(sub_iter2 < sub_end2, true);

    ObXmlBin* sub_ele2 = *sub_iter2;
    ASSERT_NE(sub_ele2, nullptr);

    ObString ele_key2;
    ASSERT_EQ(sub_ele2->get_key(ele_key2), OB_SUCCESS);
    ASSERT_EQ(sub_ele2->type(), M_ELEMENT);
    ASSERT_EQ(std::string(ele_key2.ptr(), ele_key2.length()), std::string("address"));

    ObXmlBin::iterator tmp_iter2 = sub_ele2->begin();
    ObString text_val2;
    ASSERT_EQ(tmp_iter2->get_value(text_val2), 0);
    ASSERT_EQ(std::string(text_val2.ptr(), text_val2.length()), std::string("Langgt 23"));

    sub_iter2++;
    ASSERT_EQ(sub_iter2 != sub_end2, true);
    ASSERT_EQ(sub_iter2.is_valid(), true);
    ASSERT_EQ(sub_iter2 < sub_end2, true);

    ObXmlBin* sub_ele3 = *sub_iter2;
    ASSERT_NE(sub_ele3, nullptr);

    ObString ele_key3;
    ASSERT_EQ(sub_ele3->get_key(ele_key3), OB_SUCCESS);
    ASSERT_EQ(sub_ele3->type(), M_ELEMENT);
    ASSERT_EQ(std::string(ele_key3.ptr(), ele_key3.length()), std::string("city"));

    ObXmlBin::iterator tmp_iter3 = sub_ele3->begin();
    ObString text_val3;
    ASSERT_EQ(tmp_iter3->get_value(text_val3), 0);
    ASSERT_EQ(std::string(text_val3.ptr(), text_val3.length()), std::string("4000 Stavanger"));

    sub_iter2++;
    ASSERT_EQ(sub_iter2 != sub_end2, true);
    ASSERT_EQ(sub_iter2.is_valid(), true);
    ASSERT_EQ(sub_iter2 < sub_end2, true);

    ObXmlBin* sub_ele4 = *sub_iter2;
    ASSERT_NE(sub_ele4, nullptr);

    ObString ele_key4;
    ASSERT_EQ(sub_ele4->get_key(ele_key4), OB_SUCCESS);
    ASSERT_EQ(sub_ele4->type(), M_ELEMENT);
    ASSERT_EQ(std::string(ele_key4.ptr(), ele_key4.length()), std::string("country"));

    ObXmlBin::iterator tmp_iter4 = sub_ele4->begin();
    ObString text_val4;
    ASSERT_EQ(tmp_iter4->get_value(text_val4), 0);
    ASSERT_EQ(std::string(text_val4.ptr(), text_val4.length()), std::string("Norway"));

    sub_iter2++;

    ASSERT_EQ(sub_iter2.end(), true);
    ASSERT_EQ(sub_iter2 <= sub_end2, true);
    ASSERT_EQ(sub_iter2 == sub_end2, true);
  }

}

TEST_F(TestXmlBin, reader)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObString key1("key1");
  ObString key2("key2");
  ObString key3("key3");
  ObString key4("key4");
  ObString key5("key5");

  ObString value1("value1");
  ObString value2("value2");
  ObString value3("value3");
  ObString value3_1("value3_1");
  ObString value3_2("value3_2");
  ObString value4("value4");
  ObString value5("value5");


  ObString element_key("element_key");
  ObString element_value("value");


  ObString sub_key1("sub_key1");
  ObString sub_key2("sub_key2");
  ObString sub_key3("sub_key3");

  ObString sub_value1("sub_value1");
  ObString sub_value2("sub_value2");
  ObString sub_value3("sub_value3");

  ObXmlElement sub1_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_1.set_xml_key(sub_key1);
  sub1_1.set_prefix(sub_value1);

  ObXmlElement sub1_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_2.set_xml_key(sub_key2);
  sub1_2.set_prefix(sub_value2);

  ObXmlElement sub1_3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_3.set_xml_key(sub_key3);
  sub1_3.set_prefix(sub_value3);



  ObXmlElement sub1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1.set_xml_key(key1);
  sub1.set_prefix(value1);

  // sub children
  ASSERT_EQ(sub1.add_element(&sub1_1), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_2), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_3), OB_SUCCESS);



  ObXmlElement sub2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub2.set_xml_key(key2);
  sub2.set_prefix(value2);

  ObXmlElement sub3_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_1.set_xml_key(key3);
  sub3_1.set_prefix(value3_1);

  ObXmlElement sub3_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_2.set_xml_key(key3);
  sub3_2.set_prefix(value3_2);

  ObXmlElement sub3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3.set_xml_key(key3);
  sub3.set_prefix(value3);

  ObXmlElement sub4(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub4.set_xml_key(key4);
  sub4.set_prefix(value4);

  ObXmlElement sub5(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5.set_xml_key(key5);
  sub5.set_prefix(value5);


  ObString key5_1("key5_1");
  ObString key5_2("key5_2");
  ObString key5_3("key5_3");

  ObString value5_1("value5_1");
  ObString value5_2("value5_2");
  ObString value5_3("value5_3");

  ObXmlElement sub5_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5_1.set_xml_key(key5_1);
  sub5_1.set_prefix(value5_1);

  ObXmlElement sub5_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5_2.set_xml_key(key5_2);
  sub5_2.set_prefix(value5_2);

  ObXmlElement sub5_3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5_3.set_xml_key(key5_3);
  sub5_3.set_prefix(value5_3);

  ASSERT_EQ(sub5.add_element(&sub5_1), OB_SUCCESS);
  ASSERT_EQ(sub5.add_element(&sub5_2), OB_SUCCESS);
  ASSERT_EQ(sub5.add_element(&sub5_3), OB_SUCCESS);


  ObXmlElement element(ObMulModeNodeType::M_DOCUMENT, ctx);
  element.set_xml_key(element_key);
  element.set_prefix(element_value);

  ASSERT_EQ(element.add_element(&sub1), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub2), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3_1), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3_2), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub4), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub5), OB_SUCCESS);

  ASSERT_EQ(element.size(), 7);
  ASSERT_EQ(sub1.size(), 3);

  {
    ObXmlBin xbin(ctx);
    ASSERT_EQ(xbin.parse_tree(&element), 0);

    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::ALL_KEY_TYPE;

    ObMulModeReader reader(&xbin, seek_info);

    ObIMulModeBase* node = nullptr;
    ObString key;

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key1"));


    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key2"));


    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key4"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key5"));

    ASSERT_EQ(reader.next(node), OB_ITER_END);
  }

  {
    ObXmlBin xbin(ctx);
    ASSERT_EQ(xbin.parse_tree(&element), 0);

    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::KEY_TYPE;
    seek_info.key_ = key3;

    ObMulModeReader reader(&xbin, seek_info);
    ObIMulModeBase* node = nullptr;
    ObString key;
    ObString prefix;

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value3"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value3_1"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key3"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value3_2"));

    ASSERT_EQ(reader.next(node), OB_ITER_END);
  }

  /**
   * element_key | key1 | sub_key1 ("sub_value1");
   *             |      | sub_key2 ("sub_value2");
   *             |      | sub_key3 ("sub_value3");
   *             | key2 ("value2")
   *             | key3 ("sub_value1")
   *             | key3 ("sub_value1")
   *             | key3 ("sub_value1")
   *             | key4 ("value4")
   *             | key5 | key5_1 ("value5_1")
   *             | key5 | key5_2 ("value5_2")
   *             | key5 | key5_3 ("value5_3")
   *
  */

  {
    ObXmlBin xbin(ctx);
    ASSERT_EQ(xbin.parse_tree(&element), 0);

    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::KEY_TYPE;
    seek_info.key_ = key1;

    ObMulModeReader reader(&xbin, seek_info);
    ObIMulModeBase* node = nullptr;
    ObString key;
    ObString prefix;

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key1"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value1"));

    {
      ObPathSeekInfo seek_info;
      seek_info.type_ = SimpleSeekType::ALL_KEY_TYPE;
      seek_info.key_ = ObString("");

      ObMulModeReader reader(node, seek_info);
      ObString key;
      ObString prefix;

      ASSERT_EQ(reader.next(node), OB_SUCCESS);
      ASSERT_EQ(node->get_key(key), OB_SUCCESS);
      ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("sub_key1"));
      prefix = node->get_prefix();
      ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("sub_value1"));

      ASSERT_EQ(reader.next(node), OB_SUCCESS);
      ASSERT_EQ(node->get_key(key), OB_SUCCESS);
      ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("sub_key2"));
      prefix = node->get_prefix();
      ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("sub_value2"));

      ASSERT_EQ(reader.next(node), OB_SUCCESS);
      ASSERT_EQ(node->get_key(key), OB_SUCCESS);
      ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("sub_key3"));
      prefix = node->get_prefix();
      ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("sub_value3"));

      ASSERT_EQ(reader.next(node), OB_ITER_END);
    }

  }
}



TEST_F(TestXmlBin, test_simple_print_document)
{
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
  "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
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
  "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.print_document(buf_str, type, ObXmlFormatType::NO_FORMAT);

  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}

TEST_F(TestXmlBin, test_simple_print_document_with_pretty)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.print_document(buf_str, type, ObXmlFormatType::WITH_FORMAT, 2);

  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));
}

TEST_F(TestXmlBin, test_print_content_with_pretty)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = content;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.print_content(buf_str, false, false, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));
  buf_str.reuse();
  bin.print_content(buf_str, true, false, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text_with_encoding.ptr(), serialize_text_with_encoding.length()), std::string(buf_str.ptr(), buf_str.length()));
  buf_str.reuse();
  bin.print_content(buf_str, false, true, ObXmlFormatType::WITH_FORMAT, param_list);
  ASSERT_EQ(std::string(serialize_text_with_version.ptr(), serialize_text_with_version.length()), std::string(buf_str.ptr(), buf_str.length()));
}


TEST_F(TestXmlBin, test_print_content)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = content;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.print_content(buf_str, false, false, false, param_list);
  ASSERT_EQ(std::string(ser_text.ptr(), ser_text.length()), std::string(buf_str.ptr(), buf_str.length()));
}


TEST_F(TestXmlBin, test_print_xml_node)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.at(0)->print(buf_str, false, false);

  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}

TEST_F(TestXmlBin, test_print_xml_node_with_pretty)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.at(0)->print(buf_str, ObXmlFormatType::WITH_FORMAT, 0, 3);
  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}

TEST_F(TestXmlBin, test_simple_print_document_mysqltest)
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

  ObXmlBin bin(ctx);
  ObIMulModeBase* tree = doc;
  ASSERT_EQ(bin.parse_tree(tree), OB_SUCCESS);

  bin.print_document(buf_str, type, false);

  ASSERT_EQ(std::string(serialize_text.ptr(), serialize_text.length()), std::string(buf_str.ptr(), buf_str.length()));

}



TEST_F(TestXmlBin, test_print_ns)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
    "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
      "<xsl:template match=\"/\">"
        "<note>test</note>"
      "</xsl:template>"
    "</xsl:stylesheet>");

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStringBuffer buf_str(&allocator);
  ObXmlBin bin(ctx);
  ret = doc->print_document(buf_str, type, ObXmlFormatType::NO_FORMAT);
  ASSERT_EQ(ret, 0);


  ObStringBuffer buf_str_bin(&allocator);
  ASSERT_EQ(bin.parse_tree(doc), 0);
  ASSERT_EQ(bin.print_document(buf_str_bin, type, ObXmlFormatType::NO_FORMAT), 0);

  ASSERT_EQ(std::string(buf_str.ptr(), buf_str.length()), std::string(buf_str_bin.ptr(), buf_str_bin.length()));

}

TEST_F(TestXmlBin, test_print_unparse)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text("<TEE Tee_ATT=\"1986-04-02\">1996-02-09T23:08:10.296000</TEE>");

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;

  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObIMulModeBase *xml_base = nullptr;
  ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx,
                                          xml_text,
                                          ObNodeMemType::TREE_TYPE,
                                          ObNodeMemType::BINARY_TYPE,
                                          xml_base,
                                          M_UNPARSED),   0);



  ObStringBuffer buf_str(&allocator);
  ObXmlBin bin(ctx);

  ObString bin_str;
  ASSERT_EQ(xml_base->get_raw_binary(bin_str, ctx->allocator_), 0);

  ASSERT_EQ(bin.parse(bin_str.ptr(), bin_str.length()), 0);

  ObStringBuffer buf_str_bin(&allocator);

  ASSERT_EQ(bin.print_unparsed(buf_str_bin, type, ObXmlFormatType::NO_FORMAT), 0);

  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(buf_str_bin.ptr(), buf_str_bin.length()));

}


TEST_F(TestXmlBin, test_print_document)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>"
                             "<note ls=\"根\">"
                             "<to>子元素</to>"
                             "<from>Jani</from>"
                             "<heading>Reminder</heading>"
                             "<![CDATA[<greeting>CDATA区</greeting>]]>"
                             "<body>Don''t forget me this weekend &amp;</body>"
                             "<!-- 这是注释 -->"
                             "<h:table xmlns:h=\"
                             "<h:td>Bananas</h:td>"
                             "</h:table>"
                             "<f:table xmlns:f=\"
                             "<f:name>African Coffee Table</f:name>"
                             "<f:width>80</f:width>"
                             "</f:table>"
                             "</note>");

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;

  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObIMulModeBase *xml_base = nullptr;
  ASSERT_EQ(ObMulModeFactory::get_xml_base(ctx,
                                          xml_text,
                                          ObNodeMemType::TREE_TYPE,
                                          ObNodeMemType::BINARY_TYPE,
                                          xml_base,
                                          M_UNPARSED),   0);



  ObStringBuffer buf_str(&allocator);
  ObXmlBin bin(ctx);

  ObString bin_str;
  ASSERT_EQ(xml_base->get_raw_binary(bin_str, ctx->allocator_), 0);

  ASSERT_EQ(bin.parse(bin_str.ptr(), bin_str.length()), 0);

  ObStringBuffer buf_str_bin(&allocator);

  ASSERT_EQ(bin.print_document(buf_str_bin, type, ObXmlFormatType::NO_FORMAT), 0);

  ASSERT_EQ(std::string(xml_text.ptr(), xml_text.length()), std::string(buf_str_bin.ptr(), buf_str_bin.length()));

}

TEST_F(TestXmlBin, read_by_key)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
    "<?xml version=\"1.0\"?><a age=\"18\"><b><c>你好</c></b></a>"
  );

  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);

  {
    ObXmlBin xbin(ctx);
    ObArray<ObIMulModeBase*> result1;
    ASSERT_EQ(xbin.parse_tree(doc), 0);
    ASSERT_EQ(xbin.set_child_at(0), 0);

    std::string tmp_str(xbin.meta_.key_ptr_, xbin.meta_.key_len_);
    cout << "key = " << tmp_str
          << " attr size = " << xbin.attribute_size()
          << " child size =  " << xbin.size()
          << endl;

    ASSERT_EQ(xbin.get_children("b", result1, nullptr), 0);
  }

}

# define NS_TEST_COUNT 3
ObString ns_key[NS_TEST_COUNT] = {"", "f", "h"};
ObString ns_value[NS_TEST_COUNT] = {"ns1", "ns2", "ns3"};
TEST_F(TestXmlBin, test_add_extend)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
    "<a xmlns=\"ns1\" xmlns:f=\"ns2\" xmlns:h=\"ns3\">"
      "<f:b b1=\"b1\" b2=\"b2\">"
        "<c>"
          "<h:d>"
            "<f:e></f:e>"
          "</h:d>"
        "</c>"
      "</f:b>"
      "<h:b1>"
      "</h:b1>"
    "</a>");
  ObString xml_text_entend("<f:b xmlns:f=\"ns2\" b1=\"b1\" b2=\"b2\"><c xmlns=\"ns1\"><h:d xmlns:h=\"ns3\"><f:e/></h:d></c></f:b>");

  // parse xml text
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ObXmlBin xbin(ctx);
  ASSERT_EQ(xbin.parse_tree(doc), 0);

  // seek
  ObString str0 = "/a/f:b";
  ObString str1 = "/ns1:a/ns2:b";
  ObString default_ns(ns_value[0]);
  ObPathVarObject pass(allocator);
  for (int i = 1; i < NS_TEST_COUNT && OB_SUCC(ret); ++i) {
    ObDatum* data = static_cast<ObDatum*>(allocator.alloc(sizeof(ObDatum)));
    data = new(data) ObDatum();
    data->set_string(ns_value[i]); // default ns value
    ASSERT_EQ(true, OB_NOT_NULL(data));
    ret = pass.add(ns_key[i], data);
  }
  ObJsonBuffer buf(&allocator);
  ObPathExprIter pathiter_bin(&allocator);
  pathiter_bin.init(ctx, str0, default_ns, &xbin, &pass, false);
  ret = pathiter_bin.open();
  ret = pathiter_bin.path_node_->node_to_string(buf);
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  buf.reset();
  int idx = 0;
  ObIMulModeBase* res;
  ret = pathiter_bin.get_next_node(res);
  ASSERT_EQ(OB_SUCCESS, ret);
  res->print(buf, true);

  ObXmlBin* bin_res = static_cast<ObXmlBin*>(res);

  // ns_element
  // element
  ObXmlElement element_ns(ObMulModeNodeType::M_ELEMENT, ctx);
  element_ns.init();
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, element_ns.type());
  for (int i = 0; i < NS_TEST_COUNT && OB_SUCC(ret); ++i) {
    ObXmlAttribute* ns1 = static_cast<ObXmlAttribute*>(allocator.alloc(sizeof(ObXmlAttribute)));
    ns1 = new(ns1) ObXmlAttribute(ObMulModeNodeType::M_NAMESPACE, ctx);
    ASSERT_EQ(true, OB_NOT_NULL(ns1));
    if (i != 0 ) {
      ns1->set_xml_key(ns_key[i]);
    } else {
      ns1->set_xml_key("xmlns");
    }
    ns1->set_value(ns_value[i]);
    ASSERT_EQ(OB_SUCCESS, element_ns.add_attribute(ns1));
  }

  ASSERT_EQ(OB_SUCCESS, bin_res->append_extend(&element_ns));
  ASSERT_EQ(true, bin_res->check_extend());
  ASSERT_EQ(true, res->check_extend());
  buf.reset();
  bin_res->print(buf, true);
  std::cout<<"extend str :"<<buf.ptr()<<std::endl;
  ObString extend_res(buf.ptr());
  std::cout<<"extend res :"<<buf.ptr()<<std::endl;
  ASSERT_EQ(extend_res, xml_text_entend);
}

TEST_F(TestXmlBin, test_merge_extend)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
    "<a xmlns=\"ns1\" xmlns:f=\"ns2\" xmlns:h=\"ns3\">"
      "<f:b b1=\"b1\" b2=\"b2\">"
        "<c>"
          "<h:d>"
            "<f:e></f:e>"
          "</h:d>"
        "</c>"
      "</f:b>"
      "<h:b1>"
      "</h:b1>"
    "</a>");
  ObString xml_text_entend("<f:b xmlns:f=\"ns2\" b1=\"b1\" b2=\"b2\"><c xmlns=\"ns1\"><h:d xmlns:h=\"ns3\"><f:e/></h:d></c></f:b>");

  // parse xml text
  ObArenaAllocator allocator(ObModIds::TEST);
  ObXmlDocument* doc = nullptr;
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ret = ObXmlParserUtils::parse_document_text(ctx, xml_text, doc);
  ObXmlBin xbin(ctx);
  ASSERT_EQ(xbin.parse_tree(doc), 0);

  // seek
  ObString str0 = "/a/f:b";
  ObString str1 = "/ns1:a/ns2:b";
  ObString default_ns(ns_value[0]);
  ObPathVarObject pass(allocator);
  for (int i = 1; i < NS_TEST_COUNT && OB_SUCC(ret); ++i) {
    ObDatum* data = static_cast<ObDatum*>(allocator.alloc(sizeof(ObDatum)));
    data = new(data) ObDatum();
    data->set_string(ns_value[i]); // default ns value
    ASSERT_EQ(true, OB_NOT_NULL(data));
    ret = pass.add(ns_key[i], data);
  }
  ObJsonBuffer buf(&allocator);
  ObPathExprIter pathiter_bin(&allocator);
  pathiter_bin.init(ctx, str0, default_ns, &xbin, &pass, false);
  ret = pathiter_bin.open();
  ret = pathiter_bin.path_node_->node_to_string(buf);
  ObString str2(buf.ptr());
  ASSERT_EQ(str1, str2);
  buf.reset();
  int idx = 0;
  ObIMulModeBase* res;
  ret = pathiter_bin.get_next_node(res);
  ASSERT_EQ(OB_SUCCESS, ret);
  res->print(buf, true);

  ObXmlBin* bin_res = static_cast<ObXmlBin*>(res);

  // ns_element
  // element
  ObXmlElement element_ns(ObMulModeNodeType::M_ELEMENT, ctx);
  element_ns.init();
  ASSERT_EQ(ObMulModeNodeType::M_ELEMENT, element_ns.type());
  for (int i = 0; i < NS_TEST_COUNT && OB_SUCC(ret); ++i) {
    ObXmlAttribute* ns1 = static_cast<ObXmlAttribute*>(allocator.alloc(sizeof(ObXmlAttribute)));
    ns1 = new(ns1) ObXmlAttribute(ObMulModeNodeType::M_NAMESPACE, ctx);
    ASSERT_EQ(true, OB_NOT_NULL(ns1));
    if (i != 0 ) {
      ns1->set_xml_key(ns_key[i]);
    } else {
      ns1->set_xml_key("xmlns");
    }
    ns1->set_value(ns_value[i]);
    ASSERT_EQ(OB_SUCCESS, element_ns.add_attribute(ns1));
  }

  ASSERT_EQ(OB_SUCCESS, bin_res->append_extend(&element_ns));
  ASSERT_EQ(true, bin_res->check_extend());
  ASSERT_EQ(true, res->check_extend());
  buf.reset();
  bin_res->print(buf, true);
  ObString exptend_res(buf.ptr());
  ASSERT_EQ(exptend_res, xml_text_entend);

  ObXmlBin bin_merge(ctx);
  ASSERT_EQ(OB_SUCCESS, bin_res->merge_extend(bin_merge));
  buf.reset();
  ret = bin_merge.print(buf, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString merge_res(buf.ptr());
  std::cout<<"extend res:"<<merge_res.ptr()<<std::endl;
  std::cout<<"extend str:"<<xml_text_entend.ptr()<<std::endl;
  ASSERT_EQ(merge_res, xml_text_entend);
}

TEST_F(TestXmlBin, print_empty_element)
{
  int ret = 0;
  ObCollationType type = CS_TYPE_UTF8MB4_GENERAL_CI;
  common::ObString xml_text(
    "<></>"
  );

  ObArenaAllocator allocator(ObModIds::TEST);

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  ObXmlDocument root(M_UNPARSED, ctx);
  ObXmlElement child1(M_UNPARSED, ctx);

  ASSERT_EQ(root.add_element(&child1), 0);
  ObXmlDocument* doc = &root;

  {
    ObXmlBin xbin(ctx);
    ObArray<ObIMulModeBase*> result1;
    ASSERT_EQ(xbin.parse_tree(doc), 0);
    ASSERT_EQ(xbin.set_child_at(0), 0);

    ObStringBuffer buf_str_bin(&allocator);
    ASSERT_EQ(xbin.print_xml(buf_str_bin, 0, 0 ,0), 0);
    cout << buf_str_bin.ptr() << endl;
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