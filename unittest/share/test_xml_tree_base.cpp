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
#define protected public
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_tree_base.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/timezone/ob_timezone_info.h"
#undef private

#include <sys/time.h>
using namespace std;
namespace oceanbase {
namespace common {

class TestXmlTreeBase : public ::testing::Test {
public:
  TestXmlTreeBase()
  {}
  ~TestXmlTreeBase()
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
  DISALLOW_COPY_AND_ASSIGN(TestXmlTreeBase);
};

class TXmlNodeBase : public ObIMulModeBase , public ObLibContainerNode {
public:
  TXmlNodeBase(ObMulModeMemCtx* mem_ctx)
    : ObIMulModeBase(TREE_TYPE, OB_XML_TYPE, mem_ctx->allocator_),
      ObLibContainerNode(OB_XML_TYPE, mem_ctx),
      name_(),
      value_()
  {
  }

  ObMulModeNodeType type() const { return M_NULL; }

  int get_before(ObIArray<ObIMulModeBase*>& nodes, ObMulModeFilter* filter = nullptr) { return 0; }
  int get_after(ObIArray<ObIMulModeBase*>& nodes, ObMulModeFilter* filter = nullptr) { return 0; }
  int get_descendant(ObIArray<ObIMulModeBase*>& nodes, scan_type type, ObMulModeFilter* filter = nullptr) { return 0; }

  TXmlNodeBase(ObMulModeMemCtx* mem_ctx, const ObString& name, const ObString& value)
    : ObIMulModeBase(TREE_TYPE, OB_XML_TYPE, mem_ctx->allocator_),
      ObLibContainerNode(OB_XML_TYPE, mem_ctx),
      name_(name),
      value_(value)
  {
  }


  TXmlNodeBase(const ObString& name, const ObString& value)
  : ObIMulModeBase(TREE_TYPE, OB_XML_TYPE),
    ObLibContainerNode(OB_XML_TYPE),
    name_(name),
    value_(value)
  {
  }

  virtual ~TXmlNodeBase() {}

  int64_t size() { return ObLibContainerNode::size(); }
  int64_t count() { return size(); }

  const common::ObString& get_key()
  {
    return name_;
  }

  int get_key(ObString& res, int64_t index = -1)  override
  {
    INIT_SUCC(ret);

    res = name_;
    return ret;
  }

  int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value)
  {
    return 0;
  }

  int get_ns_value(const ObString& prefix, ObString& ns_value) {
    return 0;
  }

  bool is_equal_node(const ObIMulModeBase* other) {
    return false;
  }

  bool is_node_before(const ObIMulModeBase* other) {
    return false;
  }

  int get_value(ObString& value, int64_t index = -1)  override
  {
    INIT_SUCC(ret);

    value = value_;
    return ret;
  }

  int get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags = 0) {
    return 0;
  }

  void set_standalone(uint16_t standalone) {  }

  // 用于确定key是否匹配
  int compare(const ObString& key, int& res) {
    UNUSED(key);
    res = 0;
    return 0;
  }

  virtual int64_t attribute_size() { return 0; }
  virtual int64_t attribute_count() { return 0; }
  ObString get_version() { return ObString(); }
  ObString get_prefix() { return ObString(); }
  ObString get_encoding() { return ObString(); }
  uint16_t get_standalone() { return 0; }
  ObIMulModeBase* attribute_at(int64_t pos, ObIMulModeBase* buffer = nullptr) { return nullptr; }
  bool has_flags(ObMulModeNodeFlag flag) { return false; }
  bool get_unparse() { return false; }
  bool get_is_empty() { return false; }
  // 返回节点具体类型
  // 例如：json返回jsonInt，jsonDouble
  // xml 返回xmlElment, XmlAttribute
  int node_type() const { return 0; }

  // @return see ObObjType.
  // 用于对应该数据的原始sql类型，当前是json在使用
  ObObjType field_type() const { return ObNullType;}

  virtual int append(ObIMulModeBase* node)
  {
    return ObLibContainerNode::append(static_cast<TXmlNodeBase*>(node));
  }

  virtual int insert(int64_t pos, ObIMulModeBase* node)
  {
    return ObLibContainerNode::insert(pos, static_cast<TXmlNodeBase*>(node));
  }

  virtual int get_node_count(ObMulModeNodeType node_type, int &count)
  {
    return 0;
  }

  ObMulModeNodeType type() { return M_ELEMENT; }

  virtual int remove(int64_t pos)
  {
    return ObLibContainerNode::remove(pos);
  }

  virtual int remove(ObIMulModeBase* node)
  {
     return ObLibContainerNode::remove(static_cast<TXmlNodeBase*>(node));
  }

  virtual int get_raw_binary(common::ObString &out, ObIAllocator *allocator) {
    return 0;
  }

  int get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2 = ObString()) {
    return 0;
  }

  int get_value(ObIMulModeBase*& value, int64_t index = -1)
  {
    return 0;
  }

  int get_range(int64_t start, int64_t end, ObIArray<ObIMulModeBase*> &res)
  {
    INIT_SUCC(ret);
    ObArray<ObLibTreeNodeBase*> tmp_array;
    if (OB_FAIL(ObLibContainerNode::get_range(start, end, tmp_array))) {
    } else if (OB_FAIL(res.reserve(tmp_array.count()))) {
    } else {
      for (size_t i = 0; i < tmp_array.count(); ++i) {
        ObLibTreeNodeBase* tmp_tree_node = tmp_array.at(i);
        TXmlNodeBase* xnode = static_cast<TXmlNodeBase*>(tmp_tree_node);
        if (OB_FAIL(res.push_back(xnode))) {
        }
      }
    }

    return ret;
  }

  virtual int get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr)
  {
    return get_range(-1, static_cast<uint32_t>(-1), res);
  }

  virtual int get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res, ObMulModeFilter* filter = nullptr)
  {
    INIT_SUCC(ret);
    ObArray<ObLibTreeNodeBase*> tmp_array;
    if (OB_FAIL(ObLibContainerNode::get_children(key, tmp_array))) {
    } else if (OB_FAIL(res.reserve(tmp_array.count()))) {
    } else {
      for (size_t i = 0; i < tmp_array.count(); ++i) {
        ObLibTreeNodeBase* tmp_tree_node = tmp_array.at(i);
        TXmlNodeBase* xnode = static_cast<TXmlNodeBase*>(tmp_tree_node);
        if (OB_FAIL(res.push_back(xnode))) {
        }
      }
    }
    return ret;
  }

  virtual ObIMulModeBase* at(int64_t pos, ObIMulModeBase* buffer = nullptr)
  {
    ObLibTreeNodeBase* tmp = ObLibContainerNode::member(pos);
    TXmlNodeBase* res = nullptr;

    if (OB_NOT_NULL(tmp)) {
      res = static_cast<TXmlNodeBase*>(tmp);
    }
    return res;
  }

  int print(ObJsonBuffer& j_buf, uint32_t format_flag, uint64_t depth = 0, uint64_t size = 0)
  {
    return 0;
  }

  virtual int update(int64_t pos, ObIMulModeBase* new_node)
  {
    return ObLibContainerNode::update(pos, static_cast<TXmlNodeBase*>(new_node));
  }

  virtual int update(ObIMulModeBase* old_node, ObIMulModeBase* new_node)
  {
    return ObLibContainerNode::update(static_cast<TXmlNodeBase*>(old_node), static_cast<TXmlNodeBase*>(new_node));
  }

  int compare(const ObIMulModeBase &other, int &res) {
    return 0;
  }

  ObString name_;
  ObString value_;
};

class XmlElement : public TXmlNodeBase {
public:
  XmlElement(ObMulModeMemCtx* mem_ctx, const ObString& name, const ObString& value)
    : TXmlNodeBase(mem_ctx, name, value),
      attributes_(mem_ctx)
  {
  }

  ~XmlElement() {}

  const ObString& get_key();

  int add_attributes(TXmlNodeBase* node);
  int add_children(TXmlNodeBase* node);

  TXmlNodeBase attributes_;
};

class XmlText : public TXmlNodeBase {
public:
  XmlText(const ObString& name, const ObString& value)
    : TXmlNodeBase(name, value)
  {
  }

  const ObString& get_key();
};

TEST_F(TestXmlTreeBase, append)
{
  ObArenaAllocator allocator(ObModIds::TEST);

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

  ObMulModeMemCtx* mem_ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx), OB_SUCCESS);

  XmlElement element_node(mem_ctx, element_key, element_value);

  XmlText xml_text1(key1, value1);
  XmlText xml_text2(key2, value2);
  XmlText xml_text3(key3, value3);
  XmlText xml_text3_1(key3, value3_1);
  XmlText xml_text3_2(key3, value3_2);
  XmlText xml_text4(key4, value4);
  XmlText xml_text5(key5, value5);

  int ret = OB_SUCCESS;

  ret = element_node.append(&xml_text5);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text3_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text3_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(7, element_node.size());

  ObArray<ObIMulModeBase*> res;
  ret = element_node.get_children(key3, res);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (ObArray<ObIMulModeBase*>::iterator iter = res.begin();
       iter != res.end();
       iter++) {
    XmlText* p_xml_text = static_cast<XmlText*>(*iter);
    cout << "name: "  << p_xml_text->name_.ptr_
          << ", " << p_xml_text->value_.ptr_ << endl;
  }

  // for (ObLibTreeNodeVector::iterator iter = element_node.sorted_children_.begin();
  //      iter != element_node.sorted_children_.end();
  //      iter++) {
  //   XmlText* p_xml_text = static_cast<XmlText*>(*iter);
  //   cout << "name: "  << p_xml_text->name_.ptr_
  //         << ", " << p_xml_text->value_.ptr_ << endl;
  // }
}

TEST_F(TestXmlTreeBase, remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObString key1("key1");
  ObString key2("key2");
  ObString key3("key3");
  ObString key4("key4");
  ObString key5("key5");

  ObString value1("value1");
  ObString value2("value2");
  ObString value3("value3");
  ObString value4("value4");
  ObString value5("value5");


  ObString element_key("element_key");
  ObString element_value("value");

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  XmlElement element_node(ctx, element_key, element_value);

  XmlText xml_text1(key1, value1);
  XmlText xml_text2(key2, value2);
  XmlText xml_text3(key3, value3);
  XmlText xml_text4(key4, value4);
  XmlText xml_text5(key5, value5);

  int ret = OB_SUCCESS;

  ret = element_node.append(&xml_text5);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.append(&xml_text1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // remove node
  ret = element_node.remove(2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(4, element_node.size());

  XmlText* tmp = nullptr;

  // check deleted element is target
  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(2));
  ASSERT_STREQ(tmp->name_.ptr_, key4.ptr_);

  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(1));
  ASSERT_STREQ(tmp->name_.ptr_, key2.ptr_);


  //  for (ObLibTreeNodeVector::iterator iter = element_node.sorted_children_.begin();
  //       iter != element_node.sorted_children_.end();
  //       iter++) {
  //    XmlText* p_xml_text = static_cast<XmlText*>(*iter);
  //    cout << "name: "  << p_xml_text->name_.ptr_
  //          << ", " << p_xml_text->value_.ptr_ << endl;
  //  }

  ret = element_node.append(&xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, element_node.size());

  // remove node
  ret = element_node.remove(&xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(4, element_node.size());

  // check deleted element is target
  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(2));
  ASSERT_STREQ(tmp->name_.ptr_, key4.ptr_);

  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(1));
  ASSERT_STREQ(tmp->name_.ptr_, key2.ptr_);

  // for (ObLibTreeNodeVector::iterator iter = element_node.sorted_children_.begin();
  //      iter != element_node.sorted_children_.end();
  //      iter++) {
  //   XmlText* p_xml_text = static_cast<XmlText*>(*iter);
  //   cout << "name: "  << p_xml_text->name_.ptr_
  //         << ", " << p_xml_text->value_.ptr_ << endl;
  // }
}

TEST_F(TestXmlTreeBase, insert_update)
{
  ObArenaAllocator allocator(ObModIds::TEST);

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

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  XmlElement element_node(ctx, element_key, element_value);

  XmlText xml_text1(key1, value1);
  XmlText xml_text2(key2, value2);
  XmlText xml_text3(key3, value3);
  XmlText xml_text3_1(key3, value3_1);
  XmlText xml_text3_2(key3, value3_2);
  XmlText xml_text4(key4, value4);
  XmlText xml_text5(key5, value5);

  int ret = OB_SUCCESS;
  ret = element_node.insert(0, &xml_text1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.insert(0, &xml_text2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.insert(0, &xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.insert(0, &xml_text4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.insert(0, &xml_text5);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = element_node.update(&xml_text3, &xml_text3_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  XmlText* tmp = nullptr;

  // check deleted element is target
  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(2));
  ASSERT_STREQ(tmp->name_.ptr_, key3.ptr_);
  ASSERT_STREQ(tmp->value_.ptr_, value3_1.ptr_);


  // for (ObLibTreeNodeVector::iterator iter = element_node.sorted_children_.begin();
  //      iter != element_node.sorted_children_.end();
  //      iter++) {
  //   XmlText* p_xml_text = static_cast<XmlText*>(*iter);
  //   cout << "name: "  << p_xml_text->name_.ptr_
  //         << ", " << p_xml_text->value_.ptr_ << endl;
  // }
  //
  // cout << "----------------------------------------------------" << endl;
  //
  // for (ObLibTreeNodeVector::iterator iter = element_node.children_.begin();
  //      iter != element_node.children_.end();
  //      iter++) {
  //   XmlText* p_xml_text = static_cast<XmlText*>(*iter);
  //   cout << "name: "  << p_xml_text->name_.ptr_
  //         << ", " << p_xml_text->value_.ptr_ << endl;
  // }

  ret = element_node.update(2, &xml_text3_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(2));
  ASSERT_STREQ(tmp->name_.ptr_, key3.ptr_);
  ASSERT_STREQ(tmp->value_.ptr_, value3_2.ptr_);

}


TEST_F(TestXmlTreeBase, insert_brother)
{
  ObArenaAllocator allocator(ObModIds::TEST);

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
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  XmlElement element_node(ctx, element_key, element_value);

  XmlText xml_text1(key1, value1);
  XmlText xml_text2(key2, value2);
  XmlText xml_text3(key3, value3);
  XmlText xml_text3_1(key3, value3_1);
  XmlText xml_text3_2(key3, value3_2);
  XmlText xml_text4(key4, value4);
  XmlText xml_text5(key5, value5);

  int ret = OB_SUCCESS;
  ret = element_node.insert(0, &xml_text1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = xml_text1.insert_prev(&xml_text2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = xml_text2.insert_prev(&xml_text3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = xml_text3.insert_prev(&xml_text4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = xml_text4.insert_prev(&xml_text5);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (ObLibTreeNodeVector::iterator iter = element_node.sorted_children_->begin();
       iter != element_node.sorted_children_->end();
       iter++) {
    XmlText* p_xml_text = static_cast<XmlText*>(*iter);
    cout << "name: "  << p_xml_text->name_.ptr_
          << ", " << p_xml_text->value_.ptr_ << endl;
  }

  cout << "----------------------------------------------------" << endl;

  for (ObLibTreeNodeVector::iterator iter = element_node.children_->begin();
       iter != element_node.children_->end();
       iter++) {
    XmlText* p_xml_text = static_cast<XmlText*>(*iter);
    cout << "name: "  << p_xml_text->name_.ptr_
          << ", " << p_xml_text->value_.ptr_ << endl;
  }

  XmlText* tmp = nullptr;

  // check deleted element is target
  tmp = static_cast<XmlText*>(element_node.sorted_children_->at(2));
  ASSERT_STREQ(tmp->name_.ptr_, key3.ptr_);
  ASSERT_STREQ(tmp->value_.ptr_, value3.ptr_);

}


class IntContainer : public ObLibContainerNode {
public:
  IntContainer(ObMulModeMemCtx* mem_ctx, int64_t value)
    : ObLibContainerNode(OB_XML_TYPE, MEMBER_SEQUENT_FLAG,  mem_ctx),
      is_container_(true),
      value_(value) {}

  IntContainer(int64_t value)
    : ObLibContainerNode(OB_XML_TYPE),
      is_container_(0),
      value_(value) {}

  int is_container_;
  int64_t value_;
};

TEST_F(TestXmlTreeBase, tree_iterator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  int ret = OB_SUCCESS;
  ObLibContainerNode* tmp = nullptr;

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);
  IntContainer root(ctx, 123456789);
  // just scan root
  {
    // init tree_iterator
    IntContainer::tree_iterator iter_pre(&root, scan_type::PRE_ORDER, &allocator);

    ret = iter_pre.start();
    ASSERT_EQ(OB_SUCCESS, ret);

    tmp = nullptr;
    while (OB_SUCC(iter_pre.next(tmp))) {
      IntContainer *node = static_cast<IntContainer*>(tmp);
      cout << "is_cont = " << node->is_container_
            << ", value = " << node->value_ << endl;
    }

    ASSERT_EQ(ret, OB_ITER_END);
  }


  IntContainer sub1(ctx, 12345678);
  IntContainer sub2(ctx, 1234567);
  IntContainer sub3(ctx, 123456);
  IntContainer sub4(ctx, 12345);

  IntContainer scalar1(1);
  IntContainer scalar2(2);
  IntContainer scalar3(3);
  IntContainer scalar4(4);

  ASSERT_EQ(OB_SUCCESS, sub1.append(&scalar1));
  ASSERT_EQ(OB_SUCCESS, sub1.append(&scalar2));
  ASSERT_EQ(OB_SUCCESS, sub1.append(&scalar3));
  ASSERT_EQ(OB_SUCCESS, sub1.append(&scalar4));


  IntContainer scalar100(100);
  IntContainer scalar101(101);
  IntContainer scalar102(102);
  IntContainer scalar103(103);

  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar100));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar101));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar102));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar103));


  ASSERT_EQ(OB_SUCCESS, root.append(&sub1));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub2));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub3));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub4));

  // pre order scan tree
  {  // init tree_iterator
    IntContainer::tree_iterator iter_pre(&root, scan_type::PRE_ORDER, &allocator);

    ret = iter_pre.start();
    ASSERT_EQ(OB_SUCCESS, ret);

    tmp = nullptr;
    while (OB_SUCC(iter_pre.next(tmp))) {
      IntContainer *node = static_cast<IntContainer*>(tmp);
      cout << "is_cont = " << node->is_container_
            << ", value = " << node->value_ << endl;
    }

    ASSERT_EQ(ret, OB_ITER_END);
  }

  // post order scan tree
  {
    // init tree_iterator
    IntContainer::tree_iterator iter_post(&root, scan_type::POST_ORDER, &allocator);

    ret = iter_post.start();
    ASSERT_EQ(OB_SUCCESS, ret);

    ObLibContainerNode* tmp1 = nullptr;
    while (OB_SUCC(iter_post.next(tmp1))) {
      IntContainer *node = static_cast<IntContainer*>(tmp1);
      cout << "is_cont = " << node->is_container_
            << ", value = " << node->value_ << endl;
    }

    ASSERT_EQ(ret, OB_ITER_END);
  }
}

TEST_F(TestXmlTreeBase, iterator)
{

  ObArenaAllocator allocator(ObModIds::TEST);
  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  IntContainer root(ctx, 123456789);

  {
    IntContainer::iterator iter = root.begin();
    ASSERT_EQ(iter.end(), true);
    ASSERT_EQ(true, root.begin() == root.end());
    ASSERT_EQ(0, root.size());
  }

  IntContainer sub1(ctx, 12345678);
  IntContainer sub2(ctx, 1234567);
  IntContainer sub3(ctx, 123456);
  IntContainer sub4(ctx, 12345);

  ASSERT_EQ(OB_SUCCESS, root.append(&sub1));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub2));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub3));
  ASSERT_EQ(OB_SUCCESS, root.append(&sub4));

  {
    IntContainer::iterator iter = root.begin();
    IntContainer* tmp = static_cast<IntContainer*>(*iter);
    ASSERT_NE(tmp, nullptr);
    ASSERT_EQ(tmp->value_, 12345678);
    iter += 3;
    tmp = static_cast<IntContainer*>(*iter);
    ASSERT_EQ(tmp->value_, 12345);
    ++iter;
    ASSERT_EQ(iter == root.end(), true);
  }

  IntContainer scalar100(100);
  IntContainer scalar101(101);
  IntContainer scalar102(102);
  IntContainer scalar103(103);

  {
    IntContainer::iterator iter = scalar100.begin();
    iter.next();
    ASSERT_EQ(iter == scalar100.end(), true);
  }

  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar100));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar101));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar102));
  ASSERT_EQ(OB_SUCCESS, sub2.append(&scalar103));

  {
    IntContainer::iterator iter = sub2.begin();
    IntContainer::iterator iter1 = iter++;
    IntContainer::iterator iter2 = iter;
    ASSERT_EQ(iter2 - iter1, 1);

    IntContainer *p1 = static_cast<IntContainer*>(*iter1);
    IntContainer *p2 = static_cast<IntContainer*>(*iter2);

    ASSERT_EQ(p1->value_, 100);
    ASSERT_EQ(p2->value_, 101);
  }

  {
    IntContainer::iterator iter = sub2.begin();
    IntContainer::iterator iter1 = ++iter;
    IntContainer::iterator iter2 = iter;
    ASSERT_EQ(iter2 - iter1, 0);

    IntContainer *p1 = static_cast<IntContainer*>(*iter1);
    IntContainer *p2 = static_cast<IntContainer*>(*iter2);

    ASSERT_EQ(p1->value_, 101);
    ASSERT_EQ(p2->value_, 101);
  }

  {
    IntContainer::iterator iter = sub2.begin();
    IntContainer::iterator iter1 = iter + 2;
    IntContainer::iterator iter2 = iter + 3;
    ASSERT_EQ(iter2 < iter1, false);
    ASSERT_EQ(iter2 > iter1, true);
  }
}

TEST_F(TestXmlTreeBase, reader)
{
  ObArenaAllocator allocator(ObModIds::TEST);

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

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObXmlElement sub1_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_1.set_key(sub_key1);
  sub1_1.set_prefix(sub_value1);

  ObXmlElement sub1_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_2.set_key(sub_key2);
  sub1_2.set_prefix(sub_value2);

  ObXmlElement sub1_3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_3.set_key(sub_key3);
  sub1_3.set_prefix(sub_value3);



  ObXmlElement sub1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1.set_key(key1);
  sub1.set_prefix(value1);

  // sub children
  ASSERT_EQ(sub1.add_element(&sub1_1), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_2), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_3), OB_SUCCESS);



  ObXmlElement sub2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub2.set_key(key2);
  sub2.set_prefix(value2);

  ObXmlElement sub3_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_1.set_key(key3);
  sub3_1.set_prefix(value3_1);

  ObXmlElement sub3_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_2.set_key(key3);
  sub3_2.set_prefix(value3_2);

  ObXmlElement sub3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3.set_key(key3);
  sub3.set_prefix(value3);

  ObXmlElement sub4(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub4.set_key(key4);
  sub4.set_prefix(value4);

  ObXmlElement sub5(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5.set_key(key5);
  sub5.set_prefix(value5);


  ObXmlElement element(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5.set_key(element_key);
  sub5.set_prefix(element_value);

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
    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::KEY_TYPE;
    seek_info.key_ = key3;

    ObMulModeReader reader(&element, seek_info);
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

  {
    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::POST_SCAN_TYPE;

    ObArray<ObIMulModeBase*> result1;
    ASSERT_EQ(element.get_descendant(result1, POST_ORDER), OB_SUCCESS);
    ASSERT_EQ(result1.size(), 11);

    for (int64_t pos = 0; pos < result1.count(); ++pos) {
      ObXmlElement* tmp = static_cast<ObXmlElement*>(result1.at(pos));
      cout << tmp->tag_info_.ptr_ << ", "
          << tmp->prefix_.ptr_ << endl;
    }
  }

  {
    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::PRE_SCAN_TYPE;

    ObArray<ObIMulModeBase*> result1;

    ASSERT_EQ(element.get_descendant(result1, PRE_ORDER), OB_SUCCESS);
    ASSERT_EQ(result1.size(), 11);

    cout << "pre scan type..." << endl;
    for (int64_t pos = 0; pos < result1.count(); ++pos) {
      ObXmlElement* tmp = static_cast<ObXmlElement*>(result1.at(pos));
      cout << tmp->tag_info_.ptr_ << endl;
    }
  }


  {
    ObPathSeekInfo seek_info;
    seek_info.type_ = SimpleSeekType::ALL_KEY_TYPE;

    ObMulModeReader reader(&element, seek_info);

    ObIMulModeBase* node = nullptr;
    ObString key;
    ObString prefix;

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key1"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value1"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key2"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value2"));


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

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("key4"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value4"));

    ASSERT_EQ(reader.next(node), OB_SUCCESS);
    ASSERT_EQ(node->get_key(key), OB_SUCCESS);
    ASSERT_EQ(std::string(key.ptr(), key.length()), std::string("element_key"));
    prefix = node->get_prefix();
    ASSERT_EQ(std::string(prefix.ptr(), prefix.length()), std::string("value"));

    ASSERT_EQ(reader.next(node), OB_ITER_END);
  }
}

TEST_F(TestXmlTreeBase, lazy_sort)
{
  ObArenaAllocator allocator(ObModIds::TEST);

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

  ObMulModeMemCtx* ctx = nullptr;
  ASSERT_EQ(ObXmlUtil::create_mulmode_tree_context(&allocator, ctx), OB_SUCCESS);

  ObXmlElement sub1_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_1.set_key(sub_key1);
  sub1_1.set_prefix(sub_value1);

  ObXmlElement sub1_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_2.set_key(sub_key2);
  sub1_2.set_prefix(sub_value2);

  ObXmlElement sub1_3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1_3.set_key(sub_key3);
  sub1_3.set_prefix(sub_value3);



  ObXmlElement sub1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub1.alter_member_sort_policy(false);
  sub1.set_key(key1);
  sub1.set_prefix(value1);

  // sub children
  ASSERT_EQ(sub1.add_element(&sub1_3), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_2), OB_SUCCESS);
  ASSERT_EQ(sub1.add_element(&sub1_1), OB_SUCCESS);

  ASSERT_EQ(sub1.size(), 3);
  // orginal order
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(sub1.children_->at(i), sub1.sorted_children_->at(i));
  }


  ObXmlElement sub2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub2.set_key(key2);
  sub2.set_prefix(value2);

  ObXmlElement sub3_1(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_1.set_key(key3);
  sub3_1.set_prefix(value3_1);

  ObXmlElement sub3_2(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3_2.set_key(key3);
  sub3_2.set_prefix(value3_2);

  ObXmlElement sub3(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub3.set_key(key3);
  sub3.set_prefix(value3);

  ObXmlElement sub4(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub4.set_key(key4);
  sub4.set_prefix(value4);

  ObXmlElement sub5(ObMulModeNodeType::M_DOCUMENT, ctx);
  sub5.set_key(key5);
  sub5.set_prefix(value5);


  ObXmlElement element(ObMulModeNodeType::M_DOCUMENT, ctx);
  element.alter_member_sort_policy(false);
  sub5.set_key(element_key);
  sub5.set_prefix(element_value);

  ASSERT_EQ(element.add_element(&sub5), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub4), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3_2), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3_1), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub3), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub2), OB_SUCCESS);
  ASSERT_EQ(element.add_element(&sub1), OB_SUCCESS);

  ASSERT_EQ(element.size(), 7);

  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(element.children_->at(i), element.sorted_children_->at(i));
  }
}

TEST_F(TestXmlTreeBase, stack_abc)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObStack<ObString*> stk(&allocator);

  ObString key1("key1");
  ObString key2("key2");
  ObString key3("key3");

  ASSERT_EQ(stk.push(&key1), 0);


  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(stk.push(&key1), 0);
    ASSERT_EQ(stk.push(&key2), 0);
    ASSERT_EQ(stk.push(&key3), 0);
  }

  ASSERT_EQ(stk.size(), 1000 * 3 + 1);

  for (int i = 0; i < 1000 ; ++i) {
    stk.pop();
    stk.pop();
    stk.pop();
  }

  ASSERT_EQ(stk.size(), 1);

  stk.pop();

  ASSERT_EQ(stk.size(), 0);

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