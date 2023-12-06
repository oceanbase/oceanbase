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
 * This file contains interface support for the xml tree abstraction.
 */

#ifndef OCEANBASE_SQL_OB_XML_TREE
#define OCEANBASE_SQL_OB_XML_TREE

#include "lib/xml/ob_multi_mode_interface.h"
#include "ob_tree_base.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
namespace common {

class ObXmlNode;
class ObXmlAttribute;

enum ObXmlStandaloneType {
  OB_XML_STANDALONE_NONE = 0,
  OB_XML_STANDALONE_YES,
  OB_XML_STANDALONE_NO,

  OB_XML_STANDALONE_OTHER,
};

// valid type for ns check : one floor or all tree
enum ValidType : int8_t {
  FLOOR,
  ALL
};

// flag for operator type : add node or delete node
enum OperaType : int8_t {
  APPEND,
  DELETE
};

#pragma pack(4)
class ObXmlNode : public ObIMulModeBase, public ObLibContainerNode {
public:
  ObXmlNode(ObMulModeNodeType type, ObMulModeMemCtx *ctx)
    : ObIMulModeBase(ObNodeMemType::TREE_TYPE, ObNodeDataType::OB_XML_TYPE, ctx->allocator_),
      ObLibContainerNode(ObNodeDataType::OB_XML_TYPE, ctx),
      xml_type_(type),
      serialize_size_(0)
    {}

  ObXmlNode(ObMulModeNodeType type)
    : ObIMulModeBase(ObNodeMemType::TREE_TYPE, ObNodeDataType::OB_XML_TYPE),
      ObLibContainerNode(ObNodeDataType::OB_XML_TYPE),
      xml_type_(type),
      serialize_size_(0)
    {}
  ObXmlNode(const ObXmlNode& src)
    :  ObXmlNode(src.type(), src.ctx_)
    {}

  virtual ~ObXmlNode() {}

  class iterator : public ObLibContainerNode::iterator {
public:
    iterator()
      : ObLibContainerNode::iterator() {}

    iterator(const iterator& from)
      : ObLibContainerNode::iterator(from) {}

    iterator(const ObLibContainerNode::iterator& from)
      : ObLibContainerNode::iterator(from) {}


    ObIMulModeBase* operator*() {
      ObLibContainerNode* tmp = ObLibContainerNode::iterator::operator*();
      ObXmlNode* res = static_cast<ObXmlNode*>(tmp);
      return res;
    }

    ObIMulModeBase* operator[](int64_t pos) {
      ObLibContainerNode* tmp = ObLibContainerNode::iterator::operator[](pos);
      ObXmlNode* res = static_cast<ObXmlNode*>(tmp);
      return res;
    }

    bool end() { return ObLibContainerNode::iterator::end(); }
    iterator next() { return iterator(ObLibContainerNode::iterator::next()); };
    iterator operator++() { return iterator(ObLibContainerNode::iterator::operator++()); }
    iterator operator--() { return iterator(ObLibContainerNode::iterator::operator--()); }
    iterator operator++(int) { return iterator(ObLibContainerNode::iterator::operator++(0)); }
    iterator operator--(int) { return iterator(ObLibContainerNode::iterator::operator--(0)); }
    void set_range(int64_t start, int64_t finish) { ObLibContainerNode::iterator::set_range(start, finish); }

    bool operator<(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator<(*p);
    }

    bool operator>(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator>(*p);
    }

    bool operator<=(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator<=(*p);
    }

    iterator operator-(int size) { return iterator(ObLibContainerNode::iterator::operator-(size)); }
    iterator operator+(int size) { return iterator(ObLibContainerNode::iterator::operator+(size)); }
    iterator operator+=(int size) { return iterator(ObLibContainerNode::iterator::operator+=(size)); }
    iterator operator-=(int size) { return iterator(ObLibContainerNode::iterator::operator-=(size)); }

    int64_t operator-(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator-(*p);
    }

    bool operator==(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator==(*p);
    }

    bool operator!=(const iterator& iter) {
      const ObLibContainerNode::iterator* p = &iter;
      return ObLibContainerNode::iterator::operator!=(*p);
    }
  };

  iterator begin() { return iterator(ObLibContainerNode::begin()); }
  iterator end() { return iterator(ObLibContainerNode::end()); }

  iterator sorted_begin() { return iterator(ObLibContainerNode::sorted_begin()); }
  iterator sorted_end() { return iterator(ObLibContainerNode::sorted_end()); }

  void set_flags(uint32_t flags) { ObLibContainerNode::set_flags(flags); }

  int get_range(int64_t start, int64_t end, ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr);

  int get_before(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr);
  int get_after(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr);
  int get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr) override;
  int get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res,  ObMulModeFilter* filter = nullptr) override;
  int get_node_count(ObMulModeNodeType node_type, int &count);
  int get_descendant(ObIArray<ObIMulModeBase*>& res, scan_type type,  ObMulModeFilter* filter = nullptr);
  int64_t to_string(char *buf, const int64_t buf_len) {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "is_tree = %d", 1);
    return pos;
  }

  virtual ObXmlNode* get_parent() {return static_cast<ObXmlNode*>(ObLibContainerNode::get_parent());}
  virtual void set_parent(ObXmlNode* parent) { ObLibContainerNode::set_parent(parent); }
  virtual int node_type() { return OB_XML_TYPE; }
  // get xml node type
  ObMulModeNodeType type() const { return xml_type_; }
  void set_xml_type(ObMulModeNodeType xml_type) { xml_type_ = xml_type; }

  // get children size
  int64_t size() { return ObLibContainerNode::size(); }
  int64_t count() { return size(); }
  virtual int set_xml_key(ObString& res) { return OB_SUCCESS; }
  virtual int get_key(ObString& res, int64_t index = -1) { return 0; };
  virtual ObString get_key() { return ""; }
  virtual void set_value(ObIMulModeBase*& value) {}
  virtual void set_value(const ObString &value) {}
  virtual int get_value(ObString& value, int64_t index = -1) { return 0; }
  int get_value(ObIMulModeBase*& value, int64_t index = -1);
  // get all child node (child, attribute, ns)
  virtual int get_value(ObIArray<ObXmlNode*> &value, const ObString& key_name) { return 0; }
  // compare
  virtual int compare(const ObIMulModeBase &other, int &res) { return 0; }

  virtual int set_flag_by_descandant();

  virtual ObMulModeMemCtx* get_mem_ctx() { return ObLibContainerNode::get_mem_ctx(); }

  // key compare
  virtual int compare(const ObString& key, int& res) { return 0; }
  ObObjType field_type() const { return ObNullType;}
  // Add text order to add new nodes
  virtual int append(ObIMulModeBase* node);
  // Add members at fixed positions
  virtual int insert(int64_t pos, ObIMulModeBase* node);
  // drop node
  virtual int remove(int64_t pos);
  // delete the specified node
  virtual int remove(ObIMulModeBase* node);
  // replace node
  virtual int update(int64_t pos, ObIMulModeBase* new_node);
  // replace old node
  virtual int update(ObIMulModeBase* old_node, ObIMulModeBase* new_node);
  // find child with pos
  virtual ObXmlNode* at(int64_t pos, ObIMulModeBase* buffer = nullptr) { return static_cast<ObXmlNode*>(ObLibContainerNode::member(pos)); }
  virtual ObIMulModeBase* attribute_at(int64_t pos, ObIMulModeBase* buffer = nullptr) { return nullptr; }
  virtual int64_t attribute_size() { return 0; }
  virtual int64_t attribute_count() { return 0; }
  // path need
  // node judgeprotected

  // serialize
  virtual int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend) { ns_value = ObString(); return OB_SUCCESS; }
  virtual int get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx) { ns_value = ObString(); return OB_SUCCESS; }
  virtual int64_t get_serialize_size() { return serialize_size_; }
  virtual void update_serialize_size(int64_t size);
  virtual int get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags = 0) { return OB_NOT_SUPPORTED; }

  virtual int get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2 = ObString()) { return OB_NOT_SUPPORTED; }
  int get_raw_binary(common::ObString &out, ObIAllocator *allocator);
  bool is_xml_doc_over_depth(uint64_t depth);

  void set_delta_serialize_size(int64_t size) { serialize_size_ += size; }

  virtual bool has_flags(ObMulModeNodeFlag flag) { return false; }
  virtual ObString get_version()  { return ObString(); }
  virtual ObString get_encoding()  { return ObString(); }
  virtual ObString get_prefix()  { return ObString(); }
  virtual uint16_t get_encoding_flag() { return 0; }
  virtual uint16_t has_xml_decl() { return 0; }
  virtual uint16_t is_unparse() { return 0; }
  virtual ObIMulModeBase* get_attribute_handle() { return nullptr; }
  uint16_t get_standalone() { return 0; }
  bool get_unparse() { return 0; }
  bool get_is_empty() { return false; }
  virtual void set_standalone(uint16_t standalone) {}
  virtual bool is_equal_node(const ObIMulModeBase* other);
  virtual bool is_node_before(const ObIMulModeBase* other);
  virtual bool check_extend() { return false; }
  virtual bool check_if_defined_ns() { return false; }
protected:

  ObMulModeNodeType xml_type_;
  int64_t serialize_size_;
};
#pragma pack()

#pragma pack(4)
class ObXmlElement : public ObXmlNode
{
public:
  ObXmlElement(ObMulModeNodeType type, ObMulModeMemCtx *ctx);
  ObXmlElement(ObMulModeNodeType type, ObMulModeMemCtx *ctx, const ObString& tag);
  virtual ~ObXmlElement() {}

  int init();
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "xml_type = %d", type());
    return pos;
  }
  virtual int64_t get_serialize_size();
  // use key as tag data.
  int get_key(ObString& res, int64_t index = -1);
  void set_xml_key(ObString str) { tag_info_.assign_ptr(str.ptr(), str.length()); }
  ObString get_key() {return tag_info_;}
  uint64_t get_attribute_node_size() { return attributes_ == nullptr ? 0 : attributes_->size(); }
  ObIMulModeBase* get_attribute_handle() { return attributes_; }
  int get_value(ObString& value, int64_t index = -1);
  int get_value(ObIArray<ObIMulModeBase*> &value, const ObString& key_name); // child ， attr ， ns。

  virtual int compare(const ObString& key, int& res);
  // attribute
  // path seek
  bool is_element(ObString tag);
  bool has_attribute() { return attribute_size() > 0; };  // size > 0
  bool has_attribute(const ObString& ns_value, const ObString& name); // name if exist
  bool has_attribute_with_ns(ObXmlAttribute *ns); // find if has the namespace of attribute is the given ns
  int get_attribute_pos(ObMulModeNodeType xml_type, const ObString& name, int64_t &pos); // return attribute pos
  ObXmlAttribute* get_attribute_by_name(const ObString& ns_value, const ObString& name); // get attr by name
  ObXmlAttribute* get_ns_by_name(const ObString& name); // get namespace by name
  bool is_invalid_namespace(ObXmlAttribute* ns); // whether valid namespace
  int get_namespace_default(ObIArray<ObIMulModeBase*> &value); // get all default ns
  int get_namespace_list(ObIArray<ObIMulModeBase*> &value); // get all namespace

  int get_attribute(ObXmlAttribute*& res, int64_t pos);
  virtual int64_t attribute_count() { return attributes_ == nullptr ? 0 : attributes_->size(); }
  ObIMulModeBase* attribute_at(int64_t pos, ObIMulModeBase* buffer = nullptr);
  int64_t attribute_size() {return is_init_ ? attributes_->size() : 0;}
  int add_attribute(ObXmlNode* xnode, bool ns_check = false, int pos = -1);
  int add_attr_by_str(const ObString& name,
                      const ObString& value,
                      ObMulModeNodeType type = ObMulModeNodeType::M_NAMESPACE,
                      bool ns_check = false,
                      int pos = -1);
  int update_attribute(ObXmlNode* xnode, int pos, bool ns_check = false);
  int remove_attribute(int pos);
  int remove_namespace(int pos, bool ns_check = false);
  int get_attribute_list(ObIArray<ObIMulModeBase*> &value); //
  int add_element(ObXmlNode* xnode, bool ns_check = false, int pos = -1);
  int append_unparse_text(const ObString &str);
  int update_element(ObXmlNode* xnode, int pos, bool ns_check = false);
  int update_element(ObXmlNode* xnode, const ObString& name, bool ns_check = false);
  int remove_element(ObXmlNode* xnode);
  int get_element_list(ObIArray<ObIMulModeBase*> &value); //
  int get_element_by_name(const ObString& ns_value, const ObString& name, ObIArray<ObIMulModeBase*> &value);
  // namespace
  void set_ns(ObXmlAttribute* xnode);  // set namespace
  int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend);  // get value of namespace
  int get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx);
   ObXmlAttribute* get_ns() { return name_spaces_;} // get namespace
  // flag & prefix & tag
   void set_prefix(const ObString &prefix) { prefix_.assign_ptr(prefix.ptr(), prefix.length()); }
   virtual ObString get_prefix() { return prefix_; }
   void set_standalone(uint16_t standalone) { standalone_ = standalone; }
   uint16_t get_standalone() { return standalone_; }
   void set_has_xml_decl(uint16_t has_xml_decl) { has_xml_decl_ = has_xml_decl; }
  virtual uint16_t has_xml_decl() { return has_xml_decl_; }
  void set_empty(uint16_t empty) {is_empty_ = empty;}
  uint16_t is_empty() {return is_empty_;}
  void set_unparse(uint16_t unparse) {is_unparse_ = unparse;}
  virtual uint16_t is_unparse() {return is_unparse_;}
  void set_encoding_flag(uint16_t encoding_val_empty) {encoding_val_empty_ = encoding_val_empty;}
  virtual uint16_t get_encoding_flag() {return encoding_val_empty_;}
  typedef common::hash::ObHashMap<ObString, ObXmlAttribute*> NsMap;
  typedef ObArray<ObXmlAttribute*> NsArray;

  int Validate_XML_Tree_Legitimacy(ObXmlNode* node, int8_t operator_data, int8_t valid_type = 0);
  int check_node_valid_with_ns(NsArray& ns_array, ObXmlNode* cur_node, int8_t operator_data, int8_t valid_type);
  int get_valid_ns_from_parent(NsMap &ns_map, ObXmlNode* cur_node);
  int add_update_ns_map(NsMap &ns_map, ObString key, ObXmlNode* cur_node, bool overwrite = false);
  ObXmlAttribute* get_ns_value_from_array(NsArray& ns_array, const ObString& prefix);
  int remove_ns_value_from_array(NsArray& ns_array, ObXmlAttribute* node);

  virtual bool has_flags(ObMulModeNodeFlag flag);
  virtual bool get_is_empty() { return is_empty_; }
  virtual bool get_unparse() { return is_unparse_; }

  virtual int get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags = 0);
  virtual int get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2 = ObString());
  virtual bool check_if_defined_ns();
private:
  // namespace prefix
  ObString prefix_;
  // tag info
  ObString tag_info_;
  // attr
  ObXmlNode* attributes_; // include namespace
  // namespace
  ObXmlAttribute* name_spaces_;         // point to namespace in attr
  // parse flag
  union {
    uint16 flags_;
    struct {
      uint16_t standalone_ : 4;    // : default 0, yes 1, no 2, other 3;
      uint16_t has_xml_decl_: 1;   // no 0， yes 1
      uint16_t is_empty_: 1;       // empty
      uint16_t is_unparse_: 1;     // well format element
      uint16_t encoding_val_empty_: 1;    //  has encoding clause but encoding value is null
      uint16_t reserved_: 8;
    };
  };
  bool is_init_;

  DISALLOW_COPY_AND_ASSIGN(ObXmlElement);
};
#pragma pack()

#pragma pack(4)
// document or content
class ObXmlDocument : public ObXmlElement
{
public:
  ObXmlDocument(ObMulModeNodeType type, ObMulModeMemCtx *ctx)
      : ObXmlElement(type, ctx)
  {}
  virtual ~ObXmlDocument() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "xml_type = %d", type());
    return pos;
  }

  void set_version(ObString version) {version_.assign_ptr(version.ptr(), version.length());}
  void set_encoding(ObString encoding) {encoding_.assign_ptr(encoding.ptr(), encoding.length());}
  virtual ObString get_version()  { return version_ ; }
  virtual ObString get_encoding()  { return encoding_; }

  void set_inSubset(ObXmlNode* intSubset) { intSubset_ = intSubset; }
  void set_extSubset(ObXmlNode* extSubset) { extSubset_ = extSubset; }
  ObXmlNode* get_inSubset() { return intSubset_; }
  ObXmlNode* get_extSubset() { return extSubset_; }

  int64_t get_serialize_size();

protected:
  // xml prolog
  // <?xml version="1.0" encoding="UTF-8"?>
  ObString version_;
  ObString encoding_;
  ObXmlNode* intSubset_; // int DTD
  ObXmlNode* extSubset_; // ext DTD
  DISALLOW_COPY_AND_ASSIGN(ObXmlDocument);
};

// attribute & namespace & PI
class ObXmlAttribute : public ObXmlNode
{
  public:
  ObXmlAttribute()
      : ObXmlNode(M_MAX_TYPE, nullptr),
        prefix_(),
        ns_(nullptr),
        attr_decl_(NULL),
        only_key_(false)
  {}
  ObXmlAttribute(ObMulModeNodeType type, ObMulModeMemCtx *ctx)
      : ObXmlNode(type, ctx),
        prefix_(),
        ns_(nullptr),
        attr_decl_(NULL),
        only_key_(false)
  {}
  ObXmlAttribute(ObMulModeNodeType type, ObMulModeMemCtx *ctx, const ObString& key, const ObString& value)
      : ObXmlNode(type, ctx),
        prefix_(),
        ns_(nullptr),
        name_(key),
        value_(value),
        attr_decl_(NULL),
        only_key_(false)
  {}
  ObXmlAttribute(const ObXmlAttribute& src)
      : ObXmlAttribute(src.type(), src.ctx_, src.name_, src.value_)
  {}
  virtual ~ObXmlAttribute() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "xml_type = %d", type());
    return pos;
  }

  int eval_crc_value(ObXmlNode& xnode); // cal crc_value
  int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend);
  bool is_pi(ObString target);

  ObXmlNode* get_parent() { return xml_type_ == M_INSTRUCT ? ObXmlNode::get_parent() : ObXmlNode::get_parent()->get_parent(); }

  void set_xml_key(const ObString &new_key) {name_.assign_ptr(new_key.ptr(), new_key.length());}
  int get_key(ObString& res, int64_t index = -1);
  ObString get_key() { return name_; }
  void set_value(const ObString &value) {value_.assign_ptr(value.ptr(), value.length());}
  int get_value(ObString& value, int64_t index = -1);
  ObString& get_value() { return value_; }
  void set_only_key() { only_key_ = true; }
  bool get_only_key() { return only_key_; }

   void set_attr_decl(ObXmlNode *attr_decl) {attr_decl_ = attr_decl;}
   void get_attr_decl(const ObXmlNode *&attr_decl) {attr_decl = attr_decl_;}

   void set_prefix(const ObString &prefix) {prefix_.assign_ptr(prefix.ptr(), prefix.length());}
   void get_prefix(ObString &prefix) {prefix.assign_ptr(prefix_.ptr(), prefix_.length());}
   ObString get_prefix() { return prefix_;}
   void set_ns(ObXmlAttribute* ns) {ns_ = ns;}
   ObXmlAttribute* get_ns() { return ns_;}
  // ObXmlNode *clone(ObIAllocator* allocator) const;
  virtual int compare(const ObString& key, int& res);
  int64_t get_serialize_size();
protected:
  // namespace prefix
  ObString prefix_;
  // namespace point (attribute type)
  ObXmlAttribute* ns_;
  ObString name_;  // key
  ObString value_;  // value
  ObXmlNode *attr_decl_; // point to ns
  bool only_key_; // only for mysql
};
#pragma pack()

#pragma pack(4)
// text / cdata / comment
class ObXmlText : public ObXmlNode {
public:
  explicit ObXmlText(ObMulModeNodeType type, ObMulModeMemCtx *ctx)
      : ObXmlNode(type, ctx),
        text_(),
        length_(0),
        is_space_(false)
  {}

  explicit ObXmlText(ObMulModeNodeType type)
      : ObXmlNode(type),
        text_(),
        length_(0),
        is_space_(false)
  {}

  virtual ~ObXmlText() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "xml_type = %d", type());
    return pos;
  }
  void set_value(const ObString &value) {text_.assign_ptr(value.ptr(), value.length()); length_ = value.length();}
  virtual int get_key(ObString& res, int64_t index = -1);
  // text without key
  virtual ObString get_key() { return ""; }
  virtual int get_value(ObString& value, int64_t index = -1);
  virtual int compare(const ObString& key, int& res) override;

  // virtual int get_value(ObIArray<ObLibTreeNodeBase*> &value, const ObString& key_name);
  const ObString get_text() { return text_; }  // get TEXT
  size_t get_length() { return length_; }  // get length
  void set_text(const ObString text) {text_.assign_ptr(text.ptr(), text.length()); length_ = text.length();} // set text and length
  void set_length(int length) { length_ = length; } // update length。
  bool is_space() { return is_space_; }
  void set_is_space(bool is_space) { is_space_ = is_space; }
  int64_t get_serialize_size();
protected:
  ObString text_;
  int64_t length_;
  bool is_space_;           // TODO  xml tree to string content \n affect of space : last node is text then not add new line, ignore space;
  DISALLOW_COPY_AND_ASSIGN(ObXmlText);
};
#pragma pack()

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_JSON_TREE
