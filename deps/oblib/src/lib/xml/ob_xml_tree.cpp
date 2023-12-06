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
#define USING_LOG_PREFIX LIB
#include "lib/utility/ob_hang_fatal_error.h"
#include "common/ob_smart_call.h"
#include "lib/ob_errno.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xml_parser.h"

namespace oceanbase {
namespace common {

ObXmlElement::ObXmlElement(ObMulModeNodeType type, ObMulModeMemCtx *ctx)
      : ObXmlNode(type, ctx),
        prefix_(),
        attributes_(nullptr),
        name_spaces_(nullptr),
        flags_(0),
        is_init_(false)
{}

ObXmlElement::ObXmlElement(ObMulModeNodeType type, ObMulModeMemCtx *ctx, const ObString& tag)
      : ObXmlNode(type, ctx),
        prefix_(),
        tag_info_(tag),
        attributes_(nullptr),
        name_spaces_(nullptr),
        flags_(0),
        is_init_(false)
{}

int ObXmlElement::init()
{
  INIT_SUCC(ret);
  attributes_ = static_cast<ObXmlNode *> (get_allocator()->alloc(sizeof(ObXmlNode)));
  if (OB_ISNULL(attributes_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
  } else {
    attributes_ = new (attributes_) ObXmlNode(ObMulModeNodeType::M_ATTRIBUTE, ctx_);
    attributes_->set_parent(this);
    attributes_->set_flags(is_lazy_sort() ? MEMBER_LAZY_SORTED: 0);
    is_init_ = true;
  }
  return ret;
}
int ObXmlNode::append(ObIMulModeBase* node)
{
  ObXmlNode* n_node = static_cast<ObXmlNode*>(node);
  int ret = ObLibContainerNode::append(n_node);
  if (OB_SUCC(ret)) {
    update_serialize_size(node->get_serialize_size());
  }
  return ret;
}

int ObXmlNode::insert(int64_t pos, ObIMulModeBase* node)
{
  ObXmlNode* n_node = static_cast<ObXmlNode*>(node);
  int ret = ObLibContainerNode::insert(pos, n_node);
  if (OB_SUCC(ret)) {
    update_serialize_size(node->get_serialize_size());
  }
  return ret;
}

int ObXmlNode::remove(int64_t pos)
{
  int64_t delta_size = 0;
  if (size() > pos && pos >= 0) {
    delta_size = -1 * (static_cast<ObXmlNode*>(at(pos)))->get_serialize_size();
  }

  int ret = ObLibContainerNode::remove(pos);
  if (OB_SUCC(ret)) {
    update_serialize_size(delta_size);
  }
  return ret;
}

int ObXmlNode::remove(ObIMulModeBase* node)
{
  ObXmlNode* n_node = static_cast<ObXmlNode*>(node);
  int ret = ObLibContainerNode::remove(n_node);
  if (OB_SUCC(ret)) {
    update_serialize_size(-1 * n_node->get_serialize_size());
  }
  return ret;
}

int ObXmlNode::get_value(ObIMulModeBase*& value, int64_t index)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp = nullptr;
  if (index == -1) {
    value = this;
  } else if (OB_ISNULL(tmp = at(index))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get specified child", K(ret), K(index), K(size()));
  } else {
    value = tmp;
  }
  return ret;
}

// tree base update use as leaf node : text
int ObXmlNode::update(int64_t pos, ObIMulModeBase* new_node)
{
  INIT_SUCC(ret);
  ObXmlNode* n_node = NULL;
  if (OB_ISNULL(new_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new node input is null", K(ret));
  } else {
    int64_t delta_size = 0;
    if (size() > pos && pos >= 0) {
      delta_size = at(pos)->get_serialize_size();
    }

    n_node = static_cast<ObXmlNode*>(new_node);
    if (OB_FAIL(ObLibContainerNode::update(pos, n_node))) {
      LOG_WARN("fail to update new node in pos", K(ret), K(pos));
    } else {
      delta_size -= n_node->get_serialize_size();
      update_serialize_size(delta_size);
    }
  }
  return ret;
}

// new node replace old node in element child
int ObXmlNode::update(ObIMulModeBase* old_node, ObIMulModeBase* new_node)
{
  INIT_SUCC(ret);
  ObXmlNode* o_node = NULL;
  ObXmlNode* n_node = NULL;
  if (OB_ISNULL(old_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node input is null", K(ret));
  } else if (OB_ISNULL(new_node)) {
    if (OB_FAIL(remove(o_node))) {
      LOG_WARN("fail remove old node", K(ret));
    }
  } else {
    o_node = static_cast<ObXmlNode*>(old_node);
    n_node = static_cast<ObXmlNode*>(new_node);
    if (OB_FAIL(ObLibContainerNode::update(o_node, n_node))) {
      LOG_WARN("fail to update new node in pos", K(ret));
    } else {
      int64_t delta_size = o_node->get_serialize_size() - n_node->get_serialize_size();
      update_serialize_size(delta_size);
    }
  }
  return ret;
}

bool ObXmlNode::is_equal_node(const ObIMulModeBase* other)
{
  bool res = false;

  if (OB_ISNULL(other)) {
  } else if (other->is_tree()) {
    res = static_cast<ObIMulModeBase*>(this) == other;
  }

  return res;
}

bool ObXmlNode::is_node_before(const ObIMulModeBase* other)
{
  bool res = false;

  if (OB_ISNULL(other)) {
  } else if (other->is_tree()) {
    res = static_cast<ObIMulModeBase*>(this) < other;
  }

  return res;
}
bool ObXmlElement::is_element(ObString tag)
{
  return 0 == tag_info_.compare(tag);
}

bool ObXmlAttribute::is_pi(ObString target)
{
  return 0 == name_.compare(target);
}

int ObXmlNode::set_flag_by_descandant()
{
  INIT_SUCC(ret);

  ObLibContainerNode* current = this;
  ObLibContainerNode::tree_iterator iter(current, PRE_ORDER, ctx_->allocator_);

  if (!(type() == M_ELEMENT || type() == M_DOCUMENT || type() == M_CONTENT || type() == M_UNPARSED || type() == M_UNPARESED_DOC)) {
  } else if (OB_FAIL(iter.start())) {
    LOG_WARN("fail to prepare scan iterator", K(ret));
  } else {
    ObLibContainerNode* tmp = nullptr;

    while (OB_SUCC(iter.next(tmp))) {
      ObXmlNode* xnode = static_cast<ObXmlNode*>(tmp);

      if (xnode->type() == M_ELEMENT) {
        ObXmlElement* tmp = static_cast<ObXmlElement*>(xnode);
        if (tmp->is_unparse()) {
          (static_cast<ObXmlElement*>(this))->set_unparse(1);
          break;
        }
      }
    }

    if (ret == OB_ITER_END || ret == OB_SUCCESS) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail scan liberty tree", K(ret), K(type()));
    }
  }

  return ret;
}

bool ObXmlElement::has_attribute(const ObString& ns_value, const ObString& name) // name if exist
{
  return NULL != get_attribute_by_name(ns_value, name);
}

bool ObXmlElement::has_attribute_with_ns(ObXmlAttribute *ns)
{
  bool is_found = false;
  for (int64_t i = 0; !is_found && i < attribute_size(); i++) {
    ObXmlAttribute *t_attr = static_cast<ObXmlAttribute*>(attributes_->at(i));
    if (t_attr->type() == ObMulModeNodeType::M_ATTRIBUTE
        && ns == t_attr->get_ns()) {
      is_found = true;
    }
  }
  return is_found;
}

// only check attribute and ns name, do not check attribute with ns
int ObXmlElement::get_attribute_pos(ObMulModeNodeType xml_type, const ObString& name, int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  if (xml_type != M_ATTRIBUTE && xml_type != M_NAMESPACE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid xml node type", K(ret));
  } else {
    for (int64_t i = 0; !is_found && i < attribute_size(); i++) {
      if (attributes_->at(i)->type() == xml_type &&
        0 == name.case_compare(attributes_->at(i)->get_key())) {
        pos = i;
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_SEARCH_NOT_FOUND;
    }
  }

  return ret;
}

ObXmlAttribute* ObXmlElement::get_attribute_by_name(const ObString& ns_value, const ObString& name) // get attr by name
{
  ObXmlAttribute* res_node = NULL;
  ObXmlAttribute* t_attr = NULL;
  for (int64_t i = 0; i < attribute_size(); i++) {
    if (attributes_->at(i)->type() == ObMulModeNodeType::M_ATTRIBUTE
        && 0 == name.compare(attributes_->at(i)->get_key())) {
      t_attr = dynamic_cast<ObXmlAttribute*>(attributes_->at(i));
      if (!ns_value.empty()
          && ((OB_ISNULL(t_attr->get_ns())
                  && !(t_attr->get_prefix().compare(ObXmlConstants::XML_STRING) == 0
                        && ns_value.compare(ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI) == 0))   // ns_value not null but attr without ns
              || (OB_NOT_NULL(t_attr->get_ns())   // attr with ns but ns mismatch
                  && 0 != ns_value.compare(t_attr->get_ns()->get_value())))) { // ns mismatch do nothing
      } else if (ns_value.empty() && OB_NOT_NULL(t_attr->get_ns())) {  // input prefix is null but attr ns has value
      } else {
        res_node = t_attr;
        break;
      }
    }
  }
  return res_node;
}

ObXmlAttribute* ObXmlElement::get_ns_by_name(const ObString& name)
{
  ObXmlAttribute* res_node = NULL;
  for (int64_t i = 0; i < attribute_size(); i++) {
    if (attributes_->at(i)->type() == ObMulModeNodeType::M_NAMESPACE
        && 0 == name.compare(attributes_->at(i)->get_key())) {
      res_node = dynamic_cast<ObXmlAttribute*>(attributes_->at(i));
      break;
    }
  }
  return res_node;
}

int ObXmlElement::get_namespace_default(ObIArray<ObIMulModeBase*> &value)
{
  INIT_SUCC(ret);
  ObArray<ObIMulModeBase*> t_value;
  if (OB_FAIL(get_namespace_list(t_value))) {
    LOG_WARN("fail to get all ns from attribute", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < t_value.size(); i++) {
      if (0 == dynamic_cast<ObXmlAttribute*>(dynamic_cast<ObXmlNode*>(t_value.at(i)))->get_key().compare(ObXmlConstants::XMLNS_STRING)) {
        ret = value.push_back(t_value.at(i));
      }
    }
  }
  return ret;
}

bool ObXmlElement::is_invalid_namespace(ObXmlAttribute* ns)
{
  return get_ns()->type() == ObMulModeNodeType::M_NAMESPACE
          && (0 == ns->get_key().compare(get_ns()->get_key()))
          && (0 == ns->get_value().compare(get_ns()->get_value()));
}

int ObXmlElement::get_value(ObString& value, int64_t index)
{
  int ret = OB_SUCCESS;
  UNUSED(index);
  UNUSED(value);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("element has no value", K(ret));
  return ret;
}

int ObXmlNode::get_range(int64_t start_idx, int64_t last_idx, ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);

  IndexRange range = ObLibContainerNode::get_effective_range(start_idx, last_idx);

  iterator first = begin() + range.first;
  iterator finish = end();
  iterator last = begin() + range.second;
  for (; OB_SUCC(ret) && first < finish && first <= last ; ++first) {
    ObIMulModeBase* tmp = *first;
    bool filtered = false;
    if (OB_ISNULL(filter)) {
      filtered = true;
    } else if (OB_FAIL((*filter)(tmp, filtered))) {
      LOG_WARN("fail to filter xnode", K(ret));
    }
    if (OB_SUCC(ret) && filtered && OB_FAIL(res.push_back(tmp))) {
      LOG_WARN("fail to store scan result", K(ret));
    }
  }

  return ret;
}

int ObXmlNode::get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  return get_range(-1, static_cast<uint32_t>(-1), res, filter);
}

int ObXmlNode::get_node_count(ObMulModeNodeType node_type, int &count)
{
  INIT_SUCC(ret);
  count = 0;
  for (int i = 0; i < size(); i++) {
    if (at(i)->type() == node_type) ++count;
  }
  return ret;
}

int ObXmlNode::get_descendant(ObIArray<ObIMulModeBase*>& res, scan_type type, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);

  ObLibContainerNode* current = this;
  ObLibContainerNode::tree_iterator iter(current, type, ctx_->allocator_);

  if (OB_FAIL(iter.start())) {
    LOG_WARN("fail to prepare scan iterator", K(ret));
  } else {
    ObLibContainerNode* tmp = nullptr;
    while (OB_SUCC(iter.next(tmp))) {
      bool filtered = false;
      ObXmlNode* xnode = static_cast<ObXmlNode*>(tmp);
      if (OB_ISNULL(filter)) {
        filtered = true;
      } else if (OB_FAIL((*filter)(xnode, filtered))) {
        LOG_WARN("fail to filter xnode", K(ret));
      }
      if (OB_SUCC(ret) && filtered && OB_FAIL(res.push_back(xnode))) {
        LOG_WARN("fail to store scan result", K(ret));
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail scan liberty tree", K(ret), K(type));
    }
  }

  return ret;
}

int ObXmlNode::get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);
  IterRange range;
  if (OB_FAIL(ObLibContainerNode::get_children(key, range))) {
    LOG_WARN("fail to get range child", K(ret));
  } else if (!range.first.end()) {
    iterator start(range.first);
    iterator last(range.second);
    iterator finish = iterator(ObLibContainerNode::sorted_end());
    for (; OB_SUCC(ret) && start <= last && start < finish; start++) {
      bool filtered = false;
      if (OB_ISNULL(filter)) {
        filtered = true;  // do not need filter
      } else if (OB_FAIL((*filter)(*start, filtered))) {
        LOG_WARN("fail to filter xnode", K(ret));
      }
      if (OB_SUCC(ret) && filtered && OB_FAIL(res.push_back(*start))) {
        LOG_WARN("fail to store scan result", K(ret));
      }
    }
  }
  return ret;
}

int ObXmlNode::get_before(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);
  int64_t pos = get_index();

  return get_range(-1, pos - 1, res, filter);
}

int ObXmlNode::get_after(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);

  int64_t pos = get_index();

  return get_range(pos + 1, static_cast<uint32_t>(-1), res, filter);
}

void ObXmlNode::update_serialize_size(int64_t size)
{
  serialize_size_ += size;
  ObXmlNode* parent = get_parent();

  while (parent) {
    parent->set_delta_serialize_size(size);
    parent = parent->get_parent();
  };
}

int64_t ObXmlElement::get_serialize_size()
{
  int64_t res = 0;
  if (serialize_size_ <= 0) {
    serialize_size_ = 0;
    iterator finish = end();
    int64_t children_num = 0;
    for (iterator iter = begin(); iter < finish; iter++, children_num++) {
      ObXmlNode* cur = static_cast<ObXmlNode*>(*iter);
      res += cur->get_serialize_size();
    }

    if (OB_NOT_NULL(attributes_)) {
      iterator finish = attributes_->end();
      for (iterator iter = attributes_->begin(); iter < finish; iter++, children_num++) {
        ObXmlNode* cur = static_cast<ObXmlNode*>(*iter);
        res += cur->get_serialize_size();
      }
    }
    // binary common header
    res += sizeof(uint8_t) * 2 + 2 * sizeof(uint64_t);

    res += sizeof(uint16_t);
    if (prefix_.length() > 0) {
      res += sizeof(uint16_t) + prefix_.length();
    }

    // node key stirng key entry
    res += get_key().length();
    res += (sizeof(uint32_t) * 4 + sizeof(uint8_t)) * children_num;
    serialize_size_ = res;
  } else {
    res = serialize_size_;
  }

  return res;
}

int ObXmlNode::get_raw_binary(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  ObXmlBin bin(ctx_);

  ObIAllocator* used_allocator = allocator == nullptr ? ObIMulModeBase::allocator_ : allocator;

  if (OB_ISNULL(used_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator null param", K(ret));
  } else if (OB_FAIL(bin.parse_tree(this))) {
    LOG_WARN("failed to serialize to bin", K(ret));
  } else if (OB_FAIL(bin.get_raw_binary(out, used_allocator))) {
    LOG_WARN("failed to get bin", K(ret));
  }
  return ret;
}

int ObXmlElement::get_key(ObString& res, int64_t index)
{
  INIT_SUCC(ret);
  UNUSED(index);
  res.assign_ptr(tag_info_.ptr(), tag_info_.length());
  return ret;
}

int ObXmlElement::add_element(ObXmlNode* xnode, bool ns_check, int pos)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(xnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node is null", K(ret));
  } else if (pos > count() || pos < -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret));
  } else {
    xnode->set_parent(this);
    if (ns_check && OB_FAIL(Validate_XML_Tree_Legitimacy(xnode, OperaType::APPEND, ValidType::ALL))) {
      LOG_WARN("add element failed", K(ret));
    } else if (pos == -1 && OB_FAIL(append(xnode))) {
      LOG_WARN("element fail to add xnode in the end", K(ret));
    } else if (pos >= 0 && OB_FAIL(insert(pos, xnode))) {
      LOG_WARN("element fail to insert xnode in pos", K(ret));
    } else {
      set_empty(0);
      update_serialize_size(xnode->get_serialize_size());
    }
  }
  return ret;
}

int ObXmlElement::update_element(ObXmlNode* xnode, int pos, bool ns_check)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(xnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update element is null", K(ret));
  } else {
    xnode->set_parent(this);
    if (ns_check && OB_FAIL(Validate_XML_Tree_Legitimacy(xnode, OperaType::APPEND, ValidType::ALL))) {
      LOG_WARN("update element failed", K(ret));
    } else if (OB_FAIL(update(pos, xnode))) {
      LOG_WARN("update xml node with pos failed", K(ret));
    }
  }
  return ret;
}

int ObXmlElement::remove_element(ObXmlNode* xnode)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(xnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remove element is null", K(ret));
  } else if (OB_FAIL(remove(xnode))) {
    LOG_WARN("remove xml element with pos failed", K(ret));
  }
  return ret;
}

int ObXmlElement::compare(const ObString& key, int& res)  // 0， 1，-1
{
  INIT_SUCC(ret);
  res = key.compare(tag_info_);
  return ret;
}

int ObXmlElement::get_value(ObIArray<ObIMulModeBase*> &value, const ObString& key_name)
{
  INIT_SUCC(ret);
  int child_size = size();
  for (int i = 0; OB_SUCC(ret) && i < child_size; i ++) {
    if (OB_FAIL(value.push_back(static_cast<ObXmlNode*>(static_cast<ObXmlNode*>(this)->at(i))))) {
      LOG_WARN("add child failed", K(ret));
    }
  }
  int attr_size = attribute_size();
  for (int i = 0; OB_SUCC(ret) && i < attr_size; i ++) {
    if (type() != ObMulModeNodeType::M_ATTRIBUTE) {
    } else if (OB_FAIL(value.push_back(attributes_->at(i)))) {
      LOG_WARN("add attribute failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObXmlElement::get_element_list(ObIArray<ObIMulModeBase*> &value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObXmlNode::get_children(value))) {
    LOG_WARN("get children element failed", K(ret));
  }
  return ret;
}

int ObXmlElement::get_element_by_name(const ObString& prefix, const ObString& name, ObIArray<ObIMulModeBase*> &value) // get element by name
{
  INIT_SUCC(ret);
  ObArray<ObIMulModeBase*> t_value;
  ObXmlNode* t_node = NULL;
  if (OB_FAIL(ObXmlNode::get_children(t_value))) {
    LOG_WARN("get children element failed", K(ret), K(prefix), K(name));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < t_value.size(); i++) {
      t_node = dynamic_cast<ObXmlNode*>(t_value.at(i));
      if (t_node->type() == ObMulModeNodeType::M_ELEMENT
          && 0 == name.compare(t_node->get_key())) {
        if (!prefix.empty() && 0 != prefix.compare(dynamic_cast<ObXmlElement*>(t_node)->get_prefix())) { // ns mismatch do nothing
        } else if (prefix.empty() && !dynamic_cast<ObXmlElement*>(t_node)->get_prefix().empty()) {  // input prefix is null but attr prefix has value
        } else {
          ret = value.push_back(t_value.at(i));
          break;
        }
      }
    }
  }
  return ret;
}

int ObXmlElement::add_attribute(ObXmlNode* xnode, bool ns_check, int pos)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(xnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node is null", K(ret));
  } else if (!is_init_) { // init attribute if first use,
    attributes_ = static_cast<ObXmlNode *> (get_allocator()->alloc(sizeof(ObXmlNode)));
    if (OB_ISNULL(attributes_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      attributes_ = new (attributes_) ObXmlNode(ObMulModeNodeType::M_ATTRIBUTE, ctx_);
      attributes_->set_parent(this);
      is_init_ = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (pos > attributes_->count() || pos < -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret));
  } else {
    xnode->set_parent(this);
    if (ns_check && OB_FAIL(Validate_XML_Tree_Legitimacy(xnode, OperaType::APPEND, ValidType::ALL))) {
      LOG_WARN("update namespace failed", K(ret));
    } else if (pos == -1 && OB_FAIL(attributes_->append(xnode))) {
      LOG_WARN("attribute fail to add xnode in the end", K(ret));
    } else if (pos >= 0 && OB_FAIL(attributes_->insert(pos, xnode))) {
      LOG_WARN("attribute fail to insert xnode in pos", K(ret));
    }
  }
  return ret;
}

int ObXmlElement::get_valid_ns_from_parent(NsMap &ns_map, ObXmlNode* cur_node)
{
  INIT_SUCC(ret);
  ObXmlNode* t_node = cur_node;
  while(OB_SUCC(ret) && OB_NOT_NULL(t_node->get_parent())) {
    t_node = t_node->get_parent();
    ObXmlElement *t_element = static_cast<ObXmlElement*>(t_node);
    for (int i = 0; OB_SUCC(ret) && i < t_element->attribute_size(); i ++) {
      if (!is_init_) {
      } else if (is_init_ && OB_ISNULL(t_element->attributes_->at(i))) {
        LOG_WARN("node in pos is null", K(ret), K(i));
      } else if (t_element->attributes_->at(i)->type() == ObMulModeNodeType::M_NAMESPACE
          && OB_ISNULL(ns_map.get(t_element->attributes_->at(i)->get_key()))) {
        ret = add_update_ns_map(ns_map, t_element->attributes_->at(i)->get_key(), t_element->attributes_->at(i));
      }
    }
  }
  return ret;
}

int ObXmlElement::Validate_XML_Tree_Legitimacy(ObXmlNode* node, int8_t operator_data, int8_t valid_type)
{
  INIT_SUCC(ret);
  ObXmlElement::NsMap ns_map;
  ObXmlElement::NsArray ns_array;
  if (OB_ISNULL(node)) {    // report error
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node input is null", K(ret));
  } else if (OB_FAIL(ns_map.create(64, "XML_PARENT_NS"))) {
    LOG_WARN("ns map create failed", K(ret));
  } else if (OB_FAIL(get_valid_ns_from_parent(ns_map, node))) {
    LOG_WARN("get ns from parent failed", K(ret));
  } else {
    for (NsMap::iterator it = ns_map.begin(); OB_SUCC(ret) && it != ns_map.end(); it++) {
      if (OB_FAIL(ns_array.push_back(it->second))) {
        LOG_WARN("fail to add parent ns to array", K(ret), K(it->first), K(it->second));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(check_node_valid_with_ns(ns_array, node, operator_data, valid_type))) {
      // check node and child valid
      LOG_WARN("failed to check node valid", K(ret));
    }
  }
  return ret;
}

// find namespace in array
ObXmlAttribute* ObXmlElement::get_ns_value_from_array(NsArray& ns_array, const ObString& prefix)
{
  ObXmlAttribute* res = NULL;
  int64_t size_arr = ns_array.size();
  for (int64_t i = (size_arr - 1); i >= 0; i--) {
    if (0 == prefix.compare(ns_array.at(i)->get_key())) {
      res = ns_array.at(i);
      break;
    }
  }
  return res;
}

int ObXmlElement::append_unparse_text(const ObString &str)
{
  INIT_SUCC(ret);
  ObXmlElement* new_element = NULL;
  ObXmlText* new_text = NULL;
  bool need_com = false;
  char* str_buf = NULL;
  size_t str_len = str.length();
  // need_com record whether last element is unparse node
  need_com = this->size() > 1 && this->at(this->size() - 1)->type() == M_ELEMENT
              && dynamic_cast<ObXmlElement*>(this->at(this->size() - 1))->is_unparse();
  if (need_com) {
    new_text = dynamic_cast<ObXmlText*>(this->at(this->size() - 1)->at(0));
    if (OB_ISNULL(new_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get unparse text node", K(ret), K(this->size() - 1));
    } else {
      str_len += new_text->get_length();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (str_len > 0 && OB_ISNULL(str_buf = static_cast<char*>(get_allocator()->alloc(str_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(str_len));
  } else {
    ObString res_str(str_len, 0, str_buf);
    if (need_com) {
      if (new_text->get_text().length() != res_str.write(new_text->get_text().ptr(), new_text->get_text().length())) {
        LOG_WARN("fail to get unparse text from unparse node", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (str.length() != res_str.write(str.ptr(), str.length())) {
      LOG_WARN("fail to get text from expr", K(ret), K(str));
    } else {
      new_text = NULL;
      if (OB_ISNULL(new_text = OB_NEWx(ObXmlText, get_allocator(), ObMulModeNodeType::M_TEXT, ctx_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else {
        new_text->set_text(res_str);
        if (need_com) {  // need combine
          if (OB_FAIL(this->at(this->size() - 1)->update((int64_t)0, new_text))) {
            LOG_WARN("fail to update unparse node", K(ret));
          }
        } else {
          if (OB_ISNULL(new_element = OB_NEWx(ObXmlElement, get_allocator(), ObMulModeNodeType::M_ELEMENT, ctx_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed", K(ret));
          } else {
            new_element->set_unparse(1);
            set_unparse(1);
            if (OB_FAIL(new_element->add_element(new_text))) {
              LOG_WARN("fail to add well form text", K(ret));
            } else if (OB_FAIL(this->add_element(new_element))) {
              LOG_WARN("fail to add well form element node", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

// remove namespace in array
int ObXmlElement::remove_ns_value_from_array(NsArray& ns_array, ObXmlAttribute* node)
{
  INIT_SUCC(ret);
  ObXmlAttribute* res = NULL;
  int64_t pos = -1;
  int64_t size_arr = ns_array.size();
  for (int64_t i = (size_arr - 1); i >= 0; i--) {
    if (0 == node->get_key().compare(ns_array.at(i)->get_key())
        && 0 == node->get_value().compare(ns_array.at(i)->get_value())) {
      pos = i;
      break;
    }
  }
  if (pos >= 0 && OB_FAIL(ns_array.remove(pos))) {
    LOG_WARN("fail to remove namespace from array", K(ret));
  }
  return ret;
}

// TODO down to top
int ObXmlElement::check_node_valid_with_ns(NsArray& ns_array, ObXmlNode* cur_node, int8_t operator_data, int8_t valid_type)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cur_node)) {
  } else {
    int64_t ns_count = 0;
    ObXmlAttribute* t_attr = NULL;
    switch(cur_node->type()) {
      case ObMulModeNodeType::M_DOCUMENT :
      case ObMulModeNodeType::M_CONTENT :
      case ObMulModeNodeType::M_ELEMENT : {
        ObXmlElement *cur_element = static_cast<ObXmlElement*>(cur_node);
        // add ns in cur node
        for (int i = 0; OB_SUCC(ret) && i < cur_element->attribute_size(); i ++) {
          if (operator_data == OperaType::APPEND
              && cur_element->attributes_->at(i)->type() == ObMulModeNodeType::M_NAMESPACE) {
            if (OB_FAIL(ns_array.push_back(dynamic_cast<ObXmlAttribute*>(cur_element->attributes_->at(i))))) {
              LOG_WARN("fail to add ns to array", K(ret));
            } else {
              ns_count ++;
            }
          }
        }
        // check ns valid in cur node
        if (OB_SUCC(ret) && cur_element->type() == ObMulModeNodeType::M_ELEMENT) {
          t_attr = get_ns_value_from_array(ns_array, cur_element->get_prefix());
          if (OB_NOT_NULL(t_attr)) {
            cur_element->set_ns(t_attr);
          } else if (cur_element->get_prefix().empty()) {
            t_attr = get_ns_value_from_array(ns_array, ObXmlConstants::XMLNS_STRING);
            if (OB_NOT_NULL(t_attr)) {
              cur_element->set_ns(t_attr);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to check element node ns", K(ret), K(cur_element->get_prefix()));
          }
        }
        // check attribute ns valid in cur_node
        for (int i = 0; OB_SUCC(ret) && i < cur_element->attribute_size(); i ++) {
          if (cur_element->attributes_->at(i)->type() == ObMulModeNodeType::M_ATTRIBUTE
              && OB_FAIL(SMART_CALL(check_node_valid_with_ns(ns_array, cur_element->attributes_->at(i), operator_data, valid_type)))) {
            LOG_WARN("failed to check attribute node", K(ret), K(i));
          }
        }
        // iterator child element
        for (int i = 0; valid_type == ValidType::ALL && OB_SUCC(ret) && i < cur_element->size(); i ++) {
          if (OB_FAIL(SMART_CALL(check_node_valid_with_ns(ns_array, cur_element->at(i), operator_data, valid_type)))) {
            LOG_WARN("failed to check element child node", K(ret), K(i));
          }
        }
        // delete ns from cur element
        for (int64_t i = 0; i < ns_count; i++) {
          ns_array.pop_back();
        }
        break;
      }
      case ObMulModeNodeType::M_ATTRIBUTE : {
        ObXmlAttribute *cur_attr = static_cast<ObXmlAttribute*>(cur_node);
        t_attr = NULL;
        if (cur_attr->get_prefix().empty()) { // default namespace do nothing
        } else if (OB_ISNULL(t_attr = get_ns_value_from_array(ns_array, cur_attr->get_prefix()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update this node", K(ret), K(cur_attr->get_prefix()));
        } else {
          cur_attr->set_ns(t_attr);
        }
        break;
      }
      case ObMulModeNodeType::M_NAMESPACE : {
        ObXmlAttribute *cur_attr = static_cast<ObXmlAttribute*>(cur_node);
        if (operator_data == OperaType::APPEND && OB_FAIL(ns_array.push_back(cur_attr))) { // add ns to map
          LOG_WARN("fail to add ns to array", K(ret), K(cur_attr->get_key()));
        } else if (operator_data == OperaType::DELETE && OB_FAIL(remove_ns_value_from_array(ns_array, cur_attr))) { // delete ns from array
          LOG_WARN("fail to delete ns in array", K(ret), K(cur_attr->get_key()));
        } else if (OB_FAIL(SMART_CALL(check_node_valid_with_ns(ns_array, cur_attr->get_parent(), operator_data, valid_type)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to check element from namespace", K(ret));
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObXmlElement::add_update_ns_map(NsMap &ns_map, ObString key, ObXmlNode* cur_node, bool overwrite)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(ns_map.get(key)) && !overwrite) { // not overwrite
    // do nothing
  } else if (OB_NOT_NULL(ns_map.get(key)) && OB_FAIL(ns_map.erase_refactored(key))) { // overwrite
    LOG_WARN("fail to delete ns from map", K(ret), K(key));
  } else if (OB_FAIL(ns_map.set_refactored(key, dynamic_cast<ObXmlAttribute*>(cur_node)))) {
    LOG_WARN("fail to add ns from map", K(ret), K(key));
  }
  return ret;
}

int ObXmlElement::update_attribute(ObXmlNode* xnode, int pos, bool ns_check)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(xnode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node is null", K(ret));
  } else if (!is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute node is null", K(ret));
  } else if (pos >= attributes_->count() || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret));
  } else {
    xnode->set_parent(this);

    ObXmlAttribute *cur_attr = static_cast<ObXmlAttribute*>(xnode);
    if (ns_check && cur_attr->type() == ObMulModeNodeType::M_NAMESPACE
                && OB_FAIL(Validate_XML_Tree_Legitimacy(xnode, OperaType::APPEND, ValidType::ALL))) {
      LOG_WARN("fail to upadate namespace", K(ret));
    } else if (ns_check && cur_attr->type() == ObMulModeNodeType::M_ATTRIBUTE
                && OB_FAIL(Validate_XML_Tree_Legitimacy(xnode, OperaType::APPEND))) {
      LOG_WARN("fail to namespace failed", K(ret));
    } else if (OB_FAIL(attributes_->update(pos, xnode))) {
      LOG_WARN("attribute update fail", K(ret));
    } else {
      xnode->set_parent(this);
    }
  }
  return ret;
}

bool ObXmlElement::has_flags(ObMulModeNodeFlag flag)
{
  bool res = false;
  if (flag & XML_DECL_FLAG) {
    res = has_xml_decl_;
  } else if (flag & XML_ENCODING_EMPTY_FLAG) {
    res = encoding_val_empty_;
  }

  return res;
}

int64_t ObXmlDocument::get_serialize_size()
{
  int64_t res = 0;
  if (serialize_size_ <= 0) {
    res = ObXmlElement::get_serialize_size();
  res += sizeof(uint64_t) * 3 + version_.length() + encoding_.length();
  serialize_size_ = res;
  } else {
    res = serialize_size_;
  }

  return res;
}

int ObXmlElement::remove_attribute(int pos)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute node is null", K(ret));
  } else if (pos >= attributes_->count() || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret), K(pos));
  } else {
    int64_t delta_size = -1 * attributes_->at(pos)->get_serialize_size();
    if (attributes_->at(pos)->type() != ObMulModeNodeType::M_ATTRIBUTE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("remove node is not attribute", K(ret), K(pos));
    } else if (OB_FAIL(attributes_->remove(pos))) {
      LOG_WARN("attribute update fail", K(ret));
    }
  }
  return ret;
}

int ObXmlElement::remove_namespace(int pos, bool ns_check)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute node is null", K(ret));
  } else if (pos >= attributes_->count() || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret), K(pos));
  } else {
    if (ns_check && OB_FAIL(Validate_XML_Tree_Legitimacy(attributes_->at(pos), OperaType::DELETE, ValidType::ALL))) {
      LOG_WARN("invalid xml tree after remove ns node", K(ret), K(pos));
    } else if (OB_FAIL(attributes_->remove(pos))) {
      LOG_WARN("attribute update fail", K(ret));
    }
  }
  return ret;
}

int ObXmlElement::get_attribute_list(ObIArray<ObIMulModeBase*> &value)
{
  INIT_SUCC(ret);
  ObArray<ObIMulModeBase*> t_value;
  if (!is_init_) {
  } else if (OB_FAIL((static_cast<ObXmlNode*>(attributes_))->get_children(t_value))) {
    LOG_WARN("get attribute list failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < t_value.size(); i++) {
      if (dynamic_cast<ObXmlNode*>(t_value.at(i))->type() == ObMulModeNodeType::M_ATTRIBUTE) {
        ret = value.push_back(t_value.at(i));
      }
    }
  }
  return ret;
}

int ObXmlElement::get_namespace_list(ObIArray<ObIMulModeBase*> &value)
{
  INIT_SUCC(ret);
  ObArray<ObIMulModeBase*> t_value;
  if (!is_init_) {
  } else if (OB_FAIL((static_cast<ObXmlNode*>(attributes_))->get_children(t_value))) {
    LOG_WARN("get attribute list failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < t_value.size(); i++) {
      if (dynamic_cast<ObXmlNode*>(t_value.at(i))->type() == ObMulModeNodeType::M_NAMESPACE) {
        ret = value.push_back(t_value.at(i));
      }
    }
  }
  return ret;
}

int ObXmlElement::get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx)
{
  INIT_SUCC(ret);
  ObXmlNode* handle = nullptr;
  if (OB_ISNULL(handle = static_cast<ObXmlNode*>(get_attribute_handle()))) {
  } else {
    iterator iter = handle->begin();

    for (int i = 0; OB_SUCC(ret) && !iter.end(); ++iter, ++i) {
      ObXmlNode* node = static_cast<ObXmlNode*>(*iter);
      if (node->type() != M_NAMESPACE) {
      } else {
        ObString tmp_prefix, tmp_ns_value;
        node->get_key(tmp_prefix);
        node->get_value(tmp_ns_value);

        if (prefix.compare(tmp_prefix) == 0) {
          ns_value = tmp_ns_value;
          ans_idx = i;
          break;
        }
      }
    }
  }

  return ret;
}

void ObXmlElement::set_ns(ObXmlAttribute* xnode)
{
  name_spaces_ = xnode;
}

ObIMulModeBase* ObXmlElement::attribute_at(int64_t pos, ObIMulModeBase* buffer)
{
  ObXmlAttribute* res = nullptr;
  get_attribute(res, pos);
  return res;
}

int ObXmlElement::get_attribute(ObXmlAttribute*& res, int64_t pos)
{
  INIT_SUCC(ret);
  if (pos < 0 || pos >= attribute_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get attr, index out of range", K(ret), K(attribute_size()), K(pos));
  } else if (OB_ISNULL(res = dynamic_cast<ObXmlAttribute*>(attributes_->at(pos)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get attr at pos", K(ret), K(pos));
  }
  return ret;
}

int ObXmlElement::get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags)
{
  INIT_SUCC(ret);

  if (filter_type == M_NAMESPACE) {
    if (flags) {
      if (OB_FAIL(get_namespace_default(res))) {
        LOG_WARN("failed to get default ns list", K(ret));
      }
    } else if (OB_FAIL(get_namespace_list(res))) {
      LOG_WARN("failed to get ns list", K(ret));
    }
  } else if (filter_type == M_ATTRIBUTE) {
    if (OB_FAIL(get_attribute_list(res))) {
      LOG_WARN("failed to get ns list", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get attr list", K(ret), K(filter_type));
  }

  return ret;
}

int ObXmlElement::get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2)
{
  INIT_SUCC(ret);

  if (filter_type == M_NAMESPACE) {
    res = get_ns_by_name(key1);
  } else if (filter_type == M_ATTRIBUTE) {
    res = get_attribute_by_name(key1, key2);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get attr list", K(ret), K(filter_type));
  }

  return ret;
}

bool ObXmlElement::check_if_defined_ns()
{
  bool ret_bool = false;
  ObArray<ObIMulModeBase*> t_value;
  if (!is_init_) {
  } else {
    static_cast<ObXmlNode*>(attributes_)->get_children(t_value);
    for (int64_t i = 0; i < t_value.size() && !ret_bool; i++) {
      if (dynamic_cast<ObXmlNode*>(t_value.at(i))->type() == ObMulModeNodeType::M_NAMESPACE) {
        ret_bool = true;
      }
    }
  }
  return ret_bool;
}

int ObXmlElement::add_attr_by_str(const ObString& name,
                                  const ObString& value,
                                  ObMulModeNodeType type,
                                  bool ns_check,
                                  int pos)
{
  INIT_SUCC(ret);
  ObXmlAttribute* new_node = NULL;
  if (OB_ISNULL(new_node = OB_NEWx(ObXmlAttribute, get_allocator(), type, ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    new_node->set_xml_key(name);
    new_node->set_value(value);
    if (OB_FAIL(this->add_attribute(new_node, ns_check, pos))) {
      LOG_WARN("fail to add attribute in element", K(ret), K(pos));
    }
  }
  return ret;
}

int ObXmlElement::get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend)
{
  INIT_SUCC(ret);
  (void)stk;
  if (prefix_.compare(ObXmlConstants::XML_STRING) == 0) {
    ns_value = ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI;
  } else if (OB_ISNULL(name_spaces_)) {
    // do nothing
  } else if (OB_FAIL(name_spaces_->get_value(ns_value))) {
    LOG_WARN("get namespace failed", K(ret));
  }
  return ret;
}

int ObXmlText::compare(const ObString& key, int& res)
{
  INIT_SUCC(ret);
  res = get_text().compare(key);
  return ret;
}

int64_t ObXmlAttribute::get_serialize_size()
{
  int64_t res = 0;
  if (serialize_size_ <= 0) {
    res += sizeof(uint8_t) * 3 + get_prefix().length() + sizeof(uint16_t) + get_value().length();
    serialize_size_ = res;
  } else {
    res = serialize_size_;
  }

  return res;
}

int ObXmlAttribute::compare(const ObString& key, int& res)
{
  res = key.compare(name_);
  return OB_SUCCESS;
}

int ObXmlAttribute::get_key(ObString& res, int64_t index) {
  UNUSED(index);
  res.assign_ptr(name_.ptr(), name_.length());
  return OB_SUCCESS;
}

int ObXmlAttribute::get_value(ObString& value, int64_t decrease_index_after) {
  UNUSED(decrease_index_after);
  value.assign_ptr(value_.ptr(), value_.length());
  return OB_SUCCESS;
}

int ObXmlAttribute::get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend)
{
  INIT_SUCC(ret);
  (void)stk;
  if (prefix_.compare(ObXmlConstants::XML_STRING) == 0) {
    ns_value = ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI;
  } else if (OB_ISNULL(ns_)) {
    // do nothing
  } else if (OB_FAIL(ns_->get_value(ns_value))) {
    LOG_WARN("get namespace failed", K(ret));
  }
  return ret;
}

// text get_key() is for sort child
int ObXmlText::get_key(ObString& res, int64_t index) {
  UNUSED(index);
  res = "";
  return OB_SUCCESS;
}

int64_t ObXmlText::get_serialize_size()
{
  int64_t res = 0;
  if (serialize_size_ == 0) {
    res += sizeof(uint8_t) * 2 + sizeof(uint64_t) + text_.length();
    serialize_size_ = res;
  } else {
    res = serialize_size_;
  }

  return res;
}

int ObXmlText::get_value(ObString& value, int64_t index) {
  UNUSED(index);
  value.assign_ptr(text_.ptr(), text_.length());
  return OB_SUCCESS;
};

} // namespace common
} // namespace oceanbase
