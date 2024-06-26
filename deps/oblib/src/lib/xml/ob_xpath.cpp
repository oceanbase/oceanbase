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
 * This file contains implementation support for the XML path abstraction.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "lib/xml/ob_xpath.h"
#include "lib/xml/ob_path_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "common/data_buffer.h"
#include <rapidjson/encodings.h>
#include <rapidjson/memorystream.h>

namespace oceanbase {
namespace common {
int ObPathCtx::init(ObMulModeMemCtx* ctx, ObIMulModeBase *doc_root, ObIMulModeBase *cur_doc,
                    ObIAllocator *tmp_alloc, bool is_auto_wrap, bool need_record, bool add_ns)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx) || OB_ISNULL(doc_root)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ctx_ = ctx;
    alloc_ = ctx->allocator_;
    tmp_alloc_ = tmp_alloc;
    doc_root_ = doc_root;
    cur_doc_ = cur_doc;
    is_auto_wrap_ = is_auto_wrap ? 1 : 0;
    need_record_ = need_record ? 1 : 0;
    add_ns_ = add_ns ? 1 : 0;
    is_inited_ = 1;
    defined_ns_ = 0;
    if (doc_root->data_type() == OB_XML_TYPE) {
      ret = bin_pool_.init(sizeof(ObXmlBin), tmp_alloc_);
      if (OB_FAIL(bin_pool_.init(sizeof(ObXmlBin), tmp_alloc_))) {
        LOG_WARN("fail to init binary pool", K(ret));
      } else if (OB_FAIL(init_extend())) {
        LOG_WARN("fail to init extend", K(ret));
      }
    } else if (doc_root->data_type() == OB_JSON_TYPE) {
      // ret = bin_pool_.init(sizeof(ObJsonBin), tmp_alloc_);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported yet", K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't be path node", K(ret));
    }
  }
  return ret;
}

int ObPathCtx::init_extend()
{
  INIT_SUCC(ret);
  if (doc_root_->check_extend()) {
    ObXmlBin* bin = static_cast<ObXmlBin*>(doc_root_);
    ObXmlBin extend(doc_root_->get_mem_ctx());
    extend_ = &extend;
    if (OB_FAIL(bin->get_extend(extend))) {
      LOG_WARN("fail to get extend", K(ret));
    } else if (OB_FAIL(bin->remove_extend())) {
      LOG_WARN("fail to remove extend", K(ret));
    } else if (OB_FAIL(alloc_new_bin(extend_))){
      LOG_WARN("fail init extend", K(ret));
    }
  } else {
    extend_ = nullptr; // without extend area
  }
  return ret;
}

int ObPathCtx::reinit(ObIMulModeBase* doc, ObIAllocator *tmp_alloc)
{
  INIT_SUCC(ret);
  cur_doc_ = doc;
  doc_root_ = doc;
  tmp_alloc_ = tmp_alloc;
  ancestor_record_.reset();
  bin_pool_.reset();
  defined_ns_ = 0;
  if (doc->data_type() == OB_XML_TYPE) {
    if (OB_FAIL(bin_pool_.init(sizeof(ObXmlBin), tmp_alloc_))) {
      LOG_WARN("fail to init binary pool", K(ret));
    } else if (OB_FAIL(init_extend())) {
      LOG_WARN("fail to init extend", K(ret));
    }
  } else if (doc->data_type() == OB_JSON_TYPE) {
    // ret = bin_pool_.init(sizeof(ObJsonBin), tmp_alloc_);
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't be path node", K(ret));
  }
  return ret;
}

int ObPathCtx::push_ancestor(ObIMulModeBase*& base_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(base_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (base_node->is_tree()) {
    if (OB_FAIL(ancestor_record_.push(base_node))) {
      LOG_WARN("should be inited", K(ret));
    }
  } else {
    if (add_ns_ && base_node->check_if_defined_ns()) {
      ++defined_ns_;
    }
    if (OB_FAIL(alloc_new_bin(base_node))) {
      LOG_WARN("allocate xmlbin failed", K(ret));
    } else if (OB_FAIL(ancestor_record_.push(base_node))) {
      LOG_WARN("should be inited", K(ret));
    }
  }
  return ret;
}

int ObPathCtx::alloc_new_bin(ObIMulModeBase*& base_node)
{
  INIT_SUCC(ret);
  if (base_node->is_binary()) {
    // is binary
    ObXmlBin* bin_node = static_cast<ObXmlBin*>(base_node);
    ObXmlBin* new_bin = static_cast<ObXmlBin*>(bin_pool_.alloc());
    if (OB_ISNULL(new_bin)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate xmlbin failed", K(ret));
    } else {
      new_bin = new (new_bin) ObXmlBin(*bin_node, base_node->get_mem_ctx());
      base_node = new_bin;
    }
  }
  return ret;
}

int ObPathCtx::alloc_new_bin(ObXmlBin*& bin, ObMulModeMemCtx* ctx)
{
  INIT_SUCC(ret);
  bin = static_cast<ObXmlBin*>(bin_pool_.alloc());
  if (OB_ISNULL(bin)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate xmlbin failed", K(ret));
  } else {
    bin = new (bin) ObXmlBin(ctx);
  }
  return ret;
}

int ObPathCtx::pop_ancestor()
{
  INIT_SUCC(ret);
  if (ancestor_record_.size() <= 0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fail to pop", K(ret));
  } else {
    ObIMulModeBase* top = ancestor_record_.top();
    if (OB_ISNULL(top)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (top->is_tree()) {
      ancestor_record_.pop();
    } else {
      if (add_ns_ && top->check_if_defined_ns()) {
        --defined_ns_;
      }
      ObXmlBin* bin_top = static_cast<ObXmlBin*>(top);
      ancestor_record_.pop();
      bin_pool_.free(bin_top);
    }
  }
  return ret;
}

ObIMulModeBase* ObPathCtx::top_ancestor()
{
  return ancestor_record_.top();
}

bool ObPathCtx::if_need_record() const
{
  return need_record_ == 1;
}

bool ObPathCtx::is_inited() const
{
  return is_inited_ == 1;
}

int ObPathLocationNode::init(const ObLocationType& location_type)
{
  INIT_SUCC(ret);
  if (location_type > ObLocationType::PN_LOCATION_ERROR
    && location_type < ObLocationType::PN_LOCATION_MAX) {
    node_type_.node_subtype_ = location_type;
    set_prefix_ns_info(false);
    set_default_prefix_ns(false);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init location node", K(ret));
  }
  return ret;
}

int ObPathLocationNode::init(const ObLocationType& location_type, const ObSeekType& seek_type)
{
  INIT_SUCC(ret);
  if (location_type > ObLocationType::PN_LOCATION_ERROR
    && location_type < ObLocationType::PN_LOCATION_MAX
    && seek_type > ObSeekType::ERROR_SEEK
    && seek_type < ObSeekType::MAX_SEEK) {
    node_type_.node_subtype_ = location_type;
    seek_type_ = seek_type;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init location node", K(ret));
  }
  return ret;
}

int ObPathLocationNode::init(const ObLocationType& location_type, const ObPathNodeAxis& axis_type)
{
  INIT_SUCC(ret);
  if (location_type > ObLocationType::PN_LOCATION_ERROR
    && location_type < ObLocationType::PN_LOCATION_MAX
    && axis_type > ObPathNodeAxis::ERROR_AXIS
    && axis_type < ObPathNodeAxis::MAX_AXIS) {
    node_type_.node_subtype_ = location_type;
    node_axis_ = axis_type;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init location node", K(ret));
  }
  return ret;
}

int ObPathLocationNode::init(const ObLocationType& location_type, const ObSeekType& seek_type, const ObPathNodeAxis& axis_type)
{
  INIT_SUCC(ret);
  if (location_type > ObLocationType::PN_LOCATION_ERROR
    && location_type < ObLocationType::PN_LOCATION_MAX
    && seek_type > ObSeekType::ERROR_SEEK
    && seek_type < ObSeekType::MAX_SEEK
    && axis_type > ObPathNodeAxis::ERROR_AXIS
    && axis_type < ObPathNodeAxis::MAX_AXIS) {
    node_type_.node_subtype_ = location_type;
    seek_type_ = seek_type;
    node_axis_ = axis_type;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init location node", K(ret));
  }
  return ret;
}

int ObPathFilterNode::init(const ObXpathFilterChar& filter_char, ObPathNode* left, ObPathNode* right, bool pred)
{
  INIT_SUCC(ret);
  ObFilterType type = ObFilterType::PN_FILTER_ERROR;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(ObPathUtil::char_to_filter_type(filter_char, type))) {
    LOG_WARN("fail to get filter type", K(ret));
  } else {
    node_type_.set_filter_type(type);
    if (ObPathParserUtil::is_boolean_ans(type)) {
      is_boolean_ = true;
    }
    in_predication_ = pred;
    contain_relative_path_ = (ObPathUtil::check_contain_relative_path(left) || ObPathUtil::check_contain_relative_path(right));
    need_cache_ = (ObPathUtil::check_need_cache(left) || ObPathUtil::check_need_cache(right));
    if (OB_FAIL(this->append(left))) {
      LOG_WARN("fail to append arg", K(ret));
    } else if (OB_FAIL(this->append(right))) {
      LOG_WARN("fail to append arg", K(ret));
    } else if (!pred && ObPathParserUtil::is_illegal_comp_for_filter(type, left, right)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("Given XPATH expression not supported", K(ret));
    }
  }
  return ret;
}

int ObPathFilterOpNode::append_filter(ObPathNode* filter)
{
  INIT_SUCC(ret);
  if (!contain_relative_path_ && filter->contain_relative_path_) {
    contain_relative_path_ = true;
  }
  if (!need_cache_ && filter->need_cache_) {
    need_cache_ = true;
  }
  if (OB_FAIL(append(filter))) {
    LOG_WARN("fail to append filter", K(ret));
  }
  return ret;
}

int ObPathFilterNode::init(ObFilterType type)
{
  INIT_SUCC(ret);
  if (type > ObFilterType::PN_FILTER_ERROR && type < ObFilterType::PN_FILTER_MAX) {
    node_type_.set_filter_type(type);
  } else {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_WARN("fail to init func", K(ret));
  }
  return ret;
}

int ObPathFuncNode::init(ObFuncType& func_type)
{
  INIT_SUCC(ret);
  if (func_type > ObFuncType::PN_FUNC_ERROR && func_type < ObFuncType::PN_FUNC_MAX) {
    node_type_.set_func_type(func_type);
    min_arg_num_ = func_arg_num[func_type - ObFuncType::PN_ABS][0];
    max_arg_num_ = func_arg_num[func_type - ObFuncType::PN_ABS][1];
  } else {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_WARN("fail to init func", K(ret));
  }
  return ret;
}

int ObPathArgNode::init(char* str, uint64_t len, bool pred)
{
  INIT_SUCC(ret);
  node_type_.set_arg_type(ObArgType::PN_STRING);
  arg_.str_.name_ = str;
  arg_.str_.len_ = len;
  in_predication_ = pred;
  return ret;
}

int ObPathArgNode::init(double num, bool pred)
{
  INIT_SUCC(ret);
  node_type_.node_subtype_ = ObArgType::PN_DOUBLE;
  arg_.double_ = num;
  in_predication_ = pred;
  return ret;
}

int ObPathArgNode::init(bool boolean, bool pred)
{
  INIT_SUCC(ret);
  node_type_.node_subtype_ = ObArgType::PN_BOOLEAN;
  arg_.boolean_ = boolean;
  in_predication_ = pred;
  return ret;
}

int ObPathArgNode::init(ObPathNode* node, bool pred)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    node_type_.node_subtype_ = ObArgType::PN_SUBPATH;
    arg_.subpath_ = node;
    in_predication_ = pred;
  }
  return ret;
}

void ObPathLocationNode::set_nodetest_by_name(ObSeekType seek_type, const char* name, uint64_t len)
{
  seek_type_ = seek_type;
  if (seek_type_ == ObSeekType::PROCESSING_INSTRUCTION) {
    node_content_.key_.len_ = len;
    node_content_.key_.name_ = name;
    len > 0 ? set_wildcard_info(false) : set_wildcard_info(true);
  } else {
    set_wildcard_info(true);
  }
  check_namespace_ = false;
}

// when there is not nodetest, set seek type by axis
void ObPathLocationNode::set_nodetest_by_axis()
{
  if (node_axis_ != ObPathNodeAxis::NAMESPACE && node_axis_ != ObPathNodeAxis::ATTRIBUTE) {
    seek_type_ = ObSeekType::ELEMENT;
  }
}

int ObPathLocationNode::set_check_ns_by_nodetest(ObIAllocator *allocator, ObString& default_ns)
{
  INIT_SUCC(ret);
  if (check_namespace_) { // with prefix ns
    // make sure is element or attribute
    if (seek_type_ != ObSeekType::ELEMENT
        && node_axis_ != ObPathNodeAxis::ATTRIBUTE
        && node_axis_ !=  ObPathNodeAxis::NAMESPACE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("must be element or attribute when there is prefix ns.", K(ret), K(seek_type_));
    }
  } else {
    // without prefix ns
    // if has wildcard , don't need check ns
    // else use default ns when is element, use null ns when is attribute
    if (!node_content_.has_wildcard_) {
      check_namespace_ = true;
      // for attribute, if there is no prefix ns, use null as default ns
      if (node_axis_ == ObPathNodeAxis::ATTRIBUTE) {
        set_ns_info(nullptr, 0);
      } else if (node_axis_ == ObPathNodeAxis::NAMESPACE) {
      // for namespace, mustn't have prefix ns(checked)
      // if there is tag name after namespace axis, find first prefix ns, else find first default ns
        set_ns_info(nullptr, 0);
      } else if (seek_type_ == ObSeekType::ELEMENT) {
      // for element, if there is not prefix ns
      // if default ns is null, use null ns, else use default ns anyway
        ObString ns_str;
        if (default_ns.length() != 0) {
          if (OB_FAIL(ob_write_string(*allocator, default_ns, ns_str))) {
            LOG_WARN("fail to wirte string", K(ret), K(default_ns));
          }
        }
        if (OB_SUCC(ret)) set_ns_info(ns_str.ptr(), ns_str.length());
      }
    } else {
      check_namespace_ = false;
    }
  } // do not have prefix ns
  return ret;
}

int ObPathRootNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_root()) {
    if (node_type_.is_xml_path() && size() == 0) {
      if (OB_FAIL(str.append("/"))) {
        LOG_WARN("root fail to string", K(ret), K(str));
      }
    } /*else if (node_type_.is_json_path()) {
      if (OB_FAIL(str.append("$"))) {
        LOG_WARN("root fail to string", K(ret), K(str));
    }*/
    if (OB_SUCC(ret) && size() > 0) {
      for (int64_t i = 0; i < size() && OB_SUCC(ret); ++i) {
        ObPathNode* temp_node = static_cast<ObPathNode*>(member(i));
        if (OB_ISNULL(temp_node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null", K(ret), K(str));
        } else if (OB_FAIL(temp_node->node_to_string(str))) {
          LOG_WARN("location node fail to string", K(ret), K(str));
        }
      } // end for
    } // end child is null
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be root node", K(ret), K(str));
  }
  return ret;
}

bool ObPathRootNode::is_abs_subpath()
{
  bool is_absolute = false;
  ObPathNode* first_node = static_cast<ObPathNode*>(member(0));
  if (OB_NOT_NULL(first_node) && (first_node->node_type_.is_location())) {
    ObPathLocationNode* location = static_cast<ObPathLocationNode*>(first_node);
    is_absolute = location->is_absolute_;
  }
  return is_absolute;
}

int ObPathLocationNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    if (is_absolute_ && OB_FAIL(str.append("/"))) {
      LOG_WARN("fail to append slash", K(ret));
    } else if (node_type_.get_location_type() == ObLocationType::PN_ELLIPSIS && OB_FAIL(str.append("/"))) {
    } else if (OB_FAIL(axis_to_string(str))) {
      LOG_WARN("fail to append axis", K(ret));
    } else if (OB_FAIL(nodetest_to_string(str))) {
      LOG_WARN("fail to append nodetest", K(ret));
    } else if (has_filter_ && size() > 0) {
      for (int64_t i = 0; i < size() && OB_SUCC(ret); ++i) {
        ObPathNode* temp_node = static_cast<ObPathNode*>(member(i));
        if (OB_ISNULL(temp_node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null", K(ret), K(str));
        } else if (OB_FAIL(str.append("["))) {
          LOG_WARN("fail to append [", K(ret));
        } else if (OB_FAIL(temp_node->node_to_string(str))) {
          LOG_WARN("location node fail to string", K(ret), K(str));
        } else if (OB_FAIL(str.append("]"))) {
          LOG_WARN("fail to append ]", K(ret));
        }
      }
    } // if without filter, do nothing
  } // if not xml node
  return ret;
}

int ObPathLocationNode::axis_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_axis_ != ObPathNodeAxis::CHILD) {
    if (OB_FAIL(str.append(axis_str_map[node_axis_ - ObPathNodeAxis::SELF]))) {
      LOG_WARN("fail to append axis", K(axis_str_map[node_axis_ - ObPathNodeAxis::SELF]), K(ret));
    }
  }
  return ret;
}
int ObPathLocationNode::nodetest_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (check_namespace_ && OB_NOT_NULL(node_content_.namespace_.name_) && node_content_.namespace_.len_ > 0) {
    if (OB_FAIL(str.append(node_content_.namespace_.name_, node_content_.namespace_.len_))) {
      LOG_WARN("fail to append axis", K(ret));
    } else if (OB_FAIL(str.append(":"))) {
      LOG_WARN("fail to append :", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    switch (seek_type_) {
      case ObSeekType::NODES:
      case ObSeekType::TEXT:
      case ObSeekType::COMMENT: {
        if (OB_FAIL(str.append(nodetest_str_map[seek_type_ - ObSeekType::NODES]))) {
          LOG_WARN("fail to append axis", K(nodetest_str_map[seek_type_ - ObSeekType::NODES]), K(ret));
        }
        break;
      }
      case ObSeekType::PROCESSING_INSTRUCTION: {
        if (OB_FAIL(str.append(ObPathItem::PROCESSING_INSTRUCTION))) {
          LOG_WARN("fail to append processing_instruction", K(ret));
        } else if (node_content_.key_.len_ > 0 && OB_FAIL(str.append("\""))) {
          LOG_WARN("fail to append \"", K(ret));
        } else if (OB_FAIL(str.append(node_content_.key_.name_, node_content_.key_.len_))) {
          LOG_WARN("fail to append processing_instruction name", K(node_content_.key_.name_), K(ret));
        } else if (node_content_.key_.len_ > 0 &&OB_FAIL(str.append("\""))) {
          LOG_WARN("fail to append \"", K(ret));
        } else if (OB_FAIL(str.append(")"))) {
          LOG_WARN("fail to append )", K(ret));
        }
        break;
      }
      default: {
        if (node_content_.has_wildcard_) {
          if (OB_FAIL(str.append("*"))) {
            LOG_WARN("fail to append *", K(ret));
          }
        } else if (OB_FAIL(str.append(node_content_.key_.name_, node_content_.key_.len_))) {
          LOG_WARN("fail to append key name", K(node_content_.key_.name_), K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObPathFilterNode::filter_arg_to_string(ObStringBuffer& str, bool is_left)
{
  INIT_SUCC(ret);
  int index = (is_left)? 0 : 1;
  ObPathNode* temp_node = static_cast<ObPathNode*>(member(index));
  if (OB_ISNULL(temp_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret), K(str));
  } else if (OB_FAIL(temp_node->node_to_string(str))) {
    LOG_WARN("location node fail to string", K(ret), K(str));
  }
  return ret;
}

int ObPathFilterNode::filter_type_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    ObFilterType xml_filter = node_type_.get_filter_type();
    if (xml_filter >= ObFilterType::PN_CMP_UNION && xml_filter <= ObFilterType::PN_CMP_MOD) {
      if (OB_FAIL(str.append(filter_type_str_map[xml_filter - ObFilterType::PN_NOT_COND]))) {
        LOG_WARN("fail to append axis",
        K(filter_type_str_map[xml_filter - ObFilterType::PN_NOT_COND]), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrong filter type", K(ret), K(xml_filter));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  }
  return ret;
}

int ObPathFilterNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    if (size() != 2) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("wrong arg num", K(size()), K(ret));
    } else if (OB_FAIL(filter_arg_to_string(str, true))) {
      LOG_WARN("left arg fail to str", K(size()), K(ret));
    } else if (OB_FAIL(filter_type_to_string(str))) {
      LOG_WARN("filter type fail to str", K(size()), K(ret));
    } else if (OB_FAIL(filter_arg_to_string(str, false))) {
      LOG_WARN("rigth arg fail to str", K(size()), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  }
  return ret;
}

int ObPathFilterOpNode::filter_op_arg_to_str(bool is_left, ObStringBuffer& str)
{
  INIT_SUCC(ret);
  ObPathNode* node = is_left ? left_ : right_;
  if (node->get_node_type().is_root()) {
    ObPathRootNode* root = static_cast<ObPathRootNode*>(node);
    if (!root->is_abs_path_ && root->size() > 0) {
      ObPathNode* first = static_cast<ObPathNode*>(root->member(0));
      if (first->get_node_type().is_location()
        && first->get_node_type().get_location_type() != PN_ELLIPSIS
        && OB_FAIL(str.append("/"))) {
          LOG_WARN("fail to append /", K(ret));
      }
    }
  }
  return ret;
}

int ObPathFilterOpNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    if (OB_ISNULL(left_)) {
    } else if (OB_FAIL(filter_op_arg_to_str(true, str))) {
    } else if (OB_FAIL(left_->node_to_string(str))) {
      LOG_WARN("left arg fail to str", K(size()), K(ret));
    } else {
      for (int64_t i = 0; i < size() && OB_SUCC(ret); ++i) {
        ObPathNode* temp_node = static_cast<ObPathNode*>(member(i));
        if (OB_ISNULL(temp_node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null", K(ret), K(str));
        } else if (OB_FAIL(str.append("["))) {
          LOG_WARN("fail to append [", K(ret));
        } else if (OB_FAIL(temp_node->node_to_string(str))) {
          LOG_WARN("location node fail to string", K(ret), K(str));
        } else if (OB_FAIL(str.append("]"))) {
          LOG_WARN("fail to append ]", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(right_)) {
    } else if (OB_FAIL(filter_op_arg_to_str(false, str))) {
    } else if (OB_FAIL(right_->node_to_string(str))) {
      LOG_WARN("right arg fail to str", K(size()), K(ret));
    } // right to string
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  }
  return ret;
}

int ObPathFuncNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    ObFuncType xml_func = node_type_.get_func_type();
    if (OB_FAIL(str.append(func_str_map[xml_func - ObFuncType::PN_ABS]))) {
      LOG_WARN("fail to append function",
      K(func_str_map[xml_func - ObFuncType::PN_ABS]), K(ret));
    } else if (OB_FAIL(func_arg_to_string(str))) {
      LOG_WARN("arg fail to append )", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  }
  return ret;
}

int ObPathFuncNode::func_arg_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (size() > 0) {
    for (int64_t i = 0; i < size() && OB_SUCC(ret); ++i) {
      ObPathNode* temp_node = static_cast<ObPathNode*>(member(i));
      if (OB_ISNULL(temp_node)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret), K(str));
      } else if (OB_FAIL(temp_node->node_to_string(str))) {
        LOG_WARN("location node fail to string", K(ret), K(str));
      } else if ( i + 1 < size() && OB_FAIL(str.append(", "))) {
        LOG_WARN("fail to append ','", K(ret));
      }
    } // end for
  }// end child is null

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(str.append(")"))) {
    LOG_WARN("fail to append ')'", K(ret));
  }
  return ret;
}

int ObPathArgNode::node_to_string(ObStringBuffer& str)
{
  INIT_SUCC(ret);
  if (node_type_.is_xml_path()) {
    ObArgType xml_arg = node_type_.get_arg_type();
    switch (xml_arg) {
      case ObArgType::PN_STRING: {
        if (OB_FAIL(str.append("\""))) {
          LOG_WARN("fail to append quote", K(ret));
        } else if (OB_FAIL(str.append(arg_.str_.name_, arg_.str_.len_))) {
          LOG_WARN("fail to append literal", K(ret));
        } else if (OB_FAIL(str.append("\""))) {
          LOG_WARN("fail to append quote", K(ret));
        }
        break;
      }
      case ObArgType::PN_DOUBLE: {
        char buf[DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE] = {0};
        uint64_t length = ob_gcvt(arg_.double_, ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                                  sizeof(buf) - 1, buf, NULL);
        if (length== 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail to convert double to string", K(ret));
        } else {
          ObString num_str(sizeof(buf), static_cast<int32_t>(length), buf);
          if (OB_FAIL(str.append(num_str))) {
            LOG_WARN("fail to set j_buf len", K(ret), K(str.length()), K(num_str));
          }
        }
        break;
      }
      case ObArgType::PN_SUBPATH: {
        if (OB_ISNULL(arg_.subpath_)) {
           ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null", K(ret), K(str));
        } else if (OB_FAIL(arg_.subpath_->node_to_string(str))) {
          LOG_WARN("fail to append processing_instruction name", K(ret));
        }
        break;
      }
      case ObArgType::PN_BOOLEAN: {
        if (arg_.boolean_ == true) {
          if (OB_FAIL(str.append("true"))) {
            LOG_WARN("fail to append true", K(ret));
          }
        } else if (OB_FAIL(str.append("false"))) {
          LOG_WARN("fail to append false", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Wrong arg type", K(ret), K(xml_arg));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet", K(ret));
  }
  return ret;
}

int ObPathUtil::alloc_seek_result(ObIAllocator *allocator, ObIMulModeBase* base, ObSeekResult*& res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    res = static_cast<ObSeekResult*> (allocator->alloc(sizeof(ObSeekResult)));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at seek result", K(ret));
    } else {
      res = new (res) ObSeekResult(false);
      res->result_.base_ = base;
    }
  }
  return ret;
}

int ObPathUtil::alloc_seek_result(ObIAllocator *allocator, ObPathArgNode* arg, ObSeekResult*& res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    res = static_cast<ObSeekResult*> (allocator->alloc(sizeof(ObSeekResult)));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at seek result", K(ret));
    } else {
      res = new (res) ObSeekResult(true);
      res->result_.scalar_ = arg;
    }
  }
  return ret;
}

int ObPathUtil::trans_scalar_to_base(ObIAllocator *allocator, ObPathArgNode* arg, ObIMulModeBase*& base)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator) || OB_ISNULL(arg)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObXmlText* res = static_cast<ObXmlText*> (allocator->alloc(sizeof(ObXmlText)));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at seek result", K(ret));
    } else {
      res = new (res) ObXmlText(ObMulModeNodeType::M_TEXT);
      if (arg->node_type_.get_arg_type() == ObArgType::PN_STRING) {
        res->set_text(ObString(arg->arg_.str_.len_, arg->arg_.str_.name_));
      } else {
        ObStringBuffer buf(allocator);
        if (OB_FAIL(arg->node_to_string(buf))) {
          LOG_WARN("fail to string", K(ret));
        } else {
          res->set_text(ObString(buf.length(), buf.ptr()));
          base = res;
        }
      }
    } // alloc success
  }
  return ret;
}

int ObPathFuncNode::check_is_all_location_without_filter(ObPathNode* arg_root)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(arg_root) || !arg_root->node_type_.is_root() || arg_root->size() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be root", K(ret));
  } else {
    for (int i = 0; i < arg_root->size() && OB_SUCC(ret); ++i) {
      ObPathNode* node = static_cast<ObPathNode*>(arg_root->member(i));
      if (OB_ISNULL(node)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret));
      } else if (node->node_type_.is_location()) {
        ObPathLocationNode* location = static_cast<ObPathLocationNode*>(node);
        if (location->has_filter_) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("Given XPATH expression not supported", K(ret));
        } // check if without filter
      } else if (node->node_type_.is_func()) {
        ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
        LOG_WARN("Function call with invalid number of arguments", K(ret), K(node->node_type_.node_class_));
      }
    } // end for
  }
  return ret;
}

int ObPathFuncNode::check_is_legal_count_arg()
{
  INIT_SUCC(ret);
  // after size check, size must be 1
  ObPathNode* func_arg = static_cast<ObPathNode*>(member(0));
  if (OB_ISNULL(func_arg) || func_arg->node_type_.is_location()) {
    ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
    LOG_WARN("Function call with invalid arguments", K(ret), K(func_arg->node_type_.node_class_));
  }

  return ret;
}

int ObPathFuncNode::checek_cache_and_abs()
{
  INIT_SUCC(ret);
  switch(get_node_type().get_func_type()) {
    case ObFuncType::PN_FALSE:
    case ObFuncType::PN_TRUE: {
      need_cache_ = false;
      contain_relative_path_ = false;
      break;
    }
    case ObFuncType::PN_COUNT:
    case ObFuncType::PN_NOT_FUNC:
    case ObFuncType::PN_BOOLEAN_FUNC: {
      if (size() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Function call with invalid arguments", K(ret), K(size()));
      } else {
        ObPathNode* path = static_cast<ObPathNode*>(member(0));
        need_cache_ = path->need_cache_;
        contain_relative_path_ = path->contain_relative_path_;
      }
      break;
    }
    case ObFuncType::PN_ABS:
    case ObFuncType::PN_BOOL_ONLY:
    case ObFuncType::PN_CEILING:
    case ObFuncType::PN_DATE_FUNC:
    case ObFuncType::PN_DOUBLE_FUNC:
    case ObFuncType::PN_FLOOR:
    case ObFuncType::PN_LENGTH:
    case ObFuncType::PN_LOWER:
    case ObFuncType::PN_NUMBER_FUNC:
    case ObFuncType::PN_NUM_ONLY:
    case ObFuncType::PN_SIZE:
    case ObFuncType::PN_STRING_FUNC:
    case ObFuncType::PN_STR_ONLY:
    case ObFuncType::PN_TIMESTAMP:
    case ObFuncType::PN_TYPE:
    case ObFuncType::PN_UPPER:
    case ObFuncType::PN_CONCAT:
    case ObFuncType::PN_CONTAINS:
    case ObFuncType::PN_LOCAL_NAME:
    case ObFuncType::PN_LANG:
    case ObFuncType::PN_SUM:
    case ObFuncType::PN_NAME:
    case ObFuncType::PN_NS_URI:
    case ObFuncType::PN_NORMALIZE_SPACE:
    case ObFuncType::PN_SUBSTRING_FUNC:
    case ObFuncType::PN_POSITION:
    case ObFuncType::PN_LAST:
    case ObFuncType::PN_ROUND: {
      need_cache_ = true;
      contain_relative_path_ = true;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("axis not supported yet", K(ret));
      break;
    }
  }
  return ret;
}

int ObPathFuncNode::check_is_legal_arg()
{
  INIT_SUCC(ret);
  if (min_arg_num_ > size() || max_arg_num_ < size()) { // check_arg_num
    ret = OB_ERR_PARSER_SYNTAX; // ORA-31011: XML parsing failed
    LOG_WARN("Function call with invalid number of arguments", K(ret), K(min_arg_num_), K(max_arg_num_));
  } else { // check arg type
    switch (node_type_.get_func_type()) {
      case ObFuncType::PN_COUNT: {
        if (OB_FAIL(check_is_legal_count_arg())) {
          LOG_WARN("Function call with invalid arguments for count", K(ret));
        }
        break;
      }
      default: {
        break;
      }
    } // end of check arg type
  }
  return ret;
}

int ObPathNode::node_to_string(ObStringBuffer& str)
{
  return OB_NOT_SUPPORTED;
}

int ObPathNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  return OB_NOT_SUPPORTED;
}

int ObPathRootNode::init_adapt(ObPathCtx &ctx, ObIMulModeBase*& ans)
{
  INIT_SUCC(ret);
  ans = (is_abs_path_ = is_abs_subpath()) ? ctx.doc_root_ : ctx.cur_doc_;
  if (adapt_.size() == 0) { // alloc and init for the first time
    for (int i = 0; i < size() && OB_SUCC(ret); ++i) {
      ObPathLocationNode* loc = static_cast<ObPathLocationNode*>(member(i));
      ObSeekIterator* ada = nullptr;
      if (OB_ISNULL(ans) || OB_ISNULL(loc)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ans), K(loc));
      } else if (OB_FAIL(ObPathUtil::get_seek_iterator(ctx.alloc_, loc, ada))) {
        LOG_WARN("fail to alloc ada", K(ret));
      } else if (OB_FAIL(ada->init(ctx, loc, ans))) {
        LOG_WARN("fail to init ada", K(ret));
      } else if (OB_FAIL(adapt_.push_back(ada))) {
        LOG_WARN("fail to push ada", K(ret));
      } else {
        ++iter_pos_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(next_adapt(ctx, ans))) {
        LOG_WARN("fail to get next", K(ret));
      } else if (OB_ISNULL(ans)) {
        ret = OB_ITER_END;
      }
    } // end for
  } else {
    for (int i = 0; i < size() && iter_pos_ < adapt_.size() && OB_SUCC(ret); ++i) {
      ObSeekIterator* ada = nullptr;
      if (i < adapt_.size()) {
        ada = adapt_[i];
        ada->reset(ans);
      } else {
        ObPathLocationNode* loc = static_cast<ObPathLocationNode*>(member(i));
        ObSeekIterator* ada = nullptr;
        if (OB_ISNULL(ans) || OB_ISNULL(loc)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null", K(ans), K(loc));
        } else if (OB_FAIL(ObPathUtil::get_seek_iterator(ctx.alloc_, loc, ada))) {
          LOG_WARN("fail to alloc ada", K(ret));
        } else if (OB_FAIL(ada->init(ctx, loc, ans))) {
          LOG_WARN("fail to init ada", K(ret));
        } else if (OB_FAIL(adapt_.push_back(ada))) {
          LOG_WARN("fail to push ada", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ++iter_pos_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(next_adapt(ctx, ans))) {
        LOG_WARN("fail to get next", K(ret));
      } else if (OB_ISNULL(ans)) {
        ret = OB_ITER_END;
      }
    } // end for
  }

  is_seeked_ = true;
  return ret;
}

int ObPathRootNode::next_adapt(ObPathCtx &ctx, ObIMulModeBase*& ans)
{
  INIT_SUCC(ret);
  if (iter_pos_ >= adapt_.size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong pos", K(ret));
  } else if (iter_pos_ < 0) {
    for (int i = 0; i < adapt_.size() && OB_NOT_NULL(adapt_[i]); ++i) {
      adapt_[i]->reset();
    }
    is_seeked_ = false;
    iter_pos_  = -1;
    ret = OB_ITER_END;
  } else {
    ObIMulModeBase* tmp_ans = nullptr;
    ObSeekIterator* top = adapt_[iter_pos_];
    if (OB_ISNULL(top)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (OB_SUCC(top->next(ctx, tmp_ans)) && OB_NOT_NULL(tmp_ans)) {
      ans = tmp_ans;
    } else {
      --iter_pos_;
      ObIMulModeBase* new_root = nullptr;
      if (OB_SUCC(next_adapt(ctx, new_root)) && OB_NOT_NULL(new_root)) {
        top->reset(new_root);
        ++iter_pos_;
        if (OB_SUCC(next_adapt(ctx, tmp_ans)) && OB_NOT_NULL(tmp_ans)) {
          ans = tmp_ans;
        } else {
          ret = OB_ITER_END;
        }
      } else { // fail to update root, search end
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObPathRootNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* ans = nullptr;
  if (!ctx.is_inited()) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else if (!node_type_.is_root()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be root node", K(ret));
  } else if (size() == 0) { // last path node
    if (!is_seeked_) {
      ans = is_abs_path_ ? ctx.doc_root_ : ctx.cur_doc_;
      is_seeked_ = true;
    } else {
      ret = OB_ITER_END;
      is_seeked_ = false;
    }
  } else if (!is_seeked_ || iter_pos_ == -1) {
    if (OB_FAIL(init_adapt(ctx, ans))) {
      LOG_WARN("fail to init", K(ret));
    }
  } else if (OB_FAIL(next_adapt(ctx, ans))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ans)) {
    ret = OB_ITER_END;
  } else {
    res.is_scalar_ = false;
    res.result_.base_ = ans;
  }
  return ret;
}

 int ObPathLocationNode::set_seek_info(ObPathSeekInfo& seek_info)
 {
  INIT_SUCC(ret);
  ObLocationType sub_type = ObLocationType(node_type_.node_subtype_);
  bool wildcard = get_wildcard_info();
  if (sub_type == ObLocationType::PN_KEY) {
    if (node_axis_ == ATTRIBUTE || node_axis_ == NAMESPACE) {
      seek_info.type_ = SimpleSeekType::ATTR_KEY;
      if (node_content_.key_.len_ > 0) {
        seek_info.key_ = get_key_name();
      } else {
        seek_info.key_ = ObString(0, nullptr);
      }
    } else if (wildcard || node_axis_ == DESCENDANT_OR_SELF || node_axis_ == DESCENDANT) {
      seek_info.type_ = SimpleSeekType::ALL_KEY_TYPE;
    } else {
      seek_info.type_ = SimpleSeekType::KEY_TYPE;
      seek_info.key_ = ObString(node_content_.key_.len_, node_content_.key_.name_);
    }
  } else if (sub_type == ObLocationType::PN_ELLIPSIS) {
    seek_info.type_ = SimpleSeekType::ALL_KEY_TYPE;
  } else if (sub_type == ObLocationType::PN_ARRAY) {
    if (wildcard) {
      seek_info.type_ = SimpleSeekType::ALL_ARR_TYPE;
    } else {
      seek_info.type_ = SimpleSeekType::INDEX_TYPE;
      // seek_info.index_ = node_content_.index;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be sub location type", K(ret));
  }
  return ret;
}

int ObPathLocationNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  return OB_NOT_SUPPORTED; // should finish by SeekIterator
}

int ObPathFuncNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  switch(node_type_.get_func_type()) {
    case ObFuncType::PN_FALSE:
    case ObFuncType::PN_TRUE: {
      if (OB_FAIL(eval_true_or_false(ctx, node_type_.get_func_type() == ObFuncType::PN_TRUE, res))) {
        LOG_WARN("fail to eval position", K(ret));
      }
      break;
    }
    case ObFuncType::PN_COUNT: {
      if (OB_FAIL(eval_count(ctx, res))) {
        LOG_WARN("fail to eval position", K(ret));
      }
      break;
    }
    case ObFuncType::PN_NOT_FUNC:
    case ObFuncType::PN_BOOLEAN_FUNC:
    case ObFuncType::PN_ABS:
    case ObFuncType::PN_BOOL_ONLY:
    case ObFuncType::PN_CEILING:
    case ObFuncType::PN_DATE_FUNC:
    case ObFuncType::PN_DOUBLE_FUNC:
    case ObFuncType::PN_FLOOR:
    case ObFuncType::PN_LENGTH:
    case ObFuncType::PN_LOWER:
    case ObFuncType::PN_NUMBER_FUNC:
    case ObFuncType::PN_NUM_ONLY:
    case ObFuncType::PN_SIZE:
    case ObFuncType::PN_STRING_FUNC:
    case ObFuncType::PN_STR_ONLY:
    case ObFuncType::PN_TIMESTAMP:
    case ObFuncType::PN_TYPE:
    case ObFuncType::PN_UPPER:
    case ObFuncType::PN_CONCAT:
    case ObFuncType::PN_CONTAINS:
    case ObFuncType::PN_LOCAL_NAME:
    case ObFuncType::PN_LANG:
    case ObFuncType::PN_SUM:
    case ObFuncType::PN_NAME:
    case ObFuncType::PN_NS_URI:
    case ObFuncType::PN_NORMALIZE_SPACE:
    case ObFuncType::PN_SUBSTRING_FUNC:
    case ObFuncType::PN_POSITION:
    case ObFuncType::PN_LAST:
    case ObFuncType::PN_ROUND: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("axis not supported yet", K(ret));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("axis not supported yet", K(ret));
      break;
    }
  }
  return ret;
}

bool ObPathUtil::is_filter_nodetest(const ObSeekType& seek_type)
{
  bool ret_bool = false;
  switch (seek_type)
  {
    case ObSeekType::COMMENT:
    case ObSeekType::PROCESSING_INSTRUCTION:{
      ret_bool = true;
      break;
    }
    default:
      break;
  }
  return ret_bool;
}

bool ObPathUtil::is_upper_axis(const ObPathNodeAxis& axis)
{
  bool ret_bool = false;
  switch (axis)
  {
    case ObPathNodeAxis::PARENT:
    case ObPathNodeAxis::ANCESTOR:
    case ObPathNodeAxis::ANCESTOR_OR_SELF: {
      ret_bool = true;
      break;
    }
    default:
      break;
  }
  return ret_bool;
}

bool ObPathUtil::is_down_axis(const ObPathNodeAxis& axis)
{
  bool ret_bool = false;
  switch (axis)
  {
    case ObPathNodeAxis::CHILD:
    case ObPathNodeAxis::DESCENDANT:
    case ObPathNodeAxis::DESCENDANT_OR_SELF: {
      ret_bool = true;
      break;
    }
    default:
      break;
  }
  return ret_bool;
}

bool ObPathUtil::include_self_axis(const ObPathNodeAxis& axis)
{
  bool ret_bool = false;
  switch (axis)
  {
    case ObPathNodeAxis::SELF:
    case ObPathNodeAxis::ANCESTOR_OR_SELF:
    case ObPathNodeAxis::DESCENDANT_OR_SELF: {
      ret_bool = true;
      break;
    }
    default:
      break;
  }
  return ret_bool;
}

bool ObPathUtil::check_contain_relative_path(ObPathNode* path)
{
  bool ret_bool = true;
  if (path->get_node_type().is_arg()) {
    ret_bool = false;
  } else {
    ret_bool = path->contain_relative_path_;
  }
  return ret_bool;
}

bool ObPathUtil::check_need_cache(ObPathNode* path)
{
  return path->need_cache_;
}

int ObPathUtil::add_dup_if_missing(ObIAllocator* allocator, ObIMulModeBase*& path_res, ObIBaseSortedVector &dup, bool& end_seek)
{
  INIT_SUCC(ret);
  ObIMulModeBaseCmp cmp;
  ObIMulModeBaseUnique unique;
  ObIBaseSortedVector::iterator pos = dup.end();
  if (OB_ISNULL(path_res)) {
  } else if (path_res->is_tree()) {
    if ((OB_SUCC(dup.insert_unique(path_res, pos, cmp, unique)))) {
      end_seek = true;
    } else if (ret == OB_CONFLICT_VALUE) {
      ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
    }
  } else if (OB_FAIL(dup.find(path_res, pos, cmp))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      ObXmlBin* old_bin = static_cast<ObXmlBin*>(path_res);
      ObXmlBin* new_ans = nullptr;
      if (OB_FAIL(ObPathUtil::alloc_binary(allocator, new_ans))) {
        LOG_WARN("fail to alloc", K(ret));
      } else {
        new_ans = new (new_ans) ObXmlBin(*old_bin);
        pos = dup.end();
        if (OB_FAIL(dup.insert(new_ans, pos, cmp))) {
        } else if (OB_NOT_NULL(new_ans)) {
          path_res = new_ans;
          end_seek = true;
        }
      } // alloc binary
    } // duplicate bin, do nothing
  }
  return ret;
}

int ObPathUtil::add_scalar(ObIAllocator *allocator, ObPathArgNode* ans, ObSeekVector &res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ans)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSeekResult* final_ans = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_seek_result(allocator, ans, final_ans))) {
      LOG_WARN("fail to get final ans", K(ret), K(res.size()));
    } else if (OB_FAIL(res.push_back(final_ans))) {
      LOG_WARN("fail to push_back value into result", K(ret), K(res.size()));
    }
  }
  return ret;
}

int ObPathUtil::alloc_num_arg(ObMulModeMemCtx *ctx, ObPathArgNode*& arg, ObParserType parser_type, double num)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathArgNode* num_arg = static_cast<ObPathArgNode*> (ctx->allocator_->alloc(sizeof(ObPathArgNode)));
    if (OB_ISNULL(num_arg)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret));
    } else {
      num_arg = new (num_arg) ObPathArgNode(ctx, parser_type);
      if (OB_FAIL(num_arg->init(num, false))) {
       LOG_WARN("fail to init", K(ret));
      } else {
        arg = num_arg;
      }
    }
  }
  return ret;
}

int ObPathUtil::alloc_boolean_arg(ObMulModeMemCtx *ctx, ObPathArgNode*& arg, ObParserType parser_type, bool ans)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathArgNode* boolean_arg = static_cast<ObPathArgNode*> (ctx->allocator_->alloc(sizeof(ObPathArgNode)));
    if (OB_ISNULL(boolean_arg)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at location_node", K(ret));
    } else {
      boolean_arg = new (boolean_arg) ObPathArgNode(ctx, parser_type);
      if (OB_FAIL(boolean_arg->init(ans, false))) {
       LOG_WARN("fail to init", K(ret));
      } else {
        arg = boolean_arg;
      }
    }
  }
  return ret;
}

int ObPathFuncNode::eval_count(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  if (is_seeked_ == true) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(ans_) || contain_relative_path_) {
    is_seeked_ = true;
    if (size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error arg", K(ret));
    } else {
      ObPathNode* arg = static_cast<ObPathNode*>(member(0));
      if (OB_ISNULL(arg)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("value is NULL", K(ret));
      } else {
        int count  = 0;
        ObSeekResult tmp_res;
        while (OB_SUCC(arg->eval_node(ctx, res))) {
          ++count;
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          ObPathArgNode* ans = nullptr;
          if (OB_FAIL(ObPathUtil::alloc_num_arg(ctx.ctx_, ans, node_type_.get_path_type(), count))) {
            LOG_WARN("fail to alloc arg", K(ret));
          } else {
            res.is_scalar_ = true;
            res.result_.scalar_ = ans;
            ans_ = ans;
          }
        }
      }
    }
  } else {
    res.is_scalar_ = true;
    res.result_.scalar_ = ans_;
    is_seeked_ = true;
  }
  return  ret;
}

int ObPathFuncNode::eval_true_or_false(ObPathCtx &ctx, bool is_true, ObSeekResult& res)
{
  INIT_SUCC(ret);
  if (is_seeked_) {
    is_seeked_ = false;
    ret = OB_ITER_END;
  } else if (OB_ISNULL(ans_)) {
    is_seeked_ = true;
    ObPathArgNode* ans = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_boolean_arg(ctx.ctx_, ans, node_type_.get_path_type(), is_true))) {
      LOG_WARN("fail to alloc arg", K(ret));
    } else {
      res.is_scalar_ = true;
      res.result_.scalar_ = ans;
      ans_ = ans;
    }
  } else {
    res.is_scalar_ = true;
    res.result_.scalar_ = ans_;
    is_seeked_ = true;
  }
  return ret;
}

int ObSeekIterator::init(ObPathCtx &ctx, ObPathLocationNode* location, ObIMulModeBase* ada_root)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(location) || OB_ISNULL(ada_root)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("value is NULL", K(ret));
  } else {
    ada_root_ = ada_root;
    ObXmlPathFilter* filter = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_filter_node(ctx.alloc_, filter))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(filter = new (filter) ObXmlPathFilter(location, &ctx))) {
    } else if (OB_FALSE_IT(seek_info_.filter_ = filter)) {
    } else if (location->get_node_type().get_location_type() == PN_ELLIPSIS) {
      if (ObPathUtil::is_upper_axis(location->get_axis())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't be upper axis for ellipsis node", K(ret));
      } else if (ObPathUtil::include_self_axis(location->get_axis())) {
        axis_ = DESCENDANT_OR_SELF;
      } else {
        axis_ = DESCENDANT;
      }
    } else {
      axis_ = location->get_axis();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(location->set_seek_info(seek_info_))) {
      LOG_WARN("fail to set seek info", K(ret));
    }
  }
  return ret;
}

int ObSeekIterator::next(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  switch(axis_) {
    case ObPathNodeAxis::CHILD: {
      if (OB_FAIL(next_child(ctx, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find child", K(ret));
      }
      break;
    }
    case ObPathNodeAxis::ATTRIBUTE: {
      if (OB_FAIL(next_attribute(ctx, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find attribute", K(ret));
      }
      break;
    }
    case ObPathNodeAxis::NAMESPACE: {
      if (OB_FAIL(next_namespace(ctx, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find attribute", K(ret));
      }
      break;
    }
    case ObPathNodeAxis::PARENT: {
      if (OB_FAIL(next_parent(ctx, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find self", K(ret));
      }
      break;
    }
    case ObPathNodeAxis::SELF: {
      if (OB_FAIL(next_self(ctx, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find self", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObSeekComplexIterator::next(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  switch(axis_) {
    case ObPathNodeAxis::DESCENDANT_OR_SELF:
    case ObPathNodeAxis::DESCENDANT: {
      if (OB_FAIL(next_descendant(ctx, DESCENDANT_OR_SELF == axis_, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find child", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObSeekAncestorIterator::next(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  switch(axis_) {
    case ObPathNodeAxis::ANCESTOR_OR_SELF:
    case ObPathNodeAxis::ANCESTOR:
    {
      if (OB_FAIL(next_ancestor(ctx, ANCESTOR_OR_SELF == axis_, res)) && ret != OB_ITER_END) {
        LOG_WARN("fail to find child", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

void ObSeekIterator::reset(ObIMulModeBase* new_ada_root)
{
  is_seeked_ = false;
  ada_root_ = new_ada_root;
}

int ObSeekIterator::next_attribute(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp_res = nullptr;
  ObXmlPathFilter* filter = static_cast<ObXmlPathFilter*> (seek_info_.filter_);
  ObPathLocationNode* path = (filter == nullptr) ? nullptr : filter->path_;
  if (OB_NOT_NULL(path) && (path->get_default_prefix_ns_info()
    || ObPathUtil::is_filter_nodetest(path->get_seek_type()))) {
    ret = OB_ITER_END;
  } else if (!is_seeked_) {
    if (OB_FAIL(ctx.push_ancestor(ada_root_))) {
      LOG_WARN("fail to push ancestor", K(ret));
    } else {
      iter_.construct(ada_root_, seek_info_);
      is_seeked_ = true;
    }
  }
  while (OB_SUCC(ret) && OB_ISNULL(tmp_res)) {
    ret = iter_.attr_next(tmp_res, M_ATTRIBUTE);
  }
  if (OB_SUCC(ret)) {
    res = tmp_res;
  }
  return ret;
}

int ObSeekIterator::next_namespace(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp_res = nullptr;
  ObXmlPathFilter* filter = static_cast<ObXmlPathFilter*> (seek_info_.filter_);
  ObPathLocationNode* path = (filter == nullptr) ? nullptr : filter->path_;
  if (OB_ISNULL(ada_root_) || ada_root_->type() != ObMulModeNodeType::M_ELEMENT
    // such as: namespace::comment(), return null
    || (path->get_wildcard_info() && ObPathUtil::is_filter_nodetest(path->get_seek_type())) ) {
    ret = OB_ITER_END;
  } else if (!is_seeked_) {
    if (OB_FAIL(ctx.push_ancestor(ada_root_))) {
      LOG_WARN("fail to push ancestor", K(ret));
    } else {
      iter_.construct(ada_root_, seek_info_);
      is_seeked_ = true;
    }
  }
  while (OB_SUCC(ret) && OB_ISNULL(tmp_res)) {
    ret = iter_.attr_next(tmp_res, M_NAMESPACE);
  }
  if (OB_SUCC(ret)) {
    res = tmp_res;
  }
  return ret;
}

int ObSeekIterator::next_child(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp_res = nullptr;
  if (!is_seeked_) {
    if (OB_FAIL(ctx.push_ancestor(ada_root_))) {
      LOG_WARN("fail to push parent", K(ret));
    } else {
      iter_.construct(ada_root_, seek_info_);
    }
    is_seeked_ = true;
  }
  while (OB_SUCC(ret) && OB_ISNULL(tmp_res)) {
    ret = iter_.next(tmp_res);
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_ITER_END) {
      res = nullptr;
      if (OB_SUCC(ctx.pop_ancestor())) {
        ret = OB_ITER_END;
      }
    }
  } else {
    res = tmp_res;
  }
  return ret;
}

int ObSeekIterator::next_parent(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  if (is_seeked_) {
    // parent node must only one, if already seeked, then ITER_END
    // push cur node into ancestor_record
    if (OB_FAIL(ctx.push_ancestor(ada_root_))) {
      LOG_WARN("fail to push parent", K(ret));
    } else {
      res = nullptr;
      ret = OB_ITER_END;
    }
  } else if (ctx.ancestor_record_.size() > 0) { // check if have parent
    ObIMulModeBase* top = ctx.top_ancestor();
    ada_root_ = top;
    bool filtered = false;
    is_seeked_ = true;
    if (OB_ISNULL(top)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (OB_FAIL(filter_ans(top, filtered))) {
      LOG_WARN("fail to filter", K(ret));
    } else if (filtered) {
      if (top->is_tree()) {
        ret = ctx.pop_ancestor();
      } else {
        // record parent
        ada_root_ = top;
        if (OB_FAIL(ctx.alloc_new_bin(ada_root_))) {
          LOG_WARN("fail to record parent", K(ret));
        } else if (OB_FAIL(ctx.pop_ancestor())) {
          LOG_WARN("fail to pop parent", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        res = ada_root_;
      }
    } else {
      res = nullptr;
      ret = OB_ITER_END;
    }
  } else {
    res = nullptr;
    ret = OB_ITER_END;
  }
  return ret;
}

// just filter cur node
int ObSeekIterator::next_self(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  if (is_seeked_) {
    ret = OB_ITER_END;
  } else {
    is_seeked_ = true;
    bool filtered = false;
    ObXmlPathFilter* filter = static_cast<ObXmlPathFilter*> (seek_info_.filter_);
    ObPathLocationNode* path = (filter == nullptr) ? nullptr : filter->path_;
    if (OB_ISNULL(filter) || OB_ISNULL(path)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (path->get_wildcard_info()
          && path->get_seek_type() == ObSeekType::NODES) {
      // if wildcard, do not need filter
      filtered = true;
    } else if (OB_FAIL((*filter)(ada_root_, filtered))) {
      LOG_WARN("fail to filter xnode", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (filtered) {
      res = ada_root_;
    } else {
      res = nullptr;
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObSeekComplexIterator::ellipsis_inner_next(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  if (iter_stack_.size() == 0) {
    ret = OB_ITER_END;
  } else {
    ObMulModeReader& top = iter_stack_.top();
    bool filtered = false;
    // check top node, if valid, return ans
    // if not valid, seek its child
    if (!top.is_eval_cur_) {
      if (OB_FAIL(filter_ans(top.cur_, filtered))) {
        LOG_WARN("fail to filter", K(ret));
      } else if (filtered) {
        res = top.cur_;
      }
      top.is_eval_cur_ = true;
    } else {
      ObIMulModeBase* unfilter_res = nullptr;
      // checked top node, get its child
      if (OB_FAIL(ctx.push_ancestor(top.cur_))) {
        LOG_WARN("fail to push", K(ret));
      } else if (OB_FAIL(top.next(unfilter_res)) || OB_ISNULL(unfilter_res)) {
        if (ret == OB_ITER_END) {
          res = nullptr;
          iter_stack_.pop();
          ret = ctx.pop_ancestor();
        }
      } else if (get_mul_mode_tc(unfilter_res->type()) == MulModeContainer) {
        // check is leaf node, if node leaf node, push
        ObMulModeReader child_iter(nullptr);
        child_iter.construct(unfilter_res, seek_info_);
        child_iter.alter_filter(nullptr);
        child_iter.is_eval_cur_ = false;
        iter_stack_.push(child_iter);
      } else if (OB_FAIL(filter_ans(unfilter_res, filtered))) { // leaf node, do not push, but filter
        LOG_WARN("fail to filter", K(ret));
      } else if (filtered) {
        res = unfilter_res;
      }
    }
  }
  return ret;
}

int ObSeekComplexIterator::next_descendant(ObPathCtx &ctx, bool include_self, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp_res = nullptr;
  if (!is_seeked_) {
    ObMulModeReader iter(nullptr);
    iter.construct(ada_root_, seek_info_);
    iter.is_eval_cur_ = !include_self;
    // first time, seek without filter
    iter.alter_filter(nullptr);
    iter_stack_.push(iter);
    is_seeked_ = true;
  }
  while (OB_SUCC(ret) && OB_ISNULL(tmp_res)) {
    // get valid next
    if (OB_FAIL(ellipsis_inner_next(ctx, tmp_res)) && ret != OB_ITER_END) {
      LOG_WARN("fail to get next", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(tmp_res)) {
    res = tmp_res;
  } else {
    res = nullptr;
    iter_stack_.reset();
    ret = OB_ITER_END;
  }
  return ret;
}

int ObSeekAncestorIterator::next_ancestor(ObPathCtx &ctx, bool include_self, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  ObIMulModeBase* tmp_res = nullptr;
  if (!is_seeked_) {
    is_seeked_ = true;
    if (include_self) {
      ret = anc_stack_push(ctx, ada_root_);
    }
    while (OB_SUCC(ret) && ctx.ancestor_record_.size() > 0) {
      // record ancestor
      ObIMulModeBase* top = ctx.top_ancestor();
      if (OB_FAIL(anc_stack_push(ctx, top))) {
        LOG_WARN("fail to push", K(ret));
      } else {
        ctx.pop_ancestor();
      }
    } // end while
  }
  while (OB_SUCC(ret) && OB_ISNULL(tmp_res)) {
    if (OB_FAIL(ancestor_inner_next(ctx, tmp_res)) && ret != OB_ITER_END) {
      LOG_WARN("fail to get next", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(tmp_res)) {
    res = tmp_res;
  } else {
    res = nullptr;
    anc_stack_.reset();
    ret = OB_ITER_END;
  }
  return ret;
}

int ObSeekAncestorIterator::anc_stack_push(ObPathCtx &ctx, ObIMulModeBase* push_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(push_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (push_node->is_tree()) {
    if (OB_FAIL(anc_stack_.push(push_node))) {
      LOG_WARN("should be inited", K(ret));
    }
  } else if (OB_FAIL(ctx.alloc_new_bin(push_node))) {
    LOG_WARN("allocate xmlbin failed", K(ret));
  } else if (OB_FAIL(anc_stack_.push(push_node))) {
    LOG_WARN("should be inited", K(ret));
  }
  return ret;
}

void ObSeekAncestorIterator::anc_stack_pop(ObPathCtx &ctx)
{
  ObIMulModeBase* top = anc_stack_.top();
  if (OB_ISNULL(top)) {
  } else if (top->is_tree()) {
    anc_stack_.pop();
  } else {
    ObXmlBin* bin_top = static_cast<ObXmlBin*>(top);
    anc_stack_.pop();
    ctx.bin_pool_.free(bin_top);
  }
}

int ObSeekAncestorIterator::ancestor_inner_next(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  // first, return root
  // last, return parent
  INIT_SUCC(ret);
  if (anc_stack_.size() == 0) {
    ret = OB_ITER_END;
  } else {
    ObIMulModeBase* top = anc_stack_.top();
    if (OB_FAIL(ctx.push_ancestor(top))) {
      LOG_WARN("fail to push", K(ret));
    } else {
      anc_stack_pop(ctx);
      bool filtered = false;
      if (OB_FAIL(filter_ans(top, filtered))) {
        LOG_WARN("fail to filter", K(ret));
      } else if (filtered) {
        res = top;
      }
    }
  }
  return ret;
}

int ObSeekIterator::filter_ans(ObIMulModeBase* ans, bool& filtered)
{
  INIT_SUCC(ret);
  filtered = false;
  ObXmlPathFilter* seek_filter = static_cast<ObXmlPathFilter*> (seek_info_.filter_);
  if (OB_ISNULL(seek_filter)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL((*seek_filter)(ans, filtered))) {
    LOG_WARN("fail to filter xnode", K(ret));
  }
  return ret;
}

ObDatum *ObPathVarObject::get_value(const common::ObString &key) const
{
  ObDatum *data = NULL;
  ObPathVarPair pair(key, NULL);
  ObPathKeyCompare cmp;

  ObPathVarArray::const_iterator low_iter = std::lower_bound(object_array_.begin(),
                                                              object_array_.end(),
                                                              pair, cmp);
  if (low_iter != object_array_.end() && low_iter->get_key() == key) {
    data = low_iter->get_value();
  }

  return data;
}

int ObPathVarObject::add(const common::ObString &key, ObDatum *value, bool with_unique_key)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else {
    ObPathVarPair pair(key, value);
    ObPathKeyCompare cmp;
    ObPathVarArray::iterator low_iter = std::lower_bound(object_array_.begin(),
                                                        object_array_.end(), pair, cmp);
    if (low_iter != object_array_.end() && low_iter->get_key() == key) { // Found and covered
      if (with_unique_key) {
        ret = OB_ERR_DUPLICATE_KEY;
        LOG_WARN("Found duplicate key inserted before!", K(key), K(ret));
      } else {
        low_iter->set_value(value);
      }
    } else if (OB_FAIL(object_array_.push_back(pair))) {// not found, push back, sort
      LOG_WARN("fail to push back", K(ret));
    } else { // sort again.
      ObPathKeyCompare cmp;
      lib::ob_sort(object_array_.begin(), object_array_.end(), cmp);
    }
  }
  return ret;
}

int ObPathExprIter::init(ObMulModeMemCtx *ctx, ObString& path, ObString& default_ns,
                          ObIMulModeBase* doc, ObPathVarObject* pass_var, bool add_namespace)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx)  || OB_ISNULL(ctx->allocator_) || OB_ISNULL(doc) || OB_ISNULL(path.ptr())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret), K(ctx->allocator_), K(doc), K(path.ptr()));
  } else {
    ctx_ = ctx;
    allocator_ = ctx->allocator_;
    path_ = path;
    doc_ = doc;
    default_ns_ = default_ns;
    pass_var_ = pass_var;
    path_node_ = nullptr;
    is_inited_ = true;
    is_open_ = false;
    add_ns_ = add_namespace;
  }
  return ret;
}

int ObPathUtil::get_parser_type(ObIMulModeBase *doc, ObParserType& parser_type)
{
  INIT_SUCC(ret);
  switch (doc->data_type()) {
    case ObNodeDataType::OB_XML_TYPE: {
      parser_type = ObParserType::PARSER_XML_PATH;
      break;
    }
    case ObNodeDataType::OB_JSON_TYPE: {
      if (lib::is_oracle_mode()) {
        parser_type = ObParserType::PARSER_JSON_PATH_LAX;
      } else {
        parser_type = ObParserType::PARSER_JSON_PATH_STRICT;
      }
    }
    default: {
      ret = OB_INVALID_DATA;
      LOG_WARN("Wrong type for seek", K(ret));
    }
  }
  return ret;
}

int ObPathUtil::char_to_filter_type(const ObXpathFilterChar& ch, ObFilterType& type)
{
  INIT_SUCC(ret);
  if (ch >= ObXpathFilterChar::CHAR_UNION && ch <= ObXpathFilterChar::CHAR_MOD) {
    type = (ObFilterType)ch;
  } else {
    type = ObFilterType::PN_FILTER_ERROR;
  }
  return ret;
}

int ObPathUtil::pop_char_stack(ObFilterCharPointers& char_stack)
{
  INIT_SUCC(ret);
  uint64_t size = char_stack.size();
  if (size > 0) {
    if (OB_FAIL(char_stack.remove(size - 1))) {
      LOG_WARN("fail to remove char top.", K(ret), K(char_stack[size - 1]));
    }
  } else {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("should not be null", K(ret));
  }
  return ret;
}

int ObPathUtil::pop_node_stack(ObPathVectorPointers& node_stack, ObPathNode*& top_node)
{
  INIT_SUCC(ret);
  uint64_t size = node_stack.size();
  if (size > 0) {
    top_node = node_stack[size - 1];
    if (OB_FAIL(node_stack.remove(size - 1))) {
      LOG_WARN("fail to remove char top.",K(ret), K(top_node));
    }
  } else {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("should not be null", K(ret));
  }
  return ret;
}

int ObPathExprIter::open()
{
  INIT_SUCC(ret);
  if (!is_inited_) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else {
    ObParserType parser_type;
    if (OB_FAIL(ObPathUtil::get_parser_type(doc_, parser_type))) {
      LOG_WARN("fail to init parser type", K(ret));
    } else {
      ObPathParser parser(ctx_, parser_type, path_, default_ns_, pass_var_);
      // parse
      if (OB_FAIL(parser.parse_path())) {
        LOG_WARN("fail to parse", K(ret));
      } else {
        path_node_ = parser.get_root();
        ret = path_ctx_.init(ctx_, doc_, doc_, tmp_allocator_, true, need_record_, add_ns_);
        is_open_ = true;
      }
    }
  }
  return ret;
}

int ObPathExprIter::get_first_node(ObPathNode*& loc)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(path_node_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("param value is NULL", K(ret));
  } else if (path_node_->get_node_type().is_root()) {
    if (path_node_->size() > 0) {
      loc = static_cast<ObPathNode*>(path_node_->member(0));
    }
  } else if (path_node_->get_node_type().is_location_filter()) {
    ObPathFilterOpNode* op_node = static_cast<ObPathFilterOpNode*>(path_node_);
    ObPathNode* left = op_node->left_;
    if (left->get_node_type().is_root()) {
      if (path_node_->size() > 0) {
        loc = static_cast<ObPathNode*>(path_node_->member(0));
      }
    }
  }
  return ret;
}

int ObPathExprIter::get_first_axis(ObPathNodeAxis& first_axis)
{
  INIT_SUCC(ret);
  ObPathNode* first = nullptr;
  if (OB_FAIL(get_first_node(first))) {
    LOG_WARN("fail to get first node", K(ret));
  } else if (OB_ISNULL(first)) {
    first_axis = ObPathNodeAxis::ERROR_AXIS;
  } else if (first->get_node_type().is_location()
    && first->get_node_type().get_location_type() == PN_ELLIPSIS) {
    first_axis = ObPathNodeAxis::DESCENDANT_OR_SELF;
  } else if (first->get_node_type().is_location()) {
    ObPathLocationNode* loc = static_cast<ObPathLocationNode*>(first);
    first_axis = loc->get_axis();
  } else {
    first_axis = ObPathNodeAxis::ERROR_AXIS;
  }
  return ret;
}
int ObPathExprIter::get_first_seektype(ObSeekType& first_seektype)
{
  INIT_SUCC(ret);
  ObPathNode* first = nullptr;
  if (OB_FAIL(get_first_node(first))) {
    LOG_WARN("fail to get first node", K(ret));
  } else if (OB_ISNULL(first)) {
    first_seektype = ObSeekType::ERROR_SEEK;
  } else if (first->get_node_type().is_location()
    && first->get_node_type().get_location_type() == PN_ELLIPSIS) {
    first_seektype = ObSeekType::NODES;
  } else if (first->get_node_type().is_location()) {
    ObPathLocationNode* loc = static_cast<ObPathLocationNode*>(first);
    first_seektype = loc->get_seek_type();
  } else {
    first_seektype = ObSeekType::ERROR_SEEK;
  }
  return ret;
}

ObIMulModeBase* ObPathExprIter::get_cur_res_parent()
{
  return path_ctx_.ancestor_record_.size() > 0 ? path_ctx_.ancestor_record_.top() : nullptr;
}

int ObPathExprIter::get_next_node(ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  if (!is_inited_ || OB_ISNULL(path_node_)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else {
    ObSeekResult path_res;
    bool end_seek = false;
    while (OB_SUCC(ret) && !end_seek) {
      if (OB_FAIL(path_node_->eval_node(path_ctx_, path_res))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to seek", K(ret));
        }
      } else if (path_res.is_scalar_) {
        if (OB_FAIL(ObPathUtil::trans_scalar_to_base(tmp_allocator_, path_res.result_.scalar_, res))) {
          LOG_WARN("fail to trans", K(ret));
        }
        end_seek = true;
      } else if (OB_ISNULL(path_res.result_.base_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res is NULL", K(ret));
      } else if (OB_FAIL(ObPathUtil::add_dup_if_missing(tmp_allocator_, path_res.result_.base_, dup_, end_seek))) {
        LOG_WARN("fail to trans", K(ret));
      } else if (add_ns_ && ((path_res.result_.base_->type() == M_ELEMENT && path_ctx_.defined_ns_ > 0)
                 || ((path_res.result_.base_->type() == M_ELEMENT || path_res.result_.base_->type() == M_DOCUMENT
                 || path_res.result_.base_->type() == M_CONTENT) && OB_NOT_NULL(path_ctx_.extend_)))
                && OB_FAIL(ObPathUtil::add_ns_if_need(path_ctx_, path_res.result_.base_))) {
        LOG_WARN("fail to add_ns", K(ret));
      } else if (end_seek) {
        res = path_res.result_.base_;
      }
    } // end while
  }
  return ret;
}

int ObPathExprIter::close()
{
  INIT_SUCC(ret);
  if (is_inited_) {
    is_inited_ = false;
    dup_.reset();
    path_ctx_.reset();
  }
  return ret;
}

int ObPathExprIter::reset()
{
  INIT_SUCC(ret);
  if (is_inited_) {
    dup_.reset();
    is_inited_ = false;
  }
  return ret;
}

int ObPathExprIter::reset(ObIMulModeBase* doc, ObIAllocator *tmp_allocator)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(doc) || OB_ISNULL(tmp_allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null.", K(ret));
  } else {
    doc_ = doc;
    tmp_allocator_ = tmp_allocator;
    dup_.reset();
    ret = path_ctx_.reinit(doc, tmp_allocator);
  }
  return ret;
}

void ObPathExprIter::set_add_ns(bool add_ns) {
  add_ns_ = add_ns;
  if (path_ctx_.is_inited()) {
    path_ctx_.add_ns_ = add_ns;
  }
}

int ObPathExprIter::set_tmp_alloc(ObIAllocator *tmp_allocator)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null.", K(ret));
  } else {
    tmp_allocator_ = tmp_allocator;
  }
  return ret;
}

int ObXmlPathFilter::operator()(ObIMulModeBase* doc, bool& filtered)
{
  INIT_SUCC(ret);
  filtered = false;
  if (OB_ISNULL(doc) || OB_ISNULL(path_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("param value is NULL", K(ret));
  } else if (doc->data_type() == ObNodeDataType::OB_XML_TYPE) {
    ObMulModeNodeType xtype = doc->type();
    ObSeekType seek_info  = path_->get_seek_type();
    ObString ns_value = path_->get_ns_name();
    ObString key_value =  path_->get_key_name();
    if (path_->get_prefix_ns_info() && path_->get_default_prefix_ns_info() && path_->get_axis() != NAMESPACE) {
      filtered = false;
    } else if (path_->get_axis() == ATTRIBUTE || path_->get_axis() == NAMESPACE) {
      filtered = (path_->get_axis() == ATTRIBUTE) ? (xtype == ObMulModeNodeType::M_ATTRIBUTE) : (xtype == ObMulModeNodeType::M_NAMESPACE);
      if (!filtered) {
      } else if (path_->get_wildcard_info()) {
        if (xtype == ObMulModeNodeType::M_ATTRIBUTE) {
          filtered = true;
        } else if (seek_info >= ObSeekType::TEXT) {
          filtered = false;
        } else {
          filtered = true;
        }
      } else if (key_value.length() > 0 && OB_NOT_NULL(key_value.ptr())) {
        int res = 0;
        doc->compare(key_value, res);
        filtered = (res == 0)? true : false;
        if (filtered && xtype == ObMulModeNodeType::M_NAMESPACE
            && !path_->get_prefix_ns_info()
            && !path_->get_default_prefix_ns_info()
            && key_value.compare("xmlns") != 0) {
          filtered = false;
        }
      }
    } else if (path_->get_wildcard_info()) {
      switch (seek_info) {
        case ObSeekType::NODES: {
          filtered = true;
          break;
        }
        case ObSeekType::TEXT: {
          if (xtype == ObMulModeNodeType::M_TEXT || xtype == ObMulModeNodeType::M_CDATA) {
            filtered = true;
          }
          break;
        }
        case ObSeekType::ELEMENT: {
          filtered = (xtype == ObMulModeNodeType::M_ELEMENT);
          break;
        }
        case ObSeekType::COMMENT: {
          filtered = (xtype == ObMulModeNodeType::M_COMMENT);
          break;
        }
        case ObSeekType::PROCESSING_INSTRUCTION: {
          filtered = (xtype  == ObMulModeNodeType::M_INSTRUCT);
          break;
        }
        default: {
          filtered = false;
          break;
        }
      }
    } else if (key_value.length() > 0 && OB_NOT_NULL(key_value.ptr())) {
      int res = 0;
      doc->compare(key_value, res);
      filtered = (res == 0)? true : false;
      if (filtered && seek_info == ObSeekType::PROCESSING_INSTRUCTION) {
        filtered = (xtype  == ObMulModeNodeType::M_INSTRUCT);
      }
    }

    if (OB_SUCC(ret) && filtered
    && (seek_info == ObSeekType::ELEMENT || path_->get_axis() == ATTRIBUTE)// if element, check ns
    && !(path_->get_wildcard_info() && !path_->get_prefix_ns_info())) { // if * without prefix_ns do not check
      ObString node_ns_value;
      if (OB_FAIL(doc->get_ns_value(path_ctx_->ancestor_record_, node_ns_value, path_ctx_->extend_))) {
        LOG_WARN("failed to get ns from element node", K(ret), K(doc->is_tree()));
      } else if (ns_value.length() == 0 || OB_ISNULL(ns_value.ptr())) {
        filtered = (node_ns_value.length() == 0 || OB_ISNULL(node_ns_value.ptr())) ? true : false;
      } else {
        filtered = (ns_value.compare(node_ns_value) == 0)? true : false;
      }
    }
  } // may be json type
  return ret;
}

int ObPathUtil::logic_compare_rule(ObPathCtx &ctx, ObPathNode *path_node, bool &ret_bool)
{
  INIT_SUCC(ret);
  ObArgType node_type;
  ObPathArgNode *arg_node;
  if (OB_ISNULL(ctx.alloc_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_ISNULL(path_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("get path node null", K(ret));
  } else if (OB_FAIL(ObPathUtil::get_arg_type(node_type, path_node))) {
    LOG_WARN("fail to get node type", K(ret));
  } else if (OB_ISNULL(arg_node = static_cast<ObPathArgNode*>(path_node))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("get arg node null", K(ret));
  } else if (ObArgType::PN_SUBPATH == node_type)  {
    ObSeekVector seek_vector;
    if (OB_FAIL(get_seek_vec(ctx, path_node, seek_vector))) {
      LOG_WARN("left get_seek_vec failed", K(ret), K(path_node));
    } else if (seek_vector.size() > 0) {
      ret_bool = true;
    } else {
      ret_bool = false;
    }
    if (OB_SUCC(ret) && OB_FAIL(release_seek_vector(ctx,seek_vector))) {
      LOG_WARN("release_seek_vec failed", K(ret), K(path_node));
    }
  } else {
    ObNodeTypeAndContent *content = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &arg_node->arg_, node_type, content))) {
      LOG_WARN("alloc node content info failed", K(ret), K(node_type));
    } else if (OB_FAIL(ObXmlUtil::check_bool_rule(content, ret_bool))) {
      LOG_WARN("check bool rule failed", K(ret), K(content));
    }
  }
  return ret;
}

int ObPathUtil::alloc_node_set_vector(ObPathCtx &ctx, ObPathNode *path_node, ObArgType& node_type, ObNodeSetVector &node_vec)
{
  INIT_SUCC(ret);
  ObNodeTypeAndContent *content = nullptr;
  if (OB_ISNULL(ctx.alloc_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_ISNULL(path_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("get path node null", K(ret));
  } else if (ObArgType::PN_SUBPATH == node_type) {
    ObSeekVector seek_vector;
    if (OB_FAIL(get_seek_vec(ctx, path_node, seek_vector))) {
      LOG_WARN("left get_seek_vec failed", K(ret), K(path_node));
    } else if (seek_vector.size() == 0) {
      if (path_node->node_type_.is_root()) {
        ObPathNode *member_path = nullptr;
        ObPathLocationNode *location_node = nullptr;
        if (path_node->size() > 0
            && OB_NOT_NULL(path_node->member((path_node->size()) - 1))
            && OB_NOT_NULL(member_path = static_cast<ObPathNode*>(path_node->member((path_node->size()) - 1)))) {
          if (member_path->node_type_.is_location() && OB_NOT_NULL(location_node = static_cast<ObPathLocationNode *>(member_path))) {
            if (location_node->get_axis() == ObPathNodeAxis::ATTRIBUTE) {
              ObString input_str("");
              if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &input_str, content))) {
                LOG_WARN("alloc node content info failed", K(ret));
              } else if (OB_FAIL(node_vec.push_back(content))){
                LOG_WARN("failed to push back content.", K(ret));
              }
            }
          }
        }
      }
    } else if (seek_vector.size() > 0 && seek_vector[0].is_scalar_) {
      ObPathArgNode* arg_node;
      if (OB_ISNULL(arg_node = seek_vector[0].result_.scalar_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("scalar get null", K(ret));
      } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &(arg_node->arg_),
                                                              arg_node->node_type_.get_arg_type(),
                                                              content))) {
        LOG_WARN("alloc node content info failed", K(ret));
      } else if (OB_FAIL(node_vec.push_back(content))) {
        LOG_WARN("failed to push back content.", K(ret));
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < seek_vector.size(); i++) {
        ObSeekResult* tmp_result = &seek_vector[i];
        ObArray<ObIMulModeBase*> node_array;
        ObIMulModeBase *base = tmp_result->result_.base_;
        ObString text_str;
        if (OB_ISNULL(tmp_result)) {
          LOG_WARN("seek result is null", K(ret), K(i));
        } else if (tmp_result->is_scalar_) {
			    ret = OB_ERR_UNEXPECTED;
			    LOG_WARN("compare get scalar unexpected", K(ret));
        } else if (base->size() == 0) { // leaf node
          if ((base->type() == ObMulModeNodeType::M_TEXT ||
              base->type() == ObMulModeNodeType::M_ATTRIBUTE ||
              base->type() == ObMulModeNodeType::M_NAMESPACE)
              && OB_FAIL(base->get_value(text_str))) {
            LOG_WARN("get value failed", K(ret));
          } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &text_str, content))) {
            LOG_WARN("alloc node content info failed", K(ret), K(text_str));
          } else if (OB_FAIL(node_vec.push_back(content))) {
            LOG_WARN("failed to push back content.", K(ret));
          }
		    } else if (OB_FAIL(ObXmlUtil::get_array_from_mode_base(base, node_array))) { // get children
			    LOG_WARN("get child array failed", K(ret));
		    } else if (node_array.size() == 0) {
          ObString text("");
          if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &text, content))) {
            LOG_WARN("alloc node content info", K(ret));
          } else if (OB_FAIL(node_vec.push_back(content))) {
            LOG_WARN("failed to push back content.", K(ret));
          }
        } else {
          ObString text;
          bool is_scalar = (base->type() == ObMulModeNodeType::M_TEXT ||
                            base->type() == ObMulModeNodeType::M_ATTRIBUTE ||
                            base->type() == ObMulModeNodeType::M_NAMESPACE);
          if (is_scalar && OB_FAIL(base->get_value(text))) {
            LOG_WARN("get value failed", K(ret));
          } else if (!is_scalar && OB_FAIL(ObXmlUtil::dfs_xml_text_node(ctx.ctx_, base, text))) {
            LOG_WARN("dfs get text failed", K(ret));
          } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &text, content))) {
            LOG_WARN("alloc node content info failed", K(ret), K(text));
          } else if (OB_FAIL(node_vec.push_back(content))) {
            LOG_WARN("failed to push back content.", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(release_seek_vector(ctx,seek_vector))) {
      LOG_WARN("release_seek_vec failed", K(ret), K(path_node));
    }
  } else {
    ObPathArgNode* arg_node = static_cast<ObPathArgNode*>(path_node);
    if (OB_ISNULL(arg_node)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("scalar get null", K(ret));
    } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &arg_node->arg_, node_type, content))) {
      LOG_WARN("alloc node content info failed", K(ret), K(node_type));
    } else if (OB_FAIL(node_vec.push_back(content))) {
      LOG_WARN("failed to push back content.", K(ret));
    }
  }
  return ret;
}

int ObPathUtil::filter_compare(ObPathCtx &ctx,
                                ObNodeSetVector &left, ObArgType left_type,
                                ObNodeSetVector &right, ObArgType right_type,
                                ObFilterType op, ObSeekResult& res)
{
  INIT_SUCC(ret);
  bool ret_bool = false;
  ObXpathCompareType compare_type = ObXmlUtil::filter_type_correspondence(op);
  ObXpathArgType left_arg_type = ObXmlUtil::arg_type_correspondence(left_type);
  ObXpathArgType right_arg_type = ObXmlUtil::arg_type_correspondence(right_type);
  ObXpathArgType target_type = ObXmlUtil::compare_cast[left_arg_type][right_arg_type][compare_type];

  if (OB_FAIL(ret)) {
  } else if ((ObFilterType::PN_CMP_UNEQUAL == op || ObFilterType::PN_CMP_EQUAL == op)
      &&(left_arg_type == ObXpathArgType::XC_TYPE_BOOLEAN &&
          right_arg_type == ObXpathArgType::XC_TYPE_STRING)) {
    // special for left=bool, right=string, use to_boolean
    bool str_bool = false;
    if (right.size() != 1 || left.size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right size unexpect", K(ret));
    } else if (OB_FAIL(ObXmlUtil::to_boolean(&right[0]->content_->str_, str_bool))) {
      LOG_WARN("right to boolean failed", K(ret), K(right[0]));
    } else if (OB_FAIL(ObXmlUtil::compare(left[0]->content_->boolean_, str_bool, op, ret_bool))) {
      LOG_WARN("compare failed", K(ret), K(left[0]), K(str_bool));
    }
  } else if ((ObFilterType::PN_CMP_UNEQUAL == op || ObFilterType::PN_CMP_EQUAL == op) &&
              ((left_type == ObArgType::PN_SUBPATH && left.size() == 0 && right_type == ObArgType::PN_BOOLEAN) ||
              (left_type == ObArgType::PN_BOOLEAN && right_type == ObArgType::PN_SUBPATH && right.size() == 0))) {
    if (left.size() == 0 && OB_FAIL(ObXmlUtil::compare(false, right[0]->content_->boolean_, op, ret_bool))) {
      LOG_WARN("left size = 0 and compare failed", K(ret));
    } else if (right.size() == 0 && OB_FAIL(ObXmlUtil::compare(left[0]->content_->boolean_, false, op, ret_bool))) {
      LOG_WARN("right size = 0 and compare failed", K(ret));
    }
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && !ret_bool && i < left.size(); i++) {
      for (int32_t j = 0; OB_SUCC(ret) && !ret_bool && j < right.size(); j++) {
        if (ObXpathArgType::XC_TYPE_BOOLEAN == target_type) {
          bool left_bool = false;
          bool right_bool = false;
          if (OB_FAIL(ObXmlUtil::check_bool_rule(left[i], left_bool))) {
            LOG_WARN("check left bool rule failed", K(ret), K(left[i]));
          } else if (OB_FAIL(ObXmlUtil::check_bool_rule(right[j], right_bool))) {
            LOG_WARN("check right bool rule failed", K(ret), K(right[j]));
          } else if (OB_FAIL(ObXmlUtil::compare(left_bool, right_bool, op, ret_bool))) {
            LOG_WARN("compare failed", K(ret), K(left_bool), K(right_bool));
          }
        } else if (ObXpathArgType::XC_TYPE_NUMBER == target_type) {
          double left_double = 0.0;
          double right_double = 0.0;
          if (OB_FAIL(ObXmlUtil::to_number(left[i], left_double)) && ret != OB_INVALID_DATA) {
            LOG_WARN("check left bool rule failed", K(ret), K(left[i]));
          } else if (ret == OB_INVALID_DATA) {
            ret = OB_SUCCESS;
            ret_bool = op == ObFilterType::PN_CMP_UNEQUAL ? true : false;
          } else if (OB_FAIL(ObXmlUtil::to_number(right[j], right_double)) && ret != OB_INVALID_DATA) {
            LOG_WARN("check right bool rule failed", K(ret), K(right[j]));
          } else if (ret == OB_INVALID_DATA) {
            ret = OB_SUCCESS;
            ret_bool = op == ObFilterType::PN_CMP_UNEQUAL ? true : false;
          } else if (OB_FAIL(ObXmlUtil::compare(left_double, right_double, op, ret_bool))) {
            LOG_WARN("compare failed", K(ret), K(left_double), K(right_double));
          }
        } else if (ObXpathArgType::XC_TYPE_STRING == target_type) {
          char *str_left = NULL;
          char *str_right = NULL;
          double left_double = 0.0;
          double right_double = 0.0;
          // bugfix 49485495: if left and right are both subpath then try to cover to number
          if (left_type == ObArgType::PN_SUBPATH && right_type == ObArgType::PN_SUBPATH
              && OB_SUCC(ObXmlUtil::to_number(left[i], left_double))
              && OB_SUCC(ObXmlUtil::to_number(right[j], right_double))) {
            if (OB_FAIL(ObXmlUtil::compare(left_double, right_double, op, ret_bool))) {
              LOG_WARN("compare failed", K(ret), K(left_double), K(right_double));
            }
          } else if (OB_FAIL(ObXmlUtil::to_string(*ctx.alloc_, left[i], str_left))) {
            LOG_WARN("check left bool rule failed", K(ret), K(left[i]));
          } else if (OB_FAIL(ObXmlUtil::to_string(*ctx.alloc_, right[j], str_right))) {
            LOG_WARN("check right bool rule failed", K(ret), K(right[j]));
          } else if ((op == ObFilterType::PN_CMP_EQUAL || op == ObFilterType::PN_CMP_UNEQUAL)
                      && OB_ISNULL(str_left) && OB_ISNULL(str_right)) {
            ret_bool = op == ObFilterType::PN_CMP_EQUAL ? true : false;
          } else if (OB_ISNULL(str_left) || OB_ISNULL(str_right)) {
            ret_bool = false;
          } else if (OB_FAIL(ObXmlUtil::compare(ObString(strlen(str_left), str_left), ObString(strlen(str_right), str_right), op, ret_bool))) {
            LOG_WARN("compare failed", K(ret), K(str_left), K(str_right));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid target type", K(ret), K(target_type), K(left_type), K(right_type));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObPathArgNode* ans = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_arg_node(ctx.alloc_, ans))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(ans = new (ans) ObPathArgNode(ctx.ctx_, ObParserType::PARSER_XML_PATH))) {
    } else if (OB_FAIL(ans->init(ret_bool, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else {
      res.is_scalar_ = true;
      res.result_.scalar_ = ans;
    }
  }
  return ret;
}

int ObPathUtil::filter_calculate(ObPathCtx &ctx,
                                ObNodeSetVector &left, ObArgType left_type,
                                ObNodeSetVector &right, ObArgType right_type,
                                ObFilterType op, ObSeekVector &res)
{
  INIT_SUCC(ret);
  bool ret_bool = true;
  double ret_double = 0.0;
  double left_double = 0.0;
  double right_double = 0.0;
  if (left.size() == 0 || right.size() == 0) {
    ret_bool = false;
  } else if (OB_FAIL(ObXmlUtil::to_number(left[0], left_double)) && OB_OP_NOT_ALLOW != ret) {
    LOG_WARN("to_number failed", K(ret), K(op));
  } else if (OB_OP_NOT_ALLOW == ret) {
    ret = OB_SUCCESS;
    ret_bool = false;
  } else if (OB_FAIL(ObXmlUtil::to_number(right[0], right_double)) && OB_OP_NOT_ALLOW != ret) {
    LOG_WARN("to_number failed", K(ret), K(op));
  } else if (OB_OP_NOT_ALLOW == ret) {
    ret = OB_SUCCESS;
    ret_bool = false;
  } else if (OB_FAIL(ObXmlUtil::calculate(left_double, right_double, op, ret_double))) {
    LOG_WARN("calculate failed", K(ret), K(left_double), K(right_double));
  }
  if (OB_SUCC(ret)) {
    ObPathArgNode* ans = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_arg_node(ctx.alloc_, ans))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(ans = new (ans) ObPathArgNode(ctx.ctx_, ObParserType::PARSER_XML_PATH))) {
    } else if (ret_bool && OB_FAIL(ans->init(ret_double, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else if (!ret_bool && OB_FAIL(ans->init(ret_bool, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else if (OB_FAIL(ObPathUtil::add_scalar(ctx.alloc_, ans, res))) {
      LOG_WARN("add scalar failed");
    }
  }
  return ret;
}

int ObPathUtil::filter_logic_compare(ObPathCtx &ctx, ObPathNode* left_node, ObPathNode* right_node, ObFilterType op, ObSeekResult &res)
{
  INIT_SUCC(ret);
  bool ret_bool = false;
  bool left_bool = false;
  bool right_bool = false;
  if (OB_FAIL(ObPathUtil::logic_compare_rule(ctx, left_node, left_bool))) {
    LOG_WARN("left logic compare rule failed", K(ret), K(left_node));
  } else if (op == ObFilterType::PN_AND_COND && !left_bool) {
    ret_bool = false;
  } else if (op == ObFilterType::PN_OR_COND && left_bool) {
    ret_bool = true;
  } else if (OB_FAIL(ObPathUtil::logic_compare_rule(ctx, right_node, right_bool))) {
    LOG_WARN("right logic compare rule failed", K(ret), K(right_node));
  } else if (OB_FAIL(ObXmlUtil::logic_compare(left_bool, right_bool, op, ret_bool))) {
    LOG_WARN("logic compare failed", K(ret), K(left_bool), K(right_bool));
  }
  if (OB_SUCC(ret)) {
    ObPathArgNode* ans = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_arg_node(ctx.alloc_, ans))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(ans = new (ans) ObPathArgNode(ctx.ctx_, ObParserType::PARSER_XML_PATH))) {
    } else if (OB_FAIL(ans->init(ret_bool, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else {
      res.is_scalar_ = true;
      res.result_.scalar_ = ans;
    }
  }
  return ret;
}

int ObPathUtil::filter_union(ObPathCtx &ctx, ObPathNode* left_node, ObPathNode* right_node, ObFilterType op, ObSeekVector &res)
{
  INIT_SUCC(ret);
  bool ret_bool = false;
  ObArgType left_type = left_node->node_type_.get_arg_type();
  ObArgType right_type = right_node->node_type_.get_arg_type();
  ObPathArgNode *left_arg_node = static_cast<ObPathArgNode*>(left_node);
  ObPathArgNode *right_arg_node = static_cast<ObPathArgNode*>(right_node);
  bool left_union = true;
  if (ObArgType::PN_SUBPATH == left_type) {
    ObSeekVector left_vec;
    if (OB_FAIL(get_seek_vec(ctx, left_node, left_vec))) {
      LOG_WARN("right get_seek_vec failed", K(ret));
    } else if (left_vec.size() <= 0) {
      left_union = false;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ret failed", K(ret));
  } else if (ObArgType::PN_SUBPATH == right_type) {
    ObNodeTypeAndContent *left_info = nullptr;
    if (ObArgType::PN_SUBPATH == left_type && left_union == true) {
      ret_bool = true;
    } else if (OB_FAIL(ObArgType::PN_SUBPATH != left_type && ObPathUtil::alloc_node_content_info(ctx.alloc_, &left_arg_node->arg_, left_type, left_info))) {
      LOG_WARN("alloc left content info failed", K(ret));
    } else if (OB_FAIL(ObArgType::PN_SUBPATH != left_type && ObXmlUtil::check_bool_rule(left_info, left_union))) {
      LOG_WARN("check bool_rule failed", K(ret));
    } else if (left_union) {
      ret_bool = true;
    } else {
      ObSeekVector right_vec;
      if (OB_FAIL(get_seek_vec(ctx, right_node, right_vec))) {
        LOG_WARN("right get_seek_vec failed", K(ret));
      } else {
        ret_bool = right_vec.size() > 0 ? true : false;
      }
    }
  } else {
    if (ObArgType::PN_SUBPATH == left_type) {
      ret_bool = left_union;
    } else {
      ObNodeTypeAndContent *left_info = nullptr;
      ObNodeTypeAndContent *right_info = nullptr;
      if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &left_arg_node->arg_, left_type, left_info))) {
        LOG_WARN("alloc left content info failed", K(ret));
      } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &right_arg_node->arg_, right_type, right_info))) {
        LOG_WARN("alloc left content info failed", K(ret));
      } else if (OB_FAIL(ObXmlUtil::inner_union(left_info, right_info, ret_bool))) {
        LOG_WARN("inner union failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObPathArgNode* ans = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_arg_node(ctx.alloc_, ans))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(ans = new (ans) ObPathArgNode(ctx.ctx_, ObParserType::PARSER_XML_PATH))) {
    } else if (OB_FAIL(ans->init(ret_bool, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else if (OB_FAIL(ObPathUtil::add_scalar(ctx.alloc_, ans, res))) {
      LOG_WARN("add scalar failed");
    }
  }
  return ret;
}

int ObPathUtil::filter_single_node(ObPathCtx &ctx, ObPathNode* filter_node, ObSeekVector &res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(filter_node)) {
    ret= OB_BAD_NULL_ERROR;
    LOG_WARN("get filter node null", K(ret));
  } else if (filter_node->node_type_.is_arg()
             && (PN_DOUBLE != filter_node->node_type_.get_arg_type()
                || PN_STRING != filter_node->node_type_.get_arg_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get filter node type unexpect", K(ret), K(filter_node->node_type_.get_arg_type()));
  } else {
    bool ret_bool = false;
    ObPathArgNode* ans = nullptr;
    ObPathArgNode *filter_arg_node;
    ObNodeTypeAndContent *filter_content = nullptr;
    if (OB_FAIL(ObXmlUtil::alloc_arg_node(ctx.alloc_, ans))) {
      LOG_WARN("fail to alloc arg node", K(ret));
    } else if (OB_FALSE_IT(ans = new (ans) ObPathArgNode(ctx.ctx_, ObParserType::PARSER_XML_PATH))) {
    } else if (OB_ISNULL(filter_arg_node = static_cast<ObPathArgNode*>(filter_node))) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("filter arg node null", K(ret));
    } else if (OB_FAIL(ObPathUtil::alloc_node_content_info(ctx.alloc_, &filter_arg_node->arg_,
                                                            filter_node->node_type_.get_arg_type(),
                                                            filter_content))) {
      LOG_WARN("alloc filter content info failed", K(ret));
    } else if (OB_FAIL(ObXmlUtil::check_bool_rule(filter_content, ret_bool))) {
      LOG_WARN("check bool rule failed", K(ret), K(filter_content));
    } else if (OB_FAIL(ans->init(ret_bool, false))) {
      LOG_WARN("fail to init arg node", K(ret));
    } else if (OB_FAIL(ObPathUtil::add_scalar(ctx.alloc_, ans, res))) {
      LOG_WARN("add scalar failed");
    }
  }
  return ret;
}

int ObPathFilterOpNode::get_filter_ans(ObFilterOpAns& ans, ObPathCtx& filter_ctx)
{
  INIT_SUCC(ret);
  bool end_loop = false;
  ObSeekResult filter;

  for (int i = 0; i < size() && OB_SUCC(ret) && !end_loop; ++i) {
    bool filter_ans = false;
    ObPathNode* filter_node = static_cast<ObPathNode*>(member(i));
    if (OB_ISNULL(filter_node)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (filter_node->node_type_.is_arg()) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("single arg not support");
    } else if (OB_FALSE_IT(filter_node->is_seeked_ = false)) {
    } else if (OB_FAIL(filter_node->eval_node(filter_ctx, filter))) {
    } else if (OB_FAIL(ObPathUtil::seek_res_to_boolean(filter, filter_ans))) {
      LOG_WARN("seek res to boolean failed", K(ret));
    } else if (!filter_ans) {
      end_loop = true;
      ans = FILTERED_FALSE;
    } else if (i == size() - 1) {
      ans= FILTERED_TRUE;
    }
  }
  return ret;
}

int ObPathFilterOpNode::get_valid_res(ObPathCtx &ctx, ObSeekResult& res, bool is_left)
{
  INIT_SUCC(ret);
  ObPathNode* eval_path = is_left ? left_ : right_;
  bool end_seek = false;
  while (OB_SUCC(ret) && !end_seek) {
    if (OB_FAIL(eval_path->eval_node(ctx, res))) {
      LOG_WARN("fail to seek", K(ret));
    } else if (res.is_scalar_) {
      end_seek = true;
    } else if (OB_NOT_NULL(res.result_.base_)) {
      end_seek = true;
    }
  } // end while

  return ret;
}

int ObPathFilterOpNode::init_right_without_filter(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  bool get_valid_right = false;
  if (OB_NOT_NULL(right_)) {
    right_->is_seeked_ = false;
  }
  while (OB_SUCC(ret) && !get_valid_right) {
    ObSeekResult left_res;
    if (OB_FAIL(get_valid_res(ctx, left_res, true))) {
      LOG_WARN("fail to eval left");
    } else if (left_res.is_scalar_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shoudl not be scalar");
    } else if (OB_ISNULL(right_)) {
      res = left_res;
      get_valid_right = true;
    } else {
      ctx.cur_doc_ = left_res.result_.base_;
      ObSeekResult right_res;
      if (OB_FAIL(get_valid_res(ctx, right_res, false))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to eval right");
        } else {
          ret = OB_SUCCESS;
          get_valid_right = false;
        }
      } else {
        res = right_res;
        get_valid_right = true;
      }
    } // eval left success
  }
  return ret;
}

int ObPathFilterOpNode::init_right_with_filter(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  bool get_valid_right = false;
  while (OB_SUCC(ret) && !get_valid_right) {
    ObSeekResult left_res;
    if (OB_FAIL(get_valid_res(ctx, left_res, true))) {
      LOG_WARN("fail to eval left");
    } else if (left_res.is_scalar_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shoudl not be scalar");
    } else {
      ctx.cur_doc_ = left_res.result_.base_;
      ObFilterOpAns ans;
      if (OB_FAIL(get_filter_ans(ans, ctx))) {
        LOG_WARN("fail to get filter ans");
      } else if (ans == FILTERED_TRUE) {
        if (OB_NOT_NULL(right_)) {
          ObSeekResult right_res;
          if (OB_FAIL(get_valid_res(ctx, right_res, false))) {
            if (ret == OB_ITER_END) {
              // didn't get valid ans, get next left and continue
              ret = OB_SUCCESS;
              get_valid_right = false;
            } else {
              LOG_WARN("fail to get right");
            }
          } else {
            res = right_res;
            get_valid_right = true;
          }
        } else {
          res = left_res;
          get_valid_right = true;
        }
      } else if (ans == FILTERED_FALSE) {
        // this filter ans if false, get another left, and try again
        get_valid_right = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("there should have ans");
      }
    } // eval left success
  }
  return ret;
}

int ObPathFilterOpNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  ObSeekVector res_filtered;
  if (OB_ISNULL(left_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left can't be null", K(ret));
  } else if (!contain_relative_path_ && !need_cache_) {  // could get filter ans directly
    if (ans_ == ObFilterOpAns::NOT_FILTERED) {
      ObFilterOpAns ans;
      if (OB_FAIL(get_filter_ans(ans, ctx))) {
        LOG_WARN("fail to get filter ans");
      }
      ans_ = ans;
    }
    if (OB_FAIL(ret)) {
    } else if (ans_ == FILTERED_TRUE) {
      if (!is_seeked_) { // seek left and then right
        ret = init_right_without_filter(ctx, res);
        is_seeked_ = true;
      } else if (OB_NOT_NULL(right_)) {
        if (OB_FAIL(get_valid_res(ctx, res, false))) { // get_right_next
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to eval right");
          } else {
            ret = init_right_without_filter(ctx, res);
          }
        }
      } else { // right is null, just eval left
        ret = get_valid_res(ctx, res, true);
      }
    } else if (ans_ == ObFilterOpAns::FILTERED_FALSE) {
      // filter result is false
      ret = OB_ITER_END;
    } else if (ans_ == ObFilterOpAns::NOT_FILTERED) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("there should have ans");
    }
  } else if (!is_seeked_ || OB_ISNULL(right_)) {
    ret = init_right_with_filter(ctx, res);
  } else if (OB_FAIL(get_valid_res(ctx, res, false))) { // get_right_next
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to eval right");
    } else {
      ret = init_right_without_filter(ctx, res);
    }
  }

  if (OB_SUCC(ret)) {
    is_seeked_ = true;
  } else if (ret == OB_ITER_END) {
    is_seeked_ = false; // reset
    left_->is_seeked_ = false;
    if (OB_NOT_NULL(right_)) {
      right_->is_seeked_ = false;
    }
    if (contain_relative_path_ || need_cache_) {
      ans_ = NOT_FILTERED; // reset ans
    }
  }
  return ret;
}

int ObPathFilterNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  if (is_seeked_) {
    ret = OB_ITER_END;
    is_seeked_ = false;
  } else if (!need_cache_ && !contain_relative_path_ && filtered_) {
    res = ans_;
    is_seeked_ = true;
  } else if (!ctx.is_inited()) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else if (1 == count()) {
    // only one param, check bool rule for result
    ret = OB_NOT_SUPPORTED;
  } else if (count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is not filter type", K(ret));
  } else {
    // two params, filter for op
    ObPathNode* left_node;
    ObPathNode* right_node;
    ObArgType left_type;
    ObArgType right_type;
    ObFilterType op = node_type_.get_filter_type();
    ObNodeSetVector left_node_vec;
    ObNodeSetVector rigth_node_vec;
    if (OB_FAIL(ObPathUtil::get_filter_node_result(ctx, member(0), left_node))) {
      LOG_WARN("get left filter result failed", K(ret));
    } else if (OB_FAIL(ObPathUtil::get_filter_node_result(ctx, member(1), right_node))) {
      LOG_WARN("get right filter result failed", K(ret));
    } else if (OB_ISNULL(left_node) || OB_ISNULL(right_node)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("node is null", K(ret), K(left_node), K(right_node));
    } else if (op == ObFilterType::PN_AND_COND || op == ObFilterType::PN_OR_COND) {
      if (OB_FAIL(ObPathUtil::filter_logic_compare(ctx, left_node, right_node, op, res))) {
        LOG_WARN("filter_logic_compare failed", K(ret));
      }
    } else if (op == ObFilterType::PN_CMP_UNION) {
      if (in_predication_) {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("union out_predication_ not implement", K(ret));
        // if (OB_FAIL(ObPathUtil::filter_union(ctx, left_node, right_node, op, res))) {
        //   LOG_WARN("filter_union failed", K(ret));
        // }
      } else {
        // TODO outter_union
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("union out_predication_ not implement", K(ret));
      }
    } else if (OB_FAIL(ObPathUtil::get_arg_type(left_type, left_node))) {
      LOG_WARN("fail to get left type", K(ret));
    } else if (OB_FAIL(ObPathUtil::get_arg_type(right_type, right_node))) {
      LOG_WARN("fail to get right type", K(ret));
    } else if (OB_FAIL(ObPathUtil::alloc_node_set_vector(ctx, left_node, left_type, left_node_vec))) {
      LOG_WARN("alloc left node set vector failed", K(ret));
    } else if (OB_FAIL(ObPathUtil::alloc_node_set_vector(ctx, right_node, right_type, rigth_node_vec))) {
      LOG_WARN("alloc right node set vector failed", K(ret));
    } else {
      switch (op) {
      case ObFilterType::PN_CMP_UNEQUAL:
      case ObFilterType::PN_CMP_EQUAL:
      case ObFilterType::PN_CMP_GT:
      case ObFilterType::PN_CMP_GE:
      case ObFilterType::PN_CMP_LT:
      case ObFilterType::PN_CMP_LE:
        if (OB_FAIL(ObPathUtil::filter_compare(ctx,
                                                left_node_vec, left_type,
                                                rigth_node_vec, right_type,
                                                op, res))) {
          LOG_WARN("filter_compare failed", K(ret));
        }
        break;

      case ObFilterType::PN_CMP_ADD:
      case ObFilterType::PN_CMP_SUB:
      case ObFilterType::PN_CMP_MUL:
      case ObFilterType::PN_CMP_DIV:
      case ObFilterType::PN_CMP_MOD:
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("oparete implement", K(ret));
        // calculate TODO
        // if (OB_FAIL(ObPathUtil::filter_calculate(ctx,
        //                                           left_node_vec, left_type,
        //                                           rigth_node_vec, right_type,
        //                                           op, res))) {
        //   LOG_WARN("filter calculate failed", K(ret));
        // }
        break;

      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter node eval_node unexpected err", K(ret), K(op));
      }
    }
    is_seeked_ = true;
    if (OB_SUCC(ret) && !need_cache_ && !contain_relative_path_) {
      ans_ = res;
      filtered_ = true;
    }
  }
  return ret;
}

int ObPathUtil::get_filter_node_result(ObPathCtx &ctx, ObLibTreeNodeBase* filter_node_base, ObPathNode* &res)
{
  INIT_SUCC(ret);
  ObPathNode* filter_node;
  if (OB_ISNULL(filter_node = static_cast<ObPathNode*>(filter_node_base))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("filter node get null", K(ret));
  } else if (!filter_node->node_type_.is_filter()) {
    res = filter_node;
  } else {
    ObSeekResult seek_result;
    filter_node->is_seeked_ = false;
    if (OB_FAIL(filter_node->eval_node(ctx, seek_result))) {
      LOG_WARN("filter node eval node fail", K(ret));
    } else if (!seek_result.is_scalar_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("eval result size 0", K(ret));
    } else {
      res = seek_result.result_.scalar_;
    }
  }
  return ret;
}

int ObPathArgNode::eval_node(ObPathCtx &ctx, ObSeekResult& res)
{
  INIT_SUCC(ret);
  if (!ctx.is_inited()) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else if (!node_type_.is_xml_path()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect node_type", K(ret));
  } else {
    // TODO waiting
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("others type not implement", K(ret));
  }
  return ret;
}

int ObPathUtil::get_seek_vec(ObPathCtx &ctx, ObPathNode *from_node, ObSeekVector &res)
{
  INIT_SUCC(ret);
  ObArgType node_type = ObArgType::PN_ARG_ERROR;
  if (OB_FAIL(ObPathUtil::get_arg_type(node_type, from_node))) {
    LOG_WARN("fail to get node type", K(ret));
  } else if (node_type == ObArgType::PN_SUBPATH) {
    ObSeekResult path_res;
    bool end_seek = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(from_node->eval_node(ctx, path_res))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to seek", K(ret));
        }
      } else if (path_res.is_scalar_) {
        if (OB_FAIL(res.push_back(path_res))) {
          LOG_WARN("fail to push", K(ret));
        }
      } else if (path_res.result_.base_->is_tree()) {
        if (OB_FAIL(res.push_back(path_res))) {
          LOG_WARN("fail to push_back value into result", K(ret), K(res.size()));
        }
      } else if (OB_FAIL(ctx.alloc_new_bin(path_res.result_.base_))) {
        LOG_WARN("fail to alloc", K(ret));
      } else if (OB_FAIL(res.push_back(path_res))) {
        LOG_WARN("fail to push_back value into result", K(ret), K(res.size()));
      }
    } // end while
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be subpath", K(ret));
  }
  return ret;
}

int ObPathUtil::seek_res_to_boolean(ObSeekResult& filter, bool &res)
{
  INIT_SUCC(ret);
  if (!filter.is_scalar_ ) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("now, filter ans must be scalar(boolean)", K(ret));
  } else if(OB_ISNULL(filter.result_.scalar_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("arg is null", K(ret));
  } else {
    res = filter.result_.scalar_->arg_.boolean_;
  }
  return ret;
}

int ObPathUtil::get_arg_type(ObArgType& arg_type, ObPathNode *path_node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(path_node)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (path_node->get_node_type().is_arg()) {
    arg_type = path_node->get_node_type().get_arg_type();
  } else {
    arg_type = ObArgType::PN_SUBPATH;
  }
  return ret;
}

int ObPathUtil::release_seek_vector(ObPathCtx &ctx, ObSeekVector& seek_vector)
{
  INIT_SUCC(ret);
  bool need_release = true;
  for (int i = 0; OB_SUCC(ret) && need_release &&  i < seek_vector.size(); ++i) {
    ObSeekResult result = seek_vector[i];
    if (!result.is_scalar_ && OB_NOT_NULL(result.result_.base_) && result.result_.base_->is_binary()) {
      ObXmlBin* bin = static_cast<ObXmlBin*> (result.result_.base_);
      ctx.bin_pool_.free(bin);
    } else {
      need_release = false;
    }
  }
  return ret;
}

int ObPathUtil::collect_ancestor_ns(ObIMulModeBase* extend,
                                    ObStack<ObIMulModeBase*> &ancestor_record,
                                    ObXmlElement::NsMap &ns_map,
                                    ObArray<ObXmlAttribute*> &ns_vec,
                                    common::ObIAllocator* tmp_alloc)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_alloc)) {
  } else {
    int size = ancestor_record.size();
    for (int64_t pos = -1; OB_SUCC(ret) && pos < size; ++pos) {
      // get parent node
      ObXmlBin* current = nullptr;
      int64_t attribute_num = 0;
      if (pos == -1) {
        if (OB_ISNULL(extend)) { // normal, means without extend
        } else {
          current = static_cast<ObXmlBin*>(extend);
          attribute_num = current->attribute_size();
        }
      } else {
        current = static_cast<ObXmlBin*>(ancestor_record.at(pos));
        attribute_num = current->attribute_size();
      }
      bool not_att_or_ns = false;
      for (int pos = 0; OB_SUCC(ret) && pos < attribute_num && !not_att_or_ns; ++pos) {
        ObXmlBin buff(*current);
        ObXmlBin* tmp = &buff;
        if (OB_FAIL(current->construct(tmp, nullptr))) {
          LOG_WARN("failed to dup bin.", K(ret));
        } else if (OB_FAIL(tmp->set_at(pos))) {
          LOG_WARN("failed to set at child.", K(ret));
        } else if (tmp->type() == M_NAMESPACE) {
          // get ns info
          ObXmlAttribute* ns_node = nullptr;
          ObString key;
          ObString value;
          // init ns node
          if (OB_FAIL(tmp->get_key(key))) {
            LOG_WARN("failed to eval key.", K(ret));
          } else if (OB_FAIL(tmp->get_value(value))) {
            LOG_WARN("failed to eval value.", K(ret));
          } else if (OB_ISNULL(ns_node = static_cast<ObXmlAttribute*>(tmp_alloc->alloc(sizeof(ObXmlAttribute))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate attribute node.", K(ret));
          } else {
            ns_node = new(ns_node) ObXmlAttribute(ObMulModeNodeType::M_NAMESPACE, current->ctx_);
            ns_node->set_xml_key(key);
            ns_node->set_value(value);
            ret = ns_vec.push_back(ns_node);
          }

          // if found duplicate key, overwrite
          if (OB_FAIL(ret)) {
          } else if (OB_NOT_NULL(ns_map.get(key)) && OB_FAIL(ns_map.erase_refactored(key))) {
            LOG_WARN("fail to delete ns from map", K(ret), K(key));
          } else if (OB_FAIL(ns_map.set_refactored(key, ns_vec[ns_vec.size() - 1]))) {
            LOG_WARN("fail to add ns from map", K(ret), K(key));
          }
        } else if (tmp->type() == M_ATTRIBUTE) {
        } else {
          not_att_or_ns = true;  // neither ns nor attribute, stop searching
        }
      }
    }
  }
  return ret;
}

int ObPathUtil::add_ns_if_need(ObPathCtx &ctx, ObIMulModeBase*& res)
{
  INIT_SUCC(ret);
  if (!ctx.is_inited()) {
    ret = OB_INIT_FAIL;
    LOG_WARN("should be inited", K(ret));
  } else if (ctx.doc_root_->is_tree()) { // do not need add ns
  } else if (ctx.defined_ns_ > 0 || (OB_NOT_NULL(ctx.extend_) && ctx.extend_->attribute_size() > 0)) {
    ObXmlBin* bin = static_cast<ObXmlBin*>(res);
    ObXmlElement::NsMap ns_map;
    ObXmlElement::NsMap::iterator ns_map_iter;
    // be used as ns node buffer pool
    ObArray<ObXmlAttribute*> ns_vec;
    ns_vec.set_block_size(PATH_DEFAULT_PAGE_SIZE);
    int map_size = (ctx.ancestor_record_.size() > 0 ) ? 4 * ctx.ancestor_record_.size() : ctx.extend_->attribute_size();
    if (OB_FAIL(ns_map.create(map_size, "PATH_PARENT_NS"))) {
      LOG_WARN("ns map create failed", K(ret));
    } else if (OB_FAIL(collect_ancestor_ns(ctx.extend_, ctx.ancestor_record_, ns_map, ns_vec, ctx.get_tmp_alloc()))) {
      LOG_WARN("ns map init failed", K(ret));
    } else {
      ObXmlElement element_ns(ObMulModeNodeType::M_ELEMENT, ctx.doc_root_->get_mem_ctx());
      ret = element_ns.init();
      for (ns_map_iter = ns_map.begin(); OB_SUCC(ret) && ns_map_iter != ns_map.end(); ns_map_iter++) {
        if (OB_FAIL(element_ns.add_attribute(ns_map_iter->second))) {
          LOG_WARN("fail to add ns", K(ret));
        }
      }
      // serialize element node as extend area
      if (OB_SUCC(ret) && (OB_FAIL(bin->append_extend(&element_ns)))) {
        LOG_WARN("fail to append extend", K(ret));
      }
    }
    ns_map.clear();
    ns_vec.destroy();
  } // need to add ns
  return ret;
}
int ObPathUtil::alloc_path_node(ObIAllocator *allocator, ObPathNode*& node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObPathNode* path_node =
    static_cast<ObPathNode*> (allocator->alloc(sizeof(ObPathNode)));
    if (OB_ISNULL(path_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret));
    } else {
      node = path_node;
    }
  }
  return ret;
}

int ObPathUtil::alloc_binary(common::ObIAllocator *allocator, ObXmlBin*& bin)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObXmlBin* bin_node = static_cast<ObXmlBin*> (allocator->alloc(sizeof(ObXmlBin)));
    if (OB_ISNULL(bin_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret));
    } else {
      bin = bin_node;
    }
  }
  return ret;
}

int ObPathUtil::alloc_iterator(common::ObIAllocator *allocator, ObSeekIterator*& ada)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSeekIterator* node =
    static_cast<ObSeekIterator*> (allocator->alloc(sizeof(ObSeekIterator)));
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret));
    } else {
      ada = node;
    }
  }
  return ret;
}

int ObPathUtil::get_seek_iterator(common::ObIAllocator *allocator, ObPathLocationNode* loc, ObSeekIterator*& ada)
{
  INIT_SUCC(ret);
  if (loc->is_complex_seektype()) {
    ObSeekComplexIterator* tmp_ada = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_complex_iterator(allocator, tmp_ada))) {
      LOG_WARN("fail to alloc location node", K(ret));
    } else {
      tmp_ada = new (tmp_ada) ObSeekComplexIterator(allocator);
      ada = tmp_ada;
    }
  } else if (loc->is_ancestor_axis()) {
    ObSeekAncestorIterator* tmp_ada = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_ancestor_iterator(allocator, tmp_ada))) {
      LOG_WARN("fail to alloc location node", K(ret));
    } else {
      tmp_ada = new (tmp_ada) ObSeekAncestorIterator(allocator);
      ada = tmp_ada;
    }
  } else {
    ObSeekIterator* tmp_ada = nullptr;
    if (OB_FAIL(ObPathUtil::alloc_iterator(allocator, tmp_ada))) {
      LOG_WARN("fail to alloc location node", K(ret));
    } else {
      tmp_ada = new (tmp_ada) ObSeekIterator();
      ada = tmp_ada;
    }
  }
  return ret;
}

int ObPathUtil::alloc_complex_iterator(common::ObIAllocator *allocator, ObSeekComplexIterator*& ada)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSeekComplexIterator* node =
    static_cast<ObSeekComplexIterator*> (allocator->alloc(sizeof(ObSeekComplexIterator)));
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret));
    } else {
      ada = node;
    }
  }
  return ret;
}

int ObPathUtil::alloc_ancestor_iterator(common::ObIAllocator *allocator, ObSeekAncestorIterator*& ada)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSeekAncestorIterator* node =
    static_cast<ObSeekAncestorIterator*> (allocator->alloc(sizeof(ObSeekAncestorIterator)));
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at path_node", K(ret));
    } else {
      ada = node;
    }
  }
  return ret;
}

int ObPathUtil::alloc_node_content_info(ObIAllocator *allocator, ObString *str, ObNodeTypeAndContent *&res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    res = static_cast<ObNodeTypeAndContent*> (allocator->alloc(sizeof(ObNodeTypeAndContent)));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at seek result", K(ret));
    } else {
      ObPathStr path_str;
      path_str.len_ = str->length();
      path_str.name_ = str->ptr();
      ObArgNodeContent *content;
      if (OB_ISNULL(content = static_cast<ObArgNodeContent*> (allocator->alloc(sizeof(ObArgNodeContent))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at seek result", K(ret));
      } else if (OB_FALSE_IT(content->str_ = path_str)) {
      } else {
        res->type_ = ObArgType::PN_STRING;
        res->content_ = content;
      }
    }
  }
  return ret;
}

int ObPathUtil::alloc_node_content_info(ObIAllocator *allocator, ObArgNodeContent *content, ObArgType type, ObNodeTypeAndContent *&res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    res = static_cast<ObNodeTypeAndContent*> (allocator->alloc(sizeof(ObNodeTypeAndContent)));
    if (OB_ISNULL(res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at seek result", K(ret));
    } else {
      res->type_ = type;
      res->content_ = content;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase