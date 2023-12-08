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
 * This file contains interface implement for multi mode reader abstraction.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_xml_util.h"

namespace oceanbase {
namespace common {


int ObMulModeReader::check_if_match(bool& is_match, bool& filtered, ObIMulModeBase* base)
{
  INIT_SUCC(ret);
  is_match = false;
  filtered = false;
  if (seek_info_.type_ == KEY_TYPE) {
    ObString tmp_key;
    if (OB_FAIL(base->get_key(tmp_key))) {
      LOG_WARN("fail to get key match children xnode.", K(ret));
    } else {
      is_match = (tmp_key.compare(seek_info_.key_) == 0);
    }
  } else if (seek_info_.type_ == ALL_ARR_TYPE || seek_info_.type_ == ALL_KEY_TYPE) {
    is_match = true;
  }
  if (!is_match) {
  } else if (OB_ISNULL(seek_info_.filter_)) {
    filtered = true;
  } else if (OB_FAIL((*seek_info_.filter_)(base, filtered))) {
    LOG_WARN("fail to filter xnode", K(ret));
  }
  return ret;
}

void ObMulModeReader::reuse()
{
  is_filtered_ = true;
  init();
}

void ObMulModeReader::init()
{
  if (OB_NOT_NULL(cur_)) {
    bool is_simple_scan = false;
    bool is_ordered_scan = false;
    bool is_attr_scan = false;

    if (!(flags_ & SEEK_FLAG)) {
      is_simple_scan = true;
    } else if (seek_info_.type_ == ALL_ARR_TYPE || seek_info_.type_ == ALL_KEY_TYPE) {
      is_simple_scan = true;
    } else if (seek_info_.type_ == KEY_TYPE) {
      is_ordered_scan = true;
    } else if (seek_info_.type_ == ATTR_KEY) {
      is_attr_scan = true;
    }

    if (is_simple_scan) {
      if (cur_->is_binary()) {
        ObXmlBin* tmp = static_cast<ObXmlBin*>(cur_);
        new (&bin_iter_) ObXmlBinIterator(tmp);
        bin_iter_.set_range(tmp->get_child_start(), tmp->get_child_start() + tmp->count());
      } else {
        new (&tree_iter_) ObXmlNode::iterator(((ObXmlNode*)cur_)->begin());
      }
    } else if (is_ordered_scan) {
      if (cur_->is_binary()) {
        ObXmlBin* bin = static_cast<ObXmlBin*>(cur_);

        if (!ObXmlUtil::is_container_tc(cur_->type())) {
          bin_iter_.set_range(0, 0);
        } else {
          int64_t low = bin->low_bound(seek_info_.key_);
          int64_t upper = bin->up_bound(seek_info_.key_);

          new (&bin_iter_) ObXmlBinIterator(static_cast<ObXmlBin*>(cur_), true);
          bin_iter_.set_range(low, upper);
        }
      } else {
        IterRange range;
        ObXmlNode* node = static_cast<ObXmlNode*>(cur_);
        node->ObLibContainerNode::get_children(seek_info_.key_, range);
        new (&tree_iter_) ObXmlNode::iterator(node->sorted_begin());

        tree_iter_.set_range(range.first - tree_iter_, range.second - tree_iter_ + 1);
      }
    } else if (is_attr_scan) {
      if (cur_->is_binary()) {
        new (&bin_iter_) ObXmlBinIterator(static_cast<ObXmlBin*>(cur_));
        ObXmlBin* bin = static_cast<ObXmlBin*>(cur_);
        bin_iter_.set_range(0, bin->get_child_start());
      } else {
        ObXmlNode* handle = nullptr;
        if (!(cur_->type() == M_ELEMENT || cur_->type() == M_DOCUMENT)) {
          new (&tree_iter_) ObXmlNode::iterator((static_cast<ObXmlNode*>(cur_))->sorted_begin());
          tree_iter_.set_range(0, 0);
        } else if (OB_ISNULL(handle = static_cast<ObXmlNode*>(cur_->get_attribute_handle()))) {
          new (&tree_iter_) ObXmlNode::iterator((static_cast<ObXmlNode*>(cur_))->sorted_begin());
          tree_iter_.set_range(0, 0);
        } else {
          new (&tree_iter_) ObXmlNode::iterator(handle->begin());
        }
      }
    }
  }
}

int ObMulModeReader::eval_entry(ObIMulModeBase*& node)
{
  node = cur_;
  return OB_SUCCESS;
}

void ObMulModeReader::alter_seek_param(const ObPathSeekInfo& info)
{
  seek_info_ = info;
}

void ObMulModeReader::alter_filter(ObMulModeFilter* filter)
{
  seek_info_.filter_ = filter;
}


int ObMulModeReader::attr_next(ObIMulModeBase*& node, ObMulModeNodeType filter_type)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(cur_) || cur_->data_type() != OB_XML_TYPE || seek_info_.type_ != ATTR_KEY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_ is null or data is not xml type not supported yet.", K(ret), KP(cur_), K(seek_info_.type_));
  } else {
    bool is_found = false;

    for (; OB_SUCC(ret) && !is_found; ) {
      if (cur_->is_binary()) {
        if (!bin_iter_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bin iter is invalid.", K(ret), KP(cur_));
        } else if (bin_iter_.end()) {
          ret = OB_ITER_END;
        } else {
          node = *bin_iter_;
          ++bin_iter_;
        }
      } else {
        if (tree_iter_.end()) {
          ret = OB_ITER_END;
        } else {
          node = *tree_iter_;
          ++tree_iter_;
        }
      }

      is_found = false;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(seek_info_.filter_)) {
        is_found = true;
      } else if (OB_FAIL((*seek_info_.filter_)(node, is_found))) {
        LOG_WARN("failed to filter node.", K(ret));
      }
    }
  }

  return ret;
}

int ObMulModeReader::scan_next(ObIMulModeBase*& node)
{
  INIT_SUCC(ret);
  bool is_found = false;
  ObXmlNode::iterator save_iterator;
  ObXmlBin::iterator save_bin;

  while (OB_SUCC(ret) && !is_found) {
    if (cur_->is_binary()) {
      if (!bin_iter_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bin iter is invalid.", K(ret), KP(cur_));
      } else if (bin_iter_.end()) {
        ret = OB_ITER_END;
      } else {
        save_bin = bin_iter_;
        node = *bin_iter_;
        ++bin_iter_;
      }
    } else {
      if (tree_iter_.end()) {
        ret = OB_ITER_END;
      } else {
        save_iterator = tree_iter_;
        node = *tree_iter_;
        ++tree_iter_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(seek_info_.filter_)) {
        is_found = true;
      } else if (OB_FAIL((*seek_info_.filter_)(node, is_found))) {
        LOG_WARN("failed to filter node.", K(ret));
      }
    }
  }

  return ret;
}

int ObMulModeReader::next(ObIMulModeBase*& node)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(cur_) || cur_->data_type() != OB_XML_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_ is null or data is not xml type not supported yet.", K(ret), KP(cur_));
  } else {
    if (!(flags_ & SEEK_FLAG)) {
      if (OB_FAIL(scan_next(node))) {
        LOG_WARN("fail to filter next node.", K(ret));
      }
    } else if (seek_info_.type_ == KEY_TYPE) {
      if (get_mul_mode_tc(cur_->type()) != MulModeContainer) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(scan_next(node))) {
        LOG_WARN("fail to get key match children xnode.", K(ret));
      }
    } else if (seek_info_.type_ == INDEX_TYPE) {
      node = cur_->at(seek_info_.index_);
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get node.", K(ret), K(seek_info_.index_));
      }
    } else if (seek_info_.type_ == ALL_ARR_TYPE || seek_info_.type_ == ALL_KEY_TYPE) {
      if (get_mul_mode_tc(cur_->type()) != MulModeContainer) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(scan_next(node))) {
        LOG_WARN("fail to filter next node.", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to all children xnode.", K(ret), K(seek_info_.type_), K(cur_->data_type()));
    }
  }

  return ret;
}

int ObMulModeReader::get_children_nodes(ObIArray<ObIMulModeBase*>& nodes)
{
  INIT_SUCC(ret);
  return ret;
}

} // namespace common
} // namespace oceanbase
