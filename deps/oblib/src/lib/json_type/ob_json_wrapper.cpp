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
 * This file contains interface support for the JSON path abstraction.
 */

#define USING_LOG_PREFIX LIB

#include "ob_json_wrapper.h"
#include "ob_json_path.h"
#include "ob_json_base.h"
#include "ob_json_compare.h"
#include "lib/xml/ob_binary_aggregate.h"

namespace oceanbase {
namespace common {

int ObJsonWrapper::seek(const ObJsonPath &path,
                        ObIAllocator &alloc,
                        ObIArray<ObJsonWrapper> &hits,
                        bool auto_wrap,
                        bool only_need_one) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == path.path_node_cnt())) {
    // Empty path ($): push self to preserve doc context for zero-copy get_raw_binary
    ret = hits.push_back(*this);
  } else if (is_bin_) {
    if (OB_FAIL(bin_.seek(path, hits, auto_wrap, only_need_one))) {
      LOG_WARN("bin view seek failed", K(ret));
    }
  } else {
    // res_json must be heap-allocated: add_if_missing stores res_point_ as the first-result
    // backing store (via clone_new_node), so the pointer must outlive this seek() call.
    void *res_json_buf = alloc.alloc(sizeof(ObJsonBin));
    if (OB_ISNULL(res_json_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ObJsonBin for DOM seek failed", K(ret));
    } else {
      ObJsonBin *res_json = new (res_json_buf) ObJsonBin(&alloc);
      ObJsonSeekResult dom_hit;
      dom_hit.res_point_ = res_json;
      if (OB_FAIL(dom_->seek(path, path.path_node_cnt(), auto_wrap, only_need_one, dom_hit))) {
        LOG_WARN("dom seek failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < dom_hit.size(); i++) {
          if (OB_FAIL(hits.push_back(ObJsonWrapper(dom_hit[i])))) {
            LOG_WARN("push dom wrapper hit failed", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonWrapper::element(uint64_t index, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    ObJsonBinView child_view;
    if (OB_FAIL(bin_.element(index, child_view))) {
      LOG_WARN("bin element fail", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_view);
    }
  } else {
    ObIJsonBase *child_base = nullptr;
    if (OB_ISNULL(dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret));
    } else if (json_type() == ObJsonNodeType::J_OBJECT) {
      if (OB_FAIL(dom_->get_object_value(index, child_base))) {
        LOG_WARN("dom get_object_value fail", K(ret), K(index));
      }
    } else {
      if (OB_FAIL(dom_->get_array_element(index, child_base))) {
        LOG_WARN("dom get_array_element fail", K(ret), K(index));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(child_base)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("child is null", K(ret), K(index));
      } else {
        out = ObJsonWrapper(child_base);
      }
    }
  }
  return ret;
}

int ObJsonWrapper::lookup(const ObString &key, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    ObJsonBinView sub_view;
    if (OB_FAIL(bin_.lookup(key, sub_view))) {
      // OB_SEARCH_NOT_FOUND is an expected miss code; let the caller handle it.
      if (ret != OB_SEARCH_NOT_FOUND) {
        LOG_WARN("bin view lookup failed", K(ret));
      }
    } else {
      out = ObJsonWrapper(sub_view);
    }
  } else {
    ObIJsonBase *child = NULL;
    if (OB_ISNULL(dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret));
    } else if (OB_FAIL(dom_->get_object_value(key, child))) {
      if (ret != OB_SEARCH_NOT_FOUND) {
        LOG_WARN("dom get_object_value by key failed", K(ret));
      }
    } else {
      out = ObJsonWrapper(child);
    }
  }
  return ret;
}

int ObJsonWrapper::get_key(uint64_t index, ObString &key) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    if (OB_FAIL(bin_.get_key(index, key))) {
      LOG_WARN("bin get_key fail", K(ret), K(index));
    }
  } else {
    if (OB_ISNULL(dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret));
    } else if (OB_FAIL(dom_->get_key(index, key))) {
      LOG_WARN("dom get_key fail", K(ret), K(index));
    }
  }
  return ret;
}

int ObJsonWrapper::get_key_value(uint64_t index, ObString &key, ObJsonWrapper &out) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    ObJsonBinView child_view;
    if (OB_FAIL(bin_.get_key_value(index, key, child_view))) {
      LOG_WARN("bin get_key_value fail", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_view);
    }
  } else {
    ObIJsonBase *child_base = nullptr;
    if (OB_ISNULL(dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret));
    } else if (OB_FAIL(dom_->get_object_value(index, key, child_base))) {
      LOG_WARN("dom get_object_value fail", K(ret), K(index));
    } else if (OB_ISNULL(child_base)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("child is null", K(ret), K(index));
    } else {
      out = ObJsonWrapper(child_base);
    }
  }
  return ret;
}

int ObJsonWrapper::get_value(uint64_t index, ObJsonWrapper &value) const
{
  return element(index, value);
}

int ObJsonWrapper::compare(const ObJsonWrapper &a, const ObJsonWrapper &b, int &result)
{
  int ret = OB_SUCCESS;
  result = -1;
  if (a.is_bin_ && b.is_bin_) {
    // both bin: use the ObJsonBinView overload to keep the zero-virtual fast path.
    ret = ObJsonCompare::compare(a.bin_, b.bin_, result, false);
  } else if (!a.is_bin_ && !b.is_bin_) {
    if (OB_ISNULL(a.dom_) || OB_ISNULL(b.dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret), KP(a.dom_), KP(b.dom_));
    } else {
      ret = ObJsonCompare::compare(*a.dom_, *b.dom_, result, false);
    }
  } else if (a.is_bin_) {
    // a is bin, b is dom. Compare dom-vs-bin via the mixed overload (which
    // keeps b on the bin fast path) and negate, since result is a<=>b.
    if (OB_ISNULL(b.dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret), KP(b.dom_));
    } else if (OB_FAIL(ObJsonCompare::compare(*b.dom_, a.bin_, result, false))) {
      LOG_WARN("compare dom vs bin fail", K(ret));
    } else {
      result = -result;
    }
  } else {
    // a is dom, b is bin.
    if (OB_ISNULL(a.dom_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dom is null", K(ret), KP(a.dom_));
    } else if (OB_FAIL(ObJsonCompare::compare(*a.dom_, b.bin_, result, false))) {
      LOG_WARN("compare dom vs bin fail", K(ret));
    }
  }
  return ret;
}


int ObJsonWrapper::binary_search(const ObIArray<ObJsonWrapper> &sorted_arr,
                                 const ObJsonWrapper &target,
                                 bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  int64_t low = 0;
  int64_t high = static_cast<int64_t>(sorted_arr.count()) - 1;
  while (OB_SUCC(ret) && low <= high) {
    int64_t mid = low + (high - low) / 2;
    int cmp = 0;
    if (OB_FAIL(ObJsonWrapper::compare(sorted_arr.at(mid), target, cmp))) {
      LOG_WARN("compare failed during binary search", K(ret), K(mid), K(low), K(high));
    } else if (cmp == 0) {
      found = true;
      break;
    } else if (cmp > 0) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return ret;
}

int ObJsonWrapper::get_raw_binary(ObIAllocator &alloc, ObString &out, bool has_lob_header) const
{
  int ret = OB_SUCCESS;
  if (is_bin_) {
    ret = bin_.get_raw_binary(alloc, out, has_lob_header);
  } else {
    ret = dom_->get_raw_binary(out, &alloc);
  }
  return ret;
}

int ObJsonWrapper::append_to_bin_agg(ObIAllocator &alloc,
                                     ObBinAggSerializer &bin_agg,
                                     ObStringBuffer &value) const
{
  int ret = OB_SUCCESS;
  ObString key;
  ObString raw_slice;
  const bool can_append_raw = is_bin_
                              && (raw_slice = bin_.raw_binary()).length() > 0
                              && ObJsonBin::need_type_prefix(static_cast<uint8_t>(bin_.vertype()));
  if (can_append_raw) {
    // Container/string/opaque: raw_binary() includes type prefix, zero-copy append
    if (OB_FAIL(bin_agg.append_raw_json_value(key, raw_slice.ptr(),
                raw_slice.length(),
                static_cast<uint8_t>(bin_.json_type())))) {
      LOG_WARN("append_raw_json_value failed", K(ret));
    }
  } else {
    ObString raw_str;
    // Inline scalar / DOM path: get_raw_binary + ObJsonBin + append_key_and_value
    if (OB_FAIL(get_raw_binary(alloc, raw_str))) {
      LOG_WARN("get_raw_binary failed", K(ret));
    } else {
      ObJsonBin j_node(raw_str.ptr(), raw_str.length(), &alloc);
      if (OB_FAIL(j_node.reset_iter())) {
        LOG_WARN("reset_iter fail", K(ret));
      } else if (OB_FAIL(bin_agg.append_key_and_value(key, value, &j_node))) {
        LOG_WARN("append_key_and_value failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonWrapper::to_json_base(ObIAllocator &alloc, ObIJsonBase *&base) const
{
  int ret = OB_SUCCESS;
  if (!is_bin_) {
    base = dom_;
  } else {
    // Get raw binary and create ObJsonBin from it
    ObString raw_bin;
    if (OB_FAIL(bin_.get_raw_binary(alloc, raw_bin))) {
      LOG_WARN("get_raw_binary fail", K(ret));
    } else {
      void *buf = alloc.alloc(sizeof(ObJsonBin));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc ObJsonBin failed", K(ret));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(raw_bin.ptr(), raw_bin.length(), &alloc);
        if (OB_FAIL(j_bin->reset_iter())) {
          LOG_WARN("reset_iter fail", K(ret));
        } else {
          base = j_bin;
        }
      }
    }
  }
  return ret;
}

int ObJsonWrapper::parse_bin(const char *data, int64_t len)
{
  int ret = OB_SUCCESS;
  is_bin_ = true;
  if (OB_FAIL(bin_.init(data, len))) {
    LOG_WARN("init json binary view fail", K(ret));
  }
  return ret;
}

int get_json_wrapper(const ObString &data,
                     ObJsonInType in_type,
                     ObIAllocator &alloc,
                     ObJsonWrapper &wrapper,
                     uint32_t parse_flag,
                     uint32_t max_depth,
                     bool enable_json_bin_view)
{
  int ret = OB_SUCCESS;
  bool input_bin_type = (in_type == ObJsonInType::JSON_BIN);
  bool parse_success = false;
  if (input_bin_type && enable_json_bin_view) {
    if (OB_FAIL(wrapper.parse_bin(data.ptr(), data.length()))) {
      if (ret == OB_NOT_SUPPORTED) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("parse json binary via view fail", K(ret));
      }
    } else {
      parse_success = true;
    }
  }
  if (OB_SUCC(ret) && !parse_success) {
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&alloc, data, in_type, in_type,
                                                 j_base, parse_flag, max_depth))) {
      LOG_WARN("get_json_base fail", K(ret), K(in_type));
    } else {
      wrapper = ObJsonWrapper(j_base);
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
