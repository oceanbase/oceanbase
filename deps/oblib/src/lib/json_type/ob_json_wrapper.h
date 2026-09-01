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

#ifndef OCEANBASE_SQL_OB_JSON_WRAPPER
#define OCEANBASE_SQL_OB_JSON_WRAPPER

#include "ob_json_bin_view.h"
#include "ob_json_base.h"

namespace oceanbase {
namespace common {

class ObBinAggSerializer;
class ObStringBuffer;

// ObJsonWrapper: wraps either an ObIJsonBase* DOM node or an ObJsonBinView,
// matching MySQL's Json_wrapper role while keeping binary input on the view path.
class ObJsonWrapper
{
public:
  ObJsonWrapper() : is_bin_(false), dom_(nullptr) {}

  // Construct from binary view
  explicit ObJsonWrapper(const ObJsonBinView &view) : is_bin_(true), bin_(view) {}

  // Construct from DOM pointer (does not own it)
  explicit ObJsonWrapper(ObIJsonBase *dom) : is_bin_(false), dom_(dom) {}

  OB_INLINE bool is_bin() const { return is_bin_; }

  OB_INLINE ObJsonNodeType json_type() const
  {
    return is_bin_ ? bin_.json_type() : dom_->json_type();
  }

  OB_INLINE ObObjType field_type() const
  {
    return is_bin_ ? bin_.field_type() : dom_->field_type();
  }

  OB_INLINE uint32_t depth() const { return is_bin_ ? bin_.depth() : dom_->depth(); }

  OB_INLINE uint64_t member_count() const
  {
    return is_bin_ ? bin_.member_count() : dom_->member_count();
  }

  OB_INLINE uint32_t element_count() const
  {
    return static_cast<uint32_t>(member_count());
  }

  // Get array/object element by index, wrapped. View path returns a sub-view
  // (zero-copy into the same document buffer); DOM path returns a child node.
  int element(uint64_t index, ObJsonWrapper &out) const;

  // Object: O(log n) lookup by key, wrapped. Returns OB_SEARCH_NOT_FOUND when
  // the key is absent (both bin and DOM paths agree on this code).
  int lookup(const ObString &key, ObJsonWrapper &out) const;

  // Object: get key string at the given index without materializing the value.
  // Both bin and DOM paths return OB_OUT_OF_ELEMENT when index >= member_count.
  int get_key(uint64_t index, ObString &key) const;

  // Object/array: get value at the given index without materializing the value.
  // Both bin and DOM paths return OB_OUT_OF_ELEMENT when index >= member_count.
  int get_value(uint64_t index, ObJsonWrapper &value) const;

  // Object: get key string and value (wrapped) at the given index in one call.
  int get_key_value(uint64_t index, ObString &key, ObJsonWrapper &out) const;

  // Compare two wrappers regardless of representation. both-bin uses the
  // ObJsonBinView::compare fast path; both-dom uses the ObIJsonBase path. For
  // mixed bin/dom it calls the ObJsonCompare mixed overload directly, which
  // keeps the bin side on its zero-virtual view path and the dom side on its
  // virtual path without materializing either side (containers included).
  static int compare(const ObJsonWrapper &a, const ObJsonWrapper &b, int &result);

  // Binary search `target` in an array already sorted by ObJsonWrapperLess. Sets
  // found=true on a type+value match. Hand-written low/high loop (mirrors the
  // bin-path lookup_impl) so a compare error breaks immediately and is reported
  // via ret, instead of std::binary_search which cannot surface an error.
  static int binary_search(const ObIArray<ObJsonWrapper> &sorted_arr,
                           const ObJsonWrapper &target,
                           bool &found);

  // Unified seek - works for both view and DOM paths
  // View path: dispatches to ObJsonBinView::seek, wraps results
  // DOM path: dispatches to ObIJsonBase::seek, wraps results
  int seek(const ObJsonPath &path,
           ObIAllocator &alloc,
           ObIArray<ObJsonWrapper> &hits,
           bool auto_wrap = false,
           bool only_need_one = false) const;

  // Get raw binary for serialization.
  // has_lob_header: if true, prepend ObLobCommon header before the JSON binary.
  int get_raw_binary(ObIAllocator &alloc, ObString &out, bool has_lob_header = false) const;

  // Parse a binary JSON document into this wrapper.
  int parse_bin(const char *data, int64_t len);

  // Append this node to a ObBinAggSerializer for building JSON arrays.
  // View path: zero-copy append_raw_json_value for containers/strings/opaque;
  //            fallback to ObJsonBin for inline scalars.
  // DOM path: get_raw_binary + ObJsonBin + append_key_and_value.
  int append_to_bin_agg(ObIAllocator &alloc,
                        ObBinAggSerializer &bin_agg,
                        ObStringBuffer &value) const;

  // Upgrade to full ObIJsonBase* (for write operations or when DOM is needed)
  int to_json_base(ObIAllocator &alloc, ObIJsonBase *&base) const;

  // Access the underlying view (only valid when is_bin_ is true)
  OB_INLINE const ObJsonBinView& get_bin_view() const
  {
    OB_ASSERT(is_bin_);
    return bin_;
  }
  OB_INLINE ObIJsonBase* get_dom() const { return is_bin_ ? nullptr : dom_; }

  // Required by ObSEArray/ObIArray for printing
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "{is_bin:%d, type:%d}",
                    is_bin_, static_cast<int>(json_type()));
    return pos;
  }

private:
  bool is_bin_;
  union {
    ObJsonBinView bin_;
    ObIJsonBase *dom_;
  };
};

// Total order over ObJsonWrapper for ob_sort / binary_search.
// Cross-type ordering is decided by json_type(), so unrelated types never reach
// compare(). For same-type elements compare() only fails on corrupted data (e.g.
// out-of-bounds binary offsets); when it does we record the first error code
// through ret_ (if provided) and return a deterministic false. The deterministic
// false keeps the comparator self-consistent so ob_sort / binary_search never enter
// undefined behaviour, while the recorded ret_ lets the caller fail the statement
// after sorting instead of silently producing a wrong result.
struct ObJsonWrapperLess {
  ObJsonWrapperLess() : ret_(NULL) {}
  explicit ObJsonWrapperLess(int *ret) : ret_(ret) {}
  bool operator()(const ObJsonWrapper &a, const ObJsonWrapper &b) const {
    int cmp = 0;
    int ret = ObJsonWrapper::compare(a, b, cmp);
    if (OB_SUCCESS != ret) {
      if (NULL != ret_ && OB_SUCCESS == *ret_) {
        *ret_ = ret; // record first error; caller checks after sort / search
      }
      return false;
    }
    return cmp < 0;
  }
  int *ret_;
};

// Unified input function: parses JSON data into ObJsonWrapper. The DOM-vs-bin
// representation is chosen solely from in_type: JSON_BIN input uses ObJsonBinView
// (fast path, zero virtual dispatch) and falls back to ObJsonBaseFactory::
// get_json_base on OB_NOT_SUPPORTED; JSON_TREE input goes directly to the DOM path.
// enable_json_bin_view gates the ObJsonBinView fast path: when false, JSON_BIN input
// skips parse_bin and goes straight to the get_json_base tree path. This lets
// callers (e.g. json_contains/member_of/overlaps) honor _enable_fast_json_path_lookup.
int get_json_wrapper(const ObString &data,
                     ObJsonInType in_type,
                     ObIAllocator &alloc,
                     ObJsonWrapper &wrapper,
                     uint32_t parse_flag = 0,
                     uint32_t max_depth = 0,
                     bool enable_json_bin_view = true);

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_JSON_WRAPPER
