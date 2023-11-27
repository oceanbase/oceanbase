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
 * This file contains interface define for multi mode reader abstraction.
 */

#ifndef OCEANBASE_SQL_OB_MULTI_MODE_READER
#define OCEANBASE_SQL_OB_MULTI_MODE_READER

#include "lib/xml/ob_multi_mode_interface.h"
#include "ob_tree_base.h"
#include "lib/xml/ob_xml_tree.h"
#include "ob_multi_mode_bin.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber

namespace oceanbase {
namespace common {


enum SimpleSeekType {
  ATTR_KEY,
  KEY_TYPE, // .KEY
  INDEX_TYPE, // [IDX]
  ALL_ARR_TYPE, // all element
  ALL_KEY_TYPE, //  element
  POST_SCAN_TYPE,
  PRE_SCAN_TYPE
};

class ObPathLocationNode;

typedef struct ObPathSeekInfo {
  SimpleSeekType type_;
  ObMulModeFilter* filter_;
  ObString key_;
  int64_t index_;

  ObPathSeekInfo()
    : type_(ALL_ARR_TYPE),
      filter_(nullptr),
      key_(),
      index_(-1) {}

  ObPathSeekInfo(SimpleSeekType seek_type)
    : ObPathSeekInfo()
  {
    type_ = seek_type;
  }

  ObPathSeekInfo(ObMulModeFilter* filter)
    : ObPathSeekInfo()
  {
    filter_ = filter;
  }

  ObPathSeekInfo(const ObPathSeekInfo& from)
    : type_(from.type_),
      filter_(from.filter_),
      key_(from.key_),
      index_(from.index_)
  {
  }

  ObPathSeekInfo& operator=(const ObPathSeekInfo& from)
  {
    type_ = from.type_;
    filter_ = from.filter_;
    key_ = from.key_;
    index_= from.index_;
    return *this;
  }
} ObPathSeekInfo;

class ObIMulModeBase;

struct ObMulModeReader {
  friend class ObSeekIterator;
  friend class ObSeekComplexIterator;
  enum MulModeIterFlag {
    DEFAULT_FLAG,
    SEEK_FLAG = 0x01
  };

  ~ObMulModeReader() {}

  // construct
  ObMulModeReader(ObIMulModeBase* node, MulModeIterFlag flag = DEFAULT_FLAG)
    : cur_(node),
      flags_(flag),
      is_eval_cur_(false),
      is_filtered_(false),
      seek_info_()
  {
    init();
  }

  ObMulModeReader(const ObMulModeReader& from)
  {
    cur_ = from.cur_;
    flags_ = from.flags_;
    seek_info_ = from.seek_info_;
    is_eval_cur_ = from.is_eval_cur_;
    is_filtered_= from.is_filtered_;
    init();
  }

  ObMulModeReader(ObIMulModeBase* node, const ObPathSeekInfo& info)
  {
    seek_info_ = info;
    is_eval_cur_ = false;
    is_filtered_ = false;
    cur_ = node;
    flags_ = SEEK_FLAG;
    init();
  }
  void construct(ObIMulModeBase* node, const ObPathSeekInfo& info)
  {
    cur_ = node;
    seek_info_ = info;
    is_eval_cur_ = false;
    is_filtered_ = false;
    flags_ = SEEK_FLAG;
    init();
  }
  int next(ObIMulModeBase*& node);

  int attr_next(ObIMulModeBase*& node, ObMulModeNodeType filter_type);

  int eval_entry(ObIMulModeBase*& node);

  void set_entry(ObIMulModeBase* node) {
    cur_ = node;
    init();
  }
  void reuse();
  int check_if_match(bool& is_match, bool& filtered, ObIMulModeBase* base);

  void alter_seek_param(const ObPathSeekInfo& info);

  void alter_filter(ObMulModeFilter* filter);


  // for compile

  int get_parent_node(ObIMulModeBase*& nodes) {
    return 0;
  }

  int get_upward_nodes(ObIArray<ObIMulModeBase*>& nodes) {
    return 0;
  }

  int get_children_nodes(ObIArray<ObIMulModeBase*>& nodes);

protected:
  void init();
  int scan_next(ObIMulModeBase*& node);

protected:
  ObIMulModeBase* cur_;

  uint32_t flags_;
  bool is_eval_cur_;
  bool is_filtered_;
  ObPathSeekInfo seek_info_;
  union {
    ObXmlBin::iterator bin_iter_;
    ObXmlNode::iterator tree_iter_;
  };
};


} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_MULTI_MODE_READER
