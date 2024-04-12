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
 * This file contains implementation support for the json and xml binary aggregate.
 */

#ifndef OCEANBASE_SQL_OB_BINARY_AGGREGATE
#define OCEANBASE_SQL_OB_BINARY_AGGREGATE

#include "lib/container/ob_array_iterator.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_multi_mode_bin.h"
#include "lib/xml/ob_tree_base.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/number/ob_number_v2.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"

namespace oceanbase {
namespace common {

enum ObBinAggType {
  AGG_JSON,
  AGG_XML,
  AGG_MAX
};

enum ObBinAggAllocFlag {
  AGG_ALLOC_A,
  AGG_ALLOC_B,
  AGG_ALLOC_MAX
};

struct ObAggBinKeyInfo {
  uint8_t type_;
  bool unparsed_; // special for xml
  uint64_t value_offset_;
  uint64_t value_len_;
  uint64_t offset_;
  uint32_t key_len_;
  uint32_t origin_index_; // special for xml
  TO_STRING_KV(K(type_),
                K(unparsed_),
                K(value_offset_),
                K(offset_),
                K(key_len_),
                K(origin_index_));
};

typedef common::ObArray<ObAggBinKeyInfo*> ObAggBinKeyArray;
class ObBinAggSerializer {
public:
  ObBinAggSerializer(ObIAllocator* allocator_,
                     ObBinAggType type,
                     uint8_t header_type,
                     bool need_merge_unparsed = false,
                     ObIAllocator* tmp_allocator = nullptr,
                     ObIAllocator* arr_allocator = nullptr);

  // finaly serialize
  int serialize();
  int append_key_and_value(ObString key, ObStringBuffer &value, ObJsonBin *json_val);
  int append_key_and_value(ObXmlBin *xml_bin);
  void set_header_type(uint8_t header_type) { header_type_ = header_type; }
  void set_xml_decl(ObString version, ObString encoding, uint16_t standalone);
  void set_sort_and_unique() {
    if (type_ == AGG_JSON) {
      sort_and_unique_ = true;
    }
  }
  ObStringBuffer *get_buffer() { return &buff_; };
  int64_t get_key_info_count() { return key_info_.count(); }
  int64_t get_last_count() { return count_; }
  void close_merge_text() { merge_text_ = false; }
  void open_merge_text() { merge_text_ = true; }
  int64_t get_approximate_length() { return key_.length() + value_.length(); }
  int64_t get_key_length() { return key_.length(); }
  int64_t get_value_length() { return value_.length(); }

private:
  int construct_meta();
  void construct_key_and_value();

  int rewrite_total_size();

  int construct_header();
  bool has_unique_flag() { return type_ == AGG_JSON && sort_and_unique_; }
  bool is_xml_type() { return type_ == AGG_XML; }
  bool is_json_type() { return type_ == AGG_JSON; }
  bool json_not_sort() {
    return type_ == AGG_JSON
           && header_type_ == (static_cast<uint8_t>(ObJsonNodeType::J_OBJECT))
           && !sort_and_unique_ ;
  }
  // stable sort
  // The internal sorting rules of json and xml binary are different
  void do_json_sort();
  void do_xml_sort();
  int add_unparsed_xml(ObXmlBin *xml_bin);
  int add_parsed_xml(ObXmlBin *xml_bin);
  int add_attribute_xml(ObXmlBin *xml_bin);
  int add_element_xml(ObXmlBin *xml_bin);
  int add_single_leaf_xml(ObXmlBin *xml_bin);
  int deal_last_unparsed();
  int serialize_value(int idx);
  int serialize_key(int idx);
  int reserve_meta();
  void set_key_entry(int64_t entry_idx,  int64_t key_offset, int64_t key_len);
  void set_index_entry(int64_t origin_index, int64_t sort_index);
  void set_value_entry(int64_t entry_idx,  uint8_t type, int64_t value_offset);
  bool need_to_add_node(int64_t key_count, ObMulModeNodeType type);
  int copy_and_reset(ObIAllocator* new_allocator,
                     ObIAllocator* old_allocator,
                     ObStringBuffer &add_value);
  bool first_alloc_flag() { return alloc_flag_ == ObBinAggAllocFlag::AGG_ALLOC_A; }
  void set_first_alloc() { alloc_flag_ = ObBinAggAllocFlag::AGG_ALLOC_A;}
  void set_second_alloc() { alloc_flag_ = ObBinAggAllocFlag::AGG_ALLOC_B;}
  bool check_three_allocator() { return back_allocator_ == nullptr || arr_allocator_ == nullptr; }
  bool is_json_array() { return header_type_ == static_cast<uint8_t>(ObJsonNodeType::J_ARRAY); }
  ObIAllocator* get_array_allocator() { return arr_allocator_ == nullptr ? allocator_ : arr_allocator_; }

  void set_value_entry_for_json(int64_t entry_idx,  uint8_t type, int64_t value_offset);
  void set_key(int64_t key_offset, int64_t key_len);
  void set_value(int64_t value_offset, int64_t value_len);
  static int64_t estimate_total(int64_t base_length, int64_t count, int32_t type, int64_t xml_header_size = 4);
  static int text_serialize(ObString value, ObStringBuffer &res);
  static int text_deserialize(ObString value, ObStringBuffer &res);
  static int element_serialize(ObIAllocator* allocator_, ObString value, ObStringBuffer &res);
  static constexpr int REPLACE_MEMORY_SIZE_THRESHOLD = 8 << 20; // 8M
private:
  // At present, there is no encapsulated interface for lob's append.
  // Use ObStringBuffer, and replace it with lob after the implementation of subsequent lob placement.
  ObStringBuffer value_; // value buffer
  ObStringBuffer key_; // key buffer
  ObStringBuffer buff_; // finaly buff
  ObMulBinHeaderSerializer header_; // header
  ObXmlDocBinHeader doc_header_;// special for xml
  bool last_is_unparsed_text_;
  bool last_is_text_node_;
  bool is_xml_agg_;
  bool sort_and_unique_;
  bool merge_text_;
  uint8_t header_type_;
  uint8_t alloc_flag_;

  int32_t type_; // ObBinAggType 0:json 1:xml
  int64_t key_len_;
  int64_t value_len_;
  int64_t count_;
  int64_t index_start_;
  int8_t index_entry_size_;
  int64_t key_entry_start_;
  int8_t key_entry_size_;
  int64_t value_entry_start_;
  int8_t value_entry_size_;
  int64_t key_start_;

  ObIAllocator* allocator_;
  ObIAllocator* back_allocator_;
  ObIAllocator* arr_allocator_;
  ModulePageAllocator page_allocator_;
  ObAggBinKeyArray key_info_;
};

}; // namespace common

}; // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_BINARY_AGGREGATE