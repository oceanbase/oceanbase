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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_binary_aggregate.h"

namespace oceanbase {
namespace common {

struct ObJsonBinAggKeyCompare {
  ObStringBuffer *buff_;

  int operator()(const ObAggBinKeyInfo *left, const ObAggBinKeyInfo *right) {
    int res = 0;
    if (left->key_len_ != right->key_len_) {
      res = left->key_len_ < right->key_len_;
    } else {
      ObString left_str = ObString(left->key_len_, buff_->ptr() + left->offset_);
      ObString right_str = ObString(right->key_len_, buff_->ptr() + right->offset_);
      res = (left_str.compare(right_str) < 0);
    }
    return res;
  }

  int compare(const ObAggBinKeyInfo *left, const ObAggBinKeyInfo *right) {
    int res = 0;
    if (left->key_len_ != right->key_len_) {
      res = left->key_len_ < right->key_len_;
    } else {
      ObString left_str = ObString(left->key_len_, buff_->ptr() + left->offset_);
      ObString right_str = ObString(right->key_len_, buff_->ptr() + right->offset_);
      res = (left_str.compare(right_str) < 0);
    }
    return res;
  }
};

struct ObXmlBinAggKeyCompare {
  ObStringBuffer *buff_;

  int operator()(const ObAggBinKeyInfo *left, const ObAggBinKeyInfo *right) {
    ObString left_str = ObString(left->key_len_, buff_->ptr() + left->offset_);
    ObString right_str = ObString(right->key_len_, buff_->ptr() + right->offset_);
    return (left_str.compare(right_str) < 0);
  }

  int compare(const ObAggBinKeyInfo *left, const ObAggBinKeyInfo *right) {
    ObString left_str = ObString(left->key_len_, buff_->ptr() + left->offset_);
    ObString right_str = ObString(right->key_len_, buff_->ptr() + right->offset_);
    return (left_str.compare(right_str) < 0);
  }
};

ObBinAggSerializer::ObBinAggSerializer(ObIAllocator* allocator,
                                       ObBinAggType type,
                                       uint8_t header_type,
                                       bool need_merge_unparsed,
                                       ObIAllocator* back_allocator,
                                       ObIAllocator* arr_allocator)
  : value_(allocator),
    key_(allocator),
    buff_(allocator),
    last_is_unparsed_text_(false),
    last_is_text_node_(false),
    is_xml_agg_(need_merge_unparsed),
    sort_and_unique_(false),
    merge_text_(true),
    header_type_(header_type),
    alloc_flag_(ObBinAggAllocFlag::AGG_ALLOC_A),
    type_(type),
    key_len_(0),
    value_len_(0),
    count_(0),
    index_start_(0),
    index_entry_size_(0),
    key_entry_start_(0),
    key_entry_size_(0),
    value_entry_start_(0),
    value_entry_size_(0),
    key_start_(0),
    allocator_(allocator),
    back_allocator_(back_allocator),
    arr_allocator_(arr_allocator),
    page_allocator_(*(arr_allocator == nullptr ? allocator : arr_allocator), common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
    key_info_(OB_MALLOC_NORMAL_BLOCK_SIZE, page_allocator_)
{
  new (&header_) ObMulBinHeaderSerializer();
  if (type_ == AGG_XML) {
    new (&doc_header_) ObXmlDocBinHeader(MEMBER_LAZY_SORTED);
  }

}

// for json
int ObBinAggSerializer::append_key_and_value(ObString key, ObStringBuffer &value, ObJsonBin *json_val)
{
  INIT_SUCC(ret);
  value.reuse();
  ObAggBinKeyInfo *key_info = nullptr;
  int64_t value_record = value_.length();

  ObIAllocator * arr_allocator = get_array_allocator();
  if (OB_ISNULL(key_info = static_cast<ObAggBinKeyInfo*>
                          (arr_allocator->alloc(sizeof(ObAggBinKeyInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate key info struct failed", K(ret));
  } else {
    int key_count = key_info_.count();
    key_info->key_len_ = key.length();
    key_info->origin_index_ = 0;
    key_info->unparsed_ = false;
    key_info->type_ = static_cast<uint8_t>(json_val->json_type());
    key_info->value_offset_ = value_record;
    key_info->offset_ = key_count == 0 ?
                        0 : key_info_.at(key_count-1)->offset_ + key_info_.at(key_count-1)->key_len_;

    if (OB_FAIL(json_val->get_total_value(value))) {
      LOG_WARN("get total value failed", K(ret));
    } else if (OB_FAIL(key_.append(key.ptr(), key.length()))) {
      LOG_WARN("failed to append key into key_", K(ret), K(key));
    } else {
      uint64_t need_size = value_.length() + value.length() + 8;
      if (check_three_allocator() || need_size <= value_.capacity() || need_size < REPLACE_MEMORY_SIZE_THRESHOLD) {
        if (OB_FAIL(value_.append(value.ptr(), value.length(), 0))) {
          LOG_WARN("failed to append key into key_", K(ret), K(value));
        }
      } else {
        if (first_alloc_flag()) {
          if (OB_FAIL(copy_and_reset(back_allocator_, allocator_, value))) {
            LOG_WARN("failed to copy and reset.", K(ret));
          } else {
            set_second_alloc();
          }
        } else {
          if (OB_FAIL(copy_and_reset(allocator_, back_allocator_, value))) {
            LOG_WARN("failed to copy and reset.", K(ret));
          } else {
            set_first_alloc();
          }
        }
      }
      key_info->value_len_ = value.length();
      if (OB_SUCC(ret) && OB_FAIL(key_info_.push_back(key_info))) {
        LOG_WARN("failed to push back key_info.", K(ret));
      }
    }
  }

  return ret;
}

// for xml
int ObBinAggSerializer::append_key_and_value(ObXmlBin *xml_bin)
{
  INIT_SUCC(ret);
  if (is_xml_agg_) {
    if (xml_bin->meta_.is_unparse_) {
      if (OB_FAIL(add_unparsed_xml(xml_bin))) {
        LOG_WARN("add parsed xml failed", K(ret));
      }
    } else {
      if (OB_FAIL(add_parsed_xml(xml_bin))) {
        LOG_WARN("add parsed xml failed", K(ret));
      }
    }
  } else {
    ObMulModeNodeType type = xml_bin->type();
    if (type == M_ELEMENT || type == M_INSTRUCT) {
      if (OB_FAIL(add_element_xml(xml_bin))) {
        LOG_WARN("add element failed", K(ret));
      }
    } else if (type == M_DOCUMENT || type == M_CONTENT) {
      if (OB_FAIL(add_parsed_xml(xml_bin))) {
        LOG_WARN("add parsed xml failed", K(ret));
      }
    } else {
      if (OB_FAIL(add_single_leaf_xml(xml_bin))) {
        LOG_WARN("add single leaf xml failed", K(ret));
      }
    }
  }

  return ret;
}

// for element
int ObBinAggSerializer::add_element_xml(ObXmlBin *xml_bin)
{
  INIT_SUCC(ret);
  ObString value;
  ObAggBinKeyInfo *key_info = nullptr;
  int32_t count = xml_bin->meta_.count_;
  int64_t key_count = key_info_.count();
  int64_t value_record = value_.length();
  ObString key;

  if (last_is_text_node_ && OB_FAIL(deal_last_unparsed())) {
    LOG_WARN("failed to deal with last unparsed.", K(ret));
  } else {
    value_record = value_.length();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(xml_bin->get_key(key))) {
    LOG_WARN("get key failed.", K(ret));
  } else if (OB_FAIL(value_.append(xml_bin->get_element_buffer()))) {
  } else if (OB_ISNULL(key_info = static_cast<ObAggBinKeyInfo*>
                                  (allocator_->alloc(sizeof(ObAggBinKeyInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate key info struct failed", K(ret));
  } else {
    int64_t current_count = key_count;
    key_info->key_len_ = key.length();
    key_info->value_len_ = 0;
    key_info->type_ = xml_bin->type();
    key_info->unparsed_ = xml_bin->meta_.is_unparse_;
    key_info->origin_index_ = key_count;
    key_info->value_offset_ = value_record;
    key_info->offset_ = current_count == 0 ?
                        0 : key_info_.at(current_count - 1)->offset_ + key_info_.at(current_count -1)->key_len_;

    if (OB_FAIL(key_info_.push_back(key_info))) {
      LOG_WARN("failed to append key info.", K(ret));
    } else if (OB_FAIL(key_.append(key.ptr(), key.length()))) {
      LOG_WARN("failed to append key into key_", K(ret), K(key));
    }
  }

  return ret;
}

// for leaf
int ObBinAggSerializer::add_single_leaf_xml(ObXmlBin *xml_bin)
{
  INIT_SUCC(ret);
  ObString value;
  ObAggBinKeyInfo *key_info = nullptr;
  int32_t count = xml_bin->meta_.count_;
  int64_t key_count = key_info_.count();

  ObMulModeNodeType type = xml_bin->type();
  ObStringBuffer xml_text(allocator_);
  if (last_is_text_node_ && !merge_text_ && OB_FAIL(deal_last_unparsed())) {
    LOG_WARN("failed to deal with last unparsed.", K(ret));
  } else if (type == M_ATTRIBUTE || type == M_NAMESPACE) {
    type = M_TEXT;
    last_is_text_node_ = true;
    if (OB_FAIL(xml_bin->get_value(value))) {
      LOG_WARN("failed to get value.", K(ret));
    } else if (OB_FAIL(xml_text.append(value.ptr(), value.length()))) {
      LOG_WARN("failed to append vlaue.", K(ret));
    }
  } else if (type == M_TEXT) {
    last_is_text_node_ = true;
    if (OB_FAIL(xml_bin->get_text_value(value))) {
      LOG_WARN("failed to get text value", K(ret));
    } else if (OB_FAIL(xml_text.append(value.ptr(), value.length()))) {
      LOG_WARN("failed to append vlaue.", K(ret));
    }
  } else {
    if (last_is_text_node_ && OB_FAIL(deal_last_unparsed())) {
      LOG_WARN("failed to deal with last unparsed.", K(ret));
    } else if (OB_FAIL(xml_text.append(xml_bin->meta_.data_, xml_bin->meta_.len_))) {
      LOG_WARN("failed to append xmltext.", K(ret));
    }
  }
  int64_t value_record = value_.length();

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(value_.append(xml_text.ptr(), xml_text.length()))) {
    LOG_WARN("append failed.", K(xml_text), K(ret));
  } else if (need_to_add_node(key_count, type)) {
    if (OB_ISNULL(key_info = static_cast<ObAggBinKeyInfo*>
                            (allocator_->alloc(sizeof(ObAggBinKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate key info struct failed", K(ret));
    } else {
      key_info->key_len_ = 0;
      key_info->value_len_ = 0;
      key_info->type_ = type;
      key_info->unparsed_ = false;
      key_info->origin_index_ = key_count;
      key_info->value_offset_ = value_record;
      key_info->offset_ = key_.length();
      if (OB_FAIL(key_info_.push_back(key_info))) {
        LOG_WARN("failed to push back key info into array", K(ret), K(key_info));
      }
    }
  }

  return ret;
}

bool ObBinAggSerializer::need_to_add_node(int64_t key_count, ObMulModeNodeType type)
{
  bool res = false;
  if (key_count == 0) {
    res = true;
  } else if (!merge_text_) {
    res = true;
  } else if (!(type == M_TEXT && key_info_.at(key_count - 1)->type_ == M_TEXT)) {
    res = true;
  } else {
    res = false;
  }
  return res;
}

// for content and document
int ObBinAggSerializer::add_parsed_xml(ObXmlBin *xml_bin)
{
  INIT_SUCC(ret);

  ObString value;
  ObAggBinKeyInfo *key_info = nullptr;
  int32_t count = xml_bin->meta_.count_;
  int64_t key_count = key_info_.count();
  int64_t value_record = value_.length();
  int64_t bin_value_start = 0;

  if (last_is_unparsed_text_ && OB_FAIL(deal_last_unparsed())) {
    LOG_WARN("failed to deal with last unparsed.", K(ret));
  } else {
    value_record = value_.length();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(xml_bin->get_value_start(bin_value_start))) {
    LOG_WARN("failed to get value start.", K(ret));
  } else if (OB_FAIL(xml_bin->get_total_value(value, bin_value_start))) {
    LOG_WARN("failed to get total value.", K(ret));
  } else if (OB_FAIL(value_.append(value.ptr(), value.length(), 0))) {
    LOG_WARN("failed to append key into key_", K(ret), K(value));
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint8_t type = 0;
    int64_t origin_index = 0;
    int64_t bin_value_offset = 0;

    ObString key;
    if (OB_FAIL(xml_bin->get_index_key(key, origin_index, bin_value_offset, i))) {
      LOG_WARN("get index key failed", K(i));
    } else if (OB_FAIL(xml_bin->get_value_entry_type(type, origin_index))) {
      LOG_WARN("get value entry type failed", K(ret));
    } else if (type < 0 || type >= M_MAX_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get type invalid.", K(ret), K(type));
    } else if (OB_ISNULL(key_info = static_cast<ObAggBinKeyInfo*>
                                    (allocator_->alloc(sizeof(ObAggBinKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate key info struct failed", K(ret));
    } else {
      int64_t current_count = key_count + i;
      key_info->key_len_ = key.length();
      key_info->value_len_ = 0;
      key_info->type_ = type;
      key_info->unparsed_ = xml_bin->meta_.is_unparse_;
      key_info->origin_index_ = current_count;
      key_info->value_offset_ = bin_value_offset - bin_value_start + value_record;
      key_info->offset_ = current_count == 0 ?
                          0 : key_info_.at(current_count - 1)->offset_ + key_info_.at(current_count -1)->key_len_;

      if (OB_FAIL(key_info_.push_back(key_info))) {
        LOG_WARN("failed to push back key info into array", K(ret), K(key_info));
      } else if (OB_FAIL(key_.append(key.ptr(), key.length()))) {
        LOG_WARN("failed to append key into key_", K(ret), K(key));
      }
    }
  }

  return ret;
}

// for text
int ObBinAggSerializer::add_unparsed_xml(ObXmlBin *xml_bin)
{
  INIT_SUCC(ret);
  ObString value;
  ObAggBinKeyInfo *key_info = nullptr;
  int64_t key_count = key_info_.count();
  int64_t value_record = value_.length();
  ObStringBuffer xml_text(allocator_);
  if (xml_bin->type() == M_ATTRIBUTE) {
    if (OB_FAIL(xml_bin->get_value(value))) {
      LOG_WARN("failed to get value.", K(ret));
    } else if (OB_FAIL(xml_text.append(value.ptr(), value.length()))){
      LOG_WARN("failed to append value.", K(ret), K(value));
    }
  } else {
    if (OB_FAIL(xml_bin->print_xml(xml_text, 0, 0, 0))) {
      LOG_WARN("failed to print xml bin.", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(value_.append(xml_text.ptr(), xml_text.length()))) {
    LOG_WARN("failed to append value.", K(ret));
  } else if (key_count == 0 || !key_info_.at(key_count - 1)->unparsed_) {
    if (OB_ISNULL(key_info = static_cast<ObAggBinKeyInfo*>
                            (allocator_->alloc(sizeof(ObAggBinKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate key info struct failed", K(ret));
    } else {
      key_info->key_len_ = 0;
      key_info->value_len_ = 0;
      key_info->type_ = ObMulModeNodeType::M_ELEMENT;
      key_info->unparsed_ = true;
      key_info->origin_index_ = key_count;
      key_info->value_offset_ = value_record;
      key_info->offset_ = key_.length();
      if (OB_FAIL(key_info_.push_back(key_info))) {
        LOG_WARN("failed to push back key info into array", K(ret), K(key_info));
      }
    }
  }
  last_is_unparsed_text_ = true;

  return ret;
}

/*
Estimated total length
@param[in]  part length
@return approximate size

First estimate an estimate_smaller based on the existing length,
then calculate the total size based on this estimate_smaller,
then calculate the estimated block size based on the calculated total,
and then compare the estimated block size with the initial estimate_smaller,
if it is not in the same order of magnitude, then Go up one order of magnitude

  K: key_len V: value_len C: count S(a): sizeof(a)
  Roughly think:
      S(total) = ceil((log2 total)  / 8)
      S(total) = [1, 4]
      head = 0
      FOR XML: S(C) * C
      header_: obj_var_offset_ + obj_var_size_
K + V + (1 + S(total)) * C + 2 * S(total) * C + S(C) * C + header = total
*/
int64_t ObBinAggSerializer::estimate_total(int64_t base_length, int64_t count,
                                            int32_t type, int64_t xml_header_size)
{
  int64_t res = 0;
  uint8_t estimate_smaller_type = ObMulModeVar::get_var_type(base_length);
  uint8_t estimate_smaller = ObMulModeVar::get_var_size(estimate_smaller_type);
  uint8_t estimated_size_type = 0;
  do {
    estimate_smaller = ObMulModeVar::get_var_size(estimate_smaller_type);
    uint8_t count_type = ObMulModeVar::get_var_type(count);
    uint8_t count_size = ObMulModeVar::get_var_size(count_type);

    // for head_
    uint8_t header_obj_var_size_type = ObMulModeVar::get_var_type(res > 0 ? res : base_length);
    uint8_t header_obj_var_size = ObMulModeVar::get_var_size(header_obj_var_size_type);
    uint8_t header_obj_var_offset = MUL_MODE_BIN_HEADER_LEN + count_size;
    uint64_t header_size = header_obj_var_offset + header_obj_var_size;

    // for total
    int64_t total = base_length + (sizeof(uint8_t) + estimate_smaller) * count +
                    2 * estimate_smaller * count + header_size;
    if (type == AGG_XML) {
      total += count_size * count + xml_header_size;
    }
    estimated_size_type = ObMulModeVar::get_var_type(total);
    res = total;
  } while (estimate_smaller_type < ObMulModeBinLenSize::MBL_UINT64
            && estimate_smaller_type++ < estimated_size_type);
  return res;
}

int ObBinAggSerializer::construct_header()
{
  INIT_SUCC(ret);
  ObStringBuffer header_buff(allocator_);
  ObStringBuffer doc_header_buff(allocator_);

  uint64_t count = key_info_.count();
  int64_t key_len = key_.length();
  int64_t value_len = value_.length();

  if (has_unique_flag()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_info_.count() - 1; i++) {
      ObAggBinKeyInfo *key_info = key_info_.at(i);
      ObAggBinKeyInfo *next_key_info = key_info_.at(i + 1);
      if (key_info->key_len_ == next_key_info->key_len_) {
        int64_t this_key_start = i == 0 ? 0 : key_info_.at(i - 1)->offset_ + key_info_.at(i - 1)->key_len_;
        int64_t next_key_start = this_key_start + key_info->key_len_;
        ObString this_key(key_info->key_len_, key_.ptr() + key_info->offset_);
        ObString next_key(next_key_info->key_len_, key_.ptr() + next_key_info->offset_);
        if (this_key.compare(next_key) == 0) {
          key_info->unparsed_ = true;
          count--;
          key_len -= key_info->key_len_;
          value_len -= key_info->value_len_;
        }
      }
    }
  }
  count_ = count;
  key_len_ = key_len;
  value_len_ = value_len;

  ObString header_str;
  int64_t buff_length = buff_.length();
  if (type_ == AGG_XML) {
    buff_length = buff_.length();
    doc_header_.serialize(doc_header_buff);
  }

  int64_t total_size = ObBinAggSerializer::estimate_total(value_len_ + key_len_,
                                                          count_, type_, doc_header_buff.length());
  ObMulBinHeaderSerializer header_serializer(&header_buff,
                                              static_cast<ObMulModeNodeType>(header_type_),
                                              total_size,
                                              count_);
  if (OB_FAIL(header_serializer.serialize())) {
    LOG_WARN("header serialize failed.", K(ret));
  } else if (OB_FALSE_IT(header_str = header_serializer.buffer()->string())) {
  } else if (OB_FAIL(buff_.reserve(total_size))) {
    LOG_WARN("buff reserver failed.", K(ret), K(total_size));
  } else if (OB_FAIL(buff_.append(header_str.ptr(), header_str.length(), 0))) {
    LOG_WARN("failed to append.", K(header_str));
  } else if (doc_header_buff.length() != 0 &&
              OB_FAIL(buff_.append(doc_header_buff.ptr(), doc_header_buff.length(), 0))) {
    LOG_WARN("failed to append.", K(doc_header_buff));
  } else {
    header_ = header_serializer;
  }

  return ret;
}

void ObBinAggSerializer::set_index_entry(int64_t origin_index, int64_t sort_index)
{
  int64_t offset = index_start_ + origin_index * index_entry_size_;
  char* write_buf = buff_.ptr() + offset;
  ObMulModeVar::set_var(sort_index, header_.get_count_var_size_type(), write_buf);
}

void ObBinAggSerializer::set_key_entry(int64_t entry_idx,  int64_t key_offset, int64_t key_len)
{
  int64_t offset = key_entry_start_ + entry_idx * (key_entry_size_ * 2);
  char* write_buf = buff_.ptr() + offset;
  ObMulModeVar::set_var(key_offset, header_.get_entry_var_size_type(), write_buf);

  write_buf += key_entry_size_;
  ObMulModeVar::set_var(key_len, header_.get_entry_var_size_type(), write_buf);
}

void ObBinAggSerializer::set_value_entry(int64_t entry_idx,  uint8_t type, int64_t value_offset)
{
  int64_t offset = value_entry_start_ + entry_idx * (value_entry_size_ + sizeof(uint8_t));
  char* write_buf = buff_.ptr() + offset;
  *reinterpret_cast<uint8_t*>(write_buf) = type;
  ObMulModeVar::set_var(value_offset, header_.get_entry_var_size_type(), write_buf + sizeof(uint8_t));
}

void ObBinAggSerializer::set_value_entry_for_json(int64_t entry_idx,  uint8_t type, int64_t value_offset)
{
  int64_t offset = value_entry_start_ + entry_idx * (value_entry_size_ + sizeof(uint8_t));
  char* write_buf = buff_.ptr() + offset;
  ObMulModeVar::set_var(value_offset, header_.get_entry_var_size_type(), write_buf);
  write_buf += value_entry_size_;
  *reinterpret_cast<uint8_t*>(write_buf) = type;
}

void ObBinAggSerializer::set_key(int64_t key_offset, int64_t key_len)
{
  char* write_buf = key_.ptr() + key_offset;
  buff_.append(write_buf, key_len);
}

void ObBinAggSerializer::set_value(int64_t value_offset, int64_t value_len)
{
  char* write_buf = value_.ptr() + value_offset;
  buff_.append(write_buf, value_len, 0);
}

void ObBinAggSerializer::set_xml_decl(ObString version, ObString encoding, uint16_t standalone)
{
  ObXmlDocBinHeader new_doc_header(version, encoding, 0, standalone, 1);
  doc_header_ = new_doc_header;
}

int ObBinAggSerializer::reserve_meta()
{
  INIT_SUCC(ret);
  int64_t pos = buff_.length();
  uint32_t reserve_size = key_start_ - index_start_;
  if (OB_FAIL(buff_.set_length(pos + reserve_size))) {
    LOG_WARN("failed to set length.", K(ret), K(pos + reserve_size));
  }
  return ret;
}

int ObBinAggSerializer::construct_meta()
{
  INIT_SUCC(ret);
  index_start_ = header_.header_size();
  if (type_ == ObBinAggType::AGG_XML) {
    index_start_ += doc_header_.header_size();
  }

  index_entry_size_ = type_ == AGG_XML ? header_.get_count_var_size() : 0;
  key_entry_start_ = index_start_ + index_entry_size_ * count_;
  key_entry_size_ = value_entry_size_ = header_.get_entry_var_size();
  value_entry_start_ = (type_ == ObBinAggType::AGG_JSON &&
                        header_type_ == static_cast<uint8_t>(ObJsonNodeType::J_ARRAY)) ?
                        key_entry_start_ :
                        key_entry_start_ + (key_entry_size_ * 2) * count_;
  key_start_ = value_entry_start_ + (sizeof(uint8_t) + value_entry_size_) * count_;
  int64_t value_start = key_start_ + key_len_;

  if (key_start_ > header_.total_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key start unexpected.", K(ret), K(key_start_));
  } else if (OB_FAIL(reserve_meta())) {
    LOG_WARN("failed to reserve meta.", K(ret), K(buff_.length()));
  } else {
    int64_t key_offset = 0;
    int64_t i_offset = 0;
    int64_t value_offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_info_.count(); i++) {
      ObAggBinKeyInfo *key_info = key_info_.at(i);
      if (type_ == ObBinAggType::AGG_XML) {
        set_index_entry(key_info->origin_index_, i);
        set_key_entry(i, key_start_ + key_offset, key_info->key_len_);
        set_value_entry(i, key_info->type_, value_start + key_info->value_offset_);
        key_offset += key_info->key_len_;
      } else if (!has_unique_flag()) {
        if (header_type_ == static_cast<uint8_t>(ObJsonNodeType::J_OBJECT)) {
          set_key_entry(i, key_start_ + key_offset, key_info->key_len_);
        }
        set_value_entry_for_json(i, key_info->type_, value_start + key_info->value_offset_);
        key_offset += key_info->key_len_;
      } else if (!key_info->unparsed_) {
        if (header_type_ == static_cast<uint8_t>(ObJsonNodeType::J_OBJECT)) {
          set_key_entry(i_offset, key_start_ + key_offset, key_info->key_len_);
        }
        set_value_entry_for_json(i_offset, key_info->type_, value_offset + value_start);
        key_offset += key_info->key_len_;
        value_offset += key_info->value_len_;
        i_offset++;
      }

    }
  }

  return ret;
}

int ObBinAggSerializer::text_serialize(ObString value, ObStringBuffer &res)
{
  INIT_SUCC(ret);

  int64_t header_size = sizeof(uint8_t);
  int64_t ser_len = serialization::encoded_length_vi64(value.length());

  if (OB_FAIL(res.reserve(ser_len + header_size + value.length()))) {
    LOG_WARN("failed to resoerve serialize size for text.", K(ret), K(ser_len));
  } else if (OB_FAIL(ObMulModeVar::set_var(ObMulModeNodeType::M_TEXT,
                                            ObMulModeBinLenSize::MBL_UINT8,
                                            res.ptr() + res.length()))) {
    LOG_WARN("failed to set var.", K(ret));
  } else {
    res.set_length(res.length() + header_size);
  }

  int64_t pos = res.length();

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_vi64(res.ptr(), res.capacity(), pos, value.length()))) {
    LOG_WARN("failed to encode str.", K(ret), K(pos));
  } else if (OB_FAIL(res.set_length(pos))) {
    LOG_WARN("failed to update len for res.", K(ret), K(pos));
  } else if (OB_FAIL(res.append(value.ptr(), value.length()))) {
    LOG_WARN("failed to append value.", K(ret), K(value));
  }

  return ret;
}

int ObBinAggSerializer::text_deserialize(ObString value, ObStringBuffer &res)
{
  INIT_SUCC(ret);

  int64_t pos = sizeof(uint8_t);
  int64_t val = 0;
  char *data = value.ptr();
  int64_t data_len = value.length();

  if (OB_FAIL(serialization::decode_vi64(data, data_len, pos, &val))) {
    LOG_WARN("failed to decode value", K(ret), K(value), K(pos));
  } else if (data_len < pos + val) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get val unexpected.", K(ret), K(val), K(value));
  } else if (OB_FAIL(res.append(data + pos, val))) {
    LOG_WARN("res append failed", K(ret), K(val));
  }

  return ret;
}

int ObBinAggSerializer::element_serialize(ObIAllocator* allocator, ObString value, ObStringBuffer &res)
{
  INIT_SUCC(ret);
  ObStringBuffer header_buff(allocator);
  ObStringBuffer ele_header_buff(allocator);

  ObString header_str;
  uint64_t count = 1;
  ObString prefix;
  int64_t total_size = 0;
  ObXmlElementBinHeader element_serializer(true, prefix);
  if (OB_FAIL(element_serializer.serialize(ele_header_buff))) {
    LOG_WARN("element serialize failed.", K(ret));
  } else {

    total_size = ObBinAggSerializer::estimate_total(value.length(), 1, AGG_XML, ele_header_buff.length());
    ObMulBinHeaderSerializer header_serializer(&header_buff, M_ELEMENT, total_size, count);
    if (OB_FAIL(header_serializer.serialize())) {
      LOG_WARN("header serialize failed.", K(ret));
    } else if (OB_FALSE_IT(header_str = header_serializer.buffer()->string())) {
    } else if (OB_FAIL(res.append(header_str.ptr(), header_str.length()))) {
      LOG_WARN("failed to append.", K(header_str));
    } else if (OB_FAIL(res.append(ele_header_buff.ptr(), ele_header_buff.length()))) {
      LOG_WARN("failed to append.", K(ele_header_buff));
    } else {
      int64_t index_start = res.length();
      int64_t key_entry_start = index_start + header_serializer.get_count_var_size();
      int64_t value_entry_start = key_entry_start + header_serializer.get_entry_var_size() * 2;
      int64_t key_start = value_entry_start + header_serializer.get_entry_var_size() + sizeof(uint8_t);
      uint32_t reserve_size = key_start - index_start;
      if (OB_FAIL(res.reserve(reserve_size))) {
        LOG_WARN("failed to reserve buffer.", K(ret), K(reserve_size));
      } else {
        res.set_length(index_start + reserve_size);
        char* write_buf = res.ptr() + index_start;
        ObMulModeVar::set_var(0, header_serializer.get_count_var_size_type(), write_buf); // index
        write_buf += header_serializer.get_count_var_size();
        ObMulModeVar::set_var(key_start, header_serializer.get_entry_var_size_type(), write_buf); // key_entry offset
        write_buf += header_serializer.get_entry_var_size();
        ObMulModeVar::set_var(0, header_serializer.get_entry_var_size_type(), write_buf); // key_entry length
        write_buf += header_serializer.get_entry_var_size();
        *reinterpret_cast<uint8_t*>(write_buf) = M_TEXT; // value_entry type
        ObMulModeVar::set_var(key_start, header_serializer.get_entry_var_size_type(), write_buf + sizeof(uint8_t)); // value_entry offset
        if (OB_FAIL(res.append(value))) {
          LOG_WARN("failed to append value.", K(ret), K(value));
        }
      }
    }
  }
  return ret;
}

int ObBinAggSerializer::deal_last_unparsed()
{
  INIT_SUCC(ret);
  ObAggBinKeyInfo *key_info = nullptr;
  int64_t key_count = key_info_.count();

  if (type_ != AGG_XML || key_count <= 0) {
    // do nothing
  } else {
    ObStringBuffer element_buff(allocator_);
    ObStringBuffer text_buff(allocator_);

    ObAggBinKeyInfo *last_key_info = key_info_.at(key_count -1);
    ObString value(value_.length() - last_key_info->value_offset_,
                    value_.ptr() + last_key_info->value_offset_);

    if (last_is_unparsed_text_) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBinAggSerializer::text_serialize(value, text_buff))) {
        LOG_WARN("failed to serialize text.", K(ret), K(value));
      } else if (OB_FAIL(ObBinAggSerializer::element_serialize(allocator_, text_buff.string(), element_buff))) {
        LOG_WARN("failed to build element serialize.", K(ret), K(value));
      } else if (OB_FAIL(value_.set_length(last_key_info->value_offset_))) {
        LOG_WARN("set length failed", K(ret));
      } else if (OB_FAIL(value_.append(element_buff.ptr(), element_buff.length()))) {
        LOG_WARN("failed to append key into key_", K(ret), K(element_buff));
      } else {
        last_is_unparsed_text_ = false;
      }
    } else if (last_is_text_node_) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBinAggSerializer::text_serialize(value, text_buff))) {
        LOG_WARN("failed to serialize text.", K(ret), K(value));
      } else if (OB_FAIL(value_.set_length(last_key_info->value_offset_))) {
        LOG_WARN("set length failed", K(ret));
      } else if (OB_FAIL(value_.append(text_buff.ptr(), text_buff.length()))) {
        LOG_WARN("failed to append key into key_", K(ret), K(text_buff));
      } else {
        last_is_text_node_ = false;
      }
    }
  }

  return ret;
}

void ObBinAggSerializer::construct_key_and_value()
{
  if (!is_json_array()) {
    for (int64_t i = 0; i < key_info_.count(); i++) {
      ObAggBinKeyInfo *key_info = key_info_.at(i);
      if ((has_unique_flag() && key_info->unparsed_)) {
        // do nothing
      } else {
        set_key(key_info->offset_, key_info->key_len_);
      }
    }
  }

  if (!has_unique_flag()) {
    buff_.append(value_.ptr(), value_.length(), 0);
  } else {
    for (int64_t i = 0; i < key_info_.count(); i++) {
      ObAggBinKeyInfo *key_info = key_info_.at(i);
      if (key_info->unparsed_) {
        // do nothing
      } else {
        set_value(key_info->value_offset_, key_info->value_len_);
      }
    }
  }
}

int ObBinAggSerializer::copy_and_reset(ObIAllocator* new_allocator,
                                       ObIAllocator* old_allocator,
                                       ObStringBuffer &add_value)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(new_allocator)) {
    // do nothing
  } else {
    ObStringBuffer new_key(new_allocator);
    ObStringBuffer new_value(new_allocator);
    ObAggBinKeyArray new_key_info;

    if (OB_FAIL(new_value.reserve(value_.length() + add_value.length()))) {
      LOG_WARN("failed to reserve new value", K(ret), K(value_.length()), K(add_value.length()));
    } else if (OB_FAIL(new_value.append(value_.ptr(), value_.length(), 0))) {
      LOG_WARN("failed to append value.", K(new_value.length()), K(value_.length()));
    } else if (OB_FAIL(new_value.append(add_value.ptr(), add_value.length(), 0))) {
      LOG_WARN("failed to append add value.", K(new_value.length()), K(add_value));
    } else if (OB_FAIL(new_key.append(key_.ptr(), key_.length(), 0))) {
      LOG_WARN("failed to reserve new key", K(ret), K(new_key.length()), K(key_.length()));
    } else {
      key_.reset();
      value_.reset();
      old_allocator->reset();
      if (OB_FAIL(key_.deep_copy(new_allocator, new_key))) {
        LOG_WARN("failed to copy new key into key", K(key_), K(new_key));
      } else if (OB_FAIL(value_.deep_copy(new_allocator, new_value))) {
        LOG_WARN("failed to copy new value into value", K(value_), K(new_value));
      }
    }

  }

  return ret;
}

int ObBinAggSerializer::rewrite_total_size()
{
  INIT_SUCC(ret);
  int64_t actual_total_size = buff_.length();
  int64_t calculate_total_size = header_.get_obj_size();
  if (calculate_total_size == actual_total_size) {
    // do nothing
  } else if (ObMulModeVar::get_var_type(calculate_total_size) <
              ObMulModeVar::get_var_type(actual_total_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header size invalided", K(ret));
  } else {
    if (header_.obj_var_size_ == 1) {
      *reinterpret_cast<uint8_t*>(buff_.ptr() + header_.begin_ + header_.obj_var_offset_) = static_cast<uint8_t>(actual_total_size);
    } else if (header_.obj_var_size_ == 2) {
      *reinterpret_cast<uint16_t*>(buff_.ptr() + header_.begin_ + header_.obj_var_offset_) = static_cast<uint16_t>(actual_total_size);
    } else if (header_.obj_var_size_ == 4) {
      *reinterpret_cast<uint32_t*>(buff_.ptr() + header_.begin_ + header_.obj_var_offset_) = static_cast<uint32_t>(actual_total_size);
    } else {
      *reinterpret_cast<uint64_t*>(buff_.ptr() + header_.begin_ + header_.obj_var_offset_) = actual_total_size;
    }
  }
  return ret;
}

int ObBinAggSerializer::serialize()
{
  INIT_SUCC(ret);

  if (OB_FAIL(deal_last_unparsed())) { // unparsed
    LOG_WARN("failed to deal with last unprased.", K(ret));
  } else if (is_json_type() && !json_not_sort() && OB_FALSE_IT(do_json_sort())) { // do json sort
  } else if (is_xml_type() && OB_FALSE_IT(do_xml_sort())) { // do xml sort
  } else if (OB_FAIL(construct_header())) { // calculate header
    LOG_WARN("failed to construct header.", K(ret));
  } else if (OB_FAIL(construct_meta())) { // construct meta_
    LOG_WARN("failed to construct meta.", K(ret));
  } else if (OB_FALSE_IT(construct_key_and_value())) { // merge key_ and value_
  } else if (OB_FAIL(rewrite_total_size())) { // write total
    LOG_WARN("failed to rewrite total size.", K(ret));
  }

  return ret;
}

void ObBinAggSerializer::do_json_sort()
{
  ObJsonBinAggKeyCompare cmp;
  cmp.buff_ = &key_;
  std::stable_sort(key_info_.begin(), key_info_.end(), cmp);
}

void ObBinAggSerializer::do_xml_sort()
{
  ObXmlBinAggKeyCompare cmp;
  cmp.buff_ = &key_;
  std::stable_sort(key_info_.begin(), key_info_.end(), cmp);
}


}; // namespace common

}; // namespace oceanbase