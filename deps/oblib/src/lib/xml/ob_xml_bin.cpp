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
 * This file contains implement for the xml bin abstraction.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_bin.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xml_util.h"

namespace oceanbase {
namespace common {

uint32_t ObXmlElementBinHeader::header_size()
{
  uint32_t len = sizeof(uint8_t);

  if (is_prefix_) {
    len += prefix_len_size_ + prefix_len_;
  }

  return len;
}

int ObXmlElementBinHeader::serialize(ObStringBuffer& buffer)
{
  INIT_SUCC(ret);
  uint32_t header_len = header_size();
  if (OB_FAIL(buffer.reserve(header_len))) {
    LOG_WARN("failed to reserve header", K(ret));
  } else {
    /**
     * | flag | prefix | standalone |
    */
    char* data = buffer.ptr();
    int64_t pos = buffer.length();

    *reinterpret_cast<uint8_t*>(data + pos) = flags_;
    pos += sizeof(uint8_t);
    buffer.set_length(pos);

    uint32_t left = header_len - sizeof(uint8_t);
    if (is_prefix_) {
      if (OB_FAIL(serialization::encode_vi64(data, pos + left, pos, prefix_len_))) {
        LOG_WARN("failed to serialize for str xml obj", K(ret), K(prefix_len_size_));
      } else if (OB_FAIL(buffer.set_length(pos))) {
        LOG_WARN("failed to update len for str obj", K(ret), K(pos));
      } else if (OB_FAIL(buffer.append(prefix_.ptr(), prefix_len_))) {
        LOG_WARN("failed to append string obj value", K(ret));
      } else {
        pos += prefix_len_;
        buffer.set_length(pos);
      }
    }
  }

  return ret;
}

int ObXmlElementBinHeader::deserialize(const char* data, int64_t length)
{
  INIT_SUCC(ret);

  if (length < sizeof(uint8_t) || OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to deserialize element header.", K(ret), K(length));
  } else {
    flags_ = *reinterpret_cast<const uint8_t*>(data);
    int64_t pos = sizeof(uint8_t);

    if (is_prefix_) {
      int64_t val = 0;

      if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
        LOG_WARN("failed to deserialize element header.", K(ret), K(length));
      } else if (length < pos + val) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to deserialize element header.", K(ret), K(length), K(pos), K(val));
      } else if (val == 0) {
        prefix_len_size_ = val;
        prefix_len_ = val;
      } else {
        prefix_.assign_ptr(data + pos, val);
        prefix_len_ = val;
        prefix_len_size_ = pos - sizeof(uint8_t);
        pos += prefix_len_;
      }
    }
  }

  return ret;
}

uint32_t ObXmlAttrBinHeader::header_size()
{
  return is_prefix_ ?
      sizeof(int8_t) + prefix_len_size_ + prefix_len_ + sizeof(int8_t)
      : sizeof(int8_t) + sizeof(int8_t);
}

int ObXmlAttrBinHeader::serialize(ObStringBuffer* buffer)
{
  INIT_SUCC(ret);
  uint32_t header_len = header_size();
  if (OB_FAIL(buffer->reserve(header_len))) {
    LOG_WARN("failed to reserve header", K(ret));
  } else {
    /**
     * | type_ | prefix_ |
    */
    char* data = buffer->ptr();
    int64_t pos = buffer->length();

    data[pos++] = type_;

    *reinterpret_cast<int8_t*>(data + pos) = flags_;
    pos += sizeof(int8_t);

    uint32_t left = header_len - sizeof(int8_t);
    if (is_prefix_) {
      if (OB_FAIL(serialization::encode_vi64(data, pos + left, pos, prefix_len_))) {
        LOG_WARN("failed to serialize for str xml obj", K(ret), K(prefix_len_size_));
      } else if (OB_FAIL(buffer->set_length(pos))) {
        LOG_WARN("failed to update len for str obj", K(ret), K(pos));
      } else if (OB_FAIL(buffer->append(prefix_.ptr(), prefix_len_))) {
        LOG_WARN("failed to append string obj value", K(ret));
      }
    } else {
      buffer->set_length(pos);
    }
  }

  return ret;
}

int ObXmlAttrBinHeader::deserialize(const char* data, int64_t length)
{
  INIT_SUCC(ret);

  if (length <= sizeof(int8_t) || OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to deserialize attibute header.", K(ret), K(length));
  } else {
    int64_t pos = 0;
    type_ = static_cast<ObMulModeNodeType>(data[pos++]);
    flags_ = static_cast<uint8_t>(data[pos++]);
    if (is_prefix_) {
      int64_t val = 0;

      if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
        LOG_WARN("failed to deserialize attibute header.", K(ret), K(length));
      } else if (length < pos + val) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to deserialize attibute header.", K(ret), K(length), K(pos), K(val));
      } else if (val == 0) {
        prefix_len_size_ = val;
      } else {
        prefix_.assign_ptr(data + pos, val);
        prefix_len_ = val;
        prefix_len_size_ = pos - (sizeof(uint8_t) * 2);
      }
    }
  }

  return ret;
}

uint64_t ObXmlDocBinHeader::header_size()
{
  uint32_t len = version_len_ + encode_len_  +
                 is_version_ + is_encoding_ +
                 is_standalone_ + sizeof(uint16_t) +
                 elem_header_.header_size();
  return len;
}

int ObXmlDocBinHeader::serialize(ObStringBuffer& buffer)
{
  INIT_SUCC(ret);

  uint64_t header_len = header_size();
  if (OB_FAIL(buffer.reserve(header_len))) {
    LOG_WARN("failed to reserve document header.", K(ret), K(header_len), K(buffer.length()));
  } else {
    /**
     * | flag | version | encoding | standalone | element_header |
    */

    char* data = buffer.ptr();
    uint64_t pos = buffer.length();

    *reinterpret_cast<uint16_t*>(data + pos) = flags_;
    pos += sizeof(uint16_t);
    buffer.set_length(pos);


    if (is_version_) {
      data[pos++] = version_len_;
      MEMCPY(data + pos, version_.ptr(), version_len_);
      pos += version_len_;
      buffer.set_length(pos);
    }

    if (is_encoding_) {
      data[pos++] = encode_len_;
      MEMCPY(data + pos, encoding_.ptr(), encode_len_);
      pos += encode_len_;
      buffer.set_length(pos);
    }

    if (is_standalone_) {
      data[pos++] = static_cast<uint8_t>(standalone_);
      buffer.set_length(pos);
    }

    if (OB_FAIL(elem_header_.serialize(buffer))) {
      LOG_WARN("failed to serialize element header.", K(ret), K(header_len), K(buffer.length()));
    }
  }

  return ret;
}

int ObXmlDocBinHeader::deserialize(const char* data, int64_t length)
{
  INIT_SUCC(ret);

  if (length < sizeof(uint16_t) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to deserialize element header.", K(ret), K(length));
  } else {
    flags_ = *reinterpret_cast<const uint16_t*>(data);
    int32_t pos = sizeof(uint16_t);


    if (is_version_) {
      if (length - pos < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to deserialize version header.", K(ret), K(length), K(pos));
      } else {
        version_len_ = data[pos++];
        if (length - pos < version_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize version header.", K(ret), K(length), K(pos), K(version_len_));
        } else {
          version_.assign_ptr(data + pos, version_len_);
          pos += version_len_;
        }
      }
    }

    if (OB_SUCC(ret) && is_encoding_) {
      if (length - pos < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to deserialize encoding header.", K(ret), K(length), K(pos));
      } else {
        encode_len_ = data[pos++];
        if (length - pos < encode_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize encoding header.", K(ret), K(length), K(pos), K(encode_len_));
        } else {
          encoding_.assign_ptr(data + pos, encode_len_);
          pos += encode_len_;
        }
      }
    }

    if (OB_SUCC(ret) && is_standalone_) {
      if (length - pos < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to deserialize standalone.", K(ret), K(length), K(pos));
      } else {
        standalone_ = data[pos++];
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(elem_header_.deserialize(data + pos, length - pos))) {
      LOG_WARN("failed to deserialize element header.", K(ret), K(length), K(pos), K(encode_len_));
    }
  }

  return ret;
}

ObXmlAttributeSerializer::ObXmlAttributeSerializer(ObIMulModeBase* root, ObStringBuffer& buffer)
  : root_(root),
    buffer_(&buffer),
    header_((static_cast<ObXmlAttribute*>(root))->get_prefix(), (static_cast<ObXmlAttribute*>(root))->type())
{
}

ObXmlAttributeSerializer::ObXmlAttributeSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx)
  : header_(),
    data_(data),
    data_len_(length),
    allocator_(ctx->allocator_),
    ctx_(ctx) {}

int ObXmlAttributeSerializer::serialize()
{
  INIT_SUCC(ret);

  ObXmlAttribute* attr = static_cast<ObXmlAttribute*>(root_);
  if (OB_FAIL(header_.serialize(buffer_))) {
    LOG_WARN("failed to serialize attribute header.", K(ret), K(buffer_->length()), K(header_.type_));
  } else {
    ObString value = attr->get_value();

    int64_t ser_len = serialization::encoded_length_vi64(value.length());
    int64_t pos = buffer_->length();

    if (OB_FAIL(buffer_->reserve(ser_len))) {
      LOG_WARN("failed to reserver serialize size for str obj", K(ret), K(ser_len));
    } else if (OB_FAIL(serialization::encode_vi64(buffer_->ptr(), buffer_->capacity(), pos, value.length()))) {
      LOG_WARN("failed to serialize for str obj", K(ret), K(ser_len));
    } else if (OB_FAIL(buffer_->set_length(pos))) {
      LOG_WARN("failed to update len for str obj", K(ret), K(pos));
    } else if (OB_FAIL(buffer_->append(value.ptr(), value.length()))) {
      LOG_WARN("failed to append string value", K(ret));
    }
  }

  return ret;
}

int ObXmlAttributeSerializer::deserialize(ObIMulModeBase*& handle)
{
  INIT_SUCC(ret);
  if (OB_FAIL(header_.deserialize(data_, data_len_))) {
    LOG_WARN("failed to serialize attribute header.", K(ret), K(data_len_));
  } else if (header_.header_size() > data_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to serialize attribute header.", K(ret), K(header_.header_size()), K(data_len_));
  } else {
    int64_t val = 0;
    int64_t pos = header_.header_size();

    ObString value;

    if (OB_FAIL(serialization::decode_vi64(data_, data_len_, pos, &val))) {
      LOG_WARN("failed to deserialize attribute value string.", K(ret), K(val));
    } else if (data_len_ < pos + val) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize attribute value string.", K(ret), K(data_len_), K(pos), K(val));
    } else {
      value.assign_ptr(data_ + pos, val);
    }

    ObXmlAttribute* attr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(attr = static_cast<ObXmlAttribute*>(allocator_->alloc(sizeof(ObXmlAttribute))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate attribute node.", K(ret));
    } else {
      attr = new(attr) ObXmlAttribute(header_.type_, ctx_);
      attr->set_prefix(header_.prefix_);
      attr->set_value(value);
    }

    if (OB_SUCC(ret)) {
      handle = attr;
    }
  }

  return ret;
}

ObXmlTextSerializer::ObXmlTextSerializer(ObIMulModeBase* root, ObStringBuffer& buffer)
  : root_(root),
    buffer_(&buffer)
{
  type_ = root->type();
}

ObXmlTextSerializer::ObXmlTextSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx)
  : data_(data),
    data_len_(length),
    allocator_(ctx->allocator_),
    ctx_(ctx)
{
}

int ObXmlTextSerializer::serialize()
{
  INIT_SUCC(ret);

  ObXmlText* text = static_cast<ObXmlText*>(root_);
  ObString value;
  text->get_value(value);

  int8_t header_len = header_size();

  int64_t ser_len = serialization::encoded_length_vi64(value.length());

  if (OB_FAIL(buffer_->reserve(ser_len + header_len + value.length()))) {
    LOG_WARN("failed to reserver serialize size for str obj", K(ret), K(ser_len));
  } else if (OB_FAIL(ObMulModeVar::set_var(type_, ObMulModeBinLenSize::MBL_UINT8, buffer_->ptr() + buffer_->length()))) {
    LOG_WARN("failed to set var", K(ret), K(type_));
  } else {
    buffer_->set_length(buffer_->length() + header_len);
  }

  int64_t pos = buffer_->length();

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_vi64(buffer_->ptr(), buffer_->capacity(), pos, value.length()))) {
    LOG_WARN("failed to serialize for str obj", K(ret), K(ser_len));
  } else if (OB_FAIL(buffer_->set_length(pos))) {
    LOG_WARN("failed to update len for str obj", K(ret), K(pos));
  } else if (OB_FAIL(buffer_->append(value.ptr(), value.length()))) {
    LOG_WARN("failed to append string value", K(ret));
  }

  return ret;
}

int ObXmlTextSerializer::deserialize(ObIMulModeBase*& handle)
{
  INIT_SUCC(ret);

  if (data_len_ <= 0 || OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to deserialize text", K(ret), K(data_len_));
  } else {
    type_ = static_cast<ObMulModeNodeType>(data_[0]);

    int64_t val = 0;
    int64_t pos = header_size();
    ObString value;

    if (OB_FAIL(serialization::decode_vi64(data_, data_len_, pos, &val))) {
      LOG_WARN("failed to deserialize text string.", K(ret), K(val));
    } else if (data_len_ < pos + val) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize text string.", K(ret), K(data_len_), K(pos), K(val));
    } else {
      value.assign_ptr(data_ + pos, val);
    }

    ObXmlText* text = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(text = static_cast<ObXmlText*>(allocator_->alloc(sizeof(ObXmlText))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate text node.", K(ret));
    } else {
      text = new(text) ObXmlText(type_, ctx_);
      text->set_value(value);
    }

    if (OB_SUCC(ret)) {
      handle = text;
    }
  }

  return ret;
}

ObXmlElementSerializer::ObXmlElementSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx)
  : ObMulModeContainerSerializer(data, length),
    attr_count_(0),
    child_count_(0),
    data_(data),
    data_len_(length),
    allocator_(ctx->allocator_),
    ctx_(ctx)
{
}

// root must be xml_element or xml_document
ObXmlElementSerializer::ObXmlElementSerializer(ObIMulModeBase* root, ObStringBuffer* buffer, bool serialize_key)
  : ObMulModeContainerSerializer(root, buffer),
    child_arr_(),
    serialize_key_(serialize_key),
    serialize_try_time_(0)
{
  attr_count_ = root->attribute_count();
  child_count_ = root->size();
  int64_t children_count = size();

  if (ObMulModeVar::get_var_type(children_count) != ObMulModeVar::get_var_type(child_count_)) {
    new (this) ObMulModeContainerSerializer(root, buffer, children_count);
  }

  // child_arr_[0] : attribute, namespace
  // child_arr_[1] : child such as pi, element, text, comment, cdata

  int is_has_attr = attr_count_ > 0 ? 1 : 0;
  if (is_has_attr) {
    // only element node have attribute, so root must be element node
    child_arr_[0].l_start_ = child_arr_[0].g_start_ = 0;
    child_arr_[0].l_last_ = child_arr_[0].g_last_ = attr_count_ - 1;
    child_arr_[0].entry_ = root->get_attribute_handle();
  }

  if (child_count_ > 0) {
    child_arr_[is_has_attr].l_start_ = 0;
    child_arr_[is_has_attr].l_last_ = child_count_ - 1;

    child_arr_[is_has_attr].g_start_ = attr_count_;
    child_arr_[is_has_attr].g_last_ = attr_count_ + child_count_ - 1;
    child_arr_[is_has_attr].entry_ = root;
  }

  int64_t header_len = header_.header_size();
  if (type_ == M_DOCUMENT || type_ == M_UNPARSED || type_ == M_CONTENT || type_ == M_UNPARESED_DOC) {
    new (&doc_header_) ObXmlDocBinHeader(root->get_version(),
                                         root->get_encoding(),
                                         root->get_encoding_flag(),
                                         root->get_standalone(),
                                         root->has_xml_decl());
    new(&doc_header_.elem_header_) ObXmlElementBinHeader(root->is_unparse(),
                                                         root->get_prefix());
    header_len += doc_header_.header_size();
  } else {
    new(&ele_header_) ObXmlElementBinHeader(root->is_unparse(),
                                            root->get_prefix());
    header_len += ele_header_.header_size();
  }

  index_start_ = header_.start() + header_len;
  index_entry_size_ = header_.count_var_size_;

  // offset start
  key_entry_start_ = index_start_ + children_count * index_entry_size_;
  key_entry_size_ = header_.get_entry_var_size();

  value_entry_start_ = children_count * (key_entry_size_ * 2) + key_entry_start_;
  value_entry_size_ = header_.get_entry_var_size();

  key_start_ = (value_entry_size_ + sizeof(uint8_t)) * size() + value_entry_start_;
}

void ObXmlElementSerializer::set_index_entry(int64_t origin_index, int64_t sort_index)
{
  int64_t offset = index_start_ + origin_index * header_.get_count_var_size();
  char* write_buf = header_.buffer()->ptr() + offset;
  ObMulModeVar::set_var(sort_index, header_.get_count_var_size_type(), write_buf);
}

void ObXmlElementSerializer::set_key_entry(int64_t entry_idx,  int64_t key_offset, int64_t key_len)
{
  int64_t offset = key_entry_start_ + entry_idx * (header_.get_entry_var_size() * 2);
  char* write_buf = header_.buffer()->ptr() + offset;
  ObMulModeVar::set_var(key_offset, header_.get_entry_var_size_type(), write_buf);

  write_buf += header_.get_entry_var_size();
  ObMulModeVar::set_var(key_len, header_.get_entry_var_size_type(), write_buf);
}

int ObXmlElementSerializer::reserve_meta()
{
  INIT_SUCC(ret);
  ObStringBuffer& buffer = *header_.buffer();

  int64_t pos = buffer.length();
  uint32_t reserve_size = key_start_ - index_start_;
  if (OB_FAIL(buffer.reserve(reserve_size))) {
    LOG_WARN("failed to reserve buffer.", K(ret), K(reserve_size), K(header_.start()));
  } else {
    buffer.set_length(pos + reserve_size);
  }
  return ret;
}

void ObXmlElementSerializer::set_value_entry(int64_t entry_idx,  uint8_t type, int64_t value_offset)
{
  int64_t offset = value_entry_start_ + entry_idx * (header_.get_entry_var_size() + sizeof(uint8_t));
  char* write_buf = header_.buffer()->ptr() + offset;
  *reinterpret_cast<uint8_t*>(write_buf) = type;
  ObMulModeVar::set_var(value_offset, header_.get_entry_var_size_type(), write_buf + sizeof(uint8_t));
}

int ObXmlElementSerializer::serialize_child_key(const ObString& key, int64_t idx)
{
  INIT_SUCC(ret);
  ObStringBuffer& buffer = *header_.buffer();
  int64_t key_offset = buffer.length() - header_.start();

  if (OB_FAIL(buffer.append(key.ptr(), key.length()))) {
    LOG_WARN("failed to append key string.", K(ret), K(buffer.length()), K(key.length()));
  } else {
    // idx fill later
    set_key_entry(idx, key_offset, key.length());
  }

  return ret;
}

int ObXmlElementSerializer::serialize_key(int arr_idx, int64_t depth)
{
  INIT_SUCC(ret);
  if (child_arr_[arr_idx].is_valid()) {
    ObXmlNode* xnode = static_cast<ObXmlNode*>(child_arr_[arr_idx].entry_);
    int64_t g_idx = child_arr_[arr_idx].g_start_;
    ObXmlNode::iterator iter = xnode->sorted_begin();
    ObXmlNode::iterator end = xnode->sorted_end();
    ObStringBuffer& buffer = *header_.buffer();
    for (; OB_SUCC(ret) && iter < end ; ++iter, g_idx++) {
      ObXmlNode* cur = static_cast<ObXmlNode*>(*iter);
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get cur null", K(ret));
      } else {
        ObMulModeNodeType cur_type = cur->type();
        switch (cur_type) {
          case M_UNPARSED:
          case M_UNPARESED_DOC:
          case M_DOCUMENT:
          case M_ELEMENT:
          case M_CONTENT:
          case M_ATTRIBUTE:
          case M_NAMESPACE:
          case M_INSTRUCT:
          case M_TEXT:
          case M_COMMENT:
          case M_CDATA: {
            if (OB_FAIL(serialize_child_key(cur->get_key(), g_idx))) {
              LOG_WARN("failed to serialize key string.", K(ret), K(cur->get_key().length()), K(buffer.length()));
            } else {
              set_index_entry( cur->get_index() + child_arr_[arr_idx].g_start_, g_idx);
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to serialize key, current node type not correct.", K(ret), K(cur_type));
            break;
          }
        }
      }
    }
  }

  return ret;
}

int ObXmlElementSerializer::serialize_value(int arr_idx, int64_t depth)
{
  INIT_SUCC(ret);
  if (child_arr_[arr_idx].is_valid()) {
    ObXmlNode* xnode = static_cast<ObXmlNode*>(child_arr_[arr_idx].entry_);
    int64_t g_idx = child_arr_[arr_idx].g_start_;

    ObXmlNode::iterator iter = xnode->sorted_begin();
    ObXmlNode::iterator end = xnode->sorted_end();
    ObStringBuffer& buffer = *header_.buffer();

    for (; OB_SUCC(ret) && iter < end ; ++iter, g_idx++) {
      ObXmlNode* cur = static_cast<ObXmlNode*>(*iter);
      ObMulModeNodeType cur_type = cur->type();

      int64_t value_start = buffer.length() - header_.start();

      switch (cur_type) {
        case M_UNPARSED:
        case M_UNPARESED_DOC:
        case M_DOCUMENT:
        case M_ELEMENT:
        case M_CONTENT: {
          ObXmlElementSerializer ele_serializer(cur, header_.buffer());
          if (OB_FAIL(ele_serializer.serialize(depth + 1))) {
            LOG_WARN("failed to serialize element child", K(ret), K(buffer.length()));
          } else {
            set_value_entry(g_idx, cur_type, value_start);
          }
          break;
        }
        case M_ATTRIBUTE:
        case M_NAMESPACE:
        case M_INSTRUCT: {
          ObXmlAttributeSerializer attr_serializer(cur, buffer);
          if (OB_FAIL(attr_serializer.serialize())) {
            LOG_WARN("failed to serialize attribute.", K(ret), K(cur_type), K(buffer.length()));
          } else {
            set_value_entry(g_idx, cur_type, value_start);
          }
          break;
        }
        case M_TEXT:
        case M_COMMENT:
        case M_CDATA: {
          ObXmlTextSerializer serializer(cur, buffer);
          if (OB_FAIL(serializer.serialize())) {
            LOG_WARN("failed to serialize text.", K(ret), K(cur_type), K(buffer.length()));
          } else {
            set_value_entry(g_idx, cur_type, value_start);
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to serialize key, current node type not correct.", K(ret), K(cur_type));
          break;
        }
      }
    }
  }

  return ret;
}

int ObXmlElementSerializer::deserialize(ObIMulModeBase*& node)
{
  INIT_SUCC(ret);
  int64_t pos = 0;
  int64_t left_data_len = data_len_;
  const char* data = data_;

  ObXmlElement *handle = nullptr;
  if (OB_FAIL(header_.deserialize())) {
    LOG_WARN("failed to deserialize header.", K(ret));
  } else if (OB_ISNULL(handle = static_cast<ObXmlElement*>(allocator_->alloc(
             (header_.type() == ObMulModeNodeType::M_DOCUMENT
              || header_.type() == ObMulModeNodeType::M_UNPARESED_DOC
              || header_.type() == ObMulModeNodeType::M_UNPARSED
              || header_.type() == ObMulModeNodeType::M_CONTENT) ?
               sizeof(ObXmlDocument) : sizeof(ObXmlElement))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for element node", K(ret));
  } else {
    type_ = header_.type();
    pos += header_.header_size();
    left_data_len = data_len_ - pos;
    if (type_ == ObMulModeNodeType::M_DOCUMENT
        || type_ == ObMulModeNodeType::M_UNPARSED
        || type_ == ObMulModeNodeType::M_CONTENT
        || type_ == ObMulModeNodeType::M_UNPARESED_DOC) {
      new (&doc_header_)ObXmlDocBinHeader();
      if (OB_FAIL(doc_header_.deserialize(data + pos, left_data_len))) {
        LOG_WARN("failed to deserialize header.", K(ret), K(left_data_len));
      } else {
        ObXmlDocument* doc = new(handle) ObXmlDocument(type_, ctx_);
        doc->set_version(doc_header_.version_);
        doc->set_encoding(doc_header_.encoding_);
        doc->set_prefix(doc_header_.elem_header_.prefix_);
        doc->set_unparse(doc_header_.elem_header_.is_unparse_);
        doc->set_has_xml_decl(doc_header_.is_xml_decl_);
        doc->set_encoding_flag(doc_header_.is_encoding_empty_);
        doc->set_standalone(doc_header_.standalone_);
        pos += doc_header_.header_size();
        left_data_len = data_len_ - pos;
        handle = doc;
      }
    } else if (type_ == ObMulModeNodeType::M_ELEMENT) {
      new (&ele_header_)ObXmlElementBinHeader();
      if (OB_FAIL(ele_header_.deserialize(data + pos, left_data_len))) {
        LOG_WARN("failed to deserialize header.", K(ret), K(left_data_len));
      } else {
        handle = new(handle) ObXmlElement(type_, ctx_);
        handle->set_prefix(ele_header_.prefix_);
        handle->set_unparse(ele_header_.is_unparse_);
        pos += ele_header_.header_size();
        left_data_len = data_len_ - pos;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize header, unexpected type.", K(ret), K(type_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(handle->alter_member_sort_policy(false))) {
    LOG_WARN("failed to alter sort policy.", K(ret));
  } else {
    int64_t count = header_.count();

    index_start_ = pos;
    index_entry_size_ = header_.get_count_var_size();

    key_entry_start_ = index_start_ + index_entry_size_ * count;

    key_entry_size_ = value_entry_size_ = header_.get_entry_var_size();

    value_entry_start_ = key_entry_start_ + (key_entry_size_ * 2) * count;

    key_start_ = value_entry_start_ + (sizeof(uint8_t) + value_entry_size_) * count;

    if (key_start_ > data_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize.", K(ret), K(key_start_), K(data_len_), K(pos), K(key_entry_size_));
    } else if (count && value_entry_start_ + (count - 1) * (value_entry_size_ + sizeof(uint8_t)) > data_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize.", K(ret), K(value_entry_start_), K(data_len_), K(pos), K(key_entry_size_));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
        int64_t key_offset = 0;
        int64_t key_len = 0;

        int64_t type = 0;
        int64_t value_offset = 0;

        int64_t sort_index = 0;

        const char* cur_val_entry_ptr = data + value_entry_start_;
        const char* cur_key_entry_ptr = data + key_entry_start_;

        if (OB_FAIL(ObMulModeVar::read_size_var(data_ + index_start_ + index_entry_size_ * idx, index_entry_size_, &sort_index))) {
          LOG_WARN("failed to read sort index.", K(ret), K(idx), K(index_entry_size_));
        } else if (sort_index >= count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize, sort index too large.", K(ret), K(sort_index), K(count));
        } else if (OB_FAIL(ObMulModeVar::read_size_var(cur_val_entry_ptr + (sizeof(uint8_t) + value_entry_size_) * sort_index, sizeof(uint8_t), &type))
          || OB_FAIL(ObMulModeVar::read_size_var(cur_val_entry_ptr + (sizeof(uint8_t) + value_entry_size_) * sort_index + sizeof(uint8_t), value_entry_size_, &value_offset))) {
          LOG_WARN("failed to read size var.", K(ret), K(sort_index), K(value_entry_start_), K(pos), K(value_entry_size_));
        } else if (!is_valid_xml_type(type) || value_offset > data_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize.", K(ret), K(sort_index), K(count));
        } else if (OB_FAIL(ObMulModeVar::read_size_var(cur_key_entry_ptr + (key_entry_size_ * 2) * sort_index, key_entry_size_, &key_offset))
          || OB_FAIL(ObMulModeVar::read_size_var(cur_key_entry_ptr + (key_entry_size_ * 2) * sort_index + key_entry_size_, key_entry_size_, &key_len))) {
          LOG_WARN("failed to deserialize.", K(ret), K(key_start_), K(data_len_), K(pos), K(key_entry_size_));
        } else if (key_offset + key_len > data_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize.", K(ret), K(key_offset), K(key_len), K(data_len_));
        } else {
          ObString key(key_len, data + key_offset);
          const char* value = value_offset + data;
          switch (type) {
            case M_ELEMENT: {
              ObIMulModeBase* child = nullptr;
              ObXmlElementSerializer serializer(value, data_len_ - value_offset, ctx_);

              if (OB_FAIL(serializer.deserialize(child))) {
                LOG_WARN("fail to deserialize element", K(ret), K(data_len_), K(value_offset));
              } else if (OB_FAIL(handle->add_element(static_cast<ObXmlElement*>(child)))) {
                LOG_WARN("fail to append element", K(ret));
              } else {
                static_cast<ObXmlElement*>(child)->set_xml_key(key);
                child_count_++;
              }
              break;
            }

            case M_ATTRIBUTE:
            case M_NAMESPACE:
            case M_INSTRUCT: {
              ObIMulModeBase* child = nullptr;
              ObXmlAttributeSerializer serializer(value, data_len_ - value_offset, ctx_);

              if (OB_FAIL(serializer.deserialize(child))) {
                LOG_WARN("fail to deserialize element", K(ret), K(data_len_), K(value_offset));
              } else if (type != M_INSTRUCT && OB_FAIL(handle->add_attribute(static_cast<ObXmlAttribute*>(child)))) {
                LOG_WARN("fail to append element", K(ret));
              } else if (type == M_INSTRUCT && OB_FAIL(handle->add_element(static_cast<ObXmlAttribute*>(child)))) {
                LOG_WARN("fail to append element", K(ret));
              } else {
                static_cast<ObXmlAttribute*>(child)->set_xml_key(key);
                attr_count_++;
              }
              break;
            }

            case M_TEXT:
            case M_COMMENT:
            case M_CDATA: {
              ObIMulModeBase* child = nullptr;
              ObXmlTextSerializer serializer(value, data_len_ - value_offset, ctx_);

              if (OB_FAIL(serializer.deserialize(child))) {
                LOG_WARN("fail to deserialize element", K(ret), K(data_len_), K(value_offset));
              } else if (OB_FAIL(handle->add_element(static_cast<ObXmlText*>(child)))) {
                LOG_WARN("fail to append element", K(ret));
              } else {
                static_cast<ObXmlText*>(child)->set_xml_key(key);
                attr_count_++;
              }
              break;
            }

            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to deserialize node", K(ret), K(type));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    node = handle;
  }


  return ret;
}

int ObXmlElementSerializer::serialize(int64_t depth)
{
  INIT_SUCC(ret);
  ObStringBuffer& buffer = *header_.buffer();
  int64_t start = buffer.length();

  ObXmlElement* ele = static_cast<ObXmlElement*>(root_);

  if (depth > 0) {
  } else if (OB_FAIL(ele->set_flag_by_descandant())) {
    LOG_WARN("failed to eval sepecail flag on header", K(ret));
  } else if (ele->type() != M_UNPARESED_DOC && ele->is_unparse()) {
    header_.type_ = M_UNPARSED;
    type_ = M_UNPARSED;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(header_.serialize())) {
    LOG_WARN("failed to serialize header.", K(ret), K(buffer.length()));
  } else if (type_ == M_DOCUMENT || type_ == M_UNPARSED || type_ == M_UNPARESED_DOC || type_ == M_CONTENT) {
    if (OB_FAIL(doc_header_.serialize(buffer))) {
      LOG_WARN("failed to document header key string.", K(ret), K(doc_header_.header_size()), K(buffer.length()));
    }
  } else if (type_ == M_ELEMENT) {
    if(OB_FAIL(ele_header_.serialize(buffer))) {
      LOG_WARN("failed to serialize element header.", K(ret), K(ele_header_.header_size()), K(buffer.length()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to serialize header, not leggal type.", K(ret), K(type_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reserve_meta())) {
    LOG_WARN("failed to reserve meta.", K(ret), K(buffer.length()));
  } else if (OB_FAIL(serialize_key(0, depth))) {
    LOG_WARN("failed to serialize key array 0.", K(ret), K(buffer.length()));
  } else if (OB_FAIL(serialize_key(1, depth))) {
    LOG_WARN("failed to serialize key array 1.", K(ret), K(buffer.length()));
  } else if (OB_FAIL(serialize_value(0, depth))) {
    LOG_WARN("failed to serialize value array 0.", K(ret), K(buffer.length()));
  } else if (OB_FAIL(serialize_value(1, depth))) {
    LOG_WARN("failed to serialize value array 1.", K(ret), K(buffer.length()));
  } else {
    int64_t end = buffer.length();
    int64_t total_size = end - start;
    int64_t children_num = size();

    if (ObMulModeVar::get_var_type(total_size) > header_.get_obj_var_size_type()
        || ObMulModeVar::get_var_type(children_num) > header_.get_count_var_size_type()) {
      if (serialize_try_time_ >= MAX_RETRY_TIME) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to serialize as meta info not match.", K(ret), K(total_size), K(children_num), K(header_));
      } else {
        int64_t delta = total_size - header_.get_obj_size();
        ele->set_delta_serialize_size(delta);
        serialize_try_time_++;
        buffer.set_length(start);
        new (this) ObXmlElementSerializer(root_, &buffer);
        if (OB_FAIL(serialize(depth))) {
          LOG_WARN("failed to serialize.", K(ret), K(buffer.length()));
        }
      }
    } else {
      header_.set_obj_size(total_size);
      header_.set_count(children_num);
    }
  }

  return ret;
}

ObXmlBinIndexMeta::ObXmlBinIndexMeta(const char* index_entry, int64_t idx, int64_t var_size)
{
  ObMulModeVar::read_size_var(index_entry + idx * var_size, var_size, &pos_);
}

ObXmlBinIndexMeta::ObXmlBinIndexMeta(const char* index_entry, int64_t idx, uint8_t var_type)
{
  ObMulModeVar::read_var(index_entry + ObMulModeVar::get_var_size(var_type), var_type, &pos_);
}

int64_t ObXmlBinIndexMeta::get_index()
{
  return pos_;
}

void ObXmlBinKeyMeta::read(const char* cur_entry, int64_t var_size)
{
  ObMulModeVar::read_size_var(cur_entry, var_size, &offset_);
  ObMulModeVar::read_size_var(cur_entry + var_size, var_size, &len_);
}

void ObXmlBinKeyMeta::read(const char* cur_entry, uint8_t var_type)
{
  ObMulModeVar::read_var(cur_entry, var_type, &offset_);
  ObMulModeVar::read_var(cur_entry + ObMulModeVar::get_var_size(var_type), var_type, &len_);
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(const ObXmlBinKeyMeta& other)
  : offset_(other.offset_), len_(other.len_)
{
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(const char* cur_entry, uint8_t var_type)
{
  read(cur_entry, var_type);
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(const char* cur_entry, int64_t var_size)
{
  read(cur_entry, var_size);
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(const char* key_entry, int64_t idx, uint8_t var_type)
{
  const char* cur_entry = key_entry + ObMulModeVar::get_var_size(var_type) * 2 * idx;
  read(cur_entry, var_type);
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(const char* key_entry, int64_t idx, int64_t var_size)
{
  const char* cur_entry = key_entry + var_size * 2 * idx;
  read(cur_entry, var_size);
}

ObXmlBinKeyMeta::ObXmlBinKeyMeta(int64_t offset, int32_t len)
  : offset_(offset),
    len_(len)
{
}

ObXmlBinKey::ObXmlBinKey(const char* data, int64_t cur_entry, uint8_t var_type)
  : meta_(data + cur_entry, var_type)
{
  key_.assign_ptr(data + meta_.offset_, meta_.len_);
}

ObXmlBinKey::ObXmlBinKey(const char* data, int64_t cur_entry, int64_t var_size)
: meta_(data + cur_entry, var_size)
{
  key_.assign_ptr(data + meta_.offset_, meta_.len_);
}

ObXmlBinKey::ObXmlBinKey(const char* data, int64_t key_entry, int64_t idx, uint8_t var_type)
: meta_(data + key_entry, idx, var_type)
{
  key_.assign_ptr(data + meta_.offset_, meta_.len_);
}

ObXmlBinKey::ObXmlBinKey(const char* data, int64_t key_entry, int64_t idx, int64_t var_size)
: meta_(data + key_entry, idx, var_size)
{
  key_.assign_ptr(data + meta_.offset_, meta_.len_);
}

ObXmlBinKey::ObXmlBinKey(const char* data, int64_t offset, int32_t len)
: meta_(offset, len)
{
  key_.assign_ptr(data + offset, len);
}

ObXmlBinKey::ObXmlBinKey(const ObXmlBinKey& other)
  : meta_(other.meta_),
    key_(other.key_)
{
}

ObXmlBinKey::ObXmlBinKey(const ObString& key)
  : key_(key)
{
}

ObString ObXmlBin::get_version()
{
  return meta_.get_version();
}

ObString ObXmlBin::get_encoding()
{
  return meta_.get_encoding();
}

ObString ObXmlBin::get_prefix()
{
  return meta_.get_prefix();
}

uint16_t ObXmlBin::get_standalone()
{
  return meta_.standalone_;
}

bool ObXmlBin::get_is_empty()
{
  return meta_.is_empty_;
}

bool ObXmlBin::get_unparse()
{
  return meta_.is_unparse_;
}

bool ObXmlBin::check_extend()
{
  bool ret_bool = false;
  if (meta_.type_ != M_ELEMENT && meta_.type_ != M_CONTENT && meta_.type_ != M_DOCUMENT) {
  } else if (buffer_for_extend_) {
    ret_bool = true;
  } else if (meta_.len_ > meta_.total_ && meta_.total_ > 0) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObXmlBin::check_if_defined_ns()
{
  bool ret_bool = false;
  if (type() != M_ELEMENT) {
  } else {
    int64_t attribute_num = attribute_size();
    INIT_SUCC(ret);
    for (int pos = 0; !ret_bool && OB_SUCC(ret) && pos < attribute_num ; ++pos) {
      ObXmlBin buff(*this);
      ObXmlBin* tmp = &buff;

      if (OB_FAIL(construct(tmp, allocator_))) {
      } else if (OB_FAIL(tmp->set_at(pos))) {
      } else if (tmp->type() == M_NAMESPACE) {
        ret_bool = true;
      } else if (tmp->type() == M_ATTRIBUTE) {
      } else {
        break;
      }
    }
  }
  return ret_bool;
}

int ObXmlBin::parse_tree(ObIMulModeBase* root, bool set_alter_member)
{
  INIT_SUCC(ret);
  ObXmlNode *xml_node = NULL;

  if (OB_ISNULL(root) || root->is_binary()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to parse tree null pointer.", K(ret));
  } else if (OB_ISNULL(xml_node = static_cast<ObXmlNode*>(root))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast to xmlnode get null", K(ret));
  } else if (OB_FAIL(set_alter_member && xml_node->alter_member_sort_policy(true))) {
    LOG_WARN("fail to sort child element", K(ret));
  } else {
    buffer_.reset();
    if (ObXmlUtil::use_text_serializer(root->type())) {
      ObXmlTextSerializer serializer(root, buffer_);
      if (OB_FAIL(serializer.serialize())) {
        LOG_WARN("failed to serialize.", K(ret), K(root->type()), K(root->get_serialize_size()));
      }
    } else if (ObXmlUtil::use_element_serializer(root->type())) {
      ObXmlElementSerializer serializer(root, &buffer_);
      if (OB_FAIL(serializer.serialize(0))) {
        LOG_WARN("failed to serialize.", K(ret), K(root->type()), K(root->get_serialize_size()));
      }
    } else if (ObXmlUtil::use_attribute_serializer(root->type())) {
      ObXmlAttributeSerializer serializer(root, buffer_);
      if (OB_FAIL(serializer.serialize())) {
        LOG_WARN("failed to serialize.", K(ret), K(root->type()), K(root->get_serialize_size()));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error type to serialize.",K(ret), K(root->type()));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_.parser(buffer_.ptr(), buffer_.length()))) {
      LOG_WARN("failed to parse meta.", K(ret));
    }
  }

  return ret;
}

int ObXmlBin::append_extend(char* start, int64_t len)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(start)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("failed to parse tree null pointer.", K(ret));
  } else if (meta_.parsed_ || OB_SUCC(parse())) {
    if (buffer_.length() == 0) {
      buffer_for_extend_ = true;
    } else if (buffer_.length() == meta_.len_ && buffer_.ptr() == meta_.data_) {
      buffer_for_extend_ = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer should be data_ or null", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to parse", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_.append(start, len))) {
      LOG_WARN("fail to append extend", K(ret));
    } else if (!buffer_for_extend_) {
      meta_.len_ += len;
    }
  }
  return ret;
}

int ObXmlBin::append_extend(ObXmlElement* ele)
{
  INIT_SUCC(ret);

  int buffer_length = 0;
  if (OB_ISNULL(ele)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("failed to parse tree null pointer.", K(ret));
  } else if (OB_FAIL(ele->alter_member_sort_policy(true))) {
    LOG_WARN("fail to sort child element", K(ret));
  } else if (meta_.parsed_ || OB_SUCC(parse())) {
    if (buffer_.length() == 0) {
      buffer_for_extend_ = true;
    } else if (buffer_.length() == meta_.len_ && buffer_.ptr() == meta_.data_) {
      buffer_length = buffer_.length();
      buffer_for_extend_ = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer should be data_ or null", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to parse", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObXmlElementSerializer serializer_element(ele, &buffer_);
    if (OB_FAIL(serializer_element.serialize(0))) {
      LOG_WARN("failed to serialize.", K(ret), K(ele->type()), K(ele->get_serialize_size()));
    } else if (!buffer_for_extend_) {
      meta_.len_ += (buffer_.length() - buffer_length);
    }
  }

  return ret;
}

int ObXmlBin::remove_extend()
{
  INIT_SUCC(ret);
  if (buffer_for_extend_) {
    buffer_for_extend_ = false;
    buffer_.reset();
  } else {
    meta_.len_ = meta_.total_ > 0 ? meta_.total_ : meta_.len_;
  }
  return ret;
}

int ObXmlBin::get_extend(char*& start, int64_t& len)
{
  INIT_SUCC(ret);
  if (buffer_for_extend_) {
    start = buffer_.ptr();
    len = buffer_.length();
  } else if (meta_.total_ > 0 && meta_.len_ > meta_.total_) {
    start = meta_.get_data() + meta_.total_;
    len = meta_.len_ - meta_.total_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no extend", K(ret));
  }
  return ret;
}

int ObXmlBin::get_extend(ObXmlBin& extend)
{
  INIT_SUCC(ret);
  extend.buffer_.reset();
  int64_t len = 0;
  char* start = buffer_.ptr();
  if (!check_extend()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no extend", K(ret));
  } else if ((meta_.total_ > 0 && meta_.len_ > meta_.total_) || buffer_for_extend_) {
    len = buffer_for_extend_? buffer_.length() : meta_.len_ - meta_.total_;
    start = buffer_for_extend_ ? buffer_.ptr() : const_cast<char *> (meta_.data_) + meta_.total_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no extend", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(extend.buffer_.reset())) {
  } else if (OB_FAIL(extend.parse(start, len))) {
    LOG_WARN("failed to parse meta.", K(ret));
  }
  return ret;
}

int ObXmlBin::merge_extend(ObXmlBin& res)
{
  INIT_SUCC(ret);
  ObXmlBin extend;
  ObXmlBinMerge bin_merge;
  if (OB_FAIL(get_extend(extend))) {
    LOG_WARN("failed to get extend.", K(ret));
  } else if (OB_FAIL(bin_merge.merge(*this, extend, res))) {
    LOG_WARN("failed to merge.", K(ret));
  } else if (OB_FAIL(res.meta_.parser(res.buffer_.ptr(), res.buffer_.length()))) {
    LOG_WARN("failed to parse.", K(ret));
  } else {
    res.meta_.key_len_ = meta_.key_len_;
    res.meta_.key_ptr_ = meta_.key_ptr_;
  }
  return ret;
}

int ObXmlBin::to_tree(ObIMulModeBase*& root)
{
  INIT_SUCC(ret);

  ObMulModeNodeType node_type;

  if (check_extend()) {
    ObXmlBin merge(ctx_);
    if (OB_FAIL(merge_extend(merge))) {
      LOG_WARN("failed to merge extend.", K(ret));
    } else if (OB_FAIL(merge.to_tree(root))) {
      LOG_WARN("failed to tree.", K(ret));
    }
  } else if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse meta.", K(ret));
  } else if (FALSE_IT(node_type = type())) {
  } else if (ObXmlUtil::use_element_serializer(node_type)) {
    ObXmlElementSerializer deserializer(meta_.data_, meta_.len_, ctx_);
    if (OB_FAIL(deserializer.deserialize(root))) {
      LOG_WARN("failed to deserialize.", K(ret), K(meta_));
    } else if (node_type == M_ELEMENT) {
      (static_cast<ObXmlElement*>(root))->set_xml_key(meta_.get_key());
    }

    if (OB_SUCC(ret)) {
      ObXmlNode* xnode = static_cast<ObXmlNode*>(root);
      if (OB_FAIL(xnode->alter_member_sort_policy(true))) {
        LOG_WARN("failed to sort member.", K(ret), K(meta_));
      }
    }
  } else if (ObXmlUtil::use_text_serializer(node_type)) {
    ObXmlTextSerializer serializer(meta_.data_, meta_.len_, ctx_);

    if (OB_FAIL(serializer.deserialize(root))) {
      LOG_WARN("fail to deserialize text", K(ret), K(meta_.data_), K(meta_.len_), K(node_type));
    }
  } else if (ObXmlUtil::use_attribute_serializer(node_type)) {
    ObXmlAttributeSerializer serializer(meta_.data_, meta_.len_, ctx_);
    if (OB_FAIL(serializer.deserialize(root))) {
      LOG_WARN("fail to deserialize attrubyte", K(ret), K(meta_.data_), K(meta_.len_), K(node_type));
    } else {
      (static_cast<ObXmlAttribute*>(root))->set_xml_key(meta_.get_key());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to parse meta.", K(ret));
  }


  return ret;
}

int ObXmlBinMetaParser::parser(const char* data, int64_t len)
{
  INIT_SUCC(ret);

  if (data_ == data && len_ == len) {
    // do nothing
  } else {
    new (this) ObXmlBinMetaParser();
    data_ = data;
    len_ = len;
  }
  return parser();
}

inline ObString ObXmlBinMetaParser::get_key()
{
  return ObString(key_len_, key_ptr_);
}

inline ObString ObXmlBinMetaParser::get_value()
{
  return ObString(value_len_, value_ptr_);
}

inline ObString ObXmlBinMetaParser::get_version()
{
  return ObString(version_len_, version_ptr_);
}

inline ObString ObXmlBinMetaParser::get_encoding()
{
  return ObString(encoding_len_, encoding_ptr_);
}

inline uint16_t ObXmlBinMetaParser::get_standalone()
{
  return standalone_;
}

inline ObString ObXmlBinMetaParser::get_prefix()
{
  return ObString(prefix_len_, prefix_ptr_);
}

inline uint8_t ObXmlBinMetaParser::get_key_entry_size()
{
  return key_entry_size_;
}

inline uint8_t ObXmlBinMetaParser::get_key_entry_size_type()
{
  return key_entry_size_type_;
}

inline uint8_t ObXmlBinMetaParser::get_value_entry_size()
{
  return value_entry_size_;
}

inline uint8_t ObXmlBinMetaParser::get_value_entry_size_type()
{
  return value_entry_size_type_;
}

inline int64_t ObXmlBinMetaParser::get_value_offset(int64_t index)
{
  return  value_entry_ + index * (value_entry_size_ + sizeof(uint8_t));
}

inline int64_t ObXmlBinMetaParser::get_key_offset(int64_t index)
{
  return  key_entry_ + index * (key_entry_size_ * 2);
}

inline int64_t ObXmlBinMetaParser::get_index(int64_t index)
{
  return index_entry_ + index * index_entry_size_;
}

int ObXmlBinMetaParser::parser()
{
  INIT_SUCC(ret);

  if (len_ < 1 || OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to parser header.", K(ret), K(len_));
  } else if (parsed_) {
  } else {
    type_ = static_cast<ObMulModeNodeType>(data_[0]);
    switch (type_) {
      case M_UNPARSED:
      case M_UNPARESED_DOC:
      case M_DOCUMENT:
      case M_ELEMENT:
      case M_CONTENT: {
        ObMulBinHeaderSerializer header(data_, len_);
        ObXmlDocBinHeader doc_header;
        ObXmlElementBinHeader ele_header;
        ObXmlElementBinHeader * ele_ptr = &ele_header;

        if (OB_FAIL(header.deserialize())) {
          LOG_WARN("failed to parser header.", K(ret), K(len_));
        } else {
          type_ = header.type();
          total_ = header.get_obj_size();
          count_ = header.count();

          int64_t pos = header.header_size();

          if (type_ == M_DOCUMENT || type_ == M_UNPARSED || type_ == M_CONTENT || type_ == M_UNPARESED_DOC) {
            if (OB_FAIL(doc_header.deserialize(data_ + pos, len_ - pos))) {
              LOG_WARN("failed to doc header.", K(ret), K(len_), K(pos));
            } else {
              ObString version =  doc_header.get_version();
              version_ptr_ = version.ptr();
              version_len_ = version.length();

              ObString encoding = doc_header.get_encoding();
              encoding_ptr_ = encoding.ptr();
              encoding_len_ = encoding.length();

              standalone_ = doc_header.get_standalone();
              is_empty_ = count_ == 0;
              has_xml_decl_ = doc_header.has_xml_decl();
              encoding_val_empty_ = doc_header.get_encoding_empty();

              pos += doc_header.header_size();
              ele_ptr = &doc_header.elem_header_;
            }
          } else if (type_ == M_ELEMENT) {

            if (OB_FAIL(ele_header.deserialize(data_ + pos, len_ - pos))) {
              LOG_WARN("failed to doc header.", K(ret), K(len_), K(pos));
            } else {
              pos += ele_header.header_size();
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("failed to parser header.", K(ret), K(type_));
          }

          if (OB_SUCC(ret)) {
            ObString prefix = ele_ptr->get_prefix();
            prefix_len_ = prefix.length();
            prefix_ptr_ = prefix.ptr();

            is_unparse_ = ele_ptr->get_unparse();

            index_entry_ = pos;
            key_entry_ = index_entry_ + count_ * header.get_count_var_size();

            key_entry_size_ = header.get_entry_var_size();
            key_entry_size_type_ = header.get_entry_var_size_type();

            index_entry_size_ = header.get_count_var_size();
            index_entry_size_type_ = header.get_count_var_size_type();

            value_entry_ = key_entry_ + key_entry_size_ * 2 * count_;
            value_entry_size_ = header.get_entry_var_size();
            value_entry_size_type_ = header.get_entry_var_size_type();
            parsed_ = true;
          }
        }

        break;
      }
      case M_ATTRIBUTE:
      case M_NAMESPACE:
      case M_INSTRUCT: {
        ObXmlAttrBinHeader attr_header;
        if (OB_FAIL(attr_header.deserialize(data_, len_))) {
          LOG_WARN("failed to parser header.", K(ret), K(len_), K(type_));
        } else {
          ObString prefix = attr_header.get_prefix();
          prefix_ptr_ = prefix.ptr();
          prefix_len_ = prefix.length();

          value_entry_ = attr_header.header_size();
          count_ = 1;

          int64_t pos = value_entry_;
          int64_t val = 0;
          if (OB_FAIL(serialization::decode_vi64(data_, len_, pos, &val))) {
            LOG_WARN("failed to deserialize text string.", K(ret), K(val), K(len_), K(pos));
          } else if (len_ < pos + val) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to deserialize text string.", K(ret), K(len_), K(pos), K(val));
          } else {
            value_ptr_ = const_cast<char*>(data_ + pos);
            value_len_ = val;
            total_ = value_len_ + pos;
            parsed_ = true;
          }
        }
        break;
      }

      case M_TEXT:
      case M_COMMENT:
      case M_CDATA: {
        value_entry_ = 1;
        count_ = 1;
        int64_t pos = value_entry_;
        int64_t val = 0;
        if (OB_FAIL(serialization::decode_vi64(data_, len_, pos, &val))) {
          LOG_WARN("failed to deserialize text string.", K(ret), K(val));
        } else if (len_ < pos + val) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to deserialize text string.", K(ret), K(len_), K(pos), K(val));
        } else {
          value_ptr_ = const_cast<char*>(data_ + pos);
          value_len_ = val;
          total_ = value_len_ + pos;
          parsed_ = true;
        }
        break;
      }
      default : {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("failed to parser header.", K(ret), K(len_), K(type_));
      }
    };
  }

  return ret;
}

int64_t ObXmlBin::attribute_size()
{
  return get_child_start();
}

int64_t ObXmlBin::attribute_count()
{
  return attribute_size();
}

int64_t ObXmlBin::size()
{
  return child_size();
}

int64_t ObXmlBin::count()
{
  return ObXmlBin::size();
}

int64_t ObXmlBin::child_size()
{
  return meta_.count_ - get_child_start();
}

int ObXmlBin::compare(const ObString& key, int& res)
{
  INIT_SUCC(ret);
  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse.", K(ret));
  } else {
    res = key.compare(meta_.get_key());
  }

  return ret;
}

int ObXmlBin::parse(const char* data, int64_t len)
{
  meta_.data_ = data;
  meta_.len_ = len;
  return parse();
}

int ObXmlBin::parse()
{
  INIT_SUCC(ret);
  if (OB_FAIL(meta_.parser())) {
    LOG_WARN("failed to parser meta string.", K(ret));
  }
  return ret;
}

int ObXmlBin::construct(ObXmlBin*& res, ObIAllocator *allocator)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) && OB_ISNULL(res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct bin, not valid allocator is provided.", K(ret));
  } else {
    ObXmlBin* tmp_res = nullptr;

    if (OB_ISNULL(res)) {
      tmp_res = static_cast<ObXmlBin*>(allocator->alloc(sizeof(ObXmlBin)));
      if (OB_ISNULL(tmp_res)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate bin.", K(ret));
      } else {
        new (tmp_res) ObXmlBin(*this);
      }
    } else {
      tmp_res = static_cast<ObXmlBin*>(res);
      tmp_res->reset();
      if (OB_FAIL(tmp_res->deep_copy(*this))) {
        LOG_WARN("failed to deep copy bin.", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_res->parse())) {
      LOG_WARN("fail to parse meta.", K(ret), K(tmp_res->meta_));
    } else {
      res = tmp_res;
    }
  }

  return ret;
}

int ObXmlBin::get_node_count(ObMulModeNodeType filter_type, int &count)
{
  INIT_SUCC(ret);
  count = 0;

  if (ObXmlUtil::is_container_tc(type())) {
    int64_t entry_size = meta_.value_entry_size_ + sizeof(uint8_t);
    const char* entry = meta_.data_ + meta_.value_entry_;

    bool is_attr_filter = (filter_type == M_ATTRIBUTE || filter_type == M_NAMESPACE);

    int32_t tmp = 0;
    for (; tmp < meta_.count_; ++tmp) {
      ObMulModeNodeType type = static_cast<ObMulModeNodeType>(entry[tmp * entry_size]);
      if (filter_type == type) {
        count++;
      }

      bool is_attr_node = (type == M_ATTRIBUTE || type == M_NAMESPACE);

      if (!is_attr_node && is_attr_filter) {
        break;
      }
    }
  }

  return ret;
}

int ObXmlBin::get_range(int64_t start, int64_t last, ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);
  start = start < get_child_start() ? meta_.child_pos_ : start;
  last = last >= meta_.count_ ? (meta_.count_ - 1) : last;

  ObXmlBinIterator iter = begin();

  if (!iter.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create iterator.", K(ret), K(iter.error_code()));
  }

  for (int64_t pos = start; iter.is_valid() && OB_SUCC(ret) && pos <= last && pos < meta_.count_; ++pos) {
    ObXmlBin* tmp = iter[pos - meta_.child_pos_];
    ObXmlBin* tmp_res = nullptr;
    bool is_match = true;
    if (!iter.is_valid() || OB_ISNULL(tmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct binary.", K(ret), K(iter.error_code()));
    } else if (OB_FAIL(tmp->construct(tmp_res, allocator_))) {
      LOG_WARN("failed to construct binary.", K(ret));
    } else if (OB_NOT_NULL(filter) && OB_FAIL(filter->operator()(tmp_res, is_match))) {
      LOG_WARN("failed to filter.", K(ret));
    } else if (is_match && OB_FAIL(res.push_back(tmp_res))) {
      LOG_WARN("failed to store result.", K(ret), K(res.count()));
    }
  }

  return ret;
}

int ObXmlBin::get_index_content(int64_t index, int64_t &index_content)
{
  INIT_SUCC(ret);
  int64_t index_pos = meta_.index_entry_ + index * meta_.index_entry_size_;
  if (index >= meta_.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index unexpected.", K(ret), K(index), K(meta_.count_));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + index_pos, meta_.index_entry_size_, &index_content))) {
    LOG_WARN("failed to read index.", K(ret));
  }
  return ret;
}

int ObXmlBin::get_sorted_key_info(int64_t index, int64_t &key_len, int64_t &key_offset)
{
  INIT_SUCC(ret);
  int64_t key_entry_offset_pos = meta_.key_entry_ + index * (2 * meta_.key_entry_size_);
  int64_t key_entry_len_pos = key_entry_offset_pos + meta_.key_entry_size_;
  if (index >= meta_.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index unexpected.", K(ret), K(index), K(meta_.count_));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + key_entry_offset_pos, meta_.key_entry_size_, &key_offset))) {
    LOG_WARN("failed to get key offset.", K(ret));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + key_entry_len_pos, meta_.key_entry_size_, &key_len))) {
    LOG_WARN("failed to get key length.", K(ret));
  }
  return ret;
}

int ObXmlBin::get_key_info(int64_t text_index, int64_t& sorted_index, int64_t &key_offset, int64_t &key_len)
{
  INIT_SUCC(ret);
  if (OB_FAIL(get_index_content(text_index, sorted_index))) {
    LOG_WARN("failed to get sorted index.", K(ret));
  } else {
    int64_t key_entry_offset_pos = meta_.key_entry_ + sorted_index * (2 * meta_.key_entry_size_);
    int64_t key_entry_len_pos = key_entry_offset_pos + meta_.key_entry_size_;
    if (sorted_index >= meta_.count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index unexpected.", K(ret), K(index), K(meta_.count_));
    } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + key_entry_offset_pos, meta_.key_entry_size_, &key_offset))) {
      LOG_WARN("failed to get key offset.", K(ret));
    } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + key_entry_len_pos, meta_.key_entry_size_, &key_len))) {
      LOG_WARN("failed to get key length.", K(ret));
    }
  }
  return ret;
}

// index is sorted index
int ObXmlBin::get_value_info(int64_t index, uint8_t &type, int64_t &value_offset, int64_t &value_len)
{
  INIT_SUCC(ret);
  int64_t get_type = 0;
  int64_t type_pos = meta_.value_entry_ + index * (sizeof(uint8_t) + meta_.value_entry_size_);
  if (index >= meta_.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index unexpected.", K(ret), K(index), K(meta_.count_));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + type_pos, sizeof(uint8_t), &get_type))) {
    LOG_WARN("failed to read index.", K(ret));
  } else {
    type = static_cast<uint8_t>(get_type);
  }

  int64_t value_entry_offset_pos = meta_.value_entry_ + index * (sizeof(uint8_t) + meta_.value_entry_size_) + sizeof(uint8_t);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + value_entry_offset_pos, meta_.value_entry_size_, &value_offset))) {
    LOG_WARN("failed to get value_offset.", K(ret));
  } else if (value_offset >= meta_.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value offset unexpect.", K(ret), K(value_offset), K(meta_.len_));
  } else if (index + 1 >= meta_.count_) {
    value_len = meta_.total_ - value_offset;
  } else {
    value_entry_offset_pos += (sizeof(uint8_t) + meta_.value_entry_size_);
    int64_t next_value_offset = 0;
    if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + value_entry_offset_pos, meta_.value_entry_size_, &next_value_offset))) {
      LOG_WARN("failed to get value_offset.", K(ret));
    } else if (value_offset >= meta_.len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get value offset unexpect.", K(ret), K(value_offset), K(meta_.len_));
    } else {
      value_len = next_value_offset - value_offset;
    }
  }
  return ret;
}

int ObXmlBin::get_child_value_start(int64_t &value_start)
{
  INIT_SUCC(ret);
  int64_t get_type = 0;
  int64_t index = attribute_size();
  int64_t value_entry_offset_pos = meta_.value_entry_ + index * (sizeof(uint8_t) + meta_.value_entry_size_) + sizeof(uint8_t);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + value_entry_offset_pos, meta_.value_entry_size_, &value_start))) {
    LOG_WARN("failed to get value_offset.", K(ret));
  } else if (value_start >= meta_.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value offset unexpect.", K(ret), K(value_start), K(meta_.len_));
  }
  return ret;
}

int32_t ObXmlBin::get_child_start()
{
  INIT_SUCC(ret);
  if (meta_.child_pos_ != -1) {
  } else {
    int64_t entry_size = meta_.value_entry_size_ + sizeof(uint8_t);
    const char* entry = meta_.data_ + meta_.value_entry_;
    if (meta_.count_ - 1 <= 0) {
      meta_.child_pos_ = 0;
    } else {
      meta_.child_pos_ = meta_.count_ - 1;
    }

    int32_t tmp = 0;
    for (; tmp < meta_.count_; ++tmp) {
      ObMulModeNodeType type = static_cast<ObMulModeNodeType>(entry[tmp * entry_size]);
      if (type == M_ATTRIBUTE || type == M_NAMESPACE) {
        meta_.child_pos_ = tmp + 1;
      } else {
        meta_.child_pos_ = tmp;
        break;
      }
    }

  }
  return meta_.child_pos_;
}

int ObXmlBin::get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);

  if (!ObXmlUtil::is_container_tc(type())) {
  } else if (OB_FAIL(get_range(get_child_start(), meta_.count_, res, filter))) {
    LOG_WARN("failed get range.", K(ret), K(meta_));
  }

  return ret;
}

int ObXmlBin::get_value(ObString& value, int64_t index)
{
  INIT_SUCC(ret);
  ObXmlBin *res = nullptr;

  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse binary.", K(ret));
  } else if (index == -1) {
    value = meta_.get_value();
  } else if (meta_.count_ - get_child_start() < index) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to eval value on binary.", K(ret), K(meta_.count_), K(get_child_start()));
  } else {
    ObXmlBin tmp(*this, nullptr);
    if (OB_FAIL(tmp.set_child_at(index))) {
      LOG_WARN("failed to set child at.", K(ret), K(index));
    } else {
      value = meta_.get_value();
    }
  }

  return ret;
}

int ObXmlBin::get_value(ObIMulModeBase*& value, int64_t index)
{
  INIT_SUCC(ret);
  ObXmlBin *res = nullptr;

  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse binary.", K(ret));
  } else if (index == -1) {
    if (OB_FAIL(construct(res, allocator_))) {
      LOG_WARN("failed to construct binary.", K(ret));
    } else {
      value = res;
    }
  } else if (meta_.count_ - get_child_start() < index) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to eval value on binary.", K(ret), K(meta_.count_), K(get_child_start()));
  } else if (OB_FAIL(construct(res, allocator_))) {
    LOG_WARN("failed to construct binary.", K(ret));
  } else if (OB_FAIL(res->set_child_at(index))) {
    LOG_WARN("failed to set child at.", K(ret), K(index));
  } else {
    value = res;
  }

  return ret;
}

int ObXmlBin::get_key(ObString& res, int64_t index)
{
  INIT_SUCC(ret);

  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse binary.", K(ret));
  } else if (index == -1) {
    res = meta_.get_key();
  } else if (meta_.count_ - get_child_start() < index) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to eval value on binary.", K(ret), K(meta_.count_), K(get_child_start()));
  } else {
    ObXmlBin tmp(*this);
    if (OB_FAIL(tmp.set_child_at(index))) {
      LOG_WARN("failed to set child at.", K(ret), K(index));
    } else {
      res = meta_.get_key();
    }
  }

  return ret;
}

int ObXmlBin::get_value_entry_type(uint8_t &type, int64_t index)
{
  INIT_SUCC(ret);
  ObXmlBin *xml_bin = (ObXmlBin *)this;
  ObXmlBinMetaParser meta = xml_bin->meta_;
  int64_t get_type = 0;
  const char *data = meta.data_;
  int32_t count = meta.count_;
  int64_t index_pos = meta.value_entry_ + index * (sizeof(uint8_t) + meta.value_entry_size_);

  if (OB_FAIL(ObMulModeVar::read_size_var(data + index_pos, sizeof(uint8_t), &get_type))) {
    LOG_WARN("failed to read index.", K(ret));
  } else if (get_type >= M_MAX_TYPE || get_type < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sort index unexpected", K(ret), K(get_type), K(meta.len_));
  } else {
    type = static_cast<uint8_t>(get_type);
  }

  return ret;
}

int ObXmlBin::get_index_key(ObString& key, int64_t &origin_index, int64_t &value_offset, int64_t index)
{
  INIT_SUCC(ret);
  ObXmlBin *xml_bin = (ObXmlBin *)this;
  ObXmlBinMetaParser meta = xml_bin->meta_;
  const char *data = buffer_.length() != 0 ? buffer_.ptr() : meta_.data_;
  int32_t count = meta.count_;
  int64_t val = 0;
  int64_t key_length = 0;
  int64_t key_offset = 0;
  int64_t index_pos = meta.index_entry_ + index * meta.index_entry_size_;

  if (index >= count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index unexpected.", K(ret), K(index), K(count));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(data + index_pos,
                                                  meta.index_entry_size_,
                                                  &origin_index))) {
    LOG_WARN("failed to read index.", K(ret));
  } else if (origin_index >= count || origin_index < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sort index unexpected", K(ret), K(origin_index), K(count));
  } else {
    int64_t key_entry_offset_pos = meta.key_entry_ + origin_index * (2 * meta.key_entry_size_);
    int64_t key_entry_len_pos = key_entry_offset_pos + meta.key_entry_size_;
    int64_t value_entry_offset_pos = meta.value_entry_ + origin_index * (sizeof(uint8_t)
                                     + meta.value_entry_size_) + sizeof(uint8_t);
    if (OB_FAIL(ObMulModeVar::read_size_var(data + key_entry_len_pos,
                                            meta.key_entry_size_,
                                            &key_length))) {
      LOG_WARN("failed to get key length.", K(ret));
    } else if (key_length >= meta.len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get value offset index unexpected", K(ret), K(key_length), K(meta.total_));
    } else if (OB_FAIL(ObMulModeVar::read_size_var(data + key_entry_offset_pos,
                                                   meta.key_entry_size_,
                                                   &key_offset))) {
      LOG_WARN("failed to get key offset.", K(ret));
    } else if (key_offset >= meta.total_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get key offset unexpect.", K(ret), K(key_offset), K(meta.total_));
    } else if (OB_FAIL(ObMulModeVar::read_size_var(data + value_entry_offset_pos,
                                                   meta.value_entry_size_,
                                                   &value_offset))) {
      LOG_WARN("failed to get value_offset.", K(ret));
    } else if (value_offset >= meta.total_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get value offset unexpect.", K(ret), K(value_offset), K(meta.total_));
    } else {
      key.assign_ptr(data + key_offset, key_length);
    }
  }

  return ret;
}

int ObXmlBin::get_total_value(ObString& res, int64_t value_start)
{
  INIT_SUCC(ret);
  ObXmlBin *xml_bin = (ObXmlBin *)this;
  ObXmlBinMetaParser meta = xml_bin->meta_;
  const char *data = buffer_.length() != 0 ? buffer_.ptr() : meta_.data_;
  res.assign_ptr(data + value_start, meta.total_ - value_start);
  return ret;
}

ObString ObXmlBin::get_element_buffer()
{
  ObString res;
  ObXmlBin *xml_bin = (ObXmlBin *)this;
  res.assign_ptr(xml_bin->meta_.data_, xml_bin->meta_.len_);
  return res;
}

int ObXmlBin::get_text_value(ObString &value)
{
  INIT_SUCC(ret);
  int64_t pos = sizeof(uint8_t);
  const char *data = meta_.data_;
  int64_t data_len = meta_.len_;
  int64_t val = 0;

  if (this->type() != M_TEXT) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(data, data_len, pos, &val))) {
    LOG_WARN("failed to deserialize text string.", K(ret), K(val));
  } else if ((data_len < pos + val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to deserialize text string.", K(ret), K(data_len), K(pos), K(val));
  } else {
    value.assign_ptr(data + pos, val);
  }

  return ret;
}

int ObXmlBin::get_value_start(int64_t &value_start)
{
  INIT_SUCC(ret);
  int32_t count = meta_.count_;
  const char *data = meta_.data_;
  int64_t last_key_len = 0;
  int64_t last_key_offset = 0;
  int64_t last_key_len_pos = meta_.key_entry_ + (count - 1) * meta_.key_entry_size_ * 2;
  int64_t last_key_offset_pos = last_key_len_pos + meta_.key_entry_size_;
  if (OB_FAIL(ObMulModeVar::read_size_var(data + last_key_len_pos,
                                          meta_.key_entry_size_,
                                          &last_key_len))) {
    LOG_WARN("failed to get key length.", K(ret));
  } else if (last_key_len > meta_.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value offset index unexpected", K(ret), K(last_key_len), K(meta_.len_));
  } else if (OB_FAIL(ObMulModeVar::read_size_var(data + last_key_offset_pos,
                                                  meta_.key_entry_size_,
                                                  &last_key_offset))) {
    LOG_WARN("failed to get key offset.", K(ret));
  } else if (last_key_offset > meta_.total_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value offset index unexpected", K(ret), K(last_key_offset), K(meta_.total_));
  } else {
    value_start = last_key_offset + last_key_len;
  }
  return ret;
}

int64_t ObXmlBin::low_bound(const ObString& key)
{
  ObXmlKeyCompare comparator;
  ObString tmp_key;
  int64_t child_start = get_child_start();

  int64_t low =  child_start;
  int64_t high = meta_.count_  - 1;
  int64_t iter = low;

  int64_t step = 0;
  int64_t count = high - low + 1;

  // do binary search
  while (count > 0) {
    iter = low;
    step = count / 2;
    iter += step;

    ObXmlBinKey bin_key(meta_.data_, meta_.key_entry_, iter, meta_.key_entry_size_type_);
    tmp_key = bin_key.get_key();

    int compare_result = comparator(tmp_key, key);
    if (compare_result < 0) {
      low = ++iter;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return low;
}

int64_t ObXmlBin::up_bound(const ObString& key)
{
  ObXmlKeyCompare comparator;
  ObString tmp_key;

  int64_t child_start = get_child_start();

  int64_t low =  child_start;
  int64_t high = meta_.count_ - 1;
  int64_t iter = low;

  int64_t step = 0;
  int64_t count = high - low + 1;

  // do binary search
  while (count > 0) {
    iter = low;
    step = count / 2;
    iter += step;

    ObXmlBinKey bin_key(meta_.data_, meta_.key_entry_, iter, meta_.key_entry_size_type_);
    tmp_key = bin_key.get_key();

    int compare_result = comparator(tmp_key, key);
    if (compare_result <= 0) {
      low = ++iter;
      count -= step + 1;
    } else {
      count = step;
    }
  }

  return low;
}

int ObXmlBin::get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res, ObMulModeFilter* filter)
{
  INIT_SUCC(ret);
  ObXmlKeyCompare comparator;
  ObString tmp_key;

  int64_t low = low_bound(key);
  int64_t upper = up_bound(key);
  int64_t count = meta_.count_;

  if (low < count) {
    for (int64_t iter = low; OB_SUCC(ret) && iter < upper && iter < count; ++iter) {
      ObXmlBinIndexMeta index_meta(meta_.data_ + meta_.index_entry_, iter, meta_.index_entry_size_type_);
      ObXmlBin* tmp_res = nullptr;

      bool is_match = true;
      if (OB_FAIL(construct(tmp_res, allocator_))) {
        LOG_WARN("failed to construct binary.", K(ret));
      } else if (OB_FAIL(tmp_res->set_sorted_at(iter))) {
        LOG_WARN("failed to set at.", K(ret), K(index_meta.get_index()));
      } else if (OB_NOT_NULL(filter) && OB_FAIL(filter->operator()(tmp_res, is_match))) {
        LOG_WARN("failed to filter.", K(ret));
      } else if (is_match && OB_FAIL(res.push_back(tmp_res))) {
        LOG_WARN("fail to store scan result", K(ret), K(res.count()));
      }
    }
  }

  return ret;
}

bool ObXmlBin::has_flags(ObMulModeNodeFlag flag)
{
  bool res = false;
  if (flag & XML_DECL_FLAG) {
    res = meta_.has_xml_decl_;
  } else if (flag & XML_ENCODING_EMPTY_FLAG) {
    res = meta_.encoding_val_empty_;
  }

  return res;
}

ObIMulModeBase* ObXmlBin::at(int64_t pos, ObIMulModeBase* buffer)
{
  INIT_SUCC(ret);
  ObXmlBin *res = nullptr;

  if (OB_NOT_NULL(buffer)) {
    res = static_cast<ObXmlBin*>(buffer);
  }

  if (OB_FAIL(construct(res, allocator_))) {
    res = nullptr;
    LOG_WARN("failed to construct binary.", K(ret));
  } else if (OB_FAIL(res->set_child_at(pos))) {
    res = nullptr;
    LOG_WARN("failed to set child at.", K(ret), K(pos));
  }

  return res;
}

ObIMulModeBase* ObXmlBin::attribute_at(int64_t pos, ObIMulModeBase* buffer)
{
  INIT_SUCC(ret);
  ObXmlBin *res = nullptr;

  if (OB_NOT_NULL(buffer)) {
    res = static_cast<ObXmlBin*>(buffer);
  }

  if (OB_FAIL(construct(res, allocator_))) {
    res = nullptr;
    LOG_WARN("failed to construct binary.", K(ret));
  } else if (OB_FAIL(res->set_at(pos))) {
    res = nullptr;
    LOG_WARN("failed to set child at.", K(ret), K(pos));
  }

  return res;
}

ObIMulModeBase* ObXmlBin::sorted_at(int64_t pos, ObIMulModeBase* buffer)
{
  INIT_SUCC(ret);
  ObXmlBin *res = nullptr;
  res = static_cast<ObXmlBin*>(buffer);

  if (OB_FAIL(construct(res, allocator_))) {
    res = nullptr;
    LOG_WARN("failed to construct binary.", K(ret));
  } else if (OB_FAIL(res->set_sorted_at(pos))) {
    res = nullptr;
    LOG_WARN("failed to set child at.", K(ret), K(pos));
  }

  return res;
}

int ObXmlBin::get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags)
{
  INIT_SUCC(ret);

  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse.", K(ret), K(type()));
  } else if (filter_type == M_NAMESPACE) {
    int64_t attribute_num = attribute_size();
    for (int pos = 0; OB_SUCC(ret) && pos < attribute_num ; ++pos) {
      ObXmlBin buff(*this);
      ObXmlBin* tmp = &buff;

      if (OB_FAIL(construct(tmp, allocator_))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_NAMESPACE) {
        bool is_match = true;
        if (flags) {
          ObString prefix;
          if (OB_FAIL(tmp->get_key(prefix))) {
            LOG_WARN("failed to eval key.", K(ret));
          } else if (prefix.compare(ObXmlConstants::XMLNS_STRING)) {
            is_match = false;
          }
        }

        if (OB_SUCC(ret) && is_match) {
          ObXmlBin* dup = nullptr;
          if (OB_FAIL(tmp->construct(dup, allocator_))) {
            LOG_WARN("failed to dup bin.", K(ret));
          } else if (OB_FAIL(res.push_back(dup))) {
            LOG_WARN("fail to store bin ptr", K(ret));
          }
        }
      } else if (tmp->type() == M_ATTRIBUTE) {
      } else {
        break;
      }
    }
  } else if (filter_type == M_ATTRIBUTE) {
    int64_t attribute_num = attribute_size();
    for (int pos = 0; OB_SUCC(ret) && pos < attribute_num ; ++pos) {
      ObXmlBin* tmp = nullptr;
      if (OB_FAIL(construct(tmp, allocator_))) {
        LOG_WARN("failed to dup bin.", K(ret));
      } else if (OB_FAIL(tmp->set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp->type() == M_ATTRIBUTE) {
        if (OB_FAIL(res.push_back(tmp))) {
          LOG_WARN("failed to store result.", K(ret), K(res.count()));
        }
      } else if (tmp->type() == M_NAMESPACE) {
      } else {
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get attr list", K(ret), K(filter_type));
  }

  return ret;
}

int ObXmlBin::node_ns_value(ObString& prefix, ObString& ns_value)
{
  INIT_SUCC(ret);
  bool found = false;

  int64_t attribute_num = attribute_size();
  for (int attr_pos = 0; OB_SUCC(ret) && attr_pos < attribute_num; ++attr_pos) {
    ObString tmp_prefix;
    ObXmlBin tmp(*this);
    if (OB_FAIL(tmp.set_at(attr_pos))) {
      LOG_WARN("failed to set at child.", K(ret));
    } else if (tmp.type() == M_NAMESPACE) {
      if (OB_FAIL(tmp.get_key(tmp_prefix))) {
        LOG_WARN("failed to get ns key.", K(ret));
      } else if (prefix.empty()) {
        if (tmp_prefix.compare(ObXmlConstants::XMLNS_STRING) == 0) {
          ns_value = tmp.meta_.get_value();
          found = true;
          break;
        }
      } else if (tmp_prefix.compare(prefix) == 0) {
        ns_value = tmp.meta_.get_value();
        found = true;
        break;
      }
    } else if (tmp.type() == M_ATTRIBUTE || tmp.type() == M_INSTRUCT) {
    } else {
      break;
    }
  }
  return ret;
}

int ObXmlBin::get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString& ns_value, ObIMulModeBase* extend)
{
  INIT_SUCC(ret);
  bool found = false;
  int64_t size = stk.size();
  ObString prefix = get_prefix();

  if (type() == M_ATTRIBUTE && prefix.empty()) {
  } else if (prefix.compare(ObXmlConstants::XML_STRING) == 0) {
    ns_value = ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI;
  } else if (OB_FAIL(node_ns_value(prefix, ns_value))) {
    LOG_WARN("failed get node ns value.", K(ret));
  } else if (!ns_value.empty()) {
  } else if (size > 0) {
    for (int64_t pos = size - 1; !found && OB_SUCC(ret) && pos >= 0; --pos) {
      ObXmlBin* current = static_cast<ObXmlBin*>(stk.at(pos));

      if (OB_FAIL(current->node_ns_value(prefix, ns_value))) {
        LOG_WARN("failed get node ns value.", K(ret));
      } else if (!ns_value.empty()) {
        found = true;
        break;
      }
    }

    // if didn't find ns definition after traversing ancestor nodes, check exrend area
    if (ns_value.empty() && size > 0) {
      // get root node
      ObXmlBin* extend_bin;
      if (OB_ISNULL(extend)) { // without extend, its normal
      } else if (OB_ISNULL(extend_bin = static_cast<ObXmlBin*>(extend))) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret));
      } else if (OB_FAIL(extend_bin->node_ns_value(prefix, ns_value))) {
        LOG_WARN("failed get node ns value.", K(ret));
      }
    }

  }
  return ret;
}

int ObXmlBin::get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx)
{
  INIT_SUCC(ret);
  ObString tmp_prefix;

  if (prefix.compare(ObXmlConstants::XML_STRING) == 0) {
    ns_value = ObXmlConstants::XML_NAMESPACE_SPECIFICATION_URI;
  } else {
    bool found = false;
    int64_t attribute_num = attribute_size();
    for (int pos = 0; OB_SUCC(ret) && pos < attribute_num && !found ; ++pos) {
      ObXmlBin tmp(*this);
      ObString tmp_prefix;
      if (OB_FAIL(tmp.set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (tmp.type() == M_NAMESPACE) {
        if (OB_FAIL(tmp.get_key(tmp_prefix))) {
          LOG_WARN("failed to get ns key.", K(ret));
        } else if (prefix.empty()) {
          if (tmp_prefix.compare(ObXmlConstants::XMLNS_STRING) == 0) {
            tmp.get_value(ns_value);
            found = true;
            ans_idx = pos;
            break;
          }
        } else if (tmp_prefix.compare(prefix) == 0) {
          tmp.get_value(ns_value);
          found = true;
          ans_idx = pos;
          break;
        }
      } else if (tmp.type() == M_ATTRIBUTE || tmp.type() == M_INSTRUCT) {
      } else {
        break;
      }
    }
  }

  return ret;
}

int ObXmlBin::get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& ns_name, const ObString &node_key)
{
  INIT_SUCC(ret);
  res = nullptr;
  ObString prefix;

  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse.", K(ret), K(type()));
  } else if (filter_type == M_NAMESPACE || filter_type == M_ATTRIBUTE) {
    int64_t attribute_num = attribute_size();
    bool found = false;
    ObXmlBin tmp(*this);

    for (int pos = 0; OB_SUCC(ret) && !found && pos < attribute_num ; ++pos) {
      ObString tmp_key;
      tmp.deep_copy(*this);
      if (OB_FAIL(tmp.set_at(pos))) {
        LOG_WARN("failed to set at child.", K(ret));
      } else if (OB_FAIL(tmp.get_key(tmp_key))) {
        LOG_WARN("failed to eval key.", K(ret));
      } else if (tmp.type() == M_NAMESPACE) {
        if (filter_type != M_NAMESPACE) {
        } else if (tmp_key.compare(ns_name) == 0) {
          found = true;
          break;
        }
      } else if (tmp.type() == M_ATTRIBUTE) {
        if (filter_type != M_ATTRIBUTE) {
        } else if (node_key.compare(tmp_key) == 0) {
          ObString ns_value;
          if (OB_FAIL(tmp.get_ns_value(ns_value))) {
            LOG_WARN("failed to get valid namesapce value.", K(ret));
          } else if (!ns_name.empty() && (ns_value.empty() || (!ns_value.empty() && ns_value.compare(ns_name)))) {
          } else if (ns_name.empty() && !ns_value.empty()) {
            found = true;
            break;
          } else {
            found = true;
            break;
          }
        }
      } else if (tmp.type() == M_INSTRUCT) {
      } else {
        break;
      }
    }

    if (OB_SUCC(ret) && found) {
      ObXmlBin* tmp_res = nullptr;
      if (OB_FAIL(tmp.construct(tmp_res, ctx_->allocator_))) {
        LOG_WARN("failed to dup res.", K(ret));
      } else{
        res = tmp_res;
      }
    }
  }

  return ret;
}

int ObXmlBin::set_child_at(int64_t pos)
{
  return set_at(get_child_start() + pos);
}

int ObXmlBin::set_sorted_at(int64_t sort_index)
{
  INIT_SUCC(ret);

  meta_.sort_idx_ = sort_index;
  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse header.", K(ret));
  } else if (meta_.count_ <= sort_index || sort_index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to set iter on child.", K(ret), K(sort_index), K(meta_.count_));
  } else {
    uint64_t value_start = 0;
    uint64_t key_offset = 0;
    uint64_t key_len = 0;

    if (OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_value_offset(sort_index) + sizeof(uint8_t),
                                       meta_.value_entry_size_type_, &value_start))) {
      LOG_WARN("failed to read value offset.", K(ret), K(meta_));
    } else if (meta_.len_ <= value_start) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parser value.", K(meta_.len_), K(value_start));
    } else if (OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_key_offset(sort_index), meta_.key_entry_size_type_, &key_offset))
              || OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_key_offset(sort_index) + meta_.key_entry_size_, meta_.key_entry_size_type_, &key_len))) {
      LOG_WARN("failed to read key index.", K(meta_), K(ret));
    } else if (key_offset + key_len >= meta_.len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get key index.", K(meta_), K(key_offset), K(key_len));
    } else {
      ObString tmp_key(key_len, meta_.data_ + key_offset);
      if (OB_FAIL(meta_.parser(meta_.data_ + value_start, meta_.len_ - value_start))) {
        LOG_WARN("failed to parser value header.", K(ret), K(meta_));
      } else {
        meta_.key_ptr_ = tmp_key.ptr();
        meta_.key_len_ = tmp_key.length();
        // set at child, update len_ to child len
        meta_.len_ = meta_.total_;
      }
    }
  }
  return ret;
}

int ObXmlBin::set_at(int64_t pos)
{
  INIT_SUCC(ret);

  meta_.idx_ = pos;
  if (OB_FAIL(parse())) {
    LOG_WARN("failed to parse header.", K(ret));
  } else if (meta_.count_ <= pos || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to set iter on child.", K(ret), K(pos), K(meta_.count_));
  } else {
    uint64_t value_start = 0;
    uint64_t key_offset = 0;
    uint64_t key_len = 0;
    int64_t sort_index = 0;
    if (OB_FAIL(ObMulModeVar::read_size_var(meta_.data_ + meta_.get_index(pos), meta_.index_entry_size_, &sort_index))) {
      LOG_WARN("failed to read sort index.", K(ret), K(meta_), K(pos));
    } else if (sort_index >= meta_.count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parser index.", K(ret), K(sort_index), K(meta_.count_));
    } else if (OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_value_offset(sort_index) + sizeof(uint8_t),
                                       meta_.value_entry_size_type_, &value_start))) {
      LOG_WARN("failed to read value offset.", K(ret), K(meta_));
    } else if (meta_.len_ <= value_start) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parser value.", K(meta_.len_), K(value_start));
    } else if (OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_key_offset(sort_index), meta_.key_entry_size_type_, &key_offset))
              || OB_FAIL(ObMulModeVar::read_var(meta_.data_ + meta_.get_key_offset(sort_index) + meta_.key_entry_size_, meta_.key_entry_size_type_, &key_len))) {
      LOG_WARN("failed to read key index.", K(meta_), K(ret));
    } else if (key_offset + key_len >= meta_.len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get key index.", K(meta_), K(key_offset), K(key_len));
    } else {
      ObString tmp_key(key_len, meta_.data_ + key_offset);
      if (OB_FAIL(meta_.parser(meta_.data_ + value_start, meta_.len_ - value_start))) {
        LOG_WARN("failed to parser value header.", K(ret), K(meta_));
      } else {
        meta_.sort_idx_ = sort_index;
        meta_.key_ptr_ = tmp_key.ptr();
        meta_.key_len_ = tmp_key.length();
        // set at child, update len_ to child len
        meta_.len_ = meta_.total_;
      }
    }
  }
  return ret;
}


int ObXmlBin::deep_copy(ObXmlBin& from)
{
  INIT_SUCC(ret);
  meta_ = from.meta_;
  return ret;
}

int ObXmlBin::get_raw_binary(common::ObString &out, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  ObIAllocator *alloc = allocator == nullptr ? allocator_ : allocator;
  if (OB_ISNULL(alloc)) {
    char* buf = static_cast<char*>(alloc->alloc(meta_.len_ + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret), K(meta_.len_));
    } else {
      out.assign_buffer(buf, meta_.len_ + 1);
      if (OB_FAIL(out.write(meta_.data_, meta_.len_))) {
        LOG_WARN("failed to write buffer.", K(ret), K(meta_.len_));
      }
    }
  } else {
    out.assign_ptr(meta_.data_, meta_.len_);
  }

  return ret;
}

bool ObXmlBin::is_equal_node(const ObIMulModeBase* other)
{
  bool res = false;
  if (OB_ISNULL(other)) {
  } else if (other->is_binary()) {
    ObIMulModeBase* ref_other = const_cast<ObIMulModeBase*>(other);
    ObXmlBin* tmp = static_cast<ObXmlBin*>(ref_other);
    res = tmp->meta_ == meta_;
  }
  return res;
}

bool ObXmlBin::is_node_before(const ObIMulModeBase* other)
{
  bool res = false;
  if (OB_ISNULL(other)) {
  } else if (other->is_binary()) {
    ObIMulModeBase* ref_other = const_cast<ObIMulModeBase*>(other);
    ObXmlBin* tmp = static_cast<ObXmlBin*>(ref_other);
    res = tmp->meta_ < meta_;
  }
  return res;
}
ObXmlBin::iterator ObXmlBin::begin()
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = allocator_;
  ObXmlBin::iterator iter(this);

  if (OB_FAIL(meta_.parser())) {
    LOG_WARN("failed to parse meta header.", K(meta_), K(ret));
    iter.error_code_ = ret;
    iter.is_valid_ = false;
  } else {
    iter.cur_pos_ = 0;
    iter.total_ = meta_.count_;
    iter.meta_header_ = iter.cur_node_.meta_;
    iter.is_valid_ = true;
  }

  return iter;
}

ObXmlBin::iterator ObXmlBin::end()
{
  INIT_SUCC(ret);
  ObXmlBin::iterator iter(this);

  if (OB_FAIL(meta_.parser())) {
    iter.error_code_ = ret;
    iter.is_valid_ = false;
    LOG_WARN("failed to parse meta header.", K(meta_), K(ret));
  } else {
    iter.total_ = meta_.count_;
    iter.cur_pos_ = iter.total_;
    iter.meta_header_ = iter.cur_node_.meta_;
    iter.is_valid_ = true;
  }

  return iter;
}

void ObXmlBinIterator::set_range(int64_t start, int64_t finish)
{
  cur_pos_ = start;
  if (finish < total_) {
    total_ = finish;
  }
}

bool ObXmlBinIterator::end()
{
  return cur_pos_ >= total_;
}

bool ObXmlBinIterator::begin()
{
  return cur_pos_ <= 0;
}

ObXmlBin* ObXmlBinIterator::current()
{
  ObXmlBin* res = nullptr;
  INIT_SUCC(ret);
  if (!is_valid()) {
  } else {
    res = &cur_node_;
  }

  return res;
}

ObXmlBin* ObXmlBinIterator::operator*()
{
  return operator[](cur_pos_);
}

ObXmlBin* ObXmlBinIterator::operator->()
{
  return operator*();
}

ObXmlBin* ObXmlBinIterator::operator[](int64_t pos)
{
  INIT_SUCC(ret);
  ObXmlBin* res = nullptr;

  if (!is_valid()) {
  } else { //
    cur_node_.meta_ = meta_header_;
    if (is_sorted_iter_ && OB_FAIL(cur_node_.set_sorted_at(pos))) {
      LOG_WARN("failed to set sorted iter at.", K(cur_node_.meta_), K(pos), K(ret));
      is_valid_ = false;
      error_code_ = ret;
    } else if (!is_sorted_iter_ && OB_FAIL(cur_node_.set_at(pos))) {
      LOG_WARN("failed to set iter at.", K(cur_node_.meta_), K(pos), K(ret));
      is_valid_ = false;
      error_code_ = ret;
    } else {
      res = &cur_node_;
    }
  }

  return res;
}

ObXmlBinIterator& ObXmlBinIterator::next()
{
  cur_pos_++;
  return *this;
}

ObXmlBinIterator& ObXmlBinIterator::operator++()
{
  return next();
}

ObXmlBinIterator& ObXmlBinIterator::operator--()
{
  cur_pos_--;
  return *this;
}

ObXmlBinIterator ObXmlBinIterator::operator++(int)
{
  ObXmlBinIterator iter(*this);
  cur_pos_++;
  return iter;
}

ObXmlBinIterator ObXmlBinIterator::operator--(int)
{
  ObXmlBinIterator iter(*this);
  cur_pos_++;
  return iter;
}

bool ObXmlBinIterator::operator<(const ObXmlBinIterator& iter)
{
  return (meta_header_ == iter.meta_header_ && cur_pos_ < iter.cur_pos_);
}

bool ObXmlBinIterator::operator>(const ObXmlBinIterator& iter)
{
  return (meta_header_ == iter.meta_header_ && cur_pos_ > iter.cur_pos_);
}

ObXmlBinIterator& ObXmlBinIterator::operator-=(int size)
{
  cur_pos_ -= size;
  return *this;
}

ObXmlBinIterator& ObXmlBinIterator::operator+=(int size)
{
  cur_pos_ += size;
  return *this;
}

ObXmlBinIterator ObXmlBinIterator::operator+(int size)
{
  ObXmlBinIterator iter(*this);
  iter.cur_pos_ += size;
  return iter;
}


ObXmlBinIterator ObXmlBinIterator::operator-(int size)
{
  ObXmlBinIterator iter(*this);
  iter.cur_pos_ -= size;
  return iter;
}

int64_t ObXmlBinIterator::operator-(const ObXmlBinIterator& iter)
{
  return cur_pos_ - iter.cur_pos_;
}

bool ObXmlBinIterator::operator==(const ObXmlBinIterator& rhs)
{
  return (meta_header_ == rhs.meta_header_ && cur_pos_ == rhs.cur_pos_);
}

bool ObXmlBinIterator::operator!=(const ObXmlBinIterator& rhs)
{
  return !(*this == rhs);
}

bool ObXmlBinIterator::operator<=(const ObXmlBinIterator& rhs)
{
  return (meta_header_ == rhs.meta_header_ && cur_pos_ <= rhs.cur_pos_);
}


int ObXmlBinMerge::init_merge_info(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  // use for xml binary merge, make sure is xml binary
  if (origin.data_type() != OB_XML_TYPE || patch.data_type() != OB_XML_TYPE ||origin.is_tree() || patch.is_tree()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be binary", K(origin.is_tree()), K(patch.is_tree()), K(ret));
  } else {
    ObXmlBin* bin_res = static_cast<ObXmlBin*>(&res);
    ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
    ObXmlBin* bin_patch = static_cast<ObXmlBin*>(&patch);
    if (OB_ISNULL(bin_res) || OB_ISNULL(bin_origin)|| OB_ISNULL(bin_patch)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should not be null", K(ret));
    } else if (!bin_origin->meta_.parsed_ && OB_FAIL(bin_origin->parse())) {
      LOG_WARN("fail to parse", K(ret));
    } else if (!bin_patch->meta_.parsed_ && OB_FAIL(bin_patch->parse())) {
      LOG_WARN("fail to parse", K(ret));
    } else if (OB_FAIL(bin_res->buffer_.reserve(estimated_length(false, ctx, origin, patch)))) {
      LOG_WARN("fail to reserve", K(ret));
    } else {
      int64_t ns_num = patch.attribute_size();
      // if only merge attribute, we only resort attribute key
      if (patch.size() == 0 && patch.attribute_size() > 0) {
        ctx.only_merge_ns_ = 1;
      }
      ObXmlBin bin_buffer;
      ObIMulModeBase* cur = nullptr;
      // init delete vector
      for (int i = 0; OB_SUCC(ret) && i < ns_num; ++i) {
        cur = patch.attribute_at(i, &bin_buffer);
        if (OB_ISNULL(cur)) {
        } else if (cur->type() != ObMulModeNodeType::M_NAMESPACE) {
          ctx.only_merge_ns_ = 0;
        }

        if (OB_FAIL(ctx.del_map_.push(false))) {
          LOG_WARN("failed to init delete map.", K(ret));
        }
      }
      ctx.buffer_ = &bin_res->buffer_;
      ctx.reuse_del_map_ = 1;
      ctx.reserve_ = 0;
      ctx.retry_count_ = 0;
      ctx.retry_len_ = 0;
    }
  }
  return ret;
}

// first, delete duplicate ns, then check:
//	 1. when there is no valid ns in patch, return false;
//	 2. when origin ns didn't defined in patch, and origin have no elemen child, return false;
int ObXmlBinMerge::if_need_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObIMulModeBase& res, bool& need_merge)
{
  INIT_SUCC(ret);
  need_merge = true;
  if (origin.type() == M_DOCUMENT || origin.type() == M_CONTENT) {
    int64_t origin_children = origin.size();
    bool has_element = false;
    ObIMulModeBase* cur = nullptr;
    ObXmlBin bin_buffer;
    for (int64_t i = 0; OB_SUCC(ret) && !has_element && i < origin_children; i++) {
      cur = origin.at(i, &bin_buffer);
      if (OB_NOT_NULL(cur) && cur->type() == M_ELEMENT) {
        has_element = true;
      }
    }
    need_merge = has_element;
  } else if (origin.type() == M_ELEMENT && patch.type() == M_ELEMENT) {
    int64_t origin_ns_num = origin.attribute_size();
    ObXmlBin bin_buffer;
    ObIMulModeBase* cur = nullptr;
    ctx.defined_ns_idx_.reset();

    // check if has duplicate ns definition
    for (int64_t i = 0; i < origin_ns_num && OB_SUCC(ret); i++) {
      cur = origin.attribute_at(i, &bin_buffer);
      ObString tmp_ns_key;
      ObString tmp_ns_value;
      int ans_idx = -1;
      if (OB_ISNULL(cur)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret));
      } else if (cur->type() == ObMulModeNodeType::M_NAMESPACE) {
        cur->get_key(tmp_ns_key);
        patch.get_ns_value(tmp_ns_key, tmp_ns_value, ans_idx);
        if (0 <= ans_idx && !tmp_ns_key.empty() && ans_idx < ctx.del_map_.size()) {
          ret = ctx.del_map_.set(ans_idx, true);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ctx.defined_ns_idx_.size() == 0 && ctx.is_all_deleted()) {
      need_merge = false;
    } else {
      // if origin ns defined in patch
      ObString ns_prefix = origin.get_prefix();
      ObString tmp_ns_value;
      int ans_idx = -1;
      ret = patch.get_ns_value(ns_prefix, tmp_ns_value, ans_idx);
      if (OB_FAIL(ret)) {
      } else if (0 <= ans_idx && ans_idx < ctx.del_map_.size() && !(ctx.del_map_.at(ans_idx))) {
        int origin_ans_idx = -1;
        ret = ctx.del_map_.set(ans_idx, true);
        if (OB_FAIL(ret)) {
        } else if (origin.type() == M_ELEMENT
        && OB_FAIL(origin.get_ns_value(ns_prefix, tmp_ns_value, origin_ans_idx))) {
          LOG_WARN("fail to check ns definition in origin", K(ret));
        } else if (origin_ans_idx == -1) {
          ctx.defined_ns_idx_.push(ans_idx);
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        for (int64_t i = 0; i < origin_ns_num && OB_SUCC(ret); i++) {
          cur = origin.attribute_at(i, &bin_buffer);
          ObString tmp_ns_key;
          ObString tmp_ns_value;
          int ans_idx = -1;
          if (OB_ISNULL(cur)) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("should not be null", K(ret));
          }  else if (cur->type() == ObMulModeNodeType::M_ATTRIBUTE) {
            // check attribute ns
            tmp_ns_key = cur->get_prefix();
            if (!tmp_ns_key.empty()) {
              ret = patch.get_ns_value(tmp_ns_key, tmp_ns_value, ans_idx);
              if (OB_SUCC(ret) && 0 <= ans_idx && ans_idx < ctx.del_map_.size() && !(ctx.del_map_.at(ans_idx))) {
                if (OB_SUCC(ctx.del_map_.set(ans_idx, true))) {
                  ret = ctx.defined_ns_idx_.push(ans_idx);
                }
              }
            } // default attribute do not add default ns
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ctx.defined_ns_idx_.size() == 0) {
        int64_t origin_children = origin.size();
        bool has_element = false;
        for (int64_t i = 0; OB_SUCC(ret) && !has_element && i < origin_children; i++) {
          cur = origin.at(i, &bin_buffer);
          if (OB_NOT_NULL(cur) && cur->type() == M_ELEMENT) {
            has_element = true;
          }
        }
        need_merge = has_element;
      } else {
        need_merge = true;
      }
    } // has valid ns
  } else {
    // not element, do not need merge ns
    need_merge = false;
  }
  return ret;
}

bool ObXmlBinMerge::if_need_append_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                      ObIMulModeBase& patch, ObIMulModeBase& res)
{
  return ctx.defined_ns_idx_.size() > 0;
}

// for xml, must be append origin as res
// but for json, may be patch or origin
int ObXmlBinMerge::append_res_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                            ObIMulModeBase& patch, ObIMulModeBase& res)
{
  return append_value_without_merge(ctx, origin, res);
}

int ObXmlBinMerge::append_value_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& value, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  ObXmlBin* bin_val = static_cast<ObXmlBin*>(&value);
  ObXmlBin* bin_res = static_cast<ObXmlBin*>(&res);
  if (OB_ISNULL(bin_val) || OB_ISNULL(bin_res)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(bin_res->buffer_.reserve(bin_val->meta_.len_))) {
    LOG_WARN("fail to reserve val", K(ret));
  } else if (OB_FAIL(bin_res->buffer_.append(bin_val->meta_.data_, bin_val->meta_.len_))) {
    LOG_WARN("fail to append val", K(ret));
  }
  return ret;
}

int ObXmlBinMergeMeta::init_merge_meta(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObMulBinHeaderSerializer& header, bool with_patch)
{
  INIT_SUCC(ret);
  header_ = &header;
  attr_count_ = with_patch ? (origin.attribute_size() + ctx.defined_ns_idx_.size()) : origin.attribute_size();
  child_count_ = origin.size();
  int64_t children_count = attr_count_ + child_count_;
  header.count_ = children_count;
  header_start_ = header_->start();
  int64_t header_len = header_->header_size();
  bool element_header = true;
  if (header.type_ == M_DOCUMENT || header.type_ == M_CONTENT) {
    new (&doc_header_) ObXmlDocBinHeader(origin.get_version(),
                                         origin.get_encoding(),
                                         origin.get_encoding_flag(),
                                         origin.get_standalone(),
                                         origin.has_xml_decl());
    new(&doc_header_.elem_header_) ObXmlElementBinHeader(origin.get_unparse(), origin.get_prefix());
    header_len += doc_header_.header_size();
    element_header = false;
  } else {
    new(&ele_header_) ObXmlElementBinHeader(origin.get_unparse(), origin.get_prefix());
    header_len += ele_header_.header_size();
  }
  index_start_ = header_->start() + header_len;
  index_entry_size_ = header_->count_var_size_;
  char* start_ = header.buffer_->ptr() + index_start_;
  // offset start
  key_entry_start_ = index_start_ + children_count * index_entry_size_;
  key_entry_size_ = header_->get_entry_var_size();

  value_entry_start_ = children_count * (key_entry_size_ * 2) + key_entry_start_;
  value_entry_size_ = header_->get_entry_var_size();

  key_start_ = (value_entry_size_ + sizeof(uint8_t)) *children_count + value_entry_start_;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ctx.buffer_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (element_header) {
    if (OB_FAIL(ele_header_.serialize(*ctx.buffer_))) {
      LOG_WARN("fail to serialize element header", K(ret));
    }
  } else if (OB_FAIL(doc_header_.serialize(*ctx.buffer_))) {
    LOG_WARN("fail to serialize element header", K(ret));
  }
  return ret;
}

void ObXmlBinMergeMeta::set_index_entry(int64_t origin_index, int64_t sort_index)
{
  int64_t offset = index_start_ + origin_index * index_entry_size_;
  char* write_buf = header_->buffer_->ptr() + offset;
  ObMulModeVar::set_var(sort_index, header_->get_count_var_size_type(), write_buf);
}

void ObXmlBinMergeMeta::set_key_entry(int64_t entry_idx, int64_t key_offset, int64_t key_len)
{
  int64_t offset = key_entry_start_ + entry_idx * (key_entry_size_ * 2);
  char* write_buf = header_->buffer_->ptr() + offset;
  ObMulModeVar::set_var(key_offset, header_->get_entry_var_size_type(), write_buf);
  write_buf += key_entry_size_;
  ObMulModeVar::set_var(key_len, header_->get_entry_var_size_type(), write_buf);
}

void ObXmlBinMergeMeta::set_value_entry(int64_t entry_idx, uint8_t type, int64_t value_offset)
{
  int64_t offset = value_entry_start_ + entry_idx * (value_entry_size_ + sizeof(uint8_t));
  char* write_buf = header_->buffer_->ptr() + offset;
  *reinterpret_cast<uint8_t*>(write_buf) = type;
  ObMulModeVar::set_var(value_offset, header_->get_entry_var_size_type(), write_buf + sizeof(uint8_t));
}

void ObXmlBinMergeMeta::set_value_offset(int64_t entry_idx, int64_t value_offset)
{
  int64_t offset = value_entry_start_ + entry_idx * (value_entry_size_ + sizeof(uint8_t));
  char* write_buf = header_->buffer_->ptr() + offset;
  ObMulModeVar::set_var(value_offset, header_->get_entry_var_size_type(), write_buf + sizeof(uint8_t));
}

int ObXmlBinMerge::reserve_meta(ObMulBinHeaderSerializer& header)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(header.buffer_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    int64_t pos = header.buffer_->length();
    uint32_t reserve_size = merge_meta_.key_start_ - merge_meta_.index_start_;
    if (OB_FAIL(merge_meta_.header_->buffer_->reserve(reserve_size))) {
      LOG_WARN("failed to reserve buffer.", K(ret), K(reserve_size));
    } else {
      header.buffer_->set_length(pos + reserve_size);
    }
  }
  return ret;
}

int ObXmlBinMerge::append_key_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                            ObMulBinHeaderSerializer& header, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  if (ctx.only_merge_ns_) {
    // in this case, we don't need add ns definition
    if (OB_FAIL(merge_meta_.init_merge_meta(ctx, origin, header, false))) {
      LOG_WARN("fail to init element header", K(ret));
    } else if (OB_FAIL(reserve_meta(header))) {
      LOG_WARN("failed to reserve meta.", K(ret));
    } else {
      ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
      ObXmlBin* bin_res = static_cast<ObXmlBin*>(&res);
      int64_t children_count = origin.attribute_count() + origin.size();
      uint64_t key_start = (bin_origin->meta_.get_value_entry_size() + sizeof(uint8_t)) *children_count + bin_origin->meta_.value_entry_;
      uint64_t res_key_offset = merge_meta_.key_start_;
      int64_t value_start = 0;
      int64_t child_value_start = 0;
      uint64_t res_value_offset = 0;
      if (OB_FAIL(bin_origin->get_value_start(value_start))
       || OB_FAIL(bin_origin->get_child_value_start(child_value_start))) {
        LOG_WARN("failed get origin value start.", K(value_start));
      } else if (value_start < key_start || child_value_start < value_start) {
        // value_start == key_start: when there is no k-v child
        // child_value_start == value_start: when there is no attribute child
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed get total key len.", K(value_start));
      } else {
        res_value_offset = merge_meta_.key_start_ + (value_start - key_start);
      }

      // same size of count and entry, copy directly
      if (OB_FAIL(ret)) {
      } else if (bin_origin->meta_.key_entry_size_ == header.get_entry_var_size()
         && bin_origin->meta_.index_entry_size_ == header.get_count_var_size()) {
        MEMCPY(header.buffer_->ptr() + merge_meta_.index_start_,
              bin_origin->meta_.data_ + bin_origin->meta_.index_entry_,
              merge_meta_.key_start_ - merge_meta_.index_start_ + 1);
      } else {
        // set sorted index one by one
        for (int i = 0; i < bin_origin->meta_.count_ && OB_SUCC(ret); ++i) {
          int64_t index_content = 0;
          if (OB_FAIL(bin_origin->get_index_content(i, index_content))) {
            LOG_WARN("failed get sorted index .", K(value_start));
          } else {
            merge_meta_.set_index_entry(i, index_content);
          }
        }
      }

      // set info one by one
      for (int i = 0; i < bin_origin->meta_.count_ && OB_SUCC(ret); ++i) {
        int64_t key_len = 0;
        int64_t origin_key_offset = 0;
        uint8_t type = 0;
        int64_t value_offset = 0;
        int64_t value_len = 0;
        if (OB_FAIL(bin_origin->get_sorted_key_info(i, key_len, origin_key_offset))
          || OB_FAIL(bin_origin->get_value_info(i, type, value_offset, value_len))) {
          LOG_WARN("failed get origin info.", K(value_start));
        } else {
          merge_meta_.set_key_entry(i, res_key_offset - merge_meta_.header_start_, key_len);
          merge_meta_.set_value_entry(i, type, res_value_offset - merge_meta_.header_start_);
          res_key_offset += key_len;
          res_value_offset += value_len;
        }
      }

      // if don't append key, because only element value may need merge
      // so, copy attribute value directly
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(bin_res->buffer_.reserve(child_value_start - key_start))) {
        LOG_WARN("fail to reserve", K(ret));
      } else if (OB_FAIL(bin_res->buffer_.append(bin_origin->meta_.data_ + key_start,
                                                child_value_start - key_start))) {
        LOG_WARN("fail to append", K(ret));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supporte to merge other type yet", K(ret));
  }
  return ret;
}

int ObXmlBinMerge::collect_merge_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                                    ObMulBinHeaderSerializer& header, ObArray<ObBinMergeKeyInfo>& attr_vec)
{
  INIT_SUCC(ret);
  ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
  ObXmlBin* bin_patch = static_cast<ObXmlBin*>(&patch);
  int defined_ns_size = ctx.defined_ns_idx_.size();
  int64_t new_attr_size = bin_origin->attribute_size() + defined_ns_size;
  for (int i = 0; OB_SUCC(ret) && i < new_attr_size; ++i) {
    int64_t sorted_index = 0;
    int64_t key_offset = 0;
    int64_t key_len = 0;
    ObBinMergeKeyInfo merge_key_info;
    if (i < defined_ns_size) {
      int index = ctx.defined_ns_idx_.at(i);
      if (OB_FAIL(bin_patch->get_key_info(index, sorted_index, key_offset, key_len))) {
        LOG_WARN("failed to get key_info.", K(ret));
      } else if (OB_FALSE_IT(merge_key_info = ObBinMergeKeyInfo(bin_patch->meta_.get_data() + key_offset, key_len, sorted_index, i, false))) {
      } else if (OB_FAIL(attr_vec.push_back(merge_key_info))) {
        LOG_WARN("failed to record key_info.", K(ret));
      }
    } else if (OB_FAIL(bin_origin->get_key_info(i - defined_ns_size, sorted_index, key_offset, key_len))) {
      LOG_WARN("failed to get key_info.", K(ret));
    } else if (OB_FALSE_IT(merge_key_info = ObBinMergeKeyInfo(bin_origin->meta_.get_data() + key_offset, key_len, sorted_index, i, true))) {
    } else if (OB_FAIL(attr_vec.push_back(merge_key_info))) {
      LOG_WARN("failed to record key_info.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    do_sort(attr_vec);
  }
  return ret;
}

int ObXmlBinMerge::append_merge_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                                    ObMulBinHeaderSerializer& header, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  if (ctx.only_merge_ns_) {
    ObArray<ObBinMergeKeyInfo> attr_vec;
    if (OB_FAIL(merge_meta_.init_merge_meta(ctx, origin, header, true))) {
      LOG_WARN("fail to init element header", K(ret));
    } else if (OB_FAIL(reserve_meta(header))) {
      LOG_WARN("failed to reserve meta.", K(ret));
    } else if (OB_FAIL(collect_merge_key(ctx, origin, patch, header, attr_vec))) {
      LOG_WARN("failed to collect merge key.", K(ret));
    } else {
      ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
      ObXmlBin* bin_patch = static_cast<ObXmlBin*>(&patch);
      ObXmlBin* bin_res = static_cast<ObXmlBin*>(&res);
      uint64_t res_key_offset = merge_meta_.key_start_;
      int attr_size = attr_vec.size();
      int origin_attr_size = origin.attribute_size();
      int defined_ns_size = ctx.defined_ns_idx_.size();
      // set attribute key&index info by attr_vec
      for (int i = 0; i < attr_size && OB_SUCC(ret); ++i) {
        // set index
        merge_meta_.set_index_entry(attr_vec[i].text_index_, i);
        // set key_entry
        merge_meta_.set_key_entry(i, res_key_offset - merge_meta_.header_start_, attr_vec[i].key_len_);
        // set key
        if (OB_FAIL(bin_res->buffer_.reserve(attr_vec[i].key_len_))) {
          LOG_WARN("fail to reserve", K(ret));
        } else if (OB_FAIL(bin_res->buffer_.append(attr_vec[i].key_ptr_, attr_vec[i].key_len_))) {
          LOG_WARN("fail to append", K(ret));
        } else {
          res_key_offset += attr_vec[i].key_len_;
        }
      }
      // set element index info
      for (int i = 0; i < origin.size() && OB_SUCC(ret); ++i) {
        int64_t index_content = 0;
        if (OB_FAIL(bin_origin->get_index_content(origin_attr_size + i, index_content))) {
          LOG_WARN("failed get sorted index .", K(ret), K(i));
        } else {
          merge_meta_.set_index_entry(attr_size + i, index_content + defined_ns_size);
        }
      }
      // set element key info
      for (int i = 0; i < origin.size() && OB_SUCC(ret); ++i) {
        int64_t key_len = 0;
        int64_t origin_key_offset = 0;
        if (OB_FAIL(bin_origin->get_sorted_key_info(origin_attr_size + i, key_len, origin_key_offset))) {
          LOG_WARN("failed get origin info.", K(origin_attr_size + i));
        } else if (OB_FAIL(bin_res->buffer_.reserve(key_len))) {
          LOG_WARN("fail to reserve", K(ret));
        } else if (OB_FAIL(bin_res->buffer_.append(bin_origin->meta_.get_data() + origin_key_offset, key_len))) {
          LOG_WARN("fail to append", K(ret));
        } else {
          merge_meta_.set_key_entry(attr_size + i, res_key_offset - merge_meta_.header_start_, key_len);
          res_key_offset += key_len;
        }
      }

      // set value entry
      uint64_t res_value_offset = res_key_offset;
      for (int i = 0; i < header.count_ && OB_SUCC(ret); ++i) {
        uint8_t type = 0;
        int64_t value_len = 0;
        int64_t value_offset = 0;
        int64_t origin_sorted_idx = 0;
        // attribute : set type, value_offset and append value
        if (i < attr_size) {
          origin_sorted_idx = attr_vec[i].origin_index_;
          ObXmlBin* bin = attr_vec[i].is_origin_ ? bin_origin : bin_patch;
          if (OB_FAIL(bin->get_value_info(origin_sorted_idx, type, value_offset, value_len))) {
            LOG_WARN("failed get origin info.", K(i));
          } else if (OB_FAIL(bin_res->buffer_.reserve(value_len))) {
            LOG_WARN("fail to reserve", K(ret));
          } else if (OB_FAIL(bin_res->buffer_.append(bin->meta_.get_data() + value_offset, value_len))) {
            LOG_WARN("fail to append", K(ret));
          } else {
            merge_meta_.set_value_entry(i, type, res_value_offset - merge_meta_.header_start_);
            res_value_offset += value_len;
          }
        } else {
          // element: only set type
          origin_sorted_idx = i - defined_ns_size;
          if (OB_FAIL(bin_origin->get_value_info(origin_sorted_idx, type, value_offset, value_len))) {
            LOG_WARN("failed get origin info.", K(i));
          } else {
            merge_meta_.set_value_entry(i, type, res_value_offset - merge_meta_.header_start_);
            res_value_offset += value_len;
          }
        }
      }
      // append attribute value
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supporte to merge other type yet", K(ret));
  }
  return ret;
}

void ObXmlBinMerge::do_sort(ObArray<ObBinMergeKeyInfo>& attr_vec)
{
  ObBinMergeKeyCompare cmp;
  std::stable_sort(attr_vec.begin(), attr_vec.end(), cmp);
}

uint64_t ObXmlBinMerge::estimated_count(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch)
{
  return retry ? ctx.retry_count_ : origin.attribute_count() + origin.size() + patch.attribute_count();
}

uint64_t ObXmlBinMerge::estimated_length(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch)
{
  ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
  ObXmlBin* bin_patch = static_cast<ObXmlBin*>(&patch);
  return retry ? ctx.retry_len_ : ceil((bin_origin->meta_.len_ + bin_patch->meta_.len_) * 1.2);
}
int ObXmlBinMerge::set_value_offset(int idx, uint64_t offset, ObBinMergeCtx& ctx, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  if (idx < merge_meta_.attr_count_) {
  } else if (idx < merge_meta_.child_count_ + merge_meta_.attr_count_) {
    merge_meta_.set_value_offset(idx, offset);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error idx", K(ret));
  }
  return ret;
}
int ObXmlBinMerge::append_value_by_idx(bool is_origin, int index, ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                       ObIMulModeBase& patch, ObMulBinHeaderSerializer& header, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  if (ctx.only_merge_ns_) {
    ObXmlBin* bin_origin = static_cast<ObXmlBin*>(&origin);
    if (!is_origin) { // ns value, already appended
    } else if (index < bin_origin->meta_.count_) {
      ObXmlBin bin_buffer;
      ObIMulModeBase* cur = nullptr;
      ObXmlBinMerge bin_merge;
      cur = bin_origin->sorted_at(index, &bin_buffer);
      if (OB_ISNULL(cur)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("fail to get value", K(ret));
      } else if (cur->type() != M_ELEMENT) {
        if (OB_FAIL(append_value_without_merge(ctx, *cur, res))) {
          LOG_WARN("fail to append value", K(ret));
        }
      } else if (OB_FAIL(bin_merge.inner_merge(ctx, *cur, patch, res))) {
        LOG_WARN("fail to append value", K(ret));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supporte to merge other type yet", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
