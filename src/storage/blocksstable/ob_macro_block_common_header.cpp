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

#define USING_LOG_PREFIX STORAGE
#include "ob_macro_block_common_header.h"
#include "share/ob_errno.h"
#include "share/ob_force_print_log.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {
ObMacroBlockCommonHeader::ObMacroBlockCommonHeader()
{
  reset();
}

void ObMacroBlockCommonHeader::reset()
{
  header_size_ = (int32_t)get_serialize_size();
  version_ = MACRO_BLOCK_COMMON_HEADER_VERSION;
  magic_ = MACRO_BLOCK_COMMON_HEADER_MAGIC;
  attr_ = 0;
  data_version_ = 0;
  reserved_ = 0;
}

int ObMacroBlockCommonHeader::build_serialized_header(char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(serialize(buf, len, pos))) {
    STORAGE_LOG(ERROR, "fail to serialize record header, ", K(ret), KP(buf), K(len), K(pos), K(*this));
  } else if (get_serialize_size() != pos) {
    ret = OB_SERIALIZE_ERROR;
    STORAGE_LOG(ERROR, "serialize size mismatch, ", K(ret), K(pos), K(*this));
  }
  return ret;
}

int ObMacroBlockCommonHeader::check_integrity()
{
  int ret = OB_SUCCESS;
  if (header_size_ != get_serialize_size() || version_ != MACRO_BLOCK_COMMON_HEADER_VERSION ||
      magic_ != MACRO_BLOCK_COMMON_HEADER_MAGIC) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid common header", K(ret), K(*this));
  }
  return ret;
}

bool ObMacroBlockCommonHeader::is_valid() const
{
  bool b_ret = header_size_ > 0 && version_ == MACRO_BLOCK_COMMON_HEADER_VERSION &&
               MACRO_BLOCK_COMMON_HEADER_MAGIC == magic_ && attr_ >= 0 && attr_ < MaxMacroType;
  if (b_ret) {
    if ((PartitionMeta == attr_ || MacroMeta == attr_) && previous_block_index_ < -1) {
      b_ret = false;
    }
  }
  return b_ret;
}

int ObMacroBlockCommonHeader::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len));
  } else if (pos + get_serialize_size() > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(ERROR, "data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "common header is invalid", K(ret), K(*this));
  } else {
    ObMacroBlockCommonHeader* common_header = reinterpret_cast<ObMacroBlockCommonHeader*>(buf + pos);
    common_header->header_size_ = header_size_;
    common_header->version_ = version_;
    common_header->magic_ = magic_;
    common_header->attr_ = attr_;
    common_header->data_version_ = data_version_;
    common_header->reserved_ = reserved_;
    pos += common_header->get_serialize_size();
  }
  return ret;
}

int ObMacroBlockCommonHeader::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObMacroBlockCommonHeader* ptr = reinterpret_cast<const ObMacroBlockCommonHeader*>(buf + pos);
    header_size_ = ptr->header_size_;
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    attr_ = ptr->attr_;
    data_version_ = ptr->data_version_;
    reserved_ = ptr->reserved_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      STORAGE_LOG(ERROR, "deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

int64_t ObMacroBlockCommonHeader::get_serialize_size(void) const
{
  return sizeof(ObMacroBlockCommonHeader);
}

int ObMacroBlockCommonHeader::get_attr_name(int32_t attr, common::ObString& attr_name)
{
  int ret = OB_SUCCESS;
  attr_name = common::ObString("");
  switch (attr) {
    case Free: {
      attr_name = common::ObString("Free");
      break;
    }
    case SSTableData: {
      attr_name = common::ObString("SSTableData");
      break;
    }
    case PartitionMeta: {
      attr_name = common::ObString("PartitionMeta");
      break;
    }
    case MacroMeta: {
      attr_name = common::ObString("MacroMeta");
      break;
    }
    case Reserved: {
      attr_name = common::ObString("Reserved");
      break;
    }
    case MacroBlockSecondIndex: {
      attr_name = common::ObString("MacroBlockSecondIndex");
      break;
    }
    case SortTempData: {
      attr_name = common::ObString("SortTempData");
      break;
    }
    case LobData: {
      attr_name = common::ObString("LobData");
      break;
    }
    case LobIndex: {
      attr_name = common::ObString("LobData");
      break;
    }
    case BloomFilterData: {
      attr_name = common::ObString("BloomFilterData");
      break;
    }
    default: {
      ret = OB_INVALID_MACRO_BLOCK_TYPE;
      attr_name = common::ObString("Not Valid Type");
      STORAGE_LOG(WARN, "not valid macro block type", K(attr));
    }
  }
  return ret;
}
}  // namespace blocksstable
}  // namespace oceanbase
