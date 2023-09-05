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
#define USING_LOG_PREFIX SHARE
#include "share/restore/ob_import_item_format_provider.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace share
{

// ObIImportItemFormatProvider
int ObIImportItemFormatProvider::format_serialize(common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  char *serialize_buf = NULL;
  int64_t serialize_pos = 0;
  int64_t serialize_size = get_format_serialize_size();
  if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(serialize_size));
  } else if (OB_FAIL(format_serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("fail to format serialize", K(ret));
  } else {
    str.assign_ptr(serialize_buf, serialize_size);
  }
  return ret;
}

// ObImportItemHexFormatImpl
int64_t ObImportItemHexFormatImpl::get_hex_format_serialize_size() const
{
  return 2 * get_serialize_size();
}

int ObImportItemHexFormatImpl::hex_format_serialize(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator;
  char *serialize_buf = NULL;
  int64_t serialize_pos = 0;
  int64_t serialize_size = get_serialize_size();
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(tmp_allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(serialize_size));
  } else if (OB_FAIL(serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("fail to serialize", K(ret));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, buf, buf_len, pos))) {
    LOG_WARN("fail to print hex", K(ret), K(serialize_pos), K(buf_len), K(pos));
  }

  return ret;
}


int ObImportItemHexFormatImpl::hex_format_deserialize(
    const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator;
  int64_t hex_size = data_len - pos;
  char *deserialize_buf = NULL;
  int64_t deserialize_size = hex_size / 2 + 1;
  int64_t deserialize_pos = 0;
  if ((NULL == buf) || (data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(data_len));
  } else if (hex_size <= 0) {
    // skip
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(tmp_allocator.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(buf, hex_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", K(ret), K(hex_size), K(deserialize_size));
  } else if (OB_FAIL(deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize", K(ret));
  }

  return ret;
}

}
}