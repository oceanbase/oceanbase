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

#ifndef OCEANBASE_COMMON_OB_ROW_CHECKSUM_H_
#define OCEANBASE_COMMON_OB_ROW_CHECKSUM_H_

#include <utility>
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{

class ObRow;
struct ObRowChecksumValue
{
  typedef std::pair<uint64_t, uint64_t> ObColumnIdChecksum;


  ObRowChecksumValue() { reset(); }

  NEED_SERIALIZE_AND_DESERIALIZE;

  template <typename Allocator>
  int deep_copy(const ObRowChecksumValue &src, Allocator &allocator);

  void reset();
  void reset_checksum();

  void sort();

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int column_checksum2string(char *buf, const int64_t buf_len, int64_t &pos) const;
  template <typename Allocator>
  int string2column_checksum(Allocator &allocator, const char *str);

  uint64_t checksum_;
  // we do not store detail column checksum if %column_count_ is zero
  int64_t column_count_;
  ObColumnIdChecksum *column_checksum_array_;
};

template <typename Allocator>
int ObRowChecksumValue::deep_copy(const ObRowChecksumValue &src, Allocator &allocator)
{
  int ret = OB_SUCCESS;

  checksum_ = src.checksum_;
  column_count_ = src.column_count_;
  if (0 == column_count_) {
    column_checksum_array_ = NULL;
  } else {
    if (NULL == (column_checksum_array_ = reinterpret_cast<ObColumnIdChecksum *>(
        allocator.alloc(sizeof(ObColumnIdChecksum) * column_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "allocate memory failed", K(ret),
          "size", sizeof(ObColumnIdChecksum) * column_count_);
    } else {
      MEMCPY(column_checksum_array_, src.column_checksum_array_,
             sizeof(ObColumnIdChecksum) * column_count_);
    }
  }

  return ret;
}

template <typename Allocator>
int ObRowChecksumValue::string2column_checksum(Allocator &allocator, const char *str)
{
  // %str can be NULL
  int64_t count = (NULL != str && strlen(str) > 0) ? 1 : 0;
  const char *p = str;
  int ret = OB_SUCCESS;
  while (NULL != p && NULL != (p = strchr(p, ','))) {
    p++;
    count++;
  }
  column_count_ = count;
  if (count == 0) {
    column_checksum_array_ = NULL;
  } else if (NULL == (column_checksum_array_ = reinterpret_cast<ObColumnIdChecksum *>(
      allocator.alloc(sizeof(*column_checksum_array_) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "allocate memory failed", K(ret),
        "size", sizeof(*column_checksum_array_) * count);
  } else {
    p = str;
    char *end = NULL;
    for (int64_t i = 0; i < count; i++) {
      column_checksum_array_[i].first = strtoul(p, &end, 10);
      if (*end != ':') {
        ret = OB_INVALID_DATE_FORMAT;
        COMMON_LOG(WARN, "invalid column check string format", K(ret), K(str));
        break;
      }
      end++;
      p = end;
      column_checksum_array_[i].second = strtoul(p, &end, 10);
      if (*end != ',' && *end != '\0') {
        ret = OB_INVALID_DATE_FORMAT;
        COMMON_LOG(WARN, "invalid column check string format", K(ret), K(str));
        break;
      }
      end++;
      p = end;
    }

    if (OB_FAIL(ret)) {
      allocator.free(column_checksum_array_);
      column_checksum_array_ = NULL;
    }
  }

  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_ROW_CHECKSUM_H_
