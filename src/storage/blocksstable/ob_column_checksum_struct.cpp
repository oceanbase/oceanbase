//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/ob_column_checksum_struct.h"
#include "lib/allocator/page_arena.h"
namespace oceanbase
{
namespace blocksstable
{

int ObColumnCkmStruct::assign(
    ObArenaAllocator &allocator,
    const ObIArray<int64_t> &column_checksums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != column_checksums_ || count_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column checksum isn't empty, cannot initialize twice", K(ret), KP_(column_checksums), K_(count));
  } else if (0 == (count_ = column_checksums.count())) {
    // do nothing
  } else if (OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate column checksum memory", K(ret), K_(count));
  } else {
    for (int64_t i = 0; i < count_; ++i) {
      column_checksums_[i] = column_checksums.at(i);
    }
  }
  return ret;
}

int ObColumnCkmStruct::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(column_checksums_, count_);
  return ret;
}

int ObColumnCkmStruct::deserialize(ObArenaAllocator &allocator, const char *buf,
                const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(count_);
  if (OB_FAIL(ret) || 0 == count_) {
  } else if (OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate column checksum memory", K(ret), K(count_));
  } else {
    OB_UNIS_DECODE_ARRAY(column_checksums_, count_);
  }
  return ret;
}

int64_t ObColumnCkmStruct::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(column_checksums_, count_);
  return len;
}

int64_t ObColumnCkmStruct::get_deep_copy_size() const
{
  return sizeof(int64_t) * count_;
}

int ObColumnCkmStruct::deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObColumnCkmStruct &dest) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size + pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size), K(pos));
  } else {
    dest.count_ = count_;
    if (count_ > 0) {
      dest.column_checksums_ = reinterpret_cast<int64_t *>(buf + pos);
      MEMCPY(dest.column_checksums_, column_checksums_, deep_copy_size);
      pos += deep_copy_size;
    } else {
      dest.column_checksums_ = nullptr;
    }
  }
  return ret;
}

int ObColumnCkmStruct::assign(
  ObArenaAllocator &allocator,
  const ObColumnCkmStruct &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    reset();
    if (!other.is_empty()) {
      const int64_t alloc_mem_size = other.get_deep_copy_size();
      if (OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(alloc_mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate column checksum memory", K(ret), K(other));
      } else {
        MEMCPY(column_checksums_, other.column_checksums_, alloc_mem_size);
        count_ = other.count_;
      }
    }
  }
  return ret;
}

int ObColumnCkmStruct::get_column_checksums(ObIArray<int64_t> &column_checksums) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current checksum struct is invalid", K(ret), KPC(this));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count_; ++idx) {
      if (OB_FAIL(column_checksums.push_back(column_checksums_[idx]))) {
        LOG_WARN("failed to push column checksum", K(ret), K(idx), K(column_checksums_[idx]));
      }
    }
  }
  return ret;
}

int ObColumnCkmStruct::reserve(
  ObArenaAllocator &allocator,
  const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("have alloc before, can't reserve twice", K(ret), KPC(this));
  } else {
    const int64_t alloc_mem_size = sizeof(int64_t) * column_cnt;
    if (OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(alloc_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate column checksum memory", K(ret), K(column_cnt));
    } else {
      MEMSET(column_checksums_, 0, alloc_mem_size);
      count_ = column_cnt;
    }
  }
  return ret;
}

int64_t ObColumnCkmStruct::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(count));
    if (count_ > 0) {
      J_COMMA();
    }
    for (int i = 0; i < count_; ++i) {
      if (count_ - 1 != i) {
        common::databuff_printf(buf, buf_len, pos, "%d:%ld,", i, column_checksums_[i]);
      } else {
        common::databuff_printf(buf, buf_len, pos, "%d:%ld", i, column_checksums_[i]);
      }
    }
    J_OBJ_END();
  }
  return pos;
}

} // namespace blocksstable
} // namespace oceanbase
