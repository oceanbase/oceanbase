/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/blocksstable/ob_sstable_skip_index.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

ObSSTableMetaSkipIndex::ObSSTableMetaSkipIndex() : buf_(nullptr), size_(0) {}

void ObSSTableMetaSkipIndex::reset()
{
  buf_ = nullptr;
  size_ = 0;
}

int ObSSTableMetaSkipIndex::init(const char *buf,
                                 const int64_t size,
                                 common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  if (size > 0 && nullptr != buf) {
    // Deep copy sstable level skip index row
    char *index_buf = nullptr;
    if (OB_ISNULL(index_buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for sstable skip index buf", K(ret), K(size));
    } else {
      MEMCPY(index_buf, buf, size);
      buf_ = index_buf;
      size_ = size;
    }
  }
  return ret;
}

int ObSSTableMetaSkipIndex::serialize(const uint64_t data_version,
                                      char *buf,
                                      const int64_t buf_len,
                                      int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (data_version >= share::ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2
      && OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, buf_, size_))) {
    LOG_WARN("fail to serialize sstable skip index", K(ret), K(buf_len), K(pos), K(size_));
  }
  return ret;
}

int ObSSTableMetaSkipIndex::deserialize(common::ObArenaAllocator &allocator,
                                        const char *buf,
                                        const int64_t data_len,
                                        int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len) {
    int64_t skip_index_size = 0;
    const char *skip_index_data = serialization::decode_vstr(buf, data_len, pos, &skip_index_size);
    if (skip_index_size < 0 || (skip_index_size > 0 && OB_ISNULL(skip_index_data))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to decode sstable skip index", K(ret), K(data_len), K(pos));
    } else if (skip_index_size > 0) {
      char *skip_index_buf = nullptr;
      if (OB_ISNULL(skip_index_buf = static_cast<char *>(allocator.alloc(skip_index_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for sstable skip index buf", K(ret), K(skip_index_size));
      } else {
        MEMCPY(skip_index_buf, skip_index_data, skip_index_size);
        buf_ = skip_index_buf;
        size_ = skip_index_size;
      }
    }
  }
  return ret;
}

int64_t ObSSTableMetaSkipIndex::get_serialize_size(const uint64_t data_version) const
{
  int64_t len = 0;
  if (data_version >= share::ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2) {
    len += serialization::encoded_length_vstr(size_);
  }
  return len;
}

int ObSSTableMetaSkipIndex::deep_copy(char *buf,
                                      const int64_t buf_len,
                                      int64_t &pos,
                                      ObSSTableMetaSkipIndex &dest) const
{
  int ret = OB_SUCCESS;
  dest.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (size_ > 0 && nullptr != buf_) {
    if (OB_UNLIKELY(pos + size_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN(
          "buf not enough for sstable skip index deep copy", K(ret), K(pos), K(buf_len), K(size_));
    } else {
      MEMCPY(buf + pos, buf_, size_);
      dest.buf_ = buf + pos;
      dest.size_ = size_;
      pos += size_;
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
