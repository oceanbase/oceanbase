/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_SKIP_INDEX_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_SKIP_INDEX_H

#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObSSTableMetaSkipIndex final
{
public:
  ObSSTableMetaSkipIndex();
  ~ObSSTableMetaSkipIndex() = default;
  void reset();
  int init(const char *buf, const int64_t size, common::ObArenaAllocator &allocator);
  int serialize(const uint64_t data_version, char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size(const uint64_t data_version) const;
  int64_t get_variable_size() const { return size_; }
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObSSTableMetaSkipIndex &dest) const;
  OB_INLINE void set(const char *buf, const int64_t size)
  {
    buf_ = buf;
    size_ = size;
  }
  OB_INLINE const char *get_buf() const { return buf_; }
  OB_INLINE int64_t get_size() const { return size_; }
  OB_INLINE bool has_data() const { return nullptr != buf_ && size_ > 0; }
  OB_INLINE bool is_valid() const { return (buf_ == nullptr) == (size_ == 0); }
  TO_STRING_KV(KP_(buf), K_(size));
private:
  const char *buf_;
  int64_t size_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_SKIP_INDEX_H */
