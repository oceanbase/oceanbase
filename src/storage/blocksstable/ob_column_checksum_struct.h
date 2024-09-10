//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_BLOCKSSTABLE_COLUMN_CHECKSUM_STRUCT_H_
#define OB_STORAGE_BLOCKSSTABLE_COLUMN_CHECKSUM_STRUCT_H_
#include "lib/container/ob_iarray.h"
namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
}
namespace blocksstable
{

struct ObColumnCkmStruct final
{
public:
  ObColumnCkmStruct()
    : column_checksums_(nullptr),
      count_(0)
  {}
  ~ObColumnCkmStruct() { reset(); }
  void reset()
  {
    count_ = 0;
    column_checksums_ = nullptr;
  }
  bool is_valid() const
  {
    return 0 == count_ || (count_ > 0 && NULL != column_checksums_);
  }
  bool is_empty() const
  {
    return 0 == count_;
  }
  int assign(common::ObArenaAllocator &allocator, const ObColumnCkmStruct &other);
  int reserve(common::ObArenaAllocator &allocator, const int64_t column_cnt);
  int assign(
    common::ObArenaAllocator &allocator,
    const common::ObIArray<int64_t> &column_checksums);
  int get_column_checksums(ObIArray<int64_t> &column_checksums) const;
  /* serialize need consider count */
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObArenaAllocator &allocator, const char *buf,
                  const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  /* only deep copy array */
  int64_t get_deep_copy_size() const;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObColumnCkmStruct &dest) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t *column_checksums_;
  int64_t count_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OB_STORAGE_BLOCKSSTABLE_COLUMN_CHECKSUM_STRUCT_H_
