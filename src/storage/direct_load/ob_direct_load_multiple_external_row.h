// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/direct_load/ob_direct_load_datum.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleExternalRow
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadMultipleExternalRow();
  void reset();
  void reuse();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadMultipleExternalRow &src, char *buf, const int64_t len,
                int64_t &pos);
  int from_datums(blocksstable::ObStorageDatum *datums, int64_t column_count);
  int to_datums(blocksstable::ObStorageDatum *datums, int64_t column_count) const;
  OB_INLINE bool is_valid() const
  {
    return tablet_id_.is_valid() && buf_size_ > 0 && nullptr != buf_;
  }
  TO_STRING_KV(K_(tablet_id), K_(buf_size), KP_(buf));
public:
  common::ObArenaAllocator allocator_;
  common::ObTabletID tablet_id_;
  int64_t buf_size_;
  const char *buf_;
};

} // namespace storage
} // namespace oceanbase
