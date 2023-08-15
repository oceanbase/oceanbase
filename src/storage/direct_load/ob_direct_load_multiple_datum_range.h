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
#pragma once

#include "storage/blocksstable/ob_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleDatumRange
{
public:
  ObDirectLoadMultipleDatumRange();
  ObDirectLoadMultipleDatumRange(const ObDirectLoadMultipleDatumRange &other) = delete;
  ~ObDirectLoadMultipleDatumRange();
  void reset();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadMultipleDatumRange &src, common::ObIAllocator &allocator);
  ObDirectLoadMultipleDatumRange &operator=(const ObDirectLoadMultipleDatumRange &other);
  int assign(const ObDirectLoadMultipleDatumRange &other);
  int assign(const common::ObTabletID &tablet_id, const blocksstable::ObDatumRange &range);
  OB_INLINE bool is_valid() const { return start_key_.is_valid() && end_key_.is_valid(); }
  void set_whole_range();
  int set_tablet_whole_range(const common::ObTabletID &tablet_id);
  OB_INLINE bool is_left_open() const { return !border_flag_.inclusive_start(); }
  OB_INLINE bool is_left_closed() const { return border_flag_.inclusive_start(); }
  OB_INLINE bool is_right_open() const { return !border_flag_.inclusive_end(); }
  OB_INLINE bool is_right_closed() const { return border_flag_.inclusive_end(); }
  OB_INLINE void set_left_open() { border_flag_.unset_inclusive_start(); }
  OB_INLINE void set_left_closed() { border_flag_.set_inclusive_start(); }
  OB_INLINE void set_right_open() { border_flag_.unset_inclusive_end(); }
  OB_INLINE void set_right_closed() { border_flag_.set_inclusive_end(); }
  TO_STRING_KV(K_(start_key), K_(end_key), K_(border_flag));
public:
  ObDirectLoadMultipleDatumRowkey start_key_;
  ObDirectLoadMultipleDatumRowkey end_key_;
  common::ObBorderFlag border_flag_;
};

} // namespace storage
} // namespace oceanbase
