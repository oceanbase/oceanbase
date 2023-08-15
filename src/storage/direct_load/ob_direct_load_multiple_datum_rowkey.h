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

#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/direct_load/ob_direct_load_datum.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleDatumRowkey
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadMultipleDatumRowkey();
  ObDirectLoadMultipleDatumRowkey(const ObDirectLoadMultipleDatumRowkey &other) = delete;
  ~ObDirectLoadMultipleDatumRowkey();
  void reset();
  void reuse();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObDirectLoadMultipleDatumRowkey &src, char *buf, const int64_t len,
                int64_t &pos);
  int deep_copy(const ObDirectLoadMultipleDatumRowkey &src, common::ObIAllocator &allocator);
  int assign(const common::ObTabletID &tablet_id, blocksstable::ObStorageDatum *datums,
             int64_t count);
  int get_rowkey(blocksstable::ObDatumRowkey &rowkey) const;
  int compare(const ObDirectLoadMultipleDatumRowkey &rhs,
              const blocksstable::ObStorageDatumUtils &datum_utils, int &cmp_ret) const;
  void set_min_rowkey() { tablet_id_ = 0; datum_array_.reset(); }
  void set_max_rowkey() { tablet_id_ = UINT64_MAX; datum_array_.reset(); }
  OB_INLINE bool is_min_rowkey() const { return tablet_id_.id() == 0; }
  OB_INLINE bool is_max_rowkey() const { return tablet_id_.id() == UINT64_MAX; }
  int set_tablet_min_rowkey(const common::ObTabletID &tablet_id);
  int set_tablet_max_rowkey(const common::ObTabletID &tablet_id);
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(datum_array));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadDatumArray datum_array_;
};

class ObDirectLoadMultipleDatumRowkeyCompare
{
public:
  ObDirectLoadMultipleDatumRowkeyCompare();
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  bool operator()(const ObDirectLoadMultipleDatumRowkey *lhs,
                  const ObDirectLoadMultipleDatumRowkey *rhs);
  int get_error_code() const { return result_code_; }
public:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  int result_code_;
};

} // namespace storage
} // namespace oceanbase
