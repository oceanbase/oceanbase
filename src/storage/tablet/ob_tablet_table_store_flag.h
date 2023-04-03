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

#ifndef OCEANBASE_STORAGE_TABLET_TABLET_TABLE_STORE_FLAG_H_
#define OCEANBASE_STORAGE_TABLET_TABLET_TABLE_STORE_FLAG_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{

class ObTabletTableStoreWithMajorFlag final
{
public:
  enum FLAG 
  {
    WITHOUT_MAJOR_SSTABLE = 0,
    WITH_MAJOR_SSTABLE = 1,
  };
};

class ObTabletTableStoreFlag final
{
public:
  ObTabletTableStoreFlag();
  ~ObTabletTableStoreFlag();
  void reset();
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
  bool with_major_sstable() const { return ObTabletTableStoreWithMajorFlag::WITH_MAJOR_SSTABLE == with_major_sstable_; }
  void set_with_major_sstable() { with_major_sstable_ = ObTabletTableStoreWithMajorFlag::WITH_MAJOR_SSTABLE; }
  void set_without_major_sstable() { with_major_sstable_ = ObTabletTableStoreWithMajorFlag::WITHOUT_MAJOR_SSTABLE; }
  TO_STRING_KV(K_(with_major_sstable));

public:
  static const uint64_t SF_BIT_WITH_MAJOR_SSTABLE = 1;
  static const uint64_t SF_BIT_RESERVED = 63;

private:
  union {
    int64_t status_;
    struct {
      ObTabletTableStoreWithMajorFlag::FLAG with_major_sstable_ : SF_BIT_WITH_MAJOR_SSTABLE;
      int64_t reserved_: SF_BIT_RESERVED;
    };
  };
};

} // end namespace storage
} // end namespace oceanbase

#endif