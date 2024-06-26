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

#ifndef OCEANBASE_LIB_ROARINGBITMAP_OB_RB_BIN_
#define OCEANBASE_LIB_ROARINGBITMAP_OB_RB_BIN_

#include "ob_roaringbitmap.h"

namespace oceanbase {
namespace common {

class ObRoaringBin
{
public:
  ObRoaringBin(ObIAllocator *allocator, ObString &roaring_bin)
      : allocator_(allocator),
        roaring_bin_(roaring_bin),
        bin_length_(0),
        size_(0),
        hasrun_(false),
        bitmapOfRunContainers_(nullptr) {}
  virtual ~ObRoaringBin() = default;

  int init();

  int get_cardinality(uint64_t &cardinality);
  size_t get_bin_length() {return bin_length_;}
  int get_container_size(uint32_t n, size_t &container_size);

private:
  ObIAllocator* allocator_;
  ObString roaring_bin_;
  size_t bin_length_;
  int32_t size_; // container count
  uint16_t *keyscards_;
  uint32_t *offsets_;
  bool hasrun_;
  char *bitmapOfRunContainers_;

};

class ObRoaring64Bin
{
public:
  ObRoaring64Bin(ObIAllocator *allocator, ObString &roaring_bin)
      : allocator_(allocator),
        roaring_bin_(roaring_bin),
        roaring_bufs_(nullptr) {}
  virtual ~ObRoaring64Bin() = default;

  int init();
  int get_cardinality(uint64_t &cardinality);

private:
  ObIAllocator* allocator_;
  ObString roaring_bin_;
  uint64_t buckets_;
  uint32_t *high32_;
  uint32_t *offsets_;
  ObRoaringBin **roaring_bufs_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ROARINGBITMAP_OB_ROARINGBITMAP_