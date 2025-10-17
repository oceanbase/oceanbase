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
        keyscards_(nullptr),
        offsets_(nullptr),
        hasrun_(false),
        run_bitmap_(nullptr),
        inited_(false) {}
  virtual ~ObRoaringBin() = default;

  int init();
  inline bool is_inited() {return inited_;}

  int get_cardinality(uint64_t &cardinality);
  int contains(uint32_t value, bool &is_contains);
  int calc_and_cardinality(ObRoaringBin *rb, uint64_t &cardinality);
  int calc_and(ObRoaringBin *rb, ObStringBuffer &res_buf, uint64_t &res_card, uint32_t high32);
  int calc_andnot(ObRoaringBin *rb, ObStringBuffer &res_buf, uint64_t &res_card, uint32_t high32);
  inline char *get_bin_ptr() { return roaring_bin_.ptr(); }
  inline size_t get_bin_length() { return bin_length_; }
  inline uint16_t get_key_at_index(uint16_t idx) { return keyscards_[idx * 2]; }
  inline int32_t get_card_at_index(uint16_t idx) { return keyscards_[idx * 2 + 1] + 1; }
  inline bool is_run_at_index(uint16_t idx) { return hasrun_ && (run_bitmap_[idx / 8] & (1 << (idx % 8))) != 0; }
  int get_container_size_at_index(uint16_t idx, size_t &container_size);
  int get_container_at_index(uint16_t idx, uint8_t &container_type, container_s *&container);
  inline int32_t key_advance_until(int32_t idx, uint16_t min);
private:
  ObIAllocator* allocator_;
  ObString roaring_bin_;
  size_t bin_length_;
  int32_t size_; // container count
  uint16_t *keyscards_;
  uint32_t *offsets_;
  bool hasrun_;
  char *run_bitmap_;
  bool inited_;

};

class ObRoaring64Bin
{
public:
  ObRoaring64Bin(ObIAllocator *allocator, ObString &roaring_bin)
      : allocator_(allocator),
        roaring_bin_(roaring_bin),
        buckets_(0),
        high32_(nullptr),
        roaring_bufs_(nullptr),
        inited_(false) {}
  virtual ~ObRoaring64Bin() = default;

  int init();
  inline bool is_inited() {return inited_;}

  int get_cardinality(uint64_t &cardinality);
  int contains(uint64_t value, bool &is_contains);
  int calc_and_cardinality(ObRoaring64Bin *rb, uint64_t &cardinality);
  int calc_and(ObRoaring64Bin *rb, ObStringBuffer &res_buf, uint64_t &res_card);
  int calc_andnot(ObRoaring64Bin *rb, ObStringBuffer &res_buf, uint64_t &res_card);
  inline uint64_t high32_advance_until(uint64_t idx, uint32_t min);

private:
  ObIAllocator* allocator_;
  ObString roaring_bin_;
  uint64_t buckets_;
  uint32_t *high32_;
  ObRoaringBin **roaring_bufs_;
  bool inited_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ROARINGBITMAP_OB_RB_BIN_