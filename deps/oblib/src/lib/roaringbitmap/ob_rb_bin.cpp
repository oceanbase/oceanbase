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

#define USING_LOG_PREFIX LIB
#include "lib/ob_errno.h"
#include "lib/roaringbitmap/ob_rb_bin.h"

namespace oceanbase {
namespace common {

static const uint32_t ROARING_SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
static const uint32_t ROARING_SERIAL_COOKIE = 12347;
static const uint32_t ROARING_NO_OFFSET_THRESHOLD = 4;
static const uint32_t ROARING_DEFAULT_MAX_SIZE = 4096;
static const uint32_t ROARING_BITSET_CONTAINER_SIZE_IN_WORDS = (1 << 16) / 64;

int ObRoaringBin::init()
{
  int ret = OB_SUCCESS;
  size_t read_bytes = 0;
  char * buf = roaring_bin_.ptr();
  uint32_t cookie = 0;

  if (roaring_bin_ == nullptr || roaring_bin_.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret));
  }

  /* roaring_bitmap binary format
   * Only 2 cookies are accepted for the roaring binary: ROARING_SERIAL_COOKIE_NO_RUNCONTAINER and ROARING_SERIAL_COOKIE (start with 3A30 and 3B30)
   *
   * The 3A30 binary format as below:
   *  | cookie | size  | keyscards[0] | ... | keyscards[size - 1] | offset[0] | ... | offset[size - 1] | container_data[0] | ... | container_data[size - 1] |
   *  | 4Byte  | 4Byte |    8Byte     | ... |        8Byte        |   8Byte   | ... |      8Byte       |     not fixed     | ... |        not fixed         |
   *
   * This is the format of roaring_bitmap without run containers.
   * - size: number of containers in the bitmaps
   * - keyscards[i]: keys and cardinality for each container. The keys refer to the high 16 bits of the value.
   *                 The cardinality is the real cardinality - 1, because the maximum cardinality is 65536(2^16)
   * - offset[i]: offsets for each container, the offsets are relative to the beginning of the binary
   * - container_data[i]: for cardinality less than 4096, the data is stored as a uint16 array,
   *                      for else, the data stored as in a 2^16 bits (8192 Bytes) bitset.
   *
   * The 3B30 binary format as below:
   *  | cookie | size  |     run_bitmap      | keyscards[0] | ... | keyscards[size - 1] | offset[0] | ... | offset[size - 1] | container_data[0] | ... | container_data[size - 1] |
   *  | 2Byte  | 2Byte | (size + 7) / 8 Byte |    8Byte     | ... |        8Byte        |   8Byte   | ... |      8Byte       |     not fixed     | ... |        not fixed         |
   * This is the format of roaring_bitmap includes one or more run containers.
   * - size: number of containers in the bitmaps (real size - 1), no possible to have 0 container in 3B30.
   * - run_bitmap: to mark which container is the run container.
   * - keyscards: same as 3A30
   * - offset: exists only when container number greater or equal to 5 (size >= 4).
   * - keyscards: same as 3A30
   *
   * The binary format of run container as below:
   *  | n_runs | start_val[0] | length[0] | ... | start_val[n_runs - 1] | length[n_runs - 1] |
   *  | 2Bytes |    2Bytes    |   2Bytes  | ... |         2Bytes        |        2Bytes      |
   * - n_runs: number of start_val/length pairs
   *
   */

  // read cookies
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(read_bytes += sizeof(int32_t))) {
  } else if (read_bytes > roaring_bin_.length()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ran out of bytes while reading cookies", K(ret), K(read_bytes), K(roaring_bin_.length()));
  } else {
    cookie = *reinterpret_cast<const int32_t*>(buf);
    buf +=  sizeof(int32_t);
    if ((cookie & 0xFFFF) != ROARING_SERIAL_COOKIE && cookie != ROARING_SERIAL_COOKIE_NO_RUNCONTAINER) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid cookie from roaring binary", K(ret), K(cookie));
    } else if ((cookie & 0xFFFF) == ROARING_SERIAL_COOKIE) {
      size_ = (cookie >> 16) + 1;
    } else if (OB_FALSE_IT(read_bytes += sizeof(int32_t))){
    } else if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading second part of the cookie", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else {
      size_ = *reinterpret_cast<const int32_t*>(buf);
      buf += sizeof(int32_t);
    }
  }
  // check size (contaniner count)
  if (OB_FAIL(ret)) {
  } else if (size_ < 0 || size_ > UINT16_MAX + 1) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid size get from roaring binary", K(ret), K(size_));
  }
  // get run container bitmap
  if (OB_FAIL(ret)) {
  } else {
    hasrun_ = (cookie & 0xFFFF) == ROARING_SERIAL_COOKIE;
    if (hasrun_) {
      int32_t s = (size_ + 7) / 8;
      read_bytes += s;
      if(read_bytes > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading run bitmap", K(ret), K(read_bytes), K(roaring_bin_.length()));
      } else {
        bitmapOfRunContainers_ = buf;
        buf += s;
      }
    }
  }
  // get keyscards
  if (OB_FAIL(ret) || size_ == 0) {
  } else if (OB_FALSE_IT(read_bytes += size_ * 2 * sizeof(uint16_t))) {
  } else if (read_bytes > roaring_bin_.length()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ran out of bytes while reading keycards", K(ret), K(read_bytes), K(roaring_bin_.length()));
  } else if (OB_ISNULL(keyscards_ = static_cast<uint16_t *>(allocator_->alloc(size_ * 2 * sizeof(uint16_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for keyscards", K(ret), K(size_ * 2 * sizeof(uint16_t)));
  } else {
    MEMCPY(keyscards_, buf, size_ * 2 * sizeof(uint16_t));
    buf += size_ * 2 * sizeof(uint16_t);
  }
  // get offsets
  if (OB_FAIL(ret) || size_ == 0) {
  } else if ((!hasrun_) || (size_ >= ROARING_NO_OFFSET_THRESHOLD)) {
    // has offsets
    read_bytes += size_ * sizeof(uint32_t);
    if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading offsets", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else if ((uintptr_t)buf % sizeof(uint32_t) == 0) {
      // use buffer directly
      offsets_ = (uint32_t *)buf;
      buf += size_ * sizeof(uint32_t);
    } else if (OB_ISNULL(offsets_ = static_cast<uint32_t *>(allocator_->alloc(size_ * sizeof(uint32_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for offsets_", K(ret), K(size_ * sizeof(uint32_t)));
    } else {
      MEMCPY(offsets_, buf, size_ * sizeof(uint32_t));
      buf += size_ * sizeof(uint32_t);
    }
    // check binary length
    if (OB_FAIL(ret)) {
    } else {
      // the last container
      size_t offset = offsets_[size_ - 1];
      size_t container_size = 0;
      if (OB_FAIL(get_container_size(size_ - 1, container_size))) {
        LOG_WARN("failed to get container size", K(ret), K(size_));
      } else if (offset + container_size > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while checking the last container", K(ret), K(size_), K(offset), K(container_size), K(roaring_bin_.length()));
      } else {
        bin_length_ = offset + container_size;
      }
    }
  } else if (OB_ISNULL(offsets_ = static_cast<uint32_t *>(allocator_->alloc(size_ * sizeof(uint32_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for offsets", K(ret), K(size_ * sizeof(uint32_t)));
  } else {
    // Reading the containers to fill offsets
    for (int32_t k = 0; OB_SUCC(ret) && k < size_; ++k) {
      offsets_[k] = read_bytes;
      size_t container_size = 0;
      if (OB_FAIL(get_container_size(k, container_size))) {
        LOG_WARN("failed to get container size", K(ret), K(k));
      } else {
        read_bytes += container_size;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while filling offsets", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else {
      // set the bin_length
      bin_length_ = read_bytes;
    }
  }

  return ret;
}

int ObRoaringBin::get_cardinality(uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (size_ == 0) {
    // do nothing
  } else if (OB_ISNULL(keyscards_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not init", K(ret));
  } else {
    for (int i = 0; i < size_; ++i)
    {
      cardinality += keyscards_[2 * i + 1] + 1;
    }
  }
  return ret;
}

int ObRoaringBin::get_container_size(uint32_t n, size_t &container_size)
{
  int ret = OB_SUCCESS;
  uint32_t this_card = keyscards_[2 * n + 1] + 1;
  size_t offset = offsets_[n];
  bool is_bitmap = (this_card > ROARING_DEFAULT_MAX_SIZE);
  bool is_run = false;
  if (hasrun_ && (bitmapOfRunContainers_[n / 8] & (1 << (n % 8))) != 0) {
    is_bitmap = false;
    is_run = true;
  }
  if (is_bitmap) {
    // bitmap container
    container_size = ROARING_BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
  } else if (is_run) {
    // run container
    if (offset + sizeof(uint16_t) > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading a run container (header)", K(ret), K(offset), K(n), K(roaring_bin_.length()));
    } else {
      uint16_t n_runs = *reinterpret_cast<const uint16_t*>(roaring_bin_.ptr() + offset);
      container_size = sizeof(uint16_t) + n_runs * 2 * sizeof(uint16_t);
    }
  } else {
    // array container
    container_size = this_card * sizeof(uint16_t);
  }
  return ret;
}


int ObRoaring64Bin::init()
{
  int ret = OB_SUCCESS;
  size_t read_bytes = 0;
  char * buf = roaring_bin_.ptr();

  if (roaring_bin_ == nullptr || roaring_bin_.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret));
  }

  /* roaring64_bitmap binary format
   *  | buckets | high32[0] | roaring_bitmap[0] | ... | high32[buckets-1] | roaring_bitmap[buckets-1] |
   *  |  8Byte  |  4Byte    |     not fixed     | ... |       4Byte       |         not fixed         |
   *
   * A roaring64_bitmap contains several roaring_bitmap.
   * - buckets: number of roaring_bitmap contained in this roaring64_bitmap
   * - high32[i]: high 32 bits of the value, also used to distinguish the roaring_bitmap
   * - roaring_bitmap[i]: the format of roaring_bitmap tells above
   */

  // get buckets
  if (OB_FAIL(ret)) {
  } else {
    read_bytes += sizeof(buckets_);
    if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading buckets", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else {
      buckets_ = *reinterpret_cast<uint64_t*>(buf);
      buf += sizeof(buckets_);
      if (buckets_ > UINT32_MAX) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid buckets get from roaring binary", K(ret), K(buckets_));
      }
    }
  }
  // get roaring_buf
  if (OB_FAIL(ret) || buckets_ == 0) {
  } else if (OB_ISNULL(high32_ = static_cast<uint32_t *>(allocator_->alloc(buckets_ * sizeof(uint32_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for high32", K(ret), K(buckets_ * sizeof(uint32_t)));
  } else if (OB_ISNULL(roaring_bufs_ = static_cast<ObRoaringBin **>(allocator_->alloc(buckets_ * sizeof(ObRoaringBin *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for roaring_bufs", K(ret), K(buckets_ * sizeof(ObRoaringBin *)));
  } else if (OB_ISNULL(offsets_ = static_cast<uint32_t *>(allocator_->alloc(buckets_ * sizeof(uint32_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for offsets", K(ret), K(buckets_ * sizeof(uint32_t)));
  } else {
    for (uint64_t bucket = 0; OB_SUCC(ret) && bucket < buckets_; ++bucket) {
      ObString roaring_bin;
      // get high32
      read_bytes += sizeof(uint32_t);
      if (read_bytes > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading high32", K(ret), K(read_bytes), K(bucket), K(roaring_bin_.length()));
      } else {
        high32_[bucket] = *reinterpret_cast<uint32_t*>(buf);
        buf += sizeof(uint32_t);
        offsets_[bucket] =  read_bytes;
      }
      // get roaring_buf (32bits)
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(roaring_bin.assign_ptr(buf, roaring_bin_.length() - read_bytes))) {
      } else if (OB_ISNULL(roaring_bufs_[bucket] = OB_NEWx(ObRoaringBin, allocator_, allocator_, roaring_bin))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for roaring_bufs_", K(ret), K(bucket));
      } else if (OB_FAIL(roaring_bufs_[bucket]->init())) {
        LOG_WARN("failed to init roaring_buf", K(ret), K(bucket));
      } else {
        buf += roaring_bufs_[bucket]->get_bin_length();
        read_bytes += roaring_bufs_[bucket]->get_bin_length();
      }
    }
  }

  return ret;
}

int ObRoaring64Bin::get_cardinality(uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (buckets_ == 0) {
    // do nothing
  } else if (OB_ISNULL(roaring_bufs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not init", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < buckets_; ++i)
    {
      uint64_t this_card = 0;
      if (OB_FAIL(roaring_bufs_[i]->get_cardinality(this_card))) {
        LOG_WARN("fail to get cardinality from roaring_buf", K(ret), K(i), K(roaring_bufs_[i]));
      } else {
        cardinality += this_card;
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase