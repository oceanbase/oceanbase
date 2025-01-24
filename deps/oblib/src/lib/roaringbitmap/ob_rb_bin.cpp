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
#include "lib/roaringbitmap/ob_rb_bin.h"
#include "roaring/roaring_array.h"

namespace oceanbase {
namespace common {

int ObRoaringBin::init()
{
  int ret = OB_SUCCESS;
  size_t read_bytes = 0;
  char *buf = roaring_bin_.ptr();
  int32_t cookie = 0;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (roaring_bin_ == nullptr || roaring_bin_.empty()) {
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
    if ((cookie & 0xFFFF) != roaring::internal::SERIAL_COOKIE
         && cookie != roaring::internal::SERIAL_COOKIE_NO_RUNCONTAINER) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid cookie from roaring binary", K(ret), K(cookie));
    } else if ((cookie & 0xFFFF) == roaring::internal::SERIAL_COOKIE) {
      size_ = (cookie >> 16) + 1;
      hasrun_ = true;
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
  } else if (hasrun_) {
    int32_t run_bitmap_size = (size_ + 7) / 8;
    read_bytes += run_bitmap_size;
    if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading run bitmap", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else {
      run_bitmap_ = buf;
      buf += run_bitmap_size;
    }
  }
  // get keyscards
  if (OB_FAIL(ret) || size_ == 0) {
  } else if (OB_FALSE_IT(read_bytes += size_ * 2 * sizeof(uint16_t))) {
  } else if (read_bytes > roaring_bin_.length()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ran out of bytes while reading keycards", K(ret), K(read_bytes), K(roaring_bin_.length()));
  } else {
    keyscards_ = (uint16_t *)buf;
    buf += size_ * 2 * sizeof(uint16_t);
    if ((uintptr_t)keyscards_ % sizeof(uint16_t) != 0) {
      uint16_t * tmp_buf = nullptr;
      if (OB_ISNULL(tmp_buf = static_cast<uint16_t *>(allocator_->alloc(size_ * 2 * sizeof(uint16_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for tmpbuf", K(ret), K(size_ * 2 * sizeof(uint16_t)));
      } else {
        MEMCPY(tmp_buf, keyscards_, size_ * 2 * sizeof(uint16_t));
        keyscards_ = tmp_buf;
      }
    }
  }
  // get offsets
  if (OB_FAIL(ret) || size_ == 0) {
  } else if ((!hasrun_) || (size_ >= roaring::internal::NO_OFFSET_THRESHOLD)) {
    // has offsets
    read_bytes += size_ * sizeof(uint32_t);
    if (read_bytes > roaring_bin_.length()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading offsets", K(ret), K(read_bytes), K(roaring_bin_.length()));
    } else {
      offsets_ = (uint32_t *)buf;
      buf += size_ * sizeof(uint32_t);
      if ((uintptr_t)offsets_ % sizeof(uint32_t) != 0) {
        uint32_t * tmp_buf = nullptr;
        if (OB_ISNULL(tmp_buf = static_cast<uint32_t *>(allocator_->alloc(size_ * sizeof(uint32_t))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for tmpbuf", K(ret), K(size_ * sizeof(uint32_t)));
        } else {
          MEMCPY(tmp_buf, offsets_, size_ * sizeof(uint32_t));
          offsets_ = tmp_buf;
        }
      }
    }
    // check binary length
    if (OB_FAIL(ret)) {
    } else {
      // the last container
      size_t offset = offsets_[size_ - 1];
      size_t container_size = 0;
      if (OB_FAIL(get_container_size_at_index(size_ - 1, container_size))) {
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
      if (OB_FAIL(get_container_size_at_index(k, container_size))) {
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

  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ObRoaringBin::get_cardinality(uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (!this->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else if (size_ == 0) {
    // do nothing
  } else {
    for (int i = 0; i < size_; ++i)
    {
      cardinality += this->get_card_at_index(i);
    }
  }
  return ret;
}

int ObRoaringBin::contains(uint32_t value, bool &is_contains)
{
  int ret = OB_SUCCESS;
  is_contains = false;
  uint16_t key = static_cast<uint16_t>(value >> 16);
  uint16_t lowvalue = static_cast<uint16_t>(value & 0xFFFF);
  int32_t idx = 0;
  if (!this->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else if (OB_FALSE_IT(idx = this->key_advance_until(-1, key))){
  } else if (key == this->get_key_at_index(idx)) {
    uint8_t container_type = 0;
    roaring::api::container_s *container = nullptr;
    if (OB_FAIL(this->get_container_at_index(idx, container_type, container))) {
      LOG_WARN("failed to get container at index", K(ret), K(idx));
    } else {
      is_contains = roaring::internal::container_contains(container, lowvalue, container_type);
    }
    if (OB_NOT_NULL(container)) {
      roaring::internal::container_free(container, container_type);
    }
  }
  return ret;
}

int ObRoaringBin::calc_and_cardinality(ObRoaringBin *rb, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else {
    int32_t l_idx = 0;
    int32_t r_idx = 0;
    while (OB_SUCC(ret) && l_idx < size_ && r_idx < rb->size_) {
      uint16_t l_key = this->get_key_at_index(l_idx);
      uint16_t r_key = rb->get_key_at_index(r_idx);
      if (l_key < r_key) {
        l_idx = this->key_advance_until(l_idx, r_key);
      } else if (l_key > r_key) {
        r_idx = rb->key_advance_until(r_idx, l_key);
      } else {
        // l_key == r_key
        int container_card = 0;
        uint8_t l_container_type = 0;
        uint8_t r_container_type = 0;
        roaring::api::container_s *l_container = nullptr;
        roaring::api::container_s *r_container = nullptr;
        if (OB_FAIL(this->get_container_at_index(l_idx, l_container_type, l_container))) {
          LOG_WARN("failed to get container at index from left ObRoaringBin", K(ret), K(l_idx));
        } else if (OB_FAIL(rb->get_container_at_index(r_idx, r_container_type, r_container))) {
          LOG_WARN("failed to get container at index from right ObRoaringBin", K(ret), K(r_idx));
        } else {
          container_card = roaring::internal::container_and_cardinality(l_container, l_container_type, r_container, r_container_type);
          cardinality += container_card;
        }
        l_idx++;
        r_idx++;
        if (OB_NOT_NULL(l_container)) {
          roaring::internal::container_free(l_container, l_container_type);
        }
        if (OB_NOT_NULL(r_container)) {
          roaring::internal::container_free(r_container, r_container_type);
        }
      }
    } // end while
  }
  return ret;
}

int ObRoaringBin::calc_and(ObRoaringBin *rb, ObStringBuffer &res_buf, uint64_t &res_card, uint32_t high32)
{
  int ret = OB_SUCCESS;
  int32_t need_size = 0;
  roaring_bitmap_t *res_bitmap = nullptr;
  res_card = 0;
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else if (OB_FALSE_IT(need_size = this->size_ > rb->size_ ? this->size_ : rb->size_)) {
  } else {
    ROARING_TRY_CATCH(res_bitmap = roaring::api::roaring_bitmap_create_with_capacity(need_size));
    if (OB_SUCC(ret) && OB_ISNULL(res_bitmap)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create roaring_bitmap", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    int32_t l_idx = 0;
    int32_t r_idx = 0;
    while (OB_SUCC(ret) && l_idx < size_ && r_idx < rb->size_) {
      uint16_t l_key = this->get_key_at_index(l_idx);
      uint16_t r_key = rb->get_key_at_index(r_idx);
      if (l_key < r_key) {
        l_idx = this->key_advance_until(l_idx, r_key);
      } else if (l_key > r_key) {
        r_idx = rb->key_advance_until(r_idx, l_key);
      } else {
        // l_key == r_key
        uint8_t l_container_type = 0;
        uint8_t r_container_type = 0;
        uint8_t res_container_type = 0;
        roaring::api::container_s *l_container = nullptr;
        roaring::api::container_s *r_container = nullptr;
        roaring::api::container_s *res_container = nullptr;
        int res_container_card = 0;
        if (OB_FAIL(this->get_container_at_index(l_idx, l_container_type, l_container))) {
          LOG_WARN("failed to get container at index from left ObRoaringBin", K(ret), K(l_idx));
        } else if (OB_FAIL(rb->get_container_at_index(r_idx, r_container_type, r_container))) {
          LOG_WARN("failed to get container at index from right ObRoaringBin", K(ret), K(r_idx));
        } else {
          ROARING_TRY_CATCH(res_container = roaring::internal::container_and(l_container, l_container_type, r_container, r_container_type, &res_container_type));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(res_container)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to calculate container and", K(ret), K(l_container), K(l_container_type), K(r_container_type), K(r_container));
        } else if (OB_FALSE_IT(res_container_card = roaring::internal::container_get_cardinality(res_container, res_container_type))) {
        } else if (res_container_card > 0) {
          ROARING_TRY_CATCH(roaring::internal::ra_append(&res_bitmap->high_low_container, l_key, res_container, res_container_type));
          if (OB_SUCC(ret)) {
            res_card += res_container_card;
          }
        } else {
          roaring::internal::container_free(res_container, res_container_type);
        }
        l_idx++;
        r_idx++;
        if (OB_NOT_NULL(l_container)) {
          roaring::internal::container_free(l_container, l_container_type);
        }
        if (OB_NOT_NULL(r_container)) {
          roaring::internal::container_free(r_container, r_container_type);
        }
      }
    } // end while
    // append high32 and serialized roaring_bitmap to res_buf
    if (OB_SUCC(ret) && res_card > 0) {
      uint64_t serial_size = 0;
      uint64_t real_serial_size = 0;
      ROARING_TRY_CATCH(serial_size = static_cast<uint64_t>(roaring::api::roaring_bitmap_portable_size_in_bytes(res_bitmap)));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_buf.reserve(sizeof(uint32_t) + serial_size))) {
        LOG_WARN("failed to reserve buffer", K(ret), K(serial_size));
      } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&high32), sizeof(uint32_t)))) {
        LOG_WARN("fail to append high32", K(ret), K(high32));
      } else {
        ROARING_TRY_CATCH(real_serial_size = roaring::api::roaring_bitmap_portable_serialize(res_bitmap, res_buf.ptr() + res_buf.length()));
        if (OB_FAIL(ret)) {
        } else if (serial_size != real_serial_size) {
          ret = OB_SERIALIZE_ERROR;
          LOG_WARN("serialize size not match", K(ret), K(serial_size));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_buf.set_length(res_buf.length() + serial_size))) {
        LOG_WARN("failed to set buffer length", K(ret));
      }
    }
  }
  if (OB_NOT_NULL(res_bitmap)) {
    roaring::api::roaring_bitmap_free(res_bitmap);
  }
  return ret;
}

int ObRoaringBin::calc_andnot(ObRoaringBin *rb, ObStringBuffer &res_buf, uint64_t &res_card, uint32_t high32)
{
  int ret = OB_SUCCESS;
  roaring_bitmap_t *res_bitmap = nullptr;
  res_card = 0;
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else {
    ROARING_TRY_CATCH(res_bitmap = roaring::api::roaring_bitmap_create_with_capacity(this->size_));
    if (OB_SUCC(ret) && OB_ISNULL(res_bitmap)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create roaring_bitmap", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    int32_t l_idx = 0;
    int32_t r_idx = 0;
    while (OB_SUCC(ret) && l_idx < size_ && r_idx < rb->size_) {
      uint16_t l_key = this->get_key_at_index(l_idx);
      uint16_t r_key = rb->get_key_at_index(r_idx);
      if (l_key == r_key) {
        uint8_t l_container_type = 0;
        uint8_t r_container_type = 0;
        uint8_t res_container_type = 0;
        roaring::api::container_s *l_container = nullptr;
        roaring::api::container_s *r_container = nullptr;
        roaring::api::container_s *res_container = nullptr;
        int res_container_card = 0;
        if (OB_FAIL(this->get_container_at_index(l_idx, l_container_type, l_container))) {
          LOG_WARN("failed to get container at index from left ObRoaringBin", K(ret), K(l_idx));
        } else if (OB_FAIL(rb->get_container_at_index(r_idx, r_container_type, r_container))) {
          LOG_WARN("failed to get container at index from right ObRoaringBin", K(ret), K(r_idx));
        } else {
          ROARING_TRY_CATCH(res_container = roaring::internal::container_andnot(l_container, l_container_type, r_container, r_container_type, &res_container_type));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(res_container)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to calculate container andnot", K(ret), K(l_container), K(l_container_type), K(r_container_type), K(r_container));
        } else if (OB_FALSE_IT(res_container_card = roaring::internal::container_get_cardinality(res_container, res_container_type))) {
        } else if (res_container_card > 0) {
          ROARING_TRY_CATCH(roaring::internal::ra_append(&res_bitmap->high_low_container, l_key, res_container, res_container_type));
          if (OB_SUCC(ret)) {
            res_card += res_container_card;
          }
        } else {
          roaring::internal::container_free(res_container, res_container_type);
        }
        l_idx++;
        r_idx++;
        if (OB_NOT_NULL(l_container)) {
          roaring::internal::container_free(l_container, l_container_type);
        }
        if (OB_NOT_NULL(r_container)) {
          roaring::internal::container_free(r_container, r_container_type);
        }
      } else {
        uint8_t res_container_type = 0;
        roaring::api::container_s *res_container = nullptr;
        int res_container_card = 0;
        if (OB_FAIL(this->get_container_at_index(l_idx, res_container_type, res_container))) {
          LOG_WARN("failed to get container at index from left ObRoaringBin", K(ret), K(l_idx));
        } else if (OB_FALSE_IT(res_container_card = roaring::internal::container_get_cardinality(res_container, res_container_type))) {
        } else if (res_container_card > 0) {
          ROARING_TRY_CATCH(roaring::internal::ra_append(&res_bitmap->high_low_container, l_key, res_container, res_container_type));
          if (OB_SUCC(ret)) {
            res_card += res_container_card;
          }
        } else if (OB_NOT_NULL(res_container)) {
          roaring::internal::container_free(res_container, res_container_type);
        }
        l_idx++;
      }
    } // end while
    while (OB_SUCC(ret) && l_idx < size_) {
      uint16_t l_key = this->get_key_at_index(l_idx);
      uint8_t res_container_type = 0;
      roaring::api::container_s *res_container = nullptr;
      int res_container_card = 0;
      if (OB_FAIL(this->get_container_at_index(l_idx, res_container_type, res_container))) {
        LOG_WARN("failed to get container at index from left ObRoaringBin", K(ret), K(l_idx));
      } else if (OB_FALSE_IT(res_container_card = roaring::internal::container_get_cardinality(res_container, res_container_type))) {
      } else if (res_container_card > 0) {
        ROARING_TRY_CATCH(roaring::internal::ra_append(&res_bitmap->high_low_container, l_key, res_container, res_container_type));
        if (OB_SUCC(ret)) {
          res_card += res_container_card;
        }
      } else if (OB_NOT_NULL(res_container)) {
        roaring::internal::container_free(res_container, res_container_type);
      }
      l_idx++;
    } // end while
    // append high32 and serialized roaring_bitmap to res_buf
    if (OB_SUCC(ret) && res_card > 0) {
      uint64_t serial_size = 0;
      uint64_t real_serial_size = 0;
      ROARING_TRY_CATCH(serial_size = static_cast<uint64_t>(roaring::api::roaring_bitmap_portable_size_in_bytes(res_bitmap)));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_buf.reserve(sizeof(uint32_t) + serial_size))) {
        LOG_WARN("failed to reserve buffer", K(ret), K(serial_size));
      } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&high32), sizeof(uint32_t)))) {
        LOG_WARN("fail to append high32", K(ret), K(high32));
      } else {
        ROARING_TRY_CATCH(real_serial_size = roaring::api::roaring_bitmap_portable_serialize(res_bitmap, res_buf.ptr() + res_buf.length()));
        if (OB_FAIL(ret)) {
        } else if (serial_size != real_serial_size) {
          ret = OB_SERIALIZE_ERROR;
          LOG_WARN("serialize size not match", K(ret), K(serial_size));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_buf.set_length(res_buf.length() + serial_size))) {
        LOG_WARN("failed to set buffer length", K(ret));
      }
    }
  }
  if (OB_NOT_NULL(res_bitmap)) {
    roaring::api::roaring_bitmap_free(res_bitmap);
  }
  return ret;
}

int ObRoaringBin::get_container_size_at_index(uint16_t idx, size_t &container_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(keyscards_) || OB_ISNULL(offsets_)
      || (hasrun_ && OB_ISNULL(run_bitmap_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else if (idx >= size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(size_));
  } else {
    int32_t this_card = this->get_card_at_index(idx);
    size_t offset = offsets_[idx];
    bool is_bitmap = (this_card > roaring::internal::DEFAULT_MAX_SIZE);
    bool is_run = false;
    if (is_run_at_index(idx)) {
      is_bitmap = false;
      is_run = true;
    }
    if (is_bitmap) {
      // bitmap container
      container_size = roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
    } else if (is_run) {
      // run container
      if (offset + sizeof(uint16_t) > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading a run container (header)", K(ret), K(offset), K(idx), K(roaring_bin_.length()));
      } else {
        uint16_t n_runs = *reinterpret_cast<const uint16_t*>(roaring_bin_.ptr() + offset);
        container_size = sizeof(uint16_t) + n_runs * 2 * sizeof(uint16_t);
      }
    } else {
      // array container
      container_size = this_card * sizeof(uint16_t);
    }
  }
  return ret;
}

int ObRoaringBin::get_container_at_index(uint16_t idx, uint8_t &container_type, container_s *&container)
{
  int ret = OB_SUCCESS;
  if (!this->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaringBin is not inited", K(ret));
  } else if (idx >= size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(size_));
  } else {
    size_t read_bytes = offsets_[idx];
    char *buf = roaring_bin_.ptr() + offsets_[idx];
    int32_t container_card = this->get_card_at_index(idx);
    bool is_bitmap = (container_card > roaring::internal::DEFAULT_MAX_SIZE);
    bool is_run = false;
    if (is_run_at_index(idx)) {
      is_bitmap = false;
      is_run = true;
    }
    if (is_bitmap) {
      // bitmap container
      size_t container_size = roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
      read_bytes += container_size;
      if (read_bytes > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading a bitmap container", K(ret), K(read_bytes), K(idx), K(container_size), K(offsets_[idx]), K(roaring_bin_.length()));
      } else {
        roaring::internal::bitset_container_t * container_ptr = nullptr;
        ROARING_TRY_CATCH(container_ptr = roaring::internal::bitset_container_create());
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(container_ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for container", K(ret));
        } else {
          roaring::internal::bitset_container_read(container_card, container_ptr, buf);
          container = container_ptr;
          container_type = BITSET_CONTAINER_TYPE;
        }
      }
    } else if (is_run) {
      // run container
      read_bytes += sizeof(uint16_t);
      if(read_bytes > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading a run container (header)", K(ret), K(read_bytes), K(idx), K(roaring_bin_.length()));
      } else {
        uint16_t n_runs = *reinterpret_cast<const uint16_t*>(buf);
        size_t container_size = n_runs * sizeof(roaring::internal::rle16_t);
		    read_bytes += container_size;
        if (read_bytes > roaring_bin_.length()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("ran out of bytes while reading a run container", K(ret), K(read_bytes), K(idx), K(container_size), K(offsets_[idx]), K(roaring_bin_.length()));
        } else {
          roaring::internal::run_container_t * container_ptr = nullptr;
          ROARING_TRY_CATCH(container_ptr = roaring::internal::run_container_create());
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(container_ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for container", K(ret));
          } else {
            roaring::internal::run_container_read(container_card, container_ptr, buf);
            container = container_ptr;
            container_type = RUN_CONTAINER_TYPE;
          }
        }
      }
    } else {
      // array container
      size_t container_size = container_card * sizeof(uint16_t);
      read_bytes += container_size;
      if (read_bytes > roaring_bin_.length()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ran out of bytes while reading an array container", K(ret), K(read_bytes), K(idx), K(roaring_bin_.length()));
      } else {
        roaring::internal::array_container_t * container_ptr = nullptr;
        ROARING_TRY_CATCH(container_ptr = roaring::internal::array_container_create());
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(container_ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for container", K(ret));
        } else {
          roaring::internal::array_container_read(container_card, container_ptr, buf);
          container = container_ptr;
          container_type = ARRAY_CONTAINER_TYPE;
        }
      }
    }
  }
  return ret;
}

int32_t ObRoaringBin::key_advance_until(int32_t idx, uint16_t min)
{
  int32_t res_idx = 0;
  int32_t lower = idx + 1;
  if ((lower >= size_) || (this->get_card_at_index(lower) >= min)) {
    res_idx = lower;
  } else {
    int32_t spansize = 1;
    while ((lower + spansize < size_) && (this->get_card_at_index(lower + spansize) < min)) {
      spansize *= 2;
    }
    int32_t upper = (lower + spansize < size_) ? lower + spansize : size_ - 1;
    if (this->get_card_at_index(upper) == min) {
      res_idx = upper;
    } else if (this->get_card_at_index(upper) < min) {
      // means keyscards_ has no item >= min
      res_idx = size_;
    } else {
      lower += (spansize / 2);
      int32_t mid = 0;
      while (lower + 1 != upper) {
        mid = (lower + upper) / 2;
        if (this->get_card_at_index(mid) == min) {
          return mid;
        } else if (this->get_card_at_index(mid) < min) {
          lower = mid;
        } else {
          upper = mid;
        }
      }
      res_idx = upper;
    }
  }
  return res_idx;
}

int ObRoaring64Bin::init()
{
  int ret = OB_SUCCESS;
  size_t read_bytes = 0;
  char * buf = roaring_bin_.ptr();

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (roaring_bin_ == nullptr || roaring_bin_.empty()) {
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

  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ObRoaring64Bin::get_cardinality(uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (!this->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaring64Bin is not inited", K(ret));
  } else if (buckets_ == 0) {
    // do nothing
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

int ObRoaring64Bin::contains(uint64_t value, bool &is_contains)
{
  int ret = OB_SUCCESS;
  is_contains = false;
  uint32_t high32 = static_cast<uint32_t>(value >> 32);
  uint32_t low32 = static_cast<uint32_t>(value & 0xFFFFFFFF);
  if (!this->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaring64Bin is not inited", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < buckets_ && high32 <= high32_[i]; ++i) {
      if (high32 == high32_[i] && OB_FAIL(roaring_bufs_[i]->contains(low32, is_contains))) {
        LOG_WARN("fail to check value is_contains in ObRoaringBin", K(ret), K(i), K(low32));
      }
    }
  }
  return ret;
}

int ObRoaring64Bin::calc_and_cardinality(ObRoaring64Bin *rb, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaring64Bin is not inited", K(ret));
  } else {
    uint64_t l_idx = 0;
    uint64_t r_idx = 0;
    while(OB_SUCC(ret) && l_idx < buckets_ && r_idx < rb->buckets_) {
      uint32_t l_high32 = high32_[l_idx];
      uint32_t r_high32 = rb->high32_[r_idx];
      if (l_high32 < r_high32) {
        l_idx++;
      } else if (l_high32 > r_high32){
        r_idx++;
      } else {
        // l_high32 == r_high32
        uint64_t rb32_card = 0;
        if (OB_FAIL(roaring_bufs_[l_idx]->calc_and_cardinality(rb->roaring_bufs_[r_idx], rb32_card))) {
          LOG_WARN("fail to calc and cardinality", K(ret), K(l_idx), K(r_idx));
        } else {
          cardinality += rb32_card;
          l_idx++;
          r_idx++;
        }
      }
    } // end while
  }
  return ret;
}

int ObRoaring64Bin::calc_and(ObRoaring64Bin *rb, ObStringBuffer &res_buf, uint64_t &res_card)
{
  int ret = OB_SUCCESS;
  res_card = 0;
  uint64_t buckets = 0;
  uint64_t buckets_offset = res_buf.length();
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaring64Bin is not inited", K(ret));
  } else if (OB_FAIL(res_buf.reserve(roaring_bin_.length() > rb->roaring_bin_.length() ? roaring_bin_.length() : rb->roaring_bin_.length()))) {
    LOG_WARN("failed to reserve buffer", K(ret), K(roaring_bin_.length()), K(rb->roaring_bin_.length()));
  } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&buckets), sizeof(uint64_t)))) {
    LOG_WARN("fail to append buckets");
  } else {
    uint64_t l_idx = 0;
    uint64_t r_idx = 0;
    while(OB_SUCC(ret) && l_idx < buckets_ && r_idx < rb->buckets_) {
      uint32_t l_high32 = high32_[l_idx];
      uint32_t r_high32 = rb->high32_[r_idx];
      if (l_high32 < r_high32) {
        l_idx = this->high32_advance_until(l_idx, r_high32);
      } else if (l_high32 > r_high32){
        r_idx = rb->high32_advance_until(r_idx, l_high32);
      } else {
        // l_high32 == r_high32
        uint64_t rb32_card = 0;
        ObString rb32_bin = nullptr;
        if (OB_FAIL(roaring_bufs_[l_idx]->calc_and(rb->roaring_bufs_[r_idx], res_buf ,rb32_card, l_high32))) {
          LOG_WARN("fail to calculate ObRoaringBin andnot", K(ret), K(l_idx), K(r_idx));
        } else if (rb32_card > 0) {
          res_card += rb32_card;
          buckets++;
        }
        l_idx++;
        r_idx++;
      }
    } // end while
  }
  // modify buckets in res_buf
  if (OB_SUCC(ret) && buckets > 0) {
    uint64_t *buckets_ptr = reinterpret_cast<uint64_t*>(res_buf.ptr() + buckets_offset);
    *buckets_ptr = buckets;
  }
  return ret;
}

int ObRoaring64Bin::calc_andnot(ObRoaring64Bin *rb, ObStringBuffer &res_buf, uint64_t &res_card)
{
  int ret = OB_SUCCESS;
  uint64_t buckets = 0;
  uint64_t buckets_offset = res_buf.length();
  if (!this->is_inited() || !rb->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoaring64Bin is not inited", K(ret));
  } else if (OB_FAIL(res_buf.reserve(roaring_bin_.length()))) {
    LOG_WARN("failed to reserve buffer", K(ret), K(roaring_bin_.length()));
  } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&buckets), sizeof(uint64_t)))) {
    LOG_WARN("fail to append buckets");
  } else {
    uint64_t l_idx = 0;
    uint64_t r_idx = 0;
    while(OB_SUCC(ret) && l_idx < buckets_ && r_idx < rb->buckets_) {
      uint32_t l_high32 = high32_[l_idx];
      uint32_t r_high32 = rb->high32_[r_idx];
      if (l_high32 == r_high32) {
        uint64_t rb32_card = 0;
        ObString rb32_bin = nullptr;
        if (OB_FAIL(roaring_bufs_[l_idx]->calc_andnot(rb->roaring_bufs_[r_idx], res_buf ,rb32_card, l_high32))) {
          LOG_WARN("fail to calculate ObRoaringBin andnot", K(ret), K(l_idx), K(r_idx));
        } else if (rb32_card > 0) {
          res_card += rb32_card;
          buckets++;
        }
        l_idx++;
        r_idx++;
      } else {
        if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&l_high32), sizeof(uint32_t)))) {
          LOG_WARN("fail to append high32", K(ret), K(l_idx), K(l_high32));
        } else if (OB_FAIL(res_buf.append(roaring_bufs_[l_idx]->get_bin()))) {
          LOG_WARN("fail to append roaring_bin", K(ret), K(l_idx));
        }
        l_idx++;
        buckets++;
      }
    } // end while
    while(OB_SUCC(ret) && l_idx < buckets_) {
      uint32_t l_high32 = high32_[l_idx];
      if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&l_high32), sizeof(uint32_t)))) {
        LOG_WARN("fail to append high32", K(ret), K(l_idx), K(l_high32));
      } else if (OB_FAIL(res_buf.append(roaring_bufs_[l_idx]->get_bin()))) {
        LOG_WARN("fail to append roaring _bin", K(ret), K(l_idx));
      }
      l_idx++;
      buckets++;
    } // end while
  }
  // modify buckets in res_buf
  if (OB_SUCC(ret) && buckets > 0) {
    uint64_t *buckets_ptr = reinterpret_cast<uint64_t*>(res_buf.ptr() + buckets_offset);
    *buckets_ptr = buckets;
  }
  return ret;
}

uint64_t ObRoaring64Bin::high32_advance_until(uint64_t idx, uint32_t min)
{
  uint64_t res_idx = 0;
  uint64_t lower = idx + 1;
  if ((lower >= buckets_) || (this->high32_[lower] >= min)) {
    res_idx = lower;
  } else {
    uint64_t spansize = 1;
    while ((lower + spansize < buckets_) && (this->high32_[lower + spansize] < min)) {
      spansize *= 2;
    }
    uint64_t upper = (lower + spansize < buckets_) ? lower + spansize : buckets_ - 1;
    if (this->high32_[upper] == min) {
      res_idx = upper;
    } else if (this->high32_[upper] < min) {
      // means keyscards_ has no item >= min
      res_idx = buckets_;
    } else {
      lower += (spansize / 2);
      uint64_t mid = 0;
      while (lower + 1 != upper) {
        mid = (lower + upper) / 2;
        if (this->high32_[mid] == min) {
          return mid;
        } else if (this->high32_[mid] < min) {
          lower = mid;
        } else {
          upper = mid;
        }
      }
      res_idx = upper;
    }
  }
  return res_idx;
}

} // namespace common
} // namespace oceanbase