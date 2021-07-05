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

#ifndef OCEANBASE_COMMON_BLOOM_FILTER_H_
#define OCEANBASE_COMMON_BLOOM_FILTER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <limits.h>
#include <cmath>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
template <class T, class HashFunc>
class ObBloomFilter {
public:
  ObBloomFilter();
  ~ObBloomFilter();
  int init(int64_t element_count, double false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB);
  void destroy();
  void clear();
  int deep_copy(const ObBloomFilter<T, HashFunc>& other);
  int deep_copy(const ObBloomFilter<T, HashFunc>& other, char* buf);
  int64_t get_deep_copy_size() const;
  int insert(const T& element);
  int insert_hash(const uint32_t key_hash);
  int may_contain(const T& element, bool& is_contain) const;
  int64_t calc_nbyte(const int64_t nbit) const;
  bool is_valid() const
  {
    return NULL != bits_ && nbit_ > 0 && nhash_ > 0;
  }
  OB_INLINE int64_t get_nhash() const
  {
    return nhash_;
  }
  OB_INLINE int64_t get_nbit() const
  {
    return nbit_;
  }
  OB_INLINE int64_t get_nbytes() const
  {
    return calc_nbyte(nbit_);
  }
  OB_INLINE uint8_t* get_bits()
  {
    return bits_;
  }
  OB_INLINE const uint8_t* get_bits() const
  {
    return bits_;
  }
  TO_STRING_KV(K_(nhash), K_(nbit), KP_(bits));
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilter);
  static constexpr double BLOOM_FILTER_FALSE_POSITIVE_PROB = 0.01;
  mutable HashFunc hash_func_;
  ObArenaAllocator allocator_;
  int64_t nhash_;
  int64_t nbit_;
  uint8_t* bits_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T, class HashFunc>
ObBloomFilter<T, HashFunc>::ObBloomFilter() : allocator_(ObModIds::OB_BLOOM_FILTER), nhash_(0), nbit_(0), bits_(NULL)
{}

template <class T, class HashFunc>
ObBloomFilter<T, HashFunc>::~ObBloomFilter()
{
  destroy();
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::deep_copy(const ObBloomFilter<T, HashFunc>& other)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObBloomFilter has data.", K(ret));
  } else if (NULL == (bits_ = reinterpret_cast<uint8_t*>(allocator_.alloc(calc_nbyte(other.nbit_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else {
    nbit_ = other.nbit_;
    nhash_ = other.nhash_;
    MEMCPY(bits_, other.bits_, calc_nbyte(nbit_));
  }

  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::deep_copy(const ObBloomFilter<T, HashFunc>& other, char* buffer)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObBloomFilter has data.", K(ret));
  } else {
    nbit_ = other.nbit_;
    nhash_ = other.nhash_;
    bits_ = reinterpret_cast<uint8_t*>(buffer);
    MEMCPY(bits_, other.bits_, calc_nbyte(nbit_));
  }

  return ret;
}

template <class T, class HashFunc>
int64_t ObBloomFilter<T, HashFunc>::get_deep_copy_size() const
{
  return calc_nbyte(nbit_);
}

template <class T, class HashFunc>
int64_t ObBloomFilter<T, HashFunc>::calc_nbyte(const int64_t nbit) const
{
  return (nbit / CHAR_BIT + (nbit % CHAR_BIT ? 1 : 0));
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::init(const int64_t element_count, const double false_positive_prob)
{
  int ret = OB_SUCCESS;
  if (element_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter element_count should be > 0 ", K(element_count), K(ret));
  } else if (!(false_positive_prob < 1.0 && false_positive_prob > 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter false_positive_prob should be < 1.0 and > 0.0", K(false_positive_prob), K(ret));
  } else {
    double num_hashes = -std::log(false_positive_prob) / std::log(2);
    int64_t num_bits =
        static_cast<int64_t>((static_cast<double>(element_count) * num_hashes / static_cast<double>(std::log(2))));

    int64_t num_bytes = calc_nbyte(num_bits);

    bits_ = (uint8_t*)allocator_.alloc(static_cast<int32_t>(num_bytes));
    if (NULL == bits_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "bits_ null pointer, ", K_(nbit), K(ret));
    } else {
      memset(bits_, 0, num_bytes);
      nhash_ = static_cast<int64_t>(num_hashes);
      nbit_ = num_bits;
    }
  }
  return ret;
}

template <class T, class HashFunc>
void ObBloomFilter<T, HashFunc>::destroy()
{
  if (NULL != bits_) {
    allocator_.reset();
    bits_ = NULL;
    nhash_ = 0;
    nbit_ = 0;
  }
}

template <class T, class HashFunc>
void ObBloomFilter<T, HashFunc>::clear()
{
  if (NULL != bits_) {
    memset(bits_, 0, calc_nbyte(nbit_));
  }
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::insert(const T& element)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ", K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    const uint64_t hash = static_cast<uint32_t>(hash_func_(element, 0));
    const uint64_t delta = ((hash >> 17) | (hash << 15)) % nbit_;
    uint64_t bit_pos = hash % nbit_;
    for (int64_t i = 0; i < nhash_; i++) {
      bits_[bit_pos / CHAR_BIT] = static_cast<unsigned char>(bits_[bit_pos / CHAR_BIT] | (1 << (bit_pos % CHAR_BIT)));
      bit_pos = (bit_pos + delta) < nbit_ ? bit_pos + delta : bit_pos + delta - nbit_;
    }
  }
  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::insert_hash(const uint32_t key_hash)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ", K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    const uint64_t hash = key_hash;
    const uint64_t delta = ((hash >> 17) | (hash << 15)) % nbit_;
    uint64_t bit_pos = hash % nbit_;
    for (int64_t i = 0; i < nhash_; i++) {
      bits_[bit_pos / CHAR_BIT] = static_cast<unsigned char>(bits_[bit_pos / CHAR_BIT] | (1 << (bit_pos % CHAR_BIT)));
      bit_pos = (bit_pos + delta) < nbit_ ? bit_pos + delta : bit_pos + delta - nbit_;
    }
  }
  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::may_contain(const T& element, bool& is_contain) const
{
  int ret = OB_SUCCESS;
  is_contain = true;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ", K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    const uint64_t hash = static_cast<uint32_t>(hash_func_(element, 0));
    const uint64_t delta = ((hash >> 17) | (hash << 15)) % nbit_;
    uint64_t bit_pos = hash % nbit_;
    for (int64_t i = 0; i < nhash_; ++i) {
      if (0 == (bits_[bit_pos / CHAR_BIT] & (1 << (bit_pos % CHAR_BIT)))) {
        is_contain = false;
        break;
      }
      bit_pos = (bit_pos + delta) < nbit_ ? bit_pos + delta : bit_pos + delta - nbit_;
    }
  }
  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Unexpected invalid bloomfilter to serialize", K_(nhash), K_(nbit), KP_(bits), K(ret));
  } else if (OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "bloofilter serialize size overflow", K(serialize_size), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, nhash_))) {
    LIB_LOG(WARN, "Failed to encode nhash", K(buf_len), K(pos), K_(nhash), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, nbit_))) {
    LIB_LOG(WARN, "Failed to encode nbit", K(buf_len), K(pos), K_(nbit), K(ret));
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, bits_, calc_nbyte(nbit_)))) {
    LIB_LOG(WARN, "Failed to encode bits", K(buf_len), K(pos), KP_(bits), K(ret));
  }

  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int64_t min_bf_size = 3;
  int64_t decode_nhash = 0;
  int64_t decode_nbit = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len - pos < min_bf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument to deserialize bloomfilter", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_nhash))) {
    LIB_LOG(WARN, "Failed to decode nhash", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_nbit))) {
    LIB_LOG(WARN, "Failed to decode nbit", K(data_len), K(pos), K(ret));
  } else if (OB_UNLIKELY(decode_nhash <= 0 || decode_nbit <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Unexpected deserialize nhash or nbit", K(decode_nhash), K(decode_nbit), K(ret));
  } else {
    int64_t nbyte = calc_nbyte(decode_nbit);
    if (!is_valid() || calc_nbyte(nbit_) != nbyte) {
      destroy();
      if (OB_ISNULL(bits_ = (uint8_t*)allocator_.alloc(static_cast<int32_t>(nbyte)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "bits alloc memory failed", K(decode_nbit), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t decode_byte = 0;
      nhash_ = decode_nhash;
      nbit_ = decode_nbit;
      clear();
      if (OB_ISNULL(
              serialization::decode_vstr(buf, data_len, pos, reinterpret_cast<char*>(bits_), nbyte, &decode_byte))) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "Failed to decode bits", K(data_len), K(pos), K(ret));
      } else if (nbyte != decode_byte) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "Unexpected bits decode length", K(decode_byte), K(nbyte), K(ret));
      }
    }
  }

  return ret;
}

template <class T, class HashFunc>
int64_t ObBloomFilter<T, HashFunc>::get_serialize_size() const
{
  return serialization::encoded_length_vi64(nhash_) + serialization::encoded_length_vi64(nbit_) +
         serialization::encoded_length_vstr(calc_nbyte(nbit_));
}

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_BLOOM_FILTER_H_
