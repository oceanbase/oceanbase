#ifndef OCEANBASE_COMMON_XOR_FILTER_H_
#define OCEANBASE_COMMON_XOR_FILTER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <limits.h>
#include <cmath>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/filter/ob_filter.h"

namespace oceanbase {
namespace common {

OB_INLINE uint32_t reduce(uint32_t hash, uint32_t n)
{
  return (uint32_t)(((uint64_t)hash * n) >> 32);
}

OB_INLINE uint64_t rotl64(uint64_t n, unsigned int c)
{
  const unsigned int mask = (CHAR_BIT * sizeof(n) - 1);
  c &= mask;
  return (n << c) | (n >> ((-c) & mask));
}

OB_INLINE size_t getHashFromHash(uint64_t hash, int index, int blockLength)
{
  uint32_t r = rotl64(hash, index * 21);
  return (size_t)reduce(r, blockLength) + index * blockLength;
}

template <class T, class HashFunc>
class ObXorFilter : public ObFilter<T, HashFunc> {
public:
  ObXorFilter();
  ~ObXorFilter();
  int init(int64_t element_count);
  void destroy();
  void clear();
  int deep_copy(const ObFilter<T, HashFunc>& other);
  int deep_copy(const ObFilter<T, HashFunc>& other, char* buffer);
  int deep_copy(const ObXorFilter<T, HashFunc>& other);
  int deep_copy(const ObXorFilter<T, HashFunc>& other, char* buf);
  int64_t get_deep_copy_size() const;
  int insert(const T& element);
  int insert_hash(const uint32_t key_hash);
  int insert_all(const T* keys, const int64_t size);
  int may_contain(const T& item, bool& is_contain) const;
  bool could_merge(const ObFilter<T, HashFunc>& other);
  int merge(const ObFilter<T, HashFunc>& other);
  int64_t calc_nbyte(const int64_t nbit) const;

  OB_INLINE uint8_t fingerprint(const uint64_t hash) const
  {
    return (uint8_t)hash ^ (hash >> 32);
  }

  OB_INLINE bool is_valid() const
  {
    return NULL != bits_ && array_length_ > 0;
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
    return static_cast<int64_t>(array_length_ * sizeof(uint8_t));
  }
  OB_INLINE uint8_t* get_bits()
  {
    return bits_;
  }
  OB_INLINE uint8_t* get_bits() const
  {
    return bits_;
  }
  TO_STRING_KV(K_(nbit), KP_(bits));
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;

private:
  DISALLOW_COPY_AND_ASSIGN(ObXorFilter);
  ObArenaAllocator allocator_;
  int64_t size_;
  int64_t array_length_;
  int64_t block_length_;
  int64_t seed_;
  mutable HashFunc hash_func_;
  static constexpr int64_t nhash_ = 3;
  int64_t nbit_;
  uint8_t* bits_;
};

template <class T, class HashFunc>
ObXorFilter<T, HashFunc>::ObXorFilter() : allocator_(ObModIds::OB_BLOOM_FILTER), array_length_(0), seed_(0), bits_(NULL)
{}

template <class T, class HashFunc>
ObXorFilter<T, HashFunc>::~ObXorFilter()
{
  destroy();
}

template <class T, class HashFunc>
void ObXorFilter<T, HashFunc>::destroy()
{
  bits_ = NULL;
  nbit_ = 0;
  size_ = 0;
  array_length_ = 0;
  block_length_ = 0;
  allocator_.reset();
}

template <class T, class HashFunc>
void ObXorFilter<T, HashFunc>::clear()
{
  // reuse, new size_ must euqal to old size_
  if (NULL != bits_) {
    memset(bits_, 0, get_nbytes());
  }
}

template <class T, class HashFunc>
int64_t ObXorFilter<T, HashFunc>::get_deep_copy_size() const
{
  // bits_'s byte size
  return get_nbytes();
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::deep_copy(const ObFilter<T, HashFunc>& other)
{
  const ObXorFilter<T, HashFunc>& filter = static_cast<const ObXorFilter<T, HashFunc>&>(other);
  return deep_copy(filter);
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::deep_copy(const ObFilter<T, HashFunc>& other, char* buffer)
{
  const ObXorFilter<T, HashFunc>& filter = static_cast<const ObXorFilter<T, HashFunc>&>(other);
  return deep_copy(filter, buffer);
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::deep_copy(const ObXorFilter<T, HashFunc>& other)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObXorFilter has data.", K(ret));
  } else if (NULL == (bits_ = reinterpret_cast<uint8_t*>(allocator_.alloc(other.get_nbytes())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "Failed to allocate memory, ", K(ret));
  } else {
    size_ = other.size_;
    array_length_ = other.array_length_;
    block_length_ = other.block_length_;
    seed_ = other.seed_;
    nbit_ = other.nbit_;
    MEMCPY(bits_, other.bits_, other.get_nbytes());
  }

  return ret;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::deep_copy(const ObXorFilter<T, HashFunc>& other, char* buffer)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObXorFilter has data.", K(ret));
  } else {
    size_ = other.size_;
    array_length_ = other.array_length_;
    block_length_ = other.block_length_;
    seed_ = other.seed_;
    nbit_ = other.nbit_;
    bits_ = reinterpret_cast<uint8_t*>(buffer);
    MEMCPY(bits_, other.bits_, get_nbytes());
  }

  return ret;
}

template <class T, class HashFunc>
int64_t ObXorFilter<T, HashFunc>::calc_nbyte(const int64_t nbit) const
{
  return (nbit / CHAR_BIT + (nbit % CHAR_BIT ? 1 : 0));
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::init(const int64_t element_count)
{
  int ret = OB_SUCCESS;

  if (element_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "xor filter element_count should be > 0 ", K(element_count), K(ret));
  } else {
    size_ = element_count;
    array_length_ = 32 + 1.23 * size_;
    block_length_ = array_length_ / 3;
    bits_ = (uint8_t*)allocator_.alloc(static_cast<int32_t>(array_length_ * sizeof(uint8_t)));
    if (NULL == bits_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "bits_ null pointer, ", K_(nbit), K(ret));
    } else {
      memset(bits_, 0, array_length_);
      nbit_ = static_cast<int64_t>(array_length_ * CHAR_BIT);
    }
  }
  return ret;
}

// should not be called
template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::insert(const T& elements)
{
  int ret = OB_NOT_SUPPORTED;
  LIB_LOG(WARN, "xor filter does not support insert single key, ", K(ret), K(elements));
  return ret;
}

// should not be called
template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::insert_hash(const uint32_t key_hash)
{
  int ret = OB_NOT_SUPPORTED;
  LIB_LOG(WARN, "xor filter does not support insert single key, ", K(ret), K(key_hash));
  return ret;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::insert_all(const T* keys, const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size != size_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "the number of keys is not equal to the size_, ", K_(nbit), K(ret));
    return ret;
  }

  ObArenaAllocator tmp_allocator;

  uint64_t* stack_hash = (uint64_t*)tmp_allocator.alloc(sizeof(uint64_t) * size_);
  uint32_t* stack_index = (uint32_t*)tmp_allocator.alloc(sizeof(uint32_t) * size_);
  uint32_t* counting = (uint32_t*)tmp_allocator.alloc(sizeof(uint32_t) * array_length_);
  uint64_t* buckets = (uint64_t*)tmp_allocator.alloc(sizeof(uint64_t) * array_length_);
  int* queue = (int*)tmp_allocator.alloc(sizeof(int) * array_length_);

  if (stack_hash == NULL) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "stack_hash null pointer, ", K(stack_hash), K(ret));
  } else if (stack_index == NULL) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "stack_index null pointer, ", K(stack_index), K(ret));
  } else if (counting == NULL) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "counting null pointer, ", K(counting), K(ret));
  } else if (buckets == NULL) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "buckets null pointer, ", K(buckets), K(ret));
  } else if (queue == NULL) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "queue null pointer, ", K(queue), K(ret));
  } else {
    int max_try = 20;
    while (max_try-- > 0) {
      seed_++;
      memset(stack_hash, 0, sizeof(uint64_t) * size_);
      memset(stack_index, 0, sizeof(uint32_t) * size_);
      memset(counting, 0, array_length_ * sizeof(uint32_t));
      memset(buckets, 0, array_length_ * sizeof(uint64_t));
      memset(queue, 0, array_length_ * sizeof(int));

      for (int64_t i = 0; i < size; i++) {
        uint64_t hash = hash_func_(keys[i], seed_);
        for (int64_t hi = 0; hi < 3; hi++) {
          size_t index = getHashFromHash(hash, hi, block_length_);
          counting[index]++;
          buckets[index] ^= hash;
        } 
      }

      int qi = 0;
      for (int64_t i = 0; i < array_length_; i++) {
        if (counting[i] == 1) {
          queue[qi++] = i;
        }
      }

      int si = 0;
      while (si < size_) {
        if (qi <= 0)
          break;
        int i = queue[--qi];
        uint64_t x = buckets[i];
        if (counting[i] == 1) {
          stack_hash[si] = x;
          stack_index[si] = i;
          si++;

          for (int j = 0; j < 3; j++) {
            int index = getHashFromHash(x, j, block_length_);
            counting[index]--;
            if (counting[index] == 1) {
              queue[qi++] = index;
            }
            buckets[index] ^= x;
          }
        }
      } 

      if (si == size_) {
        for (int stack_pos = size_ - 1; stack_pos >= 0; stack_pos--) {
          int index = stack_index[stack_pos];
          int64_t hash = stack_hash[stack_pos];
          int change = -1;
          uint8_t f = fingerprint(hash);
          for (int hi = 0; hi < 3; hi++) {
            int idx = getHashFromHash(hash, hi, block_length_);
            if (idx == index) {
              change = index;
            } else {
              f ^= bits_[idx];
            }
          }
          bits_[change] = f;
        }
        return ret;
      }
    } 
    ret = OB_NEED_RETRY;
  }

  return ret;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::may_contain(const T& key, bool& is_contain) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "xor filter has not inited, ", K_(bits), K_(nbit), K(ret));
  } else {
    uint64_t hash = hash_func_(key, seed_);
    uint8_t f = fingerprint(hash);
    uint32_t i0 = getHashFromHash(hash, 0, block_length_);
    uint32_t i1 = getHashFromHash(hash, 1, block_length_);
    uint32_t i2 = getHashFromHash(hash, 2, block_length_);
    f ^= bits_[i0] ^ bits_[i1] ^ bits_[i2];
    is_contain = f == 0 ? true : false;
  }
  return ret;
}

template <class T, class HashFunc>
bool ObXorFilter<T, HashFunc>::could_merge(const ObFilter<T, HashFunc>& other)
{
  // xor filter doesn't support merge
  (void)other;
  LIB_LOG(WARN, "xor filter does not support merge");
  return false;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::merge(const ObFilter<T, HashFunc>& other)
{
  // xor filter doesn't support merge
  int ret = OB_NOT_SUPPORTED;

  (void)other;
  LIB_LOG(WARN, "xor filter does not support merge, ", K(ret));
  return ret;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Unexpected invalid xor to serialize", K_(nbit), KP_(bits), K(ret));
  } else if (OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "xorfilter serialize size overflow", K(serialize_size), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, size_))) {
    LIB_LOG(WARN, "Failed to encode size", K(buf_len), K(pos), K_(size), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, array_length_))) {
    LIB_LOG(WARN, "Failed to encode array_length", K(buf_len), K(pos), K_(array_length), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, block_length_))) {
    LIB_LOG(WARN, "Failed to encode block_length", K(buf_len), K(pos), K_(block_length), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, seed_))) {
    LIB_LOG(WARN, "Failed to encode seed", K(buf_len), K(pos), K_(seed), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, nbit_))) {
    LIB_LOG(WARN, "Failed to encode nbit", K(buf_len), K(pos), K_(nbit), K(ret));
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, bits_, get_nbytes()))) {
    LIB_LOG(WARN, "Failed to encode bits", K(buf_len), K(pos), KP_(bits), K(ret));
  }

  return ret;
}

template <class T, class HashFunc>
int ObXorFilter<T, HashFunc>::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int64_t min_bf_size = 6;
  int64_t decode_size = 0;
  int64_t decode_array_length = 0;
  int64_t decode_block_length = 0;
  int64_t decode_seed = 0;
  int64_t decode_nbit = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len - pos < min_bf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument to deserialize xorfilter", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_size))) {
    LIB_LOG(WARN, "Failed to decode size", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_array_length))) {
    LIB_LOG(WARN, "Failed to decode array_length", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_block_length))) {
    LIB_LOG(WARN, "Failed to decode block_length", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_seed))) {
    LIB_LOG(WARN, "Failed to decode seed", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &decode_nbit))) {
    LIB_LOG(WARN, "Failed to decode nbit", K(data_len), K(pos), K(ret));
  } else if (OB_UNLIKELY(
                 decode_size <= 0 || decode_array_length <= 0 || decode_block_length <= 0 || decode_nbit <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN,
        "Unexpected deserialize size, array_length, block_length or nbit",
        K(decode_size),
        K(decode_array_length),
        K(decode_block_length),
        K(decode_nbit),
        K(ret));
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
      size_ = decode_size;
      array_length_ = decode_array_length;
      block_length_ = decode_block_length;
      seed_ = decode_seed;
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
int64_t ObXorFilter<T, HashFunc>::get_serialize_size() const
{
  return serialization::encoded_length_vi64(size_) + serialization::encoded_length_vi64(array_length_) +
         serialization::encoded_length_vi64(block_length_) + serialization::encoded_length_vi64(seed_) +
         serialization::encoded_length_vi64(nbit_) + serialization::encoded_length_vstr(get_nbytes());
}

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_XOR_FILTER_H_