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

#include "ob_bloom_filter_cache.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "lib/atomic/ob_atomic.h"
#include "storage/access/ob_rows_info.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "ob_datum_rowkey.h"
#include "storage/access/ob_empty_read_bucket.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

ObBloomFilter::ObBloomFilter() : allocator_(ObModIds::OB_BLOOM_FILTER), nhash_(0), nbit_(0), bits_(NULL)
{
}

ObBloomFilter::~ObBloomFilter()
{
  destroy();
}

int ObBloomFilter::deep_copy(const ObBloomFilter &other)
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

int ObBloomFilter::deep_copy(const ObBloomFilter &other, char *buffer)
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


int64_t ObBloomFilter::get_deep_copy_size() const
{
  return calc_nbyte(nbit_);
}

int64_t ObBloomFilter::calc_nbyte(const int64_t nbit) const
{
  return (nbit / CHAR_BIT + (nbit % CHAR_BIT ? 1 : 0));
}

int ObBloomFilter::init(const int64_t element_count, const double false_positive_prob)
{
  int ret = OB_SUCCESS;
  if (element_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter element_count should be > 0", K(element_count), K(ret));
  } else if (!(false_positive_prob < 1.0 && false_positive_prob > 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter false_positive_prob should be < 1.0 and > 0.0", K(false_positive_prob), K(ret));
  } else {
    double num_hashes = -std::log(false_positive_prob) / std::log(2);
    int64_t num_bits = static_cast<int64_t>((static_cast<double>(element_count)
                                             * num_hashes / static_cast<double>(std::log(2))));
    int64_t num_bytes = calc_nbyte(num_bits);
    bits_ = (uint8_t *)allocator_.alloc(static_cast<int32_t>(num_bytes));
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

void ObBloomFilter::destroy()
{
  if (NULL != bits_) {
    allocator_.reset();
    bits_ = NULL;
    nhash_ = 0;
    nbit_ = 0;
  }
}

void ObBloomFilter::clear()
{
  if (NULL != bits_) {
    memset(bits_, 0, calc_nbyte(nbit_));
  }
}

int ObBloomFilter::insert(const uint32_t key_hash)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited", K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    const uint64_t hash = key_hash;
    const uint64_t delta = ((hash >> 17) | (hash << 15)) % nbit_;
    uint64_t  bit_pos = hash % nbit_;
    for (int64_t i = 0; i < nhash_; i++) {
      bits_[bit_pos / CHAR_BIT] = static_cast<unsigned char>(bits_[bit_pos / CHAR_BIT] | (1 << (bit_pos % CHAR_BIT)));
      bit_pos = (bit_pos + delta) < nbit_ ? bit_pos + delta : bit_pos + delta - nbit_;
    }
  }
  return ret;
}

int ObBloomFilter::may_contain(const uint32_t key_hash, bool &is_contain) const
{
  int ret = OB_SUCCESS;
  is_contain = true;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ", K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    const uint64_t hash = key_hash;
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

int ObBloomFilter::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Unexcepted invalid bloomfilter to serialize", K_(nhash), K_(nbit), KP_(bits), K(ret));
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

int ObBloomFilter::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
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
      if (OB_ISNULL(bits_ = (uint8_t *)allocator_.alloc(static_cast<int32_t>(nbyte)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "bits alloc memory failed", K(decode_nbit), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t decode_byte = 0;
      nhash_ = decode_nhash;
      nbit_ = decode_nbit;
      clear();
      if (OB_ISNULL(serialization::decode_vstr(buf, data_len, pos,
                                               reinterpret_cast<char*>(bits_), nbyte, &decode_byte))) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "Failed to decode bits", K(data_len), K(pos), K(ret));
      } else if (nbyte != decode_byte) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "Unexcepted bits decode length", K(decode_byte), K(nbyte), K(ret));
      }
    }
  }

  return ret;
}

int64_t ObBloomFilter::get_serialize_size() const
{
  return serialization::encoded_length_vi64(nhash_)
    + serialization::encoded_length_vi64(nbit_)
    + serialization::encoded_length_vstr(calc_nbyte(nbit_));
}

/**
 * ----------------------------------------------------ObBloomFilterCacheKey--------------------------------------------------
 */
ObBloomFilterCacheKey::ObBloomFilterCacheKey(
  const uint64_t tenant_id, const MacroBlockId &block_id, const int8_t prefix_rowkey_len)
  : tenant_id_(tenant_id), macro_block_id_(block_id), prefix_rowkey_len_(prefix_rowkey_len)
{
}

ObBloomFilterCacheKey::~ObBloomFilterCacheKey()
{
}

uint64_t ObBloomFilterCacheKey::hash() const
{
  uint64_t hash_val = macro_block_id_.hash();
  const uint64_t sum = tenant_id_ + prefix_rowkey_len_;
  hash_val = murmurhash(&sum, sizeof(uint64_t), hash_val);
  return hash_val;
}

bool ObBloomFilterCacheKey::operator==(const common::ObIKVCacheKey &other) const
{
  const ObBloomFilterCacheKey &other_bfkey = reinterpret_cast<const ObBloomFilterCacheKey&> (other);
  return tenant_id_ == other_bfkey.tenant_id_
      && macro_block_id_ == other_bfkey.macro_block_id_
      && prefix_rowkey_len_ == other_bfkey.prefix_rowkey_len_;
}

uint64_t ObBloomFilterCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

int64_t ObBloomFilterCacheKey::size() const
{
  return static_cast<int64_t>(sizeof(*this));
}

int ObBloomFilterCacheKey::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The bloom filter cache key is invalid, ", K(*this), K(ret));
  } else {
    key = new (buf) ObBloomFilterCacheKey(tenant_id_, macro_block_id_, prefix_rowkey_len_);
  }
  return ret;
}

bool ObBloomFilterCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && macro_block_id_.is_valid()
      && 0 < prefix_rowkey_len_;
}

/**
 * --------------------------------------------------ObBloomFilterCacheValue--------------------------------------------------
 */
ObBloomFilterCacheValue::ObBloomFilterCacheValue()
  : version_(BLOOM_FILTER_CACHE_VALUE_VERSION),
    rowkey_column_cnt_(0),
    row_count_(0),
    bloom_filter_(),
    is_inited_(false)
{
}

ObBloomFilterCacheValue::~ObBloomFilterCacheValue()
{
}

void ObBloomFilterCacheValue::reset()
{
  rowkey_column_cnt_ = 0;
  bloom_filter_.destroy();
  row_count_ = 0;
  is_inited_ = false;
}

void ObBloomFilterCacheValue::reuse()
{
  row_count_ = 0;
  bloom_filter_.clear();
}

int64_t ObBloomFilterCacheValue::size() const
{
  return static_cast<int64_t>(sizeof(*this) + bloom_filter_.get_deep_copy_size());
}

int ObBloomFilterCacheValue::deep_copy(ObBloomFilterCacheValue &bf_cache_value) const
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The bloom filter cache value is not valid", K(*this), K(ret));
  } else {
    bf_cache_value.reset();
    if (OB_FAIL(bf_cache_value.bloom_filter_.deep_copy(bloom_filter_))) {
      STORAGE_LOG(WARN, "Fail to deep copy bloom filter cache value", K(ret));
    } else {
      bf_cache_value.version_ = version_;
      bf_cache_value.rowkey_column_cnt_ = rowkey_column_cnt_;
      bf_cache_value.row_count_ = row_count_;
      bf_cache_value.is_inited_ = true;
    }
  }

  return ret;
}

int ObBloomFilterCacheValue::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The bloom filter cache value is not valid, ", K(*this), K(ret));
  } else {
    ObBloomFilterCacheValue *bfcache_value = new (buf) ObBloomFilterCacheValue();
    if (OB_FAIL(bfcache_value->bloom_filter_.deep_copy(bloom_filter_, buf + sizeof(*bfcache_value)))) {
      STORAGE_LOG(WARN, "Fail to deep copy bloom filter cache value, ", K(ret));
    } else {
      bfcache_value->version_ = version_;
      bfcache_value->rowkey_column_cnt_ = rowkey_column_cnt_;
      bfcache_value->row_count_ = row_count_;
      bfcache_value->is_inited_ = true;
      value = bfcache_value;
    }
  }

  return ret;
}

int ObBloomFilterCacheValue::init(const int64_t rowkey_column_cnt, const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_column_cnt <= 0 || row_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(rowkey_column_cnt), K(row_cnt), K(ret));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The bloom filter cache value has been inited, ", K(ret));
  } else if (OB_FAIL(bloom_filter_.init(row_cnt))) {
    STORAGE_LOG(WARN, "Fail to init bloom filter, ", K(ret));
  } else {
    rowkey_column_cnt_ = static_cast<int16_t>(rowkey_column_cnt);
    row_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObBloomFilterCacheValue::insert(const uint32_t hash)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The bloom filter cache value has not been inited, ", K(ret));
  } else if (OB_FAIL(bloom_filter_.insert(hash))) {
    STORAGE_LOG(WARN, "Fail to insert rowkey to bloom filter, ", K(hash), K(ret));
  } else {
    row_count_++;
  }
  return ret;
}

int ObBloomFilterCacheValue::may_contain(const uint32_t hash, bool &is_contain) const
{
  int ret = OB_SUCCESS;
  is_contain = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The bloom filter cache value has not been inited, ", K(ret));
  } else if (OB_FAIL(bloom_filter_.may_contain(hash, is_contain))) {
    STORAGE_LOG(WARN, "The bloom filter judge failed, ", K(ret));
  }
  return ret;
}

bool ObBloomFilterCacheValue::is_valid() const
{
  return is_inited_ && rowkey_column_cnt_ > 0;
}

bool ObBloomFilterCacheValue::could_merge_bloom_filter(const ObBloomFilterCacheValue &bf_cache_value) const
{
  bool bret = false;

  if (OB_UNLIKELY(!is_valid() || !bf_cache_value.is_valid())) {
  } else if (bf_cache_value.version_ != version_ || bf_cache_value.rowkey_column_cnt_ != rowkey_column_cnt_) {
  } else if (bf_cache_value.bloom_filter_.get_nhash() != bloom_filter_.get_nhash()
          || bf_cache_value.bloom_filter_.get_nbit() != bloom_filter_.get_nbit()) {
  } else {
    bret = true;
  }

  return bret;
}

int ObBloomFilterCacheValue::merge_bloom_filter(const ObBloomFilterCacheValue &bf_cache_value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Unexcepted invalid bloomfilter to merge", K_(rowkey_column_cnt), K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid bloomfilter cache to merge", K(ret));
  } else if (OB_UNLIKELY(!could_merge_bloom_filter(bf_cache_value))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexcepted bloomfitler cache to merge", K(bf_cache_value), K_(rowkey_column_cnt), K_(bloom_filter), K(ret));
  } else {
    int64_t num_bytes = bloom_filter_.get_nbytes();
    const uint8_t *merge_bits = bf_cache_value.get_bloom_filter_bits();
    uint8_t *dest_bits = bloom_filter_.get_bits();
    for (int64_t i = 0; i < num_bytes; i++) {
      dest_bits[i] |= merge_bits[i];
    }
    row_count_ += bf_cache_value.get_row_count();
  }

  return ret;
}

DEFINE_SERIALIZE(ObBloomFilterCacheValue)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Unexcepted invalid bloomfilter cache to serialize", K_(rowkey_column_cnt), K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "bloofilter cache serialize size overflow", K(serialize_size), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    STORAGE_LOG(WARN, "Failed to encode version", K(buf_len), K(pos), K_(version), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, rowkey_column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to encode rowkey column cnt", K(buf_len), K(pos), K_(rowkey_column_cnt), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, row_count_))) {
    STORAGE_LOG(WARN, "Failed to encode row cnt", K(buf_len), K(pos), K_(row_count), K(ret));
  } else if (OB_FAIL(bloom_filter_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Failed to serialize bloom_filter", K(buf_len), K(pos), K(ret));
  }

  return ret;
}

DEFINE_DESERIALIZE(ObBloomFilterCacheValue)
{
  int ret = OB_SUCCESS;
  const int64_t min_bf_size = 4;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len - pos < min_bf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument to deserialize bloomfilter", KP(buf), K(data_len), K(pos), K(ret));
  } else {
    reset();
    if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
      STORAGE_LOG(WARN, "Failed to decode version", K(data_len), K(pos), K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &rowkey_column_cnt_))) {
      STORAGE_LOG(WARN, "Failed to decode rowkey column cnt", K(data_len), K(pos), K(ret));
    } else if (rowkey_column_cnt_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected deserialize rowkey column cnt", K_(rowkey_column_cnt), K(ret));
    } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &row_count_))) {
      STORAGE_LOG(WARN, "Failed to decode row cnt", K(data_len), K(pos), K(ret));
    } else if (OB_FAIL(bloom_filter_.deserialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "Failed to deserialize bloom_filter", K(data_len), K(pos), K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBloomFilterCacheValue)
{
  return bloom_filter_.get_serialize_size()
       + serialization::encoded_length_i16(version_)
       + serialization::encoded_length_i16(rowkey_column_cnt_)
       + serialization::encoded_length_vi32(row_count_);
}

/**
 * ----------------------------------------------------ObBloomFilterCache----------------------------------------------------
 */
ObBloomFilterCache::ObBloomFilterCache()
  : bf_cache_miss_count_threshold_(DEFAULT_EMPTY_READ_CNT_THRESHOLD)
{
}

ObBloomFilterCache::~ObBloomFilterCache()
{
}

int ObBloomFilterCache::put_bloom_filter(
    const uint64_t tenant_id,
    const MacroBlockId& macro_block_id,
    const ObBloomFilterCacheValue &bf_value,
    const bool adaptive)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCacheKey bf_key(tenant_id, macro_block_id, static_cast<int8_t>(bf_value.get_prefix_len()) );
  bool overwrite = true;
  if (OB_UNLIKELY(!bf_key.is_valid() || !bf_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(bf_key), K(bf_value), K(ret));
  } else if (OB_FAIL(put(bf_key, bf_value, overwrite))) {
    STORAGE_LOG(WARN, "Fail to put bloomfilter to cache, ", K(ret));
  }

  if (OB_SUCC(ret) && adaptive) {
    storage::ObEmptyReadCell *cell = NULL;
    if (OB_UNLIKELY(tenant_id != MTL_ID())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "mtl id not match, ", K(ret), K(tenant_id), K(MTL_ID()));
    } else if (OB_FAIL(MTL(ObEmptyReadBucket *)->get_cell(bf_key.hash(), cell))) {
      STORAGE_LOG(WARN, "get_bucket_cell fail, ", K(ret));
    } else if (OB_ISNULL(cell)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the cell value is NULL, ", K(ret));
    } else {
      cell->reset();//ignore ret
    }
    auto_bf_cache_miss_count_threshold(MTL(compaction::ObTenantTabletScheduler *)->get_bf_queue_size());
  }
  return ret;
}

int ObBloomFilterCache::may_contain(
    const uint64_t tenant_id,
    const MacroBlockId &macro_block_id,
    const ObDatumRowkey &rowkey,
    const ObStorageDatumUtils &datum_utils,
    bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = true;
  ObBloomFilterCacheKey bf_key(tenant_id, macro_block_id, static_cast<int8_t>(rowkey.get_datum_cnt()) );
  const ObBloomFilterCacheValue *bf_value = NULL;
  ObKVCacheHandle handle;
  uint64_t key_hash = 0;

  if (OB_UNLIKELY(!bf_key.is_valid() || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(bf_key), K(rowkey), K(ret));
  } else if (0 == bf_cache_miss_count_threshold_) {
    //disable bf cache
  } else if (OB_FAIL(get(bf_key, bf_value, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get bloom filter cache, ", K(ret));
    }
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
  } else {
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_HIT);
    if (OB_ISNULL(bf_value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the bf_value is NULL, ", K(ret));
    } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, key_hash))) {
      STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey));
    } else if (OB_FAIL(bf_value->may_contain(static_cast<uint32_t>(key_hash), is_contain))) {
      STORAGE_LOG(WARN, "Fail to check rowkey exist from bloom filter, ", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "debug bloom_filter may contain", K(ret), KP(bf_value), K(key_hash), K(is_contain), K(rowkey));
      if (is_contain) {
        EVENT_INC(ObStatEventIds::BLOOM_FILTER_PASSES);
      } else {
        EVENT_INC(ObStatEventIds::BLOOM_FILTER_FILTS);
      }
    }
  }
  return ret;
}

int ObBloomFilterCache::may_contain(
    const uint64_t tenant_id,
    const MacroBlockId &macro_block_id,
    const storage::ObRowsInfo *rows_info,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx,
    const ObStorageDatumUtils &datum_utils,
    bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  auto *my_rows_info = const_cast<storage::ObRowsInfo *>(rows_info);
  ObBloomFilterCacheKey bf_key(tenant_id, macro_block_id, static_cast<int8_t>(my_rows_info->get_datum_cnt()));
  const ObBloomFilterCacheValue *bf_value = NULL;
  ObKVCacheHandle handle;
  uint64_t key_hash = 0;
  if (OB_UNLIKELY(!bf_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(bf_key), K(ret));
  } else if (0 == bf_cache_miss_count_threshold_) {
    is_contain = true;
  } else if (OB_FAIL(get(bf_key, bf_value, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get bloom filter cache, ", K(ret));
    }
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
  } else {
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_HIT);
    if (OB_ISNULL(bf_value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null bf value", K(ret));
    } else {
      for (int64_t i = rowkey_begin_idx; OB_SUCC(ret) && i < rowkey_end_idx; ++i) {
        bool tmp_contain = false;
        const ObDatumRowkey &rowkey = rows_info->get_rowkey(i);
        if (rows_info->is_row_skipped(i)) {
          continue;
        } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, key_hash))) {
          STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey));
        } else if (OB_FAIL(bf_value->may_contain(static_cast<uint32_t>(key_hash), tmp_contain))) {
          STORAGE_LOG(WARN, "Fail to check rowkey exist from bloom filter, ", K(ret));
        } else {
          if (tmp_contain) {
            is_contain = true;
            EVENT_INC(ObStatEventIds::BLOOM_FILTER_PASSES);
          } else {
            if (!my_rows_info->is_row_bf_checked(i)) {
              my_rows_info->set_row_non_existent(i);
            }
            EVENT_INC(ObStatEventIds::BLOOM_FILTER_FILTS);
          }
          my_rows_info->set_row_bf_checked(i);
        }
      }
    }
  }
  return ret;
}

int ObBloomFilterCache::may_contain(
    const uint64_t tenant_id,
    const MacroBlockId &macro_block_id,
    const storage::ObRowKeysInfo *rowkeys_info,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx,
    const ObStorageDatumUtils &datum_utils,
    bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  storage::ObRowKeysInfo *my_rowkeys_info = const_cast<storage::ObRowKeysInfo *>(rowkeys_info);
  ObBloomFilterCacheKey bf_key(tenant_id, macro_block_id, static_cast<int8_t>(my_rowkeys_info->get_rowkey(rowkey_begin_idx).get_datum_cnt()));
  const ObBloomFilterCacheValue *bf_value = NULL;
  ObKVCacheHandle handle;
  uint64_t key_hash = 0;
  if (OB_UNLIKELY(!bf_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(bf_key), K(ret));
  } else if (0 == bf_cache_miss_count_threshold_) {
    is_contain = true;
  } else if (OB_FAIL(get(bf_key, bf_value, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get bloom filter cache, ", K(ret));
    }
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
  } else {
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_HIT);
    if (OB_ISNULL(bf_value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null bf value", K(ret));
    } else {
      for (int64_t i = rowkey_begin_idx; OB_SUCC(ret) && i < rowkey_end_idx; ++i) {
        bool tmp_contain = false;
        const ObDatumRowkey &rowkey = rowkeys_info->get_rowkey(i);
        if (rowkeys_info->is_rowkey_not_exist(i)) {
          continue;
        } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, key_hash))) {
          STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey));
        } else if (OB_FAIL(bf_value->may_contain(static_cast<uint32_t>(key_hash), tmp_contain))) {
          STORAGE_LOG(WARN, "Fail to check rowkey exist from bloom filter, ", K(ret));
        } else {
          if (tmp_contain) {
            EVENT_INC(ObStatEventIds::BLOOM_FILTER_PASSES);
            if (i == rowkey_begin_idx) {
              is_contain = true;
            }
          } else {
            my_rowkeys_info->set_rowkey_not_exist(i);
            EVENT_INC(ObStatEventIds::BLOOM_FILTER_FILTS);
          }
        }
      }
    }
  }
  return ret;
}

int ObBloomFilterCache::get_sstable_bloom_filter(const uint64_t tenant_id,
                                                  const MacroBlockId &macro_block_id,
                                                  const uint64_t rowkey_column_number,
                                                  const ObBloomFilterCacheValue *bloom_filter,
                                                  ObKVCacheHandle &cache_handle)
{
  int ret = OB_SUCCESS;
  ObBloomFilterCacheKey bf_key(tenant_id, macro_block_id, static_cast<int8_t>(rowkey_column_number));
  bloom_filter = NULL;
  cache_handle.reset();

  if (OB_UNLIKELY(!bf_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(bf_key), K(ret));
  } else if (OB_FAIL(get(bf_key, bloom_filter, cache_handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get bloom filter cache, ", K(ret));
    }
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
  } else if (OB_ISNULL(bloom_filter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the bf_value is NULL, ", K(ret));
  } else {
    EVENT_INC(ObStatEventIds::BLOOM_FILTER_CACHE_HIT);
  }

  return ret;
}

int ObBloomFilterCache::inc_empty_read(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const MacroBlockId &macro_id,
    const int64_t empty_read_prefix,
    const int64_t empty_read_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || empty_read_prefix <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ",
        K(ret), K(tenant_id), K(macro_id), K(empty_read_prefix));
  } else if (0 == bf_cache_miss_count_threshold_) {
    // bf cache is disabled, do nothing
  } else {
    const ObBloomFilterCacheKey bfc_key(tenant_id, macro_id, empty_read_prefix);
    uint64_t key_hash = bfc_key.hash();
    uint64_t cur_cnt = 1;
    storage::ObEmptyReadCell *cell = nullptr;
    if (OB_UNLIKELY(!bfc_key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument, ", K(bfc_key), K(ret));
    } else if (OB_UNLIKELY(tenant_id != MTL_ID())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "mtl id not match, ", K(ret), K(tenant_id), K(MTL_ID()));
    } else if (OB_FAIL(MTL(ObEmptyReadBucket *)->get_cell(key_hash, cell))) {
      STORAGE_LOG(WARN, "get_bucket_cell fail, ", K(ret));
    } else if (OB_ISNULL(cell)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the cell value is NULL, ", K(ret));
    } else if (OB_FAIL(cell->inc_and_fetch(key_hash, empty_read_cnt, cur_cnt))) {
      STORAGE_LOG(WARN, "Fail to increase empty read count in bucket, ", K(ret));
    } else if (cur_cnt > bf_cache_miss_count_threshold_) {
      if (cell->check_timeout()) {
      } else if (OB_FAIL(MTL(compaction::ObTenantTabletScheduler *)->schedule_build_bloomfilter(
          table_id, macro_id, empty_read_prefix))) {
        STORAGE_LOG(WARN, "Fail to schedule build bloom filter, ", K(ret), K(bfc_key),
                    K(cur_cnt), K_(bf_cache_miss_count_threshold));
      } else {
        cell->reset();
      }
    }
    STORAGE_LOG(DEBUG, "inc_empty_read", K(tenant_id), K(table_id), K(macro_id), K(empty_read_cnt),
                 K(cur_cnt), K(bf_cache_miss_count_threshold_));
  }
  return ret;
}

int ObBloomFilterCache::check_need_build(const ObBloomFilterCacheKey &bf_key,
    bool &need_build)
{
  int ret = OB_SUCCESS;
  const ObBloomFilterCacheValue *bf_value = NULL;
  ObKVCacheHandle handle;
  need_build = false;
  if (!bf_key.is_valid()) {
    //do nothing;
  } else if (OB_FAIL(get(bf_key, bf_value, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get bloom filter cache, ", K(ret));
    } else {
      need_build = true;
      ret = OB_SUCCESS;
    }
  } else if (bf_value->get_prefix_len() != bf_key.get_prefix_rowkey_len()) {
    need_build = true;
  }
  return ret;
}

int ObBloomFilterCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  //size must be 2^n, for fast mod
  if (OB_FAIL((common::ObKVCache<ObBloomFilterCacheKey, ObBloomFilterCacheValue>::init(cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  }
  return ret;
}

void ObBloomFilterCache::destroy()
{
  common::ObKVCache<ObBloomFilterCacheKey, ObBloomFilterCacheValue>::destroy();
}

/**
 * -----------------------------------------ObMacroBloomFilterCacheWriter-----------------------------------------------
 */

ObMacroBloomFilterCacheWriter::ObMacroBloomFilterCacheWriter()
  : bf_cache_value_(),
    max_row_count_(0),
    need_build_(false),
    is_inited_(false)
{
}

ObMacroBloomFilterCacheWriter::~ObMacroBloomFilterCacheWriter()
{
}

int ObMacroBloomFilterCacheWriter::init(const int64_t rowkey_column_count, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  max_row_count_ = row_count + row_count / 16;
  if (OB_FAIL(bf_cache_value_.init(rowkey_column_count, max_row_count_))) {
    STORAGE_LOG(WARN, "blolomfilter cache value init failed, ", K(ret));
  } else {
    need_build_ = true;
    is_inited_ = true;
  }
  return ret;
}

void ObMacroBloomFilterCacheWriter::reset()
{
  bf_cache_value_.reset();
  max_row_count_ = 0;
  need_build_ = false;
  is_inited_ = false;
}

void ObMacroBloomFilterCacheWriter::reuse()
{
  if (is_inited_) {
    bf_cache_value_.reuse();
    need_build_ = true;
  }
}

void ObMacroBloomFilterCacheWriter::set_not_need_build()
{
  bf_cache_value_.reuse();
  need_build_ = false;
}

int ObMacroBloomFilterCacheWriter::append(const common::ObArray<uint32_t> &hashs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMacroBloomFilterCacheWriter not init", K(ret));
  } else if (!need_build_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Not need build bloomfilter, ", K_(need_build), K(ret));
  } else if (get_row_count() + hashs.count() > max_row_count_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Bloomfilter is full, ", K_(max_row_count), K(get_row_count()), K(hashs.count()));
    bf_cache_value_.reuse();
    need_build_ = false;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < hashs.count(); ++i) {
      if (OB_FAIL(bf_cache_value_.insert(hashs.at(i)))) {
        bf_cache_value_.reuse();
        need_build_ = false;
        STORAGE_LOG(WARN, "bloomfilter insert hash value failed, ", K(i), K(hashs.at(i)), K(ret));
      }
    }
  }
  return ret;
}

bool ObMacroBloomFilterCacheWriter::can_merge(const ObMacroBloomFilterCacheWriter &other)
{
  bool bret = false;
  if (OB_UNLIKELY(!is_valid() || !other.is_valid())) {
  } else if (!is_need_build() || !other.is_need_build()) {
  } else if (get_row_count() + other.get_row_count() > max_row_count_) {
  } else {
    bret = true;
  }
  return bret;
}

int ObMacroBloomFilterCacheWriter::merge(const ObMacroBloomFilterCacheWriter &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Cache writer not valid, ", K(*this), K(other), K(ret));
  } else if (!need_build_ || !other.is_need_build()) {
    //not need build
  } else if (get_row_count() + other.get_row_count() > max_row_count_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Exceed bloomfilter cache writer max count, ", K(*this), K(other), K(ret));
  } else if (!bf_cache_value_.could_merge_bloom_filter(other.bf_cache_value_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Can not merge bloomfilter cache value, ", K(*this), K(other), K(ret));
  } else if (OB_FAIL(bf_cache_value_.merge_bloom_filter(other.bf_cache_value_))) {
    STORAGE_LOG(WARN, "Failed to merge bloomfilter cache value, ", K(*this), K(other), K(ret));
  }
  return ret;
}

int ObMacroBloomFilterCacheWriter::flush_to_cache(
    const uint64_t tenant_id,
    const MacroBlockId& macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument,", K(ret), K(macro_id), K(*this));
  } else if (need_build_
      && OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().put_bloom_filter(
          tenant_id, macro_id, bf_cache_value_))) {
    STORAGE_LOG(WARN, "Fail to put value to bloom filter cache",
                K(tenant_id), K(macro_id), K_(bf_cache_value), K(ret));
  }
  return ret;
}


} /* namespace blocksstable */
} /* namespace oceanbase */
