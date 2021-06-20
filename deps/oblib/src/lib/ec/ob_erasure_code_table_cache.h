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

#ifndef OCEANBASE_COMMON_EC_OB_EC_TABLE_CACHE_H
#define OCEANBASE_COMMON_EC_OB_EC_TABLE_CACHE_H

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase {
namespace common {

struct ObErasureCodeEncodeKey {
  ObErasureCodeEncodeKey() : data_count_(0), parity_count_(0)
  {}

  ObErasureCodeEncodeKey(int64_t data_count, int64_t parity_count)
      : data_count_(data_count), parity_count_(parity_count)
  {}

  bool operator==(const ObErasureCodeEncodeKey& other) const
  {
    return data_count_ == other.data_count_ && parity_count_ == other.parity_count_;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&data_count_, sizeof(data_count_), hash_val);
    hash_val = common::murmurhash(&parity_count_, sizeof(parity_count_), hash_val);
    return hash_val;
  }

  int64_t data_count_;
  int64_t parity_count_;
};

struct ObErasureCodeDecodeKey {
  ObErasureCodeDecodeKey() : data_count_(0), parity_count_(0), signature_()
  {}

  ObErasureCodeDecodeKey(int64_t data_count, int64_t parity_count, const ObString& signature)
      : data_count_(data_count), parity_count_(parity_count), signature_(signature)
  {}

  bool operator==(const ObErasureCodeDecodeKey& other) const
  {
    return data_count_ == other.data_count_ && parity_count_ == other.parity_count_ && signature_ == other.signature_;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&data_count_, sizeof(data_count_), hash_val);
    hash_val = common::murmurhash(&parity_count_, sizeof(parity_count_), hash_val);
    hash_val = signature_.hash(hash_val);

    return hash_val;
  }

  int64_t data_count_;
  int64_t parity_count_;
  ObString signature_;
};

class ObECTableCache {
public:
  ObECTableCache();

  virtual ~ObECTableCache();

  int init(const int64_t bucket_size);
  void destroy();

  int get_encoding_table(
      const int64_t data_count, const int64_t parity_count, bool& table_exist_flag, unsigned char*& encoding_table);
  int set_encoding_table(const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table);

  int get_encoding_matrix(
      const int64_t data_count, const int64_t parity_count, bool& matrix_exist_flag, unsigned char*& encoding_matrix);
  int set_encoding_matrix(const int64_t data_count, const int64_t parity_count, unsigned char* ec_matrix);

  int get_decoding_table(const ObString& signature, const int64_t data_count, const int64_t parity_count,
      bool& table_exist_flag, unsigned char*& decoding_table);
  int set_decoding_table(
      const ObString& signature, const int64_t data_count, const int64_t parity_count, unsigned char* decoding_table);

  int get_decoding_matrix(const ObString& signature, const int64_t data_count, const int64_t parity_count,
      bool& matrix_exist_flag, unsigned char*& decoding_matrix);
  int set_decoding_matrix(
      const ObString& signature, const int64_t data_count, const int64_t parity_count, unsigned char* decoding_matrix);

private:
  bool is_inited_;

  hash::ObHashMap<ObErasureCodeEncodeKey, unsigned char*> encoding_table_map_;
  hash::ObHashMap<ObErasureCodeEncodeKey, unsigned char*> encoding_matrix_map_;

  hash::ObHashMap<ObErasureCodeDecodeKey, unsigned char*> decoding_table_map_;
  hash::ObHashMap<ObErasureCodeDecodeKey, unsigned char*> decoding_matrix_map_;
};

class ObECCacheManager {
public:
  static ObECCacheManager& get_ec_cache_mgr();

  int init(const int64_t bucket_size = 1024);
  void destroy();

  int get_encoding_table(const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_table);
  int set_encoding_table(const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table);

  int get_encoding_matrix(const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_matrix);
  int set_encoding_matrix(const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table);

  int get_decoding_matrix(const common::ObIArray<int64_t>& erasure_indexes,
      const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
      unsigned char*& decoding_matrix);
  int set_decoding_matrix(const common::ObIArray<int64_t>& erasure_indexes,
      const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
      unsigned char* decoding_matrix);

  int get_decoding_table(const common::ObIArray<int64_t>& erasure_indexes,
      const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
      unsigned char*& decoding_table);
  int set_decoding_table(const common::ObIArray<int64_t>& erasure_indexes,
      const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
      unsigned char* decoding_table);

private:
  ObECCacheManager();
  virtual ~ObECCacheManager();
  DISALLOW_COPY_AND_ASSIGN(ObECCacheManager);

  int alloc_table_mem(const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_table);
  int alloc_matrix_mem(const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_matrix);
  void free_encoding_table_or_matrix_mem(unsigned char* ptr);

  int get_erasure_code_signature(const common::ObIArray<int64_t>& erasure_indexes,
      const common::ObIArray<int64_t>& ready_indexes, ObString& signature);

  bool erasure_contains(const ObIArray<int64_t>& erasures, int64_t index);

  //@brief Generate decode matrix from encode matrix
  //@param encode_matrix [in]  encode matrix(all_block_count * data_block_count)
  //@param decode_index  [in]  no error row index list
  //@param src_err_list  [in]  error row index list
  //@param decode_matrix [out]  decode matrix(nerrs * data_block_count)
  int gen_decode_matrix(const unsigned char* encode_matrix, const ObIArray<int64_t>& decode_indexes,
      const ObIArray<int64_t>& erase_indexes, const int64_t data_block_count, const int64_t parity_block_count,
      unsigned char* decode_matrix);

private:
  bool is_inited_;

  ObECTableCache cache_;
  ObArenaAllocator table_allocator_;
  lib::ObMutex table_allocator_mutex_;  // mutex used to protect modifications in allocating table memory

  ObArenaAllocator signature_allocator_;
  lib::ObMutex signature_allocator_mutex_;  // mutex used to protect modifications in allocating signature memory
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_EC_OB_EC_TABLE_CACHE_H
