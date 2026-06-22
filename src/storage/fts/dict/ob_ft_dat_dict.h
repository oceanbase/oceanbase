/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DAT_DICT_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DAT_DICT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict.h"

#include <cstddef>

/* File Description:
 * @brief tools to build a double array trie dictionary.
 */

namespace oceanbase
{
namespace storage
{
/* @class ObArrayHashMap
 * @description: a fixed size hash map, used to store words and their code.
 */
class ObArrayHashMap final
{
public:
  struct Entry
  {
    ObFTSingleToken token;
    ObFTTokenCode code;
    bool used = false;
  } __attribute__((packed));

  struct Header
  {
    uint32_t buffer_size_;
    uint32_t capacity_;
    uint32_t count_;
    uint32_t locator_;
    Entry data[1];
  } __attribute__((packed));

  static size_t calc_capacity(size_t token_cnt);

  static size_t calc_memory_size(size_t token_cnt);

  int init(size_t token_cnt);

  int insert(const ObString &token, ObFTTokenCode code);

  int find(const ObString &token, ObFTTokenCode &code) const;

private:
  struct Header header_;
} __attribute__((packed));

/**
 * @class ObFTDAT
 * @brief ObFTDATrie data structure, which is a memory block, which contains
 *          the code map(optional), the base and check array, and the data block(optional).
 */
struct ObFTDAT
{
public:
  static constexpr int32_t FIRST_BASE = 1;
  static constexpr int32_t FIRST_INDEX = 1;
  // all size in bytes
  int64_t mem_block_size_ = 0;
  // node_num_: number of nodes in the trie.
  size_t node_num_ = 0;
  // word_num_: number of words in the trie.
  size_t word_num_ = 0;
  // array_size_ for the base and check array.
  size_t array_size_ = 0;

  // start offset of every data block
  size_t map_offset_ = 0;
  size_t base_offset_ = 0;
  size_t check_offset_ = 0;

  // use if offset is not 0, it means the data is in the data block
  size_t data_index_offset_ = 0;
  // use if offset is not 0
  size_t data_offset_ = 0;
  // a buffer to store dat
  ObFTSingleToken start_token_;
  ObFTSingleToken end_token_;
  char buff[1] = {0};

public:
  ObArrayHashMap *get_map();
  const ObArrayHashMap *get_map() const;
} __attribute__((packed));

template <typename DataType>
class ObFTTrie;

/**
 * @class OBFTDATBuilder
 * @brief oceanbase double array trie builder, build a trie and then a dat.
 */
template <typename DATA_TYPE = void>
class ObFTDATBuilder
{
public:
  ObFTDATBuilder(ObIAllocator &alloc)
      : alloc_(alloc), dat_(nullptr), map_(nullptr), is_inited_(false), next_code_(1)
  {
  }

  ~ObFTDATBuilder() {}

  int init(ObFTTrie<DATA_TYPE> &trie);

  int build_from_trie(ObFTTrie<DATA_TYPE> &trie);

  void get_mem_block(ObFTDAT *&mem)
  {
    mem = dat_;
  }

private:
  // Grow base/check arrays. If min_array_size > array_size_, grow to at least min_array_size.
  int expand(const size_t min_array_size = 0);

  int encode(const ObString &single_token, ObFTTokenCode &code);

private:
  common::ObIAllocator &alloc_;
  ObFTDAT *dat_;
  ObArrayHashMap *map_;

  bool is_inited_;
  int32_t next_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTDATBuilder);
};

/**
 * @class ObFTDATReader
 * @brief Reader to do match on a mem block dat.
 */
template <typename DataType = void>
class ObFTDATReader
{
public:
  ObFTDATReader(const ObFTDAT *dat) : dat_(dat), map_(dat->get_map()) { }

  // Match single token with hit.
  int match_with_hit(const ObString &single_token,
                     const ObDATrieHit &last_hit,
                     ObDATrieHit &hit) const;

private:
  const ObFTDAT *dat_;
  const ObArrayHashMap *map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTDATReader);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DAT_DICT_H_
