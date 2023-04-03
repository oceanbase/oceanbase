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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_READER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "ob_block_manager.h"
#include "ob_macro_block_reader.h"
#include "ob_macro_block.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBloomFilterCacheValue;

class ObBloomFilterMacroBlockReader
{
public:
  ObBloomFilterMacroBlockReader(const bool is_sys_read = false);
  virtual ~ObBloomFilterMacroBlockReader();
  void reset();
  int read_macro_block(
      const MacroBlockId &macro_id,
      const char *&bf_buf,
      int64_t &bf_size);
private:
  int decompress_micro_block(const char *&block_buf, int64_t &block_size);
  int read_micro_block(const char *buf, const int64_t buf_size, const char *&bf_buf, int64_t &bf_size);
  int read_macro_block(const MacroBlockId &macro_id);
private:
  ObMacroBlockReader macro_reader_;
  ObMacroBlockHandle macro_handle_;
  ObMacroBlockCommonHeader common_header_;
  const ObBloomFilterMacroBlockHeader *bf_macro_header_;
  bool is_sys_read_;
};

class ObBloomFilterDataReader
{
public:
  ObBloomFilterDataReader(const bool is_sys_read = false);
  virtual ~ObBloomFilterDataReader();
  void reset();
  void reuse();
  int read_bloom_filter(
      const MacroBlockId &macro_id,
      ObBloomFilterCacheValue &bf_cache_value);
private:
  ObBloomFilterMacroBlockReader bf_macro_reader_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_READER_H_
