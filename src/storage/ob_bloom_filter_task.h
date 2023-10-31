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

#ifndef OCEANBASE_STORAGE_OB_BLOOM_FILTER_TASK_H_
#define OCEANBASE_STORAGE_OB_BLOOM_FILTER_TASK_H_

#include "lib/queue/ob_dedup_queue.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

class ObBloomFilterBuildTask: public common::IObDedupTask
{
public:
  ObBloomFilterBuildTask(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const blocksstable::MacroBlockId &macro_id,
      const int64_t prefix_len);
  virtual ~ObBloomFilterBuildTask();
  virtual int64_t hash() const;
  virtual bool operator ==(const IObDedupTask &other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask *deep_copy(char *buffer, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const {  return 0;  }
  virtual int process();
private:
  int build_bloom_filter();
  uint64_t tenant_id_;
  uint64_t table_id_;
  blocksstable::MacroBlockId macro_id_;
  blocksstable::ObMacroBlockHandle macro_handle_;
  int64_t prefix_len_;
  common::ObArenaAllocator allocator_;
  char *io_buf_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterBuildTask);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_BLOOM_FILTER_TASK_H_ */
