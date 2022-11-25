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

#ifndef OB_ALL_VIRTUAL_DTL_MEMORY_H
#define OB_ALL_VIRTUAL_DTL_MEMORY_H

#include "sql/dtl/ob_dtl_channel.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"
#include "sql/dtl/ob_dtl_fc_server.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualDtlMemoryPoolInfo
{
public:
  ObAllVirtualDtlMemoryPoolInfo() :
    tenant_id_(0), channel_total_cnt_(0), channel_block_cnt_(0), max_parallel_cnt_(0), max_blocked_buffer_size_(0),
    accumulated_block_cnt_(0), current_buffer_used_(0), seqno_(0), alloc_cnt_(0), free_cnt_(0),
    free_queue_len_(0), total_memory_size_(0), real_alloc_cnt_(0), real_free_cnt_(0)
  {}

  void set_mem_pool_info(sql::dtl::ObTenantDfc *&tenant_dfc, sql::dtl::ObDtlChannelMemManager *mgr);

  TO_STRING_KV(K(tenant_id_), K(seqno_));

public:
  uint64_t tenant_id_;                // 1
  int64_t channel_total_cnt_;
  int64_t channel_block_cnt_;
  int64_t max_parallel_cnt_;
  int64_t max_blocked_buffer_size_;   // 5
  int64_t accumulated_block_cnt_;
  int64_t current_buffer_used_;
  int64_t seqno_;
  int64_t alloc_cnt_;
  int64_t free_cnt_;                  // 10
  int64_t free_queue_len_;
  int64_t total_memory_size_;
  int64_t real_alloc_cnt_;
  int64_t real_free_cnt_;             // 14
};

class ObAllVirtualDtlMemoryIterator
{
public:
  ObAllVirtualDtlMemoryIterator(common::ObArenaAllocator *iter_allocator);
  virtual ~ObAllVirtualDtlMemoryIterator();

  void destroy();
  void reset();

  int init();
  int get_tenant_ids();
  int get_next_memory_pools();
  int get_tenant_memory_pool_infos(uint64_t tenant_id);

  int get_next_mem_pool_info(ObAllVirtualDtlMemoryPoolInfo &memory_pool_info);
private:
  int64_t cur_tenant_idx_;
  int64_t cur_mem_pool_idx_;
  common::ObArenaAllocator *iter_allocator_;
  common::ObArray<uint64_t> tenant_ids_;
  common::ObArray<ObAllVirtualDtlMemoryPoolInfo, common::ObWrapperAllocator> mem_pool_infos_;
};

class ObAllVirtualDtlMemory : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDtlMemory();
  virtual ~ObAllVirtualDtlMemory();

  void destroy();
  void reset();
  int inner_open();
  int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_row(ObAllVirtualDtlMemoryPoolInfo &mem_pool_info, common::ObNewRow *&row);
private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    CHANNEL_TOTAL_CNT,
    CHANNEL_BLOCK_CNT,
    MAX_PARALLEL_CNT,         // OB_APP_MIN_COLUMN_ID + 5
    MAX_BLOCKED_BUFFER_SIZE,
    ACCUMULATED_BLOCK_CNT,
    CURRENT_BUFFER_USED,
    SQENO,
    ALLOC_CNT,               // OB_APP_MIN_COLUMN_ID + 10
    FREE_CNT,
    FREE_QUEUE_LEN,
    TOTAL_MEMORY_SIZE,
    REAL_ALLOC_CNT,
    REAL_FREE_CNT,            // OB_APP_MIN_COLUMN_ID + 15
  };

private:
  common::ObString ipstr_;
  int32_t port_;
  common::ObArenaAllocator arena_allocator_;
  ObAllVirtualDtlMemoryIterator iter_;
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_DTL_MEMORY_H */