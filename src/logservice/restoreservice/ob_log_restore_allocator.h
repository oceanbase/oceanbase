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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_ALLOCATOR_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_ALLOCATOR_H_

#include "lib/utility/ob_macro_utils.h"
#include "logservice/archiveservice/large_buffer_pool.h"
#include <cstdint>
namespace oceanbase
{
namespace logservice
{
class ObLogRestoreAllocator
{
public:
  ObLogRestoreAllocator();
  ~ObLogRestoreAllocator();

  int init(const uint64_t tenant_id);
  void destroy();

public:
  archive::LargeBufferPool *get_buferr_pool() { return &iterator_buf_allocator_; }
  char *alloc_iterator_buffer(const int64_t size);
  void free_iterator_buffer(void *buf);

  void weed_out_iterator_buffer();

private:
  bool inited_;
  uint64_t tenant_id_;
  archive::LargeBufferPool iterator_buf_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreAllocator);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_ALLOCATOR_H_ */
