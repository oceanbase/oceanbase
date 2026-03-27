/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
