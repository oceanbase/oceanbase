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

#ifndef OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_
#define OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace obrpc
{
class ObRpcMemPool
{
public:
  enum { RPC_POOL_PAGE_SIZE = 1<<16 };
  struct Page;
  ObRpcMemPool(): last_(NULL) {}
  ~ObRpcMemPool() { destroy(); }
  static ObRpcMemPool* create(int64_t sz);
  void* alloc(int64_t sz);
  void reuse();
  void destroy();
private:
  void add_page(Page* page);
private:
  Page* last_;
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_ */
