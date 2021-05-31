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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_addrs_provider_factory.h"
#include "sql/executor/ob_random_addrs_provider.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObAddrsProviderFactory::ObAddrsProviderFactory() : store_()
{}

ObAddrsProviderFactory::~ObAddrsProviderFactory()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObAddrsProvider* ap = store_.at(i);
    if (OB_LIKELY(NULL != ap)) {
      ap->~ObAddrsProvider();
    }
  }
}

void ObAddrsProviderFactory::reset()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObAddrsProvider* ap = store_.at(i);
    if (OB_LIKELY(NULL != ap)) {
      ap->~ObAddrsProvider();
    }
  }
  store_.reset();
}

int ObAddrsProviderFactory::create(ObExecContext& exec_ctx, int provider_type, ObAddrsProvider*& servers_provider)
{
  int ret = OB_SUCCESS;
  ObIAllocator& allocator = exec_ctx.get_allocator();
  void* ptr = NULL;
  switch (provider_type) {
    case ObAddrsProvider::RANDOM_PROVIDER: {
      ObAddrsProvider* ap = NULL;
      ptr = allocator.alloc(sizeof(ObRandomAddrsProvider));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc ObRandomAddrsProvider", K(ret));
      } else if (OB_ISNULL(ap = new (ptr) ObRandomAddrsProvider())) {
        LOG_WARN("fail to new ObRandomAddrsProvider", K(ret));
      } else if (OB_FAIL(store_.push_back(ap))) {
        LOG_WARN("fail to push back ObAddrsProvider ptr to store", K(ret));
        ap->~ObAddrsProvider();
      } else {
        servers_provider = ap;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected provider type", K(ret), K(provider_type));
      break;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
