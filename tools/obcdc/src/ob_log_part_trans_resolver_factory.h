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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_RESOLVER_FACTORY_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_RESOLVER_FACTORY_H__

#include "ob_log_part_trans_resolver.h"           // IObLogPartTransResolver, PartTransTaskMap

#include "lib/allocator/ob_small_allocator.h"     // ObSmallAllocator

namespace oceanbase
{
namespace liboblog
{

class IObLogPartTransResolverFactory
{
public:
  virtual ~IObLogPartTransResolverFactory() {}

public:
  virtual int alloc(const char *pkey_str, IObLogPartTransResolver *&ptr) = 0;
  virtual void free(IObLogPartTransResolver *ptr) = 0;
};

/////////////////////////////////////////////////////////////////////

typedef ObLogTransTaskPool<PartTransTask> TaskPool;
class IObLogEntryTaskPool;
class IObLogFetcherDispatcher;
class IObLogClusterIDFilter;

class ObLogPartTransResolverFactory : public IObLogPartTransResolverFactory
{
  static const int64_t DEFAULT_BLOCK_SIZE = (1L << 24);

public:
  ObLogPartTransResolverFactory();
  virtual ~ObLogPartTransResolverFactory();

public:
  int init(TaskPool &task_pool,
      IObLogEntryTaskPool &log_entry_task_pool,
      IObLogFetcherDispatcher &dispatcher,
      IObLogClusterIDFilter &cluster_id_filter);
  void destroy();

public:
  virtual int alloc(const char *pkey_str, IObLogPartTransResolver *&ptr);
  virtual void free(IObLogPartTransResolver *ptr);

  struct TransInfoClearerByCheckpoint
  {
    int64_t checkpoint_;
    int64_t purge_count_;

    explicit TransInfoClearerByCheckpoint(const int64_t checkpoint) : checkpoint_(checkpoint), purge_count_(0)
    {}
    bool operator()(const transaction::ObTransID &key, TransCommitInfo &trans_commit_info);
  };
  void gc_commit_trans_info(const int64_t checkpoint);

private:
  bool                      inited_;
  TaskPool                  *task_pool_;
  IObLogEntryTaskPool       *log_entry_task_pool_;
  IObLogFetcherDispatcher   *dispatcher_;
  IObLogClusterIDFilter     *cluster_id_filter_;

  common::ObSmallAllocator  allocator_;
  PartTransTaskMap          task_map_;
  TransCommitMap            trans_commit_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartTransResolverFactory);
};

}
}

#endif
