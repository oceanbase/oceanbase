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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_FETCH_MGR_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_FETCH_MGR_H__

#include "ob_log_part_fetch_ctx.h"              // PartFetchCtx, PartFetchInfoForPrint

#include "share/ob_define.h"                    // OB_SERVER_TENANT_ID
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/container/ob_array.h"             // ObArray
#include "lib/allocator/ob_safe_arena.h"        // ObSafeArena
#include "common/ob_partition_key.h"            // ObPartitionKey

#include "ob_log_config.h"                      // ObLogConfig

namespace oceanbase
{
namespace liboblog
{

// Partition fetch manager
class IObLogPartFetchMgr
{
public:
  typedef common::ObArray<PartFetchInfoForPrint> PartFetchInfoArray;
  typedef common::ObArray<common::ObPartitionKey> PartArray;

public:
  virtual ~IObLogPartFetchMgr() {}

public:
  /// add a new partition
  virtual int add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id) = 0;

  /// recycle a partition
  /// mark partition deleted and begin recycle resource
  virtual int recycle_partition(const common::ObPartitionKey &pkey) = 0;

  /// remove partition
  /// delete partition by physical
  virtual int remove_partition(const common::ObPartitionKey &pkey) = 0;

  /// get part fetch context
  virtual int get_part_fetch_ctx(const common::ObPartitionKey &pkey, PartFetchCtx *&ctx) = 0;

  /// get the slowest k partition
  virtual void print_k_slowest_partition() = 0;

  // check partition split state
  virtual int check_part_split_state(const common::ObPartitionKey &pkey,
      const uint64_t split_log_id,
      const int64_t split_log_ts,
      bool &split_done) = 0;

  // active split target partitin
  // partition split is done and notify partitin dispatch data
  virtual int activate_split_dest_part(const common::ObPartitionKey &pkey,
      volatile bool &stop_flag) = 0;

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version) = 0;
};

///////////////////////////////////////////////////////////////////////////////////////////////////

class PartProgressController;
class IObLogPartTransResolverFactory;
template <typename T> class ObLogTransTaskPool;

class ObLogPartFetchMgr : public IObLogPartFetchMgr
{
  // static golbal class variable
  static int64_t g_print_slowest_part_num;

  static const int64_t PART_FETCH_CTX_POOL_BLOCK_SIZE = 1L << 24;
  static const uint64_t DEFAULT_TENANT_ID = common::OB_SERVER_TENANT_ID;

  typedef common::ObSmallObjPool<PartFetchCtx> PartFetchCtxPool;
  typedef common::ObLinearHashMap<common::ObPartitionKey, PartFetchCtx *> PartFetchCtxMap;

public:
  ObLogPartFetchMgr();
  virtual ~ObLogPartFetchMgr();

public:
  int init(const int64_t max_cached_part_fetch_ctx_count,
      PartProgressController &progress_controller,
      IObLogPartTransResolverFactory &part_trans_resolver_factory);
  void destroy();

public:
  virtual int add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id);
  virtual int recycle_partition(const common::ObPartitionKey &pkey);
  virtual int remove_partition(const common::ObPartitionKey &pkey);
  virtual int get_part_fetch_ctx(const common::ObPartitionKey &pkey, PartFetchCtx *&ctx);
  virtual void print_k_slowest_partition();
  virtual int check_part_split_state(const common::ObPartitionKey &pkey,
      const uint64_t split_log_id,
      const int64_t split_log_ts,
      bool &split_done);
  virtual int activate_split_dest_part(const common::ObPartitionKey &pkey,
      volatile bool &stop_flag);
  virtual int set_start_global_trans_version(const int64_t start_global_trans_version);

  template <typename Func> int for_each_part(Func &func)
  {
    return ctx_map_.for_each(func);
  }

public:
  static void configure(const ObLogConfig & config);

private:
  int init_pkey_info_(const common::ObPartitionKey &pkey,
      char *&pkey_str);
  struct CtxRecycleCond
  {
    bool operator() (const common::ObPartitionKey &pkey, PartFetchCtx *&ctx);
  };

  struct CtxPartProgressCond
  {
    CtxPartProgressCond() : ctx_cnt_(0), part_fetch_info_array_() {}
    int init(const int64_t count);
    bool operator() (const common::ObPartitionKey &pkey, PartFetchCtx *ctx);

    int64_t ctx_cnt_;
    PartFetchInfoArray part_fetch_info_array_;
  };

  // do top-k
  int find_k_slowest_partition_(const PartFetchInfoArray &part_fetch_ctx_array,
      const int64_t slowest_part_num,
      PartFetchInfoArray &slow_part_array);

  class FetchProgressCompFunc
  {
  public:
    bool operator() (const PartFetchInfoForPrint &a, const PartFetchInfoForPrint &b)
    {
      return a.get_progress() < b.get_progress();
    }
  };

  class DispatchProgressCompFunc
  {
  public:
    bool operator() (const PartFetchInfoForPrint &a, const PartFetchInfoForPrint &b)
    {
      return a.get_dispatch_progress() < b.get_dispatch_progress();
    }
  };

  struct PartSplitStateChecker
  {
    bool operator() (const common::ObPartitionKey &pkey, PartFetchCtx *&ctx);

    PartSplitStateChecker(const common::ObPartitionKey &pkey, const uint64_t split_log_id,
        const int64_t split_log_ts) :
        pkey_(pkey),
        split_done_(false),
        split_log_id_(split_log_id),
        split_log_ts_(split_log_ts)
    {}

    const common::ObPartitionKey  &pkey_;         // partition key of split source partition
    bool                          split_done_;
    uint64_t                      split_log_id_;  // log id of partition split log in source partition
    int64_t                       split_log_ts_;  // timestamp of source partition(in partition split)
  };

  struct ActivateSplitDestPartFunc
  {
    bool operator() (const common::ObPartitionKey &pkey, PartFetchCtx *ctx);
    ActivateSplitDestPartFunc(const common::ObPartitionKey &pkey, volatile bool &stop_flag) :
        err_code_(0),
        pkey_(pkey),
        stop_flag_(stop_flag)
    {}

    int                           err_code_;
    const common::ObPartitionKey  &pkey_;         // partition key of target partition for partitin split
    volatile bool                 &stop_flag_;
  };

private:
  bool                            inited_;
  PartProgressController          *progress_controller_;
  IObLogPartTransResolverFactory  *part_trans_resolver_factory_;

  PartFetchCtxMap                 ctx_map_;
  PartFetchCtxPool                ctx_pool_;
  // 1. pkey's serialized string is maintained globally, and cannot be placed in modules such as PartTransDispatcher,
  // because after partition deletion, when the partition has been ordered, the partition context may be cleaned up,
  // but the Reader still needs to read data based on pkey_str
  // 2. TODO supports pkey deletion and recycling
  common::ObSafeArena             pkey_serialize_allocator_;

  int64_t                         start_global_trans_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartFetchMgr);
};

}
}
#endif
