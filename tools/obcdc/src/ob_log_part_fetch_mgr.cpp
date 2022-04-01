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

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_log_part_fetch_mgr.h"

#include "share/ob_errno.h"                     // OB_SUCCESS, ..
#include "lib/oblog/ob_log_module.h"            // LOG_*
#include "lib/allocator/ob_mod_define.h"        // ObModIds

#include "ob_log_part_progress_controller.h"    // PartProgressController
#include "ob_log_part_trans_resolver_factory.h" // IObLogPartTransResolverFactory
#include <algorithm>

#define FREE_CTX(ctx, pool) \
    do {\
      int free_ret = OB_SUCCESS; \
      if (NULL != ctx) { \
        ctx->reset(); \
        free_ret = pool.free(ctx); \
        if (OB_SUCCESS != free_ret) { \
          LOG_ERROR("free PartFetchCtx fail", K(free_ret), K(ctx), KPC(ctx)); \
        } else { \
          ctx = NULL; \
        } \
      } \
    } while (0)

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
int64_t ObLogPartFetchMgr::g_print_slowest_part_num = ObLogConfig::default_print_fetcher_slowest_part_num;

ObLogPartFetchMgr::ObLogPartFetchMgr() :
    inited_(false),
    progress_controller_(NULL),
    part_trans_resolver_factory_(NULL),
    ctx_map_(),
    ctx_pool_(),
    pkey_serialize_allocator_("PkeySerialize"),
    start_global_trans_version_(OB_INVALID_TIMESTAMP)
{
}

ObLogPartFetchMgr::~ObLogPartFetchMgr()
{
  destroy();
}

int ObLogPartFetchMgr::init(const int64_t max_cached_part_fetch_ctx_count,
    PartProgressController &progress_controller,
    IObLogPartTransResolverFactory &part_trans_resolver_factory)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(max_cached_part_fetch_ctx_count <= 0)) {
    LOG_ERROR("invalid argument", K(max_cached_part_fetch_ctx_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_.init(ObModIds::OB_LOG_PART_FETCH_CTX_MAP))) {
    LOG_ERROR("init PartFetchCtxMap fail", KR(ret));
  } else if (OB_FAIL(ctx_pool_.init(max_cached_part_fetch_ctx_count,
      ObModIds::OB_LOG_PART_FETCH_CTX_POOL,
      DEFAULT_TENANT_ID,
      PART_FETCH_CTX_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init PartFetchCtxPool fail", KR(ret), K(max_cached_part_fetch_ctx_count),
        LITERAL_K(PART_FETCH_CTX_POOL_BLOCK_SIZE));
  } else {
    progress_controller_ = &progress_controller;
    part_trans_resolver_factory_ = &part_trans_resolver_factory;
    start_global_trans_version_ = OB_INVALID_TIMESTAMP;
    inited_ = true;

    LOG_INFO("init part fetch mgr succ", K(max_cached_part_fetch_ctx_count));
  }
  return ret;
}

void ObLogPartFetchMgr::destroy()
{
  // TODO: recycle all task in map

  inited_ = false;
  progress_controller_ = NULL;
  part_trans_resolver_factory_ = NULL;
  (void)ctx_map_.destroy();
  ctx_pool_.destroy();
  pkey_serialize_allocator_.clear();
  start_global_trans_version_ = OB_INVALID_TIMESTAMP;
  LOG_INFO("destroy part fetch mgr succ");
}

int ObLogPartFetchMgr::add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id)
{
  int ret = OB_SUCCESS;
  PartFetchCtx *ctx = NULL;
  int64_t progress_id = -1;
  IObLogPartTransResolver *part_trans_resolver = NULL;
  char *pkey_str = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  }
  // start timestamp must be valid!
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_tstamp)) {
    LOG_ERROR("invalid start tstamp", K(start_tstamp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(progress_controller_)
      || OB_ISNULL(part_trans_resolver_factory_)) {
    LOG_ERROR("invalid handlers", K(progress_controller_), K(part_trans_resolver_factory_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(init_pkey_info_(pkey, pkey_str))) {
    LOG_ERROR("init_pkey_info_ fail", KR(ret), K(pkey), K(pkey_str));
  }
  // alloc a part trans resolver
  else if (OB_FAIL(part_trans_resolver_factory_->alloc(pkey_str, part_trans_resolver))) {
    LOG_ERROR("alloc IObLogPartTransResolver fail", KR(ret), K(pkey_str));
  } else if (OB_ISNULL(part_trans_resolver)) {
    LOG_ERROR("invalid part_trans_resolver", K(part_trans_resolver));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(part_trans_resolver->init(pkey, start_tstamp, start_global_trans_version_))) {
    LOG_ERROR("init part trans resolver fail", KR(ret), K(pkey), K(start_tstamp),
        K(start_global_trans_version_));
  }
  // alloc a PartFetchCtx
  else if (OB_FAIL(ctx_pool_.alloc(ctx))) {
    LOG_ERROR("alloc PartFetchCtx fail", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    LOG_ERROR("PartFetchCtx is NULL", K(ctx));
    ret = OB_ERR_UNEXPECTED;
  }
  // alloc a progress id which should be unique for each partition
  else if (OB_FAIL(progress_controller_->acquire_progress(progress_id, start_tstamp))) {
    LOG_ERROR("acquire_progress fail", KR(ret), K(start_tstamp));
  // init PartFetchCtx
  } else if (OB_FAIL(ctx->init(pkey, start_tstamp, start_log_id, progress_id, *part_trans_resolver, *this))) {
    LOG_ERROR("ctx init fail", KR(ret), K(pkey), K(start_tstamp), K(start_log_id), K(progress_id));
  } else {

    if (OB_FAIL(ctx_map_.insert(pkey, ctx))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_ERROR("partition has been added", KR(ret), K(pkey), K(start_tstamp), K(start_log_id));
      } else {
        LOG_ERROR("insert into map fail", KR(ret), K(pkey), K(ctx));
      }
    } else {
      _LOG_INFO("[STAT] [PartFetchMgr] [ADD_PART] pkey=%s start_log_id=%lu start_tstamp=%ld(%s) "
          "progress_id=%ld fetch_task=%p part_trans_resolver=%p",
          to_cstring(pkey), start_log_id, start_tstamp, TS_TO_STR(start_tstamp),
          progress_id, ctx, part_trans_resolver);
    }
  }

  if (OB_SUCCESS != ret) {
    // recycle progress id, delete from global_progress_controller and should not effect progress of normal partition
    int release_ret = OB_SUCCESS;

    if (-1 != progress_id && NULL != progress_controller_) {
      if (OB_UNLIKELY(OB_SUCCESS != (release_ret = progress_controller_->release_progress(progress_id)))) {
        LOG_ERROR("release progress fail", K(release_ret), K(progress_id), K(pkey), K(ctx),
            KPC(ctx));
      } else {
        progress_id = -1;
      }
    }

    if (NULL != ctx) {
      FREE_CTX(ctx, ctx_pool_);
    }

    if (NULL != part_trans_resolver) {
      part_trans_resolver_factory_->free(part_trans_resolver);
      part_trans_resolver = NULL;
    }
  }

  return ret;
}

int ObLogPartFetchMgr::init_pkey_info_(const common::ObPartitionKey &pkey,
    char *&pkey_str)
{
  int ret = OB_SUCCESS;
  pkey_str = NULL;
  // 1024 is enough for seriailized pkey
  static const int64_t PKEY_BUF_SIZE = 1024;
  char pkey_str_buf[PKEY_BUF_SIZE];
  int64_t pkey_local_buf_pos = pkey.to_string(pkey_str_buf, PKEY_BUF_SIZE);

  if (OB_UNLIKELY(pkey_local_buf_pos <= 0 || pkey_local_buf_pos >= PKEY_BUF_SIZE)) {
    LOG_ERROR("pkey_local_buf_pos is not valid", K(pkey_local_buf_pos), K(pkey));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t buf_len = pkey_local_buf_pos + 1;
    pkey_str = static_cast<char*>(pkey_serialize_allocator_.alloc(buf_len));

    if (OB_ISNULL(pkey_str)) {
      LOG_ERROR("allocator_ alloc for pkey str fail", K(pkey_str), K(buf_len));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(pkey_str, pkey_str_buf, pkey_local_buf_pos);
      pkey_str[pkey_local_buf_pos] = '\0';
    }
  }

  return ret;
}

bool ObLogPartFetchMgr::CtxRecycleCond::operator() (const common::ObPartitionKey &pkey,
    PartFetchCtx *&ctx)
{
  bool bool_ret = false;
  if (OB_ISNULL(ctx)) {
    LOG_ERROR("invalid part fetch ctx", K(ctx), K(pkey));
    bool_ret = false;
  } else {
    _LOG_INFO("[STAT] [PartFetchMgr] [RECYCLE_PART] pkey=%s "
        "fetch_task=%p fetch_task=%s",
        to_cstring(pkey), ctx, to_cstring(*ctx));

    // modify partitin status to DISCARDED
    ctx->set_discarded();

    bool_ret = true;
  }

  return bool_ret;
}

int ObLogPartFetchMgr::recycle_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  CtxRecycleCond recycle_cond;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_map_.operate(pkey, recycle_cond))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // as expected
    } else {
      LOG_ERROR("operate on ctx map fail", KR(ret), K(pkey));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogPartFetchMgr::remove_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(progress_controller_) || OB_ISNULL(part_trans_resolver_factory_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_), K(part_trans_resolver_factory_));
    ret = OB_INVALID_ERROR;
  } else {
    PartFetchCtx *fetch_ctx = NULL;

    // remove node from map first to guarantee the correctness of the concurrent operation on the map
    if (OB_FAIL(ctx_map_.erase(pkey, fetch_ctx))) {
      LOG_ERROR("erase PartFetchCtx from map fail", KR(ret), K(pkey));
    } else if (OB_ISNULL(fetch_ctx)) {
      LOG_ERROR("PartFetchCtx is NULL, unexcepted error", K(pkey), K(fetch_ctx));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(! fetch_ctx->is_discarded())) {
      LOG_ERROR("partition is not discarded, can not remove", K(pkey), KPC(fetch_ctx));
      ret = OB_ERR_UNEXPECTED;
    } else {
      IObLogPartTransResolver *ptr = fetch_ctx->get_part_trans_resolver();
      int64_t progress_id = fetch_ctx->get_progress_id();

      _LOG_INFO("[STAT] [PartFetchMgr] [REMOVE_PART] pkey=%s progress_id=%ld "
          "fetch_task=%p part_trans_resolver=%p fetch_task=%s",
          to_cstring(pkey), progress_id, fetch_ctx, ptr, to_cstring(*fetch_ctx));

      // recycle progress id, delete from global progress_controller
      int release_ret = progress_controller_->release_progress(progress_id);
      if (OB_UNLIKELY(OB_SUCCESS != release_ret)) {
        LOG_ERROR("release progress fail", K(release_ret), K(progress_id), K(pkey), K(fetch_ctx),
            KPC(fetch_ctx));
      }

      if (NULL != fetch_ctx) {
        FREE_CTX(fetch_ctx, ctx_pool_);
      }

      if (NULL != ptr) {
        part_trans_resolver_factory_->free(ptr);
        ptr = NULL;
      }
    }
  }

  return ret;
}

int ObLogPartFetchMgr::get_part_fetch_ctx(const common::ObPartitionKey &pkey, PartFetchCtx *&ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_map_.get(pkey, ctx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // pkey not exist in map
      ctx = NULL;
    } else {
      LOG_ERROR("get PartFetchCtx from map fail", KR(ret), K(pkey));
    }
  } else if (OB_ISNULL(ctx)) {
    LOG_ERROR("PartFetchCtx is NULL", K(ctx));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // succ
  }
  return ret;
}

int ObLogPartFetchMgr::CtxPartProgressCond::init(const int64_t count)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(part_fetch_info_array_.reserve(count))) {
    LOG_ERROR("reserve array fail", KR(ret), K(count));
  } else {
    ctx_cnt_ = 0;
  }

  return ret;
}

bool ObLogPartFetchMgr::CtxPartProgressCond::operator() (const common::ObPartitionKey &pkey,
    PartFetchCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (NULL != ctx) {
    PartFetchInfoForPrint part_fetch_info;

    if (OB_FAIL(part_fetch_info.init(*ctx))) {
      LOG_ERROR("init part_fetch_info fail", KR(ret), K(pkey), KPC(ctx));
    } else if(OB_FAIL(part_fetch_info_array_.push_back(part_fetch_info))) {
      LOG_ERROR("part_progress_array_ push back fail", KR(ret), K(pkey), KPC(ctx), K(ctx_cnt_));
    } else {
      ctx_cnt_++;
    }
  }

  return OB_SUCCESS == ret;
}

void ObLogPartFetchMgr::print_k_slowest_partition()
{
  int ret = OB_SUCCESS;
  PartFetchInfoArray fetch_slow_array;       // array of partitions with slowest log fetch progress
  PartFetchInfoArray dispatch_slow_array;    // array of partitions with slowest task dispatch progress
  int64_t slowest_part_num = ATOMIC_LOAD(&g_print_slowest_part_num);
  int64_t part_num = ctx_map_.count();
  CtxPartProgressCond part_progress_cond;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (part_num > 0) {
    if (OB_FAIL(part_progress_cond.init(part_num))) {
      LOG_ERROR("part progree cond init fail", KR(ret), K(part_num));
    } else if (OB_FAIL(ctx_map_.for_each(part_progress_cond))) {
      LOG_ERROR("ctx_map_ for_each fail", KR(ret));
    } else {
      const PartFetchInfoArray &part_fetch_info_array = part_progress_cond.part_fetch_info_array_;
      FetchProgressCompFunc fetch_progress_comp_func;
      DispatchProgressCompFunc dispatch_progress_comp_func;

      // get TOP-K partition with slowest log fetch progress
      if (OB_FAIL(top_k(part_fetch_info_array, slowest_part_num, fetch_slow_array,
          fetch_progress_comp_func))) {
        LOG_ERROR("find the k slowest partition fail", KR(ret), K(slowest_part_num),
            K(fetch_slow_array));
      }
      // get TOP-K partition with slowest task dispatch progress
      else if (OB_FAIL(top_k(part_fetch_info_array, slowest_part_num, dispatch_slow_array,
          dispatch_progress_comp_func))) {
        LOG_ERROR("find the k slowest partition fail", KR(ret), K(slowest_part_num),
            K(dispatch_slow_array));
      } else {
        int64_t cur_time = get_timestamp();
        int64_t array_cnt = fetch_slow_array.count();

        // ************************ Print the K partitions with the slowest fetch log progress
        _LOG_INFO("[STAT] slow fetch progress start. part_count=%ld/%ld", array_cnt,
            part_fetch_info_array.count());

        for (int64_t idx = 0; OB_SUCCESS == ret && idx < array_cnt; ++idx) {
          fetch_slow_array.at(idx).print_fetch_progress("slow fetch progress",
              idx, array_cnt, cur_time);
        }

        LOG_INFO("[STAT] slow fetch progress end");

        // ************************ Print the K partitions with the slowest part dispatch  progress
        array_cnt = dispatch_slow_array.count();
        _LOG_INFO("[STAT] slow dispatch progress start. part_count=%ld/%ld", array_cnt,
            part_fetch_info_array.count());

        for (int64_t idx = 0; OB_SUCCESS == ret && idx < array_cnt; ++idx) {
          dispatch_slow_array.at(idx).print_dispatch_progress("slow dispatch progress",
              idx, array_cnt, cur_time);
        }

        LOG_INFO("[STAT] slow dispatch progress end");
      }
    }
  }
}

bool ObLogPartFetchMgr::PartSplitStateChecker::operator() (const common::ObPartitionKey &pkey,
    PartFetchCtx *&ctx)
{
  bool bool_ret = false;
  if (pkey_ == pkey) {
    bool_ret = true;
    if (NULL != ctx) {
      split_done_ = ctx->is_split_done(split_log_id_, split_log_ts_);
    }
  }
  return bool_ret;
}

int ObLogPartFetchMgr::check_part_split_state(const common::ObPartitionKey &pkey,
    const uint64_t split_log_id,
    const int64_t split_log_ts,
    bool &split_done)
{
  int ret = OB_SUCCESS;
  PartSplitStateChecker checker(pkey, split_log_id, split_log_ts);
  split_done = false;

  if (OB_FAIL(ctx_map_.operate(pkey, checker))) {
    // consider partition already split if partitin not exist
    if (OB_ENTRY_NOT_EXIST == ret) {
      split_done = true;
      ret = OB_SUCCESS;
      LOG_INFO("[STAT] [SPLIT] [CHECK_STATE] partition not exist, "
          "it must have been split done", K(pkey), K(split_log_id), K(split_log_ts));
    } else {
      LOG_ERROR("operate on ctx map fail", KR(ret), K(pkey));
    }
  } else {
    split_done = checker.split_done_;
  }
  return ret;
}

bool ObLogPartFetchMgr::ActivateSplitDestPartFunc::operator() (const common::ObPartitionKey &pkey, PartFetchCtx *ctx)
{
  int ret = OB_SUCCESS;
  err_code_ = OB_SUCCESS;
  if (pkey_ == pkey) {
    if (NULL != ctx) {
      if (OB_FAIL(ctx->handle_when_src_split_done(stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle_when_src_split_done fail", KR(ret), K(pkey), KPC(ctx));
        }
      }
    }
  }
  err_code_ = ret;
  return true;
}

int ObLogPartFetchMgr::activate_split_dest_part(const ObPartitionKey &pkey,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ActivateSplitDestPartFunc func(pkey, stop_flag);
  if (OB_FAIL(ctx_map_.operate(pkey, func))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("[STAT] [SPLIT] [ACTIVATE_DEST] split dest partition not exist", K(pkey));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("operate on ctx map fail", KR(ret), K(pkey));
    }
  } else {
    // return error code
    ret = func.err_code_;
  }
  return ret;
}

int ObLogPartFetchMgr::set_start_global_trans_version(const int64_t start_global_trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_global_trans_version)) {
    LOG_ERROR("invalid argument", K(start_global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    start_global_trans_version_ = start_global_trans_version;
  }

  return ret;
}

void ObLogPartFetchMgr::configure(const ObLogConfig & config)
{
  int64_t print_slowest_part_num = config.print_fetcher_slowest_part_num;

  ATOMIC_STORE(&g_print_slowest_part_num, print_slowest_part_num);
  LOG_INFO("[CONFIG]", K(print_slowest_part_num));
}

}
}
