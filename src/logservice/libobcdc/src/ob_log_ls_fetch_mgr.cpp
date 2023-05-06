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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_ls_fetch_mgr.h"

#include "share/ob_errno.h"                     // OB_SUCCESS, ..
#include "lib/oblog/ob_log_module.h"            // LOG_*
#include "lib/container/ob_array_iterator.h"
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
          LOG_ERROR("free LSFetchCtx fail", K(free_ret), K(ctx), KPC(ctx)); \
        } else { \
          ctx = NULL; \
        } \
      } \
    } while (0)

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
int64_t ObLogLSFetchMgr::g_print_slowest_ls_num = ObLogConfig::default_print_fetcher_slowest_ls_num;

ObLogLSFetchMgr::ObLogLSFetchMgr() :
    is_inited_(false),
    fetcher_(nullptr),
    progress_controller_(NULL),
    part_trans_resolver_factory_(NULL),
    ctx_map_(),
    ctx_pool_(),
    tls_info_serialize_allocator_("TLSIDSerialize"),
    start_global_trans_version_(OB_INVALID_TIMESTAMP)
{
}

ObLogLSFetchMgr::~ObLogLSFetchMgr()
{
  destroy();
}

int ObLogLSFetchMgr::init(
    const int64_t max_cached_ls_fetch_ctx_count,
    PartProgressController &progress_controller,
    IObLogPartTransResolverFactory &part_trans_resolver_factory,
    void *fetcher_host)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_UNLIKELY(max_cached_ls_fetch_ctx_count <= 0)
      || OB_ISNULL(fetcher_host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(max_cached_ls_fetch_ctx_count), K(fetcher_host));
  } else if (OB_FAIL(ctx_map_.init(ObModIds::OB_LOG_PART_FETCH_CTX_MAP))) {
    LOG_ERROR("init LSFetchCtxMap fail", KR(ret));
  } else if (OB_FAIL(ctx_pool_.init(max_cached_ls_fetch_ctx_count,
      ObModIds::OB_LOG_PART_FETCH_CTX_POOL,
      DEFAULT_TENANT_ID,
      PART_FETCH_CTX_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init LSFetchCtxPool fail", KR(ret), K(max_cached_ls_fetch_ctx_count),
        LITERAL_K(PART_FETCH_CTX_POOL_BLOCK_SIZE));
  } else {
    progress_controller_ = &progress_controller;
    part_trans_resolver_factory_ = &part_trans_resolver_factory;
    start_global_trans_version_ = OB_INVALID_TIMESTAMP;
    fetcher_ = fetcher_host;
    is_inited_ = true;

    LOG_INFO("init LS fetch mgr succ", K(max_cached_ls_fetch_ctx_count));
  }

  return ret;
}

void ObLogLSFetchMgr::destroy()
{
  // TODO: recycle all task in map

  is_inited_ = false;
  fetcher_ = nullptr;
  progress_controller_ = NULL;
  part_trans_resolver_factory_ = NULL;
  (void)ctx_map_.destroy();
  ctx_pool_.destroy();
  tls_info_serialize_allocator_.clear();
  start_global_trans_version_ = OB_INVALID_TIMESTAMP;
  LOG_INFO("destroy LS fetch mgr succ");
}

int ObLogLSFetchMgr::add_ls(
    const logservice::TenantLSID &tls_id,
    const logfetcher::ObLogFetcherStartParameters &start_parameters,
    const bool is_loading_data_dict_baseline_data,
    const ClientFetchingMode fetching_mode,
    const ObBackupPathString &archive_dest_str)
{
  int ret = OB_SUCCESS;
  LSFetchCtx *ctx = NULL;
  int64_t progress_id = -1;
  IObCDCPartTransResolver *part_trans_resolver = NULL;
  char *tls_id_str = NULL;
  const int64_t start_tstamp_ns = start_parameters.get_start_tstamp_ns();
  const palf::LSN &start_lsn = start_parameters.get_start_lsn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  }
  // start timestamp must be valid!
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_tstamp_ns)) {
    LOG_ERROR("invalid start tstamp", K(start_tstamp_ns));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(progress_controller_)
      || OB_ISNULL(part_trans_resolver_factory_)) {
    LOG_ERROR("invalid handlers", K(progress_controller_), K(part_trans_resolver_factory_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(init_tls_info_(tls_id, tls_id_str))) {
    LOG_ERROR("init_tls_info_ failed", KR(ret), K(tls_id), K(tls_id_str));
  }
  // alloc a part trans resolver
  else if (OB_FAIL(part_trans_resolver_factory_->alloc(tls_id_str, part_trans_resolver))) {
    LOG_ERROR("alloc IObCDCPartTransResolver fail", KR(ret), K(tls_id_str));
  } else if (OB_ISNULL(part_trans_resolver)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid part_trans_resolver", K(part_trans_resolver));
  } else if (OB_FAIL(part_trans_resolver->init(tls_id, start_tstamp_ns))) {
    LOG_ERROR("init part trans resolver fail", KR(ret), K(tls_id), K(start_tstamp_ns));
  }
  // alloc a LSFetchCtx
  else if (OB_FAIL(ctx_pool_.alloc(ctx))) {
    LOG_ERROR("alloc LSFetchCtx fail", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    LOG_ERROR("LSFetchCtx is NULL", K(ctx));
    ret = OB_ERR_UNEXPECTED;
  }
  // alloc a progress id which should be unique for each ls
  else if (OB_FAIL(progress_controller_->acquire_progress(progress_id, start_tstamp_ns))) {
    LOG_ERROR("acquire_progress fail", KR(ret), K(start_tstamp_ns));
  // init LSFetchCtx
  } else if (OB_FAIL(ctx->init(tls_id, start_parameters, is_loading_data_dict_baseline_data,
          progress_id, fetching_mode, archive_dest_str, *part_trans_resolver, *this))) {
    LOG_ERROR("ctx init fail", KR(ret), K(tls_id), K(start_tstamp_ns), K(start_lsn), K(progress_id));
  } else {
    if (OB_FAIL(ctx_map_.insert(tls_id, ctx))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_ERROR("ls has been added", KR(ret), K(tls_id), K(start_tstamp_ns), K(start_lsn));
      } else {
        LOG_ERROR("insert into map fail", KR(ret), K(tls_id), K(ctx));
      }
    } else {
      _LOG_INFO("[STAT] [LSFetchMgr] [ADD_LS] tls_id=%s start_lsn=%s start_tstamp_ns=%ld(%s) "
          "progress_id=%ld fetch_task=%p part_trans_resolver=%p start_parameters=%s",
          to_cstring(tls_id), to_cstring(start_lsn), start_tstamp_ns, NTS_TO_STR(start_tstamp_ns),
          progress_id, ctx, part_trans_resolver, to_cstring(start_parameters));
    }
  }

  if (OB_SUCCESS != ret) {
    // recycle progress id, delete from global_progress_controller and should not effect progress of normal ls
    int release_ret = OB_SUCCESS;

    if (-1 != progress_id && NULL != progress_controller_) {
      if (OB_UNLIKELY(OB_SUCCESS != (release_ret = progress_controller_->release_progress(progress_id)))) {
        LOG_ERROR("release progress fail", K(release_ret), K(progress_id), K(tls_id), K(ctx),
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

int ObLogLSFetchMgr::init_tls_info_(const logservice::TenantLSID &tls_id, char *&tls_id_str)
{
  int ret = OB_SUCCESS;
  tls_id_str = NULL;
  static const int64_t TLS_BUF_SIZE = 50; // 50 is enough for serialized tenant_id(uint64_t max len 20) + ls_id(int64_t max len 10)
  char tls_id_str_local_buf[TLS_BUF_SIZE];
  int64_t pos = 0;

  if (OB_UNLIKELY(!tls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_ls_id to seriailize", KR(ret), K(tls_id));
  } else if (OB_FAIL(common::databuff_printf(tls_id_str_local_buf, TLS_BUF_SIZE, pos, "%lu_%ld", tls_id.get_tenant_id(), tls_id.get_ls_id().id()))) {
    LOG_ERROR("serialize tenant_ls_id failed", KR(ret), K(tls_id), K(TLS_BUF_SIZE), K(pos), K(tls_id_str_local_buf));
  } else {
    const int64_t buf_len = pos + 1;
    tls_id_str = static_cast<char*>(tls_info_serialize_allocator_.alloc(buf_len));

    if (OB_ISNULL(tls_id_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc tls_id_str buf failed", KR(ret), K(tls_id), K(buf_len), K(tls_id_str));
    } else {
      MEMCPY(tls_id_str, tls_id_str_local_buf, pos);
      tls_id_str[pos] = '\0';
    }
  }

  return ret;
}

bool ObLogLSFetchMgr::CtxRecycleCond::operator() (const logservice::TenantLSID &tls_id,
    LSFetchCtx *&ctx)
{
  bool bool_ret = false;

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid part fetch ctx", K(ctx), K(tls_id));
    bool_ret = false;
  } else {
    _LOG_INFO("[STAT] [LSFetchMgr] [RECYCLE_LS] tls_id=%s "
        "fetch_task=%p fetch_task=%s",
        to_cstring(tls_id), ctx, to_cstring(*ctx));

    // modify partitin status to DISCARDED
    ctx->set_discarded();

    bool_ret = true;
  }

  return bool_ret;
}

int ObLogLSFetchMgr::recycle_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  CtxRecycleCond recycle_cond;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (OB_FAIL(ctx_map_.operate(tls_id, recycle_cond))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // as expected
    } else {
      LOG_ERROR("operate on ctx map fail", KR(ret), K(tls_id));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogLSFetchMgr::remove_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (OB_ISNULL(progress_controller_) || OB_ISNULL(part_trans_resolver_factory_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_), K(part_trans_resolver_factory_));
    ret = OB_INVALID_ERROR;
  } else {
    LSFetchCtx *fetch_ctx = NULL;

    // remove node from map first to guarantee the correctness of the concurrent operation on the map
    if (OB_FAIL(ctx_map_.erase(tls_id, fetch_ctx))) {
      LOG_ERROR("erase LSFetchCtx from map fail", KR(ret), K(tls_id));
    } else if (OB_ISNULL(fetch_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("LSFetchCtx is NULL, unexcepted error", KR(ret), K(tls_id), K(fetch_ctx));
    } else if (OB_UNLIKELY(! fetch_ctx->is_discarded())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is not discarded, can not remove", KR(ret), K(tls_id), KPC(fetch_ctx));
    } else {
      IObCDCPartTransResolver *ptr = fetch_ctx->get_part_trans_resolver();
      int64_t progress_id = fetch_ctx->get_progress_id();

      _LOG_INFO("[STAT] [PartFetchMgr] [REMOVE_LS] tls_id=%s progress_id=%ld "
          "fetch_task=%p part_trans_resolver=%p fetch_task=%s",
          to_cstring(tls_id), progress_id, fetch_ctx, ptr, to_cstring(*fetch_ctx));

      // recycle progress id, delete from global progress_controller
      int release_ret = progress_controller_->release_progress(progress_id);
      if (OB_UNLIKELY(OB_SUCCESS != release_ret)) {
        LOG_ERROR("release progress fail", K(release_ret), K(progress_id), K(tls_id), K(fetch_ctx),
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

int ObLogLSFetchMgr::get_ls_fetch_ctx(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (OB_FAIL(ctx_map_.get(tls_id, ctx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // tls_id not exist in map
      ctx = NULL;
    } else {
      LOG_ERROR("get LSFetchCtx from map fail", KR(ret), K(tls_id));
    }
  } else if (OB_ISNULL(ctx)) {
    LOG_ERROR("LSFetchCtx is NULL", K(ctx));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // succ
  }

  return ret;
}

int ObLogLSFetchMgr::CtxLSProgressCond::init(const int64_t count)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ls_fetch_info_array_.reserve(count))) {
    LOG_ERROR("reserve array fail", KR(ret), K(count));
  } else {
    ctx_cnt_ = 0;
  }

  return ret;
}

bool ObLogLSFetchMgr::CtxLSProgressCond::operator() (const logservice::TenantLSID &tls_id,
    LSFetchCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (NULL != ctx) {
    LSFetchInfoForPrint ls_fetch_info;

    if (OB_FAIL(ls_fetch_info.init(*ctx))) {
      LOG_ERROR("init ls_fetch_info fail", KR(ret), K(tls_id), KPC(ctx));
    } else if (OB_FAIL(ls_fetch_info_array_.push_back(ls_fetch_info))) {
      LOG_ERROR("part_progress_array_ push back fail", KR(ret), K(tls_id), KPC(ctx), K(ctx_cnt_));
    } else {
      ctx_cnt_++;
    }
  }

  return OB_SUCCESS == ret;
}

void ObLogLSFetchMgr::print_k_slowest_ls()
{
  int ret = OB_SUCCESS;
  LSFetchInfoArray fetch_slow_array;       // array of LS with slowest log fetch progress
  LSFetchInfoArray dispatch_slow_array;    // array of LS with slowest task dispatch progress
  int64_t slowest_part_num = ATOMIC_LOAD(&g_print_slowest_ls_num);
  int64_t part_num = ctx_map_.count();
  CtxLSProgressCond ls_progress_cond;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (part_num > 0) {
    if (OB_FAIL(ls_progress_cond.init(part_num))) {
      LOG_ERROR("part progree cond init fail", KR(ret), K(part_num));
    } else if (OB_FAIL(ctx_map_.for_each(ls_progress_cond))) {
      LOG_ERROR("ctx_map_ for_each fail", KR(ret));
    } else {
      const LSFetchInfoArray &ls_fetch_info_array = ls_progress_cond.ls_fetch_info_array_;
      FetchProgressCompFunc fetch_progress_comp_func;
      DispatchProgressCompFunc dispatch_progress_comp_func;

      // get TOP-K ls with slowest log fetch progress
      if (OB_FAIL(top_k(ls_fetch_info_array, slowest_part_num, fetch_slow_array,
          fetch_progress_comp_func))) {
        LOG_ERROR("find the k slowest ls fail", KR(ret), K(slowest_part_num),
            K(fetch_slow_array));
      }
      // get TOP-K ls with slowest task dispatch progress
      else if (OB_FAIL(top_k(ls_fetch_info_array, slowest_part_num, dispatch_slow_array,
          dispatch_progress_comp_func))) {
        LOG_ERROR("find the k slowest ls fail", KR(ret), K(slowest_part_num),
            K(dispatch_slow_array));
      } else {
        int64_t cur_time = get_timestamp();
        int64_t array_cnt = fetch_slow_array.count();

        // ************************ Print the K LSs with the slowest fetch log progress
        _LOG_INFO("[STAT] slow fetch progress start. ls_count=%ld/%ld", array_cnt,
            ls_fetch_info_array.count());

        for (int64_t idx = 0; OB_SUCCESS == ret && idx < array_cnt; ++idx) {
          fetch_slow_array.at(idx).print_fetch_progress("slow fetch progress",
              idx, array_cnt, cur_time);
        }

        LOG_INFO("[STAT] slow fetch progress end");

        // ************************ Print the K LSs with the slowest LS dispatch progress
        array_cnt = dispatch_slow_array.count();
        _LOG_INFO("[STAT] slow dispatch progress start. ls_count=%ld/%ld", array_cnt,
            ls_fetch_info_array.count());

        for (int64_t idx = 0; OB_SUCCESS == ret && idx < array_cnt; ++idx) {
          dispatch_slow_array.at(idx).print_dispatch_progress("slow dispatch progress",
              idx, array_cnt, cur_time);
        }

        LOG_INFO("[STAT] slow dispatch progress end");
      }
    }
  }
}

int ObLogLSFetchMgr::set_start_global_trans_version(const int64_t start_global_trans_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_global_trans_version)) {
    LOG_ERROR("invalid argument", K(start_global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    start_global_trans_version_ = start_global_trans_version;
  }

  return ret;
}

void ObLogLSFetchMgr::configure(const ObLogConfig & config)
{
  int64_t print_slowest_ls_num = config.print_fetcher_slowest_ls_num;

  ATOMIC_STORE(&g_print_slowest_ls_num, print_slowest_ls_num);
  LOG_INFO("[CONFIG]", K(print_slowest_ls_num));
}

}
}
