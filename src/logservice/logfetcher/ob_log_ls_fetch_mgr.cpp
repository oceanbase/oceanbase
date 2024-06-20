/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_ls_fetch_mgr.h"

#include "share/ob_errno.h"                     // OB_SUCCESS, ..
#include "lib/oblog/ob_log_module.h"            // LOG_*
#include "lib/container/ob_array_iterator.h"
#include "lib/allocator/ob_mod_define.h"        // ObModIds

#include "ob_log_part_progress_controller.h"    // PartProgressController
#include "ob_log_fetcher_ls_ctx_factory.h"      // ObILogFetcherLSCtxFactory
#include "ob_log_fetcher_ls_ctx_additional_info_factory.h" // ObILogFetcherLSCtxAddInfoFactory
#include <algorithm>

#define FREE_CTX(ctx, pool) \
    do {\
      int free_ret = OB_SUCCESS; \
      if (NULL != ctx) { \
        ctx->reset(); \
        free_ret = pool->free(ctx); \
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
namespace logfetcher
{
int64_t ObLogLSFetchMgr::g_print_slowest_ls_num = ObLogFetcherConfig::default_print_fetcher_slowest_ls_num;

ObLogLSFetchMgr::ObLogLSFetchMgr() :
    is_inited_(false),
    fetcher_(nullptr),
    progress_controller_(NULL),
    ls_ctx_factory_(NULL),
    ls_ctx_add_info_factory_(NULL),
    ctx_map_(),
    tls_info_serialize_allocator_("TLSIDSerialize")
{
}

ObLogLSFetchMgr::~ObLogLSFetchMgr()
{
  destroy();
}

int ObLogLSFetchMgr::init(
    PartProgressController &progress_controller,
    ObILogFetcherLSCtxFactory &ls_ctx_factory,
    ObILogFetcherLSCtxAddInfoFactory &ls_ctx_add_info_factory,
    void *fetcher_host)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_ISNULL(fetcher_host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(fetcher_host));
  } else if (OB_FAIL(ctx_map_.init(ObModIds::OB_LOG_PART_FETCH_CTX_MAP))) {
    LOG_ERROR("init LSFetchCtxMap fail", KR(ret));
  } else {
    progress_controller_ = &progress_controller;
    ls_ctx_factory_ = &ls_ctx_factory;
    ls_ctx_add_info_factory_ = &ls_ctx_add_info_factory;
    fetcher_ = fetcher_host;
    is_inited_ = true;

    LOG_INFO("init LS fetch mgr succ");
  }

  return ret;
}

void ObLogLSFetchMgr::destroy()
{
  // TODO: recycle all task in map

  is_inited_ = false;
  fetcher_ = nullptr;
  progress_controller_ = NULL;
  ls_ctx_factory_ = NULL;
  ls_ctx_add_info_factory_ = NULL;
  (void)ctx_map_.destroy();
  tls_info_serialize_allocator_.clear();
  LOG_INFO("destroy LS fetch mgr succ");
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(LOG_FETCHER_ALLOC_LS_CTX_ADD_INFO_FAIL);
ERRSIM_POINT_DEF(LOG_FETCHER_ALLOC_LS_CTX_FAIL);
#endif
int ObLogLSFetchMgr::add_ls(
    const logservice::TenantLSID &tls_id,
    const ObLogFetcherStartParameters &start_parameters,
    const bool is_loading_data_dict_baseline_data,
    const ClientFetchingMode fetching_mode,
    const ObBackupPathString &archive_dest_str,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  LSFetchCtx *ctx = NULL;
  int64_t progress_id = -1;
  ObILogFetcherLSCtxAddInfo *ls_ctx_add_info = NULL;
  char *tls_id_str = NULL;
  const int64_t start_tstamp_ns = start_parameters.get_start_tstamp_ns();
  const palf::LSN &start_lsn = start_parameters.get_start_lsn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  }
  // start timestamp must be valid!
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_tstamp_ns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid start tstamp", KR(ret), K(start_tstamp_ns));
  } else if (OB_ISNULL(progress_controller_)
      || OB_ISNULL(ls_ctx_add_info_factory_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid handlers", KR(ret), K(progress_controller_), K(ls_ctx_add_info_factory_));
  } else if (OB_FAIL(init_tls_info_(tls_id, tls_id_str))) {
    LOG_ERROR("init_tls_info_ failed", KR(ret), K(tls_id), K(tls_id_str));
  }
  // alloc a part trans resolver
#ifdef ERRSIM
  else if (OB_FAIL(LOG_FETCHER_ALLOC_LS_CTX_ADD_INFO_FAIL)) {
    LOG_ERROR("ERRSIM: failed to alloc ls_ctx_add_info", K(tls_id));
  }
#endif
  else if (OB_FAIL(ls_ctx_add_info_factory_->alloc(tls_id_str, ls_ctx_add_info))) {
    LOG_ERROR("alloc ObILogFetcherLSCtxAddInfo fail", KR(ret), K(tls_id_str));
  } else if (OB_ISNULL(ls_ctx_add_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls_ctx_add_info", K(ls_ctx_add_info));
  } else if (OB_FAIL(ls_ctx_add_info->init(tls_id, start_tstamp_ns))) {
    LOG_ERROR("init part trans resolver fail", KR(ret), K(tls_id), K(start_tstamp_ns));
  }
  // alloc a LSFetchCtx
#ifdef ERRSIM
  else if (OB_FAIL(LOG_FETCHER_ALLOC_LS_CTX_FAIL)) {
    LOG_ERROR("ERRSIM: failed to alloc ls_fetch_ctx", K(tls_id));
  }
#endif
  else if (OB_FAIL(ls_ctx_factory_->alloc(ctx))) {
    LOG_ERROR("alloc LSFetchCtx fail", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("LSFetchCtx is NULL", KR(ret), K(ctx));
  }
  // alloc a progress id which should be unique for each ls
  else if (OB_FAIL(progress_controller_->acquire_progress(progress_id, start_tstamp_ns))) {
    LOG_ERROR("acquire_progress fail", KR(ret), K(start_tstamp_ns));
  // init LSFetchCtx
  } else if (OB_FAIL(ctx->init(tls_id, start_parameters, is_loading_data_dict_baseline_data,
          progress_id, fetching_mode, archive_dest_str, *ls_ctx_add_info, err_handler))) {
    LOG_ERROR("ctx init fail", KR(ret), K(tls_id), K(start_tstamp_ns), K(start_lsn), K(progress_id));
  } else {
    ctx->set_host(*this);

    if (OB_FAIL(ctx_map_.insert(tls_id, ctx))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_ERROR("LS has been added", KR(ret), K(tls_id), K(start_tstamp_ns), K(start_lsn));
      } else {
        LOG_ERROR("insert into map fail", KR(ret), K(tls_id), K(ctx));
      }
    } else {
      _LOG_INFO("[STAT] [LSFetchMgr] [ADD_LS] tls_id=%s start_lsn=%s start_tstamp_ns=%ld(%s) "
          "progress_id=%ld fetch_task=%p ls_ctx_add_info=%p start_parameters=%s",
          to_cstring(tls_id), to_cstring(start_lsn), start_tstamp_ns, NTS_TO_STR(start_tstamp_ns),
          progress_id, ctx, ls_ctx_add_info, to_cstring(start_parameters));
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
      FREE_CTX(ctx, ls_ctx_factory_);
    }

    if (NULL != ls_ctx_add_info) {
      ls_ctx_add_info_factory_->free(ls_ctx_add_info);
      ls_ctx_add_info = NULL;
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
  } else if (OB_ISNULL(progress_controller_) || OB_ISNULL(ls_ctx_add_info_factory_)) {
    LOG_ERROR("invalid progress controller", K(progress_controller_), K(ls_ctx_add_info_factory_));
    ret = OB_INVALID_ERROR;
  } else {
    LSFetchCtx *fetch_ctx = NULL;

    // remove node from map first to guarantee the correctness of the concurrent operation on the map
    if (OB_FAIL(ctx_map_.erase(tls_id, fetch_ctx))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("erase LSFetchCtx from map fail", KR(ret), K(tls_id));
      } else {
        LOG_WARN("erase LSFetchCtx from map fail, ctx not exist", K(tls_id));
      }
    } else if (OB_ISNULL(fetch_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("LSFetchCtx is NULL, unexcepted error", KR(ret), K(tls_id), K(fetch_ctx));
    } else if (OB_UNLIKELY(! fetch_ctx->is_discarded())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is not discarded, can not remove", KR(ret), K(tls_id), KPC(fetch_ctx));
    } else {
      ObILogFetcherLSCtxAddInfo *ptr = fetch_ctx->get_ls_ctx_add_info();
      int64_t progress_id = fetch_ctx->get_progress_id();

      _LOG_INFO("[STAT] [PartFetchMgr] [REMOVE_LS] tls_id=%s progress_id=%ld "
          "fetch_task=%p ls_ctx_add_info=%p fetch_task=%s",
          to_cstring(tls_id), progress_id, fetch_ctx, ptr, to_cstring(*fetch_ctx));

      // recycle progress id, delete from global progress_controller
      int release_ret = progress_controller_->release_progress(progress_id);
      if (OB_UNLIKELY(OB_SUCCESS != release_ret)) {
        LOG_ERROR("release progress fail", K(release_ret), K(progress_id), K(tls_id), K(fetch_ctx),
            KPC(fetch_ctx));
      }

      if (NULL != fetch_ctx) {
        FREE_CTX(fetch_ctx, ls_ctx_factory_);
      }

      if (NULL != ptr) {
        ls_ctx_add_info_factory_->free(ptr);
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

bool ObLogLSFetchMgr::is_tls_exist(const logservice::TenantLSID &tls_id) const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  LSFetchCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init");
  } else if (OB_FAIL(ctx_map_.get(tls_id, ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get LSFetchCtx from map fail", K(tls_id));
    }
  } else {
    bret = true;
  }
  return bret;
}

int ObLogLSFetchMgr::get_tls_proposal_id(const logservice::TenantLSID &tls_id, int64_t &proposal_id) const
{
  int ret = OB_SUCCESS;
  LSFetchCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init");
  } else if (OB_FAIL(ctx_map_.get(tls_id, ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get LSFetchCtx from map fail", K(tls_id));
    }
  } else {
    proposal_id = ctx->get_proposal_id();
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
      if (OB_LS_NOT_EXIST != ret) {
        LOG_ERROR("init ls_fetch_info fail", KR(ret), K(tls_id), KPC(ctx));
      } else {
        ret = OB_SUCCESS;
      }
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
  const int64_t slowest_ls_num = ATOMIC_LOAD(&g_print_slowest_ls_num);
  const int64_t ls_num = ctx_map_.count();
  CtxLSProgressCond ls_progress_cond;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSFetchMgr is not init", KR(ret));
  } else if (ls_num > 0) {
    if (OB_FAIL(ls_progress_cond.init(ls_num))) {
      LOG_ERROR("part progree cond init fail", KR(ret), K(ls_num));
    } else if (OB_FAIL(ctx_map_.for_each(ls_progress_cond))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("ctx_map_ for_each fail", KR(ret));
      }
    } else {
      const LSFetchInfoArray &ls_fetch_info_array = ls_progress_cond.ls_fetch_info_array_;
      FetchProgressCompFunc fetch_progress_comp_func;
      DispatchProgressCompFunc dispatch_progress_comp_func;

      // get TOP-K ls with slowest log fetch progress
      if (OB_FAIL(top_k(ls_fetch_info_array, slowest_ls_num, fetch_slow_array,
          fetch_progress_comp_func))) {
        LOG_ERROR("find the k slowest ls fail", KR(ret), K(slowest_ls_num),
            K(fetch_slow_array));
      }
      // get TOP-K ls with slowest task dispatch progress
      else if (OB_FAIL(top_k(ls_fetch_info_array, slowest_ls_num, dispatch_slow_array,
          dispatch_progress_comp_func))) {
        LOG_ERROR("find the k slowest ls fail", KR(ret), K(slowest_ls_num),
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

void ObLogLSFetchMgr::configure(const ObLogFetcherConfig & config)
{
  int64_t print_slowest_ls_num = config.print_fetcher_slowest_ls_num;

  ATOMIC_STORE(&g_print_slowest_ls_num, print_slowest_ls_num);
  LOG_INFO("[CONFIG]", K(print_slowest_ls_num));
}

}
}
