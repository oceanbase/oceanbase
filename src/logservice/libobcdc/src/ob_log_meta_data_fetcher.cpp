// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX OBLOG

#include "lib/utility/ob_macro_utils.h"  // RETRY_FUNC
#include "ob_log_meta_data_fetcher.h"
#include "ob_log_fetcher.h"

#define INIT(v, type, args...) \
    do {\
      if (OB_SUCC(ret)) { \
        type *tmp_var = NULL; \
        if (OB_ISNULL(tmp_var = new(std::nothrow) type())) { \
          _LOG_ERROR("construct %s fail", #type); \
          ret = OB_ALLOCATE_MEMORY_FAILED; \
        } else if (OB_FAIL(tmp_var->init(args))) { \
          _LOG_ERROR("init %s fail, ret=%d", #type, ret); \
          delete tmp_var; \
          tmp_var = NULL; \
        } else { \
          v = tmp_var; \
          _LOG_INFO("init component \'%s\' succ", #type); \
        } \
      } \
    } while (0)

#define DESTROY(v, type) \
    do {\
      if (NULL != v) { \
        type *var = static_cast<type *>(v); \
        (void)var->destroy(); \
        delete v; \
        v = NULL; \
      } \
    } while (0)


namespace oceanbase
{
namespace libobcdc
{
ObLogMetaDataFetcher::ObLogMetaDataFetcher() :
    is_inited_(false),
    trans_task_pool_alloc_(),
    trans_task_pool_(),
    log_entry_task_pool_(),
    log_fetcher_(nullptr)
{
}

ObLogMetaDataFetcher::~ObLogMetaDataFetcher()
{
  destroy();
}

int ObLogMetaDataFetcher::init(
    const ClientFetchingMode fetching_mode,
    const share::ObBackupPathString &archive_dest,
    IObLogFetcherDispatcher *fetcher_dispatcher,
    IObLogSysLsTaskHandler *sys_ls_handler,
    ObISQLClient *proxy,
    IObLogErrHandler *err_handler,
    const int64_t cluster_id,
    const ObLogConfig &cfg,
    const int64_t start_seq,
    const bool enable_direct_load_inc)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(trans_task_pool_alloc_.init(
          TASK_POOL_ALLOCATOR_TOTAL_LIMIT,
          TASK_POOL_ALLOCATOR_HOLD_LIMIT,
          TASK_POOL_ALLOCATOR_PAGE_SIZE))) {
    LOG_ERROR("trans_task_pool_alloc_ allocator init failed", KR(ret));
  } else if (OB_FAIL(trans_task_pool_.init(
          &trans_task_pool_alloc_,
          PART_TRANS_TASK_PREALLOC_COUNT,
          true/*allow_dynamic_alloc*/,
          PART_TRANS_TASK_PREALLOC_PAGE_COUNT))) {
    LOG_ERROR("trans_task_pool_ init failed", KR(ret));
  } else if (OB_FAIL(log_entry_task_pool_.init(LOG_ENTRY_TASK_COUNT))) {
    LOG_ERROR("log_entry_task_pool_ init failed", KR(ret));
  } else {
    trans_task_pool_alloc_.set_label("DictConFAlloc");
    INIT(log_fetcher_, ObLogFetcher, true/*is_loading_data_dict_baseline_data*/,
        enable_direct_load_inc, fetching_mode, archive_dest, fetcher_dispatcher,
        sys_ls_handler, &trans_task_pool_, &log_entry_task_pool_, proxy, err_handler,
        cluster_id, cfg, start_seq);

    is_inited_ = true;
    LOG_INFO("ObLogMetaDataFetcher init success",
        K(fetching_mode), "fetching_log_mode", print_fetching_mode(fetching_mode));
  }

  return ret;
}

int ObLogMetaDataFetcher::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_fetcher_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_fetcher_ is nullptr", KR(ret), K(log_fetcher_));
  } else if (OB_FAIL(log_fetcher_->start())) {
    LOG_ERROR("log_fetcher_ start failed", KR(ret));
  } else {
    LOG_INFO("ObLogMetaDataFetcher start success");
  }

  return ret;
}

void ObLogMetaDataFetcher::stop()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_fetcher_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_fetcher_ is nullptr", KR(ret), K(log_fetcher_));
  } else {
    LOG_INFO("ObLogMetaDataFetcher stop begin");
    bool stop_flag = false;
    RETRY_FUNC(stop_flag, *log_fetcher_, wait_for_all_ls_to_be_removed, FETCHER_WAIT_LS_TIMEOUT);
    log_fetcher_->mark_stop_flag();
    log_fetcher_->stop();

    LOG_INFO("ObLogMetaDataFetcher stop success");
  }
}

void ObLogMetaDataFetcher::destroy()
{
  if (is_inited_) {
    stop();
    LOG_INFO("ObLogMetaDataFetcher destroy begin");

    DESTROY(log_fetcher_, ObLogFetcher);
    trans_task_pool_.destroy();
    log_entry_task_pool_.destroy();
    is_inited_ = false;

    LOG_INFO("ObLogMetaDataFetcher destroy finish");
  }
}

int ObLogMetaDataFetcher::add_ls_and_fetch_until_the_progress_is_reached(
    const uint64_t tenant_id,
    const logfetcher::ObLogFetcherStartParameters &start_parameters,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataFetcher is not initialized", KR(ret));
  } else if (OB_ISNULL(log_fetcher_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_fetcher_ is nullptr", KR(ret), K(log_fetcher_));
  } else {
    // Tenant sys log stream needs to be fetched to get the data dictionary
    logservice::TenantLSID tls_id(tenant_id, share::SYS_LS);

    if (OB_FAIL(log_fetcher_->add_ls(tls_id, start_parameters))) {
      LOG_ERROR("log_fetcher_ add_ls failed", KR(ret), K(tls_id), K(start_parameters));
    } else {
      bool is_exceeded = false;
      const int64_t end_tstamp_ns = start_parameters.get_end_tstamp_ns();
      int64_t start_time = get_timestamp();
      int64_t cur_progress;

      while (OB_SUCC(ret) && ! is_exceeded) {
        if (OB_FAIL(log_fetcher_->check_progress(tenant_id, end_tstamp_ns, is_exceeded, cur_progress))) {
          LOG_ERROR("ObLogFetcher check_progress failed", KR(ret), K(tenant_id), K(end_tstamp_ns),
              K(is_exceeded), K(cur_progress));
        }

        if (! is_exceeded) {
          if (REACH_TIME_INTERVAL(10 * _SEC_)) {
            LOG_INFO("ObLogFetcher check_progress is less than end_timestamp",
                K(tenant_id), K(cur_progress), K(end_tstamp_ns));
            trans_task_pool_.print_stat_info();
          }
          int64_t end_time = get_timestamp();

          if (end_time - start_time > timeout) {
            ret = OB_TIMEOUT;
            break;
          }
        }
      } // while
    }

    if (OB_SUCC(ret)) {
      // Recycle tenant sys_ls
      if (OB_FAIL(log_fetcher_->recycle_ls(tls_id))) {
        LOG_ERROR("log_fetcher_ recycle_ls failed", KR(ret));
      } else {
        LOG_INFO("log_fetcher_ recycle_ls success", K(tls_id));
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
