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
#include "ob_log_meta_data_service.h"
#include "ob_log_instance.h"
#include "ob_log_fetching_mode.h"
#include "ob_log_meta_data_queryer.h"
#include "share/backup/ob_archive_struct.h"
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#include "logservice/data_dictionary/ob_data_dict_meta_info.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "ob_log_part_trans_parser.h"


#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[LOG_META_DATA] [SERVICE] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[LOG_META_DATA] [SERVICE] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

namespace oceanbase
{
namespace libobcdc
{
ObLogMetaDataService &ObLogMetaDataService::get_instance()
{
  static ObLogMetaDataService THE_ONE;
  return THE_ONE;
}

ObLogMetaDataService::ObLogMetaDataService() :
    is_inited_(false),
    fetcher_(),
    baseline_loader_(),
    incremental_replayer_(),
    fetcher_dispatcher_(),
    part_trans_parser_(NULL)
{
}

ObLogMetaDataService::~ObLogMetaDataService()
{
  destroy();
}

int ObLogMetaDataService::init(
    const int64_t start_tstamp_ns,
    const ClientFetchingMode fetching_mode,
    const share::ObBackupPathString &archive_dest,
    IObLogSysLsTaskHandler *sys_ls_handler,
    common::ObMySQLProxy *proxy,
    IObLogErrHandler *err_handler,
    IObLogPartTransParser &part_trans_parser,
    const int64_t cluster_id,
    const ObLogConfig &cfg,
    const int64_t start_seq,
    const bool enable_direct_load_inc)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(baseline_loader_.init(cfg))) {
    LOG_ERROR("ObLogMetaDataBaselineLoader init fail", KR(ret));
  } else if (OB_FAIL(incremental_replayer_.init(part_trans_parser))) {
    LOG_ERROR("ObLogMetaDataReplayer init fail", KR(ret));
  } else if (OB_FAIL(fetcher_dispatcher_.init(&incremental_replayer_, start_seq))) {
    LOG_ERROR("ObLogMetaDataFetcherDispatcher init fail", KR(ret));
  } else if (OB_FAIL(fetcher_.init(fetching_mode, archive_dest, &fetcher_dispatcher_, sys_ls_handler,
      proxy, err_handler, cluster_id, cfg, start_seq, enable_direct_load_inc))) {
    LOG_ERROR("ObLogMetaDataFetcher init fail", KR(ret),
        K(fetching_mode), "fetching_log_mode", print_fetching_mode(fetching_mode), K(archive_dest));
  } else if (OB_FAIL(fetcher_.start())) {
    LOG_ERROR("fetcher_ start failed", KR(ret));
  } else {
    is_inited_ = true;
    ISTAT("ObLogMetaDataService init success");
  }

  return ret;
}

void ObLogMetaDataService::destroy()
{
  if (IS_INIT) {
    fetcher_.destroy();
    baseline_loader_.destroy();
    incremental_replayer_.destroy();
    fetcher_dispatcher_.destroy();
    part_trans_parser_ = NULL;
    is_inited_ = false;
  }
}

int ObLogMetaDataService::refresh_baseline_meta_data(
    const uint64_t tenant_id,
    const int64_t start_timestamp_ns,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataService is not initialized", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sys tenant is unexpected", KR(ret), K(tenant_id));
  } else {
    logservice::TenantLSID tls_id(tenant_id, share::SYS_LS);
    logfetcher::DataDictionaryInLogInfo data_dict_in_log_info;
    logfetcher::ObLogFetcherStartParameters start_parameters;

    if (OB_FAIL(baseline_loader_.add_tenant(tenant_id))) {
      LOG_ERROR("baseline_loader_ add_tenant success", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_data_dict_in_log_info_(tenant_id, start_timestamp_ns, data_dict_in_log_info))) {
      LOG_ERROR("log_meta_data_service get_data_dict_in_log_info failed", KR(ret), K(tenant_id));
    } else {
      ISTAT("get_data_dict_in_log_info success", K(tenant_id), K(start_timestamp_ns), K(data_dict_in_log_info));
      start_parameters.reset(data_dict_in_log_info.snapshot_scn_, std::max(start_timestamp_ns, data_dict_in_log_info.end_scn_), data_dict_in_log_info);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fetcher_.add_ls_and_fetch_until_the_progress_is_reached(tenant_id, start_parameters, timeout))) {
        LOG_ERROR("fetcher_ add_ls_and_fetch_until_the_progress_is_reached", KR(ret), K(tenant_id),
            K(start_parameters), K(timeout));
      } else {
      }
    }

    // To get the accurate meta data for start timestamp, we should fetch and parse the corresponding baseline data,
    // and the incremental meta data is then replayed.
    // Note: non-blocking DDL, we must ensure that all DDL transactions are not missed.
    if (OB_SUCC(ret)) {
      ObDictTenantInfoGuard dict_tenant_info_guard;
      ObDictTenantInfo *tenant_info = nullptr;

      if (OB_FAIL(get_tenant_info_guard(tenant_id, dict_tenant_info_guard))) {
        LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant_info is nullptr", K(tenant_id));
      } else {
        ISTAT("Increment data replayed begin", K(tenant_id), KPC(tenant_info));

        if (OB_FAIL(incremental_replayer_.replay(tenant_id, start_timestamp_ns, *tenant_info))) {
          LOG_ERROR("incremental_replayer_ replay failed", KR(ret), K(tenant_id), K(start_timestamp_ns));
        } else {}

        ISTAT("Increment data replayed end", KR(ret), K(tenant_id), KPC(tenant_info));
      }
    }
  }

  return ret;
}

int ObLogMetaDataService::finish_when_all_tennats_are_refreshed()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataService is not initialized", KR(ret));
  } else {
    ISTAT("finish_when_all_tennats_are_refreshed begin");

    fetcher_.stop();
    fetcher_.destroy();

    ISTAT("finish_when_all_tennats_are_refreshed end, fetcher_ is destroyed");
  }

  return ret;
}

int ObLogMetaDataService::read(
    const uint64_t tenant_id,
    datadict::ObDataDictIterator &data_dict_iterator,
    const char *buf,
    const int64_t buf_len,
    const int64_t pos_after_log_header,
    const palf::LSN &lsn,
    const int64_t submit_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataService is not initialized", KR(ret));
  } else {
    if (OB_FAIL(baseline_loader_.read(tenant_id, data_dict_iterator, buf, buf_len, pos_after_log_header, lsn, submit_ts))) {
      LOG_ERROR("baseline_loader_ read failed", K(tenant_id), K(buf_len), K(pos_after_log_header), K(lsn), K(submit_ts));
    }
  }

  return ret;
}

int ObLogMetaDataService::ObLogMetaDataService::get_tenant_info_guard(
    const uint64_t tenant_id,
    ObDictTenantInfoGuard &guard)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(baseline_loader_.get_tenant_info_guard(tenant_id, guard))) {
    LOG_ERROR("baseline_loader_ get_tenant_info_guard failed", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObLogMetaDataService::get_tenant_id_in_archive(
    const int64_t start_timestamp_ns,
    uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  datadict::ObDataDictMetaInfo data_dict_meta_info;

  if (OB_FAIL(read_meta_info_in_archive_log_(start_timestamp_ns, data_dict_meta_info))) {
    LOG_ERROR("read_meta_info_in_archive_log_ failed", K(ret), K(start_timestamp_ns),
        K(data_dict_meta_info));
  } else {
    tenant_id = data_dict_meta_info.get_tenant_id();
  }

  return ret;
}

int ObLogMetaDataService::get_data_dict_in_log_info_in_archive_(
    const int64_t start_timestamp_ns,
    logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info)
{
  int ret = OB_SUCCESS;
  datadict::ObDataDictMetaInfo data_dict_meta_info;

  if (OB_FAIL(read_meta_info_in_archive_log_(start_timestamp_ns, data_dict_meta_info))) {
    LOG_ERROR("read_meta_info_in_archive_log_ failed", K(ret), K(start_timestamp_ns),
        K(data_dict_meta_info));
  } else {
    const datadict::DataDictMetaInfoItemArr &item_arr = data_dict_meta_info.get_item_arr();

    if (item_arr.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("no datadict metainfo item in item_arr, unexpected", KR(ret), K(data_dict_meta_info));
    } else {
      const int64_t item_arr_size = item_arr.count();
      int64_t target_index = 0;
      while (target_index < item_arr_size && item_arr.at(target_index).snapshot_scn_ > start_timestamp_ns) {
        target_index++;
      }

      if (target_index >= item_arr_size) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_ERROR("can't find datadict snapshot_scn older than start_timestamp_ns", K(target_index), K(data_dict_meta_info),
            K(start_timestamp_ns));
      } else {
        datadict::ObDataDictMetaInfoItem data_dict_item;
        data_dict_item = item_arr.at(target_index);
        const int64_t end_scn_val = (data_dict_item.end_scn_ == share::OB_INVALID_SCN_VAL) ? start_timestamp_ns : data_dict_item.end_scn_;

        data_dict_in_log_info.reset(data_dict_item.snapshot_scn_,
            end_scn_val,
            palf::LSN(data_dict_item.start_lsn_),
            palf::LSN(data_dict_item.end_lsn_));
        LOG_INFO("get_data_dict_in_log_info_in_archive_ succ", K(data_dict_meta_info), K(start_timestamp_ns), K(data_dict_in_log_info));
      }
    }
  }

  return ret;
}

int ObLogMetaDataService::read_meta_info_in_archive_log_(
    const int64_t start_timestamp_ns,
    datadict::ObDataDictMetaInfo &data_dict_meta_info)
{
  int ret = OB_SUCCESS;
  const char *archive_dest_c_str = TCONF.archive_dest;
  share::ObBackupPathString archive_dest_str(archive_dest_c_str);
  share::ObBackupDest archive_dest;
  logservice::ObLogArchivePieceContext piece_ctx;
  char *data_dict_in_log_info_buffer = NULL;
  const int64_t buffer_size = archive::MAX_META_RECORD_FILE_SIZE; // 2M (data) + 4K (header)
  int64_t data_size = 0;
  share::SCN start_scn;

  if (OB_ISNULL(data_dict_in_log_info_buffer = static_cast<char*>(ob_malloc(buffer_size, "DataDictMetaInf")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer for datadict metainfo failed", KR(ret), K(buffer_size));
  } else if (OB_FAIL(archive_dest.set(archive_dest_str))) {
    LOG_ERROR("archive_dest set archive_dest_str failed", KR(ret), K(archive_dest), K(archive_dest_str));
  } else if (OB_FAIL(piece_ctx.init(share::SYS_LS, archive_dest))) {
    LOG_ERROR("piece_ctx init failed", KR(ret), K(archive_dest), K(archive_dest_str));
  } else  {
    // Assume the recycle interval of data in __all_data_dictionary_in_log is a month
    constexpr int64_t DATADICT_META_RECYCLE_INTERVAL_NS = 30L * _DAY_ * NS_CONVERSION;
    // skip half interval to find more precise meta info
    const int64_t get_ls_meta_data_key = start_timestamp_ns + DATADICT_META_RECYCLE_INTERVAL_NS / 2;
    if (OB_FAIL(start_scn.convert_from_ts(get_ls_meta_data_key/1000L))) {
      LOG_ERROR("convert start_timestamp_ns to start scn failed", KR(ret), K(start_timestamp_ns));
    } else if (OB_FAIL(piece_ctx.get_ls_meta_data(
        share::ObArchiveLSMetaType(share::ObArchiveLSMetaType::Type::SCHEMA_META),
        start_scn, data_dict_in_log_info_buffer, buffer_size, data_size))) {
      LOG_WARN("piece ctx get logstream meta data failed", KR(ret), K(start_scn), K(data_dict_in_log_info_buffer),
          K(buffer_size), K(data_size));
    } else {
      int64_t archive_header_pos = 0;
      int64_t data_pos = archive::COMMON_HEADER_SIZE;
      archive::ObLSMetaFileHeader ls_meta_file_header;

      if (OB_FAIL(ls_meta_file_header.deserialize(data_dict_in_log_info_buffer,
          data_size, archive_header_pos))) {
        LOG_WARN("failed to deserialize ls meta file header", KR(ret),
            K(data_size), K(archive_header_pos));
      } else if (! ls_meta_file_header.is_valid()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ls meta file header is not valid, maybe data is invalid", KR(ret), K(ls_meta_file_header));
      } else if (OB_FAIL(data_dict_meta_info.deserialize(data_dict_in_log_info_buffer,
          data_size, data_pos))) {
        LOG_WARN("data dict info deserialize failed", KR(ret), K(data_dict_in_log_info_buffer),
            K(data_size), K(data_pos));
      } else if (! data_dict_meta_info.check_integrity()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("data dict info is not valid", KR(ret), K(data_dict_meta_info));
      } else {
        const uint64_t tenant_id = data_dict_meta_info.get_tenant_id();
        LOG_INFO("read_meta_info_in_archive_log success", K(tenant_id), K(start_timestamp_ns), K(data_dict_meta_info));
      }
    }
  }

  if (OB_NOT_NULL(data_dict_in_log_info_buffer)) {
    ob_free(data_dict_in_log_info_buffer);
  }

  return ret;
}

int ObLogMetaDataService::get_data_dict_in_log_info_(
    const uint64_t tenant_id,
    const int64_t start_timestamp_ns,
    logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t record_count = 0;
  const ClientFetchingMode fetching_mode =  TCTX.fetching_mode_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataService is not initialized", KR(ret));
  } else {
    while (! done) {
      if (is_direct_fetching_mode(fetching_mode)) {
        if (OB_FAIL(get_data_dict_in_log_info_in_archive_(start_timestamp_ns, data_dict_in_log_info))) {
          LOG_WARN("get_data_dict_in_log_info_in_archive_ failed", KR(ret), K(start_timestamp_ns), K(tenant_id), K(data_dict_in_log_info));
        } else {
          done = true;
        }
      } else if (is_integrated_fetching_mode(fetching_mode)) {
        common::ObMySQLProxy& sql_proxy = TCTX.is_tenant_sync_mode() ? TCTX.mysql_proxy_.get_ob_mysql_proxy() : TCTX.tenant_sql_proxy_.get_ob_mysql_proxy();
        ObLogMetaDataSQLQueryer sql_queryer(start_timestamp_ns, sql_proxy);

        if (OB_FAIL(sql_queryer.get_data_dict_in_log_info(tenant_id, start_timestamp_ns, data_dict_in_log_info))) {
          LOG_WARN("sql_queryer get_data_dict_in_log_info fail", KR(ret), K(tenant_id), K(start_timestamp_ns), K(data_dict_in_log_info));
        } else {
          done = true;
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        done = true;
        LOG_ERROR("[FATAL]fetching mode of TCTX is not valid", KR(ret), K(fetching_mode), K(tenant_id));
      }

      if (OB_SUCC(ret) || done) {
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        done = true;
        LOG_ERROR("[FATAL][DATA_DICT] Can't find suitable data_dict to launch OBCDC, please try use online schema(refresh_mode=online && skip_ob_version_compat_check=1)",
            KR(ret), K(tenant_id), K(start_timestamp_ns));
      } else {
        const static int64_t RETRY_FUNC_PRINT_INTERVAL = 10 * _SEC_;
        const int64_t sleep_usec_on_error = 100 * _MSEC_;
        if (REACH_TIME_INTERVAL(RETRY_FUNC_PRINT_INTERVAL)) {
          LOG_WARN("[DATA_DICT] retry get data_dict_in_log_info", KR(ret));
        } else {
          LOG_TRACE("[DATA_DICT] retry get data_dict_in_log_info", KR(ret));
        }
        ret = OB_SUCCESS;
        ob_usleep(sleep_usec_on_error);
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
