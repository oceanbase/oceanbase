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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_br_producer.h"
#include "ob_log_miner_br_filter.h"
#include "ob_log_miner_args.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{

const int64_t ObLogMinerBRProducer::BR_PRODUCER_THREAD_NUM = 1L;
const int64_t ObLogMinerBRProducer::BR_PRODUCER_POOL_DEFAULT_NUM = 4L * 1024;

class ObCdcConfigMapWrap {
public:
  explicit ObCdcConfigMapWrap(const std::map<std::string, std::string> &cdc_config):
      cdc_config_(cdc_config) { }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    FOREACH(it, cdc_config_) {
      if (need_print_config(it->first)) {
        databuff_print_kv(buf, buf_len, pos, it->first.c_str(), it->second.c_str());
      }
    }
    return pos;
  }

  bool need_print_config(const std::string &config_key) const {
    bool need_print = true;
    if ((0 == config_key.compare("tenant_password"))
        || (0 == config_key.compare("archive_dest"))) {
      need_print = false;
    }

    return need_print;
  }

private:
  const std::map<std::string, std::string> &cdc_config_;
};

ObLogMinerBRProducer::ObLogMinerBRProducer():
    ThreadPool(BR_PRODUCER_THREAD_NUM),
    is_inited_(false),
    is_dispatch_end_(false),
    start_time_us_(OB_INVALID_TIMESTAMP),
    end_time_us_(OB_INVALID_TIMESTAMP),
    curr_trans_id_(),
    cdc_factory_(),
    cdc_instance_(nullptr),
    br_filter_(nullptr),
    err_handle_(nullptr) { }

int ObLogMinerBRProducer::init(const AnalyzerArgs &args,
    ILogMinerBRFilter *filter,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;
  std::map<std::string, std::string> cdc_config;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("oblogminer binlogrecord producer init twice", K(is_inited_), K(args.start_time_us_));
  } else if (OB_FAIL(build_cdc_config_map_(args, cdc_config))) {
    LOG_ERROR("build cdc config map failed", K(args));
  } else if (OB_ISNULL(cdc_instance_ = cdc_factory_.construct_obcdc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc cdc_instance failed", K(cdc_instance_));
  } else if (OB_ISNULL(br_filter_ = filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("filter for br producer is null, unexpected", K(br_filter_), K(filter));
  } else if (OB_FAIL(cdc_instance_->init_with_start_tstamp_usec(cdc_config,
      args.start_time_us_))) {
    LOG_ERROR("init cdc instance failed", K(args.start_time_us_), K(ObCdcConfigMapWrap(cdc_config)));
    LOGMINER_STDOUT("init cdc instance failed\n");
  } else if (OB_FAIL(lib::ThreadPool::init())) {
    LOG_ERROR("thread pool failed to init");
  } else {
    is_inited_ = true;
    is_dispatch_end_ = false;
    err_handle_ = err_handle;
    start_time_us_ = args.start_time_us_;
    end_time_us_ = args.end_time_us_;
    LOG_INFO("ObLogMinerBRProducer finished to init", K(ObCdcConfigMapWrap(cdc_config)),
        K(args.start_time_us_), K(filter));
    LOGMINER_STDOUT_V("ObLogMinerBRProducer finished to init\n");
  }
  return ret;
}

void ObLogMinerBRProducer::run1()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRProducer not init", K(is_inited_), K(cdc_instance_));
  } else if (OB_ISNULL(cdc_instance_) || OB_ISNULL(br_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cdc_instance is null in br producer", K(cdc_instance_), K(is_inited_), K(br_filter_));
  } else {
    bool next_dispatch_end = false;
    const int64_t TIMEOUT_US = 1000L * 1000;
    while (! has_set_stop() && OB_SUCC(ret)) {
      ICDCRecord *br = nullptr;
      ObLogMinerBR *logminer_br = nullptr;
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int32_t major_version = 0;
      lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
      if (OB_FAIL(cdc_instance_->next_record(&br, major_version, tenant_id,TIMEOUT_US))) {
        if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
          LOG_ERROR("get record from cdc_instance failed", K(cdc_instance_));
          LOGMINER_STDOUT_V("get record from cdc_instance failed\n");
        }
      } else if (OB_ISNULL(br)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get a null ICDCRecord, unexpected", K(br), K(major_version), K(tenant_id));
      } else if (OB_INVALID_TENANT_ID == tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get an invaild tenant_id", K(br), K(tenant_id), K(major_version));
      } else if (OB_FAIL(get_tenant_compat_mode_(static_cast<RecordType>(br->recordType()),
          tenant_id, compat_mode))) {
        LOG_ERROR("get tenant compat mode failed", K(tenant_id), K(compat_mode));
      } else if (lib::Worker::CompatMode::INVALID == compat_mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get an invalid compat mode", K(compat_mode), K(tenant_id));
      } else if (OB_FAIL(set_current_trans_id_(br))) {
        LOG_ERROR("failed to set current trans_id", KP(br));
      } else if (OB_ISNULL(logminer_br = reinterpret_cast<ObLogMinerBR*>(
            ob_malloc(sizeof(ObLogMinerBR), "LogMnrBR")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc logminer_br failed", K(logminer_br));
      } else {
        logminer_br = new(logminer_br) ObLogMinerBR();
        if (OB_FAIL(logminer_br->init(cdc_instance_, br, compat_mode,
            curr_trans_id_, tenant_id, major_version))) {
          LOG_ERROR("logminer br failed to init", K(br), K(tenant_id), K(major_version));
        } else if (end_time_us_ < logminer_br->get_commit_scn().convert_to_ts()) {
          // don't let any logminer_br whose commit_ts is larger than end_time_us go through
          if (is_dispatch_end_) {
            LOG_TRACE("destroy any br whose commit_ts larger than end_time", "commit_ts_us",
                logminer_br->get_commit_scn(), K(end_time_us_));
            logminer_br->destroy();
            ob_free(logminer_br);
          } else if (is_trans_end_record_type(logminer_br->get_record_type())) {
            next_dispatch_end = true;
          } else {
            // still dispatch
          }
        }
      }

      if (OB_SUCC(ret) && ! is_dispatch_end_) {
        if (OB_FAIL(br_filter_->push(logminer_br))) {
          LOG_ERROR("push logminer_br into br_filter failed", K(logminer_br), K(br_filter_));
        } else {
          is_dispatch_end_ = next_dispatch_end;
        }
      }

      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      } else if (OB_IN_STOP_STATE == ret) {
        LOG_INFO("cdc instance is stopped, br producer thread exits", K(cdc_instance_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerBRProducer exit unexpected\n");
  }
}

int ObLogMinerBRProducer::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("br producer is not inited", K(cdc_instance_), K(br_filter_));
  } else if (OB_FAIL(cdc_instance_->launch())) {
    LOG_ERROR("cdc instance failed to start");
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("thread pool for br producer failed to start");
  } else {
    LOG_INFO("ObLogMinerBRProducer starts");
  }

  return ret;
}

void ObLogMinerBRProducer::stop()
{
  lib::ThreadPool::stop();
  LOG_INFO("ObLogMinerBRProducer stopped");
}

void ObLogMinerBRProducer::wait()
{
  lib::ThreadPool::wait();
  cdc_instance_->stop();
  LOG_INFO("ObLogMinerBRProducer finished to wait");
}

void ObLogMinerBRProducer::destroy()
{
  if (IS_INIT) {
    ThreadPool::destroy();
    cdc_factory_.deconstruct(cdc_instance_);
    cdc_instance_ = nullptr;
    br_filter_ = nullptr;
    err_handle_ = nullptr;
    curr_trans_id_.reset();
    is_inited_ = false;
    LOG_INFO("ObLogMinerBRProducer destroyed");
    LOGMINER_STDOUT_V("ObLogMinerBRProducer destroyed\n");
  }
}

int ObLogMinerBRProducer::build_cdc_config_map_(const AnalyzerArgs &args,
    std::map<std::string, std::string> &cdc_config)
{
  int ret = OB_SUCCESS;
  const char *REFRESH_MODE = "meta_data_refresh_mode";
  const char *FETCHING_MODE = "fetching_log_mode";
  const char *TENANT_ENDPOINT = "tenant_endpoint";
  const char *TENANT_USER = "tenant_user";
  const char *TENANT_PASSWORD = "tenant_password";
  const char *ARCHIVE_DEST = "archive_dest";
  const char *LOG_LEVEL = "log_level";
  const char *MEMORY_LIMIT = "memory_limit";
  const char *REDO_DISPATCH_LIMIT = "redo_dispatcher_memory_limit";
  const char *SQL_OUTPUT_ORDER = "enable_output_trans_order_by_sql_operation";
  const char *TB_WHITE_LIST = "tb_white_list";

  try {
    cdc_config[LOG_LEVEL] = args.log_level_;
    cdc_config[REFRESH_MODE] = "data_dict";
    cdc_config[MEMORY_LIMIT] = "10G";
    cdc_config[REDO_DISPATCH_LIMIT] = "128M";
    cdc_config[SQL_OUTPUT_ORDER] = "1";
    cdc_config[TB_WHITE_LIST] = args.table_list_;
    if (nullptr != args.archive_dest_) {
      cdc_config[FETCHING_MODE] = "direct";
      cdc_config[ARCHIVE_DEST] = args.archive_dest_;
    } else {
      cdc_config[TENANT_ENDPOINT] = args.cluster_addr_;
      cdc_config[TENANT_USER] = args.user_name_;
      cdc_config[TENANT_PASSWORD] = args.password_;
    }
  } catch (std::exception &e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("build cdc config map failed, exception occurs", "exception_info", e.what());
  }

  return ret;
}

int ObLogMinerBRProducer::get_tenant_compat_mode_(const RecordType type,
    const uint64_t tenant_id,
    lib::Worker::CompatMode &mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("br producer has not been initialized yet", K(is_inited_), K(cdc_instance_));
  } else {
    libobcdc::ObLogInstance *oblog_instance = static_cast<libobcdc::ObLogInstance*>(cdc_instance_);
    if (HEARTBEAT == type) {
      // in OBCDC, tenant_id is invalid for the HEARTBEAT type,
      // and LogMiner won't deal with CompatMode for HEARTBEAT Record.
      mode = lib::Worker::CompatMode::MYSQL;
    } else if (OB_FAIL(oblog_instance->get_tenant_compat_mode(tenant_id, mode))) {
      LOG_ERROR("get tenant compat mode failed", K(oblog_instance), K(tenant_id));
    }
  }
  return ret;
}

int ObLogMinerBRProducer::set_current_trans_id_(ICDCRecord *cdc_rec)
{
  int ret = OB_SUCCESS;
  ObString cdc_trans_id_str;
  uint filter_val_cnt = 0;
  BinlogRecordImpl *br_impl = static_cast<BinlogRecordImpl*>(cdc_rec);
  const binlogBuf *vals = br_impl->filterValues(filter_val_cnt);
  const RecordType type = static_cast<RecordType>(cdc_rec->recordType());
  if (EBEGIN == type) {
    if (OB_ISNULL(vals) || filter_val_cnt <= 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("trans_id values is null for begin record", KP(vals), K(filter_val_cnt));
    } else if (filter_val_cnt > 2) {
      int64_t data = 0, pos = 0;
      ObString start_str;
      cdc_trans_id_str.assign_ptr(vals[1].buf, vals[1].buf_used_size);
      start_str = cdc_trans_id_str.split_on('_');
      if (OB_FAIL(parse_int(cdc_trans_id_str.ptr(), cdc_trans_id_str.length(), pos, data))) {
        LOG_ERROR("failed to parse into for trans_id", K(start_str), K(cdc_trans_id_str));
      } else {
        curr_trans_id_ = data;
        LOG_TRACE("parse trans_id for EBEGIN", K(start_str), K(cdc_trans_id_str), K(data));
      }
    } else {
      LOG_TRACE("defensive branch, do not set trans_id for EBEGIN whose vals is NULL", KP(vals));
    }
  } else if (HEARTBEAT == type || EDDL == type) {
    curr_trans_id_.reset();
    LOG_TRACE("reset trans_id for type HEARTBEAT and EDDL", K(type));
  } else {
    // do not process other types
    LOG_TRACE("ignore set current trans_id for type", K(type));
  }
  return ret;
}

}
}