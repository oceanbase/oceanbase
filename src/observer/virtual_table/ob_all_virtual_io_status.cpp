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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_io_status.h"
#include "src/share/ob_server_struct.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObAllVirtualIOStatusIterator::ObAllVirtualIOStatusIterator()
  : is_inited_(false), addr_()
{
  memset(ip_buf_, 0, sizeof(ip_buf_));
}

ObAllVirtualIOStatusIterator::~ObAllVirtualIOStatusIterator()
{

}


int ObAllVirtualIOStatusIterator::init_addr(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    addr_ = addr;
    MEMSET(ip_buf_, 0, sizeof(ip_buf_));
    if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ip to string failed", K(ret), K(addr_));
    }
  }
  return ret;
}

void ObAllVirtualIOStatusIterator::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  addr_.reset();
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

/******************               IOCalibrationStatus                *******************/

ObAllVirtualIOCalibrationStatus::ObAllVirtualIOCalibrationStatus()
  : is_end_(false), start_ts_(0), finish_ts_(0), ret_code_(OB_SUCCESS)
{

}

ObAllVirtualIOCalibrationStatus::~ObAllVirtualIOCalibrationStatus()
{

}

int ObAllVirtualIOCalibrationStatus::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else if (OB_FAIL(ObIOCalibration::get_instance().get_benchmark_status(start_ts_, finish_ts_, ret_code_))) {
    LOG_WARN("get io benchmark timestamp failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObAllVirtualIOCalibrationStatus::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  is_end_ = false;
  start_ts_ = 0;
  finish_ts_ = 0;
  ret_code_ = OB_SUCCESS;
}

int ObAllVirtualIOCalibrationStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (is_end_) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case STORAGE_NAME: {
          cells[i].set_varchar("DATA");
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STATUS: {
          if (0 == start_ts_ && 0 == finish_ts_) {
            cells[i].set_varchar("NOT AVAILABLE");
          } else if (start_ts_ > 0 && 0 == finish_ts_) {
            cells[i].set_varchar("IN PROGRESS");
          } else if (start_ts_ > 0 && finish_ts_ > 0) {
            if (OB_SUCCESS == ret_code_) {
              cells[i].set_varchar("READY");
            } else {
              cells[i].set_varchar("FAILED");
            }
          }
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case START_TIME: {
          if (0 == start_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(start_ts_);
          }
          break;
        }
        case FINISH_TIME: {
          if (0 == finish_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(finish_ts_);
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(column_id), K(i), K(output_column_ids_));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    is_end_ = true;
  }
  return ret;
}

/******************               IOBenchmark                *******************/

ObAllVirtualIOBenchmark::ObAllVirtualIOBenchmark()
  : io_ability_(), mode_pos_(0), size_pos_(0)
{

}

ObAllVirtualIOBenchmark::~ObAllVirtualIOBenchmark()
{

}

int ObAllVirtualIOBenchmark::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else if (OB_FAIL(ObIOCalibration::get_instance().get_io_ability(io_ability_))) {
    LOG_WARN("get io ability failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObAllVirtualIOBenchmark::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  io_ability_.reset();
  mode_pos_ = 0;
  size_pos_ = 0;
}

int ObAllVirtualIOBenchmark::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (!io_ability_.is_valid()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    ObIOBenchResult item;
    while (mode_pos_ < static_cast<int64_t>(ObIOMode::MAX_MODE)) {
      const ObIArray<ObIOBenchResult> &bench_items = io_ability_.get_measure_items(static_cast<ObIOMode>(mode_pos_));
      if (size_pos_ < bench_items.count()) {
        item = bench_items.at(size_pos_);
        ++size_pos_;
        break;
      } else {
        ++mode_pos_;
        size_pos_ = 0;
      }
    }
    if (mode_pos_ >= static_cast<int64_t>(ObIOMode::MAX_MODE)) {
      row = nullptr;
      ret = OB_ITER_END;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case STORAGE_NAME: {
          cells[i].set_varchar("DATA");
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MODE: {
          const char *io_mode_string = get_io_mode_string(item.mode_);
          cells[i].set_varchar(io_mode_string);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIZE: {
          cells[i].set_int(item.size_);
          break;
        }
        case IOPS: {
          cells[i].set_int(static_cast<int64_t>(item.iops_));
          break;
        }
        case MBPS: {
          int64_t mbps = item.size_ * item.iops_ / 1024L / 1024L; // unit MB/s
          cells[i].set_int(mbps);
          break;
        }
        case LATENCY: {
          cells[i].set_int(static_cast<int64_t>(item.rt_us_));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(column_id), K(i), K(output_column_ids_));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

/******************               IOQuota                *******************/

ObAllVirtualIOQuota::QuotaInfo::QuotaInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    group_id_(0),
    group_mode_(ObIOGroupMode::LOCALREAD),
    size_(0),
    real_iops_(0),
    min_iops_(0),
    max_iops_(0),
    schedule_us_(0),
    io_delay_us_(0),
    total_us_(0)
{

}

ObAllVirtualIOQuota::QuotaInfo::~QuotaInfo()
{

}

ObAllVirtualIOQuota::ObAllVirtualIOQuota()
  : quota_infos_(), quota_pos_(0)
{

}

ObAllVirtualIOQuota::~ObAllVirtualIOQuota()
{

}

int ObAllVirtualIOQuota::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObVector<uint64_t> tenant_ids;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else {
    GCTX.omt_->get_tenant_ids(tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (is_virtual_tenant_id(cur_tenant_id)) {
        // do nothing
      } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
        } else {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", K(ret), K(cur_tenant_id));
        }
      } else if (OB_FAIL(record_user_group(cur_tenant_id, tenant_holder.get_ptr()->get_io_usage(), tenant_holder.get_ptr()->get_io_config()))) {
        LOG_WARN("fail to record user group item", K(ret), K(cur_tenant_id), K(tenant_holder.get_ptr()->get_io_config()));
      } else if (OB_FAIL(record_sys_group(cur_tenant_id, tenant_holder.get_ptr()->get_sys_io_usage()))) {
        LOG_WARN("fail to record sys group item", K(ret), K(cur_tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      // TODO fengshuo.fs: remove this after tenant rpc bandwidth supported.
      int64_t ibw = OB_IO_MANAGER.get_tc().get_net_ibw();
      int64_t obw = OB_IO_MANAGER.get_tc().get_net_obw();
      if (ibw > 0) {
        QuotaInfo read;
        read.tenant_id_ = OB_SERVER_TENANT_ID;
        read.group_id_ = -1;
        read.group_mode_ = ObIOGroupMode::LOCALREAD;
        read.real_iops_ = OB_IO_MANAGER.get_tc().get_net_ibw();
        read.max_iops_ = OB_IO_MANAGER.get_tc().get_device_bandwidth();
        if (OB_FAIL(quota_infos_.push_back(read))) {
          LOG_WARN("fail to push ibw info", K(ret));
        }
      }
      if (obw > 0) {
        QuotaInfo write;
        write.tenant_id_ = OB_SERVER_TENANT_ID;
        write.group_id_ = -1;
        write.group_mode_ = ObIOGroupMode::LOCALWRITE;
        write.real_iops_ = OB_IO_MANAGER.get_tc().get_net_obw();
        write.max_iops_ = OB_IO_MANAGER.get_tc().get_device_bandwidth();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(quota_infos_.push_back(write))) {
          LOG_WARN("fail to push obw info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualIOQuota::record_user_group(const uint64_t tenant_id, ObIOUsage &io_usage, const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    const int64_t MODE_COUNT = static_cast<int64_t>(ObIOMode::MAX_MODE) + 1;
    const int64_t GROUP_MODE_CNT = static_cast<int64_t>(ObIOGroupMode::MODECNT);
    io_usage.calculate_io_usage();
    const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &info = io_usage.get_io_usage();
    uint64_t group_config_index = 0;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < info.count(); ++i) {
      if (OB_TMP_FAIL(oceanbase::common::transform_usage_index_to_group_config_index(i, group_config_index))) {
      } else if (group_config_index >= io_config.group_configs_.count()) {
      } else if (io_config.group_configs_.at(group_config_index).deleted_) {
      } else if (info.at(i).avg_byte_ > std::numeric_limits<double>::epsilon()) {
        QuotaInfo item;
        item.tenant_id_ = tenant_id;
        item.group_mode_ = static_cast<ObIOGroupMode>(i % GROUP_MODE_CNT);
        item.group_id_ = io_config.group_configs_.at(group_config_index).group_id_;
        item.size_ = static_cast<int64_t>(info.at(i).avg_byte_);
        item.real_iops_ = static_cast<int64_t>(info.at(i).avg_iops_);
        item.schedule_us_ = info.at(i).avg_schedule_delay_us_;
        item.io_delay_us_ = info.at(i).avg_device_delay_us_;
        item.total_us_ = info.at(i).avg_total_delay_us_;
        int64_t group_min = 0, group_max = 0, group_weight = 0;
        double iops_scale = 0;
        if (OB_FAIL(io_config.get_group_config(group_config_index,
                                               group_min,
                                               group_max,
                                               group_weight))) {
          LOG_WARN("get group config failed", K(ret), K(group_config_index));
        } else {
          LOG_INFO("get group config", K(ret), K(tenant_id), K(group_config_index), K(io_config), K(item), K(group_min), K(group_max), K(group_weight));
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (ObIOGroupMode::LOCALREAD == item.group_mode_ ||
                   ObIOGroupMode::LOCALWRITE == item.group_mode_) {
          const ObIOMode access_mode = (ObIOGroupMode::LOCALREAD == item.group_mode_ ? ObIOMode::READ : ObIOMode::WRITE);
          bool is_io_ability_valid = false; // useless
          ObIOCalibration::get_instance().get_iops_scale(access_mode,
                                                         info.at(i).avg_byte_,
                                                         iops_scale,
                                                         is_io_ability_valid);
          if (!is_io_ability_valid) {
            group_min = group_max = INT64_MAX;
            LOG_INFO("invalid io ability", K(ret), K(item), K(access_mode), K(info), K(iops_scale));
          }
        } else {
          iops_scale = 1.0 / info.at(i).avg_byte_;
        }
        if (OB_SUCC(ret)) {
          item.min_iops_ = group_min == INT64_MAX ? INT64_MAX : static_cast<int64_t>((double)group_min * iops_scale);
          item.max_iops_ = group_max == INT64_MAX ? INT64_MAX : static_cast<int64_t>((double)group_max * iops_scale);
          if (OB_FAIL(quota_infos_.push_back(item))) {
            LOG_WARN("push back io group item failed", K(i), K(ret), K(item));
          } else {
            LOG_INFO("push back item", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualIOQuota::record_sys_group(const uint64_t tenant_id, ObIOUsage &sys_io_usage)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    const int64_t MODE_COUNT = static_cast<int64_t>(ObIOMode::MAX_MODE) + 1;
    const int64_t GROUP_MODE_CNT = static_cast<int64_t>(ObIOGroupMode::MODECNT);
    sys_io_usage.calculate_io_usage();
    const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &info = sys_io_usage.get_io_usage();
    int tmp_ret = OB_SUCCESS;
    uint64_t group_config_index = 0;
    for (uint64_t i = 0; i < info.count(); ++i) {
      if (OB_TMP_FAIL(oceanbase::common::transform_usage_index_to_group_config_index(i, group_config_index))) {
      } else if (info.at(i).avg_byte_ <= std::numeric_limits<double>::epsilon()) {
      } else {
        QuotaInfo item;
        item.tenant_id_ = tenant_id;
        item.group_mode_ = static_cast<ObIOGroupMode>(i % GROUP_MODE_CNT);
        item.group_id_ = SYS_MODULE_START_ID + i / GROUP_MODE_CNT;
        item.size_ = static_cast<int64_t>(info.at(i).avg_byte_);
        item.real_iops_ = static_cast<int64_t>(info.at(i).avg_iops_);
        item.min_iops_ = 0;
        item.max_iops_ = 0;
        item.schedule_us_ = info.at(i).avg_schedule_delay_us_;
        item.io_delay_us_ = info.at(i).avg_device_delay_us_;
        item.total_us_ = info.at(i).avg_total_delay_us_;
        if (OB_FAIL(quota_infos_.push_back(item))) {
          LOG_WARN("push back io group item failed", K(i), K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

void ObAllVirtualIOQuota::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  quota_infos_.reset();
  quota_pos_ = 0;
}

int ObAllVirtualIOQuota::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (quota_pos_ >= quota_infos_.count()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    QuotaInfo &item = quota_infos_.at(quota_pos_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(item.tenant_id_);
          break;
        }
        case GROUP_ID: {
          cells[i].set_int(item.group_id_);
          break;
        }
        case MODE: {
          const char *str = nullptr;
          if (item.group_id_ == -1) {
            str = item.group_mode_ == ObIOGroupMode::LOCALREAD ? "RPC READ" : "RPC WRITE";
          } else {
            str = get_io_mode_string(item.group_mode_);
          }
          cells[i].set_varchar(str);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIZE: {
          cells[i].set_int(static_cast<int64_t>(item.size_));
          break;
        }
        case MIN_IOPS: {
          if (-1 == item.group_id_) {
            cells[i].set_int(0);
          } else if (item.group_mode_ == ObIOGroupMode::REMOTEREAD || item.group_mode_ == ObIOGroupMode::REMOTEWRITE){
            cells[i].set_int(INT64_MAX);
          } else {
            cells[i].set_int(static_cast<int64_t>(item.min_iops_));
          }
          break;
        }
        // TODO fengshuo.fs: remove group_id == -1 after tenant rpc bandwidth supported.
        case MAX_IOPS: {
          if (-1 == item.group_id_) {
            cells[i].set_int(0);
          } else if (item.group_mode_ == ObIOGroupMode::REMOTEREAD || item.group_mode_ == ObIOGroupMode::REMOTEWRITE){
            cells[i].set_int(INT64_MAX);
          } else {
            cells[i].set_int(static_cast<int64_t>(item.max_iops_));
          }
          break;
        }
        case REAL_IOPS: {
          if (-1 == item.group_id_) {
            cells[i].set_int(0);
          } else {
            cells[i].set_int(static_cast<int64_t>(item.real_iops_));
          }
          break;
        }
        case MIN_MBPS: {
          if (item.min_iops_ == INT64_MAX) {
            cells[i].set_int(INT64_MAX);
          } else if (-1 == item.group_id_) {
            cells[i].set_int(static_cast<int64_t>(item.min_iops_ / 1024L / 1024L));
          } else {
            cells[i].set_int(static_cast<int64_t>(item.min_iops_ * item.size_ / 1024L / 1024L));
          }
          break;
        }
        case MAX_MBPS: {
          if (item.max_iops_ == INT64_MAX){
            cells[i].set_int(INT64_MAX);
          } else if (-1 == item.group_id_) {
            cells[i].set_int(static_cast<int64_t>(item.max_iops_ / 1024L / 1024L));
          } else {
            cells[i].set_int(static_cast<int64_t>(item.max_iops_ * item.size_ / 1024L / 1024L));
          }
          break;
        }
        case REAL_MBPS: {
          if (-1 == item.group_id_) {
            cells[i].set_int(static_cast<int64_t>(item.real_iops_ / 1024L / 1024L));
          } else {
            cells[i].set_int(static_cast<int64_t>(item.real_iops_ * item.size_ / 1024L / 1024L));
          }
          break;
        }
        case SCHEDULE_US: {
          cells[i].set_int(item.schedule_us_);
          break;
        }
        case IO_DELAY_US: {
          cells[i].set_int(item.io_delay_us_);
          break;
        }
        case TOTAL_US: {
          cells[i].set_int(item.total_us_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(column_id), K(i), K(output_column_ids_));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++quota_pos_;
  }
  return ret;
}

/******************               IOScheduler                *******************/
ObAllVirtualIOScheduler::ScheduleInfo::ScheduleInfo()
  : thread_id_ (-1),
    tenant_id_(OB_INVALID_TENANT_ID),
    group_id_(0),
    queuing_count_(0),
    reservation_ts_(INT_MAX64),
    limitation_ts_(INT_MAX64),
    proportion_ts_(INT_MAX64)
{

}

ObAllVirtualIOScheduler::ScheduleInfo::~ScheduleInfo()
{

}

ObAllVirtualIOScheduler::ObAllVirtualIOScheduler()
  : schedule_pos_(0), schedule_infos_()
{

}

ObAllVirtualIOScheduler::~ObAllVirtualIOScheduler()
{

}

int ObAllVirtualIOScheduler::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObVector<uint64_t> tenant_ids;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else {
    GCTX.omt_->get_tenant_ids(tenant_ids);
    ObIOScheduler *io_scheduler = OB_IO_MANAGER.get_scheduler();
    int64_t thread_num = io_scheduler->get_senders_count();
    for (int64_t thread_id = 0; OB_SUCC(ret) && thread_id < thread_num; ++thread_id) {
      ObIOSender *cur_sender = io_scheduler->get_sender(thread_id);
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
        const uint64_t cur_tenant_id = tenant_ids.at(i);
        ObRefHolder<ObTenantIOManager> tenant_holder;
        if (is_virtual_tenant_id(cur_tenant_id)) {
          // do nothing
        } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
          } else {
            ret = OB_TENANT_NOT_EXIST;
            LOG_WARN("tenant not exist", K(ret), K(cur_tenant_id));
          }
        } else {
          const ObTenantIOConfig &io_config = tenant_holder.get_ptr()->get_io_config();
          int64_t group_num = io_config.group_configs_.count();
          for (int64_t index = 0; OB_SUCC(ret) && index < group_num; ++index) {
            if (io_config.group_configs_.at(index).deleted_) {
              continue;
            }
            ScheduleInfo item;
            item.thread_id_ = thread_id;
            item.tenant_id_ = cur_tenant_id;
            item.group_id_ = io_config.group_configs_.at(index).group_id_;
            ObSenderInfo sender_info;
            if (OB_FAIL(cur_sender->get_sender_status(cur_tenant_id, index, sender_info))) {
              LOG_WARN("get sender status failed", K(ret), K(cur_tenant_id), K(index));
            } else {
              item.queuing_count_ = sender_info.queuing_count_;
              item.reservation_ts_ = sender_info.reservation_ts_;
              item.limitation_ts_ = sender_info.limitation_ts_;
              item.proportion_ts_ = sender_info.proportion_ts_;
              if (OB_FAIL(schedule_infos_.push_back(item))) {
                LOG_WARN("push back io quota item failed", K(ret), K(item));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObAllVirtualIOScheduler::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  schedule_pos_ = 0;
  schedule_infos_.reset();
}

int ObAllVirtualIOScheduler::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (schedule_pos_ >= schedule_infos_.count()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    ScheduleInfo &item = schedule_infos_.at(schedule_pos_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case THREAD_ID: {
          cells[i].set_int(item.thread_id_);
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(item.tenant_id_);
          break;
        }
        case GROUP_ID: {
          cells[i].set_int(item.group_id_);
          break;
        }
        case QUEUING_COUNT: {
          cells[i].set_int(item.queuing_count_);
          break;
        }
        case RESERVATION_TS: {
          if (INT_MAX64 == item.reservation_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(item.reservation_ts_);
          }
          break;
        }
        case CATEGORY_LIMIT_TS: {
          if (INT_MAX64 == item.limitation_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(item.limitation_ts_);
          }
          break;
        }
        case TENANT_LIMIT_TS: {
          if (INT_MAX64 == item.limitation_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(item.limitation_ts_);
          }
          break;
        }
        case PROPORTION_TS: {
          if (INT_MAX64 == item.proportion_ts_) {
            cells[i].set_null();
          } else {
            cells[i].set_timestamp(item.proportion_ts_);
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(column_id), K(i), K(output_column_ids_));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++schedule_pos_;
  }
  return ret;
}

ObAllVirtualGroupIOStat::ObAllVirtualGroupIOStat()
  : group_io_stats_(), group_io_stats_pos_(0)
  {}

ObAllVirtualGroupIOStat::~ObAllVirtualGroupIOStat()
  {}

void ObAllVirtualGroupIOStat::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  group_io_stats_.reset();
  is_inited_ = false;
  group_io_stats_pos_ = 0;
}

int ObAllVirtualGroupIOStat::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;

  ObVector<uint64_t> tenant_ids;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else {
    GCTX.omt_->get_tenant_ids(tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (is_virtual_tenant_id(cur_tenant_id)) {
        // do nothing
      } else if (is_sys_tenant(effective_tenant_id_) || effective_tenant_id_ == cur_tenant_id) {
        if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
          } else {
            ret = OB_TENANT_NOT_EXIST;
            LOG_WARN("tenant not exist", K(ret), K(cur_tenant_id));
          }
        } else if (OB_FAIL(record_user_group_io_status(cur_tenant_id, tenant_holder.get_ptr()))) {
          LOG_WARN("fail to record group io status", K(ret), K(cur_tenant_id));
        } else if (OB_FAIL(record_sys_group_io_status(cur_tenant_id, tenant_holder.get_ptr()))) {
          LOG_WARN("fail to record sys group io status", K(ret), K(cur_tenant_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObAllVirtualGroupIOStat::record_user_group_io_status(const int64_t tenant_id, ObTenantIOManager *io_manager)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(io_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    ObIOUsage io_usage;
    if (OB_FAIL(io_usage.init(tenant_id, 2))) {
      LOG_WARN("init io usage failed", K(ret));
    } else if (OB_FAIL(io_usage.assign(io_manager->get_io_usage()))) {
      LOG_WARN("assign io usage failed", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      const ObTenantIOConfig io_config = io_manager->get_io_config();
      const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &info = io_usage.get_io_usage();
      const int64_t MODE_COUNT = static_cast<int64_t>(ObIOMode::MAX_MODE) + 1;
      const int64_t GROUP_MODE_CNT = static_cast<int64_t>(ObIOGroupMode::MODECNT);
      uint64_t local_group_config_index = 0;
      uint64_t remote_group_config_index = 0;

      if (info.count() % GROUP_MODE_CNT != 0 ) {
        LOG_WARN("unexpected group count", K(ret), K(tenant_id), K(info.count()));
      } else {
        for (int64_t left = 0; left < info.count() && OB_SUCC(ret); left += GROUP_MODE_CNT) {
          int64_t local_read_index = -1, remote_read_index = -1;
          int64_t local_write_index = -1, remote_write_index = -1;

          local_read_index = left + static_cast<int64_t>(ObIOGroupMode::LOCALREAD);
          remote_read_index = left + static_cast<int64_t>(ObIOGroupMode::REMOTEREAD);
          local_write_index = left + static_cast<int64_t>(ObIOGroupMode::LOCALWRITE);
          remote_write_index = left + static_cast<int64_t>(ObIOGroupMode::REMOTEWRITE);

          int64_t group_min_iops = 0, group_max_iops = 0, group_iops_weight = 0;
          int64_t group_min_net_bandwidth = 0, group_max_net_bandwidth = 0, group_net_bandwidth_weight = 0;

          if (local_read_index < 0 || remote_read_index < 0 ||
              local_write_index < 0 || local_write_index < 0) {
          } else if (OB_TMP_FAIL(oceanbase::common::transform_usage_index_to_group_config_index(local_read_index, local_group_config_index))) {
          } else if (OB_TMP_FAIL(oceanbase::common::transform_usage_index_to_group_config_index(remote_read_index, remote_group_config_index))) {
          } else if (io_config.group_configs_.at(local_group_config_index).cleared_ ||
                     io_config.group_configs_.at(local_group_config_index).deleted_ ||
                     io_config.group_configs_.at(remote_group_config_index).cleared_ ||
                     io_config.group_configs_.at(remote_group_config_index).deleted_) {
            // do nothing
          } else if (OB_FAIL(io_config.get_group_config(local_group_config_index,
                                                      group_min_iops,
                                                      group_max_iops,
                                                      group_iops_weight))) {
            LOG_WARN("get group io config failed", K(ret), K(local_group_config_index));
          } else if (OB_FAIL(io_config.get_group_config(remote_group_config_index,
                                                      group_min_net_bandwidth,
                                                      group_max_net_bandwidth,
                                                      group_net_bandwidth_weight))) {
            LOG_WARN("get group net config failed", K(ret), K(remote_group_config_index));
          } else {
            // local read and remote read
            GroupIoStat read_item;
            read_item.tenant_id_ = tenant_id;
            read_item.mode_ = ObIOMode::READ;
            read_item.group_id_ = io_config.group_configs_.at(local_group_config_index).group_id_;
            const int64_t read_item_group_name_len = std::strlen(io_config.group_configs_.at(local_group_config_index).group_name_) + 1;
            memcpy(read_item.group_name_,
                   io_config.group_configs_.at(local_group_config_index).group_name_,
                   read_item_group_name_len);
            read_item.group_name_[read_item_group_name_len] = '\0';

            read_item.min_iops_ = group_min_iops;
            read_item.max_iops_ = group_max_iops;
            read_item.max_net_bandwidth_ = group_max_net_bandwidth;
            read_item.real_iops_ = info.at(local_read_index).avg_iops_;
            read_item.real_net_bandwidth_ = info.at(remote_read_index).avg_iops_ *
                                            info.at(remote_read_index).avg_byte_;
            read_item.norm_iops_ = oceanbase::common::get_norm_iops(
                info.at(local_read_index).avg_byte_, info.at(local_read_index).avg_iops_, ObIOMode::READ);
            if (OB_FAIL(convert_bandwidth_format(read_item.max_net_bandwidth_,
                                                 read_item.max_net_bandwidth_display_))) {
              LOG_WARN("convert bandwidth format failed", K(ret), K(read_item));
            } else if (OB_FAIL(convert_bandwidth_format(read_item.real_net_bandwidth_,
                                                        read_item.real_net_bandwidth_display_))) {
              LOG_WARN("convert bandwidth format failed", K(ret), K(read_item));
            } else if (OB_FAIL(group_io_stats_.push_back(read_item))) {
              LOG_WARN("push back group io stat failed", K(ret), K(read_item));
            }
            // local write and remote write
            if (OB_FAIL(ret)) {
            } else {
              GroupIoStat write_item;
              write_item.tenant_id_ = tenant_id;
              write_item.mode_ = ObIOMode::WRITE;
              write_item.group_id_ = io_config.group_configs_.at(local_group_config_index).group_id_;
              const int64_t write_item_group_name_len = std::strlen(io_config.group_configs_.at(local_group_config_index).group_name_) + 1;
              memcpy(write_item.group_name_,
                     io_config.group_configs_.at(local_group_config_index).group_name_,
                     write_item_group_name_len);
              write_item.group_name_[write_item_group_name_len] = '\0';

              write_item.min_iops_ = group_min_iops;
              write_item.max_iops_ = group_max_iops;
              write_item.max_net_bandwidth_ = group_max_net_bandwidth;
              write_item.real_iops_ = info.at(local_write_index).avg_iops_;
              write_item.real_net_bandwidth_ = info.at(remote_write_index).avg_iops_ *
                                               info.at(remote_write_index).avg_byte_;
              write_item.norm_iops_ = oceanbase::common::get_norm_iops(
                  info.at(local_write_index).avg_byte_, info.at(local_write_index).avg_iops_, ObIOMode::WRITE);
              if (OB_FAIL(convert_bandwidth_format(write_item.max_net_bandwidth_,
                                                   write_item.max_net_bandwidth_display_))) {
                LOG_WARN("convert bandwidth format failed", K(ret), K(write_item));
              } else if (OB_FAIL(convert_bandwidth_format(write_item.real_net_bandwidth_,
                                                          write_item.real_net_bandwidth_display_))) {
                LOG_WARN("convert bandwidth format failed", K(ret), K(write_item));
              } else if (OB_FAIL(group_io_stats_.push_back(write_item))) {
                LOG_WARN("push back group io stat failed", K(ret), K(write_item));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObAllVirtualGroupIOStat::record_sys_group_io_status(const int64_t tenant_id, ObTenantIOManager *io_manager)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    const int64_t MODE_COUNT = static_cast<int64_t>(ObIOMode::MAX_MODE) + 1;
    const int64_t GROUP_MODE_CNT = static_cast<int64_t>(ObIOGroupMode::MODECNT);
    ObIOUsage sys_io_usage;
    if (OB_FAIL(sys_io_usage.init(tenant_id, 2))) {
      LOG_WARN("init io usage failed", K(ret));
    } else if (OB_FAIL(sys_io_usage.assign(io_manager->get_sys_io_usage()))) {
      LOG_WARN("assign io usage failed", K(ret));
    } else {
      sys_io_usage.calculate_io_usage();
      const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &info = sys_io_usage.get_io_usage();
      int tmp_ret = OB_SUCCESS;
      uint64_t group_config_index = 0;

      if (info.count() % GROUP_MODE_CNT != 0 ) {
        LOG_WARN("unexpected group count", K(ret), K(tenant_id), K(info.count()));
      } else {
        for (int64_t left = 0; left < info.count() && OB_SUCC(ret); left += GROUP_MODE_CNT) {
          int64_t local_read_index = -1, remote_read_index = -1;
          int64_t local_write_index = -1, remote_write_index = -1;

          local_read_index = left + static_cast<int64_t>(ObIOGroupMode::LOCALREAD);
          remote_read_index = left + static_cast<int64_t>(ObIOGroupMode::REMOTEREAD);
          local_write_index = left + static_cast<int64_t>(ObIOGroupMode::LOCALWRITE);
          remote_write_index = left + static_cast<int64_t>(ObIOGroupMode::REMOTEWRITE);

          if (local_read_index < 0 || remote_read_index < 0 ||
               local_write_index < 0 || local_write_index < 0) {
          } else {
            // local read and remote read
            const int64_t sys_group_id =  SYS_MODULE_START_ID + left / GROUP_MODE_CNT;
            GroupIoStat read_item;
            read_item.tenant_id_ = tenant_id;
            read_item.mode_ = ObIOMode::READ;
            read_item.group_id_ = sys_group_id;
            const char *tmp_name = get_io_sys_group_name(static_cast<common::ObIOModule>(sys_group_id));
            const int64_t read_item_group_name_len = std::strlen(tmp_name) + 1;
            memcpy(read_item.group_name_, tmp_name, read_item_group_name_len);
            read_item.group_name_[read_item_group_name_len] = '\0';
            read_item.min_iops_ = 0;
            read_item.max_iops_ = INT64_MAX;
            read_item.max_net_bandwidth_ = INT64_MAX;
            // sys group real net bandwidth = iops * bytes
            read_item.real_iops_ = static_cast<int64_t>(info.at(local_read_index).avg_iops_);
            read_item.real_net_bandwidth_ = static_cast<int64_t>(info.at(remote_read_index).avg_iops_) *
                                            static_cast<int64_t>(info.at(remote_read_index).avg_byte_);
            read_item.norm_iops_ = oceanbase::common::get_norm_iops(
                info.at(local_read_index).avg_byte_, info.at(local_read_index).avg_iops_, ObIOMode::READ);
            if (OB_FAIL(convert_bandwidth_format(read_item.max_net_bandwidth_,
                                                 read_item.max_net_bandwidth_display_))) {
                LOG_WARN("convert bandwidth format failed", K(ret), K(read_item));
            } else if (OB_FAIL(convert_bandwidth_format(read_item.real_net_bandwidth_,
                                                        read_item.real_net_bandwidth_display_))) {
              LOG_WARN("convert bandwidth format failed", K(ret), K(read_item));
            } else if (OB_FAIL(group_io_stats_.push_back(read_item))) {
              LOG_WARN("push back group io stat failed", K(ret), K(read_item));
            }
            // local write and remote write
            if (OB_FAIL(ret)) {
            } else {
              GroupIoStat write_item;
              write_item.tenant_id_ = tenant_id;
              write_item.mode_ = ObIOMode::WRITE;
              write_item.group_id_ = sys_group_id;
              const char *tmp_name = get_io_sys_group_name(static_cast<common::ObIOModule>(sys_group_id));
              const int64_t write_item_group_name_len = std::strlen(tmp_name) + 1;
              memcpy(write_item.group_name_, tmp_name, std::strlen(tmp_name));
              write_item.group_name_[write_item_group_name_len] = '\0';
              write_item.min_iops_ = 0;
              write_item.max_iops_ = INT64_MAX;
              write_item.max_net_bandwidth_ = INT64_MAX;
              // sys group real net bandwidth = iops * bytes
              write_item.real_iops_ = static_cast<int64_t>(info.at(local_write_index).avg_iops_);
              write_item.real_net_bandwidth_ = static_cast<int64_t>(info.at(remote_write_index).avg_iops_) *
                                               static_cast<int64_t>(info.at(remote_write_index).avg_byte_);
              write_item.norm_iops_ = oceanbase::common::get_norm_iops(
                  info.at(local_write_index).avg_byte_, info.at(local_write_index).avg_iops_, ObIOMode::WRITE);
              if (OB_FAIL(convert_bandwidth_format(write_item.max_net_bandwidth_,
                                                   write_item.max_net_bandwidth_display_))) {
                  LOG_WARN("convert bandwidth format failed", K(ret), K(write_item));
              } else if (OB_FAIL(convert_bandwidth_format(write_item.real_net_bandwidth_,
                                                          write_item.real_net_bandwidth_display_))) {
                LOG_WARN("convert bandwidth format failed", K(ret), K(write_item));
              } else if (OB_FAIL(group_io_stats_.push_back(write_item))) {
                LOG_WARN("push back group io stat failed", K(ret), K(write_item));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualGroupIOStat::convert_bandwidth_format(const int64_t bandwidth, char *buf)
{
  int ret = OB_SUCCESS;

  if (bandwidth < 0 || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bandwidth), KP(buf));
  } else if (bandwidth == INT64_MAX) {
    sprintf(buf, "unlimited");
  } else {
    if (bandwidth < KBYTES) {
      sprintf(buf, "%ldB/s", bandwidth);
    } else if (bandwidth < MBYTES) {
      double kb = static_cast<double>(bandwidth) / KBYTES;
      sprintf(buf, "%.3fKB/s", kb);
    } else if (bandwidth < GBYTES) {
      double mb = static_cast<double>(bandwidth) / MBYTES;
      sprintf(buf, "%.3fMB/s", mb);
    } else {
      double gb = static_cast<double>(bandwidth) / GBYTES;
      sprintf(buf, "%.3fGB/s", gb);
    }
  }

  return ret;
}

int ObAllVirtualGroupIOStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (group_io_stats_pos_ >= group_io_stats_.count()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    GroupIoStat &item = group_io_stats_.at(group_io_stats_pos_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case TENANT_ID: {
          cells[i].set_int(item.tenant_id_);
          break;
        }
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case GROUP_ID: {
          cells[i].set_int(item.group_id_);
          break;
        }
        case GROUP_NAME: {
          cells[i].set_varchar(item.group_name_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MODE: {
          cells[i].set_varchar(get_io_mode_string(item.mode_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MAX_IOPS: {
          cells[i].set_int(item.max_iops_);
          break;
        }
        case MIN_IOPS: {
          cells[i].set_int(item.min_iops_);
          break;
        }
        case NORM_IOPS: {
          cells[i].set_int(item.norm_iops_);
          break;
        }
        case REAL_IOPS: {
          cells[i].set_int(item.real_iops_);
          break;
        }
        case MAX_NET_BANDWIDTH: {
          cells[i].set_int(item.max_net_bandwidth_);
          break;
        }
        case MAX_NET_BANDWIDTH_DISPLAY: {
          cells[i].set_varchar(item.max_net_bandwidth_display_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case REAL_NET_BANDWIDTH: {
          cells[i].set_int(item.real_net_bandwidth_);
          break;
        }
        case REAL_NET_BANDWIDTH_DISPLAY: {
          cells[i].set_varchar(item.real_net_bandwidth_display_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++group_io_stats_pos_;
  }

  return ret;
}

/******************              Function IO Stat                *******************/
ObAllVirtualFunctionIOStat::FuncInfo::FuncInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    function_type_(share::ObFunctionType::DEFAULT_FUNCTION),
    group_mode_(ObIOGroupMode::MODECNT),
    size_(0),
    real_iops_(0),
    real_bw_(0),
    schedule_us_(0),
    io_delay_us_(0),
    total_us_(0)
{

}

ObAllVirtualFunctionIOStat::FuncInfo::~FuncInfo()
{

}

ObAllVirtualFunctionIOStat::ObAllVirtualFunctionIOStat()
  : func_infos_(), func_pos_(0)
{

}
ObAllVirtualFunctionIOStat::~ObAllVirtualFunctionIOStat()
{

}
int ObAllVirtualFunctionIOStat::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObVector<uint64_t> tenant_ids;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else {
    (void)GCTX.omt_->get_tenant_ids(tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (is_virtual_tenant_id(cur_tenant_id)) {
        // do nothing
      } else if ((!is_sys_tenant(effective_tenant_id_)) && (effective_tenant_id_ != cur_tenant_id)) {
      } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
        } else {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", K(ret), K(cur_tenant_id));
        }
      } else if (OB_FAIL(record_function_info(cur_tenant_id, tenant_holder.get_ptr()->get_io_func_infos().func_usages_))) {
        LOG_WARN("fail to record function item", K(ret), K(cur_tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObAllVirtualFunctionIOStat::record_function_info(const uint64_t tenant_id,
    const ObIOFuncUsageArr &func_usages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    const int FUNC_NUM = static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM);
    const int GROUP_MODE_NUM = static_cast<uint8_t>(ObIOGroupMode::MODECNT);
    for (int i = 0; OB_SUCC(ret) && i < FUNC_NUM; ++i) {
      for (int j = 0; OB_SUCC(ret) && j < GROUP_MODE_NUM; ++j) {
        FuncInfo item;
        item.tenant_id_ = tenant_id;
        item.function_type_ = static_cast<share::ObFunctionType>(i);
        item.group_mode_ = static_cast<ObIOGroupMode>(j);
        if (i >= func_usages.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("func usages out of range", K(i), K(func_usages.count()));
        } else if (j >= func_usages.at(i).count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("func usages by mode out of range", K(i), K(j), K(func_usages.at(i).count()));
        } else {
          item.size_ = static_cast<int64_t>(func_usages.at(i).at(j).last_stat_.avg_size_ + 0.5);
          item.real_iops_ = static_cast<int64_t>(func_usages.at(i).at(j).last_stat_.avg_iops_ + 0.99);
          item.real_bw_ = func_usages.at(i).at(j).last_stat_.avg_bw_;
          item.schedule_us_ = func_usages.at(i).at(j).last_stat_.avg_delay_arr_.schedule_delay_us_;
          item.io_delay_us_ = func_usages.at(i).at(j).last_stat_.avg_delay_arr_.device_delay_us_;
          item.total_us_ = func_usages.at(i).at(j).last_stat_.avg_delay_arr_.total_delay_us_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(func_infos_.push_back(item))) {
          LOG_WARN("fail to push back func info", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

void ObAllVirtualFunctionIOStat::reset()
{
  ObAllVirtualIOStatusIterator::reset();
  func_infos_.reset();
  func_pos_ = 0;
}

int ObAllVirtualFunctionIOStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else if (func_pos_ >= func_infos_.count()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    const FuncInfo &item = func_infos_.at(func_pos_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(item.tenant_id_);
          break;
        }
        case FUNCTION_NAME: {
          cells[i].set_varchar(get_io_function_name(item.function_type_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MODE: {
          const char *str = get_io_mode_string(item.group_mode_);
          cells[i].set_varchar(str);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIZE: {
          cells[i].set_int(item.size_);
          break;
        }
        case REAL_IOPS: {
          cells[i].set_int(item.real_iops_);
          break;
        }
        case REAL_MBPS: {
          cells[i].set_int(item.real_bw_ / 1024L / 1024L);
          break;
        }
        case SCHEDULE_US: {
          cells[i].set_int(item.schedule_us_);
          break;
        }
        case IO_DELAY_US: {
          cells[i].set_int(item.io_delay_us_);
          break;
        }
        case TOTAL_US: {
          cells[i].set_int(item.total_us_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(column_id), K(i), K(output_column_ids_));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++func_pos_;
  }
  return ret;
}
}// namespace observer
}// namespace oceanbase
