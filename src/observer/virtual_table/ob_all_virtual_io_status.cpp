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
    category_(ObIOCategory::MAX_CATEGORY),
    mode_(ObIOMode::MAX_MODE),
    size_(0),
    real_iops_(0),
    min_iops_(0),
    max_iops_(0)
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
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(init_addr(addr))) {
    LOG_WARN("init failed", K(ret), K(addr));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant id failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (OB_SERVER_TENANT_ID == cur_tenant_id) {
        // do nothing
      } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
        } else {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", K(ret), K(cur_tenant_id));
        }
      } else {
        const ObIOUsage &io_usage = tenant_holder.get_ptr()->get_io_usage();
        const ObTenantIOConfig &io_config = tenant_holder.get_ptr()->get_io_config();
        ObIOUsage::AvgItems avg_iops, avg_size, avg_rt;
        io_usage.get_io_usage(avg_iops, avg_size, avg_rt);
        for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
          for (int64_t j = 0; OB_SUCC(ret) && j < static_cast<int>(ObIOMode::MAX_MODE); ++j) {
            if (avg_size[i][j] > std::numeric_limits<double>::epsilon()) {
              QuotaInfo item;
              item.tenant_id_ = cur_tenant_id;
              item.category_ = static_cast<ObIOCategory>(i);
              item.mode_ = static_cast<ObIOMode>(j);
              item.size_ = avg_size[i][j];
              item.real_iops_ = avg_iops[i][j];
              int64_t category_min_iops = 0, category_max_iops = 0, category_iops_weight = 0;
              double iops_scale = 0;
              bool is_io_ability_valid = true;
              if (OB_FAIL(io_config.get_category_config(static_cast<ObIOCategory>(i),
                                                        category_min_iops,
                                                        category_max_iops,
                                                        category_iops_weight))) {
                LOG_WARN("get category config failed", K(ret), "category", get_io_category_name(static_cast<ObIOCategory>(i)));
              } else if (OB_FAIL(ObIOCalibration::get_instance().get_iops_scale(static_cast<ObIOMode>(j), avg_size[i][j], iops_scale, is_io_ability_valid))) {
                LOG_WARN("get iops scale failed", K(ret), "mode", get_io_mode_string(static_cast<ObIOMode>(j)));
              } else {
                item.min_iops_ = category_min_iops * iops_scale;
                item.max_iops_ = category_max_iops * iops_scale;
                if (OB_FAIL(quota_infos_.push_back(item))) {
                  LOG_WARN("push back io quota item failed", K(ret), K(item));
                }
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
        case CATEGORY: {
          cells[i].set_varchar(get_io_category_name(item.category_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MODE: {
          cells[i].set_varchar(get_io_mode_string(item.mode_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIZE: {
          cells[i].set_int(static_cast<int64_t>(round(item.size_)));
          break;
        }
        case MIN_IOPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.min_iops_)));
          break;
        }
        case MAX_IOPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.max_iops_)));
          break;
        }
        case REAL_IOPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.real_iops_)));
          break;
        }
        case MIN_MBPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.min_iops_ * item.size_ / 1024L / 1024L)));
          break;
        }
        case MAX_MBPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.max_iops_ * item.size_ / 1024L / 1024L)));
          break;
        }
        case REAL_MBPS: {
          cells[i].set_int(static_cast<int64_t>(round(item.real_iops_ * item.size_ / 1024L / 1024L)));
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

}// namespace observer
}// namespace oceanbase

