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

#ifndef OB_ALL_VIRTUAL_IO_STATUS_H
#define OB_ALL_VIRTUAL_IO_STATUS_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/io/ob_io_calibration.h"
#include "share/io/ob_io_struct.h"
namespace oceanbase
{
namespace observer
{
class ObAllVirtualIOStatusIterator : public ObVirtualTableScannerIterator
{
public:
  ObAllVirtualIOStatusIterator();
  virtual ~ObAllVirtualIOStatusIterator();
  int init_addr(const common::ObAddr &addr);
  virtual void reset() override;
protected:
  bool is_inited_;
  common::ObAddr addr_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOStatusIterator);
};

class ObAllVirtualIOCalibrationStatus : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualIOCalibrationStatus();
  virtual ~ObAllVirtualIOCalibrationStatus();
  int init(const common::ObAddr &addr);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    STORAGE_NAME,
    STATUS,
    START_TIME,
    FINISH_TIME,
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOCalibrationStatus);
private:
  bool is_end_;
  int64_t start_ts_;
  int64_t finish_ts_;
  int ret_code_;
};

class ObAllVirtualIOBenchmark : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualIOBenchmark();
  virtual ~ObAllVirtualIOBenchmark();
  int init(const common::ObAddr &addr);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    STORAGE_NAME,
    MODE,
    SIZE,
    IOPS,
    MBPS,
    LATENCY,
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOBenchmark);
private:
  common::ObIOAbility io_ability_;
  int64_t mode_pos_;
  int64_t size_pos_;
};

class ObAllVirtualIOQuota : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualIOQuota();
  virtual ~ObAllVirtualIOQuota();
  int init(const common::ObAddr &addr);
  int record_user_group(const uint64_t tenant_id, ObIOUsage &io_usage, const ObTenantIOConfig &io_config);
  int record_sys_group(const uint64_t tenant_id, ObIOUsage &sys_io_usage);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    GROUP_ID,
    MODE,
    SIZE,
    MIN_IOPS,
    MAX_IOPS,
    REAL_IOPS,
    MIN_MBPS,
    MAX_MBPS,
    REAL_MBPS,
    SCHEDULE_US,
    IO_DELAY_US,
    TOTAL_US
  };
  struct QuotaInfo
  {
  public:
    QuotaInfo();
    ~QuotaInfo();
    TO_STRING_KV(K(tenant_id_), K(group_id_), K(group_mode_), K(size_), K(real_iops_), K(min_iops_), K(max_iops_), K(schedule_us_), K(io_delay_us_), K(total_us_));
  public:
    uint64_t tenant_id_;
    uint64_t group_id_;
    common::ObIOGroupMode group_mode_;
    int64_t size_;
    int64_t real_iops_;
    int64_t min_iops_;
    int64_t max_iops_;
    int64_t schedule_us_;
    int64_t io_delay_us_;
    int64_t total_us_;
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOQuota);
private:
  ObArray<QuotaInfo> quota_infos_;
  int64_t quota_pos_;
};

class ObAllVirtualIOScheduler : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualIOScheduler();
  virtual ~ObAllVirtualIOScheduler();
  int init(const common::ObAddr &addr);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    THREAD_ID,
    TENANT_ID,
    GROUP_ID,
    QUEUING_COUNT,
    RESERVATION_TS,
    CATEGORY_LIMIT_TS,
    TENANT_LIMIT_TS,
    PROPORTION_TS
  };
  struct ScheduleInfo
  {
  public:
    ScheduleInfo();
    ~ScheduleInfo();
    TO_STRING_KV(K(thread_id_),K(tenant_id_), K(group_id_), K(queuing_count_));
  public:
    uint64_t thread_id_;
    uint64_t tenant_id_;
    uint64_t group_id_;
    int64_t queuing_count_;
    int64_t reservation_ts_;
    int64_t limitation_ts_;
    int64_t proportion_ts_;
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIOScheduler);
private:
  int64_t schedule_pos_;
  ObArray<ScheduleInfo> schedule_infos_;
};

const int64_t GroupIoStatusStringLength = 128;
const int64_t KBYTES = 1024;
const int64_t MBYTES = 1024 * KBYTES;
const int64_t GBYTES = 1024 * MBYTES;
class ObAllVirtualGroupIOStat : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualGroupIOStat();
  virtual ~ObAllVirtualGroupIOStat();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  int init(const common::ObAddr &addr);
  int record_user_group_io_status(const int64_t tenant_id, ObTenantIOManager *io_manager);
  int record_sys_group_io_status(const int64_t tenant_id, ObTenantIOManager *io_manager);
  int convert_bandwidth_format(const int64_t bandwidth, char *buf);
private:
  enum COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    GROUP_ID,
    GROUP_NAME,
    MODE,
    MIN_IOPS,
    MAX_IOPS,
    REAL_IOPS,
    MAX_NET_BANDWIDTH,
    MAX_NET_BANDWIDTH_DISPLAY,
    REAL_NET_BANDWIDTH,
    REAL_NET_BANDWIDTH_DISPLAY,
    NORM_IOPS,
  };
  struct GroupIoStat
  {
  public:
    GroupIoStat() {
      reset();
    }
    ~GroupIoStat() {
      reset();
    }
    void reset() {
      tenant_id_ = 0;
      group_id_ = 0;
      mode_ = common::ObIOMode::MAX_MODE;
      min_iops_ = 0;
      max_iops_ = 0;
      norm_iops_ = 0;
      real_iops_ = 0;
      max_net_bandwidth_ = 0;
      real_net_bandwidth_ = 0;
      memset(group_name_, 0, sizeof(group_name_));
      memset(max_net_bandwidth_display_, 0, sizeof(max_net_bandwidth_display_));
      memset(real_net_bandwidth_display_, 0, sizeof(real_net_bandwidth_display_));
    }
    TO_STRING_KV(K(tenant_id_), K(group_id_), K(mode_), K_(group_name),
                 K(min_iops_), K(max_iops_), K_(norm_iops), K_(real_iops),
                 K_(max_net_bandwidth), K_(max_net_bandwidth_display),
                 K_(real_net_bandwidth), K_(real_net_bandwidth_display));
  public:
    uint64_t tenant_id_;
    uint64_t group_id_;
    common::ObIOMode mode_;
    char group_name_[GroupIoStatusStringLength];
    int64_t min_iops_;
    int64_t max_iops_;
    int64_t norm_iops_;
    int64_t real_iops_;
    int64_t max_net_bandwidth_;
    char max_net_bandwidth_display_[GroupIoStatusStringLength];
    int64_t real_net_bandwidth_;
    char real_net_bandwidth_display_[GroupIoStatusStringLength];
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualGroupIOStat);
private:
  ObArray<GroupIoStat> group_io_stats_;
  int64_t group_io_stats_pos_;
};

class ObAllVirtualFunctionIOStat : public ObAllVirtualIOStatusIterator
{
public:
  ObAllVirtualFunctionIOStat();
  virtual ~ObAllVirtualFunctionIOStat();
  int init(const common::ObAddr &addr);
  int record_function_info(const uint64_t tenant_id, const ObIOFuncUsageArr& func_infos);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  enum COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    FUNCTION_NAME,
    MODE,
    SIZE,
    REAL_IOPS,
    REAL_MBPS,
    SCHEDULE_US,
    IO_DELAY_US,
    TOTAL_US
  };
  struct FuncInfo
  {
  public:
    FuncInfo();
    ~FuncInfo();
    TO_STRING_KV(K(tenant_id_), K(function_type_), K(group_mode_), K(size_), K(real_iops_), K(real_bw_), K(schedule_us_), K(io_delay_us_), K(total_us_));
  public:
    uint64_t tenant_id_;
    share::ObFunctionType function_type_;
    common::ObIOGroupMode group_mode_;
    int64_t size_;
    int64_t real_iops_;
    int64_t real_bw_;
    int64_t schedule_us_;
    int64_t io_delay_us_;
    int64_t total_us_;
  };
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualFunctionIOStat);
private:
  ObArray<FuncInfo> func_infos_;
  int64_t func_pos_;
};



}// namespace observer
}// namespace oceanbase

#endif /* !OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_H */
