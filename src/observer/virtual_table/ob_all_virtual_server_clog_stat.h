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

#ifndef OCEANBASE_OB_ALL_VIRTUAL_SERVER_CLOG_STAT_H_
#define OCEANBASE_OB_ALL_VIRTUAL_SERVER_CLOG_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_zone_info.h"

namespace oceanbase {
namespace share {
class ObLocalityInfo;
}
namespace observer {
class ObAllVirtualServerClogStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualServerClogStat();
  virtual ~ObAllVirtualServerClogStat();
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = &addr;
  }
  virtual int set_ip(common::ObAddr* addr);

private:
  enum DISK_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    SYSTEM_CLOG_MIN_USING_FILE_ID,
    USER_CLOG_MIN_USING_FILE_ID,
    SYSTEM_ILOG_MIN_USING_FILE_ID,
    USER_ILOG_MIN_USING_FILE_ID,
    ZONE,
    REGION,
    IDC,
    ZONE_TYPE,
    MERGE_STATUS,
    ZONE_STATUS,
    SVR_MIN_LOG_TIMESTAMP
  };

  struct LocalLocalityInfo {
    LocalLocalityInfo()
        : region_(),
          zone_(),
          idc_(),
          zone_type_(common::ObZoneType::ZONE_TYPE_INVALID),
          merge_status_(share::ObZoneInfo::MERGE_STATUS_MAX),
          zone_status_(share::ObZoneStatus::UNKNOWN)
    {}
    ~LocalLocalityInfo()
    {}

    void reset();
    int assign(const share::ObLocalityInfo &locality_info);

    common::ObRegion region_;
    common::ObZone zone_;
    common::ObIDC idc_;
    common::ObZoneType zone_type_;
    share::ObZoneInfo::MergeStatus merge_status_;
    share::ObZoneStatus::Status zone_status_;
  };

  common::ObAddr* addr_;
  common::ObString ipstr_;
  int32_t port_;
  bool is_end_;
  LocalLocalityInfo local_locality_info_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerClogStat);
};

}  // namespace observer
}  // namespace oceanbase

#endif  // OCEANBASE_OB_ALL_VIRTUAL_SERVER_CLOG_STAT_H_
