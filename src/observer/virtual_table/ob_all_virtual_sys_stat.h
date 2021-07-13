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

#ifndef OB_ALL_VIRTUAL_SYS_STAT_H_
#define OB_ALL_VIRTUAL_SYS_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/statistic_event/ob_stat_class.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_tenant_mgr.h"
#include "observer/virtual_table/ob_all_virtual_diag_index_scan.h"

namespace oceanbase {
namespace observer {

class ObAllVirtualSysStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualSysStat();
  virtual ~ObAllVirtualSysStat();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = &addr;
  }
  virtual int set_ip(common::ObAddr* addr);

protected:
  virtual int get_all_diag_info();
  common::ObSEArray<std::pair<uint64_t, common::ObDiagnoseTenantInfo*>, common::OB_MAX_SERVER_TENANT_CNT> tenant_dis_;

private:
  int64_t get_tenant_memory_usage(int64_t tenant_id) const;
  int get_cache_size(ObStatEventSetStatArray& stat_events);

private:
  enum SYS_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    STATISTIC,
    VALUE,
    STAT_ID,
    NAME,
    CLASS,
    CAN_VISIBLE
  };
  common::ObAddr* addr_;
  common::ObString ipstr_;
  int32_t port_;
  int32_t iter_;
  int32_t stat_iter_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSysStat);
};

class ObAllVirtualSysStatI1 : public ObAllVirtualSysStat, public ObAllVirtualDiagIndexScan {
public:
  ObAllVirtualSysStatI1()
  {}
  virtual ~ObAllVirtualSysStatI1()
  {}
  virtual int inner_open() override
  {
    return set_index_ids(key_ranges_);
  }

protected:
  virtual int get_all_diag_info();

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSysStatI1);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_SYS_STAT_H_ */
