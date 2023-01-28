// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of __all_virtual_plan_real_info
#ifndef SRC_OBSERVER_VIRTUAL_PLAN_REAL_INFO_H_
#define SRC_OBSERVER_VIRTUAL_PLAN_REAL_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/mysql/ob_ra_queue.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{
class ObPlanRealInfoMgr;
class ObPlanRealInfoRecord;
}
namespace common
{
class ObIAllocator;
}

namespace share
{
class ObTenantSpaceFetcher;
}

namespace observer
{
class ObAllVirtualPlanRealInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualPlanRealInfo ();
  virtual ~ObAllVirtualPlanRealInfo();

  int inner_open();
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);
  int check_ip_and_port(bool &is_valid);
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int extract_tenant_ids();
  int fill_cells(sql::ObPlanRealInfoRecord &record);

private:
  enum WAIT_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    PLAN_ITEM_ID,
    SQL_ID,
    PLAN_ID,
    PLAN_HASH,
    ID,
    REAL_COST,
    REAL_CARDINALITY,
    CPU_COST,
    IO_COST
  };

  const static int64_t KEY_TENANT_ID_IDX = 0;
  const static int64_t KEY_IP_IDX        = 1;
  const static int64_t KEY_PORT_IDX      = 2;
  const static int64_t ROWKEY_COUNT      = 4;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPlanRealInfo);
  sql::ObPlanRealInfoMgr *plan_real_info_mgr_;
  int64_t start_id_;
  int64_t end_id_;
  int64_t cur_id_;
  common::ObRaQueue::Ref ref_;
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];

  bool is_first_get_;

  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;

  share::ObTenantSpaceFetcher *with_tenant_ctx_;
};
}
}

#endif /* SRC_OBSERVER_VIRTUAL_PLAN_REAL_INFO_H_ */