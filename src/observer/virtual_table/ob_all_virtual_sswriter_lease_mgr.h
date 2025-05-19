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

#ifndef OB_ALL_VIRTUAL_SSWRITER_LEASE_MGR_H_
#define OB_ALL_VIRTUAL_SSWRITER_LEASE_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_simple_iterator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_sswriter_stat.h"
#endif

namespace oceanbase
{
namespace observer
{
class ObAllVirtualSSWriterLeaseMgr : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:
  explicit ObAllVirtualSSWriterLeaseMgr() { reset(); }
  virtual ~ObAllVirtualSSWriterLeaseMgr() { destroy(); }
public:
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row) { return execute(row); }
  virtual void reset();
  virtual void destroy();
public:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int get_next_sswriter_mgr_stat_(ObSSWriterMgrStat &stat);
#endif
private:
  enum
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    LS_ID,
    TYPE,
    GROUP_ID,
    REGION,
    STATE,
    TARGET_SVR_IP,
    TARGET_SVR_PORT,
    CURRENT_TS,
    LAST_UPDATE_STATE_TS,
    LEASE_EXPIRE_TS,
    EPOCH,
  };

  static const int64_t OB_MAX_BUFFER_SIZE = 1024;
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char target_ip_buffer_[common::OB_IP_STR_BUFF];
  common::ObRegion region_;
  int64_t ls_id_;
private:
  bool is_inited_;
  bool is_ready_;
#ifdef OB_BUILD_SHARED_STORAGE
  storage::ObSSWriterMgrStatIterator stat_iter_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSWriterLeaseMgr);
};

}
}
#endif /* OB_ALL_VIRTUAL_SSWRITER_LEASE_MGR_H_ */
