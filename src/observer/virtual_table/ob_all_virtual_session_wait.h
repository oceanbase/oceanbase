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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/stat/ob_session_stat.h"
#include "observer/virtual_table/ob_all_virtual_diag_index_scan.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace common
{
class ObObj;
}

namespace observer
{

class ObAllVirtualSessionWait : public common::ObVirtualTableScannerIterator,
                                public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSessionWait() : ObVirtualTableScannerIterator(),
    alloc_(ObMemAttr(MTL_ID(), "VT_SessStatus")),
    alloc_wrapper_(),
    session_status_(OB_MALLOC_NORMAL_BLOCK_SIZE, alloc_wrapper_),
    addr_(NULL),
    ipstr_(),
    port_(0),
    session_iter_(0),
    collect_(NULL) { }
  virtual ~ObAllVirtualSessionWait() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);
  inline void set_session_mgr(sql::ObSQLSessionMgr *session_mgr) { session_mgr_ = session_mgr; }


protected:
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual bool is_need_process(uint64_t tenant_id) override {
    if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
      return true;
    }
    return false;
  }
  virtual void release_last_tenant() override;
  virtual int get_all_diag_info();
  inline sql::ObSQLSessionMgr* get_session_mgr() const { return session_mgr_; }
  common::ObArenaAllocator alloc_;
  ObWrapperAllocator alloc_wrapper_;
  common::ObSEArray<std::pair<uint64_t, common::ObDISessionCollect>, 8, ObWrapperAllocator &>
      session_status_;

private:
  enum WAIT_COLUMN
  {
    SESSION_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    TENANT_ID,
    EVENT,
    P1TEXT,
    P1,
    P2TEXT,
    P2,
    P3TEXT,
    P3,
    LEVEL,
    WAIT_CLASS_ID,
    WAIT_CLASS_NO,
    WAIT_CLASS,
    STATE,
    WAIT_TIME_MICRO,
    TIME_REMAINING_MICRO,
    TIME_SINCE_LAST_WAIT_MICRO
  };
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  uint32_t session_iter_;
  sql::ObSQLSessionMgr *session_mgr_;
  common::ObDISessionCollect *collect_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionWait);
};

class ObAllVirtualSessionWaitI1 : public ObAllVirtualSessionWait, public ObAllVirtualDiagIndexScan
{
public:
  ObAllVirtualSessionWaitI1() {}
  virtual ~ObAllVirtualSessionWaitI1() {}
  virtual int inner_open() override
  {
    return set_index_ids(key_ranges_);
  }

protected:
  virtual int get_all_diag_info();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionWaitI1);
};

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT */

