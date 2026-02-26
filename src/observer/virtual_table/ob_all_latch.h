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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_LATCH_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_LATCH_H_

#include "lib/net/ob_addr.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace common
{
class ObDiagnoseTenantInfo;
}

namespace observer
{

class ObAllLatch : public common::ObVirtualTableScannerIterator,
                   public omt::ObMultiTenantOperator
{
public:
  ObAllLatch() : ObVirtualTableScannerIterator(),
      addr_(NULL),
      latch_iter_(0),
      tenant_di_(allocator_) {}
  virtual ~ObAllLatch() { reset(); }
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual void release_last_tenant() override {
    latch_iter_ = 0;
    tenant_di_.reset();
  }
protected:
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual bool is_need_process(uint64_t tenant_id) override {
    if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
      return true;
    }
    return false;
  }
private:
  int get_the_diag_info(const uint64_t tenant_id);
private:
  enum SYS_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    LATCH_ID,
    NAME,
    ADDR,
    LEVEL,
    HASH,
    GETS,
    MISSES,
    SLEEPS,
    IMMEDIATE_GETS,
    IMMEDIATE_MISSES,
    SPIN_GETS,
    WAIT_TIME
  };
  common::ObAddr *addr_;
  int64_t latch_iter_;
  common::ObDiagnoseTenantInfo tenant_di_;
  DISALLOW_COPY_AND_ASSIGN(ObAllLatch);
}; // end of class ObAllLatch

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_LATCH_H_
