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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_H_

#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/container/ob_se_array.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "observer/omt/ob_multi_tenant_operator.h"


namespace oceanbase
{
namespace storage
{
  class ObITenantMetaObjPool;
}
namespace observer
{

class ObAllVirtualStorageMetaMemoryStatus : public common::ObVirtualTableScannerIterator,
                                            public omt::ObMultiTenantOperator
{
private:
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    NAME,
    USED_SIZE,
    TOTAL_SIZE,
    USED_OBJ_CNT,
    FREE_OBJ_CNT,
    EACH_OBJ_SIZE
  };

public:
  ObAllVirtualStorageMetaMemoryStatus();
  virtual ~ObAllVirtualStorageMetaMemoryStatus();
  int init(common::ObIAllocator *allocator, common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  // 过滤得到需要处理的租户
  virtual bool is_need_process(uint64_t tenant_id) override;
  // 处理当前迭代的租户
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // 释放上一个租户的资源
  virtual void release_last_tenant() override;

private:
  static const int64_t STRING_LEN = 128;
  static const int64_t POOL_NUM = 10;

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char address_[STRING_LEN];
  /* 跨租户访问的资源必须由ObMultiTenantOperator来处理释放*/
  int64_t pool_idx_;
  ObSEArray<ObTenantMetaMemStatus, POOL_NUM> status_arr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualStorageMetaMemoryStatus);
};

}
}

#endif