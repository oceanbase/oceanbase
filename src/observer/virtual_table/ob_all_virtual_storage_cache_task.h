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

#ifndef OB_ALL_VIRTUAL_STORAGE_CACHE_TASK_H_
#define OB_ALL_VIRTUAL_STORAGE_CACHE_TASK_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/stat/ob_di_cache.h"
#include "observer/omt/ob_multi_tenant_operator.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#endif

namespace oceanbase
{
namespace observer
{
const static int64_t OB_VIRTUAL_TABLE_TABLET_TASK_NUM = 64;

class ObAllVirtualStorageCacheTask : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualStorageCacheTask();
  virtual ~ObAllVirtualStorageCacheTask();
  virtual void reset() override;

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

  // omt::ObMultiTenantOperator interface
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  virtual bool is_need_process(uint64_t tenant_id) override;

private:
  enum TABLE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,	
    TABLET_ID,
    STATUS,
    SPEED,
    START_TIME,
    COMPLETE_TIME,
    RESULT,
    COMMENT
  };

private:
  ObArenaAllocator tmp_str_allocator_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  char speed_buf_[64];
  uint64_t tenant_id_;
  int64_t cur_idx_;

#ifdef OB_BUILD_SHARED_STORAGE
  // This func creates a snapshot of the current map data to prevent
  // issues arising from concurrent modifications during virtual table's iteration
  int copy_tablet_tasks_map_(
      const SCPTabletTaskMap &tablet_tasks_map);
  ObSEArray<ObStorageCacheTabletTaskHandle, OB_VIRTUAL_TABLE_TABLET_TASK_NUM> tablet_tasks_;
#else
  ObSEArray<int64_t, 1> tablet_tasks_;
#endif
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_STORAGE_CACHE_TASK_H_ */