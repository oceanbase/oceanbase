/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_SS_OBJECT_TYPE_IO_STAT_H_
#define OB_ALL_VIRTUAL_SS_OBJECT_TYPE_IO_STAT_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/stat/ob_di_cache.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#endif

namespace oceanbase
{
namespace observer
{

class ObAllVirtualSSObjectTypeIoStat : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSSObjectTypeIoStat();
  virtual ~ObAllVirtualSSObjectTypeIoStat();
  virtual void reset() override;

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

  // omt::ObMultiTenantOperator interface
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  virtual bool is_need_process(uint64_t tenant_id) override;
  #ifdef OB_BUILD_SHARED_STORAGE
  int get_object_type_stat(const int64_t cur_idx_, blocksstable::ObStorageObjectType &object_type,
    ObSSObjectTypeStat &ss_object_type_stat, ObSSObjectTypeCachedStat &ss_object_type_cached_stat, bool &is_remote);
  #endif

private:
  enum TABLE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    OBJECT_TYPE,
    MODE,
    READ_CNT,
    READ_SIZE,
    READ_FAIL_CNT,
    READ_IOPS,
    WRITE_CNT,
    WRITE_SIZE,
    WRITE_FAIL_CNT,
    WRITE_IOPS,
    DELETE_CNT,
    DELETE_FAIL_CNT,
    DELETE_IOPS
  };

private:
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  uint64_t tenant_id_;
  int64_t cur_idx_;
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_SS_OBJECT_TYPE_IO_STAT_H_ */