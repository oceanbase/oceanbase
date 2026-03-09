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

#ifndef OB_ALL_VIRTUAL_SS_LOCAL_CACHE_DIAGNOSE_INFO_H_
#define OB_ALL_VIRTUAL_SS_LOCAL_CACHE_DIAGNOSE_INFO_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/stat/ob_di_cache.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_local_cache_stat.h"
#endif

namespace oceanbase
{
namespace observer
{

class ObAllVirtualSSLocalCacheDiagnoseInfo : public common::ObVirtualTableScannerIterator,
                                             public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSSLocalCacheDiagnoseInfo();
  virtual ~ObAllVirtualSSLocalCacheDiagnoseInfo();
  virtual void reset() override;

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

  int get_the_diag_info(const uint64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info);

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
    MOD_NAME,
    SUB_MOD_NAME,
    STATUS,
    MODIFY_TIME,
    DIAGNOSE_INFO,
    EXTRA_INFO
  };

#ifdef OB_BUILD_SHARED_STORAGE
  int get_diagnose_info_list_();
#endif

private:
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  common::ObStringBuf str_buf_;
  uint64_t tenant_id_;
  common::ObDiagnoseTenantInfo tenant_di_info_;
  int64_t cur_idx_;
#ifdef OB_BUILD_SHARED_STORAGE
  ObArray<storage::ObSSLocalCacheDiagnoseInfo> diag_info_list_;
#endif
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_SS_LOCAL_CACHE_DIAGNOSE_INFO_H_ */