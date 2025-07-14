/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_TENANT_VECTOR_MEM_INFO_H_
#define OB_ALL_VIRTUAL_TENANT_VECTOR_MEM_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTenantVectorMemInfo : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    RAW_MALLOC_SIZE,
    INDEX_METADATA_SIZE,
    VECTOR_MEM_HOLD,
    VECTOR_MEM_USED,
    VECTOR_MEM_LIMIT,
    TX_SHARE_LIMIT,
    VECTOR_MEM_DETAIL_INFO,
  };
  ObAllVirtualTenantVectorMemInfo();
  virtual ~ObAllVirtualTenantVectorMemInfo();
public:
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int64_t fill_glibc_used_info(uint64_t tenant_id);
  common::ObAddr addr_;
  uint64_t current_pos_;
  lib::ObMallocSampleMap::const_iterator it_;
  lib::ObMallocSampleMap malloc_sample_map_;
  char vector_used_str_[OB_MAX_MYSQL_VARCHAR_LENGTH];
  common::ObSEArray<obrpc::ObLSTabletPair, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> complete_tablet_ids_;
  common::ObSEArray<obrpc::ObLSTabletPair, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> partial_tablet_ids_;
  common::ObSEArray<obrpc::ObLSTabletPair, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> cache_tablet_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantVectorMemInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
