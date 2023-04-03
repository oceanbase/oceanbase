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

#ifndef OB_ALL_VIRTUAL_DTL_FIRST_CACHED_BUFFER_H
#define OB_ALL_VIRTUAL_DTL_FIRST_CACHED_BUFFER_H

#include "sql/dtl/ob_dtl_channel.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"
#include "sql/dtl/ob_dtl_fc_server.h"


namespace oceanbase
{
namespace observer
{


class ObAllVirtualDtlFirstBufferInfo
{
public:
  ObAllVirtualDtlFirstBufferInfo() :
    tenant_id_(0), channel_id_(0), calced_val_(0), buffer_pool_id_(0), timeout_ts_(0)
  {}

  void set_first_buffer_info(uint64_t tenant_id, sql::dtl::ObDtlCacheBufferInfo *buffer_info);

  TO_STRING_KV(K(tenant_id_), K(channel_id_));

public:
  uint64_t tenant_id_;                // 1
  int64_t channel_id_;
  int64_t calced_val_;
  int64_t buffer_pool_id_;
  int64_t timeout_ts_;                // 5
};

class ObAllVirtualDtlFirstCachedBufferIterator
{
public:
  ObAllVirtualDtlFirstCachedBufferIterator(common::ObArenaAllocator *iter_allocator);
  virtual ~ObAllVirtualDtlFirstCachedBufferIterator();

  void destroy();
  void reset();

  int init();
  int get_tenant_ids();
  int get_next_tenant_buffer_infos();
  int get_tenant_buffer_infos(uint64_t tenant_id);

  int get_all_first_cached_buffer(int64_t tenant_id, sql::dtl::ObTenantDfc *tenant_dfc);
  int get_all_first_cached_buffer_old(int64_t tenant_id, sql::dtl::ObTenantDfc *tenant_dfc);

  int get_next_buffer_info(ObAllVirtualDtlFirstBufferInfo &buffer_info);
private:
  static const int64_t MAX_BUFFER_CAPCITY = 1000;
  int64_t cur_tenant_idx_;
  int64_t cur_buffer_idx_;
  common::ObArenaAllocator *iter_allocator_;
  common::ObArray<uint64_t> tenant_ids_;
  common::ObArray<ObAllVirtualDtlFirstBufferInfo, common::ObWrapperAllocator> buffer_infos_;
};

class ObAllVirtualDtlFirstCachedBuffer : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDtlFirstCachedBuffer();
  virtual ~ObAllVirtualDtlFirstCachedBuffer();

  void destroy();
  void reset();
  int inner_open();
  int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_row(ObAllVirtualDtlFirstBufferInfo &buffer_info, common::ObNewRow *&row);
private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    CHANNEL_ID,
    CALCED_VAL,
    BUFFER_POOL_ID,         // OB_APP_MIN_COLUMN_ID + 5
    TIMEOUT_TS,
  };

private:
  common::ObString ipstr_;
  int32_t port_;
  common::ObArenaAllocator arena_allocator_;
  ObAllVirtualDtlFirstCachedBufferIterator iter_;
};


} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_DTL_FIRST_CACHED_BUFFER_H */
