/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_H_
#define OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualKVCacheStoreMemblock : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualKVCacheStoreMemblock();
  virtual ~ObAllVirtualKVCacheStoreMemblock();
  virtual void reset();
  OB_INLINE void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  virtual int set_ip();
  virtual int inner_open() override;
  int process_row(const ObKVCacheStoreMemblockInfo &info);
private:
  enum CACHE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    CACHE_ID,
    CACHE_NAME,
    MEMBLOCK_PTR,
    REF_COUNT,
    STATUS,
    POLICY,
    KV_CNT,
    GET_CNT,
    RECENT_GET_CNT,
    PRIORITY,
    SCORE,
    ALIGN_SIZE
  };
  int64_t memblock_iter_;
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  ObSEArray<ObKVCacheStoreMemblockInfo, 1024> memblock_infos_;
  common::ObStringBuf str_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKVCacheStoreMemblock);
};

}  // observer
}  // oceanbase

#endif // OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_H_
