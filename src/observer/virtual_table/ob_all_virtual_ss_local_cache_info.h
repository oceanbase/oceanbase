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

#ifndef OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_H_
#define OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_H_

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

class ObAllVirtualSSLocalCacheInfo : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSSLocalCacheInfo();
  virtual ~ObAllVirtualSSLocalCacheInfo();
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
    CACHE_NAME,
    PRIORITY,
    HIT_RATIO,
    TOTAL_HIT_CNT,
    TOTAL_HIT_BYTES,
    TOTAL_MISS_CNT,
    TOTAL_MISS_BYTES,
    HOLD_SIZE,
    ALLOC_DISK_SIZE,
    USED_DISK_SIZE,
    USED_MEM_SIZE
  };

  struct ObSSLocalCacheInfoInst
  {
  public:
    ObSSLocalCacheInfoInst()
      : tenant_id_(OB_INVALID_TENANT_ID), cache_name_(nullptr), priority_(0), hit_ratio_(0),
        total_hit_cnt_(0), total_hit_bytes_(0), total_miss_cnt_(0), total_miss_bytes_(0),
        hold_size_(0), alloc_disk_size_(0), used_disk_size_(0), used_mem_size_(0)
    {}
    virtual ~ObSSLocalCacheInfoInst() {}
  public:
    uint64_t tenant_id_;
    const char *cache_name_;
    int64_t priority_;
    double hit_ratio_;
    int64_t total_hit_cnt_;
    int64_t total_hit_bytes_;
    int64_t total_miss_cnt_;
    int64_t total_miss_bytes_;
    int64_t hold_size_;
    int64_t alloc_disk_size_;
    int64_t used_disk_size_;
    int64_t used_mem_size_;

    TO_STRING_KV(K_(tenant_id), K_(cache_name), K_(priority), K_(hit_ratio), K_(total_hit_cnt),
        K_(total_hit_bytes), K_(total_miss_cnt), K_(total_miss_bytes),
        K_(hold_size), K_(alloc_disk_size), K_(used_disk_size), K_(used_mem_size));
  };

  int add_micro_cache_inst_();
  int add_macro_cache_inst_();
  int get_macro_cache_used_disk_size(ObSSLocalCacheInfoInst &inst);
  int add_tmpfile_cache_inst_();
  int add_macro_block_inst_(const ObSSMacroBlockType macro_block_type);
  int add_local_cache_inst_();
  int set_all_cache_insts_();
#ifdef OB_BUILD_SHARED_STORAGE
  void get_hit_stat_(const ObStorageCacheHitStat &hit_stat, ObSSLocalCacheInfoInst &inst);
#endif

private:
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  common::ObStringBuf str_buf_;
  uint64_t tenant_id_;
  common::ObDiagnoseTenantInfo tenant_di_info_;
  int64_t cur_idx_;
  ObArray<ObSSLocalCacheInfoInst> inst_list_;
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_H_ */