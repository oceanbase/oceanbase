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

#ifndef SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_
#define SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_

#include "common/ob_simple_iterator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_array.h"
#include "storage/compaction/ob_compaction_suggestion.h"
#include "storage/ob_sstable_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace storage
{

class ObTenantSSTableMergeInfoMgr
{
public:
  static int mtl_init(ObTenantSSTableMergeInfoMgr *&sstable_merge_info);
  static int64_t cal_max();
  static int get_next_info(compaction::ObIDiagnoseInfoMgr::Iterator &major_iter,
      compaction::ObIDiagnoseInfoMgr::Iterator &minor_iter,
      ObSSTableMergeInfo &info, char *buf, const int64_t buf_len);
  // TODO need init memory limit with tenant config
  ObTenantSSTableMergeInfoMgr();
  virtual ~ObTenantSSTableMergeInfoMgr();
  int init(const int64_t page_size=compaction::ObIDiagnoseInfoMgr::INFO_PAGE_SIZE);
  int add_sstable_merge_info(ObSSTableMergeInfo &input_info);
  void reset();
  void destroy();
  int open_iter(compaction::ObIDiagnoseInfoMgr::Iterator &major_iter,
                compaction::ObIDiagnoseInfoMgr::Iterator &minor_iter);

  int set_max(int64_t max_size);
  int gc_info();

  // for unittest
  int size();

public:
  static const int64_t MEMORY_PERCENTAGE = 2;   // max size = tenant memory size * MEMORY_PERCENTAGE / 100
  static const int64_t MINOR_MEMORY_PERCENTAGE = 75;
  static const int64_t POOL_MAX_SIZE = 256LL * 1024LL * 1024LL; // 256MB

private:
  bool is_inited_;
  compaction::ObIDiagnoseInfoMgr major_info_pool_;
  compaction::ObIDiagnoseInfoMgr minor_info_pool_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSSTableMergeInfoMgr);
};

}//storage
}//oceanbase

#endif /* SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_ */
