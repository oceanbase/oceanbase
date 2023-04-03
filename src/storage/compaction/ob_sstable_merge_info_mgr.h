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

class ObSSTableMergeInfoIterator
{
public:
  ObSSTableMergeInfoIterator();
  virtual ~ObSSTableMergeInfoIterator();
  int open(const int64_t tenant_id);
  int get_next_merge_info(ObSSTableMergeInfo &merge_info);
  void reset();
private:
  int next_tenant(share::ObTenantSwitchGuard &tenant_guard);
private:
  omt::TenantIdList all_tenants_;
  int64_t tenant_idx_;
  int64_t cur_tenant_id_;
  int64_t major_info_idx_;
  int64_t major_info_cnt_;
  int64_t minor_info_idx_;
  int64_t minor_info_cnt_;
  bool is_opened_;
};

class ObTenantSSTableMergeInfoMgr
{
public:
  static int mtl_init(ObTenantSSTableMergeInfoMgr *&sstable_merge_info);
  // TODO need init memory limit with tenant config
  int init(const int64_t memory_limit = MERGE_INFO_MEMORY_LIMIT);
  int add_sstable_merge_info(ObSSTableMergeInfo &merge_info);
  ObTenantSSTableMergeInfoMgr();
  virtual ~ObTenantSSTableMergeInfoMgr();
  void destroy();

  int get_major_info(const int64_t idx, ObSSTableMergeInfo &merge_info);
  int get_minor_info(const int64_t idx, ObSSTableMergeInfo &merge_info);
  int get_major_info_array_cnt() const { return major_merge_infos_.size(); }
  int get_minor_info_array_cnt() const { return minor_merge_infos_.size(); }
private:
  void release_info(ObSSTableMergeInfo &merge_info);
  static const int64_t MERGE_INFO_MEMORY_LIMIT = 64LL * 1024LL * 1024LL; //64MB
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  compaction::ObInfoRingArray<ObSSTableMergeInfo> major_merge_infos_;
  compaction::ObInfoRingArray<ObSSTableMergeInfo> minor_merge_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSSTableMergeInfoMgr);

};

}//storage
}//oceanbase

#endif /* SRC_STORAGE_OB_SSTABLE_MERGE_INFO_MGR_H_ */
