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

#ifndef OB_STORAGE_COMPACTION_TTL_OB_BASE_VERSION_FILTER_H_
#define OB_STORAGE_COMPACTION_TTL_OB_BASE_VERSION_FILTER_H_

#include "storage/tablet/ob_tablet.h"
#include "sql/engine/basic/ob_base_version_filter_struct.h"

namespace oceanbase
{
namespace storage
{

class ObBaseVersionFilter
{
public:
  ObBaseVersionFilter(ObMDSFilterMgr &mds_filter_mgr)
      : mds_filter_mgr_(mds_filter_mgr),
        base_version_filter_node_(nullptr),
        base_version_filter_executor_(nullptr),
        schema_rowkey_cnt_(-1),
        is_inited_(false)
  {
  }

  ~ObBaseVersionFilter();

  // for query and dml
  int init(ObTablet &tablet, const ObVersionRange &read_version_range);

  // for query reuse
  void reuse();
  int switch_info(ObTablet &tablet, const ObVersionRange &read_version_range);

  // for destory self
  OB_INLINE ObMDSFilterMgr &get_mds_filter_mgr() const { return mds_filter_mgr_; }

  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;

  int check_filter_row_complete(const blocksstable::ObDatumRow &row, bool &complete) const;

  OB_INLINE sql::ObBaseVersionFilterExecutor *get_base_version_filter_executor() const { return base_version_filter_executor_; }
  OB_INLINE bool is_valid() const { return is_inited_; }

  TO_STRING_KV(K(is_inited_), K(schema_rowkey_cnt_), KPC(base_version_filter_node_), KPC(base_version_filter_executor_));

private:
  int init_filter_executor(const int64_t schema_rowkey_cnt, const int64_t base_version);

  // for unittest
  int init_base_version_filter_for_unittest(const int64_t schema_rowkey_cnt,
                                            const int64_t base_version);

private:
  ObMDSFilterMgr &mds_filter_mgr_;

  // base version filter node and executor
  sql::ObBaseVersionFilterNode *base_version_filter_node_;
  sql::ObBaseVersionFilterExecutor *base_version_filter_executor_;

  int64_t schema_rowkey_cnt_;
  bool is_inited_;
};

class ObBaseVersionFilterFactory
{
public:
  static int build_base_version_filter(ObTablet &tablet,
                                       const common::ObVersionRange &read_version_range,
                                       ObMDSFilterMgr &mds_filter_mgr,
                                       ObBaseVersionFilter *&base_version_filter);

  static void destroy_base_version_filter(ObBaseVersionFilter *&base_version_filter);
};

}
}

#endif