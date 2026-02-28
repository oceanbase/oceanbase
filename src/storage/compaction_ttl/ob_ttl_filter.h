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

#ifndef OB_STORAGE_COMPACTION_TTL_OB_TTL_FILTER_H_
#define OB_STORAGE_COMPACTION_TTL_OB_TTL_FILTER_H_

#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/compaction_ttl/ob_ttl_filter_val.h"
#include "storage/tablet/ob_tablet.h"
#include "sql/engine/basic/ob_ttl_filter_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTTLFilter
{
public:
  ObTTLFilter(ObMDSFilterMgr &mds_filter_mgr)
      : mds_filter_mgr_(mds_filter_mgr),
        ttl_info_allocator_("TTLFilterInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        use_single_ttl_filter_(false),
        single_node_(nullptr),
        single_executor_(nullptr),
        ttl_filter_node_(nullptr),
        ttl_filter_executor_(nullptr),
        schema_rowkey_cnt_(-1),
        filter_val_(),
        is_inited_(false)
  {
  }

  ~ObTTLFilter();

  // for query and dml
  int init(ObTablet &tablet,
           const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
           const ObIArray<ObColDesc> &cols_desc,
           const ObIArray<ObColumnParam *> *cols_param,
           const ObVersionRange &read_version_range);

  // for query reuse
  void reuse();
  int switch_info(ObTablet &tablet,
                  const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                  const ObIArray<ObColDesc> &cols_desc,
                  const ObIArray<ObColumnParam *> *cols_param,
                  const ObVersionRange &read_version_range);

  // for compaction
  int init(const int64_t schema_rowkey_cnt,
           const ObIArray<ObColDesc> &cols_desc,
           const ObIArray<ObColumnParam *> *cols_param,
           const ObTTLFilterInfoDistinctMgr &mds_info_mgr);

  // for destory self
  OB_INLINE ObMDSFilterMgr &get_mds_filter_mgr() const { return mds_filter_mgr_; }

  // compaction use
  // single fuse row use
  OB_INLINE int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;

  OB_INLINE int rowscn_filter(const ObStorageDatum &datum, bool &filtered) const;

  int check_filter_row_complete(const ObDatumRow &row, bool &complete) const;

  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_empty() const { return ttl_filter_info_array_.empty(); }

  // getter
  OB_INLINE sql::ObPushdownFilterExecutor *get_ttl_filter_executor() const
  {
    return use_single_ttl_filter_
               ? static_cast<sql::ObPushdownFilterExecutor *>(single_executor_)
               : static_cast<sql::ObPushdownFilterExecutor *>(ttl_filter_executor_);
  }
  OB_INLINE const ObTTLFilterVal &get_filter_val() const { return filter_val_; }

  TO_STRING_KV(K_(is_inited), K_(filter_val), K(ttl_filter_info_array_));

private:
  int init_ttl_filter(const ObIArray<ObColDesc> &cols_desc,
                      const ObIArray<ObColumnParam *> *cols_param,
                      const ObTTLFilterInfoArray &ttl_filter_info_array);

  // contains col idx & the max val of this col idx
  int build_filter_helper_arrays(const ObTTLFilterInfoArray &ttl_filter_info_array);
private:
  // for unittest
  int init_ttl_filter_for_unittest(const int64_t schema_rowkey_cnt,
                                   const ObIArray<ObColDesc> &cols_desc,
                                   const ObIArray<ObColumnParam *> *cols_param,
                                   const ObTTLFilterInfoArray &ttl_filter_info_array);

private:
  ObMDSFilterMgr &mds_filter_mgr_;
  ObArenaAllocator ttl_info_allocator_;
  ObTTLFilterInfoArray ttl_filter_info_array_;

  // ttl filter info getter
  ObTTLFilterInfoDistinctMgr ttl_filter_info_mgr_;

  // ttl filter node and executor
  bool use_single_ttl_filter_;
  sql::ObTTLWhiteFilterNode *single_node_;
  sql::ObTTLWhiteFilterExecutor *single_executor_;
  sql::ObTTLAndFilterNode *ttl_filter_node_;
  sql::ObTTLAndFilterExecutor *ttl_filter_executor_;

  int64_t schema_rowkey_cnt_;

  ObSEArray<int64_t, 1> filter_col_idx_array_;
  ObTTLFilterVal filter_val_;
  bool is_inited_;
};

class ObTTLFilterFactory
{
public:
  static int build_ttl_filter(ObTablet &tablet,
                              const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                              const common::ObIArray<share::schema::ObColDesc> &cols_desc,
                              const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
                              const common::ObVersionRange &read_version_range,
                              ObMDSFilterMgr &mds_filter_mgr,
                              ObTTLFilter *&ttl_filter);

  static void destroy_ttl_filter(ObTTLFilter *&ttl_filter);
};


OB_INLINE int ObTTLFilter::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  filtered = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", KR(ret));
  } else if (OB_UNLIKELY(row.row_flag_.is_delete())) {
    filtered = false;
  } else if (!ttl_filter_info_array_.empty()
             && OB_FAIL(use_single_ttl_filter_ ? single_executor_->filter(row, filtered)
                                               : ttl_filter_executor_->filter(row, filtered))) {
    STORAGE_LOG(WARN, "Fail to filter", KR(ret));
  }

  return ret;
}


OB_INLINE int ObTTLFilter::rowscn_filter(const ObStorageDatum &datum, bool &filtered) const
{
  // TODO(menglan): This interface is used for do sstable level filter, now only support ora_rowscn

  int ret = OB_SUCCESS;

  filtered = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", KR(ret));
  } else if (!ttl_filter_info_array_.empty()
             && use_single_ttl_filter_
             && single_executor_->is_ora_rowscn()
             && OB_FAIL(single_executor_->filter(&datum, 1, filtered))) {
    STORAGE_LOG(WARN, "Fail to filter", KR(ret), K(datum), K(filtered), KPC(single_executor_));
  }

  return ret;
}

}
}

#endif
