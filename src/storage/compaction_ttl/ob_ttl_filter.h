/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_TTL_OB_TTL_FILTER_H_
#define OB_STORAGE_COMPACTION_TTL_OB_TTL_FILTER_H_

#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/compaction_ttl/ob_ttl_filter_val.h"
#include "storage/tablet/ob_tablet.h"
#include "sql/engine/basic/ob_ttl_filter_struct.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace storage
{

enum class ObTTLFilterMode
{
  EMPTY = 0,
  SINGLE_WHITE,   // single rowscn filter, use single_executor_
  SINGLE_OR,      // single column filter (col > ttl OR rowscn > ttl), use single_or_executor_
  AND_MULTI,      // multiple filters, use ttl_filter_executor_
};

struct ObTTLFilterInitParams
{
  ObTTLFilterInitParams(const ObIArray<ObColDesc> &cols_desc,
                        const ObColumnIndexArray *column_index_array = nullptr,
                        const ObIArray<ObColumnParam *> *column_params = nullptr,
                        const ObStorageSchema *schema = nullptr)
      : cols_desc_(cols_desc), column_index_array_(column_index_array),
        column_params_(column_params), schema_(schema)
  {
  }

  TO_STRING_KV(K_(cols_desc), K_(column_index_array), K_(column_params), K_(schema));

  const ObIArray<ObColDesc> &cols_desc_;
  const ObColumnIndexArray *column_index_array_;
  const ObIArray<ObColumnParam *> *column_params_;
  const ObStorageSchema *schema_;
};

class ObTTLFilter
{
public:
  ObTTLFilter(ObArenaAllocator &filter_allocator)
      : filter_allocator_(filter_allocator),
        ttl_info_allocator_("TTLFilterInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        filter_mode_(ObTTLFilterMode::EMPTY),
        single_node_(nullptr),
        single_executor_(nullptr),
        single_or_node_(nullptr),
        single_or_executor_(nullptr),
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
           const ObTTLFilterInitParams &init_params,
           const ObVersionRange &read_version_range);

  // for query reuse
  void reuse();
  int switch_info(ObTablet &tablet,
                  const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                  const ObTTLFilterInitParams &init_params,
                  const ObVersionRange &read_version_range);

  // for compaction
  int init(const int64_t schema_rowkey_cnt,
           const ObTTLFilterInitParams &init_params,
           const ObTTLFilterInfoDistinctMgr &mds_info_mgr);

  // compaction use
  // single fuse row use
  OB_INLINE int filter(const ObDatumRow &row, bool &filtered) const;

  // compaction and query use
  int skip_index_filter(ObAggRowCachedReader &agg_row_cached_reader, sql::ObBoolMask &fal_desc, const ObSkipIndexExtraParam &extra_param = ObSkipIndexExtraParam()) const;

  int check_filter_row_complete(const ObDatumRow &row, bool &complete) const;

  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_empty() const { return ttl_filter_info_array_.empty(); }

  // getter
  OB_INLINE sql::ObPushdownFilterExecutor *get_ttl_filter_executor() const
  {
    // TODO(menglan): use array and index to optimize this select logic
    sql::ObPushdownFilterExecutor *executor = nullptr;
    switch (filter_mode_) {
      case ObTTLFilterMode::SINGLE_WHITE:
        executor = static_cast<sql::ObPushdownFilterExecutor *>(single_executor_);
        break;
      case ObTTLFilterMode::SINGLE_OR:
        executor = static_cast<sql::ObPushdownFilterExecutor *>(single_or_executor_);
        break;
      case ObTTLFilterMode::AND_MULTI:
        executor = static_cast<sql::ObPushdownFilterExecutor *>(ttl_filter_executor_);
        break;
      default:
        break;
    }
    return executor;
  }

  OB_INLINE const ObTTLFilterVal &get_filter_val() const { return filter_val_; }

  TO_STRING_KV(K_(is_inited), K_(filter_mode), K_(filter_val), K_(ttl_filter_info_array));

private:
  static int calc_filter_mode(const ObTTLFilterInfoArray &ttl_filter_info_array, ObTTLFilterMode &filter_mode);

  int init_ttl_filter(const int64_t schema_rowkey_cnt,
                      const ObTTLFilterInitParams &init_params,
                      const ObTTLFilterInfoArray &ttl_filter_info_array);

  template <sql::PushdownFilterType NODE_TYPE_VAL,
            sql::PushdownExecutorType EXECUTOR_TYPE_VAL,
            int64_t CHILD_COUNT = 0,
            typename EXECUTOR_TYPE,
            typename NODE_TYPE,
            typename TTL_FILTER_INFOS>
  int init_or_switch_filter_executor(const TTL_FILTER_INFOS &ttl_filter_info,
                                     const ObTTLFilterInitParams &init_params,
                                     const int64_t schema_rowkey_cnt,
                                     EXECUTOR_TYPE &executor,
                                     NODE_TYPE &node);

  // contains col idx & the max val of this col idx
  int build_filter_helper_arrays(const ObTTLFilterInfoArray &ttl_filter_info_array);

private:
  // for unittest
  int init_ttl_filter_for_unittest(const int64_t schema_rowkey_cnt,
                                   const ObIArray<ObColDesc> &cols_desc,
                                   const ObTTLFilterInfoArray &ttl_filter_info_array);

private:
  ObArenaAllocator &filter_allocator_;
  ObArenaAllocator ttl_info_allocator_;
  ObTTLFilterInfoArray ttl_filter_info_array_;

  // ttl filter info getter
  ObTTLFilterInfoDistinctMgr ttl_filter_info_mgr_;

  // ttl filter node and executor
  ObTTLFilterMode filter_mode_;
  sql::ObTTLWhiteFilterNode *single_node_;
  sql::ObTTLWhiteFilterExecutor *single_executor_;
  sql::ObTTLOrFilterNode *single_or_node_;
  sql::ObTTLOrFilterExecutor *single_or_executor_;
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
                              const ObITableReadInfo &read_info,
                              const common::ObVersionRange &read_version_range,
                              ObArenaAllocator &filter_allocator,
                              ObTTLFilter *&ttl_filter);
};


OB_INLINE int ObTTLFilter::filter(const ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  filtered = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", KR(ret));
  } else if (OB_UNLIKELY(row.row_flag_.is_delete())) {
    filtered = false;
  } else {

    switch (filter_mode_) {
      case ObTTLFilterMode::SINGLE_WHITE:
        ret = single_executor_->filter(row, filtered);
        break;
      case ObTTLFilterMode::SINGLE_OR:
        ret = single_or_executor_->filter(row, filtered);
        break;
      case ObTTLFilterMode::AND_MULTI:
        ret = ttl_filter_executor_->filter(row, filtered);
        break;
      case ObTTLFilterMode::EMPTY:
        ret = OB_SUCCESS;
        break;
    }

    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "Fail to filter", KR(ret), K(filter_mode_), KPC(this));
    }
  }

  return ret;
}

}
}

#endif
