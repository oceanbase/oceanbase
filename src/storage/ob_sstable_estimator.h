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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_ESTIMATOR_H
#define OCEANBASE_STORAGE_OB_SSTABLE_ESTIMATOR_H

#include "storage/ob_sstable.h"

namespace oceanbase {
namespace storage {

struct ObSSTableEstimateContext {
public:
  ObSSTableEstimateContext();
  ~ObSSTableEstimateContext();
  void reset();
  bool is_valid() const;

  ObSSTable* sstable_;
  union {
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys_;  // for multi get and single get
    const common::ObExtStoreRange* range_;                       // for scan
    const common::ObIArray<common::ObExtStoreRange>* ranges_;    // for multi scan
  };
  common::ObExtStoreRange multi_version_range_;
  blocksstable::ObStorageCacheContext cache_context_;
  static const int64_t LOCAL_ARRAY_SIZE = 16;
  common::ObSEArray<blocksstable::ObMacroBlockCtx, LOCAL_ARRAY_SIZE> macro_blocks_;

  TO_STRING_KV(KP_(sstable), KP_(rowkeys), K_(macro_blocks), K_(multi_version_range));
};

class ObISSTableEstimator {
public:
  explicit ObISSTableEstimator()
  {}
  virtual ~ObISSTableEstimator()
  {}
  virtual int set_context(ObSSTableEstimateContext& context)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!context.is_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "failed to set context", K(ret), K(context));
    } else {
      context_ = context;
    }
    return ret;
  }
  virtual int estimate_row_count(ObPartitionEst& part_est) = 0;

protected:
  void reset()
  {
    context_.reset();
  }

  // This method is used for estimate border micro block row count in two cases:
  // 1. consider_multi_version = false in major sstable;
  // 2. consider_multi_version = true in minor major sstable, where multiple versions
  //    of row keys should be considered.
  int estimate_border_row_count(const blocksstable::ObMicroBlockInfo& micro_info,
      const blocksstable::ObMacroBlockCtx& macro_block_ctx, bool consider_multi_version, int64_t& logical_row_count,
      int64_t& physical_row_count);
  static int get_rowkey_column_desc(
      const blocksstable::ObFullMacroBlockMeta& meta, common::ObIArray<share::schema::ObColDesc>& columns);

protected:
  ObSSTableEstimateContext context_;
};

class ObISSTableEstimatorWrapper {
public:
  explicit ObISSTableEstimatorWrapper() : estimator_(NULL)
  {}
  virtual ~ObISSTableEstimatorWrapper()
  {
    reset();
  }
  virtual int estimate_row_count(ObPartitionEst& part_est)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(estimator_)) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "estimator is null", K(ret));
    } else if (OB_FAIL(estimator_->estimate_row_count(part_est))) {
      STORAGE_LOG(WARN, "failed to estimate cost", K(ret));
    }
    return ret;
  }

  void reset()
  {
    if (NULL != estimator_) {
      estimator_->~ObISSTableEstimator();
      estimator_ = NULL;
    }
    allocator_.reset();
  }

  int set_context(ObSSTableEstimateContext& context)
  {
    int ret = common::OB_SUCCESS;
    if (!context.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "estimate context is not valid", K(ret), K(context));
    } else if (OB_FAIL(alloc_estimator(*context.sstable_))) {
      STORAGE_LOG(WARN, "failed to alloc estimator", K(ret));
    } else if (OB_FAIL(estimator_->set_context(context))) {
      STORAGE_LOG(WARN, "failed to set context", K(ret), K(context));
    }
    return ret;
  }

protected:
  virtual int alloc_estimator(const ObSSTable& sstable) = 0;

protected:
  common::ObArenaAllocator allocator_;
  ObISSTableEstimator* estimator_;
};

class ObStoreRowMultiGetEstimator : public ObISSTableEstimator {
public:
  ObStoreRowMultiGetEstimator();
  virtual ~ObStoreRowMultiGetEstimator();
  virtual int set_context(ObSSTableEstimateContext& context) override;
  virtual int estimate_row_count(ObPartitionEst& part_est) override;
};

class ObSSTableScanEstimator : public ObISSTableEstimatorWrapper {
public:
  ObSSTableScanEstimator()
  {}
  virtual ~ObSSTableScanEstimator()
  {}
  virtual int alloc_estimator(const ObSSTable& sstable) override;
};

class ObStoreRowSingleScanEstimator : public ObISSTableEstimator {
public:
  ObStoreRowSingleScanEstimator();
  virtual ~ObStoreRowSingleScanEstimator();
  int set_context(ObSSTableEstimateContext& context);
  int open();
  void reset();
  virtual int estimate_row_count(ObPartitionEst& part_est);

private:
  int estimate_macro_row_count(const blocksstable::ObMacroBlockCtx& macro_block_ctx, const bool is_start_block,
      const bool is_last_block, ObPartitionEst& part_est);
  int estimate_border_cost(const common::ObIArray<blocksstable::ObMicroBlockInfo>& micro_infos,
      const blocksstable::ObMacroBlockCtx& macro_block_ctx, const bool check_start, const int64_t average_row_count,
      const int64_t average_row_size, int64_t& micro_block_count, ObPartitionEst& part_est);
  int estimate_border_count_(const blocksstable::ObMicroBlockInfo& micro_info,
      const blocksstable::ObMacroBlockCtx& macro_block_ctx, const int64_t average_row_size, int64_t& result_row_count,
      int64_t& micro_block_count, ObPartitionEst& part_est);
  static int get_common_prefix_length(
      const common::ObStoreRowkey& key1, const common::ObStoreRowkey& key2, int64_t& length);
  static int get_common_rowkey(const common::ObStoreRange& range, common::ObStoreRowkey& rowkey);

private:
  bool is_empty_scan_;
  bool is_first_scan_;
};

class ObMultiVersionSingleScanEstimator : public ObISSTableEstimator {
public:
  ObMultiVersionSingleScanEstimator(common::ObArenaAllocator& allocator) : allocator_(allocator)
  {}
  virtual ~ObMultiVersionSingleScanEstimator()
  {
    reset();
  }
  int set_context(ObSSTableEstimateContext& context) override;
  int estimate_row_count(ObPartitionEst& part_est) override;

private:
  int estimate_macro_row_count(const blocksstable::ObMacroBlockCtx& macro_block_ctx, const bool is_left_border,
      const bool is_right_border, ObPartitionEst& part_est, int64_t& gap_size, int64_t& purged_phy_row_count);

private:
  common::ObArenaAllocator& allocator_;
};

class ObStoreRowMultiScanEstimator : public ObISSTableEstimator {
public:
  ObStoreRowMultiScanEstimator();
  virtual ~ObStoreRowMultiScanEstimator();
  virtual int estimate_row_count(ObPartitionEst& part_est);
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OCEANBASE_STORAGE_OB_SSTABLE_ESTIMATOR_H */
