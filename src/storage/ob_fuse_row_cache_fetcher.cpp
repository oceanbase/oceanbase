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

#define USING_LOG_PREFIX STORAGE

#include "ob_fuse_row_cache_fetcher.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "blocksstable/ob_storage_cache_suite.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

ObFuseRowCacheFetcher::ObFuseRowCacheFetcher() : is_inited_(false), access_param_(nullptr), access_ctx_(nullptr)
{}

int ObFuseRowCacheFetcher::init(const ObTableAccessParam& access_param, ObTableAccessContext& access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!access_param.is_valid() || !access_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(access_param), K(access_ctx));
  } else {
    access_param_ = &access_param;
    access_ctx_ = &access_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObFuseRowCacheFetcher::get_fuse_row_cache(const ObStoreRowkey& rowkey, ObFuseRowValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFuseRowCacheFetcher has not been inited", K(ret));
  } else {
    ObFuseRowCacheKey cache_key(access_param_->iter_param_.table_id_, rowkey);
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_fuse_row_cache().get_row(
            cache_key, access_ctx_->pkey_.get_partition_id(), handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get row from fuse row cache", K(ret), K(cache_key));
      }
    } else if (access_param_->iter_param_.schema_version_ != handle.value_->get_schema_version() ||
               access_ctx_->store_ctx_->mem_ctx_->get_read_snapshot() < handle.value_->get_snapshot_version()) {
      // TODO(): use fuse row cache here
      handle.reset();
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObFuseRowCacheFetcher::put_fuse_row_cache(
    const ObStoreRowkey& rowkey, const int64_t sstable_end_log_ts, ObStoreRow& row, ObFuseRowValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFuseRowCacheFetcher has not been inited", K(ret));
  } else {
    // try to put row cache
    if (row.snapshot_version_ == INT64_MAX) {
      // uncommited row value, do not put into row cache
    } else {
      bool need_update_row_cache = false;
      const int64_t read_snapshot_version = access_ctx_->store_ctx_->mem_ctx_->get_read_snapshot();
      const bool found_row_cache = nullptr != handle.value_;
      int64_t row_cache_snapshot_version = 0;
      if (found_row_cache) {
        row_cache_snapshot_version = handle.value_->get_snapshot_version();
        if (row.snapshot_version_ == 0) {
          // cache row has not been modified, just update row cache snapshot version
          handle.value_->set_snapshot_version(read_snapshot_version);
          STORAGE_LOG(DEBUG,
              "update row cache snapshot version",
              K(read_snapshot_version),
              K(row.snapshot_version_),
              K(row_cache_snapshot_version));
        } else {
          need_update_row_cache = read_snapshot_version > (row_cache_snapshot_version + FUSE_ROW_CACHE_PUT_INTERVAL);
        }
      } else {
        need_update_row_cache = true;
      }

      if (need_update_row_cache && row.snapshot_version_ > 0) {
        // update row cache
        int tmp_ret = OB_SUCCESS;
        ObFuseRowCacheValue row_cache_value;
        ObFuseRowCacheKey cache_key(access_param_->iter_param_.table_id_, rowkey);
        if (OB_SUCCESS != (tmp_ret = row_cache_value.init(row,
                               access_param_->iter_param_.schema_version_,
                               read_snapshot_version,
                               access_ctx_->pkey_.get_partition_id(),
                               sstable_end_log_ts,
                               row.fq_ctx_))) {
          STORAGE_LOG(WARN, "fail to init row cache value", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = ObStorageCacheSuite::get_instance().get_fuse_row_cache().put_row(
                                      cache_key, row_cache_value))) {
          STORAGE_LOG(WARN, "fail to put row into fuse row cache", K(tmp_ret));
        } else {
          STORAGE_LOG(DEBUG,
              "update row cache",
              K(cache_key),
              K(row_cache_value),
              K(row),
              K(read_snapshot_version),
              K(row_cache_snapshot_version),
              K(sstable_end_log_ts));
        }
      }
    }
  }

  return ret;
}
