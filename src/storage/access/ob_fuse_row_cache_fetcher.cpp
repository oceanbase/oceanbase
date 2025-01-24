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
#include "storage/blocksstable/ob_storage_cache_suite.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

ObFuseRowCacheFetcher::ObFuseRowCacheFetcher()
  : is_inited_(false),
    type_(StorageScanType::NORMAL),
    tablet_id_(),
    read_info_(nullptr),
    tablet_version_(0),
    read_start_version_(0),
    read_snapshot_version_(0)
{
}

int ObFuseRowCacheFetcher::init(const StorageScanType type,
                                const ObTabletID &tablet_id,
                                const ObITableReadInfo *read_info,
                                const int64_t tablet_version,
                                const int64_t read_start_version,
                                const int64_t read_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == read_info || tablet_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), KP(read_info), K(tablet_version));
  } else {
    type_ = type;
    tablet_id_ = tablet_id;
    read_info_ = read_info;
    tablet_version_ = tablet_version;
    read_start_version_ = read_start_version;
    read_snapshot_version_ = read_snapshot_version;
    is_inited_ = true;
  }
  return ret;
}

int ObFuseRowCacheFetcher::get_fuse_row_cache(const ObDatumRowkey &rowkey, ObFuseRowValueHandle &handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFuseRowCacheFetcher has not been inited", K(ret));
  } else if (rowkey.get_datum_cnt() > read_info_->get_datum_utils().get_rowkey_count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid read info", K(ret), K(rowkey), KPC(read_info_));
  } else if (is_mview_table_scan(type_)) {
    ObMultiVersionFuseRowCacheKey cache_key(read_start_version_, read_snapshot_version_, MTL_ID(), tablet_id_, rowkey,
                                            read_info_->get_schema_column_count(), read_info_->get_datum_utils());
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_multi_version_fuse_row_cache().get_row(cache_key, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get row from multi version fuse row cache", K(ret), K(cache_key));
      }
    } else {
      EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_HIT);
      EVENT_INC(ObStatEventIds::MULTI_VERSION_FUSE_ROW_CACHE_HIT);
    }
    STORAGE_LOG(DEBUG, "get from multi version fuse row cache", K(ret), K(cache_key));
  } else {
    ObFuseRowCacheKey cache_key(MTL_ID(), tablet_id_, rowkey, tablet_version_, read_info_->get_schema_column_count(), read_info_->get_datum_utils());
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_fuse_row_cache().get_row(cache_key, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get row from fuse row cache", K(ret), K(cache_key));
      }
    } else {
      EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_HIT);
    }
  }

  return ret;
}

int ObFuseRowCacheFetcher::put_fuse_row_cache(const ObDatumRowkey &rowkey, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFuseRowCacheFetcher has not been inited", K(ret));
  } else if (OB_UNLIKELY(read_snapshot_version_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to put fuse row cache", K(ret), K(read_snapshot_version_));
  } else  if (row.snapshot_version_ == INT64_MAX) {
    // uncommited row value, do not put into row cache
  } else if (is_mview_table_scan(type_)) {
    int tmp_ret = OB_SUCCESS;
    ObMultiVersionFuseRowCacheKey cache_key(read_start_version_, read_snapshot_version_, MTL_ID(), tablet_id_, rowkey,
                                            read_info_->get_schema_column_count(), read_info_->get_datum_utils());
    ObFuseRowCacheValue row_cache_value;
    if (OB_SUCCESS != (tmp_ret = row_cache_value.init(row, read_snapshot_version_))) {
      STORAGE_LOG(WARN, "fail to init row cache value", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = ObStorageCacheSuite::get_instance().get_multi_version_fuse_row_cache().put_row(cache_key, row_cache_value))) {
      STORAGE_LOG(WARN, "fail to put row into multi version fuse row cache", K(tmp_ret));
    } else {
      STORAGE_LOG(DEBUG, "update multi version fuse row cache", K(cache_key), K(row_cache_value), K(row), KPC(read_info_));
    }
  } else {
    // update row cache
    int tmp_ret = OB_SUCCESS;
    ObFuseRowCacheKey cache_key(MTL_ID(), tablet_id_, rowkey, tablet_version_, read_info_->get_schema_column_count(), read_info_->get_datum_utils());
    ObFuseRowCacheValue row_cache_value;
    if (OB_SUCCESS != (tmp_ret = row_cache_value.init(row, read_snapshot_version_))) {
      STORAGE_LOG(WARN, "fail to init row cache value", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = ObStorageCacheSuite::get_instance().get_fuse_row_cache().put_row(cache_key, row_cache_value))) {
      STORAGE_LOG(WARN, "fail to put row into fuse row cache", K(tmp_ret));
    } else {
      STORAGE_LOG(DEBUG, "update fuse row cache", K(cache_key), K(row_cache_value), K(row), KPC(read_info_));
    }
  }

  return ret;
}
