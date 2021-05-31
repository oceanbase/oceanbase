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

#define USING_LOG_PREFIX SHARE

#include "ob_sstable_checksum_iterator.h"
#include "observer/ob_server_struct.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace share {

const int64_t ObSSTableDataChecksumIterator::BATCH_CNT;

void ObSSTableDataChecksumInfo::reset()
{
  replicas_.reuse();
}

bool ObSSTableDataChecksumInfo::is_same_table(const ObSSTableDataChecksumItem& item) const
{
  bool bret = true;
  if (replicas_.count() > 0) {
    bret = replicas_.at(0).is_same_table(item);
  }
  return bret;
}

int ObSSTableDataChecksumInfo::compare_sstable(const ObSSTableDataChecksumInfo& other) const
{
  int ret = 0;
  if (replicas_.count() > 0 && other.replicas_.count() > 0) {
    const ObSSTableDataChecksumItem& item = replicas_.at(0);
    const ObSSTableDataChecksumItem& other_item = other.replicas_.at(0);
    if (item.tenant_id_ == other_item.tenant_id_) {
      if (item.data_table_id_ == other_item.data_table_id_) {
        if (item.sstable_id_ == other_item.sstable_id_) {
          if (item.partition_id_ == other_item.partition_id_) {
            ret = 0;
          } else if (item.partition_id_ > other_item.partition_id_) {
            ret = 1;
          } else {
            ret = -1;
          }
        } else if (item.sstable_id_ > other_item.sstable_id_) {
          ret = 1;
        } else {
          ret = -1;
        }
      } else if (item.data_table_id_ > other_item.data_table_id_) {
        ret = 1;
      } else {
        ret = -1;
      }
    } else if (item.tenant_id_ > other_item.tenant_id_) {
      ret = 1;
    } else {
      ret = -1;
    }
  } else if (replicas_.count() > 0) {
    ret = 1;
  } else {
    ret = -1;
  }
  return ret;
}

int ObSSTableDataChecksumInfo::add_item(const ObSSTableDataChecksumItem& item)
{
  int ret = OB_SUCCESS;
  if (!is_same_table(item) || ObITable::MAJOR_SSTABLE != item.sstable_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("this item is not same table", K(ret), K(item));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count(); ++i) {
      const ObSSTableDataChecksumItem& tmp = replicas_.at(i);
      if (tmp.snapshot_version_ == item.snapshot_version_) {
        if (tmp.data_checksum_ != item.data_checksum_ || tmp.row_count_ != item.row_count_) {
          ret = OB_CHECKSUM_ERROR;
          LOG_ERROR("sstable checksum not equal!", K(ret), K(item), K(tmp));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replicas_.push_back(item))) {
        LOG_WARN("failed to push back replicas", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumInfo::add_item_ignore_checksum_error(
    const ObSSTableDataChecksumItem& item, bool& is_checksum_error)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_item(item))) {
    if (OB_CHECKSUM_ERROR == ret) {
      LOG_ERROR("sstable checksum not equal!", K(ret), K(item));
      is_checksum_error = true;
      if (OB_FAIL(replicas_.push_back(item))) {
        LOG_WARN("failed to push sstable data checksum item", K(ret), K(item));
      }
    }
  }
  return ret;
}

void ObSSTableDataChecksumIterator::reset()
{
  sql_proxy_ = nullptr;
  merge_error_cb_ = nullptr;
  cur_idx_ = 0;
  fetched_checksum_items_.reuse();
  while (!filters_.is_empty()) {
    filters_.remove_first();
  }
}

int ObSSTableDataChecksumIterator::init(ObISQLClient* sql_proxy, ObIMergeErrorCb* merge_error_cb)
{
  int ret = OB_SUCCESS;
  // allow reinit
  reset();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    merge_error_cb_ = merge_error_cb;
  }
  return ret;
}

int ObSSTableDataChecksumIterator::next(ObSSTableDataChecksumInfo& checksum_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableDataChecksumIterator is not inited", K(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(-1 == cur_idx_)) {
    ret = OB_ITER_END;
  } else {
    ObSSTableDataChecksumItem item;
    checksum_info.reset();
    while (OB_SUCC(ret)) {
      if (cur_idx_ < fetched_checksum_items_.count()) {
        bool is_filtered = false;
        bool is_checksum_error = false;
        if (OB_FAIL(fetched_checksum_items_.at(cur_idx_, item))) {
          LOG_WARN("failed to get checksum item", K(ret), K_(cur_idx));
        } else if (OB_FAIL(filter_(item, is_filtered))) {
          LOG_WARN("failed to filter checksum item", K(ret), K_(cur_idx));
        } else if (!is_filtered) {
          if (!checksum_info.is_same_table(item)) {
            break;
          } else if (OB_FAIL(checksum_info.add_item_ignore_checksum_error(item, is_checksum_error))) {
            LOG_WARN("failed to add checksum item", K(ret), K(item));
          } else if (is_checksum_error && OB_NOT_NULL(merge_error_cb_)) {
            if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
              LOG_WARN("failed to submit_merge_error_task", K(tmp_ret));
            }
          }
        }
        ++cur_idx_;
      } else if (OB_FAIL(fetch_next_batch_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch next batch", K(ret));
        }
        cur_idx_ = -1;
      } else {
        cur_idx_ = 0;
      }
    }
  }
  return ret;
}

int ObSSTableDataChecksumIterator::fetch_next_batch_()
{
  int ret = OB_SUCCESS;
  uint64_t start_tenant_id = 0;
  uint64_t start_data_table_id = 0;
  uint64_t start_sstable_id = 0;
  int64_t start_partition_id = 0;
  if (0 < fetched_checksum_items_.count()) {
    ObSSTableDataChecksumItem tmp_item;
    if (OB_FAIL(fetched_checksum_items_.at(fetched_checksum_items_.count() - 1, tmp_item))) {
      LOG_WARN("failed to fetch last item", K(ret), K_(fetched_checksum_items));
    } else {
      start_tenant_id = tmp_item.tenant_id_;
      start_data_table_id = tmp_item.data_table_id_;
      start_sstable_id = tmp_item.sstable_id_;
      start_partition_id = tmp_item.partition_id_;
    }
  }
  if (OB_SUCC(ret)) {
    fetched_checksum_items_.reuse();
    if (OB_FAIL(ObSSTableDataChecksumOperator::get_major_checksums(start_tenant_id,
            start_data_table_id,
            start_sstable_id,
            start_partition_id,
            BATCH_CNT,
            fetched_checksum_items_,
            *sql_proxy_))) {
      LOG_WARN("failed to get major sstable checksums",
          K(ret),
          K(start_tenant_id),
          K(start_data_table_id),
          K(start_sstable_id),
          K(start_partition_id));
    } else if (OB_UNLIKELY(0 == fetched_checksum_items_.count())) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObSSTableDataChecksumIterator::filter_(const ObSSTableDataChecksumItem& item, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  DLIST_FOREACH(it, filters_)
  {
    if (OB_FAIL(it->filter(item, is_filtered))) {
      LOG_WARN("failed to filter checksum item", K(ret), K(item));
    } else if (is_filtered) {
      break;
    }
  }
  return ret;
}

int ObSSTableDataChecksumIterator::set_only_user_tenant_filter()
{
  int ret = OB_SUCCESS;
  only_user_tenant_filter_.unlink();
  if (!filters_.add_last(&only_user_tenant_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add filter should not fail", K(ret));
  }
  return ret;
}

int ObSSTableDataChecksumIterator::set_special_table_filter()
{
  int ret = OB_SUCCESS;
  special_table_filter_.unlink();
  if (!filters_.add_last(&special_table_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add filter should not fail", K(ret));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
