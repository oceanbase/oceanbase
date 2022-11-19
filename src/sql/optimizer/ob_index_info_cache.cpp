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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_index_info_cache.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObIndexInfoCache::~ObIndexInfoCache()
{
  for (int i = 0; i < entry_count_; ++i) {
    if (NULL != index_entrys_[i]) {
      index_entrys_[i]->~IndexInfoEntry();
    }
  }
}

int ObIndexInfoCache::get_index_info_entry(const uint64_t table_id,
                                           const uint64_t index_id,
                                           IndexInfoEntry *&entry) const
{
  int ret = OB_SUCCESS;
  entry = NULL;
  if (table_id != table_id_ ||
      OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", K(index_id), K_(table_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < entry_count_; ++i) {
      if (OB_ISNULL(index_entrys_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("entry should not be null", K(ret));
      } else if (index_entrys_[i]->get_index_id() == index_id) {
        entry = index_entrys_[i];
        break;
      }
    }
  }
  return ret;
}

int ObIndexInfoCache::get_query_range(const uint64_t table_id,
                                      const uint64_t index_id,
                                      const QueryRangeInfo *&range_info) const
{
  int ret = OB_SUCCESS;
  range_info = NULL;
  if (table_id != table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", K(table_id), K_(table_id), K(ret));
  } else {
    IndexInfoEntry *entry = NULL;
    if (OB_FAIL(get_index_info_entry(table_id, index_id, entry))) {
      LOG_WARN("failed to get index_info entry", K(index_id), K(ret));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entry should not be null", K(ret));
    } else if (entry->get_range_info().is_valid()){
      range_info = &entry->get_range_info();
    } else {
      LOG_TRACE("entry is invalid", K(table_id), K(index_id));
    }
  }
  return ret;
}

/*
 * 这个接口和ob_join_order.cpp 的 get_access_path_ordering一样，只是从cache里面拿到ordering info
 * */
int ObIndexInfoCache::get_access_path_ordering(const uint64_t table_id,
                                               const uint64_t index_id,
                                               const OrderingInfo *&ordering_info) const
{
  int ret = OB_SUCCESS;
  ordering_info = NULL;
  if (table_id != table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", K(table_id), K_(table_id), K(ret));
  } else {
    IndexInfoEntry *entry = NULL;
    if (OB_FAIL(get_index_info_entry(table_id, index_id, entry))) {
      LOG_WARN("failed to get index_info entry", K(index_id), K(ret));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entry should not be null", K(ret));
    } else {
      ordering_info = &entry->get_ordering_info();
    }
  }
  return ret;
}

int ObIndexInfoCache::add_index_info_entry(IndexInfoEntry *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entry should not be null", K(ret));
  } else if (entry_count_ >= common::OB_MAX_INDEX_PER_TABLE + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry count", K(ret), K_(entry_count),
             K(common::OB_MAX_INDEX_PER_TABLE));
  } else {
    index_entrys_[entry_count_] = entry;
    ++entry_count_;
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase


