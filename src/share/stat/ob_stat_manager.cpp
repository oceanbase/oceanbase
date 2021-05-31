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

#include "share/stat/ob_stat_manager.h"
#include "common/ob_partition_key.h"
#include "share/stat/ob_user_tab_col_statistics.h"
namespace oceanbase {
using namespace share::schema;
namespace common {

ObStatManager::ObStatManager()
    : column_stat_service_(NULL),
      table_stat_service_(NULL),
      virtual_column_stat_service_(NULL),
      virtual_table_stat_service_(NULL),
      inited_(false)
{}

ObStatManager::~ObStatManager()
{}

int ObStatManager::init(
    ObColumnStatDataService* cs, ObTableStatDataService* ts, ObColumnStatDataService* vcs, ObTableStatDataService* vts)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "stat manager has already been initialized.", K(ret));
  } else if (NULL == cs || NULL == ts) {
    // it is allowed to set vcs and vts with NULL.
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "initialize stat manager with invalid arguments.", KP(cs), KP(ts), KP(vcs), KP(vts), K(ret));
  } else {
    column_stat_service_ = cs;
    table_stat_service_ = ts;
    virtual_column_stat_service_ = vcs;
    virtual_table_stat_service_ = vts;
    inited_ = true;
  }
  return ret;
}

ObStatManager& ObStatManager::get_instance()
{
  static ObStatManager instance_;
  return instance_;
}

const ObTableStat& ObStatManager::get_default_table_stat()
{
  static ObTableStat table_stat(OB_EST_DEFAULT_ROW_COUNT,
      OB_EST_DEFAULT_DATA_SIZE,
      OB_EST_DEFAULT_MACRO_BLOCKS,
      OB_EST_DEFAULT_MICRO_BLOCKS,
      DEFAULT_ROW_SIZE);
  return table_stat;
}

/*
int ObStatManager::update_column_stat(const ObColumnStat &cstat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (!cstat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat value.", K(cstat), K(ret));
  } else if (OB_FAIL(column_stat_service_->update_column_stat(cstat))) {
    COMMON_LOG(WARN, "update_column_stat failed.", K(ret));
  }
  return ret;
}
*/

int ObStatManager::update_column_stats(const ObIArray<ObColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (OB_FAIL(column_stat_service_->update_column_stats(column_stats))) {
    COMMON_LOG(WARN, "updat_column_stats failed.", K(ret));
  }

  return ret;
}

int ObStatManager::get_column_stat(
    const ObColumnStat::Key& key, ObColumnStat& cstat, ObIAllocator& alloc, const bool force_new /*= false*/)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat key.", K(key), K(ret));
  } else if (common::is_virtual_table(key.table_id_) && NULL != virtual_column_stat_service_) {
    if (OB_FAIL(virtual_column_stat_service_->get_column_stat(key, force_new, cstat, alloc))) {
      COMMON_LOG(WARN, "get_column_stat of virtual table failed.", K(ret));
    }
  } else if (OB_FAIL(column_stat_service_->get_column_stat(key, force_new, cstat, alloc))) {
    COMMON_LOG(WARN, "get_column_stat failed.", K(ret));
  }

  return ret;
}

int ObStatManager::get_column_stat(
    const uint64_t table_id, const uint64_t partition_id, const uint64_t column_id, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  ObColumnStat::Key key(table_id, partition_id, column_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat key.", K(key), K(ret));
  } else if (OB_FAIL(get_column_stat(key, handle))) {
    COMMON_LOG(WARN, "get_column_stat failed.", K(ret));
  }
  return ret;
}

int ObStatManager::get_batch_stat(const uint64_t table_id, const common::ObIArray<uint64_t>& partition_ids,
    const common::ObIArray<uint64_t>& column_ids, ObIArray<ObColumnStatValueHandle>& handles)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (common::is_virtual_table(table_id) && NULL != virtual_column_stat_service_) {
    if (OB_FAIL(virtual_column_stat_service_->get_batch_stat(table_id, partition_ids, column_ids, handles))) {
      COMMON_LOG(WARN, "failed to get column stat of virtual table", K(ret), K(table_id), K(partition_ids));
    }
  } else if (OB_FAIL(column_stat_service_->get_batch_stat(table_id, partition_ids, column_ids, handles))) {
    COMMON_LOG(WARN, "failed to get column stat", K(ret), K(table_id), K(partition_ids));
  }
  return ret;
}

int ObStatManager::get_batch_stat(const ObTableSchema& table_schema, const uint64_t partition_id,
    ObIArray<ObColumnStat*>& cstats, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized. ", K(ret));
  } else if (common::is_virtual_table(table_id) && NULL != virtual_column_stat_service_) {
    if (OB_FAIL(virtual_column_stat_service_->get_batch_stat(table_schema, partition_id, cstats, alloc))) {
      COMMON_LOG(WARN, "fail to get_column_stat of virtual table. ", K(ret), K(table_id), K(partition_id));
    }
  } else if (OB_FAIL(column_stat_service_->get_batch_stat(table_schema, partition_id, cstats, alloc))) {
    COMMON_LOG(WARN, "fail to get_column_stat. ", K(ret), K(table_id), K(partition_id));
  }

  return ret;
}

int ObStatManager::get_table_stat(const ObPartitionKey& key, ObTableStat& tstat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat key.", K(key), K(ret));
  } else if (common::is_virtual_table(key.table_id_) && NULL != virtual_table_stat_service_) {
    if (OB_FAIL(virtual_table_stat_service_->get_table_stat(key, tstat))) {
      COMMON_LOG(WARN, "get_table_stat of virtual table failed.", K(ret));
    }
  } else if (OB_FAIL(table_stat_service_->get_table_stat(key, tstat))) {
    COMMON_LOG(WARN, "get_table_stat failed.", K(ret));
  }
  return ret;
}

int ObStatManager::erase_column_stat(const ObPartitionKey& pkey, const int64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || 0 >= column_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(pkey), K(column_id));
  } else if (OB_FAIL(column_stat_service_->erase_column_stat(pkey, column_id))) {
    COMMON_LOG(WARN, "failed to erase column stat", K(ret), K(pkey), K(column_id));
  }
  return ret;
}

int ObStatManager::erase_table_stat(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid pkey", K(ret), K(pkey));
  } else if (OB_FAIL(reinterpret_cast<ObTableStatService*>(table_stat_service_)->erase_table_stat(pkey))) {
    COMMON_LOG(WARN, "failed to erase table stat", K(ret), K(pkey));
  }
  return ret;
}

int ObStatManager::get_column_stat(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat key.", K(key), K(ret));
  } else if (OB_FAIL(column_stat_service_->get_column_stat(key, false, handle))) {
    COMMON_LOG(WARN, "get_column_stat failed.", K(ret));
  }
  return ret;
}

}  // namespace common
}  // end of namespace oceanbase
