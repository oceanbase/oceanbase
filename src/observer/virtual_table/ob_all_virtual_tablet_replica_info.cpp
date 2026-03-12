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

#include "observer/virtual_table/ob_all_virtual_tablet_replica_info.h"

#include <stdint.h>

#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/schema/ob_part_mgr_util.h"
#include "lib/time/ob_time_utility.h"
#include "storage/tx_storage/ob_ls_service.h"


namespace oceanbase
{
using namespace storage;
namespace observer
{

ObTabletReplicaInfoCacheMgr::ObTabletReplicaInfoCacheMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    cache_(),
    lock_(common::ObLatchIds::TABLET_REPLICA_INFO_CACHE_LOCK),
    status_(UNAVAILABLE),
    ref_cnt_(0),
    last_build_time_(0),
    schema_version_(common::OB_INVALID_VERSION)
{}

int ObTabletReplicaInfoCacheMgr::init()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", KR(ret));
  } else {
    is_inited_ = true;
    tenant_id_ = MTL_ID();
  }
  return ret;
}

void ObTabletReplicaInfoCacheMgr::destroy()
{
  common::ObSpinLockGuard guard(lock_);
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    cache_.reset();
    status_ = UNAVAILABLE;
    ref_cnt_ = 0;
    last_build_time_ = 0;
    schema_version_ = common::OB_INVALID_VERSION;
  }
}

int ObTabletReplicaInfoCacheMgr::mtl_init(ObTabletReplicaInfoCacheMgr* &cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to new tablet replica info cache mgr", KR(ret));
  } else if (OB_FAIL(cache_mgr->init())) {
    SERVER_LOG(WARN, "fail to init tablet replica info cache mgr", KR(ret));
  }
  return ret;
}

int ObTabletReplicaInfoCacheMgr::add_cache(
  const ObTabletReplicaInfo &info)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);

  if (OB_UNLIKELY(status_ != BUILDING)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cache is not building", KR(ret), K_(status));
  } else if (OB_FAIL(cache_.push_back(info))) {
    SERVER_LOG(WARN, "fail to push back info", KR(ret), K(info));
  }

  return ret;
}

void ObTabletReplicaInfoCacheMgr::try_invalidate(const int64_t schema_version)
{
  common::ObSpinLockGuard guard(lock_);
  int64_t expire_time = GCONF._tablet_replica_info_cache_expire_time;
  int64_t current_time = ObTimeUtility::current_time();
  if (BUILDING != status_) { // building can not be interrupted
    if (0 == expire_time // cache disabled
        || current_time - last_build_time_ > expire_time // cache is expired
        || schema_version_ != schema_version) { // schema version changed
      status_ = UNAVAILABLE;
      schema_version_ = common::OB_INVALID_VERSION;
      if (0 == ref_cnt_) {
        // no reader is holding the cache snapshot, safe to reset
        cache_.reset();
      }
    }
  }
}

bool ObTabletReplicaInfoCacheMgr::begin_build(const int64_t schema_version)
{
  bool i_am_builder = false;
  common::ObSpinLockGuard guard(lock_);
  if (UNAVAILABLE == status_ && 0 == ref_cnt_) {
    // no reader is holding the cache snapshot,
    // so we can safely reset the cache and start building
    status_ = BUILDING;
    schema_version_ = schema_version;
    i_am_builder = true;
    cache_.reset();
  }
  return i_am_builder;
}

void ObTabletReplicaInfoCacheMgr::finish_build(bool is_build_success)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(BUILDING != status_ || ref_cnt_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cache is not building or ref cnt is not 0, this should not happen", KR(ret), K_(status), K_(ref_cnt));
  } else if (is_build_success) {
    status_ = AVAILABLE;
    last_build_time_ = ObTimeUtility::current_time();
    print_memory_usage();
  } else {
    status_ = UNAVAILABLE;
    cache_.reset();
    schema_version_ = common::OB_INVALID_VERSION;
  }
}

bool ObTabletReplicaInfoCacheMgr::acquire_snapshot(
  const int64_t schema_version,
  const ObTabletReplicaInfoCache *&cache_snapshot)
{
  common::ObSpinLockGuard guard(lock_);
  bool is_available = false;
  cache_snapshot = NULL;
  // if schema version is not matching, cache is expired
  if (AVAILABLE == status_ && schema_version_ == schema_version) {
    is_available = true;
    cache_snapshot = &cache_;
    ref_cnt_++;
  }
  return is_available;
}

void ObTabletReplicaInfoCacheMgr::dec_ref()
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "ref cnt is 0, this should not happen", KR(ret), K_(tenant_id), K_(status));
  } else {
    ref_cnt_--;
    if (UNAVAILABLE == status_ && 0 == ref_cnt_) {
      // cache is not available, and no readers holding the cache snapshot,
      // so we can safely reset the cache
      cache_.reset();
    }
  }
}

// NOTICE: NOT THREAD SAFE, MUST BE CALLED IN LOCK GUARD
void ObTabletReplicaInfoCacheMgr::print_memory_usage()
{
  int64_t tablet_replica_count = cache_.count();
  int64_t memory_usage = cache_.get_data_size();
  SERVER_LOG(INFO, "[ALL_VIRTUAL_TABLET_REPLICA_INFO] tablet replica info cache memory usage", K_(tenant_id), K(tablet_replica_count), K(memory_usage));
}

ObTabletReplicaInfoCacheIterator::ObTabletReplicaInfoCacheIterator()
  : row_idx_(0),
    cache_snapshot_(NULL),
    cache_mgr_(NULL)
{}

ObTabletReplicaInfoCacheIterator::~ObTabletReplicaInfoCacheIterator()
{
  reset();
}

int ObTabletReplicaInfoCacheIterator::init(
  const int64_t schema_version,
  bool &cache_available)
{
  int ret = OB_SUCCESS;
  cache_available = false;
  if (OB_NOT_NULL(cache_mgr_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cache iterator is already inited", KR(ret));
  } else if (OB_ISNULL(cache_mgr_ = MTL(ObTabletReplicaInfoCacheMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet replica info cache mgr is null", KR(ret));
  } else {
    row_idx_ = 0;
    cache_available = cache_mgr_->acquire_snapshot(schema_version, cache_snapshot_);
    if (!cache_available) {
      cache_mgr_ = NULL;
      cache_snapshot_ = NULL;
      row_idx_ = 0;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObTabletReplicaInfoCacheIterator::get_next(ObTabletReplicaInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_snapshot_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "cache iterator is not inited", KR(ret));
  } else if (OB_UNLIKELY(row_idx_ >= cache_snapshot_->count())) {
    ret = OB_ITER_END;
  } else {
    info = cache_snapshot_->at(row_idx_);
    row_idx_++;
  }
  return ret;
}

void ObTabletReplicaInfoCacheIterator::reset()
{
  if (OB_NOT_NULL(cache_mgr_) && OB_NOT_NULL(cache_snapshot_)) {
    cache_mgr_->dec_ref();
    cache_mgr_ = NULL;
    cache_snapshot_ = NULL;
    row_idx_ = 0;
  }
}

ObAllVirtualTabletReplicaInfo::ObAllVirtualTabletReplicaInfo()
  : is_inited_(false),
    addr_(),
    schema_service_(NULL),
    ip_buf_(),
    iter_buf_(NULL),
    need_fetch_table_schema_(false),
    need_fetch_database_schema_(false),
    need_fetch_tablegroup_schema_(false),
    is_curr_tenant_inited_(false),
    is_build_success_(false),
    tablet_to_table_map_(),
    tablet_iter_(NULL),
    read_path_(PATH_NORMAL),
    cache_iter_(),
    cache_mgr_(NULL)
{}

ObAllVirtualTabletReplicaInfo::~ObAllVirtualTabletReplicaInfo()
{
  reset();
}

int ObAllVirtualTabletReplicaInfo::init(
  ObIAllocator *allocator,
  share::schema::ObMultiVersionSchemaService *schema_service,
  common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  MEMSET(ip_buf_, 0, common::OB_IP_STR_BUFF);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "allocator is null", KR(ret));
  } else if (!addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
  } else if (OB_ISNULL(iter_buf_ = allocator->alloc(sizeof(ObTenantTabletPtrWithInMemObjIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", KR(ret));
  } else {
    is_inited_ = true;
    allocator_ = allocator;
    schema_service_ = schema_service;
    addr_ = addr;
  }
  return ret;
}

void ObAllVirtualTabletReplicaInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  is_inited_ = false;
  addr_.reset();
  schema_service_ = NULL;
  MEMSET(ip_buf_, 0, common::OB_IP_STR_BUFF);
  if (OB_NOT_NULL(iter_buf_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = NULL;
  }
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTabletReplicaInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_need_fetch_flags_())) {
    SERVER_LOG(WARN, "fail to init need fetch flags", KR(ret));
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::fill_row_(
  common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTabletReplicaInfo info;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  const ObSimpleDatabaseSchema *database_schema = NULL;
  const ObSimpleTablegroupSchema *tablegroup_schema = NULL;
  bool valid_row_found = true;
  uint64_t object_id = OB_INVALID_ID;
  common::ObString partition_name;
  common::ObString subpartition_name;

  do {
    valid_row_found = true;
    if (OB_FAIL(get_next_tablet_replica_info_(info))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next tablet replica info", KR(ret));
      }
    } else if (OB_FAIL(get_table_related_schemas_(info.table_id, table_schema, database_schema, tablegroup_schema))) {
      SERVER_LOG(WARN, "fail to get table related schemas", KR(ret), K(info.table_id));
    } else {
      const uint64_t tenant_id = MTL_ID();
      const int64_t col_count = output_column_ids_.count();
      for (int64_t i = 0; OB_SUCC(ret) && valid_row_found && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case SERVER_IP:
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case SERVER_PORT:
            cur_row_.cells_[i].set_int(addr_.get_port());
            break;
          case TENANT_ID:
            cur_row_.cells_[i].set_int(tenant_id);
            break;
          case LS_ID:
            cur_row_.cells_[i].set_int(info.ls_id);
            break;
          case TABLET_ID:
            cur_row_.cells_[i].set_int(info.tablet_id);
            break;
          case ROLE: {
            common::ObRole role;
            if (OB_FAIL(get_ls_role_(info.ls_id, role))) {
              if (OB_LS_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                valid_row_found = false;
              } else {
                SERVER_LOG(WARN, "fail to get ls role", KR(ret), K(info.ls_id));
              }
            } else {
              cur_row_.cells_[i].set_int(role);
            }
            break;
          }
          case ZONE:
            cur_row_.cells_[i].set_varchar(GCONF.zone.str());
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case TABLE_ID:
            cur_row_.cells_[i].set_int(info.table_id);
            break;
          case TABLE_NAME: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_varchar(table_schema->get_table_name());
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case DATABASE_ID: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_int(table_schema->get_database_id());
            }
            break;
          }
          case DATABASE_NAME: {
            if (OB_ISNULL(database_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_varchar(database_schema->get_database_name());
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case TABLE_TYPE: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_int(table_schema->get_table_type());
            }
            break;
          }
          case TABLEGROUP_ID: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else if (OB_INVALID_ID == table_schema->get_tablegroup_id()) {
              cur_row_.cells_[i].set_null();
            } else {
              cur_row_.cells_[i].set_int(table_schema->get_tablegroup_id());
            }
            break;
          }
          case TABLEGROUP_NAME: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else if (OB_INVALID_ID == table_schema->get_tablegroup_id()) {
              cur_row_.cells_[i].set_null();
            } else if (OB_ISNULL(tablegroup_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_varchar(tablegroup_schema->get_tablegroup_name());
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case DATA_TABLE_ID: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else {
              cur_row_.cells_[i].set_int(table_schema->get_data_table_id());
            }
            break;
          }
          case OCCUPY_SIZE:
            cur_row_.cells_[i].set_int(info.occupy_size);
            break;
          case REQUIRED_SIZE:
            cur_row_.cells_[i].set_int(info.required_size);
            break;
          case OBJECT_ID: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else if (OB_FAIL(get_object_id_(info,
                                              table_schema,
                                              object_id))) {
              SERVER_LOG(WARN, "fail to get object id", KR(ret), K(info.table_id), K(info.part_idx), K(info.subpart_idx));
            } else if (OB_INVALID_ID == object_id) {
              cur_row_.cells_[i].set_null();
            } else {
              cur_row_.cells_[i].set_int(object_id);
            }
            break;
          }
          case PARTITION_NAME: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else if (OB_FAIL(get_partition_name_(info,
                                                   table_schema,
                                                   partition_name))) {
              SERVER_LOG(WARN, "fail to get partition name", KR(ret), K(info.table_id), K(info.part_idx), K(info.subpart_idx));
            } else if (partition_name.empty()) {
              cur_row_.cells_[i].set_null();
            } else {
              cur_row_.cells_[i].set_varchar(partition_name);
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case SUBPARTITION_NAME: {
            if (OB_ISNULL(table_schema)) {
              valid_row_found = false;
            } else if (OB_FAIL(get_subpartition_name_(info,
                                                      table_schema,
                                                      subpartition_name))) {
              SERVER_LOG(WARN, "fail to get subpartition name", KR(ret), K(info.table_id), K(info.part_idx), K(info.subpart_idx));
            } else if (subpartition_name.empty()) {
              cur_row_.cells_[i].set_null();
            } else {
              cur_row_.cells_[i].set_varchar(subpartition_name);
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
            break;
        }
      }
    }
  } while (OB_SUCC(ret) && !valid_row_found);

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_object_id_(
  const ObTabletReplicaInfo &info,
  const ObSimpleTableSchemaV2 *table_schema,
  uint64_t &object_id)
{
  int ret = OB_SUCCESS;
  object_id = OB_INVALID_ID;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table schema is null", KR(ret));
  } else {
    const share::schema::ObPartitionLevel part_level = table_schema->get_part_level();
    if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
      object_id = info.table_id;
    } else {
      const int64_t part_idx = info.part_idx;
      const int64_t subpart_idx = info.subpart_idx;
      share::schema::ObBasePartition *base_part = NULL;
      if (OB_FAIL(table_schema->get_part_by_idx(part_idx, subpart_idx, base_part))) {
        SERVER_LOG(WARN, "fail to get subpart by idx", KR(ret), K(part_idx), K(subpart_idx), K(info.table_id));
      } else if (OB_ISNULL(base_part)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "subpart is null", KR(ret), K(part_idx), K(subpart_idx), K(info.table_id));
      } else {
        object_id = base_part->get_object_id();
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_partition_name_(
  const ObTabletReplicaInfo &info,
  const ObSimpleTableSchemaV2 *table_schema,
  common::ObString &partition_name)
{
  int ret = OB_SUCCESS;
  partition_name.reset();
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table schema is null", KR(ret));
  } else {
    const share::schema::ObPartitionLevel part_level = table_schema->get_part_level();
    if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
      // keep empty
    } else {
      const int64_t part_idx = info.part_idx;
      const int64_t part_num = table_schema->get_partition_num();
      share::schema::ObPartition **part_array = table_schema->get_part_array();
      if (OB_ISNULL(part_array) || part_idx < 0 || part_idx >= part_num || OB_ISNULL(part_array[part_idx])) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid part array", KR(ret), K(part_idx), K(part_num), K(info.table_id));
      } else {
        partition_name = part_array[part_idx]->get_part_name();
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_subpartition_name_(
  const ObTabletReplicaInfo &info,
  const ObSimpleTableSchemaV2 *table_schema,
  common::ObString &subpartition_name)
{
  int ret = OB_SUCCESS;
  subpartition_name.reset();
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table schema is null", KR(ret));
  } else {
    const share::schema::ObPartitionLevel part_level = table_schema->get_part_level();
    if (share::schema::PARTITION_LEVEL_TWO != part_level) {
      // keep empty
    } else {
      const int64_t part_idx = info.part_idx;
      const int64_t subpart_idx = info.subpart_idx;
      share::schema::ObBasePartition *base_part = NULL;
      if (OB_FAIL(table_schema->get_part_by_idx(part_idx, subpart_idx, base_part))) {
        SERVER_LOG(WARN, "fail to get subpart by idx", KR(ret), K(part_idx), K(subpart_idx), K(info.table_id));
      } else if (OB_ISNULL(base_part)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "subpart is null", KR(ret), K(part_idx), K(subpart_idx), K(info.table_id));
      } else {
        subpartition_name = base_part->get_part_name();
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(execute(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to execute", KR(ret));
    }
  }

  return ret;
}

int ObAllVirtualTabletReplicaInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_curr_tenant_inited_ && OB_FAIL(init_curr_tenant_())) {
    SERVER_LOG(WARN, "fail to init curr tenant", KR(ret));
  } else if (OB_FAIL(fill_row_(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to fill row", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::decide_read_path_(
  const uint64_t tenant_id,
  const int64_t tenant_schema_version)
{
  int ret = OB_SUCCESS;
  bool cache_available = false;
  bool cache_enabled = 0 != GCONF._tablet_replica_info_cache_expire_time;
  cache_mgr_->try_invalidate(tenant_schema_version);
  if (!cache_enabled) {
    // cache is disabled, use normal path
    read_path_ = PATH_NORMAL;
  } else {
    if (OB_FAIL(cache_iter_.init(tenant_schema_version, cache_available))) {
      SERVER_LOG(WARN, "fail to init cache iterator", KR(ret), K(tenant_id));
    } else if (cache_available) {
      // cache is available, use cache path
      read_path_ = PATH_CACHE;
    } else if (cache_mgr_->begin_build(tenant_schema_version)) {
      // I am the builder, use build path
      read_path_ = PATH_BUILD;
    } else {
      // someone else is building, use normal path
      read_path_ = PATH_NORMAL;
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::init_curr_tenant_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t tenant_schema_version = common::OB_INVALID_VERSION;
  static const int64_t BUCKET_NUM = 64;
  if (OB_FAIL(ls_to_role_map_.create(BUCKET_NUM, "LsToRoleMap", "LsToRoleMap", tenant_id))) {
    SERVER_LOG(WARN, "fail to create ls to role map", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_mgr_ = MTL(ObTabletReplicaInfoCacheMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get tablet replica info cache mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!schema_service_->is_tenant_refreshed(tenant_id))) {
    ret = OB_ITER_END;
    SERVER_LOG(INFO, "tenant schema is not refreshed, skip this tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    SERVER_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_schema_version(tenant_id, tenant_schema_version))) {
    SERVER_LOG(WARN, "fail to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(decide_read_path_(tenant_id, tenant_schema_version))) {
    SERVER_LOG(WARN, "fail to decide read path", KR(ret), K(tenant_id));
  } else if (PATH_CACHE != read_path_) {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    if (OB_FAIL(prepare_tablet_to_table_map_())) {
      SERVER_LOG(WARN, "fail to prepare tablet map", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(t3m)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get t3m", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tablet_iter_ = new (iter_buf_) ObTenantTabletPtrWithInMemObjIterator(*t3m))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to new tablet_iter_", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    is_curr_tenant_inited_ = true;
  }

  return ret;
}

void ObAllVirtualTabletReplicaInfo::release_last_tenant()
{
  is_curr_tenant_inited_ = false;
  tablet_to_table_map_.destroy();
  ls_to_role_map_.destroy();
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletPtrWithInMemObjIterator();
    tablet_iter_ = NULL;
  }
  if (OB_NOT_NULL(cache_mgr_) && PATH_BUILD == read_path_) {
    cache_mgr_->finish_build(is_build_success_);
  }
  is_build_success_ = false;
  read_path_ = PATH_NORMAL;
  cache_iter_.reset();
  cache_mgr_ = NULL;
  schema_guard_.reset();
}

int ObAllVirtualTabletReplicaInfo::prepare_tablet_to_table_map_()
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  const uint64_t tenant_id = MTL_ID();
  const int64_t MIN_BUCKET_NUM = 1000;
  if (OB_FAIL(schema_guard_.get_table_schemas_in_tenant(tenant_id, tables))) {
    SERVER_LOG(WARN, "fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    int64_t bucket_num = 0;
    int64_t tablet_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      const ObSimpleTableSchemaV2 *table = tables.at(i);
      if (OB_ISNULL(table)) {
        // skip
      } else if (table->has_tablet()) {
        tablet_num += table->get_all_part_num();
      }
    }
    if (OB_SUCC(ret)) {
      bucket_num = MAX(tablet_num * 2, MIN_BUCKET_NUM);
      if (OB_FAIL(tablet_to_table_map_.create(common::hash::cal_next_prime(bucket_num), "TmpTabletMap", "TmpTabletMap", tenant_id))) {
        SERVER_LOG(WARN, "create tablet-table map failed", KR(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
          const ObSimpleTableSchemaV2 *table = tables.at(i);
          if (OB_ISNULL(table)) {
            // skip
          } else if (table->has_tablet()) {
            ObPartitionSchemaIter::Info part_info;
            ObPartitionSchemaIter iter(*table, ObCheckPartitionMode::CHECK_PARTITION_MODE_NORMAL);
            while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_info(part_info))) {
              if (OB_UNLIKELY(0 == part_info.tablet_id_.id())) {
                // skip, oracle global tmp table may have tablet id 0
              } else {
                ObPartInfo map_value;
                map_value.table_id = table->get_table_id();
                map_value.part_idx = part_info.part_idx_;
                map_value.subpart_idx = part_info.subpart_idx_;
                if (OB_FAIL(tablet_to_table_map_.set_refactored(part_info.tablet_id_, map_value))) {
                  SERVER_LOG(WARN, "fail to set tablet-table map", KR(ret), K(tenant_id), K(part_info), KPC(table));
                }
              }
            } // end while
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              SERVER_LOG(WARN, "iter tablet failed", KR(ret), K(tenant_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_next_tablet_replica_info_(ObTabletReplicaInfo &tablet_replica_info)
{
  int ret = OB_SUCCESS;
  switch (read_path_) {
    case PATH_NORMAL: {
      if (OB_FAIL(get_next_tablet_replica_info_from_iter_(tablet_replica_info))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tablet replica info from iter", KR(ret));
        }
      }
      break;
    }
    case PATH_CACHE: {
      if (OB_FAIL(get_next_tablet_replica_info_from_cache_(tablet_replica_info))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tablet replica info from cache", KR(ret));
        }
      }
      break;
    }
    case PATH_BUILD: {
      if (OB_ISNULL(cache_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cache mgr is null", KR(ret));
      } else if (OB_FAIL(get_next_tablet_replica_info_from_iter_(tablet_replica_info))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tablet replica info from iter", KR(ret));
        } else {
          is_build_success_ = true;
        }
      } else if (OB_FAIL(cache_mgr_->add_cache(tablet_replica_info))) {
        SERVER_LOG(WARN, "fail to add cache", KR(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected read path", KR(ret), K(read_path_));
      break;
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_next_tablet_replica_info_from_cache_(ObTabletReplicaInfo &tablet_replica_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_iter_.get_next(tablet_replica_info))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next tablet replica info from cache", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_next_tablet_replica_info_from_iter_(ObTabletReplicaInfo &tablet_replica_info)
{
  int ret = OB_SUCCESS;

  ObTabletMapKey key;
  ObTabletPointerHandle ptr_hdl;
  ObTabletHandle tablet_hdl;
  ObTabletResidentInfo tablet_resident_info;
  ObPartInfo part_info;
  ObTabletPointer *tablet_pointer = NULL;

  if (OB_ISNULL(tablet_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet iter is null", KR(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter_->get_next_tablet_pointer(key, ptr_hdl, tablet_hdl))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        SERVER_LOG(WARN, "fail to get tablet iter", KR(ret));
      }
    } else if (OB_UNLIKELY(!ptr_hdl.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected invalid tablet", KR(ret), K(ptr_hdl));
    } else if (OB_ISNULL(tablet_pointer = ptr_hdl.get_tablet_pointer())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get tablet pointer", KR(ret), K(ptr_hdl));
    } else {
      if (OB_FAIL(tablet_to_table_map_.get_refactored(key.tablet_id_, part_info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "fail to get tablet_map", KR(ret), K(key));
        } else {
          // skip this tablet
          ret = OB_SUCCESS;
        }
      } else {
        tablet_replica_info.tablet_id = key.tablet_id_.id();
        tablet_replica_info.table_id = part_info.table_id;
        tablet_replica_info.part_idx = part_info.part_idx;
        tablet_replica_info.subpart_idx = part_info.subpart_idx;
        tablet_replica_info.ls_id = key.ls_id_.id();
        tablet_resident_info = tablet_pointer->get_tablet_resident_info(key);
        tablet_replica_info.occupy_size = tablet_resident_info.get_occupy_size();
        tablet_replica_info.required_size = tablet_resident_info.get_required_size();
        break;
      }
    }
  }

  return ret;
}

int ObAllVirtualTabletReplicaInfo::init_need_fetch_flags_()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TABLE_NAME:
      case DATABASE_ID:
      case TABLE_TYPE:
      case TABLEGROUP_ID:
      case DATA_TABLE_ID:
      case OBJECT_ID:
      case PARTITION_NAME:
      case SUBPARTITION_NAME:
        need_fetch_table_schema_ = true;
        break;
      case DATABASE_NAME:
        need_fetch_table_schema_ = true;
        need_fetch_database_schema_ = true;
        break;
      case TABLEGROUP_NAME:
        need_fetch_table_schema_ = true;
        need_fetch_tablegroup_schema_ = true;
        break;
      default:
        break;
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_table_related_schemas_(
  const uint64_t table_id,
  const ObSimpleTableSchemaV2 *&table_schema,
  const ObSimpleDatabaseSchema *&database_schema,
  const ObSimpleTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  table_schema = NULL;
  database_schema = NULL;
  tablegroup_schema = NULL;

  if (OB_SUCC(ret) && need_fetch_table_schema_) {
    if (OB_FAIL(schema_guard_.get_simple_table_schema(tenant_id, table_id, table_schema))) {
      SERVER_LOG(WARN, "fail to get table schema", KR(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(table_schema) && need_fetch_database_schema_) {
    if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
      SERVER_LOG(WARN, "fail to get database schema", KR(ret), K(table_schema->get_database_id()));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(table_schema) && need_fetch_tablegroup_schema_ && OB_INVALID_ID != table_schema->get_tablegroup_id()) {
    if (OB_FAIL(schema_guard_.get_tablegroup_schema(tenant_id, table_schema->get_tablegroup_id(), tablegroup_schema))) {
      SERVER_LOG(WARN, "fail to get tablegroup schema", KR(ret), K(table_schema->get_tablegroup_id()));
    }
  }
  return ret;
}

int ObAllVirtualTabletReplicaInfo::get_ls_role_(const int64_t ls_id, common::ObRole &role)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_to_role_map_.get_refactored(ls_id, role))) {
    if (OB_HASH_NOT_EXIST != ret) {
      SERVER_LOG(WARN, "fail to get ls role", KR(ret), K(ls_id));
    } else {
      ObLS *ls = NULL;
      ObLSHandle ls_handle;
      ObLSService *ls_service = MTL(ObLSService*);
      if (OB_ISNULL(ls_service)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to get ls service", KR(ret));
      } else if (OB_FAIL(ls_service->get_ls(ObLSID(ls_id), ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        SERVER_LOG(WARN, "fail to get ls", KR(ret), K(ls_id));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to get ls", KR(ret), K(ls_id));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        SERVER_LOG(WARN, "fail to get ls role", KR(ret), K(ls_id));
      } else if (OB_FAIL(ls_to_role_map_.set_refactored(ls_id, role))) {
        SERVER_LOG(WARN, "fail to set ls role", KR(ret), K(ls_id), K(role));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
