/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_meta_manager.h"

#include <LogMsgFactory.h>                    // LogMsgFactory

#include "lib/atomic/ob_atomic.h"                 // ATOMIC_*
#include "observer/mysql/obsm_utils.h"            // ObSMUtils
#include "rpc/obmysql/ob_mysql_global.h"          // obmysql
#include "share/schema/ob_table_schema.h"         // ObTableSchema, ObSimpleTableSchemaV2
#include "share/schema/ob_column_schema.h"        // ObColumnSchemaV2

#include "ob_log_schema_getter.h"                 // ObLogSchemaGuard, DBSchemaInfo, TenantSchemaInfo
#include "ob_log_utils.h"                         // print_mysql_type, ob_log_malloc
#include "ob_obj2str_helper.h"                    // ObObj2strHelper
#include "ob_log_adapt_string.h"                  // ObLogAdaptString
#include "ob_log_config.h"                        // TCONF

#define META_STAT_INFO(fmt, args...) LOG_INFO("[META_STAT] " fmt, args)
#define META_STAT_DEBUG(fmt, args...) LOG_DEBUG("[META_STAT] " fmt, args)

#define SET_ENCODING(meta, charset) \
    do {\
      meta->setEncoding(ObCharset::charset_name(charset)); \
    } while (0)

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace liboblog
{
ObLogMetaManager::ObLogMetaManager() : inited_(false),
                                       enable_output_hidden_primary_key_(false),
                                       obj2str_helper_(NULL),
                                       ddl_table_meta_(NULL),
                                       db_meta_map_(),
                                       tb_meta_map_(),
                                       tb_schema_info_map_(),
                                       allocator_()
{
}

ObLogMetaManager::~ObLogMetaManager()
{
  destroy();
}

int ObLogMetaManager::init(ObObj2strHelper *obj2str_helper,
    const bool enable_output_hidden_primary_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("meta manager has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(obj2str_helper)) {
    LOG_ERROR("invalid argument", K(obj2str_helper));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(db_meta_map_.init(ObModIds::OB_LOG_DATABASE_META_MAP))) {
    LOG_ERROR("init db_meta_map fail", KR(ret));
  } else if (OB_FAIL(tb_meta_map_.init(ObModIds::OB_LOG_TABLE_META_MAP))) {
    LOG_ERROR("init tb_meta_map fail", KR(ret));
  } else if (OB_FAIL(tb_schema_info_map_.init(ObModIds::OB_LOG_TABLE_SCHEMA_META_MAP))) {
    LOG_ERROR("init tb_schema_info_map_ fail", KR(ret));
  } else if (OB_FAIL(allocator_.init(ALLOCATOR_TOTAL_LIMIT,
      ALLOCATOR_HOLD_LIMIT,
      ALLOCATOR_PAGE_SIZE))) {
    LOG_ERROR("init allocator fail", KR(ret));
  } else if (OB_FAIL(build_ddl_meta_())) {
    LOG_ERROR("build ddl meta fail", KR(ret));
  } else {
    enable_output_hidden_primary_key_ = enable_output_hidden_primary_key;
    obj2str_helper_ = obj2str_helper;
    inited_ = true;
  }

  return ret;
}

void ObLogMetaManager::destroy()
{
  destroy_ddl_meta_();

  inited_ = false;
  enable_output_hidden_primary_key_ = false;
  obj2str_helper_ = NULL;

  // note: destroy tb_schema_info_map first, then destroy allocator
  tb_schema_info_map_.destroy();
  allocator_.destroy();
  db_meta_map_.destroy();
  tb_meta_map_.destroy();

  ddl_table_meta_ = NULL;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::get_table_meta(
    const int64_t global_schema_version,
    const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
    IObLogSchemaGetter &schema_getter,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  TableMetaInfo *meta_info = NULL;

  if (OB_ISNULL(simple_table_schema)) {
    LOG_ERROR("invalid argument", K(simple_table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MetaKey key;
    key.id_ = simple_table_schema->get_table_id();

    if (OB_FAIL((get_meta_info_<TableMetaMap, TableMetaInfo>(tb_meta_map_, key, meta_info)))) {
      LOG_ERROR("get table meta info fail", KR(ret), K(key));
    } else {
      int64_t version = simple_table_schema->get_schema_version();
      ret = get_meta_from_meta_info_<TableMetaInfo, ITableMeta>(meta_info, version, table_meta);

      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get_meta_from_meta_info_ fail", KR(ret), K(version));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // refresh ObTableSchema when build meta for the first time
        const int64_t table_id = simple_table_schema->get_table_id();
        const share::schema::ObTableSchema *table_schema = NULL;
        ObLogSchemaGuard schema_mgr;

        RETRY_FUNC(stop_flag, schema_getter, get_schema_guard_and_full_table_schema, table_id, global_schema_version, GET_SCHEMA_TIMEOUT,
                schema_mgr, table_schema);

        if (OB_FAIL(ret)) {
          // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("get_schema_guard_and_full_table_schema fail", KR(ret),
                "schema_version", simple_table_schema->get_schema_version(),
                K(global_schema_version),
                "table_id", simple_table_schema->get_table_id(),
                "table_name", simple_table_schema->get_table_name(), KPC(table_schema));
          }
        } else if (OB_ISNULL(table_schema)) {
          // tenant has been dropped
          LOG_WARN("table_schema is null, tenant may be dropped", K(table_schema),
              "schema_version", simple_table_schema->get_schema_version(),
              K(global_schema_version),
              "tenant_id", simple_table_schema->get_tenant_id(),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(), KPC(simple_table_schema));
          ret = OB_TENANT_HAS_BEEN_DROPPED;
        } else if (OB_FAIL(add_and_get_table_meta_(meta_info, table_schema, schema_mgr, table_meta,
            stop_flag))) {
          // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("add_and_get_table_meta_ fail", KR(ret),
                "table_name", table_schema->get_table_name(),
                "table_id", table_schema->get_table_id());
          }
        } else {
          // succ
        }
      } else { /* OB_SUCCESS == ret*/ }
    }
  }

  return ret;
}

void ObLogMetaManager::revert_table_meta(ITableMeta *table_meta)
{
  int ret = OB_SUCCESS;

  if (NULL != table_meta && ddl_table_meta_ != table_meta) {
    int64_t ref_cnt = 0;

    if (OB_FAIL(dec_meta_ref_<ITableMeta>(table_meta, ref_cnt))) {
      LOG_ERROR("dec_meta_ref_ fail", KR(ret), K(table_meta));
    } else if (0 == ref_cnt) {
      // destroy all colMeta by default
      LogMsgFactory::destroy(table_meta);
      table_meta = NULL;
    }
  }
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::get_db_meta(
    const DBSchemaInfo &db_schema_info,
    ObLogSchemaGuard &schema_mgr,
    IDBMeta *&db_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  DBMetaInfo *meta_info = NULL;
  const int64_t db_schema_version = db_schema_info.version_;
  uint64_t db_id = db_schema_info.db_id_;

  if (OB_UNLIKELY(! db_schema_info.is_valid())) {
    LOG_ERROR("invalid argument", K(db_schema_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MetaKey key;
    key.id_ = db_id;

    if (OB_FAIL((get_meta_info_<DBMetaMap, DBMetaInfo>(db_meta_map_, key, meta_info)))) {
      LOG_ERROR("get database meta info fail", KR(ret), K(key));
    } else {
      ret = get_meta_from_meta_info_<DBMetaInfo, IDBMeta>(meta_info, db_schema_version, db_meta);

      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get_meta_from_meta_info_ fail", KR(ret), K(db_schema_version));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;

        // get db name and tenant name when first build db meta
        uint64_t tenant_id = extract_tenant_id(db_id);
        TenantSchemaInfo tenant_schema_info;

        // get tenant name
        RETRY_FUNC(stop_flag, schema_mgr, get_tenant_schema_info, tenant_id, tenant_schema_info,
            GET_SCHEMA_TIMEOUT);

        if (OB_FAIL(ret)) {
          // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id), K(db_id));
          }
        } else if (OB_FAIL(add_and_get_db_meta_(meta_info, db_schema_info, tenant_schema_info, db_meta))) {
          LOG_ERROR("add_and_get_db_meta_ fail", KR(ret), KP(meta_info), K(db_schema_info),
              K(tenant_schema_info));
        }
      } else { /* OB_SUCCESS == ret*/ }
    }
  }

  return ret;
}

void ObLogMetaManager::revert_db_meta(IDBMeta *db_meta)
{
  int ret = OB_SUCCESS;

  if (NULL != db_meta) {
    int64_t ref_cnt = 0;

    if (OB_FAIL(dec_meta_ref_<IDBMeta>(db_meta, ref_cnt))) {
      LOG_ERROR("dec_meta_ref_ fail", KR(ret), K(db_meta));
    } else if (0 == ref_cnt) {
      LogMsgFactory::destroy(db_meta);
      db_meta = NULL;
    }
  }
}

int ObLogMetaManager::drop_table(const int64_t table_id)
{
  UNUSED(table_id);
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int ObLogMetaManager::drop_database(const int64_t database_id)
{
  UNUSED(database_id);
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

template <class MetaMapType, class MetaInfoType>
int ObLogMetaManager::get_meta_info_(MetaMapType &meta_map,
    const MetaKey &key,
    MetaInfoType *&meta_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! key.is_valid())) {
    LOG_ERROR("invalid argument", K(key));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = meta_map.get(key, meta_info);

    if (OB_SUCCESS == ret && OB_ISNULL(meta_info)) {
      LOG_ERROR("get meta info from meta_map fail", KR(ret), K(meta_info));
      ret = OB_ERR_UNEXPECTED;
    }

    // insert new MetaInfo
    if (OB_ENTRY_NOT_EXIST == ret) {
      MetaInfoType *tmp_meta_info =
          static_cast<MetaInfoType *>(allocator_.alloc(sizeof(MetaInfoType)));

      if (OB_ISNULL(tmp_meta_info)) {
        LOG_ERROR("allocate memory for MetaInfo fail", "size", sizeof(MetaInfoType), K(key));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        new (tmp_meta_info) MetaInfoType();

        ret = meta_map.insert(key, tmp_meta_info);

        if (OB_SUCC(ret)) {
          meta_info = tmp_meta_info;
        } else {
          tmp_meta_info->~MetaInfoType();
          allocator_.free(static_cast<void*>(tmp_meta_info));
          tmp_meta_info = NULL;

          if (OB_ENTRY_EXIST == ret) {
            if (OB_FAIL(meta_map.get(key, meta_info))) {
              LOG_ERROR("get meta info from map fail", KR(ret), K(key));
            } else if (OB_ISNULL(meta_info)) {
              LOG_ERROR("get meta info from meta_map fail", KR(ret), K(meta_info));
              ret = OB_ERR_UNEXPECTED;
            }
          } else {
            LOG_ERROR("insert meta info into map fail", KR(ret), K(key));
          }
        }
      }
    } else if (OB_FAIL(ret)) {
      LOG_ERROR("get meta info from map fail", KR(ret), K(key));
    } else {
      // OB_SUCCESS == ret
    }
  }

  return ret;
}

template <class MetaInfoType, class MetaType>
int ObLogMetaManager::get_meta_from_meta_info_(MetaInfoType *meta_info,
    const int64_t version,
    MetaType *&meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta_info)) {
    LOG_ERROR("invalid argument", K(meta_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    meta = NULL;

    // add read lock
    RLockGuard guard(meta_info->lock_);

    ret = meta_info->get(version, meta);

    if (OB_SUCC(ret)) {
      // increase ref count of meta
      if (OB_FAIL(inc_meta_ref_<MetaType>(meta))) {
        LOG_ERROR("inc_meta_ref_ fail", KR(ret), K(meta));
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::add_and_get_table_meta_(TableMetaInfo *meta_info,
    const share::schema::ObTableSchema *table_schema,
    ObLogSchemaGuard &schema_mgr,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta_info) || OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(meta_info), K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t version = table_schema->get_schema_version();

    table_meta = NULL;

    // add write lock
    WLockGuard guard(meta_info->lock_);

    // First check if there is already a corresponding version of Meta, and if not, create a new one directly
    ret = meta_info->get(version, table_meta);

    if (OB_ENTRY_NOT_EXIST == ret) { // not exist
      ret = OB_SUCCESS;

      // Create a new Table Meta and insert the Meta into the Meta Info chain(linked list)
      if (OB_FAIL(build_table_meta_(table_schema, schema_mgr, table_meta, stop_flag))) {
        // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("build_table_meta_ fail", KR(ret), KP(table_schema));
        }
      } else if (OB_FAIL(meta_info->set(version, table_meta))) {
        LOG_ERROR("set meta info meta info fail", KR(ret), K(version), KP(table_meta));
      }
    } else if (OB_FAIL(ret)) {
      LOG_ERROR("get meta from meta info fail", KR(ret), K(version));
    } else {
      // succ
    }

    // increase ref count after get meta
    if (OB_SUCC(ret)) {
      if (OB_FAIL(inc_meta_ref_<ITableMeta>(table_meta))) {
        LOG_ERROR("inc_meta_ref_ fail", KR(ret), K(table_meta));
      }
    }
  }

  return ret;
}

int ObLogMetaManager::add_and_get_db_meta_(DBMetaInfo *meta_info,
    const DBSchemaInfo &db_schema_info,
    const TenantSchemaInfo &tenant_schema_info,
    IDBMeta *&db_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta_info)) {
    LOG_ERROR("invalid argument", K(meta_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    db_meta = NULL;
    int64_t db_schema_version = db_schema_info.version_;

    // add write lock
    WLockGuard guard(meta_info->lock_);

    // First check if there is already a corresponding version of Meta, and if not, create a new one directly
    ret = meta_info->get(db_schema_version, db_meta);

    if (OB_ENTRY_NOT_EXIST == ret) { // not exist
      ret = OB_SUCCESS;

      // Create a new DB Meta and insert the Meta into the Meta Info chain
      if (OB_FAIL(build_db_meta_(db_schema_info, tenant_schema_info, db_meta))) {
        LOG_ERROR("build_db_meta_ fail", KR(ret), K(db_schema_info), K(tenant_schema_info));
      } else if (OB_FAIL(meta_info->set(db_schema_version, db_meta))) {
        LOG_ERROR("set meta info meta info fail", KR(ret), K(db_schema_version), KP(db_meta));
      }
    } else if (OB_FAIL(ret)) {
      LOG_ERROR("get meta from meta info fail", KR(ret), K(db_schema_version));
    } else {
      // succ
    }

    // increase ref count after get Meta
    if (OB_SUCC(ret)) {
      if (OB_FAIL(inc_meta_ref_<IDBMeta>(db_meta))) {
        LOG_ERROR("inc_meta_ref_ fail", KR(ret), K(db_meta));
      }
    }
  }

  return ret;
}

template <class MetaType>
int ObLogMetaManager::inc_meta_ref_(MetaType *meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta)) {
    LOG_ERROR("invalid argument", K(meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    (void)ATOMIC_AAF(reinterpret_cast<int64_t *>(meta->getUserDataPtr()), 1);
  }

  return ret;
}

template <class MetaType>
int ObLogMetaManager::dec_meta_ref_(MetaType *meta, int64_t &ref_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta)) {
    LOG_ERROR("invalid argument", K(meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ref_cnt = ATOMIC_AAF(reinterpret_cast<int64_t *>(meta->getUserDataPtr()), -1);
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::build_table_meta_(const share::schema::ObTableSchema *table_schema,
    ObLogSchemaGuard &schema_mgr,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ITableMeta *tmp_table_meta = LogMsgFactory::createTableMeta();
    TableSchemaInfo *tb_schema_info = NULL;

    if (OB_FAIL(alloc_table_schema_info_(tb_schema_info))) {
      LOG_ERROR("alloc_table_schema_info_ fail", KR(ret), KPC(tb_schema_info));
    } else if (OB_ISNULL(tb_schema_info)) {
      LOG_ERROR("tb_schema_info is null");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tb_schema_info->init(table_schema))) {
      LOG_ERROR("tb_schema_info init fail", KR(ret), K(table_schema),
          "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name());
    } else if (OB_ISNULL(tmp_table_meta)) {
      LOG_ERROR("createTableMeta fail, return NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(build_column_metas_(tmp_table_meta, table_schema, *tb_schema_info,
          schema_mgr, stop_flag))) {
      // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
      if (OB_IN_FATAL_STATE != ret && OB_IN_STOP_STATE != ret) {
        LOG_ERROR("build_column_metas_ fail", KR(ret), KP(tmp_table_meta), KPC(tb_schema_info));
      }
    } else {
      tmp_table_meta->setName(table_schema->get_table_name());
      tmp_table_meta->setDBMeta(NULL);  // NOTE: default to NULL
      // The encoding of DB is set to empty, because there is currently an ambiguity, it can be either database or tenant
      // to avoid ambiguity, it is set to empty here
      SET_ENCODING(tmp_table_meta, table_schema->get_charset_type());
      tmp_table_meta->setUserData(reinterpret_cast<void*>(1));
    }

    if (OB_SUCC(ret)) {
      table_meta = tmp_table_meta;
    } else {
      int tmp_ret = OB_SUCCESS;

      if (NULL != tmp_table_meta) {
        LogMsgFactory::destroy(tmp_table_meta);
      }

      if (OB_NOT_NULL(tb_schema_info)) {
        if (OB_SUCCESS != (tmp_ret = try_erase_table_schema_(
            table_schema->get_table_id(),
            table_schema->get_schema_version()))) {
          LOG_ERROR("try_erase_table_schema_ failed", KR(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = free_table_schema_info_(tb_schema_info))) {
          LOG_ERROR("free_table_schema_info_ fail", KR(tmp_ret), K(tb_schema_info));
        }
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::build_column_metas_(ITableMeta *table_meta,
    const share::schema::ObTableSchema *table_schema,
    TableSchemaInfo &tb_schema_info,
    ObLogSchemaGuard &schema_mgr,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_meta) || OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_meta), K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t version = table_schema->get_schema_version();
    uint64_t table_id = table_schema->get_table_id();
    const bool is_hidden_pk_table = table_schema->is_no_pk_table();
    // index of column, numbering staarts from 0
    // note: hidden column won't task into numbering
    int64_t column_index = 0;
    ObColumnIterByPrevNextID pre_next_id_iter(*table_schema);

    // build Meata for each column
    // ignore hidden column
    while (OB_SUCCESS == ret && ! stop_flag) {
      const share::schema::ObColumnSchemaV2 *column_table_schema = NULL;
      IColMeta *col_meta = NULL;
      int append_ret = 2;
      bool is_column_filter = false;
      bool is_hidden_pk_table_pk_increment_column = false;

      if (OB_FAIL(pre_next_id_iter.next(column_table_schema))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("pre_next_id_iter next fail", KR(ret), KPC(column_table_schema));
        }
      } else if (OB_ISNULL(column_table_schema)) {
        LOG_ERROR("column_table_schema is null", KPC(column_table_schema));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(filter_column_(*table_schema, is_hidden_pk_table, *column_table_schema, is_column_filter,
              is_hidden_pk_table_pk_increment_column))) {
        LOG_ERROR("filter_column_ fail", KR(ret), K(is_column_filter),
            K(is_hidden_pk_table_pk_increment_column),
            "table_name", table_schema->get_table_name(),
            "table_id", table_schema->get_table_id(),
            "column", column_table_schema->get_column_name(),
            "column_id", column_table_schema->get_column_id());
      } else if (is_column_filter) {
        // do nothing
      } else if (NULL != (col_meta = table_meta->getCol(column_table_schema->get_column_name()))) {
        // LOG WARN and won't treate it as ERROR
        LOG_WARN("col_meta is added into table_meta multiple times",
            "table", table_schema->get_table_name(),
            "column", column_table_schema->get_column_name());
      } else if (OB_ISNULL(col_meta = LogMsgFactory::createColMeta())) {
        LOG_ERROR("createColMeta fails", "col_name", column_table_schema->get_column_name());
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(set_column_meta_(col_meta, *column_table_schema, *table_schema))) {
        LOG_ERROR("set_column_meta_ fail", KR(ret), KP(col_meta));
      } else if (0 !=
          (append_ret = table_meta->append(column_table_schema->get_column_name(), col_meta))) {
        LOG_ERROR("append col_meta to table_meta fail", K(append_ret),
            "table_name", table_schema->get_table_name(),
            "column_name", column_table_schema->get_column_name());
        ret = OB_ERR_UNEXPECTED;
      } else {
        // success
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_column_schema_info_(*table_schema, tb_schema_info,
                column_index, *column_table_schema))) {
          LOG_ERROR("set_column_schema_info_ fail", KR(ret), KPC(table_schema), K(tb_schema_info),
              K(column_index), KPC(column_table_schema));
        }
      }

      if (OB_SUCC(ret)) {
        if (! is_column_filter) {
          ++column_index;
        }
      }
    } // while

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    // iterator finish for all columns
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_table_schema_(version, table_id, table_schema->get_table_name(), column_index,
              tb_schema_info))) {
        LOG_ERROR("set_table_schema_ fail", KR(ret), K(version), K(table_id),
           "table_name", table_schema->get_table_name(),
           "non_hidden_column_cnt", column_index, K(tb_schema_info));
      } else {
        // succ
      }
    }

    if (OB_SUCC(ret)) {
      // set primary key column and index column
      if (OB_FAIL(set_primary_keys_(table_meta, table_schema, tb_schema_info))) {
        LOG_ERROR("set_primary_keys_ fail", KR(ret), "table_name", table_schema->get_table_name(),
            "table_id", table_schema->get_table_id(), K(tb_schema_info));
      } else if (OB_FAIL(set_unique_keys_(table_meta, table_schema, tb_schema_info, schema_mgr, stop_flag))) {
        // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("set_unique_keys_ fail", KR(ret), "table_name", table_schema->get_table_name(),
              "table_id", table_schema->get_table_id(), K(tb_schema_info));
        }
      }
    }
  }

  return ret;
}

int ObLogMetaManager::filter_column_(const share::schema::ObTableSchema &table_schema,
    const bool is_hidden_pk_table,
    const share::schema::ObColumnSchemaV2 &column_table_schema,
    bool &is_filter,
    bool &is_hidden_pk_table_pk_increment_column)
{
  int ret = OB_SUCCESS;
  // won't filter by default
  is_filter = false;
  is_hidden_pk_table_pk_increment_column = false;
  bool is_non_user_column = false;
  const uint64_t column_id = column_table_schema.get_column_id();
  const char *column_name = column_table_schema.get_column_name();
  const bool enable_output_invisible_column = TCONF.enable_output_invisible_column;

  if (OB_FAIL(filter_non_user_column(is_hidden_pk_table, enable_output_hidden_primary_key_, column_id,
          is_non_user_column, is_hidden_pk_table_pk_increment_column))) {
    LOG_ERROR("filter_non_user_column fail", KR(ret), K(is_hidden_pk_table),
        K(enable_output_hidden_primary_key_),
        K(column_id), K(column_name),
        K(is_non_user_column),
        K(is_hidden_pk_table_pk_increment_column));
  } else if (is_non_user_column) {
    is_filter = true;
    META_STAT_INFO("ignore non user column", K(is_non_user_column),
        "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id(),
        K(column_id), K(column_name));
  } else if (is_hidden_pk_table_pk_increment_column) {
    is_filter = false;
    LOG_INFO("handle hidden pk table __pk_increment column", K(is_filter), K(is_hidden_pk_table),
        "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id(),
        K(column_id), K(column_name));
  } else if (column_table_schema.is_hidden()) {
    is_filter = true;
    META_STAT_INFO("ignore hidden column",
        "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id(),
        K(column_id), K(column_name));
  } else if (column_table_schema.is_invisible_column() && ! enable_output_invisible_column) {
    is_filter = true;
    META_STAT_INFO("ignore invisible column",
        "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id(),
        K(column_id), K(column_name));
  } else {
    // do nothing
  }

  return ret;
}

int ObLogMetaManager::set_column_meta_(IColMeta *col_meta,
    const share::schema::ObColumnSchemaV2 &column_schema,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(col_meta)) {
    LOG_ERROR("invalid argument", K(col_meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint16_t type_flag = 0;
    ObScale decimals = 0; // FIXME: does liboblog need this?
    EMySQLFieldType mysql_type = MYSQL_TYPE_NOT_DEFINED;
    const ColumnType col_type = column_schema.get_data_type();

    if (OB_FAIL(ObSMUtils::get_mysql_type(column_schema.get_data_type(),
        mysql_type, type_flag, decimals))) {
      LOG_ERROR("get_mysql_type fail", KR(ret), "ob_type", column_schema.get_data_type());
    } else {
      if (ObEnumType == col_type || ObSetType == col_type) {
        // get extended_type_info from column schema and convert it to array of std::string
        const ObIArray<ObString> &extended_type_info = column_schema.get_extended_type_info();
        std::vector<const char *> extended_type_info_vec;
        for (int64_t i = 0; i < extended_type_info.count(); i++) {
          const ObString &extended_type_info_item = extended_type_info.at(i);
          extended_type_info_vec.push_back(extended_type_info_item.ptr());
        }
        col_meta->setValuesOfEnumSet(extended_type_info_vec);

        //mysql treat it as MYSQL_TYPE_STRING, it is not suitable for liboblog
        if (ObEnumType == col_type) {
          mysql_type = obmysql::MYSQL_TYPE_ENUM;
        } else if (ObSetType == col_type) {
          mysql_type = obmysql::MYSQL_TYPE_SET;
        } else if (ObNumberType == col_type || ObUNumberType == col_type) {
          col_meta->setScale(column_schema.get_data_scale());
          col_meta->setPrecision(column_schema.get_data_precision());
        }
      }
      bool signed_flag = ((type_flag & UNSIGNED_FLAG) == 0);


      if (ObBitType == col_type) {
        // the length of BIT type is required,
        // the "length" of the BIT type is store in precision
        col_meta->setLength(column_schema.get_data_precision());
      } else {
        // for types with valid length(string\enumset\rowid\json\raw\lob\geo),
        // get_data_length returns the valid length, returns 0 for other types.
        col_meta->setLength(column_schema.get_data_length());
      }

      col_meta->setName(column_schema.get_column_name());
      col_meta->setType(static_cast<int>(mysql_type));
      col_meta->setSigned(signed_flag);
      col_meta->setIsPK(column_schema.is_original_rowkey_column());
      col_meta->setNotNull(! column_schema.is_nullable());
      SET_ENCODING(col_meta, column_schema.get_charset_type());

      if (column_schema.is_heap_alter_rowkey_column()) {
        col_meta->setHiddenRowKey();
      }
      // mark if is generate column
      // default value of IColMeta::isGenerated is false
      // call setGenerated if is col is generated
      if (column_schema.is_generated_column()) {
        col_meta->setGenerated(true);
      }

      META_STAT_DEBUG("build_col_meta: ",
          "table_name", table_schema.get_table_name(),
          "table_id", table_schema.get_table_id(),
          "column", column_schema.get_column_name(),
          "type", column_schema.get_data_type(),
          "mysql_type", get_emysql_field_type_str(mysql_type),
          "signed", col_meta->isSigned(),
          "is_pk", col_meta->isPK(),
          "not_null", col_meta->isNotNull(),
          "encoding", col_meta->getEncoding(),
          "default", col_meta->getDefault(),
          "isHiddenRowKey", col_meta->isHiddenRowKey(),
          "isGeneratedColumn", col_meta->isGenerated());

      // Do not need
      //col_meta->setLength(data_length);
      //col_meta->setDecimals(int decimals);
      //col_meta->setRequired(int required);
      //col_meta->setValuesOfEnumSet(std::vector<std::string> &v);
      //col_meta->setValuesOfEnumSet(std::vector<const char*> &v);
      //col_meta->setValuesOfEnumSet(const char** v, size_t size);
    }
  }

  return ret;
}

int ObLogMetaManager::set_primary_keys_(ITableMeta *table_meta,
    const share::schema::ObTableSchema *schema,
    const TableSchemaInfo &tb_schema_info)
{
  int ret = OB_SUCCESS;
  int64_t valid_pk_num = 0;
  const ObRowkeyInfo &rowkey_info = schema->get_rowkey_info();
  ObLogAdaptString pks(ObModIds::OB_LOG_TEMP_MEMORY);
  ObLogAdaptString pk_info(ObModIds::OB_LOG_TEMP_MEMORY);

  if (OB_ISNULL(table_meta) || OB_ISNULL(schema)) {
    LOG_ERROR("invalid argument", K(table_meta), K(schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); i++) {
      uint64_t column_id = OB_INVALID_ID;
      int64_t column_index = -1;
      const share::schema::ObColumnSchemaV2 *column_schema = NULL;
      ColumnSchemaInfo *column_schema_info = NULL;
      ColumnPropertyFlag column_property_flag;

      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_ERROR("get_column_id from rowkey info fail", K(rowkey_info), KR(ret));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(column_schema = schema->get_column_schema(column_id))) {
        LOG_ERROR("get column schema fail", "table", schema->get_table_name(), K(column_id));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(tb_schema_info.get_column_schema_info(column_id, enable_output_hidden_primary_key_,
              column_schema_info, column_property_flag))) {
        LOG_ERROR("get_column_schema_info", KR(ret), "table_id", schema->get_table_id(),
            "table_name", schema->get_table_name(),
            K(column_id), K(enable_output_hidden_primary_key_), K(column_schema_info), K(column_property_flag));
      // Only ColumnPropertyFlag non-user columns are judged here, column deletion and hidden columns depend on column_schema
      } else if (column_property_flag.is_non_user()) {
        // filter non user column
        META_STAT_INFO("ignore non user rowkey column", K(column_property_flag),
            "table_name", schema->get_table_name(),
            "table_id", schema->get_table_id(),
            K(column_id), "rowkey_index", i,
            "rowkey_count", rowkey_info.get_size());
      } else if (column_property_flag.is_hidden()) {  // NOTE: ignore hidden column
        META_STAT_INFO("ignore hidden rowkey column", "table_name", schema->get_table_name(),
            "table_id", schema->get_table_id(),
            "column", column_schema->get_column_name(), K(column_id), "rowkey_index", i,
            "rowkey_count", rowkey_info.get_size());
      } else if (column_property_flag.is_invisible()) {
        META_STAT_INFO("ignore invisible rowkey column", "table_name", schema->get_table_name(),
            "table_id", schema->get_table_id(),
            "column", column_schema->get_column_name(), K(column_id), "rowkey_index", i,
            "rowkey_count", rowkey_info.get_size());
      } else if (OB_ISNULL(column_schema_info)) {
        LOG_ERROR("column_schema_info is null", K(column_schema_info));
        ret = OB_ERR_UNEXPECTED;
      } else if (!column_schema_info->is_rowkey()) { // not rowkey
        if (schema->is_new_no_pk_table()) {
          META_STAT_INFO("ignore not rowkey column", "table_name", schema->get_table_name(),
              "table_id", schema->get_table_id(),
              "column", column_schema->get_column_name(), K(column_id), "rowkey_index", i,
              "rowkey_count", rowkey_info.get_size());
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("not a heap table and have no-rowkey in TableSchema::rowley_info_", K(ret),
              K(column_schema_info));
        }
      } else {
        column_index = column_schema_info->get_column_idx();
        const bool is_hidden_pk_table_pk_increment_column = column_schema_info->is_hidden_pk_table_pk_increment_column();

        if (OB_UNLIKELY(column_index < 0 || column_index >= OB_MAX_COLUMN_NUMBER)) {
          LOG_ERROR("column_index is invalid", K(column_index),
              "table_id", schema->get_table_id(),
              "table_name", schema->get_table_name(),
              "column_id", column_schema->get_column_id(),
              "column_name", column_schema->get_column_name());
          ret = OB_ERR_UNEXPECTED;
        } else {
          ret = pks.append(column_schema->get_column_name());

          if (OB_SUCCESS == ret) {
            if (i < (rowkey_info.get_size() - 1)) {
              if (is_hidden_pk_table_pk_increment_column) {
                // do nothing
              } else {
                ret = pks.append(",");
              }
            }
          }

          if (OB_SUCCESS == ret) {
            if (0 == valid_pk_num) {
              ret = pk_info.append("(");
            } else {
              ret = pk_info.append(",");
            }
          }

          if (OB_SUCCESS == ret) {
            ret = pk_info.append_int64(column_index);
          }

          if (OB_SUCCESS == ret) {
            valid_pk_num++;
          } else {
            LOG_ERROR("pks or pk_info append fail", KR(ret), K(pks), K(pk_info), K(column_index));
          }
        }
      }
    } // for

    if (OB_SUCC(ret)) {
      table_meta->setHasPK((valid_pk_num > 0));

      // 只有在存在pk的情况下，才设置主键信息
      if (valid_pk_num > 0) {
        if (OB_FAIL(pk_info.append(")"))) {
          LOG_ERROR("pk_info append fail", KR(ret), K(pk_info));
        } else {
          const char *pk_info_str = "";
          const char *pks_str = "";

          if (OB_FAIL(pk_info.cstr(pk_info_str))) {
            LOG_ERROR("get pk_info str fail", KR(ret), K(pk_info));
          } else if (OB_FAIL(pks.cstr(pks_str))) {
            LOG_ERROR("get pks str fail", KR(ret), K(pks));
          }
          // 要求cstr是有效的
          else if (OB_ISNULL(pk_info_str) || OB_ISNULL(pks_str)) {
            LOG_ERROR("pk_info_str or pks_str is invalid", K(pk_info_str), K(pks_str), K(pk_info),
                K(pks), K(valid_pk_num));
            ret = OB_ERR_UNEXPECTED;
          } else {
            table_meta->setPkinfo(pk_info_str);
            table_meta->setPKs(pks_str);
          }
        }
      }

      META_STAT_INFO("set_primary_keys", KR(ret), "table_name", schema->get_table_name(),
          "table_id", schema->get_table_id(),
          "has_pk", table_meta->hasPK(), "pk_info", table_meta->getPkinfo(),
          "pks", table_meta->getPKs());
    }
  }
  return ret;
}

int ObLogMetaManager::set_unique_keys_from_unique_index_table_(const share::schema::ObTableSchema *table_schema,
    const TableSchemaInfo &tb_schema_info,
    const share::schema::ObTableSchema *index_table_schema,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info,
    int64_t &valid_uk_column_count)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)
      || OB_ISNULL(is_uk_column_array)
      || OB_ISNULL(index_table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema), K(is_uk_column_array), K(index_table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! index_table_schema->is_unique_index())) {
    LOG_ERROR("invalid index table schema which is not unique index",
        K(index_table_schema->is_unique_index()));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObIndexInfo &index_info = index_table_schema->get_index_info();
    int64_t index_key_count = index_info.get_size();
    valid_uk_column_count = 0;

    for (int64_t index_info_id = 0;
        OB_SUCC(ret) && index_info_id < index_key_count;
        index_info_id++) {
      const share::schema::ObColumnSchemaV2 *column_schema = NULL;
      uint64_t index_column_id = OB_INVALID_ID;
      if (OB_FAIL(index_info.get_column_id(index_info_id, index_column_id))) {
        LOG_ERROR("get_column_id from index_info fail", KR(ret), K(index_info_id), K(index_info),
            "index_table_name", index_table_schema->get_table_name(),
            "index_table_id", index_table_schema->get_table_id());
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(index_column_id))) {
        if (index_column_id > OB_MIN_SHADOW_COLUMN_ID) {
          LOG_DEBUG("ignore shadow column", K(index_column_id),
              "table_name", table_schema->get_table_name(),
              "table_id", table_schema->get_table_id(),
              "index_table_name", index_table_schema->get_table_name());
        } else if (ObColumnSchemaV2::is_hidden_pk_column_id(index_column_id)) {
          LOG_DEBUG("ignore hidden column", K(index_column_id),
              "table_name", table_schema->get_table_name(),
              "table_id", table_schema->get_table_id(),
              "index_table_name", index_table_schema->get_table_name());
        } else {
          LOG_ERROR("get index column schema fail", K(index_column_id),
              "table_name", table_schema->get_table_name(),
              "table_id", table_schema->get_table_id(),
              "index_table_name", index_table_schema->get_table_name());
          ret = OB_ERR_UNEXPECTED;
        }
      } else if (column_schema->is_hidden()) {
        LOG_WARN("ignore hidden index column", "table_name", table_schema->get_table_name(),
            "table_id", table_schema->get_table_id(),
            "column_name", column_schema->get_column_name(), K(index_info));
      } else if (column_schema->is_shadow_column()) {
        LOG_WARN("ignore shadow column", "table_name", table_schema->get_table_name(),
            "table_id", table_schema->get_table_id(),
            "column_name", column_schema->get_column_name(), K(index_info));
      } else {
        int64_t user_column_index = -1;   // Column index as seen from the user's perspective
        ColumnSchemaInfo *column_schema_info = NULL;
        ColumnPropertyFlag column_property_flag;

        if (OB_FAIL(tb_schema_info.get_column_schema_info(index_column_id, enable_output_hidden_primary_key_,
                column_schema_info, column_property_flag))) {
          LOG_ERROR("get_column_schema_info", KR(ret), "table_id", table_schema->get_table_id(),
              "table_name", table_schema->get_table_name(),
              K(index_column_id), K(enable_output_hidden_primary_key_),
              K(column_schema_info), K(column_property_flag));
        // Only ColumnPropertyFlag non-user columns are judged here, column deletion and hidden columns depend on column_schema
        } else if (column_property_flag.is_non_user()) {
          // Filtering non-user columns
          META_STAT_INFO("ignore non user column", K(column_property_flag),
              "table_id", table_schema->get_table_id(),
              "table_name", table_schema->get_table_name(),
              K(index_column_id), K(index_info_id), K(index_key_count));
        } else if (column_property_flag.is_invisible()) {
          // Filtering invisible columns
          META_STAT_INFO("ignore invisible column", K(column_property_flag),
              "table_id", table_schema->get_table_id(),
              "table_name", table_schema->get_table_name(),
              K(index_column_id), K(index_info_id), K(index_key_count));
        } else if (OB_ISNULL(column_schema_info)) {
          LOG_ERROR("column_schema_info is null", K(column_schema_info));
          ret = OB_ERR_UNEXPECTED;
        } else {
          user_column_index = column_schema_info->get_column_idx();

          if (OB_UNLIKELY(user_column_index < 0 || user_column_index >= OB_MAX_COLUMN_NUMBER)) {
            LOG_ERROR("user_column_index is invalid", K(user_column_index),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                K(index_column_id));
            ret = OB_ERR_UNEXPECTED;
          } else {
            LOG_DEBUG("set_unique_keys_from_unique_index_table",
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "index_table_id", index_table_schema->get_table_id(),
                "index_table_name", index_table_schema->get_table_name(),
                "schema_version", table_schema->get_schema_version(),
                K(index_column_id),
                K(user_column_index));

            if (0 == valid_uk_column_count) {
              ret = uk_info.append("(");
            } else {
              ret = uk_info.append(",");
            }

            if (OB_SUCC(ret)) {
              ret = uk_info.append_int64(user_column_index);
            }

            if (OB_FAIL(ret)) {
              LOG_ERROR("uk_info append string fail", KR(ret), K(uk_info));
            } else {
              is_uk_column_array[user_column_index] = true;
              valid_uk_column_count++;
            }
          }
        }
      }
    } // for

    if (OB_SUCC(ret)) {
      if (valid_uk_column_count > 0) {
        if (OB_FAIL(uk_info.append(")"))) {
          LOG_ERROR("uk_info append string fail", KR(ret), K(uk_info), K(valid_uk_column_count));
        }
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::set_unique_keys_(ITableMeta *table_meta,
    const share::schema::ObTableSchema *table_schema,
    const TableSchemaInfo &tb_schema_info,
    ObLogSchemaGuard &schema_mgr,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_meta) || OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_meta), K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObLogAdaptString uks(ObModIds::OB_LOG_TEMP_MEMORY);
    ObLogAdaptString uk_info(ObModIds::OB_LOG_TEMP_MEMORY);

    // Identifies which column is the unique index column
    bool *is_uk_column_array = NULL;
    // Number of valid unique index tables
    int64_t valid_uk_table_count = 0;
    int64_t index_table_count = table_schema->get_index_tid_count();
    int64_t version = table_schema->get_schema_version();
    uint64_t table_id = table_schema->get_table_id();
    int64_t column_count = tb_schema_info.get_non_hidden_column_count();

    if (column_count < 0) {
      LOG_ERROR("column_num is invalid", "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(), K(column_count));
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (index_table_count > 0) {
        int64_t is_uk_column_array_size = column_count * sizeof(bool);
        is_uk_column_array = static_cast<bool *>(ob_log_malloc(is_uk_column_array_size));

        if (OB_ISNULL(is_uk_column_array)) {
          LOG_ERROR("allocate memory for is_uk column array fail", K(column_count),
              K(is_uk_column_array_size));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          (void)memset(is_uk_column_array, 0, column_count * sizeof(bool));

          // Set unique index information from all index tables
          if (OB_FAIL(set_unique_keys_from_all_index_table_(valid_uk_table_count, *table_schema, tb_schema_info,
                  schema_mgr, stop_flag, is_uk_column_array, uk_info))) {
            // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("set_unique_keys_from_all_index_table_ fail", KR(ret), K(valid_uk_table_count),
                  K(is_uk_column_array));
            }
          }
          // Set the UKs() field value if it contains a valid unique index
          else if (valid_uk_table_count > 0) {
            bool is_first_uk_column = true;
            for (int64_t index = 0; OB_SUCC(ret) && index < column_count; index++) {
              if (is_uk_column_array[index]) {
                const ObColumnSchemaV2 *column_schema = NULL;
                uint64_t column_id = OB_INVALID_ID;

                if (OB_FAIL(tb_schema_info.get_column_id(index, column_id))) {
                  LOG_ERROR("tb_schema_info get_column_id fail", KR(ret), K(version), K(table_id),
                      "column_idx", index, K(column_id));
                } else if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
                  LOG_ERROR("column_id is not valid", K(column_id));
                  ret = OB_ERR_UNEXPECTED;
                } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
                  LOG_ERROR("get column schema fail", K(column_id), K(index), K(column_count),
                      "table_id", table_schema->get_table_id(),
                      "table_name", table_schema->get_table_name(),
                      "table_schame_version", table_schema->get_schema_version());
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  if (is_first_uk_column) {
                    is_first_uk_column = false;
                  } else {
                    // If not the first uk column, append comma
                    ret = uks.append(",");
                  }

                  // then append column name
                  if (OB_SUCC(ret)) {
                    ret = uks.append(column_schema->get_column_name());
                  }

                  if (OB_FAIL(ret)) {
                    LOG_ERROR("uks append fail", KR(ret), K(uks), K(column_schema->get_column_name()),
                        K(table_schema->get_table_name()));
                  }
                }
              }
            } // for
          }
        }
      } // if (index_table_count > 0)
    }

    if (OB_SUCC(ret)) {
      table_meta->setHasUK((valid_uk_table_count > 0));

      if (valid_uk_table_count > 0) {
        const char *uks_str = "";
        const char *uk_info_str = "";

        if (OB_FAIL(uks.cstr(uks_str))) {
          LOG_ERROR("get uks string fail", KR(ret), K(uks));
        } else if (OB_FAIL(uk_info.cstr(uk_info_str))) {
          LOG_ERROR("get uks string fail", KR(ret), K(uk_info));
        } else if (OB_ISNULL(uks_str) || OB_ISNULL(uk_info_str)) {
          LOG_ERROR("invalid uks_str or uk_info_str", K(uks_str), K(uk_info_str), K(uks), K(uk_info));
          ret = OB_ERR_UNEXPECTED;
        } else {
          table_meta->setUkinfo(uk_info_str);
          table_meta->setUKs(uks_str);
        }
      }
    }

    META_STAT_INFO("set_unique_keys", KR(ret), "table_name", table_schema->get_table_name(),
        "table_id", table_schema->get_table_id(), K(valid_uk_table_count),
        "has_uk", table_meta->hasUK(), "uk_info", table_meta->getUkinfo(),
        "uks", table_meta->getUKs());

    if (NULL != is_uk_column_array) {
      ob_log_free(is_uk_column_array);
      is_uk_column_array = NULL;
    }
  }
  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::set_unique_keys_from_all_index_table_(int64_t &valid_uk_table_count,
    const share::schema::ObTableSchema &table_schema,
    const TableSchemaInfo &tb_schema_info,
    ObLogSchemaGuard &schema_mgr,
    volatile bool &stop_flag,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info)
{
  int ret = OB_SUCCESS;
  int64_t index_table_count = table_schema.get_index_tid_count();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_ISNULL(is_uk_column_array)) {
    LOG_ERROR("invalid argument", K(is_uk_column_array));
    ret = OB_INVALID_ARGUMENT;
  } else if (index_table_count <= 0) {
    // no index table
  } else {
    // get array of index table id
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_ERROR("get_index_tid_array fail", KR(ret), "table_name", table_schema.get_table_name(),
          "table_id", table_schema.get_table_id());
    } else {
      LOG_DEBUG("set_unique_keys_from_all_index_table_ begin",
          "table_id", table_schema.get_table_id(),
          "table_name", table_schema.get_table_name(),
          K(simple_index_infos));

      // Iterate through all index tables to find the unique index table
      for (int64_t index = 0; OB_SUCC(ret) && index < index_table_count; index++) {
        const share::schema::ObTableSchema *index_table_schema = NULL;

        // retry to fetch schma until success of quit
        // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
        RETRY_FUNC(stop_flag, schema_mgr, get_table_schema, simple_index_infos.at(index).table_id_,
            index_table_schema, GET_SCHEMA_TIMEOUT);

        if (OB_FAIL(ret)) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("get index table schema fail", KR(ret), K(simple_index_infos.at(index).table_id_));
          }
        } else if (OB_ISNULL(index_table_schema)) {
          LOG_ERROR("get index table schema fail", "table_id", table_schema.get_table_id(),
              "table_name", table_schema.get_table_name(),
              "index_table_id", simple_index_infos.at(index).table_id_, K(index_table_count), K(index));
          ret = OB_ERR_UNEXPECTED;
        } else if (index_table_schema->is_dropped_schema()) {
          LOG_INFO("index table is dropped, need filter", "table_id", table_schema.get_table_id(),
              "table_name", table_schema.get_table_name(),
              "index_table_id", simple_index_infos.at(index).table_id_,
              "index_table_name", index_table_schema->get_table_name(),
              K(index_table_count), K(index));
        }
        // Handling uniquely indexed tables
        else if (index_table_schema->is_unique_index()) {
          ObLogAdaptString tmp_uk_info(ObModIds::OB_LOG_TEMP_MEMORY);
          int64_t valid_uk_column_count = 0;

          // Get unique key information from a unique index table
          if (OB_FAIL(set_unique_keys_from_unique_index_table_(&table_schema,
              tb_schema_info,
              index_table_schema,
              is_uk_column_array,
              tmp_uk_info,
              valid_uk_column_count))) {
            LOG_ERROR("set_unique_keys_from_unique_index_table_ fail", KR(ret),
                "table_name", table_schema.get_table_name(),
                "table_id", table_schema.get_table_id(),
                "index_table_name", index_table_schema->get_table_name(),
                K(is_uk_column_array));
          }
          // Process only when valid unique index column information is obtained
          else if (valid_uk_column_count > 0) {
            const char *tmp_uk_info_str = NULL;
            if (OB_FAIL(tmp_uk_info.cstr(tmp_uk_info_str))) {
              LOG_ERROR("get tmp_uk_info str fail", KR(ret), K(tmp_uk_info));
            } else if (OB_ISNULL(tmp_uk_info_str)) {
              LOG_ERROR("tmp_uk_info_str is invalid", K(tmp_uk_info_str), K(tmp_uk_info),
                  K(valid_uk_column_count), K(index_table_schema->get_table_name()));
              ret = OB_ERR_UNEXPECTED;
            } else {
              if (valid_uk_table_count > 0) {
                ret = uk_info.append(",");
              }

              if (OB_SUCC(ret)) {
                ret = uk_info.append(tmp_uk_info_str);
              }

              if (OB_FAIL(ret)) {
                LOG_ERROR("uk_info append string fail", KR(ret), K(uk_info));
              } else {
                valid_uk_table_count++;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogMetaManager::build_db_meta_(
    const DBSchemaInfo &db_schema_info,
    const TenantSchemaInfo &tenant_schema_info,
    IDBMeta *&db_meta)
{
  int ret = OB_SUCCESS;

  IDBMeta *tmp_db_meta = LogMsgFactory::createDBMeta();

  if (OB_ISNULL(tmp_db_meta)) {
    LOG_ERROR("createDBMeta fail, return NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    // set DB Name to：TENANT.DATABASE
    std::string db_name_str = tenant_schema_info.name_;
    db_name_str.append(".");
    db_name_str.append(db_schema_info.name_);

    tmp_db_meta->setName(db_name_str.c_str());
    SET_ENCODING(tmp_db_meta, CHARSET_BINARY);
    tmp_db_meta->setUserData((void*)1);
  }

  if (OB_SUCC(ret)) {
    db_meta = tmp_db_meta;
  }

  return ret;
}

int ObLogMetaManager::build_ddl_meta_()
{
  int ret = OB_SUCCESS;

  if (NULL != ddl_table_meta_) {
    LOG_ERROR("meta has been built", K(ddl_table_meta_));
    ret = OB_INIT_TWICE;
  } else {
    ITableMeta *tmp_table_meta = LogMsgFactory::createTableMeta();
    IColMeta *ddl_stmt_col_meta = NULL;
    IColMeta *ddl_schema_version_col_meta = NULL;
    const char *ddl_stmt_col_name = "ddl";
    const char *ddl_schema_version_col_name = "ddl_schema_version";

    if (OB_ISNULL(tmp_table_meta)) {
      LOG_ERROR("createTableMeta fail, return NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(build_col_meta_(ddl_stmt_col_name, ddl_stmt_col_meta))) {
      LOG_ERROR("build_col_meta_ fail", KR(ret), K(ddl_stmt_col_name),
          K(ddl_stmt_col_meta));
    } else if (OB_FAIL(build_col_meta_(ddl_schema_version_col_name, ddl_schema_version_col_meta))) {
      LOG_ERROR("build_col_meta_ fail", KR(ret), K(ddl_schema_version_col_name),
          K(ddl_schema_version_col_meta));
    } else {
      (void)tmp_table_meta->append(ddl_stmt_col_name, ddl_stmt_col_meta);
      (void)tmp_table_meta->append(ddl_schema_version_col_name, ddl_schema_version_col_meta);
      tmp_table_meta->setName("");
      tmp_table_meta->setDBMeta(NULL);
      tmp_table_meta->setEncoding("");
      tmp_table_meta->setUserData(NULL);

      ddl_table_meta_ = tmp_table_meta;
    }

    if (OB_FAIL(ret)) {
      // destroy all column meta of table meta when destroy table meta
      if (NULL != tmp_table_meta) {
        LogMsgFactory::destroy(tmp_table_meta);
        tmp_table_meta = NULL;
      }
      ddl_stmt_col_meta = NULL;
      ddl_schema_version_col_meta = NULL;
    }
  }

  return ret;
}

int ObLogMetaManager::build_col_meta_(const char *ddl_col_name,
    IColMeta *&col_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(col_meta = LogMsgFactory::createColMeta())) {
    LOG_ERROR("createColMeta fail, return NULL", K(col_meta));
    ret = OB_ERR_UNEXPECTED;
  } else {
    EMySQLFieldType mysql_type = obmysql::MYSQL_TYPE_VAR_STRING;

    col_meta->setName(ddl_col_name);
    col_meta->setType(static_cast<int>(mysql_type));
    col_meta->setSigned(true);
    col_meta->setIsPK(false);
    col_meta->setNotNull(false);
    col_meta->setEncoding("");
    col_meta->setDefault("");
  }

  if (OB_FAIL(ret)) {
    if (NULL != col_meta) {
      LogMsgFactory::destroy(col_meta);
      col_meta = NULL;
    }
  }

  return ret;
}

void ObLogMetaManager::destroy_ddl_meta_()
{
  if (NULL != ddl_table_meta_) {
    // destroy all column meta of table meta when destroy table meta
    LogMsgFactory::destroy(ddl_table_meta_);
    ddl_table_meta_ = NULL;
  }
}

int ObLogMetaManager::get_table_schema_meta(const int64_t version,
    const uint64_t table_id,
    TableSchemaInfo *&tb_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == version)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_ERROR("invalid argument", K(version), K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MulVerTableKey table_key(version, table_id);

    if (OB_FAIL(tb_schema_info_map_.get(table_key, tb_schema_info))) {
      LOG_ERROR("tb_schema_info_map_ get fail", KR(ret), K(table_key), K(tb_schema_info));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogMetaManager::set_table_schema_(const int64_t version,
    const uint64_t table_id,
    const char *table_name,
    const int64_t non_hidden_column_cnt,
    TableSchemaInfo &tb_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == version)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_ISNULL(table_name)) {
    LOG_ERROR("invalid argument", K(version), K(table_id), K(table_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tb_schema_info.set_non_hidden_column_count(non_hidden_column_cnt);

    MulVerTableKey table_key(version, table_id);

    if (OB_FAIL(tb_schema_info_map_.insert(table_key, &tb_schema_info))) {
      LOG_ERROR("tb_schema_info_map_ insert fail", KR(ret), K(table_key), K(tb_schema_info));
    } else {
      LOG_INFO("set_table_schema succ", "schema_version", version,
          K(table_id), K(table_name), K(tb_schema_info));
    }
  }

  return ret;
}

int ObLogMetaManager::try_erase_table_schema_(
    const uint64_t table_id,
    const int64_t version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("meta manager has not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_UNLIKELY(OB_INVALID_VERSION == version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(table_id), K(version));
  } else {
    MulVerTableKey table_key(version, table_id);

    if (OB_FAIL(tb_schema_info_map_.erase(table_key))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("erase table_key from tb_schema_info_map_ failed, may not affect main process, ignore", KR(ret), K(table_key));
      }
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLogMetaManager::alloc_table_schema_info_(TableSchemaInfo *&tb_schema_info)
{
  int ret = OB_SUCCESS;
  tb_schema_info = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tb_schema_info = static_cast<TableSchemaInfo *>(allocator_.alloc(
      sizeof(TableSchemaInfo))))) {
    LOG_ERROR("allocate memory fail", K(sizeof(TableSchemaInfo)));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new(tb_schema_info) TableSchemaInfo(allocator_);
  }

  return ret;
}

int ObLogMetaManager::free_table_schema_info_(TableSchemaInfo *&tb_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tb_schema_info)) {
    LOG_ERROR("tb_schema_info is null", K(tb_schema_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tb_schema_info->~TableSchemaInfo();
    allocator_.free(tb_schema_info);
    tb_schema_info = NULL;
  }

  return ret;
}

int ObLogMetaManager::set_column_schema_info_(const share::schema::ObTableSchema &table_schema,
    TableSchemaInfo &tb_schema_info,
    const int64_t column_idx,
    const share::schema::ObColumnSchemaV2 &column_table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(tb_schema_info.init_column_schema_info(table_schema, column_table_schema,
          column_idx, enable_output_hidden_primary_key_, *obj2str_helper_))) {
    LOG_ERROR("tb_schema_info init_column_schema_info fail", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        K(column_table_schema), K(column_idx),
        K(enable_output_hidden_primary_key_));
  } else {
    // succ
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
