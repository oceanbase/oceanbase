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
 *
 * Meta Manager
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_meta_manager.h"

#include "lib/atomic/ob_atomic.h"                 // ATOMIC_*
#include "observer/mysql/obsm_utils.h"            // ObSMUtils
#include "rpc/obmysql/ob_mysql_global.h"          // obmysql
#include "share/schema/ob_table_schema.h"         // ObTableSchema, ObSimpleTableSchemaV2
#include "share/schema/ob_column_schema.h"        // ObColumnSchemaV2
#include "logservice/data_dictionary/ob_data_dict_struct.h"

#include "ob_log_schema_getter.h"                 // ObLogSchemaGuard, DBSchemaInfo, TenantSchemaInfo
#include "ob_log_utils.h"                         // print_mysql_type, ob_cdc_malloc
#include "ob_obj2str_helper.h"                    // ObObj2strHelper
#include "ob_log_adapt_string.h"                  // ObLogAdaptString
#include "ob_log_config.h"                        // TCONF
#include "ob_log_instance.h"                      // TCTX
#include "ob_log_schema_cache_info.h"             // TableSchemaInfo
#include "ob_log_timezone_info_getter.h"          // IObCDCTimeZoneInfoGetter

#define DEFAULT_ENCODING  ""

#define META_STAT_INFO(fmt, args...) LOG_INFO("[META_STAT] " fmt, args)
#define META_STAT_DEBUG(fmt, args...) LOG_DEBUG("[META_STAT] " fmt, args)

#define SET_ENCODING(meta, charset) \
    do {\
      meta->setEncoding(ObCharset::charset_name(charset)); \
    } while (0)

#define SET_UK_INFO \
    ObLogAdaptString tmp_uk_info(ObModIds::OB_LOG_TEMP_MEMORY); \
    int64_t valid_uk_column_count = 0; \
    /** Get unique key information from a unique index table */ \
    if (OB_FAIL(set_unique_keys_from_unique_index_table_( \
        &table_schema, \
        index_table_schema, \
        tb_schema_info, \
        is_uk_column_array, \
        tmp_uk_info, \
        valid_uk_column_count))) { \
      LOG_ERROR("set_unique_keys_from_unique_index_table_ fail", KR(ret), \
          "table_name", table_schema.get_table_name(), \
          "table_id", table_schema.get_table_id(), \
          "index_table_name", index_table_schema->get_table_name(), \
          K(is_uk_column_array)); \
    } \
    /** Process only when valid unique index column information is obtained */ \
    else if (valid_uk_column_count > 0) { \
      const char *tmp_uk_info_str = NULL; \
      if (OB_FAIL(tmp_uk_info.cstr(tmp_uk_info_str))) { \
        LOG_ERROR("get tmp_uk_info str fail", KR(ret), K(tmp_uk_info)); \
      } else if (OB_ISNULL(tmp_uk_info_str)) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_ERROR("tmp_uk_info_str is invalid", KR(ret), K(tmp_uk_info_str), K(tmp_uk_info), \
            K(valid_uk_column_count), K(index_table_schema->get_table_name())); \
      } else { \
        if (valid_uk_table_count > 0) { \
          ret = uk_info.append(","); \
        } \
        if (OB_SUCC(ret)) { \
          ret = uk_info.append(tmp_uk_info_str); \
        } \
        if (OB_FAIL(ret)) { \
          LOG_ERROR("uk_info append string fail", KR(ret), K(uk_info)); \
        } else { \
          valid_uk_table_count++; \
        } \
      } \
    }

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
using namespace oceanbase::datadict;
namespace oceanbase
{
namespace libobcdc
{
void ObLogMetaManager::set_column_encoding_(const common::ObObjType &col_type,
     const common::ObCharsetType &cs_type,
     IColMeta *meta)
{
  if (1 == TCONF.enable_compatible_charset) {
    if (cs_type > ObCharsetType::CHARSET_BINARY) {
      SET_ENCODING(meta, cs_type);
    } else if (ob_is_string_or_lob_type(col_type) ||
        ob_is_enum_or_set_type(col_type) ||
        ob_is_json(col_type)) {
      SET_ENCODING(meta, cs_type);
    } else {
      meta->setEncoding(DEFAULT_ENCODING);
    }
  } else {
    SET_ENCODING(meta, cs_type);
  }
}

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

int ObLogMetaManager::get_usr_def_col_from_table_schema_(
    const share::schema::ObTableSchema &schema,
    ObIArray<uint64_t> &usr_def_col)
{
  int ret = OB_SUCCESS;
  ObColumnIterByPrevNextID iter(schema);
  const ObColumnSchemaV2 *column_schema = NULL;
  usr_def_col.reset();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.next(column_schema))) {
      if (ret != OB_ITER_END) {
        LOG_ERROR("iterater table schema failed", K(usr_def_col), K(schema));
      }
    } else if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get column_schema failed when iterating the schema", K(column_schema), K(schema), K(usr_def_col));
    } else if (OB_FAIL(usr_def_col.push_back(column_schema->get_column_id()))) {
      LOG_ERROR("usr_def_col push back column id failed", K(usr_def_col), K(column_schema), K(schema));
    } else {
      _LOG_DEBUG("table_id: %lu, table_name: %s, column_id: %lu, column_name: %s, schema_version: %ld, tenant_id:%lu",
          schema.get_table_id(), schema.get_table_name(),
          column_schema->get_column_id(), column_schema->get_column_name(),
          schema.get_schema_version(), schema.get_tenant_id());
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::get_table_meta(
    const uint64_t tenant_id,
    const int64_t global_schema_version,
    const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  TableMetaInfo *meta_info = NULL;

  if (OB_ISNULL(simple_table_schema)) {
    LOG_ERROR("invalid argument", K(simple_table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MetaKey key(simple_table_schema->get_tenant_id(), simple_table_schema->get_table_id());

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
        ObSEArray<uint64_t, 16> usr_def_col;

        IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
        if (OB_ISNULL(schema_getter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("expect valid schema_getter", KR(ret));
        } else {
          ObTimeGuard time_guard("get_table_meta", 2 * 1000 * 1000);
          RETRY_FUNC(stop_flag, *schema_getter, get_schema_guard_and_full_table_schema, tenant_id, table_id, global_schema_version, GET_SCHEMA_TIMEOUT,
              schema_mgr, table_schema);
          time_guard.click("get_full_table_schema");

          if (OB_FAIL(ret)) {
            // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("get_schema_guard_and_full_table_schema fail", KR(ret), K(tenant_id),
                  K(global_schema_version),
                  "table_id", simple_table_schema->get_table_id(),
                  "table_name", simple_table_schema->get_table_name(), KPC(table_schema));
            }
          } else if (OB_ISNULL(table_schema)) {
            // tenant has been dropped
            ret = OB_TENANT_HAS_BEEN_DROPPED;
            LOG_WARN("table_schema is null, tenant may be dropped", KR(ret), K(table_schema),
                "tenant_id", simple_table_schema->get_tenant_id(),
                K(global_schema_version),
                "table_id", simple_table_schema->get_table_id(),
                "table_name", simple_table_schema->get_table_name(), KPC(simple_table_schema));
          } else if (OB_FAIL(get_usr_def_col_from_table_schema_(*table_schema, usr_def_col))) {
            LOG_ERROR("get_usr_def_col_from_table_schema failed", K(tenant_id), K(global_schema_version),
                K(table_schema), K(usr_def_col));
          } else if (OB_FAIL(add_and_get_table_meta_(meta_info, table_schema, usr_def_col,
              schema_mgr, table_meta, stop_flag))) {
            // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("add_and_get_table_meta_ fail", KR(ret), K(tenant_id),
                  K(global_schema_version), K(meta_info),
                  "table_name", table_schema->get_table_name(),
                  "table_id", table_schema->get_table_id());
            }
          }
        }
      } // OB_ENTRY_NOT_EXIST
    }
  }

  return ret;
}

int ObLogMetaManager::get_table_meta(
    const uint64_t tenant_id,
    const int64_t global_schema_version,
    const datadict::ObDictTableMeta *simple_table_schema,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  TableMetaInfo *meta_info = NULL;

  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(simple_table_schema));
  } else {
    MetaKey key(tenant_id, simple_table_schema->get_table_id());

    if (OB_FAIL((get_meta_info_<TableMetaMap, TableMetaInfo>(tb_meta_map_, key, meta_info)))) {
      LOG_ERROR("get table meta info fail", KR(ret), K(key));
    } else {
      int64_t version = simple_table_schema->get_schema_version();
      ret = get_meta_from_meta_info_<TableMetaInfo, ITableMeta>(meta_info, version, table_meta);

      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get_meta_from_meta_info_ fail", KR(ret), K(version));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // refresh ObDictTableMeta when build meta for the first time
        const int64_t table_id = simple_table_schema->get_table_id();
        datadict::ObDictTableMeta *table_schema = NULL;
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;

        if (OB_FAIL(get_dict_tenant_info_(tenant_id, dict_tenant_info_guard, tenant_info))) {
          LOG_ERROR("get_dict_tenant_info_ failed", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_info->get_table_meta(table_id, table_schema))) {
          LOG_ERROR("get_table_meta from tenant_dict_info failed", KR(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          // tenant has been dropped
          ret = OB_TENANT_HAS_BEEN_DROPPED;
          LOG_WARN("table_schema is null, tenant may be dropped", KR(ret), K(table_schema),
              "tenant_id", simple_table_schema->get_tenant_id(),
              K(global_schema_version),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(), KPC(simple_table_schema));
        } else if (OB_FAIL(add_and_get_table_meta_(meta_info, table_schema, table_schema->get_column_id_arr_order_by_table_define(),
            *tenant_info, table_meta, stop_flag))) {
          // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("add_and_get_table_meta_ fail", KR(ret), K(tenant_id),
                K(global_schema_version), K(meta_info),
                "table_name", table_schema->get_table_name(),
                "table_id", table_schema->get_table_id());
          }
        }
      }
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
      DRCMessageFactory::destroy(table_meta);
      table_meta = NULL;
    }
  }
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
int ObLogMetaManager::get_db_meta(
    const uint64_t tenant_id,
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
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(db_schema_info));
  } else {
    MetaKey key(tenant_id, db_id);

    if (OB_FAIL((get_meta_info_<DBMetaMap, DBMetaInfo>(db_meta_map_, key, meta_info)))) {
      LOG_ERROR("get database meta info fail", KR(ret), K(key));
    } else {
      ret = get_meta_from_meta_info_<DBMetaInfo, IDBMeta>(meta_info, db_schema_version, db_meta);

      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get_meta_from_meta_info_ fail", KR(ret), K(db_schema_version));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;

        // get db name and tenant name when first build db meta
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

int ObLogMetaManager::get_db_meta(
    const uint64_t tenant_id,
    const DBSchemaInfo &db_schema_info,
    IDBMeta *&db_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  DBMetaInfo *meta_info = NULL;
  const int64_t db_schema_version = db_schema_info.version_;
  uint64_t db_id = db_schema_info.db_id_;

  if (OB_UNLIKELY(! db_schema_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(db_schema_info));
  } else {
    MetaKey key(tenant_id, db_id);

    if (OB_FAIL((get_meta_info_<DBMetaMap, DBMetaInfo>(db_meta_map_, key, meta_info)))) {
      LOG_ERROR("get database meta info fail", KR(ret), K(key));
    } else {
      ret = get_meta_from_meta_info_<DBMetaInfo, IDBMeta>(meta_info, db_schema_version, db_meta);

      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get_meta_from_meta_info_ fail", KR(ret), K(db_schema_version));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;
        // get db name and tenant name when first build db meta
        TenantSchemaInfo tenant_schema_info;

        if (OB_FAIL(get_dict_tenant_info_(tenant_id, dict_tenant_info_guard, tenant_info))) {
          LOG_ERROR("get_dict_tenant_info_ failed", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_info->get_tenant_schema_info(tenant_schema_info))) {
          LOG_ERROR("get_tenant_schema_info from dict_tenant_meta failed", KR(ret), K(tenant_info));
        } else if (OB_FAIL(add_and_get_db_meta_(
            meta_info,
            db_schema_info,
            tenant_schema_info,
            db_meta))) {
          LOG_ERROR("add_and_get_db_meta_ fail", KR(ret), KP(meta_info), K(db_schema_info),
              K(tenant_schema_info));
        }
      } else { /* get_meta_from_meta_info_ return OB_SUCCESS */ }
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
      DRCMessageFactory::destroy(db_meta);
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

int ObLogMetaManager::get_dict_tenant_info_(
    const uint64_t tenant_id,
    ObDictTenantInfoGuard &dict_tenant_info_guard,
    ObDictTenantInfo *&tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info = nullptr;

  if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
      tenant_id,
      dict_tenant_info_guard))) {
    LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
  }

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
          LOG_TRACE("insert meta_info into meta_map succ", K(key), K(meta_info));
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
            } else {
              LOG_TRACE("get meta_info from meta_map succ", K(key), K(meta_info));
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
      LOG_TRACE("get meta_info from meta_map succ", K(key), K(meta_info));
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
template<class SCHEMA_GUARD, class TABLE_SCHEMA>
int ObLogMetaManager::add_and_get_table_meta_(
    TableMetaInfo *meta_info,
    const TABLE_SCHEMA *table_schema,
    const ObIArray<uint64_t> &usr_def_col_ids,
    SCHEMA_GUARD &schema_mgr,
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
      if (OB_FAIL(build_table_meta_(table_schema, usr_def_col_ids, schema_mgr, table_meta, stop_flag))) {
        // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("build_table_meta_ fail", K(version), K(meta_info), KR(ret), KP(table_schema));
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

int ObLogMetaManager::add_and_get_db_meta_(
    DBMetaInfo *meta_info,
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
template<class SCHEMA_GUARD, class TABLE_SCHEMA>
int ObLogMetaManager::build_table_meta_(
    const TABLE_SCHEMA *table_schema,
    const ObIArray<uint64_t> &usr_def_col_ids,
    SCHEMA_GUARD &schema_mgr,
    ITableMeta *&table_meta,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ITableMeta *tmp_table_meta = DRCMessageFactory::createTableMeta();
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
    } else if (OB_FAIL(build_column_metas_(tmp_table_meta, table_schema, usr_def_col_ids, *tb_schema_info,
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
      if (OB_NOT_NULL(tb_schema_info)) {
        if (OB_TMP_FAIL(try_erase_table_schema_(
            table_schema->get_tenant_id(),
            table_schema->get_table_id(),
            table_schema->get_schema_version()))) {
          LOG_ERROR("try_erase_table_schema_ failed", KR(tmp_ret), K(tb_schema_info), KPC(table_schema));
        } else if (OB_TMP_FAIL(free_table_schema_info_(tb_schema_info))) {
          LOG_ERROR("free_table_schema_info_ fail", KR(tmp_ret), K(tb_schema_info), KPC(table_schema));
        }
      }
    }
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogMetaManager::build_column_idx_mappings_(
    const TABLE_SCHEMA *table_schema,
    const ObIArray<uint64_t> &usr_def_col_ids,
    const common::ObIArray<share::schema::ObColDesc> &column_ids,
    ObIArray<int16_t> &store_idx_to_usr_idx,
    int16_t &usr_column_cnt,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t stored_column_cnt = column_ids.count();
  const int64_t column_id_cnt = usr_def_col_ids.count();
  int16_t usr_column_idx = 0;
  // but here is a implicit requirement, which requires usr_def_col_ids is a superset of column_ids
  ObLinearHashMap<share::schema::ObColumnIdKey, int16_t> col_id_to_usr_col_idx;
  store_idx_to_usr_idx.reset();
  if (OB_FAIL(store_idx_to_usr_idx.prepare_allocate(stored_column_cnt))) {
    LOG_ERROR("prepare_allocate for store_idx_to_usr_idx failed", K(stored_column_cnt), K(column_ids));
  } else if (OB_FAIL(col_id_to_usr_col_idx.init("ColIdToUsrColId"))) {
    LOG_ERROR("col_id_to_usr_col_idx init failed");
  }

  for (int16_t column_stored_idx = 0; OB_SUCC(ret) && column_stored_idx < stored_column_cnt
          && ! stop_flag; column_stored_idx++) {
    const share::schema::ObColDesc &col_desc = column_ids.at(column_stored_idx);
    const uint64_t column_id = col_desc.col_id_;
    const auto *column_table_schema = table_schema->get_column_schema(column_id);
    bool is_usr_column = false;
    bool is_heap_table_pk_increment_column = false;

    if (OB_UNLIKELY(OB_HIDDEN_PK_INCREMENT_COLUMN_ID < column_id && OB_APP_MIN_COLUMN_ID > column_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column id can't handle currently", KR(ret), K(column_id), KPC(table_schema));
    } else if (OB_ISNULL(column_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get_column_schema_by_id_internal fail", KR(ret), K(column_id),
          KPC(table_schema), KPC(column_table_schema));
    } else if (OB_FAIL(col_id_to_usr_col_idx.insert(share::schema::ObColumnIdKey(column_id),
        column_stored_idx))) {
      LOG_ERROR("set key and value for the mapping from column_id to usr_column_idx failed",
          K(column_id), K(column_stored_idx), K(stored_column_cnt), "map_size", col_id_to_usr_col_idx.count());
    } else if (OB_FAIL(check_column_(*table_schema,
        *column_table_schema,
        is_usr_column,
        is_heap_table_pk_increment_column))) {
      LOG_ERROR("filter_column_ fail", KR(ret), K(is_usr_column),
          K(is_heap_table_pk_increment_column),
          "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(),
          "column", column_table_schema->get_column_name(),
          "column_id", column_table_schema->get_column_id());
    } else if (is_usr_column) {
      store_idx_to_usr_idx.at(column_stored_idx) = usr_column_idx;
      usr_column_idx++;
    } else {
      store_idx_to_usr_idx.at(column_stored_idx) = -1;
      LOG_INFO("ignore column which is not usr column", K(is_usr_column), K(column_id),
          "column_name", column_table_schema->get_column_name(),
          "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id());
    }
  }

  if (1 == TCONF.enable_output_by_table_def) {
    int16_t usr_def_col_idx = 0;

    for (int16_t column_idx = 0; OB_SUCC(ret) && column_idx < column_id_cnt && ! stop_flag; column_idx++) {
      const uint64_t column_id = usr_def_col_ids.at(column_idx);
      int16_t column_stored_idx = -1;
      if (OB_FAIL(col_id_to_usr_col_idx.get(share::schema::ObColumnIdKey(column_id), column_stored_idx))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get from col_id_to_usr_idx failed", K(column_id), "map_size", col_id_to_usr_col_idx.count());
        } else {
          // virtual column is not stored, just ignore
          ret = OB_SUCCESS;
          LOG_INFO("ignore column which is not stored", K(column_id), "table_name", table_schema->get_table_name(),
              "table_id", table_schema->get_table_id());
        }
      } else if (-1 != store_idx_to_usr_idx.at(column_stored_idx)) {
        LOG_INFO("rewrite store_idx_to_usr_idx", K(column_stored_idx), K(usr_def_col_idx));
        store_idx_to_usr_idx.at(column_stored_idx) = usr_def_col_idx;
        usr_def_col_idx++;
      } else {
        // ignore non-user column
        LOG_INFO("ignore column which is not usr column when output_by_table_def is enabled", K(column_id),
            "table_name", table_schema->get_table_name(),
            "table_id", table_schema->get_table_id());
      }
    }

    if (usr_def_col_idx != usr_column_idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("usr_def_col_idx is not equal to usr_column_idx, unexpected", K(usr_def_col_idx),
          K(usr_column_idx), K(store_idx_to_usr_idx));
    }
  }

  if (OB_SUCC(ret)) {
    usr_column_cnt = usr_column_idx;
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
template<class SCHEMA_GUARD, class TABLE_SCHEMA>
int ObLogMetaManager::build_column_metas_(
    ITableMeta *table_meta,
    const TABLE_SCHEMA *table_schema,
    const ObIArray<uint64_t> &usr_def_col_ids,
    TableSchemaInfo &tb_schema_info,
    SCHEMA_GUARD &schema_mgr,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::schema::ObColDesc> column_ids;
  const bool ignore_virtual_column = true;
  const uint64_t tenant_id = table_schema->get_tenant_id();
  IObCDCTimeZoneInfoGetter *tz_info_getter = TCTX.timezone_info_getter_;
  ObTimeZoneInfoWrap *tz_info_wrap = nullptr;
  ObCDCTenantTimeZoneInfo *obcdc_tenant_tz_info = nullptr;

  if (OB_ISNULL(table_meta) || OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_meta), K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids, ignore_virtual_column))) {
    LOG_ERROR("get column_ids from table_schema failed", KR(ret), KPC(table_schema));
  } else if (OB_ISNULL(tz_info_getter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tz_info_getter is nullptr", KR(ret), K(tz_info_getter));
  } else if (OB_FAIL(tz_info_getter->get_tenant_tz_info(tenant_id, obcdc_tenant_tz_info))) {
    LOG_ERROR("get_tenant_tz_info failed", KR(ret), K(tenant_id));
  } else {
    const ObTimeZoneInfoWrap *tz_info_wrap = &(obcdc_tenant_tz_info->get_tz_wrap());
    int64_t version = table_schema->get_schema_version();
    uint64_t table_id = table_schema->get_table_id();
    const bool is_heap_table = table_schema->is_heap_table();
    const int64_t column_cnt = column_ids.count();
    int16_t usr_column_cnt = 0;

    ObSEArray<int16_t, 16> column_stored_idx_to_usr_idx;
    // the to_string method of ObSEArray requires the to_string method of IColMeta
    // so we use void* to bypass the requirement
    ObSEArray<void*, 16> col_metas;

    // build Meta for each column
    // iter all column:
    // 1. all column should build column_schema and set into table_schema
    // 2. filter user_column, the user_column:
    // 2.1. should build column_meta(used for logmsg) and append to table_meta.
    // 2.2. inc usr_column_idx and recorded into column_schema, which will used to decide format
    //      idata into br or not.

    if (OB_FAIL(build_column_idx_mappings_(table_schema, usr_def_col_ids, column_ids,
        column_stored_idx_to_usr_idx, usr_column_cnt, stop_flag))) {
      LOG_ERROR("build column idx mapping failed", K(table_schema), K(usr_def_col_ids), K(column_ids));
    } else if (OB_FAIL(col_metas.prepare_allocate(usr_column_cnt))) {
      LOG_ERROR("reserve slots for col_metas failed", K(usr_column_cnt));
    } else {
      LOG_DEBUG("finish build column idx map", K(usr_column_cnt), K(column_stored_idx_to_usr_idx));
    }

    for (int16_t column_stored_idx = 0; OB_SUCC(ret) && column_stored_idx < column_cnt && ! stop_flag; column_stored_idx ++) {
      IColMeta *col_meta = NULL;
      const share::schema::ObColDesc &col_desc = column_ids.at(column_stored_idx);
      const uint64_t column_id = col_desc.col_id_;
      const int16_t usr_column_idx = column_stored_idx_to_usr_idx.at(column_stored_idx);
      const bool is_usr_column = (-1 != usr_column_idx);
      const auto *column_table_schema = table_schema->get_column_schema(column_id);

      if (! is_usr_column) {
        // do nothing, otherwise will try generate column_meta.
      } else if (OB_ISNULL(col_meta = DRCMessageFactory::createColMeta())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("createColMeta fails", KR(ret), "col_name", column_table_schema->get_column_name());
      } else if (OB_FAIL(set_column_meta_(col_meta, *column_table_schema, *table_schema))) {
        LOG_ERROR("set_column_meta_ fail", KR(ret), KP(col_meta));
      } else {
        // success
        col_metas.at(usr_column_idx) = col_meta;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_column_schema_info_(
            *table_schema,
            *column_table_schema,
            column_stored_idx,
            is_usr_column,
            usr_column_idx,
            tb_schema_info,
            tz_info_wrap))) {
          LOG_ERROR("set_column_schema_info_ fail", KR(ret), KPC(table_schema), K(tb_schema_info),
              K(column_stored_idx), K(usr_column_idx), KPC(column_table_schema));
        }
      }

    } // while

    for (int64_t idx = 0, col_meta_cnt = col_metas.count(); OB_SUCC(ret) && idx < col_meta_cnt && ! stop_flag; idx++) {
      IColMeta *col_meta = static_cast<IColMeta*>(col_metas.at(idx));
      int append_ret = 2;
      const char *column_name = col_meta->getName();
      if (0 != (append_ret = table_meta->append(column_name, col_meta))) {
        // DRCMessage doesn't support append IColMeta to ITableMeta with
        // same column name but different capitalization temporarily.
        // In Oracle mode, there may exist such columns with the same name but different capitalization.
        DRCMessageFactory::destroy(col_meta);
        LOG_ERROR("append col_meta to table_meta fail, there may exist such "
            "columns with the same name but different capitalization", K(append_ret),
            "table_name", table_schema->get_table_name(),
            "column_name", column_name);
      } else {
        // succ
        LOG_DEBUG("append IColMeta to ITableMeta succ", K(idx), K(col_meta_cnt), K(column_name));
      }
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_table_schema_(
            version,
            table_schema->get_tenant_id(),
            table_id,
            table_schema->get_table_name(),
            usr_column_cnt,
            tb_schema_info))) {
        LOG_ERROR("set_table_schema_ fail", KR(ret), K(version),
            "tenant_id", table_schema->get_tenant_id(),
            K(table_id),
            "table_name", table_schema->get_table_name(),
            "table_column_cnt", column_cnt, K(usr_column_cnt), K(tb_schema_info));
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

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ObLogMetaManager::check_column_(
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_schema,
    bool &is_user_column,
    bool &is_heap_table_pk_increment_column)
{
  int ret = OB_SUCCESS;
  // won't filter by default
  is_user_column = true;
  const uint64_t column_id = column_schema.get_column_id();
  const uint64_t udt_set_id = column_schema.get_udt_set_id();
  const uint64_t sub_data_type = column_schema.get_sub_data_type();
  const bool is_heap_table = table_schema.is_heap_table();
  const bool is_rowkey_column = column_schema.is_rowkey_column();
  const bool is_invisible_column = column_schema.is_invisible_column();
  const bool is_hidden_column = column_schema.is_hidden();
  const bool enable_output_hidden_primary_key = (0 != TCONF.enable_output_hidden_primary_key);
  const bool enable_output_invisible_column = (0 != TCONF.enable_output_invisible_column);
  // is column not user_column in OB.
  bool is_non_ob_user_column = (column_id < OB_APP_MIN_COLUMN_ID) || (column_id >= OB_MIN_SHADOW_COLUMN_ID);
  is_heap_table_pk_increment_column = is_heap_table  && (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id);

  if (is_rowkey_column) {
    // user specified rowkey column should output;
    // invisible rowkey column should output;
    // hidden_pk of heap_table will output if user config enable_output_hidden_primary_key=1
    if (is_non_ob_user_column) {
      is_user_column = is_heap_table_pk_increment_column && enable_output_hidden_primary_key;
    }
  } else if (is_hidden_column) {
    is_user_column = false;
  } else if (column_schema.is_invisible_column() && !enable_output_invisible_column){
    is_user_column = false;
  }

  META_STAT_INFO("check_column_",
      "table_name", table_schema.get_table_name(),
      "table_id", table_schema.get_table_id(),
      K(is_heap_table),
      K(column_id),
      K(udt_set_id),
      K(sub_data_type),
      "column_name", column_schema.get_column_name(),
      K(is_user_column),
      K(is_rowkey_column),
      K(is_heap_table_pk_increment_column),
      K(enable_output_hidden_primary_key),
      "is_invisible_column", column_schema.is_invisible_column(),
      K(enable_output_invisible_column),
      K(is_hidden_column));

  return ret;
}

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ObLogMetaManager::set_column_meta_(
    IColMeta *col_meta,
    const COLUMN_SCHEMA &column_schema,
    const TABLE_SCHEMA &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(col_meta)) {
    LOG_ERROR("invalid argument", K(col_meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint16_t type_flag = 0;
    ObScale decimals = 0; // FIXME: does libobcdc need this?
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

        //mysql treat it as MYSQL_TYPE_STRING, it is not suitable for libobcdc
        if (ObEnumType == col_type) {
          mysql_type = obmysql::MYSQL_TYPE_ENUM;
        } else if (ObSetType == col_type) {
          mysql_type = obmysql::MYSQL_TYPE_SET;
        }
      } else if (ObNumberType == col_type || ObUNumberType == col_type) {
        col_meta->setScale(column_schema.get_data_scale());
        col_meta->setPrecision(column_schema.get_data_precision());
      } else if (column_schema.is_xmltype()) {
        mysql_type = obmysql::MYSQL_TYPE_ORA_XML;
      } else if (ObRoaringBitmapType == col_type) {
        mysql_type = obmysql::MYSQL_TYPE_ROARINGBITMAP;
      }

      col_meta->setScale(column_schema.get_data_scale());
      col_meta->setPrecision(column_schema.get_data_precision());

      bool signed_flag = ((type_flag & UNSIGNED_FLAG) == 0);

      // for types with valid length(string\enumset\rowid\json\raw\lob\geo),
      // get_data_length returns the valid length, returns 0 for other types.
      col_meta->setLength(column_schema.get_data_length());

      if (column_schema.is_tbl_part_key_column()) {
        col_meta->setPartitioned(true);
      }
      if (column_schema.has_generated_column_deps()) {
        col_meta->setDependent(true);
      }
      col_meta->setName(column_schema.get_column_name());
      col_meta->setSigned(signed_flag);
      col_meta->setIsPK(column_schema.is_original_rowkey_column());
      col_meta->setNotNull(! column_schema.is_nullable());

      set_column_type_(*col_meta, mysql_type);
      set_column_encoding_(col_type, column_schema.get_charset_type(), col_meta);

      if (column_schema.is_xmltype()) {
        SET_ENCODING(col_meta, CS_TYPE_UTF8MB4_GENERAL_CI);
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
          "isGeneratedColumn", col_meta->isGenerated(),
          "isPartitionColumn", col_meta->isPartitioned(),
          "isGenerateDepColumn", col_meta->isDependent(),
          "columnFlag", column_schema.get_column_flags());

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

// convert column type for drcmsg and oblogmsg
void ObLogMetaManager::set_column_type_(IColMeta &col_meta, const obmysql::EMySQLFieldType &col_type)
{
  if (EMySQLFieldType::MYSQL_TYPE_ORA_BINARY_FLOAT == col_type) {
    col_meta.setType(drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_FLOAT);
  } else if (EMySQLFieldType::MYSQL_TYPE_ORA_BINARY_DOUBLE == col_type) {
    col_meta.setType(drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_DOUBLE);
  } else if (EMySQLFieldType::MYSQL_TYPE_ORA_XML == col_type) {
    col_meta.setType(drcmsg_field_types::DRCMSG_TYPE_ORA_XML);
  } else {
    col_meta.setType(static_cast<int>(col_type));
  }
}

template<class TABLE_SCHEMA>
int ObLogMetaManager::set_primary_keys_(ITableMeta *table_meta,
    const TABLE_SCHEMA *table_schema,
    const TableSchemaInfo &tb_schema_info)
{
  int ret = OB_SUCCESS;
  int64_t valid_pk_num = 0;
  ObLogAdaptString pks(ObModIds::OB_LOG_TEMP_MEMORY);
  ObLogAdaptString pk_info(ObModIds::OB_LOG_TEMP_MEMORY);

  if (OB_ISNULL(table_meta) || OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_meta), K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (! table_schema->is_heap_table()) {
      const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();

      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_num; i++) {
        int64_t column_index = -1;
        ColumnSchemaInfo *column_schema_info = NULL;

        if (OB_FAIL(tb_schema_info.get_column_schema_info_for_rowkey(i, column_schema_info))) {
          LOG_ERROR("get_column_schema_info", KR(ret), "table_id", table_schema->get_table_id(),
              "table_name", table_schema->get_table_name(),
              K_(enable_output_hidden_primary_key), K(column_schema_info));
        } else if (! column_schema_info->is_rowkey()) { // not rowkey
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("not a heap table and have no-rowkey in TableSchema::rowley_info_", K(ret),
              K(column_schema_info));
        } else if (OB_FAIL(fill_primary_key_info_(
            *table_schema,
            *column_schema_info,
            pks,
            pk_info,
            valid_pk_num))) {
          LOG_ERROR("fill_primary_key_info_ failed", KR(ret), K(pks), K(pk_info), K(valid_pk_num), K(column_schema_info));
        }
      } // for
    } else {
      ObArray<uint64_t> logic_pks;
      auto fn = [](uint64_t &a, uint64_t &b) { return a < b; };

      if (OB_FAIL(get_logic_primary_keys_for_heap_table_(*table_schema, logic_pks))) {
        LOG_ERROR("get_logic_primary_keys_for_heap_table_ failed", KR(ret), KPC(table_schema), K(logic_pks));
      } else if (OB_FAIL(sort_and_unique_array(logic_pks, fn))) {
        LOG_ERROR("sort and unique logic_pks failed", KR(ret), K(logic_pks), K(table_schema));
      } else {
        ARRAY_FOREACH_N(logic_pks, column_id_idx, pk_count) {
          const uint64_t column_id = logic_pks.at(column_id_idx);
          ColumnSchemaInfo *column_schema_info = NULL;

          if (OB_UNLIKELY(OB_HIDDEN_PK_INCREMENT_COLUMN_ID < column_id && OB_APP_MIN_COLUMN_ID > column_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid column_id", KR(ret), K(column_id_idx), K(column_id), K(logic_pks), KPC(table_schema));
          } else if (OB_FAIL(tb_schema_info.get_column_schema_info_of_column_id(column_id, column_schema_info))) {
            LOG_ERROR("get_column_schema_info_of_column_id failed", KR(ret), K(column_id), KPC(table_schema));
          } else if (OB_FAIL(fill_primary_key_info_(
              *table_schema,
              *column_schema_info,
              pks,
              pk_info,
              valid_pk_num))) {
            LOG_ERROR("fill_primary_key_info_ failed",
                KR(ret), K(pks), K(pk_info), K(valid_pk_num), K(column_id_idx), K(column_id), K(logic_pks), K(column_schema_info));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      const bool has_pk = (valid_pk_num > 0);
      table_meta->setHasPK((has_pk));

      // only set primary_key_info if primary key is exist
      if (has_pk) {
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
          // require cstr is valid
          else if (OB_ISNULL(pk_info_str) || OB_ISNULL(pks_str)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("pk_info_str or pks_str is invalid", KR(ret), K(pk_info_str), K(pks_str), K(pk_info),
                K(pks), K(valid_pk_num));
          } else {
            table_meta->setPkinfo(pk_info_str);
            table_meta->setPKs(pks_str);
          }
        }
      }

      META_STAT_INFO("set_primary_keys", KR(ret), "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(),
          "has_pk", table_meta->hasPK(), "pk_info", table_meta->getPkinfo(),
          "pks", table_meta->getPKs());
    }
  }
  return ret;
}

template<class TABLE_SCHEMA>
int ObLogMetaManager::get_logic_primary_keys_for_heap_table_(
    const TABLE_SCHEMA &table_schema,
    ObIArray<uint64_t> &pk_list)
{
  int ret = OB_SUCCESS;
  const bool enable_output_hidden_primary_key = (1 == TCONF.enable_output_hidden_primary_key);
  pk_list.reset();

  if (table_schema.is_heap_table() && enable_output_hidden_primary_key) {
    ObArray<ObColDesc> col_ids;
    if (OB_FAIL(table_schema.get_column_ids(col_ids))) {
      LOG_ERROR("get all column info failed", KR(ret), K(table_schema));
    } else {
      ARRAY_FOREACH_N(col_ids, col_idx, col_cnt) {
        bool chosen = false;
        const ObColDesc &col_desc = col_ids.at(col_idx);
        const uint64_t column_id = col_desc.col_id_;
        if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
          chosen = true;
        } else {
          auto *column_schema = table_schema.get_column_schema(column_id);

          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid column_schema", KR(ret), K(column_id), K(col_desc), K(table_schema));
          } else if (column_schema->is_tbl_part_key_column()) {
            // if not virtual: set as logic pk
            // otherwise set its dep columns as logic pk
            if (column_schema->is_virtual_generated_column()) {
              ObArray<uint64_t> deped_cols;
              if (OB_FAIL(column_schema->get_cascaded_column_ids(deped_cols))) {
                LOG_ERROR("get_cascaded_column_ids from column_schema failed", KR(ret), K(column_schema));
              } else {
                ARRAY_FOREACH_N(deped_cols, dep_col_idx, dep_col_cnt) {
                  const uint64_t deped_col_id = deped_cols.at(dep_col_idx);
                  if (OB_UNLIKELY(OB_HIDDEN_PK_INCREMENT_COLUMN_ID < deped_col_id && OB_APP_MIN_COLUMN_ID > deped_col_id)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("invalid deped column", KR(ret), K(column_id), K(deped_col_id));
                  } else if (OB_FAIL(pk_list.push_back(deped_col_id))) {
                    LOG_ERROR("push_back column_id into pk_list failed", KR(ret), K(column_id));
                  }
                }
              }
            } else {
              chosen = true;
            }
          }
        }

        if (OB_SUCC(ret) && chosen && OB_FAIL(pk_list.push_back(column_id))) {
          LOG_ERROR("push_back column_id into pk_list failed", KR(ret), K(column_id));
        }
      } // for
    }
    LOG_INFO("get_logic_primary_keys_for_heap_table_", KR(ret),
        "tenant_id", table_schema.get_tenant_id(),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        K(pk_list));
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogMetaManager::fill_primary_key_info_(
    const TABLE_SCHEMA &table_schema,
    const ColumnSchemaInfo &column_schema_info,
    ObLogAdaptString &pks,
    ObLogAdaptString &pk_info,
    int64_t &valid_pk_num)
{
  int ret = OB_SUCCESS;
  int64_t column_index = column_schema_info.get_usr_column_idx();
  const auto *column_table_schema = table_schema.get_column_schema(column_schema_info.get_column_id());

  if (OB_ISNULL(column_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_column_schema_by_id_internal fail", KR(ret), K(column_schema_info), K(table_schema), KPC(column_table_schema));
  } else if (OB_UNLIKELY(! column_schema_info.is_usr_column())) {
      // filter non user column
      META_STAT_INFO("ignore non user-required column for set_row_keys_",
          "tenant_id", table_schema.get_tenant_id(),
          "table_name", table_schema.get_table_name(),
          "table_id", table_schema.get_table_id(),
          K(column_schema_info));
  } else if (OB_UNLIKELY(column_index < 0 || column_index >= OB_MAX_COLUMN_NUMBER)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_index is invalid", KR(ret),
        K(column_index),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "column_id", column_schema_info.get_column_id(),
        "column_name", column_table_schema->get_column_name());
  } else if (valid_pk_num > 0 && OB_FAIL(pks.append(","))) {
    LOG_ERROR("append pks delimeter failed", KR(ret), K(valid_pk_num), K(pks));
  } else if (OB_FAIL(pks.append(column_table_schema->get_column_name()))) {
    LOG_ERROR("append column_name into pks failed", KR(ret), K(pks), KPC(column_table_schema));
  } else {
    if (OB_SUCC(ret)) {
      if (0 == valid_pk_num) {
        ret = pk_info.append("(");
      } else {
        ret = pk_info.append(",");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pk_info.append_int64(column_index))) {
      LOG_ERROR("append column_index into pk_info failed", KR(ret), K(column_index), K(pk_info), KPC(column_table_schema));
    } else {
      valid_pk_num++;
    }
  }

  return ret;
}

int ObLogMetaManager::set_unique_keys_from_unique_index_table_(
    const share::schema::ObTableSchema *table_schema,
    const share::schema::ObTableSchema *index_table_schema,
    const TableSchemaInfo &tb_schema_info,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info,
    int64_t &valid_uk_column_count)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)
      || OB_ISNULL(is_uk_column_array)
      || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(table_schema), K(is_uk_column_array), K(index_table_schema));
  } else if (OB_UNLIKELY(! index_table_schema->is_unique_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid index table schema which is not unique index", KR(ret),
        K(index_table_schema->is_unique_index()));
  } else {
    const ObIndexInfo &index_info = index_table_schema->get_index_info();
    const int64_t index_key_count = index_info.get_size();
    valid_uk_column_count = 0;

    for (int64_t index_info_idx = 0;
        OB_SUCC(ret) && index_info_idx < index_key_count;
        index_info_idx++) {
      if (OB_FAIL(build_unique_keys_with_index_column_(
          table_schema,
          index_table_schema,
          index_info,
          index_info_idx,
          tb_schema_info,
          is_uk_column_array,
          uk_info,
          valid_uk_column_count))) {
        LOG_ERROR("build_unique_keys_with_index_column_ failed", KR(ret), K(index_info_idx), K(index_info), K(table_schema), K(uk_info));
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

int ObLogMetaManager::set_unique_keys_from_unique_index_table_(
    const datadict::ObDictTableMeta *table_schema,
    const datadict::ObDictTableMeta *index_table_schema,
    const TableSchemaInfo &tb_schema_info,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info,
    int64_t &valid_uk_column_count)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_schema)
      || OB_ISNULL(is_uk_column_array)
      || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(table_schema), K(is_uk_column_array), K(index_table_schema));
  } else if (OB_UNLIKELY(! index_table_schema->is_unique_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid index table schema which is not unique index", KR(ret),
        K(index_table_schema->is_unique_index()));
  } else {
    valid_uk_column_count = 0;
    ObIndexInfo index_info;

    if (OB_FAIL(index_table_schema->get_index_info(index_info))) {
      LOG_ERROR("get_index_info failed", KR(ret), KPC(index_table_schema), K(table_schema));
    } else {
      const int64_t index_key_count = index_info.get_size();

      for (int64_t index_info_idx = 0;
          OB_SUCC(ret) && index_info_idx < index_key_count;
          index_info_idx++) {
        if (OB_FAIL(build_unique_keys_with_index_column_(
            table_schema,
            index_table_schema,
            index_info,
            index_info_idx,
            tb_schema_info,
            is_uk_column_array,
            uk_info,
            valid_uk_column_count))) {
          LOG_ERROR("build_unique_keys_with_index_column_ failed", KR(ret), K(index_info_idx), K(index_key_count),
              K(index_info), K(table_schema), K(uk_info));
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
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogMetaManager::build_unique_keys_with_index_column_(
    const TABLE_SCHEMA *table_schema,
    const TABLE_SCHEMA *index_table_schema,
    const common::ObIndexInfo &index_info,
    const int64_t index_column_idx,
    const TableSchemaInfo &tb_schema_info,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info,
    int64_t &valid_uk_column_count)
{
  int ret = OB_SUCCESS;
  uint64_t index_column_id = OB_INVALID_ID;

  if (OB_FAIL(index_info.get_column_id(index_column_idx, index_column_id))) {
    LOG_ERROR("get_column_id from index_info fail", KR(ret), K(index_column_idx), K(index_info),
        "index_table_name", index_table_schema->get_table_name(),
        "index_table_id", index_table_schema->get_table_id());
  } else {
    const auto *column_schema = table_schema->get_column_schema(index_column_id);

    if (OB_ISNULL(column_schema)) {
      if (is_shadow_column(index_column_id)) {
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
    } else if (column_schema->is_virtual_generated_column()) {
      LOG_WARN("ignore virtual generate column", "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(),
          "column_name", column_schema->get_column_name(), K(index_info));
    } else {
      int64_t user_column_index = -1;   // Column index as seen from the user's perspective
      ColumnSchemaInfo *column_schema_info = NULL;

      if (OB_FAIL(tb_schema_info.get_column_schema_info_of_column_id(index_column_id, column_schema_info))) {
        LOG_ERROR("get_column_schema_info", KR(ret), "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(),
            K(index_column_id), K(enable_output_hidden_primary_key_),
            K(column_schema_info));
      } else if (! column_schema_info->is_usr_column()) {
        // Filtering non-user columns
        META_STAT_INFO("ignore non user-required column",
            "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(),
            K(index_column_id),
            K(index_column_idx));
      } else {
        user_column_index = column_schema_info->get_usr_column_idx();

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
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
template<class SCHEMA_GUARD, class TABLE_SCHEMA>
int ObLogMetaManager::set_unique_keys_(ITableMeta *table_meta,
    const TABLE_SCHEMA *table_schema,
    const TableSchemaInfo &tb_schema_info,
    SCHEMA_GUARD &schema_mgr,
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
    int64_t column_count = tb_schema_info.get_usr_column_count();
    LOG_TRACE("set_unique_keys_ begin", KPC(table_schema), K(index_table_count), K(tb_schema_info));
    if (column_count < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column_num is invalid", "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(), K(column_count));
    } else if (0 == column_count) {
      LOG_INFO("ignore table without usr_column", "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(), K(column_count) );
    } else {
      if (index_table_count > 0) {
        int64_t is_uk_column_array_size = column_count * sizeof(bool);
        is_uk_column_array = static_cast<bool *>(ob_cdc_malloc(is_uk_column_array_size));

        if (OB_ISNULL(is_uk_column_array)) {
          LOG_ERROR("allocate memory for is_uk column array fail", K(column_count),
              K(is_uk_column_array_size));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          (void)memset(is_uk_column_array, 0, column_count * sizeof(bool));

          // Set unique index information from all index tables
          if (OB_FAIL(set_unique_keys_from_all_index_table_(
              *table_schema,
              tb_schema_info,
              schema_mgr,
              stop_flag,
              is_uk_column_array,
              uk_info,
              valid_uk_table_count))) {
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
                ColumnSchemaInfo *column_schema_info = NULL;
                uint64_t column_id = OB_INVALID_ID;

                if (OB_FAIL(tb_schema_info.get_column_schema_info(index, false/*is_column_stored_idx*/, column_schema_info))) {
                  LOG_ERROR("tb_schema_info get_column_id fail", KR(ret), K(version),
                      "table_id", table_schema->get_table_id(),
                      "column_idx", index, K(column_id));
                } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = column_schema_info->get_column_id()))) {
                  LOG_ERROR("column_id is not valid", K(column_id));
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  const auto *column_schema = table_schema->get_column_schema(column_id);

                  if (OB_ISNULL(column_schema)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("get column schema fail", K(column_id), K(index), K(column_count),
                        "table_id", table_schema->get_table_id(),
                        "table_name", table_schema->get_table_name(),
                        "table_schame_version", table_schema->get_schema_version());
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
      ob_cdc_free(is_uk_column_array);
      is_uk_column_array = NULL;
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// #retval other error code             fail
template<class SCHEMA_GUARD, class TABLE_SCHEMA>
int ObLogMetaManager::set_unique_keys_from_all_index_table_(
    const TABLE_SCHEMA &table_schema,
    const TableSchemaInfo &tb_schema_info,
    SCHEMA_GUARD &schema_mgr,
    volatile bool &stop_flag,
    bool *is_uk_column_array,
    ObLogAdaptString &uk_info,
    int64_t &valid_uk_table_count)
{
  int ret = OB_SUCCESS;
  const bool is_using_online_schema = true; // TODO use function instead.
  const uint64_t tenant_id = table_schema.get_tenant_id();
  int64_t index_table_count = table_schema.get_index_tid_count();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_ISNULL(is_uk_column_array)) {
    LOG_ERROR("invalid argument", K(is_uk_column_array));
    ret = OB_INVALID_ARGUMENT;
  } else if (index_table_count <= 0) {
    // no index table
  }
  // get array of index table id
  else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_ERROR("get_index_tid_array fail", KR(ret), "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id());
  } else {
    LOG_DEBUG("set_unique_keys_from_all_index_table_ begin",
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        K(simple_index_infos));

    // Iterate through all index tables to find the unique index table
    for (int64_t index = 0; OB_SUCC(ret) && index < index_table_count; index++) {
      const TABLE_SCHEMA *index_table_schema = NULL;

      // retry to fetch schma until success of quit
      // caller deal with error code OB_TENANT_HAS_BEEN_DROPPED
      RETRY_FUNC(stop_flag, schema_mgr, get_table_schema, tenant_id, simple_index_infos.at(index).table_id_,
          index_table_schema, GET_SCHEMA_TIMEOUT);

      if (OB_FAIL(ret)) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("get index table schema fail", KR(ret), K(simple_index_infos.at(index).table_id_));
        }
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get index table schema fail", KR(ret), "table_id", table_schema.get_table_id(),
            "table_name", table_schema.get_table_name(),
            "index_table_id", simple_index_infos.at(index).table_id_, K(index_table_count), K(index));
      }
      // Handling uniquely indexed tables
      else if (index_table_schema->is_unique_index()) {
        SET_UK_INFO;
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

  IDBMeta *tmp_db_meta = DRCMessageFactory::createDBMeta();

  if (OB_ISNULL(tmp_db_meta)) {
    LOG_ERROR("createDBMeta fail, return NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    // set DB Name to：TENANT.DATABASE
    std::string db_name_str = tenant_schema_info.name_;
    db_name_str.append(".");
    db_name_str.append(db_schema_info.name_);

    tmp_db_meta->setName(db_name_str.c_str());
    tmp_db_meta->setEncoding(DEFAULT_ENCODING);
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
    ITableMeta *tmp_table_meta = DRCMessageFactory::createTableMeta();
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
        DRCMessageFactory::destroy(tmp_table_meta);
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

  if (OB_ISNULL(col_meta = DRCMessageFactory::createColMeta())) {
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
      DRCMessageFactory::destroy(col_meta);
      col_meta = NULL;
    }
  }

  return ret;
}

void ObLogMetaManager::destroy_ddl_meta_()
{
  if (NULL != ddl_table_meta_) {
    // destroy all column meta of table meta when destroy table meta
    DRCMessageFactory::destroy(ddl_table_meta_);
    ddl_table_meta_ = NULL;
  }
}

int ObLogMetaManager::get_table_schema_meta(
    const int64_t version,
    const uint64_t tenant_id,
    const uint64_t table_id,
    TableSchemaInfo *&tb_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == version)
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_ERROR("invalid argument", K(version), K(tenant_id), K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MulVerTableKey table_key(version, tenant_id, table_id);

    if (OB_FAIL(tb_schema_info_map_.get(table_key, tb_schema_info))) {
      LOG_ERROR("tb_schema_info_map_ get fail", KR(ret), K(table_key), K(tb_schema_info));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogMetaManager::set_table_schema_(
    const int64_t version,
    const uint64_t tenant_id,
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
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_ISNULL(table_name)) {
    LOG_ERROR("invalid argument", K(version), K(tenant_id), K(table_id), K(table_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tb_schema_info.set_non_hidden_column_count(non_hidden_column_cnt);

    MulVerTableKey table_key(version, tenant_id, table_id);

    if (OB_FAIL(tb_schema_info_map_.insert(table_key, &tb_schema_info))) {
      LOG_ERROR("tb_schema_info_map_ insert fail", KR(ret), K(table_key), K(tb_schema_info));
    } else {
      LOG_INFO("set_table_schema succ", "schema_version", version, K(tenant_id),
          K(table_id), K(table_name), K(tb_schema_info));
    }
  }

  return ret;
}

int ObLogMetaManager::try_erase_table_schema_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("meta manager has not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_UNLIKELY(OB_INVALID_VERSION == version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tenant_id), K(table_id), K(version));
  } else {
    MulVerTableKey table_key(version, tenant_id, table_id);

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

template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
int ObLogMetaManager::set_column_schema_info_(
    const TABLE_SCHEMA &table_schema,
    const COLUMN_SCHEMA &column_table_schema,
    const int16_t column_stored_idx,
    const bool is_usr_column,
    const int16_t usr_column_idx,
    TableSchemaInfo &tb_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("meta manager has not inited");
    ret = OB_NOT_INIT;
  // For ObDatumRow format, we can not get column id.
  // So we need maintain __pk_increment column regardless of whether output hidden primary key.
  // init_column_schema_info(...) enable_output_hidden_primary_key_ is equal to true
  } else if (OB_FAIL(tb_schema_info.init_column_schema_info(
      table_schema,
      column_table_schema,
      column_stored_idx,
      is_usr_column,
      usr_column_idx,
      tz_info_wrap,
      *obj2str_helper_))) {
    LOG_ERROR("tb_schema_info init_column_schema_info fail", KR(ret),
        "table_id", table_schema.get_table_id(),
        "table_name", table_schema.get_table_name(),
        "version", table_schema.get_schema_version(),
        K(column_table_schema), K(column_stored_idx),
        K(enable_output_hidden_primary_key_));
  } else {
    // succ
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
