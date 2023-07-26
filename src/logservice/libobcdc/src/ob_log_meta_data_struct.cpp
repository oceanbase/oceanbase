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
#include "ob_log_meta_data_struct.h"
#include "ob_log_meta_data_service.h"
#include "ob_log_schema_getter.h"           // TenantSchemaInfo, DBSchemaInfo

namespace oceanbase
{
namespace libobcdc
{
ObDictTenantInfo::ObDictTenantInfo() :
    is_inited_(false),
    arena_allocator_(),
    cfifo_allocator_(),
    dict_tenant_meta_(&arena_allocator_),
    db_map_(),
    table_map_()
{
}

ObDictTenantInfo::~ObDictTenantInfo()
{
  destroy();
}

int ObDictTenantInfo::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObDictTenantInfo has been initialized", KR(ret));
  } else if (OB_FAIL(cfifo_allocator_.init(ALLOCATOR_TOTAL_LIMIT,
      ALLOCATOR_HOLD_LIMIT,
      ALLOCATOR_PAGE_SIZE))) {
    LOG_ERROR("init allocator fail", KR(ret));
  } else if (OB_FAIL(db_map_.init("DATADICTDB"))) {
    LOG_ERROR("db_map_ init fail", KR(ret));
  } else if (OB_FAIL(table_map_.init("DATADICTTB"))) {
    LOG_ERROR("table_map_ init fail", KR(ret));
  } else {
    cfifo_allocator_.set_label("DictTenantInfo");
    is_inited_ = true;

    LOG_INFO("ObDictTenantInfo init success");
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void ObDictTenantInfo::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    dict_tenant_meta_.reset();
    db_map_.destroy();
    table_map_.destroy();
    arena_allocator_.reset();
    cfifo_allocator_.destroy();
  }
}

int ObDictTenantInfo::replace_dict_tenant_meta(
    datadict::ObDictTenantMeta *new_dict_tenant_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    if (OB_FAIL(dict_tenant_meta_.incremental_data_update(*new_dict_tenant_meta))) {
      LOG_ERROR("dict_tenant_meta_ incremental_data_update failed", KR(ret), K(dict_tenant_meta_), KPC(new_dict_tenant_meta));
    }
  }

  return ret;
}

int ObDictTenantInfo::incremental_data_update(const share::ObLSAttr &ls_atrr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_FAIL(dict_tenant_meta_.incremental_data_update(ls_atrr))) {
    LOG_ERROR("dict_tenant_meta_ incremental_data_update failed", KR(ret), K(dict_tenant_meta_), K(ls_atrr));
  } else {}

  return ret;
}

int ObDictTenantInfo::alloc_dict_db_meta(datadict::ObDictDatabaseMeta *&dict_db_meta)
{
  int ret = OB_SUCCESS;
  dict_db_meta = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_ISNULL(dict_db_meta = static_cast<datadict::ObDictDatabaseMeta *>(
          cfifo_allocator_.alloc(sizeof(datadict::ObDictDatabaseMeta))))) {
    LOG_ERROR("allocate dict_db_meta failed", KR(ret), K(dict_db_meta));
  } else {
    new (dict_db_meta) datadict::ObDictDatabaseMeta(&arena_allocator_);
  }

  return ret;
}

int ObDictTenantInfo::free_dict_db_meta(datadict::ObDictDatabaseMeta *dict_db_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_ISNULL(dict_db_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dict_db_meta is nullptr", KR(ret));
  } else {
    cfifo_allocator_.free(dict_db_meta);
    dict_db_meta = nullptr;
  }

  return ret;
}

int ObDictTenantInfo::insert_dict_db_meta(datadict::ObDictDatabaseMeta *dict_db_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    const uint64_t tenant_id = get_tenant_id();
    const uint64_t db_id = dict_db_meta->get_database_id();
    MetaDataKey meta_data_key(db_id);

    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("expect valid tenant_id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(db_map_.insert(meta_data_key, dict_db_meta))) {
      LOG_ERROR("db_map_ insert failed", KR(ret), K(meta_data_key), KPC(dict_db_meta));
    } else {
      // NOTICE: tenant_id should set while load baseline dict.
      dict_db_meta->set_tenant_id(tenant_id);
    }
  }

  return ret;
}

int ObDictTenantInfo::replace_dict_db_meta(
    const datadict::ObDictDatabaseMeta &new_dict_db_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    const uint64_t db_id = new_dict_db_meta.get_database_id();
    MetaDataKey meta_data_key(db_id);
    datadict::ObDictDatabaseMeta *old_db_meta = nullptr;
    bool need_insert = false;

    if (OB_FAIL(get_db_meta(db_id, old_db_meta))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("tenant_info get_db_meta failed", KR(ret), K(db_id), K(old_db_meta));
      } else {
        // Does not exist locally, insert directly
        ret = OB_SUCCESS;
        need_insert = true;
      }
    } else {
      // Exist locally, replace it
      if (OB_FAIL(free_dict_db_meta(old_db_meta))) {
        LOG_ERROR("free_dict_db_meta failed", KR(ret));
      } else if (OB_FAIL(db_map_.erase(meta_data_key))) {
        LOG_ERROR("db_map_ erase failed", KR(ret), K(meta_data_key));
      } else {
        need_insert = true;
      }
    }

    if (OB_SUCC(ret) && need_insert) {
      datadict::ObDictDatabaseMeta *dict_db_meta = nullptr;

      if (OB_FAIL(alloc_dict_db_meta(dict_db_meta))) {
        LOG_ERROR("alloc_dict_db_meta failed", KR(ret), K(db_id), K(new_dict_db_meta));
      } else if (OB_FAIL(dict_db_meta->assign(new_dict_db_meta))) {
        LOG_ERROR("dict_db_meta assign failed", KR(ret), K(db_id), K(new_dict_db_meta));
      } else if (OB_FAIL(insert_dict_db_meta(dict_db_meta))) {
        LOG_ERROR("tenant_info insert_dict_db_meta failed", KR(ret), K(db_id), K(new_dict_db_meta));
      }
    }
  }

  return ret;
}

int ObDictTenantInfo::insert_dict_table_meta(datadict::ObDictTableMeta *dict_table_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    const uint64_t tenant_id = get_tenant_id();
    const uint64_t table_id = dict_table_meta->get_table_id();
    MetaDataKey meta_data_key(table_id);

    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("expect valid tenant_id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_map_.insert(meta_data_key, dict_table_meta))) {
      LOG_ERROR("table_map_ insert failed", KR(ret), K(meta_data_key), KPC(dict_table_meta));
    } else {
      // NOTICE: tenant_id should set while load baseline dict.
      dict_table_meta->set_tenant_id(tenant_id);
    }
  }

  return ret;
}

int ObDictTenantInfo::alloc_dict_table_meta(datadict::ObDictTableMeta *&dict_table_meta)
{
  int ret = OB_SUCCESS;
  dict_table_meta = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_ISNULL(dict_table_meta = static_cast<datadict::ObDictTableMeta *>(
          cfifo_allocator_.alloc(sizeof(datadict::ObDictTableMeta))))) {
    LOG_ERROR("allocate dict_table_meta failed", KR(ret), K(dict_table_meta));
  } else {
    new (dict_table_meta) datadict::ObDictTableMeta(&arena_allocator_);
  }

  return ret;
}

int ObDictTenantInfo::free_dict_table_meta(datadict::ObDictTableMeta *dict_table_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_ISNULL(dict_table_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dict_table_meta is nullptr", KR(ret));
  } else {
    dict_table_meta->reset();
    cfifo_allocator_.free(dict_table_meta);
    dict_table_meta = nullptr;
  }

  return ret;
}

int ObDictTenantInfo::replace_dict_table_meta(
    const datadict::ObDictTableMeta &new_dict_table_meta)
{
  int ret = OB_SUCCESS;
  int a = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    const uint64_t table_id = new_dict_table_meta.get_table_id();
    MetaDataKey meta_data_key(table_id);
    datadict::ObDictTableMeta *old_table_meta = nullptr;
    bool need_insert = false;

    if (OB_FAIL(get_table_meta(table_id, old_table_meta))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("tenant_info get_table_meta failed", KR(ret), K(table_id), K(old_table_meta));
      } else {
        // Does not exist locally, insert directly
        ret = OB_SUCCESS;
        need_insert = true;
        a = 1;
      }
    } else {
        a = 2;
      // Exist locally, replace it
      if (OB_FAIL(free_dict_table_meta(old_table_meta))) {
        LOG_ERROR("free_dict_table_meta failed", KR(ret));
      } else if (OB_FAIL(table_map_.erase(meta_data_key))) {
        LOG_ERROR("db_map_ erase failed", KR(ret), K(meta_data_key));
      } else {
        need_insert = true;
      }
    }

    if (OB_SUCC(ret) && need_insert) {
      datadict::ObDictTableMeta *dict_table_meta = nullptr;

      if (OB_FAIL(alloc_dict_table_meta(dict_table_meta))) {
        LOG_ERROR("alloc_dict_table_meta failed", KR(ret), K(table_id), K(new_dict_table_meta));
      } else if (OB_FAIL(dict_table_meta->assign(new_dict_table_meta))) {
        LOG_ERROR("dict_db_meta assign failed", KR(ret), K(table_id), K(new_dict_table_meta));
      } else if (OB_FAIL(insert_dict_table_meta(dict_table_meta))) {
        LOG_ERROR("tenant_info insert_dict_table_meta failed", KR(ret), K(table_id), K(new_dict_table_meta));
      }
    }
  }

  // TODO remove
  LOG_INFO("replace_dict_table_meta", KR(ret), K(new_dict_table_meta), K(a));

  return ret;
}

int ObDictTenantInfo::get_tenant_schema_info(TenantSchemaInfo &tenant_schema_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! dict_tenant_meta_.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid tenant_meta", KR(ret), K_(dict_tenant_meta));
  } else {
    tenant_schema_info.reset(
        dict_tenant_meta_.get_tenant_id(),
        dict_tenant_meta_.get_schema_version(),
        dict_tenant_meta_.get_tenant_name(),
        dict_tenant_meta_.is_restore());
  }

  return ret;
}


int ObDictTenantInfo::get_table_metas_in_tenant(
    common::ObIArray<const datadict::ObDictTableMeta *> &table_metas)
{
  int ret = OB_SUCCESS;
  TableMetasGetter table_metas_getter(table_metas);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_FAIL(table_map_.for_each(table_metas_getter))) {
    LOG_ERROR("table_map_ for_each failed", KR(ret));
  } else {}

  return ret;
}

int ObDictTenantInfo::get_table_meta(
    const uint64_t table_id,
    datadict::ObDictTableMeta *&table_meta)
{
  int ret = OB_SUCCESS;
  table_meta = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    MetaDataKey meta_data_key(table_id);

    if (OB_FAIL(table_map_.get(meta_data_key, table_meta))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("table_map_ get failed", KR(ret), K(meta_data_key));
      }
    } else if (OB_ISNULL(table_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table_meta is nullptr", KR(ret), K(meta_data_key), K(table_meta));
    }
  }

  return ret;
}

int ObDictTenantInfo::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const datadict::ObDictTableMeta *&table_schema,
    int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSEDx(timeout);
  datadict::ObDictTableMeta *table_meta = nullptr;

  if (OB_FAIL(get_table_meta(table_id, table_meta))) {
    LOG_ERROR("get_table_meta failed", KR(ret), K(tenant_id));
  } else {
    table_schema = table_meta;
  }

  return ret;
}

int ObDictTenantInfo::get_db_meta(
    const uint64_t db_id,
    datadict::ObDictDatabaseMeta *&db_meta)
{
  int ret = OB_SUCCESS;
  db_meta = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else {
    MetaDataKey meta_data_key(db_id);

    if (OB_FAIL(db_map_.get(meta_data_key, db_meta))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("db_map_ get failed", KR(ret), K(meta_data_key));
      }
    } else if (OB_ISNULL(db_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("db_meta is nullptr", KR(ret), K(meta_data_key), K(db_meta));
    }
  }

  return ret;
}
int ObDictTenantInfo::get_database_schema_info(
    const uint64_t db_id,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;
  datadict::ObDictDatabaseMeta *db_meta = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObDictTenantInfo has not been initialized", KR(ret));
  } else if (OB_FAIL(get_db_meta(db_id, db_meta))) {
    LOG_ERROR("get_db_meta failed", KR(ret), K(db_id));
  } else if (OB_UNLIKELY(! db_meta->is_valid())) {
    LOG_ERROR("db_meta is not valid", KR(ret), KPC(db_meta));
  } else {
    db_schema_info.reset(
        db_id,
        db_meta->get_schema_version(),
        db_meta->get_database_name());
  }

  return ret;
}

bool ObDictTenantInfo::TableMetasGetter::operator()(
    const MetaDataKey &key,
    datadict::ObDictTableMeta *value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid value", K(key), K(value));
  } else if (OB_FAIL(table_metas_.push_back(value))) {
    LOG_ERROR("table_metas_ push_back failed", KR(ret), K(key), K(value));
  } else {}

  return OB_SUCCESS == ret;
}

void ObDictTenantInfoGuard::revert_tenant()
{
  int revert_ret = GLOGMETADATASERVICE.get_baseline_loader().revert_tenant(tenant_info_);
  if (OB_SUCCESS != revert_ret) {
    LOG_ERROR_RET(revert_ret, "revert ObLogTenant fail", K(revert_ret), KPC(tenant_info_));
  } else {
    tenant_info_ = NULL;
  }
}

} // namespace libobcdc
} // namespace oceanbase
