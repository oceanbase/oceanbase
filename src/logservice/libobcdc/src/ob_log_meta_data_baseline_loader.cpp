// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX OBLOG

#include "ob_log_meta_data_baseline_loader.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[LOG_META_DATA] [LOADER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[LOG_META_DATA] [LOADER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

using namespace oceanbase::datadict;

namespace oceanbase
{
namespace libobcdc
{
ObLogMetaDataBaselineLoader::ObLogMetaDataBaselineLoader() :
    is_inited_(false),
    data_dict_tenant_map_()
{
}

ObLogMetaDataBaselineLoader::~ObLogMetaDataBaselineLoader()
{

}

int ObLogMetaDataBaselineLoader::init(
    const ObLogConfig &cfg)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(data_dict_tenant_map_.init("DDTENANT"))) {
    LOG_ERROR("data_dict_tenant_map_ init fail", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObLogMetaDataBaselineLoader init success");
  }

  return ret;
}

void ObLogMetaDataBaselineLoader::destroy()
{
  if (is_inited_) {
    LOG_INFO("ObLogMetaDataBaselineLoader destroy begin");

    data_dict_tenant_map_.destroy();
    is_inited_ = false;

    LOG_INFO("ObLogMetaDataBaselineLoader destroy finish");
  }
}

// ObLogTenantMgr
int ObLogMetaDataBaselineLoader::add_tenant(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataBaselineLoader is not initialized", KR(ret));
  } else {
    MetaDataKey meta_data_key(tenant_id);
    ObDictTenantInfo *dict_tenant_info = nullptr;

    if (OB_ENTRY_EXIST == (ret = data_dict_tenant_map_.contains_key(meta_data_key))) {
      LOG_ERROR("cannot add duplicated tenant", KR(ret), K(tenant_id));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("tenant hash map contains key failed", KR(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;

      // alloc a tenant struct
      if (OB_FAIL(data_dict_tenant_map_.alloc_value(dict_tenant_info))) {
        LOG_ERROR("alloc tenant failed", KR(ret), K(tenant_id), K(dict_tenant_info));
      } else if (OB_ISNULL(dict_tenant_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("dict_tenant_info is NULL", KR(ret), K(tenant_id), K(dict_tenant_info));
      } else if (OB_FAIL(dict_tenant_info->init())) {
        LOG_ERROR("dict_tenant_info init failed", KR(ret), K(tenant_id), KPC(dict_tenant_info));
      }
      // Ensure that a valid tenant is inserted and that the consumer will not see an invalid tenant
      else if (OB_FAIL(data_dict_tenant_map_.insert_and_get(meta_data_key, dict_tenant_info))) {
        LOG_ERROR("data_dict_tenant_map_ insert and get failed", KR(ret), K(tenant_id));
      } else {
        // make sure to revert here, otherwise there will be a memory/ref leak
        revert_tenant_(dict_tenant_info);
        // The tenant structure cannot be referenced again afterwards and may be deleted at any time
        dict_tenant_info = NULL;
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != dict_tenant_info) {
        (void)data_dict_tenant_map_.del(meta_data_key);
        data_dict_tenant_map_.free_value(dict_tenant_info);
        dict_tenant_info = NULL;
      }
    } else {
      ISTAT("Add tenant success", K(tenant_id));
    }
  }

  return ret;
}

int ObLogMetaDataBaselineLoader::read(
    const uint64_t tenant_id,
    datadict::ObDataDictIterator &data_dict_iterator,
    const char *buf,
    const int64_t buf_len,
    const int64_t pos_after_log_header,
    const palf::LSN &lsn,
    const int64_t submit_ts)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfo *dict_tenant_info = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataBaselineLoader is not initialized", KR(ret));
  } else if (OB_FAIL(get_tenant_(tenant_id, dict_tenant_info))) {
    LOG_ERROR("get_tenant_ failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(data_dict_iterator.append_log_buf(buf, buf_len, pos_after_log_header))) {
    LOG_ERROR("data_dict_iterator append_log failed", KR(ret), K(tenant_id));
  } else {
    bool is_done = false;
    datadict::ObDictMetaHeader meta_header;

    while (OB_SUCC(ret) && ! is_done) {
      meta_header.reset();

      if (OB_FAIL(data_dict_iterator.next_dict_header(meta_header))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("data_dict_iterator next_dict_heaer failed", KR(ret), K(tenant_id));
        }
      } else {
        const datadict::ObDictMetaType &meta_type = meta_header.get_dict_meta_type();

        switch (meta_type) {
          case datadict::ObDictMetaType::TABLE_META:
          {
            datadict::ObDictTableMeta *table_meta = nullptr;

            if (OB_FAIL(dict_tenant_info->alloc_dict_table_meta(table_meta))) {
              LOG_ERROR("alloc_dict_table_meta failed", K(ret), K(tenant_id));
            } else if (OB_FAIL(data_dict_iterator.next_dict_entry(meta_header, *table_meta))) {
              LOG_ERROR("data_dict_iterator next_dict_entry for table_meta failed", K(ret), K(tenant_id),
                  K(table_meta));
            } else if (OB_FAIL(dict_tenant_info->insert_dict_table_meta(table_meta))) {
              LOG_ERROR("dict_tenant_info insert_dict_table_meta failed", KR(ret), K(tenant_id), KPC(table_meta));
            } else {
              // TODO debug
              LOG_INFO("table_meta", K(tenant_id), KPC(table_meta));
            }
            break;
          }
          case datadict::ObDictMetaType::DATABASE_META:
          {
            datadict::ObDictDatabaseMeta *db_meta = nullptr;

            if (OB_FAIL(dict_tenant_info->alloc_dict_db_meta(db_meta))) {
              LOG_ERROR("alloc_dict_db_meta failed", K(ret), K(tenant_id));
            } else if (OB_FAIL(data_dict_iterator.next_dict_entry(meta_header, *db_meta))) {
              LOG_ERROR("data_dict_iterator next_dict_entry for db_meta failed", K(ret), K(tenant_id),
                  K(db_meta));
            } else if (OB_FAIL(dict_tenant_info->insert_dict_db_meta(db_meta))) {
              LOG_ERROR("dict_tenant_info insert_dict_db_meta failed", KR(ret), K(tenant_id), KPC(db_meta));
            } else {
              // TODO debug
              LOG_INFO("db_meta", K(tenant_id), KPC(db_meta));
            }
            break;
          }
          case datadict::ObDictMetaType::TENANT_META:
          {
            datadict::ObDictTenantMeta &tenant_meta = dict_tenant_info->get_dict_tenant_meta();

            if (OB_FAIL(data_dict_iterator.next_dict_entry(meta_header, tenant_meta))) {
              LOG_ERROR("data_dict_iterator next_dict_entry for tenant_meta failed", K(ret), K(tenant_id),
                  K(tenant_meta));
            } else {
              tenant_meta.set_tenant_id(tenant_id);
              // TODO debug
              LOG_INFO("tenant_meta", K(tenant_id), K(tenant_meta));
            }
            break;
          }
          default:
          {
            ret = OB_NOT_SUPPORTED;
            LOG_ERROR("not support type", KR(ret), K(tenant_id), KPC(dict_tenant_info),
                K(buf_len), K(pos_after_log_header), K(lsn), K(submit_ts));
            break;
          }
        }
      }

      if (OB_ITER_END == ret) {
        is_done = true;
        ret = OB_SUCCESS;
      }
    } // while

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = revert_tenant_(dict_tenant_info))) {
      LOG_ERROR("revert_tenant_ failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObLogMetaDataBaselineLoader::get_tenant_info_guard(
    const uint64_t tenant_id,
    ObDictTenantInfoGuard &guard)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfo *tenant_info = NULL;

  if (OB_FAIL(get_tenant_(tenant_id, tenant_info))) {
    // Failed, or non-existent
  } else {
    guard.set_tenant(tenant_info);
  }

  return ret;
}

int ObLogMetaDataBaselineLoader::get_tenant_(
    const uint64_t tenant_id,
    ObDictTenantInfo *&tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataBaselineLoader is not initialized", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tenant_id));
  } else {
    MetaDataKey meta_data_key(tenant_id);
    ObDictTenantInfo *tmp_tenant = NULL;

    if (OB_FAIL(data_dict_tenant_map_.get(meta_data_key, tmp_tenant))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("tenant_hash_map_ get fail", KR(ret), K(meta_data_key), K(tmp_tenant));
      }
    } else if (OB_ISNULL(tmp_tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is null", KR(ret), K(tenant_id), K(tmp_tenant));
    } else {
      // succeed
      tenant_info = tmp_tenant;
    }
  }

  return ret;
}

int ObLogMetaDataBaselineLoader::revert_tenant(ObDictTenantInfo *tenant)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataBaselineLoader is not initialized", KR(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tenant is NULL", KR(ret), K(tenant));
  } else {
    data_dict_tenant_map_.revert(tenant);
    tenant = nullptr;
  }

  return ret;
}

int ObLogMetaDataBaselineLoader::revert_tenant_(ObDictTenantInfo *tenant)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataBaselineLoader is not initialized", KR(ret));
  } else if (nullptr != tenant) {
    data_dict_tenant_map_.revert(tenant);
    tenant = nullptr;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
