/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS
#include "storage/fts/dict/ob_dic_loader.h"
#include "storage/fts/dict/ob_dic_lock.h"
namespace oceanbase
{
namespace storage
{
/**
* -----------------------------------ObTenantDicLoader-----------------------------------
*/
int ObTenantDicLoader::load_dictionary_in_trans(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the dic loader is not initialized", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dic_tables_info_.count(); ++i) {
      int64_t array_size = dic_tables_info_.at(i).array_size_;
      const char *table_name = dic_tables_info_.at(i).table_name_;
      common::ObSqlString query_string;
      common::ObSqlString columns;
      common::ObSqlString values;
      share::ObDMLSqlSplicer dml;
      int64_t pos = 0;
      while (array_size > 0 && OB_SUCC(ret)) {
        columns.reuse();
        query_string.reuse();
        for (int64_t j = 0; OB_SUCC(ret) && j < DEFAULT_BATCH_SIZE && j < array_size; ++j, ++pos) {
          ObDicItem item;
          dml.reuse();
          if (OB_FAIL(get_dic_item(i, pos, item))) {
            LOG_WARN("fail to get dic item", K(ret), K(i), K(pos));
          } else if (OB_FAIL(fill_dic_item(item, dml))){
            LOG_WARN("fail to fill dic item", K(ret));
          } else {
            if (0 == j) {
              if (OB_FAIL(dml.splice_column_names(columns))) {
                LOG_WARN("fail to splice column names", K(ret));
              } else if (OB_FAIL(query_string.append_fmt("INSERT INTO %s (%s) VALUES",
                          table_name, columns.ptr()))) {
                LOG_WARN("assign sql string failed", KR(ret), K(query_string));
              }
            }

            if (OB_SUCC(ret)) {
              values.reset();
              if (OB_FAIL(dml.splice_values(values))) {
                LOG_WARN("fail to splice values", K(ret));
              } else if (OB_FAIL(query_string.append_fmt("%s(%s)",
                      0 == j ? " " : " , ", values.ptr()))) {
                LOG_WARN("fail to assign sql string", K(ret));
              }
            }
          }
        }
        array_size -= DEFAULT_BATCH_SIZE;
        if (OB_SUCC(ret)) {
          int64_t affected_rows = 0;
          if (OB_ISNULL(GCTX.sql_proxy_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sql proxy is null", K(ret));
          } else if (OB_FAIL(trans.write(tenant_id, query_string.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", K(ret));
          } else if (OB_UNLIKELY(((array_size > 0) && affected_rows != DEFAULT_BATCH_SIZE) || (affected_rows <= 0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid affected rows", K(ret), K(affected_rows));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantDicLoader::try_load_dictionary_in_trans(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the dic loader is not initialized", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    if (!is_load_) {
      bool is_need_load_dic = false;
      if (OB_FAIL(check_need_load_dic(tenant_id, is_need_load_dic))) {
        LOG_WARN("failed to check is real load", K(ret), K(tenant_id));
      } else if (is_need_load_dic) {
        if (OB_FAIL(ObDicLock::lock_dic_tables_in_trans(tenant_id,
                                                        *this,
                                                        transaction::tablelock::EXCLUSIVE,
                                                        trans))) {
          LOG_WARN("failed to lock all dictionary table", K(ret), K(tenant_id), KPC(this));
        }
        if (OB_SUCC(ret)) {
          if (OB_FALSE_IT(is_need_load_dic = false)) {
          } else if (OB_FAIL(check_need_load_dic(tenant_id, is_need_load_dic))) {
            LOG_WARN("failed to check is real load", K(ret), K(tenant_id));
          } else if (is_need_load_dic) {
            if (OB_FAIL(load_dictionary_in_trans(tenant_id, trans))) {
              LOG_WARN("failed to load dictionary", K(ret), K(tenant_id));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_load_ = true;
      }
    }
  }
  return ret;
}

int ObTenantDicLoader::try_load_dictionary_in_trans(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the dic loader is not initialized", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    if (!is_load_) {
      ObMySQLTransaction trans;
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", K(ret));
      } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
        LOG_WARN("failed to start trans", K(ret), K(tenant_id));
      } else if (OB_FAIL(try_load_dictionary_in_trans(tenant_id, trans))) {
        LOG_WARN("fail to try load dictionary in trans", K(ret), K(tenant_id));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", K(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObTenantDicLoader::check_need_load_dic(const uint64_t tenant_id, bool &is_need_load_dic)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_need_load_dic = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the dic loader is not initialized", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s LIMIT 1", dic_tables_info_.at(0).table_name_))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(res.get_result()->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", KR(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          is_need_load_dic = true;
        }
      } else {
        is_need_load_dic = false;
      }
    }
  }
  return ret;
}

/**
* -----------------------------------ObTenantDicLoaderHandle-----------------------------------
*/
ObTenantDicLoaderHandle &ObTenantDicLoaderHandle::operator =(const ObTenantDicLoaderHandle &other)
{
  if (this != &other) {
    reset();
    if (OB_NOT_NULL(other.loader_)) {
      loader_ = other.loader_;
      loader_->inc_ref();
    }
  }
  return *this;
}

void ObTenantDicLoaderHandle::reset()
{
  if (nullptr != loader_) {
    const int64_t ref_cnt = loader_->dec_ref();
    if (0 == ref_cnt) {
      ObMemAttr attr(OB_SERVER_TENANT_ID, "dic_loader");
      OB_DELETE(ObTenantDicLoader, attr, loader_);
    }
    loader_ = nullptr;
  }
}

int ObTenantDicLoaderHandle::set_loader(ObTenantDicLoader *loader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(loader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(loader));
  } else {
    reset();
    loader_ = loader;
    loader_->inc_ref();
  }
  return ret;
}
} // end storage
} // end oceanbase