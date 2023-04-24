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
 */

#include "ob_testbench_server_provider.h"

namespace oceanbase
{
  namespace testbench
  {

    ObTestbenchServerProvider::ObTestbenchServerProvider() : is_inited_(false),
                                                             tenant_list_(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE),
                                                             tenant_name_list_(ObModIds::OB_LOG_SERVER_PROVIDER,
                                                                               OB_MALLOC_NORMAL_BLOCK_SIZE),
                                                             server_list_(ObModIds::OB_LOG_SERVER_PROVIDER, OB_MALLOC_NORMAL_BLOCK_SIZE),
                                                             tenant_server_map_(),
                                                             systable_helper_(NULL),
                                                             refresh_server_lock_()
    {
    }

    int ObTestbenchServerProvider::init(ObTestbenchSystableHelper &systable_helper)
    {
      int ret = OB_SUCCESS;
      if (IS_INIT)
      {
        ret = OB_INIT_TWICE;
        TESTBENCH_LOG(WARN, "ObTestbenchServerProvider init twice", KR(ret));
      }
      else if (OB_FAIL(tenant_server_map_.init(ObModIds::OB_SQL_CONNECTION_POOL)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchServerProvider tenant_server_map_ init fail", KR(ret));
      }
      else
      {
        is_inited_ = true;
        systable_helper_ = &systable_helper;
        TESTBENCH_LOG(INFO, "ObTestbenchServerProvider init success");
      }
      return ret;
    }

    void ObTestbenchServerProvider::destroy()
    {
      if (IS_INIT)
      {
        is_inited_ = false;
        tenant_list_.destroy();
        server_list_.destroy();
        tenant_server_map_.reset();
      }
    }

    int ObTestbenchServerProvider::del_tenant(const uint64_t tenant_id)
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchServerProvider not init", KR(ret));
      }
      else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id))
      {
        ret = OB_INVALID_ARGUMENT;
        TESTBENCH_LOG(ERROR, "invalid tenant_id", K(ret), K(tenant_id));
      }
      else if (OB_FAIL(tenant_server_map_.del(tenant_id)))
      {
        TESTBENCH_LOG(WARN, "delete tenant_id from tenant_server_map_ failed", K(ret), K(tenant_id));
      }
      return ret;
    }

    int ObTestbenchServerProvider::query_all_tenants()
    {
      int ret = OB_SUCCESS;
      tenant_list_.reset();
      tenant_name_list_.reset();

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "tenant_sql_server_provider is not inited", K(ret));
      }
      else if (OB_ISNULL(systable_helper_))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "systable_helper_ should not be null", K(ret));
      }
      else if (OB_FAIL(systable_helper_->query_tenant_list(tenant_list_, tenant_name_list_)))
      {
        TESTBENCH_LOG(ERROR, "get tenant_id_list by systable_helper_ failed", K(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "query_all_tenants updates tenant_list_", K(tenant_list_));
        TESTBENCH_LOG(INFO, "query_all_tenants updates tenant_name_list_", K(tenant_name_list_));
      }

      return ret;
    }

    int ObTestbenchServerProvider::query_all_servers()
    {
      int ret = OB_SUCCESS;

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "tenant_sql_server_provider is not inited", KR(ret));
      }
      else if (OB_ISNULL(systable_helper_))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "systable_helper_ should not be null", KR(ret));
      }
      else
      {
        server_list_.reset();

        if (OB_FAIL(systable_helper_->query_sql_server_list(server_list_)))
        {
          TESTBENCH_LOG(ERROR, "query_server_list failed", KR(ret));
        }
        else if (OB_UNLIKELY(0 == server_list_.count()))
        {
          ret = OB_NEED_RETRY;
          TESTBENCH_LOG(WARN, "server_list query from cluster is empty", KR(ret));
        }
      }

      return ret;
    }

    int ObTestbenchServerProvider::query_tenant_servers()
    {
      int ret = OB_SUCCESS;

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
      }
      else if (OB_ISNULL(systable_helper_))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "systable_helper_ should not be null", KR(ret));
      }
      else if (OB_UNLIKELY(0 == server_list_.count() || 0 == tenant_list_.count()))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(WARN, "expect valid server_list and tenant_list");
      }
      else
      {
        for (int tenant_idx = 0; tenant_idx < tenant_list_.count(); tenant_idx++)
        {
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          TenantServerList *tenant_server_list = NULL;

          if (OB_FAIL(tenant_list_.at(tenant_idx, tenant_id)))
          {
            TESTBENCH_LOG(WARN, "get_tenant_id failed", KR(ret), K_(tenant_list), K(tenant_idx));
          }
          else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id))
          {
            ret = OB_ERR_UNEXPECTED;
            TESTBENCH_LOG(ERROR, "expect valid tenant_id", KR(ret), K_(tenant_list), K(tenant_idx), K(tenant_id));
          }
          else if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list)))
          {
            if (OB_ENTRY_NOT_EXIST == ret)
            {
              if (OB_FAIL(tenant_server_map_.create(tenant_id, tenant_server_list)))
              {
                TESTBENCH_LOG(ERROR, "create tenant_server_list failed");
              }
            }
            else
            {
              TESTBENCH_LOG(ERROR, "get tenant_server_list from tenant_server_map_ failed", KR(ret), K(tenant_id));
            }
          }

          if (OB_SUCC(ret))
          {
            if (OB_ISNULL(tenant_server_list))
            {
              ret = OB_ERR_UNEXPECTED;
              TESTBENCH_LOG(ERROR, "tenant_server_list get/created from tenant_server_map_ should not be null", KR(ret), K(tenant_id));
            }
            else if (OB_FAIL(systable_helper_->query_tenant_sql_server_list(tenant_id, tenant_server_list->get_server_list())))
            {
              TESTBENCH_LOG(ERROR, "query_tenant_sql_server_list by systable_helper failed", KR(ret), K(tenant_id));
            }
            else if (OB_UNLIKELY(0 == tenant_server_list->get_server_count()))
            {
              share::schema::TenantStatus tenant_status = share::schema::TenantStatus::TENANT_STATUS_INVALID;

              if (OB_FAIL(systable_helper_->query_tenant_status(tenant_id, tenant_status)))
              {
                TESTBENCH_LOG(ERROR, "query_tenant_status failed", KR(ret), K(tenant_id));
              }
              else if (share::schema::TenantStatus::TENANT_DELETED == tenant_status)
              {
                TESTBENCH_LOG(INFO, "tenant_already dropped", K(tenant_id), K(tenant_status));
                ret = OB_ENTRY_NOT_EXIST;
              }
              else
              {
                TESTBENCH_LOG(WARN, "tenant_server_list is empty but tenant is not dropped", K(tenant_id), K(tenant_server_list));
              }
            }
            else
            {
              TESTBENCH_LOG(INFO, "find tenant servers", K(tenant_id), K(*tenant_server_list));
            }

            if (OB_NOT_NULL(tenant_server_list))
            {
              // revertt tenant_server_list anyway cause already get from tenant_server_map_ succ.
              tenant_server_map_.revert(tenant_server_list);
            }
          }
        }
      }

      return ret;
    }

    int ObTestbenchServerProvider::get_server(const int64_t svr_idx, common::ObAddr &server)
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchServerProvider not inited", K(ret), K(is_inited_));
      }
      else if (OB_UNLIKELY(svr_idx >= get_server_count()))
      {
        ret = OB_ENTRY_NOT_EXIST;
        TESTBENCH_LOG(WARN, "request server does not exist", K(ret), K(svr_idx), K(server_list_));
      }
      else if (OB_FAIL(server_list_.at(svr_idx, server)))
      {
        TESTBENCH_LOG(WARN, "get server at specified position failed", K(ret), K(svr_idx), K(server_list_));
      }
      return ret;
    }

    int64_t ObTestbenchServerProvider::get_server_count() const
    {
      int64_t server_cnt = 0;
      if (IS_INIT)
      {
        server_cnt = server_list_.count();
      }
      return server_cnt;
    }

    int ObTestbenchServerProvider::get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids)
    {
      int ret = OB_SUCCESS;
      tenant_ids.reset();
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchServerProvider not inited", K(ret));
      }
      else if (OB_FAIL(tenant_ids.assign(tenant_list_)))
      {
        TESTBENCH_LOG(ERROR, "assign tenant_ids fail", K(ret), K(tenant_ids));
      }
      else
      {
        for (int i = 0; i < tenant_ids.count(); i++)
        {
          const uint64_t &tenant_id = tenant_list_.at(i);
          TESTBENCH_LOG(INFO, "get_tenants", K(tenant_id));
        }
      }
      return ret;
    }

    int ObTestbenchServerProvider::get_tenants(common::ObIArray<ObTenantName> &tenants)
    {
      int ret = OB_SUCCESS;
      tenants.reset();
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchServerProvider not inited", K(ret));
      }
      else if (OB_FAIL(tenants.assign(tenant_name_list_)))
      {
        TESTBENCH_LOG(ERROR, "assign tenants fail", K(ret), K(tenants), K(tenant_name_list_));
      }
      return ret;
    }

    int ObTestbenchServerProvider::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &tenant_servers)
    {
      int ret = OB_SUCCESS;
      tenant_servers.reset();
      TenantServerList *tenant_server_list = NULL;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchServerProvider not inited", K(ret));
      }
      else if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list)))
      {
        TESTBENCH_LOG(ERROR, "cannot find tenant in tenant_server_map_", K(ret), K(tenant_id));
      }
      else if (OB_FAIL(tenant_servers.assign(tenant_server_list->get_server_list())))
      {
        TESTBENCH_LOG(ERROR, "fail to get server list from tenant_server_list", K(ret), K(tenant_id), K(tenant_servers));
      }
      return ret;
    }

    int ObTestbenchServerProvider::refresh_server_list(void)
    {
      // TODO What's the meaning of refresh_server_list, since we get servers/tenants in the prepare_refresh.
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObTestbenchServerProvider::prepare_refresh()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchServerProvider not inited", K(ret));
      }
      else if (OB_FAIL(refresh_server_lock_.wrlock()))
      {
        TESTBENCH_LOG(ERROR, "lock refresh_server_lock_ failed", K(ret));
      }
      else if (OB_FAIL(query_all_servers()))
      {
        TESTBENCH_LOG(ERROR, "query_all_servers failed", K(ret));
      }
      else if (OB_FAIL(query_all_tenants()))
      {
        TESTBENCH_LOG(ERROR, "query_all_tenants failed", K(ret));
      }
      else if (OB_FAIL(query_tenant_servers()))
      {
        TESTBENCH_LOG(ERROR, "query_tenant_servers failed", K(ret));
      }
      return ret;
    }

    int ObTestbenchServerProvider::end_refresh()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchServerProvider not inited", K(ret));
      }
      else if (OB_FAIL(refresh_server_lock_.unlock()))
      {
        TESTBENCH_LOG(ERROR, "release refresh_server_lock_ failed", K(ret));
      }
      return ret;
    }
  }
}
