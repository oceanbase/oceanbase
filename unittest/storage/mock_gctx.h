/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MOCK_GCTX_H_
#define OCEANBASE_STORAGE_MOCK_GCTX_H_
#include "observer/ob_server.h"
#include "sql/optimizer/mock_locality_manger.h"
namespace oceanbase
{
namespace storage
{

void init_global_context()
{
  static obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  static common::ObMySQLProxy sql_proxy_;
  static test::MockLocalityManager locality_manager_;

  GCTX.srv_rpc_proxy_ = &srv_rpc_proxy_;
  GCTX.sql_proxy_ = &sql_proxy_;
  GCTX.locality_manager_ = &locality_manager_;

  GCONF._enable_ha_gts_full_service = false;
}

}//namespace storage
}//namespace oceanbase
#endif // OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
