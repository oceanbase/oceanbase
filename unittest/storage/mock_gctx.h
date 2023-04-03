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
