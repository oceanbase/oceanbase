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

#define USING_LOG_PREFIX COMMON
#include "ob_domain_index_builder_util.h"
#include "src/rootserver/ob_root_service.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace share
{

int ObDomainIndexBuilderUtil::prepare_aux_table(bool &task_submitted,
                                                uint64_t &aux_table_id,
                                                int64_t &res_task_id,
                                                const common::TCRWLock& lock,
                                                const uint64_t& data_table_id,
                                                const uint64_t& tenant_id,
                                                const int64_t& task_id,
                                                obrpc::ObCreateIndexArg& index_arg,
                                                rootserver::ObRootService *root_service,
                                                common::hash::ObHashMap<uint64_t, ObDomainDependTaskStatus> &map,
                                                const oceanbase::common::ObAddr &addr,
                                                int map_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service is nullptr", K(ret));
  } else {
    int64_t ddl_rpc_timeout = 0;
    rootserver::ObDDLService &ddl_service = root_service->get_ddl_service();
    obrpc::ObCommonRpcProxy *common_rpc = nullptr;
    if (!map.created() &&
      OB_FAIL(map.create(map_num, lib::ObLabel("DepTasMap")))) {
      LOG_WARN("create dependent task map failed", K(ret));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root_service is nullptr", K(ret));
    } else if (OB_FALSE_IT(common_rpc = root_service->get_ddl_service().get_common_rpc())) {
    } else if (OB_ISNULL(common_rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc is nullptr", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id,
                                                      data_table_id,
                                                      ddl_rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else {
      SMART_VARS_2((obrpc::ObCreateAuxIndexArg, arg),
                    (obrpc::ObCreateAuxIndexRes, res)) {
      arg.tenant_id_ = tenant_id;
      arg.exec_tenant_id_ = tenant_id;
      arg.data_table_id_ = data_table_id;
      arg.task_id_ = task_id;
      if (task_submitted) {
          // do nothing
      } else if (OB_FAIL(arg.create_index_arg_.assign(index_arg))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to assign create index arg", K(ret));
      } else if (OB_FAIL(common_rpc-> to(addr).
                          timeout(ddl_rpc_timeout).create_aux_index(arg, res))) {
        LOG_WARN("generate aux index schema failed", K(ret), K(arg));
      } else if (res.schema_generated_) {
        task_submitted = true;
        aux_table_id = res.aux_table_id_;
        if (res.ddl_task_id_ < 0) {
          // res_task_id is int64, while OB_INVALID_ID is unit64_max, so use "res.ddl_task_id_ < 0" other than "res.ddl_task_id_ == OB_INVALID_ID"
          // rowkey_vid/vid_rowkey table already exist and data is ready
          res_task_id = OB_INVALID_ID;
        } else { // need to wait data complement finish
          res_task_id = res.ddl_task_id_;
          TCWLockGuard guard(lock);
          share::ObDomainDependTaskStatus status;
          // check if child task is already added
          if (OB_FAIL(map.get_refactored(aux_table_id, status))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              status.task_id_ = res.ddl_task_id_;
              if (OB_FAIL(map.set_refactored(aux_table_id, status))) {
                  LOG_WARN("set dependent task map failed", K(ret), K(aux_table_id));
              }
            } else {
              LOG_WARN("get from dependent task map failed", K(ret));
            }
          }
        }
      }
      } // SMART_VAR
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase