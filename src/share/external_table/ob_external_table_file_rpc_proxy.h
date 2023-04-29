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

#ifndef OBDEV_SRC_EXTERNAL_TABLE_FILE_RPC_PROXY_H_
#define OBDEV_SRC_EXTERNAL_TABLE_FILE_RPC_PROXY_H_
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/external_table/ob_external_table_file_task.h"
#include "observer/ob_server_struct.h"
namespace oceanbase
{
namespace obrpc
{
class ObExtenralTableRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObExtenralTableRpcProxy);
  virtual ~ObExtenralTableRpcProxy() {}
  // sync rpc for das task result
  RPC_AP(PR5 flush_file_kvcahce, obrpc::OB_FLUSH_EXTERNAL_TABLE_FILE_CACHE, (share::ObFlushExternalTableFileCacheReq), share::ObFlushExternalTableFileCacheRes);
  RPC_AP(PR5 load_external_file_list, obrpc::OB_LOAD_EXTERNAL_FILE_LIST, (share::ObLoadExternalFileListReq), share::ObLoadExternalFileListRes);
};
}  // namespace obrpc


}  // namespace oceanbase
#endif /* OBDEV_SRC_EXTERNAL_TABLE_FILE_RPC_PROXY_H_ */
