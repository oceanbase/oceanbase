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

#ifndef OCEANBASE_STORAGE_OB_LOB_REMOTE_H_
#define OCEANBASE_STORAGE_OB_LOB_REMOTE_H_

#include "storage/ob_storage_rpc.h"
#include "storage/lob/ob_lob_rpc_struct.h"
#include "storage/lob/ob_lob_access_param.h"

namespace oceanbase
{
namespace storage
{
class ObLobQueryRemoteReader
{
public:
  ObLobQueryRemoteReader() : rpc_buffer_pos_(0), data_buffer_() {}
  ~ObLobQueryRemoteReader() {}
  int open(ObLobAccessParam& param, common::ObDataBuffer &rpc_buffer);
  int get_next_block(
                     common::ObDataBuffer &rpc_buffer,
                     obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle,
                     ObString &data);
private:
  int do_fetch_rpc_buffer(
                          common::ObDataBuffer &rpc_buffer,
                          obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle);
private:
  int64_t rpc_buffer_pos_;
  ObString data_buffer_;
};

struct ObLobRemoteQueryCtx
{
  ObLobRemoteQueryCtx() : handle_(), rpc_buffer_(), query_arg_(), remote_reader_() {}
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> handle_;
  common::ObDataBuffer rpc_buffer_;
  obrpc::ObLobQueryArg query_arg_;
  ObLobQueryRemoteReader remote_reader_;
};


class ObLobRemoteUtil
{
public:
  static int query(ObLobAccessParam& param, const ObLobQueryArg::QueryType qtype, const ObAddr &dst_addr, ObLobRemoteQueryCtx *&ctx);


private:
  static int remote_query_init_ctx(ObLobAccessParam &param, const ObLobQueryArg::QueryType qtype, ObLobRemoteQueryCtx *&ctx);
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_REMOTE_H_