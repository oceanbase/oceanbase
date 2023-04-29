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

#ifndef OBDEV_SRC_EXTERNAL_TABLE_FILE_TASK_H_
#define OBDEV_SRC_EXTERNAL_TABLE_FILE_TASK_H_
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "deps/oblib/src/lib/lock/ob_thread_cond.h"
namespace oceanbase
{
namespace share
{

class ObFlushExternalTableFileCacheReq
{
  OB_UNIS_VERSION(1);
public:
  ObFlushExternalTableFileCacheReq() :
    tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID) {}
public:
  uint64_t tenant_id_;
  int64_t table_id_;
  int64_t partition_id_;
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(partition_id));
};

class ObFlushExternalTableFileCacheRes
{
  OB_UNIS_VERSION(1);
public:
  ObFlushExternalTableFileCacheRes() : rcode_() {}
  TO_STRING_KV(K_(rcode));
public:
  obrpc::ObRpcResultCode rcode_;
};

class ObLoadExternalFileListReq
{
  OB_UNIS_VERSION(1);
public:
  ObLoadExternalFileListReq() :
   location_() {}
public:
  ObString location_;
  TO_STRING_KV(K_(location));
};

class ObLoadExternalFileListRes
{
  OB_UNIS_VERSION(1);
public:
  ObLoadExternalFileListRes() : rcode_(), file_urls_(), file_sizes_(), allocator_() {}

  ObIAllocator &get_alloc() { return allocator_; }
  int assign(const ObLoadExternalFileListRes &other);
  TO_STRING_KV(K_(rcode));
public:
  obrpc::ObRpcResultCode rcode_; //返回的错误信息
  ObSEArray<ObString, 8> file_urls_;
  ObSEArray<int64_t, 8> file_sizes_;

private:
  ObArenaAllocator allocator_;
};




}  // namespace share
}  // namespace oceanbase
#endif /* OBDEV_SRC_EXTERNAL_TABLE_FILE_TASK_H_ */
