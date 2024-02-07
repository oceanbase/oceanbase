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
#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_external_table_file_task.h"
#include "share/external_table/ob_external_table_file_rpc_processor.h"

namespace oceanbase
{
namespace share
{


OB_SERIALIZE_MEMBER(ObFlushExternalTableFileCacheReq, tenant_id_, table_id_, partition_id_);

OB_SERIALIZE_MEMBER(ObFlushExternalTableFileCacheRes, rcode_);

OB_SERIALIZE_MEMBER(ObLoadExternalFileListReq, location_, pattern_, regexp_vars_);

OB_DEF_SERIALIZE(ObLoadExternalFileListRes)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rcode_, file_urls_, file_sizes_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadExternalFileListRes)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rcode_, file_urls_, file_sizes_);
  return len;
}

OB_DEF_DESERIALIZE(ObLoadExternalFileListRes)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rcode_, file_urls_, file_sizes_);
  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls_.count(); i++) {
    ObString file_url;
    OZ (ob_write_string(allocator_, file_urls_.at(i), file_url));
    file_urls_.at(i).assign_ptr(file_url.ptr(), file_url.length());

  }
  return ret;
}

int ObLoadExternalFileListRes::assign(const ObLoadExternalFileListRes &other)
{
  int ret = OB_SUCCESS;
  rcode_ = other.rcode_;
  file_sizes_.assign(other.file_sizes_);
  for (int64_t i = 0; OB_SUCC(ret) && i < other.file_urls_.count(); i++) {
    ObString tmp;
    OZ (ob_write_string(allocator_, other.file_urls_.at(i), tmp));
    OZ (file_urls_.push_back(tmp));
  }
  return ret;
}


}  // namespace share
}  // namespace oceanbase
