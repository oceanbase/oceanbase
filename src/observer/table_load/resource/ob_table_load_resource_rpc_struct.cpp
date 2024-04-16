/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_resource_rpc_struct.h"
#include "observer/table_load/ob_table_load_utils.h"

namespace oceanbase
{
namespace observer
{

OB_SERIALIZE_MEMBER(ObDirectLoadResourceOpRequest,
                    command_type_,
                    arg_content_);

OB_UNIS_DEF_SERIALIZE(ObDirectLoadResourceOpResult,
                      command_type_,
                      res_content_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObDirectLoadResourceOpResult,
                           command_type_,
                           res_content_);

OB_DEF_DESERIALIZE(ObDirectLoadResourceOpResult)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator in deserialize", K(ret));
  } else {
    ObString tmp_res_content;
    LST_DO_CODE(OB_UNIS_DECODE,
                command_type_,
                tmp_res_content);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(*allocator_, tmp_res_content, res_content_))) {
      LOG_WARN("fail to copy string", K(ret));
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceUnit,
                           addr_,
                           thread_count_,
                           memory_size_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceApplyArg,
                           tenant_id_,
                           task_key_,
                           apply_array_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceReleaseArg,
                           tenant_id_,
                           task_key_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceUpdateArg,
                           tenant_id_,
                           thread_count_,
                           memory_size_,
                           addrs_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceCheckArg,
                           tenant_id_,
                           avail_memory_,
                           first_check_);

OB_SERIALIZE_MEMBER_SIMPLE(ObDirectLoadResourceOpRes,
                           error_code_,
                           avail_memory_,
                           assigned_array_);

} // namespace observer
} // namespace oceanbase
