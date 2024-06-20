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

#define USING_LOG_PREFIX CLIENT
#include "ob_table_rpc_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::table;

OB_SERIALIZE_MEMBER(ObTableLoginRequest,
                    auth_method_,
                    client_type_,
                    client_version_,
                    reserved1_,
                    client_capabilities_,
                    max_packet_size_,
                    reserved2_,
                    reserved3_,
                    tenant_name_,
                    user_name_,
                    pass_secret_,
                    pass_scramble_,
                    database_name_,
                    ttl_us_);

OB_SERIALIZE_MEMBER(ObTableLoginResult,
                    server_capabilities_,
                    reserved1_,
                    reserved2_,
                    server_version_,
                    credential_,
                    tenant_id_,
                    user_id_,
                    database_id_);

OB_SERIALIZE_MEMBER(ObTableOperationRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    tablet_id_,
                    entity_type_,
                    table_operation_,
                    consistency_level_,
                    option_flag_,
                    returning_affected_entity_,
                    returning_affected_rows_,
                    binlog_row_image_type_);

OB_SERIALIZE_MEMBER(ObTableBatchOperationRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    entity_type_,
                    batch_operation_,
                    consistency_level_,
                    option_flag_,
                    returning_affected_entity_,
                    returning_affected_rows_,
                    tablet_id_,
                    batch_operation_as_atomic_,
                    binlog_row_image_type_);

OB_SERIALIZE_MEMBER(ObTableQueryRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    tablet_id_,
                    entity_type_,
                    consistency_level_,
                    query_
                    );
////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObTableQueryAndMutateRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    tablet_id_,
                    entity_type_,
                    query_and_mutate_,
                    binlog_row_image_type_);

OB_SERIALIZE_MEMBER((ObTableQueryAsyncRequest, ObTableQueryRequest),
                    query_session_id_,
                    query_type_
                    );
////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadRequestHeader,
                           addr_,
                           operation_type_);

OB_SERIALIZE_MEMBER(ObTableDirectLoadRequest,
                    header_,
                    credential_,
                    arg_content_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadResultHeader,
                           addr_,
                           operation_type_);

OB_UNIS_DEF_SERIALIZE(ObTableDirectLoadResult,
                      header_,
                      res_content_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableDirectLoadResult,
                           header_,
                           res_content_);

OB_DEF_DESERIALIZE(ObTableDirectLoadResult)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator in deserialize", K(ret));
  } else {
    ObString tmp_res_content;
    LST_DO_CODE(OB_UNIS_DECODE,
                header_,
                tmp_res_content);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(*allocator_, tmp_res_content, res_content_))) {
      LOG_WARN("fail to copy string", K(ret));
    }
  }
  return ret;
}
