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
                    ttl_us_,
                    client_info_);

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

OB_SERIALIZE_MEMBER(ObTableLSOpRequest,
                    credential_,
                    entity_type_,
                    consistency_level_,
                    ls_op_);

OB_DEF_SERIALIZE(ObTableLSOpResult) {
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(properties_names_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableLSOpResult::BaseType::serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLSOpResult) {
  int64_t len = 0;
  int ret = OB_SUCCESS;
  OB_UNIS_ADD_LEN(properties_names_);
  len += ObTableLSOpResult::BaseType::get_serialize_size();

  return len;
}

OB_DEF_DESERIALIZE(ObTableLSOpResult, )
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE, properties_names_);

  int64_t tablet_op_size = 0;
  OB_UNIS_DECODE(tablet_op_size);
  ObTableTabletOpResult tablet_op_result;
  tablet_op_result.assign_properties_names(&properties_names_);
  tablet_op_result.set_all_rowkey_names(&rowkey_names_);

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op_size; ++i) {
    tablet_op_result.set_allocator(alloc_);
    tablet_op_result.set_entity_factory(entity_factory_);
    OB_UNIS_DECODE(tablet_op_result);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_back(tablet_op_result))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }  // end for
  return ret;
}

OB_UNIS_DEF_SERIALIZE((ObTableTabletOpResult, ObTableTabletOpResult::BaseType));

OB_UNIS_DEF_SERIALIZE_SIZE((ObTableTabletOpResult, ObTableTabletOpResult::BaseType));

OB_DEF_DESERIALIZE(ObTableTabletOpResult,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  int64_t single_op_size = 0;
  OB_UNIS_DECODE(single_op_size);
  ObTableSingleOpResult single_op_result;
  ObITableEntity *entity = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < single_op_size; ++i) {
    if (NULL == (entity = entity_factory_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      entity->set_dictionary(all_rowkey_names_, all_properties_names_);

      single_op_result.set_entity(*entity);
      if (OB_FAIL(serialization::decode(buf, data_len, pos, single_op_result))) {
        LOG_WARN("fail to decode array item", K(ret), K(i), K(single_op_size), K(data_len),
                  K(pos), K(single_op_result));
      } else if (OB_FAIL(push_back(single_op_result))) {
        LOG_WARN("fail to add item to array", K(ret), K(i), K(single_op_size));
      }
    }
  } // end for
  return ret;
}

ObTableTabletOpResult::ObTableTabletOpResult(const ObTableTabletOpResult& other)
  : BaseType(other)
{
  reserved_ = other.reserved_;
}

OB_SERIALIZE_MEMBER(ObRedisRpcRequest,
                    credential_,
                    redis_db_,
                    ls_id_,
                    tablet_id_,
                    table_id_,
                    reserved_,
                    resp_str_);
