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
#include "ob_table_object.h"
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

OB_DEF_DESERIALIZE(ObTableQueryRequest,)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              consistency_level_,
              query_);
  if (OB_SUCC(ret) && pos < data_len) {
    OB_UNIS_DECODE(option_flag_);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableQueryRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              consistency_level_,
              query_,
              option_flag_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableQueryRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              consistency_level_,
              query_,
              option_flag_);
  return len;
}
////////////////////////////////////////////////////////////////
OB_DEF_DESERIALIZE(ObTableQueryAndMutateRequest,)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              query_and_mutate_,
              binlog_row_image_type_);
  if (OB_SUCC(ret) && pos < data_len) {
    OB_UNIS_DECODE(option_flag_);
  }
  return ret;
}
OB_DEF_SERIALIZE(ObTableQueryAndMutateRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              query_and_mutate_,
              binlog_row_image_type_,
              option_flag_);
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObTableQueryAndMutateRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              credential_,
              table_name_,
              table_id_,
              tablet_id_,
              entity_type_,
              query_and_mutate_,
              binlog_row_image_type_,
              option_flag_);
  return len;
}

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

OB_SERIALIZE_MEMBER(ObTableLSOpRequest, // FARM COMPAT WHITELIST
                    credential_,
                    entity_type_,
                    consistency_level_,
                    *ls_op_);

OB_DEF_SERIALIZE(ObTableLSOpResult)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(properties_names_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableLSOpResult::BaseType::serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    }
  }
  return ret;
}

void ObTableLSOpRequest::shaddow_copy_without_op(const ObTableLSOpRequest &other)
{
  this->credential_ = other.credential_;
  this->entity_type_ = other.entity_type_;
  this->consistency_level_ = other.consistency_level_;
  this->ls_op_->shaddow_copy_without_op(*other.ls_op_);
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

OB_UNIS_DEF_SERIALIZE(ObHbaseRpcRequest, credential_, table_name_);

OB_DEF_SERIALIZE_SIZE(ObHbaseRpcRequest)
{
  return 0;
}

OB_DEF_DESERIALIZE(ObHbaseRpcRequest,)
{
  int ret = OB_SUCCESS;
  // credential_ + table_name_ + option_flag_ + op_type_
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              option_flag_,
              op_type_);
  if (OB_SUCC(ret)) {
    if (op_type_ != ObTableOperationType::INSERT_OR_UPDATE) { // HBase Put
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation not HBase Put");
      LOG_WARN("operation not HBase Put is not supported yet", K(ret), K_(op_type));
    } else {
      // decode keys
      int64_t now_ms = -(ObTimeUtility::current_time() / 1000);
      int64_t key_num = 0;
      keys_.set_allocator(deserialize_allocator_);
      cf_rows_.set_allocator(deserialize_allocator_);
      OB_UNIS_DECODE(key_num);
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(deserialize_allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("deserialize_allocator_ is null", K(ret));
        } else if (OB_FAIL(keys_.prepare_allocate(key_num))) {
          LOG_WARN("fail to prepare allocate keys", K(ret), K(key_num));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < key_num; i++) {
            if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, keys_.at(i)))) {
              LOG_WARN("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
            }
          }
          // decode cf_rows_
          if (OB_SUCC(ret)) {
            int64_t cf_rows_num = 0;
            OB_UNIS_DECODE(cf_rows_num);
            if (OB_FAIL(cf_rows_.prepare_allocate(cf_rows_num))) {
              LOG_WARN("fail to prepare allocate same_cf_rows", K(ret), K(cf_rows_num));
            }
            for (int i = 0; OB_SUCC(ret) && i < cf_rows_num; ++i) {
              ObHCfRows &same_cf_rows = cf_rows_.at(i);
              same_cf_rows.deserialize_alloc_ = deserialize_allocator_;
              same_cf_rows.rows_.set_allocator(deserialize_allocator_);
              same_cf_rows.set_keys(&keys_);
              same_cf_rows.now_ms_ = now_ms;
              OB_UNIS_DECODE(same_cf_rows);
            }
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObHbaseResult)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(op_type_);
  if (OB_SUCC(ret)) {
    int64_t res_num = 0;
    OB_UNIS_DECODE(res_num);
    char *tmp_buf = nullptr;
    if (OB_ISNULL(deserialize_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize allocator is NULL", K(ret));
    } else if (FALSE_IT(cell_results_.set_allocator(deserialize_allocator_))) {
      // do nothgin
    } else if (OB_FAIL(cell_results_.prepare_allocate(res_num))) {
      LOG_WARN("fail to prepare allocate cell results", K(ret));
    } else if (OB_ISNULL(tmp_buf = reinterpret_cast<char*>(deserialize_allocator_->alloc(sizeof(ObHBaseCellResult) * res_num)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < res_num; ++i) {
       cell_results_.at(i) = new (tmp_buf + sizeof(ObHBaseCellResult) * i) ObHBaseCellResult();
       OB_UNIS_DECODE(*cell_results_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObHbaseResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, errno_, sqlstate_, msg_, op_type_);
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(cell_results_.count());
    for (int i = 0; OB_SUCC(ret) && i < cell_results_.count(); ++i) {
      ObHBaseCellResult *cell_res = cell_results_.at(i);
      if (OB_ISNULL(cell_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell result is NULL", K(ret));
      } else {
        OB_UNIS_ENCODE(*cell_res);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHbaseResult)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, errno_, sqlstate_, msg_, op_type_);
  if (OB_SUCC(ret)) {
    int64_t res_len = cell_results_.count();
    OB_UNIS_ADD_LEN(res_len);
    for (int i = 0; OB_SUCC(ret) && i < res_len; ++i) {
      ObHBaseCellResult *cell_res = cell_results_.at(i);
      if (OB_ISNULL(cell_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell result is NULL", K(ret));
      } else {
        OB_UNIS_ADD_LEN(*cell_res);
      }
    }
  }
  return len;
}