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

#ifndef _OB_TABLE_RPC_STRUCT_H
#define _OB_TABLE_RPC_STRUCT_H 1
#include "ob_table.h"
#include "common/data_buffer.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace table
{
/// @see PCODE_DEF(OB_TABLE_API_LOGIN, 0x1101)
class ObTableLoginRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoginRequest()
    : auth_method_(1),
      client_type_(0),
      client_version_(1),
      reserved1_(0),
      client_capabilities_(0),
      max_packet_size_(0),
      reserved2_(0),
      reserved3_(0),
      tenant_name_(),
      user_name_(),
      pass_secret_(),
      pass_scramble_(),
      database_name_(),
      ttl_us_(0),
      client_info_(),
      allow_distribute_capability_(false)
  {}
public:
  uint8_t auth_method_;  // always 1 for now
  uint8_t client_type_;  // 1: libobtable; 2: java client 3: hbase client
  uint8_t client_version_;  // always 1 for now
  uint8_t reserved1_;
  uint32_t client_capabilities_;
  uint32_t max_packet_size_;  // for stream result
  uint32_t reserved2_;  // always 0 for now
  uint64_t reserved3_;  // always 0 for now
  ObString tenant_name_;
  ObString user_name_;
  ObString pass_secret_;
  ObString pass_scramble_;  // 20 bytes random string
  ObString database_name_;
  int64_t ttl_us_;  // 0 means no TTL
  ObString client_info_; // json format string, record client parameters
  bool allow_distribute_capability_; // used to determine whether the version of client and odp fully support distributed capability
public:
  TO_STRING_KV(K_(auth_method),
               K_(client_type),
               K_(client_version),
               K_(reserved1),
               K_(client_capabilities),
               K_(max_packet_size),
               K_(reserved2),
               K_(reserved3),
               K_(tenant_name),
               K_(user_name),
               K_(database_name),
               K_(ttl_us),
               K_(client_info),
               K_(allow_distribute_capability));
};

enum ObTableLoginFlag
{
  LOGIN_FLAG_NONE = 0,
  REDIS_PROTOCOL_V2 = 1 << 0,
  LOGIN_FLAG_MAX = 1 << 1,
};

enum ObTableServerCapacity
{
  CAPACITY_NONE = 0,
  DISTRIBUTED_EXECUTE = 1 << 0,
  CAPACITY_MAX = 1 << 31,
};

enum ObTableClientType
{
  INVALID_CLIENT = 0,
  LIBTABLE_CLIENT = 1, // c++ client
  JAVA_TABLE_CLIENT = 2,
  JAVA_HTABLE_CLIENT = 3,
  MAX_CLIENT = 15,
};

class ObTableLoginResult final
{
  OB_UNIS_VERSION(1);
public:
  uint32_t server_capabilities_;
  uint32_t reserved1_;  // used for ObTableLoginFlag
  uint64_t reserved2_;  // always 0 for now
  ObString server_version_;
  ObString credential_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
public:
  TO_STRING_KV(K_(server_capabilities),
               K_(reserved1),
               K_(reserved2),
               K_(server_version),
               "credential", common::ObHexStringWrap(credential_),
               K_(tenant_id),
               K_(user_id),
               K_(database_id));
};

////////////////////////////////////////////////////////////////
enum ObTableRequsetType
{
  TABLE_REQUEST_INVALID,
  TABLE_OPERATION_REQUEST,
  TABLE_REDIS_REQUEST,
  TABLE_HBASE_REQUEST,
  TABLE_REQUEST_MAX,
};

class ObITableRequest
{
public:
  ObITableRequest() {}
  ~ObITableRequest() {}
  virtual ObTableRequsetType get_type() const = 0;
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};


/// @see PCODE_DEF(OB_TABLE_API_EXECUTE, 0x1102)
class ObTableOperationRequest final : public ObITableRequest
{
  OB_UNIS_VERSION(1);
public:
  ObTableOperationRequest() : credential_(), table_name_(), table_id_(common::OB_INVALID_ID),
      tablet_id_(), entity_type_(), table_operation_(),
      consistency_level_(), option_flag_(OB_TABLE_OPTION_DEFAULT), returning_affected_entity_(false),
      returning_affected_rows_(false),
      binlog_row_image_type_(ObBinlogRowImageType::FULL)
      {}
  ~ObTableOperationRequest() {}

  TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(table_name),
               K_(table_id),
               K_(tablet_id),
               K_(entity_type),
               K_(table_operation),
               K_(consistency_level),
               K_(option_flag),
               K_(returning_affected_entity),
               K_(returning_affected_rows));
public:
  OB_INLINE bool use_put() const { return option_flag_ & OB_TABLE_OPTION_USE_PUT; }
  OB_INLINE bool returning_rowkey() const { return option_flag_ & OB_TABLE_OPTION_RETURNING_ROWKEY; }
  OB_INLINE bool server_can_retry() const { return option_flag_ & OB_TABLE_OPTION_SERVER_CAN_RETRY; }
  OB_INLINE uint8_t get_option_flag() const { return option_flag_; }
  OB_INLINE bool returning_affected_entity() const { return returning_affected_entity_; }
  ObTableRequsetType get_type() const override { return ObTableRequsetType::TABLE_OPERATION_REQUEST; }
  void set_server_can_retry(bool can_retry)
  {
    if (can_retry) {
      option_flag_ |= OB_TABLE_OPTION_SERVER_CAN_RETRY;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_SERVER_CAN_RETRY);
    }
  }
public:
  /// the credential returned when login.
  ObString credential_;
  /// table name.
  ObString table_name_;
  /// table id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t table_id_;  // for optimize purpose
  /// tablet id. If unknown, set it to be INVALID_TABLET_ID
  common::ObTabletID tablet_id_;  // for optimize purpose
  /// entity type. Set it to gain better performance. If unknown, set it to be ObTableEntityType::DYNAMIC.
  ObTableEntityType entity_type_;  // for optimize purpose
  /// table operation.
  ObTableOperation table_operation_;
  /// read consistency level. currently only support STRONG.
  ObTableConsistencyLevel consistency_level_;
  /// option flag, specific option switch.
  uint8_t option_flag_;
  /// Whether return the row which has been modified, currently the value MUST be false (In the case of Append/Increment, the value could be true)
  bool returning_affected_entity_;
  /// Whether return affected_rows
  bool returning_affected_rows_;
  /// Whether record the full row in binlog of modification
  ObBinlogRowImageType binlog_row_image_type_;
};

////////////////////////////////////////////////////////////////
/// batch operation of ONE partition
/// @see PCODE_DEF(OB_TABLE_API_BATCH_EXECUTE, 0x1103)
class ObTableBatchOperationRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableBatchOperationRequest()
      : credential_(),
        table_name_(),
        table_id_(common::OB_INVALID_ID),
        tablet_id_(),
        entity_type_(),
        batch_operation_(),
        consistency_level_(),
        option_flag_(OB_TABLE_OPTION_DEFAULT),
        returning_affected_entity_(false),
        returning_affected_rows_(false),
        batch_operation_as_atomic_(false),
        binlog_row_image_type_(ObBinlogRowImageType::FULL)
      {}
  ~ObTableBatchOperationRequest() {}

  TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(table_name),
               K_(table_id),
               K_(tablet_id),
               K_(entity_type),
               K_(batch_operation),
               K_(consistency_level),
               K_(option_flag),
               K_(returning_affected_entity),
               K_(returning_affected_rows),
               K_(batch_operation_as_atomic));
public:
  OB_INLINE bool use_put() const { return option_flag_ & OB_TABLE_OPTION_USE_PUT; }
  OB_INLINE bool returning_rowkey() const { return option_flag_ & OB_TABLE_OPTION_RETURNING_ROWKEY; }
  OB_INLINE bool return_one_result() const { return option_flag_ & OB_TABLE_OPTION_RETURN_ONE_RES; }
  OB_INLINE bool server_can_retry() const { return option_flag_ & OB_TABLE_OPTION_SERVER_CAN_RETRY; }
  OB_INLINE bool returning_affected_entity() const { return returning_affected_entity_; }
  void set_server_can_retry(bool can_retry)
  {
    if (can_retry) {
      option_flag_ |= OB_TABLE_OPTION_SERVER_CAN_RETRY;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_SERVER_CAN_RETRY);
    }
  }
public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// tablet id. If unknown, set it to be INVALID_TABLET_ID
  common::ObTabletID tablet_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableBatchOperation batch_operation_;
  // Only support STRONG
  ObTableConsistencyLevel consistency_level_;
  // option flag, specific option switch.
  uint8_t option_flag_;
  // Only support false (Support true for only Append/Increment)
  bool returning_affected_entity_;
  /// whether return affected_rows
  bool returning_affected_rows_;
  // batch oepration suppoert atomic operation
  bool batch_operation_as_atomic_;
  /// Whether record the full row in binlog of modification
  ObBinlogRowImageType binlog_row_image_type_;
};

////////////////////////////////////////////////////////////////
// @see PCODE_DEF(OB_TABLE_API_EXECUTE_QUERY, 0x1104)
class ObTableQueryRequest
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryRequest()
      :table_id_(common::OB_INVALID_ID),
       tablet_id_(),
       entity_type_(ObTableEntityType::ET_DYNAMIC),
       consistency_level_(ObTableConsistencyLevel::STRONG),
       option_flag_(OB_TABLE_OPTION_DEFAULT),
       hbase_op_type_(OHOperationType::INVALID)
  {}

  void set_server_can_retry(bool can_retry)
  {
    if (can_retry) {
      option_flag_ |= OB_TABLE_OPTION_SERVER_CAN_RETRY;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_SERVER_CAN_RETRY);
    }
  }
  OB_INLINE bool server_can_retry() const { return option_flag_ & OB_TABLE_OPTION_SERVER_CAN_RETRY; }
  void set_distribute_need_tablet_id(bool need_tablet_id)
  {
    if (need_tablet_id) {
      option_flag_ |= OB_TABLE_OPTION_NEED_TABLET_ID;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_NEED_TABLET_ID);
    }
  }
  OB_INLINE bool distribute_need_tablet_id() const { return option_flag_ & OB_TABLE_OPTION_NEED_TABLET_ID; }

  VIRTUAL_TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(table_name),
               K_(table_id),
               K_(tablet_id),
               K_(entity_type),
               K_(consistency_level),
               K_(query),
               K_(option_flag),
               K_(hbase_op_type));
public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// tablet id. If unknown, set it to be INVALID_TABLET_ID
  common::ObTabletID tablet_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  // only support STRONG
  ObTableConsistencyLevel consistency_level_;
  ObTableQuery query_;
  // option flag, specific option switch.
  uint8_t option_flag_;
  OHOperationType hbase_op_type_;
};

class ObTableQueryAndMutateRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryAndMutateRequest()
      :table_id_(common::OB_INVALID_ID),
      binlog_row_image_type_(ObBinlogRowImageType::FULL),
      option_flag_(OB_TABLE_OPTION_DEFAULT),
      hbase_op_type_(OHOperationType::INVALID)
  {}
  void set_server_can_retry(bool can_retry)
  {
    if (can_retry) {
      option_flag_ |= OB_TABLE_OPTION_SERVER_CAN_RETRY;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_SERVER_CAN_RETRY);
    }
  }
  OB_INLINE bool server_can_retry() const { return option_flag_ & OB_TABLE_OPTION_SERVER_CAN_RETRY; }
  void set_distribute_need_tablet_id(bool need_tablet_id)
  {
    if (need_tablet_id) {
      option_flag_ |= OB_TABLE_OPTION_NEED_TABLET_ID;
    } else {
      option_flag_ &= (~OB_TABLE_OPTION_NEED_TABLET_ID);
    }
  }
  OB_INLINE bool distribute_need_tablet_id() const { return option_flag_ & OB_TABLE_OPTION_NEED_TABLET_ID; }
  TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(table_name),
               K_(table_id),
               K_(tablet_id),
               K_(entity_type),
               K_(query_and_mutate),
               K_(binlog_row_image_type),
               K_(option_flag),
               K_(hbase_op_type));
public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// tablet id. Set it to gain better performance. If unknown, set it to be INVALID_TABLET_ID
  common::ObTabletID tablet_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableQueryAndMutate query_and_mutate_;
  ObBinlogRowImageType binlog_row_image_type_;
  // option flag, specific option switch.
  uint8_t option_flag_;
  OHOperationType hbase_op_type_;
};

class ObTableQueryAsyncRequest : public ObTableQueryRequest
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryAsyncRequest()
      :ObTableQueryRequest(),
       query_session_id_(0),
       query_type_(ObQueryOperationType::QUERY_MAX)
  {}
  virtual ~ObTableQueryAsyncRequest(){}
  INHERIT_TO_STRING_KV("ObTableQueryRequest", ObTableQueryRequest,
               K_(query_session_id),
               K_(query_type));
public:
  uint64_t query_session_id_;
  ObQueryOperationType query_type_;
};

struct ObTableDirectLoadRequestHeader
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadRequestHeader() : operation_type_(ObTableDirectLoadOperationType::MAX_TYPE) {}
  TO_STRING_KV(K_(addr), K_(operation_type));
public:
  ObAddr addr_;
  ObTableDirectLoadOperationType operation_type_;
};

class ObTableDirectLoadRequest
{
  OB_UNIS_VERSION(2);
public:
  ObTableDirectLoadRequest() {}
  template <class Arg>
  int set_arg(const Arg &arg, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = arg.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory", K(ret), K(size));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      SERVER_LOG(WARN, "fail to serialize arg", K(ret), K(arg));
    } else {
      arg_content_.assign_ptr(buf, size);
    }
    return ret;
  }
  template <class Arg>
  int get_arg(Arg &arg) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(arg_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", K(ret), KPC(this));
    } else if (OB_FAIL(arg.deserialize(arg_content_.ptr(), arg_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize arg content", K(ret), KPC(this));
    }
    return ret;
  }
  TO_STRING_KV(K_(header),
               "credential", common::ObHexStringWrap(credential_),
               "arg_content", common::ObHexStringWrap(arg_content_));
public:
  ObTableDirectLoadRequestHeader header_;
  ObString credential_;
  ObString arg_content_;
};

struct ObTableDirectLoadResultHeader
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadResultHeader() : operation_type_(ObTableDirectLoadOperationType::MAX_TYPE) {}
  TO_STRING_KV(K_(addr), K_(operation_type));
public:
  ObAddr addr_;
  ObTableDirectLoadOperationType operation_type_;
};

class ObTableDirectLoadResult
{
  OB_UNIS_VERSION(2);
public:
  ObTableDirectLoadResult() : allocator_(nullptr) {}
  template <class Res>
  int set_res(const Res &res, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = res.get_serialize_size();
    if (size > 0) {
      char *buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", K(ret), K(size));
      } else if (OB_FAIL(res.serialize(buf, size, pos))) {
        SERVER_LOG(WARN, "fail to serialize res", K(ret), K(res));
      } else {
        res_content_.assign_ptr(buf, size);
      }
    }
    return ret;
  }
  template <class Res>
  int get_res(Res &res) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(res_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", K(ret), KPC(this));
    } else if (OB_FAIL(res.deserialize(res_content_.ptr(), res_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize res content", K(ret), KPC(this));
    }
    return ret;
  }
  TO_STRING_KV(K_(header), "res_content", common::ObHexStringWrap(res_content_));
public:
  common::ObIAllocator *allocator_; // for deserialize
  ObTableDirectLoadResultHeader header_;
  ObString res_content_;
};

class ObTableLSOpRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableLSOpRequest()
    : credential_(),
      entity_type_(),
      consistency_level_(),
      ls_op_(nullptr),
      hbase_op_type_(OHOperationType::INVALID)
  {
  }
  ~ObTableLSOpRequest() {}

  TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(entity_type),
               K_(consistency_level),
               KPC_(ls_op),
               K_(hbase_op_type));
public:
  void reset()
  {
    credential_.reset();
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    if (OB_NOT_NULL(ls_op_)) {
      ls_op_->reset();
      ls_op_ = nullptr;
    }
  }

  bool is_hbase_put() const
  {
    bool bret = false;
    if (entity_type_ == ObTableEntityType::ET_HKV
        && OB_NOT_NULL(ls_op_)
        && ls_op_->is_same_type()
        && ls_op_->count() > 0
        && ls_op_->at(0).count() > 0) {
      const ObTableSingleOp &op = ls_op_->at(0).at(0);
      bret = op.get_op_type() == ObTableOperationType::INSERT_OR_UPDATE;
    }

    return bret;
  }
  bool is_hbase_batch_get() const
  {
    bool bret = false;
    if (entity_type_ == ObTableEntityType::ET_HKV
        && OB_NOT_NULL(ls_op_)
        && ls_op_->is_same_type()
        && ls_op_->count() > 0
        && ls_op_->at(0).count() > 0) {
      const ObTableSingleOp &op = ls_op_->at(0).at(0);
      bret = op.get_op_type() == ObTableOperationType::SCAN;
    }

    return bret;
  }

  bool is_readonly() const
  {
    bool bret = false;
    if (OB_NOT_NULL(ls_op_) && ls_op_->is_same_type() && ls_op_->count() > 0 && ls_op_->at(0).count() > 0) {
      const ObTableSingleOp &op = ls_op_->at(0).at(0);
      bret = op.get_op_type() == ObTableOperationType::GET || op.get_op_type() == ObTableOperationType::SCAN;
    }
    return bret;
  }

  bool is_hbase_query_and_mutate() const
  {
    bool bret = false;
    if (entity_type_ == ObTableEntityType::ET_HKV
        && OB_NOT_NULL(ls_op_)
        && ls_op_->is_same_type()
        && ls_op_->count() > 0
        && ls_op_->at(0).count() > 0) {
      const ObTableSingleOp &op = ls_op_->at(0).at(0);
      bret = op.get_op_type() == ObTableOperationType::QUERY_AND_MUTATE;
    }
    return bret;
  }

  bool is_hbase_batch() const
  {
    int64_t op_cnt = 0;
    if (entity_type_ == ObTableEntityType::ET_HKV && OB_NOT_NULL(ls_op_)) {
      for (int64_t i = 0; op_cnt <= 1 && i < ls_op_->count(); i++) {
        op_cnt += ls_op_->at(i).count();
      }
    }
    return op_cnt > 1;
  }

  bool is_hbase_mix_batch() const
  {
    bool bret = false;
    if (entity_type_ == ObTableEntityType::ET_HKV
        && OB_NOT_NULL(ls_op_)
        && !ls_op_->is_same_type()
        && ls_op_->count() > 0) {
      bret = true;
    }
    return bret;
  }

  void shaddow_copy_without_op(const ObTableLSOpRequest &other);
public:
  ObString credential_;
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableConsistencyLevel consistency_level_;
  ObTableLSOp *ls_op_; // FARM COMPAT WHITELIST
  OHOperationType hbase_op_type_;
};

using ObTableSingleOpResult = ObTableOperationResult;
class ObTableTabletOpResult : public common::ObSEArrayImpl<ObTableSingleOpResult, ObTableTabletOp::COMMON_OPS_SIZE>
{
  OB_UNIS_VERSION(1);
public:
  ObTableTabletOpResult()
      : BaseType("TblTabletOpRes", common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        entity_factory_(NULL),
        alloc_(NULL)
  {}
  virtual ~ObTableTabletOpResult() = default;
  ObTableTabletOpResult(const ObTableTabletOpResult& other);
  void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  ObITableEntityFactory *get_entity_factory() { return entity_factory_; }
  void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  common::ObIAllocator *get_allocator() { return alloc_; }
  OB_INLINE void assign_properties_names(const ObIArray<ObString> *all_properties_names) {
    all_properties_names_ = all_properties_names;
  }

  OB_INLINE void set_all_rowkey_names(const ObIArray<ObString> *all_rowkey_names) {
    all_rowkey_names_ = all_rowkey_names;
  }
private:
  using BaseType = common::ObSEArrayImpl<ObTableSingleOpResult, ObTableTabletOp::COMMON_OPS_SIZE>;
  uint64_t reserved_;
  ObITableEntityFactory *entity_factory_;
  common::ObIAllocator *alloc_;
  const ObIArray<ObString>* all_properties_names_;
  const ObIArray<ObString>* all_rowkey_names_;
};

class ObTableLSOpResult : public common::ObSEArrayImpl<ObTableTabletOpResult, ObTableLSOp::COMMON_BATCH_SIZE>,
                          public ObITableResult,
                          public common::ObDLinkBase<ObTableLSOpResult>,
                          public ObTableObject
{
  OB_UNIS_VERSION(1);
public:
  ObTableLSOpResult()
    : BaseType("TblLSOpRes", common::OB_MALLOC_NORMAL_BLOCK_SIZE),
      entity_factory_(NULL),
      alloc_(NULL)
  {}
  virtual ~ObTableLSOpResult() = default;
  void reset() override
  {
    BaseType::reset();
    rowkey_names_.reset();
    properties_names_.reset();
    entity_factory_ = NULL;
    alloc_ = NULL;
  }
  virtual void reuse() override
  {
    BaseType::reuse();
    reset_last_active_ts();
  }
  TO_STRING_KV(K(rowkey_names_));
  OB_INLINE void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  OB_INLINE common::ObIAllocator *get_allocator() { return alloc_; }
  OB_INLINE int assign_rowkey_names(const ObIArray<ObString>& all_rowkey_names)
  {
    return rowkey_names_.assign(all_rowkey_names);
  }
  OB_INLINE int assign_properties_names(const ObIArray<ObString>& all_properties_names)
  {
    return properties_names_.assign(all_properties_names);
  }
  OB_INLINE const ObIArray<ObString>& get_rowkey_names() const { return rowkey_names_; }
  OB_INLINE const ObIArray<ObString>& get_properties_names() const { return properties_names_; }
  virtual int get_errno() const override
  {
    int ret = OB_SUCCESS;
    if (count() != 0) {
      const ObTableTabletOpResult &tablet_result = at(0);
      if (tablet_result.count() != 0) {
        ret = tablet_result.at(0).get_errno();
      }
    }
    return ret;
  }
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) override
  {
    if (count() != 0) {
      for (int64_t i = 0; i < count(); i++) {
        ObTableTabletOpResult &tablet_result = at(i);
        for (int64_t j = 0; j < tablet_result.count(); j++) {
          tablet_result.at(j).generate_failed_result(ret_code, result_entity, op_type);
        }
      }
    }
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLSOpResult);
  using BaseType = common::ObSEArrayImpl<ObTableTabletOpResult, ObTableLSOp::COMMON_BATCH_SIZE>;
  // allways empty
  ObSEArray<ObString, 8> rowkey_names_;
  // Only when this batch of operations is read-only is it not empty.
  ObSEArray<ObString, 16> properties_names_;
  // do not serialize
  // ObITableEntityFactory *entity_factory_;
  ObTableEntityFactory<ObTableSingleOpEntity> *entity_factory_;
  common::ObIAllocator *alloc_;
};

class ObRedisRpcRequest final : public ObITableRequest
{
  OB_UNIS_VERSION(1);
public:
  ObRedisRpcRequest() :
      credential_(),
      redis_db_(common::OB_INVALID_ID),
      ls_id_(),
      tablet_id_(),
      table_id_(common::OB_INVALID_ID),
      reserved_(0),
      resp_str_()
      {}
  ~ObRedisRpcRequest() {}

  bool is_valid() {
    return table_id_ != common::OB_INVALID_ID
      && tablet_id_.is_valid()
      && ls_id_.is_valid()
      && !resp_str_.empty()
      && redis_db_ != common::OB_INVALID_ID;
  }

  ObTableRequsetType get_type() const override { return ObTableRequsetType::TABLE_REDIS_REQUEST; }

  TO_STRING_KV("credential", common::ObHexStringWrap(credential_),
               K_(resp_str),
               K_(table_id),
               K_(tablet_id),
               K_(ls_id),
               K_(redis_db),
               K_(reserved));

public:
  /// the credential returned when login.
  ObString credential_;
  uint64_t redis_db_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  uint64_t table_id_;
  uint64_t reserved_; // reserved, fix 8 bytes
  ObString resp_str_;
};

class ObHbaseRpcRequest final : public ObITableRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHbaseRpcRequest()
      : deserialize_allocator_(nullptr),
        option_flag_(0),
        hbase_op_type_(OHOperationType::INVALID)
  {}
  ~ObHbaseRpcRequest() = default;
  void reset()
  {
    deserialize_allocator_ = nullptr;
    credential_.reset();
    op_type_ = ObTableOperationType::GET;
    hbase_op_type_ = OHOperationType::INVALID;
    table_name_.reset();
    keys_.reset();
    cf_rows_.reset();
  }
  TO_STRING_KV("credential", common::ObHexStringWrap(credential_), K_(table_name), K_(op_type), K_(hbase_op_type), K_(cf_rows));
  virtual ObTableRequsetType get_type() const { return ObTableRequsetType::TABLE_HBASE_REQUEST; }
  bool server_can_retry() { return server_can_retry_; }
  void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_allocator_ = allocator; }
public:
  common::ObIAllocator *deserialize_allocator_; // do not serialize
  ObString credential_;
  union
  {
    uint64_t option_flag_;
    struct
    {
      bool server_can_retry_ : 1;
      uint64_t reserved : 63;
    };
  };
  ObTableOperationType::Type op_type_;
  OHOperationType hbase_op_type_;
  common::ObString table_name_; // tablegroup_name, real_table_name in cf_rows
  common::ObFixedArray<ObObj, ObIAllocator> keys_;
  common::ObFixedArray<ObHCfRows, ObIAllocator> cf_rows_;
};

class ObHbaseResult final : public ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObHbaseResult()
    : deserialize_allocator_(nullptr),
      op_type_(ObTableOperationType::INSERT_OR_UPDATE)
  {}
  ~ObHbaseResult() = default;
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type)
  {
    UNUSED(result_entity);
    op_type_ = op_type;
    errno_ = ret_code;
  }
  virtual void reset() {}
  virtual ObTableResultType get_type() const { return ObTableResultType::HBASE_RESULT; }
  ObTableOperationType::Type get_op_type() const { return op_type_; }
  void set_op_type(ObTableOperationType::Type op_type) { op_type_ = op_type; }
  TO_STRING_KV(K_(op_type));
private:
  common::ObIAllocator *deserialize_allocator_;  // do not serialize
  ObTableOperationType::Type op_type_;
  common::ObFixedArray<ObHBaseCellResult*, ObIAllocator> cell_results_; // used for HBase Get
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_STRUCT_H */
