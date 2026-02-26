/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_I_TABLE_GROUP_H_
#define OCEANBASE_OBSERVER_OB_I_TABLE_GROUP_H_

#include "rpc/ob_request.h"
#include "lib/list/ob_list.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/allocator/page_arena.h"
#include "ob_table_group_register.h"
#include "src/share/table/ob_table.h"
namespace oceanbase
{

namespace table
{

class ObTableGroupCtx;
class ObITableGroupMeta;
class ObTableCreateCbFunctor;
class ObTableGroupInfo;
class ObITableGroupValue;
struct ObITableOp : public common::ObDLinkBase<ObITableOp>
{
public:
  ObITableOp()
    : type_(ObTableGroupType::TYPE_INVALID),
      req_(nullptr),
      timeout_ts_(0),
      timeout_(0)
  {}

  ObITableOp(ObTableGroupType op_type)
    : type_(op_type),
      req_(nullptr),
      timeout_ts_(0),
      timeout_(0)
  {}

  VIRTUAL_TO_STRING_KV(K_(type),
                       KPC_(req),
                       K_(timeout_ts),
                       K_(timeout));

  virtual ~ObITableOp() {}
  OB_INLINE virtual ObTableGroupType type() { return type_; }
  OB_INLINE virtual ObTabletID tablet_id() const = 0;
  OB_INLINE virtual int get_result(ObITableResult *&result) = 0;
  virtual void set_failed_result(int ret_code,
                                 ObTableEntity &result_entity,
                                 ObTableOperationType::Type op_type) = 0;
  OB_INLINE virtual bool is_valid() const { return OB_NOT_NULL(req_) && timeout_ts_ != 0 && timeout_ != 0; }
  virtual int check_legality() const { return OB_SUCCESS; }
  virtual void reset()
  {
    req_ = nullptr;
    timeout_ts_ = 0;
    timeout_ = 0;
  }
  virtual void reset_result() = 0;
public:
  ObTableGroupType type_;
  rpc::ObRequest *req_; // rpc request
  int64_t timeout_ts_;
  int64_t timeout_;
};

struct ObITableGroupKey
{
  ObITableGroupKey()
    : type_(ObTableGroupType::TYPE_INVALID)
  {}

  ObITableGroupKey(ObTableGroupType op_type)
    : type_(op_type)
  {}

  VIRTUAL_TO_STRING_KV(K_(type));
  virtual ~ObITableGroupKey() {}
  virtual int check_legality() const { return OB_SUCCESS; }
  virtual uint64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupKey &other) = 0;
  virtual bool is_equal(const ObITableGroupKey &other) const = 0;
  virtual bool operator==(const ObITableGroupKey &other) const
  {
    return is_equal(other);
  }

  ObTableGroupType type_;
};

struct ObTableGroupInfo final
{
public:
  enum GroupType: uint8_t {
    // start tableapi
    GET = 0,
    PUT,
    DEL,
    REPLACE,
    INSERT,
    HYBIRD,
    // end tableapi
    REDIS,
    FAIL,
    INVALID
  };
public:
  ObTableGroupInfo()
    : client_addr_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      group_type_(GroupType::INVALID),
      ls_id_(),
      schema_version_(OB_INVALID_VERSION),
      queue_size_(-1),
      batch_size_(-1),
      gmt_created_(0),
      gmt_modified_(0)
  {}
  ~ObTableGroupInfo() {}
  bool is_fail_group() { return group_type_ == GroupType::FAIL; }
  int set_group_type(const ObTableOperationType::Type op_type);
  const char* get_group_type_str();
  TO_STRING_KV(K_(client_addr),
              K_(tenant_id),
              K_(table_id),
              K_(schema_version),
              K_(table_id),
              K_(ls_id),
              K_(queue_size),
              K_(batch_size),
              K_(gmt_created),
              K_(gmt_modified));
public:
  common::ObAddr client_addr_;
  int64_t tenant_id_;
  common::ObTableID table_id_;
  GroupType group_type_;
  share::ObLSID ls_id_;
  int64_t schema_version_;
  int64_t queue_size_;
  int64_t batch_size_;
  int64_t gmt_created_;
  int64_t gmt_modified_;
};

struct ObITableGroupValue
{
public:
  friend class ObTableGroupInfo;
public:
  ObITableGroupValue()
    : type_(ObTableGroupType::TYPE_INVALID),
      group_info_()
  {}

  ObITableGroupValue(ObTableGroupType op_type)
    : type_(op_type),
      group_info_()
  {}

  virtual ~ObITableGroupValue() {}

  VIRTUAL_TO_STRING_KV(K_(type), K_(group_info));
  OB_INLINE ObTableGroupType type() { return type_; }
  virtual int init(const ObTableGroupCtx &ctx);
  virtual int get_executable_group(int64_t batch_size, ObIArray<ObITableOp *> &ops, bool check_queue_size = true) = 0;
  virtual int add_op_to_group(ObITableOp *op) = 0;
  virtual bool has_executable_batch() = 0;
  virtual int64_t get_group_size() = 0;
public:
  ObTableGroupType type_;
  ObTableGroupInfo group_info_;
};

struct ObITableGroupMeta
{
public:
  ObITableGroupMeta()
    : type_(ObTableGroupType::TYPE_INVALID)
  {}
  ObITableGroupMeta(ObTableGroupType op_type)
    : type_(op_type)
  {}
  VIRTUAL_TO_STRING_KV(K_(type));
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupMeta &other) { type_ = other.type_; return OB_SUCCESS; }
  virtual void reset() { type_ = ObTableGroupType::TYPE_INVALID; }
public:
  ObTableGroupType type_;
};

class ObITableOpProcessor
{
public:
  ObITableOpProcessor()
    : is_inited_(false),
      type_(ObTableGroupType::TYPE_INVALID),
      group_ctx_(nullptr),
      ops_(nullptr),
      functor_(nullptr)
  {}
  ObITableOpProcessor(ObTableGroupType op_type,
                      ObTableGroupCtx *group_ctx,
                      ObIArray<ObITableOp*> *ops,
                      ObTableCreateCbFunctor *functor)
    : is_inited_(false),
      type_(op_type),
      group_ctx_(group_ctx),
      ops_(ops),
      functor_(functor)
  {}
  VIRTUAL_TO_STRING_KV(K_(is_inited),
                       K_(type));
  virtual ~ObITableOpProcessor() {};
  virtual int init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops);
  virtual int process() = 0;

protected:
  bool is_inited_;
  ObTableGroupType type_;
  ObTableGroupCtx *group_ctx_;
  ObIArray<ObITableOp*> *ops_;
  ObTableCreateCbFunctor *functor_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_I_TABLE_GROUP_H_ */
