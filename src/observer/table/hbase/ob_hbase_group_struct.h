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

#ifndef _OB_HBASE_GROUP_STRUCT_H
#define _OB_HBASE_GROUP_STRUCT_H

#include "observer/table/group/ob_i_table_struct.h"
#include "share/table/ob_table_rpc_struct.h"

namespace oceanbase
{

namespace table
{
class ObHbaseGroupKey : public ObITableGroupKey
{
public:
  ObHbaseGroupKey()
      : ObITableGroupKey(ObTableGroupType::TYPE_HBASE_GROUP),
        ls_id_(),
        table_id_(OB_INVALID_ID),
        schema_version_(OB_INVALID_VERSION),
        op_type_(ObTableOperationType::Type::INVALID)
  {}

  ObHbaseGroupKey(share::ObLSID ls_id,
                  ObTableID table_id,
                  int64_t schema_version,
                  ObTableOperationType::Type op_type)
      : ObITableGroupKey(ObTableGroupType::TYPE_HBASE_GROUP),
        ls_id_(ls_id),
        table_id_(table_id),
        schema_version_(schema_version),
        op_type_(op_type)
  {}

  virtual uint64_t hash() const override;
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupKey &other) override;
  virtual bool is_equal(const ObITableGroupKey &other) const override;
public:
  share::ObLSID ls_id_;
  common::ObTableID table_id_;
  int64_t schema_version_;
  ObTableOperationType::Type op_type_;
};

struct ObHbaseOp : public ObITableOp
{
public:
  ObHbaseOp()
      : ObITableOp(ObTableGroupType::TYPE_HBASE_GROUP),
        is_inited_(false)
  {}
  virtual ~ObHbaseOp() {}
  VIRTUAL_TO_STRING_KV(K_(ls_req), K_(is_inited));
public:
  OB_INLINE virtual int get_result(ObITableResult *&result) override
  {
    result = &result_;
    return common::OB_SUCCESS;
  }
  virtual void set_failed_result(int ret_code,
                                 ObTableEntity &result_entity,
                                 ObTableOperationType::Type op_type) override
  {
    result_.generate_failed_result(ret_code, result_entity, op_type);
  }
  OB_INLINE virtual ObTabletID tablet_id() const override { return ObTabletID(); }
  virtual void reset()
  {
    ObITableOp::reset();
    is_inited_ = false;
    ls_req_.reset();
    result_.reset();
    result_entity_.reset();
  }
  void reuse()
  {
    reset();
  }
  int init();
public:
  bool is_inited_;
  ObTableLSOpRequest ls_req_;
  ObTableLSOpResult result_;
  ObTableSingleOpEntity result_entity_;
};

}  // namespace table
}  // namespace oceanbase

#endif /* _OB_HBASE_GROUP_STRUCT_H */
