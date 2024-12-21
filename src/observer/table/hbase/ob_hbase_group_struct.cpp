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

#define USING_LOG_PREFIX SERVER

#include "ob_hbase_group_struct.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

uint64_t ObHbaseGroupKey::hash() const
{
  uint64_t hash_val = 0;
  uint64_t seed = 0;
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), seed);
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  hash_val = murmurhash(&op_type_, sizeof(op_type_), hash_val);
  return hash_val;
}

int ObHbaseGroupKey::deep_copy(common::ObIAllocator &allocator, const ObITableGroupKey &other)
{
  int ret = OB_SUCCESS;
  const ObHbaseGroupKey &other_key = static_cast<const ObHbaseGroupKey &>(other);
  ls_id_ = other_key.ls_id_;
  table_id_ = other_key.table_id_;
  schema_version_ = other_key.schema_version_;
  op_type_ = other_key.op_type_;
  return ret;
}

bool ObHbaseGroupKey::is_equal(const ObITableGroupKey &other) const
{
  const ObHbaseGroupKey &other_key = static_cast<const ObHbaseGroupKey &>(other);
  return type_ == other.type_
    && ls_id_ == static_cast<const ObHbaseGroupKey &>(other).ls_id_
    && table_id_ == static_cast<const ObHbaseGroupKey &>(other).table_id_
    && schema_version_ == static_cast<const ObHbaseGroupKey &>(other).schema_version_
    && op_type_ == static_cast<const ObHbaseGroupKey &>(other).op_type_;
}

int ObHbaseOp::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    bool need_all_prop = ls_req_.ls_op_.need_all_prop_bitmap();
    const ObIArray<ObString>& all_rowkey_names = ls_req_.ls_op_.get_all_rowkey_names();
    const ObIArray<ObString>& all_properties_names = ls_req_.ls_op_.get_all_properties_names();
    int64_t tablet_count = ls_req_.ls_op_.count();
    if (OB_FAIL(result_.prepare_allocate(tablet_count))) {
      LOG_WARN("fail to prepare allocate results", K(ret), K(tablet_count));
    } else if (OB_FAIL(result_.assign_rowkey_names(all_rowkey_names))) {
      LOG_WARN("fail to assign rowkey names", K(ret), K(all_rowkey_names));
    } else if (!need_all_prop && OB_FAIL(result_.assign_properties_names(all_properties_names))) {
      LOG_WARN("fail to assign properties names", K(ret), K(all_properties_names));
    } else {
      bool init_once = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
        const ObTableTabletOp &tablet_op = ls_req_.ls_op_.at(i);
        ObTableTabletOpResult &tablet_result = result_.at(i);
        int64_t single_op_count = tablet_op.count();
        if (OB_FAIL(tablet_result.prepare_allocate(single_op_count))) {
          LOG_WARN("fail to prepare allocate single result", K(ret), K(single_op_count));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < single_op_count; j++) {
            ObTableSingleOpResult &single_result = tablet_result.at(j);
            single_result.set_errno(OB_SUCCESS);
            single_result.set_type(tablet_op.at(j).get_op_type());
            if (!init_once) {
              result_entity_.set_dictionary(&all_rowkey_names, &all_properties_names);
              const ObTableSingleOpEntity& req_entity= tablet_op.at(0).get_entities().at(0);
              if (OB_FAIL(result_entity_.construct_names_bitmap(req_entity))) {
                LOG_WARN("fail to construct_names_bitmap", KR(ret), K(req_entity));
              } else {
                init_once = true;
              }
            }
            single_result.set_entity(&result_entity_);
          }
        }
      }
    }
    is_inited_ = true;
  }

  return ret;
}
