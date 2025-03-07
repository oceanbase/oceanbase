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

#define USING_LOG_PREFIX STORAGE


#include "ob_tablet_split_mds_helper.h"
#include "storage/tablet/ob_tablet_split_replay_executor.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;
using namespace oceanbase::memtable;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::PartitionType;
using oceanbase::sqlclient::ObISQLConnection;
using oceanbase::observer::ObInnerSQLConnection;

namespace oceanbase
{
namespace storage
{

bool ObTabletSplitMdsArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && ls_id_.is_valid()
    && (split_data_tablet_ids_.count() == split_datas_.count() || 0 == split_datas_.count())
    && ((tablet_status_ != ObTabletStatus::NONE) ^ tablet_status_tablet_ids_.empty())
    && ((tablet_status_ != ObTabletStatus::NONE) == (tablet_status_data_type_ != ObTabletMdsUserDataType::NONE));
}

int ObTabletSplitMdsArg::assign(const ObTabletSplitMdsArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  tablet_status_ = other.tablet_status_;
  tablet_status_data_type_ = other.tablet_status_data_type_;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else if (OB_FAIL(split_data_tablet_ids_.assign(other.split_data_tablet_ids_))) {
    LOG_WARN("failed to assign tablet ids", K(ret));
  } else if (OB_FAIL(split_datas_.assign(other.split_datas_))) {
    LOG_WARN("failed to assign datas", K(ret));
  } else if (OB_FAIL(tablet_status_tablet_ids_.assign(other.tablet_status_tablet_ids_))) {
    LOG_WARN("failed to assign tablet ids", K(ret));
  } else if (OB_FAIL(set_freeze_flag_tablet_ids_.assign(other.set_freeze_flag_tablet_ids_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(autoinc_seq_arg_.assign(other.autoinc_seq_arg_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

void ObTabletSplitMdsArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  split_data_tablet_ids_.reset();
  split_datas_.reset();
  tablet_status_tablet_ids_.reset();
  tablet_status_ = ObTabletStatus::NONE;
  tablet_status_data_type_ = ObTabletMdsUserDataType::NONE;
  set_freeze_flag_tablet_ids_.reset();
  autoinc_seq_arg_.reset();
}

bool ObTabletSplitMdsArg::is_split_data_table(const ObTableSchema &table_schema)
{
  return table_schema.is_user_table() || table_schema.is_tmp_table() || table_schema.is_global_index_table();
}

template<typename F>
int ObTabletSplitMdsArg::foreach_part(const ObTableSchema &table_schema, const int64_t part_type_filter, F &&op)
{
  int ret = OB_SUCCESS;
  const bool is_hidden = schema::is_hidden_partition(static_cast<PartitionType>(part_type_filter));
  const ObPartitionLevel part_level = table_schema.get_part_level();
  const int64_t part_num = is_hidden ? table_schema.get_hidden_partition_num() : table_schema.get_partition_num();
  ObPartition **part_array = is_hidden ? table_schema.get_hidden_part_array() : table_schema.get_part_array();
  int64_t part_idx = 0;
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part array is null", K(ret), K(table_schema));
  } else {
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i), K(table_schema));
      } else if (PARTITION_LEVEL_ONE == part_level) {
        if (part_type_filter == part_array[i]->get_partition_type()) {
          if (OB_FAIL(op(part_idx, *part_array[i]))) {
            LOG_WARN("failed to op part", K(ret));
          } else {
            part_idx++;
          }
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        ObSubPartition **sub_part_array = is_hidden ? part_array[i]->get_hidden_subpart_array() : part_array[i]->get_subpart_array();
        int64_t sub_part_num = is_hidden ? part_array[i]->get_hidden_subpartition_num() : part_array[i]->get_subpartition_num();
        if (OB_ISNULL(sub_part_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array is null", K(ret), K(table_schema));
        } else {
          for (int64_t j = 0; j < sub_part_num && OB_SUCC(ret); j++) {
            if (OB_ISNULL(sub_part_array[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(j), K(table_schema));
            } else if (part_type_filter == sub_part_array[j]->get_partition_type()) {
              if (OB_FAIL(op(part_idx, *sub_part_array[j]))) {
                LOG_WARN("failed to op sub part", K(ret));
              } else {
                part_idx++;
              }
            }
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("4.0 not support part type", K(ret), K(table_schema));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsArg::get_partkey_projector(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &table_schema,
    ObIArray<uint64_t> &partkey_projector)
{
  int ret = OB_SUCCESS;
  const ObPartitionKeyInfo &partkey_info = data_table_schema.get_partition_key_info();
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  partkey_projector.reset();
  if (OB_UNLIKELY(!partkey_info.is_valid() || !rowkey_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partkey info or rowkey info", K(ret), K(partkey_info), K(rowkey_info));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partkey_info.get_size(); i++) {
    uint64_t column_id = OB_INVALID_ID;
    int64_t idx = -1;
    if (OB_FAIL(partkey_info.get_column_id(i, column_id))) {
      LOG_WARN("failed to get partkey column id", K(ret), K(partkey_info), K(i));
    } else if (OB_FAIL(rowkey_info.get_index(column_id, idx))) {
      LOG_WARN("failed to get partkey index in rowkey", K(ret), K(partkey_info), K(column_id), K(i), K(rowkey_info));
    } else if (OB_FAIL(partkey_projector.push_back(idx))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    partkey_projector.reset();
  }
  return ret;
}

int ObTabletSplitMdsArg::prepare_basic_args(
    const ObIArray<ObTableSchema *> &upd_table_schemas,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    ObIArray<ObTabletID> &src_tablet_ids,
    ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    ObIArray<ObRowkey> &dst_high_bound_vals)
{
  int ret = OB_SUCCESS;
  src_tablet_ids.reset();
  dst_tablet_ids.reset();
  dst_high_bound_vals.reset();
  if (OB_UNLIKELY(upd_table_schemas.count() != inc_table_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema count", K(ret), K(upd_table_schemas.count()), K(inc_table_schemas.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < upd_table_schemas.count(); i++) {
    ObTableSchema *table_schema = nullptr;
    if (OB_ISNULL(table_schema = upd_table_schemas.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret));
    } else if (OB_UNLIKELY((i == 0 && !is_split_data_table(*table_schema))
            || (i != 0 && !(table_schema->is_index_local_storage() || table_schema->is_aux_lob_table())))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected table schema type", K(ret), K(i), K(is_split_data_table(*table_schema)),
          K(table_schema->is_index_local_storage()), K(table_schema->is_aux_lob_table()));
    } else if (OB_UNLIKELY(i >= inc_table_schemas.count() || OB_ISNULL(inc_table_schemas.at(i))
            || table_schema->get_table_id() != inc_table_schemas.at(i)->get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid inc table schemas", K(ret), K(i), K(inc_table_schemas.count()));
    } else if (!table_schema->is_partitioned_table()) {
      if (OB_FAIL(src_tablet_ids.push_back(table_schema->get_tablet_id()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else {
      ObTabletSplitMdsArgPrepareSrcOp op(src_tablet_ids, dst_tablet_ids);
      if (OB_FAIL(foreach_part(*table_schema, PartitionType::PARTITION_TYPE_SPLIT_SOURCE, op))) {
        LOG_WARN("failed to foreach part", K(ret), K(table_schema->get_table_id()));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < inc_table_schemas.count(); i++) {
    const ObTableSchema *table_schema = nullptr;
    const ObArray<ObTabletID> empty_array;
    const bool is_data_table = i == 0;
    if (OB_ISNULL(table_schema = inc_table_schemas.at(i))
        || OB_UNLIKELY(table_schema->get_table_id() != upd_table_schemas.at(i)->get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret), K(i), K(dst_tablet_ids));
    } else if (OB_FAIL(dst_tablet_ids.push_back(empty_array))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      ObTabletSplitMdsArgPrepareDstOp op(is_data_table, dst_tablet_ids.at(i), dst_high_bound_vals);
      if (OB_FAIL(foreach_part(*table_schema, PartitionType::PARTITION_TYPE_NORMAL, op))) {
        LOG_WARN("failed to foreach part", K(ret));
      } else if (OB_UNLIKELY(dst_tablet_ids.at(i).count() != dst_tablet_ids.at(0).count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid dst tablet count", K(ret), K(dst_tablet_ids));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsArg::init_split_start_src(
    const uint64_t tenant_id,
    const bool is_oracle_mode,
    const ObLSID &ls_id,
    const ObIArray<ObTableSchema *> &new_table_schemas,
    const ObIArray<ObTableSchema *> &upd_table_schemas,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids)
{
  int ret = OB_SUCCESS;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  tablet_status_ = ObTabletStatus::SPLIT_SRC;
  tablet_status_data_type_ = ObTabletMdsUserDataType::START_SPLIT_SRC;
  if (OB_UNLIKELY(upd_table_schemas.count() != src_tablet_ids.count()
        || upd_table_schemas.count() != dst_tablet_ids.count()
        || new_table_schemas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema count", K(ret), K(upd_table_schemas.count()), K(new_table_schemas.count()),
        K(src_tablet_ids.count()), K(dst_tablet_ids.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < upd_table_schemas.count(); i++) {
    ObTabletSplitMdsUserData data;
    ObArray<uint64_t> partkey_projector;
    const ObTabletID &src_tablet_id = src_tablet_ids.at(i);
    ObTableSchema *src_table_schema = upd_table_schemas.at(i);
    ObTableSchema *new_data_table_schema = new_table_schemas.at(0);
    if (OB_ISNULL(src_table_schema) || OB_ISNULL(new_data_table_schema) || !is_split_data_table(*new_data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret));
    } else if (!src_table_schema->is_aux_lob_table()
            && OB_FAIL(get_partkey_projector(*new_data_table_schema, *src_table_schema, partkey_projector))) {
      LOG_WARN("failed to get parkey projector", K(ret));
    } else if (OB_FAIL(data.init_range_part_split_src(dst_tablet_ids.at(i), partkey_projector, *src_table_schema, is_oracle_mode))) {
      LOG_WARN("failed to set data", K(ret));
    } else if (OB_FAIL(split_data_tablet_ids_.push_back(src_tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(split_datas_.push_back(data))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(tablet_status_tablet_ids_.push_back(src_tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitMdsArg::init_set_freeze_flag(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  if (OB_FAIL(set_freeze_flag_tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsArg::init_split_start_dst(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObIArray<const ObTableSchema *> &inc_table_schemas,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    const ObIArray<ObRowkey> &dst_high_bound_vals)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  tablet_status_tablet_ids_.reset(); // split dst tablet status is NORMAL but without major, so no need to change its tablet status
  tablet_status_ = ObTabletStatus::NONE;
  tablet_status_data_type_ = ObTabletMdsUserDataType::NONE;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_table_schemas.count(); i++) {
    const ObTableSchema *inc_table_schema = inc_table_schemas.at(i);
    const ObTabletID &src_tablet_id = src_tablet_ids.at(i);
    if (OB_UNLIKELY(nullptr == inc_table_schema || dst_tablet_ids.at(i).count() != dst_high_bound_vals.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KP(inc_table_schema), K(dst_tablet_ids), K(i), K(dst_high_bound_vals.count()));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < dst_tablet_ids.at(i).count(); j++) {
      const int64_t part_idx = j;
      const ObTabletID &dst_tablet_id = dst_tablet_ids.at(i).at(part_idx);
      const ObRowkey &high_bound_val = dst_high_bound_vals.at(part_idx);
      ObTabletSplitMdsUserData data;
      if (OB_FAIL(split_data_tablet_ids_.push_back(dst_tablet_id))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (is_split_data_table(*inc_table_schema)) {
        const int64_t auto_part_size = inc_table_schema->is_auto_partitioned_table() ? inc_table_schema->get_auto_part_size() : OB_INVALID_SIZE;
        ObDatumRowkey end_partkey;
        allocator.reuse();
        if (OB_FAIL(end_partkey.from_rowkey(high_bound_val, allocator))) {
          LOG_WARN("failed to convert", K(ret));
        } else if (OB_FAIL(data.init_range_part_split_dst(auto_part_size, src_tablet_id, end_partkey))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(split_datas_.push_back(data))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (inc_table_schema->is_index_local_storage()) {
        ObDatumRowkey data_tablet_end_partkey;
        if (OB_UNLIKELY(part_idx >= split_datas_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid data cnt", K(ret), K(*this), K(part_idx));
        } else if (OB_FAIL(split_datas_[part_idx].get_end_partkey(data_tablet_end_partkey))) {
          LOG_WARN("failed to get data tablet's end partkey", K(ret));
        } else if (OB_FAIL(data.init_range_part_split_dst(OB_INVALID_SIZE/*auto_part_size*/, src_tablet_id, data_tablet_end_partkey))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(split_datas_.push_back(data))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (inc_table_schema->is_aux_lob_table()) {
        ObDatumRowkey empty_partkey;
        if (OB_FAIL(data.init_range_part_split_dst(OB_INVALID_SIZE/*auto_part_size*/, src_tablet_id, empty_partkey))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(split_datas_.push_back(data))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type", K(ret), K(inc_table_schema->get_table_id()));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsArg::set_autoinc_seq_arg(const obrpc::ObBatchSetTabletAutoincSeqArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(autoinc_seq_arg_.assign(arg))) {
    LOG_WARN("failed to assign", K(ret), K(arg));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!autoinc_seq_arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid autoinc seq arg", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsArg::init_split_end_src(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &src_data_tablet_id,
    const ObIArray<ObTabletID> &src_local_index_tablet_ids,
    const ObIArray<ObTabletID> &src_lob_tablet_ids)
{
  int ret = OB_SUCCESS;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  split_data_tablet_ids_.reset();
  split_datas_.reset();
  tablet_status_tablet_ids_.reset();
  tablet_status_ = ObTabletStatus::SPLIT_SRC_DELETED;
  tablet_status_data_type_ = ObTabletMdsUserDataType::FINISH_SPLIT_SRC;
  if (OB_FAIL(tablet_status_tablet_ids_.push_back(src_data_tablet_id))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(append(tablet_status_tablet_ids_, src_local_index_tablet_ids))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(append(tablet_status_tablet_ids_, src_lob_tablet_ids))) {
    LOG_WARN("failed to append", K(ret));
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

int ObTabletSplitMdsArg::init_split_end_dst(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t auto_part_size,
    const ObIArray<ObTabletID> &dst_data_tablet_ids,
    const ObIArray<ObSArray<ObTabletID>> &dst_local_index_tablet_ids,
    const ObIArray<ObSArray<ObTabletID>> &dst_lob_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletSplitMdsUserData data;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  tablet_status_tablet_ids_.reset();
  tablet_status_ = ObTabletStatus::NORMAL;
  tablet_status_data_type_ = ObTabletMdsUserDataType::FINISH_SPLIT_DST;

  // data tablets
  if (OB_FAIL(data.init_no_split(auto_part_size))) {
    LOG_WARN("failed to set split data", K(ret));
  } else if (OB_FAIL(append(split_data_tablet_ids_, dst_data_tablet_ids))) {
    LOG_WARN("failed to assign", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dst_data_tablet_ids.count(); i++) {
    if (OB_FAIL(split_datas_.push_back(data))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  // local index tablets
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data.init_no_split(OB_INVALID_SIZE))) {
    LOG_WARN("failed to set split data", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dst_local_index_tablet_ids.count(); i++) {
    if (OB_FAIL(append(split_data_tablet_ids_, dst_local_index_tablet_ids.at(i)))) {
      LOG_WARN("failed to assign", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < dst_local_index_tablet_ids.at(i).count(); j++) {
      if (OB_FAIL(split_datas_.push_back(data))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  // lob tablets
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data.init_no_split(OB_INVALID_SIZE))) {
    LOG_WARN("failed to set split data", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dst_lob_tablet_ids.count(); i++) {
    if (OB_FAIL(append(split_data_tablet_ids_, dst_lob_tablet_ids.at(i)))) {
      LOG_WARN("failed to assign", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < dst_lob_tablet_ids.at(i).count(); j++) {
      if (OB_FAIL(split_datas_.push_back(data))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_status_tablet_ids_.assign(split_data_tablet_ids_))) {
    LOG_WARN("failed to push back", K(ret));
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

int ObTabletSplitMdsArg::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletSplitMdsUserData> &split_datas)
{
  int ret = OB_SUCCESS;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  if (OB_FAIL(split_data_tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(split_datas_.assign(split_datas))) {
    LOG_WARN("failed to assign", K(ret));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletSplitMdsArg, tenant_id_, ls_id_, split_data_tablet_ids_, split_datas_, tablet_status_tablet_ids_, tablet_status_, tablet_status_data_type_, set_freeze_flag_tablet_ids_, autoinc_seq_arg_);

int ObTabletSplitMdsArgPrepareSrcOp::operator()(const int64_t part_idx, ObBasePartition &part)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != part_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support one split src part now", K(ret), K(part_idx), K(src_tablet_ids_), K(part.get_tablet_id()));
  } else if (OB_FAIL(src_tablet_ids_.push_back(part.get_tablet_id()))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsArgPrepareDstOp::operator()(const int64_t part_idx, ObBasePartition &part)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_tablet_ids_.push_back(part.get_tablet_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (is_data_table_) {
    if (OB_FAIL(dst_high_bound_vals_.push_back(part.get_high_bound_val()))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::get_valid_timeout(const int64_t abs_timeout_us, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  if (abs_timeout_us == 0) { // e.g., select for update nowait
    timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S;
  } else if (OB_UNLIKELY(abs_timeout_us <= cur_time)) {
    ret = OB_TIMEOUT;
    LOG_WARN("timed out", K(ret), K(abs_timeout_us), K(cur_time));
  } else {
    timeout_us = abs_timeout_us - cur_time;
  }
  return ret;
}

int ObTabletSplitMdsHelper::get_split_data_with_timeout(ObTablet &tablet, ObTabletSplitMdsUserData &split_data, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
    LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
  } else if (OB_FAIL(tablet.ObITabletMdsInterface::get_split_data(split_data, timeout_us))) {
    LOG_WARN("failed to get split data", K(ret), K(abs_timeout_us), K(timeout_us), K(tablet.get_tablet_meta()));
  }
  return ret;
}

int ObTabletSplitMdsHelper::prepare_calc_split_dst(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t abs_timeout_us,
    ObTabletSplitMdsUserData &src_split_data,
    ObIArray<ObTabletSplitMdsUserData> &dst_split_datas)
{
  int ret = OB_SUCCESS;
  const ObTabletID &src_tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObArray<ObTabletID> dst_tablet_ids;
  ObTabletHandle dst_tablet_handle;
  src_split_data.reset();
  dst_split_datas.reset();
  if (OB_FAIL(get_split_data_with_timeout(tablet, src_split_data, abs_timeout_us))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_UNLIKELY(!src_split_data.is_split_src())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not split src", K(ret), K(src_tablet_id), K(src_split_data));
  } else if (OB_FAIL(src_split_data.get_split_dst_tablet_ids(dst_tablet_ids))) {
    LOG_WARN("failed to get split dst tablet ids", K(ret));
  } else if (OB_FAIL(dst_split_datas.prepare_allocate(dst_tablet_ids.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_tablet_ids.count(); i++) {
      dst_tablet_handle.reset();
      if (OB_FAIL(ls.get_tablet_with_timeout(dst_tablet_ids.at(i), dst_tablet_handle, abs_timeout_us))) {
        LOG_WARN("failed to get split dst tablet", K(ret));
      } else if (OB_FAIL(get_split_data_with_timeout(*dst_tablet_handle.get_obj(), dst_split_datas.at(i), abs_timeout_us))) {
        LOG_WARN("failed to get split dst split data", K(ret));
      } else if (OB_UNLIKELY(!dst_split_datas.at(i).is_split_dst())) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("no longer split dst", K(ret), K(src_tablet_id), K(dst_tablet_ids.at(i)), K(dst_split_datas.at(i)));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::calc_split_dst(
    ObLS &ls,
    ObTablet &tablet,
    const blocksstable::ObDatumRowkey &rowkey,
    const int64_t abs_timeout_us,
    common::ObTabletID &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletSplitMdsUserData split_data;
  if (OB_FAIL(get_split_data_with_timeout(tablet, split_data, abs_timeout_us))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(split_data.calc_split_dst(ls, rowkey, abs_timeout_us, dst_tablet_id))) {
    LOG_WARN("failed to calc split dst tablet id", K(ret), K(tablet.get_tablet_meta()));
  }
  return ret;
}

int ObTabletSplitMdsHelper::calc_split_dst_lob(
    ObLS &ls,
    ObTablet &tablet,
    const blocksstable::ObDatumRow &data_row,
    const int64_t abs_timeout_us,
    ObTabletID &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  const ObTabletID &src_tablet_id = tablet_meta.tablet_id_;
  const ObTabletID &data_tablet_id = tablet.get_data_tablet_id();
  ObTabletHandle data_tablet_handle;
  ObTabletHandle dst_data_tablet_handle;
  ObTabletBindingMdsUserData ddl_data;
  ObTabletBindingMdsUserData dst_ddl_data;
  ObTabletSplitMdsUserData split_data;
  ObTabletID dst_data_tablet_id;
  int64_t timeout_us = 0;
  if (OB_FAIL(ls.get_tablet_with_timeout(data_tablet_id, data_tablet_handle, abs_timeout_us, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_meta));
  } else {
    const int64_t rowkey_column_cnt = data_tablet_handle.get_obj()->get_rowkey_read_info().get_schema_rowkey_count();
    ObDatumRowkey data_rowkey;
    if (OB_FAIL(data_rowkey.assign(data_row.storage_datums_, rowkey_column_cnt))) {
      LOG_WARN("failed to assign datum rowkey", K(ret), K(data_row), K(rowkey_column_cnt));
    } else if (OB_FAIL(calc_split_dst(ls, *data_tablet_handle.get_obj(), data_rowkey, abs_timeout_us, dst_data_tablet_id))) {
      LOG_WARN("failed to get split data", K(ret), K(tablet_meta));
    } else if (OB_FAIL(ls.get_tablet_with_timeout(dst_data_tablet_id, dst_data_tablet_handle,
            abs_timeout_us, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_meta), K(dst_data_tablet_id));
    } else if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
      LOG_WARN("failed to get valid timeout", K(ret));
    } else if (OB_FAIL(data_tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(
            share::SCN::max_scn(), ddl_data, timeout_us))) {
      LOG_WARN("failed to get ddl data", K(ret), K(tablet_meta));
    } else if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
      LOG_WARN("failed to get valid timeout", K(ret));
    } else if (OB_FAIL(dst_data_tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(
            share::SCN::max_scn(), dst_ddl_data, timeout_us))) {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_meta), K(dst_data_tablet_id));
    } else {
      if (src_tablet_id == ddl_data.lob_meta_tablet_id_) {
        dst_tablet_id = dst_ddl_data.lob_meta_tablet_id_;
      } else if (src_tablet_id == ddl_data.lob_piece_tablet_id_) {
        dst_tablet_id = dst_ddl_data.lob_piece_tablet_id_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob tablet id", K(ret), K(src_tablet_id), K(data_tablet_id),
            K(dst_data_tablet_id), K(ddl_data), K(dst_ddl_data));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::get_split_info(ObTablet &tablet, ObIAllocator &allocator, ObTabletSplitTscInfo &split_info)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  const ObLSID &ls_id = tablet_meta.ls_id_;
  const ObTabletID &tablet_id = tablet_meta.tablet_id_;
  ObTabletSplitMdsUserData data;
  const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S;
  if (OB_FAIL(get_split_data_with_timeout(tablet, data, abs_timeout_us))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (data.is_split_dst()) {
    ObLSService *ls_service = nullptr;
    ObLSHandle ls_handle;
    if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls", K(ret), K(ls_id));
    } else if (OB_FAIL(data.get_tsc_split_info(tablet_id, *ls_handle.get_ls(), abs_timeout_us, allocator, split_info))) {
      LOG_WARN("failed to get split info", K(ret));
    }
  } else {
    LOG_INFO("not split dst", K(ret), K(tablet_meta), K(data));
  }
  return ret;
}

int ObTabletSplitMdsHelper::get_is_spliting(ObTablet &tablet, bool &is_split_dst)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  is_split_dst = false;
  if (OB_UNLIKELY(!tablet_meta.table_store_flag_.with_major_sstable())) {
    const int64_t timeout = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S;
    ObTabletSplitMdsUserData data;
    if (OB_FAIL(tablet.ObITabletMdsInterface::get_split_data(data, timeout))) {
      LOG_WARN("failed to get split data", K(ret));
    } else if (data.is_split_dst()) {
      is_split_dst = true;
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::batch_get_tablet_split(
    const int64_t abs_timeout_us,
    const ObBatchGetTabletSplitArg &arg,
    ObBatchGetTabletSplitRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSService *ls_service = MTL(ObLSService *);
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObRole role = INVALID_ROLE;
      if (OB_ISNULL(ls_service) || OB_ISNULL(log_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls_service or log_service", K(ret));
      } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls", K(ret), K(arg.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls not leader", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); i++) {
          const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
          ObTabletHandle tablet_handle;
          ObTabletSplitMdsUserData data;

          if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(abs_timeout_us));
          } else if (OB_FAIL(get_split_data_with_timeout(*tablet_handle.get_obj(), data, abs_timeout_us))) {
            LOG_WARN("failed to get split data", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(res.split_datas_.push_back(data))) {
            LOG_WARN("failed to push back", K(ret));
          }

          // currently not support to read uncommitted mds set by this transaction, so check and avoid such usage
          if (OB_SUCC(ret) && arg.check_committed_) {
            ObTabletSplitMdsUserData tmp_data;
            mds::MdsWriter writer;// will be removed later
            mds::TwoPhaseCommitState trans_stat;// will be removed later
            share::SCN trans_version;// will be removed later
            if (OB_FAIL(tablet_handle.get_obj()->get_latest_split_data(tmp_data, writer, trans_stat, trans_version))) {
              if (OB_EMPTY_RESULT == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get latest split data", K(ret));
              }
            } else if (OB_UNLIKELY(mds::TwoPhaseCommitState::ON_COMMIT != trans_stat)) {
              ret = OB_EAGAIN;
              LOG_WARN("check committed failed", K(ret), K(tenant_id), K(arg.ls_id_), K(tablet_id), K(tmp_data));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::get_tablet_split_mds_by_rpc(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletSplitMdsUserData> &datas)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  share::ObLocationService *location_service = nullptr;
  ObAddr leader_addr;
  obrpc::ObBatchGetTabletSplitArg arg;
  obrpc::ObBatchGetTabletSplitRes res;
  if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", K(ret), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id, tablet_ids, true/*check_committed*/))) {
    LOG_WARN("failed to init arg", K(ret), K(tenant_id), K(ls_id), K(ls_id));
  } else {
    bool force_renew = false;
    bool finish = false;
    for (int64_t retry_times = 0; OB_SUCC(ret) && !finish; retry_times++) {
      int64_t timeout_us = 0;
      if (OB_FAIL(location_service->get_leader(cluster_id, tenant_id, ls_id, force_renew, leader_addr))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
        LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
      } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(timeout_us).batch_get_tablet_split(arg, res))) {
        LOG_WARN("fail to batch get tablet split", K(ret), K(retry_times), K(abs_timeout_us));
      } else {
        finish = true;
      }
      if (OB_FAIL(ret)) {
        force_renew = true;
        if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_GET_LOCATION_TIME_OUT == ret || OB_NOT_MASTER == ret
            || OB_ERR_SHARED_LOCK_CONFLICT == ret || OB_LS_OFFLINE == ret
            || OB_NOT_INIT == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret || OB_LS_LOCATION_NOT_EXIST == ret) {
          // overwrite ret
          if (OB_UNLIKELY(ObTimeUtility::current_time() > abs_timeout_us)) {
            ret = OB_TIMEOUT;
            LOG_WARN("timeout", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret), K(abs_timeout_us));
          } else if (retry_times >= 3) {
            ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(100 * 1000L); // 100ms
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(datas.assign(res.split_datas_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsHelper::set_auto_part_size_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &create_arg,
    const ObIArray<int64_t> &auto_part_size_arr,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!auto_part_size_arr.empty()) {
    const ObLSID &ls_id = create_arg.id_;
    ObTabletSplitMdsArg arg;
    ObArray<ObTabletID> split_data_tablet_ids;
    ObArray<ObTabletSplitMdsUserData> split_datas;
    ObTabletSplitMdsUserData tmp_data;
    if (OB_UNLIKELY(!create_arg.table_schemas_.empty() || create_arg.create_tablet_schemas_.count() != auto_part_size_arr.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg for set auto part size", K(ret), K(create_arg.table_schemas_.count()), K(create_arg.create_tablet_schemas_.count()),
          K(auto_part_size_arr.count()), K(create_arg));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < create_arg.tablets_.count(); i++) {
      const ObIArray<ObTabletID> &tablet_ids = create_arg.tablets_[i].tablet_ids_;
      const ObIArray<int64_t> &table_schema_idx_arr = create_arg.tablets_[i].table_schema_index_;
      if (OB_UNLIKELY(tablet_ids.count() != table_schema_idx_arr.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table schema idx arr", K(ret), K(tablet_ids.count()), K(table_schema_idx_arr.count()));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); j++) {
        const ObTabletID &tablet_id = tablet_ids.at(j);
        const int64_t schema_idx = table_schema_idx_arr.at(j);
        if (OB_UNLIKELY(schema_idx >= auto_part_size_arr.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("schema idx out of range", K(ret), K(i), K(j), K(create_arg), K(auto_part_size_arr));
        } else {
          const int64_t auto_part_size = auto_part_size_arr.at(schema_idx);
          if (OB_INVALID_SIZE != auto_part_size) {
            if (OB_FAIL(tmp_data.init_no_split(auto_part_size))) {
              LOG_WARN("failed to set split data", K(ret));
            } else if (OB_FAIL(split_data_tablet_ids.push_back(tablet_id))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (OB_FAIL(split_datas.push_back(tmp_data))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (split_data_tablet_ids.count() >= ObTabletSplitMdsArg::BATCH_TABLET_CNT) {
              if (OB_FAIL(arg.init(tenant_id, ls_id, split_data_tablet_ids, split_datas))) {
                LOG_WARN("failed to init tablet split mds arg", K(ret));
              } else if (OB_FAIL(register_mds(arg, false/*need_flush_redo*/, trans))) {
                LOG_WARN("failed to register mds", K(ret));
              } else {
                split_data_tablet_ids.reuse();
                split_datas.reuse();
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !split_data_tablet_ids.empty()) {
      if (OB_FAIL(arg.init(tenant_id, ls_id, split_data_tablet_ids, split_datas))) {
        LOG_WARN("failed to init tablet split mds arg", K(ret));
      } else if (OB_FAIL(register_mds(arg, false/*need_flush_redo*/, trans))) {
        LOG_WARN("failed to register mds", K(ret));
      }
    }
  }
  return ret;
}

int ObModifyAutoPartSizeOp::operator()(ObTabletSplitMdsUserData &data)
{
  return data.modify_auto_part_size(auto_part_size_);
}

int ObTabletSplitMdsHelper::modify_auto_part_size(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t auto_part_size,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObModifyAutoPartSizeOp op(auto_part_size);
  if (OB_FAIL(modify_tablet_split_(tenant_id, tablet_ids, abs_timeout_us, op, trans))) {
    LOG_WARN("failed to modify table split", K(ret));
  }
  return ret;
}

template<typename F>
int ObTabletSplitMdsHelper::modify_tablet_split_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!tablet_ids.empty()) {
    ObTabletSplitMdsArg arg;
    ObArray<std::pair<ObLSID, ObTabletID>> ls_tablets;
    ObArray<ObTabletID> this_batch_tablet_ids;
    if (OB_FAIL(ObTabletBindingMdsHelper::get_sorted_ls_tablets(tenant_id, tablet_ids, ls_tablets, trans))) {
      LOG_WARN("failed to get sorted ls tablets", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); i++) {
      const ObLSID &ls_id = ls_tablets.at(i).first;
      const ObTabletID &tablet_id = ls_tablets.at(i).second;
      const bool is_last_or_next_ls_id_changed = i == ls_tablets.count() - 1 || ls_id != ls_tablets.at(i+1).first;
      if (OB_FAIL(this_batch_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (is_last_or_next_ls_id_changed || this_batch_tablet_ids.count() >= ObTabletSplitMdsArg::BATCH_TABLET_CNT) {

        ObArray<ObTabletSplitMdsUserData> datas;
        if (OB_FAIL(get_tablet_split_mds_by_rpc(tenant_id, ls_id, this_batch_tablet_ids, abs_timeout_us, datas))) {
          LOG_WARN("failed to get tablet binding mds", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < datas.count(); j++) {
          ret = op(datas.at(j));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(arg.init(tenant_id, ls_id, this_batch_tablet_ids, datas))) {
          LOG_WARN("failed to init", K(ret));
        } else if (OB_FAIL(register_mds(arg, false/*need_flush_redo*/, trans))) {
          LOG_WARN("failed to register mds", K(ret));
        }

        if (OB_SUCC(ret)) {
          this_batch_tablet_ids.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!this_batch_tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch not consumed out", K(ret), K(this_batch_tablet_ids));
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::register_mds(
    const ObTabletSplitMdsArg &arg,
    const bool need_flush_redo,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *isql_conn = nullptr;
  if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection", K(ret));
  } else {
    const int64_t size = arg.get_serialize_size();
    ObArenaAllocator allocator;
    char *buf = nullptr;
    int64_t pos = 0;
    ObRegisterMdsFlag flag;
    flag.need_flush_redo_instantly_ = need_flush_redo;
    flag.mds_base_scn_.reset();
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize arg", K(ret));
    } else if (OB_FAIL(static_cast<ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, ObTxDataSourceType::TABLET_SPLIT, buf, pos, flag))) {
      LOG_WARN("failed to register mds", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletSplitMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsHelper::on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletSplitMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, scn, ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsHelper::modify(
    const ObTabletSplitMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const bool is_reset_split_data = arg.split_datas_.empty();
  const share::ObLSID &ls_id = arg.ls_id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool need_empty_shell_trigger = false;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(arg), KP(ls));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.split_data_tablet_ids_.count(); i++) {
    const ObTabletID &tablet_id = arg.split_data_tablet_ids_[i];
    if (is_reset_split_data) {
      ObTabletSplitMdsUserData data;
      if (OB_FAIL(set_tablet_split_mds(ls_id, tablet_id, scn, data, ctx))) {
        LOG_WARN("failed to modify", K(ret), K(ls_id), K(tablet_id), K(scn));
      }
    } else {
      const ObTabletSplitMdsUserData &data = arg.split_datas_[i]; // index range is checked by is_valid()
      if (OB_FAIL(set_tablet_split_mds(ls_id, tablet_id, scn, data, ctx))) {
        LOG_WARN("failed to modify", K(ret), K(ls_id), K(tablet_id), K(scn));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_status_tablet_ids_.count(); i++) {
    const ObTabletID &tablet_id = arg.tablet_status_tablet_ids_[i];
    if (OB_FAIL(set_tablet_status(*ls, tablet_id, arg.tablet_status_, arg.tablet_status_data_type_, scn, ctx))) {
      LOG_WARN("failed to modify", K(ret), K(ls_id), K(tablet_id), K(scn));
    } else if (ObTabletStatus::SPLIT_SRC_DELETED == arg.tablet_status_) {
      need_empty_shell_trigger = true;
    }
  }

  if (OB_SUCC(ret) && arg.autoinc_seq_arg_.is_valid()) {
    if (OB_FAIL(ObTabletAutoincSeqRpcHandler::get_instance().batch_set_tablet_autoinc_seq_in_trans(*ls, arg.autoinc_seq_arg_, scn, ctx))) {
      LOG_WARN("failed to batch set tablet autoinc seq", K(ret), K(scn));
    }
  }

  if (OB_SUCC(ret) && need_empty_shell_trigger) {
    if (OB_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(ls_id))) {
      LOG_WARN("failed to set_tablet_empty_shell_trigger", K(ret), K(arg));
    }
  }

  if (scn.is_valid()) { // on_replay
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.set_freeze_flag_tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = arg.set_freeze_flag_tablet_ids_[i];
      if (OB_FAIL(set_freeze_flag(*ls, tablet_id, scn))) {
        LOG_WARN("failed to set freeze flag", K(ret), K(tablet_id), K(scn));
      }
    }
  }
  LOG_INFO("modify tablet split data", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  return ret;
}

int ObTabletSplitMdsHelper::set_tablet_split_mds(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    const ObTabletSplitMdsUserData &data,
    mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  if (!replay_scn.is_valid()) {
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret));
    } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::set(data, user_ctx, 0/*lock_timeout_us*/))) {
      LOG_WARN("failed to set mds data", K(ret));
    }
  } else {
    ObTabletSplitReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, replay_scn, data))) {
      LOG_WARN("failed to init replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(replay_scn, ls_id, tablet_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay mds", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::set_freeze_flag(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObArray<ObTableHandleV2> memtables;
  if (OB_FAIL(ObTabletBindingHelper::get_tablet_for_new_mds(ls, tablet_id, replay_scn, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_all_memtables_from_memtable_mgr(memtables))) {
    LOG_WARN("failed to get_memtable_mgr for get all memtable", K(ret), KPC(tablet));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      ObITable *table = memtables.at(i).get_table();
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table in tables_handle is invalid", K(ret), KP(table));
      } else {
        ObMemtable *memtable = static_cast<ObMemtable *>(table);
        if (memtable->is_active_memtable()) {
          memtable->set_transfer_freeze(replay_scn);
          LOG_INFO("succ set transfer freeze", K(ls_id), K(tablet_id), KP(memtable), K(replay_scn));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitMdsHelper::set_tablet_status(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const ObTabletStatus tablet_status,
    const ObTabletMdsUserDataType data_type,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;// will be removed later
  mds::TwoPhaseCommitState trans_stat;// will be removed later
  share::SCN trans_version;// will be removed later
  if (OB_FAIL(ObTabletBindingHelper::get_tablet_for_new_mds(ls, tablet_id, replay_scn, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet));
  } else if (OB_UNLIKELY(trans_stat != mds::TwoPhaseCommitState::ON_COMMIT)) {
    ret = OB_EAGAIN;
    LOG_WARN("tablet status not committed, retry", K(ret), K(user_data), KPC(tablet));
  } else {
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
    user_data.data_type_ = data_type;
    user_data.tablet_status_ = tablet_status;
    if (!replay_scn.is_valid()) {
      if (OB_FAIL(ls.get_tablet_svr()->set_tablet_status(tablet_id, user_data, user_ctx))) {
        LOG_WARN("failed to set user data", K(ret), K(user_data), KPC(tablet));
      }
    } else {
      if (OB_FAIL(ls.get_tablet_svr()->replay_set_tablet_status(tablet_id, replay_scn, user_data, user_ctx))) {
        LOG_WARN("failed to set user data", K(ret), K(user_data), KPC(tablet));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
