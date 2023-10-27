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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_location_router.h"
#include "sql/das/ob_das_define.h"
#include "share/ob_ls_id.h"
#include "observer/ob_server_struct.h"
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/ob_sql_context.h"
#include "storage/tx/wrs/ob_black_list.h"
#include "lib/rc/context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace transaction;
namespace sql
{
OB_SERIALIZE_MEMBER(DASRelatedTabletMap::MapEntry,
                    key_.src_tablet_id_,
                    key_.related_table_id_,
                    val_.tablet_id_,
                    val_.part_id_,
                    val_.first_level_part_id_);

int VirtualSvrPair::init(ObIAllocator &allocator,
                         ObTableID vt_id,
                         ObIArray<ObAddr> &part_locations)
{
  int ret = OB_SUCCESS;
  all_server_.set_allocator(&allocator);
  all_server_.set_capacity(part_locations.count());
  table_id_ = vt_id;
  if (OB_FAIL(all_server_.assign(part_locations))) {
    LOG_WARN("store addr to all server list failed", K(ret));
  }
  return ret;
}

int VirtualSvrPair::get_server_by_tablet_id(const ObTabletID &tablet_id, ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  //tablet id must start with 1, so the server addr index is (tablet_id - 1) in virtual table
  int64_t idx = tablet_id.id() - 1;
  if (VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID == tablet_id.id()) {
    addr = GCTX.self_addr();
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(idx < 0)
      || OB_UNLIKELY(idx >= all_server_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_id is invalid", K(ret), K(tablet_id), K(all_server_));
  } else {
    addr = all_server_.at(idx);
  }
  return ret;
}

int VirtualSvrPair::get_all_part_and_tablet_id(ObIArray<ObObjectID> &part_ids,
                                               ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_server_.count(); ++i) {
    //tablet id must start with 1,
    //so use the all server index + 1 as the tablet id and partition_id in virtual table
    if (OB_FAIL(part_ids.push_back(i + 1))) {
      LOG_WARN("mock part id failed", K(ret), K(i));
    } else if (OB_FAIL(tablet_ids.push_back(ObTabletID(i + 1)))) {
      LOG_WARN("mock tablet id failed", K(ret), K(i));
    }
  }
  return ret;
}

int VirtualSvrPair::get_part_and_tablet_id_by_server(const ObAddr &addr,
                                                     ObObjectID &part_id,
                                                     ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < all_server_.count(); ++i) {
    if (addr == all_server_.at(i)) {
      //tablet id must start with 1,
      //so use the all server index + 1 as the tablet id and partition_id in virtual table
      part_id = i + 1;
      tablet_id = i + 1;
      break;
    }
  }
  if (!tablet_id.is_valid()) {
    LOG_DEBUG("virtual table partition not exists", K(ret), K(addr));
  }

  return ret;
}

void VirtualSvrPair::get_default_tablet_and_part_id(ObTabletID &tablet_id, ObObjectID &part_id) const
{
  //tablet id must start with 1, （0 is invalid tablet id）
  part_id = VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID;
  tablet_id = VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID;
}

int DASRelatedTabletMap::add_related_tablet_id(ObTabletID src_tablet_id,
                                               ObTableID related_table_id,
                                               ObTabletID related_tablet_id,
                                               ObObjectID related_part_id,
                                               ObObjectID related_first_level_part_id)
{
  int ret = OB_SUCCESS;
  if (nullptr == get_related_tablet_id(src_tablet_id, related_table_id)) {
    MapEntry map_entry;
    map_entry.key_.src_tablet_id_ = src_tablet_id;
    map_entry.key_.related_table_id_ = related_table_id;
    map_entry.val_.tablet_id_ = related_tablet_id;
    map_entry.val_.part_id_ = related_part_id;
    map_entry.val_.first_level_part_id_ = related_first_level_part_id;
    if (OB_FAIL(list_.push_back(map_entry))) {
      LOG_WARN("store the related tablet entry failed", K(ret), K(map_entry));
    } else if (list_.size() > FAST_LOOP_LIST_LEN) {
      //The length of the list is already long enough,
      //and searching through it using iteration will be slow.
      //Therefore, constructing a map can accelerate the search in this situation.
      if (OB_FAIL(insert_related_tablet_map())) {
        LOG_WARN("create related tablet map failed", K(ret));
      }
    }
  }
  return ret;
}

const DASRelatedTabletMap::Value *DASRelatedTabletMap::get_related_tablet_id(ObTabletID src_tablet_id,
                                                                             ObTableID related_table_id)
{
  const Value *val = nullptr;
  if (list_.size() > FAST_LOOP_LIST_LEN) {
    Key tmp_key;
    tmp_key.src_tablet_id_ = src_tablet_id;
    tmp_key.related_table_id_ = related_table_id;
    Value* const *val_ptr = map_.get(&tmp_key);
    val = (val_ptr != nullptr ? *val_ptr : nullptr);
  } else {
    MapEntry *final_entry = nullptr;
    FOREACH_X(node, list_, final_entry == nullptr) {
      MapEntry &entry = *node;
      if (entry.key_.src_tablet_id_ == src_tablet_id &&
          entry.key_.related_table_id_ == related_table_id) {
        final_entry = &entry;
      }
    }
    if (OB_LIKELY(final_entry != nullptr)) {
      val = &final_entry->val_;
    }
  }
  return val;
}

int DASRelatedTabletMap::assign(const RelatedTabletList &list)
{
  int ret = OB_SUCCESS;
  clear();
  FOREACH_X(node, list, OB_SUCC(ret)) {
    const MapEntry &entry = *node;
    if (OB_FAIL(add_related_tablet_id(entry.key_.src_tablet_id_,
                                      entry.key_.related_table_id_,
                                      entry.val_.tablet_id_,
                                      entry.val_.part_id_,
                                      entry.val_.first_level_part_id_))) {
      LOG_WARN("add related tablet id failed", K(ret), K(entry));
    }
  }
  return ret;
}

int DASRelatedTabletMap::insert_related_tablet_map()
{
  int ret = OB_SUCCESS;
  if (!map_.created()) {
    if (OB_FAIL(map_.create(1000, "DASRelTblKey", "DASRelTblVal", MTL_ID()))) {
      LOG_WARN("create related tablet map failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (map_.empty()) {
      FOREACH_X(node, list_, OB_SUCC(ret)) {
        MapEntry &entry = *node;
        if (OB_FAIL(map_.set_refactored(&entry.key_, &entry.val_))) {
          LOG_WARN("insert entry to map failed", K(ret), K(entry));
        }
      }
    } else if (!list_.empty()) {
      MapEntry &final_entry = list_.get_last();
      if (OB_FAIL(map_.set_refactored(&final_entry.key_, &final_entry.val_))) {
        LOG_WARN("insert final entry to map failed", K(ret), K(final_entry));
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::get_tablet_and_object_id(
    const ObPartitionLevel part_level,
    const ObPartID part_id,
    const ObNewRange &range,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &object_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 4> tmp_tablet_ids;
  ObSEArray<ObObjectID, 4> tmp_part_ids;
  if (OB_NOT_NULL(table_schema_)) {
    share::schema::RelatedTableInfo *related_info_ptr = nullptr;
    if (related_info_.related_tids_ != nullptr && !related_info_.related_tids_->empty()) {
      related_info_ptr = &related_info_;
    }
    if (OB_FAIL(ret)) {
    } else if (table_schema_->is_external_table()) {
      if (OB_FAIL(tmp_tablet_ids.push_back(ObTabletID(ObTabletID::INVALID_TABLET_ID)))) {
        LOG_WARN("fail to push back tablet_id", KR(ret));
      } else if (OB_FAIL(tmp_part_ids.push_back(table_schema_->get_object_id()))) {
        LOG_WARN("fail to push back object_id", KR(ret));
      }
    } else if (PARTITION_LEVEL_ZERO == part_level) {
      ObTabletID tablet_id;
      ObObjectID object_id;
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_object_id(
          *table_schema_, tablet_id, object_id, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and object_id", KR(ret), KPC_(table_schema));
      } else if (OB_FAIL(tmp_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("fail to push back tablet_id", KR(ret), K(tablet_id));
      } else if (OB_FAIL(tmp_part_ids.push_back(object_id))) {
        LOG_WARN("fail to push back object_id", KR(ret), K(object_id));
      }
    } else if (PARTITION_LEVEL_ONE == part_level) {
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_part_id(
          *table_schema_, range, tmp_tablet_ids, tmp_part_ids, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and part_id", KR(ret), K(range), KPC_(table_schema));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_subpart_id(
          *table_schema_, part_id, range, tmp_tablet_ids, tmp_part_ids, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and part_id", KR(ret), K(part_id), K(range), KPC_(table_schema));
      } else if (OB_FAIL(set_partition_id_map(part_id, tmp_part_ids))) {
        LOG_WARN("failed to set partition id map");
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part level", KR(ret), K(part_level));
    }
    OZ(append_array_no_dup(tablet_ids, tmp_tablet_ids));
    OZ(append_array_no_dup(object_ids, tmp_part_ids));
  } else {
    if (part_level == PARTITION_LEVEL_TWO) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("virtual table with subpartition table not supported", KR(ret), KPC(vt_svr_pair_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual table with subpartition table");
    } else if (!range.is_whole_range()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("virtual table get tablet_id only with whole range is supported", KR(ret), KPC(vt_svr_pair_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual table get tablet_id with precise range info");
    } else if (OB_FAIL(vt_svr_pair_->get_all_part_and_tablet_id(object_ids, tablet_ids))) {
      LOG_WARN("get all part and tablet id failed", K(ret));
    } else if (OB_FAIL(mock_vtable_related_tablet_id_map(tablet_ids, object_ids))) {
      LOG_WARN("fail to mock vtable related tablet id map", KR(ret), K(tablet_ids), K(object_ids));
    }
  }
  return ret;
}

int ObDASTabletMapper::get_tablet_and_object_id(const ObPartitionLevel part_level,
                                                const ObPartID part_id,
                                                const ObNewRow &row,
                                                ObTabletID &tablet_id,
                                                ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  tablet_id = ObTabletID::INVALID_TABLET_ID;
  if (OB_NOT_NULL(table_schema_)) {
    share::schema::RelatedTableInfo *related_info_ptr = nullptr;
    if (related_info_.related_tids_ != nullptr && !related_info_.related_tids_->empty()) {
      related_info_ptr = &related_info_;
    }
    if (OB_FAIL(ret)) {
    } else if (PARTITION_LEVEL_ZERO == part_level) {
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_object_id(
          *table_schema_, tablet_id, object_id, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and object_id", KR(ret), KPC_(table_schema));
      }
    } else if (PARTITION_LEVEL_ONE == part_level) {
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_part_id(
          *table_schema_, row, tablet_id, object_id, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and part_id", KR(ret), K(row), KPC_(table_schema));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      if (OB_FAIL(ObPartitionUtils::get_tablet_and_subpart_id(
          *table_schema_, part_id, row, tablet_id, object_id, related_info_ptr))) {
        LOG_WARN("fail to get tablet_id and part_id", KR(ret), K(part_id), K(row), KPC_(table_schema));
      } else if (OB_FAIL(set_partition_id_map(part_id, object_id))) {
        LOG_WARN("failed to set partition id map");
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part level", KR(ret), K(part_level));
    }
  } else {
    //virtual table, only supported partition by list(svr_ip, svr_port) ...
    ObAddr svr_addr;
    if (part_level == PARTITION_LEVEL_TWO) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("virtual table with subpartition table not supported", KR(ret), KPC(vt_svr_pair_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual table with subpartition table");
    } else if (row.get_count() != 2) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("virtual table, only supported partition by list(svr_ip, svr_port)", KR(ret), K(row));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual table partition by other than list(svr_ip, svr_port)");
    } else {
      const ObObj &svr_ip = row.get_cell(0);
      const ObObj &port_obj = row.get_cell(1);
      int64_t port_int = 0;
      if (is_oracle_mode()) {
        OZ(port_obj.get_number().extract_valid_int64_with_trunc(port_int));
      } else {
        port_int = port_obj.get_int();
      }
      svr_addr.set_ip_addr(svr_ip.get_string(), port_int);
    }
    if (OB_SUCC(ret) && OB_FAIL(vt_svr_pair_->get_part_and_tablet_id_by_server(svr_addr, object_id, tablet_id))) {
      LOG_WARN("get part and tablet id by server failed", K(ret));
    } else if (OB_FAIL(mock_vtable_related_tablet_id_map(tablet_id, object_id))) {
      LOG_WARN("fail to mock vtable related tablet id map", KR(ret), K(tablet_id), K(object_id));
    }
  }
  return ret;
}

int ObDASTabletMapper::mock_vtable_related_tablet_id_map(
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;
  if (!tablet_ids.empty() && related_info_.related_tids_ != nullptr && !related_info_.related_tids_->empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &src_tablet_id = tablet_ids.at(i);
      const ObObjectID &src_part_id = part_ids.at(i);
      if (OB_FAIL(mock_vtable_related_tablet_id_map(src_tablet_id, src_part_id))) {
        LOG_WARN("mock related tablet id map failed", KR(ret), K(src_tablet_id), K(src_part_id));
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::mock_vtable_related_tablet_id_map(
    const ObTabletID &tablet_id,
    const ObObjectID &part_id)
{
  int ret = OB_SUCCESS;
  if (tablet_id.is_valid() && related_info_.related_tids_ != nullptr && !related_info_.related_tids_->empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < related_info_.related_tids_->count(); ++i) {
      ObTableID related_table_id = related_info_.related_tids_->at(i);
      ObTabletID related_tablet_id = tablet_id;
      ObObjectID related_object_id = part_id;
      if (OB_FAIL(related_info_.related_map_->add_related_tablet_id(tablet_id,
                                                                    related_table_id,
                                                                    related_tablet_id,
                                                                    related_object_id,
                                                                    OB_INVALID_ID))) {
        LOG_WARN("add related tablet id to map failed", KR(ret), K(tablet_id),
                 K(related_table_id), K(related_tablet_id), K(related_object_id));
      } else {
        LOG_DEBUG("mock related tablet id map",
                  K(tablet_id), K(part_id),
                  K(related_table_id), K(related_tablet_id), K(related_object_id));
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::get_non_partition_tablet_id(ObIArray<ObTabletID> &tablet_ids,
                                                   ObIArray<ObObjectID> &out_part_ids)
{
  int ret = OB_SUCCESS;
  if (is_non_partition_optimized_) {
    if (OB_FAIL(tablet_ids.push_back(tablet_id_))) {
      LOG_WARN("failed to push back tablet ids", K(ret));
    } else if (OB_FAIL(out_part_ids.push_back(object_id_))) {
      LOG_WARN("failed to push back partition ids", K(ret));
    } else {
      DASRelatedTabletMap *map = static_cast<DASRelatedTabletMap *>(related_info_.related_map_);
      if (OB_NOT_NULL(map) && OB_NOT_NULL(related_list_)
          && OB_FAIL(map->assign(*related_list_))) {
        LOG_WARN("failed to assign related map list", K(ret));
      }
    }
  } else {
    ObNewRange range;
    // here need whole range, for virtual table calc tablet and object id
    range.set_whole_range();
    OZ(get_tablet_and_object_id(PARTITION_LEVEL_ZERO, OB_INVALID_ID,
                                range, tablet_ids, out_part_ids));
  }
  return ret;
}

int ObDASTabletMapper::get_tablet_and_object_id(const ObPartitionLevel part_level,
                                                const ObPartID part_id,
                                                const ObIArray<ObNewRange*> &ranges,
                                                ObIArray<ObTabletID> &tablet_ids,
                                                ObIArray<ObObjectID> &out_part_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 4> tmp_tablet_ids;
  ObSEArray<ObObjectID, 4> tmp_part_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
    tmp_tablet_ids.reset();
    tmp_part_ids.reset();
    OZ(get_tablet_and_object_id(part_level, part_id, *ranges.at(i), tmp_tablet_ids, tmp_part_ids));
    OZ(append_array_no_dup(tablet_ids, tmp_tablet_ids));
    OZ(append_array_no_dup(out_part_ids, tmp_part_ids));
  }

  return ret;
}

int ObDASTabletMapper::get_tablet_and_object_id(const ObPartitionLevel part_level,
                                                const ObPartID part_id,
                                                const ObObj &value,
                                                ObIArray<ObTabletID> &tablet_ids,
                                                ObIArray<ObObjectID> &out_part_ids)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = NULL == table_schema_ ? vt_svr_pair_->get_table_id()
                                            : table_schema_->get_table_id();
  ObRowkey rowkey(const_cast<ObObj*>(&value), 1);
  ObNewRange range;
  ObSEArray<ObTabletID, 4> tmp_tablet_ids;
  ObSEArray<ObObjectID, 4> tmp_part_ids;
  if (OB_FAIL(range.build_range(table_id, rowkey))) {
    LOG_WARN("failed to build range", K(ret));
  } else if (OB_FAIL(get_tablet_and_object_id(part_level, part_id, range, tmp_tablet_ids, tmp_part_ids))) {
    LOG_WARN("fail to get tablet id", K(part_level), K(part_id), K(range), K(ret));
  } else {
    OZ(append_array_no_dup(tablet_ids, tmp_tablet_ids));
    OZ(append_array_no_dup(out_part_ids, tmp_part_ids));
  }

  return ret;
}

int ObDASTabletMapper::get_all_tablet_and_object_id(const ObPartitionLevel part_level,
                                                    const ObPartID part_id,
                                                    ObIArray<ObTabletID> &tablet_ids,
                                                    ObIArray<ObObjectID> &out_part_ids)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = NULL == table_schema_ ? vt_svr_pair_->get_table_id()
                                            : table_schema_->get_table_id();
  ObNewRange whole_range;
  whole_range.set_whole_range();
  whole_range.table_id_ = table_id;
  ObSEArray<ObTabletID, 4> tmp_tablet_ids;
  ObSEArray<ObObjectID, 4> tmp_part_ids;
  OZ (get_tablet_and_object_id(part_level, part_id, whole_range, tmp_tablet_ids, tmp_part_ids));
  OZ(append_array_no_dup(tablet_ids, tmp_tablet_ids));
  OZ(append_array_no_dup(out_part_ids, tmp_part_ids));

  return ret;
}

int ObDASTabletMapper::get_all_tablet_and_object_id(ObIArray<ObTabletID> &tablet_ids,
                                                    ObIArray<ObObjectID> &out_part_ids)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_schema_)) {
    if (!table_schema_->is_partitioned_table()) {
      if (OB_FAIL(get_non_partition_tablet_id(tablet_ids, out_part_ids))) {
        LOG_WARN("get non partition tablet id failed", K(ret));
      }
    } else if (PARTITION_LEVEL_ONE == table_schema_->get_part_level()) {
      if (OB_FAIL(get_all_tablet_and_object_id(PARTITION_LEVEL_ONE, OB_INVALID_ID,
                                             tablet_ids, out_part_ids))) {
        LOG_WARN("fail to get tablet ids", K(ret));
      }
    } else {
      ObArray<ObTabletID> tmp_tablet_ids;
      ObArray<ObObjectID> tmp_part_ids;
      if (OB_FAIL(get_all_tablet_and_object_id(PARTITION_LEVEL_ONE, OB_INVALID_ID,
                                               tmp_tablet_ids, tmp_part_ids))) {
        LOG_WARN("Failed to get all part ids", K(ret));
      }
      for (int64_t idx = 0; OB_SUCC(ret) && idx < tmp_part_ids.count(); ++idx) {
        ObObjectID part_id = tmp_part_ids.at(idx);
        if (OB_FAIL(get_all_tablet_and_object_id(PARTITION_LEVEL_TWO, part_id,
                                                 tablet_ids, out_part_ids))) {
          LOG_WARN("fail to get tablet ids", K(ret));
        }
      }
    }
  }
  return ret;
}

//If the part_id calculated by the partition filter in the where clause is empty,
//we will use the default part id in this query as the final part_id,
//because optimizer needs at least one part_id to generate a plan
int ObDASTabletMapper::get_default_tablet_and_object_id(const ObPartitionLevel part_level,
                                                        const ObIArray<ObObjectID> &part_hint_ids,
                                                        ObTabletID &tablet_id,
                                                        ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == vt_svr_pair_)) {
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartitionSchemaIter iter(*table_schema_, check_partition_mode);
    ObPartitionSchemaIter::Info info;
    while (OB_SUCC(ret) && !tablet_id.is_valid()) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("switch the src partition info failed", K(ret));
        }
      } else if (part_hint_ids.empty()) {
        //if partition hint is empty,
        //we use the first partition in table schema as the default partition
        object_id = info.object_id_;
        tablet_id = info.tablet_id_;
      } else if (info.object_id_ == part_hint_ids.at(0)) {
        //if partition hint is specified, we must use the first part id in part_hint_ids_
        //as the default partition,
        //and can't use the first part id in table schema,
        //otherwise, the result of some cases will be incorrect,
        //such as:
        //create table t1(a int primary key, b int) partition by hash(a) partitions 2;
        //select * from t1 partition(p1) where a=0;
        //if where a=0 prune result is partition_id=0 and first_part_id in table schema is 0
        //but query specify that use partition_id=1 to access table, so the result is empty
        //if we use the first part id in table schema as the default partition to access table
        //the result of this query will not be empty
        object_id = info.object_id_;
        tablet_id = info.tablet_id_;
      }
      if (OB_FAIL(ret)) {
      } else if (!tablet_id.is_valid()) {
        // no nothing
      } else if (PARTITION_LEVEL_TWO == part_level &&
                OB_NOT_NULL(info.part_) &&
                OB_FAIL(set_partition_id_map(info.part_->get_part_id(), object_id))) {
        LOG_WARN("failed to set partition id map");
      } else if (related_info_.related_tids_ != nullptr &&
                 !related_info_.related_tids_->empty()) {
        //calculate related partition id and tablet id
        ObSchemaGetterGuard guard;
        const uint64_t tenant_id=  table_schema_->get_tenant_id();
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid schema service", KR(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(table_schema_->get_tenant_id(), guard))) {
          LOG_WARN("get tenant schema guard fail", KR(ret), K(tenant_id));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < related_info_.related_tids_->count(); ++i) {
          ObTableID related_table_id = related_info_.related_tids_->at(i);
          const ObSimpleTableSchemaV2 *table_schema = nullptr;
          ObObjectID related_part_id = OB_INVALID_ID;
          ObObjectID related_first_level_part_id = OB_INVALID_ID;
          ObTabletID related_tablet_id;
          if (OB_FAIL(guard.get_simple_table_schema(tenant_id, related_table_id, table_schema))) {
            LOG_WARN("get_table_schema fail", K(ret), K(tenant_id), K(related_table_id));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("fail to get table schema", KR(ret), K(related_table_id));
          } else if (OB_FAIL(table_schema->get_part_id_and_tablet_id_by_idx(info.part_idx_,
                                                                            info.subpart_idx_,
                                                                            related_part_id,
                                                                            related_first_level_part_id,
                                                                            related_tablet_id))) {
            LOG_WARN("get part by idx failed", K(ret), K(info), K(related_table_id));
          } else if (OB_FAIL(related_info_.related_map_->add_related_tablet_id(tablet_id,
                                                                               related_table_id,
                                                                               related_tablet_id,
                                                                               related_part_id,
                                                                               related_first_level_part_id))) {
            LOG_WARN("add related tablet id failed", K(ret),
                     K(tablet_id), K(related_table_id), K(related_part_id), K(related_tablet_id));
          } else {
            LOG_DEBUG("add related tablet id to map",
                      K(tablet_id), K(related_table_id), K(related_part_id), K(related_tablet_id));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  } else if (!part_hint_ids.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("specify partition name in virtual table not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify partition name in virtual table");
  } else {
    //virtual table start partition id and tablet id with id=1
    vt_svr_pair_->get_default_tablet_and_part_id(tablet_id, object_id);
    if (related_info_.related_tids_ != nullptr && !related_info_.related_tids_->empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < related_info_.related_tids_->count(); ++i) {
        ObTableID related_table_id = related_info_.related_tids_->at(i);
        //all related tables have the same part_id and tablet_id
        if (OB_FAIL(related_info_.related_map_->add_related_tablet_id(tablet_id, related_table_id, tablet_id,
                                                                      object_id, OB_INVALID_ID))) {
          LOG_WARN("add related tablet id failed", K(ret), K(related_table_id), K(object_id));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid first tablet id", K(ret), KPC(table_schema_), KPC(vt_svr_pair_));
  }
  return ret;
}

//get the local index partition id by data table partition id
//or get the local index partition id by other local index partition id
//or get the data table partition id by its local index partition id
int ObDASTabletMapper::get_related_partition_id(const ObTableID &src_table_id,
                                                const ObObjectID &src_part_id,
                                                const ObTableID &dst_table_id,
                                                ObObjectID &dst_object_id)
{
  int ret = OB_SUCCESS;
  if (src_table_id == dst_table_id || nullptr != vt_svr_pair_) {
    dst_object_id = src_part_id;
  } else {
    bool is_found = false;
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartitionSchemaIter iter(*table_schema_, check_partition_mode);
    ObPartitionSchemaIter::Info info;
    while (OB_SUCC(ret) && !is_found) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("switch the src partition info failed", K(ret));
        }
      } else if (info.object_id_ == src_part_id) {
        //find the partition array offset by search partition id
        is_found = true;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && is_found) {
      ObSchemaGetterGuard guard;
      const ObSimpleTableSchemaV2 *dst_table_schema = nullptr;
      ObObjectID related_part_id = OB_INVALID_ID;
      ObObjectID related_first_level_part_id = OB_INVALID_ID;
      ObTabletID related_tablet_id;
      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid schema service", KR(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(table_schema_->get_tenant_id(), guard))) {
        LOG_WARN("get tenant schema guard fail", KR(ret), K(table_schema_->get_tenant_id()));
      } else if (OB_FAIL(guard.get_simple_table_schema(table_schema_->get_tenant_id(), dst_table_id, dst_table_schema))) {
        LOG_WARN("get_table_schema fail", K(ret), K(dst_table_id));
      } else if (OB_ISNULL(dst_table_schema)) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("fail to get table schema", KR(ret), K(dst_table_id));
      } else if (OB_FAIL(dst_table_schema->get_part_id_and_tablet_id_by_idx(info.part_idx_,
                                                                            info.subpart_idx_,
                                                                            related_part_id,
                                                                            related_first_level_part_id,
                                                                            related_tablet_id))) {
        LOG_WARN("get part by idx failed", K(ret), K(info), K(dst_table_id));
      } else {
        dst_object_id = related_part_id;
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::set_partition_id_map(ObObjectID first_level_part_id,
                                            ObIArray<ObObjectID> &partition_ids)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(partition_id_map_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
      if (OB_FAIL(partition_id_map_->set_refactored(partition_ids.at(i), first_level_part_id))) {
        if (OB_LIKELY(OB_HASH_EXIST == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to set partition map", K(first_level_part_id), K(partition_ids.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::set_partition_id_map(ObObjectID first_level_part_id,
                                            ObObjectID partition_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(partition_id_map_)) {
    if (OB_FAIL(partition_id_map_->set_refactored(partition_id, first_level_part_id))) {
      if (OB_LIKELY(OB_HASH_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to set partition map", K(first_level_part_id), K(partition_id));
      }
    }
  }
  return ret;
}

int ObDASTabletMapper::get_partition_id_map(ObObjectID partition_id,
                                            ObObjectID &first_level_part_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(partition_id_map_)) {
    if (OB_FAIL(partition_id_map_->get_refactored(partition_id, first_level_part_id))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        // do nothing
      } else {
        LOG_WARN("failed to set partition map", K(partition_id), K(first_level_part_id));
      }
    }
  }
  return ret;
}


ObDASLocationRouter::ObDASLocationRouter(ObIAllocator &allocator)
  : last_errno_(OB_SUCCESS),
    cur_errno_(OB_SUCCESS),
    history_retry_cnt_(0),
    cur_retry_cnt_(0),
    all_tablet_list_(allocator),
    succ_tablet_list_(allocator),
    virtual_server_list_(allocator),
    allocator_(allocator)
{
}

ObDASLocationRouter::~ObDASLocationRouter()
{
  //try to refresh location when location exception occurred
  refresh_location_cache_by_errno(true, cur_errno_);
  cur_errno_ = OB_SUCCESS;
}

int ObDASLocationRouter::nonblock_get_readable_replica(const uint64_t tenant_id,
                                                       const ObTabletID &tablet_id,
                                                       ObDASTabletLoc &tablet_loc)
{
  int ret = OB_SUCCESS;
  ObLSLocation ls_loc;
  tablet_loc.tablet_id_ = tablet_id;
  if (OB_FAIL(all_tablet_list_.push_back(tablet_id))) {
    LOG_WARN("store access tablet id failed", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get(tenant_id,
                                                          tablet_id,
                                                          tablet_loc.ls_id_))) {
    LOG_WARN("nonblock get ls id failed", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get(GCONF.cluster_id,
                                                          tenant_id,
                                                          tablet_loc.ls_id_,
                                                          ls_loc))) {
    LOG_WARN("get ls replica location failed", K(ret), K(tablet_loc));
  }
  if (is_partition_change_error(ret)) {
    /*During the execution phase, if nonblock location interface is used to obtain the location
     * and an exception occurs, retries are necessary.
     * However, statement-level retries cannot rollback many execution states,
     * so it is necessary to avoid retries in this scenario as much as possible.
     * During the execution phase, when encountering a location exception for the first time,
     * try to refresh the location once synchronously.
     * If it fails, then proceed with statement-level retries.*/
    int tmp_ret = block_renew_tablet_location(tablet_id, ls_loc);
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("block renew tablet location failed", KR(tmp_ret), K(tablet_id));
    } else {
      tablet_loc.ls_id_ = ls_loc.get_ls_id();
      ret = OB_SUCCESS;
    }
  }
  ObBLKey bl_key;
  bool in_black_list = true;
  ObSEArray<const ObLSReplicaLocation *, 3> remote_replicas;
  const ObLSReplicaLocation *local_replica = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_loc.get_replica_locations().count(); ++i) {
    const ObLSReplicaLocation &tmp_replica_loc = ls_loc.get_replica_locations().at(i);
    if (OB_FAIL(bl_key.init(tmp_replica_loc.get_server(), tenant_id, tablet_loc.ls_id_))) {
      LOG_WARN("init black list key failed", K(ret));
    } else if (OB_FAIL(ObBLService::get_instance().check_in_black_list(bl_key, in_black_list))) {
      LOG_WARN("check in black list failed", K(ret));
    } else if (!in_black_list) {
      if (tmp_replica_loc.get_server() == GCTX.self_addr()) {
        //prefer choose the local replica
        local_replica = &tmp_replica_loc;
      } else if (OB_FAIL(remote_replicas.push_back(&tmp_replica_loc))) {
        LOG_WARN("store tmp replica failed", K(ret));
      }
    } else {
      LOG_INFO("this replica is in the blacklist, thus filtered it", K(bl_key));
    }
  }
  if (OB_SUCC(ret)) {
    if (local_replica != nullptr) {
      tablet_loc.server_ = local_replica->get_server();
    } else if (remote_replicas.empty()) {
      ret = OB_NO_READABLE_REPLICA;
      LOG_WARN("there has no readable replica", K(ret), K(tablet_id), K(ls_loc));
    } else {
      //no local copy, randomly select a readable replica
      int64_t select_idx = rand() % remote_replicas.count();
      const ObLSReplicaLocation *remote_loc = remote_replicas.at(select_idx);
      tablet_loc.server_ = remote_loc->get_server();
    }
  }
  save_cur_exec_status(ret);
  return ret;
}

int ObDASLocationRouter::nonblock_get(const ObDASTableLocMeta &loc_meta,
                                      const common::ObTabletID &tablet_id,
                                      ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  bool is_vt = is_virtual_table(loc_meta.ref_table_id_);
  bool is_mapping_real_vt = is_oracle_mapping_real_virtual_table(loc_meta.ref_table_id_);
  uint64_t ref_table_id = loc_meta.ref_table_id_;
  if (is_mapping_real_vt) {
    is_vt = false;
    ref_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(loc_meta.ref_table_id_);
  }
  if (OB_UNLIKELY(is_vt)) {
    if (OB_FAIL(get_vt_ls_location(ref_table_id, tablet_id, location))) {
      LOG_WARN("get virtual table ls location failed", K(ret), K(ref_table_id), K(tablet_id));
    }
  } else if (loc_meta.is_external_table_) {
    ret = get_external_table_ls_location(location);
  } else {
    ObLSID ls_id;
    if (OB_FAIL(all_tablet_list_.push_back(tablet_id))) {
      LOG_WARN("store all tablet list failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(GCTX.location_service_->nonblock_get(tenant_id, tablet_id, ls_id))) {
      LOG_WARN("nonblock get ls id failed", K(ret));
    } else if (OB_FAIL(GCTX.location_service_->nonblock_get(GCONF.cluster_id,
                                                            tenant_id,
                                                            ls_id,
                                                            location))) {
      LOG_WARN("fail to get tablet locations", K(ret), K(tenant_id), K(ls_id));
    }
    if (is_partition_change_error(ret)) {
      /*During the execution phase, if nonblock location interface is used to obtain the location
       * and an exception occurs, retries are necessary.
       * However, statement-level retries cannot rollback many execution states,
       * so it is necessary to avoid retries in this scenario as much as possible.
       * During the execution phase, when encountering a location exception for the first time,
       * try to refresh the location once synchronously.
       * If it fails, then proceed with statement-level retries.*/
      int tmp_ret = block_renew_tablet_location(tablet_id, location);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("block renew tablet location failed", KR(tmp_ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  save_cur_exec_status(ret);

  return ret;
}

int ObDASLocationRouter::nonblock_get_candi_tablet_locations(const ObDASTableLocMeta &loc_meta,
                                                             const ObIArray<ObTabletID> &tablet_ids,
                                                             const ObIArray<ObObjectID> &partition_ids,
                                                             const ObIArray<ObObjectID> &first_level_part_ids,
                                                             ObIArray<ObCandiTabletLoc> &candi_tablet_locs)
{
  int ret = OB_SUCCESS;
  NG_TRACE(get_location_cache_begin);
  candi_tablet_locs.reset();
  int64_t N = tablet_ids.count();
  if (OB_FAIL(candi_tablet_locs.prepare_allocate(N))) {
    LOG_WARN("Partition location list prepare error", K(ret));
  } else {
    ObLSLocation location;
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < N; ++i) {
      location.reset();
      ObCandiTabletLoc &candi_tablet_loc = candi_tablet_locs.at(i);
      //after 4.1, all modules that need to access location will use nonblock_get to fetch location
      //if the location has expired, DAS location router will refresh all accessed tablets
      if (OB_FAIL(nonblock_get(loc_meta, tablet_ids.at(i), location))) {
        LOG_WARN("Get partition error, the location cache will be renewed later",
                 K(ret), "tablet_id", tablet_ids.at(i), K(candi_tablet_loc));
      } else {
        ObObjectID first_level_part_id = first_level_part_ids.empty() ? OB_INVALID_ID : first_level_part_ids.at(i);
        if (OB_FAIL(candi_tablet_loc.set_part_loc_with_only_readable_replica(partition_ids.at(i),
                                                                             first_level_part_id,
                                                                             tablet_ids.at(i),
                                                                             location))) {
          LOG_WARN("fail to set partition location with only readable replica",
                   K(ret),K(i), K(location), K(candi_tablet_locs), K(tablet_ids), K(partition_ids));
        }
        LOG_TRACE("set partition location with only readable replica",
                 K(ret),K(i), K(location), K(candi_tablet_locs), K(tablet_ids), K(partition_ids));
      }
    } // for end
  }
  NG_TRACE(get_location_cache_end);
  return ret;
}

int ObDASLocationRouter::get_tablet_loc(const ObDASTableLocMeta &loc_meta,
                                        const ObTabletID &tablet_id,
                                        ObDASTabletLoc &tablet_loc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  bool is_vt = is_virtual_table(loc_meta.ref_table_id_);
  if (OB_UNLIKELY(is_vt)) {
    if (OB_FAIL(get_vt_tablet_loc(loc_meta.ref_table_id_, tablet_id, tablet_loc))) {
      LOG_WARN("get virtual tablet loc failed", K(ret), K(loc_meta));
    }
  } else {
    if (OB_LIKELY(loc_meta.select_leader_) || OB_UNLIKELY(last_errno_ == OB_NOT_MASTER)) {
      //if this statement is retried because of OB_NOT_MASTER, we will choose the leader directly
      ret = nonblock_get_leader(tenant_id, tablet_id, tablet_loc);
    } else {
      ret = nonblock_get_readable_replica(tenant_id, tablet_id, tablet_loc);
    }
  }
  return ret;
}

int ObDASLocationRouter::nonblock_get_leader(const uint64_t tenant_id,
                                             const ObTabletID &tablet_id,
                                             ObDASTabletLoc &tablet_loc)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  tablet_loc.tablet_id_ = tablet_id;
  if (OB_FAIL(all_tablet_list_.push_back(tablet_id))) {
    LOG_WARN("store access tablet id failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get(tenant_id,
                                                          tablet_id,
                                                          tablet_loc.ls_id_))) {
    LOG_WARN("nonblock get ls id failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(GCONF.cluster_id,
                                                                 tenant_id,
                                                                 tablet_loc.ls_id_,
                                                                 tablet_loc.server_))) {
    LOG_WARN("nonblock get ls location failed", K(ret), K(tablet_loc));
  }
  if (is_partition_change_error(ret)) {
    /*During the execution phase, if nonblock location interface is used to obtain the location
     * and an exception occurs, retries are necessary.
     * However, statement-level retries cannot rollback many execution states,
     * so it is necessary to avoid retries in this scenario as much as possible.
     * During the execution phase, when encountering a location exception for the first time,
     * try to refresh the location once synchronously.
     * If it fails, then proceed with statement-level retries.*/
    ObLSLocation ls_loc;
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = block_renew_tablet_location(tablet_id, ls_loc)))) {
      LOG_WARN("block renew tablet location failed", KR(tmp_ret), K(tablet_id));
    } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ls_loc.get_leader(tablet_loc.server_)))) {
      LOG_WARN("get leader of ls location failed", KR(tmp_ret), K(tablet_id), K(ls_loc));
    } else {
      tablet_loc.ls_id_ = ls_loc.get_ls_id();
      ret = OB_SUCCESS;
    }
  }
  save_cur_exec_status(ret);
  return ret;
}

int ObDASLocationRouter::get_leader(const uint64_t tenant_id,
                                    const ObTabletID &tablet_id,
                                    ObAddr &leader_addr,
                                    int64_t expire_renew_time)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObLSID ls_id;
  if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                          tablet_id,
                                          expire_renew_time,
                                          is_cache_hit,
                                          ls_id))) {
    LOG_WARN("nonblock get ls id failed", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,
                                                        tenant_id,
                                                        ls_id,
                                                        false,
                                                        leader_addr))) {
    LOG_WARN("nonblock get ls location failed", K(ret));
  }
  return ret;
}


int ObDASLocationRouter::get_full_ls_replica_loc(const ObObjectID &tenant_id,
                                                 const ObDASTabletLoc &tablet_loc,
                                                 ObLSReplicaLocation &replica_loc)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObLSLocation ls_loc;
  if (OB_FAIL(GCTX.location_service_->nonblock_get(GCONF.cluster_id,
                                                   tenant_id,
                                                   tablet_loc.ls_id_,
                                                   ls_loc))) {
    LOG_WARN("get ls replica location failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_loc.get_replica_locations().count(); ++i) {
    const ObLSReplicaLocation &tmp_replica_loc = ls_loc.get_replica_locations().at(i);
    if (tmp_replica_loc.get_server() == tablet_loc.server_) {
      replica_loc = tmp_replica_loc;
      break;
    }
  }
  if (OB_SUCC(ret) && !replica_loc.is_valid()) {
    ret = OB_LOCATION_NOT_EXIST;
    LOG_WARN("replica location not found", K(ret), K(tablet_loc));
  }
  return ret;
}

int ObDASLocationRouter::get_vt_svr_pair(uint64_t vt_id, const VirtualSvrPair *&vt_pair)
{
  int ret = OB_SUCCESS;
  FOREACH(tmp_node, virtual_server_list_) {
    if (tmp_node->get_table_id() == vt_id) {
      vt_pair =&(*tmp_node);
    }
  }
  if (!is_virtual_table(vt_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vt_id is not virtual table id", K(ret), K(vt_id));
  } else if (nullptr == vt_pair) {
    VirtualSvrPair empty_pair;
    VirtualSvrPair *tmp_pair = nullptr;
    bool is_cache_hit = false;
    ObSEArray<ObAddr, 8> part_locations;
    if (OB_FAIL(virtual_server_list_.push_back(empty_pair))) {
      LOG_WARN("extend virtual server list failed", K(ret));
    } else if (OB_ISNULL(GCTX.location_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location_service_ is null", KR(ret));
    } else if (OB_FAIL(GCTX.location_service_->vtable_get(
        MTL_ID(),
        vt_id,
        0,/*expire_renew_time*/
        is_cache_hit,
        part_locations))) {
      LOG_WARN("fail to get virtual table location", KR(ret), K(vt_id));
    } else {
      tmp_pair = &virtual_server_list_.get_last();
      if (OB_FAIL(tmp_pair->init(allocator_, vt_id, part_locations))) {
        LOG_WARN("init tmp virtual table svr pair failed", K(ret), K(vt_id));
      } else {
        vt_pair = tmp_pair;
      }
    }
  }
  return ret;
}

OB_NOINLINE int ObDASLocationRouter::get_vt_tablet_loc(uint64_t table_id,
                                                       const ObTabletID &tablet_id,
                                                       ObDASTabletLoc &tablet_loc)
{
  int ret = OB_SUCCESS;
  VirtualSvrPair *final_pair = nullptr;
  FOREACH(tmp_node, virtual_server_list_) {
    if (tmp_node->get_table_id() == table_id) {
      final_pair = &(*tmp_node);
      break;
    }
  }
  if (OB_ISNULL(final_pair)) {
    ret = OB_LOCATION_NOT_EXIST;
    LOG_WARN("virtual table location not exists", K(table_id), K(virtual_server_list_));
  } else if (OB_FAIL(final_pair->get_server_by_tablet_id(tablet_id, tablet_loc.server_))) {
    LOG_WARN("get server by tablet id failed", K(ret), K(tablet_id));
  } else {
    tablet_loc.tablet_id_ = tablet_id;
    tablet_loc.ls_id_ = ObLSID::VT_LS_ID;
  }
  return ret;
}

OB_NOINLINE int ObDASLocationRouter::get_vt_ls_location(uint64_t table_id,
                                                        const ObTabletID &tablet_id,
                                                        ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  VirtualSvrPair *server_pair = nullptr;
  FOREACH(tmp_node, virtual_server_list_) {
    if (tmp_node->get_table_id() == table_id) {
      server_pair = &(*tmp_node);
      break;
    }
  }
  if (OB_ISNULL(server_pair)) {
    ret = OB_LOCATION_NOT_EXIST;
    LOG_WARN("not found virtual location", K(ret), K(tablet_id), K(virtual_server_list_));
  } else {
    // mock ls location
    int64_t now = ObTimeUtility::current_time();
    ObReplicaProperty mock_prop;
    ObLSReplicaLocation ls_replica;
    ObAddr server;
    ObLSRestoreStatus restore_status(ObLSRestoreStatus::RESTORE_NONE);
    if (OB_FAIL(location.init(GCONF.cluster_id, MTL_ID(), ObLSID(ObLSID::VT_LS_ID), now))) {
      LOG_WARN("init location failed", KR(ret));
    } else if (OB_FAIL(server_pair->get_server_by_tablet_id(tablet_id, server))) {
      LOG_WARN("get server by tablet id failed", K(ret));
    } else if (OB_FAIL(ls_replica.init(server, common::LEADER,
                       GCONF.mysql_port, REPLICA_TYPE_FULL, mock_prop,
                       restore_status, 1 /*proposal_id*/))) {
      LOG_WARN("init ls replica failed", K(ret));
    } else if (OB_FAIL(location.add_replica_location(ls_replica))) {
      LOG_WARN("add replica location failed", K(ret));
    }
  }
  return ret;
}

void ObDASLocationRouter::refresh_location_cache_by_errno(bool is_nonblock, int err_no)
{
  NG_TRACE_TIMES(1, get_location_cache_begin);
  if (is_master_changed_error(err_no)
      || is_partition_change_error(err_no)
      || is_get_location_timeout_error(err_no)
      || is_server_down_error(err_no)
      || is_has_no_readable_replica_err(err_no)
      || is_unit_migrate(err_no)) {
    // Refresh tablet ls mapping and ls locations according to err_no.
    //
    // The timeout has been set inner the interface when renewing location synchronously.
    // It will use the timeout of ObTimeoutCtx or THIS_WORKER if it has been set.
    // Otherwise it uses GCONF.location_cache_refresh_sql_timeout.
    // Timeout usage priority: ObTimeoutCtx > THIS_WORKER > GCONF
    //
    // all_tablet_list_ may contain duplicate tablet_id
    force_refresh_location_cache(is_nonblock, err_no);
  }
  NG_TRACE_TIMES(1, get_location_cache_end);
}

void ObDASLocationRouter::force_refresh_location_cache(bool is_nonblock, int err_no)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "DasRefrLoca", ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ObList<ObTabletID, ObIAllocator> failed_list(CURRENT_CONTEXT->get_allocator());
    FOREACH_X(id_iter, all_tablet_list_, OB_SUCC(ret)) {
      if (!element_exist(succ_tablet_list_, *id_iter) && !element_exist(failed_list, *id_iter)) {
        if (OB_FAIL(failed_list.push_back(*id_iter))) {
          LOG_WARN("store failed tablet id failed", KR(ret), K(id_iter));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(GCTX.location_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("GCTX.location_service_ is null", KR(ret));
      } else if (OB_FAIL(GCTX.location_service_->batch_renew_tablet_locations(MTL_ID(),
                                                                              failed_list,
                                                                              err_no,
                                                                              is_nonblock))) {
        LOG_WARN("batch renew tablet locations failed", KR(ret),
            "tenant_id", MTL_ID(), K(err_no), K(is_nonblock), K(failed_list));
      }
    }
  }
  all_tablet_list_.clear();
  succ_tablet_list_.clear();
}

int ObDASLocationRouter::block_renew_tablet_location(const ObTabletID &tablet_id, ObLSLocation &ls_loc)
{
  int ret = OB_SUCCESS;
  const int64_t expire_renew_time = INT64_MAX; // means must renew location
  bool is_cache_hit = false;
  ObLSID ls_id;
  int64_t query_timeout_ts = THIS_WORKER.get_timeout_ts();
  ObTimeoutCtx timeout_ctx;
  timeout_ctx.set_timeout(GCONF.location_cache_refresh_sql_timeout);
  //The maximum timeout period is location_cache_refresh_sql_timeout
  if (timeout_ctx.get_abs_timeout() > query_timeout_ts && query_timeout_ts > 0) {
    timeout_ctx.set_abs_timeout(query_timeout_ts);
  }
  //the timeout limit for "refresh location" is within 1s
  THIS_WORKER.set_timeout_ts(timeout_ctx.get_abs_timeout());
  if (OB_FAIL(GCTX.location_service_->get(MTL_ID(),
                                          tablet_id,
                                          expire_renew_time,
                                          is_cache_hit,
                                          ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->get(GCONF.cluster_id,
                                                 MTL_ID(),
                                                 ls_id,
                                                 expire_renew_time,
                                                 is_cache_hit,
                                                 ls_loc))) {
    LOG_WARN("failed to get location", K(ls_id), K(ret));
  } else {
    LOG_INFO("LOCATION: block refresh table cache succ", K(tablet_id), K(ls_loc));
  }
  //recover query timeout ts
  THIS_WORKER.set_timeout_ts(query_timeout_ts);
  return ret;
}

void ObDASLocationRouter::set_retry_info(const ObQueryRetryInfo* retry_info)
{
  last_errno_ = retry_info->get_last_query_retry_err();
  history_retry_cnt_ = retry_info->get_retry_cnt();
}

int ObDASLocationRouter::get_external_table_ls_location(ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  ObReplicaProperty mock_prop;
  ObLSReplicaLocation ls_replica;
  ObLSRestoreStatus ls_restore_status(ObLSRestoreStatus::RESTORE_NONE);
  OZ (location.init(GCONF.cluster_id, MTL_ID(), ObLSID(ObLSID::VT_LS_ID), now));
  OZ (ls_replica.init(GCTX.self_addr(), common::LEADER,
                      GCONF.mysql_port, REPLICA_TYPE_FULL,
                      mock_prop, ls_restore_status, 1 /*proposal_id*/));
  OZ (location.add_replica_location(ls_replica));
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASLocationRouter, all_tablet_list_);

}  // namespace sql
}  // namespace oceanbase
