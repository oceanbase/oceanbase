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
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_context.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

//not assign array member
void ObDASTableLocMeta::light_assign(const ObDASTableLocMeta &other)
{
  table_loc_id_ = other.table_loc_id_;
  ref_table_id_ = other.ref_table_id_;
  flags_ = other.flags_;
}

int ObDASTableLocMeta::assign(const ObDASTableLocMeta &other)
{
  int ret = OB_SUCCESS;
  light_assign(other);
  ret = related_table_ids_.assign(other.related_table_ids_);
  return ret;
}

int ObDASTableLocMeta::init_related_meta(uint64_t related_table_id,
                                         ObDASTableLocMeta &related_meta) const
{
  int ret = OB_SUCCESS;
  related_meta.light_assign(*this);
  related_meta.ref_table_id_ = related_table_id;
  related_meta.related_table_ids_.set_capacity(related_table_ids_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < related_table_ids_.count(); ++i) {
    //the related table ids of other table meta are the other related table id and source table id
    uint64_t tmp_related_id = (related_table_ids_.at(i) == related_table_id ?
                              ref_table_id_ : related_table_ids_.at(i));
    ret = related_meta.related_table_ids_.push_back(tmp_related_id);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASTableLocMeta,
                    table_loc_id_,
                    ref_table_id_,
                    related_table_ids_,
                    flags_);

OB_SERIALIZE_MEMBER(ObDASTabletLoc,
                    tablet_id_,
                    ls_id_,
                    server_,
                    flags_,
                    partition_id_,
                    first_level_part_id_);

OB_DEF_SERIALIZE(ObDASTableLoc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(*loc_meta_);
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(tablet_locs_.size());
  FOREACH_X(tmp_node, tablet_locs_, OB_SUCC(ret)) {
    ObDASTabletLoc *tablet_loc = *tmp_node;
    OB_UNIS_ENCODE(*tablet_loc);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASTableLoc)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  void *meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
  ObDASTableLocMeta *loc_meta = nullptr;
  if (OB_ISNULL(meta_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table loc meta failed", K(ret));
  } else {
    loc_meta = new(meta_buf) ObDASTableLocMeta(allocator_);
    loc_meta_ = loc_meta;
  }
  OB_UNIS_DECODE(*loc_meta);
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObDASTabletLoc *tablet_loc = nullptr;
    void *tablet_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(tablet_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc buf failed", K(ret));
    } else {
      tablet_loc = new(tablet_buf) ObDASTabletLoc();
      tablet_loc->loc_meta_ = loc_meta_;
      if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
        LOG_WARN("store tablet locs failed", K(ret));
      }
    }
    OB_UNIS_DECODE(*tablet_loc);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASTableLoc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(*loc_meta_);
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(tablet_locs_.size());
  FOREACH(tmp_node, tablet_locs_) {
    ObDASTabletLoc *tablet_loc = *tmp_node;
    OB_UNIS_ADD_LEN(*tablet_loc);
  }
  return len;
}

int ObDASTableLoc::assign(const ObCandiTableLoc &candi_table_loc)
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLocIArray &candi_tablet_locs = candi_table_loc.get_phy_part_loc_info_list();
  ObLSReplicaLocation replica_loc;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs.count(); ++i) {
    replica_loc.reset();
    const ObCandiTabletLoc &candi_tablet_loc = candi_tablet_locs.at(i);
    const ObOptTabletLoc &opt_tablet_loc = candi_tablet_loc.get_partition_location();
    ObDASTabletLoc *tablet_loc = nullptr;
    void *tablet_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(tablet_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc buf failed", K(ret));
    } else if (OB_ISNULL(tablet_loc = new(tablet_buf) ObDASTabletLoc())) {
      //do nothing
    } else if (OB_FAIL(candi_tablet_loc.get_selected_replica(replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(candi_tablet_loc));
    } else {
      tablet_loc->server_ = replica_loc.get_server();
      tablet_loc->tablet_id_ = opt_tablet_loc.get_tablet_id();
      tablet_loc->ls_id_ = opt_tablet_loc.get_ls_id();
      tablet_loc->loc_meta_ = loc_meta_;
      tablet_loc->partition_id_ = opt_tablet_loc.get_partition_id();
      tablet_loc->first_level_part_id_ = opt_tablet_loc.get_first_level_part_id();
      if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
        LOG_WARN("store tablet loc failed", K(ret), K(tablet_loc));
      }
    }
  }
  LOG_DEBUG("das table loc assign", K(candi_table_loc), KPC_(loc_meta), K(tablet_locs_));
  return ret;
}

int ObDASTableLoc::get_tablet_loc_by_id(const ObTabletID &tablet_id,
                                        ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  tablet_loc = NULL;
  lookup_cnt_++;
  if (tablet_locs_map_.created()) {
    if (OB_FAIL(tablet_locs_map_.get(tablet_id, tablet_loc))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("look up from hash map failed", KR(ret), K(tablet_id));
      }
    }
  }
  if (OB_SUCC(ret) && NULL != tablet_loc) {
    // found in hash map
  } else if (OB_HASH_NOT_EXIST == ret) {
    // key not found
    ret = OB_SUCCESS;
  } else {
    FOREACH(tmp_node, tablet_locs_) {
      ObDASTabletLoc *tmp_tablet_loc = *tmp_node;
      if (tmp_tablet_loc->tablet_id_ == tablet_id) {
        tablet_loc = tmp_tablet_loc;
        break;
      }
    }
  }
  if (OB_FAIL(ret) || tablet_locs_map_.created()) {
    // do nothing
  } else if (lookup_cnt_ > DAS_TABLET_LOC_LOOKUP_THRESHOLD
             && tablet_locs_.size() > DAS_TABLET_LOC_SIZE_THRESHOLD
             && OB_FAIL(create_tablet_locs_map())) {
    LOG_WARN("create tablet locs hash map failed", KR(ret));
  }
  return ret;
}

int ObDASTableLoc::add_tablet_loc(ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet loc is null", KR(ret), KP(tablet_loc));
  } else if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
    LOG_WARN("push into tablet locs list failed", KR(ret), KPC(tablet_loc));
  } else if (tablet_locs_map_.created()) {
    if (OB_FAIL(tablet_locs_map_.set(tablet_loc->tablet_id_, tablet_loc))) {
      LOG_WARN("insert into tablet locs map failed", KR(ret), KPC(tablet_loc));
    }
  }
  return ret;
}

int TabletHashMap::create(int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= bucket_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket number", KR(ret), K(bucket_num));
  } else if (created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("hash map was already created", KR(ret));
  } else if (FALSE_IT(bucket_num = hash::cal_next_prime(bucket_num))) {
  } else if (OB_ISNULL(buckets_ = static_cast<TabletHashNode **>(
          allocator_.alloc(bucket_num * sizeof(TabletHashNode *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buckets failed", KR(ret));
  } else {
    MEMSET(buckets_, 0, bucket_num * sizeof(TabletHashNode *));
    bucket_num_ = bucket_num;
    is_inited_ = true;
  }
  return ret;
}

int TabletHashMap::find_node(const ObTabletID key,
                             TabletHashNode *head,
                             TabletHashNode *&node) const
{
  int ret = OB_SUCCESS;
  node = NULL;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (NULL == head) {
    // do nothing
  } else {
    TabletHashNode *cur = head;
    while (NULL != cur && NULL == node) {
      if (cur->key_ == key) {
        node = cur;
      } else {
        cur = cur->next_;
      }
    }
  }
  return ret;
}

int TabletHashMap::set(const ObTabletID key, ObDASTabletLoc *value)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (OB_FAIL(key.hash(hash_val))) {
    LOG_WARN("hash failed", KR(ret));
  } else {
    TabletHashNode *&bucket = buckets_[hash_val % bucket_num_];
    TabletHashNode *dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", KR(ret));
    } else if (NULL != dst_node) {
      ret = OB_HASH_EXIST;
      LOG_WARN("key already exists", KR(ret), K(key), KP(value));
    } else {
      TabletHashNode *new_node = static_cast<TabletHashNode *>(
              allocator_.alloc(sizeof(TabletHashNode)));
      if (OB_ISNULL(new_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc new node failed", KR(ret));
      } else {
        new_node->key_ = key;
        new_node->value_ = value;
        new_node->next_ = bucket;
        bucket = new_node;
      }
    }
  }
  return ret;
}

int TabletHashMap::get(const ObTabletID key, ObDASTabletLoc *&value)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (OB_FAIL(key.hash(hash_val))) {
    LOG_WARN("hash failed", KR(ret));
  } else {
    TabletHashNode *&bucket = buckets_[hash_val % bucket_num_];
    TabletHashNode *dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", KR(ret));
    } else if (NULL == dst_node) {
      ret = OB_HASH_NOT_EXIST;
    } else {
      value = dst_node->value_;
    }
  }
  return ret;
}

int ObDASTableLoc::create_tablet_locs_map()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_locs_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet locs map was already created", KR(ret), K(tablet_locs_map_.created()));
  } else if (OB_FAIL(tablet_locs_map_.create(DAS_TABLET_LOC_MAP_BUCKET_SIZE))) {
    LOG_WARN("create tablet locs map failed", KR(ret));
  } else {
    for (DASTabletLocListIter tablet_node = tablet_locs_begin();
         OB_SUCC(ret) && tablet_node != tablet_locs_end(); ++tablet_node) {
      ObDASTabletLoc *tablet_loc = *tablet_node;
      if (OB_FAIL(tablet_locs_map_.set(tablet_loc->tablet_id_, tablet_loc))) {
        LOG_WARN("insert into tablet locs map failed", KR(ret), KPC(tablet_loc));
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
