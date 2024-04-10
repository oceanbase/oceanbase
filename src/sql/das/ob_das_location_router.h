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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_LOCATION_ROUTER_H_
#define DEV_SRC_SQL_DAS_OB_DAS_LOCATION_ROUTER_H_
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/das/ob_das_define.h"
namespace oceanbase
{
namespace common
{
class ObTabletID;
class ObNewRow;
}  // namespace common
namespace share
{
class ObLSID;
class ObLSLocation;
class ObLSReplicaLocation;
}  // namespace share
namespace sql
{
struct ObDASTableLocMeta;
struct ObDASTabletLoc;
class ObQueryRetryInfo;
class ObDASCtx;
typedef common::ObFixedArray<common::ObAddr, common::ObIAllocator> AddrArray;
typedef common::hash::ObHashMap<common::ObObjectID, common::ObObjectID, common::hash::NoPthreadDefendMode> ObPartitionIdMap;

class VirtualSvrPair
{
public:
  // for Virtual Table
  static const uint64_t EMPTY_VIRTUAL_TABLE_TABLET_ID  = ((uint64_t)1 << 55);
public:
  VirtualSvrPair()
    : table_id_(common::OB_INVALID_ID),
      all_server_()
  { }
  TO_STRING_KV(K_(table_id),
               K_(all_server));
  int init(common::ObIAllocator &allocator,
           common::ObTableID vt_id,
           common::ObIArray<common::ObAddr> &part_locations);
  common::ObTableID get_table_id() const { return table_id_; }
  int get_server_by_tablet_id(const common::ObTabletID &tablet_id, common::ObAddr &addr) const;
  int get_all_part_and_tablet_id(common::ObIArray<common::ObObjectID> &part_ids,
                                 common::ObIArray<common::ObTabletID> &tablet_ids) const;
  int get_part_and_tablet_id_by_server(const common::ObAddr &addr,
                                       common::ObObjectID &part_id,
                                       common::ObTabletID &tablet_id) const;
  void get_default_tablet_and_part_id(common::ObTabletID &tablet_id,
                                    common::ObObjectID &part_id) const;
private:
  uint64_t table_id_;
  AddrArray all_server_;
};

class DASRelatedTabletMap : public share::schema::IRelatedTabletMap
{
  friend class ObDASCtx;
public:
  struct Key
  {
    int hash(uint64_t &res)
    {
      res += common::murmurhash(&src_tablet_id_, sizeof(src_tablet_id_), res);
      res += common::murmurhash(&related_table_id_, sizeof(related_table_id_), res);
      return common::OB_SUCCESS;
    }

    bool operator ==(const Key &other) const
    {
      return (other.src_tablet_id_ == src_tablet_id_
          && other.related_table_id_ == related_table_id_);
    }

    TO_STRING_KV(K_(src_tablet_id),
                 K_(related_table_id));
    common::ObTabletID src_tablet_id_;
    common::ObTableID related_table_id_;
  };

  struct Value
  {
    TO_STRING_KV(K_(tablet_id),
                 K_(part_id),
                 K_(first_level_part_id));
    common::ObTabletID tablet_id_;
    common::ObObjectID part_id_;
    // only valid if partition level of related table is level two
    common::ObObjectID first_level_part_id_;
  };

  struct MapEntry
  {
    OB_UNIS_VERSION(1);
  public:
    TO_STRING_KV(K_(key),
                 K_(val));
    Key key_;
    Value val_;
  };
  typedef common::ObList<MapEntry, common::ObIAllocator> RelatedTabletList;
  typedef common::hash::ObHashMap<Key*, Value*, common::hash::NoPthreadDefendMode> RelatedTabletMap;
public:
  DASRelatedTabletMap(common::ObIAllocator &allocator)
    : list_(allocator),
      allocator_(allocator)
  { }
  virtual ~DASRelatedTabletMap() = default;

  virtual int add_related_tablet_id(common::ObTabletID src_tablet_id,
                                    common::ObTableID related_table_id,
                                    common::ObTabletID related_tablet_id,
                                    common::ObObjectID related_part_id,
                                    common::ObObjectID related_first_level_part_id) override;
  const Value *get_related_tablet_id(common::ObTabletID src_tablet_id,
                                     common::ObTableID related_table_id);
  int assign(const RelatedTabletList &list);
  int insert_related_tablet_map();
  void clear()
  {
    if (!empty()) {
      list_.clear();
      map_.clear();
    }
  }
  bool empty() const { return list_.empty() && map_.empty(); }
  const RelatedTabletList &get_list() const { return list_; }
  TO_STRING_KV(K_(list), "map_size", map_.size());
private:
  const int64_t FAST_LOOP_LIST_LEN = 100;
  //There are usually not many tablets for a query.
  //At this stage, use list to simulate map search,
  //and then optimize if there are performance problems later.
  RelatedTabletList list_;
  //
  RelatedTabletMap map_;
  common::ObIAllocator &allocator_;
};

class ObDASTabletMapper
{
  friend class ObDASCtx;
  typedef common::ObList<DASRelatedTabletMap::MapEntry, common::ObIAllocator> RelatedTabletList;
public:
  ObDASTabletMapper()
    : table_schema_(nullptr),
      vt_svr_pair_(nullptr),
      related_info_(),
      is_non_partition_optimized_(false),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      object_id_(OB_INVALID_ID),
      related_list_(nullptr),
      partition_id_map_(nullptr)
  {
  }

  int get_tablet_and_object_id(const share::schema::ObPartitionLevel part_level,
                               const common::ObPartID part_id,
                               const ObIArray<common::ObNewRange*> &ranges,
                               common::ObIArray<common::ObTabletID> &tablet_ids,
                               common::ObIArray<common::ObObjectID> &out_part_ids);

  int get_tablet_and_object_id(const share::schema::ObPartitionLevel part_level,
                               const common::ObPartID part_id,
                               const common::ObObj &value,
                               common::ObIArray<common::ObTabletID> &tablet_ids,
                               common::ObIArray<common::ObObjectID> &out_part_ids);

  /**
   * Get a set of partition_id and tablet_id according to the range
   * For non-partitioned table or one-level partitions, tablet_ids and object_ids have a one-to-one relationship
   * For sub-partition, only returns the sub-partition id of the specified first partition,
   * and tablet_ids is empty at this time
   *
   * @param[in] part_level: partition level, PARTITION_LEVEL_ZERO or PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO
   * @param[in] part_id: the first partition id, be used when fetch the sub-partition id,
   * it's useless when part_level is PARTITION_LEVEL_ZERO.
   * @param[in] range: the partition key range value
   *
   * @param[out] tablet_ids: the output tablet_id, empty when fetch the first partition id in sub-partition table
   * @param[out] object_ids: the output partition object id or table object id
   * @return
   */
  int get_tablet_and_object_id(
      const share::schema::ObPartitionLevel part_level,
      const common::ObPartID part_id,
      const common::ObNewRange &range,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<common::ObObjectID> &object_ids);
  /**
   * Get partition_id and tablet_id according to the single partition key value
   * For non-partitioned table or one-level partitions, tablet_id is the final data object id,
   * object is the final partition object id or table object id.
   * For sub-partition table, only returns the first-partition id if part_level is PARTITION_LEVEL_ONE
   * tablet_id is invalid id at this time
   *
   * @param[in] part_level: partition level, PARTITION_LEVEL_ZERO or PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO
   * @param[in] part_id: the first-partition id, be used when fetch the sub-partition id,
   * it's useless when part_level is PARTITION_LEVEL_ZERO
   * @param[in] row: the specified partition key value
   *
   * @param[out] tablet_id: output tablet_id, invalid when fetch the first partition id in sub-partition table
   * @param[out] object_id: the output partition object id or table object id
   * @return
   */
  int get_tablet_and_object_id(
      const share::schema::ObPartitionLevel part_level,
      const common::ObPartID part_id,
      const common::ObNewRow &row,
      common::ObTabletID &tablet_id,
      common::ObObjectID &object_id);

  const share::schema::ObTableSchema *get_table_schema() const { return table_schema_; }

  int get_non_partition_tablet_id(common::ObIArray<common::ObTabletID> &tablet_ids,
                                  common::ObIArray<common::ObObjectID> &out_part_ids);
  int get_all_tablet_and_object_id(const share::schema::ObPartitionLevel part_level,
                                   const common::ObPartID part_id,
                                   common::ObIArray<common::ObTabletID> &tablet_ids,
                                   common::ObIArray<common::ObObjectID> &out_part_ids);
  int get_all_tablet_and_object_id(common::ObIArray<common::ObTabletID> &tablet_ids,
                                   common::ObIArray<common::ObObjectID> &out_part_ids);
  int get_default_tablet_and_object_id(const share::schema::ObPartitionLevel part_level,
                                       const common::ObIArray<common::ObObjectID> &part_hint_ids,
                                       common::ObTabletID &tablet_id,
                                       common::ObObjectID &object_id);
  int get_related_partition_id(const common::ObTableID &src_table_id,
                               const common::ObObjectID &src_part_id,
                               const common::ObTableID &dst_table_id,
                               common::ObObjectID &dst_object_id);
  share::schema::RelatedTableInfo &get_related_table_info() { return related_info_; }
  bool is_non_partition_optimized() const { return is_non_partition_optimized_; }
  void set_non_partitioned_table_ids(const common::ObTabletID &tablet_id,
                                     const common::ObObjectID &object_id,
                                     const RelatedTabletList *related_list)
  {
    tablet_id_ = tablet_id;
    object_id_ = object_id;
    related_list_ = related_list;
    is_non_partition_optimized_ = true;
  }
  void set_partition_id_map(ObPartitionIdMap *partition_id_map)
  {
    partition_id_map_ = partition_id_map;
  }
  int set_partition_id_map(common::ObObjectID first_level_part_id, common::ObObjectID object_id);
  int get_partition_id_map(common::ObObjectID first_level_part_id, common::ObObjectID &object_id);
private:
  int mock_vtable_related_tablet_id_map(const common::ObIArray<common::ObTabletID> &tablet_ids,
                                        const common::ObIArray<common::ObObjectID> &out_part_ids);
  int mock_vtable_related_tablet_id_map(const common::ObTabletID &tablet_id,
                                        const common::ObObjectID &part_id);
  int set_partition_id_map(common::ObObjectID first_level_part_id, common::ObIArray<common::ObObjectID> &object_ids);
private:
  const share::schema::ObTableSchema *table_schema_;
  const VirtualSvrPair *vt_svr_pair_;
  share::schema::RelatedTableInfo related_info_;
  bool is_non_partition_optimized_;
  ObTabletID tablet_id_;
  ObObjectID object_id_;
  const RelatedTabletList *related_list_;
  ObPartitionIdMap *partition_id_map_;
};

class ObDASLocationRouter
{
  OB_UNIS_VERSION(1);
  friend class ObDASCtx;
  typedef common::ObList<VirtualSvrPair, common::ObIAllocator> VirtualSvrList;
public:
  ObDASLocationRouter(common::ObIAllocator &allocator);
  ~ObDASLocationRouter();
  int nonblock_get(const ObDASTableLocMeta &loc_meta,
                   const common::ObTabletID &tablet_id,
                   share::ObLSLocation &location);

  int nonblock_get_candi_tablet_locations(const ObDASTableLocMeta &loc_meta,
                                          const common::ObIArray<ObTabletID> &tablet_ids,
                                          const common::ObIArray<ObObjectID> &partition_ids,
                                          const ObIArray<ObObjectID> &first_level_part_ids,
                                          common::ObIArray<ObCandiTabletLoc> &candi_tablet_locs);

  int get_tablet_loc(const ObDASTableLocMeta &loc_meta,
                     const common::ObTabletID &tablet_id,
                     ObDASTabletLoc &tablet_loc);
  int nonblock_get_leader(const uint64_t tenant_id,
                          const ObTabletID &tablet_id,
                          ObDASTabletLoc &tablet_loc);
  int get_leader(const uint64_t tenant_id,
                 const common::ObTabletID &tablet_id,
                 ObAddr &leader_addr,
                 int64_t expire_renew_time);
  int get_full_ls_replica_loc(const common::ObObjectID &tenant_id,
                              const ObDASTabletLoc &tablet_loc,
                              share::ObLSReplicaLocation &replica_loc);
  void refresh_location_cache_by_errno(bool is_nonblock, int err_no);
  void force_refresh_location_cache(bool is_nonblock, int err_no);
  int block_renew_tablet_location(const common::ObTabletID &tablet_id, share::ObLSLocation &ls_loc);
  int save_touched_tablet_id(const common::ObTabletID &tablet_id) { return all_tablet_list_.push_back(tablet_id); }
  void set_last_errno(int err_no) { last_errno_ = err_no; }
  int get_last_errno() const { return last_errno_; }
  void set_history_retry_cnt(int64_t history_retry_cnt) { history_retry_cnt_ = history_retry_cnt; }
  void accumulate_retry_count()
  {
    history_retry_cnt_ += cur_retry_cnt_;
    cur_retry_cnt_ = 0;
  }
  int64_t get_total_retry_cnt() const { return history_retry_cnt_ + cur_retry_cnt_; }
  int64_t get_cur_retry_cnt() const { return cur_retry_cnt_; }
  void reset_cur_retry_cnt() { cur_retry_cnt_ = 0; }
  void inc_cur_retry_cnt() { ++cur_retry_cnt_; }
  void set_retry_info(const ObQueryRetryInfo* retry_info);
  int get_external_table_ls_location(share::ObLSLocation &location);
  void save_cur_exec_status(int err_no)
  {
    if (OB_SUCCESS == cur_errno_) {
      //can't cover the first error code
      cur_errno_ = err_no;
    }
  }
  int save_success_task(const common::ObTabletID &succ_id)
  { return succ_tablet_list_.push_back(succ_id); }
  bool is_refresh_location_error(int err_no) const;
  TO_STRING_KV(K(all_tablet_list_));
private:
  int get_vt_svr_pair(uint64_t vt_id, const VirtualSvrPair *&vt_svr_pair);
  int get_vt_tablet_loc(uint64_t table_id,
                        const common::ObTabletID &tablet_id,
                        ObDASTabletLoc &tablet_loc);
  int get_vt_ls_location(uint64_t table_id,
                         const common::ObTabletID &tablet_id,
                         share::ObLSLocation &location);
  int nonblock_get_readable_replica(const uint64_t tenant_id,
                                    const common::ObTabletID &tablet_id,
                                    ObDASTabletLoc &tablet_loc);
private:
  int last_errno_;
  int cur_errno_;
  int64_t history_retry_cnt_; //Total number of retries before the current retry round.
  int64_t cur_retry_cnt_; // the counter of continuous retry
  // NOTE: Only all_tablet_list_ needs to be serialized and send to other server to perform das remote execution;
  // And other members will be collected by execution server self, No need to perform serialization;
  ObList<common::ObTabletID, common::ObIAllocator> all_tablet_list_;
  ObList<common::ObTabletID, common::ObIAllocator> succ_tablet_list_;
  VirtualSvrList virtual_server_list_;
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASLocationRouter);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_LOCATION_ROUTER_H_ */
