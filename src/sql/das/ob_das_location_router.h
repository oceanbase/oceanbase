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
class ObDASCtx;
typedef common::ObFixedArray<common::ObAddr, common::ObIAllocator> AddrArray;

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
    int64_t hash() const
    {
      int64_t res = 0;
      res += common::murmurhash(&src_tablet_id_, sizeof(src_tablet_id_), res);
      res += common::murmurhash(&related_table_id_, sizeof(related_table_id_), res);
      return res;
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
                 K_(part_id));
    common::ObTabletID tablet_id_;
    common::ObObjectID part_id_;
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
                                    common::ObObjectID related_part_id) override;
  const Value *get_related_tablet_id(common::ObTabletID src_tablet_id,
                                     common::ObTableID related_table_id);
  int assign(const RelatedTabletList &list);
  int insert_related_tablet_map();
  void clear()
  {
    list_.clear();
    map_.clear();
  }
  const RelatedTabletList &get_list() const { return list_; }
  TO_STRING_KV(K_(list));
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
      related_list_(nullptr)
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
  int get_default_tablet_and_object_id(const common::ObIArray<common::ObObjectID> &part_hint_ids,
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
private:
  int mock_vtable_related_tablet_id_map(const common::ObIArray<common::ObTabletID> &tablet_ids,
                                        const common::ObIArray<common::ObObjectID> &out_part_ids);
  int mock_vtable_related_tablet_id_map(const common::ObTabletID &tablet_id,
                                        const common::ObObjectID &part_id);
private:
  const share::schema::ObTableSchema *table_schema_;
  const VirtualSvrPair *vt_svr_pair_;
  share::schema::RelatedTableInfo related_info_;
  bool is_non_partition_optimized_;
  ObTabletID tablet_id_;
  ObObjectID object_id_;
  const RelatedTabletList *related_list_;
};

class ObDASLocationRouter
{
  friend class ObDASCtx;
  typedef common::ObList<VirtualSvrPair, common::ObIAllocator> VirtualSvrList;
public:
  ObDASLocationRouter(common::ObIAllocator &allocator);
  int get(const ObDASTableLocMeta &loc_meta,
          const common::ObTabletID &tablet_id,
          share::ObLSLocation &location);

  int get_tablet_loc(const ObDASTableLocMeta &loc_meta,
                     const common::ObTabletID &tablet_id,
                     ObDASTabletLoc &tablet_loc);
  static int get_leader(const uint64_t tenant_id,
                        const ObTabletID &tablet_id,
                        ObDASTabletLoc &tablet_loc,
                        int64_t expire_renew_time);
  static int get_leader(const uint64_t tenant_id,
                        const common::ObTabletID &tablet_id,
                        ObAddr &leader_addr,
                        int64_t expire_renew_time);
  int get_full_ls_replica_loc(const common::ObObjectID &tenant_id,
                              const ObDASTabletLoc &tablet_loc,
                              share::ObLSReplicaLocation &replica_loc);
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
                                    ObDASTabletLoc &tablet_loc,
                                    int64_t expire_renew_time);
private:
  VirtualSvrList virtual_server_list_;
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASLocationRouter);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_LOCATION_ROUTER_H_ */
