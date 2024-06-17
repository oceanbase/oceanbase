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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/location_cache/ob_location_struct.h"
#include "common/ob_tablet_id.h"
#include "sql/ob_phy_table_location.h"
#include "rpc/obrpc/ob_rpc_result_code.h"

#define DAS_SCAN_OP(_task_op) \
    (::oceanbase::sql::DAS_OP_TABLE_SCAN != (_task_op)->get_type() && \
        ::oceanbase::sql::DAS_OP_TABLE_BATCH_SCAN != (_task_op)->get_type() ? \
        nullptr : static_cast<::oceanbase::sql::ObDASScanOp*>(_task_op))
#define DAS_GROUP_SCAN_OP(_task_op) \
    (::oceanbase::sql::DAS_OP_TABLE_BATCH_SCAN != (_task_op)->get_type() ? \
        nullptr : static_cast<::oceanbase::sql::ObDASGroupScanOp*>(_task_op))

#define IS_DAS_DML_OP(_task_op)                         \
  ({                                                    \
    DAS_OP_TABLE_INSERT == (_task_op).get_type() ||     \
        DAS_OP_TABLE_UPDATE == (_task_op).get_type() || \
        DAS_OP_TABLE_LOCK == (_task_op).get_type() ||   \
        DAS_OP_TABLE_DELETE == (_task_op).get_type();   \
  })

namespace oceanbase
{
namespace sql
{
class ObDASTaskArg;
class ObIDASTaskOp;
class ObExecContext;
class ObPhysicalPlan;
class ObChunkDatumStore;
class ObEvalCtx;

namespace das
{
//reserve 8K for das write buffer's another structure
const int64_t OB_DAS_MAX_PACKET_SIZE = 2 * 1024 * 1024l - 8 * 1024;
/**
 * Generally, the most common configuration of a cluster is 3 zones.
 * When the leader is randomly distributed,
 * it can be considered that the DAS request will send at most one RPC request to each zone.
 * so OB_DAS_MAX_TOTAL_PACKET_SIZE was defined as:
 */
const int64_t OB_DAS_MAX_TOTAL_PACKET_SIZE = 1 * OB_DAS_MAX_PACKET_SIZE;
const int64_t OB_DAS_MAX_META_TENANT_PACKET_SIZE = 1 * 1024 * 1024l - 8 * 1024;
}  // namespace das

enum class ObDasTaskStatus: uint8_t
{
  UNSTART = 0,
  FAILED,
  FINISHED
};

enum ObDASOpType
{
  //can not adjust the order of DASOpType, append OpType at the last
  DAS_OP_INVALID = 0,
  DAS_OP_TABLE_SCAN,
  DAS_OP_TABLE_INSERT,
  DAS_OP_TABLE_UPDATE,
  DAS_OP_TABLE_DELETE,
  DAS_OP_TABLE_LOCK,
  DAS_OP_TABLE_BATCH_SCAN,
  DAS_OP_SPLIT_MULTI_RANGES,
  DAS_OP_GET_RANGES_COST,
  DAS_OP_TABLE_LOOKUP,
  DAS_OP_IR_SCAN,
  DAS_OP_IR_AUX_LOOKUP,
  DAS_OP_SORT,
  //append OpType before me
  DAS_OP_MAX
};

typedef common::ObFixedArray<common::ObTableID, common::ObIAllocator> DASTableIDArray;

typedef common::ObIArrayWrap<common::ObTableID> DASTableIDArrayWrap;

struct ObDASTableLocMeta
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLocMeta(common::ObIAllocator &alloc)
    : table_loc_id_(common::OB_INVALID_ID),
      ref_table_id_(common::OB_INVALID_ID),
      related_table_ids_(alloc),
      flags_(0)
  { }
  ~ObDASTableLocMeta() = default;
  int assign(const ObDASTableLocMeta &other);
  int init_related_meta(uint64_t related_table_id, ObDASTableLocMeta &related_meta) const;
  void reset()
  {
    table_loc_id_ = common::OB_INVALID_ID;
    ref_table_id_ = common::OB_INVALID_ID;
    flags_ = 0;
    related_table_ids_.reset();
  }

  TO_STRING_KV(K_(table_loc_id),
               K_(ref_table_id),
               K_(related_table_ids),
               K_(use_dist_das),
               K_(select_leader),
               K_(is_dup_table),
               K_(is_weak_read),
               K_(unuse_related_pruning),
               K_(is_external_table));

  uint64_t table_loc_id_; //location object id
  uint64_t ref_table_id_; //table object id
  DASTableIDArray related_table_ids_;
  union {
    uint64_t flags_;
    struct {
      uint64_t use_dist_das_                    : 1; //mark whether this table touch data through distributed DAS
      uint64_t select_leader_                   : 1; //mark whether this table use leader replica
      uint64_t is_dup_table_                    : 1; //mark if this table is a duplicated table
      uint64_t is_weak_read_                    : 1; //mark if this tale can use weak read consistency
      uint64_t unuse_related_pruning_           : 1; //mark if this table use the related pruning to prune local index tablet_id
      uint64_t is_external_table_               : 1; //mark if this table is an external table
      uint64_t is_external_files_on_disk_       : 1; //mark if files in external table are located at local disk
      uint64_t reserved_                        : 57;
    };
  };

private:
  void light_assign(const ObDASTableLocMeta &other); //without array
};
typedef common::ObFixedArray<ObDASTableLocMeta, common::ObIAllocator> DASTableLocMetaArray;
/**
 * ObDASTabletLoc is generated by PlanCache or Optimizer,
 * and used in the execution engine
 * or dynamically calculated by the operator during execution, for example:
 *               DISTRIBUTED UPDATE
 *                       |
 *                NESTED LOOP JOIN
 *                /            \
 *             TSC(t1)        TSC(t2)
 **/
struct ObDASTabletLoc
{
  OB_UNIS_VERSION(1);
public:
  ObDASTabletLoc()
    : tablet_id_(),
      ls_id_(),
      server_(),
      loc_meta_(nullptr),
      next_(this),
      flags_(0),
      partition_id_(OB_INVALID_ID),
      first_level_part_id_(OB_INVALID_ID)
  { }
  ~ObDASTabletLoc() = default;

  TO_STRING_KV(K_(tablet_id),
               K_(ls_id),
               K_(server),
               K_(in_retry),
               K_(partition_id),
               K_(first_level_part_id));
  /**
   * BE CAREFUL!!! can't declare implicit allocator or
   * data structure holding implicit allocator here,
   * such as ObSEArray, ObArray, ObRowStore, etc.
   * Otherwise it will cause memory leak,
   * because ObDASTabletLoc will not be released
   */
  common::ObTabletID tablet_id_; //the data_object_id corresponding to partition_id
  //To reduce the conversion between tablet_id to ls_id,
  //DAS caches ls_id_, SQL should not actually touch ls_id_
  share::ObLSID ls_id_;
  common::ObAddr server_;
  const ObDASTableLocMeta *loc_meta_; //reference the table location meta, not serialize it
  ObDASTabletLoc *next_; //to bind all data table and local index tablet location
  union {
    uint64_t flags_;
    struct {
      uint64_t in_retry_                        : 1; //need to refresh tablet location cache
      uint64_t reserved_                        : 63;
    };
  };
  // partition id of this tablet
  uint64_t partition_id_;
  // first level part id of this tablet, only valid for subpartitioned table.
  uint64_t first_level_part_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTabletLoc);
  int assign(const ObDASTabletLoc &other);
};

static const int64_t DAS_TABLET_LOC_LOOKUP_THRESHOLD = 1000;
static const int64_t DAS_TABLET_LOC_SIZE_THRESHOLD = 10;
static const int64_t DAS_TABLET_LOC_MAP_BUCKET_SIZE = 5000;

typedef common::ObList<ObDASTabletLoc*, common::ObIAllocator> DASTabletLocList;
typedef common::ObList<ObDASTabletLoc*, common::ObIAllocator>::iterator DASTabletLocListIter;
typedef common::ObIArray<ObDASTabletLoc*> DASTabletLocIArray;
typedef common::ObSEArray<ObDASTabletLoc*, 1> DASTabletLocSEArray;
typedef common::ObArray<ObDASTabletLoc*> DASTabletLocArray;

class TabletHashMap
{
  struct TabletHashNode
  {
    ObTabletID key_;
    ObDASTabletLoc *value_;
    struct TabletHashNode *next_;
  };
public:
  TabletHashMap(common::ObIAllocator &allocator)
    : allocator_(allocator), bucket_num_(0), buckets_(NULL), is_inited_(false)
  {}
  virtual ~TabletHashMap() {} // does not free memory
  int create(int64_t bucket_num);
  bool created() const { return is_inited_; }
  int set(const ObTabletID key, ObDASTabletLoc *value);
  int get(const ObTabletID key, ObDASTabletLoc *&value);
private:
  int find_node(const ObTabletID key, TabletHashNode *head, TabletHashNode *&node) const;

  common::ObIAllocator &allocator_;
  int64_t bucket_num_;
  TabletHashNode **buckets_;
  bool is_inited_;
};

/**
 * store the location information of which tables are accessed in this plan
 * generate this table location when this plan is chosen in the plan cache or generated this plan by CG
 * for multiple part dml operator, maybe generated when the operator is opened
 */
struct ObDASTableLoc
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLoc(common::ObIAllocator &allocator)
    : allocator_(allocator),
      loc_meta_(nullptr),
      flags_(0),
      tablet_locs_(allocator),
      tablet_locs_map_(allocator),
      lookup_cnt_(0)
  { }
  ~ObDASTableLoc() = default;

  int assign(const ObCandiTableLoc &candi_table_loc);
  int64_t get_table_location_key() const { return loc_meta_->table_loc_id_; }
  int64_t get_ref_table_id() const { return loc_meta_->ref_table_id_; }
  bool empty() const { return tablet_locs_.size() == 0; }
  const DASTabletLocList &get_tablet_locs() const { return tablet_locs_; }
  ObDASTabletLoc *get_first_tablet_loc() { return tablet_locs_.get_first(); }
  DASTabletLocListIter tablet_locs_begin() { return tablet_locs_.begin(); }
  DASTabletLocListIter tablet_locs_end() { return tablet_locs_.end(); }
  int get_tablet_loc_by_id(const ObTabletID &tablet_id, ObDASTabletLoc *&tablet_loc);
  int add_tablet_loc(ObDASTabletLoc *table_loc);

  TO_STRING_KV(KPC_(loc_meta),
               K_(tablet_locs),
               K_(is_writing),
               K_(is_reading),
               K_(need_refresh),
               K_(is_fk_check));

  /**
   * BE CAREFUL!!! can't declare implicit allocator or
   * data structure holding implicit allocator here,
   * such as ObSEArray, ObArray, ObRowStore, etc.
   * Otherwise it will cause memory leak,
   * because ObDASTableLoc will not be released
   */
  common::ObIAllocator &allocator_;
  const ObDASTableLocMeta *loc_meta_;
  union {
    /**
     * used to mark some status related to table access,
     * and reserve some expansion bits for subsequent needs
     */
    uint64_t flags_;
    struct {
      uint64_t is_writing_                      : 1; //mark this table is writing
      uint64_t is_reading_                      : 1; //mark this table is reading
      uint64_t rebuild_reference_               : 1; //mark whether rebuild the related reference
      uint64_t need_refresh_                    : 1;
      uint64_t is_fk_check_                     : 1; //mark this table is used for foreign key checking
      uint64_t reserved_                        : 59;
    };
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTableLoc);
  int assign(const ObDASTableLoc &other);
  int create_tablet_locs_map();

  /**
   * The reason for using ObList to store ObTabletLoc objects is that
   * during the execution process,
   * some operators will dynamically extend some new partitions,
   * so tablet_locs_ needs to be able to expand dynamically.
   * ObArray/ObSEArray will cause dynamic extension or release of memory when expanding members.
   * At the same time, in partition task retry,
   * it is very convenient to clear the invalid TabletLoc object,
   * so ObList is more suitable for its needs.
   * Why not use ObDList?
   * Considering that ObTabletLoc may be referenced by list of other modules, such as PX,
   * and elements in ObDList can only be referenced by one list at the same time,
   * ObDList has more restrictions and is not suitable as a container for ObTabletLoc
   **/
  DASTabletLocList tablet_locs_;
  TabletHashMap tablet_locs_map_;
  int64_t lookup_cnt_;
};
typedef common::ObList<ObDASTableLoc*, common::ObIAllocator> DASTableLocList;
typedef common::ObList<uint64_t, common::ObIAllocator> DASTableIdList;
typedef common::ObFixedArray<uint64_t, common::ObIAllocator> UIntFixedArray;
typedef common::ObFixedArray<int64_t, common::ObIAllocator> IntFixedArray;
typedef common::ObFixedArray<ObObjectID, common::ObIAllocator> ObjectIDFixedArray;
typedef common::ObFixedArray<ObDASTableLoc*, common::ObIAllocator> DASTableLocFixedArray;
typedef common::ObFixedArray<common::ObString, common::ObIAllocator> ExternalFileNameArray;

//DAS: data access service
//CtDef: Compile time Definition
struct ObDASBaseCtDef
{
  OB_UNIS_VERSION_PV();
public:
  ObDASOpType op_type_;
  ObDASBaseCtDef **children_;
  uint32_t children_cnt_;

  virtual ~ObDASBaseCtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(children_cnt));

  virtual bool has_expr() const { return false; }
  virtual bool has_pdfilter_or_calc_expr() const { return false; }
  virtual bool has_pl_udf() const { return false; }

protected:
  ObDASBaseCtDef(ObDASOpType op_type)
    : op_type_(op_type),
      children_(nullptr),
      children_cnt_(0)
  { }
};

//DAS: data access service
//RtDef: Runtime Definition
struct ObDASBaseRtDef
{
  OB_UNIS_VERSION_PV();
public:
  ObDASOpType op_type_;
  const ObDASBaseCtDef *ctdef_;
  ObEvalCtx *eval_ctx_; //nullptr in DML DAS Op
  ObDASTableLoc *table_loc_;
  ObDASBaseRtDef **children_;
  uint32_t children_cnt_;

  virtual ~ObDASBaseRtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(children_cnt));
protected:
  ObDASBaseRtDef(ObDASOpType op_type)
    : op_type_(op_type),
      ctdef_(nullptr),
      eval_ctx_(NULL),
      table_loc_(nullptr),
      children_(nullptr),
      children_cnt_(0)
  { }
};
typedef common::ObFixedArray<const ObDASBaseCtDef*, common::ObIAllocator> DASCtDefFixedArray;
typedef common::ObFixedArray<ObDASBaseRtDef*, common::ObIAllocator> DASRtDefFixedArray;

OB_INLINE void duplicate_type_to_loc_meta(ObDuplicateType v, ObDASTableLocMeta &loc_meta)
{
  switch (v) {
    case ObDuplicateType::NOT_DUPLICATE:
      loc_meta.is_dup_table_ = 0;
      break;
    case ObDuplicateType::DUPLICATE:
      loc_meta.is_dup_table_ = 1;
      loc_meta.select_leader_ = 0;
      break;
    case ObDuplicateType::DUPLICATE_IN_DML:
      loc_meta.is_dup_table_ = 1;
      loc_meta.select_leader_ = 1;
      break;
    default:
      break;
  }
}

OB_INLINE ObDuplicateType loc_meta_to_duplicate_type(const ObDASTableLocMeta &loc_meta)
{
  ObDuplicateType dup_type = ObDuplicateType::NOT_DUPLICATE;
  if (loc_meta.is_dup_table_) {
    dup_type = loc_meta.select_leader_ ?
        ObDuplicateType::DUPLICATE_IN_DML : ObDuplicateType::DUPLICATE;
  }
  return dup_type;
}

enum ObTSCIRScanType : uint8_t
{
  OB_NOT_A_SPEC_SCAN = 0,
  OB_IR_DOC_ID_IDX_AGG,
  OB_IR_INV_IDX_AGG,
  OB_IR_INV_IDX_SCAN,
  OB_IR_FWD_IDX_AGG,
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_ */
