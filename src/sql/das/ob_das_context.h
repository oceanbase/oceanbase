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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_CONTEXT_H_
#define DEV_SRC_SQL_DAS_OB_DAS_CONTEXT_H_
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_location_router.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/das/ob_das_factory.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASTabletMapper;

struct DmlRowkeyDistCtx
{
public:
  DmlRowkeyDistCtx()
    : deleted_rows_(nullptr),
    table_id_(common::OB_INVALID_ID)
  {}
  SeRowkeyDistCtx *deleted_rows_;
  uint64_t table_id_;
};
typedef common::ObList<DmlRowkeyDistCtx, common::ObIAllocator> DASDelCtxList;

struct GroupRescanParam
{
public:
  GroupRescanParam()
    : param_idx_(common::OB_INVALID_ID),
      gr_param_(nullptr)
  { }
  GroupRescanParam(int64_t param_idx, ObSqlArrayObj *gr_param)
  : param_idx_(param_idx),
    gr_param_(gr_param)
  { }
  TO_STRING_KV(K_(param_idx),
               KPC_(gr_param));
  int64_t param_idx_;
  ObSqlArrayObj *gr_param_; //group rescan param
};

typedef common::ObArrayWrap<GroupRescanParam> GroupParamArray;
class ObDASCtx
{
  friend class DASGroupScanMarkGuard;
  friend class GroupParamBackupGuard;
  OB_UNIS_VERSION(1);
public:
  ObDASCtx(common::ObIAllocator &allocator)
    : table_locs_(allocator),
      external_table_locs_(allocator),
      sql_ctx_(nullptr),
      location_router_(allocator),
      das_factory_(allocator),
      related_tablet_map_(allocator),
      allocator_(allocator),
      snapshot_(),
      savepoint_(),
      write_branch_id_(0),
      del_ctx_list_(allocator),
      group_params_(nullptr),
      skip_scan_group_id_(-1),
      group_rescan_cnt_(-1),
      same_tablet_addr_(),
      flags_(0)
  {
    is_fk_cascading_ = 0;
    need_check_server_ = 1;
    same_server_ = 1;
    iter_uncommitted_row_ = 0;
  }
  ~ObDASCtx()
  {
    // Destroy the hash set list used for checking duplicate rowkey for foreign key cascade delete
    if (!del_ctx_list_.empty()) {
      DASDelCtxList::iterator iter = del_ctx_list_.begin();
      for (; iter != del_ctx_list_.end(); iter++) {
        DmlRowkeyDistCtx& del_ctx = *iter;
        if (del_ctx.deleted_rows_ != nullptr) {
          del_ctx.deleted_rows_->destroy();
          del_ctx.deleted_rows_ = nullptr;
        }
      }
    }
    del_ctx_list_.destroy();
  }

  int init(const ObPhysicalPlan &plan, ObExecContext &ctx);
  ObDASTableLoc *get_table_loc_by_id(uint64_t table_loc_id, uint64_t ref_table_id);
  ObDASTableLoc *get_external_table_loc_by_id(uint64_t table_loc_id, uint64_t ref_table_id);
  DASTableLocList &get_table_loc_list() { return table_locs_; }
  const DASTableLocList &get_table_loc_list() const { return table_locs_; }
  DASDelCtxList& get_das_del_ctx_list() {return  del_ctx_list_;}
  DASTableLocList &get_external_table_loc_list() { return external_table_locs_; }
  int extended_tablet_loc(ObDASTableLoc &table_loc,
                          const common::ObTabletID &tablet_id,
                          ObDASTabletLoc *&tablet_loc,
                          const common::ObObjectID &partition_id = OB_INVALID_ID,
                          const common::ObObjectID &first_level_part_id = OB_INVALID_ID);
  int extended_tablet_loc(ObDASTableLoc &table_loc,
                          const ObCandiTabletLoc &candi_tablet_loc,
                          ObDASTabletLoc *&talet_loc);
  int extended_table_loc(const ObDASTableLocMeta &loc_meta, ObDASTableLoc *&table_loc);
  int add_candi_table_loc(const ObDASTableLocMeta &loc_meta, const ObCandiTableLoc &candi_table_loc);
  int add_final_table_loc(const ObDASTableLocMeta &loc_meta,
                          const ObIArray<ObTabletID> &tablet_ids,
                          const ObIArray<ObObjectID> &partition_ids,
                          const ObIArray<ObObjectID> &first_level_part_ids);
  int build_table_loc_meta(const ObDASTableLocMeta &src, ObDASTableLocMeta *&dst);
  int get_das_tablet_mapper(const uint64_t ref_table_id,
                            ObDASTabletMapper &tablet_mapper,
                            const DASTableIDArrayWrap *related_table_ids = nullptr);
  int get_all_lsid(share::ObLSArray &ls_ids);
  int64_t get_related_tablet_cnt() const;
  void set_snapshot(const transaction::ObTxReadSnapshot &snapshot) { snapshot_ = snapshot; }
  transaction::ObTxReadSnapshot &get_snapshot() { return snapshot_; }
  transaction::ObTxSEQ get_savepoint() const { return savepoint_; }
  void set_savepoint(const transaction::ObTxSEQ savepoint) { savepoint_ = savepoint; }
  void set_write_branch_id(const int16_t branch_id) { write_branch_id_ = branch_id; }
  int16_t get_write_branch_id() const { return write_branch_id_; }
  ObDASLocationRouter &get_location_router() { return location_router_; }
  int build_related_tablet_loc(ObDASTabletLoc &tablet_loc);
  int build_related_table_loc(ObDASTableLoc &table_loc);
  int rebuild_tablet_loc_reference();
  const GroupParamArray* get_group_params() { return group_params_; }
  int64_t get_skip_scan_group_id() const { return skip_scan_group_id_; }
  int64_t get_group_rescan_cnt() const { return group_rescan_cnt_; }
  void clear_all_location_info()
  {
    table_locs_.clear();
    related_tablet_map_.clear();
    external_table_locs_.clear();
    same_tablet_addr_.reset();
    same_server_ = 1;
  }
  ObDASTaskFactory &get_das_factory() { return das_factory_; }
  void set_sql_ctx(ObSqlCtx *sql_ctx) { sql_ctx_ = sql_ctx; }
  DASRelatedTabletMap &get_related_tablet_map() { return related_tablet_map_; }
  bool is_partition_hit();
  void unmark_need_check_server();

  int build_external_table_location(
      uint64_t table_loc_id, uint64_t ref_table_id, common::ObIArray<ObAddr> &locations);
  int build_related_tablet_map(const ObDASTableLocMeta &loc_meta);
  const ObAddr &same_tablet_addr() const { return same_tablet_addr_; }

  int find_group_param_by_param_idx(int64_t param_idx,
                                    bool &exist, uint64_t &array_idx);

  TO_STRING_KV(K_(table_locs),
               K_(external_table_locs),
               K_(is_fk_cascading),
               K_(snapshot),
               K_(savepoint),
               K_(write_branch_id));
private:
  int check_same_server(const ObDASTabletLoc *tablet_loc);
private:
  DASTableLocList table_locs_;
  /*  The external table locations stored in table_locs_ are fake local locations generated by optimizer.
   *  The real locations of external table are determined at runtime when building dfos by QC.
   *  external_cached_table_locs_ are "cached values" which only used by QC and do not need to serialized to SQC.
   */
  DASTableLocList external_table_locs_;
  ObSqlCtx *sql_ctx_;
  ObDASLocationRouter location_router_;
  ObDASTaskFactory das_factory_;
  DASRelatedTabletMap related_tablet_map_;
  common::ObIAllocator &allocator_;
  transaction::ObTxReadSnapshot snapshot_;           // Mvcc snapshot
  transaction::ObTxSEQ savepoint_;                   // DML savepoint
  // for DML like `insert update` and `replace`, which use savepoint to
  // resolve conflicts and when these DML executed under partition-wise
  // style, they need rollback their own writes but not all, we assign
  // id to data writes by different writer thread (named branch)
  int16_t write_branch_id_;
  //@todo: save snapshot version
  DASDelCtxList del_ctx_list_;
  const GroupParamArray *group_params_; //only allowed to be modified by GroupParamBackupGuard
  int64_t skip_scan_group_id_; //only allowed to be modified by GroupParamBackupGuard
  int64_t group_rescan_cnt_; //only allowed to be modified by GroupParamBackupGuard
  ObAddr same_tablet_addr_;
public:
  union {
    uint64_t flags_;
    struct {
      uint64_t is_fk_cascading_                 : 1; //fk starts to trigger nested sql
      uint64_t need_check_server_               : 1; //need to check if partitions hit the same server
      uint64_t same_server_                     : 1; //if partitions hit the same server, could be local or remote
      uint64_t iter_uncommitted_row_            : 1; //iter uncommitted row in fk_checker
      uint64_t in_das_group_scan_               : 1; //the current execution in das group scan
      uint64_t reserved_                        : 59;
    };
  };
};

class GroupParamBackupGuard
{
public:
  GroupParamBackupGuard(ObDASCtx &ctx)
  : ctx_(ctx)
  {
    current_group_ = ctx.skip_scan_group_id_;
    group_rescan_cnt_ = ctx.group_rescan_cnt_;
    group_params_ = ctx.get_group_params();
  }

  void bind_batch_rescan_params(int64_t current_group,
                                int64_t group_rescan_cnt,
                                const GroupParamArray *group_params)
  {
    ctx_.skip_scan_group_id_ = current_group;
    ctx_.group_rescan_cnt_ = group_rescan_cnt;
    ctx_.group_params_ = group_params;
  }

  ~GroupParamBackupGuard() {
    ctx_.skip_scan_group_id_ = current_group_;
    ctx_.group_rescan_cnt_ = group_rescan_cnt_;
    ctx_.group_params_ = group_params_;
  }

private:
  ObDASCtx &ctx_;
  int64_t current_group_;
  int64_t group_rescan_cnt_;
  const GroupParamArray *group_params_;
};

class DASGroupScanMarkGuard
{
public:
  DASGroupScanMarkGuard(ObDASCtx &das_ctx, bool in_das_group_scan)
    : das_ctx_(das_ctx)
  {
    in_das_group_scan_ = das_ctx.in_das_group_scan_;
    das_ctx.in_das_group_scan_ = in_das_group_scan;
  }
  ~DASGroupScanMarkGuard()
  {
    das_ctx_.in_das_group_scan_ = in_das_group_scan_;
  }
private:
  bool in_das_group_scan_;
  ObDASCtx &das_ctx_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_CONTEXT_H_ */
