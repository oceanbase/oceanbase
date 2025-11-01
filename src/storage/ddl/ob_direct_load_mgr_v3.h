/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_V3_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_V3_H

#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_i_direct_load_mgr.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
namespace oceanbase
{


namespace storage
{
class ObInsertMonitor;
class ObTabletDirectLoadMgrV3 : public ObBaseTabletDirectLoadMgr
{
public:
  ObTabletDirectLoadMgrV3();
  virtual ~ObTabletDirectLoadMgrV3();
  int init_v2(const ObTabletDirectLoadInsertParam &build_param,
              const int64_t execution_id,
              const ObDirectLoadMgrRole role) override;
  int prepare_index_builder() override;
  int fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                            ObIStoreRowIterator *iter,
                            ObDirectLoadSliceWriter &slice_writer,
                            blocksstable::ObMacroDataSeq &next_seq,
                            ObInsertMonitor *insert_monitor,
                            int64_t &affected_rows) override;
  int fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                            const ObBatchDatumRows &datum_rows,
                            ObDirectLoadSliceWriter &slice_writer,
                            ObInsertMonitor *insert_monitor) override;
  int close_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                             ObDirectLoadSliceWriter &slice_writer,
                             blocksstable::ObMacroDataSeq &next_seq,
                             ObInsertMonitor *insert_monitor,
                             bool &is_all_task_finish) override;
  int fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                    const ObDirectLoadSliceInfo &slice_info,
                                    share::ObTabletCacheInterval &pk_interval,
                                    blocksstable::ObDatumRow &datum_row,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    ObTabletDirectLoadMgrHandle &data_direct_load_handle) override;
  int fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                    const ObDirectLoadSliceInfo &slice_info,
                                    share::ObTabletCacheInterval &pk_interval,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    ObTabletDirectLoadMgrHandle &data_direct_load_handle,
                                    blocksstable::ObBatchDatumRows &datum_rows) override;

  int fill_lob_meta_sstable_slice(const ObDirectLoadSliceInfo &slice_info,
                                  ObIStoreRowIterator *iter,
                                  ObDirectLoadSliceWriter &slice_writer,
                                  int64_t &affected_rows) override;
  int cancel() override { return OB_NOT_SUPPORTED; }
  bool is_valid() override { return is_inited_; }
  int close() override;
  int get_tablet_cache_interval(ObTabletCacheInterval &interval);
  static int prepare_lob_param(const ObTabletDirectLoadInsertParam &build_param, ObTabletDirectLoadInsertParam &lob_param);
  int64_t get_dir_id() {return dir_id_; }
protected:
  int wrlock(const int64_t timeout_us, uint32_t &tid);
  int rdlock(const int64_t timeout_us, uint32_t &tid);
  void unlock(const uint32_t tid);
  int prepare_schema_item_on_demand(const blocksstable::ObWholeDataStoreDesc &data_block_desc,
                                    const ObTabletDirectLoadInsertParam &build_param,
                                    const ObTableSchema &table_schema,
                                    const ObSchemaGetterGuard &schema_guard,
                                    ObIAllocator &allocator,
                                    ObTableSchemaItem &schema_item,
                                    ObIArray<ObColumnSchemaItem> &column_items,
                                    ObIArray<int64_t> &lob_column_idxs,
                                    ObIArray<common::ObObjMeta> &lob_col_types);
  int prepare_index_builder(const ObTabletDirectLoadInsertParam &build_param,
                            const ObITable::TableKey &table_key,
                            const ObTableSchema &table_schema,
                            ObIAllocator &allocator,
                            blocksstable::ObSSTableIndexBuilder *&index_builder,
                            blocksstable::ObWholeDataStoreDesc &data_block_desc);
  virtual int inner_close() = 0;
  static int get_storage_schema(const ObTabletDirectLoadInsertParam &build_param, ObStorageSchema &storage_schema);
  static int get_tablet_handle(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_target_table_type(const ObStorageSchema &storage_schema,
                                   const ObDirectLoadType &direct_load_type,
                                  ObITable::TableType &table_type_);
  static inline bool is_incremental_direct_load(const ObDirectLoadType &type)
  {
    return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type;
  }
public:
  inline int64_t get_execution_id() {return execution_id_;}
  inline share::SCN get_start_scn() {return start_scn_;}
  bool need_process_cs_replica() const override { return false; }
  int64_t get_ddl_task_id() const override { return build_param_.runtime_only_param_.task_id_; }
  ObWholeDataStoreDesc &get_data_block_desc() override { return data_block_desc_; }
  ObTabletDirectLoadInsertParam &get_build_param() override { return build_param_; }
  int64_t get_task_cnt() override { return build_param_.runtime_only_param_.task_cnt_; } // ddl_task_num generate by ddl, always the same in retry
  int64_t get_table_id() { return build_param_.runtime_only_param_.table_id_; }
  int64_t get_parallel() { return build_param_.runtime_only_param_.parallel_; } // generated by px， not equal in retry
  int64_t get_cg_cnt() override
  {
    int64_t ret = -1;
    if (nullptr != storage_schema_) {
      ret = storage_schema_->get_column_group_count();
    }
    return ret;
  }
  const ObIArray<ObColumnSchemaItem> &get_column_info() const override { return column_items_; }
  bool get_micro_index_clustered() override { return micro_index_clustered_; }
  ObDirectLoadMgrRole get_role() { return role_; }
  const ObStorageSchema *get_storage_schema() const { return storage_schema_; }
  virtual void update_store_desc_exec_mode(ObStaticDataStoreDesc &ob_data_store_desc) = 0;
  static const int64_t TRY_LOCK_TIMEOUT = 10 * 1000000; // 10s
protected:
  ObArenaAllocator arena_allocator_;
  int64_t execution_id_;
  ObStorageSchema *storage_schema_;
  share::SCN start_scn_; // mock start scn to pass some valid check
  bool micro_index_clustered_;
  int64_t dir_id_;
  int64_t task_finish_count_;
  ObTableSchemaItem schema_item_; //TODO @zhuoran.zzr consider can be save on sqc_ctx on upper object
  ObArray<ObColumnSchemaItem> column_items_;
  ObArray<int64_t> lob_column_idxs_;
  ObArray<common::ObObjMeta> lob_col_types_;
  blocksstable::ObWholeDataStoreDesc data_block_desc_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
  ObTabletDirectLoadInsertParam  build_param_;
  int64_t seq_interval_task_id_;
  ObDirectLoadMgrRole role_;
  bool is_schema_item_ready_;
  bool is_inited_;
};

class ObSNTabletDirectLoadMgr: public ObTabletDirectLoadMgrV3
{
public:
  ObSNTabletDirectLoadMgr();
  ~ObSNTabletDirectLoadMgr();
  int init_v2(const ObTabletDirectLoadInsertParam &build_param,
              const int64_t execution_id,
              const ObDirectLoadMgrRole role) override;
protected:
  int inner_close();
  int schedule_merge_tablet_task(const ObTabletDDLCompleteArg &arg, const bool wait_major = false);

  void update_store_desc_exec_mode(ObStaticDataStoreDesc &ob_data_store_desc) override
  {
    UNUSED(ob_data_store_desc);
  }
};

class ObSSTabletDirectLoadMgr: public ObTabletDirectLoadMgrV3
{
public:
  ObSSTabletDirectLoadMgr();
  ~ObSSTabletDirectLoadMgr();
  int init_v2(const ObTabletDirectLoadInsertParam &build_param,
              const int64_t execution_id,
              const ObDirectLoadMgrRole role) override;
  int fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                    ObIStoreRowIterator *iter,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    blocksstable::ObMacroDataSeq &next_seq,
                                    ObInsertMonitor *insert_monitor,
                                    int64_t &affected_rows) override;
  int update_max_lob_id(const int64_t lob_id) override;
  ObMacroMetaStoreManager &get_macro_meta_store_manager() { return macro_meta_store_mgr_; }
protected:
  int inner_close() override;
  int update_table_store(const blocksstable::ObSSTable *sstable,
                         const ObStorageSchema *storage_schema,
                         ObITable::TableKey &table_key,
                         ObTablet &tablet);
  int create_ddl_ro_sstable(ObTablet &tablet,
                            common::ObArenaAllocator &allocator,
                            ObTableHandleV2 &sstable_handle);
  void update_max_data_macro_seq(const int64_t cur_data_seq);
  void update_max_meta_macro_seq(const int64_t cur_meta_seq);
  int calc_root_macro_seq(int64_t &root_seq);
  void update_store_desc_exec_mode(ObStaticDataStoreDesc &ob_data_store_desc) { ob_data_store_desc.exec_mode_ = compaction::EXEC_MODE_OUTPUT; }
  int32_t get_private_transfer_epoch() override { return private_transfer_epoch_; }
private:
  int64_t last_data_seq_;
  int64_t last_meta_seq_;
  int64_t last_lob_id_;
  /* slice_cnt_ mean that the tablet has been split some slice
   * task_cnt_ means prallelisim, the number of thread that process this task
   * one thread may process a few tasks
  */
  int64_t total_slice_cnt_;
  int32_t private_transfer_epoch_;
  ObMacroMetaStoreManager macro_meta_store_mgr_;
};

}// namespace storage
}// namespace oceanbase
#endif//OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_V3_H
