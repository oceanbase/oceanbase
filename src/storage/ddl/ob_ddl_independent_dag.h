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

#ifndef _OCEANBASE_STORAGE_DDL_OB_DDL_INDEPENDENT_DAG_
#define _OCEANBASE_STORAGE_DDL_OB_DDL_INDEPENDENT_DAG_

#include "share/scheduler/ob_independent_dag.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_pipeline.h"

namespace oceanbase
{

namespace storage
{
struct ObDDLChunk;
struct ObDDLTabletContext;
class ObDDLSlice;
class ObTabletDDLKvMgr;

struct ObDDLIndependentDagInitParam : public share::ObIDagInitParam
{
public:
  ObDDLIndependentDagInitParam() : direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), ddl_thread_count_(0), is_inc_major_log_(false) {}
  ObDDLIndependentDagInitParam(const ObDDLIndependentDagInitParam &other)
      : direct_load_type_(other.direct_load_type_),
        ddl_thread_count_(other.ddl_thread_count_),
        ddl_task_param_(other.ddl_task_param_),
        tx_info_(other.tx_info_),
        ls_tablet_ids_(other.ls_tablet_ids_),
        is_inc_major_log_(other.is_inc_major_log_) {}
  virtual bool is_valid() const override
  {
    return is_valid_direct_load(direct_load_type_) &&
           ddl_thread_count_ > 0 &&
           ddl_task_param_.is_valid() &&
           (!is_incremental_direct_load(direct_load_type_) || tx_info_.is_valid()) &&
           ls_tablet_ids_.count() > 0;
  }
  VIRTUAL_TO_STRING_KV(K(direct_load_type_), K(ddl_thread_count_), K(ddl_task_param_), K(ls_tablet_ids_), K(is_inc_major_log_));

public:
  ObDirectLoadType direct_load_type_;
  int64_t ddl_thread_count_;
  ObDDLTaskParam ddl_task_param_;
  ObDirectLoadTxInfo tx_info_;
  ObArray<std::pair<share::ObLSID, ObTabletID>> ls_tablet_ids_;
  bool is_inc_major_log_;
};

class ObDDLIndependentDag : public share::ObIndependentDag
{
public:
  ObDDLIndependentDag();
  virtual ~ObDDLIndependentDag();
  void reuse();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  int get_tablet_context(const ObTabletID &tablet_id, ObDDLTabletContext *&tablet_context);
  const ObDirectLoadType &get_direct_load_type() const { return direct_load_type_; }
  int64_t get_ddl_thread_count() const { return ddl_thread_count_; }
  const ObDDLTaskParam &get_ddl_task_param() const { return ddl_task_param_; }
  const ObDDLTableSchema &get_ddl_table_schema() const { return ddl_table_schema_; }
  const ObDirectLoadTxInfo &get_tx_info() const { return tx_info_; }
  const ObIArray<std::pair<share::ObLSID, ObTabletID>> &get_ls_tablet_ids() { return ls_tablet_ids_; }
  int64_t get_pipeline_count() const { return ATOMIC_LOAD(&pipeline_count_); }
  void inc_pipeline_count() { ATOMIC_INC(&pipeline_count_); }
  void dec_pipeline_count() { ATOMIC_DEC(&pipeline_count_); }
  int add_scan_chunk(ObDDLChunk &ddl_chunk, const int64_t timeout_us = 0);
  virtual bool is_scan_finished() = 0;
  void set_ret_code(const int ret_code);
  int generate_start_tasks(ObIArray<share::ObITask *> &start_tasks, share::ObITask *parent_task);
  int generate_write_macro_block_tasks(ObIArray<share::ObITask *> &write_macro_block_tasks, share::ObITask *next_task = nullptr);
  // for direct load now
  int generate_tablet_write_macro_block_tasks(const ObTabletID &tablet_id,
                                              ObIArray<share::ObITask *> &write_macro_block_tasks,
                                              share::ObITask *parent_task = nullptr);
  int schedule_tablet_merge_task();
  virtual bool use_tablet_mode() const { return false; }
  bool is_inc_major_log() const { return is_inc_major_log_; }
  INHERIT_TO_STRING_KV("IndependentDag", ObIndependentDag, K_(is_inited), K_(direct_load_type),
                       K_(ddl_thread_count), K_(ddl_task_param), K_(pipeline_count), K_(ret_code));

protected:
  int alloc_vector_index_write_and_build_pipeline(
      const ObIndexType &index_type,
      const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids,
      ObIArray<share::ObITask *> &vector_index_task_array);
private:
  int init_ddl_table_schema();
  int init_tablet_context_map();
  int push_chunk(ObDDLSlice *ddl_slice, ObChunk *&chunk_data);
  int add_vector_index_append_pipeline(const ObIndexType &index_type, ObDDLTabletContext *tablet_context, ObDDLSlice *ddl_slice);
  int add_pipeline(ObDDLTabletContext *tablet_context, ObDDLSlice *ddl_slice, const ObIndexType &index_type);

  template<typename T>
  int add_pipeline(ObDDLTabletContext *tablet_context, ObDDLSlice *ddl_slice, T *&pipeline);

  // for_major: true means full direct load, false means inc major
  //          : In ss mode, for inc major direct load, 'for_major' is also true.
  int init_tablet_merge_task(const ObTabletID &tablet_id, const bool for_major, share::ObITask *&data_merge_task, share::ObITask *&lob_merge_task);
  int init_merge_tasks(bool for_major, ObArray<share::ObITask*> &data_merge_tasks, ObArray<share::ObITask*> &lob_merge_tasks);
  int check_is_first_ddl_kv(bool &is_first);
  int check_is_first_ddl_kv(ObTabletDDLKvMgr &ddl_kv_mgr, bool &is_first);

  int inc_generate_write_macro_block_tasks(common::ObIArray<share::ObITask *> &write_macro_block_tasks, share::ObITask *next_task);
  int full_generate_write_macro_block_tasks(common::ObIArray<share::ObITask *> &write_macro_block_tasks, share::ObITask *next_task);
  int finish_chunk(ObChunk *&chunk);

protected:
  bool is_inited_;
  ObArenaAllocator arena_;
  ObDirectLoadType direct_load_type_;
  int64_t ddl_thread_count_;
  ObDDLTaskParam ddl_task_param_;
  ObDDLTableSchema ddl_table_schema_;
  ObDirectLoadTxInfo tx_info_;
  ObArray<std::pair<share::ObLSID, ObTabletID>> ls_tablet_ids_;
  hash::ObHashMap<ObTabletID, ObDDLTabletContext *> tablet_context_map_;
  int64_t pipeline_count_;
  int ret_code_;
  bool is_inc_major_log_;
};

}// namespace storage
}// namespace oceanbase

#endif//_OCEANBASE_STORAGE_DDL_OB_DDL_INDEPENDENT_DAG_
