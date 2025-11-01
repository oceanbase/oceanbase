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
#include "storage/ddl/ob_group_write_macro_block_task.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "storage/ddl/ob_cg_macro_block_write_op.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

ObGroupWriteMacroBlockTask::ObGroupWriteMacroBlockTask()
  : ObITask(TASK_TYPE_DDL_GROUP_WRITE_TASK), ddl_dag_(nullptr)
{

}

ObGroupWriteMacroBlockTask::~ObGroupWriteMacroBlockTask()
{

}

int ObGroupWriteMacroBlockTask::init(ObDDLIndependentDag *ddl_dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ddl_dag_ = ddl_dag;
  }
  return ret;
}

int ObGroupWriteMacroBlockTask::init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_dag), K(tablet_id));
  } else {
    ddl_dag_ = ddl_dag;
    tablet_id_ = tablet_id;
  }
  return ret;
}

int ObGroupWriteMacroBlockTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else if (tablet_id_.is_valid()) {
    if (OB_FAIL(group_write_macro_block(tablet_id_))) {
      LOG_WARN("group write macro block failed", K(ret));
    }
  } else {
    const ObIArray<std::pair<ObLSID, ObTabletID>> &ls_tablet_ids = ddl_dag_->get_ls_tablet_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids.count(); ++i) {
      if (OB_FAIL(group_write_macro_block(ls_tablet_ids.at(i).second))) {
        LOG_WARN("group write macro block failed", K(ret), K(i), "tablet_id", ls_tablet_ids.at(i).second);
      }
    }
  }
  return ret;
}

static void free_cg_block_file(ObCGBlockFile *cg_block_file)
{
  if (cg_block_file != nullptr) {
    cg_block_file->~ObCGBlockFile();
    ob_free(cg_block_file);
    cg_block_file = nullptr;
  }
}

int ObGroupWriteMacroBlockTask::group_write_macro_block(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObArray<ObDDLSlice *> ddl_slices;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_context->get_all_slices(ddl_slices))) {
    LOG_WARN("get all slices failed", K(ret));
  } else {
    struct {
      bool operator() (ObDDLSlice *left, ObDDLSlice *right) {
        return left->get_slice_idx() < right->get_slice_idx();
      }
    } slice_cmp;
    lib::ob_sort(ddl_slices.begin(), ddl_slices.end(), slice_cmp);

    ObStorageSchema *storage_schema = tablet_context->tablet_param_.with_cs_replica_ ?
                                      tablet_context->tablet_param_.cs_replica_storage_schema_ :
                                      tablet_context->tablet_param_.storage_schema_;
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    group_write_tasks_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      int64_t cg_idx = i;
      int64_t data_size = 0;
      ObArray<ObCGBlockFile *> group_files;
      int64_t start_slice_idx = -1;
      for (int64_t j = 0; OB_SUCC(ret) && j < ddl_slices.count(); ++j) {
        ObRemainCgBlock curr_slice_remain;
        if (OB_ISNULL(ddl_slices.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current ddl slice is nul", K(ret), K(j), KP(ddl_slices.at(j)));
        } else if (OB_FAIL(ddl_slices.at(j)->get_remain_block(cg_idx, curr_slice_remain))) {
          LOG_WARN("get remain block failed", K(ret), K(tablet_id), K(cg_idx));
        } else if (OB_UNLIKELY(!curr_slice_remain.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current remain block is invalid", K(ret), K(tablet_id), K(cg_idx), K(j), K(curr_slice_remain), KPC(ddl_slices.at(j)));
          free_cg_block_file(curr_slice_remain.block_file_);
        } else if (nullptr != curr_slice_remain.block_file_) {
          data_size += curr_slice_remain.block_file_->get_data_size();
          if (start_slice_idx < 0) {
            start_slice_idx = ddl_slices.at(j)->get_slice_idx();
          }
          if (OB_FAIL(group_files.push_back(curr_slice_remain.block_file_))) {
            LOG_WARN("push back block file failed", K(ret), K(tablet_id), K(cg_idx), K(j));
            free_cg_block_file(curr_slice_remain.block_file_);
          } else if (data_size >= OB_DEFAULT_MACRO_BLOCK_SIZE) {
            if (OB_FAIL(schedule_write_task(tablet_id, start_slice_idx, cg_idx, group_files))) {
              LOG_WARN("schedule write task failed", K(ret), K(tablet_id), K(cg_idx));
            } else {
              data_size = 0;
              group_files.reuse();
              start_slice_idx = -1;
            }
          }
        } else if (curr_slice_remain.has_flushed_macro_block_) { // slice flushed, data not continue
          if (data_size > 0) {
            if (OB_FAIL(schedule_write_task(tablet_id, start_slice_idx, cg_idx, group_files))) {
              LOG_WARN("schedule write task failed", K(ret), K(tablet_id), K(cg_idx));
            } else {
              data_size = 0;
              group_files.reuse();
              start_slice_idx = -1;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected remain block", K(ret), K(curr_slice_remain));
        }
      }
      if (OB_SUCC(ret) && data_size > 0) {
        if (OB_FAIL(schedule_write_task(tablet_id, start_slice_idx, cg_idx, group_files))) {
          LOG_WARN("schedule write task failed", K(ret), K(tablet_id), K(cg_idx));
        } else {
          data_size = 0;
          group_files.reuse();
          start_slice_idx = -1;
        }
      }
      if (OB_FAIL(ret) && group_files.count() > 0) {
        for (int64_t i = 0; i < group_files.count(); ++i) {
          free_cg_block_file(group_files.at(i));
        }
        group_files.reset();
      }
    }
    if (OB_SUCC(ret) && group_write_tasks_.count() > 0) {
      if (OB_FAIL(ddl_dag_->batch_add_task(group_write_tasks_))) {
        LOG_WARN("batch add task failed", K(ret), K(group_write_tasks_.count()));
      } else {
        LOG_TRACE("batch add group write task success", K(ret), K(tablet_id), K(group_write_tasks_.count()));
      }
    }
  }
  return ret;
}

int ObGroupWriteMacroBlockTask::schedule_write_task(const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t cg_idx, ObIArray<ObCGBlockFile *> &group_files)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else {
    ObGroupCGBlockFileWriteTask *write_task = nullptr;
    if (OB_FAIL(ddl_dag_->alloc_task(write_task))) {
      LOG_WARN("alloc write task failed", K(ret));
    } else if (OB_FAIL(write_task->init(ddl_dag_, tablet_id, slice_idx, cg_idx, group_files))) {
      LOG_WARN("init group write task failed", K(ret), K(tablet_id), K(slice_idx), K(cg_idx), K(group_files));
    } else if (OB_FAIL(copy_children_to(*write_task))) {
      LOG_WARN("add child failed", K(ret));
    } else if (OB_FAIL(group_write_tasks_.push_back(write_task))) {
      LOG_WARN("push back task failed", K(ret));
    }
  }
  return ret;
}

void ObGroupWriteMacroBlockTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  BUF_PRINTF("Group Write Macro Block Task: ddl_dag=%p", ddl_dag_);
}

ObGroupCGBlockFileWriteTask::ObGroupCGBlockFileWriteTask()
  : ObITask(TASK_TYPE_DDL_CG_GROUP_WRITE_TASK), is_inited_(false), ddl_dag_(nullptr), slice_idx_(-1), cg_idx_(-1), block_files_()
{
  block_files_.set_attr(ObMemAttr(MTL_ID(), "GCGBlockFileArr"));
}

ObGroupCGBlockFileWriteTask::~ObGroupCGBlockFileWriteTask()
{
  reset();
}

void ObGroupCGBlockFileWriteTask::reset()
{
  is_inited_ = false;
  ddl_dag_ = nullptr;
  tablet_id_.reset();
  slice_idx_ = -1;
  cg_idx_ = -1;
  for (int i = 0; i < block_files_.count(); ++i) {
    free_cg_block_file(block_files_.at(i));
  }
  block_files_.reset();
}

int ObGroupCGBlockFileWriteTask::init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t cg_idx, ObIArray<ObCGBlockFile *> &group_files)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == ddl_dag || !tablet_id.is_valid() || slice_idx < 0 || cg_idx < 0 || group_files.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_dag), K(tablet_id), K(slice_idx), K(cg_idx), K(group_files.count()));
  } else {
    ddl_dag_ = ddl_dag;
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    cg_idx_ = cg_idx;
    // TODO@youchuan.yc using cg block file handl to manage the cg block file ptr
    const int64_t cg_row_file_count = group_files.count();
    if (OB_FAIL(block_files_.prepare_allocate(cg_row_file_count))) {
      LOG_WARN("fail to prepare allocate", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_row_file_count; ++i) {
      block_files_.at(i) = group_files.at(i);
      group_files.at(i) = nullptr;
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObGroupCGBlockFileWriteTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t row_offset = 0;
    ObWriteMacroParam write_param;
    HEAP_VAR(ObDAGCGMacroBlockWriter, cg_writer) {
    if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, slice_idx_, cg_idx_, ddl_dag_, 0/*max_batch_size*/, write_param))) {
      LOG_WARN("fill write param failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::init_macro_block_seq(write_param.direct_load_type_,
                                                       write_param.tablet_id_,
                                                       slice_idx_,
                                                       write_param.start_sequence_))) {
      LOG_WARN("fail to initialize macro block seq", K(ret), K(write_param.direct_load_type_),
                                                     K(write_param.tablet_id_),
                                                     K(write_param.start_sequence_));
    } else if (OB_FAIL(cg_writer.open(write_param))) {
      LOG_WARN("dag cg writer open failed", K(ret), K(write_param));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < block_files_.count(); ++i) {
      ObCGBlockFile *cur_block_file = block_files_.at(i);
      ObCGBlock cg_block;
      if (OB_ISNULL(cur_block_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current block file is null", K(ret), K(i), KP(cur_block_file));
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(cur_block_file->get_next_cg_block(cg_block))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next cg block failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(cg_writer.append_cg_block(cg_block))) {
          if (OB_BUF_NOT_ENOUGH == ret) {
            // flush, reopen and append again
            ret = OB_SUCCESS;
            if (OB_FAIL(cur_block_file->put_cg_block_back(cg_block))) {
              LOG_WARN("fail to put cg block back", K(ret), K(cg_block));
            } else {
              row_offset += cg_writer.get_written_row_count();
              if (OB_FAIL(cg_writer.close())) {
                LOG_WARN("dag cg writer close failed", K(ret));
              } else {
                write_param.row_offset_ = row_offset;
                write_param.start_sequence_ = cg_writer.get_last_macro_seq();
                cg_writer.reset();
              }
            }
            if (FAILEDx(cg_writer.open(write_param))) {
              LOG_WARN("dag cg writer open failed", K(ret), K(write_param));
            }
          } else {
            LOG_WARN("append cg block failed", K(ret), K(cg_block));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_writer.close())) {
        LOG_WARN("dag cg writer close failed", K(ret));
      }
    }
    } // HEAP_VAR
    // ignore ret
    for (int64_t i = 0; i < block_files_.count(); ++i) {
      free_cg_block_file(block_files_.at(i));
    }
    block_files_.reset();
  }
  return ret;
}

void ObGroupCGBlockFileWriteTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  BUF_PRINTF("Group CG Block File Write Task: tablet_id=%ld, slice_idx=%ld, cg_idx=%ld, block_files_count=%ld, is_inited=%s",
             tablet_id_.id(), slice_idx_, cg_idx_, block_files_.count(), is_inited_ ? "true" : "false");
}
