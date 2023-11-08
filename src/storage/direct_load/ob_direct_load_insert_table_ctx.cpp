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

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace table;

/**
 * ObDirectLoadInsertTableParam
 */

ObDirectLoadInsertTableParam::ObDirectLoadInsertTableParam()
  : table_id_(OB_INVALID_ID), schema_version_(0), snapshot_version_(0), execution_id_(0), ddl_task_id_(0)
{
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam()
{
}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && schema_version_ >= 0 && snapshot_version_ >= 0 &&
         ls_partition_ids_.count() > 0;
}

int ObDirectLoadInsertTableParam::assign(const ObDirectLoadInsertTableParam &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  schema_version_ = other.schema_version_;
  snapshot_version_ = other.snapshot_version_;
  if (OB_FAIL(ls_partition_ids_.assign(other.ls_partition_ids_))) {
    LOG_WARN("fail to assign ls tablet ids", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext()
  : tablet_finish_count_(0), is_inited_(false)
{
}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext()
{
  reset();
}

void ObDirectLoadInsertTableContext::reset()
{
  int ret = OB_SUCCESS;
  if (0 != ddl_ctrl_.context_id_) {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    if (OB_FAIL(sstable_insert_mgr.finish_table_context(ddl_ctrl_.context_id_, false))) {
      LOG_WARN("fail to finish table context", KR(ret), K_(ddl_ctrl));
    }
    ddl_ctrl_.context_id_ = 0;
  }
  is_inited_ = false;
}

int ObDirectLoadInsertTableContext::init(const ObDirectLoadInsertTableParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    ObSSTableInsertTableParam table_insert_param;
    table_insert_param.dest_table_id_ = param.table_id_;
    table_insert_param.snapshot_version_ = 0;
    table_insert_param.schema_version_ = param.schema_version_;
    table_insert_param.task_cnt_ = 1;
    table_insert_param.write_major_ = true;
    table_insert_param.execution_id_ = param.execution_id_;
    table_insert_param.ddl_task_id_ = param.ddl_task_id_;
    table_insert_param.data_format_version_ = param.data_version_;
    for (int64_t i = 0; i < param.ls_partition_ids_.count(); ++i) {
      const ObTableLoadLSIdAndPartitionId &ls_partition_id = param.ls_partition_ids_.at(i);
      if (OB_FAIL(table_insert_param.ls_tablet_ids_.push_back(
            std::make_pair(ls_partition_id.ls_id_, ls_partition_id.part_tablet_id_.tablet_id_)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("fail to assign param", KR(ret));
    } else if (OB_FAIL(sstable_insert_mgr.create_table_context(table_insert_param,
                                                               ddl_ctrl_.context_id_))) {
      LOG_WARN("fail to create table context", KR(ret), K(table_insert_param));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::add_sstable_slice(const ObTabletID &tablet_id,
                                                      const ObMacroDataSeq &start_seq,
                                                      ObNewRowIterator &iter,
                                                      int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
  ObSSTableInsertTabletParam tablet_insert_param;
  tablet_insert_param.context_id_ = ddl_ctrl_.context_id_;
  tablet_insert_param.table_id_ = param_.table_id_;
  tablet_insert_param.tablet_id_ = tablet_id;
  tablet_insert_param.write_major_ = true;
  tablet_insert_param.task_cnt_ = 1;
  tablet_insert_param.schema_version_ = param_.schema_version_;
  tablet_insert_param.snapshot_version_ = param_.snapshot_version_;
  tablet_insert_param.execution_id_ = param_.execution_id_;
  tablet_insert_param.ddl_task_id_ = param_.ddl_task_id_;
  if (OB_FAIL(sstable_insert_mgr.update_table_tablet_context(ddl_ctrl_.context_id_, tablet_id,
                                                             param_.snapshot_version_))) {
    LOG_WARN("fail to update table context", KR(ret), K_(ddl_ctrl), K(tablet_id));
  } else if (OB_FAIL(sstable_insert_mgr.add_sstable_slice(tablet_insert_param, start_seq, iter,
                                                          affected_rows))) {
    LOG_WARN("fail to add sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableContext::construct_sstable_slice_writer(
  const ObTabletID &tablet_id, const ObMacroDataSeq &start_seq,
  ObSSTableInsertSliceWriter *&slice_writer, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    ObSSTableInsertTabletParam tablet_insert_param;
    tablet_insert_param.context_id_ = ddl_ctrl_.context_id_;
    tablet_insert_param.table_id_ = param_.table_id_;
    tablet_insert_param.tablet_id_ = tablet_id;
    tablet_insert_param.write_major_ = true;
    tablet_insert_param.task_cnt_ = 1;
    tablet_insert_param.schema_version_ = param_.schema_version_;
    tablet_insert_param.snapshot_version_ = param_.snapshot_version_;
    tablet_insert_param.execution_id_ = param_.execution_id_;
    tablet_insert_param.ddl_task_id_ = param_.ddl_task_id_;
    if (OB_FAIL(sstable_insert_mgr.update_table_tablet_context(ddl_ctrl_.context_id_, tablet_id,
                                                               param_.snapshot_version_))) {
      LOG_WARN("fail to update table context", KR(ret), K_(ddl_ctrl), K(tablet_id));
    } else if (OB_FAIL(sstable_insert_mgr.construct_sstable_slice_writer(
                 tablet_insert_param, start_seq, slice_writer, allocator))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::notify_tablet_finish(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    int64_t tablet_finish_count = 0;
    if (OB_FAIL(sstable_insert_mgr.notify_tablet_end(ddl_ctrl_.context_id_, tablet_id))) {
      LOG_WARN("fail to notify tablet end", KR(ret), K_(ddl_ctrl), K(tablet_id));
    } else if (FALSE_IT(tablet_finish_count = ATOMIC_AAF(&tablet_finish_count_, 1))) {
    } else if (OB_FAIL(sstable_insert_mgr.finish_ready_tablets(ddl_ctrl_.context_id_,
                                                               tablet_finish_count))) {
      LOG_WARN("fail to finish ready tablets", KR(ret), K_(ddl_ctrl), K(tablet_finish_count));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(tablet_finish_count_ != param_.ls_partition_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected finished tablet count", KR(ret), K(tablet_finish_count_),
             K(param_.ls_partition_ids_.count()));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    if (OB_FAIL(sstable_insert_mgr.finish_table_context(ddl_ctrl_.context_id_, true))) {
      LOG_WARN("fail to finish table context", KR(ret), K_(ddl_ctrl));
    } else {
      ddl_ctrl_.context_id_ = 0;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
