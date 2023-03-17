// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;
using namespace table;
using namespace lib;
using namespace share;
using namespace share::schema;
using namespace blocksstable;
using namespace sql;
namespace observer
{
ObTableLoadErrorRowHandler::PartitionRowkey::~PartitionRowkey()
{
  if (OB_NOT_NULL(last_rowkey_.get_datum_ptr())) {
    allocator_.free((void *)(last_rowkey_.get_datum_ptr()));
  }
}

ObTableLoadErrorRowHandler::PartitionRowkeyMap::~PartitionRowkeyMap()
{
  auto release_map_entry = [this](HashMapPair<ObTabletID, PartitionRowkey *> &entry) {
    ObTableLoadErrorRowHandler::PartitionRowkey *part_rowkey = entry.second;
    part_rowkey->~PartitionRowkey();
    allocator_.free((void *)part_rowkey);
    return 0;
  };
  map_.foreach_refactored(release_map_entry);
}

ObTableLoadErrorRowHandler::ObTableLoadErrorRowHandler()
  : capacity_(0),
    session_cnt_(0),
    safe_allocator_(row_allocator_),
    error_row_cnt_(0),
    repeated_row_cnt_(0),
    is_inited_(false)
{
}

ObTableLoadErrorRowHandler::~ObTableLoadErrorRowHandler()
{
}

int ObTableLoadErrorRowHandler::init(ObTableLoadTableCtx *const ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadErrorRowHandler init twice", KR(ret), KP(this));
  } else {
    param_ = ctx->param_;
    job_stat_ = ctx->job_stat_;
    datum_utils_ = &(ctx->schema_.datum_utils_);
    col_descs_ = &(ctx->schema_.column_descs_);
    capacity_ = ctx->param_.max_error_row_count_;
    rowkey_column_num_ = ctx->schema_.rowkey_column_count_;
    session_cnt_ = ctx->param_.session_count_;
    if (OB_FAIL(session_maps_.prepare_allocate(session_cnt_))) {
      LOG_WARN("failed to pre allocate session maps", K(ret), K(session_cnt_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < session_maps_.count()); ++i) {
        if (OB_FAIL(session_maps_.at(i).map_.create(1024, "TLD_err_chk_map", "TLD_err_chk_map",
                                                    ctx->param_.tenant_id_))) {
          LOG_WARN("fail to create map", KR(ret), K(ctx->param_.tenant_id_));
        } else {
          ObArenaAllocator &allocator = session_maps_.at(i).allocator_;
          allocator.set_label("TLD_err_chk");
          allocator.set_tenant_id(MTL_ID());
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadErrorRowHandler::inner_append_error_row(const ObNewRow &row,
                                                       ObIArray<ObNewRow> &error_row_array)
{
  int ret = OB_SUCCESS;
  ObNewRow new_row;
  ObMutexGuard guard(append_row_mutex_);
  if (error_row_cnt_ >= capacity_) {
    ret = OB_ERR_TOO_MANY_ROWS;
    LOG_WARN("error row count reaches its maximum value", K(ret), K(capacity_), K(error_row_cnt_));
  } else if (OB_FAIL(ObTableLoadUtils::deep_copy(row, new_row, safe_allocator_))) {
    LOG_WARN("failed to deep copy new row", K(ret), K(row));
  } else if (OB_FAIL(error_row_array.push_back(new_row))) {
    LOG_WARN("failed to push back error row", K(ret), K(new_row));
  } else {
    error_row_cnt_++;
    ATOMIC_INC(&job_stat_->detected_error_rows_);
  }
  return ret;
}

int ObTableLoadErrorRowHandler::inner_append_repeated_row(const ObNewRow &row,
                                                          ObIArray<ObNewRow> &repeated_row_array)
{
  int ret = OB_SUCCESS;
  ObNewRow new_row;
  ObMutexGuard guard(append_row_mutex_);
  if (repeated_row_cnt_ >= DEFAULT_REPEATED_ERROR_ROW_COUNT) {
    ret = OB_ERR_TOO_MANY_ROWS;
    LOG_WARN("repeated row count reaches its maximum value", K(ret), K(capacity_),
              K(repeated_row_cnt_));
  } else if (OB_FAIL(ObTableLoadUtils::deep_copy(row, new_row, safe_allocator_))) {
    LOG_WARN("failed to deep copy new row", K(ret), K(row));
  } else if (OB_FAIL(repeated_row_array.push_back(new_row))) {
    LOG_WARN("failed to push back error row", K(ret), K(new_row));
  } else {
    repeated_row_cnt_ ++;
  }
  return ret;
}

int ObTableLoadErrorRowHandler::append_error_row(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_append_error_row(row, error_row_array_))) {
    LOG_WARN("failed to append row to str error row array", K(ret), K(row));
  }
  return ret;
}

int ObTableLoadErrorRowHandler::append_error_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObNewRow new_row;
    ObObjBufArray obj_buf;
    ObArenaAllocator allocator;
    if (OB_FAIL(obj_buf.init(&allocator))) {
      LOG_WARN("fail to init obj buf", KR(ret));
    } else if (OB_FAIL(obj_buf.reserve(row.count_))) {
      LOG_WARN("Failed to reserve buf for obj buf", K(ret), K(row.count_));
    } else {
      new_row.cells_ = obj_buf.get_data();
      new_row.count_ = row.count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < (*col_descs_).count(); i++) {
        if (OB_FAIL(row.storage_datums_[i].to_obj_enhance(new_row.cells_[i], (*col_descs_).at(i).col_type_))) {
          LOG_WARN("Failed to transform datum to obj", K(ret), K(i), K(row.storage_datums_[i]));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(inner_append_error_row(new_row, error_new_row_array_))) {
          LOG_WARN("failed to append row to error row array", K(ret), K(new_row));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadErrorRowHandler::append_repeated_row(const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (param_.dup_action_ == ObLoadDupActionType::LOAD_STOP_ON_DUP) {
      if (OB_FAIL(inner_append_error_row(row, error_row_array_))) {
        LOG_WARN("failed to append row to error row array", K(ret), K(row));
      }
    } else {
      if (OB_FAIL(inner_append_repeated_row(row, repeated_row_array_))) {
        LOG_WARN("failed to append row to error row array", K(ret), K(row));
      }
    }
  }
  return ret;
}

int ObTableLoadErrorRowHandler::append_repeated_row(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObNewRow new_row;
    ObObjBufArray obj_buf;
    ObArenaAllocator allocator;
    if (OB_FAIL(obj_buf.init(&allocator))) {
      LOG_WARN("fail to init obj buf", KR(ret));
    } else if (OB_FAIL(obj_buf.reserve(row.count_))) {
      LOG_WARN("Failed to reserve buf for obj buf", K(ret), K(row.count_));
    } else {
      new_row.cells_ = obj_buf.get_data();
      new_row.count_ = row.count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < (*col_descs_).count(); i++) {
        if (OB_FAIL(row.storage_datums_[i].to_obj_enhance(new_row.cells_[i], (*col_descs_).at(i).col_type_))) {
          LOG_WARN("Failed to transform datum to obj", K(ret), K(i), K(row.storage_datums_[i]));
        }
      }
      if (OB_SUCC(ret)) {
        if (param_.dup_action_ == ObLoadDupActionType::LOAD_STOP_ON_DUP) {
          if (OB_FAIL(inner_append_error_row(new_row, error_new_row_array_))) {
            LOG_WARN("failed to append row to error row array", K(ret), K(new_row));
          }
        } else {
          if (OB_FAIL(inner_append_repeated_row(new_row, repeated_new_row_array_))) {
            LOG_WARN("failed to append row to error row array", K(ret), K(new_row));
          }
        }
      }
    }
  }
  return ret;
}

// TODO: convert each obj to string
int ObTableLoadErrorRowHandler::get_all_error_rows(ObTableLoadArray<ObObj> &obj_array)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObTableLoadErrorRowHandler::check_rowkey_order(int32_t session_id, const ObTabletID &tablet_id,
                                                   const ObDatumRow &datum_row)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(check_rowkey_order_time_us);
  int ret = OB_SUCCESS;
  int cmp_ret = 1;
  ObTableLoadErrorRowHandler::PartitionRowkey *last_part_rowkey = nullptr;
  ObDatumRowkey rowkey;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if ((session_id < 1) || (session_id > session_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session id", K(session_id), K(session_cnt_));
  } else if (OB_FAIL(rowkey.assign(datum_row.storage_datums_, rowkey_column_num_))) {
    LOG_WARN("failed to assign to rowkey", K(ret), KPC(datum_row.storage_datums_),
             K(rowkey_column_num_));
  } else {
    ObTableLoadErrorRowHandler::PartitionRowkeyMap &partition_map =
      session_maps_.at(session_id - 1);
    if (OB_FAIL(partition_map.map_.get_refactored(tablet_id, last_part_rowkey))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
      } else {
        // allocate a new last_part_rowkey for the new partition
        if (OB_ISNULL(last_part_rowkey = OB_NEWx(ObTableLoadErrorRowHandler::PartitionRowkey,
                                                 (&(partition_map.allocator_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to new ObDatumRowkey", KR(ret));
        } else if (OB_FAIL(partition_map.map_.set_refactored(tablet_id, last_part_rowkey))) {
          LOG_WARN("fail to add last rowkey to map", KR(ret), K(session_id), K(tablet_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObDatumRowkey &last_rowkey = last_part_rowkey->last_rowkey_;
      if (OB_FAIL(rowkey.compare(last_rowkey, *datum_utils_, cmp_ret))) {
        LOG_WARN("fail to compare rowkey to last rowkey", KR(ret), K(rowkey), K(last_rowkey));
      } else if (cmp_ret > 0) {
        // free last rowkey
        last_part_rowkey->allocator_.reuse();
        // overwrite last rowkey
        // TODO: deep copy one row each batch instead of each row
        if (OB_FAIL(
              ObTableLoadUtils::deep_copy(rowkey, last_rowkey, last_part_rowkey->allocator_))) {
          LOG_WARN("failed to deep copy rowkey to last rowkey", K(ret), K(rowkey), K(last_rowkey));
        }
      } else if (cmp_ret == 0) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        LOG_WARN("rowkey == last rowkey", K(ret), K(cmp_ret), K(session_id), K(tablet_id),
                 K(last_rowkey), K(rowkey));
      } else {
        ret = OB_ROWKEY_ORDER_ERROR;
        LOG_WARN("rowkey < last rowkey", K(ret), K(cmp_ret), K(session_id), K(tablet_id),
                 K(last_rowkey), K(rowkey));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
