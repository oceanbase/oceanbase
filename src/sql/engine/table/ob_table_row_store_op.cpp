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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_table_row_store_op.h"
#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
void ObTableRowStoreOpInput::set_deserialize_allocator(ObIAllocator *allocator)
{
  allocator_ = allocator;
  multi_row_store_.set_allocator(allocator);
}

int ObTableRowStoreOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("init table row store input", K(task_info), K(MY_SPEC.id_));
  allocator_ = &exec_ctx_.get_allocator();
  //相同的计划可能有多个任务，所以这里需要清除掉之前的计划状态
  multi_row_store_.reset();
  ObIArray<ObTaskInfo::ObPartLoc> &part_locs = task_info.get_range_location().part_locs_;
  multi_row_store_.set_allocator(&exec_ctx_.get_allocator());
  if (OB_FAIL(multi_row_store_.init(part_locs.count()))) {
    LOG_WARN("allocate multi row store failed", K(ret));
  }
  LOG_DEBUG("init table row store input", K(part_locs.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < part_locs.count(); ++i) {
    uint64_t value_ref_id = part_locs.at(i).value_ref_id_;
    if (value_ref_id == MY_SPEC.id_) {
       LOG_DEBUG("push table row store input", K(value_ref_id));
      if (OB_FAIL(multi_row_store_.push_back(part_locs.at(i).datum_store_))) {
        LOG_WARN("store partition row store failed", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableRowStoreOpInput)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(multi_row_store_.count());
  ARRAY_FOREACH(multi_row_store_, i) {
    LOG_DEBUG("seri table row store input", K(i));
    if (OB_ISNULL(multi_row_store_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null");
    }
    OB_UNIS_ENCODE(*multi_row_store_.at(i));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableRowStoreOpInput)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(multi_row_store_.count());
  ARRAY_FOREACH_NORET(multi_row_store_, i) {
    if (multi_row_store_.at(i) != NULL) {
      OB_UNIS_ADD_LEN(*multi_row_store_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObTableRowStoreOpInput)
{
  int ret = OB_SUCCESS;
  int64_t row_store_cnt = 0;
  OB_UNIS_DECODE(row_store_cnt);
  LOG_DEBUG("deseri table row store input", K(row_store_cnt));
  set_deserialize_allocator(&exec_ctx_.get_allocator());
  if (OB_SUCC(ret)) {
    if (OB_FAIL(multi_row_store_.init(row_store_cnt))) {
      LOG_WARN("allocate multi row store failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_store_cnt; ++i) {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObChunkDatumStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row store failed", K(sizeof(ObChunkDatumStore)));
    } else {
      ObChunkDatumStore *datum_store = new(ptr) ObChunkDatumStore("TableRowStoreOp", allocator_);
      if (OB_FAIL(datum_store->init(UINT64_MAX,
                                    common::OB_SERVER_TENANT_ID,
                                    ObCtxIds::DEFAULT_CTX_ID,
                                    "TableRowStoreOp",
                                    false/*enable_dump*/))) {
        LOG_WARN("fail to init datum store", K(ret));
      }
      OB_UNIS_DECODE(*datum_store);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(multi_row_store_.push_back(datum_store))) {
          LOG_WARN("store row_store failed", K(ret));
        }
      }
      if (OB_FAIL(ret) && datum_store != NULL) {
        datum_store->~ObChunkDatumStore();
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObTableRowStoreSpec, ObOpSpec));

int ObTableRowStoreOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MY_INPUT.multi_row_store_.empty())
      || OB_ISNULL(MY_INPUT.multi_row_store_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi row store is invalid", K(ret), K_(MY_INPUT.multi_row_store));
  } else if (OB_FAIL(MY_INPUT.multi_row_store_.at(0)->begin(row_store_it_))) {
    LOG_WARN("failed to begin iterator for chunk datum store", K(ret));
  } else {
    row_store_idx_ = 0;
  }
  return ret;
}

//not used so far, may be use later @yeti
int ObTableRowStoreOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("rescan ObNoChildrenPhyOperator failed", K(ret));
  } else if (OB_UNLIKELY(MY_INPUT.multi_row_store_.empty())
      || OB_ISNULL(MY_INPUT.multi_row_store_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi row store is invalid", K(ret), K_(MY_INPUT.multi_row_store));
  } else if (OB_FAIL(MY_INPUT.multi_row_store_.at(0)->begin(row_store_it_))) {
    LOG_WARN("failed to begin iterator for chunk datum store", K(ret));
  } else {
    row_store_idx_ = 0;
  }
  return ret;
}

int ObTableRowStoreOp::inner_close()
{
  return OB_SUCCESS;
}

int ObTableRowStoreOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(fetch_stored_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (row_store_idx_ < MY_INPUT.multi_row_store_.count() - 1) {
      //迭代下一个row_store
      ++row_store_idx_;
      ObIArray<ObChunkDatumStore *> &multi_row_store = MY_INPUT.multi_row_store_;
      ObTableModifyOpInput *dml_input = static_cast<ObTableModifyOpInput*>(parent_->get_input());
      if (OB_ISNULL(multi_row_store.at(row_store_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row store is null", K(multi_row_store), K(row_store_idx_));
      } else if (OB_FAIL(multi_row_store.at(row_store_idx_)->begin(row_store_it_))) {
        LOG_WARN("failed to begin iterator for chunk datum store", K(ret));
      } else if (OB_FAIL(fetch_stored_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("get next row from multi row store",
              K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)));
  }
  return ret;
}

int ObTableRowStoreOp::fetch_stored_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_it_.get_next_row(eval_ctx_, MY_SPEC.output_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  }

  return ret;
}

int64_t ObTableRowStoreOp::to_string_kv(char *buf, const int64_t buf_len)
{
  UNUSED(buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  return pos;
}
}  // namespace sql
}  // namespace oceanbase
