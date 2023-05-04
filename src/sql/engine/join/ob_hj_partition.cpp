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

#include "ob_hj_partition.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace join;

int ObHJPartition::init(
  int32_t part_level,
  int64_t part_shift,
  int32_t part_id,
  bool is_left,
  ObHJBufMgr *buf_mgr,
  ObHJBatchMgr *batch_mgr,
  ObHJBatch *pre_batch,
  ObSqlMemoryCallback *callback,
  int64_t dir_id)
{
  int ret = OB_SUCCESS;
  UNUSED(pre_batch);
  part_level_ = part_level;
  part_id_ = part_id;
  buf_mgr_ = buf_mgr;
  batch_mgr_ = batch_mgr;
  if (OB_ISNULL(buf_mgr) || OB_ISNULL(batch_mgr)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("buf mgr or batch_mgr is null", K(ret), K(part_level_), K(part_id_), K(is_left), K(buf_mgr), K(batch_mgr));
  } else if (OB_FAIL(batch_mgr_->get_or_create_batch(part_level_, part_shift, part_id_, is_left, batch_))) {
    LOG_WARN("fail to get batch", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_ISNULL(batch_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to get batch ", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_FAIL(check())) {
    LOG_WARN("fail to check partition", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_FAIL(batch_->init())) {
    LOG_WARN("fail to init batch", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else {
    batch_->get_chunk_row_store().set_callback(callback);
    batch_->get_chunk_row_store().set_dir_id(dir_id);
  }
  return ret;
}

int ObHJPartition::record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to get batch ", K(ret), K(pre_part_count), K(pre_bucket_number), K(total_size));
  } else {
    batch_->set_pre_part_count(pre_part_count);
    batch_->set_pre_bucket_number(pre_bucket_number);
    batch_->set_pre_total_size(total_size);
  }
  return ret;
}

int ObHJPartition::init_iterator(bool is_chunk_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_ is null", K(ret));
  } else if (OB_FAIL(batch_->set_iterator(is_chunk_iter))) {
    LOG_WARN("failed to set iterator", K(ret));
  }
  return ret;
}

int ObHJPartition::check()
{
  int ret = common::OB_SUCCESS;
  if (part_level_ == -1 || part_id_ == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_level_ and part_id_ should not be null");
  } else if (buf_mgr_ == NULL ||  batch_mgr_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf_mgr_ and batch_mgr_ should not be null");
  }
  return ret;
}

void ObHJPartition::reset() {
  buf_mgr_ = nullptr;
  batch_mgr_ = nullptr;
  batch_ = nullptr;
}

int ObHJPartition::add_row(const ObNewRow &row, ObStoredJoinRow *&stored_row) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->add_row(row, stored_row))) {
    LOG_WARN("failed to add row to chunk row store");
  }
  return ret;
}

int ObHJPartition::finish_dump(bool memory_need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->finish_dump(memory_need_dump))){
    LOG_WARN("fail to finish batch dump", K(ret));
  }
  return ret;
}

int ObHJPartition::dump(bool all_dump) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->dump(all_dump))) {
    LOG_WARN("failed to dump data to chunk row store", K(ret));
  }
  return ret;
}

int ObHJPartition::get_next_row(const ObStoredJoinRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->get_next_row(stored_row)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

