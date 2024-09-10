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
 * This file is for define of plugin vector index util
 */

#define USING_LOG_PREFIX SHARE

#include "ob_plugin_vector_index_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
namespace share
{

int ObVectorQueryVidIterator::init()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(row_ = static_cast<ObNewRow *>(allocator_->alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj *>(allocator_->alloc(sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    is_init_ = true;
  }
  return ret;
}

int ObVectorQueryVidIterator::init(int64_t total, int64_t *vids, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  if ((OB_ISNULL(vids) && total != 0) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get vids or allocator", K(ret), K(vids), K(allocator));
  } else if (OB_ISNULL(row_ =  OB_NEWx(ObNewRow, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = OB_NEWx(ObObj, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    is_init_ = true;
    total_ = total;
    cur_pos_ = 0;
    vids_ = vids;
    allocator_ = allocator;
  }
  return ret;
}

int ObVectorQueryVidIterator::get_next_row(ObNewRow *&row)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (cur_pos_ < total_) {
    obj_->reset();
    row_->reset();

    obj_->set_int(vids_[cur_pos_++]);
    row_->cells_ = obj_;
    row_->count_ = 1;
    row_->projector_ = NULL;
    row_->projector_size_ = 0;

    row = row_;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObVectorQueryVidIterator::get_next_rows(ObNewRow *&row, int64_t &size)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (cur_pos_ < total_) {
    size = 0;
    row = nullptr;
    ObObj *obj = nullptr;
    if (batch_size_ > 0) {
      if (OB_ISNULL(row = static_cast<ObNewRow *>(allocator_->alloc(sizeof(ObNewRow))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else if (OB_ISNULL(obj = static_cast<ObObj *>(allocator_->alloc(sizeof(ObObj) * batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else {
        int64_t index = 0;
        for (; index < batch_size_ && cur_pos_ < total_; ++index) {
          obj[index].set_int(vids_[cur_pos_++]);
        }
        row->cells_ = obj;
        row->count_ = index;
        row->projector_ = NULL;
        row->projector_size_ = 0;
        size = index;
      }
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

void ObVectorQueryVidIterator::reset()
{
  is_init_ = false;
  total_ = 0;
  cur_pos_ = 0;
  if (OB_NOT_NULL(allocator_)) {
    allocator_->reset();
  }
}

int ObPluginVectorIndexHelper::merge_delta_and_snap_vids(const ObVsagQueryResult &first,
                                                         const ObVsagQueryResult &second,
                                                         const int64_t total,
                                                         int64_t &actual_cnt,
                                                         int64_t *&vids_result)
{
  INIT_SUCC(ret);
  actual_cnt = 0;
  int64_t res_num = 0;
  if (first.total_ == 0) {
    while (res_num < total && res_num < second.total_) {
      vids_result[res_num] = second.vids_[res_num];
      res_num++;
    }
    actual_cnt = res_num;
  } else if (second.total_ == 0) {
    while (res_num < total && res_num < first.total_) {
      vids_result[res_num] = first.vids_[res_num];
      res_num++;
    }
    actual_cnt = res_num;
  } else if (OB_ISNULL(first.vids_) || OB_ISNULL(second.vids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vids invalid.", K(ret), K(first.vids_), K(second.vids_));
  } else {
    int64_t i = 0, j = 0;
    while (res_num < total && i < first.total_ && j < second.total_) {
      if (first.distances_[i] <= second.distances_[j]) {
        vids_result[res_num++] = first.vids_[i++];
      } else {
        vids_result[res_num++] = second.vids_[j++];
      }
    }

    while (res_num < total && i < first.total_) {
      vids_result[res_num++] = first.vids_[i++];
    }

    while (res_num < total && j < second.total_) {
      vids_result[res_num++] = second.vids_[j++];
    }

    actual_cnt = res_num;
  }

  return ret;
}

int ObPluginVectorIndexHelper::get_vector_memory_value_and_limit(const uint64_t tenant_id,int64_t& value, int64_t& upper_limit)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t extra_mem_percent = 15;
  int64_t cur_memstore_limit_percent = 0;
  MTL_SWITCH(tenant_id) {
    cur_memstore_limit_percent = MTL(ObTenantFreezer*)->get_memstore_limit_percentage();
  }
  if (tenant_config.is_valid() && OB_SUCC(ret)) {
    upper_limit = (100 - extra_mem_percent - cur_memstore_limit_percent);
    if (upper_limit < 0) {
      upper_limit = 0;
    }
    value = tenant_config->ob_vector_memory_limit_percentage;
    LOG_TRACE("check is_ob_vector_memory_valid", K(value), K(extra_mem_percent), K(cur_memstore_limit_percent), K(upper_limit));
  } else {
    upper_limit = 0;
    value = 0;
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("tenant config is invalid",K(ret), K(tenant_id));
  }
  return ret;
}

int ObPluginVectorIndexHelper::is_ob_vector_memory_valid(const uint64_t tenant_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t value = 0;
  int64_t upper_limit = 0;
  if (OB_FAIL(get_vector_memory_value_and_limit(tenant_id, value, upper_limit))) {
    LOG_WARN("fail to get vector memory value and limit", K(ret));
  } else if (0 < value && value < upper_limit) {
    is_valid = true;
  }
  return ret;
}

int ObPluginVectorIndexHelper::get_vector_memory_limit_size(const uint64_t tenant_id, int64_t& memory_limit)
{
  bool ret = OB_SUCCESS;
  ObUnitInfoGetter::ObTenantConfig unit;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(GCTX.omt_->get_tenant_unit(tenant_id, unit))) {
    LOG_WARN("get tenant unit failed", K(tmp_ret), K(tenant_id));
  } else {
    const int64_t memory_size = unit.config_.memory_size();
    int64_t ob_vector_memory_limit_percentage = 0;
    int64_t upper_limit = 0;
    if (OB_FAIL(get_vector_memory_value_and_limit(tenant_id, ob_vector_memory_limit_percentage, upper_limit))) {
      LOG_WARN("fail to get vector memory value and limit", K(ret));
    } else if (0 < ob_vector_memory_limit_percentage && ob_vector_memory_limit_percentage < upper_limit) {
      memory_limit = memory_size * ob_vector_memory_limit_percentage / 100;
      LOG_TRACE("vector index memory limit debug", K(memory_size), K(ob_vector_memory_limit_percentage),K(memory_limit));
    } else {
      memory_limit = 0;
      LOG_TRACE("vector index memory is not enough,check memstore config", K(memory_size), K(ob_vector_memory_limit_percentage),K(memory_limit));
    }
  }
  return ret;
}


};
};