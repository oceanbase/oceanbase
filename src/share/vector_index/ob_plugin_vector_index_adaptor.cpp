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

#define USING_LOG_PREFIX SHARE

#include "ob_plugin_vector_index_adaptor.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"
#include "share/ls/ob_ls_operator.h"

namespace oceanbase
{
namespace share
{

ObVectorIndexInfo::ObVectorIndexInfo()
  : ls_id_(share::ObLSID::INVALID_LS_ID),
    rowkey_vid_table_id_(common::OB_INVALID_ID),
    vid_rowkey_table_id_(common::OB_INVALID_ID),
    inc_index_table_id_(common::OB_INVALID_ID),
    vbitmap_table_id_(common::OB_INVALID_ID),
    snapshot_index_table_id_(common::OB_INVALID_ID),
    data_table_id_(common::OB_INVALID_ID),
    rowkey_vid_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    vid_rowkey_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    inc_index_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    vbitmap_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    snapshot_index_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    data_tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    statistics_(),
    sync_info_()
{
  MEMSET(statistics_, '\0', sizeof(statistics_));
  MEMSET(sync_info_, '\0', sizeof(sync_info_));
}

void ObVectorIndexInfo::reset()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  rowkey_vid_table_id_ = common::OB_INVALID_ID;
  vid_rowkey_table_id_ = common::OB_INVALID_ID;
  inc_index_table_id_ = common::OB_INVALID_ID;
  vbitmap_table_id_ = common::OB_INVALID_ID;
  snapshot_index_table_id_ = common::OB_INVALID_ID;
  data_table_id_ = common::OB_INVALID_ID;
  rowkey_vid_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  vid_rowkey_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  inc_index_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  vbitmap_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  snapshot_index_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  data_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  MEMSET(statistics_, '\0', sizeof(statistics_));
  MEMSET(sync_info_, '\0', sizeof(sync_info_));
}

OB_DEF_SERIALIZE_SIZE(ObVectorIndexParam)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              type_,
              lib_,
              dist_algorithm_,
              dim_,
              m_,
              ef_construction_,
              ef_search_);
  return len;
}

OB_DEF_SERIALIZE(ObVectorIndexParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              type_,
              lib_,
              dist_algorithm_,
              dim_,
              m_,
              ef_construction_,
              ef_search_);
  return ret;
}

OB_DEF_DESERIALIZE(ObVectorIndexAlgorithmHeader)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObVectorIndexParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              type_,
              lib_,
              dist_algorithm_,
              dim_,
              m_,
              ef_construction_,
              ef_search_);
  return ret;
}

ObVectorQueryAdaptorResultContext::~ObVectorQueryAdaptorResultContext() {
  status_ = PVQ_START;
  flag_ = PVQP_MAX;
  if (OB_NOT_NULL(bitmaps_)) {
    if (OB_NOT_NULL(bitmaps_->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPA"));
      roaring::api::roaring64_bitmap_free(bitmaps_->insert_bitmap_);
    }
    if (OB_NOT_NULL(bitmaps_->delete_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPB"));
      roaring::api::roaring64_bitmap_free(bitmaps_->delete_bitmap_);
    }
  }
  if (OB_NOT_NULL(extra_bitmaps_)) {
    if (OB_NOT_NULL(extra_bitmaps_->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPS"));
      roaring::api::roaring64_bitmap_free(extra_bitmaps_->insert_bitmap_);
    }
    if (OB_NOT_NULL(extra_bitmaps_->delete_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPT"));
      roaring::api::roaring64_bitmap_free(extra_bitmaps_->delete_bitmap_);
    }
  }

  batch_allocator_.reset();
};

int ObVectorQueryAdaptorResultContext::init_bitmaps(bool is_extra)
{
  INIT_SUCC(ret);

  ObVectorIndexRoaringBitMap *bitmaps = nullptr;
  if (OB_ISNULL(tmp_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx allocator invalid.", K(ret));
  } else if (OB_ISNULL(bitmaps = static_cast<ObVectorIndexRoaringBitMap*>
                        (tmp_allocator_->alloc(sizeof(ObVectorIndexRoaringBitMap))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create vbitmap msg", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPC"));
    ROARING_TRY_CATCH(bitmaps->insert_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_SUCC(ret) && OB_ISNULL(bitmaps->insert_bitmap_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create insert bitmap", K(ret));
    } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      bitmaps->insert_bitmap_ = nullptr;
    }
    ROARING_TRY_CATCH(bitmaps->delete_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_SUCC(ret) && OB_ISNULL(bitmaps->delete_bitmap_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create delete bitmap", K(ret));
    } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      bitmaps->delete_bitmap_ = nullptr;
    }
  }

  if (is_extra) {
    extra_bitmaps_ = bitmaps;
  } else {
    bitmaps_ = bitmaps;
  }

  return ret;
}

bool ObVectorQueryAdaptorResultContext::is_bitmaps_valid(bool is_extra)
{
  bool bret = false;

  ObVectorIndexRoaringBitMap *bitmaps = is_extra ? extra_bitmaps_ : bitmaps_;
  if (OB_NOT_NULL(bitmaps)) {
    if (OB_NOT_NULL(bitmaps->insert_bitmap_) && OB_NOT_NULL(bitmaps->delete_bitmap_)) {
      bret = true;
    }
  }

  return bret;
}

// int ObVectorQueryAdaptorResultContext::set_vector(int64_t index, ObString &str)
int ObVectorQueryAdaptorResultContext::set_vector(int64_t index, const char *ptr, common::ObString::obstr_size_t size)
{
  INIT_SUCC(ret);
  char *copy_str = nullptr;
  if (index >= get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid index.", K(ret), K(index), K(get_count()));
  } else if (size == 0 || OB_ISNULL(ptr)) {
    vec_data_.vectors_[index].reset();
  } else if (size / sizeof(float) != get_dim()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector str.", K(ret), K(size), K(ptr), K(get_dim()));
  } else if (OB_ISNULL(copy_str = static_cast<char *>(batch_allocator_.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator.", K(ret));
  } else {
    memcpy(copy_str, ptr, size);
    vec_data_.vectors_[index].reset();
    vec_data_.vectors_[index].set_string(ObVarcharType, copy_str, size);
  }

  return ret;
}

void free_memdata_resource(ObVectorIndexRecordType type,
                           ObVectorIndexMemData *&memdata,
                           ObIAllocator *allocator,
                           uint64_t tenant_id)
{
  LOG_INFO("free memdata", K(type), KP(memdata), K(allocator), K(lbt())); // remove later
  if (OB_NOT_NULL(memdata->bitmap_)) {
    if (OB_NOT_NULL(memdata->bitmap_->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIBitmapADPD"));
      roaring::api::roaring64_bitmap_free(memdata->bitmap_->insert_bitmap_);
      memdata->bitmap_->insert_bitmap_ = nullptr;
    }
    if (OB_NOT_NULL(memdata->bitmap_->delete_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIBitmapADPE"));
      roaring::api::roaring64_bitmap_free(memdata->bitmap_->delete_bitmap_);
      memdata->bitmap_->delete_bitmap_ = nullptr;
    }
    if (OB_NOT_NULL(memdata->bitmap_)) {
      allocator->free(memdata->bitmap_);
      memdata->bitmap_ = nullptr;
    }
  }
  if (OB_NOT_NULL(memdata->index_)) {
    obvectorutil::delete_index(memdata->index_);
    LOG_INFO("delete vector index", K(type), KP(memdata->index_), K(lbt())); // remove later
    memdata->index_ = nullptr;
  }
  memdata->is_init_ = false;
}

void free_hnswsq_array_data(ObVectorIndexMemData *&memdata, ObIAllocator *allocator)
{
  if (OB_NOT_NULL(memdata->vid_array_)) {
    memdata->vid_array_->~ObArray();
    allocator->free(memdata->vid_array_);
    memdata->vid_array_ = nullptr;
  }
  if (OB_NOT_NULL(memdata->vec_array_)) {
    memdata->vec_array_->~ObArray();
    allocator->free(memdata->vec_array_);
    memdata->vec_array_ = nullptr;
  }
}

static int try_free_memdata_resource(ObVectorIndexRecordType type,
                                     ObVectorIndexMemData *&memdata,
                                     ObIAllocator *allocator,
                                     uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memdata)) {
    // do nothing
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(type), KPC(memdata), K(allocator));
  } else if (memdata->dec_ref_and_check_release()) {
    free_memdata_resource(type, memdata, allocator, tenant_id);
    if (OB_NOT_NULL(memdata->mem_ctx_)) {
      memdata->mem_ctx_->~ObVsagMemContext();
      allocator->free(memdata->mem_ctx_);
      memdata->mem_ctx_ = nullptr;
    }
    allocator->free(memdata);
    memdata = nullptr;
  } else {
    // do nothing
  }
  return ret;
}

ObPluginVectorIndexAdaptor::ObPluginVectorIndexAdaptor(common::ObIAllocator *allocator,
                                                       lib::MemoryContext &entity,
                                                       uint64_t tenant_id)
  : create_type_(CreateTypeMax), type_(VIAT_MAX),
    algo_data_(nullptr), incr_data_(nullptr), snap_data_(nullptr), vbitmap_data_(nullptr), tenant_id_(tenant_id),
    snapshot_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    inc_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    vbitmap_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    data_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    rowkey_vid_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    vid_rowkey_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    inc_table_id_(OB_INVALID_ID), vbitmap_table_id_(OB_INVALID_ID),
    snapshot_table_id_(OB_INVALID_ID), data_table_id_(OB_INVALID_ID),
    rowkey_vid_table_id_(OB_INVALID_ID), vid_rowkey_table_id_(OB_INVALID_ID),
    ref_cnt_(0), idle_cnt_(0), mem_check_cnt_(0), is_mem_limited_(false), all_vsag_use_mem_(nullptr), allocator_(allocator),
    parent_mem_ctx_(entity), index_identity_(), follower_sync_statistics_()
{
}

ObPluginVectorIndexAdaptor::~ObPluginVectorIndexAdaptor()
{
  int ret = OB_SUCCESS;
  LOG_INFO("destruct adaptor and free resources", K(is_complete()), K(this), KPC(this), K(lbt())); // remove later
  // inc
  if (OB_NOT_NULL(incr_data_)
      && (OB_FAIL(try_free_memdata_resource(VIRT_INC, incr_data_, allocator_, tenant_id_)))) {
    LOG_WARN("failed to free incr memdata", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)
      && OB_NOT_NULL(vbitmap_data_)
      && OB_FAIL(try_free_memdata_resource(VIRT_BITMAP, vbitmap_data_, allocator_, tenant_id_))) {
    LOG_WARN("failed to free vbitmap memdata", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)
      && OB_NOT_NULL(snap_data_)
      && OB_FAIL(try_free_memdata_resource(VIRT_SNAP, snap_data_, allocator_, tenant_id_))) {
    LOG_WARN("failed to free snap memdata", K(ret), KPC(this));
  }

  // use another memdata struct for the following?
  if (OB_NOT_NULL(allocator_)) {
    if(!index_identity_.empty()) {
      allocator_->free(index_identity_.ptr());
      index_identity_.reset();
    }
    if (OB_NOT_NULL(algo_data_)) {
      allocator_->free(algo_data_);
      algo_data_ = nullptr;
    }
  }
}

int ObPluginVectorIndexAdaptor::init_mem(ObVectorIndexMemData *&table_info)
{
  INIT_SUCC(ret);
  void *table_buff = nullptr;
  if (OB_NOT_NULL(table_info)) {
    // do nothing
  } else if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_ISNULL(table_buff = static_cast<ObVectorIndexMemData *>(
                                    get_allocator()->alloc(sizeof(ObVectorIndexMemData))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create vbitmap msg", K(ret));
  } else if (OB_FALSE_IT(table_info = new(table_buff) ObVectorIndexMemData())) {
  } else if (OB_ISNULL(table_info->mem_ctx_ = OB_NEWx(ObVsagMemContext, get_allocator(), all_vsag_use_mem_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem_ctx msg", K(ret));
  } else {
    table_info->scn_.set_min();
    table_info->inc_ref();
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(table_buff)) {
      get_allocator()->free(table_buff);
      table_buff = nullptr;
    }
  }
  return ret;
}

bool ObPluginVectorIndexAdaptor::is_mem_data_init_atomic(ObVectorIndexRecordType type)
{
  bool bret = false;
  if (type == VIRT_INC) {
    bret = (OB_NOT_NULL(incr_data_) && incr_data_->is_inited());
  } else if (type == VIRT_BITMAP) {
    bret = (OB_NOT_NULL(vbitmap_data_) && vbitmap_data_->is_inited());
  } else if (type == VIRT_SNAP) {
    bret = (OB_NOT_NULL(snap_data_) && snap_data_->is_inited());
  }
  return bret;
}

int ObPluginVectorIndexAdaptor::init(lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_FAIL(init_mem(incr_data_))) {
    LOG_WARN("failed to init incr mem data.", K(ret));
  } else if (OB_FAIL(init_mem(vbitmap_data_))) {
    LOG_WARN("failed to init vbitmap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(snap_data_))) {
    LOG_WARN("failed to init snap mem data.", K(ret));
  } else {
    parent_mem_ctx_ = parent_mem_ctx;
    all_vsag_use_mem_ = all_vsag_use_mem;
  }
  // fail in middle success inited mem resouce should be released by the caller
  return ret;
}

int ObPluginVectorIndexAdaptor::init(ObString init_str, int64_t dim, lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem)
{
  INIT_SUCC(ret);
  ObVectorIndexAlgorithmType type;

  if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_FAIL(init_mem(incr_data_))) {
    LOG_WARN("failed to init incr mem data.", K(ret));
  } else if (OB_FAIL(init_mem(vbitmap_data_))) {
    LOG_WARN("failed to init vbitmap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(snap_data_))) {
    LOG_WARN("failed to init snap mem data.", K(ret));
  } else if (OB_FAIL(set_param(init_str, dim))){
    LOG_WARN("failed to set param.", K(ret));
  } else {
    parent_mem_ctx_ = parent_mem_ctx;
    all_vsag_use_mem_ = all_vsag_use_mem;
  }
  // fail in middle success inited mem resouce should be released by the caller
  return ret;
}

int ObPluginVectorIndexAdaptor::set_param(ObString init_str, int64_t dim)
{
  INIT_SUCC(ret);
  ObVectorIndexParam *hnsw_param = nullptr;
  if (OB_NOT_NULL(algo_data_)) {
    // do nothing
  } else if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_ISNULL(hnsw_param = static_cast<ObVectorIndexParam *>
                            (get_allocator()->alloc(sizeof(ObVectorIndexParam))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mem.", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(init_str, ObVectorIndexType::VIT_HNSW_INDEX, *hnsw_param))) {
    LOG_WARN("failed to parse params.", K(ret));
  } else {
    type_ = hnsw_param->type_;
    algo_data_ = hnsw_param;
    hnsw_param->dim_ = dim;
    LOG_INFO("init vector index adapter with param", KPC(hnsw_param)); // change log to debug level later
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(hnsw_param)) {
      get_allocator()->free(hnsw_param);
      hnsw_param = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::param_deserialize(char *ptr, int32_t length,
                                                  ObIAllocator *allocator,
                                                  ObVectorIndexAlgorithmType &type,
                                                  void *&param)
{
  INIT_SUCC(ret);
  int64_t pos = 0;
  ObVectorIndexAlgorithmHeader header;
  if (OB_FAIL(header.deserialize(ptr, length, pos))) {
    LOG_WARN("failed to deserialize header.", K(ret), K(ptr), K(pos));
  } else {
    type = header.type_;
    switch(type) {
      case VIAT_HNSW:
      case VIAT_HNSW_SQ:{
        int64_t param_pos = 0;
        ObVectorIndexParam *hnsw_param = nullptr;
        if (OB_ISNULL(hnsw_param = static_cast<ObVectorIndexParam *>
                                  (allocator->alloc(sizeof(ObVectorIndexParam))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate mem.", K(ret));
        } else if (OB_FAIL(hnsw_param->deserialize(ptr, length, param_pos))) {
          LOG_WARN("failed to deserialize hnsw param.", K(ret), K(param_pos));
        } else {
          param = hnsw_param;
        }

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(hnsw_param)) {
           allocator->free(hnsw_param);
           hnsw_param = nullptr;
          }
        }

        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("get index algorithm type not support.", K(ret), K(type));
        break;
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::get_dim(int64_t &dim)
{
  INIT_SUCC(ret);
  // TODO [WORKDOC] work document NO.1
  if (type_ == VIAT_HNSW ||
      type_ == VIAT_HNSW_SQ) {
    ObVectorIndexParam *param = nullptr;
    if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get param.", K(ret));
    } else {
      dim = param->dim_;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get index algorithm type not support.", K(ret), K(type_));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::get_hnsw_param(ObVectorIndexParam *&param)
{
  INIT_SUCC(ret);
  if (type_ == VIAT_HNSW ||
      type_ == VIAT_HNSW_SQ) {
    if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get param.", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get index algorithm type not support.", K(ret), K(type_));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::fill_vector_index_info(ObVectorIndexInfo &info)
{
  int ret = OB_SUCCESS;
  // table_id
  info.rowkey_vid_table_id_ = rowkey_vid_table_id_;
  info.vid_rowkey_table_id_ = vid_rowkey_table_id_;
  info.inc_index_table_id_ = inc_table_id_;
  info.vbitmap_table_id_ = vbitmap_table_id_;
  info.snapshot_index_table_id_ = snapshot_table_id_;
  info.data_table_id_ = data_table_id_;
  // tablet_id
  info.rowkey_vid_tablet_id_ = rowkey_vid_tablet_id_.id();
  info.vid_rowkey_tablet_id_ = vid_rowkey_tablet_id_.id();
  info.inc_index_tablet_id_ = inc_tablet_id_.id();
  info.vbitmap_tablet_id_ = vbitmap_tablet_id_.id();
  info.snapshot_index_tablet_id_ = snapshot_tablet_id_.id();
  info.data_tablet_id_ = data_tablet_id_.id();
  ObVectorIndexParam *param;
  int64_t pos = 0;

  if (OB_FAIL(databuff_printf(info.statistics_,
                 sizeof(info.statistics_), pos,
                 "is_complete=%d;", is_complete()))) {
    LOG_WARN("failed to fill statistics", K(ret), K(this));
  } else if (type_ == VIAT_MAX) {
    // partial adapter without index configuration
  } else if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (OB_FAIL(databuff_printf(info.statistics_,
                 sizeof(info.statistics_), pos,
                 "param=%s;", to_cstring(*param)))) {
    LOG_WARN("failed to fill statistics", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.statistics_,
                      sizeof(info.statistics_), pos,
                      "snap_index_type=%d;", int(get_snap_index_type())))) {
    LOG_WARN("failed to fill snap index type", K(ret), K(this));
  }
  if (FAILEDx(databuff_printf(info.statistics_,
                 sizeof(info.statistics_), pos,
                 "ref_cnt=%ld;",  ATOMIC_LOAD(&ref_cnt_) - 1))) { // delete the virtual table ref
    LOG_WARN("failed to fill statistics", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.statistics_,
             sizeof(info.statistics_), pos, "idle_cnt=%ld;", idle_cnt_))) {
    LOG_WARN("failed to fill statistics", K(ret), K(this));
  } else if (!index_identity_.empty() && OB_FAIL(databuff_printf(
             info.statistics_, sizeof(info.statistics_), pos,
             "index=%s;", to_cstring(index_identity_)))) {
    LOG_WARN("failed to fill statistic", K(ret), K(this));
  } else if (nullptr != incr_data_ && OB_FAIL(databuff_printf(
             info.statistics_, sizeof(info.statistics_), pos,
             "incr_data.scn=%lu;", incr_data_->scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to fill statistic", K(ret), K(this));
  } else if (nullptr != vbitmap_data_ && OB_FAIL(databuff_printf(
             info.statistics_, sizeof(info.statistics_), pos,
             "vbitmap_data.scn=%lu;", vbitmap_data_->scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to fill statistic", K(ret), K(this));
  } else if (nullptr != snap_data_ && OB_FAIL(databuff_printf(
             info.statistics_, sizeof(info.statistics_), pos,
             "snap_data.scn=%lu;", snap_data_->scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to fill statistic", K(ret), K(this));
  } else if (nullptr != all_vsag_use_mem_ && OB_FAIL(databuff_printf(
             info.statistics_, sizeof(info.statistics_), pos,
             "all_index_mem_used=%lu;", ATOMIC_LOAD(all_vsag_use_mem_)))) {
    LOG_WARN("failed to fill statistic", K(ret), K(this));
  } else {
    ObRbMemMgr *mem_mgr = nullptr;
    uint64_t tenant_id = MTL_ID();
    if (OB_ISNULL(mem_mgr = MTL(ObRbMemMgr *))) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_mgr is null", K(tenant_id));
    } else if (OB_FAIL(databuff_printf(
               info.statistics_, sizeof(info.statistics_), pos,
               "all_index_bitmap_used=%lu;", mem_mgr->get_vec_idx_used()))) {
      LOG_WARN("failed to fill statistic", K(ret), K(this));
    }
  }
  pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos,
             "incr_cnt=%lu;", follower_sync_statistics_.incr_count_))) {
    LOG_WARN("failed to fill sync_info", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos,
             "vbitmap_cnt=%lu;", follower_sync_statistics_.vbitmap_count_))) {
    LOG_WARN("failed to fill sync_info", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos,
             "snap_cnt=%lu;", follower_sync_statistics_.snap_count_))) {
    LOG_WARN("failed to fill sync_info", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos,
             "sync_total_cnt=%lu;", follower_sync_statistics_.sync_count_))) {
    LOG_WARN("failed to fill sync_info", K(ret), K(this));
  } else if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos,
             "sync_fail_cnt=%lu;", follower_sync_statistics_.sync_fail_))) {
    LOG_WARN("failed to fill sync_info", K(ret), K(this));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_mem_data(ObVectorIndexRecordType type, ObVectorIndexAlgorithmType enforce_type)
{
  INIT_SUCC(ret);
  ObVectorIndexParam *param = nullptr;
  const char* const DATATYPE_FLOAT32 = "float32";
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (OB_FAIL(check_vsag_mem_used())) {
    LOG_WARN("check vsag mem used failed.", K(ret));
  } else if (type == VIRT_INC) {
    TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (!incr_data_->is_inited()) {
      if (OB_FAIL(incr_data_->mem_ctx_->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
        LOG_WARN("failed to init incr data mem ctx.", K(ret));
      } else {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
        if (OB_FAIL(obvectorutil::create_index(incr_data_->index_,
                                                      obvectorlib::HNSW_TYPE,
                                                      DATATYPE_FLOAT32,
                                                      VEC_INDEX_ALGTH[param->dist_algorithm_],
                                                      param->dim_,
                                                      param->m_,
                                                      param->ef_construction_,
                                                      param->ef_search_,
                                                      incr_data_->mem_ctx_))) {
          ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
          LOG_WARN("failed to create vsag index.", K(ret), KPC(param));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(incr_data_->bitmap_ = static_cast<ObVectorIndexRoaringBitMap *>
                  (get_allocator()->alloc(sizeof(ObVectorIndexRoaringBitMap))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create delta_bitmap", K(ret));
      } else {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPF"));
        ROARING_TRY_CATCH(incr_data_->bitmap_->insert_bitmap_ = roaring::api::roaring64_bitmap_create());
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(incr_data_->bitmap_->insert_bitmap_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create delta insert bitmap", K(ret));
        } else {
          incr_data_->bitmap_->delete_bitmap_ = nullptr;
          incr_data_->set_inited(); // should release memory if fail
        }
        LOG_INFO("create incr index success.", K(ret), KP(incr_data_->index_), K(lbt())); // remove later
      }

      if (OB_FAIL(ret)) {
        free_memdata_resource(type, incr_data_, get_allocator(), tenant_id_);
        if (incr_data_->mem_ctx_->is_inited()) {
          incr_data_->mem_ctx_->~ObVsagMemContext();
        }
      }
    }
  } else if (type == VIRT_BITMAP) {
    TCWLockGuard lock_guard(vbitmap_data_->mem_data_rwlock_);
    if (!vbitmap_data_->is_inited()) {
      if (OB_ISNULL(vbitmap_data_->bitmap_ = static_cast<ObVectorIndexRoaringBitMap *>
                                          (get_allocator()->alloc(sizeof(ObVectorIndexRoaringBitMap))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create snapshot_bitmap", K(ret));
      } else {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPG"));
        ROARING_TRY_CATCH(vbitmap_data_->bitmap_->insert_bitmap_ = roaring::api::roaring64_bitmap_create());
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(vbitmap_data_->bitmap_->insert_bitmap_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create snapshot insert bitmap", K(ret));
        }
        ROARING_TRY_CATCH(vbitmap_data_->bitmap_->delete_bitmap_ = roaring::api::roaring64_bitmap_create());
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(vbitmap_data_->bitmap_->delete_bitmap_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create snapshot delete bitmap", K(ret));
        }
        if (OB_SUCC(ret)) {
          vbitmap_data_->set_inited();
        }
      }

      if (OB_FAIL(ret)) {
        free_memdata_resource(type, vbitmap_data_, get_allocator(), tenant_id_);
      }
    }
  } else if (type == VIRT_SNAP) {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (!snap_data_->is_inited()) {
      if (OB_FAIL(snap_data_->mem_ctx_->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
        LOG_WARN("failed to init incr data mem ctx.", K(ret));
      } else {
        ObVectorIndexAlgorithmType build_type = enforce_type == VIAT_MAX ? param->type_ : enforce_type;
        int64_t build_metric = param->type_ == VIAT_HNSW_SQ ? get_hnswsq_type_metric(param->m_) : param->m_;
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
        if (OB_FAIL(obvectorutil::create_index(snap_data_->index_,
                                               build_type,
                                               DATATYPE_FLOAT32,
                                               VEC_INDEX_ALGTH[param->dist_algorithm_],
                                               param->dim_,
                                               build_metric,
                                               param->ef_construction_,
                                               param->ef_search_,
                                               snap_data_->mem_ctx_))) {
          ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
          LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
        }
      }

      if (OB_SUCC(ret)) {
        snap_data_->set_inited();
        LOG_INFO("create snap data success.", K(ret), KP(snap_data_->index_), K(lbt())); // remove later
      }
      if (OB_FAIL(ret)) {
        free_memdata_resource(type, snap_data_, get_allocator(), tenant_id_);
        if (snap_data_->mem_ctx_->is_inited()) {
          snap_data_->mem_ctx_->~ObVsagMemContext();
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_snap_data_without_lock(ObVectorIndexAlgorithmType enforce_type)
{
  INIT_SUCC(ret);
  ObVectorIndexParam *param = nullptr;
  const char* const DATATYPE_FLOAT32 = "float32";
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (OB_FAIL(check_vsag_mem_used())) {
    LOG_WARN("check vsag mem used failed.", K(ret));
  } else if (!snap_data_->is_inited()) {
    if (OB_FAIL(snap_data_->mem_ctx_->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
      LOG_WARN("failed to init incr data mem ctx.", K(ret));
    } else {
      ObVectorIndexAlgorithmType build_type = enforce_type == VIAT_MAX ? param->type_ : enforce_type;
      int64_t build_metric = param->type_ == VIAT_HNSW_SQ ? get_hnswsq_type_metric(param->m_) : param->m_;
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      if (OB_FAIL(obvectorutil::create_index(snap_data_->index_,
                                             build_type,
                                             DATATYPE_FLOAT32,
                                             VEC_INDEX_ALGTH[param->dist_algorithm_],
                                             param->dim_,
                                             build_metric,
                                             param->ef_construction_,
                                             param->ef_search_,
                                             snap_data_->mem_ctx_))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
      }
    }

    if (OB_SUCC(ret)) {
      snap_data_->set_inited();
      LOG_INFO("create snap data success.", K(ret), KP(snap_data_->index_));
    }
    if (OB_FAIL(ret)) {
      free_memdata_resource(VIRT_SNAP, snap_data_, get_allocator(), tenant_id_);
      if (snap_data_->mem_ctx_->is_inited()) {
        snap_data_->mem_ctx_->~ObVsagMemContext();
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_hnswsq_mem_data()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ATOMIC_LOAD(&(snap_data_->vid_array_)))) {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (OB_NOT_NULL(ATOMIC_LOAD(&(snap_data_->vid_array_)))) {
      // do nothing
    } else if (OB_ISNULL(snap_data_->vid_array_ = OB_NEWx(ObVecIdxVidArray, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for vid array fail", K(ret));
    } else if (OB_ISNULL(snap_data_->vec_array_ = OB_NEWx(ObVecIdxVecArray, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for vector array fail", K(ret));
    } else {
      snap_data_->vid_array_->set_attr(ObMemAttr(tenant_id_, "VecIdxHNSWSQ"));
      snap_data_->vec_array_->set_attr(ObMemAttr(tenant_id_, "VecIdxHNSWSQ"));
      snap_data_->set_inited();
    }
    if (OB_FAIL(ret)) {
      free_hnswsq_array_data(snap_data_, get_allocator());
    }
  }

  return ret;
}

void *ObPluginVectorIndexAdaptor::get_incr_index()
{
  void *res = nullptr;
  if (OB_NOT_NULL(incr_data_)) {
    res = incr_data_->index_;
  }
  return res;
}

void *ObPluginVectorIndexAdaptor::get_snap_index()
{
  void *res = nullptr;
  if (OB_NOT_NULL(snap_data_)) {
    res = snap_data_->index_;
  }
  return res;
}

const roaring::api::roaring64_bitmap_t *ObPluginVectorIndexAdaptor::get_incr_ibitmap()
{
  roaring::api::roaring64_bitmap_t *res = nullptr;
  if (OB_NOT_NULL(incr_data_) && OB_NOT_NULL(incr_data_->bitmap_)) {
    res = incr_data_->bitmap_->insert_bitmap_;
  }
  return res;
}

const roaring::api::roaring64_bitmap_t *ObPluginVectorIndexAdaptor::get_vbitmap_ibitmap()
{
  roaring::api::roaring64_bitmap_t *res = nullptr;
  if (OB_NOT_NULL(vbitmap_data_) && OB_NOT_NULL(vbitmap_data_->bitmap_)) {
    res = vbitmap_data_->bitmap_->insert_bitmap_;
  }
  return res;
}

const roaring::api::roaring64_bitmap_t *ObPluginVectorIndexAdaptor::get_vbitmap_dbitmap()
{
  roaring::api::roaring64_bitmap_t *res = nullptr;
  if (OB_NOT_NULL(vbitmap_data_) && OB_NOT_NULL(vbitmap_data_->bitmap_)) {
    res = vbitmap_data_->bitmap_->delete_bitmap_;
  }
  return res;
}

int ObPluginVectorIndexAdaptor::check_tablet_valid(ObVectorIndexRecordType type)
{
  INIT_SUCC(ret);
  if (type == VIRT_INC) {
    if (!is_inc_tablet_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect insert inc index but table id invalid.", K(ret));
    }
  } else if (type == VIRT_SNAP) {
    if (!is_snap_tablet_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect insert snap index but table id invalid.", K(ret));
    }
  } else if (type == VIRT_BITMAP) {
    if (!is_vbitmap_tablet_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect insert snap index but table id invalid.", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector index record type invalid.", K(ret));
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::get_current_scn(share::SCN &current_scn)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);

  current_scn.set_invalid();
  int64_t start_us = ObTimeUtility::fast_current_time();
  const transaction::MonotonicTs stc = transaction::MonotonicTs(start_us);
  transaction::MonotonicTs rts(0);

  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else if (OB_FAIL(txs->get_ts_mgr()->get_gts(tenant_id, stc, NULL, current_scn, rts))) {
    LOG_WARN("get scn from cache.", KR(ret));
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::update_index_id_dml_scn(share::SCN &current_scn)
{
  incr_data_->last_dml_scn_.atomic_set(current_scn);
}

void ObPluginVectorIndexAdaptor::update_index_id_read_scn()
{
  int ret = OB_SUCCESS;

  share::SCN current_scn;
  if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
    ret = OB_SUCCESS;
  } else {
    incr_data_->last_read_scn_.atomic_set(current_scn);
  }
}

share::SCN ObPluginVectorIndexAdaptor::get_index_id_dml_scn()
{
  return incr_data_->last_dml_scn_.atomic_load();
}

share::SCN ObPluginVectorIndexAdaptor::get_index_id_read_scn()
{
  return incr_data_->last_read_scn_.atomic_load();
}

bool ObPluginVectorIndexAdaptor::is_pruned_read_index_id()
{
  bool b_ret = false;
  if (incr_data_->last_read_scn_ > incr_data_->last_dml_scn_) {
    b_ret = true;
  }
  return b_ret;
}

int ObPluginVectorIndexAdaptor::insert_rows(blocksstable::ObDatumRow *rows,
                                            const int64_t vid_idx,
                                            const int64_t type_idx,
                                            const int64_t vector_idx,
                                            int64_t row_count)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_ISNULL(rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get rows null.", K(ret));
  } else if (OB_FAIL(check_tablet_valid(VIRT_INC))) {
    LOG_WARN("check tablet id invalid.", K(ret));
  } else if (OB_FAIL(try_init_mem_data(VIRT_INC))) {
    LOG_WARN("failed to init incr index.", K(ret));
  } else if (row_count <= 0) {
    // do nothing
  } else if (OB_FAIL(get_dim(dim))) {
    LOG_WARN("get dim failed.", K(ret));
  } else {
    uint64_t incr_vid_count = 0;
    uint64_t del_vid_count = 0;
    uint64_t null_vid_count = 0;
    int64_t *incr_vids = nullptr;
    uint64_t *del_vids = nullptr;
    uint64_t *null_vids = nullptr;
    float *vectors = nullptr;
    ObVidBound vid_bound = ObVidBound();

    if (OB_ISNULL(incr_vids = static_cast<int64_t *>(tmp_allocator.alloc(sizeof(int64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc incr vids.", K(ret));
    } else if (OB_ISNULL(del_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    } else if (OB_ISNULL(null_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    } else if (OB_ISNULL(vectors = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * row_count * dim)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vectors.", K(ret));
    }

    for (int i = 0; OB_SUCC(ret) && i < row_count; i++) {
      int64_t vid = 0;
      ObString op_str;
      ObString vector_str;
      float *vector = nullptr;
      ObDatum &vid_datum = rows[i].storage_datums_[vid_idx];
      ObDatum &op_datum = rows[i].storage_datums_[type_idx];
      ObDatum &vector_datum = rows[i].storage_datums_[vector_idx];

      if (FALSE_IT(vid = vid_datum.get_int())) {
      } else if (FALSE_IT(op_str = op_datum.get_string())) {
      } else if (op_str.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
        // D type, only record vid
        del_vids[del_vid_count++] = vid;
      } else if (vector_datum.len_ == 0) {
        null_vids[null_vid_count++] = vid;
      } else if (vector_datum.len_ / sizeof(float) != dim) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get vector objct unexpect.", K(ret), K(vector_datum));
      } else if (FALSE_IT(vector_str = vector_datum.get_string())) {
        LOG_WARN("failed to get vector string.", K(ret));
      } else if (OB_ISNULL(vector = reinterpret_cast<float *>(vector_str.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast vectors.", K(ret));
      } else {
        for (int j = 0; j < dim; j++) {
          vectors[incr_vid_count * dim + j] = vector[j];
        }
        incr_vids[incr_vid_count++] = vid;
        vid_bound.set_vid(vid);
      }
    }

    OZ(check_vsag_mem_used());
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
      if (OB_FAIL(obvectorutil::add_index(incr_data_->index_,
                                              vectors,
                                              incr_vids,
                                              dim,
                                              incr_vid_count))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to add index.", K(ret), K(dim), K(row_count));
      } else {
        incr_data_->set_vid_bound(vid_bound);
      }
    }
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPH"));
      TCWLockGuard lock_guard(incr_data_->bitmap_rwlock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < incr_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(incr_data_->bitmap_->insert_bitmap_, incr_vids[i]));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < del_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(incr_data_->bitmap_->insert_bitmap_, del_vids[i]));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < null_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(incr_data_->bitmap_->insert_bitmap_, null_vids[i]));
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::add_extra_valid_vid(
    ObVectorQueryAdaptorResultContext *ctx,
    int64_t vid)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx invalid.", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPI"));
    ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ctx->extra_bitmaps_->insert_bitmap_, vid));
  }

  return ret;
}

/**************************************************************************
* Note:
*  The number of vids must be equal to num;
*  There cannot be null pointers in vectors;
*  The number of floats in vectors must be equal to num * dim;

*  If you want to verify the above content in the add_snap_index interface, you need to traverse vectors and vids.
   In the scenario where a large amount of data is written, there will be a lot of unnecessary performance consumption,
   so the caller needs to ensure this.
**************************************************************************/
int ObPluginVectorIndexAdaptor::add_snap_index(float *vectors, int64_t *vids, int num)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(check_tablet_valid(VIRT_SNAP))) {
    LOG_WARN("check tablet id invalid.", K(ret));
  } else if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get param.", K(ret));
  } else if (OB_FALSE_IT(dim = param->dim_)) {
  } else if (param->type_ == ObVectorIndexAlgorithmType::VIAT_HNSW) {
    if (OB_FAIL(try_init_mem_data(VIRT_SNAP))) {
      LOG_WARN("init snap index failed.", K(ret));
    } else if (num == 0 || OB_ISNULL(vectors)) {
      // do nothing
    } else if (OB_ISNULL(vids)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid data.", K(ret));
    } else if (OB_FAIL(check_vsag_mem_used())) {
      LOG_WARN("check vsag mem used failed.", K(ret));
    } else {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
      if (OB_FAIL(obvectorutil::add_index(snap_data_->index_, vectors, vids, dim, num))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to build index.", K(ret), K(dim), K(num));
      }
    }
  } else if (param->type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_SQ) {
    if (OB_FAIL(init_hnswsq_mem_data())) {
      LOG_WARN("init hnswsq snap index failed.", K(ret));
    } else if (num == 0 || OB_ISNULL(vectors)) {
      // do nothing
    } else if (OB_ISNULL(vids)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid data.", K(ret));
    } else if (OB_FAIL(check_vsag_mem_used())) {
      LOG_WARN("check vsag mem used failed.", K(ret));
    } else {
      TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
      if (snap_data_->has_build_sq_) {
        // directly write into index
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
        if (OB_FAIL(obvectorutil::add_index(snap_data_->index_, vectors, vids, dim, num))) {
          ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
          LOG_WARN("failed to build index.", K(ret), K(dim), K(num));
        } else {
          LOG_INFO("HgraphIndex add into hnswsq index success", K(ret), K(dim), K(num), K(vids[0]), K(vids[num - 1]));
        }
      } else if (OB_ISNULL(snap_data_->index_)) {
        // frist: write into cache
        for (int i = 0; OB_SUCC(ret) && i < num; i++) {
          if (OB_FAIL(snap_data_->vid_array_->push_back(vids[i]))) {
            LOG_WARN("failed to push back into vid array", K(ret));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < num * dim; i++) {
          if (OB_FAIL(snap_data_->vec_array_->push_back(vectors[i]))) {
            LOG_WARN("failed to push back into vector array", K(ret));
          }
        }
        LOG_INFO("HgraphIndex add into cache array success", K(ret), K(dim), K(num), K(vids[0]), K(vids[num - 1]), K(snap_data_->vid_array_->count()));

        // second: construct hnsw+sq index
        ObVecIdxVidArray *vids_array = snap_data_->vid_array_;
        if (OB_NOT_NULL(vids_array)
            && vids_array->count() > VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD
            && OB_ISNULL(snap_data_->index_)) {
          if (OB_FAIL(build_hnswsq_index(param))) {
            LOG_WARN("failed to build hnsw sq index.", K(ret), K(dim));
          }
        }
      } // end for No sq index was built
      // here is the ending for snap_data_ write lock
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support index type", K(ret), K(param->type_), KP(param));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::build_hnswsq_index(ObVectorIndexParam *param)
{
  INIT_SUCC(ret);
  const char* const DATATYPE_FLOAT32 = "float32";
  ObVecIdxVidArray *vid_array = snap_data_->vid_array_;
  ObVecIdxVecArray *vec_array = snap_data_->vec_array_;
  if (OB_ISNULL(ATOMIC_LOAD(&(snap_data_->index_)))) {
    if (OB_NOT_NULL(ATOMIC_LOAD(&(snap_data_->index_)))) {
      // do nothing
    } else if (OB_FAIL(snap_data_->mem_ctx_->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
      LOG_WARN("failed to init incr data mem ctx.", K(ret));
    } else {
      LOG_INFO("HgraphIndex build hnswsq index success", K(ret), K(param->dim_), K(vid_array->count()));
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      if (OB_FAIL(obvectorutil::create_index(snap_data_->index_,
                                             param->type_,
                                             DATATYPE_FLOAT32,
                                             VEC_INDEX_ALGTH[param->dist_algorithm_],
                                             param->dim_,
                                             param->m_,
                                             param->ef_construction_,
                                             param->ef_search_,
                                             snap_data_->mem_ctx_))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
      } else if (OB_FAIL(obvectorutil::build_index(snap_data_->index_,
                                                   vec_array->get_data(),
                                                   vid_array->get_data(),
                                                   param->dim_,
                                                   vid_array->count()))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
      }
      if (OB_SUCC(ret)) {
        snap_data_->set_inited();
        snap_data_->has_build_sq_ = true;
        free_hnswsq_array_data(snap_data_, get_allocator());
      }
      if (OB_FAIL(ret)) {
        free_memdata_resource(VIRT_SNAP, snap_data_, get_allocator(), tenant_id_);
        if (snap_data_->mem_ctx_->is_inited()) {
          snap_data_->mem_ctx_->~ObVsagMemContext();
        }
      }
    }
  }
  return ret;
}

ObVectorIndexAlgorithmType ObPluginVectorIndexAdaptor::get_snap_index_type()
{
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  if (OB_NOT_NULL(snap_data_)) {
    if (OB_NOT_NULL(snap_data_->index_)) {
      int type = obvectorutil::get_index_type(snap_data_->index_);
      index_type = static_cast<ObVectorIndexAlgorithmType>(type);
    }
  }
  return index_type;
}

int ObPluginVectorIndexAdaptor::check_snap_hnswsq_index()
{
  INIT_SUCC(ret);
  const char* const DATATYPE_FLOAT32 = "float32";
  ObVectorIndexParam *param = nullptr;
  ObVecIdxVidArray *vid_array = snap_data_->vid_array_;
  ObVecIdxVecArray *vec_array = snap_data_->vec_array_;
  if (OB_ISNULL(snap_data_) || OB_ISNULL(algo_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null snap data", K(ret), K(snap_data_), K(algo_data_), K(vid_array), K(vec_array));
  } else if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get param.", K(ret));
  } else if (param->type_ == VIAT_HNSW || snap_data_->has_build_sq_) {
    // do nothing
  } else if (OB_ISNULL(snap_data_->index_)) {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (OB_FAIL(snap_data_->mem_ctx_->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
      LOG_WARN("failed to init incr data mem ctx.", K(ret));
    } else if (OB_ISNULL(vid_array) || OB_ISNULL(vec_array)) {
      // do nothing :maybe null data
    } else {
      ObVectorIndexAlgorithmType build_type = VIAT_HNSW;
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      if (OB_FAIL(obvectorutil::create_index(snap_data_->index_,
                                             build_type,
                                             DATATYPE_FLOAT32,
                                             VEC_INDEX_ALGTH[param->dist_algorithm_],
                                             param->dim_,
                                             param->m_,
                                             param->ef_construction_,
                                             param->ef_search_,
                                             snap_data_->mem_ctx_))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
      } else if (OB_FAIL(obvectorutil::add_index(snap_data_->index_,
                                                 vec_array->get_data(),
                                                 vid_array->get_data(),
                                                 param->dim_,
                                                 vid_array->count()))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to create vsag index.", K(ret), K(snap_data_->index_), KPC(param));
      } else {
        LOG_INFO("HNSW build index success", K(ret), K(param->dim_), K(vid_array->count()));
      }
      if (OB_SUCC(ret)) {
        snap_data_->set_inited();
      }
      if (OB_FAIL(ret)) {
        free_memdata_resource(VIRT_SNAP, snap_data_, get_allocator(), tenant_id_);
        if (snap_data_->mem_ctx_->is_inited()) {
          snap_data_->mem_ctx_->~ObVsagMemContext();
        }
      }
      free_hnswsq_array_data(snap_data_, get_allocator());
    }
  } else {
    // maybe retry
    int64_t snap_index_size = 0;
    if (OB_FAIL(obvectorutil::get_index_number(snap_data_->index_, snap_index_size))) {
      ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
      LOG_WARN("failed to get snap index number.", K(ret));
    } else {
      LOG_INFO("get snap index element and array", K(ret), K(snap_index_size), K(vid_array->count()));
    }
    free_hnswsq_array_data(snap_data_, get_allocator());
  }

  return ret;
}

// Query Processor first
int ObPluginVectorIndexAdaptor::check_delta_buffer_table_readnext_status(ObVectorQueryAdaptorResultContext *ctx,
                                                                         common::ObNewRowIterator *row_iter,
                                                                         SCN query_scn)
{
  INIT_SUCC(ret);
  SCN min_delta_scn;

  // TODO 优先判断是否需要等待 PVQ_WAIT
  if (OB_ISNULL(ctx) || OB_ISNULL(row_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx or row_iter invalid.", K(ret), KP(row_iter));
  } else if (OB_FAIL(ctx->init_bitmaps())) {
    LOG_WARN("failed to init ctx bitmaps.", K(ret));
  } else {
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter);
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      int64_t vid = 0;
      ObString op;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (datum_row->get_column_count() != 3) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else if (OB_FALSE_IT(vid = datum_row->storage_datums_[0].get_int())) {
        LOG_WARN("failed to get vid.", K(ret));
      } else if (OB_FALSE_IT(op = datum_row->storage_datums_[1].get_string())) {
        LOG_WARN("failed to get op.", K(ret));
      } else if (op.length() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid op length.", K(ret), K(op));
      } else {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPI"));
        if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_INSERT[0]) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ctx->bitmaps_->insert_bitmap_, vid));

        } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(ctx->bitmaps_->insert_bitmap_, vid));
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ctx->bitmaps_->delete_bitmap_, vid));

        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid op.", K(ret), K(op));
        }
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

#ifndef NDEBUG
    output_bitmap(ctx->bitmaps_->insert_bitmap_);
    output_bitmap(ctx->bitmaps_->delete_bitmap_);
#endif

    if (OB_SUCC(ret)) {
      ctx->status_ = PVQ_LACK_SCN;
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::write_into_delta_mem(ObVectorQueryAdaptorResultContext *ctx, int count, float *vectors, uint64_t *vids, ObVidBound vid_bound)
{
  INIT_SUCC(ret);
  if (count == 0) {
    // do nothing
  } else if (!is_mem_data_init_atomic(VIRT_INC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write into delta mem but incr memdata uninit.", K(ret));
  } else if (OB_FAIL(check_vsag_mem_used())) {
    LOG_WARN("check vsag mem used failed.", K(ret));
  } else {
    TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (check_if_complete_delta(ctx->bitmaps_->insert_bitmap_, count)) {
      if (OB_SUCC(ret)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPJ"));
        TCWLockGuard lock_guard(incr_data_->bitmap_rwlock_);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(incr_data_->bitmap_->insert_bitmap_, vids[i]));
        }
      }

      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      if (OB_SUCC(ret) && OB_FAIL(obvectorutil::add_index(incr_data_->index_,
                                                 vectors,
                                                 reinterpret_cast<int64_t *>(vids),
                                                 ctx->get_dim(),
                                                 count))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("failed to add index.", K(ret), K(ctx->get_dim()), K(count));
      } else {
        incr_data_->set_vid_bound(vid_bound);
      }
      LOG_TRACE("write into delta mem.", K(ret), K(ctx->get_dim()), K(count));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::complete_delta_buffer_table_data(ObVectorQueryAdaptorResultContext *ctx)
{
  INIT_SUCC(ret);
  float *vectors = nullptr;
  uint64_t *vids = nullptr;
  ObVidBound vid_bound;

  int count = 0;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid ctx.", K(ret));
  } else if (ctx->get_vec_cnt() == 0) {
    // do nothing
  } else if (OB_FAIL(try_init_mem_data(VIRT_INC))) {
    LOG_WARN("failed to init incr mem data.", K(ret));
  } else if (OB_ISNULL(vectors = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * ctx->get_dim() * ctx->get_vec_cnt())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc new mem.", K(ret));
  } else if (OB_ISNULL(vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * ctx->get_vec_cnt())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc new mem.", K(ret));
  } else {
    int64_t dim = ctx->get_dim();
    int64_t ctx_vec_cnt = ctx->get_vec_cnt();
    for (int i = 0; OB_SUCC(ret) && i < ctx_vec_cnt; i++) {
      float *vector = nullptr;
      if (ctx->vec_data_.vectors_[i].is_null() || ctx->vec_data_.vectors_[i].get_string().empty()) {
        // do nothing
      } else if (ctx->vec_data_.vectors_[i].get_string().length() != dim * sizeof(float)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid string.", K(ret), K(i), K(ctx->vec_data_.vectors_[i].get_string().length()), K(dim));
      } else if (OB_ISNULL(vector = reinterpret_cast<float *>(ctx->vec_data_.vectors_[i].get_string().ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get float vector.", K(ret), K(i));
      } else {
        uint64_t vid = ctx->get_vids()[i + ctx->get_curr_idx()].get_int();
        vids[count] = vid;
        vid_bound.set_vid(vid);
        for (int j = 0; OB_SUCC(ret) && j < dim; j++) {
          vectors[count * dim + j] = vector[j];
        }
        count++;
      }
    }
    LOG_INFO("SYCN_DELTA_complete_data", K(ctx->vec_data_));
    // print_vids(vids, ctx_vec_cnt);
    // print_vectors(vectors, ctx_vec_cnt, dim);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_into_delta_mem(ctx, count, vectors, vids, vid_bound))) {
    LOG_WARN("failed to write into delta mem.", K(ret), KP(ctx));
  } else {
    ctx->batch_allocator_.reuse();
    ctx->do_next_batch();
    if (ctx->if_next_batch()) {
      ctx->status_ = PVQ_COM_DATA;
      LOG_INFO("SYCN_DELTA_next_batch", K(ctx->vec_data_));
    } else {
      ctx->status_ = PVQ_LACK_SCN;
      LOG_INFO("SYCN_DELTA_batch_end", K(ctx->vec_data_));
    }
  }

  return ret;
}

// Query Processor second
int ObPluginVectorIndexAdaptor::check_index_id_table_readnext_status(ObVectorQueryAdaptorResultContext *ctx,
                                                                     common::ObNewRowIterator *row_iter,
                                                                     SCN query_scn)
{
  INIT_SUCC(ret);
  blocksstable::ObDatumRow *datum_row = nullptr;
  int64_t read_num = 0;
  SCN read_scn = SCN::min_scn();
  ObArray<uint64_t> i_vids;
  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter);
  bool is_skip_4th_index = is_pruned_read_index_id();

  // TODO 优先判断是否需要等待 PVQ_WAIT
  if (OB_ISNULL(ctx) || OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx or row_iter invalid.", K(ret), KP(row_iter));
  } else if (snap_data_->rb_flag_) {
    ctx->status_ = PVQ_LACK_SCN;
    ctx->flag_ = PVQP_SECOND;
  } else {
    ctx->status_ = PVQ_OK;
    ctx->flag_ = PVQP_FIRST;
  }

  if (OB_FAIL(ret)) {
  } else if (is_skip_4th_index) {
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get new row.", K(ret));
    }
  } else {
    if (!datum_row->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid new row.", K(ret));
    } else if (OB_FALSE_IT(read_num = datum_row->storage_datums_[0].get_int())) {
      LOG_WARN("failed to get read scn.", K(ret));
    } else if (OB_FAIL(read_scn.convert_for_gts(read_num))) {
      LOG_WARN("failed to convert from ts.", K(ret), K(read_num));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_skip_4th_index) {
    if (ctx->vec_data_.count_ > 0) {
      ctx->status_ = PVQ_COM_DATA;
    }
  } else if (check_if_complete_index(read_scn) &&
             OB_FAIL(complete_index_mem_data(read_scn, row_iter, datum_row, i_vids))) {
    LOG_WARN("failed to check comple index mem data.", K(ret), K(read_scn), K(vbitmap_data_->scn_));
  } else if (OB_ISNULL(ctx->bitmaps_) || OB_ISNULL(ctx->bitmaps_->insert_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ctx bit map.", K(ret));
  } else if (check_if_complete_delta(ctx->bitmaps_->insert_bitmap_, i_vids.count())) {
    if (OB_FAIL(prepare_delta_mem_data(ctx->bitmaps_->insert_bitmap_, i_vids, ctx))) {
      LOG_WARN("failed to complete.", K(ret));
    } else if (ctx->vec_data_.count_ > 0) {
      ctx->status_ = PVQ_COM_DATA;
    }
  }

  if (OB_SUCC(ret) && check_if_complete_index(read_scn) && !is_skip_4th_index) {
    update_index_id_read_scn();
  }

  return ret;
}

// Query Processor third
int ObPluginVectorIndexAdaptor::check_snapshot_table_wait_status(ObVectorQueryAdaptorResultContext *ctx)
{
  INIT_SUCC(ret);
  // TODO 判断是否需要等待 PVQ_WAIT
  ctx->status_ = PVQ_OK;

  return ret;
}

int ObPluginVectorIndexAdaptor::write_into_index_mem(int64_t dim, SCN read_scn,
                                                     ObArray<uint64_t> &i_vids,
                                                     ObArray<uint64_t> &d_vids)
{
  INIT_SUCC(ret);
  TCWLockGuard lock_guard(vbitmap_data_->mem_data_rwlock_);
  if (read_scn > vbitmap_data_->scn_) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPK"));
    TCWLockGuard wr_vbit_bitmap_lock_guard(vbitmap_data_->bitmap_rwlock_);
    roaring::api::roaring64_bitmap_t *ibitmap = vbitmap_data_->bitmap_->insert_bitmap_;
    roaring::api::roaring64_bitmap_t *dbitmap = vbitmap_data_->bitmap_->delete_bitmap_;
    for (int64_t i = 0; OB_SUCC(ret) && i < i_vids.count(); i++) {
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ibitmap, i_vids[i]));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < d_vids.count(); i++) {
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(dbitmap, d_vids[i]));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < d_vids.count(); i++) {
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(ibitmap, d_vids.at(i)));
    }

#ifndef NDEBUG
    output_bitmap(ibitmap);
    output_bitmap(dbitmap);
#endif

    vbitmap_data_->scn_ = read_scn;
    LOG_TRACE("write into index mem.", K(ret), K(i_vids.count()), K(d_vids.count()), K(read_scn));
  }

  return ret;
}

bool ObPluginVectorIndexAdaptor::check_if_complete_index(SCN read_scn)
{
  bool res = false;
  SCN bitmap_scn = vbitmap_data_->scn_;
  if (read_scn > bitmap_scn) {
    res = true;
    LOG_DEBUG("need complete index mem data.", K(read_scn), K(bitmap_scn));
  }

  return res;
}

bool ObPluginVectorIndexAdaptor::check_if_complete_data(ObVectorQueryAdaptorResultContext *ctx)
{
  bool res = false;

  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->extra_bitmaps_)) {
  } else {
    roaring::api::roaring64_bitmap_t *gene_bitmap = ctx->extra_bitmaps_->insert_bitmap_;
    int64_t gene_vid_cnt = roaring64_bitmap_get_cardinality(gene_bitmap);

    if (is_mem_data_init_atomic(VIRT_INC)) {
      roaring::api::roaring64_bitmap_t *delta_bitmap = ATOMIC_LOAD(&(incr_data_->bitmap_->insert_bitmap_));
      if (!roaring64_bitmap_is_subset(gene_bitmap, delta_bitmap)) {
        res = true;
      } else if (is_mem_data_init_atomic(VIRT_BITMAP)) {
        roaring::api::roaring64_bitmap_t *index_bitmap = ATOMIC_LOAD(&(vbitmap_data_->bitmap_->insert_bitmap_));
        if (!roaring64_bitmap_is_subset(index_bitmap, delta_bitmap)) {
          res = true;
        }
      } else {
        res = gene_vid_cnt > 0;
      }
    } else {
      res = gene_vid_cnt > 0;
    }
  }

  return res;
}

int ObPluginVectorIndexAdaptor::add_datum_row_into_array(blocksstable::ObDatumRow *datum_row,
                                                         ObArray<uint64_t> &i_vids,
                                                         ObArray<uint64_t> &d_vids)
{
  INIT_SUCC(ret);
  int64_t vid = 0;
  ObString op;
  if (OB_ISNULL(datum_row)|| !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (OB_FALSE_IT(vid = datum_row->storage_datums_[1].get_int())) {
    LOG_WARN("failed to get vid.", K(ret));
  } else if (OB_FALSE_IT(op = datum_row->storage_datums_[2].get_string())) {
    LOG_WARN("failed to get op.", K(ret));
  } else if (op.length() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid op length.", K(ret), K(op));
  } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_INSERT[0]) {
    if (OB_FAIL(i_vids.push_back(vid))) {
      LOG_WARN("failed to push back into vids.", K(ret), K(vid));
    }
  } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
    if (OB_FAIL(d_vids.push_back(vid))) {
      LOG_WARN("failed to push back into vids.", K(ret), K(vid));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op.", K(ret), K(op));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::complete_index_mem_data(SCN read_scn,
                                                        common::ObNewRowIterator *row_iter,
                                                        blocksstable::ObDatumRow *last_row,
                                                        ObArray<uint64_t> &i_vids)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  ObArray<uint64_t> d_vids;
  if (OB_ISNULL(row_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx or row_iter null.", K(ret), KP(row_iter));
  } else if (OB_FAIL(try_init_mem_data(VIRT_BITMAP))) {
    LOG_WARN("failed to init valid bitmap", K(ret), K(VIRT_BITMAP));
  } else if (OB_FAIL(add_datum_row_into_array(last_row, i_vids, d_vids))) {
    LOG_WARN("failed to add vid into array.", K(ret), KP(last_row));
  } else {
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter);
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      int64_t vid = 0;
      ObString op;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_FAIL(add_datum_row_into_array(datum_row, i_vids, d_vids))) {
        LOG_WARN("failed to add vid into array.", K(ret), KP(datum_row));
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_dim(dim))) {
      LOG_WARN("failed to get dim.", K(ret));
    } else if (OB_FAIL(write_into_index_mem(dim, read_scn, i_vids, d_vids))) {
      LOG_WARN("failed to write into index mem.", K(ret), K(read_scn));
    }
  }

  return ret;
}

bool ObPluginVectorIndexAdaptor::check_if_complete_delta(roaring::api::roaring64_bitmap_t *gene_bitmap, int64_t count)
{
  bool res = false;
  int64_t gene_vid_cnt = roaring64_bitmap_get_cardinality(gene_bitmap);
  if (gene_vid_cnt == 0 && count > 0) {
    res = true;
  } else if (is_mem_data_init_atomic(VIRT_INC)) {
    roaring::api::roaring64_bitmap_t *delta_bitmap = ATOMIC_LOAD(&(incr_data_->bitmap_->insert_bitmap_));
    if (!roaring64_bitmap_is_subset(gene_bitmap, delta_bitmap)) {
      res = true;
    } else if (count > 0 && is_mem_data_init_atomic(VIRT_BITMAP)) { // andnot_bitmap is null, if count = 0, do nothing
      roaring::api::roaring64_bitmap_t *index_bitmap = ATOMIC_LOAD(&(vbitmap_data_->bitmap_->insert_bitmap_));
      if (!roaring64_bitmap_is_subset(index_bitmap, delta_bitmap)) {
        res = true;
      }
    }
  } else if (roaring64_bitmap_get_cardinality(gene_bitmap) > 0) {
    res = true;
  }
  return res;
}

int ObPluginVectorIndexAdaptor::prepare_delta_mem_data(roaring::api::roaring64_bitmap_t *gene_bitmap,
                                                       ObArray<uint64_t> &i_vids,
                                                       ObVectorQueryAdaptorResultContext *ctx)
{
  INIT_SUCC(ret);
  roaring::api::roaring64_bitmap_t *delta_bitmap = nullptr;
  if (OB_FAIL(try_init_mem_data(VIRT_INC))) {
    LOG_WARN("failed to init mem data incr.", K(ret));
  } else if (OB_ISNULL(gene_bitmap) || OB_ISNULL(delta_bitmap = incr_data_->bitmap_->insert_bitmap_)
            || OB_ISNULL(ctx) || OB_ISNULL(ctx->tmp_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid bitmap.", K(ret), KP(gene_bitmap), KP(delta_bitmap), KP(ctx));
  } else {
    roaring::api::roaring64_bitmap_t *andnot_bitmap = nullptr;
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPL"));
      TCRLockGuard rd_bitmap_lock_guard(incr_data_->bitmap_rwlock_);
      ROARING_TRY_CATCH(andnot_bitmap = roaring64_bitmap_andnot(gene_bitmap, delta_bitmap));
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(andnot_bitmap)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create andnot bitmap", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == roaring64_bitmap_get_cardinality(andnot_bitmap) + i_vids.count()) {
      ctx->vec_data_.count_ = 0;
    } else {
      uint64_t bitmap_cnt = roaring64_bitmap_get_cardinality(andnot_bitmap) + i_vids.count();
      // uint64_t use roaring64_bitmap_to_uint64_array(andnot_bitmap, bitmap_out);
      bool is_continue = true;
      int index = 0;
      int64_t dim = 0;
      ObObj *vids = nullptr;
      int64_t vector_cnt = bitmap_cnt > ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE ?
                           ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE : bitmap_cnt;
      roaring::api::roaring64_iterator_t *bitmap_iter = nullptr;
      {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPM"));
        ROARING_TRY_CATCH(bitmap_iter = roaring64_iterator_create(andnot_bitmap));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(bitmap_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create bitmap iter", K(ret));
      } else if (OB_FAIL(get_dim(dim))) {
        LOG_WARN("failed to get dim.", K(ret));
      } else if (OB_ISNULL(vids = static_cast<ObObj *>(ctx->tmp_allocator_->alloc(sizeof(ObObj) * bitmap_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator.", K(ret), K(bitmap_cnt));
      } else if (OB_ISNULL(ctx->vec_data_.vectors_ = static_cast<ObObj *>(ctx->tmp_allocator_->
                                                      alloc(sizeof(ObObj) * vector_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator.", K(ret), K(bitmap_cnt));
      } else {
        is_continue = roaring64_iterator_has_value(bitmap_iter);
        for (int64_t i = 0; OB_SUCC(ret) && i < vector_cnt; i++) {
          ctx->vec_data_.vectors_[i].set_null();
        }
      }

      while (OB_SUCC(ret) && is_continue) {
        vids[index].reset();
        vids[index++].set_int(roaring64_iterator_value(bitmap_iter));
        is_continue = roaring64_iterator_advance(bitmap_iter);
      }

      if (OB_FAIL(ret)) {
      } else if (index + i_vids.count() != bitmap_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid vid iter count.", K(ret), K(index), K(roaring64_bitmap_get_cardinality(andnot_bitmap)));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < i_vids.count() && i + index < bitmap_cnt; i++) {
          vids[i + index].reset();
          vids[i + index].set_int(i_vids.at(i));
        }

        ctx->vec_data_.dim_ = dim;
        ctx->vec_data_.count_ = bitmap_cnt;
        ctx->vec_data_.vids_ = vids;
        ctx->vec_data_.curr_idx_ = 0;
        LOG_INFO("SYCN_DELTA_prepare_data", K(ctx->vec_data_));
      }

      if (OB_NOT_NULL(bitmap_iter)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPN"));
        roaring64_iterator_free(bitmap_iter);
        bitmap_iter = nullptr;
      }

    }
    if (OB_NOT_NULL(andnot_bitmap)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPO"));
      roaring64_bitmap_free(andnot_bitmap);
      andnot_bitmap = nullptr;
    }

  }

  return ret;
}

int ObPluginVectorIndexAdaptor::serialize(ObIAllocator *allocator, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb)
{
  int ret = OB_SUCCESS;
  ObVectorIndexSerializer index_seri(*allocator);
  int64_t snap_index_size = 0;
  if (!snap_data_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("snap index is not init", K(ret));
  } else if (OB_FAIL(obvectorutil::get_index_number(snap_data_->index_, snap_index_size))) {
    ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
    LOG_WARN("failed to get snap index number.", K(ret));
  } else if (snap_index_size == 0) {
    // do nothing
    LOG_INFO("[vec index] empty snap index, do not need to serialize");
  } else if (OB_FAIL(index_seri.serialize(snap_data_->index_, cb_param, cb, tenant_id_))) {
    LOG_WARN("serialize index failed.", K(ret));
  } else {
    snap_data_->rb_flag_ = true;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::generate_snapshot_valid_bitmap(ObVectorQueryAdaptorResultContext *ctx,
                                                               common::ObNewRowIterator *row_iter,
                                                               SCN query_scn)
{
  INIT_SUCC(ret);


  return ret;
}

int ObPluginVectorIndexAdaptor::merge_and_generate_bitmap(ObVectorQueryAdaptorResultContext *ctx,
                                                          roaring::api::roaring64_bitmap_t *&ibitmap,
                                                          roaring::api::roaring64_bitmap_t *&dbitmap)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument.", K(ctx));
  } else if (ctx->is_bitmaps_valid(true/*is_extra*/)) {
    ibitmap = ctx->extra_bitmaps_->insert_bitmap_;
    dbitmap = ibitmap;
  } else if (!is_mem_data_init_atomic(VIRT_BITMAP)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPP"));
    ibitmap = ctx->bitmaps_->insert_bitmap_;
    dbitmap = ctx->bitmaps_->delete_bitmap_;
    ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(ibitmap, dbitmap));
    LOG_DEBUG("vbitmap is not inited.", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPQ"));
    ibitmap = ctx->bitmaps_->insert_bitmap_;
    dbitmap = ctx->bitmaps_->delete_bitmap_;
#ifndef NDEBUG
    output_bitmap(ibitmap);
    output_bitmap(dbitmap);
    output_bitmap(vbitmap_data_->bitmap_->insert_bitmap_);
    output_bitmap(vbitmap_data_->bitmap_->delete_bitmap_);
#endif
    if (OB_SUCC(ret)) {
      TCRLockGuard rd_bitmap_lock_guard(vbitmap_data_->bitmap_rwlock_);
      ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(ibitmap, vbitmap_data_->bitmap_->insert_bitmap_));
      ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(dbitmap, vbitmap_data_->bitmap_->delete_bitmap_));
    }

    ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(ibitmap, dbitmap));
    LOG_DEBUG("vbitmap is inited.", K(ret));

#ifndef NDEBUG
    output_bitmap(ibitmap);
    output_bitmap(dbitmap);
#endif
  }

  return ret;
}

// for debug version
int ObPluginVectorIndexAdaptor::print_bitmap(roaring::api::roaring64_bitmap_t *bitmap)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(bitmap)) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    uint64_t bitmap_cnt = roaring64_bitmap_get_cardinality(bitmap);
    uint64_t *nums = nullptr;
    if (bitmap_cnt == 0) {
      // do nothing
    } else if (OB_ISNULL(nums = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * bitmap_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc.", K(ret));
    } else {
      ObStringBuffer buffer(&tmp_allocator);
      roaring64_bitmap_to_uint64_array(bitmap, nums);
      for (int64_t i = 0; i < bitmap_cnt; i++) {
        char buf[15];
        sprintf(buf, "%llu ", static_cast<unsigned long long>(nums[i]));
        buffer.append(buf);
      }
      LOG_INFO("PRINT_BITMAP_DEBUG", K(buffer), KP(buffer.ptr()), K(buffer.string()));
    }
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::print_vids(uint64_t *vids, int64_t count)
{
  if (count != 0) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObStringBuffer buffer(&tmp_allocator);
    for (int64_t i = 0; i < count; i++) {
      char buf[10];
      sprintf(buf, "%llu ", static_cast<unsigned long long>(vids[i]));
      buffer.append(buf);
    }
    LOG_INFO("SYCN_DELTA_vids", K(buffer), KP(buffer.ptr()), K(buffer.string()));
  }
}

void ObPluginVectorIndexAdaptor::print_vectors(float *vecs, int64_t count, int64_t dim)
{
  if (count != 0) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    //ObStringBuffer buffer(&tmp_allocator);
    for (int i = 0; i < count; i++) {
      ObStringBuffer buffer(&tmp_allocator);
      for (int j = 0; j < dim; j++) {
        char buf[10];
        sprintf(buf, "%.1f ", (vecs[i * dim + j]));
        buffer.append(buf);
      }
      LOG_INFO("SYCN_DELTA_vectors", K(buffer), KP(buffer.ptr()), K(buffer.string()));
    }
  }
}

int ObPluginVectorIndexAdaptor::vsag_query_vids(ObVectorQueryAdaptorResultContext *ctx,
                                                ObVectorQueryConditions *query_cond,
                                                int64_t dim, float *query_vector,
                                                ObVectorQueryVidIterator *&vids_iter)
{
  INIT_SUCC(ret);
  roaring::api::roaring64_bitmap_t *ibitmap = nullptr;
  roaring::api::roaring64_bitmap_t *dbitmap = nullptr;

  int64_t *merge_vids = nullptr;
  float *merge_distance = nullptr;
  const int64_t *delta_vids = nullptr;
  const int64_t *snap_vids = nullptr;
  const float *delta_distances = nullptr;
  const float *snap_distances = nullptr;
  int64_t delta_res_cnt = 0;
  int64_t snap_res_cnt = 0;

  if (OB_FAIL(check_vsag_mem_used())) {
    LOG_WARN("failed to check vsag mem used.", K(ret));
  } else if (OB_FAIL(merge_and_generate_bitmap(ctx, ibitmap, dbitmap))) {
    LOG_WARN("failed to merge and generate bitmap.", K(ret));
  }

// for dubug
#ifndef NDEBUG
  if (OB_FAIL(ret)) {
  } else if (is_mem_data_init_atomic(VIRT_INC) && OB_NOT_NULL(ctx->bitmaps_) &&
             OB_FAIL(print_bitmap(ctx->bitmaps_->insert_bitmap_))) {
    LOG_WARN("failed to print bitmap.", K(ret));
  } else if (is_mem_data_init_atomic(VIRT_INC) && OB_NOT_NULL(ctx->bitmaps_) &&
             OB_FAIL(print_bitmap(ctx->bitmaps_->delete_bitmap_))) {
    LOG_WARN("failed to print bitmap.", K(ret));
  } else if (is_mem_data_init_atomic(VIRT_BITMAP) && OB_NOT_NULL(vbitmap_data_->bitmap_) &&
             OB_FAIL(print_bitmap(vbitmap_data_->bitmap_->insert_bitmap_))) {
    LOG_WARN("failed to print bitmap.", K(ret));
  } else if (is_mem_data_init_atomic(VIRT_BITMAP) && OB_NOT_NULL(vbitmap_data_->bitmap_) &&
             OB_FAIL(print_bitmap(vbitmap_data_->bitmap_->delete_bitmap_))) {
    LOG_WARN("failed to print bitmap.", K(ret));
  }
#endif

  if (OB_SUCC(ret)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
    TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (OB_FAIL(is_mem_data_init_atomic(VIRT_INC) &&
                obvectorutil::knn_search(get_incr_index(),
                                         query_vector,
                                         dim,
                                         query_cond->query_limit_,
                                         delta_distances,
                                         delta_vids,
                                         delta_res_cnt,
                                         query_cond->ef_search_,
                                         ibitmap,
                                         true/*reverse_filter*/))) {
      ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
      LOG_WARN("knn search delta failed.", K(ret), K(dim));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
    TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);

    bool is_pre_filter = ctx->is_bitmaps_valid(true/*is_extra*/);
    if (OB_FAIL(is_mem_data_init_atomic(VIRT_SNAP) &&
                obvectorutil::knn_search(get_snap_index(),
                                         query_vector,
                                         dim,
                                         query_cond->query_limit_,
                                         snap_distances,
                                         snap_vids,
                                         snap_res_cnt,
                                         query_cond->ef_search_,
                                         dbitmap,
                                         is_pre_filter/*reverse_filter*/))) {
      ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
      LOG_WARN("knn search snap failed.", K(ret), K(dim));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    int64_t actual_res_cnt = 0;
    const ObVsagQueryResult delta_data = {delta_res_cnt, delta_vids, delta_distances};
    const ObVsagQueryResult snap_data = {snap_res_cnt, snap_vids, snap_distances};
    uint64_t tmp_result_cnt = delta_res_cnt + snap_res_cnt;
    uint64_t max_res_cnt = tmp_result_cnt < query_cond->query_limit_ ? tmp_result_cnt : query_cond->query_limit_;
    LOG_DEBUG("query result info", K(delta_res_cnt), K(snap_res_cnt));

    if (max_res_cnt == 0) {
      // when max_res_cnt == 0, it means (snap_res_cnt == 0 && delta_res_cnt == 0), there is no data in table, do not need alloc memory for res_vid_array
      actual_res_cnt = 0;
    } else if (OB_ISNULL(merge_vids = static_cast<int64_t*>(ctx->allocator_->alloc /*can't use tmp allocator here, its final result of query*/
                                  (sizeof(int64_t) * max_res_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator merge vids.", K(ret));
    } else if (OB_ISNULL(merge_distance = static_cast<float*>(ctx->allocator_->alloc(sizeof(float) * max_res_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator merge distance.", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexHelper::merge_delta_and_snap_vids(delta_data, snap_data,
                                                                            query_cond->query_limit_,
                                                                            actual_res_cnt,
                                                                            merge_vids, merge_distance))) {
      LOG_WARN("failed to merge delta and snap vids.", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vids_iter->init(actual_res_cnt, merge_vids, merge_distance, ctx->allocator_))) {
      LOG_WARN("iter init failed.", K(ret), K(actual_res_cnt), K(merge_vids), K(ctx->allocator_));
    } else if (actual_res_cnt == 0) {
      LOG_INFO("query vector result 0", K(actual_res_cnt), K(delta_res_cnt), K(snap_res_cnt));
    }
  }

  ibitmap = nullptr;
  dbitmap = nullptr;

  if (delta_res_cnt != 0) {
    if (delta_distances != nullptr) {
      incr_data_->mem_ctx_->Deallocate((void *)delta_distances);
      delta_distances = nullptr;
    }

    if (delta_vids != nullptr) {
      incr_data_->mem_ctx_->Deallocate((void *)delta_vids);
      delta_vids = nullptr;
    }
  }

  if (snap_res_cnt != 0) {
    if (snap_distances != nullptr) {
      snap_data_->mem_ctx_->Deallocate((void *)snap_distances);
      snap_distances = nullptr;
    }

    if (snap_vids != nullptr) {
      snap_data_->mem_ctx_->Deallocate((void *)snap_vids);
      snap_distances = nullptr;
    }
  }
  LOG_TRACE("now all_vsag_used is: ", K(ATOMIC_LOAD(all_vsag_use_mem_)));
  return ret;
}

int ObPluginVectorIndexAdaptor::query_result(ObVectorQueryAdaptorResultContext *ctx,
                                             ObVectorQueryConditions *query_cond,
                                             ObVectorQueryVidIterator *&vids_iter)
{
  INIT_SUCC(ret);
  vids_iter = nullptr;
  int64_t dim = 0;
  int64_t *merge_vids = nullptr;
  void *iter_buff = nullptr;
  float *query_vector;

  if (OB_ISNULL(ctx) || OB_ISNULL(query_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx invalid.", K(ret));
  } else if (query_cond->query_limit_ <= 0 || query_cond->query_vector_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid query limit.", K(ret), K(query_cond->query_limit_));
  } else if (OB_FAIL(get_dim(dim))) {
    LOG_WARN("get dim failed.", K(ret));
  } else if (query_cond->query_vector_.length() / sizeof(float) != dim) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector objct unexpect.", K(ret), K(query_cond->query_vector_.length()), K(dim));
  } else if (OB_ISNULL(query_vector = reinterpret_cast<float *>(query_cond->query_vector_.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast vectors.", K(ret), K(query_cond->query_vector_));
  } else if (OB_ISNULL(iter_buff = ctx->allocator_->alloc(sizeof(ObVectorQueryVidIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator iter.", K(ret));
  } else if (OB_FALSE_IT(vids_iter = new(iter_buff) ObVectorQueryVidIterator())) {
  } else if (ctx->flag_ == PVQP_FIRST) {
    if (OB_FAIL(vsag_query_vids(ctx, query_cond, dim, query_vector, vids_iter))) {
      LOG_WARN("failed to query vids.", K(ret), K(dim));
    }

  } else if (ctx->flag_ == PVQP_SECOND) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    if (OB_ISNULL(query_cond->row_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get snapshot table iter null.", K(ret), KP(query_cond));
    } else {
      ObHNSWDeserializeCallback::CbParam param;
      param.iter_ = query_cond->row_iter_;
      param.allocator_ = &tmp_allocator;

      ObHNSWDeserializeCallback callback(static_cast<void*>(this));
      ObIStreamBuf::Callback cb = callback;

      ObVectorIndexSerializer index_seri(tmp_allocator);

      TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
      if (OB_FAIL(index_seri.deserialize(snap_data_->index_, param, cb, tenant_id_))) {
        LOG_WARN("serialize index failed.", K(ret));
      } else {
        close_snap_data_rb_flag();
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(vsag_query_vids(ctx, query_cond, dim, query_vector, vids_iter))) {
      LOG_WARN("failed to query vids.", K(ret), K(dim));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::try_init_snap_data(ObVectorIndexAlgorithmType actual_type)
{
  INIT_SUCC(ret);
  if (type_ == VIAT_HNSW_SQ) {
    if (actual_type == VIAT_HNSW_SQ) {
      // actual create hnswsq index
      if (OB_FAIL(init_snap_data_without_lock())) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else if (actual_type == VIAT_HNSW) {
      // actual create hnsw index
      if (OB_FAIL(init_snap_data_without_lock(VIAT_HNSW))) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get serialize type invalid", K(ret), K(actual_type), K(type_));
    }
  } else if (type_ == VIAT_HNSW) {
    if (OB_FAIL(init_snap_data_without_lock())) {
      LOG_WARN("failed to init snap mem data", K(ret), K(type_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get serialize type invalid", K(ret), K(actual_type), K(type_));
  }
  LOG_INFO("HgraphIndex vector index try init snap data without lock", K(ret), K(type_), K(actual_type));
  return ret;
}

int ObPluginVectorIndexAdaptor::cast_roaringbitmap_to_stdmap(const roaring::api::roaring64_bitmap_t *bitmap,
                                                             std::map<int, bool> &mymap,
                                                             uint64_t tenant_id)
{
  INIT_SUCC(ret);
  uint64_t bitmap_cnt = roaring64_bitmap_get_cardinality(bitmap);
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  uint64_t *buf = nullptr;

  if (bitmap_cnt == 0) {
    // do nothing
  } else if (OB_ISNULL(bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get bitmap invalid.", K(ret));
  } else if (OB_ISNULL(buf = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * bitmap_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf.", K(ret));
  } else {
    roaring::api::roaring64_iterator_t *roaring_iter = nullptr;
    ROARING_TRY_CATCH(roaring_iter = roaring64_iterator_create(bitmap));
    if (OB_SUCC(ret)) {
      uint64_t ele_cnt = roaring64_iterator_read(roaring_iter, buf, bitmap_cnt);
      for (int i = 0; i < ele_cnt; i++) {
        mymap[buf[i]] = false;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_tablet_id(ObVectorIndexRecordType type, ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  if (tablet_id.is_valid()) {
    ObTabletID *tablet_to_modify = nullptr;

    if (type == VIRT_INC) {
      tablet_to_modify = &inc_tablet_id_;
    } else if (type == VIRT_BITMAP) {
      tablet_to_modify = &vbitmap_tablet_id_;
    } else if (type == VIRT_SNAP) {
      tablet_to_modify = &snapshot_tablet_id_;
    } else if (type == VIRT_DATA) {
      tablet_to_modify = &data_tablet_id_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN( "invalid type", KR(ret), K(type), K(tablet_id), K(*this));
    }

    if (OB_SUCC(ret)) {
      if (tablet_to_modify->is_valid() && *tablet_to_modify != tablet_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet id already existed", KR(ret), K(type), K(tablet_id), K(*this));
      } else {
        *tablet_to_modify = tablet_id;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_table_id(ObVectorIndexRecordType type, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (table_id != OB_INVALID_ID) {
    uint64_t *table_id_to_modify = nullptr;

    if (type == VIRT_INC) {
      table_id_to_modify = &inc_table_id_;
    } else if (type == VIRT_BITMAP) {
      table_id_to_modify = &vbitmap_table_id_;
    } else if ( type == VIRT_SNAP) {
      table_id_to_modify = &snapshot_table_id_;
    } else if (type == VIRT_DATA) {
      table_id_to_modify = &data_table_id_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", KR(ret), K(type), K(table_id), K(*this));
    }

    if (OB_SUCC(ret)) {
      if (*table_id_to_modify != OB_INVALID_ID && *table_id_to_modify != table_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table id already existed", KR(ret), K(type), K(table_id), K(*this));
      } else {
        *table_id_to_modify = table_id;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_index_identity(ObString &index_identity)
{
  int ret = OB_SUCCESS;
  if (!index_identity_.empty() && index_identity_ == index_identity) {
    // do nothing
    LOG_INFO("try to change same vector index identity", K(index_identity), K(*this));
  } else if (index_identity.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index identity is empty", KR(ret), K(*this));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null allocator to set vector index identity ", KR(ret), K(*this));
  } else {
    if (!index_identity_.empty()) {
      allocator_->free(index_identity_.ptr());
      index_identity_.reset();
    }
    if (OB_FAIL(ob_write_string(*allocator_, index_identity, index_identity_))) {
      LOG_WARN("fail set vector index identity ", KR(ret), K(*this));
    } else {
      LOG_INFO("change vector index identity success", K(index_identity), K(*this));
    }
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::set_vid_rowkey_info(ObVectorIndexSharedTableInfo &info)
{
  rowkey_vid_tablet_id_ = info.rowkey_vid_tablet_id_;
  vid_rowkey_tablet_id_ = info.vid_rowkey_tablet_id_;
  rowkey_vid_table_id_ = info.rowkey_vid_table_id_;
  vid_rowkey_table_id_ = info.vid_rowkey_table_id_;
  data_table_id_ = info.data_table_id_;
}

int ObPluginVectorIndexAdaptor::set_adaptor_ctx_flag(ObVectorQueryAdaptorResultContext *ctx) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null.", K(ret));
  } else {
    ctx->flag_ = snap_data_->rb_flag_ ? PVQP_SECOND : PVQP_FIRST;
  }

  return ret;
}

// use init flag instead？
bool ObPluginVectorIndexAdaptor::is_complete()
{
  return is_inc_tablet_valid()
          && is_vbitmap_tablet_valid()
          && is_snap_tablet_valid()
          && is_data_tablet_valid()
          && (vbitmap_table_id_ != OB_INVALID_ID)
          && (inc_table_id_ != OB_INVALID_ID)
          && (snapshot_table_id_ != OB_INVALID_ID);
}

static int ref_memdata(ObVectorIndexMemData *&dst_mem_data, ObVectorIndexMemData *&src_mem_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_mem_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null input", KP(src_mem_data), KR(ret));
  } else {
    dst_mem_data = src_mem_data;
    dst_mem_data->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::merge_mem_data_(ObVectorIndexRecordType type,
                                                ObPluginVectorIndexAdaptor *src_adapter,
                                                ObVectorIndexMemData *&src_mem_data,
                                                ObVectorIndexMemData *&dst_mem_data)
{
  // ToDo: may need lock or atomic access when replace dst mem data!
  int ret = OB_SUCCESS;
  bool is_same_mem_data = false;
  if (OB_ISNULL(src_adapter) || OB_ISNULL(src_mem_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null input", KP(src_adapter), KP(src_mem_data), KR(ret));
  } else if ((this == src_adapter) || (src_mem_data == dst_mem_data)) {
    is_same_mem_data = true;
  } else if ((OB_NOT_NULL(dst_mem_data) && dst_mem_data->is_inited())
             && src_mem_data->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict use of src_mem_data", K(type), KPC(src_mem_data), KPC(dst_mem_data), K(lbt()));
  }

  if (OB_FAIL(ret) || is_same_mem_data) {
    // do nothing
  } else if (src_mem_data->is_inited()) {
    if (OB_NOT_NULL(dst_mem_data) && OB_FAIL(try_free_memdata_resource(type, dst_mem_data, allocator_, tenant_id_))) {
      LOG_WARN("failed to free mem data resource", KR(ret), K(type), KPC(dst_mem_data));
    } else {
      dst_mem_data = nullptr;
    }
    (void)ref_memdata(dst_mem_data, src_mem_data);
  } else if (OB_NOT_NULL(dst_mem_data) && dst_mem_data->is_inited()) {
    // do nothing
  } else {
    // both mem data not used, decide by type
    if (((type == VIRT_INC) && (src_adapter->get_create_type() == CreateTypeInc))
        || ((type == VIRT_BITMAP) && (src_adapter->get_create_type() == CreateTypeBitMap))
        || ((type == VIRT_SNAP) && (src_adapter->get_create_type() == CreateTypeSnap))) {
      if (OB_NOT_NULL(dst_mem_data) && OB_FAIL(try_free_memdata_resource(type, dst_mem_data, allocator_, tenant_id_))) {
        LOG_WARN("failed to free mem data resource", KR(ret), K(type), KPC(dst_mem_data));
      } else {
        (void)ref_memdata(dst_mem_data, src_mem_data);
      }
    } else if (OB_ISNULL(dst_mem_data)) {
      // when full partial merge to complete
      (void)ref_memdata(dst_mem_data, src_mem_data);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(type), KPC(src_adapter), KPC(dst_mem_data), KR(ret));
    }
  }
  return ret;
}

// if merge failed, caller should release resources
int ObPluginVectorIndexAdaptor::merge_parital_index_adapter(ObPluginVectorIndexAdaptor *partial_idx_adpt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partial_idx_adpt)) {
    // do nothing
  } else if (partial_idx_adpt == this) {
    // merge self, do nothing
  } else {
    if (partial_idx_adpt->is_inc_tablet_valid()) {
      if (OB_FAIL(set_tablet_id(VIRT_INC, partial_idx_adpt->get_inc_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_table_id(VIRT_INC, partial_idx_adpt->get_inc_table_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_tablet_id(VIRT_DATA, partial_idx_adpt->get_data_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(merge_mem_data_(VIRT_INC, partial_idx_adpt, partial_idx_adpt->incr_data_, incr_data_))){
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (partial_idx_adpt->is_vbitmap_tablet_valid()) {
      if (OB_FAIL(set_tablet_id(VIRT_BITMAP, partial_idx_adpt->get_vbitmap_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_table_id(VIRT_BITMAP, partial_idx_adpt->get_vbitmap_table_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_tablet_id(VIRT_DATA, partial_idx_adpt->get_data_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(merge_mem_data_(VIRT_BITMAP, partial_idx_adpt, partial_idx_adpt->vbitmap_data_, vbitmap_data_))){
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (partial_idx_adpt->is_snap_tablet_valid()) {
      if (OB_FAIL(set_tablet_id(VIRT_SNAP, partial_idx_adpt->get_snap_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_table_id(VIRT_SNAP, partial_idx_adpt->get_snapshot_table_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_tablet_id(VIRT_DATA, partial_idx_adpt->get_data_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(merge_mem_data_(VIRT_SNAP, partial_idx_adpt, partial_idx_adpt->snap_data_, snap_data_))){
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      }
    }

    if (OB_SUCC(ret) && !partial_idx_adpt->get_index_identity().empty()) {
      if (OB_FAIL(set_index_identity(partial_idx_adpt->get_index_identity()))) {
        LOG_WARN("failed to set index identity", KR(ret), K(*this), KPC(partial_idx_adpt));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(partial_idx_adpt->all_vsag_use_mem_)) {
      all_vsag_use_mem_ = partial_idx_adpt->all_vsag_use_mem_;
    }

    if (OB_SUCC(ret)
        && OB_ISNULL(algo_data_)
        && OB_NOT_NULL(partial_idx_adpt->algo_data_)) {
      // just replace for simple, fix memory later
      ObVectorIndexParam *hnsw_param = nullptr;
      if (OB_ISNULL(get_allocator())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("adaptor allocator invalid.", K(ret));
      } else if (OB_ISNULL(hnsw_param = static_cast<ObVectorIndexParam *>
                                (get_allocator()->alloc(sizeof(ObVectorIndexParam))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate mem.", K(ret));
      } else {
        *hnsw_param = *(ObVectorIndexParam *)partial_idx_adpt->algo_data_;
        algo_data_ = hnsw_param;
        type_ = partial_idx_adpt->type_;
      }
    }
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::inc_ref()
{
  int64_t ref_count = ATOMIC_AAF(&ref_cnt_, 1);
  // LOG_INFO("inc ref count", K(ref_count), KP(this), KPC(this), K(lbt())); // remove later
}

bool ObPluginVectorIndexAdaptor::dec_ref_and_check_release()
{
  int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
  // LOG_INFO("dec ref count", K(ref_count), KP(this), KPC(this), K(lbt()));
  return (ref_count == 0);
}

int ObPluginVectorIndexAdaptor::check_need_sync_to_follower(bool &need_sync)
{
  int ret = OB_SUCCESS;
  need_sync = false;

  if (!is_complete()) {
    // do nothing
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no complete adapter need not sync memdata", K(*this), KR(ret));
  } else  {
    // no get_index_number interface currently
    int64_t current_incr_count = 0;
    if (OB_NOT_NULL(get_incr_index())) {
      if (OB_FAIL(obvectorutil::get_index_number(get_incr_index(), current_incr_count))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("fail to get incr index number", K(ret));
        ret = OB_SUCCESS; // continue to check other parts
      }
    }

    int64_t current_bitmap_count = 0;

    if (OB_NOT_NULL(get_vbitmap_dbitmap())) {
      TCRLockGuard rd_bitmap_lock_guard(vbitmap_data_->bitmap_rwlock_);
      current_bitmap_count += roaring64_bitmap_get_cardinality(get_vbitmap_dbitmap());
    }
    if (OB_NOT_NULL(get_vbitmap_ibitmap())) {
      TCRLockGuard rd_bitmap_lock_guard(vbitmap_data_->bitmap_rwlock_);
      current_bitmap_count += roaring64_bitmap_get_cardinality(get_vbitmap_ibitmap());
    }

    int64_t current_snapshot_count = 0;
    if (OB_NOT_NULL(get_snap_index())) {
      if (OB_FAIL(obvectorutil::get_index_number(get_snap_index(), current_snapshot_count))) {
        ret = ObPluginVectorIndexHelper::vsag_errcode_2ob(ret);
        LOG_WARN("fail to get snap index number", K(ret));
        ret = OB_SUCCESS; // continue to check other parts
      }
    }

    if (current_incr_count > follower_sync_statistics_.incr_count_ + VEC_INDEX_INCR_DATA_SYNC_THRESHOLD
        || current_bitmap_count > follower_sync_statistics_.vbitmap_count_ + VEC_INDEX_INCR_DATA_SYNC_THRESHOLD
        || current_snapshot_count != follower_sync_statistics_.snap_count_) { // use scn_ in memdata for compare
      need_sync = true;
      LOG_INFO("need sync to follower",
        K(follower_sync_statistics_), K(current_incr_count), K(current_bitmap_count),
        K(current_snapshot_count), KPC(this));
    } else {
      LOG_DEBUG("not need sync to follower",
        K(follower_sync_statistics_), K(current_incr_count), K(current_bitmap_count),
        K(current_snapshot_count), KPC(this));
    }

    if (need_sync) { // if need sync, update statistics, otherwise use current statistics and check next loop
      follower_sync_statistics_.incr_count_ = current_incr_count;
      follower_sync_statistics_.vbitmap_count_ = current_bitmap_count;
      follower_sync_statistics_.snap_count_ = current_snapshot_count;
    }
  }
  return ret;
}

// debug function
void ObPluginVectorIndexAdaptor::output_bitmap(roaring::api::roaring64_bitmap_t *bitmap)
{
  ObArenaAllocator tmp_allocator;
  INIT_SUCC(ret);
  uint64_t bitmap_cnt = roaring64_bitmap_get_cardinality(bitmap);
  if (bitmap_cnt > 0) {
    uint64_t *vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * bitmap_cnt));
    if (OB_NOT_NULL(vids)) {
      roaring64_bitmap_to_uint64_array(bitmap, vids);
      LOG_INFO("BITMAP_INFO:", K(ret), K(bitmap_cnt), KP(vids), K(vids[0]), K(vids[bitmap_cnt - 1]));
    }
  }
  tmp_allocator.reset();
}

int ObPluginVectorIndexAdaptor::check_vsag_mem_used()
{
  INIT_SUCC(ret);
  int64_t mem_size = 0;
  // There is no need to worry about the thread safety of mem_check_cnt_ here,
  // because mem_check_cnt_ is used to roughly determine
  // whether to perform memory verification and does not require accurate counting.
  mem_check_cnt_++;
  if (is_mem_limited_ || mem_check_cnt_ > 10) {
    mem_check_cnt_ = 0;
    ObRbMemMgr *mem_mgr = nullptr;
    int64_t bitmap_mem_used = 0;
    if (OB_ISNULL(mem_mgr = MTL(ObRbMemMgr *))) {
      // do nothing
    } else {
      bitmap_mem_used = mem_mgr->get_vec_idx_used();
    }
    if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, mem_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
    } else if (ATOMIC_LOAD(all_vsag_use_mem_) + bitmap_mem_used > mem_size) {
      is_mem_limited_ = true;
      ret = OB_ERR_VSAG_MEM_LIMIT_EXCEEDED;
      LOG_USER_ERROR(OB_ERR_VSAG_MEM_LIMIT_EXCEEDED, int(mem_size>>20));
      LOG_WARN("Memory usage exceeds user limit.", K(ret), K(mem_size), K(ATOMIC_LOAD(all_vsag_use_mem_)), K(bitmap_mem_used));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::get_incr_vsag_mem_used()
{
  int64_t size = 0;
  if (incr_data_->is_inited()) {
    size = incr_data_->mem_ctx_->used();
  }
  return size;
}

int ObPluginVectorIndexAdaptor::get_incr_vsag_mem_hold()
{
  int64_t size = 0;
  if (incr_data_->is_inited()) {
    size = incr_data_->mem_ctx_->hold();
  }
  return size;
}

int ObPluginVectorIndexAdaptor::get_snap_vsag_mem_used()
{
  int64_t size = 0;
  if (snap_data_->is_inited()) {
    size = snap_data_->mem_ctx_->used();
  }
  return size;
}

int ObPluginVectorIndexAdaptor::get_snap_vsag_mem_hold()
{
  int64_t size = 0;
  if (snap_data_->is_inited()) {
    size = snap_data_->mem_ctx_->hold();
  }
  return size;
}

void *ObVsagMemContext::Allocate(size_t size)
{
  void *ret_ptr = nullptr;

  if (size != 0) {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + size;

    void *ptr = mem_context_->get_malloc_allocator().alloc(actual_size);
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_AAF(all_vsag_use_mem_, actual_size);

      *(int64_t*)ptr = actual_size;
      ret_ptr = (char*)ptr + MEM_PTR_HEAD_SIZE;
    }
  }

  return ret_ptr;
}

void ObVsagMemContext::Deallocate(void* p)
{
  if (OB_NOT_NULL(p)) {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t size = *(int64_t *)size_ptr;

    ATOMIC_SAF(all_vsag_use_mem_, size);
    mem_context_->get_malloc_allocator().free((char*)p - MEM_PTR_HEAD_SIZE);
    p = nullptr;
  }
}

void *ObVsagMemContext::Reallocate(void* p, size_t size)
{
  void *new_ptr = nullptr;
  if (size == 0) {
    if (OB_NOT_NULL(p)) {
      Deallocate(p);
      p = nullptr;
    }
  } else if (OB_ISNULL(p)) {
    new_ptr = Allocate(size);
  } else {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t old_size = *(int64_t *)size_ptr - MEM_PTR_HEAD_SIZE;
    if (old_size >= size) {
      new_ptr = p;
    } else {
      new_ptr = Allocate(size);
      if (OB_ISNULL(new_ptr) || OB_ISNULL(p)) {
      } else {
        MEMCPY(new_ptr, p, old_size);
        Deallocate(p);
        p = nullptr;
      }
    }
  }
  return new_ptr;
}

int ObVsagMemContext::init(lib::MemoryContext &parent_mem_context,
                           uint64_t *all_vsag_use_mem,
                           uint64_t tenant_id)
{
  INIT_SUCC(ret);
  lib::ContextParam param;
  ObMemAttr attr(tenant_id, "VIndexVsagADP");
  SET_IGNORE_MEM_VERSION(attr);
  param.set_mem_attr(attr)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_parallel(4)
    .set_properties(lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT);
  if (OB_FAIL(parent_mem_context->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory entity failed", K(ret));
  } else {
    all_vsag_use_mem_ = all_vsag_use_mem;
  }

  return ret;
}

};
};
