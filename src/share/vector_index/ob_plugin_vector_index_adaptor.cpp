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
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#include "storage/vector_index/ob_vector_index_refresh.h"

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
    sync_info_(),
    index_type_(ObVectorIndexAlgorithmType::VIAT_MAX)
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
  index_type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
}

ObVectorSegmentInfo::ObVectorSegmentInfo()
 : data_table_id_(OB_INVALID_ID),
   data_tablet_id_(OB_INVALID_ID),
   vbitmap_tablet_id_(OB_INVALID_ID),
   inc_index_tablet_id_(OB_INVALID_ID),
   snapshot_index_tablet_id_(OB_INVALID_ID),
   block_count_(0),
   segment_type_(ObVectorIndexSegmentType::INVALID),
   index_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
   mem_used_(0),
   min_vid_(0),
   max_vid_(0),
   vector_cnt_(-1),
   ref_cnt_(0),
   segment_state_(ObVectorIndexSegmentState::DEFAULT),
   scn_(0)
{
}

void ObVectorSegmentInfo::reset()
{
 data_table_id_ = OB_INVALID_ID;
 data_tablet_id_ = OB_INVALID_ID;
 vbitmap_tablet_id_ = OB_INVALID_ID;
 inc_index_tablet_id_ = OB_INVALID_ID;
 snapshot_index_tablet_id_ = OB_INVALID_ID;
 block_count_ = 0;
 segment_type_ = ObVectorIndexSegmentType::INVALID;
 index_type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
 mem_used_ = 0;
 min_vid_ = 0;
 max_vid_ = 0;
 vector_cnt_ = -1;
 ref_cnt_ = 0;
 segment_state_ = ObVectorIndexSegmentState::DEFAULT;
 scn_ = 0;
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
              ef_search_,
              extra_info_max_size_,
              extra_info_actual_size_,
              refine_type_,
              bq_bits_query_,
              refine_k_,
              bq_use_fht_,
              sync_interval_type_,
              sync_interval_value_,
              prune_,
              refine_,
              ob_sparse_drop_ratio_build_,
              window_size_,
              ob_sparse_drop_ratio_search_);
  OB_UNIS_ADD_LEN_ARRAY(endpoint_, OB_MAX_ENDPOINT_LENGTH);
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
              ef_search_,
              extra_info_max_size_,
              extra_info_actual_size_,
              refine_type_,
              bq_bits_query_,
              refine_k_,
              bq_use_fht_,
              sync_interval_type_,
              sync_interval_value_,
              prune_,
              refine_,
              ob_sparse_drop_ratio_build_,
              window_size_,
              ob_sparse_drop_ratio_search_);
  OB_UNIS_ENCODE_ARRAY(endpoint_, OB_MAX_ENDPOINT_LENGTH);
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
              ef_search_,
              extra_info_max_size_,
              extra_info_actual_size_,
              refine_type_,
              bq_bits_query_,
              refine_k_,
              bq_use_fht_,
              sync_interval_type_,
              sync_interval_value_,
              prune_,
              refine_,
              ob_sparse_drop_ratio_build_,
              window_size_,
              ob_sparse_drop_ratio_search_);
  OB_UNIS_DECODE_ARRAY(endpoint_, OB_MAX_ENDPOINT_LENGTH);
  return ret;
}

ObVectorQueryAdaptorResultContext::~ObVectorQueryAdaptorResultContext() {
  status_ = PVQ_START;
  flag_ = PVQP_MAX;
  ObVectorIndexRoaringBitMap::destroy(bitmaps_);
  if (OB_NOT_NULL(pre_filter_)) {
    pre_filter_->reset();
  }
  for (int64_t i = 0; i < segments_.count(); ++i) {
    ObVectorIndexSegmentQuerier* querier = segments_.at(i);
    if (OB_NOT_NULL(querier)) {
      querier->reset();
    }
  }
  segments_.reset();
  if (OB_NOT_NULL(ifilter_)) {
    ifilter_->set_roaring_bitmap(nullptr);
    ifilter_->reset();
    ifilter_ = nullptr;
  }
  if (OB_NOT_NULL(dfilter_)) {
    dfilter_->set_roaring_bitmap(nullptr);
    dfilter_->reset();
    dfilter_ = nullptr;
  }

  batch_allocator_.reset();
  search_allocator_.reset();
};

int ObVectorQueryAdaptorResultContext::init_bitmaps()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx allocator invalid.", K(ret));
  } else if (OB_ISNULL(bitmaps_ = OB_NEWx(ObVectorIndexRoaringBitMap, tmp_allocator_, tmp_allocator_, tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create vbitmap msg", K(ret));
  } else if (OB_FAIL(bitmaps_->init(true, true))) {
    LOG_WARN("init bitmap fail", K(ret));
  }
  return ret;
}

int ObVectorQueryAdaptorResultContext::init_prefilter(const int64_t &min, const int64_t &max)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx allocator invalid.", K(ret));
  } else {
    ObHnswBitmapFilter *vsag_filter = nullptr;
    if (OB_ISNULL(vsag_filter = OB_NEWx(ObHnswBitmapFilter, tmp_allocator_, tenant_id_, ObHnswBitmapFilter::FilterType::BYTE_ARRAY, 0, tmp_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create pre filter", K(ret));
    } else if (OB_FAIL(vsag_filter->init(min, max, is_sparse_vector_))) {
      LOG_WARN("Fail to init pre_filter", K(ret), K(min), K(max));
    } else {
      pre_filter_ = vsag_filter;
    }
  }
  return ret;
}

int ObVectorQueryAdaptorResultContext::init_prefilter(void *adaptor, double selectivity,
                                                      const ObIArray<const ObNewRange *> &range,
                                                      const sql::ExprFixedArray &rowkey_exprs,
                                                      const ObIArray<int64_t> &extra_in_rowkey_idxs)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx allocator invalid.", K(ret));
  } else {
    ObHnswBitmapFilter *vsag_filter = nullptr;
    if (OB_ISNULL(vsag_filter = OB_NEWx(ObHnswBitmapFilter, tmp_allocator_, tenant_id_, ObHnswBitmapFilter::FilterType::BYTE_ARRAY, 0, tmp_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create pre filter", K(ret));
    } else if (OB_FAIL(vsag_filter->init(adaptor, selectivity, range, rowkey_exprs, extra_in_rowkey_idxs))) {
      LOG_WARN("Fail to init pre_filter", K(ret));
    } else {
      pre_filter_ = vsag_filter;
    }
  }
  return ret;
}

bool ObVectorQueryAdaptorResultContext::is_bitmaps_valid()
{
  bool bret = false;
  if (OB_NOT_NULL(bitmaps_)) {
    if (OB_NOT_NULL(bitmaps_->insert_bitmap_) && OB_NOT_NULL(bitmaps_->delete_bitmap_)) {
      bret = true;
    }
  }
  return bret;
}

bool ObVectorQueryAdaptorResultContext::is_prefilter_valid()
{
  bool bret = false;
  if (OB_NOT_NULL(pre_filter_)) {
    bret = pre_filter_->is_valid();
  }
  return bret;
}

bool ObVectorQueryAdaptorResultContext::is_range_prefilter()
{
  bool bret = false;
  if (OB_NOT_NULL(pre_filter_)) {
    bret = pre_filter_->is_valid() && pre_filter_->is_range_filter();
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
  } else if (!is_sparse_vector() && size / sizeof(float) != get_dim()) {
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

int ObVectorQueryAdaptorResultContext::set_extra_info(int64_t index, const ObRowkey &rowkey,
                                                      const ObIArray<int64_t> &extra_in_rowkey_idxs)
{
  INIT_SUCC(ret);
  int64_t extra_column_count = rowkey.get_obj_cnt();
  if (index >= get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid index.", K(ret), K(index), K(get_count()));
  } else if (vec_data_.extra_column_count_ != extra_column_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra column count not match.", K(ret), K(index), K(extra_column_count), K(vec_data_.extra_column_count_));
  } else if (OB_ISNULL(vec_data_.extra_info_objs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info obj is null.", K(ret), K(index), K(vec_data_.extra_info_objs_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_count; ++i) {
    vec_data_.extra_info_objs_[index * extra_column_count + i].reset();
    int64_t in_rowkey_idx = extra_in_rowkey_idxs.at(i);
    if (OB_FAIL(vec_data_.extra_info_objs_[index * extra_column_count + i].from_obj(
            rowkey.get_obj_ptr()[in_rowkey_idx], &batch_allocator_))) {
      LOG_WARN("failed to from obj.", K(ret), K(index), K(i), K(extra_column_count), K(extra_in_rowkey_idxs));
    }
  }

  return ret;
}

int ObVectorQueryAdaptorResultContext::add_segment_querier(
    ObVectorIndexSegmentHandle &handle, const bool is_incr,
    ObHnswBitmapFilter* filter, const bool reverse_filter)
{
  int ret = OB_SUCCESS;
  ObVectorIndexSegmentQuerier* querier = nullptr;
  if (OB_ISNULL(querier = OB_NEWx(ObVectorIndexSegmentQuerier, tmp_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObVsagQueryResult));
  } else if (OB_FAIL(segments_.push_back(querier))) {
    LOG_WARN("push back fail", K(ret));
  } else {
    querier->handle_ = handle;
    querier->is_incr_ = is_incr;
    querier->filter_ = filter;
    querier->reverse_filter_ = reverse_filter;
    if (OB_NOT_NULL(filter) && OB_NOT_NULL(filter->valid_vid_)) {
      querier->valid_vid_ = filter->valid_vid_->get_data();
      querier->valid_vid_count_ = filter->valid_vid_->count();
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(querier)) {
    querier->reset();
  }
  return ret;
}

int64_t ObPluginVectorIndexAdaptor::instacnce_cnt_ = 0;

ObPluginVectorIndexAdaptor::ObPluginVectorIndexAdaptor(common::ObIAllocator *allocator,
                                                       lib::MemoryContext &entity,
                                                       uint64_t tenant_id)
  : create_type_(CreateTypeMax), type_(VIAT_MAX),
    algo_data_(nullptr), incr_data_(), snap_data_(), vbitmap_data_(), tenant_id_(tenant_id),
    snapshot_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    inc_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    vbitmap_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    data_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    rowkey_vid_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    vid_rowkey_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    embedded_tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
    inc_table_id_(OB_INVALID_ID), vbitmap_table_id_(OB_INVALID_ID),
    snapshot_table_id_(OB_INVALID_ID), data_table_id_(OB_INVALID_ID),
    embedded_table_id_(OB_INVALID_ID),
    rowkey_vid_table_id_(OB_INVALID_ID), vid_rowkey_table_id_(OB_INVALID_ID),
    ref_cnt_(0), idle_cnt_(0), mem_check_cnt_(0), is_mem_limited_(false), all_vsag_use_mem_(nullptr), allocator_(allocator),
    parent_mem_ctx_(entity), index_identity_(), follower_sync_statistics_(), is_in_opt_task_(false), need_be_optimized_(false), extra_info_column_count_(0),
    opt_task_lock_(common::ObLatchIds::OB_PLUGIN_VECTOR_INDEX_ADAPTOR_OPT_TASK_LOCK),
    reload_lock_(common::ObLatchIds::VECTOR_RELOAD_LOCK),
    query_lock_(common::ObLatchIds::VECTOR_QUERY_LOCK), reload_finish_(false), last_embedding_time_(ObTimeUtility::fast_current_time()), is_need_vid_(true), sparse_vector_type_(nullptr), replace_scn_()
{
  ATOMIC_INC(&instacnce_cnt_);
}

// 析构函数预期不应该出现并发, 所以不用加锁
ObPluginVectorIndexAdaptor::~ObPluginVectorIndexAdaptor()
{
  int ret = OB_SUCCESS;
  const int64_t instacnce_cnt = ATOMIC_SAF(&instacnce_cnt_, 1);
  FLOG_INFO("[VECTOR INDEX ADAPTOR] destruct adaptor and free resources", K(instacnce_cnt),
      KP(this), K_(create_type), K_(ref_cnt), K(is_complete()), K(lbt()));

  // frozen data的中的segment_handle的met_ctx是incr_data的
  // 所以需要需要在incr_data之前reset
  frozen_data_.reset();
  incr_data_.reset();
  vbitmap_data_.reset();
  snap_data_.reset();

  free_sparse_vector_type_mem();

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
    if(!snapshot_key_prefix_.empty()) {
      allocator_->free(snapshot_key_prefix_.ptr());
      snapshot_key_prefix_.reset();
    }
  }
}

template<typename T>
int ObPluginVectorIndexAdaptor::init_mem(ObVectorIndexMemDataHandle<T> &memdata_handle)
{
  int ret = OB_SUCCESS;
  T* mem_data = nullptr;
  if (memdata_handle.is_valid()) {
    // do nothing
  } else if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_ISNULL(mem_data = OB_NEWx(T, get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem data", K(ret));
  } else if (OB_FAIL(memdata_handle.set_memdata(mem_data, get_allocator(), tenant_id_))) {
    LOG_WARN("handle set memdata fail", K(ret));
  } else {
    mem_data->scn_.set_min();
    LOG_INFO("[VECTOR INDEX ADAPTOR] init memdata success", KP(this), K(memdata_handle));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(mem_data)) {
      mem_data->free_memdata_resource(get_allocator(), tenant_id_);
      get_allocator()->free(mem_data);
      mem_data = nullptr;
    }
  }
  return ret;
}

bool ObPluginVectorIndexAdaptor::is_mem_data_init_atomic(ObVectorIndexRecordType type)
{
  bool bret = false;
  if (type == VIRT_INC) {
    bret = (incr_data_.is_valid() && incr_data_->is_inited());
  } else if (type == VIRT_BITMAP) {
    bret = (vbitmap_data_.is_valid() && vbitmap_data_->is_inited());
  } else if (type == VIRT_SNAP) {
    bret = (snap_data_.is_valid() && snap_data_->is_inited());
  }
  return bret;
}

int ObPluginVectorIndexAdaptor::init(lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem)
{
  INIT_SUCC(ret);
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_FAIL(init_mem(incr_data_))) {
    LOG_WARN("failed to init incr mem data.", K(ret));
  } else if (OB_FAIL(init_mem(vbitmap_data_))) {
    LOG_WARN("failed to init vbitmap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(snap_data_))) {
    LOG_WARN("failed to init snap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(frozen_data_))) {
    LOG_WARN("failed to init frozen mem data.", K(ret));
  } else if (OB_FAIL(init_sparse_vector_type())) {
    LOG_WARN("failed to init sparse vector type", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else {
    parent_mem_ctx_ = parent_mem_ctx;
    all_vsag_use_mem_ = all_vsag_use_mem;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (tenant_config.is_valid()) {
      if (int64_t(tenant_config->ob_vector_index_merge_trigger_percentage) > 0) {
        follower_sync_statistics_.optimze_threashold_ = tenant_config->ob_vector_index_merge_trigger_percentage;
      }
      follower_sync_statistics_.use_new_check_ =
          tenant_config->_persist_vector_index_incremental
          && tenant_data_version >= DATA_VERSION_4_5_1_0;
    }
    LOG_INFO("[VECTOR INDEX ADAPTOR] init adaptor success", KPC(this));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init(ObString init_str, int64_t dim, lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem)
{
  INIT_SUCC(ret);
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_FAIL(init_mem(incr_data_))) {
    LOG_WARN("failed to init incr mem data.", K(ret));
  } else if (OB_FAIL(init_mem(vbitmap_data_))) {
    LOG_WARN("failed to init vbitmap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(snap_data_))) {
    LOG_WARN("failed to init snap mem data.", K(ret));
  } else if (OB_FAIL(init_mem(frozen_data_))) {
    LOG_WARN("failed to init frozen mem data.", K(ret));
  } else if (OB_FAIL(set_param(init_str, dim))){
    LOG_WARN("failed to set param.", K(ret));
  } else if (OB_FAIL(init_sparse_vector_type())) {
    LOG_WARN("failed to init sparse vector type", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else {
    parent_mem_ctx_ = parent_mem_ctx;
    all_vsag_use_mem_ = all_vsag_use_mem;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (tenant_config.is_valid()) {
      if (int64_t(tenant_config->ob_vector_index_merge_trigger_percentage) > 0) {
        follower_sync_statistics_.optimze_threashold_ = tenant_config->ob_vector_index_merge_trigger_percentage;
      }
      follower_sync_statistics_.use_new_check_ =
          tenant_config->_persist_vector_index_incremental
          && tenant_data_version >= DATA_VERSION_4_5_1_0;
    }
    LOG_INFO("[VECTOR INDEX ADAPTOR] init adaptor success", KPC(this));
  }
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
  } else if (OB_FALSE_IT(hnsw_param->reset())) {
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
      case VIAT_HGRAPH:
      case VIAT_HNSW_SQ:
      case VIAT_HNSW_BQ:
      case VIAT_IPIVF:
      case VIAT_IPIVF_SQ: {
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
      type_ == VIAT_HNSW_SQ ||
      type_ == VIAT_HGRAPH ||
      type_ == VIAT_HNSW_BQ ||
      type_ == VIAT_IPIVF ||
      type_ == VIAT_IPIVF_SQ) {
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

int ObPluginVectorIndexAdaptor::get_extra_info_actual_size(int64_t &extra_info_actual_size)
{
  INIT_SUCC(ret);
  if (type_ == VIAT_HNSW ||
     type_ == VIAT_HNSW_SQ ||
     type_ == VIAT_HGRAPH ||
     type_ == VIAT_HNSW_BQ ||
     type_ == VIAT_IPIVF ||
     type_ == VIAT_IPIVF_SQ) {
    ObVectorIndexParam *param = nullptr;
    if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get param.", K(ret));
    } else {
      extra_info_actual_size = param->extra_info_actual_size_;
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
      type_ == VIAT_HNSW_SQ ||
      type_ == VIAT_HNSW_BQ ||
      type_ == VIAT_HGRAPH ||
      type_ == VIAT_IPIVF ||
      type_ == VIAT_IPIVF_SQ) {
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
  info.embedded_tablet_id_ = embedded_tablet_id_.id();
  ObVectorIndexParam *param;
  int64_t pos = 0;
  ObCStringHelper helper;

  #define STAT_PRINT(...) \
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(databuff_printf(info.statistics_, sizeof(info.statistics_), pos, __VA_ARGS__))) { \
        LOG_WARN("failed to fill statistics", K(ret)); \
      } \
    }

  STAT_PRINT("{");
  STAT_PRINT("\"is_complete\":%d", is_complete());
  STAT_PRINT(",\"incr_mem_used\":%lld", get_incr_vsag_mem_used());
  STAT_PRINT(",\"incr_mem_hold\":%lld", get_incr_vsag_mem_hold());
  STAT_PRINT(",\"snap_mem_used\":%lld", get_snap_vsag_mem_used());
  STAT_PRINT(",\"snap_mem_hold\":%lld", get_snap_vsag_mem_hold());

  if (type_ != VIAT_MAX && OB_SUCC(ret)) {
    if (OB_FAIL(get_hnsw_param(param))) {
      LOG_WARN("get hnsw param failed.", K(ret));
    } else {
      info.index_type_ = param->type_;
      STAT_PRINT(",\"param\":\"");
      if (OB_SUCC(ret)) {
        if (OB_FAIL(param->print_to_string(info.statistics_, sizeof(info.statistics_), pos))) {
          LOG_WARN("failed to print param", K(ret));
        }
      }
      STAT_PRINT("\"");
    }
  }
  STAT_PRINT(",\"snap_index_type\":%d", int(get_snap_index_type()));
  STAT_PRINT(",\"incr_index_type\":%d", int(get_incr_index_type()));
  STAT_PRINT(",\"ref_cnt\":%lld", ATOMIC_LOAD(&ref_cnt_) - 1);
  STAT_PRINT(",\"idle_cnt\":%lld", idle_cnt_);

  if (OB_SUCC(ret) && !index_identity_.empty()) {
    STAT_PRINT(",\"index\":\"%s\"", helper.convert(index_identity_));
  }
  if (OB_SUCC(ret) && incr_data_.is_valid()) {
    int64_t incr_cnt = 0;
    if (OB_FAIL(get_inc_index_row_cnt_safe(incr_cnt))) {
      LOG_WARN("failed to get inc index number.", K(ret));
    }
    STAT_PRINT(",\"incr_data_scn\":%llu", incr_data_->scn_.get_val_for_inner_table_field());
    STAT_PRINT(",\"incr_index_cnt\":%lld", incr_cnt);
  }
  if (vbitmap_data_.is_valid()) {
    STAT_PRINT(",\"vbitmap_data_scn\":%llu", vbitmap_data_->scn_.get_val_for_inner_table_field());
    int64_t vbitmap_insert_cnt = 0;
    int64_t vbitmap_delete_cnt = 0;
    if (vbitmap_data_->is_inited()) {
      vbitmap_insert_cnt = roaring64_bitmap_get_cardinality(vbitmap_data_->bitmap_->insert_bitmap_);
      vbitmap_delete_cnt =  roaring64_bitmap_get_cardinality(vbitmap_data_->bitmap_->delete_bitmap_);
    }
    STAT_PRINT(",\"vbitmap_insert_cnt\":%llu", vbitmap_insert_cnt);
    STAT_PRINT(",\"vbitmap_delete_cnt\":%llu", vbitmap_delete_cnt);
  }
  if (OB_SUCC(ret) && snap_data_.is_valid()) {
    int64_t snap_cnt = 0;
    int64_t sbitmap_insert_cnt = 0;
    int64_t sbitmap_delete_cnt = 0;
    if (OB_FAIL(get_snap_index_row_cnt_safe(snap_cnt))) {
      LOG_WARN("failed to get snap index number.", K(ret));
    } else if (OB_FAIL(get_snap_vbitmap_cnt_safe(sbitmap_insert_cnt, sbitmap_delete_cnt))) {
      LOG_WARN("failed to get snap vbitmap number.", K(ret));
    }

    STAT_PRINT(",\"snapshot_key_prefix\":\"%s\"", helper.convert(snapshot_key_prefix_));
    STAT_PRINT(",\"snap_data_scn\":%llu", snap_data_->scn_.get_val_for_inner_table_field());
    STAT_PRINT(",\"snap_index_cnt\":%lld", snap_cnt);
    STAT_PRINT(",\"sbitmap_insert_cnt\":%lld", sbitmap_insert_cnt);
    STAT_PRINT(",\"sbitmap_delete_cnt\":%lld", sbitmap_delete_cnt);

    STAT_PRINT(",\"meta_data\":{");
    {
      STAT_PRINT("\"version\":%d", snap_data_->meta_.header_.version_);
      STAT_PRINT(",\"scn\":%llu", snap_data_->meta_.scn());
      STAT_PRINT(",\"incr_count\":%d", snap_data_->meta_.incr_segment_count());
      STAT_PRINT(",\"base_count\":%d", snap_data_->meta_.base_segment_count());
    }
    STAT_PRINT("}");
  }

  if (OB_SUCC(ret) && has_frozen()) {
    // TODO: may be need read lock
    int64_t frozen_cnt = 0;
    int frozen_type = -1;
    int64_t frozen_mem_used = 0;
    int64_t frozen_mem_hold = 0;
    int64_t fbitmap_insert_cnt = 0;
    int64_t fbitmap_delete_cnt = 0;
    if (frozen_data_->segment_handle_.is_valid()) {
      if (OB_FAIL(frozen_data_->segment_handle_->get_index_number(frozen_cnt))) {
        LOG_WARN("failed to get snap index number.", K(ret));
      } else {
        frozen_type = frozen_data_->segment_handle_->get_index_type();
        frozen_mem_used = frozen_data_->segment_handle_->get_mem_used();
        frozen_mem_hold = frozen_data_->segment_handle_->get_mem_hold();
      }
    }
    if (OB_SUCC(ret) && frozen_data_->vbitmap_->is_inited()
        && OB_NOT_NULL(frozen_data_->vbitmap_->bitmap_)) {
      fbitmap_insert_cnt = roaring64_bitmap_get_cardinality(frozen_data_->vbitmap_->bitmap_->insert_bitmap_);
      fbitmap_delete_cnt =  roaring64_bitmap_get_cardinality(frozen_data_->vbitmap_->bitmap_->delete_bitmap_);
    }

    STAT_PRINT(",\"frozen_data_scn\":%llu", frozen_data_->frozen_scn_.get_val_for_inner_table_field());
    STAT_PRINT(",\"frozen_index_cnt\":%lld", frozen_cnt);
    STAT_PRINT(",\"frozen_index_type\":%d", frozen_type);
    STAT_PRINT(",\"frozen_mem_used\":%lld", frozen_mem_used);
    STAT_PRINT(",\"frozen_mem_hold\":%lld", frozen_mem_hold);
    STAT_PRINT(",\"frozen_state\":%d", frozen_data_->state_);
    STAT_PRINT(",\"frozen_ret_code\":%d", frozen_data_->ret_code_);
    STAT_PRINT(",\"fbitmap_insert_cnt\":%lld", fbitmap_insert_cnt);
    STAT_PRINT(",\"fbitmap_delete_cnt\":%lld", fbitmap_delete_cnt);
  }

  if (nullptr != all_vsag_use_mem_) {
    STAT_PRINT(",\"all_index_mem_used\":%llu", ATOMIC_LOAD(all_vsag_use_mem_));
  }
  if (OB_SUCC(ret)) {
    ObRbMemMgr *mem_mgr = nullptr;
    uint64_t tenant_id = MTL_ID();
    if (OB_ISNULL(mem_mgr = MTL(ObRbMemMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_mgr is null", K(tenant_id));
    } else {
      STAT_PRINT(",\"all_index_bitmap_used\":%lld", mem_mgr->get_vec_idx_used());
    }
  }
  STAT_PRINT("}");
  #undef STAT_PRINT

  #define SYNC_INFO_PRINT(...) \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(databuff_printf(info.sync_info_, sizeof(info.sync_info_), pos, __VA_ARGS__))) { \
      LOG_WARN("failed to fill sync_info", K(ret)); \
    } \
  }
  pos = 0;
  SYNC_INFO_PRINT("{\"incr_cnt\":%lld", follower_sync_statistics_.incr_count_);
  SYNC_INFO_PRINT(",\"vbitmap_cnt\":%lld", follower_sync_statistics_.vbitmap_count_);
  SYNC_INFO_PRINT(",\"snap_cnt\":%lld", follower_sync_statistics_.snap_count_);
  SYNC_INFO_PRINT(",\"sync_total_cnt\":%lld", follower_sync_statistics_.sync_count_);
  SYNC_INFO_PRINT(",\"sync_fail_cnt\":%lld", follower_sync_statistics_.sync_fail_);
  SYNC_INFO_PRINT(",\"last_succ_time\":%lld", follower_sync_statistics_.last_succ_time_);
  SYNC_INFO_PRINT(",\"last_fail_time\":%lld", follower_sync_statistics_.last_fail_time_);
  SYNC_INFO_PRINT(",\"last_fail_code\":%d", follower_sync_statistics_.last_fail_code_);
  SYNC_INFO_PRINT("}");

  #undef SYNC_INFO_PRINT
  return ret;
}

int ObPluginVectorIndexAdaptor::fill_mem_context_detail_info(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (incr_data_.is_valid() && incr_data_->is_inited()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"vsag_incr_%lu\":%ld", get_inc_table_id(), get_incr_vsag_mem_hold()))) {
      OB_LOG(WARN, "failed to get vsag incr data mem info", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (snap_data_.is_valid() && snap_data_->is_inited()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"vsag_snap_%lu\":%ld", get_snapshot_table_id(), get_snap_vsag_mem_hold()))) {
      OB_LOG(WARN, "failed to get vsag snap data mem info", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::fill_vector_index_all_segments(common::ObIArray<ObVectorSegmentInfo> &segment_infos)
{
  int ret = OB_SUCCESS;
  segment_infos.reset();

  // Fill common fields for all segments
  int64_t data_table_id = data_table_id_;
  int64_t data_tablet_id = data_tablet_id_.id();
  int64_t vbitmap_tablet_id = vbitmap_tablet_id_.id();
  int64_t inc_index_tablet_id = inc_tablet_id_.id();
  int64_t snapshot_index_tablet_id = snapshot_tablet_id_.id();

  // Fill active segment (incr_data_)
  if (incr_data_.is_valid() && incr_data_->is_inited()) {
    TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    ObVectorSegmentInfo seg_info;
    seg_info.reset();
    seg_info.data_table_id_ = data_table_id;
    seg_info.data_tablet_id_ = data_tablet_id;
    seg_info.vbitmap_tablet_id_ = vbitmap_tablet_id;
    seg_info.inc_index_tablet_id_ = inc_index_tablet_id;
    seg_info.snapshot_index_tablet_id_ = snapshot_index_tablet_id;
    seg_info.block_count_ = 0; // Active segment not persisted yet
    seg_info.segment_type_ = ObVectorIndexSegmentType::IN_MEMORY;
    seg_info.index_type_ = get_incr_index_type();
    seg_info.segment_state_ = ObVectorIndexSegmentState::ACTIVE;
    seg_info.scn_ = incr_data_->scn_.get_val_for_inner_table_field(); // TODO: consider either use now or zero

    if (incr_data_->segment_handle_.is_valid()) {
      int64_t vec_cnt = 0;
      int64_t min_vid = 0;
      int64_t max_vid = 0;
      if (OB_FAIL(incr_data_->segment_handle_->get_index_number(vec_cnt))) {
        LOG_WARN("failed to get incr index number", K(ret));
      } else if (OB_FAIL(incr_data_->segment_handle_->get_vid_bound(min_vid, max_vid))) {
        LOG_WARN("failed to get incr vid bound", K(ret));
      } else {
        seg_info.vector_cnt_ = vec_cnt;
        seg_info.min_vid_ = min_vid;
        seg_info.max_vid_ = max_vid;
        seg_info.mem_used_ = incr_data_->segment_handle_->get_mem_used();
        seg_info.ref_cnt_ = incr_data_->segment_handle_->get_ref();
      }
    } else {
      seg_info.vector_cnt_ = -1;
      seg_info.mem_used_ = 0;
      seg_info.ref_cnt_ = 0;
      seg_info.min_vid_ = 0;
      seg_info.max_vid_ = 0;
    }

    if (OB_SUCC(ret) && OB_FAIL(segment_infos.push_back(seg_info))) {
      LOG_WARN("failed to push back active segment info", K(ret));
    }
  }

  // Fill frozen segment (frozen_data_)
  if (OB_SUCC(ret) && has_frozen() && frozen_data_->segment_handle_.is_valid()) {
    TCRLockGuard lock_guard(frozen_data_->mem_data_rwlock_);
    ObVectorSegmentInfo seg_info;
    seg_info.reset();
    seg_info.data_table_id_ = data_table_id;
    seg_info.data_tablet_id_ = data_tablet_id;
    seg_info.vbitmap_tablet_id_ = vbitmap_tablet_id;
    seg_info.inc_index_tablet_id_ = inc_index_tablet_id;
    seg_info.snapshot_index_tablet_id_ = snapshot_index_tablet_id;
    seg_info.block_count_ = 0; // Frozen segment not persisted yet
    seg_info.segment_type_ = ObVectorIndexSegmentType::FREEZE_PERSIST;
    seg_info.index_type_ = frozen_data_->segment_handle_->get_index_type();
    seg_info.segment_state_ = ObVectorIndexSegmentState::FROZEN; // TODO: consider fill info here or set in metadata
    seg_info.scn_ = frozen_data_->frozen_scn_.get_val_for_inner_table_field();

    int64_t vec_cnt = 0;
    int64_t min_vid = 0;
    int64_t max_vid = 0;
    if (OB_FAIL(frozen_data_->segment_handle_->get_index_number(vec_cnt))) {
      LOG_WARN("failed to get frozen index number", K(ret));
    } else if (OB_FAIL(frozen_data_->segment_handle_->get_vid_bound(min_vid, max_vid))) {
      LOG_WARN("failed to get frozen vid bound", K(ret));
    } else {
      seg_info.vector_cnt_ = vec_cnt;
      seg_info.min_vid_ = min_vid;
      seg_info.max_vid_ = max_vid;
      seg_info.mem_used_ = frozen_data_->segment_handle_->get_mem_used();
      seg_info.ref_cnt_ = frozen_data_->segment_handle_->get_ref();
    }

    if (OB_SUCC(ret) && OB_FAIL(segment_infos.push_back(seg_info))) {
      LOG_WARN("failed to push back frozen segment info", K(ret));
    }
  }

  // Fill snapshot segments (incrs_ and bases_ in snap_data_)
  if (OB_SUCC(ret) && snap_data_.is_valid() && snap_data_->is_inited()) {
    TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    // Fill incrs segments
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
      const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
      ObVectorSegmentInfo seg_info;
      seg_info.reset();

      seg_info.data_table_id_ = data_table_id;
      seg_info.data_tablet_id_ = data_tablet_id;
      seg_info.vbitmap_tablet_id_ = vbitmap_tablet_id;
      seg_info.inc_index_tablet_id_ = inc_index_tablet_id;
      seg_info.snapshot_index_tablet_id_ = snapshot_index_tablet_id;

      seg_info.segment_type_ = seg_meta.seg_type_;
      seg_info.index_type_ = seg_meta.index_type_;
      seg_info.scn_ = seg_meta.scn_;
      seg_info.block_count_ = seg_meta.blocks_cnt_;
      seg_info.segment_state_ = ObVectorIndexSegmentState::PERSISTED;

      // Fill segment handle information
      if (seg_meta.segment_handle_.is_valid()) {
        int64_t vec_cnt = 0;
        int64_t min_vid = 0;
        int64_t max_vid = 0;
        if (OB_FAIL(seg_meta.segment_handle_->get_index_number(vec_cnt))) {
          LOG_WARN("failed to get segment index number", K(ret), K(i));
        } else if (OB_FAIL(seg_meta.segment_handle_->get_vid_bound(min_vid, max_vid))) {
          LOG_WARN("failed to get segment vid bound", K(ret), K(i));
        } else {
          seg_info.vector_cnt_ = vec_cnt;
          seg_info.min_vid_ = min_vid;
          seg_info.max_vid_ = max_vid;
          seg_info.mem_used_ = seg_meta.segment_handle_->get_mem_used();
          seg_info.ref_cnt_ = seg_meta.segment_handle_->get_ref();
        }
      } else {
        seg_info.vector_cnt_ = -1;
        seg_info.mem_used_ = 0;
        seg_info.ref_cnt_ = 0;
        seg_info.min_vid_ = 0;
        seg_info.max_vid_ = 0;
      }

      if (OB_SUCC(ret) && OB_FAIL(segment_infos.push_back(seg_info))) {
        LOG_WARN("failed to push back incr segment info", K(ret), K(i));
      }
    }

    // Fill bases segments
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.bases_.count(); ++i) {
      const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.bases_.at(i);
      ObVectorSegmentInfo seg_info;
      seg_info.reset();

      seg_info.data_table_id_ = data_table_id;
      seg_info.data_tablet_id_ = data_tablet_id;
      seg_info.vbitmap_tablet_id_ = vbitmap_tablet_id;
      seg_info.inc_index_tablet_id_ = inc_index_tablet_id;
      seg_info.snapshot_index_tablet_id_ = snapshot_index_tablet_id;

      seg_info.segment_type_ = seg_meta.seg_type_;
      seg_info.index_type_ = seg_meta.index_type_;
      seg_info.scn_ = seg_meta.scn_;
      seg_info.block_count_ = seg_meta.blocks_cnt_;
      seg_info.segment_state_ = ObVectorIndexSegmentState::PERSISTED;

      // Fill segment handle information
      if (seg_meta.segment_handle_.is_valid()) {
        int64_t vec_cnt = 0;
        int64_t min_vid = 0;
        int64_t max_vid = 0;
        if (OB_FAIL(seg_meta.segment_handle_->get_index_number(vec_cnt))) {
          LOG_WARN("failed to get segment index number", K(ret), K(i));
        } else if (OB_FAIL(seg_meta.segment_handle_->get_vid_bound(min_vid, max_vid))) {
          LOG_WARN("failed to get segment vid bound", K(ret), K(i));
        } else {
          seg_info.vector_cnt_ = vec_cnt;
          seg_info.min_vid_ = min_vid;
          seg_info.max_vid_ = max_vid;
          seg_info.mem_used_ = seg_meta.segment_handle_->get_mem_used();
          seg_info.ref_cnt_ = seg_meta.segment_handle_->get_ref();
        }
      } else {
        seg_info.vector_cnt_ = -1;
        seg_info.mem_used_ = 0;
        seg_info.ref_cnt_ = 0;
        seg_info.min_vid_ = 0;
        seg_info.max_vid_ = 0;
      }

      if (OB_SUCC(ret) && OB_FAIL(segment_infos.push_back(seg_info))) {
        LOG_WARN("failed to push back base segment info", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::init_mem_data(ObVectorIndexRecordType type, ObVectorIndexAlgorithmType enforce_type)
{
  INIT_SUCC(ret);
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (type == VIRT_INC) {
    TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (OB_FAIL(init_incr_data(incr_data_, enforce_type, param))) {
      LOG_WARN("failed to init incr data", K(ret));
    }
  } else if (type == VIRT_BITMAP) {
    TCWLockGuard lock_guard(vbitmap_data_->bitmap_rwlock_);
    if (OB_FAIL(init_vbitmap_data(vbitmap_data_))) {
      LOG_WARN("failed to init vbitmap data", K(ret));
    }
  } else if (type == VIRT_SNAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("snap is can not init by this function", K(ret));
  }
  return ret;
}

// Each segment needs to have its own mem ctx that's created by this function
int ObPluginVectorIndexAdaptor::create_mem_ctx(ObVsagMemContext *&mem_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(mem_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem ctx is not null, can not create again", K(ret), KP(mem_ctx));
  } else if (OB_ISNULL(mem_ctx = OB_NEWx(ObVsagMemContext, get_allocator(), all_vsag_use_mem_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to aloc mem_ctx", K(ret), "size", sizeof(ObVsagMemContext));
  } else if (OB_FAIL(mem_ctx->init(parent_mem_ctx_, all_vsag_use_mem_, tenant_id_))) {
    LOG_WARN("failed to init mem ctx.", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(mem_ctx)) {
    mem_ctx->~ObVsagMemContext();
    get_allocator()->free(mem_ctx);
    mem_ctx = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::create_snap_segment(const ObVectorIndexAlgorithmType enforce_type, ObVectorIndexSegmentHandle &segment_handle)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else {
    ObVectorIndexAlgorithmType build_type = enforce_type == VIAT_MAX ? param->type_ : enforce_type;
    int64_t max_degree = param->type_ == VIAT_HNSW_SQ ? ObVectorIndexUtil::get_hnswsq_type_metric(param->m_) : param->m_;
    if (OB_FAIL(ObVectorIndexSegment::create(
        segment_handle,
        tenant_id_,
        *get_allocator(),
        *param,
        build_type,
        max_degree,
        this))) {
      LOG_WARN("failed to create sparse vsag index.", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::create_snap_segment(const ObVectorIndexAlgorithmType enforce_type, ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (enforce_type != seg_meta.index_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index type is not match", K(ret), K(enforce_type), K(seg_meta));
  } else if (OB_FAIL(create_snap_segment(enforce_type, seg_meta.segment_handle_))) {
    LOG_WARN("failed to create segment", K(ret), K(enforce_type), K(seg_meta));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_snap_data_without_lock(ObVectorIndexAlgorithmType enforce_type)
{
  INIT_SUCC(ret);
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (snap_data_->is_inited() && ! snap_data_->rb_flag_) {
    LOG_TRACE("snap is inited so skip", K(lbt()));
  } else if (snap_data_->meta_.bases_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, snap data bases count is greater than 1", K(ret), KPC(this));
  } else if (snap_data_->meta_.bases_.count() == 1) {
    ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.bases_.at(0);
    if (seg_meta.segment_handle_.is_valid()) {
      LOG_INFO("snap data base segment is valid, so skip", K(lbt()));
    } else if (OB_FAIL(create_snap_segment(seg_meta.index_type_, seg_meta.segment_handle_))) {
      LOG_WARN("failed to create segment", K(ret), K(seg_meta));
    } else {
      snap_data_->meta_.is_persistent_ = false;
      snap_data_->rb_flag_ = true;
      LOG_INFO("load snap data success.", K(ret), K(snap_data_), K(lbt()));
    }
  } else {
    ObVectorIndexSegmentMeta seg_meta;
    if (OB_FAIL(create_snap_segment(enforce_type, seg_meta.segment_handle_))) {
      LOG_WARN("failed to create segment", K(ret), K(enforce_type));
    } else if (OB_FAIL(snap_data_->meta_.bases_.push_back(seg_meta))) {
      LOG_WARN("push back fail", K(ret));
    } else {
      snap_data_->meta_.is_persistent_ = false;
      snap_data_->set_inited();
      LOG_INFO("create snap data success.", K(ret), K(snap_data_), K(lbt()));
    }
  }
  return ret;
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
  } else if (type == VIRT_EMBEDDED) {
    if (!is_embedded_tablet_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect insert inc index but table id invalid.", K(ret));
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

bool ObPluginVectorIndexAdaptor::is_hybrid_index()
{
  ObVectorIndexParam *param = static_cast<ObVectorIndexParam*>(algo_data_);
  return OB_NOT_NULL(param) && strlen(param->endpoint_) > 0;
}

bool ObPluginVectorIndexAdaptor::check_need_embedding()
{
  bool bret = false;
  const int64_t DEFAULT_INTERVAL = 10 * 1000 * 1000; // 10s
  ObVectorIndexParam *param = static_cast<ObVectorIndexParam*>(algo_data_);
  if (OB_NOT_NULL(param)) {
    if (param->sync_interval_type_ == ObVectorIndexSyncIntervalType::VSIT_IMMEDIATE) {
      bret = ObTimeUtility::fast_current_time() - last_embedding_time_ > DEFAULT_INTERVAL;
    } else if (param->sync_interval_type_ == ObVectorIndexSyncIntervalType::VSIT_NUMERIC) {
      bret = ObTimeUtility::fast_current_time() - last_embedding_time_ > param->sync_interval_value_ * 1000 * 1000;
    }
  }
  if (bret) {
    last_embedding_time_ = ObTimeUtility::fast_current_time();
  }
  return bret;
}

bool ObPluginVectorIndexAdaptor::is_sync_index()
{
  ObVectorIndexParam *param = static_cast<ObVectorIndexParam*>(algo_data_);
  return OB_NOT_NULL(param) && param->sync_interval_type_ == ObVectorIndexSyncIntervalType::VSIT_IMMEDIATE;
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

void ObPluginVectorIndexAdaptor::update_can_skip(ObCanSkip3rdAnd4thVecIndex can_skip)
{
  incr_data_->can_skip_ = can_skip;
}

ObCanSkip3rdAnd4thVecIndex ObPluginVectorIndexAdaptor::get_can_skip()
{
  return incr_data_->can_skip_;
}

int ObPluginVectorIndexAdaptor::handle_insert_incr_table_rows(blocksstable::ObDatumRow *rows,
                                                              const int64_t vid_idx,
                                                              const int64_t type_idx,
                                                              int64_t row_count)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get rows null.", K(ret));
  } else if (OB_FAIL(check_tablet_valid(VIRT_INC))) {
    LOG_WARN("check tablet id invalid.", K(ret));
  } else if (row_count <= 0) {
    // do nothing
  } else {
    uint64_t del_vid_count = 0;
    uint64_t *del_vids = nullptr;
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    if (OB_ISNULL(del_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < row_count; i++) {
      ObDatum &vid_datum = rows[i].storage_datums_[vid_idx];
      ObDatum &op_datum = rows[i].storage_datums_[type_idx];
      int64_t vid = vid_datum.get_int();
      ObString op_str = op_datum.get_string();
      if (op_str.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
        // D type, only record vid
        del_vids[del_vid_count++] = vid;
      }
    }

    ObVectorIndexSegmentWriteGuard write_guard;
    if (OB_SUCC(ret) && OB_FAIL(write_guard.prepare_write(incr_data_))) {
      LOG_WARN("prepare write fail", K(ret), K(incr_data_));
    }
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPH"));
      ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
      TCWLockGuard lock_guard(segment_handle->ibitmap_->rwlock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < del_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(incr_data_->segment_handle_->ibitmap_->insert_bitmap_, del_vids[i]));
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::handle_insert_embedded_table_rows(blocksstable::ObDatumRow *rows,
                                                                 const int64_t vid_idx,
                                                                 const int64_t vector_idx,
                                                                 const ObIArray<ObExtraIdxType>& extra_info_id_types,
                                                                 int64_t row_count)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  int64_t extra_info_actual_size = 0;
  int64_t extra_info_column_count = extra_info_id_types.count();
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_ISNULL(rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get rows null.", K(ret));
  } else if (OB_FAIL(check_tablet_valid(VIRT_EMBEDDED))) {
    LOG_WARN("check tablet id invalid.", K(ret));
  } else if (OB_FAIL(try_init_mem_data(VIRT_INC))) {
    LOG_WARN("failed to init incr index.", K(ret));
  } else if (row_count <= 0) {
    // do nothing
  } else if (OB_FAIL(get_dim(dim))) {
    LOG_WARN("get dim failed.", K(ret));
  } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("get extra_info actual size failed.", K(ret));
  } else if ((extra_info_actual_size > 0 && extra_info_id_types.count() == 0) || (extra_info_actual_size == 0 && extra_info_id_types.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info type count not match.", K(extra_info_actual_size), K(extra_info_id_types.count()), K(ret));
  } else {
    uint64_t incr_vid_count = 0;
    uint64_t null_vid_count = 0;
    int64_t *incr_vids = nullptr;
    uint64_t *null_vids = nullptr;
    float *vectors = nullptr;
    ObVidBound vid_bound = ObVidBound();
    ObVecExtraInfoObj *extra_objs = nullptr;

    if (OB_ISNULL(incr_vids = static_cast<int64_t *>(tmp_allocator.alloc(sizeof(int64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc incr vids.", K(ret));
    } else if (OB_ISNULL(null_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    } else if (OB_ISNULL(vectors = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * row_count * dim)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vectors.", K(ret));
    } else if (extra_info_id_types.count() > 0) {
      char *extra_obj_buf = nullptr;
      if (OB_ISNULL(extra_obj_buf = static_cast<char *>(
                        tmp_allocator.alloc(sizeof(ObVecExtraInfoObj) * row_count * extra_info_column_count)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc extra info.", K(ret));
      } else if (OB_FALSE_IT(extra_objs = new (extra_obj_buf) ObVecExtraInfoObj[row_count * extra_info_column_count])) {
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < row_count; i++) {
      float *vector = nullptr;
      ObDatum &vid_datum = rows[i].storage_datums_[vid_idx];
      ObDatum &vector_datum = rows[i].storage_datums_[vector_idx];
      int64_t vid = vid_datum.get_int();
      ObString vector_str = vector_datum.get_string();

      if (vector_datum.len_ == 0) {
        null_vids[null_vid_count++] = vid;
      } else if (vector_datum.len_ / sizeof(float) != dim) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get vector objct unexpect.", K(ret), K(vector_datum));
      } else if (OB_ISNULL(vector = reinterpret_cast<float *>(vector_str.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast vectors.", K(ret));
      } else {
        if (extra_info_id_types.count() > 0) {
          for (int extra_idx = 0; OB_SUCC(ret) && extra_idx < extra_info_column_count; extra_idx++) {
            ObDatum &extra_datum = rows[i].storage_datums_[extra_info_id_types.at(extra_idx).idx_];
            if (OB_FAIL(extra_objs[incr_vid_count * extra_info_column_count + extra_idx].from_datum(extra_datum, extra_info_id_types.at(extra_idx).type_))) {
              LOG_WARN("failed to from obj.", K(ret), K(extra_datum), K(incr_vid_count), K(extra_info_column_count), K(extra_idx));
            }
          }
        }
        if (OB_SUCC(ret)) {
          for (int j = 0; j < dim; j++) {
            vectors[incr_vid_count * dim + j] = vector[j];
          }
          incr_vids[incr_vid_count++] = vid;
          vid_bound.set_vid(vid);
        }
      }
    }
    char *extra_info_buf_ptr = nullptr;
    if (OB_SUCC(ret) && OB_NOT_NULL(extra_objs) && incr_vid_count > 0 && extra_info_column_count > 0) {
      if (OB_FAIL(ObVecExtraInfo::extra_infos_to_buf(tmp_allocator, extra_objs, extra_info_column_count,
                                                     extra_info_actual_size, incr_vid_count, extra_info_buf_ptr))) {
        LOG_WARN("failed to encode extra info buffer.", K(ret));
      }
    }
    ObVectorIndexSegmentWriteGuard write_guard;
    if (OB_SUCC(ret) && OB_FAIL(write_guard.prepare_write(incr_data_))) {
      LOG_WARN("prepare write fail", K(ret), K(incr_data_));
    }
    if (OB_SUCC(ret) && incr_vid_count > 0) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      lib::ObLightBacktraceGuard light_backtrace_guard(false);
      ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
      if (OB_FAIL(segment_handle->add_index(
                                              vectors,
                                              incr_vids,
                                              dim,
                                              extra_info_buf_ptr,
                                              incr_vid_count))) {
        LOG_WARN("failed to add index.", K(ret), K(dim), K(row_count));
      }
    }
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPH"));
      ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
      TCWLockGuard lock_guard(segment_handle->ibitmap_->rwlock_);
      segment_handle->set_read_vid_bound(vid_bound);
      for (int64_t i = 0; OB_SUCC(ret) && i < incr_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(incr_data_->segment_handle_->ibitmap_->insert_bitmap_, incr_vids[i]));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < null_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(incr_data_->segment_handle_->ibitmap_->insert_bitmap_, null_vids[i]));
      }
    }

  }
  return ret;
}

void ObPluginVectorIndexAdaptor::free_sparse_vector_type_mem()
{
  if (OB_NOT_NULL(allocator_)) {
    if (sparse_vector_type_) {
      if (sparse_vector_type_->key_type_) {
        ObCollectionArrayType *key_type = (ObCollectionArrayType *)sparse_vector_type_->key_type_;
        if (key_type->element_type_) {
          allocator_->free(key_type->element_type_);
        }
        allocator_->free(sparse_vector_type_->key_type_);
      }
      if(sparse_vector_type_->value_type_) {
        ObCollectionArrayType *vector_type = (ObCollectionArrayType *)sparse_vector_type_->value_type_;
        if(vector_type->element_type_) {
          allocator_->free(vector_type->element_type_);
        }
        allocator_->free(sparse_vector_type_->value_type_);
      }
      allocator_->free(sparse_vector_type_);
      sparse_vector_type_ = nullptr;
    }
  }
}

int ObPluginVectorIndexAdaptor::init_sparse_vector_type()
{
  int ret = OB_SUCCESS;
  ObCollectionArrayType *key_array_type = nullptr;
  ObCollectionBasicType *key_elem_type = nullptr;

  void *key_elem_buf = allocator_->alloc(sizeof(ObCollectionBasicType));
  void *key_array_buf = allocator_->alloc(sizeof(ObCollectionArrayType));

  if (OB_ISNULL(sparse_vector_type_ = OB_NEWx(ObCollectionMapType, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc sparse vector type", K(ret));
  } else if (OB_ISNULL(key_elem_buf) || OB_ISNULL(key_array_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for key type", K(ret));
  } else {
    key_elem_type = new (key_elem_buf) ObCollectionBasicType();
    key_elem_type->type_id_ = ObNestedType::OB_BASIC_TYPE;
    key_elem_type->basic_meta_.meta_.set_uint32();
    key_array_type = new (key_array_buf) ObCollectionArrayType(*allocator_);
    key_array_type->type_id_ = ObNestedType::OB_ARRAY_TYPE;
    key_array_type->element_type_ = key_elem_type;
    ObCollectionArrayType *value_array_type = nullptr;
    ObCollectionBasicType *value_elem_type = nullptr;

    void *value_elem_buf = allocator_->alloc(sizeof(ObCollectionBasicType));
    void *value_array_buf = allocator_->alloc(sizeof(ObCollectionArrayType));

    if (OB_ISNULL(value_elem_buf) || OB_ISNULL(value_array_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for value type", K(ret));
    } else {
      value_elem_type = new (value_elem_buf) ObCollectionBasicType();
      value_elem_type->type_id_ = ObNestedType::OB_BASIC_TYPE;
      value_elem_type->basic_meta_.meta_.set_float();
      value_array_type = new (value_array_buf) ObCollectionArrayType(*allocator_);
      value_array_type->type_id_ = ObNestedType::OB_ARRAY_TYPE;
      value_array_type->element_type_ = value_elem_type;
      sparse_vector_type_->type_id_ = ObNestedType::OB_SPARSE_VECTOR_TYPE;
      sparse_vector_type_->key_type_ = key_array_type;
      sparse_vector_type_->value_type_ = value_array_type;
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::insert_rows(blocksstable::ObDatumRow *rows,
                                            const int64_t vid_idx,
                                            const int64_t type_idx,
                                            const int64_t vector_idx,
                                            const ObIArray<ObExtraIdxType>& extra_info_id_types,
                                            int64_t row_count)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  int64_t extra_info_actual_size = 0;
  int64_t extra_info_column_count = extra_info_id_types.count();
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObVectorIndexParam *param = nullptr;
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
  } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("get extra_info actual size failed.", K(ret));
  } else if ((extra_info_actual_size > 0 && extra_info_id_types.count() == 0) || (extra_info_actual_size == 0 && extra_info_id_types.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info type count not match.", K(extra_info_actual_size), K(extra_info_id_types.count()), K(ret));
  } else if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get param.", K(ret));
  } else {
    uint64_t incr_vid_count = 0;
    uint64_t del_vid_count = 0;
    uint64_t null_vid_count = 0;
    int64_t *incr_vids = nullptr;
    uint64_t *del_vids = nullptr;
    uint64_t *null_vids = nullptr;
    float *vectors = nullptr;
    uint32_t *lens = nullptr;
    uint32_t *dims = nullptr;
    float *vals = nullptr;
    ObVidBound vid_bound = ObVidBound();
    ObVecExtraInfoObj *extra_objs = nullptr;

    if (OB_ISNULL(incr_vids = static_cast<int64_t *>(tmp_allocator.alloc(sizeof(int64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc incr vids.", K(ret));
    } else if (OB_ISNULL(del_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    } else if (OB_ISNULL(null_vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * row_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc del vids.", K(ret));
    } else if (!is_sparse_vector_index_type() && OB_ISNULL(vectors = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * row_count * dim)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vectors.", K(ret));
    } else if (extra_info_id_types.count() > 0) {
      char *extra_obj_buf = nullptr;
      if (OB_ISNULL(extra_obj_buf = static_cast<char *>(
                        tmp_allocator.alloc(sizeof(ObVecExtraInfoObj) * row_count * extra_info_column_count)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc extra info.", K(ret));
      } else if (OB_FALSE_IT(extra_objs = new (extra_obj_buf) ObVecExtraInfoObj[row_count * extra_info_column_count])) {
      }
    } else if (is_sparse_vector_index_type()) {
      uint32_t total_length = 0;
      uint32_t sparse_count = 0;
      for(int i = 0; OB_SUCC(ret) && i < row_count; i++) {
        ObDatum &vector_datum = rows[i].storage_datums_[vector_idx];
        ObDatum &op_datum = rows[i].storage_datums_[type_idx];
        if (op_datum.get_string().ptr()[0] != sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0] && vector_datum.len_ != 0) {
          ObString vec_str = vector_datum.get_string();
          uint32_t length = *(uint32_t*)(vec_str.ptr());
          total_length += length;
          sparse_count++;
        }
      }
      // for null sparse vector
      if (total_length == 0) {
        total_length = 1;
      }
      if (OB_FAIL(ret)) {
      } else if (sparse_count == 0) { // delete op
      } else if (OB_ISNULL(lens = static_cast<uint32_t *>(tmp_allocator.alloc(sizeof(uint32_t) * sparse_count)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc lens.", K(ret), K(sparse_count));
      } else if (OB_ISNULL(dims = static_cast<uint32_t *>(tmp_allocator.alloc(sizeof(uint32_t) * total_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc dims.", K(ret), K(total_length));
      } else if (OB_ISNULL(vals = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * total_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc vals.", K(ret), K(total_length));
      }
    }
    uint32_t curr_pos = 0;
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
      } else if (!is_sparse_vector_index_type() && vector_datum.len_ / sizeof(float) != dim) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get vector objct unexpect.", K(ret), K(vector_datum));
      } else if (FALSE_IT(vector_str = vector_datum.get_string())) {
        LOG_WARN("failed to get vector string.", K(ret));
      } else if (OB_ISNULL(vector = reinterpret_cast<float *>(vector_str.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast vectors.", K(ret));
      } else {
        if (extra_info_id_types.count() > 0) {
          for (int extra_idx = 0; OB_SUCC(ret) && extra_idx < extra_info_column_count; extra_idx++) {
            ObDatum &extra_datum = rows[i].storage_datums_[extra_info_id_types.at(extra_idx).idx_];
            if (OB_FAIL(extra_objs[incr_vid_count * extra_info_column_count + extra_idx].from_datum(extra_datum, extra_info_id_types.at(extra_idx).type_))) {
              LOG_WARN("failed to from obj.", K(ret), K(extra_datum), K(incr_vid_count), K(extra_info_column_count), K(extra_idx));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (is_sparse_vector_index_type()) { // parse sparse vector
            ObIArrayType *arr = nullptr;
            if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *sparse_vector_type_, arr, true))) {
              LOG_WARN("failed to construct sparse vector using factory", K(ret));
            } else if (OB_NOT_NULL(arr) && OB_FAIL(arr->init(vector_str))) {
              LOG_WARN("failed to init sparse vector with raw data", K(ret));
            }
            ObMapType *qvec = static_cast<ObMapType*>(arr);
            ObArrayFixedSize<uint32_t> *keys_arr = dynamic_cast<ObArrayFixedSize<uint32_t> *>(qvec->get_key_array());
            ObArrayFixedSize<float> *values_arr = dynamic_cast<ObArrayFixedSize<float> *>(qvec->get_value_array());
            if (OB_ISNULL(keys_arr) || OB_ISNULL(values_arr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to cast key", K(ret));
            } else {
              uint32_t *keys = reinterpret_cast<uint32_t *>(keys_arr->get_data());
              float *values = reinterpret_cast<float *>(values_arr->get_data());
              uint32_t length = *(uint32_t *)(vector_str.ptr());
              lens[incr_vid_count] = length;
              MEMCPY(dims + curr_pos, keys, length * sizeof(uint32_t));
              MEMCPY(vals + curr_pos, values, length * sizeof(float));
              curr_pos += length;
            }
          } else {
            for (int j = 0; j < dim; j++) {
              vectors[incr_vid_count * dim + j] = vector[j];
            }
          }
          incr_vids[incr_vid_count++] = vid;
          vid_bound.set_vid(vid);
        }
      }
    }
    char *extra_info_buf_ptr = nullptr;
    if (OB_SUCC(ret) && OB_NOT_NULL(extra_objs) && incr_vid_count > 0 && extra_info_column_count > 0) {
      if (OB_FAIL(ObVecExtraInfo::extra_infos_to_buf(tmp_allocator, extra_objs, extra_info_column_count,
                                                     extra_info_actual_size, incr_vid_count, extra_info_buf_ptr))) {
        LOG_WARN("failed to encode extra info buffer.", K(ret));
      }
    }
    ObVectorIndexSegmentWriteGuard write_guard;
    if (OB_SUCC(ret) && OB_FAIL(write_guard.prepare_write(incr_data_))) {
      LOG_WARN("prepare write fail", K(ret), K(incr_data_));
    }
    if (OB_SUCC(ret) && incr_vid_count > 0) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      lib::ObLightBacktraceGuard light_backtrace_guard(false);
      ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
      if (is_sparse_vector_index_type()) {
        if (OB_FAIL(segment_handle->add_index(
                                              lens,
                                              dims,
                                              vals,
                                              incr_vids,
                                              incr_vid_count,
                                              extra_info_buf_ptr
                                              ))) {
          LOG_WARN("failed to add sparse index.", K(ret), K(dim), K(row_count));
        }
      } else {
        if (OB_FAIL(segment_handle->add_index(
                                              vectors,
                                              incr_vids,
                                              dim,
                                              extra_info_buf_ptr,
                                              incr_vid_count))) {
          LOG_WARN("failed to add index.", K(ret), K(dim), K(row_count));
        }
      }
    }
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPH"));
      ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
      TCWLockGuard lock_guard(segment_handle->ibitmap_->rwlock_);
      segment_handle->set_read_vid_bound(vid_bound);
      for (int64_t i = 0; OB_SUCC(ret) && i < incr_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(segment_handle->ibitmap_->insert_bitmap_, incr_vids[i]));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < del_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(segment_handle->ibitmap_->insert_bitmap_, del_vids[i]));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < null_vid_count; i++) {
        ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(segment_handle->ibitmap_->insert_bitmap_, null_vids[i]));
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
    ret = ctx->pre_filter_->add(vid);
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::add_extra_valid_vid_without_malloc_guard(
    ObVectorQueryAdaptorResultContext *ctx,
    int64_t vid)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx invalid.", K(ret));
  } else {
    ret = ctx->pre_filter_->add(vid);
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::parse_sparse_vector(char *data, int num, uint32_t *sparse_byte_lens, ObArenaAllocator *allocator, uint32_t **lens,
    uint32_t **dims, float **vals)
{
  int ret = OB_SUCCESS;
  char *data_ptr = (char *)data;
  uint32_t total_length = 0;

  for (int i = 0; OB_SUCC(ret) && i < num; i++) {
    if (sparse_byte_lens[i] > 0) {
      ObString data_str(sparse_byte_lens[i], (char *)data_ptr);
      uint32_t length = *(uint32_t *)(data_str.ptr());
      total_length += length;
    }
    data_ptr += sparse_byte_lens[i];
  }
  // alloc memory for null sparse vector
  if (total_length == 0) {
    total_length = 1;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else {
    uint32_t *lens_ptr = nullptr;
    uint32_t *dims_ptr = nullptr;
    float *vals_ptr = nullptr;
    ObIArrayType *arr = nullptr;

    if (OB_ISNULL(lens_ptr = static_cast<uint32_t *>(allocator->alloc(sizeof(uint32_t) * num)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc lens", K(ret), K(num));
    } else if (OB_ISNULL(dims_ptr = static_cast<uint32_t *>(allocator->alloc(sizeof(uint32_t) * total_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc dims", K(ret), K(total_length));
    } else if (OB_ISNULL(vals_ptr = static_cast<float *>(allocator->alloc(sizeof(float) * total_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vals", K(ret), K(total_length));
    } else if (OB_FAIL(ObArrayTypeObjFactory::construct(*allocator, *sparse_vector_type_, arr, true))) {
      LOG_WARN("failed to construct sparse vector using factory", K(ret));
    } else if (OB_ISNULL(arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct sparse vector arr", K(ret));
    } else {
      data_ptr = (char *)data;
      uint32_t curr_pos = 0;
      for (int i = 0; OB_SUCC(ret) && i < num; i++) {
        if (sparse_byte_lens[i] > 0) {
          ObString data_str(sparse_byte_lens[i], (char *)data_ptr);
          uint32_t length = *(uint32_t *)(data_str.ptr());
          data_ptr += sparse_byte_lens[i];
          if (length > 0) {
            if (OB_NOT_NULL(arr) && OB_FAIL(arr->init(data_str))) {
              LOG_WARN("failed to init sparse vector with raw data", K(ret));
            } else {
              ObMapType *qvec = static_cast<ObMapType *>(arr);
              ObArrayFixedSize<uint32_t> *keys_arr = dynamic_cast<ObArrayFixedSize<uint32_t> *>(qvec->get_key_array());
              ObArrayFixedSize<float> *values_arr = dynamic_cast<ObArrayFixedSize<float> *>(qvec->get_value_array());

              if (OB_ISNULL(keys_arr) || OB_ISNULL(values_arr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to cast key or value array", K(ret));
              } else {
                uint32_t *keys = reinterpret_cast<uint32_t *>(keys_arr->get_data());
                float *values = reinterpret_cast<float *>(values_arr->get_data());
                lens_ptr[i] = length;
                MEMCPY(dims_ptr + curr_pos, keys, length * sizeof(uint32_t));
                MEMCPY(vals_ptr + curr_pos, values, length * sizeof(float));
                curr_pos += length;
              }
            }
          } else {
            lens_ptr[i] = 0;
          }
        } else {
          lens_ptr[i] = 0;
        }
      }
      if (OB_SUCC(ret)) {
        *lens = lens_ptr;
        *dims = dims_ptr;
        *vals = vals_ptr;
        // print_sparse_vectors(*lens, *dims, *vals, num);
      }
    }
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
int ObPluginVectorIndexAdaptor::add_snap_index(float *vectors, int64_t *vids, ObVecExtraInfoObj *extra_objs, int64_t extra_column_count, int num, uint32_t *sparse_byte_lens /* nullptr */)
{
  INIT_SUCC(ret);
  int64_t dim = 0;
  ObVectorIndexParam *param = nullptr;
  ObVectorIndexSegmentBuilder *segment_builder = nullptr;
  uint32_t *lens = nullptr;
  uint32_t *dims = nullptr;
  float *vals = nullptr;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (! snap_data_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null snap data", K(ret), K(snap_data_));
  } else if (OB_FAIL(check_tablet_valid(VIRT_SNAP))) {
    LOG_WARN("check tablet id invalid.", K(ret));
  } else if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get param.", K(ret));
  } else if (OB_FALSE_IT(dim = param->dim_)) {
  } else {
    if (param->type_ == ObVectorIndexAlgorithmType::VIAT_HNSW ||
        param->type_ == ObVectorIndexAlgorithmType::VIAT_HGRAPH ||
        param->type_ == ObVectorIndexAlgorithmType::VIAT_IPIVF) {
      if (OB_FAIL(init_snap_data_for_build(param, false/*is_buffer_mode*/))) {
        LOG_WARN("init snap index failed.", K(ret));
      } else if (OB_ISNULL(segment_builder = snap_data_->builder_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment builder is null", K(ret), K(snap_data_));
      } else if (num == 0 || OB_ISNULL(vectors)) {
        // do nothing
      } else if (OB_ISNULL(vids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid data.", K(ret));
      } else {
        if (is_sparse_vector_index_type()) {
          parse_sparse_vector((char*)vectors, num, sparse_byte_lens, &tmp_allocator, &lens, &dims, &vals);
        }

        char* extra_info_buf = nullptr;
        if (OB_NOT_NULL(extra_objs) && extra_column_count > 0 && param->extra_info_actual_size_ > 0 &&
            OB_FAIL(ObVecExtraInfo::extra_infos_to_buf(tmp_allocator, extra_objs, extra_column_count,
                                                       param->extra_info_actual_size_, num, extra_info_buf))) {
          LOG_WARN("failed to encode extra info.", K(ret), K(param->extra_info_actual_size_));
        } else {
          lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
          lib::ObLightBacktraceGuard light_backtrace_guard(false);
          if (!is_sparse_vector_index_type()) {
            if (OB_FAIL(segment_builder->add_index(vectors, vids, dim, extra_info_buf, num))) {
              LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
            }
          } else {
            if (OB_FAIL(segment_builder->add_index(lens, dims, vals, vids, num, extra_info_buf))) {
              LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
            }
          }
        }
      }
    } else if (param->type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_SQ
        || param->type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ
        || param->type_ == ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ) {
      if (OB_FAIL(init_snap_data_for_build(param, true/*is_buffer_mode*/))) {
        LOG_WARN("init snap index failed.", K(ret));
      } else if (OB_ISNULL(segment_builder = snap_data_->builder_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment builder is null", K(ret), K(snap_data_));
      } else if (num == 0 || OB_ISNULL(vectors)) {
        // do nothing
      } else if (OB_ISNULL(vids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid data.", K(ret));
      } else {
        if (is_sparse_vector_index_type()) {
          if (OB_FAIL(parse_sparse_vector((char*)vectors, num, sparse_byte_lens, &tmp_allocator, &lens, &dims, &vals))) {
            LOG_WARN("failed to parse sparse vector.", K(ret));
          }
        }
        char *extra_info_buf = nullptr;
        if (OB_SUCC(ret) && OB_NOT_NULL(extra_objs) && extra_column_count > 0 && param->extra_info_actual_size_ > 0 &&
            OB_FAIL(ObVecExtraInfo::extra_infos_to_buf(tmp_allocator, extra_objs, extra_column_count,
                                                       param->extra_info_actual_size_, num, extra_info_buf))) {
          LOG_WARN("failed to encode extra info.", K(ret), K(param->extra_info_actual_size_));
        } else {
          if (segment_builder->has_build_) {
            // directly write into index
            if (!is_sparse_vector_index_type()) {
              if (OB_FAIL(segment_builder->add_index(vectors, vids, dim, extra_info_buf, num))) {
                LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
              }
            } else {
              if (OB_FAIL(segment_builder->add_index(lens, dims, vals, vids, num, extra_info_buf))) {
                LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
              }
            }
          } else {
            TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
            if (! segment_builder->has_build_) {
              if (!is_sparse_vector_index_type()) {
                if (OB_FAIL(segment_builder->add_to_buffer(vectors, vids, dim, extra_info_buf, num, param->extra_info_actual_size_))) {
                  LOG_WARN("add vector to buffer fail", K(ret), KP(this), KPC(segment_builder));
                }
              } else {
                if (OB_FAIL(segment_builder->add_to_buffer(lens, dims, vals, vids, extra_info_buf, num, param->extra_info_actual_size_))) {
                  LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(segment_builder->build_if_need(
                  tenant_id_,
                  *get_allocator(),
                  *param,
                  this))) {
                LOG_WARN("build_if_need fail", K(ret), KP(this), KPC(segment_builder));
              }
            } else {
              /* In a multithreading scenario, it is possible for threads a and b to simultaneously acquire a lock_guard.
                 If thread a acquires the lock_guard and creates an index (i.e., snap_data_->segment_ != null),
                 then when thread b waits for thread a to release the lock_guard, it will find that snap_data_->segment_ != null.
                 At this point, thread a should call add_index to write the data; otherwise, the data will be lost. */
              lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
              lib::ObLightBacktraceGuard light_backtrace_guard(false);
              if (!is_sparse_vector_index_type()) {
                if (OB_FAIL(segment_builder->add_index(vectors, vids, dim, extra_info_buf, num))) {
                  LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
                }
              } else {
                if (OB_FAIL(segment_builder->add_index(lens, dims, vals, vids, num, extra_info_buf))) {
                  LOG_WARN("failed to add index.", K(ret), K(dim), K(num));
                }
              }
            }
          } // end for No sq index was built
          // here is the ending for snap_data_ write lock
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support index type", K(ret), K(param->type_), KP(param));
    }
  }

  return ret;
}

ObVectorIndexAlgorithmType ObPluginVectorIndexAdaptor::get_snap_index_type()
{
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  if (snap_data_.is_valid()) {
    index_type = snap_data_->get_snap_index_type();
  }
  return index_type;
}

ObVectorIndexAlgorithmType ObPluginVectorIndexAdaptor::get_incr_index_type()
{
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  if (incr_data_.is_valid()) {
    if (incr_data_->segment_handle_.is_valid()) {
      index_type = incr_data_->segment_handle_->get_index_type();
    }
  }
  return index_type;
}

int ObPluginVectorIndexAdaptor::check_if_need_optimize(ObVectorQueryAdaptorResultContext *ctx)
{
  int ret = OB_SUCCESS;
  int64_t snap_count = follower_sync_statistics_.snap_count_;
  int64_t snap_incr_seg_cnt = follower_sync_statistics_.snap_incr_seg_cnt_;
  int64_t snap_incr_vec_cnt = follower_sync_statistics_.snap_incr_vec_cnt_;
  int64_t snap_base_seg_cnt = follower_sync_statistics_.snap_base_seg_cnt_;
  int64_t snap_base_vec_cnt = follower_sync_statistics_.snap_base_vec_cnt_;
  int64_t incr_count = follower_sync_statistics_.incr_count_;
  int64_t bitmap_count = follower_sync_statistics_.vbitmap_count_;
  bitmap_count = MAX(incr_count, bitmap_count);
  if (!need_be_optimized_) {
    int64_t delete_count = 0;
    int64_t insert_count = 0;
    if (OB_NOT_NULL(ctx) && OB_NOT_NULL(ctx->bitmaps_)) {
      if (OB_NOT_NULL(ctx->bitmaps_->delete_bitmap_)) {
        delete_count = roaring64_bitmap_get_cardinality(ctx->bitmaps_->delete_bitmap_);
      }
      if (OB_NOT_NULL(ctx->bitmaps_->insert_bitmap_)) {
        insert_count = roaring64_bitmap_get_cardinality(ctx->bitmaps_->insert_bitmap_);
      }
    }

    if (follower_sync_statistics_.use_new_check_) {
      // new check logic, use merge task instead of optimze task
    } else if (snap_count + incr_count + insert_count == 0) {
    } else if (static_cast<double_t>(delete_count + insert_count + bitmap_count + snap_incr_vec_cnt) / static_cast<double_t>(snap_count + incr_count + insert_count) > VEC_INDEX_OPTIMIZE_RATIO) {
      need_be_optimized_ = true;
    }
    LOG_DEBUG("check_if_need_optimize", K(snap_count), K(incr_count), K(bitmap_count), K(insert_count), K(delete_count));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_snapshot_key_prefix(uint64_t tablet_id, uint64_t scn, uint64_t max_length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("adaptor allocator is null.", K(ret));
  } else {
    char *key_prefix_str = static_cast<char*>(get_allocator()->alloc(max_length));
    if (OB_ISNULL(key_prefix_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec key", K(ret));
    } else if (OB_FAIL(databuff_printf(key_prefix_str, max_length, pos, "%lu_%lu", tablet_id, scn))) {
      LOG_WARN("failed to print key prefix");
    } else {
      if(!snapshot_key_prefix_.empty()) {
        allocator_->free(snapshot_key_prefix_.ptr());
        snapshot_key_prefix_.reset();
      }
      snapshot_key_prefix_.assign(key_prefix_str, pos);
      LOG_INFO("change vector index snapshot_key_prefix success", K(snapshot_key_prefix_), KP(this), K(*this));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_snapshot_key_prefix(const ObString &snapshot_key_prefix)
{
  int ret = OB_SUCCESS;
  if (!snapshot_key_prefix_.empty() && snapshot_key_prefix_ == snapshot_key_prefix) {
    // do nothing
    LOG_INFO("try to change same vector index snapshot_key_prefix", K(snapshot_key_prefix), K(*this));
  } else if (snapshot_key_prefix.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index snapshot_key_prefix is empty", KR(ret), K(*this));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null allocator to set vector index snapshot_key_prefix ", KR(ret), K(*this));
  } else {
    if (!snapshot_key_prefix_.empty()) {
      allocator_->free(snapshot_key_prefix_.ptr());
      snapshot_key_prefix_.reset();
    }
    if (OB_FAIL(ob_write_string(*allocator_, snapshot_key_prefix, snapshot_key_prefix_))) {
      LOG_WARN("fail set vector index snapshot_key_prefix ", KR(ret), K(*this));
    } else {
      LOG_INFO("change vector index snapshot_key_prefix success", K(snapshot_key_prefix), KP(this), K(*this));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::set_snapshot_key_scn(const int64_t &snapshot_key_scn)
{
  int ret = OB_SUCCESS;
  if (! snap_data_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(snap_data_));
  } else if (OB_FAIL(snap_data_->scn_.convert_for_sql(snapshot_key_scn))) {
    LOG_WARN("fail to convert scn", K(ret), K(snapshot_key_scn));
  }
  LOG_INFO("set snapshot key scn", K(ret), K(snapshot_key_scn), KP(this), K(*this));
  return ret;
}


int ObPluginVectorIndexAdaptor::get_snapshot_key_scn(SCN &snapshot_key_scn)
{
  if (snap_data_.is_valid()) {
    snapshot_key_scn = snap_data_->scn_;
  }
  return OB_SUCCESS;
}

int ObPluginVectorIndexAdaptor::set_replace_scn(const SCN &replace_scn)
{
  int ret = OB_SUCCESS;
  if (replace_scn.is_valid()) {
    replace_scn_ = replace_scn;
    LOG_INFO("set replace scn", K(ret), K(replace_scn), KP(this), K(*this));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(replace_scn), KP(this), K(*this));
  }
  return ret;
}

SCN ObPluginVectorIndexAdaptor::get_replace_scn()
{
  return replace_scn_;
}

int ObPluginVectorIndexAdaptor::copy_meta_info(ObPluginVectorIndexAdaptor &other)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *hnsw_param = nullptr;
  snapshot_tablet_id_ = other.snapshot_tablet_id_;
  inc_tablet_id_ = other.inc_tablet_id_;
  vbitmap_tablet_id_ = other.vbitmap_tablet_id_;
  data_tablet_id_ = other.data_tablet_id_;
  rowkey_vid_tablet_id_ = other.rowkey_vid_tablet_id_;
  vid_rowkey_tablet_id_ = other.vid_rowkey_tablet_id_;
  snapshot_table_id_ = other.snapshot_table_id_;
  inc_table_id_ = other.inc_table_id_;
  vbitmap_table_id_ = other.vbitmap_table_id_;
  data_table_id_ = other.data_table_id_;
  rowkey_vid_table_id_ = other.rowkey_vid_table_id_;
  vid_rowkey_table_id_ = other.vid_rowkey_table_id_;
  embedded_table_id_ = other.embedded_table_id_;
  embedded_tablet_id_ = other.embedded_tablet_id_;
  type_ = other.type_;
  follower_sync_statistics_.sync_count_ = other.follower_sync_statistics_.sync_count_;
  follower_sync_statistics_.sync_fail_ = other.follower_sync_statistics_.sync_fail_;
  is_need_vid_ = other.is_need_vid_;
  if (OB_NOT_NULL(algo_data_)) {
    // do nothing
  } else if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_ISNULL(hnsw_param = static_cast<ObVectorIndexParam *>
                            (get_allocator()->alloc(sizeof(ObVectorIndexParam))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mem.", K(ret));
  } else {
    hnsw_param->reset();
    algo_data_ = hnsw_param;
    ObVectorIndexParam *other_param = static_cast<ObVectorIndexParam *>(other.algo_data_);
    if (OB_NOT_NULL(other_param) && OB_FAIL(hnsw_param->assign(*other_param))) {
      LOG_WARN("fail to assign params from vec_aux_ctdef_", K(ret));
    }
  }
  return ret;
}

// thread unsafe
int ObPluginVectorIndexAdaptor::check_snap_index()
{
  INIT_SUCC(ret);
  ObVectorIndexParam *param = nullptr;
  ObVectorIndexSegmentBuilder *segment_builder = nullptr;
  if (! snap_data_.is_valid() || OB_ISNULL(algo_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null snap data", K(ret), K(snap_data_), K(algo_data_));
  } else if (OB_ISNULL(param = static_cast<ObVectorIndexParam*>(algo_data_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get param.", K(ret));
  } else if (param->type_ == VIAT_HNSW || param->type_ == VIAT_HGRAPH || param->type_ == VIAT_IPIVF) {
    // do nothing
  } else if (! snap_data_->is_inited()) {
    LOG_INFO("snap data is not init, it's empty table", KPC(this));
  } else if (OB_ISNULL(segment_builder = snap_data_->builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment builder is null", K(ret), K(snap_data_));
  } else if (! segment_builder->has_build_) {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (segment_builder->is_empty_buffer()) {
      // do nothing :maybe null data
    } else if (is_sparse_vector_index_type()) {
      // Note. ipivf_sq must use ipivf to build index here
      ObVectorIndexAlgorithmType build_type = VIAT_IPIVF;
      if (OB_FAIL(segment_builder->create_and_add_sparse_index_from_arrays(
          tenant_id_,
          *get_allocator(),
          *param,
          build_type,
          this))) {
        LOG_WARN("failed to create and build sparse index from arrays.", K(ret));
      }
    } else {
      // TODO @zhichen
      //    hnsw_sq/hnsw_bq is not hgraph here, but active segment is hgraph
      //    and if change this, need consider old server upgrade
      ObVectorIndexAlgorithmType build_type = param->extra_info_actual_size_ > 0 ?  VIAT_HGRAPH : VIAT_HNSW;
      int64_t max_degree = param->type_ == VIAT_HNSW_SQ ? ObVectorIndexUtil::get_hnswsq_type_metric(param->m_) : param->m_;
      if (OB_FAIL(segment_builder->create_and_add(
          tenant_id_,
          *get_allocator(),
          *param,
          build_type,
          max_degree,
          this))) {
        LOG_WARN("create segment fail", K(ret));
      }
    }
  } else {
    // maybe retry
    int64_t vec_cnt = 0;
    if (OB_FAIL(segment_builder->segment_handle_->get_index_number(vec_cnt))) {
      LOG_WARN("failed to get snap index number.", K(ret));
    } else {
      LOG_INFO("get snap index element and array", K(ret), K(vec_cnt));
    }
    segment_builder->free_vec_buf_data(*get_allocator());
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
  bool can_skip = true;

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
          // if vid is not in delete_bitmap, add to insert_bitmap
          if (!roaring::api::roaring64_bitmap_contains(ctx->bitmaps_->delete_bitmap_, vid)) {
            ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ctx->bitmaps_->insert_bitmap_, vid));
          }
        } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_remove(ctx->bitmaps_->insert_bitmap_, vid));
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ctx->bitmaps_->delete_bitmap_, vid));

        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid op.", K(ret), K(op));
        }
        can_skip = false;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (get_can_skip() != NOT_SKIP && !can_skip) {
        update_can_skip(NOT_SKIP);
      }
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

int ObPluginVectorIndexAdaptor::write_into_delta_mem(ObVectorQueryAdaptorResultContext *ctx,
                                                     int count,
                                                     float *vectors,
                                                     uint64_t *vids,
                                                     ObVecExtraInfoObj *extra_objs,
                                                     int64_t extra_column_count,
                                                     ObVidBound vid_bound,
                                                     uint32_t *sparse_byte_lens /* nullptr */)
{
  INIT_SUCC(ret);
  ObVectorIndexSegmentWriteGuard write_guard;
  if (count == 0) {
    // do nothing
  } else if (!is_mem_data_init_atomic(VIRT_INC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write into delta mem but incr memdata uninit.", K(ret));
  } else if (OB_FAIL(write_guard.prepare_write(incr_data_))) {
    LOG_WARN("prepare write fail", K(ret), K(incr_data_));
  } else {
    TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    bool complete_delta = false;
    if (OB_FAIL(check_if_complete_delta(ctx->bitmaps_->insert_bitmap_, count, complete_delta))) {
      LOG_WARN("failed to check if complete delta", K(ret));
    } else if (complete_delta) {
      char *extra_info_buf = nullptr;
      ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
      if (OB_SUCC(ret) && OB_NOT_NULL(extra_objs) && extra_column_count > 0) {
        int64_t extra_info_actual_size = 0;
        if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
          LOG_WARN("failed to get extra info actual size.", K(ret));
        } else if (extra_info_actual_size > 0 &&
                   OB_FAIL(ObVecExtraInfo::extra_infos_to_buf(tmp_allocator, extra_objs, extra_column_count,
                                                              extra_info_actual_size, count, extra_info_buf))) {
          LOG_WARN("failed to encode extra info.", K(ret), K(extra_info_actual_size));
        }
      }
      if (OB_SUCC(ret)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
        lib::ObLightBacktraceGuard light_backtrace_guard(false);
        ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
        if (!is_sparse_vector_index_type()) {
          if (OB_FAIL(segment_handle->add_index(
                                             vectors,
                                             reinterpret_cast<int64_t *>(vids),
                                             ctx->get_dim(),
                                             extra_info_buf,
                                             count))) {
            LOG_WARN("failed to add index.", K(ret), K(ctx->get_dim()), K(count));
          }
        } else {
          // For sparse vector, we need to parse the vectors first
          uint32_t *lens = nullptr;
          uint32_t *dims = nullptr;
          float *vals = nullptr;
          if (OB_FAIL(parse_sparse_vector((char*)vectors, count, sparse_byte_lens, &tmp_allocator, &lens, &dims, &vals))) {
            LOG_WARN("failed to parse sparse vector", K(ret));
          } else if (OB_FAIL(segment_handle->add_index(lens, dims, vals, reinterpret_cast<int64_t *>(vids), count, extra_info_buf))) {
            LOG_WARN("failed to add sparse index.", K(ret), K(count));
          }
        }
      }
      if (OB_SUCC(ret)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPJ"));
        ObVectorIndexSegmentHandle &segment_handle = write_guard.handle();
        TCWLockGuard lock_guard(segment_handle->ibitmap_->rwlock_);
        segment_handle->set_read_vid_bound(vid_bound);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(segment_handle->ibitmap_->insert_bitmap_, vids[i]));
        }
      }
      LOG_INFO("write into delta mem.", K(ret), K(ctx->get_dim()), K(count), K(ObArrayWrap<int64_t>((int64_t*)vids, count)));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::complete_delta_buffer_table_data(ObVectorQueryAdaptorResultContext *ctx)
{
  INIT_SUCC(ret);
  float *vectors = nullptr;
  uint64_t *vids = nullptr;
  ObVecExtraInfoObj *extra_info_objs = nullptr;
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
  } else if (!is_sparse_vector_index_type() && OB_ISNULL(vectors = static_cast<float *>(tmp_allocator.alloc(sizeof(float) * ctx->get_dim() * ctx->get_vec_cnt())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc new mem.", K(ret));
  } else if (OB_ISNULL(vids = static_cast<uint64_t *>(tmp_allocator.alloc(sizeof(uint64_t) * ctx->get_vec_cnt())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc new mem.", K(ret));
  } else if (OB_NOT_NULL(ctx->vec_data_.extra_info_objs_)) {
    if (OB_ISNULL(extra_info_objs = static_cast<ObVecExtraInfoObj *>(tmp_allocator.alloc(sizeof(ObVecExtraInfoObj) * ctx->get_extra_column_count() * ctx->get_vec_cnt())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc new mem.", K(ret));
    }
  }
  uint32_t sparse_total_length = 0;
  if (OB_FAIL(ret)) {
  } else {
    int64_t dim = ctx->get_dim();
    int64_t extra_column_count = ctx->get_extra_column_count();
    int64_t ctx_vec_cnt = ctx->get_vec_cnt();
    for (int i = 0; OB_SUCC(ret) && i < ctx_vec_cnt; i++) {
      float *vector = nullptr;
      if (ctx->vec_data_.vectors_[i].is_null() || ctx->vec_data_.vectors_[i].get_string().empty()) {
        // do nothing
      } else if (!is_sparse_vector_index_type() && ctx->vec_data_.vectors_[i].get_string().length() != dim * sizeof(float)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid string.", K(ret), K(i), K(ctx->vec_data_.vectors_[i].get_string().length()), K(dim));
      } else {
        uint64_t vid = ctx->get_vids()[i + ctx->get_curr_idx()].get_int();
        vids[count] = vid;
        if (OB_NOT_NULL(extra_info_objs)) {
          for (int j = 0; OB_SUCC(ret) && j < extra_column_count; j++) {
            extra_info_objs[count * extra_column_count + j] = ctx->vec_data_.extra_info_objs_[i * extra_column_count + j];
          }
        }
        vid_bound.set_vid(vid);

        if (!is_sparse_vector_index_type()) {
          if (OB_ISNULL(vector = reinterpret_cast<float *>(ctx->vec_data_.vectors_[i].get_string().ptr()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get float vector.", K(ret), K(i));
          } else {
            for (int j = 0; OB_SUCC(ret) && j < dim; j++) {
              vectors[count * dim + j] = vector[j];
            }
          }
        } else {
          sparse_total_length += ctx->vec_data_.vectors_[i].get_string().length();
        }

        if (OB_SUCC(ret)) {
          count++;
        }
      }
    }
    LOG_INFO("SYCN_DELTA_complete_data", KP(this), K(ctx->vec_data_));
    // print_vids(vids, ctx_vec_cnt);
    // print_vectors(vectors, ctx_vec_cnt, dim);
  }

  if (OB_FAIL(ret)) {
  } else {
    if (!is_sparse_vector_index_type()) {
      if (OB_FAIL(write_into_delta_mem(ctx, count, vectors, vids, extra_info_objs, ctx->get_extra_column_count(), vid_bound))) {
        LOG_WARN("failed to write into delta mem.", K(ret), KP(ctx));
      }
    } else {
      // For sparse vectors, we need to handle the raw data differently
      // Create a buffer to hold the sparse vector data
      char *sparse_vectors = nullptr;
      uint32_t *sparse_byte_lens = nullptr;
      if (count == 0 || sparse_total_length == 0) {
        // do nothing
      } else if (OB_ISNULL(sparse_vectors = static_cast<char *>(tmp_allocator.alloc(sparse_total_length * sizeof(char))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc sparse vectors buffer", K(ret), K(sparse_total_length));
      } else if (OB_ISNULL(sparse_byte_lens = static_cast<uint32_t *>(tmp_allocator.alloc(count * sizeof(uint32_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc sparse byte lens", K(ret), K(count));
      } else {
        char *sparse_curr_pos = sparse_vectors;
        int j = 0;
        // Copy the raw sparse vector data
        for (int i = 0; OB_SUCC(ret) && i < ctx->get_vec_cnt(); i++) {
          if (!ctx->vec_data_.vectors_[i].is_null() && !ctx->vec_data_.vectors_[i].get_string().empty()) {
            ObString vec_str = ctx->vec_data_.vectors_[i].get_string();
            MEMCPY(sparse_curr_pos, vec_str.ptr(), vec_str.length());
            sparse_curr_pos += vec_str.length();
            sparse_byte_lens[j++] = vec_str.length();
          }
        }

        if (OB_SUCC(ret)) {
          // For sparse vectors, we pass the raw data to write_into_delta_mem
          // The function will handle the parsing internally
          if (OB_FAIL(write_into_delta_mem(ctx,
                  count,
                  reinterpret_cast<float *>(sparse_vectors),
                  vids,
                  extra_info_objs,
                  ctx->get_extra_column_count(),
                  vid_bound,
                  sparse_byte_lens))) {
            LOG_WARN("failed to write sparse vectors into delta mem.", K(ret), KP(ctx));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ctx->batch_allocator_.reuse();
      ctx->do_next_batch();
      if (ctx->if_next_batch()) {
        ctx->status_ = PVQ_COM_DATA;
        LOG_INFO("SYCN_DELTA_next_batch", KP(this), K(ctx->vec_data_));
      } else {
        ctx->status_ = PVQ_LACK_SCN;
        LOG_INFO("SYCN_DELTA_batch_end", KP(this), K(ctx->vec_data_));
      }
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
  } else if (! ctx->scn_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx scn is invalid", K(ret), KP(this), K(query_scn), K(ctx->scn_), K(vbitmap_data_));
  } else if (vbitmap_data_.is_valid() &&
      vbitmap_data_->scn_.is_valid() &&
      ctx->scn_ < vbitmap_data_->scn_) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("read snapshot too old, need retry", K(ret), KP(this), K(query_scn), K(ctx->scn_), K(vbitmap_data_));
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
      if (get_can_skip() == NOT_INITED) {
        update_can_skip(SKIP);
      }
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
  } else {
    bool need_check_if_complete_delta = is_hybrid_index() ? is_sync_index() : true;
    bool complete_delta = false;
    if (need_check_if_complete_delta && OB_FAIL(check_if_complete_delta(ctx->bitmaps_->insert_bitmap_, i_vids.count(), complete_delta))) {
      LOG_WARN("failed to check if complete delta", K(ret));
    } else if (complete_delta) {
      if (OB_FAIL(prepare_delta_mem_data(ctx->bitmaps_->insert_bitmap_, i_vids, ctx))) {
        LOG_WARN("failed to complete.", K(ret));
      } else if (ctx->vec_data_.count_ > 0) {
        ctx->status_ = PVQ_COM_DATA;
      }
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

int ObPluginVectorIndexAdaptor::write_into_index_mem(ObVecIdxVBitmapDataHandle &vbitmap, int64_t dim, SCN read_scn,
                                                     ObArray<uint64_t> &i_vids,
                                                     ObArray<uint64_t> &d_vids)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(vbitmap->bitmap_rwlock_);
  if (vbitmap.get() != vbitmap_data_.get()) {
    // bitmap is changed, need retry and refresh
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("need retry", K(ret), KPC(this));
  } else if (OB_FAIL(write_into_bitmap_without_lock(vbitmap, dim, read_scn, i_vids, d_vids))) {
    LOG_WARN("write bitmap fail", K(ret), KPC(this));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::write_into_bitmap_without_lock(ObVecIdxVBitmapDataHandle &bitmap,
                                                     int64_t dim, SCN read_scn,
                                                     ObArray<uint64_t> &i_vids,
                                                     ObArray<uint64_t> &d_vids)
{
  INIT_SUCC(ret);
  if (read_scn > bitmap->scn_) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPK"));
    roaring::api::roaring64_bitmap_t *ibitmap = bitmap->bitmap_->insert_bitmap_;
    roaring::api::roaring64_bitmap_t *dbitmap = bitmap->bitmap_->delete_bitmap_;
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

    bitmap->scn_ = read_scn;
    LOG_INFO("write into index mem.", K(bitmap), K(i_vids.count()), K(d_vids.count()), K(read_scn));
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
  int ret = OB_SUCCESS;
  bool res = false;

  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->pre_filter_)) {
  } else {
    int64_t gene_vid_cnt = ctx->pre_filter_->get_valid_cnt();

    if (is_mem_data_init_atomic(VIRT_INC)) {
      roaring::api::roaring64_bitmap_t *delta_bitmap = nullptr;
      if (OB_FAIL(get_full_incr_bitmap(delta_bitmap))) {
        LOG_WARN("get full incr bitmap fail", K(ret));
      } else if (!ctx->pre_filter_->is_subset(delta_bitmap)) {
        res = true;
      } else if (is_mem_data_init_atomic(VIRT_BITMAP)) {
        bool vbitmap_is_subset = false;
        if (OB_FAIL(check_vbitmap_is_subset(delta_bitmap, vbitmap_is_subset))) {
          LOG_WARN("check vbitmap is subset fail", K(ret));
        } else if (! vbitmap_is_subset) {
          res = true;
        }
      } else {
        res = gene_vid_cnt > 0;
      }

      if (OB_NOT_NULL(delta_bitmap)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPO"));
        roaring64_bitmap_free(delta_bitmap);
        delta_bitmap = nullptr;
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
  int64_t row_scn_num = 0;
  SCN row_scn;
  SCN frozen_vbitmap_scn = SCN::min_scn();
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
    // copy reference
    ObVecIdxVBitmapDataHandle vbitmap;
    {
      TCRLockGuard lock_guard(vbitmap_data_->bitmap_rwlock_);
      vbitmap = vbitmap_data_;
    }
    if (has_frozen()) {
      TCRLockGuard lock_guard(frozen_data_->vbitmap_->bitmap_rwlock_);
      frozen_vbitmap_scn = frozen_data_->vbitmap_->scn_;
    }
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter);
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      int64_t vid = 0;
      ObString op;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (!datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid new row.", K(ret));
      } else if (OB_FALSE_IT(row_scn_num = datum_row->storage_datums_[0].get_int())) {
        LOG_WARN("failed to get read scn.", K(ret));
      } else if (OB_FAIL(row_scn.convert_for_gts(row_scn_num))) {
        LOG_WARN("failed to convert from ts.", K(ret), K(row_scn_num));
      } else if (row_scn <= frozen_vbitmap_scn) {
        LOG_INFO("skip in frozen vbitmap", K(row_scn), K(frozen_vbitmap_scn), KPC(datum_row));
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
    } else if (OB_FAIL(write_into_index_mem(vbitmap, dim, read_scn, i_vids, d_vids))) {
      LOG_WARN("failed to write into index mem.", K(ret), K(read_scn));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::check_if_complete_delta(
    roaring::api::roaring64_bitmap_t *gene_bitmap, int64_t count, bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  int64_t gene_vid_cnt = roaring64_bitmap_get_cardinality(gene_bitmap);
  if (gene_vid_cnt == 0) {
    if (count == 0) {
      res = false;
    } else if (! is_mem_data_init_atomic(VIRT_INC)) {
      res = true;
      LOG_INFO("incr data is not init, but index_id table is not emptry, so need complete delta", K(count));
    } else if (is_mem_data_init_atomic(VIRT_BITMAP)) {
      roaring::api::roaring64_bitmap_t *delta_bitmap = nullptr;
      bool vbitmap_is_subset = false;
      if (OB_FAIL(get_full_incr_bitmap(delta_bitmap))) {
        LOG_WARN("get full incr bitmap fail", K(ret));
      } else if (OB_FAIL(check_vbitmap_is_subset(delta_bitmap, vbitmap_is_subset))) {
        LOG_WARN("check vbitmap is subset fail", K(ret));
      } else if (! vbitmap_is_subset) {
        res = true;
        LOG_INFO("incr data is not subset, but index_id table is not emptry, so need complete delta", KP(this), K(count));
      }
      if (OB_NOT_NULL(delta_bitmap)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPO"));
        roaring64_bitmap_free(delta_bitmap);
        delta_bitmap = nullptr;
      }
    }
  } else if (is_mem_data_init_atomic(VIRT_INC)) {
    roaring::api::roaring64_bitmap_t *delta_bitmap = nullptr;
    if (OB_FAIL(get_full_incr_bitmap(delta_bitmap))) {
      LOG_WARN("get full incr bitmap fail", K(ret));
    } else if (!roaring64_bitmap_is_subset(gene_bitmap, delta_bitmap)) {
      res = true;
      LOG_INFO("incr data is not subset, need complete delta", KP(this), K(gene_vid_cnt));
    } else if (count > 0 && is_mem_data_init_atomic(VIRT_BITMAP)) { // andnot_bitmap is null, if count = 0, do nothing
      bool vbitmap_is_subset = false;
      if (OB_FAIL(check_vbitmap_is_subset(delta_bitmap, vbitmap_is_subset))) {
        LOG_WARN("check vbitmap is subset fail", K(ret));
      } else if (! vbitmap_is_subset) {
        res = true;
        LOG_INFO("incr data is subset, but index_id table is not emptry, so need complete delta", KP(this), K(count));
      }
    }
    if (OB_NOT_NULL(delta_bitmap)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPO"));
      roaring64_bitmap_free(delta_bitmap);
      delta_bitmap = nullptr;
    }
  } else if (gene_vid_cnt > 0) {
    res = true;
    LOG_INFO("index_id table is not empty, so need complete delta", KP(this), K(gene_vid_cnt));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::prepare_delta_mem_data(roaring::api::roaring64_bitmap_t *gene_bitmap,
                                                       ObArray<uint64_t> &i_vids,
                                                       ObVectorQueryAdaptorResultContext *ctx)
{
  INIT_SUCC(ret);
  roaring::api::roaring64_bitmap_t *delta_bitmap = nullptr;
  if (OB_FAIL(try_init_mem_data(VIRT_INC))) {
    LOG_WARN("failed to init mem data incr.", K(ret));
  } else if (OB_ISNULL(gene_bitmap)
            || OB_ISNULL(ctx) || OB_ISNULL(ctx->tmp_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid bitmap.", K(ret), KP(gene_bitmap), KP(delta_bitmap), KP(ctx));
  } else {
    roaring::api::roaring64_bitmap_t *andnot_bitmap = nullptr;
    if (OB_FAIL(get_full_incr_bitmap(delta_bitmap))) {
      LOG_WARN("get miss vid bitmap fail", K(ret));
    } else {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPL"));
      TCRLockGuard rd_bitmap_lock_guard(incr_data_->segment_handle_->ibitmap_->rwlock_);
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
      int64_t extra_column_count = ctx->get_extra_column_count();
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
        if (OB_SUCC(ret) && extra_column_count > 0) {
          if (OB_ISNULL(ctx->vec_data_.extra_info_objs_ =
                                   static_cast<ObVecExtraInfoObj *>(ctx->tmp_allocator_->alloc(
                                       sizeof(ObVecExtraInfoObj) * vector_cnt * extra_column_count)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc extra_info objs.", K(ret));
          }
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
        ctx->vec_data_.extra_column_count_ = extra_column_count;
        ctx->vec_data_.count_ = bitmap_cnt;
        ctx->vec_data_.vids_ = vids;
        ctx->vec_data_.curr_idx_ = 0;
        if (is_sparse_vector_index_type()) {
          // The actual vector data will be filled later when reading from tables
          // For now, we just initialize the structure
          LOG_INFO("SYCN_DELTA_prepare_data for sparse vector", KP(this), K(ctx->vec_data_), K(i_vids.count()));
        } else {
          LOG_INFO("SYCN_DELTA_prepare_data", KP(this), K(ctx->vec_data_), K(i_vids.count()));
        }
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
  if (OB_NOT_NULL(delta_bitmap)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPO"));
    roaring64_bitmap_free(delta_bitmap);
    delta_bitmap = nullptr;
  }
  return ret;
}

// thread unsafe
int ObPluginVectorIndexAdaptor::serialize_snapshot(ObHNSWSerializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *index_param = nullptr;
  uint64_t tenant_data_version = 0;
  int64_t vec_cnt = 0;
  int64_t min_vid = INT64_MAX;
  int64_t max_vid = 0;
  ObVecIdxSnapshotDataWriteCtx *vctx = reinterpret_cast<ObVecIdxSnapshotDataWriteCtx*>(param.vctx_);
  if (!snap_data_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("snap index is not init", K(ret));
  } else if (OB_ISNULL(snap_data_->builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment builder is null", K(ret), K(snap_data_));
  } else if (OB_ISNULL(index_param = static_cast<ObVectorIndexParam*>(algo_data_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get index param.", K(ret));
  } else if (snap_data_->builder_->seg_type_ == ObVectorIndexSegmentType::INCR_MERGE
      && snap_data_->builder_->add_cnt_ == 0) {
    // store vbitmap for empty merge result
    if (! snap_data_->builder_->segment_handle_.is_valid()
        && OB_FAIL(ObVectorIndexSegment::create(snap_data_->builder_->segment_handle_,
            tenant_id_, *get_allocator(), *index_param, index_param->type_, index_param->m_, this))) {
      LOG_WARN("failed to create vsag index.", K(ret));
    } else if (OB_FALSE_IT(snap_data_->builder_->seg_type_ = ObVectorIndexSegmentType::EMPTY)) {
    } else if (OB_FAIL(snap_data_->builder_->serialize(tenant_id_, param))) {
      LOG_WARN("serialize segment fail", K(ret));
    } else if (OB_FAIL(build_snap_meta(param.tablet_id_, param.snapshot_version_, vctx->vals_.count()))) {
      LOG_WARN("build meta data fail", K(ret));
    } else if (OB_FAIL(snap_data_->build_finished(*get_allocator()))) {
      LOG_WARN("build_finished fail", K(ret));
    } else {
      LOG_INFO("build empty segment sucess", KP(this));
    }
  } else if (! snap_data_->builder_->segment_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("build segment is not init, empty index", K(ret), K(snap_data_));
  } else if (OB_FAIL(snap_data_->builder_->segment_handle_->get_index_number(vec_cnt))) {
    LOG_WARN("failed to get snap index number.", K(ret));
  } else if (vec_cnt == 0) {
    // do nothing
    LOG_INFO("[vec index] empty snap index, do not need to serialize", K(lbt()));
  } else if (OB_FAIL(snap_data_->builder_->segment_handle_->get_vid_bound(min_vid, max_vid))) {
    LOG_WARN("failed to get snap index number.", K(ret));
  // push empty meta block data as fisrt element of vals
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (param.is_vec_tablet_rebuild_
      || ! ObPluginVectorIndexHelper::enable_persist_vector_index_incremental(tenant_id_)
      || tenant_data_version < DATA_VERSION_4_5_1_0) {
    if (OB_FAIL(snap_data_->builder_->serialize(tenant_id_, param))) {
      LOG_WARN("serialize segment fail", K(ret));
    } else if (OB_FAIL(build_snap_meta(param.tablet_id_, param.snapshot_version_, vctx->vals_.count()))) {
      LOG_WARN("build meta data fail", K(ret));
    } else if (OB_FAIL(snap_data_->build_finished(*get_allocator()))) {
      LOG_WARN("build_finished fail", K(ret));
    } else {
      snap_data_->meta_.is_persistent_ = false;
      LOG_INFO("build without meta sucess", KP(this));
    }
  } else if (OB_FAIL(vctx->vals_.push_back(ObVecIdxSnapshotBlockData(true/*is_meta*/, ObString())))) {
    LOG_WARN("push back fail", K(ret));
  } else if (OB_FAIL(snap_data_->builder_->serialize(tenant_id_, param))) {
    LOG_WARN("serialize segment fail", K(ret));
  } else if (OB_FAIL(build_and_serialize_meta_data(
      param.tablet_id_, param.snapshot_version_, vctx->vals_.count() - 1, *param.allocator_, vctx->vals_.at(0).get_data()))) {
    LOG_WARN("build meta data fail", K(ret));
  } else if (OB_FAIL(snap_data_->build_finished(*get_allocator()))) {
    LOG_WARN("build_finished fail", K(ret));
  } else {
    LOG_INFO("build with meta row sucess", KP(this));
  }
  LOG_INFO("serialize_snapshot finish", K(ret), K(vec_cnt), K(min_vid), K(max_vid), KPC(this));
  return ret;
}

int ObPluginVectorIndexAdaptor::renew_single_snap_index(bool mem_saving_mode)
{
  int ret = OB_SUCCESS;
  if (mem_saving_mode) {
    ObString invalid_prefix("renew");
    if (! snap_data_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr snap_data_", K(ret), K(snap_data_));
    } else {
      TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
      if (OB_FAIL(renew_snapdata_in_lock())) {
        LOG_WARN("failed to free snap memdata", K(ret), KPC(this));
      } else if (OB_FAIL(set_snapshot_key_prefix(invalid_prefix))) {
        LOG_WARN("fail to set snapshot key prefix", K(ret));
      }
    }
  } else if (OB_FAIL(snap_data_->immutable_optimize_snap())) {
    LOG_WARN("fail to immutable_optimize", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::renew_snapdata_in_lock()
{
  int ret = OB_SUCCESS;
  if (! snap_data_.is_valid()) {
    // do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(snap_data_), K(allocator_));
  } else if (OB_FAIL(snap_data_->free_segment_memory())) {
    LOG_WARN("free segment memory fail", K(ret), K(snap_data_));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::merge_and_generate_bitmap(ObVectorQueryAdaptorResultContext *ctx,
                                                          ObHnswBitmapFilter &iFilter,
                                                          ObHnswBitmapFilter &dFilter)
{
  INIT_SUCC(ret);
  roaring::api::roaring64_bitmap_t *ibitmap = nullptr;
  roaring::api::roaring64_bitmap_t *dbitmap = nullptr;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument.", K(ctx));
  } else if (ctx->is_prefilter_valid()) {
    iFilter = *(ctx->pre_filter_);
    dFilter = iFilter;
  } else if (!is_mem_data_init_atomic(VIRT_BITMAP) &&
             !has_frozen() &&
             (!is_snap_inited() || snap_data_->meta_.incrs_.count() == 0)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPP"));
    ibitmap = ctx->bitmaps_->insert_bitmap_;
    dbitmap = ctx->bitmaps_->delete_bitmap_;
    iFilter.set_roaring_bitmap(ibitmap);
    dFilter.set_roaring_bitmap(dbitmap);
    LOG_DEBUG("vbitmap is not inited.", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPQ"));
    ibitmap = ctx->bitmaps_->insert_bitmap_;
    dbitmap = ctx->bitmaps_->delete_bitmap_;
    if (OB_SUCC(ret) && is_mem_data_init_atomic(VIRT_BITMAP)) {
      TCRLockGuard rd_bitmap_lock_guard(vbitmap_data_->bitmap_rwlock_);
      if (ctx->scn_ < vbitmap_data_->scn_) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("current bitmap has been refreshed, need retry", K(ret), KP(this), K(ctx->scn_), K(vbitmap_data_->scn_));
      } else {
        ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(ibitmap, vbitmap_data_->bitmap_->insert_bitmap_));
        ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(dbitmap, vbitmap_data_->bitmap_->delete_bitmap_));
      }
    }

    if (OB_SUCC(ret) && has_frozen()) {
      TCRLockGuard rd_bitmap_lock_guard(frozen_data_->vbitmap_->bitmap_rwlock_);
      if (frozen_data_->vbitmap_.get() == vbitmap_data_.get()) {
        LOG_INFO("frozen_data vbitmap same with vbitmap_data", KP(this), K(frozen_data_), K(vbitmap_data_));
      } else if (frozen_data_->vbitmap_.is_valid()) {
        ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(ibitmap, frozen_data_->vbitmap_->bitmap_->insert_bitmap_));
        ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(dbitmap, frozen_data_->vbitmap_->bitmap_->delete_bitmap_));
      }
    }

    if (is_snap_inited()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
        const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
        if (! seg_meta.segment_handle_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("segment handle is invalid", K(ret), K(i), K(seg_meta), KPC(this));
        } else if (OB_NOT_NULL(seg_meta.segment_handle_->vbitmap_)) {
          if (OB_SUCC(ret) && OB_NOT_NULL(seg_meta.segment_handle_->vbitmap_->insert_bitmap_)) {
            ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(ibitmap, seg_meta.segment_handle_->vbitmap_->insert_bitmap_));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(seg_meta.segment_handle_->vbitmap_->delete_bitmap_)) {
            ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(dbitmap, seg_meta.segment_handle_->vbitmap_->delete_bitmap_));
          }
        }
      }
    }

    ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(ibitmap, dbitmap));
    iFilter.set_roaring_bitmap(ibitmap);
    dFilter.set_roaring_bitmap(dbitmap);
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

void ObPluginVectorIndexAdaptor::print_sparse_vectors(uint32_t *lens, uint32_t *dims, float *vals, int64_t count)
{
  if (count != 0) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    uint32_t pos = 0;
    for (int i = 0; i < count; i++) {
      ObStringBuffer buffer(&tmp_allocator);
      for (int j = 0; j < lens[i]; j++) {
        char buf[10];
        sprintf(buf, "%d: %.1f ", dims[pos + j], vals[pos + j]);
        buffer.append(buf, -1);
      }
      pos += lens[i];
      LOG_INFO("SYCN_DELTA_vectors", K(buffer), KP(buffer.ptr()), K(buffer.string()));
    }
  }
}

int ObPluginVectorIndexAdaptor::vsag_query_vids(float *vector,
                                                const int64_t *vids,
                                                int64_t count,
                                                ObVecIdxQueryResult &result,
                                                uint32_t sparse_byte_len)
{
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  uint32_t sparse_byte_lens[1];
  sparse_byte_lens[0] = sparse_byte_len;
  uint32_t *sparse_lens = nullptr;
  uint32_t *sparse_dims = nullptr;
  float *sparse_vals = nullptr;
  const bool is_sparse = is_sparse_vector_index_type();
  if (is_sparse_vector_index_type()) {
    if (OB_FAIL(parse_sparse_vector((char *)vector, 1, sparse_byte_lens, &tmp_allocator,
                                    &sparse_lens, &sparse_dims, &sparse_vals))) {
      LOG_WARN("failed to parse sparse vector using parse_sparse_vector", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_mem_data_init_atomic(VIRT_INC)) {
    TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (OB_FAIL(result.segments_.push_back(incr_data_->segment_handle_))) {
      LOG_WARN("push back segment fail", K(ret), K(is_sparse), K(count));
    }
  }
  if (OB_SUCC(ret) && has_frozen()) {
    TCRLockGuard lock_guard(frozen_data_->mem_data_rwlock_);
    if (OB_FAIL(result.segments_.push_back(frozen_data_->segment_handle_))) {
      LOG_WARN("push back segment fail", K(ret), K(is_sparse), K(count));
    }
  }

  if (OB_SUCC(ret) && is_mem_data_init_atomic(VIRT_SNAP)) {
    TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
      if (! seg_meta.segment_handle_.is_valid()) { // its normal, there maybe have no snap index
        LOG_INFO("segment is not load when brute force", K(i), KP(this), K(seg_meta));
      } else if (OB_FAIL(result.segments_.push_back(seg_meta.segment_handle_))) {
        LOG_WARN("push back segment fail", K(ret), K(is_sparse), K(count));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.bases_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.bases_.at(i);
      if (! seg_meta.segment_handle_.is_valid()) { // its normal, there maybe have no snap index
        LOG_INFO("segment is not load when brute force", K(i), KP(this), K(seg_meta));
      } else if (OB_FAIL(result.segments_.push_back(seg_meta.segment_handle_))) {
        LOG_WARN("push back segment fail", K(ret), K(is_sparse), K(count));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < result.segments_.count(); ++i) {
    ObVectorIndexSegmentHandle& segment_handle = result.segments_.at(i);
    const float* distance = nullptr;
    if (is_sparse) {
      ret = segment_handle->cal_distance_by_id(*sparse_lens, sparse_dims, sparse_vals, vids, count, distance);
    } else {
      ret = segment_handle->cal_distance_by_id(vector, vids, count, distance);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("cal_distance_by_id fail", K(ret), K(is_sparse), K(count));
    } else if (OB_FAIL(result.distances_.push_back(distance))) {
      LOG_WARN("push back distance fail", K(ret), K(is_sparse), K(count));
      segment_handle->mem_ctx()->Deallocate(const_cast<float*>(distance));
    }
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::vsag_query_vids(ObVectorQueryAdaptorResultContext *ctx,
                                                ObVectorQueryConditions *query_cond,
                                                int64_t dim, float *query_vector,
                                                ObVectorQueryVidIterator *&vids_iter)
{
  INIT_SUCC(ret);
  int64_t *merge_vids = nullptr;
  float *merge_distance = nullptr;
  ObVecExtraInfoPtr merge_extra_info_ptr;

  int64_t extra_info_actual_size = 0;
  float ob_sparse_drop_ratio_search = query_cond->ob_sparse_drop_ratio_search_;
  int64_t n_candidate = query_cond->n_candidate_;

  const bool is_sparse_vector = is_sparse_vector_index_type();
  uint32_t *sparse_lens = nullptr;
  uint32_t *sparse_dims = nullptr;
  float *sparse_vals = nullptr;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);

  if (is_sparse_vector_index_type()) {
    ObString vector_str = query_cond->query_vector_;
    if (vector_str.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query vector is empty for sparse vector", K(ret));
    } else {
      char *data = const_cast<char*>(vector_str.ptr());
      int num = 1;
      uint32_t sparse_byte_lens[1];
      sparse_byte_lens[0] = vector_str.length();
      if (OB_FAIL(parse_sparse_vector(data, num, sparse_byte_lens, &tmp_allocator,
                                     &sparse_lens, &sparse_dims, &sparse_vals))) {
        LOG_WARN("failed to parse sparse vector using parse_sparse_vector", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ctx->ifilter_ = OB_NEWx(ObHnswBitmapFilter, ctx->tmp_allocator_, tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory fail for ifilter", K(ret), "size", sizeof(ObHnswBitmapFilter));
  } else if (OB_ISNULL(ctx->dfilter_ = OB_NEWx(ObHnswBitmapFilter, ctx->tmp_allocator_, tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory fail for dfilter", K(ret), "size", sizeof(ObHnswBitmapFilter));
  } else if (OB_FAIL(merge_and_generate_bitmap(ctx, *ctx->ifilter_, *ctx->dfilter_))) {
    LOG_WARN("failed to merge and generate bitmap.", K(ret));
  } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
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

  float valid_ratio = 1.0;
  if (OB_SUCC(ret) && ctx->is_prefilter_valid()) {
    float incr_valid_ratio = 1.0;
    float snap_valid_ratio = 1.0;
    int64_t incr_cnt = 0;
    int64_t snap_cnt = 0;
    if (OB_FAIL(get_inc_index_row_cnt(incr_cnt))) {
      LOG_WARN("failed to get inc index number.", K(ret));
    } else if (OB_FAIL(get_snap_index_row_cnt(snap_cnt))) {
      LOG_WARN("failed to get snap index number.", K(ret));
    } else {
      incr_valid_ratio = ctx->pre_filter_->get_valid_ratio(incr_cnt);
      snap_valid_ratio = ctx->pre_filter_->get_valid_ratio(snap_cnt);
      valid_ratio = incr_valid_ratio < snap_valid_ratio ? incr_valid_ratio : snap_valid_ratio;
      valid_ratio = valid_ratio < 1.0f ? valid_ratio : 1.0f;
      // ATTENTION!!!!
      // valid_ratio is relative to VSAG version, after version 0.13.4 need to be reviewed to see if any modifications are required
      // get new_ratio from (1 - new_ratio) * 0.9 = 1 - (1 - old_ratio) * 0.7
      // TOREMOVE : remove this when vsag fix skip_ratio
      // valid_ratio = (6.0f - 7.0f * valid_ratio) / 9.0f;
    }
  }

  ObVsagQueryResultArray result_list;

  ObVectorIndexSegQueryHandler handler;
  handler.tenant_id_ =  tenant_id_;
  handler.query_cond_ = query_cond;
  handler.ctx_ = ctx;
  handler.extra_info_actual_size_ = extra_info_actual_size;
  handler.valid_ratio_ = valid_ratio;
  handler.query_vector_ = query_vector;
  handler.dim_ = dim;
  handler.is_sparse_vector_ = is_sparse_vector;
  handler.sparse_lens_ = sparse_lens;
  handler.sparse_dims_ = sparse_dims;
  handler.sparse_vals_ = sparse_vals;
  if (OB_SUCC(ret) && is_mem_data_init_atomic(VIRT_INC)) {
    TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    if (! incr_data_->segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment handle is invalid", K(ret), K(incr_data_), KPC(this));
    } else if (OB_FAIL(ctx->add_segment_querier(
        incr_data_->segment_handle_, true/*is_incr*/, ctx->ifilter_, true/*reverse_filter*/))) {
      LOG_WARN("add incr segment querier fail", K(ret));
    }
  }
  if (OB_SUCC(ret) && has_frozen()) {
    TCRLockGuard lock_guard(frozen_data_->mem_data_rwlock_);
    if (! frozen_data_->segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment handle is invalid", K(ret), K(frozen_data_), KPC(this));
    } else if (ctx->segments_.count() == 1 &&
        frozen_data_->segment_handle_.get() == ctx->segments_.at(0)->handle_.get()) {
      LOG_INFO("skip frozen, beacuase it's same with active segment", K(frozen_data_), K(ctx->segments_.at(0)), K(incr_data_));
    } else if (OB_FAIL(ctx->add_segment_querier(
        frozen_data_->segment_handle_, true/*is_incr*/, ctx->ifilter_, true/*reverse_filter*/))) {
      LOG_WARN("add frozen segment querier fail", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_mem_data_init_atomic(VIRT_SNAP)) {
    bool is_pre_filter = ctx->is_prefilter_valid();
    ObHnswBitmapFilter *base_filter = (!is_pre_filter && (ctx->dfilter_ != nullptr && ctx->dfilter_->is_empty())) ? nullptr : ctx->dfilter_;
    TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
      if (! seg_meta.segment_handle_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment handle is invalid", K(ret), K(i), K(seg_meta), KPC(this));
      } else if (seg_meta.seg_type_ == ObVectorIndexSegmentType::INCR_MERGE) {
        if (OB_FAIL(ctx->add_segment_querier(
            seg_meta.segment_handle_, false/*is_incr*/, base_filter, is_pre_filter/*reverse_filter*/))) {
          LOG_WARN("add frozen segment querier fail", K(ret));
        }
      } else if (OB_FAIL(ctx->add_segment_querier(
          seg_meta.segment_handle_, true/*is_incr*/, ctx->ifilter_, true/*reverse_filter*/))) {
        LOG_WARN("add frozen segment querier fail", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.bases_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.bases_.at(i);
      if (! seg_meta.segment_handle_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment handle is invalid", K(ret), K(i), K(seg_meta), KPC(this));
      } else if (OB_FAIL(ctx->add_segment_querier(
          seg_meta.segment_handle_, false/*is_incr*/, base_filter, is_pre_filter/*reverse_filter*/))) {
        LOG_WARN("add frozen segment querier fail", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ctx->segments_.count(); ++i) {
    ObVectorIndexSegmentQuerier* segment_querier = ctx->segments_.at(i);
    handler.segment_querier_ = segment_querier;
    ObVsagQueryResult *res = nullptr;
    if (nullptr != segment_querier->filter_) {
      segment_querier->filter_->segment_handle_ = segment_querier->handle_;
    }
    if (OB_ISNULL(res = OB_NEWx(ObVsagQueryResult, ctx->tmp_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObVsagQueryResult));
    } else if (OB_FAIL(handler.knn_search(*res))) {
      LOG_WARN("knn search snap fail", K(ret));
    } else if (res->total_ > 0 && OB_FAIL(result_list.push_back(res))) {
      LOG_WARN("push back snap data fail", K(ret), KPC(res));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    int64_t actual_res_cnt = 0;
    uint64_t tmp_result_cnt = 0;
    uint64_t max_res_cnt = 0;
    for (int64_t i = 0; i < result_list.count(); ++i) tmp_result_cnt += result_list.at(i)->total_;
    /*
     *  for iter-filter or BQ, return all result of delta_res and snap_res, the results will be filter later and make sure final res is less than limit K
     *  iter-filter need sort in this function, BQ doesn't need
     */
    ObVectorIndexAlgorithmType index_type = get_snap_index_type();
    bool need_all_result = (query_cond->is_post_with_filter_ || index_type == VIAT_HNSW_BQ);
    if (need_all_result) {
      max_res_cnt = tmp_result_cnt;
    } else {
    // but for other situation, merge and make sure result is less than limit K, cuz its the final res
      max_res_cnt = tmp_result_cnt < query_cond->query_limit_ ? tmp_result_cnt : query_cond->query_limit_;
    }
    LOG_TRACE("query result info", K(tmp_result_cnt), K(max_res_cnt));

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
    } else if (query_cond->extra_column_count_ > 0) {
      char* buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(ctx->allocator_->alloc(extra_info_actual_size * max_res_cnt)))) { // can't use tmp allocator here, its final result of query
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator merge extra_info.", K(ret));
      } else if (OB_FAIL(merge_extra_info_ptr.init(ctx->allocator_, buf, extra_info_actual_size, max_res_cnt))) {
        LOG_WARN("failed to init merge_extra_info_ptr.", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (index_type == VIAT_HNSW_BQ) {
      if (OB_FAIL(ObPluginVectorIndexHelper::driect_merge_delta_and_snap_vids(
              result_list, actual_res_cnt, merge_vids, merge_distance, merge_extra_info_ptr))) {
        LOG_WARN("failed to merge delta and snap vids.", K(ret));
      }
    } else if (OB_FAIL(ObPluginVectorIndexHelper::sort_merge_delta_and_snap_vids(
        result_list, max_res_cnt, actual_res_cnt, merge_vids, merge_distance, merge_extra_info_ptr))) {
      LOG_WARN("failed to merge delta and snap vids.", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vids_iter->init(actual_res_cnt, merge_vids, merge_distance, merge_extra_info_ptr, ctx->allocator_))) {
      LOG_WARN("iter init failed.", K(ret), K(actual_res_cnt), K(merge_vids), K(merge_extra_info_ptr), K(ctx->allocator_));
    } else if (actual_res_cnt == 0) {
      LOG_INFO("query vector result 0", K(actual_res_cnt), K(tmp_result_cnt), K(result_list.count()),
          KP(query_vector), K(dim), K(query_cond->ef_search_), K(query_cond->query_limit_), K(ctx->segments_.count()), KPC(this));
    }
  }
  // TODO(ningxin.ning): remove here after sindi support setting allocator in knn_search
  // release memory
  if (is_sparse_vector_index_type()) {
    for (int64_t i = 0; i < result_list.count(); ++i) {
      ObVsagQueryResult *result = result_list.at(i);
      if (OB_NOT_NULL(result)) {
        if (OB_NOT_NULL(result->vids_)) {
          result->mem_ctx_->Deallocate((void *)result->vids_);
          result->vids_ = nullptr;
        }
        if (OB_NOT_NULL(result->distances_)) {
          result->mem_ctx_->Deallocate((void *)result->distances_);
          result->distances_ = nullptr;
        }
      }
    }
  }
  // ibitmap = nullptr;
  // dbitmap = nullptr;

  LOG_TRACE("now all_vsag_used is: ", K(ATOMIC_LOAD(all_vsag_use_mem_)));
  return ret;
}

// used only for query next result
int ObPluginVectorIndexAdaptor::query_next_result(ObVectorQueryAdaptorResultContext *ctx,
                                                  ObVectorQueryConditions *query_cond,
                                                  ObVectorQueryVidIterator *&vids_iter)
{
  INIT_SUCC(ret);
  vids_iter = nullptr;
  int64_t dim = 0;
  int64_t *merge_vids = nullptr;
  void *iter_buff = nullptr;
  float *query_vector;
  int64_t extra_info_actual_size = 0;

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
  } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
  } else if (OB_FALSE_IT(vids_iter = new(iter_buff) ObVectorQueryVidIterator(query_cond->extra_column_count_, extra_info_actual_size, query_cond->rel_count_, query_cond->rel_map_ptr_))) {
  } else {
    if (OB_NOT_NULL(ctx->bitmaps_)) {
      if (OB_FAIL(merge_and_generate_bitmap(ctx, *ctx->ifilter_, *ctx->dfilter_))) {
        LOG_WARN("failed to merge and generate bitmap.", K(ret));
      }
    }
    int64_t *merge_vids = nullptr;
    float *merge_distance = nullptr;
    ObVecExtraInfoPtr merge_extra_info_ptr;
    float valid_ratio = 1.0;
    ObVsagQueryResultArray result_list;
    ObVsagQueryResult delta_data;
    ObVsagQueryResult frozen_data;

    ObVectorIndexSegQueryHandler handler;
    handler.tenant_id_ =  tenant_id_;
    handler.query_cond_ = query_cond;
    handler.ctx_ = ctx;
    handler.extra_info_actual_size_ = extra_info_actual_size;
    handler.valid_ratio_ = valid_ratio;
    handler.query_vector_ = query_vector;
    handler.dim_ = dim;

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->segments_.count(); ++i) {
      ObVectorIndexSegmentQuerier* segment_querier = ctx->segments_.at(i);
      handler.segment_querier_ = segment_querier;
      if (nullptr != segment_querier->filter_) segment_querier->filter_->segment_handle_ = segment_querier->handle_;
      ObVsagQueryResult *res = nullptr;
      if (OB_ISNULL(res = OB_NEWx(ObVsagQueryResult, ctx->tmp_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObVsagQueryResult));
      } else if (OB_FAIL(handler.knn_search(*res))) {
        LOG_WARN("knn search snap fail", K(ret));
      } else if (res->total_ > 0 && OB_FAIL(result_list.push_back(res))) {
        LOG_WARN("push back snap data fail", K(ret), KPC(res));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t actual_res_cnt = 0;
      uint64_t max_res_cnt = 0;
      for (int64_t i = 0; i < result_list.count(); ++i) max_res_cnt += result_list.at(i)->total_;
      LOG_TRACE("query result info", K(max_res_cnt));

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
      } else if (query_cond->extra_column_count_ > 0) {
        char* buf = nullptr;
        if (OB_ISNULL(buf = static_cast<char *>(ctx->allocator_->alloc(extra_info_actual_size * max_res_cnt)))) { // can't use tmp allocator here, its final result of query
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocator merge extra_info.", K(ret));
        } else if (OB_FAIL(merge_extra_info_ptr.init(ctx->allocator_, buf, extra_info_actual_size, max_res_cnt))) {
          LOG_WARN("failed to init merge_extra_info_ptr.", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObPluginVectorIndexHelper::sort_merge_delta_and_snap_vids(result_list,
                                                                              query_cond->query_limit_,
                                                                              actual_res_cnt,
                                                                              merge_vids, merge_distance, merge_extra_info_ptr))) {
        LOG_WARN("failed to merge delta and snap vids.", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(vids_iter->init(actual_res_cnt, merge_vids, merge_distance, merge_extra_info_ptr, ctx->allocator_))) {
        LOG_WARN("iter init failed.", K(ret), K(actual_res_cnt), K(merge_vids), K(ctx->allocator_));
      } else if (actual_res_cnt == 0) {
        LOG_INFO("query vector result 0", K(actual_res_cnt), K(max_res_cnt), K(result_list.count()),
            KP(query_vector), K(dim), K(query_cond->ef_search_), K(query_cond->query_limit_), KPC(this));
      }
    }

  }
  return ret;
}

int ObPluginVectorIndexAdaptor::query_result(ObLSID &ls_id,
                                             ObVectorQueryAdaptorResultContext *ctx,
                                             ObVectorQueryConditions *query_cond,
                                             ObVectorQueryVidIterator *&vids_iter)
{
  INIT_SUCC(ret);
  vids_iter = nullptr;
  int64_t dim = 0;
  int64_t *merge_vids = nullptr;
  void *iter_buff = nullptr;
  float *query_vector;
  int64_t extra_info_actual_size = 0;

  if (OB_ISNULL(ctx) || OB_ISNULL(query_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx invalid.", K(ret));
  } else if (query_cond->query_limit_ <= 0 || query_cond->query_vector_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid query limit.", K(ret), K(query_cond->query_limit_));
  } else if (!is_sparse_vector_index_type() && OB_FAIL(get_dim(dim))) {
    LOG_WARN("get dim failed.", K(ret));
  } else if (!is_sparse_vector_index_type() && query_cond->query_vector_.length() / sizeof(float) != dim) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector objct unexpect.", K(ret), K(query_cond->query_vector_.length()), K(dim));
  } else if (OB_ISNULL(query_vector = reinterpret_cast<float *>(query_cond->query_vector_.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast vectors.", K(ret), K(query_cond->query_vector_));
  } else if (OB_ISNULL(iter_buff = ctx->allocator_->alloc(sizeof(ObVectorQueryVidIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator iter.", K(ret));
  } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
  } else if (OB_FALSE_IT(vids_iter = new(iter_buff) ObVectorQueryVidIterator(query_cond->extra_column_count_, extra_info_actual_size, query_cond->rel_count_, query_cond->rel_map_ptr_))) {
  }

  const bool need_load_data_from_table = (ctx->flag_ == PVQP_SECOND || !ctx->get_ls_leader()) ? true : false;
  if (OB_FAIL(ret)) {
  } else if (!need_load_data_from_table) {
    if (query_cond->only_complete_data_) {
      // do nothing
    } else if (OB_FAIL(vsag_query_vids(ctx, query_cond, dim, query_vector, vids_iter))) {
      LOG_WARN("failed to query vids.", K(ret), K(dim));
    }
  } else { // need load data
    if (OB_ISNULL(query_cond->row_iter_) || OB_ISNULL(query_cond->scan_param_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get snapshot table iter null.", K(ret),
        K(ctx->get_ls_leader()), KP(query_cond), KP(query_cond->row_iter_), KP(query_cond->scan_param_));
    } else {
      blocksstable::ObDatumRow *row = nullptr;
      ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(query_cond->row_iter_);
      if (OB_FAIL(table_scan_iter->get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          // when snap table is empty and adaptor mem data for snap is not empty, need to refresh adaptor
          int64_t current_snapshot_count = 0;
          if (OB_FAIL(get_snap_index_row_cnt(current_snapshot_count))) {
            LOG_WARN("fail to get snap index number", K(ret));
          } else if (current_snapshot_count > 0) {
            ctx->status_ = PVQ_REFRESH;
            LOG_INFO("query result need refresh adapter, ls leader",
                     K(ret), K(ls_id), K(snapshot_tablet_id_), K(get_snapshot_key_prefix()));
          } else {
            LOG_INFO("skip snap data refresh",
                     K(ret), K(ls_id), K(snapshot_tablet_id_), K(get_snapshot_key_prefix()));
          }
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid row", K(ret), K(row));
      } else if (row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
        ObString meta_data = row->storage_datums_[1].get_string();
        int64_t meta_scn = 0;
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(ctx->tmp_allocator_, ObLongTextType, true, meta_data, nullptr))) {
          LOG_WARN("read meta data fail", K(ret));
        } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
          LOG_WARN("get meta scn fail", K(ret));
        } else if (meta_scn > snap_data_->meta_.header_.scn_ || snap_data_->rb_flag_) {
          if (meta_scn > snap_data_->meta_.header_.scn_) {
            ctx->status_ = PVQ_REFRESH;
            LOG_INFO("query result need refresh adapter",
                K(ret), K(ls_id), K(ctx->get_ls_leader()), K(snapshot_tablet_id_), K(get_snapshot_key_prefix()), K(row->storage_datums_[0].get_string()),
                KP(this), K(meta_scn), K(snap_data_->meta_.header_.scn_));
          } else if (OB_FAIL(deserialize_snap_data(ls_id, query_cond, meta_data, meta_scn))) {
            LOG_WARN("failed to deserialize snap data", K(ret));
          }
        }
      } else if (OB_NOT_NULL(row) && row->get_column_count() == 3 && !row->storage_datums_[2].get_bool()) {
        // if table 5 has visible row, we should get key, data and visible row total 3 column
        ret = OB_SCHEMA_EAGAIN;
        LOG_INFO("row is invisible, maybe is doing async task, skip load", K(ret), KPC(row));
      } else if (get_snapshot_key_prefix().empty()
          || !row->storage_datums_[0].get_string().prefix_match(get_snapshot_key_prefix())
          || (is_snap_inited() && snap_data_->rb_flag_))
      {
        if (get_create_type() == CreateTypeComplete) {
          ctx->status_ = PVQ_REFRESH;
          LOG_INFO("query result need refresh adapter, ls leader", KP(this), K(snap_data_),
              K(ret), K(ls_id), K(ctx->get_ls_leader()), K(snapshot_tablet_id_), K(get_snapshot_key_prefix()), K(row->storage_datums_[0].get_string()));
        } else if (OB_FAIL(deserialize_snap_data(query_cond, row))) {
          LOG_WARN("failed to deserialize snap data", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (PVQ_REFRESH == ctx->status_) { // skip
    } else if (query_cond->only_complete_data_) {
      // do nothing
    } else if (OB_FAIL(vsag_query_vids(ctx, query_cond, dim, query_vector, vids_iter))) {
      LOG_WARN("failed to query vids.", K(ret), K(dim));
    } else {
      close_snap_data_rb_flag();
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (PVQ_REFRESH == ctx->status_) {
  } else if ((tmp_ret = check_if_need_optimize(ctx)) != OB_SUCCESS) {
    LOG_WARN("failed to check if vector index need optimize", K(tmp_ret));
  }

  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_data(ObVectorQueryConditions *query_cond, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  ObVectorIndexAlgorithmType index_type;
  ObString key_prefix;
  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(query_cond->row_iter_);
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObArenaAllocator allocator;
  if (OB_ISNULL(table_scan_iter) || OB_ISNULL(query_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer.", K(ret), K(table_scan_iter), K(query_cond));
  } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row", K(ret), K(row));
  } else if (OB_FAIL(ob_write_string(allocator, row->storage_datums_[0].get_string(), key_prefix))) {
    LOG_WARN("failed to write string", K(ret), K(row->storage_datums_[0].get_string()));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::iter_table_rescan(*query_cond->scan_param_, table_scan_iter))) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    ObHNSWDeserializeCallback::CbParam param;
    param.iter_ = query_cond->row_iter_;
    param.allocator_ = &tmp_allocator;
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    ObString target_prefix;
    if (!get_snapshot_key_prefix().empty() && key_prefix.prefix_match(get_snapshot_key_prefix()) && !snap_data_->rb_flag_) {
      // skip deserialize, already been deserialized by other concurrent thread
    } else if (OB_FAIL(snap_data_->load_segment(tenant_id_, this, param))) {
      LOG_WARN("deserialize index failed.", K(ret));
    } else if (OB_FALSE_IT(index_type = snap_data_->get_snap_index_type())) {
    } else if (OB_FAIL(ObPluginVectorIndexUtils::get_split_snapshot_prefix(index_type, key_prefix, target_prefix))) {
      LOG_WARN("fail to get split snapshot prefix", K(ret), K(index_type), K(key_prefix));
    } else if (OB_FAIL(set_snapshot_key_prefix(target_prefix))) {
      LOG_WARN("failed to set snapshot key prefix", K(ret), K(index_type), K(target_prefix));
    } else {
      snap_data_->set_inited();
      close_snap_data_rb_flag();
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_data(
    const ObLSID &ls_id, const share::SCN &scn,
    const ObString &meta_data, const int64_t meta_scn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
  if (meta_scn <= snap_data_->meta_.header_.scn_ && !snap_data_->rb_flag_) {
    // skip deserialize, already been deserialized by other concurrent thread
    LOG_INFO("skip deserialize", K(meta_scn), K(snap_data_), K(ls_id), K(scn));
  } else if (OB_FALSE_IT(snap_data_->free_memdata_resource(get_allocator(), tenant_id_))) {
  } else if (OB_FAIL(deserialize_snap_meta(meta_data))) {
    LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(snap_data_), K(ls_id), K(scn), K(meta_data.length()));
  } else if (OB_FAIL(snap_data_->load_persist_segments(this, tmp_allocator, ls_id, scn))) {
    LOG_WARN("load persist segments fail", K(ret));
  } else {
    snap_data_->set_inited();
    close_snap_data_rb_flag();
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_data(
    const ObLSID &ls_id, ObVectorQueryConditions *query_cond,
    const ObString &meta_data, const int64_t meta_scn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
  if (meta_scn <= snap_data_->meta_.header_.scn_ && !snap_data_->rb_flag_) {
    // skip deserialize, already been deserialized by other concurrent thread
    LOG_INFO("skip deserialize", K(meta_scn), K(snap_data_), K(ls_id));
  } else if (OB_FALSE_IT(snap_data_->free_memdata_resource(get_allocator(), tenant_id_))) {
  } else if (OB_FAIL(deserialize_snap_meta(meta_data))) {
    LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(snap_data_), K(ls_id), K(meta_data.length()));
  } else if (OB_FAIL(snap_data_->load_persist_segments(this, tmp_allocator,
      *query_cond->scan_param_, static_cast<ObTableScanIterator *>(query_cond->row_iter_)))) {
    LOG_WARN("load persist segments fail", K(ret), K(meta_scn));
  } else {
    snap_data_->set_inited();
    close_snap_data_rb_flag();
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::try_init_snap_for_deserialize(ObVectorIndexAlgorithmType actual_type)
{
  INIT_SUCC(ret);
  if (type_ == VIAT_HNSW_SQ || type_ == VIAT_HNSW_BQ) {
    if (actual_type == VIAT_HNSW_SQ || actual_type == VIAT_HNSW_BQ) {
      // actual create hnswsq index
      if (OB_FAIL(init_snap_data_without_lock())) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else if (actual_type == VIAT_HNSW || actual_type == VIAT_HGRAPH) {
      // actual create hnsw index
      if (OB_FAIL(init_snap_data_without_lock(actual_type))) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get serialize type invalid", K(ret), K(actual_type), K(type_));
    }
  } else if (type_ == VIAT_IPIVF_SQ) {
    if (actual_type == VIAT_IPIVF_SQ) {
      // actual create ipivf sq index
      if (OB_FAIL(init_snap_data_without_lock())) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else if (actual_type == VIAT_IPIVF) {
      // actual create ipivf index
      if (OB_FAIL(init_snap_data_without_lock(actual_type))) {
        LOG_WARN("failed to init snap mem data", K(ret), K(type_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get serialize type invalid", K(ret), K(actual_type), K(type_));
    }
  } else if (type_ == VIAT_HNSW || type_ == VIAT_HGRAPH || type_ == VIAT_IPIVF) {
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
    } else if (type == VIRT_EMBEDDED) {
      tablet_to_modify = &embedded_tablet_id_;
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
    } else if (type == VIRT_EMBEDDED) {
      table_id_to_modify = &embedded_table_id_;
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

void ObPluginVectorIndexAdaptor::set_data_table_id(ObVectorIndexSharedTableInfo &info)
{
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
   bool is_vaild = is_inc_tablet_valid()
                  && is_vbitmap_tablet_valid()
                  && is_snap_tablet_valid()
                  && is_data_tablet_valid()
                  && (vbitmap_table_id_ != OB_INVALID_ID)
                  && (inc_table_id_ != OB_INVALID_ID)
                  && (snapshot_table_id_ != OB_INVALID_ID);
  return is_hybrid_index() ? (is_vaild && is_embedded_tablet_valid() && (embedded_table_id_ != OB_INVALID_ID)) : is_vaild;
}

template<typename T>
static int ref_memdata(T *&dst_mem_data, T *&src_mem_data)
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

template<typename T>
int ObPluginVectorIndexAdaptor::merge_mem_data(ObVectorIndexRecordType type,
                                                ObPluginVectorIndexAdaptor *src_adapter,
                                                ObVectorIndexMemDataHandle<T> &src_mem_data,
                                                ObVectorIndexMemDataHandle<T> &dst_mem_data)
{
  // ToDo: may need lock or atomic access when replace dst mem data!
  int ret = OB_SUCCESS;
  bool is_same_mem_data = false;
  if (OB_ISNULL(src_adapter) || ! src_mem_data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null input", KP(src_adapter), K(src_mem_data), KR(ret));
  } else if ((this == src_adapter) || (src_mem_data.get() == dst_mem_data.get())) {
    is_same_mem_data = true;
  } else if ((dst_mem_data.is_valid() && dst_mem_data->is_inited())
             && src_mem_data->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict use of src_mem_data", K(type), K(src_mem_data), K(dst_mem_data), K(lbt()));
  }

  if (OB_FAIL(ret) || is_same_mem_data) {
    // do nothing
  } else if (src_mem_data->is_inited()) {
    dst_mem_data = src_mem_data;
  } else if (dst_mem_data.is_valid() && dst_mem_data->is_inited()) {
    // do nothing
  } else {
    // both mem data not used, decide by type
    if (((type == VIRT_INC) && (src_adapter->get_create_type() == CreateTypeInc))
        || ((type == VIRT_INC) && (src_adapter->get_create_type() == CreateTypeEmbedded))
        || ((type == VIRT_BITMAP) && (src_adapter->get_create_type() == CreateTypeBitMap))
        || ((type == VIRT_SNAP) && (src_adapter->get_create_type() == CreateTypeSnap))) {
      dst_mem_data = src_mem_data;
    } else if (! dst_mem_data.is_valid()) {
      // when full partial merge to complete
      dst_mem_data = src_mem_data;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(type), KPC(src_adapter), K(dst_mem_data), KR(ret));
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
      } else if (!partial_idx_adpt->is_hybrid_index() && OB_FAIL(merge_mem_data(VIRT_INC, partial_idx_adpt, partial_idx_adpt->incr_data_, incr_data_))){
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
      } else if (OB_FAIL(merge_mem_data(VIRT_BITMAP, partial_idx_adpt, partial_idx_adpt->vbitmap_data_, vbitmap_data_))){
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
      } else if (OB_FAIL(merge_mem_data(VIRT_SNAP, partial_idx_adpt, partial_idx_adpt->snap_data_, snap_data_))){
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      }
      if (OB_SUCC(ret) && !partial_idx_adpt->get_snapshot_key_prefix().empty()) {
        if (OB_FAIL(set_snapshot_key_prefix(partial_idx_adpt->get_snapshot_key_prefix()))) {
          LOG_WARN("failed to set index snapshot key prefix", KR(ret), K(*this), KPC(partial_idx_adpt));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (partial_idx_adpt->is_embedded_tablet_valid()) {
      if (OB_FAIL(set_tablet_id(VIRT_EMBEDDED, partial_idx_adpt->get_embedded_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_table_id(VIRT_EMBEDDED, partial_idx_adpt->get_embedded_table_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (OB_FAIL(set_tablet_id(VIRT_DATA, partial_idx_adpt->get_data_tablet_id()))) {
        LOG_WARN("partial vector index adapter not valid", K(partial_idx_adpt), K(*this), KR(ret));
      } else if (partial_idx_adpt->is_hybrid_index() && OB_FAIL(merge_mem_data(VIRT_INC, partial_idx_adpt, partial_idx_adpt->incr_data_, incr_data_))){
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
    if (OB_SUCC(ret)) {
      FLOG_INFO("[VECTOR INDEX ADAPTOR] merge parital adaptor success", KP(this), K_(create_type), KP(partial_idx_adpt));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::merge_incr_data(ObPluginVectorIndexAdaptor *other, const bool merge_frozen)
{
  int ret = OB_SUCCESS;
  if (! other->is_complete()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other adaptor is not complete", K(ret), KPC(other));
  } else {
    incr_data_ = other->incr_data_;
    vbitmap_data_ = other->vbitmap_data_;
    if (merge_frozen) {
      frozen_data_ = other->frozen_data_;
    }
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::inc_ref()
{
  int64_t ref_count = ATOMIC_AAF(&ref_cnt_, 1);
  //FLOG_INFO("[VECTOR INDEX REF] inc ref count", K(ref_count), KP(this), K_(create_type), K(lbt()));
}

bool ObPluginVectorIndexAdaptor::dec_ref_and_check_release()
{
  int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
  if (ref_count <= 0) {
    LOG_INFO("dec ref count", K(ref_count), KP(this), KPC(this), K(lbt()));
  }
  return (ref_count <= 0);
}

int ObPluginVectorIndexAdaptor::check_need_sync_to_follower_or_do_opt_task(ObPluginVectorIndexMgr *mgr, bool is_leader, bool &need_sync)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  need_sync = false;

  if (!is_complete()) {
    // do nothing
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no complete adapter need not sync memdata", K(*this), KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else  {
    // no get_index_number interface currently
    int64_t current_incr_count = 0;
    if (incr_data_->is_inited()) {
      if (OB_FAIL(get_inc_index_row_cnt_safe(current_incr_count))) {
        LOG_WARN("fail to get incr index number", K(ret));
        ret = OB_SUCCESS; // continue to check other parts
      }
    }

    int64_t current_bitmap_count = 0;
    if (vbitmap_data_->is_inited()) {
      if (OB_FAIL(get_vbitmap_row_cnt_safe(current_bitmap_count))) {
        LOG_WARN("fail to get vbitmap number", K(ret));
        ret = OB_SUCCESS; // continue to check other parts
      }
    }

    int64_t current_snapshot_count = 0;
    int64_t current_snap_incr_seg_cnt = 0;
    int64_t current_snap_incr_vec_cnt = 0;
    int64_t current_snap_base_seg_cnt = 0;
    int64_t current_snap_base_vec_cnt = 0;
    if (snap_data_->is_inited()) {
      TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
      current_snap_incr_seg_cnt = snap_data_->meta_.incrs_.count();
      current_snap_base_seg_cnt = snap_data_->meta_.bases_.count();
      if (OB_FAIL(get_snap_index_row_cnt(current_snapshot_count))) {
        LOG_WARN("fail to get snap index number", K(ret));
        ret = OB_SUCCESS; // continue to check other parts
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
        const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
        int64_t seg_vec_cnt = 0;
        if (! seg_meta.segment_handle_.is_valid()) { // skip
        } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(seg_vec_cnt))) {
          LOG_WARN("cal_distance_by_id fail", K(ret), K(i), K(seg_meta));
        } else {
          current_snap_incr_vec_cnt += seg_vec_cnt;
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.bases_.count(); ++i) {
        const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.bases_.at(i);
        int64_t seg_vec_cnt = 0;
        if (! seg_meta.segment_handle_.is_valid()) { // skip
        } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(seg_vec_cnt))) {
          LOG_WARN("cal_distance_by_id fail", K(ret), K(i), K(seg_meta));
        } else {
          current_snap_base_vec_cnt += seg_vec_cnt;
        }
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
    if (is_leader && OB_FAIL(check_can_sync_to_follower(mgr, current_snapshot_count, need_sync))) {
      LOG_WARN("fail to check can sync to follower", K(ret));
      ret = OB_SUCCESS;
    }

    if (need_sync) { // if need sync, update statistics, otherwise use current statistics and check next loop
      follower_sync_statistics_.incr_count_ = current_incr_count;
      follower_sync_statistics_.vbitmap_count_ = current_bitmap_count;
      follower_sync_statistics_.snap_count_ = current_snapshot_count;
      follower_sync_statistics_.snap_incr_seg_cnt_= current_snap_incr_seg_cnt;
      follower_sync_statistics_.snap_incr_vec_cnt_ = current_snap_incr_vec_cnt;
      follower_sync_statistics_.snap_base_seg_cnt_ = current_snap_base_seg_cnt;
      follower_sync_statistics_.snap_base_vec_cnt_ = current_snap_base_vec_cnt;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (tenant_config.is_valid()) {
        if (int64_t(tenant_config->ob_vector_index_merge_trigger_percentage) > 0) {
          follower_sync_statistics_.optimze_threashold_ = tenant_config->ob_vector_index_merge_trigger_percentage;
        }
        follower_sync_statistics_.use_new_check_ =
            tenant_config->_persist_vector_index_incremental
            && tenant_data_version >= DATA_VERSION_4_5_1_0;
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_if_need_optimize())) {
      LOG_WARN("failed to check if vector index need optimize", K(tmp_ret));
    } else {
      LOG_INFO("syninfo", K_(follower_sync_statistics), K_(need_be_optimized));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::check_can_sync_to_follower(ObPluginVectorIndexMgr *mgr, int64_t current_snapshot_count, bool &need_sync)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *snapshot_table_schema = NULL;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, get_snapshot_table_id(), snapshot_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(get_snapshot_table_id()));
  } else if (OB_ISNULL(snapshot_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("snapshot table not exist", K(ret), K(snapshot_table_schema));
  } else {
    if (need_sync && !snapshot_table_schema->can_read_index()) {
      need_sync = false;
      LOG_INFO("snapshot table not ready, not need sync to follower", KPC(this));
    }
    if (current_snapshot_count == 0 && snapshot_table_schema->can_read_index()) {
      if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(get_inc_tablet_id(), get_inc_table_id()))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
        }
      }
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

int64_t ObPluginVectorIndexAdaptor::get_incr_vsag_mem_used()
{
  int64_t size = 0;
  if (incr_data_.is_valid() && incr_data_->is_inited()) {
    size = incr_data_->segment_handle_->get_mem_used();
  }
  return size;
}

int64_t ObPluginVectorIndexAdaptor::get_incr_vsag_mem_hold()
{
  int64_t size = 0;
  if (incr_data_.is_valid() && incr_data_->is_inited()) {
    size = incr_data_->segment_handle_->get_mem_hold();
  }
  return size;
}

int64_t ObPluginVectorIndexAdaptor::get_snap_vsag_mem_used()
{
  int64_t size = 0;
  if (snap_data_.is_valid() && snap_data_->is_inited()) {
    size = snap_data_->get_snap_mem_used();
  }
  return size;
}

int64_t ObPluginVectorIndexAdaptor::get_snap_vsag_mem_hold()
{
  int64_t size = 0;
  if (snap_data_.is_valid() && snap_data_->is_inited()) {
    size = snap_data_->get_snap_mem_hold();
  }
  return size;
}

int ObPluginVectorIndexAdaptor::get_vid_bound(ObVidBound &bound)
{
  INIT_SUCC(ret);
  // get incr and snap data bound
  int64_t min_vid = INT64_MAX;
  int64_t max_vid = 0;
  if (incr_data_->is_inited()) {
    incr_data_->segment_handle_->get_read_bound_vid(max_vid, min_vid);
  }
  if (snap_data_->is_inited()) {
    int64_t tmp_min_vid = INT64_MAX;
    int64_t tmp_max_vid = 0;
    snap_data_->get_read_bound_vid(tmp_max_vid, tmp_min_vid);
    max_vid = max_vid > tmp_max_vid ? max_vid : tmp_max_vid;
    min_vid = min_vid < tmp_min_vid ? min_vid : tmp_min_vid;
  }
  if (max_vid < min_vid) {
    // invalid range, just set to [0, INT64_MAX]
    min_vid = 0;
    max_vid = INT64_MAX;
  }
  bound.min_vid_ = min_vid;
  bound.max_vid_ = max_vid;
  return ret;
}

int ObPluginVectorIndexAdaptor::get_inc_index_row_cnt_safe(int64_t &count)
{
  TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
  return get_inc_index_row_cnt(count);
}

int ObPluginVectorIndexAdaptor::get_inc_index_row_cnt(int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (! incr_data_.is_valid() || ! incr_data_->segment_handle_.is_valid()) {
    LOG_INFO("incr data is null", K(incr_data_));
  } else if (OB_FAIL(incr_data_->segment_handle_->get_index_number(count))) {
    LOG_WARN("failed to get inc index number.", K(ret));
  } else {
    LOG_DEBUG("succ to get inc index row cnt", K(ret), K(count));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::get_vbitmap_row_cnt_safe(int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  TCRLockGuard lock_guard(vbitmap_data_->bitmap_rwlock_);
  if (OB_NOT_NULL(vbitmap_data_->bitmap_)) {
    if (OB_NOT_NULL(vbitmap_data_->bitmap_->delete_bitmap_)) {
      count += roaring64_bitmap_get_cardinality(vbitmap_data_->bitmap_->delete_bitmap_);
    }
    if (OB_NOT_NULL(vbitmap_data_->bitmap_->insert_bitmap_)) {
      count += roaring64_bitmap_get_cardinality(vbitmap_data_->bitmap_->insert_bitmap_);
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::get_snap_vbitmap_cnt_safe(int64_t &insert_count, int64_t &delete_count)
{
  int ret = OB_SUCCESS;
  insert_count = 0;
  delete_count = 0;
  if (OB_SUCC(ret) && is_snap_inited()) {
    TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    for (int64_t i = 0; i < snap_data_->meta_.incrs_.count(); ++i) {
      const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
      const ObVectorIndexSegmentHandle &segment_handle = seg_meta.segment_handle_;
      if (segment_handle.is_valid() && OB_NOT_NULL(segment_handle->vbitmap_)) {
        if (OB_NOT_NULL(segment_handle->vbitmap_->delete_bitmap_)) {
          insert_count += roaring64_bitmap_get_cardinality(segment_handle->vbitmap_->delete_bitmap_);
        }
        if (OB_NOT_NULL(segment_handle->vbitmap_->insert_bitmap_)) {
          delete_count += roaring64_bitmap_get_cardinality(segment_handle->vbitmap_->insert_bitmap_);
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::get_snap_index_row_cnt_safe(int64_t &count)
{
  TCRLockGuard lock_guard(snap_data_->mem_data_rwlock_);
  return get_snap_index_row_cnt(count);
}

int ObPluginVectorIndexAdaptor::get_snap_index_row_cnt(int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (! snap_data_.is_valid()) {
    LOG_INFO("incr data is null", K(snap_data_));
  } else if (OB_FAIL(snap_data_->get_snap_index_row_cnt(count))) {
    LOG_WARN("get incr data vec count fail", K(ret), K(snap_data_));
  } else {
    LOG_DEBUG("snap index total count", K(count));
  }
  return ret;
}

void ObHnswBitmapFilter::reset()
{
  // release memory
  if (OB_NOT_NULL(bitmap_)) {
    if (type_ == FilterType::ROARING_BITMAP) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPR"));
      roaring::api::roaring64_bitmap_free(roaring_bitmap_);
    } else {
      if (OB_NOT_NULL(allocator_)) {
        allocator_->free(bitmap_);
      }
    }
  }
  if (OB_NOT_NULL(valid_vid_) && OB_NOT_NULL(allocator_)) {
    OB_DELETEx(ObVecIdxVidArray, allocator_, valid_vid_);
  }
  // reset members
  type_ = FilterType::BYTE_ARRAY;
  capacity_ = 0;
  base_ = 0;
  valid_cnt_ = 0;
  allocator_ = nullptr;
  bitmap_ = nullptr;
  rk_range_.reset();
  selectivity_ = 0;
  segment_handle_.reset();
  tmp_alloc_.reset();
  extra_buffer_ = nullptr;
  tmp_objs_ = nullptr;
  extra_in_rowkey_idxs_ = nullptr;
}

bool ObHnswBitmapFilter::test(int64_t id)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (type_ == FilterType::ROARING_BITMAP) {
    bret = roaring::api::roaring64_bitmap_contains(roaring_bitmap_, id);
  } else if (type_ == FilterType::BYTE_ARRAY) {
    if (id >= base_ && id - base_ < capacity_) {
      int64_t real_idx = id - base_;
      bret = ((bitmap_[real_idx >> 3] & (0x1 << (real_idx & 0x7))));
    }
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    if (segment_handle_.is_valid()) {
      int64_t extra_info_actual_size = valid_cnt_;
      int64_t extra_column_cnt = rk_range_.at(0)->get_start_key().get_obj_cnt();
      if (OB_FAIL(segment_handle_->get_extra_info_by_ids(&id, 1, extra_buffer_))) {
        LOG_WARN("fail to get extra info by id", K(ret), K(id));
      } else {
        bret = test(reinterpret_cast<const char*>(extra_buffer_));
      }
    }
  }
  return bret;
}

bool ObHnswBitmapFilter::test(const char* data)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (type_ == FilterType::SIMPLE_RANGE) {
    int64_t extra_info_actual_size = valid_cnt_;
    int64_t extra_column_cnt = rk_range_.at(0)->get_start_key().get_obj_cnt();
    if (OB_FAIL(ObVecExtraInfo::extra_buf_to_obj(data, extra_info_actual_size * extra_column_cnt, extra_column_cnt, tmp_objs_, extra_in_rowkey_idxs_))) {
      LOG_WARN("failed to decode extra info array.", K(ret), K(extra_info_actual_size));
    } else {
      ObRowkey tmp_rk(tmp_objs_, extra_column_cnt);
      ObNewRange tmp_range;
      if (OB_FAIL(tmp_range.build_range(rk_range_.at(0)->table_id_, tmp_rk))) {
        LOG_WARN("fail to build tmp range", K(ret));
      }
      // do compare
      for (int64_t i = 0; i < rk_range_.count() && !bret && OB_SUCC(ret); i++) {
        if (rk_range_.at(i)->compare_with_startkey2(tmp_range) <= 0 && rk_range_.at(i)->compare_with_endkey2(tmp_range) >= 0) {
          bret = true;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type for ex info filter", K(ret), K(type_));
  }
  return bret;
}
int ObHnswBitmapFilter::init(const int64_t &min, const int64_t &max, bool is_sparse_vector_index)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(bitmap_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (max < min || min < 0 || max < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vid bound", K(ret), K(max), K(min));
  } else {
    capacity_ = max - min;
    capacity_ = (capacity_ + 7) / 8 * 8;
    base_ = min;
    if (capacity_ > NORMAL_BITMAP_MAX_SIZE || capacity_ == 0) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPR"));
      ROARING_TRY_CATCH(roaring_bitmap_ = roaring::api::roaring64_bitmap_create());
      if (OB_SUCC(ret)) {
        type_ = FilterType::ROARING_BITMAP;
      }
    } else {
      if (OB_ISNULL(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator is nullptr", K(ret));
      } else if (OB_ISNULL(bitmap_ = static_cast<uint8_t*>(allocator_->alloc(sizeof(uint8_t) * capacity_ / 8)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create normal bitmap", K(ret), K(capacity_));
      } else {
        memset(bitmap_, 0, sizeof(uint8_t) * capacity_ / 8);
        type_ = FilterType::BYTE_ARRAY;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_sparse_vector_index) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr for sparse vector index", K(ret));
  } else if (OB_ISNULL(valid_vid_ = OB_NEWx(ObVecIdxVidArray, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc valid_vid_ array", K(ret));
  } else {
    valid_vid_->set_attr(ObMemAttr(tenant_id_, "VecIdxIPIVF"));
  }
  return ret;
}

int ObHnswBitmapFilter::init(void *adaptor, double selectivity, const ObIArray<const ObNewRange *> &range,
                             const sql::ExprFixedArray &rowkey_exprs, const ObIArray<int64_t> &extra_in_rowkey_idxs)
{
  int ret = OB_SUCCESS;
  if (rk_range_.count() != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(rk_range_));
  } else if (range.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rk range", K(ret), K(range));
  } else if (OB_FAIL(rk_range_.assign(range))) {
    LOG_WARN("fail to assign rt st", K(ret));
  } else {
    type_ = FilterType::SIMPLE_RANGE;
    adaptor_ = adaptor;
    selectivity_ = selectivity;
    int64_t extra_info_actual_size = 0;
    int64_t extra_column_cnt = rk_range_.at(0)->get_start_key().get_obj_cnt();
    ObPluginVectorIndexAdaptor *adaptor = static_cast<ObPluginVectorIndexAdaptor*>(adaptor_);
    if (OB_FAIL(adaptor->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("failed to get extra info actual size.", K(ret));
    } else if (extra_column_cnt == 0 || extra_info_actual_size == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid extra column cnt or size", K(ret), K(extra_column_cnt), K(extra_info_actual_size));
    } else if (rowkey_exprs.count() != extra_column_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_exprs count is not equal to extra_column_count", K(ret), K(rowkey_exprs), K(extra_column_cnt));
    } else if (OB_ISNULL(extra_buffer_ = static_cast<char*>(tmp_alloc_.alloc(extra_info_actual_size * extra_column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("falied to alloc extra buffer", K(ret));
    } else if (OB_ISNULL(tmp_objs_ = static_cast<ObObj*>(tmp_alloc_.alloc(sizeof(ObObj) * extra_column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("falied to alloc extra obobj", K(ret));
    } else if (OB_FALSE_IT(extra_in_rowkey_idxs_ = &extra_in_rowkey_idxs)) {
    } else {
      for (int64_t i = 0; i < extra_column_cnt; i++) {
        tmp_objs_[i].set_meta_type(rowkey_exprs.at(i)->obj_meta_); // set meta
      }
      valid_cnt_ = extra_info_actual_size;
      // init valid_vid_ for sparse vector index
      if (OB_NOT_NULL(adaptor_)) {
        ObPluginVectorIndexAdaptor *sparse_adaptor = static_cast<ObPluginVectorIndexAdaptor*>(adaptor_);
        if (sparse_adaptor->is_sparse_vector_index_type()) {
          if (OB_ISNULL(allocator_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("allocator is nullptr for sparse vector index", K(ret));
          } else if (OB_ISNULL(valid_vid_ = OB_NEWx(ObVecIdxVidArray, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc valid_vid_ array", K(ret));
          } else {
            valid_vid_->set_attr(ObMemAttr(tenant_id_, "VecIdxIPIVF"));
          }
        }
      }
    }
  }
  return ret;
}

int ObHnswBitmapFilter::upgrade_to_roaring_bitmap()
{
  int ret = OB_SUCCESS;
  roaring::api::roaring64_bitmap_t *new_bitmap = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPR"));
  ROARING_TRY_CATCH(new_bitmap = roaring::api::roaring64_bitmap_create());
  if (OB_SUCC(ret) && OB_ISNULL(new_bitmap)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create insert bitmap", K(ret));
  } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
    new_bitmap = nullptr;
  }
  for (uint64_t i = 0; i < capacity_ / 8 && OB_SUCC(ret); i++) {
    if (bitmap_[i]) {
      for (uint64_t j = 0; j < 8 && OB_SUCC(ret); j++) {
        if (bitmap_[i] & (1 << j)) {
          uint64_t val = i * 8 + j + base_;
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(new_bitmap, val));
        }
      }
    }
  }
  // release bitmap when fail
  if (OB_FAIL(ret) && OB_NOT_NULL(new_bitmap)) {
    roaring::api::roaring64_bitmap_free(new_bitmap);
  }
  if (OB_SUCC(ret)) {
    type_ = FilterType::ROARING_BITMAP;
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(bitmap_);
    }
    roaring_bitmap_ = new_bitmap;
  }
  return ret;
}

int ObHnswBitmapFilter::add(int64_t id)
{
  int ret = OB_SUCCESS;
  if (type_ == FilterType::BYTE_ARRAY && (id < base_ || id - base_ >= capacity_)) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null bitmap", K(ret));
    } else if (OB_FAIL(upgrade_to_roaring_bitmap())) {
      LOG_WARN("fail to upgrade to roaring bitmap", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (type_ == FilterType::ROARING_BITMAP) {
    ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(roaring_bitmap_, id));
  } else if (type_ == FilterType::BYTE_ARRAY) {
    int64_t real_idx = id - base_;
    bitmap_[real_idx >> 3] |= uint8_t(0x1 << (real_idx & 0x7));
    valid_cnt_++; // expect there is no dup id add
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("simple range not support add", K(ret));
  }
  // add valid id for sparse vector index
  if (OB_SUCC(ret) && OB_NOT_NULL(valid_vid_)) {
    if (OB_FAIL(valid_vid_->push_back(id))) {
      LOG_WARN("failed to push back vid to valid_vid_", K(ret), K(id));
    }
  }
  return ret;
}

int ObHnswBitmapFilter::get_valid_cnt()
{
  int ret = 0;
  if (type_ == FilterType::ROARING_BITMAP) {
    ret = roaring64_bitmap_get_cardinality(roaring_bitmap_);
  } else if (type_ == FilterType::BYTE_ARRAY) {
    ret = valid_cnt_;
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ret = selectivity_ > 0 ? 1 : 0;
  }
  return ret;
}

float ObHnswBitmapFilter::get_valid_ratio(int64_t total_cnt)
{
  float ratio = 1.0f;
  if (type_ == FilterType::ROARING_BITMAP) {
    int valid_cnt = get_valid_cnt();
    if (total_cnt > 0) ratio = (float)valid_cnt / (float)total_cnt;
  } else if (type_ == FilterType::BYTE_ARRAY) {
    int valid_cnt = get_valid_cnt();
    if (total_cnt > 0) ratio = (float)valid_cnt / (float)total_cnt;
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    //ratio = selectivity_;
    ratio = 0.18f; // in-filter fixed valid ratio
  }
  return ratio;
}

bool ObHnswBitmapFilter::is_subset(roaring::api::roaring64_bitmap_t *bitmap)
{
  bool bret = true;
  if (type_ == FilterType::ROARING_BITMAP) {
    bret = roaring64_bitmap_is_subset(roaring_bitmap_, bitmap);
  } else if (type_ == FilterType::BYTE_ARRAY) {
    for (uint64_t i = 0; i < capacity_ / 8 && bret; i++) {
      if (bitmap_[i]) {
        for (uint64_t j = 0; j < 8 && bret; j++) {
          if (bitmap_[i] & (1 << j)) {
            uint64_t id = i * 8 + j + base_;
            bret = roaring64_bitmap_contains(bitmap, id);
          }
        }
      }
    }
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    bret = true; // TODO mock as true or false?
  }
  return bret;
}

// !!!!! NOTICE
// This function will throw an exception when memory allocation fails,
// so it can only be called within vsag and cannot be used elsewhere
void *ObVsagSearchAlloc::Allocate(size_t size)
{
  void *ret_ptr = nullptr;

  if (size != 0) {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + size;

    void *ptr = alloc_.alloc(actual_size);
    if (OB_NOT_NULL(ptr)) {
      *(int64_t*)ptr = actual_size;
      ret_ptr = (char*)ptr + MEM_PTR_HEAD_SIZE;
    } else {
      // NOTICE: ObVsagSearchAlloc is used in vsag lib. And may be used for some c++ std container.
      // For this scenario, if memory allocation fails, an exception should be thrown instead of returning a null pointer
      // or will access null point in std container, eg std::vector
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to allocate memory", K(size), K(actual_size), K(lbt()));
      throw std::bad_alloc();
    }
  }

  return ret_ptr;
}

void *ObVsagSearchAlloc::Reallocate(void* p, size_t size)
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

// NOTINCE: thread unsafe function
int ObPluginVectorIndexAdaptor::init_incr_data(ObVecIdxActiveDataHandle &mem_data, ObVectorIndexAlgorithmType enforce_type, ObVectorIndexParam *param)
{
  int ret = OB_SUCCESS;
  if (! mem_data.is_valid() || OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem data or param is null", K(ret));
  } else if (mem_data->is_inited()) {
    LOG_INFO("mem data is already inited, no need to init", K(lbt()));
  } else {
    ObVectorIndexAlgorithmType build_type = enforce_type == VIAT_MAX ? param->type_ : enforce_type;
    // Note. sq/bq must use hgraph to build incr index.
    build_type = build_type == VIAT_HNSW_SQ || build_type == VIAT_HNSW_BQ ? VIAT_HGRAPH : build_type;
    // Note. ipivf_sq must use ipivf to build incr index.
    build_type = build_type == VIAT_IPIVF_SQ ? VIAT_IPIVF : build_type;
    if (OB_FAIL(ObVectorIndexSegment::create(
        mem_data->segment_handle_,
        tenant_id_,
        *get_allocator(),
        *param,
        build_type,
        param->m_,
        this))) {
      LOG_WARN("create segment fail", K(ret));
    } else if (OB_ISNULL(mem_data->segment_handle_->ibitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, get_allocator(), get_allocator(), tenant_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create incr bitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
    } else if (OB_FAIL(mem_data->segment_handle_->ibitmap_->init(true, false))) {
      LOG_WARN("init bitmap fail", K(ret));
    } else {
      mem_data->set_inited(); // should release memory if fail
      LOG_INFO("create incr index success.", K(ret), KP(this), K(mem_data), K(lbt()));
    }
  }
  if (OB_FAIL(ret)) {
    mem_data->free_memdata_resource(get_allocator(), tenant_id_);
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::create_vbitmap_data(ObVecIdxVBitmapDataHandle &mem_data)
{
  int ret = OB_SUCCESS;
  ObVecIdxVBitmapDataHandle res;
  if (mem_data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem data is not null", K(ret));
  } else if (OB_FAIL(init_mem(res))) {
    LOG_WARN("failed to init mem data mem ctx.", K(ret));
  } else if (OB_FAIL(init_vbitmap_data(res))) {
    LOG_WARN("failed to init vbitmap data", K(ret));
  }

  if (OB_FAIL(ret)) {
    res.reset();
  } else {
    mem_data = res;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_vbitmap_data(ObVecIdxVBitmapDataHandle &mem_data)
{
  int ret = OB_SUCCESS;
  if (! mem_data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem data is null", K(ret));
  } else if (mem_data->is_inited()) {
    LOG_INFO("vbitmap data is already inited, no need to init", K(lbt()));
  } else if (OB_ISNULL(mem_data->bitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, get_allocator(), get_allocator(), tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
  } else if (OB_FAIL(mem_data->bitmap_->init(true, true))) {
    LOG_WARN("init vbitmap fail", K(ret));
  } else{
    mem_data->set_inited();
    LOG_INFO("init vbitmap data success", K(mem_data));
  }
  if (OB_FAIL(ret)) {
    mem_data->free_memdata_resource(get_allocator(), tenant_id_);
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_snap_data_for_build(const bool is_init_bitmap)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *param = nullptr;
  if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (OB_FAIL(init_snap_data_for_build(param, VIAT_HNSW_SQ == param->type_ || VIAT_HNSW_BQ == param->type_))) {
    LOG_WARN("init snap data fail", K(ret));
  } else if (is_init_bitmap && OB_ISNULL(snap_data_->builder_->ibitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, get_allocator(), get_allocator(), tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create ibitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
  } else if (is_init_bitmap && OB_ISNULL(snap_data_->builder_->vbitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, get_allocator(), get_allocator(), tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create vbitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
  } else {
    snap_data_->builder_->seg_type_ = ObVectorIndexSegmentType::BASE;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::init_snap_data_for_build(
    ObVectorIndexParam *param,
    const bool is_buffer_mode)
{
  int ret = OB_SUCCESS;
  if (! snap_data_.is_valid() || OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem data is null", K(ret), K(snap_data_), KP(param));
  } else if (is_sanp_builder_inited()) {
    LOG_DEBUG("mem data is already inited, no need to init", K(snap_data_), K(lbt()));
    // ddl will retry, in this case, builder is not null, but buffer is empty
    // init_vec_buffer will double check again, so call it once no matter what
    if (is_buffer_mode && OB_FAIL(snap_data_->builder_->init_vec_buffer(
        tenant_id_, *get_allocator(), is_sparse_vector_index_type()))) {
      LOG_WARN("init buffer mode snap data fail", K(ret));
    }
  } else if (OB_FAIL(inner_init_snap_builder(param, is_buffer_mode))) {
    LOG_WARN("failed to init snap builder", K(ret));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::inner_init_snap_builder(ObVectorIndexParam *param, const bool is_buffer_mode) {
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
  if (is_sanp_builder_inited()) { // double check for concurent
    LOG_INFO("snap is inited, so skip", K(snap_data_), K(lbt()));
  // ddl may be retry, so release meta data that is previously built
  } else if (snap_data_->meta_.is_valid() && OB_FALSE_IT(snap_data_->meta_.release(get_allocator(), tenant_id_))) {
  } else if (OB_ISNULL(snap_data_->builder_ = OB_NEWx(ObVectorIndexSegmentBuilder, get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObVectorIndexSegmentBuilder));
  } else if (is_buffer_mode) {
    if (OB_FAIL(snap_data_->builder_->init_vec_buffer(
        tenant_id_, *get_allocator(), is_sparse_vector_index_type()))) {
      LOG_WARN("init buffer mode snap data fail", K(ret));
    } else {
      snap_data_->builder_->seg_type_ = ObVectorIndexSegmentType::BASE;
      snap_data_->rb_flag_ = false;
      snap_data_->set_inited();
      LOG_INFO("create snap data success.", K(ret), K(snap_data_), K(lbt()));
    }
  } else if (VIAT_HNSW_SQ == param->type_ || VIAT_HNSW_BQ == param->type_ || VIAT_IPIVF_SQ == param->type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index must be buffer mode build", K(ret), KPC(param));
  } else if (OB_FAIL(ObVectorIndexSegment::create(snap_data_->builder_->segment_handle_,
      tenant_id_,
      *get_allocator(),
      *param,
      param->type_,
      param->m_,
      this))) {
    LOG_WARN("failed to create vsag index.", K(ret));
  } else {
    snap_data_->builder_->seg_type_ = ObVectorIndexSegmentType::BASE;
    snap_data_->rb_flag_ = false;
    snap_data_->set_inited();
    LOG_INFO("create snap data success.", K(ret), K(snap_data_), K(lbt()));
  }
  return ret;
}

// 需要读取incr_data, 冻结检查不会并发, 但是incr_data可能没有被初始化
// 如果DML线程写入和冻结线程同时访问incr_data, 写入线程会触发incr_data的初始化
// 但是冻结本身不会修改incr_data, 并且incr_data只会在adaptor销毁会才会被释放
// 所以这里预期不需要加读锁, 这样也可以避免阻塞写入
// TCRLockGuard lock_guard(incr_data_->mem_data_rwlock_);
int ObPluginVectorIndexAdaptor::check_need_freeze(const int64_t freeze_threshold, bool &need_freeze)
{
  int ret = OB_SUCCESS;
  int64_t active_segment_mem = 0;
  int64_t vec_cnt = 0;
  need_freeze = false;
  int64_t amplification_factor = 1;
  if (! incr_data_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incr_data_ is null", K(ret), KP(this));
  } else if (! incr_data_->is_inited()) {
    LOG_TRACE("incr_data_ is not inited, no need to freeze", K(ret), KP(this), K_(snap_data));
  } else if (has_frozen()) {
    LOG_INFO("frozen data has frozen, can not freeze again", K(ret), K_(frozen_data));
  // 冻结不会并发, 并且只有冻结线程会将segment改为null
  // 所以这里不需要考虑其他线程并发修改, 故不需要加锁
  } else if (is_in_opt_task_) {
    LOG_INFO("there is rebuild task doing, can not freeze", KPC(this));
  } else if (is_snap_inited() && snap_data_->rb_flag_) {
    LOG_INFO("snap data is not loaded, can not freeze", KPC(this));
  } else if (! incr_data_->segment_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incr segment is null", KR(ret), KP(this), K_(incr_data));
  } else if (snap_data_.is_valid() && snap_data_->meta_.segment_count() >= OB_VECTOR_INDEX_MAX_SEGMENT_CNT) {
    LOG_WARN("too many segments, temporarily suspend freezing", K(incr_data_), K(snap_data_));
  } else if (OB_FAIL(incr_data_->segment_handle_->get_index_number(vec_cnt))) {
    LOG_WARN("failed to get index number.", K(ret), K(incr_data_));
  } else if (OB_FALSE_IT(active_segment_mem = incr_data_->segment_handle_->get_mem_hold())) {
  // 如果超过3个, 说明合并慢了, 此时需要减缓冻结
  } else if (snap_data_.is_valid() && snap_data_->meta_.incrs_.count() > 3 && OB_FALSE_IT(amplification_factor = snap_data_->meta_.incrs_.count() - 3)) {
  } else if (vec_cnt > 0 && active_segment_mem >= freeze_threshold * amplification_factor) {
    need_freeze = true;
    LOG_INFO("trigger freeze", K(vec_cnt), K(active_segment_mem), K(freeze_threshold), K(amplification_factor));
  } else {
    LOG_INFO("active segment no need to freeze", KP(this), K_(incr_data), K(vec_cnt), K(active_segment_mem), K(freeze_threshold), K(amplification_factor));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::check_can_freeze(ObLSID ls_id, const int64_t freeze_threshold, bool &can_freeze)
{
  int ret = OB_SUCCESS;
  bool need_freeze = false;
  can_freeze = false;
  if (OB_FAIL(check_need_freeze(freeze_threshold, need_freeze))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incr_data_ is null", K(ret), KP(this));
  } else if (need_freeze) {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObNewRowIterator *delta_buf_iter = nullptr;
    ObAccessService *tsc_service = MTL(ObAccessService *);
    storage::ObTableScanParam inc_scan_param;
    schema::ObTableParam inc_table_param(tmp_allocator);
    int64_t extra_info_actual_size = 0;
    SCN read_scn = SCN::min_scn();
    schema::ObIndexType delta_type = is_hybrid_index()? INDEX_TYPE_HYBRID_INDEX_LOG_LOCAL: INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;
    if (OB_FAIL(ObPluginVectorIndexUtils::get_read_scn(true/*is_leader*/, ls_id, read_scn))) {
      LOG_WARN("fail to get read scn", KR(ret), K(ls_id));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id,
                                  this,
                                  read_scn,
                                  delta_type,
                                  tmp_allocator,
                                  tmp_allocator,
                                  inc_scan_param,
                                  inc_table_param,
                                  delta_buf_iter))) {
      LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(delta_type));
    } else if (OB_FAIL(get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("fail to get extra info actual size", K(ret));
    } else {
      int64_t extra_info_column_count =
          extra_info_actual_size > 0
              ? ObPluginVectorIndexUtils::get_extra_column_count(inc_table_param, delta_type)
              : 0;
      ObVectorQueryAdaptorResultContext ada_ctx(tenant_id_, extra_info_column_count, &tmp_allocator, &tmp_allocator);
      ada_ctx.set_scn(read_scn);
      if (OB_FAIL(check_delta_buffer_table_readnext_status(&ada_ctx, delta_buf_iter, read_scn))) {
        LOG_WARN("fail to check_delta_buffer_table_readnext_status.", K(ret));
      } else if (OB_FAIL(ObPluginVectorIndexUtils::try_sync_vbitmap_memdata(ls_id, this, read_scn, tmp_allocator, ada_ctx))) {
        LOG_WARN("failed to sync vbitmap", KR(ret));
      } else if (ada_ctx.get_status() == PVQ_COM_DATA) {
        ret = OB_EAGAIN;
        LOG_WARN("need complete data, so can not freeze, retry later", K(ret), K(ls_id), K(read_scn), KPC(this));
      } else {
        can_freeze = true;
        LOG_INFO("can freeze active segment", K(ls_id), KPC(this));
      }
    }
    if (OB_NOT_NULL(delta_buf_iter) && OB_NOT_NULL(tsc_service)) {
      int tmp_ret = tsc_service->revert_scan_iter(delta_buf_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert delta_buf_iter failed", K(tmp_ret));
      }
      delta_buf_iter = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::check_need_merge(const int64_t merge_base_percentage, bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  if (! snap_data_.is_valid() || ! snap_data_->is_inited()) {
    LOG_TRACE("snap is not init, so no need merge", K(ret), KPC(this));
  } else if (snap_data_->rb_flag_) {
    LOG_INFO("snap data is not loaded, can not merge", KPC(this));
  } else if (snap_data_->meta_.segment_count() <= 1) {
    LOG_WARN("too few segments, no need merge", K(ret), KPC(this));
  } else if (snap_data_->meta_.incrs_.count() < 2
      && ! snap_data_->check_incr_mem_over_percentage(merge_base_percentage)) {
    LOG_TRACE("[VECTOR INDEX MERGE] incrs is too small, so no need merge", K(ret), KPC(this));
  } else if (snap_data_->meta_.incrs_.count() < 2
      && ! snap_data_->check_incr_can_merge_base()) {
    LOG_INFO("[VECTOR INDEX MERGE] incr bitmap is not complete, so no need merge", K(ret), KPC(this));
  } else if (is_in_opt_task()) {
    LOG_TRACE("[VECTOR INDEX MERGE] rebuild task is doing, so no need merge", K(ret), KPC(this));
  } else {
    need_merge = true;
    LOG_INFO("current vector index need merge", KPC(this));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::freeze_active_segment(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool need_freeze = false;
  share::SCN frozen_scn;
  ObVectorIndexSegmentHandle new_segment;
  // vbitmap_data可能还未初始化
  if (OB_FAIL(try_init_mem_data(VIRT_BITMAP))) {
    LOG_WARN("failed to init vbitmap mem data", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_current_read_scn(frozen_scn))) {
    LOG_WARN("fail to get scn", K(ret));
  } else if (OB_FAIL(create_incr_active_segment(new_segment))) {
    LOG_WARN("failed to create incr active segment", K(ret));
  } else {
    TCWLockGuard lock_guard(incr_data_->mem_data_rwlock_);
    // 查询时, 读incr_data时会加读锁, 所以在frozen设置, 但是incr未设置时, 一个segment查询两次
    frozen_data_->segment_handle_ = incr_data_->segment_handle_;
    // same with vbitmap_data before 3->4 refresh finish
    frozen_data_->vbitmap_ = vbitmap_data_;
    frozen_data_->frozen_scn_ = frozen_scn;
    incr_data_->segment_handle_ = new_segment;

    frozen_data_->set_has_frozen();
    int64_t seg_cnt = 0;
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    frozen_data_->segment_handle_->get_index_number(seg_cnt);
    frozen_data_->segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid);
    LOG_INFO("freeze active segment success", K(seg_cnt), K(seg_min_vid), K(seg_max_vid), K(frozen_data_));
  }

  if (OB_FAIL(ret)) {
    new_segment.reset();
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::create_incr_active_segment(
    ObVectorIndexSegmentHandle &res_segment_handle, const ObVectorIndexAlgorithmType enforce_type)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *param = nullptr;
  ObVectorIndexSegmentHandle segment_handle;
  ObVectorIndexRoaringBitMap *ibitmap = nullptr;
  if (! incr_data_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incr data is not inited, so can not create active segment", K(ret), KP(this), K_(snap_data));
  } else if (OB_FAIL(get_hnsw_param(param))) {
    LOG_WARN("get hnsw param failed.", K(ret));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret), KPC(this));
  } else if (res_segment_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("segment is not null, may be no need create", K(ret), K(res_segment_handle), K_(snap_data));
  } else {
    ObVectorIndexAlgorithmType build_type = enforce_type == VIAT_MAX ? param->type_ : enforce_type;
    // Note. sq/bq must use hgraph to build incr index.
    build_type = build_type == VIAT_HNSW_SQ || build_type == VIAT_HNSW_BQ ? VIAT_HGRAPH : build_type;
    // Note. ipivf_sq must use ipivf to build incr index.
    build_type = build_type == VIAT_IPIVF_SQ ? VIAT_IPIVF : build_type;
    if (OB_FAIL(ObVectorIndexSegment::create(
        segment_handle,
        tenant_id_,
        *get_allocator(),
        *param,
        build_type,
        param->m_,
        this))) {
      LOG_WARN("create segment fail", K(ret), K(build_type), KPC(param));
    } else if (OB_ISNULL(segment_handle->ibitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, get_allocator(), get_allocator(), tenant_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create delta_bitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
    } else if (OB_FAIL(segment_handle->ibitmap_->init(true, false))) {
      LOG_WARN("init bitmap fail", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    segment_handle.reset();
  } else {
    res_segment_handle = segment_handle;
    LOG_INFO("create incr active segment success.", K(segment_handle), K(lbt()));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::complete_bitmap_data(
    const SCN& frozen_scn,
    common::ObNewRowIterator *row_iter)
{
  INIT_SUCC(ret);
  ObVecIdxVBitmapDataHandle new_vbitmap;
  int64_t scan_row_cnt = 0;
  int64_t skip_row_cnt = 0;
  int64_t dim = 0;
  int64_t read_num = 0;
  SCN read_scn;
  SCN max_scn = SCN::min_scn();
  ObArray<uint64_t> i_vids;
  ObArray<uint64_t> d_vids;
  if (OB_ISNULL(row_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ctx or row_iter null.", K(ret), KP(row_iter));
  } else if (! is_mem_data_init_atomic(VIRT_BITMAP)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vbitmap is not init", K(ret), KPC(this));
  } else if (frozen_data_->vbitmap_.get() != vbitmap_data_.get()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frozen vbitmap is invalid", K(ret), KPC(this));
  } else if (OB_FAIL(create_vbitmap_data(new_vbitmap))) {
    LOG_WARN("failed to create vbitmap data", K(ret));
  } else {
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter);
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (!datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid new row.", K(ret));
      } else if (OB_FALSE_IT(read_num = datum_row->storage_datums_[0].get_int())) {
      } else if (OB_FAIL(read_scn.convert_for_gts(read_num))) {
        LOG_WARN("failed to convert from ts.", K(ret), K(read_num));
      } else if (read_scn > frozen_scn) {
        ++scan_row_cnt;
        ++skip_row_cnt;
        LOG_INFO("skip latest insert record after freeze", K(read_scn), K(frozen_scn), KPC(datum_row));
      } else if (OB_FAIL(add_datum_row_into_array(datum_row, i_vids, d_vids))) {
        LOG_WARN("failed to add vid into array.", K(ret), KP(datum_row));
      } else {
        max_scn = read_scn > max_scn ? read_scn : max_scn;
        ++scan_row_cnt;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_dim(dim))) {
      LOG_WARN("failed to get dim.", K(ret));
    } else {
      TCWLockGuard lock_guard(vbitmap_data_->bitmap_rwlock_);
      if (OB_FAIL(write_into_bitmap_without_lock(vbitmap_data_, dim, max_scn, i_vids, d_vids))) {
        LOG_WARN("failed to write into index mem.", K(ret), K(frozen_scn), K(max_scn));
      } else {
        new_vbitmap->scn_ = vbitmap_data_->scn_;
        vbitmap_data_ = new_vbitmap;
        LOG_INFO("complete vbitmap for frozen success", KP(this), K(vbitmap_data_), K(frozen_data_),
          K(frozen_scn), K(max_scn), K(skip_row_cnt), K(scan_row_cnt), K(i_vids.count()), K(d_vids.count()), K(d_vids));
      }
    }
  }
  if (OB_FAIL(ret)) {
    new_vbitmap.reset();
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::update_vbitmap_memdata(
    const ObLSID &ls_id,
    const share::SCN &target_scn,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  SCN current_scn;
  schema::ObIndexType index_type = INDEX_TYPE_VEC_INDEX_ID_LOCAL;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  common::ObNewRowIterator *index_id_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObTableScanParam vbitmap_scan_param;
  schema::ObTableParam vbitmap_table_param(allocator);

  if (OB_FAIL(ObPluginVectorIndexUtils::get_current_read_scn(current_scn))) {
    LOG_WARN("fail to get scn", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(const_cast<ObLSID&>(ls_id),
                                this,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                allocator,
                                allocator,
                                vbitmap_scan_param,
                                vbitmap_table_param,
                                index_id_iter))) {
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(index_id_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret));
  } else if (OB_FAIL(complete_bitmap_data(target_scn, index_id_iter))) {
    LOG_WARN("failed to check comple index mem data.", K(ret), K(target_scn), K(frozen_data_));
  } else {
    ObVecIdxVBitmapDataHandle &vbitmap = frozen_data_->vbitmap_;
    int64_t vi_cnt = roaring64_bitmap_get_cardinality(vbitmap->bitmap_->insert_bitmap_);
    int64_t vi_min_vid = roaring64_bitmap_minimum(vbitmap->bitmap_->insert_bitmap_);
    int64_t vi_max_vid = roaring64_bitmap_maximum(vbitmap->bitmap_->insert_bitmap_);
    int64_t vd_cnt = roaring64_bitmap_get_cardinality(vbitmap->bitmap_->delete_bitmap_);
    int64_t vd_min_vid = roaring64_bitmap_minimum(vbitmap->bitmap_->delete_bitmap_);
    int64_t vd_max_vid = roaring64_bitmap_maximum(vbitmap->bitmap_->delete_bitmap_);
    int64_t seg_cnt = 0;
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    frozen_data_->segment_handle_->get_index_number(seg_cnt);
    frozen_data_->segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid);
    LOG_INFO("update frozen vbitmap success",
        K(vi_cnt), K(vi_min_vid), K(vi_max_vid), K(vd_cnt), K(vd_min_vid), K(vd_max_vid),
        K(seg_cnt), K(seg_min_vid), K(seg_max_vid), K(frozen_data_));
  }

  if (OB_NOT_NULL(index_id_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(index_id_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert index_id_iter failed", K(ret));
    }
    index_id_iter = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::get_full_incr_bitmap(roaring::api::roaring64_bitmap_t *&bitmap)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPG"));
  ROARING_TRY_CATCH(bitmap = roaring::api::roaring64_bitmap_create());
  if (OB_SUCC(ret)) {
    TCRLockGuard lock_guard(incr_data_->segment_handle_->ibitmap_->rwlock_);
    roaring::api::roaring64_bitmap_t *incr_bitmap = ATOMIC_LOAD(&(incr_data_->segment_handle_->ibitmap_->insert_bitmap_));
    ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(bitmap, incr_bitmap));
  }
  if (OB_SUCC(ret) && has_frozen()) {
    TCRLockGuard lock_guard(frozen_data_->segment_handle_->ibitmap_->rwlock_);
    roaring::api::roaring64_bitmap_t *frozen_bitmap = ATOMIC_LOAD(&(frozen_data_->segment_handle_->ibitmap_->insert_bitmap_));
    ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(bitmap, frozen_bitmap));
  }
  if (OB_SUCC(ret) && is_snap_inited()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < snap_data_->meta_.incrs_.count(); ++i) {
      const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
      if (! seg_meta.segment_handle_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment handle is invalid", K(ret), K(i), K(seg_meta), KPC(this));
      } else if (ObVectorIndexSegmentType::FREEZE_PERSIST == seg_meta.seg_type_) {
        if (OB_ISNULL(seg_meta.segment_handle_->ibitmap_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("segment ibitmap is null", K(ret), K(seg_meta));
        } else if (OB_ISNULL(seg_meta.segment_handle_->ibitmap_->insert_bitmap_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("segment insert bitmap is null", K(ret), K(seg_meta));
        } else {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(bitmap, seg_meta.segment_handle_->ibitmap_->insert_bitmap_));
        }
      } else if (ObVectorIndexSegmentType::INCR_MERGE == seg_meta.seg_type_) {
        if (OB_NOT_NULL(seg_meta.segment_handle_->ibitmap_)
            && OB_NOT_NULL(seg_meta.segment_handle_->ibitmap_->insert_bitmap_)) {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(bitmap, seg_meta.segment_handle_->ibitmap_->insert_bitmap_));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ROARING_TRY_CATCH(roaring64_bitmap_free(bitmap));
    bitmap = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::check_vbitmap_is_subset(roaring::api::roaring64_bitmap_t *delta_bitmap, bool &is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = false;
  if (OB_NOT_NULL(delta_bitmap)) {
    bool is_active_subset = false;
    {
      TCRLockGuard lock_guard(vbitmap_data_->bitmap_rwlock_);
      roaring::api::roaring64_bitmap_t *active_vbitmap = ATOMIC_LOAD(&(vbitmap_data_->bitmap_->insert_bitmap_));
      is_active_subset = roaring64_bitmap_is_subset(active_vbitmap, delta_bitmap);
    }
    if (is_active_subset && has_frozen()) {
      TCRLockGuard lock_guard(frozen_data_->vbitmap_->bitmap_rwlock_);
      roaring::api::roaring64_bitmap_t *frozen_bitmap = ATOMIC_LOAD(&(frozen_data_->vbitmap_->bitmap_->insert_bitmap_));
      is_subset = roaring64_bitmap_is_subset(frozen_bitmap, delta_bitmap);
    } else {
      is_subset = is_active_subset;
    }

    if (is_subset && is_snap_inited()) {
      for (int64_t i = 0; OB_SUCC(ret) && is_subset && i < snap_data_->meta_.incrs_.count(); ++i) {
        const ObVectorIndexSegmentMeta &seg_meta = snap_data_->meta_.incrs_.at(i);
        if (OB_NOT_NULL(seg_meta.segment_handle_->vbitmap_) && OB_NOT_NULL(seg_meta.segment_handle_->vbitmap_->insert_bitmap_)) {
          is_subset = roaring64_bitmap_is_subset(seg_meta.segment_handle_->vbitmap_->insert_bitmap_, delta_bitmap);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("delta_bitmap is null", KP(this));
  }
  return ret;
}

static int get_lob_tablet_id(const ObLSID &ls_id, const ObTabletID &data_tablet_id,
    ObTabletID &lob_meta_tablet_id, ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
 // get lob tablet id
 HEAP_VARS_3((ObLSHandle, ls_handle), (ObTabletHandle, data_tablet_handle), (ObTabletBindingMdsUserData, ddl_data))
 {
   ObLSService *ls_service = nullptr;
   if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
   } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
     LOG_WARN("failed to get log stream", K(ret), K(ls_id));
   } else if (OB_ISNULL(ls_handle.get_ls())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("ls should not be null", K(ret));
   } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(data_tablet_id, data_tablet_handle))) {
     LOG_WARN("fail to get tablet handle", K(ret), K(data_tablet_id));
   } else if (OB_FAIL(data_tablet_handle.get_obj()->get_ddl_data(ddl_data))) {
     LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet_handle));
   } else {
     lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
     lob_piece_tablet_id = ddl_data.lob_piece_tablet_id_;
   }
 }
  return ret;
}

int ObPluginVectorIndexAdaptor::persist_incr_segment(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  share::SCN target_scn = frozen_data_->frozen_scn_;
  const int64_t snapshot_version = target_scn.get_val_for_inner_table_field();
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "VecIdxFrez"));
  const uint64_t timeout = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  transaction::ObTxDesc *tx_desc = nullptr;
  transaction::ObTxReadSnapshot snapshot;
  int64_t lob_inrow_threshold = -1;
  ObVectorIndexSegmentMeta seg_meta;
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  ObString meta_data;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ctx.ls_id_ = ls_id;
  ctx.data_tablet_id_ = get_data_tablet_id();
  ctx.snap_tablet_id_ = get_snap_tablet_id();
  ObVecIdxSnapTableSegAddOp snap_table_handler(tenant_id_);
  if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id, false/*is_for_read*/, timeout, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else if (OB_FAIL(get_lob_tablet_id(ls_id, ctx.data_tablet_id_, ctx.lob_meta_tablet_id_, ctx.lob_meta_tablet_id_))) {
    LOG_WARN("get_lob_tablet_id fail", K(ret), K(ls_id), K(ctx.data_tablet_id_));
  } else if (OB_FAIL(snap_table_handler.init(ls_id, this->get_data_table_id(), this->get_snapshot_table_id(), this->get_snap_tablet_id()))) {
    LOG_WARN("init snap table handler fail", K(ret));
  } else if (OB_FALSE_IT(lob_inrow_threshold = snap_table_handler.get_lob_inrow_threshold())) {
  } else if (! has_frozen()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no freeze segment", K(ret), K(frozen_data_));
  // 冻结的segment不会有其他线程写入, 所以不需要加写锁互斥
  } else if (OB_FAIL(frozen_data_->segment_handle_->serialize(tenant_id_, allocator, ctx, frozen_data_->vbitmap_->bitmap_,
      tx_desc, snapshot, lob_inrow_threshold, timeout, index_type))) {
    LOG_WARN("serialize segment fail", K(ret), K(frozen_data_));
  } else if (OB_FAIL(snap_table_handler.insert_segment_data(ctx.vals_, tx_desc, snapshot, snapshot_version, index_type, timeout))) {
    LOG_WARN("do insert fail", K(ret));
  // delete 3, 4 index table data.
  } else if (OB_FAIL(delete_incr_table_data(allocator, ls_id, snapshot,
      tx_desc, timeout, frozen_data_->frozen_scn_, frozen_data_->vbitmap_->bitmap_))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::prepare_new_segment_meta(
      allocator, seg_meta, ObVectorIndexSegmentType::FREEZE_PERSIST, index_type, get_snap_tablet_id(),
      snapshot_version, ctx.vals_, frozen_data_->vbitmap_->bitmap_))) {
    LOG_WARN("prepare new segment meta fail", K(ret));
  } else if (OB_FAIL(snap_table_handler.preprea_meta(seg_meta, snap_data_->meta_))) {
    LOG_WARN("prepare meta fail", K(ret));
  } else if (OB_FAIL(snap_table_handler.insertup_meta_row(this, tx_desc, timeout))) {
    LOG_WARN("insertup_meta_row fail", K(ret));
  }

  DEBUG_SYNC(BEFORE_VECTOR_INDEX_FREEZE_COMMIT);
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(tx_desc) && OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout))) {
    ret = tmp_ret;
    LOG_WARN("fail to end trans", K(ret));
  }

  if (OB_SUCC(ret)) {
    // just trigger, so don't afftect return
    ObSEArray<uint64_t, 2> tablet_ids;
    if (OB_TMP_FAIL(tablet_ids.push_back(get_inc_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(tablet_ids.push_back(get_vbitmap_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVectorIndexRefresher::trigger_minor_freeze(tenant_id_, tablet_ids))) {
      LOG_WARN("failed to trigger minor freeze", K(tmp_ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::delete_incr_table_data(
    ObIAllocator &allocator, const ObLSID &ls_id, transaction::ObTxReadSnapshot &snapshot,
    transaction::ObTxDesc *tx_desc, const int64_t timeout,
    const share::SCN& frozen_scn, const ObVectorIndexRoaringBitMap *bitmap)
{
  int ret = OB_SUCCESS;
  ObVectorIndexDeltaTableHandler delta_table_hanlder(tenant_id_);
  if (OB_FAIL(delta_table_hanlder.init(this, ls_id, data_table_id_, inc_table_id_,
      vbitmap_table_id_, inc_tablet_id_, vbitmap_tablet_id_, snapshot.core_.version_))) {
    LOG_WARN("init delta table handler fail", K(ret), K(snapshot));
  } else if (OB_FAIL(delta_table_hanlder.delete_incr_table_data(snapshot, tx_desc, timeout, frozen_scn, bitmap))) {
    LOG_WARN("failed to delete incr table data", K(ret), K(snapshot));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::build_snap_meta(
    const ObTabletID &tablet_id, const int64_t snapshot_version, const int64_t &data_block_cnt)
{
  int ret = OB_SUCCESS;
  if (! snap_data_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap data is not init, can not get vector index meta", KPC(this));
  } else if (data_block_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no segment data", K(ret), K(data_block_cnt));
  } else if (OB_FAIL(snap_data_->build_meta(tablet_id, snapshot_version, data_block_cnt))) {
    LOG_WARN("build_meta fail", K(ret), K(tablet_id), K(snapshot_version), K(data_block_cnt));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::build_and_serialize_meta_data(
    const ObTabletID &tablet_id,
    const int64_t snapshot_version, const int64_t &data_block_cnt,
    ObIAllocator &allocator, ObString &meta_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_snap_meta(tablet_id, snapshot_version, data_block_cnt))) {
    LOG_WARN("build snap meta fail");
  } else if (OB_FALSE_IT(snap_data_->meta_.is_persistent_ = true)) {
  } else if (OB_FAIL(snap_data_->meta_.serialize(allocator, meta_data))) {
    LOG_WARN("serialize meta fail", K(ret), K(snap_data_));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_meta(const ObString& meta_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObVectorIndexMeta& meta = snap_data_->meta_;
  if (OB_FAIL(meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
    LOG_WARN("derserialize meta data fail", K(ret), K(meta_data.length()));
  } else {
    LOG_INFO("vector index meta data", K(snap_data_->meta_));
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_data(ObHNSWDeserializeCallback::CbParam &param, ObString &row_key)
{
  int ret = OB_SUCCESS;
  ObVectorIndexAlgorithmType index_type;
  ObString target_prefix;
  int64_t index_count = 0;
  int64_t key_prefix_scn = 0;
  if (! snap_data_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap memdata is null", K(ret));
  } else {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (OB_FAIL(snap_data_->load_segment(tenant_id_, this, param))) {
      LOG_WARN("serialize index failed.", K(ret));
    } else if (OB_FALSE_IT(index_type = snap_data_->get_snap_index_type())) {
    } else if (OB_FAIL(ObPluginVectorIndexUtils::get_split_snapshot_prefix(index_type, row_key, target_prefix))) {
      LOG_WARN("fail to get split snapshot prefix", K(ret), K(index_type), K(row_key));
    } else if (OB_FALSE_IT(ObPluginVectorIndexUtils::get_key_prefix_scn(target_prefix, key_prefix_scn))) {
    } else if (OB_FAIL(set_snapshot_key_prefix(target_prefix))) {
      LOG_WARN("failed to set snapshot key prefix", K(ret), K(index_type), K(target_prefix));
    } else if (key_prefix_scn > 0 && OB_FAIL(set_snapshot_key_scn(key_prefix_scn))) {
      LOG_WARN("fail to set snapshot key scn", K(ret), K(key_prefix_scn));
    } else if (OB_FAIL(snap_data_->get_snap_index_row_cnt(index_count))) {
      LOG_WARN("fail to get incr index number", K(ret));
    } else if (index_count == 0) {
      snap_data_->free_memdata_resource(get_allocator(), tenant_id_);
      LOG_INFO("memdata sync snapshot index complement no data", K(index_count), K(index_type), KPC(this));
    } else { // index_count > 0
      close_snap_data_rb_flag();
      LOG_INFO("memdata sync snapshot index complement data", K(index_count), K(index_type), KPC(this));
    }
  }
  return ret;
}

int ObPluginVectorIndexAdaptor::deserialize_snap_data(ObHNSWDeserializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  if (! snap_data_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap memdata is null", K(ret));
  } else {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    if (OB_FAIL(snap_data_->load_segment(tenant_id_, this, param))) {
      LOG_WARN("serialize index failed.", K(ret));
    } else if (OB_FAIL(get_snap_index_row_cnt(row_cnt))) {
      LOG_WARN("fail to get snap index row cnt", K(ret));
    } else if (row_cnt > 0) { // deseriablize data from table 5
      close_snap_data_rb_flag();
    }
    LOG_INFO("try to deseriable snapshot data", K(ret), K(row_cnt), KPC(this));
  }
  return ret;
}

void ObPluginVectorIndexAdaptor::try_set_snap_data_rb_flag()
{
  if (! is_snap_inited()) {
    LOG_INFO("snap_data index is empty or not init, won't set rb_flag", K(lbt()));
  } else {
    TCWLockGuard lock_guard(snap_data_->mem_data_rwlock_);
    snap_data_->rb_flag_ = true;
  }
}

void ObPluginVectorIndexAdaptor::free_result(const ObVecIdxQueryResult& dist_result)
{
  for (int64_t i = 0; i < dist_result.distances_.count(); ++i) {
    ObVectorIndexSegmentHandle& segment_handle = const_cast<ObVectorIndexSegmentHandle&>(dist_result.segments_.at(i));
    segment_handle->mem_ctx()->Deallocate(const_cast<float*>(dist_result.distances_.at(i)));
  }
}

};
};
