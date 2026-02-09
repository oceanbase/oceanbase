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

#include "ob_vector_index_segment.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
namespace share
{

ObVectorIndexSegmentHandle::ObVectorIndexSegmentHandle(const ObVectorIndexSegmentHandle &other)
  : segment_(nullptr)
{
  *this = other;
}

ObVectorIndexSegmentHandle &ObVectorIndexSegmentHandle::operator= (const ObVectorIndexSegmentHandle &other)
{
  if (this != &other) {
    reset();
    if (nullptr != other.segment_) {
      segment_ = other.segment_;
      segment_->inc_ref();
    }
  }
  return *this;
}

void ObVectorIndexSegmentHandle::reset()
{
  if (nullptr != segment_) {
    const int64_t ref_cnt = segment_->dec_ref();
    if (0 == ref_cnt) {
      ObVectorIndexSegment::destroy(segment_);
    }
    segment_ = nullptr;
  }
}

int ObVectorIndexSegmentHandle::set_segment(ObVectorIndexSegment *segment)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(segment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(segment));
  } else {
    segment_ = segment;
    segment->inc_ref();
  }
  return ret;
}

int ObVectorIndexSegmentMeta::get_vector_index_meta_key(ObIAllocator &allocator, const ObTabletID &tablet_id, ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t key_pos = 0;
  char *key_str = nullptr;
  if (OB_ISNULL(key_str = static_cast<char*>(allocator.alloc(OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vec key", K(ret));
  } else if (OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "!%lu_meta_data", tablet_id.id()))) {
    LOG_WARN("fail to build vec snapshot key str", K(ret), K(tablet_id));
  } else {
    key.assign_ptr(key_str, key_pos);
  }
  return ret;
}

int ObVectorIndexSegmentMeta::get_segment_persist_key(
    const ObVectorIndexAlgorithmType index_type, const ObTabletID &tablet_id,
    const int64_t snapshot_version, const int64_t row_id, ObIAllocator &allocator, ObString &key)
{
  int ret = OB_SUCCESS;
  const int64_t key_len = OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH;
  char *key_str = nullptr;
  if (OB_ISNULL(key_str = static_cast<char*>(allocator.alloc(key_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vec key", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (OB_FAIL(get_segment_persist_key(index_type, tablet_id, snapshot_version, row_id, key_str, key_len, key))) {
    LOG_WARN("build segment fail", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  }
  return ret;
}

int ObVectorIndexSegmentMeta::get_segment_persist_key(
    const ObVectorIndexAlgorithmType index_type, const ObTabletID &tablet_id,
    const int64_t snapshot_version, const int64_t row_id, char *key_str, const int64_t key_len,  ObString &key)
{
  int ret = OB_SUCCESS;
  int64_t key_pos = 0;
  if (OB_ISNULL(key_str) || key_len < OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key buf arg is invalid", K(ret), KP(key_str), K(key_len), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_HNSW && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build vec snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_HGRAPH && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hgraph_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build vec hgraph snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_HNSW_SQ && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_sq_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build sq vec snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_HNSW_BQ && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_bq_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build bq vec snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_IPIVF && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_ipivf_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build ipivf vec snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else if (index_type == VIAT_IPIVF_SQ && OB_FAIL(databuff_printf(key_str, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_ipivf_sq_data_part%05ld", tablet_id.id(), snapshot_version, row_id))) {
    LOG_WARN("fail to build ipivf_sq vec snapshot key str", K(ret), K(index_type), K(tablet_id), K(snapshot_version), K(row_id));
  } else {
    key.assign_ptr(key_str, key_pos);
  }
  return ret;
}

int ObVectorIndexSegmentMeta::prepare_new_segment_meta(
    ObIAllocator &allocator,
    ObVectorIndexSegmentMeta &seg_meta,
    const ObVectorIndexSegmentType seg_type,
    const ObVectorIndexAlgorithmType index_type,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const ObIArray<ObVecIdxSnapshotBlockData> &data_blocks,
    const ObVectorIndexRoaringBitMap *bitmap)
{
  int ret = OB_SUCCESS;
  ObString start_key;
  ObString end_key;
  if (data_blocks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("no segment data", K(ret), K(data_blocks.count()));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(index_type, tablet_id, snapshot_version, 0, allocator, start_key))) {
    LOG_WARN("get start key fail", K(ret));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(index_type, tablet_id, snapshot_version, data_blocks.count() - 1, allocator, end_key))) {
    LOG_WARN("get end key fail", K(ret));
  } else {
    seg_meta.seg_type_ = seg_type;
    seg_meta.index_type_ = index_type;
    seg_meta.scn_ = snapshot_version;
    seg_meta.blocks_cnt_ = data_blocks.count();
    seg_meta.start_key_ = start_key;
    seg_meta.end_key_ = end_key;
    seg_meta.has_segment_meta_row_ = nullptr != bitmap;
  }
  return ret;
}

int ObVectorIndexSegmentMeta::deep_copy_seg_key(const ObString &src_start_key, const ObString &src_end_key)
{
  int ret = OB_SUCCESS;
  if (src_start_key.empty() || src_end_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start_key or end_key is empty", K(ret), K(src_start_key), K(src_end_key));
  } else if (src_start_key.length() > OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH
      || src_end_key.length() > OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start_key or end_key is too large", K(ret),
      K(src_start_key.length()), K(src_end_key.length()), K(src_start_key), K(src_end_key));
  } else {
    MEMSET(start_key_buf_, 0, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH);
    MEMSET(end_key_buf_, 0, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH);
    MEMCPY(start_key_buf_, src_start_key.ptr(), src_start_key.length());
    MEMCPY(end_key_buf_, src_end_key.ptr(), src_end_key.length());
    start_key_.assign(start_key_buf_, src_start_key.length());
    end_key_.assign(end_key_buf_, src_end_key.length());
    LOG_INFO("copy segment key success", KPC(this),
      K(src_start_key.length()), K(src_end_key.length()), K(src_start_key), K(src_end_key),
      K(start_key_.length()), K(end_key_.length()), K(start_key_), K(end_key_));
  }
  return ret;
}

void ObVectorIndexSegmentMeta::release()
{
  seg_type_ = ObVectorIndexSegmentType::INVALID;
  index_type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
  segment_handle_.reset();
  scn_ = 0;
  blocks_cnt_ = 0;
  start_key_.reset();
  end_key_.reset();
}

OB_DEF_SERIALIZE_SIZE(ObVectorIndexSegmentMeta)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(seg_type_);
  OB_UNIS_ADD_LEN(index_type_);
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(scn_);
  OB_UNIS_ADD_LEN(start_key_);
  OB_UNIS_ADD_LEN(end_key_);
  OB_UNIS_ADD_LEN(blocks_cnt_);
  return len;
}

OB_DEF_SERIALIZE(ObVectorIndexSegmentMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(seg_type_);
  OB_UNIS_ENCODE(index_type_);
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(scn_);
  OB_UNIS_ENCODE(start_key_);
  OB_UNIS_ENCODE(end_key_);
  OB_UNIS_ENCODE(blocks_cnt_);
  return ret;
}

OB_DEF_DESERIALIZE(ObVectorIndexSegmentMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(seg_type_);
  OB_UNIS_DECODE(index_type_);
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(scn_);
  OB_UNIS_DECODE(start_key_);
  OB_UNIS_DECODE(end_key_);
  OB_UNIS_DECODE(blocks_cnt_);
  return ret;
}

int ObVectorIndexMeta::get_meta_scn(const ObString& meta_data, int64_t &scn)
{
  int ret = OB_SUCCESS;
  if (meta_data.length() < ObVectorIndexMetaHeader::HEAD_LEN) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("no enough data", K(ret), K(meta_data.length()));
  } else {
    scn = reinterpret_cast<const ObVectorIndexMetaHeader*>(meta_data.ptr())->scn_;
  }
  return ret;
}

int64_t ObVectorIndexMeta::get_serialize_size() const
{
  int64_t len = 0;
  len += ObVectorIndexMetaHeader::HEAD_LEN;
  OB_UNIS_ADD_LEN_ARRAY(bases_, bases_.count());
  OB_UNIS_ADD_LEN_ARRAY(incrs_, incrs_.count());
  return len;
}

int ObVectorIndexMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (buf_len - pos < ObVectorIndexMetaHeader::HEAD_LEN) {
    ret = OB_SERIALIZE_ERROR;
    LOG_WARN("no enough buffer", K(ret), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, reinterpret_cast<const char*>(&header_), ObVectorIndexMetaHeader::HEAD_LEN);
    pos += ObVectorIndexMetaHeader::HEAD_LEN;
  }

  OB_UNIS_ENCODE_ARRAY(bases_, bases_.count());
  OB_UNIS_ENCODE_ARRAY(incrs_, incrs_.count());
  return ret;
}

int ObVectorIndexMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (data_len - pos < ObVectorIndexMetaHeader::HEAD_LEN) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("no enough data", K(ret), K(data_len), K(pos));
  } else {
    MEMCPY(reinterpret_cast<char*>(&header_), buf + pos, ObVectorIndexMetaHeader::HEAD_LEN);
    pos += ObVectorIndexMetaHeader::HEAD_LEN;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deserialize_seg_metas(buf, data_len, pos))) {
    LOG_WARN("deserialize seg metas fail", K(ret), K(data_len), K(pos));
  }
  return ret;
}

int ObVectorIndexMeta::deserialize_seg_metas(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t base_count = 0;
  int64_t incr_count = 0;
  OB_UNIS_DECODE(base_count);
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < base_count; ++i) {
      ObVectorIndexSegmentMeta seg_meta;
      if (OB_FAIL(seg_meta.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize seg meta", K(ret), K(i));
      } else if (OB_FAIL(add_base_seg_meta(seg_meta))) {
        LOG_WARN("failed to add base seg meta", K(ret), K(i));
      }
    }
  }
  OB_UNIS_DECODE(incr_count);
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < incr_count; ++i) {
      ObVectorIndexSegmentMeta seg_meta;
      if (OB_FAIL(seg_meta.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize seg meta", K(ret), K(i));
      } else if (OB_FAIL(add_incr_seg_meta(seg_meta))) {
        LOG_WARN("failed to add incr seg meta", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObVectorIndexMeta::add_incr_seg_meta(const ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(incrs_.push_back(seg_meta))) {
    LOG_WARN("failed to add incr seg meta", K(ret), K(seg_meta));
  } else {
    ObVectorIndexSegmentMeta &copy_seg_meta = incrs_.at(incrs_.count() - 1);
    if (OB_FAIL(copy_seg_meta.deep_copy_seg_key(seg_meta.start_key_, seg_meta.end_key_))) {
      LOG_WARN("failed to deep copy seg key", K(ret), K(seg_meta));
    }
  }
  return ret;
}

int ObVectorIndexMeta::add_base_seg_meta(const ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bases_.push_back(seg_meta))) {
    LOG_WARN("failed to add base seg meta", K(ret), K(seg_meta));
  } else {
    ObVectorIndexSegmentMeta &copy_seg_meta = bases_.at(bases_.count() - 1);
    if (OB_FAIL(copy_seg_meta.deep_copy_seg_key(seg_meta.start_key_, seg_meta.end_key_))) {
      LOG_WARN("failed to deep copy seg key", K(ret), K(seg_meta));
    }
  }
  return ret;
}

int ObVectorIndexMeta::assign(const ObVectorIndexMeta& other)
{
  int ret = OB_SUCCESS;
  header_ = other.header_;
  if (OB_FAIL(bases_.assign(other.bases_))) {
    LOG_WARN("assign base fail", K(ret));
  } else if (OB_FAIL(incrs_.assign(other.incrs_))) {
    LOG_WARN("assign incr fail", K(ret));
  }
  return ret;
}

void ObVectorIndexMeta::release(ObIAllocator *allocator, const uint64_t tenant_id)
{
  header_.reset();
  for (int64_t i = 0; i < incrs_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = incrs_.at(i);
    seg_meta.release();
  }
  incrs_.reset();
  for (int64_t i = 0; i < bases_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = bases_.at(i);
    seg_meta.release();
  }
  bases_.reset();
}

int ObVectorIndexMeta::serialize(ObIAllocator &allocator, ObString &serde_data)
{
  int ret = OB_SUCCESS;
  int64_t serde_size = get_serialize_size();
  int64_t len = sizeof(ObLobCommon) + serde_size;
  char* ptr = nullptr;
  char* serde_buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(ptr = reinterpret_cast<char*>(allocator.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory fail", K(ret), K(serde_size));
  } else if (OB_FALSE_IT(new (ptr) ObLobCommon())) {
  } else if (OB_FALSE_IT(serde_buf = ptr + sizeof(ObLobCommon))) {
  } else if (OB_FAIL(serialize(serde_buf, serde_size, pos))) {
    LOG_WARN("serialize meta fail", K(ret), K(len), K(serde_size), KP(ptr), KP(serde_buf), K(get_serialize_size()), KPC(this));
  } else {
    serde_data.assign_ptr(ptr, len);
    LOG_INFO("[VECTOR INDEX] build meta serde data success",
        K(len), K(serde_size), KP(ptr), KP(serde_buf), K(this), K(serde_data));
  }
  return ret;
}

int ObVectorIndexMeta::prepare_new_index_meta(
    ObIAllocator &allocator,
    const ObVectorIndexSegmentType seg_type,
    const ObVectorIndexAlgorithmType index_type,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const ObVectorIndexMeta &old_meta,
    const ObIArray<ObVecIdxSnapshotBlockData> &data_blocks,
    const ObVectorIndexRoaringBitMap *bitmap,
    ObString &meta_data)
{
  int ret = OB_SUCCESS;
  ObVectorIndexMeta meta;
  ObVectorIndexSegmentMeta seg_meta;
  if (OB_FAIL(ObVectorIndexSegmentMeta::prepare_new_segment_meta(
      allocator, seg_meta, seg_type, index_type, tablet_id, snapshot_version, data_blocks, bitmap))) {
    LOG_WARN("prepare new segment meta fail", K(ret));
  } else if (OB_FAIL(meta.assign(old_meta))) {
    LOG_WARN("copy meta fail", K(ret), K(old_meta));
  } else if (OB_FALSE_IT(meta.header_.version_ = 1)) {
  } else if (OB_FALSE_IT(meta.header_.scn_ = snapshot_version)) {
  } else if (OB_FAIL(meta.incrs_.push_back(seg_meta))) {
    LOG_WARN("push back seg meta fail", K(ret), K(index_type), K(snapshot_version));
  } else if (OB_FAIL(meta.serialize(allocator, meta_data))) {
    LOG_WARN("serialize meta fail", K(ret), K(meta));
  } else {
    LOG_INFO("prepare new meta success", K(meta), K(old_meta));
  }
  return ret;
}

void ObVectorIndexSegment::inc_ref()
{
  int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1);
  LOG_TRACE("segment inc ref", KP(this), "ref_cnt", cnt, K(lbt()));
}

int64_t ObVectorIndexSegment::dec_ref()
{
  int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
  LOG_TRACE("segment dec ref", KP(this), "ref_cnt", cnt, K(lbt()));
  return cnt;
}

void ObVectorIndexSegment::inc_write_ref()
{
  int64_t cnt = ATOMIC_AAF(&write_ref_cnt_, 1);
  LOG_TRACE("segment inc ref", KP(this), "ref_cnt", cnt, K(lbt()));
}

int64_t ObVectorIndexSegment::dec_write_ref()
{
  int64_t cnt = ATOMIC_SAF(&write_ref_cnt_, 1 /* just sub 1 */);
  LOG_TRACE("segment dec ref", KP(this), "ref_cnt", cnt, K(lbt()));
  return cnt;
}

int ObVectorIndexSegment::create(
    ObVectorIndexSegmentHandle &segment_handle,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    const ObVectorIndexAlgorithmType build_type,
    const int max_degree,
    ObPluginVectorIndexAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  ObVectorIndexSegment *segment = nullptr;
  if (OB_ISNULL(segment = OB_NEWx(ObVectorIndexSegment, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for vector index segment fail", K(ret), "size", sizeof(ObVectorIndexSegment));
  } else if (OB_FAIL(segment->init(tenant_id, param, build_type, max_degree, adaptor))) {
    LOG_WARN("init segment fail", K(ret), KP(segment), K(build_type), K(max_degree), KP(adaptor), K(param));
  } else if (OB_FAIL(segment_handle.set_segment(segment))) {
    LOG_WARN("set segment fail", K(ret), K(segment_handle));
  } else {
    LOG_INFO("create segment success", KPC(segment), K(build_type), K(max_degree), KP(adaptor), K(param), K(lbt()));
  }

  if (OB_FAIL(ret)) {
    destroy(segment);
    segment = nullptr;
  }
  return ret;
}

void ObVectorIndexSegment::destroy(ObVectorIndexSegment *segment)
{
  if (OB_NOT_NULL(segment)) {
    ObIAllocator *allocator = segment->allocator_;
    segment->reset();
    OB_DELETEx(ObVectorIndexSegment, allocator, segment);
  }
}

int ObVectorIndexSegment::init(
    const uint64_t tenant_id,
    const ObVectorIndexParam &param,
    const ObVectorIndexAlgorithmType build_type,
    const int max_degree,
    ObPluginVectorIndexAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  const char* const DATATYPE_FLOAT32 = "float32";
  const char* const DATATYPE_SPARSE = "sparse";
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
  lib::ObLightBacktraceGuard light_backtrace_guard(false);
  if(is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("segment inited twiced", K(ret), KPC(this));
  } else if (OB_FAIL(adaptor->create_mem_ctx(mem_ctx_))) {
    LOG_WARN("create mem ctx fail", K(ret));
  } else if (VIAT_IPIVF == build_type || VIAT_IPIVF_SQ == build_type) {
    if (OB_FAIL(obvectorutil::create_index(
        index_,
        build_type,
        DATATYPE_SPARSE,
        VEC_INDEX_ALGTH[param.dist_algorithm_],
        param.refine_,
        param.ob_sparse_drop_ratio_build_,
        param.window_size_,
        mem_ctx_,
        param.extra_info_actual_size_))) {
      LOG_WARN("failed to create sparse vsag index.", K(ret), K(build_type), KP(mem_ctx_), K(param));
    }
  } else if (OB_FAIL(obvectorutil::create_index(
      index_,
      build_type,
      DATATYPE_FLOAT32,
      VEC_INDEX_ALGTH[param.dist_algorithm_],
      param.dim_,
      max_degree,
      param.ef_construction_,
      param.ef_search_,
      mem_ctx_,
      param.extra_info_actual_size_,
      param.refine_type_,
      param.bq_bits_query_,
      param.bq_use_fht_))) {
    LOG_WARN("failed to create vsag index.", K(ret), K(build_type), K(max_degree), KP(mem_ctx_), K(param));
  }
  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObVectorIndexSegment::reset()
{
  LOG_INFO("delete vector index segment", KPC(this), K(lbt()));

  if (0 != ref_cnt_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "vector index segment ref_cnt is not zero, but trigger reset", KPC(this));
  }
  if (OB_NOT_NULL(index_)) {
    obvectorutil::delete_index(index_);
    index_ = nullptr;
  }
  if (OB_NOT_NULL(mem_ctx_)) {
    mem_ctx_->~ObVsagMemContext();
    allocator_->free(mem_ctx_);
    mem_ctx_ = nullptr;
  }

  ObVectorIndexRoaringBitMap::destroy(ibitmap_);
  ibitmap_ = nullptr;
  ObVectorIndexRoaringBitMap::destroy(vbitmap_);
  vbitmap_ = nullptr;

  is_base_ = false;
  ref_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  allocator_ = nullptr;
  is_inited_ = false;
}

int64_t ObVectorIndexSegment::get_serialize_meta_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  len += ObVectorIndexSegmentHeader::HEAD_LEN;
  bool has_ibitmap = nullptr != ibitmap_;
  OB_UNIS_ADD_LEN(has_ibitmap);
  if (has_ibitmap) {
    OB_UNIS_ADD_LEN(*ibitmap_);
  }
  bool has_vbitmap = nullptr != vbitmap_;
  OB_UNIS_ADD_LEN(has_vbitmap);
  if (has_vbitmap) {
    OB_UNIS_ADD_LEN(*vbitmap_);
  }
  return len;
}

int ObVectorIndexSegment::serialize_meta(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  ObVectorIndexSegmentHeader header;
  if (buf_len - pos < ObVectorIndexSegmentHeader::HEAD_LEN) {
    ret = OB_SERIALIZE_ERROR;
    LOG_WARN("no enough buffer", K(ret), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, reinterpret_cast<const char*>(&header), ObVectorIndexSegmentHeader::HEAD_LEN);
    pos += ObVectorIndexSegmentHeader::HEAD_LEN;
  }

  bool has_ibitmap = nullptr != ibitmap_;
  OB_UNIS_ENCODE(has_ibitmap);
  if (has_ibitmap) {
    OB_UNIS_ENCODE(*ibitmap_);
  }
  bool has_vbitmap = nullptr != vbitmap_;
  OB_UNIS_ENCODE(has_vbitmap);
  if (has_vbitmap) {
    OB_UNIS_ENCODE(*vbitmap_);
  }
  return ret;
}

int ObVectorIndexSegment::deserialize_meta(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (data_len - pos < ObVectorIndexSegmentHeader::HEAD_LEN) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("no enough data", K(ret), K(data_len), K(pos));
  } else {
    ObVectorIndexSegmentHeader header;
    MEMCPY(reinterpret_cast<char*>(&header), buf + pos, ObVectorIndexSegmentHeader::HEAD_LEN);
    pos += ObVectorIndexSegmentHeader::HEAD_LEN;
    if (! header.is_valid()) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("segment header is invalid", K(ret), K(data_len), K(pos), K(header));
    }
  }

  bool has_ibitmap = false;
  OB_UNIS_DECODE(has_ibitmap);
  if (has_ibitmap) {
    if (OB_NOT_NULL(ibitmap_)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("ibitmap has been created", K(ret), K(data_len), K(pos));
    } else if (OB_ISNULL(ibitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, allocator_, allocator_, tenant_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create segment bitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
    } else {;
      OB_UNIS_DECODE(*ibitmap_);
      if (OB_NOT_NULL(ibitmap_->insert_bitmap_)) {
        int64_t ii_cnt = roaring64_bitmap_get_cardinality(ibitmap_->insert_bitmap_);
        int64_t ii_min_vid = roaring64_bitmap_minimum(ibitmap_->insert_bitmap_);
        int64_t ii_max_vid = roaring64_bitmap_maximum(ibitmap_->insert_bitmap_);
        LOG_INFO("deserialize ibitmap success", KP(this), KPC(ibitmap_), K(ii_cnt), K(ii_min_vid), K(ii_max_vid));
      } else {
        LOG_INFO("deserialize ibitmap success", KP(this), KPC(ibitmap_));
      }

    }
  }
  bool has_vbitmap = false;
  OB_UNIS_DECODE(has_vbitmap);
  if (has_vbitmap) {
    if (OB_NOT_NULL(vbitmap_)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("vbitmap has been created", K(ret), K(data_len), K(pos));
    } else if (OB_ISNULL(vbitmap_ = OB_NEWx(ObVectorIndexRoaringBitMap, allocator_, allocator_, tenant_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create vbitmap", K(ret), "size", sizeof(ObVectorIndexRoaringBitMap));
    } else {
      OB_UNIS_DECODE(*vbitmap_);
      if (OB_NOT_NULL(vbitmap_->insert_bitmap_)) {
        int64_t vi_cnt = roaring64_bitmap_get_cardinality(vbitmap_->insert_bitmap_);
        int64_t vi_min_vid = roaring64_bitmap_minimum(vbitmap_->insert_bitmap_);
        int64_t vi_max_vid = roaring64_bitmap_maximum(vbitmap_->insert_bitmap_);
        LOG_INFO("deserialize vbitmap success", KP(this), KPC(vbitmap_), K(vi_cnt), K(vi_min_vid), K(vi_max_vid));
      } else {
        LOG_INFO("deserialize vbitmap success", KP(this), KPC(vbitmap_));
      }
    }
  }
  return ret;
}

int ObVectorIndexSegment::serialize(const uint64_t tenant_id, ObHNSWSerializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  ObHNSWSerializeCallback callback;
  ObOStreamBuf::Callback cb = callback;
  ObVectorIndexSerializer index_seri(*param.allocator_);
  int64_t vec_cnt = 0;
  if (OB_FAIL(get_index_number(vec_cnt))) {
    LOG_WARN("failed to get snap index number.", K(ret));
  } else if (OB_FAIL(index_seri.serialize(this, param, cb, tenant_id))) {
    LOG_WARN("serialize index failed.", K(ret));
  } else {
    LOG_INFO("serliaze segment success", K(vec_cnt), KPC(this));
  }
  return ret;
}

int ObVectorIndexSegment::serialize(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    ObVecIdxSnapshotDataWriteCtx &ctx,
    ObVectorIndexRoaringBitMap *vbitmap,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t lob_inrow_threshold,
    const int64_t timeout,
    ObVectorIndexAlgorithmType &index_type)
{
  int ret = OB_SUCCESS;
  ObHNSWSerializeCallback::CbParam param;
  param.vctx_ = &ctx;
  param.allocator_ = &allocator;
  param.tmp_allocator_ = &allocator;
  param.lob_inrow_threshold_ = lob_inrow_threshold;
  param.timeout_ = timeout;
  param.snapshot_ = &snapshot;
  param.tx_desc_ = tx_desc;
  param.need_serde_meta_ = nullptr != ibitmap_;
  vbitmap_ = vbitmap;
  int64_t index_size = 0;
  if (OB_FAIL(get_index_number(index_size))) {
    LOG_WARN("failed to get snap index number.", K(ret));
  } else if (index_size == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("frozen segment is empty", K(ret), K(index_size));
  } else if (OB_FALSE_IT(index_type = get_index_type())) {
  } else if (index_type >= VIAT_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index type invalid.", K(ret), K(index_type));
  } else if (OB_FAIL(serialize(tenant_id, param))) {
    LOG_WARN("serialize index failed.", K(ret));
  }
  vbitmap_ = nullptr;
  return ret;
}


int ObVectorIndexSegment::deserialize(
    ObVectorIndexSegmentHandle &segment_handle, const uint64_t tenant_id, ObPluginVectorIndexAdaptor *adaptor, ObHNSWDeserializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  ObHNSWDeserializeCallback callback(adaptor);
  ObIStreamBuf::Callback cb = callback;
  ObVectorIndexSerializer index_seri(*param.allocator_);
  if (OB_FAIL(index_seri.deserialize(segment_handle, param, cb, tenant_id))) {
    LOG_WARN("serialize index failed.", K(ret));
  } else if (! segment_handle.is_valid()) {
    // for empty snapshot table, segment may be not inited
    // beacuse index type is not sure.....
    LOG_INFO("empty segment data", K(lbt()));
  } else if (OB_FALSE_IT(segment_handle->set_is_base())) {
  } else if (OB_FAIL(segment_handle->immutable_optimize())) {
    LOG_WARN("fail to index immutable_optimize", K(ret));
  } else if (OB_FAIL(segment_handle->get_vid_bound(
      segment_handle->vid_bound_.min_vid_, segment_handle->vid_bound_.max_vid_))) {
    LOG_WARN("get_vid_bound fail", K(ret), K(segment_handle));
  }
  return ret;
}

void ObVectorIndexRoaringBitMap::destroy(ObVectorIndexRoaringBitMap *bitmap)
{
  if (OB_NOT_NULL(bitmap)) {
    ObIAllocator *allocator = bitmap->allocator_;
    if (OB_NOT_NULL(bitmap->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(bitmap->tenant_id_, "VIBitmap"));
      roaring::api::roaring64_bitmap_free(bitmap->insert_bitmap_);
      bitmap->insert_bitmap_ = nullptr;
    }
    if (OB_NOT_NULL(bitmap->delete_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(bitmap->tenant_id_, "VIBitmap"));
      roaring::api::roaring64_bitmap_free(bitmap->delete_bitmap_);
      bitmap->delete_bitmap_ = nullptr;
    }
    allocator->free(bitmap);
  }
}

int ObVectorIndexRoaringBitMap::init(const bool is_init_ibitmap, const bool is_init_dbitmap)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && is_init_ibitmap) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
    ROARING_TRY_CATCH(insert_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(insert_bitmap_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create incr insert bitmap", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_init_dbitmap) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
    ROARING_TRY_CATCH(delete_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(delete_bitmap_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create incr delete bitmap", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObVectorIndexRoaringBitMap)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t insert_bitmap_size = 0;
  int64_t delete_bitmap_size = 0;
  if (nullptr != insert_bitmap_) {
    ROARING_TRY_CATCH(insert_bitmap_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(insert_bitmap_));
    OB_UNIS_ADD_LEN(insert_bitmap_size);
    len += insert_bitmap_size;
  } else {
    insert_bitmap_size = 0;
    OB_UNIS_ADD_LEN(insert_bitmap_size);
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr != delete_bitmap_) {
    ROARING_TRY_CATCH(delete_bitmap_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(delete_bitmap_));
    OB_UNIS_ADD_LEN(delete_bitmap_size);
    len += delete_bitmap_size;
  } else {
    delete_bitmap_size = 0;
    OB_UNIS_ADD_LEN(delete_bitmap_size);
  }
  return len;
}

OB_DEF_SERIALIZE(ObVectorIndexRoaringBitMap)
{
  int ret = OB_SUCCESS;
  int64_t insert_bitmap_size = 0;
  int64_t delete_bitmap_size = 0;
  if (nullptr != insert_bitmap_) {
    ROARING_TRY_CATCH(insert_bitmap_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(insert_bitmap_));
    OB_UNIS_ENCODE(insert_bitmap_size);
    if (OB_SUCC(ret) && insert_bitmap_size > 0) {
      int64_t real_serial_size = 0;
      ROARING_TRY_CATCH(real_serial_size = roaring::api::roaring64_bitmap_portable_serialize(insert_bitmap_, buf + pos));
      if (OB_FAIL(ret)) {
      } else if (insert_bitmap_size != real_serial_size) {
        ret = OB_SERIALIZE_ERROR;
        LOG_WARN("serialize size not match", K(ret), K(insert_bitmap_size), K(real_serial_size));
      } else {
        pos += insert_bitmap_size;
      }
    }
  } else {
    insert_bitmap_size = 0;
    OB_UNIS_ENCODE(insert_bitmap_size);
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr != delete_bitmap_) {
    ROARING_TRY_CATCH(delete_bitmap_size = roaring::api::roaring64_bitmap_portable_size_in_bytes(delete_bitmap_));
    OB_UNIS_ENCODE(delete_bitmap_size);
    if (OB_SUCC(ret) && delete_bitmap_size > 0) {
      int64_t real_serial_size = 0;
      ROARING_TRY_CATCH(real_serial_size = roaring::api::roaring64_bitmap_portable_serialize(delete_bitmap_, buf + pos));
      if (OB_FAIL(ret)) {
      } else if (delete_bitmap_size != real_serial_size) {
        ret = OB_SERIALIZE_ERROR;
        LOG_WARN("serialize size not match", K(ret), K(delete_bitmap_size), K(real_serial_size));
      } else {
        pos += delete_bitmap_size;
      }
    }
  } else {
    delete_bitmap_size = 0;
    OB_UNIS_ENCODE(delete_bitmap_size);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObVectorIndexRoaringBitMap)
{
  int ret = OB_SUCCESS;
  int64_t insert_bitmap_size = 0;
  int64_t delete_bitmap_size = 0;
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(insert_bitmap_size);
    if (insert_bitmap_size > 0) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
      ROARING_TRY_CATCH(insert_bitmap_ = roaring::api::roaring64_bitmap_portable_deserialize_safe(buf + pos, data_len - pos));
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(insert_bitmap_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("failed to deserialize the bitmap", K(ret));
      } else if (!roaring::api::roaring64_bitmap_internal_validate(insert_bitmap_, NULL)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bitmap internal consistency checks failed", K(ret));
      } else {
        pos += insert_bitmap_size;
      }
    }
  }

  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(delete_bitmap_size);
    if (delete_bitmap_size > 0) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
      ROARING_TRY_CATCH(delete_bitmap_ = roaring::api::roaring64_bitmap_portable_deserialize_safe(buf + pos, data_len - pos));
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(delete_bitmap_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("failed to deserialize the bitmap", K(ret));
      } else if (!roaring::api::roaring64_bitmap_internal_validate(delete_bitmap_, NULL)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bitmap internal consistency checks failed", K(ret));
      } else {
        pos += delete_bitmap_size;
      }
    }
  }
  return ret;
}

void ObVecIdxActiveData::free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id)
{
  LOG_INFO("free active memdata", KPC(this), K(lbt())); // remove later
  segment_handle_.reset();
  is_init_ = false;
}

int ObVectorIndexSegmentWriteGuard::prepare_write(ObVecIdxActiveDataHandle& incr_data)
{
  int ret = OB_SUCCESS;
  // need read lock to aviod incr_data_->segment_handle that may be modified by freeze
  TCRLockGuard lock_guard(incr_data->mem_data_rwlock_);
  segment_handle_ = incr_data->segment_handle_;
  segment_handle_->inc_write_ref();
  return ret;
}

void ObVecIdxVBitmapData::free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id)
{
  LOG_INFO("free vbitmap memdata", KPC(this), K(lbt()));
  ObVectorIndexRoaringBitMap::destroy(bitmap_);
  is_init_ = false;
}

void ObVectorIndexSegmentBuilder::free(ObIAllocator &allocator)
{
  LOG_INFO("free segment builder", KPC(this), K(lbt()));
  segment_handle_.reset();
  free_vec_buf_data(allocator);
  ObVectorIndexRoaringBitMap::destroy(ibitmap_);
  ibitmap_ = nullptr;
  ObVectorIndexRoaringBitMap::destroy(vbitmap_);
  vbitmap_ = nullptr;
}

void ObVectorIndexSegmentBuilder::free_vec_buf_data(ObIAllocator &allocator)
{
  LOG_INFO("free vec buf data", KPC(this), K(lbt()));
  if (OB_NOT_NULL(vid_array_)) {
    vid_array_->~ObVecIdxVidArray();
    allocator.free(vid_array_);
    vid_array_ = nullptr;
  }
  if (OB_NOT_NULL(vec_array_)) {
    vec_array_->~ObVecIdxVecArray();
    allocator.free(vec_array_);
    vec_array_ = nullptr;
  }
  if (OB_NOT_NULL(extra_info_buf_)) {
    extra_info_buf_->~ObVecExtraInfoBuffer();
    allocator.free(extra_info_buf_);
    extra_info_buf_ = nullptr;
  }
  // for sparse vector (ipivf_sq)
  if (OB_NOT_NULL(lens_array_)) {
    lens_array_->~ObArray();
    allocator.free(lens_array_);
    lens_array_ = nullptr;
  }
  if (OB_NOT_NULL(dims_array_)) {
    dims_array_->~ObArray();
    allocator.free(dims_array_);
    dims_array_ = nullptr;
  }
  if (OB_NOT_NULL(vals_array_)) {
    vals_array_->~ObArray();
    allocator.free(vals_array_);
    vals_array_ = nullptr;
  }
}

int ObVectorIndexSegmentBuilder::init_vec_buffer(
    const uint64_t tenant_id, ObIAllocator &allocator, const bool is_sparse_vector)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ATOMIC_LOAD(&(vid_array_)))) {
    TCWLockGuard lock_guard(mem_data_rwlock_);
    if (OB_NOT_NULL(ATOMIC_LOAD(&(vid_array_)))) {
      // do nothing
    } else if (OB_ISNULL(vid_array_ = OB_NEWx(ObVecIdxVidArray, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for vid array fail", K(ret));
    } else if (OB_ISNULL(extra_info_buf_ = OB_NEWx(ObVecExtraInfoBuffer, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for vector array fail", K(ret));
    } else if (OB_FALSE_IT(vid_array_->set_attr(ObMemAttr(tenant_id, "VecIdxBuf")))) {
    } else if (is_sparse_vector) {
      // for sparse vector (ipivf_sq)
      if (OB_ISNULL(lens_array_ = OB_NEWx(ObVecIdxLensArray, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for lens array fail", K(ret));
      } else if (OB_ISNULL(dims_array_ = OB_NEWx(ObVecIdxDimsArray, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for dims array fail", K(ret));
      } else if (OB_ISNULL(vals_array_ = OB_NEWx(ObVecIdxValsArray, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for vals array fail", K(ret));
      } else {
        lens_array_->set_attr(ObMemAttr(tenant_id, "VecIdxIPIVFSQ"));
        dims_array_->set_attr(ObMemAttr(tenant_id, "VecIdxIPIVFSQ"));
        vals_array_->set_attr(ObMemAttr(tenant_id, "VecIdxIPIVFSQ"));
      }
    } else if (OB_ISNULL(vec_array_ = OB_NEWx(ObVecIdxVecArray, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for vector array fail", K(ret));
    } else {
      vec_array_->set_attr(ObMemAttr(tenant_id, "VecIdxBuf"));
    }
    if (OB_FAIL(ret)) {
      free_vec_buf_data(allocator);
    }
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::add_to_buffer(float* vectors, int64_t* vids, int dim, char *extra_info_buf, int num, const int64_t &extra_info_actual_size)
{
  int ret = OB_SUCCESS;
  bool has_skip_vid = false;
  // vid_array_ may be released by other thread.
  if (OB_ISNULL(vid_array_) || OB_ISNULL(vec_array_) || (OB_NOT_NULL(extra_info_buf) && OB_ISNULL(extra_info_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null array pointer", K(ret), KP(this), K(vid_array_), K(vec_array_), K(extra_info_buf_));
  } else {
    int64_t skip_cnt = 0;
    for (int i = 0; i < num && OB_SUCC(ret); ++i) {
      bool has_skip_vid = false;
      const int64_t vid = vids[i];
      if (OB_FAIL(check_vid_range(vid, has_skip_vid))) {
        LOG_WARN("check vid range fail", K(ret), KP(vids), K(num));
      } else if (has_skip_vid) {
        ++skip_cnt;
      } else {
        if (OB_FAIL(vid_array_->push_back(vids[i]))) {
          LOG_WARN("failed to push back into vid array", K(ret));
        }
        for (int j = 0; OB_SUCC(ret) && j < dim; j++) {
          if (OB_FAIL(vec_array_->push_back(vectors[i * dim + j]))) {
            LOG_WARN("failed to push back into vector array", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(extra_info_buf)) {
          if (OB_FAIL(extra_info_buf_->append(extra_info_buf + i * extra_info_actual_size, extra_info_actual_size))) {
            LOG_WARN("failed to append extra info buf", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(ibitmap_) && OB_NOT_NULL(ibitmap_->insert_bitmap_)) {
          lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ibitmap_->tenant_id_, "VIBitmap"));
          // no need lock beacuse caller add
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ibitmap_->insert_bitmap_, vid));
        }
      }
    }
    LOG_INFO("HgraphIndex add into cache array success", K(ret), K(dim), K(num), K(vids[0]), K(vids[num - 1]), K(vid_array_->count()), K(skip_cnt));
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::add_to_buffer(uint32_t *lens, uint32_t *dims, float *vals, int64_t* vids, char *extra_info_buf, int num, const int64_t &extra_info_actual_size)
{
  int ret = OB_SUCCESS;
  bool has_skip_vid = false;
  // vid_array_ may be released by other thread.
  if (OB_ISNULL(vid_array_) || OB_ISNULL(lens_array_) ||
      OB_ISNULL(dims_array_) || OB_ISNULL(vals_array_) ||
      (OB_NOT_NULL(extra_info_buf) && OB_ISNULL(extra_info_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null array pointer for sparse vector", K(ret), KP(vid_array_),
      KP(lens_array_), KP(dims_array_), KP(vals_array_), KP(extra_info_buf), KP(extra_info_buf_));
  } else {
    int64_t skip_cnt = 0;
    uint32_t total_length = 0;
    for (int i = 0; i < num && OB_SUCC(ret); ++i) {
      bool has_skip_vid = false;
      const int64_t vid = vids[i];
      if (OB_FAIL(check_vid_range(vid, has_skip_vid))) {
        LOG_WARN("check vid range fail", K(ret), KP(vids), K(num));
      } else if (has_skip_vid) {
        ++skip_cnt;
        if (OB_NOT_NULL(lens)) {
          total_length += lens[i];
        }
      } else {
        if (OB_FAIL(vid_array_->push_back(vids[i]))) {
          LOG_WARN("failed to push back into vid array", K(ret));
        }
        // for sparse vector (ipivf_sq)
        if (OB_SUCC(ret) && OB_NOT_NULL(lens) && OB_NOT_NULL(dims) && OB_NOT_NULL(vals)) {
          if (OB_FAIL(lens_array_->push_back(lens[i]))) {
            LOG_WARN("failed to push back into lens array", K(ret));
          }
          for (int j = 0; OB_SUCC(ret) && j < lens[i]; j++) {
            if (OB_FAIL(dims_array_->push_back(dims[total_length + j]))) {
              LOG_WARN("failed to push back into dims array", K(ret));
            } else if (OB_FAIL(vals_array_->push_back(vals[total_length + j]))) {
              LOG_WARN("failed to push back into vals array", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            total_length += lens[i];
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(extra_info_buf)) {
          if (OB_FAIL(extra_info_buf_->append(extra_info_buf + i * extra_info_actual_size, extra_info_actual_size))) {
            LOG_WARN("failed to append extra info buf", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(ibitmap_) && OB_NOT_NULL(ibitmap_->insert_bitmap_)) {
          lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ibitmap_->tenant_id_, "VIBitmap"));
          // no need lock beacuse caller add
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ibitmap_->insert_bitmap_, vid));
        }
      }
    }
    LOG_INFO("SindiIndex add into cache array success", K(ret), K(num), K(vids[0]), K(vids[num - 1]), K(vid_array_->count()), K(skip_cnt), K(total_length));
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::create_and_build(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
  lib::ObLightBacktraceGuard light_backtrace_guard(false);
  if (OB_FAIL(ObVectorIndexSegment::create(segment_handle_,
      tenant_id,
      allocator,
      param,
      param.type_,
      param.m_,
      adaptor))) {
    LOG_WARN("create segment fail", K(ret));
  } else if (OB_FAIL(segment_handle_->build_index(
      vec_array_->get_data(),
      vid_array_->get_data(),
      param.dim_,
      vid_array_->count(),
      extra_info_buf_->ptr()))) {
    LOG_WARN("failed to build vsag index.", K(ret));
  } else {
    LOG_INFO("HgraphIndex build success", K(ret), K(param.dim_), K(vid_array_->count()));
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::create_and_add(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    const ObVectorIndexAlgorithmType build_type,
    const int max_degree,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
  lib::ObLightBacktraceGuard light_backtrace_guard(false);
  if (OB_FAIL(ObVectorIndexSegment::create(segment_handle_,
      tenant_id,
      allocator,
      param,
      build_type,
      max_degree,
      adaptor))) {
    LOG_WARN("create segment fail", K(ret), K(param));
  } else if (OB_FAIL(segment_handle_->add_index(
      vec_array_->get_data(),
      vid_array_->get_data(),
      param.dim_,
      extra_info_buf_->ptr(),
      vid_array_->count()))) {
    LOG_WARN("failed to add vsag index.", K(ret), KPC(this));
  } else {
    LOG_INFO("HNSW build index success", K(ret), K(param.dim_), K(vid_array_->count()));
  }
  free_vec_buf_data(allocator);
  return ret;
}

int ObVectorIndexSegmentBuilder::build_if_need(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  if (need_build()) {
    if (adaptor->is_sparse_vector_index_type()) {
      if (OB_FAIL(create_and_build_sparse_index_from_arrays(
          tenant_id,
          allocator,
          param,
          adaptor))) {
        LOG_WARN("create segment fail", K(ret));
      }
    } else if (OB_FAIL(create_and_build(
        tenant_id,
        allocator,
        param,
        adaptor))) {
      LOG_WARN("create segment fail", K(ret));
    }
    if (OB_SUCC(ret)) {
      has_build_ = true;
      free_vec_buf_data(allocator);
    }
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::create_and_build_sparse_index_from_arrays(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  ObVecIdxVidArray *vid_array = vid_array_;
  ObVecExtraInfoBuffer *extra_info_buf = extra_info_buf_;
  ObVecIdxLensArray *lens_array = lens_array_;
  ObVecIdxDimsArray *dims_array = dims_array_;
  ObVecIdxValsArray *vals_array = vals_array_;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
  lib::ObLightBacktraceGuard light_backtrace_guard(false);
  if (OB_FAIL(ObVectorIndexSegment::create(segment_handle_,
      tenant_id,
      allocator,
      param,
      param.type_,
      param.m_,
      adaptor))) {
    LOG_WARN("create segment fail", K(ret));
  } else if (OB_FAIL(segment_handle_->build_index(
      lens_array->get_data(),
      dims_array->get_data(),
      vals_array->get_data(),
      vid_array->get_data(),
      vid_array->count(),
      extra_info_buf->ptr()))) {
  } else {
    LOG_INFO("IPIVF Index build success", K(ret), K(vid_array_->count()));
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::create_and_add_sparse_index_from_arrays(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const ObVectorIndexParam &param,
    const ObVectorIndexAlgorithmType build_type,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  ObVecIdxVidArray *vid_array = vid_array_;
  ObVecIdxLensArray *lens_array = lens_array_;
  ObVecIdxDimsArray *dims_array = dims_array_;
  ObVecIdxValsArray *vals_array = vals_array_;
  ObVecExtraInfoBuffer *extra_info_buf = extra_info_buf_;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
  lib::ObLightBacktraceGuard light_backtrace_guard(false);
  if (OB_FAIL(ObVectorIndexSegment::create(
      segment_handle_,
      tenant_id,
      allocator,
      param,
      build_type,
      param.m_,
      adaptor))) {
    LOG_WARN("failed to create sparse vsag index.", K(ret));
  } else if (OB_FAIL(segment_handle_->add_index(
      lens_array->get_data(),
      dims_array->get_data(),
      vals_array->get_data(),
      vid_array->get_data(),
      vid_array->count(),
      extra_info_buf->ptr()))) {
    LOG_WARN("failed to add sparse vsag index.", K(ret));
  } else {
    LOG_INFO("IPIVF build index success", K(ret), K(vid_array_->count()));
  }
  free_vec_buf_data(allocator);
  return ret;
}

int ObVectorIndexSegmentBuilder::serialize(const uint64_t tenant_id, ObHNSWSerializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  if (nullptr != vbitmap_) {
    if (nullptr == ibitmap_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ibitmap is not null", K(ret));
    } else {
      segment_handle_->ibitmap_ = ibitmap_;
      segment_handle_->vbitmap_ = vbitmap_;
      param.need_serde_meta_ = true;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(segment_handle_->serialize(tenant_id, param))) {
    LOG_WARN("serialize segment fail", K(ret));
  }
  if (OB_SUCC(ret)) {
    ibitmap_ = nullptr;
    vbitmap_ = nullptr;

    int64_t ibitmap_insert = 0;
    int64_t ibitmap_delete = 0;
    int64_t vbitmap_insert = 0;
    int64_t vbitmap_delete = 0;
    int64_t ibitmap_insert_min_vid = 0;
    int64_t ibitmap_insert_max_vid = 0;
    int64_t ibitmap_delete_min_vid = 0;
    int64_t ibitmap_delete_max_vid = 0;
    int64_t vbitmap_insert_min_vid = 0;
    int64_t vbitmap_insert_max_vid = 0;
    int64_t vbitmap_delete_min_vid = 0;
    int64_t vbitmap_delete_max_vid = 0;
    if (OB_NOT_NULL(segment_handle_->ibitmap_) && OB_NOT_NULL(segment_handle_->ibitmap_->insert_bitmap_)) {
      ibitmap_insert = roaring64_bitmap_get_cardinality(segment_handle_->ibitmap_->insert_bitmap_);
      ibitmap_insert_min_vid = roaring64_bitmap_minimum(segment_handle_->ibitmap_->insert_bitmap_);
      ibitmap_insert_max_vid = roaring64_bitmap_maximum(segment_handle_->ibitmap_->insert_bitmap_);
    }
    if (OB_NOT_NULL(segment_handle_->ibitmap_) &&OB_NOT_NULL(segment_handle_->ibitmap_->delete_bitmap_)) {
      ibitmap_delete = roaring64_bitmap_get_cardinality(segment_handle_->ibitmap_->delete_bitmap_);
      ibitmap_delete_min_vid = roaring64_bitmap_minimum(segment_handle_->ibitmap_->delete_bitmap_);
      ibitmap_delete_max_vid = roaring64_bitmap_maximum(segment_handle_->ibitmap_->delete_bitmap_);
    }
    if (OB_NOT_NULL(segment_handle_->vbitmap_) && OB_NOT_NULL(segment_handle_->vbitmap_->insert_bitmap_)) {
      vbitmap_insert = roaring64_bitmap_get_cardinality(segment_handle_->vbitmap_->insert_bitmap_);
      vbitmap_insert_min_vid = roaring64_bitmap_minimum(segment_handle_->vbitmap_->insert_bitmap_);
      vbitmap_insert_max_vid = roaring64_bitmap_maximum(segment_handle_->vbitmap_->insert_bitmap_);
    }
    if (OB_NOT_NULL(segment_handle_->vbitmap_) && OB_NOT_NULL(segment_handle_->vbitmap_->delete_bitmap_)) {
      vbitmap_delete = roaring64_bitmap_get_cardinality(segment_handle_->vbitmap_->delete_bitmap_);
      vbitmap_delete_min_vid = roaring64_bitmap_minimum(segment_handle_->vbitmap_->delete_bitmap_);
      vbitmap_delete_max_vid = roaring64_bitmap_maximum(segment_handle_->vbitmap_->delete_bitmap_);
    }
    LOG_INFO("serialize segment success", K(ret),
      K(ibitmap_insert), K(ibitmap_insert_min_vid), K(ibitmap_insert_max_vid),
      K(ibitmap_delete), K(ibitmap_delete_min_vid), K(ibitmap_delete_max_vid),
      K(vbitmap_insert), K(vbitmap_insert_min_vid), K(vbitmap_insert_max_vid),
      K(vbitmap_delete), K(vbitmap_delete_min_vid), K(vbitmap_delete_max_vid),
      KPC(this));
  } else {
    segment_handle_->ibitmap_ = nullptr;
    segment_handle_->vbitmap_ = nullptr;
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::check_vid_range(const int64_t vid, bool &has_skip_vid)
{
  int ret = OB_SUCCESS;
  has_skip_vid = false;
  if (! need_vid_check_) { // skip check
  } else if (! vid_bound_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vid bound is invalid", K(vid_bound_));
  } else {
    const bool in_vid_bound = vid >= vid_bound_.min_vid_ && vid <= vid_bound_.max_vid_;
    // const bool in_ii_bitmap = roaring::api::roaring64_bitmap_contains(ibitmap_->insert_bitmap_, vid);
    // const bool in_vi_bitmap = roaring::api::roaring64_bitmap_contains(vbitmap_->insert_bitmap_, vid);
    // const bool in_vd_bitmap = roaring::api::roaring64_bitmap_contains(vbitmap_->delete_bitmap_, vid);
    ++total_cnt_;
    if (! in_vid_bound) {
      has_skip_vid = true;
      ++skip_cnt_;
    } else {
      ++add_cnt_;
    }
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::add_index(float* vecs, int64_t* ids, int dim, char *extra_info, int size)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    bool has_skip_vid = false;
    const int64_t vid = ids[i];
    if (OB_FAIL(check_vid_range(vid, has_skip_vid))) {
      LOG_WARN("check vid range fail", K(ret), KP(ids), K(size));
    } else if (has_skip_vid) {
    } else if (OB_FAIL(segment_handle_->add_index(vecs, ids + i, dim, extra_info, 1))) {
      LOG_WARN("failed to add index.", K(ret), K(dim), K(size));
    } else if (OB_NOT_NULL(ibitmap_) && OB_NOT_NULL(ibitmap_->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ibitmap_->tenant_id_, "VIBitmap"));
      TCWLockGuard lock_guard(ibitmap_->rwlock_);
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ibitmap_->insert_bitmap_, vid));
    }
  }
  return ret;
}

int ObVectorIndexSegmentBuilder::add_index(uint32_t *lens, uint32_t *dims, float *vals, int64_t *ids, int size, char *extra_infos)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    bool has_skip_vid = false;
    const int64_t vid = ids[i];
    if (OB_FAIL(check_vid_range(vid, has_skip_vid))) {
      LOG_WARN("check vid range fail", K(ret), KP(ids), K(size));
    } else if (has_skip_vid){
    } else if (OB_FAIL(segment_handle_->add_index(lens, dims, vals, ids + i, 1, extra_infos))) {
      LOG_WARN("failed to add index.", K(ret), K(size));
    } else if (OB_NOT_NULL(ibitmap_) && OB_NOT_NULL(ibitmap_->insert_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ibitmap_->tenant_id_, "VIBitmap"));
      TCWLockGuard lock_guard(ibitmap_->rwlock_);
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(ibitmap_->insert_bitmap_, vid));
    }
  }
  return ret;
}

void ObVecIdxSnapshotData::free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id)
{
  LOG_INFO("free snap memdata", KPC(this), K(allocator), K(lbt()));
  if (OB_NOT_NULL(builder_)) {
    builder_->free(*allocator);
    builder_->~ObVectorIndexSegmentBuilder();
    allocator->free(builder_);
    builder_ = nullptr;
  }
  meta_.release(allocator, tenant_id);
  is_init_ = false;
}

int ObVecIdxSnapshotData::free_segment_memory()
{
  int ret = OB_SUCCESS;
  LOG_INFO("free segment memory", KPC(this), K(lbt()));
  for (int64_t i = 0; i < meta_.incrs_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    seg_meta.segment_handle_.reset();
  }
  for (int64_t i = 0; i < meta_.bases_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    seg_meta.segment_handle_.reset();
  }
  rb_flag_ = true;
  return ret;
}

int ObVecIdxSnapshotData::get_snap_index_row_cnt(int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    int64_t seg_vec_cnt = 0;
    if (! seg_meta.segment_handle_.is_valid()) { // skip
    } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(seg_vec_cnt))) {
      LOG_WARN("cal_distance_by_id fail", K(ret), K(i), K(seg_meta));
    } else {
      LOG_DEBUG("segment vector count", K(seg_vec_cnt), K(count));
      count += seg_vec_cnt;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    int64_t seg_vec_cnt = 0;
    if (! seg_meta.segment_handle_.is_valid()) { // skip
    } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(seg_vec_cnt))) {
      LOG_WARN("cal_distance_by_id fail", K(ret), K(i), K(seg_meta));
    } else {
      LOG_DEBUG("segment vector count", K(seg_vec_cnt), K(count));
      count += seg_vec_cnt;
    }
  }
  return ret;
}

int64_t ObVecIdxSnapshotData::get_snap_mem_used() const
{
  int64_t hold = 0;
  for (int64_t i = 0; i < meta_.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      hold += seg_meta.segment_handle_->get_mem_used();
    }
  }
  for (int64_t i = 0; i < meta_.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      hold += seg_meta.segment_handle_->get_mem_used();
    }
  }
  return hold;
}

int64_t ObVecIdxSnapshotData::get_snap_mem_hold() const
{
  int64_t hold = 0;
  for (int64_t i = 0; i < meta_.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      hold += seg_meta.segment_handle_->get_mem_hold();
    }
  }
  for (int64_t i = 0; i < meta_.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      hold += seg_meta.segment_handle_->get_mem_hold();
    }
  }
  return hold;
}

bool ObVecIdxSnapshotData::check_incr_mem_over_percentage(const int64_t incr_memory_percentage) const
{
  bool is_over_percentage = false;
  int64_t incr_hold = 0;
  int64_t base_hold = 0;
  for (int64_t i = 0; i < meta_.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      incr_hold += seg_meta.segment_handle_->get_mem_used();
    }
  }
  for (int64_t i = 0; i < meta_.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      base_hold += seg_meta.segment_handle_->get_mem_used();
    }
  }
  return base_hold > 0 && incr_memory_percentage > 0 && incr_hold * 100 > incr_memory_percentage * (incr_hold + base_hold);
}

bool ObVecIdxSnapshotData::check_incr_can_merge_base() const
{
  bool can_merge = false;
  if (meta_.incrs_.count() == 1) {
    const ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(0);
    const ObVectorIndexSegmentHandle &handle = seg_meta.segment_handle_;
    if (! handle.is_valid()) {
      LOG_INFO("segment handle is not load, so can not merge", KP(this), K(seg_meta));
    } else {
      uint64_t diff_cnt = 0;
      const bool has_vbitmap = (OB_NOT_NULL(handle->vbitmap_) && OB_NOT_NULL(handle->vbitmap_->insert_bitmap_));
      const bool has_ibitmap = (OB_NOT_NULL(handle->ibitmap_) && OB_NOT_NULL(handle->ibitmap_->insert_bitmap_));
      roaring::api::roaring64_bitmap_t *vbitmap = nullptr;
      roaring::api::roaring64_bitmap_t *ibitmap = nullptr;
      if (has_vbitmap && has_ibitmap) {
        vbitmap = handle->vbitmap_->insert_bitmap_;
        ibitmap = handle->ibitmap_->insert_bitmap_;
        diff_cnt = roaring64_bitmap_xor_cardinality(ibitmap, vbitmap);
      } else if (has_vbitmap) {
        vbitmap = handle->vbitmap_->insert_bitmap_;
        diff_cnt = roaring64_bitmap_get_cardinality(vbitmap);
      } else if (has_ibitmap) {
        ibitmap = handle->ibitmap_->insert_bitmap_;
        diff_cnt = roaring64_bitmap_get_cardinality(ibitmap);
      }
      if (0 == diff_cnt) {
        can_merge = true;
        LOG_INFO("diff is empty, merge from base", KP(this), K(diff_cnt));
      } else {
        LOG_INFO("diff is not empty, need to wait", KP(this), K(diff_cnt));
      }
    }
  }
  return can_merge;
}

ObVectorIndexAlgorithmType ObVecIdxSnapshotData::get_snap_index_type() const
{
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  if (meta_.bases_.count() > 0 && meta_.bases_.at(0).segment_handle_.is_valid()) {
    index_type = meta_.bases_.at(0).segment_handle_->get_index_type();
  }
  return index_type;
}

int ObVecIdxSnapshotData::get_snap_vid_bound(int64_t &min_vid, int64_t &max_vid)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.incrs_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = meta_.incrs_.at(i);
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    if (OB_FAIL(seg_meta.segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid))) {
      LOG_WARN("get_vid_bound fail", K(ret), K(i), K(seg_meta));
    } else {
      min_vid = OB_MIN(min_vid, seg_min_vid);
      max_vid = OB_MAX(max_vid, seg_max_vid);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.bases_.count(); ++i) {
    ObVectorIndexSegmentMeta &seg_meta = meta_.bases_.at(i);
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    if (OB_FAIL(seg_meta.segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid))) {
      LOG_WARN("get_vid_bound fail", K(ret), K(i), K(seg_meta));
    } else {
      min_vid = OB_MIN(min_vid, seg_min_vid);
      max_vid = OB_MAX(max_vid, seg_max_vid);
    }
  }
  return ret;
}

int ObVecIdxSnapshotData::build_meta(
    const ObTabletID &tablet_id, const int64_t snapshot_version, const int64_t blocks_cnt)
{
  int ret = OB_SUCCESS;
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  ObVectorIndexSegmentMeta seg_meta;
  if (OB_ISNULL(builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder is null", K(ret), KPC(this));
  } else if (meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is valid, so can not build again", K(ret), K(meta_));
  } else if (OB_FALSE_IT(index_type = builder_->segment_handle_->get_index_type())) {
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(
      index_type, tablet_id, snapshot_version, 0,
      seg_meta.start_key_buf_, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, seg_meta.start_key_))) {
    LOG_WARN("get start key fail", K(ret), K(index_type), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(
      index_type, tablet_id, snapshot_version, blocks_cnt - 1,
      seg_meta.end_key_buf_, OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH, seg_meta.end_key_))) {
    LOG_WARN("get end key fail", K(ret), K(index_type), K(tablet_id), K(snapshot_version));
  } else {
    seg_meta.seg_type_ = builder_->seg_type_;
    seg_meta.index_type_ = index_type;
    builder_->segment_handle_->set_is_base();
    seg_meta.scn_ = snapshot_version;
    seg_meta.blocks_cnt_ = blocks_cnt;
    seg_meta.segment_handle_ = builder_->segment_handle_;
    seg_meta.has_segment_meta_row_ = seg_meta.segment_handle_->vbitmap_ != nullptr;
    meta_.header_.scn_ = snapshot_version;
    if (OB_FAIL(meta_.bases_.push_back(seg_meta))) {
      LOG_WARN("push back seg meta fail", K(ret), K(seg_meta));
    } else if (OB_FAIL(meta_.bases_.at(meta_.bases_.count() - 1).deep_copy_seg_key(seg_meta.start_key_, seg_meta.end_key_))) {
      LOG_WARN("failed to deep copy seg key", K(ret), K(seg_meta), K(index_type), K(tablet_id), K(snapshot_version));
    } else if (! meta_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta is not valid after build", K(ret), K(meta_), K(index_type), K(tablet_id), K(snapshot_version));
    } else {
      LOG_INFO("add_base_segment_meta success", K(meta_));
    }
  }
  return ret;
}

int ObVecIdxSnapshotData::build_finished(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (! meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is not valid, so can not finish", K(ret), K(meta_));
  } else if (OB_ISNULL(builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder is null, may be finish twice", K(ret));
  } else if (meta_.bases_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base count is invalid", K(ret), K(meta_));
  } else if (builder_->segment_handle_.get() != meta_.bases_.at(0).segment_handle_.get()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base segment is not build segment", K(ret), KPC(builder_), K(meta_));
  } else {
    last_res_seg_info_.reset();
    int64_t min_vid = 0;
    int64_t max_vid = 0;
    if (OB_FAIL(builder_->segment_handle_->get_index_number(last_res_seg_info_.vector_cnt_))) {
      LOG_WARN("get_index_number fail", K(ret), K(builder_->segment_handle_));
    } else if (OB_FAIL(builder_->segment_handle_->get_vid_bound(min_vid, max_vid))) {
      LOG_WARN("get_vid_bound fail", K(ret), K(builder_->segment_handle_));
    } else {
      last_res_seg_info_.mem_used_ = builder_->segment_handle_->get_mem_used();
      last_res_seg_info_.min_vid_ = min_vid;
      last_res_seg_info_.max_vid_ = max_vid;
    }
    builder_->segment_handle_.reset();
    builder_->free(allocator);
    builder_->~ObVectorIndexSegmentBuilder();
    allocator.free(builder_);
    builder_ = nullptr;
  }
  return ret;
}

int ObVecIdxSnapshotData::immutable_optimize_snap()
{
  int ret = OB_SUCCESS;
  if (meta_.bases_.count() == 0) {
    LOG_INFO("empty snap, so donot optimize", K(meta_));
  } else if (meta_.bases_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap count is not expected", K(meta_));
  } else if (meta_.bases_.at(0).segment_handle_.is_valid()
      && OB_FAIL(meta_.bases_.at(0).segment_handle_->immutable_optimize())) {
    LOG_WARN("immutable_optimize fail", K(ret), K(meta_));
  }
  return ret;
}

static int build_rowkey_range(const ObVectorIndexSegmentMeta& seg_meta, const ObCollationType key_col_cs_type, ObArray<ObObj> &rowkey_objs, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = rowkey_objs.count() / 2;
  for (int64_t i = 0; i < rowkey_cnt; ++i) {
    if (0 == i) {
      rowkey_objs[i].reset();
      rowkey_objs[i].set_varchar(seg_meta.start_key_);
      rowkey_objs[i].set_collation_type(key_col_cs_type);
    } else {
      rowkey_objs[i] = ObObj::make_min_obj();
    }
  }
  ObRowkey min_row_key(rowkey_objs.get_data(), rowkey_cnt);
  for (int64_t i = rowkey_cnt; i < rowkey_objs.count(); ++i) {
    if (i - rowkey_cnt == 0) {
      rowkey_objs[i].reset();
      rowkey_objs[i].set_varchar(seg_meta.end_key_);
      rowkey_objs[i].set_collation_type(key_col_cs_type);
    } else {
      rowkey_objs[i] = ObObj::make_max_obj();
    }
  }
  ObRowkey max_row_key(rowkey_objs.get_data() + rowkey_cnt, rowkey_cnt);

  range.table_id_ = 0; // TODO make sure this is correct
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

static int rescan(
    const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
    const uint64_t timeout, ObTableScanIterator *table_scan_iter, ObArray<ObObj> &rowkey_objs)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObCollationType key_col_cs_type = scan_param.table_param_->get_read_info().get_columns()->at(0)->get_meta_type().get_collation_type();
  ObNewRange range;
  scan_param.key_ranges_.reuse();
  // update timeout
  scan_param.timeout_ = timeout;
  scan_param.for_update_wait_timeout_ = scan_param.timeout_;
  if (OB_FAIL(build_rowkey_range(seg_meta, key_col_cs_type, rowkey_objs, range))) {
    LOG_WARN("build_range fail", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param), K(range));
  } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, table_scan_iter))) {
    LOG_WARN("reuse scan iter fail", K(ret));
  } else if (OB_FAIL(oas->table_rescan(scan_param, table_scan_iter))) {
    LOG_WARN("do table rescan fail", K(ret));
  }
  return ret;
}

int ObVecIdxSnapshotData::load_segment(ObIAllocator &allocator, ObPluginVectorIndexAdaptor *adaptor,
    ObTableScanIterator *snap_data_iter, const uint64_t tenant_id, ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("VISerde", OB_MALLOC_MIDDLE_BLOCK_SIZE, tenant_id);
  ObHNSWDeserializeCallback::CbParam param;
  param.iter_ = snap_data_iter;
  param.allocator_ = &tmp_allocator;
  param.seg_meta_ = &seg_meta;
  if (OB_FAIL(ObVectorIndexSegment::deserialize(seg_meta.segment_handle_, tenant_id, adaptor, param))) {
    LOG_WARN("serialize index failed.", K(ret));
  }
  return ret;
}

int ObVecIdxSnapshotData::load_segment(ObPluginVectorIndexAdaptor *adaptor,
    ObIAllocator &allocator, const uint64_t tenant_id, const ObLSID& ls_id, const ObTabletID &tablet_id,
    const share::SCN &target_scn, ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;

  if (OB_FAIL(ObPluginVectorIndexUtils::open_segment_data_iter(adaptor, allocator, ls_id,
                                tablet_id, seg_meta.start_key_, seg_meta.end_key_,
                                target_scn, snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("open_segment_data_iter fail", K(ret));
  } else {
    ObArenaAllocator tmp_allocator("VISerde", OB_MALLOC_MIDDLE_BLOCK_SIZE, tenant_id);
    ObHNSWDeserializeCallback::CbParam param;
    param.iter_ = snap_data_iter;
    param.allocator_ = &tmp_allocator;
    param.seg_meta_ = &seg_meta;
    if (OB_FAIL(ObVectorIndexSegment::deserialize(seg_meta.segment_handle_, tenant_id, adaptor, param))) {
      LOG_WARN("serialize index failed.", K(ret));
    }
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  return ret;
}

int ObVecIdxSnapshotData::load_persist_segments(
    ObPluginVectorIndexAdaptor *adaptor, ObIAllocator &allocator,
    storage::ObTableScanParam &scan_param, ObTableScanIterator *table_scan_iter)
{
  int ret = OB_SUCCESS;
  int64_t vec_cnt = 0;
  ObArray<ObObj> rowkey_objs;
  int64_t rowkey_cnt = scan_param.table_param_->get_read_info().get_schema_rowkey_count();
  if (rowkey_objs.count() < rowkey_cnt && OB_FAIL(rowkey_objs.prepare_allocate(rowkey_cnt*2))) {
    LOG_WARN("prepare rowkey array fail", K(ret), K(rowkey_cnt));
  }
  for (int64_t i = 0; i < meta_.incrs_.count() && OB_SUCC(ret); ++i) {
    ObVectorIndexSegmentMeta& seg_meta = meta_.incrs_.at(i);
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    if (seg_meta.segment_handle_.is_valid()) {
      LOG_INFO("incr segment has been load, so skip", K(i), K(seg_meta));
    } else if (OB_FAIL(rescan(seg_meta, scan_param, timeout_us, table_scan_iter, rowkey_objs))) {
      LOG_WARN("rescan fail", K(ret));
    } else if (OB_FAIL(load_segment(allocator, adaptor, table_scan_iter, adaptor->get_tenant_id(), seg_meta))) {
      LOG_WARN("load_segment fail", K(ret), K(i), K(seg_meta));
    }
  }
  for (int64_t i = 0; i < meta_.bases_.count() && OB_SUCC(ret); ++i) {
    ObVectorIndexSegmentMeta& seg_meta = meta_.bases_.at(i);
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    if (seg_meta.segment_handle_.is_valid()) {
      LOG_INFO("base segment has been load, so skip", K(i), K(seg_meta));
    } else if (OB_FAIL(rescan(seg_meta, scan_param, timeout_us, table_scan_iter, rowkey_objs))) {
      LOG_WARN("rescan fail", K(ret));
    } else if (OB_FAIL(load_segment(allocator, adaptor, table_scan_iter, adaptor->get_tenant_id(), seg_meta))) {
      LOG_WARN("load_segment fail", K(ret), K(i), K(seg_meta));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_snap_vid_bound(vid_bound_.min_vid_, vid_bound_.max_vid_))) {
    LOG_WARN("get snap vid bound fail", K(ret), K(meta_));
  } else if (OB_FAIL(get_snap_index_row_cnt(vec_cnt))) {
    LOG_WARN("get snap index row cnt fail", K(ret), K(meta_));
  } else {
    LOG_INFO("load persist segments success", K(vec_cnt), K(vid_bound_), K(meta_));
  }
  return ret;
}

int ObVecIdxSnapshotData::load_persist_segments(
    ObPluginVectorIndexAdaptor *adaptor, ObIAllocator &allocator, const ObLSID& ls_id, const share::SCN &target_scn)
{
  int ret = OB_SUCCESS;
  int64_t vec_cnt = 0;
  for (int64_t i = 0; i < meta_.incrs_.count() && OB_SUCC(ret); ++i) {
    ObVectorIndexSegmentMeta& seg_meta = meta_.incrs_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      LOG_INFO("incr segment has been load, so skip", K(i), K(seg_meta));
    } else if (OB_FAIL(load_segment(adaptor, allocator, adaptor->get_tenant_id(), ls_id, adaptor->get_snap_tablet_id(), target_scn, seg_meta))) {
      LOG_WARN("load_segment fail", K(ret), K(i), K(seg_meta));
    }
  }
  for (int64_t i = 0; i < meta_.bases_.count() && OB_SUCC(ret); ++i) {
    ObVectorIndexSegmentMeta& seg_meta = meta_.bases_.at(i);
    if (seg_meta.segment_handle_.is_valid()) {
      LOG_INFO("base segment has been load, so skip", K(i), K(seg_meta));
    } else if (OB_FAIL(load_segment(adaptor, allocator, adaptor->get_tenant_id(), ls_id, adaptor->get_snap_tablet_id(), target_scn, seg_meta))) {
      LOG_WARN("load_segment fail", K(ret), K(i), K(seg_meta));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_snap_vid_bound(vid_bound_.min_vid_, vid_bound_.max_vid_))) {
    LOG_WARN("get snap vid bound fail", K(ret), K(meta_));
  } else if (OB_FAIL(get_snap_index_row_cnt(vec_cnt))) {
    LOG_WARN("get snap index row cnt fail", K(ret), K(meta_));
  } else {
    LOG_INFO("load persist segments success", K(vec_cnt), K(vid_bound_), K(meta_));
  }
  return ret;
}

int ObVecIdxSnapshotData::load_segment(
    const uint64_t tenant_id, ObPluginVectorIndexAdaptor *adaptor, ObHNSWDeserializeCallback::CbParam &param)
{
  int ret = OB_SUCCESS;
  ObHNSWDeserializeCallback callback(adaptor);
  ObVectorIndexSerializer index_seri(*param.allocator_);
  if (OB_FAIL(index_seri.deserialize(meta_, param, callback, tenant_id))) {
    LOG_WARN("serialize index failed.", K(ret));
  } else if (OB_FAIL(get_snap_vid_bound(vid_bound_.min_vid_, vid_bound_.max_vid_))) {
    LOG_WARN("get snap vid bound fail", K(ret), K(meta_));
  }
  return ret;
}

void ObVecIdxFrozenData::reset()
{
  LOG_INFO("reset frozen memdata", KPC(this), K(lbt()));
  state_ = NO_FROZEN;
  ret_code_ = 0;
  retry_cnt_ = 0;
  frozen_scn_.reset();
  segment_handle_.reset();
  vbitmap_.reset();
}

void ObVecIdxFrozenData::reuse()
{
  LOG_INFO("reuse frozen memdata", KPC(this), K(lbt()));
  state_ = NO_FROZEN;
  ret_code_ = 0;
  retry_cnt_ = 0;
  frozen_scn_.reset();
  segment_handle_.reset();
  vbitmap_.reset();
}

void ObVecIdxFrozenData::free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id)
{
  LOG_INFO("free frozen memdata", KPC(this), K(lbt()));
  segment_handle_.reset();
  vbitmap_.reset();
  is_init_ = false;
}

int ObVectorIndexSegQueryHandler::knn_search(ObVsagQueryResult &result)
{
  int ret = OB_SUCCESS;
  int64_t vec_cnt = 0;
  if (OB_ISNULL(segment_querier_) || OB_ISNULL(ctx_) || OB_ISNULL(query_cond_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(segment_querier_), KP(ctx_), KP(query_cond_));
  } else if (OB_FAIL(segment_querier_->handle_->get_index_number(vec_cnt))) {
    LOG_WARN("failed to get inc index number.", K(ret));
  } else if (vec_cnt < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index number", K(ret), K(vec_cnt));
  } else if (vec_cnt == 0) {
    LOG_INFO("empty index, no need seach", K(ret), K(segment_querier_->handle_), K(vec_cnt));
  } else if (query_cond_->query_limit_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid query limit", K(ret), K(query_cond_->query_limit_));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ctx_->tenant_id_, "VIndexVsagADP"));
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    result.mem_ctx_ = segment_querier_->handle_->mem_ctx_;
    if (! query_cond_->is_post_with_filter_) {
      if (is_sparse_vector_) {
        if (OB_FAIL(obvectorutil::knn_search(segment_querier_->handle_->index_,
            sparse_lens_[0],
            sparse_dims_,
            sparse_vals_,
            query_cond_->query_limit_,
            result.distances_,
            result.vids_,
            result.extra_info_buf_ptr_,
            result.total_,
            query_cond_->ob_sparse_drop_ratio_search_,
            query_cond_->n_candidate_,
            segment_querier_->filter_,
            segment_querier_->reverse_filter_,
            segment_querier_->is_extra_info_filter(),
            valid_ratio_,
            &ctx_->search_allocator_,
            query_cond_->extra_column_count_ > 0,
            segment_querier_->valid_vid_,
            segment_querier_->valid_vid_count_))) {
          LOG_WARN("knn search sparse vector failed.", K(ret));
        }
      } else if (OB_FAIL(obvectorutil::knn_search(segment_querier_->handle_->index_,
          query_vector_,
          dim_,
          query_cond_->query_limit_,
          result.distances_,
          result.vids_,
          result.extra_info_buf_ptr_,
          result.total_,
          query_cond_->ef_search_,
          segment_querier_->filter_,
          segment_querier_->reverse_filter_,
          segment_querier_->is_extra_info_filter(),
          valid_ratio_,
          &ctx_->search_allocator_,
          query_cond_->extra_column_count_ > 0,
          query_cond_->distance_threshold_))) {
        LOG_WARN("knn search dense vector failed.", K(ret));
      }
    } else if (is_sparse_vector_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("spare vector is not support iterative filter", K(ret));
    } else if (OB_FAIL(obvectorutil::knn_search(segment_querier_->handle_->index_,
        query_vector_,
        dim_,
        query_cond_->query_limit_,
        result.distances_,
        result.vids_,
        result.extra_info_buf_ptr_,
        result.total_,
        query_cond_->ef_search_,
        segment_querier_->filter_,
        segment_querier_->reverse_filter_,
        segment_querier_->is_extra_info_filter(),
        valid_ratio_,
        &ctx_->search_allocator_,
        query_cond_->extra_column_count_ > 0,
        segment_querier_->iter_ctx_,
        query_cond_->is_last_search_))) {
      LOG_WARN("knn search delta failed.", K(ret));
    }
    LOG_TRACE("search_result", K(ret),
        "vids", ObArrayWrap<int64_t>(result.vids_, result.total_),
        KPC(segment_querier_), K(query_cond_->query_limit_),
        K(query_cond_->ef_search_), K(query_cond_->is_last_search_),
        K(query_cond_->extra_column_count_), K(query_cond_->distance_threshold_));

    if (OB_SUCC(ret) && query_cond_->distance_threshold_ != FLT_MAX && result.total_ > 0) {
      // TODO: free memory of response.dist_, response.ids_ and so on.
      int64_t *tmp_vids = nullptr;
      float *tmp_distances = nullptr;
      if (OB_ISNULL(tmp_vids = static_cast<int64_t*>(ctx_->tmp_allocator_->alloc(result.total_ * sizeof(int64_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc tmp vids.", K(ret));
      } else if (OB_ISNULL(tmp_distances = static_cast<float*>(ctx_->tmp_allocator_->alloc(result.total_ * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc tmp distances.", K(ret));
      } else {
        int64_t tmp_cnt = 0;
        for (int64_t i = 0; i < result.total_; i++) {
          if (result.distances_[i] <= query_cond_->distance_threshold_) {
            tmp_vids[tmp_cnt] = result.vids_[i];
            tmp_distances[tmp_cnt] = result.distances_[i];
            tmp_cnt++;
          }
        }
        result.total_ = tmp_cnt;
        result.vids_ = tmp_vids;
        result.distances_ = tmp_distances;
        // TODO extra info
      }
    }
    if (OB_SUCC(ret) && result.total_ > 0 && query_cond_->extra_column_count_ > 0) {
      if (OB_FAIL(result.extra_info_ptr_.init(ctx_->tmp_allocator_, result.extra_info_buf_ptr_, extra_info_actual_size_, result.total_))) {
        LOG_WARN("failed to init delta_extra_info_ptr.", K(ret));
      }
    }

    if (OB_SUCC(ret) && result.total_ <= 0) {
      LOG_INFO("not found match result, but index is not empty", K(vec_cnt),
          K(query_cond_->query_limit_), K(query_cond_->ef_search_),
          K(query_cond_->distance_threshold_), KPC(segment_querier_));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
