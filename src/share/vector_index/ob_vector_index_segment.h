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
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SEGMENT_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SEGMENT_H_

#include "roaring/roaring64.h"
#include "lib/vector/ob_vector_util.h"
#include "lib/stat/ob_latch_define.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"

namespace oceanbase
{

namespace storage {
class ObTableScanIterator;
class ObTableScanParam;
}

namespace share
{

class ObPluginVectorIndexAdaptor;
class ObVectorQueryAdaptorResultContext;
class ObVectorQueryConditions;
class ObVsagQueryResult;
class ObHnswBitmapFilter;
class ObVectorIndexRoaringBitMap;
class ObVectorIndexSegmentQuerier;


class ObVectorIndexSegment;

class ObVectorIndexSegmentHandle
{
public:
  ObVectorIndexSegmentHandle():
    segment_(nullptr)
  {}
  ~ObVectorIndexSegmentHandle() { reset(); }

  bool is_valid() const { return nullptr != segment_; }
  void reset();

  ObVectorIndexSegmentHandle(const ObVectorIndexSegmentHandle &other);
  ObVectorIndexSegmentHandle &operator= (const ObVectorIndexSegmentHandle &other);
  ObVectorIndexSegment* operator->() const { return ATOMIC_LOAD(&segment_); }
  ObVectorIndexSegment* operator->() { return ATOMIC_LOAD(&segment_); }
  ObVectorIndexSegment* get() { return ATOMIC_LOAD(&segment_);}
  const ObVectorIndexSegment* get() const { return ATOMIC_LOAD(&segment_); }
  int set_segment(ObVectorIndexSegment *segment);

public:
  TO_STRING_KV(KPC_(segment));

private:
  ObVectorIndexSegment *segment_;

};

struct ObVectorIndexMetaHeader
{
  static const int8_t CURRENT_VERSION = 1;
  static const int64_t HEAD_LEN = sizeof(int64_t) + sizeof(int64_t);

  ObVectorIndexMetaHeader():
    version_(CURRENT_VERSION), reserved_(0), scn_(0)
  {}

  bool is_valid() const { return version_ > 0 && reserved_ == 0 && scn_ > 0; }
  void reset()
  {
    version_ = CURRENT_VERSION;
    reserved_ = 0;
    scn_ = 0;
  }

  TO_STRING_KV(K_(version), K_(reserved), K_(scn));

  int64_t version_ : 8;
  int64_t reserved_ : 56;
  int64_t scn_;
};

enum class ObVectorIndexSegmentType : uint16_t
{
  INVALID = 0,
  IN_MEMORY,
  FREEZE_PERSIST,
  INCR_MERGE,
  BASE,
  LEGACY_BASE,
  EMPTY,
  MAX
};

enum class ObVectorIndexSegmentState : uint16_t
{
  DEFAULT = 0,
  ACTIVE,
  FROZEN,
  FLUSHING,
  PERSISTED,
  MAX
};

class ObVectorIndexSegmentMeta
{
  OB_UNIS_VERSION(1);
public:
  static int get_vector_index_meta_key(ObIAllocator &allocator, const ObTabletID &tablet_id, ObString &key);
  static int get_segment_persist_key(
      const ObVectorIndexAlgorithmType index_type, const ObTabletID &tablet_id,
      const int64_t snapshot_version, const int64_t row_id,
      ObIAllocator &allocator, ObString &key);
  static int get_segment_persist_key(
      const ObVectorIndexAlgorithmType index_type, const ObTabletID &tablet_id,
      const int64_t snapshot_version, const int64_t row_id,
      char *key_str, const int64_t key_len,  ObString &key);
  static int prepare_new_segment_meta(
      ObIAllocator &allocator,
      ObVectorIndexSegmentMeta &seg_meta,
      const ObVectorIndexSegmentType seg_type,
      const ObVectorIndexAlgorithmType index_type,
      const ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const ObIArray<ObVecIdxSnapshotBlockData> &data_blocks,
      const ObVectorIndexRoaringBitMap *bitmap);

public:
  ObVectorIndexSegmentMeta():
    seg_type_(ObVectorIndexSegmentType::INVALID),
    index_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
    flags_(0),
    scn_(0),
    start_key_(),
    end_key_(),
    blocks_cnt_(0),
    segment_handle_()
  {}

  ~ObVectorIndexSegmentMeta() {}

  void release();
  bool has_quantization() const { return VIAT_HNSW_SQ == index_type_ || VIAT_HNSW_BQ == index_type_; }
  int deep_copy_seg_key(const ObString &src_start_key, const ObString &src_end_key);

  TO_STRING_KV(K_(seg_type), K_(index_type), K_(has_segment_meta_row), K_(reserved),
      K_(scn), K_(blocks_cnt), K_(segment_handle));

public:
  ObVectorIndexSegmentType seg_type_;
  ObVectorIndexAlgorithmType index_type_;
  union {
    uint32_t flags_;
    struct {
      uint32_t has_segment_meta_row_         : 1;
      uint32_t reserved_                    : 31;
    };
  };
  int64_t scn_;
  ObString start_key_;
  ObString end_key_;
  int64_t blocks_cnt_;
  // ObVidBound vid_bound_; TODO

  ObVectorIndexSegmentHandle segment_handle_;
  char start_key_buf_[OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH] = {0};
  char end_key_buf_[OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH] = {0};
};

class ObVectorIndexMeta
{
public:
  static int get_meta_scn(const ObString& meta_data, int64_t &scn);
  static int prepare_new_index_meta(
      ObIAllocator &allocator,
      const ObVectorIndexSegmentType seg_type,
      const ObVectorIndexAlgorithmType index_type,
      const ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const ObVectorIndexMeta &old_meta,
      const ObIArray<ObVecIdxSnapshotBlockData> &data_blocks,
      const ObVectorIndexRoaringBitMap *bitmap,
      ObString &meta_data);
public:
  ObVectorIndexMeta():
    header_(),
    flags_(0),
    bases_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("VIBases", MTL_ID())),
    incrs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("VIIncrs", MTL_ID()))
  {}
  ~ObVectorIndexMeta() {}

  bool is_valid() const { return header_.is_valid(); }
  bool is_persistent() const { return is_persistent_; }
  int64_t scn() const { return header_.scn_; }

  int assign(const ObVectorIndexMeta& other);
  void release(ObIAllocator *allocator, const uint64_t tenant_id);

  int serialize(ObIAllocator &allocator, ObString &serde_data);

  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);

  int64_t segment_count() const { return bases_.count() + incrs_.count(); }
  int64_t base_segment_count() const { return bases_.count(); }
  int64_t incr_segment_count() const { return incrs_.count(); }
  ObVectorIndexSegmentMeta& base_seg_meta(int64_t index) { return bases_.at(index); }
  const ObVectorIndexSegmentMeta& base_seg_meta(int64_t index) const { return bases_.at(index); }
  ObVectorIndexSegmentMeta& incr_seg_meta(int64_t index) { return incrs_.at(index); }
  const ObVectorIndexSegmentMeta& incr_seg_meta(int64_t index) const { return incrs_.at(index); }

  TO_STRING_KV(K_(header), K_(is_persistent), K_(reserved), K_(bases), K_(incrs));

private:
  int deserialize_seg_metas(const char *buf, const int64_t data_len, int64_t &pos);
  int add_base_seg_meta(const ObVectorIndexSegmentMeta &seg_meta);
  int add_incr_seg_meta(const ObVectorIndexSegmentMeta &seg_meta);
  int copy_base_seg_metas(const ObVectorIndexMeta &other) { return bases_.assign(other.bases_); }

public:
  ObVectorIndexMetaHeader header_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_persistent_               : 1;
      uint64_t reserved_                    : 63;
    };
  };
  ObSEArray<ObVectorIndexSegmentMeta, 1> bases_;
  ObSEArray<ObVectorIndexSegmentMeta, 1> incrs_;
};

struct ObVectorIndexSegmentHeader
{
  static const int8_t CURRENT_VERSION = 1;
  static const int64_t HEAD_LEN = sizeof(int64_t);

  ObVectorIndexSegmentHeader():
    version_(CURRENT_VERSION), reserved_(0)
  {}

  bool is_valid() const { return version_ > 0 && reserved_ == 0; }
  void reset()
  {
    version_ = CURRENT_VERSION;
    reserved_ = 0;
  }

  TO_STRING_KV(K_(version), K_(reserved));

  int64_t version_ : 8;
  int64_t reserved_ : 56;
};

struct ObVidBound {
  int64_t min_vid_;
  int64_t max_vid_;
  ObVidBound() : min_vid_(INT64_MAX), max_vid_(0) {}
  ObVidBound(int64_t min_vid, int64_t max_vid) : min_vid_(min_vid), max_vid_(max_vid) {}

  void reset()
  {
    min_vid_ = INT64_MAX;
    max_vid_ = 0;
  }

  bool is_valid() const { return min_vid_ < max_vid_; }

  void set_vid(int64_t vid)
  {
    min_vid_ = OB_MIN(min_vid_, vid);
    max_vid_ = OB_MAX(max_vid_, vid);
  }

  void set_vid_bound(const int64_t max_vid, const int64_t min_vid)
  {
    max_vid_ = OB_MAX(max_vid_, max_vid);
    min_vid_ = OB_MIN(min_vid_, min_vid);
  }

  void set_vid_bound(ObVidBound other)
  {
    max_vid_ = OB_MAX(max_vid_, other.max_vid_);
    min_vid_ = OB_MIN(min_vid_, other.min_vid_);
  }

  void get_bound_vid(int64_t &max_vid, int64_t &min_vid) const
  {
    int64_t mem_max_vid = ATOMIC_LOAD(&max_vid_);
    max_vid = OB_MAX(max_vid, mem_max_vid);
    int64_t mem_min_vid = ATOMIC_LOAD(&min_vid_);
    min_vid = OB_MIN(min_vid, mem_min_vid);
  }

  TO_STRING_KV(K_(min_vid), K_(max_vid));
};

struct ObVectorIndexRoaringBitMap
{
  OB_UNIS_VERSION(1);
public:
  ObVectorIndexRoaringBitMap(ObIAllocator *allocator, const uint64_t tenant_id) :
    allocator_(allocator), tenant_id_(tenant_id),
    insert_bitmap_(nullptr), delete_bitmap_(nullptr) {}

  int init(const bool is_init_ibitmap, const bool is_init_dbitmap);

  static void destroy(ObVectorIndexRoaringBitMap *segment);

  TO_STRING_KV(KP(this), KP_(allocator), K_(tenant_id), KP_(insert_bitmap), KP_(delete_bitmap));

  ObIAllocator *allocator_;
  uint64_t tenant_id_;
  TCRWLock rwlock_{ObLatchIds::VECTOR_BITMAP_LOCK};
  roaring::api::roaring64_bitmap_t *insert_bitmap_;
  roaring::api::roaring64_bitmap_t *delete_bitmap_;
};

class ObVectorIndexSegment
{
friend class ObVectorIndexSegQueryHandler;
public:
  static int create(
      ObVectorIndexSegmentHandle &segment_handle,
      const uint64_t tenant_id,
      ObIAllocator &allcoator,
      const ObVectorIndexParam &param,
      const ObVectorIndexAlgorithmType build_type,
      const int max_degree,
      ObPluginVectorIndexAdaptor *adaptor);
  static void destroy(ObVectorIndexSegment *segment);

public:
  ObVectorIndexSegment(ObIAllocator *allocator):
    is_inited_(false), is_base_(false), ref_cnt_(0), write_ref_cnt_(0),
    allocator_(allocator), mem_ctx_(nullptr), index_(nullptr), vid_bound_(),
    ibitmap_(nullptr), vbitmap_(nullptr)
  {}

  bool is_inited() const { return is_inited_; }
  int init(
      const uint64_t tenant_id,
      const ObVectorIndexParam &param,
      const ObVectorIndexAlgorithmType build_type,
      const int max_degree,
      ObPluginVectorIndexAdaptor *adaptor);
  void reset();

  void inc_ref();
  int64_t dec_ref();
  int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }

  // for dml write
  void inc_write_ref();
  int64_t dec_write_ref();
  int64_t get_write_ref() const { return ATOMIC_LOAD(&write_ref_cnt_); }

  ObVsagMemContext* mem_ctx() { return mem_ctx_; }
  int64_t get_mem_used() const { return (OB_NOT_NULL(mem_ctx_) && mem_ctx_->is_inited()) ? mem_ctx_->used() : 0; }
  int64_t get_mem_hold() const { return (OB_NOT_NULL(mem_ctx_) && mem_ctx_->is_inited()) ? mem_ctx_->hold() : 0; }

  int build_index(float* vecs, int64_t* ids, int dim, int size, char *extra_info = nullptr)
  {
    return obvectorutil::build_index(index_, vecs, ids, dim, size, extra_info);
  }

  int build_index(uint32_t *lens, uint32_t *dims, float *vals, int64_t* ids, int size, char *extra_info = nullptr)
  {
    return obvectorutil::build_index(index_, lens, dims, vals, ids, size, extra_info);
  }

  int add_index(float* vecs, int64_t* ids, int dim, char *extra_info, int size)
  {
    return obvectorutil::add_index(index_, vecs, ids, dim, extra_info, size);
  }

  int add_index(uint32_t *lens, uint32_t *dims, float *vals, int64_t *ids, int size, char *extra_infos)
  {
    return obvectorutil::add_index(index_, lens, dims, vals, ids, size, extra_infos);
  }

  int fserialize(std::ostream& out_stream) { return obvectorutil::fserialize(index_, out_stream); }
  int fdeserialize(std::istream& in_stream) { return obvectorutil::fdeserialize(index_, in_stream); }
  int immutable_optimize() { return obvectorutil::immutable_optimize(index_); }

  // TODO mark const
  ObVectorIndexAlgorithmType get_index_type() const { return static_cast<ObVectorIndexAlgorithmType>(obvectorutil::get_index_type(const_cast<void*>(index_))); }
  int get_index_number(int64_t &size) const { return obvectorutil::get_index_number(const_cast<void*>(index_), size); }
  int get_vid_bound(int64_t &min_vid, int64_t &max_vid) const { return obvectorutil::get_vid_bound(const_cast<void*>(index_), min_vid, max_vid); }
  void get_read_bound_vid(int64_t &max_vid, int64_t &min_vid) const { vid_bound_.get_bound_vid(max_vid, min_vid); }
  void set_read_vid_bound(ObVidBound other) { vid_bound_.set_vid_bound(other); }
  int get_extra_info_by_ids(const int64_t* ids, int64_t count, char *extra_infos) { return obvectorutil::get_extra_info_by_ids(index_, ids, count, extra_infos); }

  int cal_distance_by_id(const float *vector, const int64_t *ids, int64_t count, const float *&distances)
  {
    return obvectorutil::cal_distance_by_id(index_, vector, ids, count, distances);
  }
  int cal_distance_by_id(uint32_t len, uint32_t *dims, float *vals, const int64_t *ids, int64_t count, const float *&distances)
  {
    return obvectorutil::cal_distance_by_id(index_, len, dims, vals, ids, count, distances);
  }

  bool is_base() const { return is_base_; }
  void set_is_base() { is_base_ = true; }

  int64_t get_serialize_meta_size() const;
  int serialize_meta(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_meta(const char *buf, const int64_t data_len, int64_t &pos);

  int serialize(const uint64_t tenant_id, ObHNSWSerializeCallback::CbParam &param);
  int serialize(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      ObVecIdxSnapshotDataWriteCtx &ctx,
      ObVectorIndexRoaringBitMap *vbitmap,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const int64_t lob_inrow_threshold,
      const int64_t timeout,
      ObVectorIndexAlgorithmType &index_type);
  static int deserialize(
      ObVectorIndexSegmentHandle &segment_handle, const uint64_t tenant_id, ObPluginVectorIndexAdaptor *adaptor, ObHNSWDeserializeCallback::CbParam &param);

public:
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_base), K_(ref_cnt), K_(write_ref_cnt),
    KP_(allocator), KP_(mem_ctx), KP_(index), K_(vid_bound), KPC_(ibitmap), KPC_(vbitmap));

private:
  bool is_inited_;
  bool is_base_;
  int64_t ref_cnt_;
  int64_t write_ref_cnt_;
  uint64_t tenant_id_;
  ObIAllocator *allocator_;
  ObVsagMemContext* mem_ctx_;
  // VSAG库的向量索引是线程安全的, 所以不需要锁保护
  void* index_;
  ObVidBound vid_bound_;
public:
  ObVectorIndexRoaringBitMap *ibitmap_;
  ObVectorIndexRoaringBitMap *vbitmap_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexSegment);
};

typedef common::ObArray<int64_t>  ObVecIdxVidArray;
typedef common::ObArray<float>  ObVecIdxVecArray;
typedef common::ObArray<uint32_t> ObVecIdxLensArray; // for sparse vector (ipivf_sq)
typedef common::ObArray<uint32_t> ObVecIdxDimsArray; // for sparse vector (ipivf_sq)
typedef common::ObArray<float> ObVecIdxValsArray; // for sparse vector (ipivf_sq)

struct ObVectorIndexDataBase
{
public:
  ObVectorIndexDataBase()
    : type_(VIRT_MAX),
      is_init_(false),
      ref_cnt_(0),
      scn_() {}

  bool is_inited() const { return ATOMIC_LOAD(&is_init_); }
  void set_inited() { is_init_ = true; }

  void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
    // OB_LOG(INFO, "inc ref count", K(ref_cnt_), KP(this), KPC(this), K(lbt())); // remove later
  }
  int64_t dec_ref()
  {
    int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
    // OB_LOG(INFO, "des ref count", K(ref_cnt_), KP(this), KPC(this), K(lbt())); // remove later
    return cnt;
  }

public:
  ObVectorIndexRecordType type_;
  bool is_init_;
  uint64_t ref_cnt_;
  SCN scn_;
};

template<typename T>
class ObVectorIndexMemDataHandle
{
public:
  ObVectorIndexMemDataHandle():
    memdata_(nullptr),
    allocator_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID)
  {}

  ObVectorIndexMemDataHandle(const ObVectorIndexMemDataHandle &other)
    : ObVectorIndexMemDataHandle()
  {
    *this = other;
  }

  ObVectorIndexMemDataHandle &operator= (const ObVectorIndexMemDataHandle &other)
  {
    if (this != &other) {
      reset();
      if (nullptr != other.memdata_) {
        memdata_ = other.memdata_;
        memdata_->inc_ref();
        allocator_ = other.allocator_;
        tenant_id_ = other.tenant_id_;
      }
    }
    return *this;
  }

  ~ObVectorIndexMemDataHandle() { reset(); }

  bool is_valid() const { return nullptr != memdata_; }

  void reset()
  {
    if (nullptr != memdata_) {
      const int64_t ref_cnt = memdata_->dec_ref();
      if (0 == ref_cnt) {
        destroy();
      }
      memdata_ = nullptr;
    }
    allocator_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
  }

  T* operator->() const { return memdata_; }
  T* operator->() { return memdata_; }
  const T* get() const { return ATOMIC_LOAD(&memdata_); }

  int set_memdata(T *memdata, ObIAllocator *allocator, const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    reset();
    if (OB_ISNULL(memdata)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid argument", K(ret), KP(memdata));
    } else {
      memdata_ = memdata;
      memdata_->inc_ref();
      allocator_ = allocator;
      tenant_id_ = tenant_id;
    }
    return ret;
  }

private:
  void destroy()
  {
    if (nullptr != memdata_) {
      memdata_->free_memdata_resource(allocator_, tenant_id_);
      allocator_->free(memdata_);
    }
  }

public:
  TO_STRING_KV(KPC_(memdata), KP_(allocator), K_(tenant_id));

private:
  T *memdata_;
  ObIAllocator *allocator_;
  uint64_t tenant_id_;
};

struct ObVecIdxActiveData : public ObVectorIndexDataBase
{
public:
  ObVecIdxActiveData()
    : ObVectorIndexDataBase(),
      segment_handle_(),
      last_dml_scn_(),
      last_read_scn_(),
      can_skip_(NOT_INITED) {}

  void free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id);

  TO_STRING_KV(KP(this), K_(type), K_(is_init), K_(scn), K_(ref_cnt), K_(segment_handle),
      K_(last_dml_scn), K_(last_read_scn), K_(can_skip));

public:
  TCRWLock mem_data_rwlock_{ObLatchIds::VECTOR_MEM_DATA};
  ObVectorIndexSegmentHandle segment_handle_;
  SCN last_dml_scn_;
  SCN last_read_scn_;
  ObCanSkip3rdAnd4thVecIndex can_skip_;

};
typedef ObVectorIndexMemDataHandle<ObVecIdxActiveData> ObVecIdxActiveDataHandle;

class ObVectorIndexSegmentWriteGuard
{
public:
  public:
  ObVectorIndexSegmentWriteGuard() {}
  ~ObVectorIndexSegmentWriteGuard()
  {
    if (segment_handle_.is_valid()) {
      segment_handle_->dec_write_ref();
    }
  }

  int prepare_write(ObVecIdxActiveDataHandle& incr_data);
  ObVectorIndexSegmentHandle& handle() { return segment_handle_;}

private:
  ObVectorIndexSegmentHandle segment_handle_;

  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexSegmentWriteGuard);
};

struct ObVecIdxVBitmapData : public ObVectorIndexDataBase
{
public:
  ObVecIdxVBitmapData()
    : ObVectorIndexDataBase(),
      bitmap_() {}

  void free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id);

  TO_STRING_KV(KP(this), K_(type), K_(is_init), K_(scn), K_(ref_cnt), KP_(bitmap));

public:
  TCRWLock bitmap_rwlock_{ObLatchIds::VECTOR_BITMAP_LOCK};
  ObVectorIndexRoaringBitMap *bitmap_;
};
typedef ObVectorIndexMemDataHandle<ObVecIdxVBitmapData> ObVecIdxVBitmapDataHandle;


class ObVectorIndexSegmentBuilder
{
public:
  // for hnsw_sq/hnsw_bq
  constexpr static uint32_t VEC_INDEX_HNSW_BUILD_COUNT_THRESHOLD = 10000;

public:
  ObVectorIndexSegmentBuilder():
    seg_type_(ObVectorIndexSegmentType::INVALID),
    has_build_(false),
    need_vid_check_(false),
    segment_handle_(),
    vid_array_(nullptr),
    vec_array_(nullptr),
    extra_info_buf_(nullptr),
    lens_array_(nullptr),
    dims_array_(nullptr),
    vals_array_(nullptr),
    vid_bound_(),
    ibitmap_(nullptr),
    vbitmap_(nullptr),
    total_cnt_(0),
    add_cnt_(0),
    skip_cnt_(0)
  {}
  void free(ObIAllocator &allocator);
  void free_vec_buf_data(ObIAllocator &allocator);
  int init_vec_buffer(const uint64_t tenant_id, ObIAllocator &allocator, const bool is_sparse_vector);
  int add_to_buffer(float* vecs, int64_t* ids, int dim, char *extra_info, int size, const int64_t &extra_info_actual_size);
  int add_to_buffer(uint32_t *lens, uint32_t *dims, float *vals, int64_t* ids, char *extra_info, int size, const int64_t &extra_info_actual_size);
  bool is_empty_buffer() const  { return OB_ISNULL(vid_array_) || vid_array_->count() == 0;}
  int create_and_build(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      const ObVectorIndexParam &param,
      ObPluginVectorIndexAdaptor* adaptor);
  int create_and_add(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      const ObVectorIndexParam &param,
      const ObVectorIndexAlgorithmType build_type,
      const int max_degree,
      ObPluginVectorIndexAdaptor* adaptor);
  int build_if_need(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      const ObVectorIndexParam &param,
      ObPluginVectorIndexAdaptor* adaptor);

  int create_and_add_sparse_index_from_arrays(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      const ObVectorIndexParam &param,
      const ObVectorIndexAlgorithmType build_type,
      ObPluginVectorIndexAdaptor* adaptor);
  int create_and_build_sparse_index_from_arrays(
      const uint64_t tenant_id,
      ObIAllocator &allocator,
      const ObVectorIndexParam &param,
      ObPluginVectorIndexAdaptor* adaptor);

  bool need_build() const
  {
    return ! has_build_ && OB_NOT_NULL(vid_array_) && vid_array_->count() > VEC_INDEX_HNSW_BUILD_COUNT_THRESHOLD ;
  }

  int check_vid_range(const int64_t vid, bool &has_skip_vid);
  int serialize(const uint64_t tenant_id, ObHNSWSerializeCallback::CbParam &param);
  int add_index(float* vecs, int64_t* ids, int dim, char *extra_info, int size);
  int add_index(uint32_t *lens, uint32_t *dims, float *vals, int64_t *ids, int size, char *extra_infos);

  TO_STRING_KV(KP(this), K_(seg_type), K_(has_build), K_(need_vid_check),
      K_(segment_handle), KP_(vid_array), KP_(vec_array), KP_(extra_info_buf),
      KP_(lens_array), KP_(dims_array), KP_(vals_array),
      K_(vid_bound), KPC_(ibitmap), KPC_(vbitmap), K_(total_cnt), K_(add_cnt), K_(skip_cnt));

  ObVectorIndexSegmentType seg_type_;
  // if has_build is false, new vector will be inserted into vec_array
  bool has_build_;
  bool need_vid_check_;
  TCRWLock mem_data_rwlock_{ObLatchIds::VECTOR_MEM_DATA};
  ObVectorIndexSegmentHandle segment_handle_;
  ObVecIdxVidArray *vid_array_;
  ObVecIdxVecArray *vec_array_;
  ObVecExtraInfoBuffer *extra_info_buf_;
  ObVecIdxLensArray *lens_array_; // for sparse vector (ipivf_sq)
  ObVecIdxDimsArray *dims_array_; // for sparse vector (ipivf_sq)
  ObVecIdxValsArray *vals_array_; // for sparse vector (ipivf_sq)

  ObVidBound vid_bound_;
  ObVectorIndexRoaringBitMap *ibitmap_;
  ObVectorIndexRoaringBitMap *vbitmap_;
  int64_t total_cnt_;
  int64_t add_cnt_;
  int64_t skip_cnt_;
};

class ObVectorIndexSegmentDeserializer
{
public:
  ObVectorIndexSegmentDeserializer():
    segment_handle_()
  {}

  TO_STRING_KV(KP(this), K_(segment_handle));

  ObVectorIndexSegmentHandle segment_handle_;
};

struct ObVecIndexBuildSegInfo
{
  int64_t vector_cnt_;
  int64_t mem_used_;
  int64_t min_vid_;
  int64_t max_vid_;
  ObVecIndexBuildSegInfo() : vector_cnt_(0), mem_used_(0), min_vid_(0), max_vid_(0) {}
  void reset() { vector_cnt_ = 0; mem_used_ = 0; min_vid_ = 0; max_vid_ = 0; }
  TO_STRING_KV(K_(vector_cnt), K_(mem_used), K_(min_vid), K_(max_vid));
};

struct ObVecIdxSnapshotData : public ObVectorIndexDataBase
{
public:
  ObVecIdxSnapshotData()
    : ObVectorIndexDataBase(),
      rb_flag_(true),
      is_ready_for_read_(false),
      builder_(nullptr),
      deserializer_(nullptr),
      meta_(),
      vid_bound_() {}

  int immutable_optimize_snap();

  int build_meta(
      const ObTabletID &tablet_id, const int64_t snapshot_version, const int64_t blocks_cnt);
  int build_finished(ObIAllocator &allocator);

  int get_snap_index_row_cnt(int64_t &count) const;
  ObVectorIndexAlgorithmType get_snap_index_type() const;
  int64_t get_snap_mem_hold() const;
  int64_t get_snap_mem_used() const;
  bool check_incr_mem_over_percentage(const int64_t incr_memory_percentage) const;
  bool check_incr_can_merge_base() const;
  void get_read_bound_vid(int64_t &max_vid, int64_t &min_vid) { vid_bound_.get_bound_vid(max_vid, min_vid); }

  void free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id);
  int free_segment_memory();

  int load_persist_segments(ObPluginVectorIndexAdaptor *adaptor,
      ObIAllocator &allocator, const ObLSID& ls_id, const share::SCN &target_scn);
  int load_persist_segments(
      ObPluginVectorIndexAdaptor *adaptor, ObIAllocator &allocator,
      storage::ObTableScanParam &scan_param, storage::ObTableScanIterator *table_scan_iter);
  int load_segment(
      const uint64_t tenant_id, ObPluginVectorIndexAdaptor *adaptor, ObHNSWDeserializeCallback::CbParam &param);

private:
  int get_snap_vid_bound(int64_t &min_vid, int64_t &max_vid);
  int load_segment(ObPluginVectorIndexAdaptor *adaptor,
      ObIAllocator &allocator, const uint64_t tenant_id, const ObLSID& ls_id, const ObTabletID &tablet_id,
      const share::SCN &target_scn, ObVectorIndexSegmentMeta &seg_meta);
  int load_segment(ObIAllocator &allocator, ObPluginVectorIndexAdaptor *adaptor,
      storage::ObTableScanIterator *snap_data_iter, const uint64_t tenant_id, ObVectorIndexSegmentMeta &seg_meta);

public:
  TO_STRING_KV(KP(this), K_(type), K_(is_init), K_(rb_flag), K_(is_ready_for_read), K_(scn),
    K_(ref_cnt), KPC_(builder), KPC_(deserializer), K_(meta), K_(vid_bound), K_(last_res_seg_info));

  TCRWLock mem_data_rwlock_{ObLatchIds::VECTOR_MEM_DATA};
  bool rb_flag_;
  bool is_ready_for_read_;
  ObVectorIndexSegmentBuilder *builder_;
  ObVectorIndexSegmentDeserializer *deserializer_;
  ObVectorIndexMeta meta_;
  ObVidBound vid_bound_;
  ObVecIndexBuildSegInfo last_res_seg_info_;
};
typedef ObVectorIndexMemDataHandle<ObVecIdxSnapshotData> ObVecIdxSnapshotDataHandle;

struct ObVecIdxFrozenData : public ObVectorIndexDataBase
{
  enum State {
    NO_FROZEN = 0,
    FROZEN,
    WAIT_WRITE_FINISHED,
    REFRESH_VBITMAP,
    PERSIST,
    REFRESH_SNAP,
    FINISH,
  };
  constexpr static int64_t  RETRY_WARN_COUNT = 100;

  ObVecIdxFrozenData() :
    state_(State::NO_FROZEN), ret_code_(0), retry_cnt_(0), frozen_scn_(),
    segment_handle_(), vbitmap_() {}

  void reset();
  void reuse();
  void free_memdata_resource(ObIAllocator *allocator, const uint64_t tenant_id);

  State get_state() const { return state_; }
  bool has_frozen() const { return state_ > State::NO_FROZEN; }
  bool is_frozen_finish() const { return state_ == State::FINISH; }
  void set_has_frozen() { state_ = State::FROZEN; }
  void set_frozen_finish() { state_ = State::FINISH; ret_code_ = 0; retry_cnt_ = 0;}
  bool retry_too_many() const {  return retry_cnt_ >= RETRY_WARN_COUNT; }

  State state_;
  int ret_code_;
  int retry_cnt_;
  SCN frozen_scn_;

  TCRWLock mem_data_rwlock_{ObLatchIds::VECTOR_MEM_DATA};
  ObVectorIndexSegmentHandle segment_handle_;
  ObVecIdxVBitmapDataHandle vbitmap_;

  TO_STRING_KV(KP(this), K_(state), K_(ret_code), K_(retry_cnt), K_(frozen_scn), K_(segment_handle), K_(vbitmap));
};
typedef ObVectorIndexMemDataHandle<ObVecIdxFrozenData> ObVecIdxFrozenDataHandle;

struct ObVectorIndexSegQueryHandler
{
  ObVectorIndexSegQueryHandler()
    : tenant_id_(OB_INVALID_TENANT_ID),
      segment_querier_(nullptr),
      ctx_(nullptr),
      query_cond_(nullptr),
      valid_ratio_(0.0f),
      extra_info_actual_size_(0),
      is_sparse_vector_(false),
      query_vector_(nullptr),
      dim_(0),
      sparse_lens_(nullptr),
      sparse_dims_(nullptr),
      sparse_vals_(nullptr) {}

  int knn_search(ObVsagQueryResult &result);

  TO_STRING_KV(KP(this), K_(tenant_id), KPC_(segment_querier), KP_(ctx), KP_(query_cond),
      K_(valid_ratio), K_(extra_info_actual_size), K_(is_sparse_vector),
      KP_(query_vector), K_(dim), KP_(sparse_lens), KP_(sparse_dims), KP_(sparse_vals));


  uint64_t tenant_id_;
  ObVectorIndexSegmentQuerier *segment_querier_;
  ObVectorQueryAdaptorResultContext *ctx_;
  ObVectorQueryConditions *query_cond_;
  float valid_ratio_;
  int64_t extra_info_actual_size_;
  bool is_sparse_vector_;

  // for dense vector
  float *query_vector_;
  int64_t dim_;

  // for sparse vector
  uint32_t *sparse_lens_;
  uint32_t *sparse_dims_;
  float *sparse_vals_;
};

}  // namespace share
}  // namespace oceanbase

 #endif