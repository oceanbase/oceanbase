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


#ifndef OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_ADAPTOR_H_
#define OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_ADAPTOR_H_

#include "share/scn.h"
#include "share/datum/ob_datum.h"
#include "roaring/roaring64.h"
#include "common/object/ob_obj_type.h"
#include "common/row/ob_row_iterator.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "storage/ob_i_store.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/oblog/ob_log_module.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"

namespace oceanbase
{
namespace share
{
struct ObPluginVectorIndexTaskCtx;
class ObVsagMemContext;

struct ObVectorIndexInfo
{
public:
  ObVectorIndexInfo();
  ~ObVectorIndexInfo() { reset(); }
  void reset();
  static const int64_t OB_VECTOR_INDEX_STATISTICS_SIZE = 2048;
  static const int64_t OB_VECTOR_INDEX_SYNC_INFO_SIZE = 1024;
  TO_STRING_KV(K_(ls_id),
               K_(rowkey_vid_table_id), K_(vid_rowkey_table_id), K_(inc_index_table_id),
               K_(vbitmap_table_id), K_(snapshot_index_table_id), K_(data_table_id),
               K_(rowkey_vid_tablet_id), K_(vid_rowkey_tablet_id), K_(inc_index_tablet_id),
               K_(vbitmap_tablet_id), K_(snapshot_index_tablet_id), K_(data_tablet_id),
               K_(statistics), K_(sync_info));
public:
  int64_t ls_id_;
  // table_id
  int64_t rowkey_vid_table_id_;
  int64_t vid_rowkey_table_id_;
  int64_t inc_index_table_id_;
  int64_t vbitmap_table_id_;
  int64_t snapshot_index_table_id_;
  int64_t data_table_id_;
  // tablet_id
  int64_t rowkey_vid_tablet_id_;
  int64_t vid_rowkey_tablet_id_;
  int64_t inc_index_tablet_id_;
  int64_t vbitmap_tablet_id_;
  int64_t snapshot_index_tablet_id_;
  int64_t data_tablet_id_;
  char statistics_[OB_VECTOR_INDEX_STATISTICS_SIZE];
  char sync_info_[OB_VECTOR_INDEX_SYNC_INFO_SIZE];
};

enum ObVectorIndexDistAlgorithm
{
  VIDA_L2 = 0,
  VIDA_IP = 1,
  VIDA_COS = 2,
  VIDA_MAX
};

enum ObVectorIndexAlgorithmLib
{
  VIAL_VSAG = 0,
  VIAL_MAX
};

enum ObVectorIndexAlgorithmType
{
  VIAT_HNSW = 0,
  VIAT_MAX
};

struct ObVectorIndexAlgorithmHeader
{
  ObVectorIndexAlgorithmType type_;
  OB_UNIS_VERSION(1);
};

struct ObVectorIndexHNSWParam
{
  ObVectorIndexHNSWParam() :
    type_(VIAT_MAX), lib_(VIAL_MAX), dim_(0), m_(0), ef_construction_(0), ef_search_(0)
  {}
  void reset() {
    type_ = VIAT_MAX;
    lib_ = VIAL_MAX;
    dist_algorithm_ = VIDA_MAX;
    dim_ = 0;
    m_ = 0;
    ef_construction_ = 0;
    ef_search_ = 0;
  };
  ObVectorIndexAlgorithmType type_;
  ObVectorIndexAlgorithmLib lib_;
  ObVectorIndexDistAlgorithm dist_algorithm_;
  int64_t dim_;
  int64_t m_;
  int64_t ef_construction_;
  int64_t ef_search_;
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(type), K_(lib), K_(dist_algorithm), K_(dim), K_(m), K_(ef_construction), K_(ef_search));
};

enum ObVectorIndexRecordType
{
  VIRT_INC, // increment index
  VIRT_BITMAP,
  VIRT_SNAP, // snapshot index
  VIRT_DATA, // data tablet/table
  VIRT_MAX
};

enum ObAdapterCreateType
{
  CreateTypeInc = 0,
  CreateTypeBitMap,
  CreateTypeSnap,
  CreateTypeFullPartial,
  CreateTypeComplete,
  CreateTypeMax
};

struct ObVectorIndexRoaringBitMap
{
  TO_STRING_KV(KP_(insert_bitmap), KP_(delete_bitmap));
  roaring::api::roaring64_bitmap_t *insert_bitmap_;
  roaring::api::roaring64_bitmap_t *delete_bitmap_;
};

enum PluginVectorQueryResStatus
{
  PVQ_START,
  PVQ_WAIT,
  PVQ_LACK_SCN,
  PVQ_OK, // ok
  PVQ_COM_DATA,
  PVQ_INVALID_SCN,
  PVQ_MAX
};

enum ObVectorQueryProcessFlag
{
  PVQP_FIRST,
  PVQP_SECOND,
  PVQP_MAX,
};

struct ObVectorParamData
{
  int64_t dim_;
  int64_t count_;
  ObObj *vectors_;
  ObObj *vids_;
};

class ObVectorQueryAdaptorResultContext {
public:
  friend class ObPluginVectorIndexAdaptor;
  ObVectorQueryAdaptorResultContext(ObIAllocator *allocator, ObIAllocator *tmp_allocator)
    : status_(PVQ_START),
      flag_(PVQP_MAX),
      bitmaps_(nullptr),
      vec_data_(),
      allocator_(allocator),
      tmp_allocator_(tmp_allocator) {};
  ~ObVectorQueryAdaptorResultContext();
  int init_bitmaps();
  int is_bitmaps_valid();
  ObObj *get_vids() { return vec_data_.vids_; }
  ObObj *get_vectors() { return vec_data_.vectors_; }
  int64_t get_dim() { return vec_data_.dim_; }
  int64_t get_count() { return vec_data_.count_; }
  PluginVectorQueryResStatus get_status() { return status_; }
  ObVectorQueryProcessFlag get_flag() { return flag_; }
  ObIAllocator *get_allocator() { return allocator_; }
  ObIAllocator *get_tmp_allocator() { return tmp_allocator_; }
  int set_vector(int64_t index, const char *ptr, common::ObString::obstr_size_t size);
  void set_vectors(ObObj *vectors) { vec_data_.vectors_ = vectors; }

private:
  PluginVectorQueryResStatus status_;
  ObVectorQueryProcessFlag flag_;
  ObVectorIndexRoaringBitMap *bitmaps_;
  ObVectorParamData vec_data_;
  ObIAllocator *allocator_;
  ObIAllocator *tmp_allocator_;
};

struct ObVectorQueryConditions {
  uint32_t query_limit_;
  bool query_order_; // true: asc, false: desc
  int64_t ef_search_;
  ObString query_vector_;
  SCN query_scn_;
  common::ObNewRowIterator *row_iter_; // index_snapshot_data_table iter
};

struct ObVectorIndexMemData
{
  ObVectorIndexMemData()
    : is_init_(false),
      rb_flag_(true),
      mem_data_rwlock_(),
      bitmap_rwlock_(),
      scn_(),
      ref_cnt_(0),
      index_(nullptr),
      bitmap_(nullptr),
      mem_ctx_(nullptr) {}

public:
  TO_STRING_KV(K(rb_flag_), K_(is_init), K_(scn), K_(ref_cnt), KP_(index), KPC_(bitmap), KP_(mem_ctx));
  void free_resource(ObIAllocator *allocator_);
  bool is_inited() const { return is_init_; }
  void set_inited() { is_init_ = true; }
  void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
    // OB_LOG(INFO, "inc ref count", K(ref_cnt_), KP(this), KPC(this), K(lbt())); // remove later
  }
  bool dec_ref_and_check_release()
  {
    int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
    // OB_LOG(INFO,"dec ref count", K(ref_count), KP(this), KPC(this), K(lbt())); // remove later
    return (ref_count == 0);
  }

public:
  bool is_init_;
  bool rb_flag_;
  TCRWLock mem_data_rwlock_;
  TCRWLock bitmap_rwlock_;
  SCN scn_;
  uint64_t ref_cnt_;
  void *index_;
  ObVectorIndexRoaringBitMap *bitmap_;
  ObVsagMemContext *mem_ctx_;
  // used for memdata exchange between adaptors
};

struct ObVectorIndexFollowerSyncStatic
{
public:
  ObVectorIndexFollowerSyncStatic()
    : incr_count_(0),
      vbitmap_count_(0),
      snap_count_(0),
      sync_count_(0),
      sync_fail_(0),
      idle_count_(0)
  {}
  void reset() {
    incr_count_ = 0;
    vbitmap_count_ = 0;
    snap_count_ = 0;
    sync_count_ = 0;
    sync_fail_ = 0;
    idle_count_ = 0;
  }
  TO_STRING_KV(K_(incr_count), K_(vbitmap_count), K_(snap_count),
               K_(sync_count), K_(sync_fail), K_(idle_count));
  int64_t incr_count_;
  int64_t vbitmap_count_;
  int64_t snap_count_;

  int64_t sync_count_;
  int64_t sync_fail_;
  int64_t idle_count_; // loops not receive sync
};

struct ObVectorIndexSharedTableInfo
{
  ObVectorIndexSharedTableInfo()
    : rowkey_vid_table_id_(OB_INVALID_ID),
      vid_rowkey_table_id_(OB_INVALID_ID),
      data_table_id_(OB_INVALID_ID),
      rowkey_vid_tablet_id_(),
      vid_rowkey_tablet_id_()
  {}
  bool is_valid()
  {
    return rowkey_vid_table_id_ != OB_INVALID_ID
           && vid_rowkey_table_id_ != OB_INVALID_ID
           && data_table_id_ != OB_INVALID_ID
           && rowkey_vid_tablet_id_.is_valid()
           && vid_rowkey_tablet_id_.is_valid();
  }

  TO_STRING_KV(K_(rowkey_vid_table_id),
               K_(vid_rowkey_table_id),
               K_(rowkey_vid_tablet_id),
               K_(vid_rowkey_tablet_id),
               K_(data_table_id));

  uint64_t rowkey_vid_table_id_;
  uint64_t vid_rowkey_table_id_;
  uint64_t data_table_id_;
  ObTabletID rowkey_vid_tablet_id_;
  ObTabletID vid_rowkey_tablet_id_;
};

class ObPluginVectorIndexAdaptor
{
public:
  friend class ObVsagMemContext;
  ObPluginVectorIndexAdaptor(common::ObIAllocator *allocator, lib::MemoryContext &entity);
  ~ObPluginVectorIndexAdaptor();

  int init(ObString init_str, int64_t dim, lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem);
  // only used for background maintance handle aux table no.4 / 5 before get index aux table no.3
  int init(lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem);
  int set_param(ObString init_str, int64_t dim);
  int get_index_type() { return type_; };

  // -- start 调试使用
  void init_incr_tablet() {inc_tablet_id_ = ObTabletID(common::ObTabletID::MIN_VALID_TABLET_ID); }
  // -- end 调试使用

  bool is_snap_tablet_valid() { return snapshot_tablet_id_.is_valid(); }
  bool is_inc_tablet_valid() { return inc_tablet_id_.is_valid(); }
  bool is_vbitmap_tablet_valid() { return vbitmap_tablet_id_.is_valid(); }
  bool is_data_tablet_valid() { return data_tablet_id_.is_valid(); }
  bool is_vid_rowkey_info_valid() { return rowkey_vid_table_id_ != OB_INVALID_ID && rowkey_vid_tablet_id_.is_valid(); }

  ObTabletID& get_inc_tablet_id() { return inc_tablet_id_; }
  ObTabletID& get_vbitmap_tablet_id() { return vbitmap_tablet_id_; }
  ObTabletID& get_snap_tablet_id() { return snapshot_tablet_id_; }
  ObTabletID& get_data_tablet_id() { return data_tablet_id_; }
  ObTabletID& get_rowkey_vid_tablet_id() { return rowkey_vid_tablet_id_; }
  ObTabletID& get_vid_rowkey_tablet_id() { return vid_rowkey_tablet_id_; }

  ObVectorIndexMemData *get_incr_data() { return incr_data_; }
  ObVectorIndexMemData *get_snap_data_() { return snap_data_; }
  ObVectorIndexMemData *get_vbitmap_data() { return vbitmap_data_; }

  uint64_t get_inc_table_id() { return inc_table_id_; }
  uint64_t get_vbitmap_table_id() { return vbitmap_table_id_; }
  uint64_t get_snapshot_table_id() { return snapshot_table_id_; }
  uint64_t get_data_table_id() { return data_table_id_; }
  uint64_t get_rowkey_vid_table_id() { return rowkey_vid_table_id_; }
  uint64_t get_vid_rowkey_table_id() { return vid_rowkey_table_id_; }
  void close_snap_data_rb_flag() {
    if (is_mem_data_init_atomic(VIRT_SNAP)) {
      snap_data_->rb_flag_ = false;
    }
  }

  ObString &get_index_identity() { return index_identity_; };
  int set_index_identity(ObString &index_identity);

  bool is_valid() { return (is_inc_tablet_valid() || is_vbitmap_tablet_valid() || is_snap_tablet_valid()) && is_data_tablet_valid(); }
  bool is_complete();

  void inc_ref();
  bool dec_ref_and_check_release();
  void inc_idle() { idle_cnt_++; }
  void reset_idle() { idle_cnt_ = 0; }
  bool is_deprecated() { return idle_cnt_ > VEC_INDEX_ADAPTER_MAX_IDLE_COUNT; }
  int set_tablet_id(ObVectorIndexRecordType type, ObTabletID tablet_id);

  int set_table_id(ObVectorIndexRecordType type, uint64_t table_id);
  void set_vid_rowkey_info(ObVectorIndexSharedTableInfo &info);

  int merge_parital_index_adapter(ObPluginVectorIndexAdaptor *partial_index);

  int check_tablet_valid(ObVectorIndexRecordType type);

  int get_dim(int64_t &dim);
  int get_hnsw_param(ObVectorIndexHNSWParam *&param);

  // for virtual table
  int fill_vector_index_info(ObVectorIndexInfo &info);

  const roaring::api::roaring64_bitmap_t *get_incr_ibitmap();
  const roaring::api::roaring64_bitmap_t *get_vbitmap_ibitmap();
  const roaring::api::roaring64_bitmap_t *get_vbitmap_dbitmap();

  // VSAG ADD
  int insert_rows(blocksstable::ObDatumRow *rows,
                  const int64_t vid_idx,
                  const int64_t type_idx,
                  const int64_t vector_idx,
                  const int64_t row_count);

  int add_snap_index(float *vectors, int64_t *vids, int num);

  // Query Processor first
  int check_delta_buffer_table_readnext_status(ObVectorQueryAdaptorResultContext *ctx,
                                               common::ObNewRowIterator *row_iter,
                                               SCN query_scn);
  int complete_delta_buffer_table_data(ObVectorQueryAdaptorResultContext *ctx);
  // Query Processor second
  int check_index_id_table_readnext_status(ObVectorQueryAdaptorResultContext *ctx,
                                           common::ObNewRowIterator *row_iter,
                                           SCN query_scn);
  // Query Processor third
  int check_snapshot_table_wait_status(ObVectorQueryAdaptorResultContext *ctx);

  int query_result(ObVectorQueryAdaptorResultContext *ctx,
                   ObVectorQueryConditions *query_cond,
                   ObVectorQueryVidIterator *&vids_iter);
  static int param_deserialize(char *ptr, int32_t length,
                                    ObIAllocator *allocator,
                                    ObVectorIndexAlgorithmType &type,
                                    void *&param);
  static int cast_roaringbitmap_to_stdmap(const roaring::api::roaring64_bitmap_t *bitmap,
                                          std::map<int, bool> &mymap);
  int check_vsag_mem_used();
  uint64_t get_all_vsag_mem_used() {
    return ATOMIC_LOAD(all_vsag_use_mem_);
  }
  int get_incr_vsag_mem_used();
  int get_incr_vsag_mem_hold();
  int get_snap_vsag_mem_used();
  int get_snap_vsag_mem_hold();
  ObIAllocator *get_allocator() { return allocator_; }

  void *get_algo_data() { return algo_data_; }


  int complete_index_mem_data(SCN read_scn,
                              common::ObNewRowIterator *row_iter,
                              blocksstable::ObDatumRow *last_row,
                              ObArray<uint64_t> &i_vids);
  int prepare_delta_mem_data(roaring::api::roaring64_bitmap_t *gene_bitmap,
                             ObArray<uint64_t> &i_vids,
                             ObVectorQueryAdaptorResultContext *ctx);
  int serialize(ObIAllocator *allocator, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb);
  int complete_delta_mem_data(roaring::api::roaring64_bitmap_t *gene_bitmap,
                              roaring::api::roaring64_bitmap_t *delta_bitmap,
                              ObIAllocator *allocator);

  int check_need_sync_to_follower(bool &need_sync);

  void sync_finish() { follower_sync_statistics_.sync_count_++; }
  void sync_fail() { follower_sync_statistics_.sync_fail_++; }

  void inc_sync_idle_count() { follower_sync_statistics_.idle_count_++; }
  void reset_sync_idle_count() { follower_sync_statistics_.idle_count_ = 0;}
  int64_t get_sync_idle_count() { return follower_sync_statistics_.idle_count_; }

  int init_mem(ObVectorIndexMemData *&table_info);
  int init_mem_data(ObVectorIndexRecordType type);
  bool is_mem_data_init_atomic(ObVectorIndexRecordType type);
  int try_init_mem_data(ObVectorIndexRecordType type) {
    int ret = OB_SUCCESS;
    if (!is_mem_data_init_atomic(type)) {
      ret = init_mem_data(type);
    }
    return ret;
  }

  ObAdapterCreateType &get_create_type() { return create_type_; };
  void set_create_type(ObAdapterCreateType type) { create_type_ = type; };

  TO_STRING_KV(K_(create_type), K_(type), KP_(algo_data), KP_(incr_data), KP_(snap_data), KP_(vbitmap_data),
              K_(data_tablet_id),K_(rowkey_vid_tablet_id), K_(vid_rowkey_tablet_id),
              K_(inc_tablet_id), K_(vbitmap_tablet_id), K_(snapshot_tablet_id),
              K_(data_table_id), K_(rowkey_vid_table_id), K_(vid_rowkey_table_id),
              K_(inc_table_id),  K_(vbitmap_table_id), K_(snapshot_table_id),
              K_(ref_cnt), K_(idle_cnt), KP_(allocator),
              K_(index_identity), K_(follower_sync_statistics));

private:
  void *get_incr_index();
  void *get_snap_index();
  int add_datum_row_into_array(blocksstable::ObDatumRow *datum_row,
                               ObArray<uint64_t> &i_vids,
                               ObArray<uint64_t> &d_vids);
  bool check_if_complete_index(SCN read_scn);
  bool check_if_complete_delta(roaring::api::roaring64_bitmap_t *gene_bitmap);
  int write_into_delta_mem(ObVectorQueryAdaptorResultContext *ctx, int count, float *vectors,  uint64_t *vids);
  int write_into_index_mem(int64_t dim, SCN read_scn,
                           ObArray<uint64_t> &i_vids,
                           ObArray<uint64_t> &d_vids);
  int generate_snapshot_valid_bitmap(ObVectorQueryAdaptorResultContext *ctx,
                                     common::ObNewRowIterator *row_iter,
                                     SCN query_scn);

  void output_bitmap(roaring::api::roaring64_bitmap_t *bitmap);

  int merge_mem_data_(ObVectorIndexRecordType type,
                      ObPluginVectorIndexAdaptor *partial_idx_adpt,
                      ObVectorIndexMemData *&src_mem_data,
                      ObVectorIndexMemData *&dst_mem_data);
  int merge_and_generate_bitmap(ObVectorQueryAdaptorResultContext *ctx,
                                roaring::api::roaring64_bitmap_t *&ibitmap,
                                roaring::api::roaring64_bitmap_t *&dbitmap);

  int vsag_query_vids(ObVectorQueryAdaptorResultContext *ctx,
                      ObVectorQueryConditions *query_cond,
                      int64_t dim, float *query_vector,
                      ObVectorQueryVidIterator *&vids_iter);

private:
  ObAdapterCreateType create_type_;
  ObVectorIndexAlgorithmType type_;
  void *algo_data_;
  ObVectorIndexMemData *incr_data_;
  ObVectorIndexMemData *snap_data_;
  ObVectorIndexMemData *vbitmap_data_;

  ObTabletID snapshot_tablet_id_;
  ObTabletID inc_tablet_id_;
  ObTabletID vbitmap_tablet_id_;
  ObTabletID data_tablet_id_;
  ObTabletID rowkey_vid_tablet_id_;
  ObTabletID vid_rowkey_tablet_id_;

  uint64_t inc_table_id_;
  uint64_t vbitmap_table_id_;
  uint64_t snapshot_table_id_;
  uint64_t data_table_id_;
  uint64_t rowkey_vid_table_id_;
  uint64_t vid_rowkey_table_id_;

  int64_t ref_cnt_;
  int64_t idle_cnt_; // not merged cnt
  int64_t mem_check_cnt_;
  uint64_t *all_vsag_use_mem_;
  ObIAllocator *allocator_; // allocator for alloc adapter self
  lib::MemoryContext &parent_mem_ctx_;
  ObString index_identity_; // identify multi indexes on one table & column, generate unique uint64 to save memory?

  // statistics for judging whether need sync follower
  ObVectorIndexFollowerSyncStatic follower_sync_statistics_;

  constexpr static uint32_t VEC_INDEX_INCR_DATA_SYNC_THRESHOLD = 100;
  constexpr static uint32_t VEC_INDEX_VBITMAP_SYNC_THRESHOLD = 100;
  constexpr static uint32_t VEC_INDEX_SNAP_DATA_SYNC_THRESHOLD = 1;
  constexpr static uint32_t VEC_INDEX_ADAPTER_MAX_IDLE_COUNT = 3;
  constexpr const static char* const VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_MAX] = {
    "l2",
    "ip",
    "cos",
  };
};

class ObPluginVectorIndexAdapterGuard
{
public:
  ObPluginVectorIndexAdapterGuard(ObPluginVectorIndexAdaptor *adapter = nullptr)
    : adapter_(adapter)
  {}
  ~ObPluginVectorIndexAdapterGuard()
  {
    if (is_valid()) {
      if (adapter_->dec_ref_and_check_release()) {
        ObIAllocator *allocator = adapter_->get_allocator();
        if (OB_ISNULL(allocator)) {
          const int ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "null allocator", KPC(adapter_));
        } else {
          OB_LOG(INFO, "adatper released", KPC(adapter_), K(lbt()));
          adapter_->~ObPluginVectorIndexAdaptor();
          allocator->free(adapter_);
        }
      }
      adapter_ = nullptr;
    }
  }

  bool is_valid() { return adapter_ != nullptr; }
  ObPluginVectorIndexAdaptor* get_adatper() { return adapter_; }
  int set_adapter(ObPluginVectorIndexAdaptor *adapter)
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "vector index adapter guard can only set once", KPC(adapter_), KPC(adapter));
    } else {
      adapter_ = adapter;
      (void)adapter_->inc_ref();
    }
    return ret;
  }
  TO_STRING_KV(KPC_(adapter));

private:
  ObPluginVectorIndexAdaptor *adapter_;
};

class ObVsagMemContext : public vsag::Allocator
{
public:
  ObVsagMemContext(uint64_t *all_vsag_use_mem)
    : all_vsag_use_mem_(all_vsag_use_mem),
      mem_context_(nullptr) {};
  ~ObVsagMemContext() {
    if (mem_context_ != nullptr) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
      all_vsag_use_mem_ = nullptr;
    }
  }
  int init(lib::MemoryContext &parent_mem_context, uint64_t *all_vsag_use_mem);
  bool is_inited() { return OB_NOT_NULL(mem_context_); }

  std::string Name() override {
    return "ObVsagAlloc";
  }
  void* Allocate(size_t size) override;

  void Deallocate(void* p) override;

  void* Reallocate(void* p, size_t size) override;

  int64_t hold() {
    return mem_context_->hold();
  }

  int64_t used() {
    return mem_context_->used();
  }

private:
  uint64_t *all_vsag_use_mem_;
  lib::MemoryContext mem_context_;
  constexpr static int64_t MEM_PTR_HEAD_SIZE = sizeof(int64_t);
};

};
};
#endif // OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_ADAPTOR_H_