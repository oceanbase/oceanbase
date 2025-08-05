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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_IVF_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_IVF_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/engine/expr/ob_expr_vec_ivf_sq8_data_vector.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
#define IVF_GET_NEXT_ROWS_BEGIN(iter)                                    \
  bool index_end = false;                                                \
  iter->clear_evaluated_flag();                                          \
  int64_t scan_row_cnt = 0;                                              \
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE; \
  while (!index_end && OB_SUCC(ret)) {                                   \
    if (OB_FAIL(iter->get_next_rows(scan_row_cnt, batch_row_count))) {   \
      if (OB_ITER_END != ret) {                                          \
        LOG_WARN("failed to get next row.", K(ret));                     \
      } else {                                                           \
        index_end = true;                                                \
      }                                                                  \
    }                                                                    \
    if (OB_FAIL(ret) && OB_ITER_END != ret) {                            \
    } else if (scan_row_cnt > 0) {                                       \
      ret = OB_SUCCESS;                                                  \
    }

#define IVF_GET_NEXT_ROWS_END(iter, scan_param, tablet_id)                             \
  }                                                                                    \
  if (index_end) {                                                                     \
    int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;                             \
    if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, iter, scan_param, tablet_id))) { \
      LOG_WARN("failed to reuse rowkey cid iter.", K(ret));                            \
    } else {                                                                           \
      ret = tmp_ret;                                                                   \
    }                                                                                  \
  }

struct ObDASIvfScanIterParam : public ObDASIterParam {
public:
  explicit ObDASIvfScanIterParam(const ObVectorIndexAlgorithmType index_type)
      : ObDASIterParam(ObDASIterType::DAS_ITER_IVF_SCAN),
        ls_id_(),
        tx_desc_(nullptr),
        snapshot_(nullptr),
        inv_idx_scan_iter_(nullptr),
        centroid_iter_(nullptr),
        cid_vec_iter_(nullptr),
        rowkey_cid_iter_(nullptr),
        sq_meta_iter_(nullptr),
        pq_centroid_iter_(nullptr),
        brute_iter_(nullptr),
        vec_aux_ctdef_(nullptr),
        vec_aux_rtdef_(nullptr),
        sort_ctdef_(nullptr),
        sort_rtdef_(nullptr),
        index_type(index_type)
  {}

  virtual bool is_valid() const override
  {
    bool bret = ls_id_.is_valid() && nullptr != tx_desc_ && nullptr != snapshot_ && nullptr != inv_idx_scan_iter_ &&
                nullptr != vec_aux_ctdef_ && nullptr != vec_aux_rtdef_;
    if (bret != true) {
    } else if (index_type == ObVectorIndexAlgorithmType::VIAT_IVF_FLAT) {
      bret = nullptr != centroid_iter_ && nullptr != cid_vec_iter_ && nullptr != rowkey_cid_iter_;
    } else if (index_type == ObVectorIndexAlgorithmType::VIAT_IVF_SQ8) {
      bret = nullptr != centroid_iter_ && nullptr != cid_vec_iter_ && nullptr != rowkey_cid_iter_ &&
             nullptr != sq_meta_iter_;
    } else if (index_type == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
      bret = nullptr != centroid_iter_ && nullptr != cid_vec_iter_ && nullptr != rowkey_cid_iter_ &&
             nullptr != pq_centroid_iter_;
    } else {
      bret = false;
    }

    return bret;
  }

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASIter *inv_idx_scan_iter_;
  ObDASScanIter *centroid_iter_;
  ObDASScanIter *cid_vec_iter_;
  ObDASScanIter *rowkey_cid_iter_;
  ObDASScanIter *sq_meta_iter_;
  ObDASScanIter *pq_centroid_iter_;
  ObDASScanIter *brute_iter_;
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
  ObVectorIndexAlgorithmType index_type;
};

class ObIvfPreFilter
{
public:
  enum FilterType {
    ROARING_BITMAP = 0,
    SIMPLE_RANGE = 1,
  };
public:
  ObIvfPreFilter(uint64_t tenant_id,
               FilterType type = FilterType::ROARING_BITMAP,
               ObIAllocator *allocator = nullptr,
               uint8_t *bitmap = nullptr) :
               tenant_id_(tenant_id),
               type_(type),
               roaring_bitmap_(nullptr),
               rk_range_() {}
  ~ObIvfPreFilter() { reset(); }
  void reset();
  int init();
  int init(const ObIArray<const ObNewRange*> &range);
  bool is_valid() {
    bool bret = false;
    if (type_ == FilterType::ROARING_BITMAP) {
      bret = OB_NOT_NULL(roaring_bitmap_);
    } else if (type_ == FilterType::SIMPLE_RANGE) {
      bret = !rk_range_.empty();
    }
    return bret;
  }
  bool is_range_filter() { return type_ == FilterType::SIMPLE_RANGE; }
  bool test(const ObRowkey& main_rowkey);
  int add(int64_t id);
  TO_STRING_KV(K(tenant_id_), K_(type), KP_(roaring_bitmap));
public:
  uint64_t tenant_id_;
  FilterType type_;
  roaring::api::roaring64_bitmap_t *roaring_bitmap_;
  ObArray<const ObNewRange *> rk_range_;
};

class ObDASIvfBaseScanIter : public ObDASIter
{
public:
  using IvfRowkeyHeap = share::ObVectorCenterClusterHelper<float, ObRowkey>;
  ObDASIvfBaseScanIter()
      : ObDASIter(ObDASIterType::DAS_ITER_IVF_SCAN),
        mem_context_(nullptr),
        vec_op_alloc_("IvfIdxLookupOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        persist_alloc_(ObMemAttr(MTL_ID(), "IvfScan")),
        ls_id_(),
        tx_desc_(nullptr),
        snapshot_(nullptr),
        centroid_iter_(nullptr),
        cid_vec_iter_(nullptr),
        rowkey_cid_iter_(nullptr),
        brute_iter_(nullptr),
        centroid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        cid_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        rowkey_cid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        sq_meta_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        pq_centroid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        brute_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        centroid_scan_param_(),
        cid_vec_scan_param_(),
        rowkey_cid_scan_param_(),
        brute_scan_param_(),
        centroid_iter_first_scan_(true),
        cid_vec_iter_first_scan_(true),
        rowkey_cid_iter_first_scan_(true),
        brute_first_scan_(true),
        sort_ctdef_(nullptr),
        sort_rtdef_(nullptr),
        limit_param_(),
        vec_index_param_(),
        dim_(0),
        need_norm_(false),
        search_vec_(nullptr),
        real_search_vec_(),
        inv_idx_scan_iter_(nullptr),
        vec_aux_ctdef_(nullptr),
        vec_aux_rtdef_(nullptr),
        saved_rowkeys_itr_(nullptr)
  {
    dis_type_ = ObExprVectorDistance::ObVecDisType::MAX_TYPE;
    saved_rowkeys_.set_attr(ObMemAttr(MTL_ID(), "VecIdxKeyRanges"));
    pre_fileter_rowkeys_.set_attr(ObMemAttr(MTL_ID(), "VecIdxKeyRanges"));
  }
  virtual ~ObDASIvfBaseScanIter()
  {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids);

  ObDASIter *get_inv_idx_scan_iter()
  {
    return inv_idx_scan_iter_;
  }

  void set_ls_id(const share::ObLSID &ls_id)
  {
    ls_id_ = ls_id;
  }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int process_ivf_scan(bool is_vectorized);
  virtual int process_ivf_scan_post(bool is_vectorized) = 0;
  virtual int process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized) = 0;
protected:
  int do_table_full_scan(bool is_vectorized,
                        const ObDASScanCtDef *ctdef,
                        ObDASScanRtDef *rtdef,
                        ObDASScanIter *iter,
                        int64_t pri_key_cnt,
                        ObTabletID &tablet_id,
                        bool &first_scan,
                        ObTableScanParam &scan_param);
  int do_aux_table_scan(bool &first_scan,
                        ObTableScanParam &scan_param,
                        const ObDASScanCtDef *ctdef,
                        ObDASScanRtDef *rtdef,
                        ObDASScanIter *iter,
                        ObTabletID &tablet_id);
  int do_rowkey_cid_table_scan();

  int build_cid_vec_query_range(const ObString &cid, int64_t rowkey_cnt, ObNewRange &cid_rowkey_range);
  int build_cid_vec_query_rowkey(const ObString &cid, bool is_min, int64_t rowkey_cnt, common::ObRowkey &rowkey);
  int gen_rowkeys_itr_brute(ObDASIter *scan_iter);
  int gen_rowkeys_itr();
  int get_next_saved_rowkeys(int64_t &count);
  int get_next_saved_rowkey();
  int get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey) {
    const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(),
                                                                        ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
    ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx());
    return ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey);
  }
  int get_rowkey_pre_filter(ObIAllocator& allocator, bool is_vectorized, int64_t max_rowkey_count);
  int prepare_cid_range(const ObDASScanCtDef *cid_vec_ctdef, int64_t &cid_vec_column_count,
                        int64_t &cid_vec_pri_key_cnt, int64_t &rowkey_cnt);
  int scan_cid_range(const ObString &cid, int64_t cid_vec_pri_key_cnt, const ObDASScanCtDef *cid_vec_ctdef,
                     ObDASScanRtDef *cid_vec_rtdef, storage::ObTableScanIterator *&cid_vec_scan_iter);
  int64_t get_nprobe(const common::ObLimitParam &limit_param, int64_t enlargement_factor = 1);
  int generate_nearest_cid_heap(bool is_vectorized,
                                share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
                                bool save_center_vec = false);
  int parse_centroid_datum(const ObDASScanCtDef *cid_vec_ctdef, ObIAllocator &allocator,
                           blocksstable::ObDatumRow *datum_row, ObString &cid, ObString &cid_vec);
  int get_main_rowkey_from_cid_vec_datum(ObIAllocator &allocator, const ObDASScanCtDef *cid_vec_ctdef,
                                         const int64_t rowkey_cnt, ObRowkey &main_rowkey, bool need_alloc = true);
  int get_pre_filter_rowkey_batch(ObIAllocator &allocator,
    bool is_vectorized,
    int64_t batch_row_count,
    bool &index_end);
  int gen_near_cid_heap_from_cache(ObIvfCentCache &cent_cache,
                                   share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
                                   bool save_center_vec /*= false*/);
  int gen_near_cid_heap_from_table(
      bool is_vectorized,
      share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
      bool save_center_vec /*= false*/);
  int try_write_centroid_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized);
  int get_centers_cache(
      bool is_vectorized,
      bool is_pq_centers,
      ObIvfCacheMgrGuard &cache_guard,
      ObIvfCentCache *&cent_cache,
      bool &is_cache_usable);
  virtual int try_write_pq_centroid_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized) {
    int ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "ObDASIvfScanIter do not support try_write_pq_centroid_cache", K(ret));
    return ret;
  }
  int get_main_rowkey_brute(ObIAllocator &allocator, const ObDASScanCtDef *cid_vec_ctdef, const int64_t rowkey_cnt,
                            ObRowkey &main_rowkey);
  template <typename T>
  int calc_vec_dis(T *a, T *b, int dim, float &dis, ObExprVectorDistance::ObVecDisType dis_type);
  int get_rowkey_brute_post(bool is_vectorized, IvfRowkeyHeap& nearest_rowkey_heap);

protected:
  static const int64_t CENTROID_PRI_KEY_CNT = 1;
  static const int64_t CENTROID_ALL_KEY_CNT = 2;

  static const int64_t CID_VEC_COM_KEY_CNT = 1;        // Only the vec column is a common column
  static const int64_t CID_VEC_FIXED_PRI_KEY_CNT = 1;  // center_id is FIXED PRI KEY
  static const int64_t ROWKEY_CID_PRI_KEY_CNT = 1;

  static const int64_t SQ_MEAT_PRI_KEY_CNT = 1;
  static const int64_t SQ_MEAT_ALL_KEY_CNT = 2;
  static const int64_t POST_ENLARGEMENT_FACTOR = 2;
  static const int64_t PRE_ENLARGEMENT_FACTOR = 2;
  // in centroid table
  static const int64_t CID_IDX = 0;
  static const int64_t CID_VECTOR_IDX = 1;
  static const uint64_t IVF_MAX_BRUTE_FORCE_SIZE = 10001;
  // data table
  static const int64_t DATA_VECTOR_IDX = 0;

protected:
  lib::MemoryContext mem_context_;
  ObArenaAllocator vec_op_alloc_;
  // unlike vec_op_alloc_ do reset() in inner_resuse()
  // persist_alloc_ do reset() in inner_release()
  ObArenaAllocator persist_alloc_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *centroid_iter_;
  ObDASScanIter *cid_vec_iter_;
  ObDASScanIter *rowkey_cid_iter_;
  ObDASScanIter *brute_iter_;

  ObTabletID centroid_tablet_id_;
  ObTabletID cid_vec_tablet_id_;
  ObTabletID rowkey_cid_tablet_id_;
  ObTabletID sq_meta_tablet_id_;
  ObTabletID pq_centroid_tablet_id_;
  ObTabletID brute_tablet_id_;

  ObTableScanParam centroid_scan_param_;
  ObTableScanParam cid_vec_scan_param_;
  ObTableScanParam rowkey_cid_scan_param_;
  ObTableScanParam brute_scan_param_;

  bool centroid_iter_first_scan_;
  bool cid_vec_iter_first_scan_;
  bool rowkey_cid_iter_first_scan_;
  bool brute_first_scan_;

  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;

  common::ObLimitParam limit_param_;

  ObVectorIndexParam vec_index_param_;
  int64_t dim_;
  bool need_norm_;
  double selectivity_;
  ObExpr *search_vec_;
  ObString real_search_vec_;

  ObDASIter *inv_idx_scan_iter_;

  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  int64_t nprobes_ = 8;
  ObExprVectorDistance::ObVecDisType dis_type_;  // default metric;
  ObVectorQueryRowkeyIterator *saved_rowkeys_itr_;
  common::ObSEArray<common::ObRowkey, 16> saved_rowkeys_;
  common::ObSEArray<common::ObRowkey, 16> pre_fileter_rowkeys_;
};

class ObDASIvfScanIter : public ObDASIvfBaseScanIter
{
public:
  ObDASIvfScanIter() : ObDASIvfBaseScanIter()
  {
    near_cid_.set_attr(ObMemAttr(MTL_ID(), "NearCidVecPos"));
  }
  virtual ~ObDASIvfScanIter()
  {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;

protected:
  int get_nearest_probe_center_ids(bool is_vectorized);

  template <typename T>
  int get_rowkeys_to_heap(const ObString &cid_str, int64_t cid_vec_pri_key_cnt, int64_t cid_vec_column_count,
                          int64_t rowkey_cnt, bool is_vectorized,
                          ObVectorCenterClusterHelper<T, ObRowkey> &nearest_rowkey_heap);
  template <typename T>
  int get_nearest_limit_rowkeys_in_cids(bool is_vectorized, T *serch_vec);
  virtual int process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized);
  int check_cid_exist(const ObString &src_cid, bool &src_cid_exist);
  int get_cid_from_rowkey_cid_table(ObString &cid);
  int filter_pre_rowkey_batch(bool is_vectorized, int64_t batch_row_count);
  int filter_rowkey_by_cid(bool is_vectorized, int64_t batch_row_count, int &push_count);
  virtual int process_ivf_scan_post(bool is_vectorized);
  int parse_cid_vec_datum(
    ObIAllocator& allocator,
    int64_t cid_vec_column_count,
    const ObDASScanCtDef *cid_vec_ctdef,
    const int64_t rowkey_cnt,
    ObRowkey &main_rowkey,
    ObString &com_key);
protected:
  // cid is begin with 0, so near_cid_vec_dist_ count is nlist + 1
  common::ObArrayWrap<bool> near_cid_dist_;
  common::ObArray<ObCenterId> near_cid_;
};

class ObDASIvfPQScanIter : public ObDASIvfBaseScanIter
{
public:
  // <center id, center vector>
  using IvfCidVecPair = std::pair<ObCenterId, float *>;

  ObDASIvfPQScanIter()
      : ObDASIvfBaseScanIter(),
        pq_centroid_iter_(nullptr),
        pq_centroid_scan_param_(),
        pq_centroid_first_scan_(true),
        m_(0),
        nbits_(0)
  {
    near_cid_vec_.set_attr(ObMemAttr(MTL_ID(), "NearCidVecPos"));
    near_cid_vec_dis_.set_attr(ObMemAttr(MTL_ID(), "NearCidVecDis"));
  }
  virtual ~ObDASIvfPQScanIter()
  {}

protected:
  int inner_init(ObDASIterParam &param) override;
  int inner_reuse() override;
  int inner_release() override;

  int process_ivf_scan_post(bool is_vectorized) override;
  int process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized) override;
  int filter_pre_rowkey_batch(bool is_vectorized, int64_t batch_row_count, IvfRowkeyHeap &rowkey_heap);
  int filter_rowkey_by_cid(bool is_vectorized, int64_t batch_row_count, IvfRowkeyHeap &rowkey_heap, int &push_count);
  int parse_pq_ids_vec_datum(
    ObIAllocator &allocator,
    int64_t cid_vec_column_count,
    const ObDASScanCtDef *cid_vec_ctdef,
    const int64_t rowkey_cnt,
    ObRowkey &main_rowkey,
    ObString &com_key);
  int calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    ObIvfPreFilter *prefilter);
  int calc_distance_between_pq_ids_by_table(
      bool is_vectorized,
      const ObString &pq_center_ids,
      const ObIArray<float *> &splited_residual,
      int64_t batch_row_count,
      float &distance);
  int get_nearest_probe_centers(bool is_vectorized);
  int get_cid_from_pq_rowkey_cid_table(ObIAllocator &allocator, ObString &cid, ObString &pq_cids);
  int check_cid_exist(const ObString &src_cid, float *&center_vec, bool &src_cid_exist);
  int calc_adc_distance(
    bool is_vectorized,
    const ObString &cid,
    const ObString &pq_center_ids,
    IvfRowkeyHeap &rowkey_heap,
    ObArray<float *> &splited_residual,
    float *residual,
    int &push_count);
  int calc_distance_between_pq_ids(
    bool is_vectorized,
    const ObString &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    float &distance);
  int calc_distance_between_pq_ids_by_cache(ObIvfCentCache &cent_cache,
                                            const ObString &pq_center_ids,
                                            const ObIArray<float *> &splited_residual,
                                            float &distance);
  int try_write_pq_centroid_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized) override;
  int get_pq_precomputetable_cache(
      bool is_vectorized,
      ObIvfCacheMgrGuard &cache_guard,
      ObIvfCentCache *&cent_cache,
      bool &is_cache_usable);
  int try_write_pq_precompute_table_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized);
  int pre_compute_inner_prod_table(
    const float* search_vec,
    float* dis_table,
    bool is_vectorized);
  int build_rowkey_hash_set(
    ObIvfPreFilter &prefilter,
    bool is_vectorized,
    int64_t batch_row_count);
  int process_ivf_scan_brute_inner(bool is_vectorized);
  uint64_t hash_val_for_rk(const common::ObRowkey& rk);
  int calc_distance_with_precompute(
    ObEvalCtx::BatchInfoScopeGuard &guard,
    int64_t scan_row_cnt,
    int64_t rowkey_cnt,
    ObRowkey& filter_main_rowkey,
    float *sim_table,
    float dis0,
    IvfRowkeyHeap& nearest_rowkey_heap,
    ObIvfPreFilter *prefilter);
  int check_can_pre_compute(
    bool is_vectorized,
    ObIvfCacheMgrGuard &pre_cache_guard,
    ObIvfCentCache *&pre_cent_cache,
    bool &pre_compute_table);
private:
  // in pq_code table
  static const int64_t PQ_CENTROID_VEC_IDX = 1;
  // in pq_centroid table
  static const int64_t CIDS_IDX = 0;
  static const int64_t PQ_IDS_IDX = 1;
  // tmp enlargement factor
  static const int64_t PQ_ID_ENLARGEMENT_FACTOR = 10;

  ObDASScanIter *pq_centroid_iter_;
  ObTableScanParam pq_centroid_scan_param_;
  bool pq_centroid_first_scan_;
  int64_t m_;
  int64_t nbits_;
  // cid is begin with 0, so near_cid_vec_ptrs_ count is nlist + 1
  common::ObArrayWrap<float *> near_cid_vec_ptrs_;
  common::ObArray<IvfCidVecPair> near_cid_vec_;
  common::ObArray<float> near_cid_vec_dis_; // nearest center vec distance from query vec
};

class ObDASIvfSQ8ScanIter : public ObDASIvfScanIter
{
public:
  static const int64_t META_VECTOR_IDX = 1;
  ObDASIvfSQ8ScanIter()
      : ObDASIvfScanIter(),
        sq_meta_iter_(nullptr),
        sq_meta_scan_param_(),
        sq_meta_iter_first_scan_(true)
  {}
  virtual ~ObDASIvfSQ8ScanIter()
  {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override
  {
    int ret = OB_SUCCESS;
    if (!sq_meta_iter_first_scan_ &&
        OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, sq_meta_iter_, sq_meta_scan_param_, sq_meta_tablet_id_))) {
      LOG_WARN("failed to reuse iter", K(ret));
    } else {
      ret = ObDASIvfScanIter::inner_reuse();
    }
    return ret;
  }
  virtual int inner_release() override;
  int get_real_search_vec_u8(bool is_vectorized, ObString &real_search_vec_u8);
  int process_ivf_scan_post(bool is_vectorized) override;

private:
  ObDASScanIter *sq_meta_iter_;
  ObTableScanParam sq_meta_scan_param_;
  bool sq_meta_iter_first_scan_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_IVF_SCAN_ITER_H_ */
