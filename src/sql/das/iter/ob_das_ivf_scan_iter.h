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

namespace oceanbase
{
using namespace common;
namespace sql
{
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
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
  ObVectorIndexAlgorithmType index_type;
};

class ObDASIvfBaseScanIter : public ObDASIter
{
public:
  ObDASIvfBaseScanIter()
      : ObDASIter(ObDASIterType::DAS_ITER_IVF_SCAN),
        vec_op_alloc_("IvfIdxLookupOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        ls_id_(),
        tx_desc_(nullptr),
        snapshot_(nullptr),
        centroid_iter_(nullptr),
        cid_vec_iter_(nullptr),
        rowkey_cid_iter_(nullptr),
        centroid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        cid_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        rowkey_cid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        sq_meta_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        pq_centroid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
        centroid_scan_param_(),
        cid_vec_scan_param_(),
        rowkey_cid_scan_param_(),
        centroid_iter_first_scan_(true),
        cid_vec_iter_first_scan_(true),
        rowkey_cid_iter_first_scan_(true),
        sort_ctdef_(nullptr),
        sort_rtdef_(nullptr),
        limit_param_(),
        vec_index_param_(),
        dim_(0),
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
  virtual int inner_get_next_row() override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    return OB_NOT_SUPPORTED;
  }

protected:
  int do_table_full_scan(bool &first_scan,
                         ObTableScanParam &scan_param,
                         const ObDASScanCtDef *ctdef,
                         ObDASScanRtDef *rtdef,
                         ObDASScanIter *iter,
                         int64_t pri_key_cnt,
                         ObTabletID &tablet_id);
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

protected:
  static const int64_t CENTROID_PRI_KEY_CNT = 1;
  static const int64_t CENTROID_ALL_KEY_CNT = 2;

  static const int64_t CID_VEC_COM_KEY_CNT = 1;        // Only the vec column is a common column
  static const int64_t CID_VEC_FIXED_PRI_KEY_CNT = 1;  // center_id is FIXED PRI KEY
  static const int64_t ROWKEY_CID_PRI_KEY_CNT = 1;

  static const int64_t MAX_BRUTE_FORCE_SIZE = 2;

  static const int64_t SQ_MEAT_PRI_KEY_CNT = 1;
  static const int64_t SQ_MEAT_ALL_KEY_CNT = 2;
protected:
  common::ObArenaAllocator vec_op_alloc_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *centroid_iter_;
  ObDASScanIter *cid_vec_iter_;
  ObDASScanIter *rowkey_cid_iter_;

  ObTabletID centroid_tablet_id_;
  ObTabletID cid_vec_tablet_id_;
  ObTabletID rowkey_cid_tablet_id_;
  ObTabletID sq_meta_tablet_id_;
  ObTabletID pq_centroid_tablet_id_;

  ObTableScanParam centroid_scan_param_;
  ObTableScanParam cid_vec_scan_param_;
  ObTableScanParam rowkey_cid_scan_param_;

  bool centroid_iter_first_scan_;
  bool cid_vec_iter_first_scan_;
  bool rowkey_cid_iter_first_scan_;

  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;

  common::ObLimitParam limit_param_;

  ObString vec_index_param_;
  int64_t dim_;
  ObExpr *search_vec_;
  ObString real_search_vec_;

  ObDASIter *inv_idx_scan_iter_;

  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  int64_t nprobes_ = 8;
  ObExprVectorDistance::ObVecDisType dis_type_;  // default metric;
  ObVectorQueryRowkeyIterator *saved_rowkeys_itr_;
  common::ObSEArray<common::ObRowkey *, 16> saved_rowkeys_;
  common::ObSEArray<common::ObRowkey *, 16> pre_fileter_rowkeys_;
};

class ObDASIvfScanIter : public ObDASIvfBaseScanIter
{
public:
  ObDASIvfScanIter() : ObDASIvfBaseScanIter()
  {}
  virtual ~ObDASIvfScanIter()
  {}

protected:
  virtual int inner_init(ObDASIterParam &param) override
  {
    return ObDASIvfBaseScanIter::inner_init(param);
  };
  virtual int inner_reuse() override
  {
    return ObDASIvfBaseScanIter::inner_reuse();
  }
  virtual int inner_release() override
  {
    return ObDASIvfBaseScanIter::inner_release();
  }
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

protected:
  int get_nearest_probe_center_ids(ObIArray<ObString> &nearest_cids);
  int get_main_rowkey_from_cid_vec_datum(const ObDASScanCtDef *cid_vec_ctdef,
                                         const int64_t rowkey_cnt,
                                         ObRowkey *&main_rowkey);
  virtual int process_ivf_scan(bool is_vectorized);
  template <typename T>
  int get_nearest_limit_rowkeys_in_cids(const ObIArray<ObString> &nearest_cids, T *serch_vec);
  int get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey);
  virtual int process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized);
  bool check_cid_exist(const ObIArray<ObString> &dst_cids, const ObString &src_cid);
  int get_cid_from_rowkey_cid_table(ObString &cid);
  int get_rowkey_pre_filter(bool is_vectorized, uint64_t &rowkey_count);
  int filter_pre_rowkey_batch(const ObIArray<ObString> &nearest_cids, bool is_vectorized, int64_t batch_row_count);
  int filter_rowkey_by_cid(const ObIArray<ObString> &nearest_cids,
                                  bool is_vectorized,
                                  int64_t batch_row_count,
                                  bool &index_end);
  int get_pre_filter_rowkey_batch(ObIAllocator &allocator,
                                  bool is_vectorized,
                                  int64_t batch_row_count,
                                  bool &index_end);
  virtual int process_ivf_scan_post();
  int process_ivf_scan_brute();
  int generate_nearear_cid_heap(
    share::ObVectorCentorClusterHelper<float, ObString> &nearest_cid_heap,
    bool save_center_vec = false);
  int prepare_cid_range(
    const ObDASScanCtDef *cid_vec_ctdef,
    int64_t &cid_vec_column_count,
    int64_t &cid_vec_pri_key_cnt,
    int64_t &rowkey_cnt);
  int scan_cid_range(
    const ObString &cid,
    int64_t cid_vec_pri_key_cnt,
    const ObDASScanCtDef *cid_vec_ctdef,
    ObDASScanRtDef *cid_vec_rtdef,
    storage::ObTableScanIterator *&cid_vec_scan_iter);
  int parse_cid_vec_datum(
    int64_t cid_vec_column_count,
    const ObDASScanCtDef *cid_vec_ctdef,
    const int64_t rowkey_cnt,
    ObRowkey *&main_rowkey,
    ObString &com_key);
  int parse_centroid_datum_with_deep_copy(
    const ObDASScanCtDef *cid_vec_ctdef,
    ObIAllocator& allocator,
    blocksstable::ObDatumRow *datum_row,
    ObString &cid,
    ObString &cid_vec);
};

class ObDASIvfPQScanIter : public ObDASIvfScanIter
{
public:
  // <center id, center vector>
  using IvfCidVecPair = std::pair<ObString, float *>;
  using IvfRowkeyHeap = share::ObVectorCentorClusterHelper<float, ObRowkey *>;

  ObDASIvfPQScanIter()
      : ObDASIvfScanIter(),
        pq_centroid_iter_(nullptr),
        pq_centroid_scan_param_(),
        pq_centroid_first_scan_(true),
        m_(0),
        pq_ids_type_(nullptr),
        persist_alloc_(ObMemAttr(MTL_ID(), "IvfPQScan"))
  {}
  virtual ~ObDASIvfPQScanIter()
  {}

protected:
  int inner_init(ObDASIterParam &param) override;
  int inner_reuse() override
  {
    int ret = OB_SUCCESS;
    if (!pq_centroid_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                        ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) {
      LOG_WARN("failed to reuse iter", K(ret));
    } else {
      ret = ObDASIvfScanIter::inner_reuse();
    }
    return ret;
  }
  int inner_release() override;

  int process_ivf_scan(bool is_vectorized) override;
  int process_ivf_scan_post() override;
  int process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized) override;
  int filter_pre_rowkey_batch(const ObIArray<std::pair<ObString, float *>> &nearest_cids,
                              bool is_vectorized,
                              int64_t batch_row_count,
                              IvfRowkeyHeap &rowkey_heap);
  int filter_rowkey_by_cid(const ObIArray<std::pair<ObString, float *>> &nearest_cids,
                                  bool is_vectorized,
                                  int64_t batch_row_count,
                                  IvfRowkeyHeap &rowkey_heap,
                                  bool &index_end);
  int parse_cid_vec_datum(
    ObIAllocator &allocator,
    int64_t cid_vec_column_count,
    const ObDASScanCtDef *cid_vec_ctdef,
    const int64_t rowkey_cnt,
    ObRowkey *&main_rowkey,
    ObArrayBinary *&com_key);
  int calc_nearest_limit_rowkeys_in_cids(
    const ObIArray<std::pair<ObString, float *>> &nearest_centers,
    float *search_vec);
  int get_pq_cid_vec_by_pq_cid(const ObString &pq_cid, float *&pq_cid_vec);
  int get_nearest_probe_centers(ObIArray<std::pair<ObString, float *>> &nearest_centers);
  int get_cid_from_pq_rowkey_cid_table(ObIAllocator &allocator, ObString &cid, ObArrayBinary *&pq_cids);
  int check_cid_exist(
    const ObIArray<std::pair<ObString, float *>> &dst_cids,
    const ObString &src_cid,
    float *&center_vec,
    bool &src_cid_exist);
  int calc_adc_distance(
    const ObString &cid,
    const ObArrayBinary &pq_center_ids,
    const ObIArray<IvfCidVecPair> &nearest_cids,
    IvfRowkeyHeap &rowkey_heap,
    int &push_count);
  int get_pq_cids_from_datum(
    ObIAllocator &allocator,
    const ObString& pq_cids_str,
    ObArrayBinary *&pq_cids);
  int calc_distance_between_pq_ids(
    const ObArrayBinary &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    double &distance);
  int get_rowkey_brute_post();
  int get_limit_rowkey_brute_pre(bool is_vectorized);

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
  ObCollectionArrayType *pq_ids_type_;
  // unlike vec_op_alloc_ do reset() in inner_resuse()
  // persist_alloc_ do reset() in inner_release()
  ObArenaAllocator persist_alloc_;
};

class ObDASIvfSQ8ScanIter : public ObDASIvfScanIter
{
public:
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
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

  int get_real_search_vec_u8(ObString &real_search_vec_u8);
  int process_ivf_scan_sq(bool is_vectorized);
  int process_ivf_scan_post_sq();

private:
  ObDASScanIter *sq_meta_iter_;
  ObTableScanParam sq_meta_scan_param_;
  bool sq_meta_iter_first_scan_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_IVF_SCAN_ITER_H_ */
