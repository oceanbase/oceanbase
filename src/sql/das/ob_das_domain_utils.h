/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_DAS_DOMAIN_UTILS_H
#define OCEANBASE_DAS_DOMAIN_UTILS_H

#include "lib/allocator/page_arena.h"
#include "share/datum/ob_datum.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "storage/fts/ob_fts_doc_word_iterator.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_ft_token_processor.h"
#include "storage/fts/ob_i_ft_parser.h"

namespace oceanbase
{
namespace sql
{

class ObFTIndexRowCache final
{
public:
static const int64_t ALL_EXPR_COUNT = 4;

static const int64_t FTS_INDEX_COL_CNT = 5;
static const int64_t FTS_DOC_WORD_COL_CNT = 5;
static ObObjDatumMapType FTS_INDEX_OBJ_TYPES[FTS_INDEX_COL_CNT];
static ObObjDatumMapType FTS_DOC_WORD_OBJ_TYPES[FTS_DOC_WORD_COL_CNT];
static ObExprOperatorType FTS_INDEX_EXPR_TYPE[FTS_INDEX_COL_CNT];
static ObExprOperatorType FTS_DOC_WORD_EXPR_TYPE[FTS_DOC_WORD_COL_CNT];

public:
  ObFTIndexRowCache() :
      is_inited_(false),
      is_token_doc_(true),
      ft_doc_token_need_sort_(false),
      is_buildin_parser_(false),
      need_sample_(false),
      token_iterator_(nullptr),
      chars_per_token_(0),
      row_pool_index_(0),
      fts_index_type_(ObFTSIndexType::OB_FTS_INDEX_TYPE_INVALID),
      datum_row_(),
      token_arr_(),
      helper_(),
      metadata_allocator_(),
      scratch_allocator_(),
      meta_(),
      ft_token_processor_(scratch_allocator_),
      all_ft_exprs_() { }

  ~ObFTIndexRowCache()
  {
    reset();
  }

  int init(const bool is_token_doc,
           const bool ft_doc_token_need_sort,
           const common::ObString &parser_name,
           const common::ObString &parser_properties,
           const common::ObIArray<ObExpr *> &all_ft_exprs,
           const common::ObIArray<ObExpr *> &other_exprs,
           const ObFTSIndexType fts_index_type);

  int segment(const common::ObObjMeta &ft_obj_meta, const ObDatum &doc_id, const common::ObString &fulltext);

  int get_next_row(blocksstable::ObDatumRow *&row);

  void reset();

  void reuse();

  OB_INLINE void set_need_sample(const bool need_sample) { need_sample_ = need_sample; }

  OB_INLINE ObExpr *get_doc_id_expr() const { return all_ft_exprs_.at(DOC_ID_EXPR_IDX); }

  OB_INLINE ObExpr *get_ft_expr() const { return all_ft_exprs_.at(FT_EXPR_IDX); }

  OB_INLINE ObExpr *get_token_count_expr() const { return all_ft_exprs_.at(TOKEN_CNT_EXPR_IDX); }

  OB_INLINE ObExpr *get_doc_length_expr() const { return all_ft_exprs_.at(DOC_LEN_EXPR_IDX); }

  OB_INLINE common::ObSEArray<ObExpr *, ALL_EXPR_COUNT> &get_all_exprs() { return all_ft_exprs_; }
  OB_INLINE common::ObSEArray<ObExpr *, ALL_EXPR_COUNT> &get_other_exprs() { return other_exprs_; }

  TO_STRING_KV(K_(is_inited), K_(is_token_doc), K_(is_buildin_parser), KP_(token_iterator),
               K_(chars_per_token), K_(row_pool_index), K_(helper), K_(parser_context), K_(meta),
               K(ft_token_map_.size()), K_(ft_token_processor), K(all_ft_exprs_.count()));

private:
  class ObFTTokenComparator
  {
  public:
    ObFTTokenComparator(ObDatumCmpFuncType cmp_func)
      : cmp_func_(cmp_func),
        sort_ret_(OB_SUCCESS) { }
    ~ObFTTokenComparator() = default;
    bool operator()(const ObFTTokenPair *l, const ObFTTokenPair *r)
    {
      int cmp_ret = 0;
      if (OB_UNLIKELY(OB_SUCCESS != sort_ret_)) {
      } else if (OB_SUCCESS != (sort_ret_ = cmp_func_(l->first.get_token(),
                                                      r->first.get_token(),
                                                      cmp_ret))) {
        // fail to compare token
      }
      return cmp_ret < 0;
    }
    OB_INLINE int get_sort_ret() const { return sort_ret_; }

  private:
    ObDatumCmpFuncType cmp_func_;
    int sort_ret_;
  };

private:
  int prepare_parser(const common::ObObjMeta &ft_obj_meta,
                     const char *fulltext,
                     const int64_t fulltext_len);

  int reuse_ft_token_map(const int64_t ft_token_bkt_cnt);

  /**
   * @brief estimate the average number of characters per token
   * @details the default value is 4 (different parsers or configurations may return different values)
   */
  int estimate_chars_per_token();

  int fetch_exprs(const common::ObIArray<ObExpr *> &all_ft_exprs,
                  const common::ObIArray<ObExpr *> &other_exprs);

private:
  static const int64_t  FT_TOKEN_ARR_CAPACITY = 64L * 1024L;
  static const uint64_t MIN_FT_TOKEN_BUCKET_COUNT = 4;
  static const uint64_t MAX_FT_TOKEN_BUCKET_COUNT = 2L * 1024L * 1024L;

  static const int64_t DOC_ID_COL_IDX = 0;
  static const int64_t TOKEN_COL_IDX = 1;
  static const int64_t TOKEN_CNT_COL_IDX = 2;
  static const int64_t DOC_LEN_COL_IDX = 3;
  static const int64_t TOKEN_POS_LIST_COL_IDX = 4;

  static const int64_t DOC_ID_EXPR_IDX = 0;
  static const int64_t FT_EXPR_IDX = 1;
  static const int64_t TOKEN_CNT_EXPR_IDX = 2;
  static const int64_t DOC_LEN_EXPR_IDX = 3;

private:
  bool is_inited_;
  bool is_token_doc_;
  bool ft_doc_token_need_sort_;
  bool is_buildin_parser_;
  bool need_sample_;
  plugin::ObITokenIterator *token_iterator_;
  int64_t chars_per_token_;
  int64_t row_pool_index_;
  ObFTSIndexType fts_index_type_;
  blocksstable::ObDatumRow datum_row_;
  // for doc token sort by token
  ObArray<const ObFTTokenPair *> token_arr_;
  storage::ObFTParseHelper helper_;
  common::ObArenaAllocator metadata_allocator_;
  common::ObArenaAllocator scratch_allocator_;
  plugin::ObFTParserParam parser_context_;
  common::ObObjMeta meta_;
  ObFTTokenMap ft_token_map_;
  ObFTTokenProcessor ft_token_processor_;
  common::ObSEArray<ObExpr *, ALL_EXPR_COUNT> all_ft_exprs_;
  common::ObSEArray<ObExpr *, ALL_EXPR_COUNT> other_exprs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTIndexRowCache);
};

enum class ObDomainDMLMode : uint8
{
  DOMAIN_DML_MODE_DEFAULT  = 0,
  DOMAIN_DML_MODE_FT_SCAN  = 1,
  DOMAIN_DML_MODE_MAX      = 2,
};

class ObFTDocWordInfo final
{
public:
  ObFTDocWordInfo()
    : table_id_(OB_INVALID_ID),
      doc_word_table_id_(OB_INVALID_ID),
      doc_word_ls_id_(),
      doc_word_tablet_id_(),
      snapshot_(),
      doc_word_schema_version_(),
      doc_word_found_(false)
  {}
  ~ObFTDocWordInfo() = default;

  int assign(const ObFTDocWordInfo &src)
  {
    int ret = OB_SUCCESS;
    table_id_ = src.table_id_;
    doc_word_table_id_ = src.doc_word_table_id_;
    doc_word_ls_id_ = src.doc_word_ls_id_;
    doc_word_tablet_id_ = src.doc_word_tablet_id_;
    doc_word_schema_version_ = src.doc_word_schema_version_;
    doc_word_found_ = src.doc_word_found_;

    if (OB_FAIL(snapshot_.assign(src.snapshot_))) {
      STORAGE_LOG(WARN, "failed to assign snapshot", K(ret));
    }
    return ret;
  }

  TO_STRING_KV(K_(table_id),
               K_(doc_word_table_id),
               K_(doc_word_ls_id),
               K_(doc_word_tablet_id),
               K_(snapshot),
               K_(doc_word_schema_version),
               K_(doc_word_found));
public:
  uint64_t table_id_;
  uint64_t doc_word_table_id_;
  share::ObLSID doc_word_ls_id_;
  common::ObTabletID doc_word_tablet_id_;
  transaction::ObTxReadSnapshot snapshot_;
  int64_t doc_word_schema_version_;
  bool doc_word_found_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTDocWordInfo);
};

class ObDomainDMLParam final
{
public:
  ObDomainDMLParam(
    common::ObIAllocator &allocator,
    const IntFixedArray *row_projector,
    ObDASWriteBuffer::Iterator &write_iter,
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef)
  : mode_(ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT),
    allocator_(allocator),
    row_projector_(row_projector),
    write_iter_(write_iter),
    das_ctdef_(das_ctdef),
    main_ctdef_(main_ctdef),
    ft_doc_word_info_(nullptr)
  {}
  ~ObDomainDMLParam() = default;

  TO_STRING_KV(K_(mode), KPC_(row_projector), KPC_(das_ctdef), KPC_(main_ctdef), KPC_(ft_doc_word_info));

public:
  ObDomainDMLMode mode_;
  common::ObIAllocator &allocator_;
  const IntFixedArray *row_projector_;
  ObDASWriteBuffer::Iterator &write_iter_;
  const ObDASDMLBaseCtDef *das_ctdef_;
  const ObDASDMLBaseCtDef *main_ctdef_;
  const ObFTDocWordInfo *ft_doc_word_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDomainDMLParam);
};

class ObDASDomainUtils final
{
public:

  static const uint64_t MVI_FULL_ROWKEY_THRESHOLD = 6;
  static const uint64_t MVI_ROWKEY_SIZE_THRESHOLD = 48;

  ObDASDomainUtils() = default;
  ~ObDASDomainUtils() = default;

  static int build_ft_doc_word_infos(
      const share::ObLSID &ls_id,
      const transaction::ObTxDesc *trans_desc,
      const transaction::ObTxReadSnapshot *snapshot,
      const common::ObIArray<const ObDASBaseCtDef *> &related_ctdef,
      const common::ObIArray<common::ObTabletID> &related_tablet_ids,
      const bool is_main_table_in_fts_ddl,
      common::ObIArray<ObFTDocWordInfo> &doc_word_infos);
  static int generate_spatial_index_rows(
      ObIAllocator &allocator,
      const ObDASDMLBaseCtDef &das_ctdef,
      const ObString &wkb_str,
      const IntFixedArray &row_projector,
      const ObDASWriteBuffer::DmlRow &dml_row,
      ObDomainIndexRow &spat_rows);
  static int generate_fulltext_word_rows(common::ObIAllocator &allocator,
                                         storage::ObFTParseHelper *helper,
                                         const common::ObObjMeta &ft_obj_meta,
                                         const ObDatum &doc_id_datum,
                                         const ObString &fulltext,
                                         const bool is_fts_index_aux,
                                         const bool need_pos_list,
                                         ObDomainIndexRow &word_rows);
  static int generate_multivalue_index_rows(
      ObIAllocator &allocator,
      const ObDASDMLBaseCtDef &das_ctdef,
      int64_t mvi_idx,
      int64_t mvi_arr_idx,
      const ObString &json_data,
      const IntFixedArray &row_projector,
      const ObDASWriteBuffer::DmlRow &dml_row,
      ObDomainIndexRow &domain_rows);
private:
  static int calc_save_rowkey_policy(
    ObIAllocator &allocator,
    const ObDASDMLBaseCtDef &das_ctdef,
    const IntFixedArray &row_projector,
    const ObDASWriteBuffer::DmlRow &dml_row,
    const int64_t record_cnt,
    bool& is_save_rowkey);

  static int get_pure_mutivalue_data(
    const ObString &json_str,
    const char*& data,
    int64_t& data_len,
    uint32_t& record_num);
};

class ObDomainDMLIterator
{
public:
  static const int64_t DEFAULT_DOMAIN_ROW_COUNT = 32;
  typedef common::ObSEArray<blocksstable::ObDatumRow*, DEFAULT_DOMAIN_ROW_COUNT> ObDomainIndexRow;

  static int create_domain_dml_iterator(
      const ObDomainDMLParam &param,
      ObDomainDMLIterator *&domain_iter);

public:
  virtual ~ObDomainDMLIterator();
  virtual void reset();
  virtual int change_domain_dml_mode(const ObDomainDMLMode &mode);
  virtual int rewind()
  {
    row_idx_ = 0;
    return common::OB_SUCCESS;
  }
  void set_ctdef(
      const ObDASDMLBaseCtDef *das_ctdef,
      const IntFixedArray *row_projector,
      const ObDomainDMLMode &mode);
  void set_row_projector(const IntFixedArray *row_projector) { row_projector_ = row_projector; }
  int get_next_domain_row(blocksstable::ObDatumRow *&row);
  int get_next_domain_rows(blocksstable::ObDatumRow *&row, int64_t &row_count);
  bool is_same_domain_type(const ObDASDMLBaseCtDef *das_ctdef) const;

  TO_STRING_KV(K_(row_idx), K_(rows), KPC_(row_projector), KPC_(das_ctdef), K_(main_ctdef));
protected:
  ObDomainDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef);
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) = 0;

  virtual int check_sync_interval(bool &is_sync_interval) const { UNUSED(is_sync_interval); return OB_SUCCESS; }

protected:
  ObDomainDMLMode mode_;
  uint32_t row_idx_;
  // every element holds a pointer to in a sequential array
  ObDomainIndexRow rows_;
  const IntFixedArray *row_projector_;
  ObDASWriteBuffer::Iterator &write_iter_;
  const ObDASDMLBaseCtDef *das_ctdef_;
  const ObDASDMLBaseCtDef *main_ctdef_;
  common::ObArenaAllocator allocator_;
  bool is_update_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDomainDMLIterator);
};

class ObSpatialDMLIterator final : public ObDomainDMLIterator
{
public:
  ObSpatialDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef)
  {}
  virtual ~ObSpatialDMLIterator() = default;

private:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int get_geo_wkb(
      const ObChunkDatumStore::StoredRow *store_row,
      int64_t &geo_idx,
      ObString &geo_wkb,
      ObObjMeta &geo_meta) const;
  int get_geo_wkb_for_update(
      const ObChunkDatumStore::StoredRow *store_row,
      int64_t &geo_idx,
      ObString &geo_wkb,
      ObObjMeta &geo_meta) const;
};

class ObMultivalueDMLIterator final : public ObDomainDMLIterator
{
public:
  ObMultivalueDMLIterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef)
  {}
  virtual ~ObMultivalueDMLIterator() = default;

private:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int get_multivlaue_json_data(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t& multivalue_idx,
    int64_t& multivalue_arr_idx,
    ObString &multivalue_data);

  int get_multivlaue_json_data_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t& multivalue_idx,
    int64_t& multivalue_arr_idx,
    ObString &multivalue_data);
};

class ObFTDMLIterator final : public ObDomainDMLIterator
{
public:
  ObFTDMLIterator(
    const ObDomainDMLMode &mode,
    const ObFTDocWordInfo *ft_doc_word_info,
    common::ObIAllocator &allocator,
    const IntFixedArray *row_projector,
    ObDASWriteBuffer::Iterator &write_iter,
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef)
  : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef),
    doc_word_info_(ft_doc_word_info),
    ft_doc_word_iter_(),
    ft_parse_helper_(),
    is_inited_(false)
  {
    ObDomainDMLIterator::mode_ = mode;
  }
  virtual ~ObFTDMLIterator() = default;

  virtual void reset() override;
  void set_ft_doc_word_info(const ObFTDocWordInfo *ft_doc_word_info) { doc_word_info_ = ft_doc_word_info; }
  virtual int rewind() override;
  virtual int change_domain_dml_mode(const ObDomainDMLMode &mode) override;
  int init(const common::ObString &parser_name,
           const common::ObString &parser_property);

  INHERIT_TO_STRING_KV("ObDomainDMLIterator", ObDomainDMLIterator, K_(ft_parse_helper), K_(is_inited));
protected:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int get_ft_and_doc_id(const ObChunkDatumStore::StoredRow *store_row,
                        ObDatum &doc_id_datum,
                        ObString &ft,
                        common::ObObjMeta &ft_meta);
  int get_ft_and_doc_id_for_update(const ObChunkDatumStore::StoredRow *store_row,
                                   ObDatum &doc_id_datum,
                                   ObString &ft,
                                   common::ObObjMeta &ft_meta);
  int generate_ft_word_rows(const ObChunkDatumStore::StoredRow *store_row);
  int scan_ft_word_rows(const ObChunkDatumStore::StoredRow *store_row);
  int build_ft_word_row(blocksstable::ObDatumRow *src_row, blocksstable::ObDatumRow *&dest_row);

private:
  const ObFTDocWordInfo *doc_word_info_;
  storage::ObFTDocWordScanIterator ft_doc_word_iter_;
  storage::ObFTParseHelper ft_parse_helper_;
  bool is_inited_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_DAS_DOMAIN_UTILS_H
