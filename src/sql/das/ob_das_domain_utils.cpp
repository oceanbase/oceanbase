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

#include "lib/container/ob_se_array.h"
#include "share/ob_errno.h"
#include "share/ob_fts_pos_list_codec.h"
#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_domain_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObObjDatumMapType ObFTIndexRowCache::FTS_INDEX_OBJ_TYPES[] = {OBJ_DATUM_STRING, OBJ_DATUM_STRING, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_STRING};
ObObjDatumMapType ObFTIndexRowCache::FTS_DOC_WORD_OBJ_TYPES[] = {OBJ_DATUM_STRING, OBJ_DATUM_STRING, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_STRING};

ObExprOperatorType ObFTIndexRowCache::FTS_INDEX_EXPR_TYPE[] = {T_FUN_SYS_WORD_SEGMENT, T_FUN_SYS_DOC_ID, T_FUN_SYS_WORD_COUNT, T_FUN_SYS_DOC_LENGTH, T_FUN_SYS_POS_LIST};
ObExprOperatorType ObFTIndexRowCache::FTS_DOC_WORD_EXPR_TYPE[] = {T_FUN_SYS_DOC_ID, T_FUN_SYS_WORD_SEGMENT, T_FUN_SYS_WORD_COUNT, T_FUN_SYS_DOC_LENGTH, T_FUN_SYS_POS_LIST};

/**
* -----------------------------------ObFTIndexRowCache-----------------------------------
*/
/* public func start */

int ObFTIndexRowCache::init(
    const bool is_token_doc,
    const bool ft_doc_token_need_sort,
    const common::ObString &parser_name,
    const common::ObString &parser_properties,
    const common::ObIArray<ObExpr *> &all_ft_exprs,
    const common::ObIArray<ObExpr *> &other_exprs,
    const ObFTSIndexType fts_index_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ft index row cache has been initialized", K(ret));
  } else if (OB_FAIL(fetch_exprs(all_ft_exprs, other_exprs))) {
    LOG_WARN("fail to fetch exprs", K(ret));
  } else if (OB_FAIL(estimate_chars_per_token())) {
    LOG_WARN("fail to estimate chars per token", K(ret));
  } else if (OB_FAIL(helper_.init(&metadata_allocator_/*not used*/,
                                  parser_name,
                                  parser_properties,
                                  fts_index_type))) {
    LOG_WARN("fail to initialize ft parser helper", K(ret));
  } else if (OB_FAIL(ft_token_processor_.init(helper_.get_parser_property(),
                                              get_ft_expr()->obj_meta_,
                                              helper_.get_process_token_flags(),
                                              &ft_token_map_))) {
    LOG_WARN("fail to initialize ft token processor", K(ret));
  } else {
    const int64_t tenant_id = MTL_ID() == OB_INVALID_TENANT_ID ? OB_SYS_TENANT_ID : MTL_ID();
    is_token_doc_ = is_token_doc;
    ft_doc_token_need_sort_ = ft_doc_token_need_sort;
    is_buildin_parser_ = helper_.is_builtin_parser();
    metadata_allocator_.set_attr(common::ObMemAttr(tenant_id, "ft_seg_metadata"));
    scratch_allocator_.set_attr(common::ObMemAttr(tenant_id, "ft_segment_data"));
    token_arr_.set_attr(common::ObMemAttr(tenant_id, "ft_token_arr"));
    token_arr_.set_block_size(sizeof(const ObFTTokenPair *) * FT_TOKEN_ARR_CAPACITY);

    const ObCharsetInfo *cs = nullptr;
    const ObCollationType coll_type = get_ft_expr()->obj_meta_.get_collation_type();
    const bool need_pos_list = (fts_index_type == share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH);
    const int64_t col_cnt = ObFTSConstants::get_fts_index_col_cnt(is_token_doc, need_pos_list);
    if (OB_FAIL(datum_row_.init(metadata_allocator_, col_cnt))) {
      LOG_WARN("fail to init datum row", K(ret));
    } else if (OB_FAIL(token_arr_.reserve(FT_TOKEN_ARR_CAPACITY))) {
      LOG_WARN("fail to reserve token array", K(ret));
    } else if (OB_UNLIKELY(nullptr == (cs = static_cast<const ObCharsetInfo *>(ObCharset::get_charset(coll_type))))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported charset or collation", K(ret), K(coll_type));
    } else {
      const ObFTParserProperty &property = helper_.get_parser_property();
      parser_context_.scratch_alloc_ = &scratch_allocator_;
      parser_context_.metadata_alloc_ = is_buildin_parser_ ? &metadata_allocator_ : &scratch_allocator_;
      parser_context_.cs_ = cs;
      parser_context_.fulltext_ = nullptr;
      parser_context_.ft_length_ = 0;
      parser_context_.parser_version_ = helper_.get_parser_name().get_parser_version();
      parser_context_.plugin_param_ = helper_.get_plugin_param();
      parser_context_.ngram_token_size_ = property.ngram_token_size_;
      parser_context_.ik_param_.mode_
          = (property.ik_mode_smart_ ? plugin::ObFTIKParam::Mode::SMART : plugin::ObFTIKParam::Mode::MAX_WORD);
          parser_context_.min_ngram_size_ = property.min_ngram_token_size_;
          parser_context_.max_ngram_size_ = property.max_ngram_token_size_;
    }

  }
  if (OB_SUCC(ret)) {
    fts_index_type_ = fts_index_type;
    is_inited_ = true;
  }
  return ret;
}

int ObFTIndexRowCache::segment(const common::ObObjMeta &ft_obj_meta,
                               const ObDatum &doc_id_datum,
                               const ObString &fulltext)
{
  int ret = OB_SUCCESS;
  ObString regularized_ft_str;
  const bool need_tolower_ft = helper_.get_process_token_flags().casedown_token();
  const int64_t ft_token_bkt_cnt = MIN(MAX(fulltext.length() / chars_per_token_, MIN_FT_TOKEN_BUCKET_COUNT), MAX_FT_TOKEN_BUCKET_COUNT);
  reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ft index row cache hasn't be initialized", K(ret));
  } else if (OB_UNLIKELY(fulltext.empty())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(reuse_ft_token_map(ft_token_bkt_cnt))) {
    LOG_WARN("fail to reuse ft token map", K(ret));
  } else if (need_tolower_ft && OB_FAIL(ObCharset::tolower(ft_obj_meta.get_collation_type(),
                                                           fulltext, regularized_ft_str,
                                                           scratch_allocator_))) {
    LOG_WARN("fail to tolower fulltext", K(ret));
  } else if (need_tolower_ft && OB_UNLIKELY(regularized_ft_str.empty())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prepare_parser(ft_obj_meta,
                                    need_tolower_ft ? regularized_ft_str.ptr() : fulltext.ptr(),
                                    need_tolower_ft ? regularized_ft_str.length() : fulltext.length()))) {
    LOG_WARN("fail to prepare parser", K(ret));
  } else {
    const char *token = nullptr;
    int64_t token_len = 0;
    int64_t char_cnt = 0;
    int64_t token_freq = 0;
    int64_t doc_length = 0;
    int simple_pos = 0;
    const bool need_pos_list = (fts_index_type_ == share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(token_iterator_->get_next_token(token, token_len, char_cnt, token_freq))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
        } else {
          LOG_WARN("fail to get next token", K(ret), KPC(token_iterator_));
        }
      } else if (OB_FAIL(ft_token_processor_.process_token(need_pos_list, token, token_len, char_cnt, simple_pos++))) {
        LOG_WARN("fail to process one word", K(ret), KP(token), K(token_len), K(char_cnt));
      }
    }
    if (OB_LIKELY(OB_ITER_END == ret)) {
      doc_length = ft_token_processor_.get_non_stop_token_count();
      if (OB_LIKELY(ft_token_map_.size() > 0)) {
        // has output tokens
        ret = OB_SUCCESS;
        datum_row_.storage_datums_[DOC_ID_COL_IDX].shallow_copy_from_datum(doc_id_datum);
        datum_row_.storage_datums_[DOC_LEN_COL_IDX].set_uint(doc_length);
      }
    }
  }

  for (ObFTTokenMap::const_iterator iter = ft_token_map_.begin();
        OB_SUCC(ret) && iter != ft_token_map_.end(); ++iter) {
    if (OB_FAIL(token_arr_.push_back(iter.operator->()))) {
      LOG_WARN("fail to push back token pair", K(ret));
    }
  }

  if (OB_SUCC(ret) && ft_doc_token_need_sort_) {
    ObDatumCmpFuncType cmp_func = get_datum_cmp_func(ft_obj_meta, ft_obj_meta);
    if (OB_ISNULL(cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get cmp func", K(ret));
    } else {
      int sort_ret = OB_SUCCESS;
      ObFTTokenComparator token_cmp(cmp_func);
      lib::ob_sort(token_arr_.begin(), token_arr_.end(), token_cmp);
      if (OB_FAIL(token_cmp.get_sort_ret())) {
        LOG_WARN("fail to sort ft token", K(ret));
      }
    }
  }
  LOG_TRACE("finish to segment fulltext", K(ret), K(doc_id_datum), K(fulltext));
  return ret;
}

int ObFTIndexRowCache::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTIndexRowCache hasn't be initialized", K(ret), K(is_inited_));
  } else if (row_pool_index_ >= token_arr_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObFTTokenPair *token_pair = token_arr_.at(row_pool_index_);
    ++row_pool_index_;
    datum_row_.storage_datums_[TOKEN_COL_IDX].set_datum(token_pair->first.get_token());
    datum_row_.storage_datums_[TOKEN_CNT_COL_IDX].set_uint(token_pair->second.count_);
    if (fts_index_type_ == share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH) {
      ObFTSPositionListStore pos_list_store;
      char *store_buf = nullptr;
      int64_t store_buf_len = 0;
      if (OB_FAIL(pos_list_store.encode_and_serialize(
                                 scratch_allocator_,
                                 *token_pair->second.position_list_,
                                 store_buf,
                                 store_buf_len))) {
        LOG_WARN("fail to encode and serialize pos list", K(ret), K(token_pair->second.position_list_));
      } else {
        ObString pos_list_str(store_buf_len, store_buf);
        datum_row_.storage_datums_[TOKEN_POS_LIST_COL_IDX].set_string(pos_list_str);
      }
    }
    if (OB_SUCC(ret)) {
      row = &datum_row_;
    }
  }
  LOG_TRACE("get next row from ft index row cache", K(ret), KPC(row));
  return ret;
}

void ObFTIndexRowCache::reset()
{
  is_inited_ = false;
  is_token_doc_ = false;
  ft_doc_token_need_sort_ = false;
  is_buildin_parser_ = false;
  need_sample_ = false;
  row_pool_index_ = 0;
  fts_index_type_ = ObFTSIndexType::OB_FTS_INDEX_TYPE_INVALID;
  if (OB_UNLIKELY(nullptr != token_iterator_ &&
                  nullptr != helper_.get_parser_desc() &&
                  nullptr != parser_context_.metadata_alloc_)) {
    helper_.get_parser_desc()->free_token_iter(&parser_context_, token_iterator_);
  }
  token_iterator_ = nullptr;
  chars_per_token_ = 0;
  datum_row_.reset();
  token_arr_.reset();
  helper_.reset();
  meta_.reset();
  ft_token_processor_.reset();
  scratch_allocator_.reset();
  metadata_allocator_.reset();
  ft_token_map_.destroy();
  all_ft_exprs_.reset();
  other_exprs_.reset();
}

void ObFTIndexRowCache::reuse()
{
  row_pool_index_ = 0;
  token_arr_.reuse();
  ft_token_processor_.reuse();
  scratch_allocator_.reset_remain_one_page();
}

/* private func start */
int ObFTIndexRowCache::prepare_parser(const common::ObObjMeta &ft_obj_meta,
                                      const char *fulltext,
                                      const int64_t fulltext_len)
{
  int ret = OB_SUCCESS;
  const int64_t tokenized_ft_len = need_sample_ ? MIN(fulltext_len, 256L) : fulltext_len;
  if (is_buildin_parser_) {
    if (OB_LIKELY(nullptr != token_iterator_)) {
      if (OB_FAIL(static_cast<ObIFTParser *>(token_iterator_)->reuse_parser(fulltext, tokenized_ft_len))) {
        LOG_WARN("fail to reuse ft parser", K(ret));
      }
    } else {
      parser_context_.fulltext_ = fulltext;
      parser_context_.ft_length_ = tokenized_ft_len;
      if (OB_FAIL(helper_.get_parser_desc()->segment(&parser_context_, token_iterator_))) {
        LOG_WARN("fail to generate token iterator", K(ret), K(parser_context_));
      }
    }
  } else {
    parser_context_.fulltext_ = fulltext;
    parser_context_.ft_length_ = tokenized_ft_len;
    helper_.get_parser_desc()->free_token_iter(&parser_context_, token_iterator_);
    if (OB_FAIL(helper_.get_parser_desc()->segment(&parser_context_, token_iterator_))) {
      LOG_WARN("fail to generate token iterator", K(ret), K(parser_context_));
    }
  }
  return ret;
}

inline int ObFTIndexRowCache::reuse_ft_token_map(const int64_t ft_token_bkt_cnt)
{
  int ret = OB_SUCCESS;
  ft_token_map_.destroy();
  if (OB_FAIL(ft_token_map_.create(ft_token_bkt_cnt, common::ObMemAttr(MTL_ID(), "ft_token_map")))) {
    LOG_WARN("fail to create ft token map", K(ret), K(ft_token_bkt_cnt));
  }
  return ret;
}

inline int ObFTIndexRowCache::estimate_chars_per_token()
{
  int ret = OB_SUCCESS;
  chars_per_token_ = 4;
  return ret;
}

int ObFTIndexRowCache::fetch_exprs(const common::ObIArray<ObExpr *> &all_ft_exprs,
                                   const common::ObIArray<ObExpr *> &other_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(all_ft_exprs.count() != ALL_EXPR_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exprs count is not equal to all expr count", K(ret), K(all_ft_exprs.count()));
  } else {
    const ObExprOperatorType *expr_types = ObFTIndexRowCache::FTS_DOC_WORD_EXPR_TYPE;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_ft_exprs.count(); ++i) {
      if (OB_ISNULL(all_ft_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is nullptr", K(ret), K(i));
      } else if (OB_FAIL(all_ft_exprs_.push_back(all_ft_exprs.at(i)))) {
        LOG_WARN("fail to push back all expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && (!(expr_types[DOC_ID_EXPR_IDX] == all_ft_exprs_.at(DOC_ID_EXPR_IDX)->type_ || T_REF_COLUMN == all_ft_exprs_.at(DOC_ID_EXPR_IDX)->type_)
        || expr_types[FT_EXPR_IDX] != all_ft_exprs_.at(FT_EXPR_IDX)->type_
        || expr_types[TOKEN_CNT_EXPR_IDX] != all_ft_exprs_.at(TOKEN_CNT_EXPR_IDX)->type_
        || expr_types[DOC_LEN_EXPR_IDX] != all_ft_exprs_.at(DOC_LEN_EXPR_IDX)->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr types are not expected", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other_exprs.count(); ++i) {
      if (OB_ISNULL(other_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is nullptr", K(ret), K(i));
      } else if (OB_FAIL(other_exprs_.push_back(other_exprs.at(i)))) {
        LOG_WARN("fail to push back all expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASDomainUtils::generate_spatial_index_rows(
    ObIAllocator &allocator,
    const ObDASDMLBaseCtDef &das_ctdef,
    const ObString &wkb_str,
    const IntFixedArray &row_projector,
    const ObDASWriteBuffer::DmlRow &dml_row,
    ObDomainIndexRow &spat_rows)
{
  int ret = OB_SUCCESS;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs_item = NULL;
  const ObSrsBoundsItem *srs_bound = NULL;
  uint32_t srid = UINT32_MAX;
  uint64_t rowkey_num = das_ctdef.table_param_.get_data_table().get_rowkey_column_num();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "S2Adapter"));

  if (lib::is_oracle_mode() && wkb_str.length() == 0) {
    // in oracle mode, ignore null value
  } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb_str, srid))) {
    LOG_WARN("failed to get srid", K(ret), K(wkb_str));
  } else if (srid != 0 &&
      OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("failed to get srs guard", K(ret), K(MTL_ID()), K(srid));
  } else if (srid != 0 &&
      OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
    LOG_WARN("failed to get srs item", K(ret), K(MTL_ID()), K(srid));
  } else if (((srid == 0) || !(srs_item->is_geographical_srs())) &&
              OB_FAIL(OTSRS_MGR->get_srs_bounds(srid, srs_item, srs_bound))) {
    LOG_WARN("failed to get srs bound", K(ret), K(srid));
  } else {
    ObS2Adapter s2object(&allocator, srid != 0 ? srs_item->is_geographical_srs() : false);
    ObSpatialMBR spa_mbr;
    ObS2Cellids cellids;
    char *mbr = nullptr;
    int64_t mbr_len = 0;
    void *rows_buf = nullptr;
    blocksstable::ObDatumRow *rows = nullptr;
    if (OB_FAIL(s2object.init(wkb_str, srs_bound))) {
      LOG_WARN("Init s2object failed", K(ret));
    } else if (OB_FAIL(s2object.get_cellids(cellids, false))) {
      LOG_WARN("Get cellids from s2object failed", K(ret));
    } else if (OB_FAIL(s2object.get_mbr(spa_mbr))) {
      LOG_WARN("Get mbr from s2object failed", K(ret));
    } else if (spa_mbr.is_empty()) {
      if (cellids.size() == 0) {
        LOG_DEBUG("it's might be empty geometry collection", K(wkb_str));
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid geometry", K(ret), K(wkb_str));
      }
    } else if (OB_ISNULL(mbr = reinterpret_cast<char *>(allocator.alloc(OB_DEFAULT_MBR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for spatial index row mbr", K(ret));
    } else if (OB_FAIL(spa_mbr.to_char(mbr, mbr_len))) {
      LOG_WARN("failed transform ObSpatialMBR to string", K(ret));
    } else if (OB_ISNULL(rows_buf = reinterpret_cast<char *>(allocator.alloc(cellids.size() * sizeof(blocksstable::ObDatumRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for spatial index rows buffer", K(ret));
    } else {
      rows = new (rows_buf) blocksstable::ObDatumRow[cellids.size()];
      int64_t cellid_col_idx = 0;
      int64_t mbr_col_idx = 1;
      for (uint64_t i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
        if (OB_FAIL(rows[i].init(allocator, rowkey_num))) {
          LOG_WARN("init datum row failed", K(ret), K(rowkey_num));
        } else {
          // 索引行[cellid_obj][mbr_obj][rowkey_obj]
          for(uint64_t j = 0; OB_SUCC(ret) && j < rowkey_num; j++) {
            const ObObjMeta &col_type = das_ctdef.column_types_.at(j);
            const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
            int64_t projector_idx = row_projector.at(j);
            if (FALSE_IT(rows[i].storage_datums_[j].shallow_copy_from_datum(dml_row.cells()[projector_idx]))) {
            } else if (rows[i].storage_datums_[j].is_null()) {
            } else if (OB_FAIL(ObDASUtils::reshape_datum_value(col_type, col_accuracy, false, allocator, rows[i].storage_datums_[j]))) {
              LOG_WARN("reshape storage value failed", K(ret), K(col_type), K(projector_idx), K(j));
            }
          }
          if (OB_SUCC(ret)) {
            rows[i].storage_datums_[cellid_col_idx].set_uint(cellids.at(i));
            ObString mbr_val(mbr_len, mbr);
            rows[i].storage_datums_[mbr_col_idx].set_string(mbr_val);
            // not set_collation_type(CS_TYPE_BINARY) and set_collation_level(CS_LEVEL_IMPLICIT)
            if (OB_FAIL(spat_rows.push_back(&rows[i]))) {
              LOG_WARN("failed to push back spatial index row", K(ret), K(rows[i]));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDASDomainUtils::build_ft_doc_word_infos(
    const share::ObLSID &ls_id,
    const transaction::ObTxDesc *trans_desc,
    const transaction::ObTxReadSnapshot *snapshot,
    const common::ObIArray<const ObDASBaseCtDef *> &related_ctdefs,
    const common::ObIArray<common::ObTabletID> &related_tablet_ids,
    const bool is_main_table_in_fts_ddl,
    common::ObIArray<ObFTDocWordInfo> &doc_word_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || OB_ISNULL(snapshot))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), KPC(snapshot));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs.count(); ++i) {
    ObFTDocWordInfo doc_word_info;
    const ObDASDMLBaseCtDef *related_ctdef = static_cast<const ObDASDMLBaseCtDef *>(related_ctdefs.at(i));
    if (OB_ISNULL(related_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, related ctdef is nullptr", K(ret), KP(related_ctdef), K(i), K(related_ctdefs));
    } else if (related_ctdef->table_param_.get_data_table().is_fts_doc_word_aux()) {
      doc_word_info.table_id_ = related_ctdef->table_param_.get_data_table().get_table_id();
      doc_word_info.doc_word_table_id_ = related_ctdef->table_param_.get_data_table().get_table_id();
      doc_word_info.doc_word_ls_id_ = ls_id;
      doc_word_info.doc_word_tablet_id_ = related_tablet_ids.at(i);
      doc_word_info.doc_word_schema_version_ = related_ctdef->table_param_.get_data_table().get_schema_version();
      doc_word_info.doc_word_found_ = true;
    } else if (related_ctdef->table_param_.get_data_table().is_fts_index_aux()) {
      doc_word_info.table_id_ = related_ctdef->table_param_.get_data_table().get_table_id();
      doc_word_info.doc_word_found_ = false;
      int nwrite = 0;
      const common::ObString &index_name = related_ctdef->table_param_.get_data_table().get_index_name();
      const int64_t buf_size = OB_MAX_TABLE_NAME_BUF_LENGTH;
      char buf[buf_size] = {0};
      if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s_fts_doc_word", index_name.length(), index_name.ptr()))) {
        LOG_WARN("fail to printf fts doc word name str", K(ret), K(index_name));
      }
      for (int64_t j = 0; OB_SUCC(ret) && !doc_word_info.doc_word_found_ && j < related_ctdefs.count(); ++j) {
        const ObDASDMLBaseCtDef *doc_word_related_ctdef = static_cast<const ObDASDMLBaseCtDef *>(related_ctdefs.at(j));
        if (OB_ISNULL(doc_word_related_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, related ctdef is nullptr", K(ret), KP(doc_word_related_ctdef), K(j), K(related_ctdefs));
        } else if (doc_word_related_ctdef->table_param_.get_data_table().is_fts_doc_word_aux()
            && 0 == doc_word_related_ctdef->table_param_.get_data_table().get_index_name().case_compare(buf)) {
          doc_word_info.doc_word_table_id_ = doc_word_related_ctdef->table_param_.get_data_table().get_table_id();
          doc_word_info.doc_word_ls_id_ = ls_id;
          doc_word_info.doc_word_tablet_id_ = related_tablet_ids.at(j);
          doc_word_info.doc_word_schema_version_ = doc_word_related_ctdef->table_param_.get_data_table().get_schema_version();
          doc_word_info.doc_word_found_ = true;
        }
      }
      if (OB_SUCC(ret) && !doc_word_info.doc_word_found_ && !is_main_table_in_fts_ddl) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, fts index hasn't found fts doc word aux table", K(ret), KPC(related_ctdef),
            K(doc_word_info), K(related_tablet_ids), K(related_ctdefs));
      }
    }
    if (FAILEDx(doc_word_info.snapshot_.assign(*snapshot))) {
      LOG_WARN("fail to assign snapshot", K(ret), K(i), KPC(related_ctdef), K(doc_word_info));
    } else if (OB_FAIL(doc_word_infos.push_back(doc_word_info))) {
      LOG_WARN("fail to push back doc word info", K(ret), K(i), KPC(related_ctdef), K(doc_word_info));
    } else if (OB_FAIL(doc_word_infos.at(doc_word_infos.count()-1).snapshot_.refresh_seq_no(trans_desc->get_seq_base()))) {
      LOG_WARN("fail to refresh seq no", K(ret), K(i), KPC(related_ctdef), K(doc_word_info));
    }
  }
  LOG_TRACE("build_ft_doc_word_infos", K(ret), K(ls_id), K(snapshot), K(doc_word_infos), K(related_ctdefs),
      K(related_tablet_ids));
  return ret;
}

/*static*/ int ObDASDomainUtils::generate_fulltext_word_rows(common::ObIAllocator &allocator,
                                                             storage::ObFTParseHelper *helper,
                                                             const common::ObObjMeta &ft_obj_meta,
                                                             const ObDatum &doc_id_datum,
                                                             const ObString &fulltext,
                                                             const bool is_fts_index_aux,
                                                             const bool need_pos_list,
                                                             ObDomainIndexRow &word_rows)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t FT_MAX_WORD_BUCKET = 65536;
  const int64_t ft_word_bkt_cnt = MIN(MAX(fulltext.length() / 10, 2), FT_MAX_WORD_BUCKET);
  int64_t doc_length = 0;
  ObFTTokenMap ft_token_map;
  void *rows_buf = nullptr;
  blocksstable::ObDatumRow *rows = nullptr;
  // Use different column count based on table type
  const int64_t col_cnt = ObFTSConstants::get_fts_index_col_cnt(is_fts_index_aux, need_pos_list);
  if (OB_ISNULL(helper) || OB_UNLIKELY(!ft_obj_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(ft_obj_meta), K(doc_id_datum));
  } else if (0 == fulltext.length()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ft_token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "ft_token_map")))) {
    LOG_WARN("fail to create ft word map", K(ret), K(ft_word_bkt_cnt));
  } else if (OB_FAIL(helper->segment(ft_obj_meta, fulltext.ptr(), fulltext.length(), doc_length, ft_token_map))) {
    LOG_WARN("fail to segment", K(ret), KPC(helper), K(ft_obj_meta), K(fulltext));
  } else if (0 == ft_token_map.size()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(rows_buf = reinterpret_cast<char *>(
                       allocator.alloc(ft_token_map.size() * sizeof(blocksstable::ObDatumRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for full text index rows buffer", K(ret));
  } else {
    int64_t i = 0;
    rows = new (rows_buf) blocksstable::ObDatumRow[ft_token_map.size()];
    for (ObFTTokenMap::const_iterator iter = ft_token_map.begin();
         OB_SUCC(ret) && iter != ft_token_map.end();
         ++iter) {
      if (OB_FAIL(rows[i].init(allocator, col_cnt))) {
        LOG_WARN("init datum row failed", K(ret), K(col_cnt), K(is_fts_index_aux));
      } else {
        const ObFTToken &ft_token = iter->first;
        const ObFTTokenInfo &token_info = iter->second;
        const int64_t word_cnt = token_info.count_;


        // index row format
        //  -    FTS_INDEX: [WORD], [DOC_ID], [WORD_COUNT], [DOC_LENGTH], [POS_LIST]
        //  - FTS_DOC_WORD: [DOC_ID], [WORD], [WORD_COUNT], [DOC_LENGTH], [POS_LIST]
        if (OB_SUCC(ret)) {
          const int64_t word_idx = is_fts_index_aux ? 0 : 1;
          const int64_t doc_id_idx = is_fts_index_aux ? 1 : 0;
          const int64_t word_cnt_idx = 2;
          const int64_t doc_len_idx = 3;
          const int64_t pos_list_idx = 4;
          rows[i].storage_datums_[word_idx].set_datum(ft_token.get_token());
          rows[i].storage_datums_[doc_id_idx].shallow_copy_from_datum(doc_id_datum);
          rows[i].storage_datums_[word_cnt_idx].set_uint(word_cnt);
          rows[i].storage_datums_[doc_len_idx].set_uint(doc_length);
          if (need_pos_list) {
            ObFTSPositionListStore pos_list_store;
            char *store_buf = nullptr;
            int64_t store_buf_len = 0;
            if (OB_FAIL(pos_list_store.encode_and_serialize(allocator, *token_info.position_list_, store_buf, store_buf_len))) {
              LOG_WARN("fail to encode and serialize pos list", K(ret), K(ft_token), K(token_info),
                KPC(token_info.position_list_));
            } else {
              ObString pos_list_str(store_buf_len, store_buf);
              rows[i].storage_datums_[pos_list_idx].set_string(pos_list_str);
            }
          }
          if (FAILEDx(word_rows.push_back(&rows[i]))) {
            LOG_WARN("fail to push back row", K(ret), K(rows[i]));
          } else {
            LOG_DEBUG("succeed to add word row", K(ret), K(is_fts_index_aux), K(ft_token.get_token()), K(word_cnt), K(i), K(rows[i]));
            ++i;
          }
        }
      }
    }
  }
  return ret;
}

int ObDASDomainUtils::get_pure_mutivalue_data(const ObString &json_str, const char*& data, int64_t& data_len, uint32_t& record_num)
{
  int ret = OB_SUCCESS;

  ObJsonBin bin(json_str.ptr(), json_str.length());

  if (OB_FAIL(bin.reset_iter())) {
    LOG_WARN("failed to parse binary.", K(ret), K(json_str));
  } else if (!ObJsonVerType::is_opaque_or_string(bin.json_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to parse binary.", K(ret), K(json_str));
  } else {
    data = bin.get_data();
    data_len = bin.get_data_length();

    record_num = *reinterpret_cast<const uint32_t*>(data);
  }

  return ret;
}

int ObDASDomainUtils::calc_save_rowkey_policy(
    ObIAllocator &allocator,
    const ObDASDMLBaseCtDef &das_ctdef,
    const IntFixedArray &row_projector,
    const ObDASWriteBuffer::DmlRow &dml_row,
    const int64_t record_cnt,
    bool& is_save_rowkey)
{
  int ret = OB_SUCCESS;
  is_save_rowkey = true;

  uint64_t column_num = das_ctdef.column_ids_.count();
  const int64_t data_table_rowkey_cnt = das_ctdef.table_param_.get_data_table().get_data_table_rowkey_column_num();
  const uint64_t multivalue_arr_col_id = das_ctdef.table_param_.get_data_table().get_multivalue_array_col_id();

  uint64_t mulvalue_column_start = 0;

  // -1 : doc id column
  uint64_t mulvalue_column_end = column_num - 1 - data_table_rowkey_cnt;

  if (mulvalue_column_end <= mulvalue_column_start) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to calc save rowkey policy.", K(ret), K(mulvalue_column_end), K(mulvalue_column_start));
  } else {

    ObObj *obj_arr = nullptr;
    if (OB_ISNULL(obj_arr = reinterpret_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * column_num)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for multivalue index row cells", K(ret));
    }

    uint32_t pure_data_size = 0;
    uint64_t rowkey_column_start = mulvalue_column_end;
    // -1 :  doc id column
    uint64_t rowkey_column_end = column_num - 1;

    for(uint64_t j = rowkey_column_start; OB_SUCC(ret) && j < rowkey_column_end; j++) {
      obj_arr[j].set_nop_value();
      const ObObjMeta &col_type = das_ctdef.column_types_.at(j);
      const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
      int64_t projector_idx = row_projector.at(j);
      if (das_ctdef.column_ids_.at(j) == multivalue_arr_col_id) {
         // do nothing
      } else if (OB_FAIL(dml_row.cells()[projector_idx].to_obj(obj_arr[j], col_type))) {
        LOG_WARN("stored row to new row obj failed", K(ret),
            K(dml_row.cells()[projector_idx]), K(col_type), K(projector_idx), K(j));
      } else {
        pure_data_size += obj_arr[j].get_serialize_size();
      }
    }

    if (OB_SUCC(ret)) {
      if (record_cnt < MVI_FULL_ROWKEY_THRESHOLD) {
        is_save_rowkey = true;
      } else if (pure_data_size > MVI_ROWKEY_SIZE_THRESHOLD) {
        is_save_rowkey = false;
      } else {
        is_save_rowkey = true;
      }
    }
  }


  return ret;
}

int ObDASDomainUtils::generate_multivalue_index_rows(ObIAllocator &allocator,
                                               const ObDASDMLBaseCtDef &das_ctdef,
                                               int64_t multivalue_idx,
                                               int64_t multivalue_arr_idx,
                                               const ObString &json_str,
                                               const IntFixedArray &row_projector,
                                               const ObDASWriteBuffer::DmlRow &dml_row,
                                               ObDomainIndexRow &mvi_rows)
{
  int ret = OB_SUCCESS;
  const char* data = nullptr;
  int64_t data_len = 0;
  uint32_t record_num = 0;
  bool is_unique_index = das_ctdef.table_param_.get_data_table().get_index_type() == ObIndexType::INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL;

  if (OB_FAIL(get_pure_mutivalue_data(json_str, data, data_len, record_num))) {
    LOG_WARN("failed to parse binary.", K(ret), K(json_str));
  } else if (record_num == 0 && is_unique_index) {
  } else {
    uint32_t real_record_num = (record_num == 0 && !is_unique_index) ? 1 : record_num;
    const int64_t data_table_rowkey_cnt = das_ctdef.table_param_.get_data_table().get_data_table_rowkey_column_num();
    uint64_t column_num = das_ctdef.column_ids_.count();
    // for normal multivalue index, column_ids: [multivalue_col, rowkeys, docid]
    // when doc id optimization is enabled, column_ids: [multivalue_col, pk_increment]
    bool use_docid = !(data_table_rowkey_cnt == 1 && column_num == 2);
    if (use_docid) {
      ObDocIDType doc_id_type = das_ctdef.table_param_.get_data_table().get_multivalue_doc_id_type();
      // for non-primary key heap table with cluster key, doc_id_type is HIDDEN_INC_PK, column_ids: [multivalue_col, rowkeys(pk_increment included)]
      if (doc_id_type == ObDocIDType::HIDDEN_INC_PK) {
        use_docid = false;
      }
    }

    void *rows_buf = nullptr;
    bool is_save_rowkey = true;
    if (OB_ISNULL(rows_buf = reinterpret_cast<char *>(allocator.alloc(real_record_num * sizeof(blocksstable::ObDatumRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for multi value index rows buffer", K(ret));
    } else if (use_docid && OB_FAIL(calc_save_rowkey_policy(allocator, das_ctdef, row_projector, dml_row, record_num, is_save_rowkey))) {
      LOG_WARN("failed to calc store policy.", K(ret), K(data_table_rowkey_cnt));
    } else {
      uint64_t rowkey_column_start = column_num - 1 - data_table_rowkey_cnt; // only used when use_docid
      uint64_t rowkey_column_end = column_num - 1;  // only used when use_docid
      bool is_none_unique_done = false;
      int64_t pos = sizeof(uint32_t);
      blocksstable::ObDatumRow *rows = new (rows_buf) blocksstable::ObDatumRow[real_record_num];

      for (int i = 0; OB_SUCC(ret) && (i < record_num || !is_none_unique_done) ; ++i) {
        if (OB_FAIL(rows[i].init(allocator, column_num))) {
          LOG_WARN("init datum row failed", K(ret), K(column_num));
        } else {
          for(uint64_t j = 0; OB_SUCC(ret) && j < column_num; j++) {
            ObObjMeta col_type = das_ctdef.column_types_.at(j);
            const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
            int64_t projector_idx = row_projector.at(j);

            if (multivalue_idx == projector_idx) {
              ObObj obj;
              obj.set_nop_value();
              if (OB_FAIL(obj.deserialize(data, data_len, pos))) {
                LOG_WARN("failed to deserialize datum", K(ret), K(json_str));
              } else {
                is_none_unique_done = true;
                if (ob_is_number_or_decimal_int_tc(col_type.get_type()) || ob_is_temporal_type(col_type.get_type())) {
                  col_type.set_collation_level(CS_LEVEL_NUMERIC);
                } else {
                  col_type.set_collation_level(CS_LEVEL_IMPLICIT);
                }
                if (!obj.is_null()) {
                  obj.set_meta_type(col_type);
                }
                if (OB_FAIL(rows[i].storage_datums_[j].from_obj_enhance(obj))) {
                  LOG_WARN("failed to convert datum from obj", K(ret), K(obj));
                }
              }
            } else if (!is_save_rowkey && (j >= rowkey_column_start && j < rowkey_column_end)) {
              rows[i].storage_datums_[j].set_null();
            } else if (multivalue_arr_idx == projector_idx) {
              rows[i].storage_datums_[j].set_null();
            } else {
              rows[i].storage_datums_[j].shallow_copy_from_datum(dml_row.cells()[projector_idx]);
            }

            if (rows[i].storage_datums_[j].is_null()) {  // do nothing
            } else if (OB_SUCC(ret) && OB_FAIL(ObDASUtils::reshape_datum_value(col_type, col_accuracy, false, allocator, rows[i].storage_datums_[j]))) {
              LOG_WARN("reshape storage value failed", K(ret), K(col_type), K(projector_idx), K(j));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(mvi_rows.push_back(&rows[i]))) {
              LOG_WARN("failed to push back spatial index row", K(ret), K(rows[i]));
            }
          } // end if (OB_SUCC(ret))
        }
      }
    }
  }

  return ret;
}

int ObDomainDMLIterator::create_domain_dml_iterator(
    const ObDomainDMLParam &param,
    ObDomainDMLIterator *&domain_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.row_projector_) || OB_ISNULL(param.das_ctdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(param.row_projector_), KP(param.das_ctdef_));
  } else if (param.das_ctdef_->table_param_.get_data_table().is_spatial_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObSpatialDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate spatial dml iterator memory", K(ret), KP(buf));
    } else {
      domain_iter = new (buf) ObSpatialDMLIterator(param.allocator_, param.row_projector_, param.write_iter_,
                                                   param.das_ctdef_, param.main_ctdef_);
    }
  } else if (param.das_ctdef_->table_param_.get_data_table().is_fts_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObFTDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate fulltext dml iterator memory", K(ret), KP(buf));
    } else {
      ObFTDMLIterator *iter = new (buf) ObFTDMLIterator(param.mode_, param.ft_doc_word_info_, param.allocator_,
                                                        param.row_projector_, param.write_iter_, param.das_ctdef_,
                                                        param.main_ctdef_);
      if (OB_FAIL(iter->init(param.das_ctdef_->table_param_.get_data_table().get_fts_parser_name(),
                             param.das_ctdef_->table_param_.get_data_table().get_fts_parser_property()))) {
        LOG_WARN("fail to init fulltext dml iterator", K(ret), KPC(iter));
      } else {
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    }
  } else if (param.das_ctdef_->table_param_.get_data_table().is_multivalue_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObMultivalueDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate multivalue dml iterator memory", K(ret), KP(buf));
    } else {
      ObMultivalueDMLIterator *iter = new (buf) ObMultivalueDMLIterator(param.allocator_, param.row_projector_,
                                                                        param.write_iter_, param.das_ctdef_,
                                                                        param.main_ctdef_);
      domain_iter = static_cast<ObDomainDMLIterator *>(iter);
    }
  } else if (param.das_ctdef_->table_param_.get_data_table().is_vector_index()) {
    if (param.das_ctdef_->table_param_.get_data_table().is_sparse_vector_index()) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObSparseVecIndexDMLIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate sparse vector idnex dml iterator memory", K(ret), KP(buf));
      } else {
        ObSparseVecIndexDMLIterator *iter = new (buf) ObSparseVecIndexDMLIterator(param.allocator_, param.row_projector_,
                                                                                  param.write_iter_, param.das_ctdef_,
                                                                                  param.main_ctdef_);
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    } else if (share::schema::is_hybrid_vec_index_log_type(param.das_ctdef_->table_param_.get_data_table().get_index_type())) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObHybridVecLogDMLIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate hybrid vec log dml iterator memory", K(ret), KP(buf));
      } else {
        ObHybridVecLogDMLIterator *iter = new (buf) ObHybridVecLogDMLIterator(param.allocator_, param.row_projector_,
                                                                              param.write_iter_, param.das_ctdef_,
                                                                              param.main_ctdef_);
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    } else if (share::schema::is_hybrid_vec_index_embedded_type(param.das_ctdef_->table_param_.get_data_table().get_index_type())) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObEmbeddedVecDMLIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate embedded vec dml iterator memory", K(ret), KP(buf));
      } else {
        ObEmbeddedVecDMLIterator *iter = new (buf) ObEmbeddedVecDMLIterator(param.allocator_, param.row_projector_,
                                                                            param.write_iter_, param.das_ctdef_,
                                                                            param.main_ctdef_);
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    } else {
      void *buf = nullptr;
      if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObVecIndexDMLIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate vector index dml iterator memory", K(ret), KP(buf));
      } else {
        ObVecIndexDMLIterator *iter = new (buf) ObVecIndexDMLIterator(param.allocator_, param.row_projector_,
                                                                      param.write_iter_, param.das_ctdef_,
                                                                      param.main_ctdef_);
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported domain index type", K(ret), K(param.das_ctdef_->table_param_.get_data_table()));
  }
  return ret;
}

ObDomainDMLIterator::ObDomainDMLIterator(
    common::ObIAllocator &allocator,
    const IntFixedArray *row_projector,
    ObDASWriteBuffer::Iterator &write_iter,
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef)
  : mode_(ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT),
    row_idx_(0),
    rows_(),
    row_projector_(row_projector),
    write_iter_(write_iter),
    das_ctdef_(das_ctdef),
    main_ctdef_(main_ctdef),
    allocator_(allocator),
    is_update_(nullptr == main_ctdef)
{
}

ObDomainDMLIterator::~ObDomainDMLIterator()
{
  reset();
}

void ObDomainDMLIterator::reset()
{
  mode_ = ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
  row_idx_ = 0;
  rows_.reset();
  row_projector_ = nullptr;
  das_ctdef_ = nullptr;
  main_ctdef_ = nullptr;
  allocator_.reset();
}

void ObDomainDMLIterator::set_ctdef(
    const ObDASDMLBaseCtDef *das_ctdef,
    const IntFixedArray *row_projector,
    const ObDomainDMLMode &mode)
{
  mode_ = mode;
  row_idx_ = 0;
  das_ctdef_ = das_ctdef;
  row_projector_ = row_projector;
}

bool ObDomainDMLIterator::is_same_domain_type(const ObDASDMLBaseCtDef *das_ctdef) const
{
  bool is_same_domain_type = false;
  if (OB_NOT_NULL(das_ctdef) && OB_NOT_NULL(das_ctdef_)) {
    const ObTableSchemaParam &table_param = das_ctdef->table_param_.get_data_table();
    const ObTableSchemaParam &my_table_param = das_ctdef_->table_param_.get_data_table();
    if ((table_param.is_fts_index() && my_table_param.is_fts_index())
        || (table_param.is_multivalue_index() && my_table_param.is_multivalue_index())) {
      is_same_domain_type = true;
    }
  }
  return is_same_domain_type;
}

int ObDomainDMLIterator::change_domain_dml_mode(const ObDomainDMLMode &mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mode < ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT || mode >= ObDomainDMLMode::DOMAIN_DML_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(mode));
  } else {
    mode_ = mode;
  }
  return ret;
}

int ObDomainDMLIterator::get_next_domain_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = nullptr;
  bool got_row = false;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("worker interrupt", K(ret));
  }
  while (OB_SUCC(ret) && !got_row) {
    if (row_idx_ >= rows_.count()) {
      rows_.reuse();
      allocator_.reuse();
      row_idx_ = 0;
      if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_domain_index())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, not domain index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
      } else if (FAILEDx(write_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(generate_domain_rows(sr))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to generate domain index row", K(ret), KPC(sr));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && row_idx_ < rows_.count()) {
      row = rows_[row_idx_];
      ++row_idx_;
      got_row = true;
    }
  }
  LOG_TRACE("get next domain row", K(ret), K(got_row), K(row_idx_), K(rows_), KPC(row), KPC(sr));
  return ret;
}

int ObDomainDMLIterator::get_next_domain_rows(blocksstable::ObDatumRow *&row, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = nullptr;
  bool got_row = false;
  if (!das_ctdef_->table_param_.get_data_table().is_fts_index()) { // batch only for fulltext
    if (OB_FAIL(get_next_domain_row(row))) {
      LOG_WARN("fail to get next domain row", K(ret));
    } else {
      row_count = 1;
    }
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("worker interrupt", K(ret));
  } else {
    row_count = 0;
    while (OB_SUCC(ret) && !got_row) {
      if (row_idx_ >= rows_.count()) {
        rows_.reuse();
        allocator_.reuse();
        row_idx_ = 0;
        if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_domain_index())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, not domain index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
        } else if (FAILEDx(write_iter_.get_next_row(sr))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from result iterator failed", K(ret));
          }
        } else if (OB_FAIL(generate_domain_rows(sr))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to generate domain index row", K(ret), KPC(sr));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_SUCC(ret) && row_idx_ < rows_.count()) {
        row = rows_[row_idx_];
        row_count = rows_.count() - row_idx_;
        row_idx_ = rows_.count();
        got_row = true;
      }
    }
    LOG_TRACE("get next domain rows", K(ret), K(got_row), K(row_idx_), K(row_count), K(rows_), KPC(row), KPC(sr));
  }
  return ret;
}

int ObSpatialDMLIterator::generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store_row));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_spatial_index())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, it isn't spatial index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
  } else {
    const uint64_t geo_col_id = das_ctdef_->table_param_.get_data_table().get_spatial_geo_col_id();
    int64_t geo_idx = -1;
    ObString geo_wkb;
    ObObjMeta geo_meta;
    if (!is_update_ && OB_FAIL(get_geo_wkb(store_row, geo_idx, geo_wkb, geo_meta))) {
      LOG_WARN("fail to get geo wkb", K(ret), KPC(store_row));
    } else if (is_update_ && OB_FAIL(get_geo_wkb_for_update(store_row, geo_idx, geo_wkb, geo_meta))) {
      LOG_WARN("fail to get geo wkb for update", K(ret), KPC(store_row));
    } else if (OB_UNLIKELY(geo_idx == -1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get geo col idx", K(ret), K(is_update_), K(geo_idx), K(geo_col_id));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_, geo_meta.get_type(),
                       geo_meta.get_collation_type(), is_update_ ? true : geo_meta.has_lob_header(), geo_wkb))) {
      LOG_WARN("fail to get real geo data", K(ret));
    } else if (OB_FAIL(ObDASDomainUtils::generate_spatial_index_rows(allocator_, *das_ctdef_, geo_wkb,
                                                              *row_projector_, *store_row, rows_))) {
      LOG_WARN("generate spatial_index_rows failed", K(ret), K(geo_col_id), K(geo_wkb), K(geo_idx),
          KPC(store_row), K(geo_meta));
    }
  }
  LOG_DEBUG("generate domain rows", K(ret), K(rows_), KPC(store_row));
  return ret;
}

int ObSpatialDMLIterator::get_geo_wkb(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t &geo_idx,
    ObString &geo_wkb,
    ObObjMeta &geo_meta) const
{
  int ret = OB_SUCCESS;
  const uint64_t rowkey_num = das_ctdef_->table_param_.get_data_table().get_rowkey_column_num();
  geo_idx = row_projector_->at(rowkey_num);
  if (geo_idx >= store_row->cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index for sr", K(ret), KPC(store_row), K(row_projector_));
  } else {
    geo_wkb = store_row->cells()[geo_idx].get_string();
    bool found = false;
    uint64_t geo_col_id = das_ctdef_->table_param_.get_data_table().get_spatial_geo_col_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < main_ctdef_->column_ids_.count() && !found; ++i) {
      if (geo_col_id == main_ctdef_->column_ids_.at(i)) {
        geo_meta = main_ctdef_->column_types_.at(i);
        found = true;
      }
    }
  }
  return ret;
}

int ObSpatialDMLIterator::get_geo_wkb_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t &geo_idx,
    ObString &geo_wkb,
    ObObjMeta &geo_meta) const
{
  int ret = OB_SUCCESS;
  const uint64_t rowkey_num = das_ctdef_->table_param_.get_data_table().get_rowkey_column_num();
  const uint64_t old_proj = das_ctdef_->old_row_projector_.count();
  const uint64_t new_proj = das_ctdef_->new_row_projector_.count();
  geo_idx = -1;
  if (OB_UNLIKELY(rowkey_num + 1 != old_proj || rowkey_num + 1 != new_proj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid project count", K(ret), K(rowkey_num), K(new_proj), K(old_proj));
  } else {
    // get full row successfully
    geo_idx = row_projector_->at(rowkey_num);
    geo_wkb = store_row->cells()[geo_idx].get_string();
    geo_meta.set_type(ObGeometryType);
    geo_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

void ObFTDMLIterator::reset()
{
  is_inited_ = false;
  ft_doc_word_iter_.reset();
  ft_parse_helper_.reset();
  ObDomainDMLIterator::reset();
}

int ObFTDMLIterator::rewind()
{
  int ret = OB_SUCCESS;
  share::schema::ObFTSIndexType fts_index_type = ObFTSIndexType::OB_FTS_INDEX_TYPE_INVALID;
  row_idx_ = 0;
  if (das_ctdef_->table_param_.get_data_table().is_fts_index()) {
    if (OB_FAIL(ObDomainDMLIterator::rewind())) {
      LOG_WARN("fail to ObDomainDMLIterator::rewind", K(ret));
    } else if (OB_ISNULL(das_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, das_ctdef is nullptr", K(ret), KP(das_ctdef_));
    } else if (FALSE_IT(fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type())) {
    } else if (ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT == mode_) {
      const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
      const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
      const share::schema::ObFTSIndexType fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type();
      bool is_same = false;
      if (OB_FAIL(ft_parse_helper_.check_is_the_same(parser_str, parser_property_str, fts_index_type, is_same))) {
        LOG_WARN("fail to check is the same", K(ret), K(parser_str), K(parser_property_str));
      } else if (is_same) {
        // This is the same as the parser name of the previous index.
        // nothing to do, just skip.
      } else if (FALSE_IT(ft_parse_helper_.reset())) {
      } else {
        if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_str, parser_property_str, fts_index_type))) {
          LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
        }
      }
    } else if (ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN == mode_) {
      if (OB_ISNULL(doc_word_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, doc word info is nullptr", K(ret), KPC(doc_word_info_));
      } else if (FALSE_IT(ft_doc_word_iter_.reset())) {
      } else if (OB_FAIL(ft_doc_word_iter_.init(doc_word_info_->doc_word_table_id_,
                                                doc_word_info_->doc_word_ls_id_,
                                                doc_word_info_->doc_word_tablet_id_,
                                                &doc_word_info_->snapshot_,
                                                doc_word_info_->doc_word_schema_version_,
                                                fts_index_type))) {
        LOG_WARN("fail to init doc word iter", K(ret), KPC(doc_word_info_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown domain dml mode", K(ret), K(mode_));
    }
  }
  return ret;
}

int ObFTDMLIterator::init(
    const common::ObString &parser_name,
    const common::ObString &parser_properties)
{
  int ret = OB_SUCCESS;
  share::schema::ObFTSIndexType fts_index_type = share::schema::OB_FTS_INDEX_TYPE_INVALID;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fulltext dml iterator twice", K(ret), K(is_inited_));
  } else {
    if (OB_NOT_NULL(das_ctdef_) && das_ctdef_->table_param_.get_data_table().is_fts_index()) {
      fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type();
    }
    switch (mode_) {
      case ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT: {
        if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_name, parser_properties, fts_index_type))) {
          LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_name), K(parser_properties));
        }
        break;
      }
      case ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN: {
        if (OB_ISNULL(doc_word_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, doc word info is nullptr", K(ret), KPC(doc_word_info_));
        } else if (OB_FAIL(ft_doc_word_iter_.init(doc_word_info_->doc_word_table_id_,
                                                  doc_word_info_->doc_word_ls_id_,
                                                  doc_word_info_->doc_word_tablet_id_,
                                                  &doc_word_info_->snapshot_,
                                                  doc_word_info_->doc_word_schema_version_,
                                                  fts_index_type))) {
          LOG_WARN("fail to init doc word iter", K(ret), KPC(doc_word_info_));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown domain dml mode", K(ret), K(mode_));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObFTDMLIterator::change_domain_dml_mode(const ObDomainDMLMode &mode)
{
  int ret = OB_SUCCESS;
  const ObDomainDMLMode old_mode = mode_;
  share::schema::ObFTSIndexType fts_index_type = ObFTSIndexType::OB_FTS_INDEX_TYPE_INVALID;
  if (OB_UNLIKELY(mode < ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT || mode >= ObDomainDMLMode::DOMAIN_DML_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid domain dml mode", K(ret), K(mode));
  } else if (OB_ISNULL(das_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, das ctdef is nullptr", K(ret), KPC(das_ctdef_));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_fts_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type", K(ret), K(das_ctdef_->table_param_));
  } else if (FALSE_IT(fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type())) {
  } else if (OB_FAIL(ObDomainDMLIterator::rewind())) {
    LOG_WARN("fail to ObDomainDMLIterator::rewind", K(ret));
  } else if (mode == mode_) {
    // nothing to do
  } else {
    switch (mode) {
      case ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT: {
        const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
        const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
        const share::schema::ObFTSIndexType fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type();
        bool is_same = false;
        if (OB_FAIL(ft_parse_helper_.check_is_the_same(parser_str, parser_property_str, fts_index_type, is_same))) {
          LOG_WARN("fail to check is the same", K(ret), K(parser_str), K(parser_property_str));
        } else if (is_same) {
          // This is the same as the parser name of the previous index.
          // nothing to do, just skip.
        } else if (FALSE_IT(ft_parse_helper_.reset())) {
        } else {
          if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_str, parser_property_str, fts_index_type))) {
            LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
          }
        }
        break;
      }
      case ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN: {
        if (OB_ISNULL(doc_word_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, doc word info is nullptr", K(ret), KPC(doc_word_info_));
        } else if (FALSE_IT(ft_doc_word_iter_.reset())) {
        } else if (OB_FAIL(ft_doc_word_iter_.init(doc_word_info_->doc_word_table_id_,
                                                  doc_word_info_->doc_word_ls_id_,
                                                  doc_word_info_->doc_word_tablet_id_,
                                                  &doc_word_info_->snapshot_,
                                                  doc_word_info_->doc_word_schema_version_,
                                                  fts_index_type))) {
          LOG_WARN("fail to init doc word iter", K(ret), KPC(doc_word_info_));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown domain dml mode", K(ret), K(mode_));
      }
    }
    if (OB_SUCC(ret)) {
      mode_ = mode;
    }
  }
  LOG_TRACE("change_domain_dml_mode", K(ret), K(old_mode), K(mode_), K(mode));
  return ret;
}

int ObFTDMLIterator::generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store_row));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_fts_index())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, it isn't fulltext index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
  } else {
    switch (mode_) {
      case ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT: {
        if (OB_FAIL(generate_ft_word_rows(store_row)) &&(ret != OB_ITER_END)) {
          LOG_WARN("fail to generate ft word rows", K(ret), KPC(store_row));
        }
        break;
      }
      case ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN: {
        if (OB_FAIL(scan_ft_word_rows(store_row))) {
          LOG_WARN("fail to scan ft word rows", K(ret), KPC(store_row));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown domain dml mode", K(ret), K(mode_));
      }
    }
  }
  STORAGE_FTS_LOG(TRACE, "generate domain rows", K(ret), K(rows_), KPC(store_row));
  return ret;
}

int ObFTDMLIterator::generate_ft_word_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  common::ObObjMeta ft_meta;
  ObString ft;
  ObDatum doc_id_datum;
  const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
  const bool need_pos_list = (ObFTSIndexType::OB_FTS_INDEX_TYPE_PHRASE_MATCH
      == das_ctdef_->table_param_.get_data_table().get_fts_index_type());

  if (!is_update_ && OB_FAIL(get_ft_and_doc_id(store_row, doc_id_datum, ft, ft_meta))) {
    LOG_WARN("fail to get fulltext and doc id", K(ret), KPC(store_row));
  } else if (is_update_ && OB_FAIL(get_ft_and_doc_id_for_update(store_row, doc_id_datum, ft, ft_meta))) {
    LOG_WARN("fail to get fulltext and doc id for update", K(ret), KPC(store_row));
  } else if (OB_FAIL(ObDASDomainUtils::generate_fulltext_word_rows(allocator_,
                                                                   &ft_parse_helper_,
                                                                   ft_meta,
                                                                   doc_id_datum,
                                                                   ft,
                                                                   is_fts_index_aux,
                                                                   need_pos_list,
                                                                   rows_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to generate fulltext word rows",
               K(ret),
               K(doc_id_datum),
               K(ft_parse_helper_),
               K(ft_meta),
               K(ft),
               KPC(store_row),
               K(is_fts_index_aux),
               K(rows_),
               KPC(main_ctdef_));
    }
  }
  return ret;
}

int ObFTDMLIterator::scan_ft_word_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  uint64_t doc_id_col_id = OB_INVALID_ID; // get to check
  ObDocIDType type;
  if (OB_FAIL(das_ctdef_->table_param_.get_data_table().get_typed_doc_id_col_id(doc_id_col_id, type))) {
    LOG_WARN("Failed to get doc id", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == doc_id_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id column id", K(ret), K(doc_id_col_id));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;

    ObDatum doc_id_datum = store_row->cells()[row_projector_->at(doc_id_idx)];

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ft_doc_word_iter_.do_scan(doc_word_info_->doc_word_table_id_, doc_id_datum))) {
      LOG_WARN("fail to do scan", K(ret), KPC(doc_word_info_), K(doc_id_datum));
    } else {
      // gather ft word rows
      ObDomainIndexRow tmp_rows;
      do {
        blocksstable::ObDatumRow *row = nullptr;
        blocksstable::ObDatumRow *ft_word_row = nullptr;
        if (OB_FAIL(ft_doc_word_iter_.get_next_row(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(build_ft_word_row(row, ft_word_row))) {
          LOG_WARN("fail to build ft word row", K(ret), KPC(row));
        } else if (OB_FAIL(tmp_rows.push_back(ft_word_row))) {
          LOG_WARN("fail push back ft word row", K(ret), KPC(ft_word_row));
        } else {
          LOG_TRACE("succeed to get one ft word from fts doc word", KPC(ft_word_row));
        }
      } while (OB_SUCC(ret));

      // make it sequential
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        blocksstable::ObDatumRow *row_array = nullptr;
        if (0 == tmp_rows.count()) {
          // do nothing
        } else if (OB_ISNULL(row_array = OB_NEW_ARRAY(blocksstable::ObDatumRow, &allocator_, tmp_rows.count()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < tmp_rows.count(); ++i) {
            if (OB_ISNULL(tmp_rows[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get row", K(ret));
            } else if (OB_FAIL(row_array[i].shallow_copy(*(tmp_rows[i])))) {
              LOG_WARN("fail to shallow copy row", K(ret));
            } else if (OB_FAIL(rows_.push_back(&row_array[i]))) {
              LOG_WARN("fail to push back row", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            LOG_TRACE("succeed to scan ft word rows", K(rows_.count()));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && GCONF.enable_strict_defensive_check()) {
    common::ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "FTIterDEF"));
    const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
    const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
    storage::ObFTParseHelper ft_parse_helper;
    ObDatum check_id_datum;
    ObString doc_id;
    ObString ft;
    common::ObObjMeta ft_meta;
    ObDomainIndexRow rows;
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const bool need_pos_list = (ObFTSIndexType::OB_FTS_INDEX_TYPE_PHRASE_MATCH
        == das_ctdef_->table_param_.get_data_table().get_fts_index_type());
    const share::schema::ObFTSIndexType fts_index_type = das_ctdef_->table_param_.get_data_table().get_fts_index_type();
    if (OB_FAIL(ft_parse_helper.init(&allocator, parser_str, parser_property_str, fts_index_type))) {
      LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
    }
    if (OB_SUCC(ret) && !is_update_ && OB_FAIL(get_ft_and_doc_id(store_row, check_id_datum, ft, ft_meta))) {
      LOG_WARN("fail to get fulltext and doc id", K(ret), KPC(store_row));
    } else if (is_update_ && OB_FAIL(get_ft_and_doc_id_for_update(store_row, check_id_datum, ft, ft_meta))) {
      LOG_WARN("fail to get fulltext and doc id for update", K(ret), KPC(store_row));
    } else if (OB_FAIL(ObDASDomainUtils::generate_fulltext_word_rows(allocator,
                                                                     &ft_parse_helper,
                                                                     ft_meta,
                                                                     check_id_datum,
                                                                     ft,
                                                                     is_fts_index_aux,
                                                                     need_pos_list,
                                                                     rows))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to generate fulltext word rows",
                 K(ret),
                 K(check_id_datum),
                 K(ft_parse_helper),
                 K(ft_meta),
                 K(ft),
                 KPC(store_row),
                 K(is_fts_index_aux),
                 K(rows),
                 KPC(main_ctdef_));
      }
    }
    if (OB_ITER_END == ret || OB_SUCC(ret)) {
      ret = OB_SUCCESS;
      if (OB_UNLIKELY(rows_.count() != rows.count())) {
        bool need_check_tx_active = true;
        if (OB_NOT_NULL(doc_word_info_) && doc_word_info_->snapshot_.tx_id().is_valid()) {
          const transaction::ObTransID &tx_id = doc_word_info_->snapshot_.tx_id();
          transaction::ObTransService *txs = MTL(transaction::ObTransService *);
          bool is_tx_active = true;
          if (OB_NOT_NULL(txs)) {
            if (OB_FAIL(txs->is_tx_active(tx_id, is_tx_active))) {
              LOG_WARN("fail to check if tx is active", K(ret), K(tx_id));
            } else if (!is_tx_active) {
              need_check_tx_active = false;
              LOG_WARN("tx is aborted, skip this check.", K(tx_id));
            }
          }
        }

        if (OB_SUCC(ret) && need_check_tx_active) {
          ret = OB_ERR_DEFENSIVE_CHECK;
          common::ObDocId docid;
          docid.from_string(doc_id);
          LOG_ERROR("row count isn't equal between scan ft words and generate ft words", K(ret), K(rows_), K(rows),
              K(is_fts_index_aux), K(doc_id), K(docid), K(ft_meta), K(ft), K(ft_parse_helper), KPC(store_row),
              KPC(main_ctdef_));
        }
      }
    }
  }
  return ret;
}

int ObFTDMLIterator::build_ft_word_row(
    blocksstable::ObDatumRow *src_row,
    blocksstable::ObDatumRow *&dest_row)
{
  int ret = OB_SUCCESS;

  const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
  const bool need_pos_list = (ObFTSIndexType::OB_FTS_INDEX_TYPE_PHRASE_MATCH
      == das_ctdef_->table_param_.get_data_table().get_fts_index_type());
  const int64_t DOC_ID_IDX = is_fts_index_aux ? 1 : 0;
  const int64_t WORD_SEGMENT_IDX = is_fts_index_aux ? 0 : 1;
  static constexpr int64_t WORD_COUNT_IDX = 2;
  static constexpr int64_t DOC_LENGTH_IDX = 3;
  static constexpr int64_t POS_LIST_IDX = 4; // if needed;
  const int64_t expected_src_col_cnt = need_pos_list ? 5 : 4;
  void *buf = nullptr;
  blocksstable::ObDatumRow *tmp_row = nullptr;
  if (OB_ISNULL(src_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(src_row));
  } else if (OB_UNLIKELY(expected_src_col_cnt != src_row->count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, count of src row isn't expected", K(ret),
             K(expected_src_col_cnt), K(src_row->count_), KPC(src_row));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObDatumRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate datum row", K(ret));
  } else if (FALSE_IT(tmp_row = new (buf) blocksstable::ObDatumRow())) {
  } else if (OB_FAIL(tmp_row->init(expected_src_col_cnt))) {
  } else if (OB_FAIL(tmp_row->copy_attributes_except_datums(*src_row))) {
    LOG_WARN("fail to copy attributes expcept datums", K(ret), KPC(src_row));
  } else if (OB_FAIL(tmp_row->storage_datums_[DOC_ID_IDX].deep_copy(src_row->storage_datums_[0], allocator_))) {
    LOG_WARN("fail to deep copy doc id datum", K(ret), K(DOC_ID_IDX));
  } else if (OB_FAIL(tmp_row->storage_datums_[WORD_SEGMENT_IDX].deep_copy(src_row->storage_datums_[1], allocator_))) {
    LOG_WARN("fail to deep copy word segment datum", K(ret), K(WORD_SEGMENT_IDX));
  } else if (OB_FAIL(tmp_row->storage_datums_[WORD_COUNT_IDX].deep_copy(src_row->storage_datums_[2], allocator_))) {
    LOG_WARN("fail to deep copy word count datum", K(ret), K(WORD_COUNT_IDX));
  } else if (OB_FAIL(tmp_row->storage_datums_[DOC_LENGTH_IDX].deep_copy(src_row->storage_datums_[3], allocator_))) {
    LOG_WARN("fail to deep copy doc lenght datum", K(ret), K(DOC_LENGTH_IDX));
  } else if (need_pos_list
             && OB_FAIL(tmp_row->storage_datums_[POS_LIST_IDX].deep_copy(src_row->storage_datums_[4], allocator_))) {
    LOG_WARN("fail to deep copy pos list datum", K(ret), K(POS_LIST_IDX));
  } else {
    dest_row = tmp_row;
  }
  return ret;
}

int ObFTDMLIterator::get_ft_and_doc_id(const ObChunkDatumStore::StoredRow *store_row,
                                       ObDatum &doc_id_datum,
                                       ObString &ft,
                                       common::ObObjMeta &ft_meta)
{
  int ret = OB_SUCCESS;
  const uint64_t fts_col_id = das_ctdef_->table_param_.get_data_table().get_fulltext_col_id();
  if (OB_UNLIKELY(OB_INVALID_ID == fts_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id or fulltext column id", K(ret), K(fts_col_id));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;
    const int64_t ft_idx = !is_fts_index_aux ? 1 : 0;

    ft = store_row->cells()[row_projector_->at(ft_idx)].get_string();
    ft_meta = das_ctdef_->column_types_.at(ft_idx);
    doc_id_datum = store_row->cells()[row_projector_->at(doc_id_idx)];

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                          ft_meta.get_type(),
                                                          ft_meta.get_collation_type(),
                                                          ft_meta.has_lob_header(),
                                                          ft))) {
      LOG_WARN("fail to get real text data", K(ret));
    } else {
      STORAGE_FTS_LOG(DEBUG, "succeed to get fulltext and doc id", K(doc_id_datum), K(ft_meta), K(ft));
    }
  }
  return ret;
}

int ObFTDMLIterator::get_ft_and_doc_id_for_update(const ObChunkDatumStore::StoredRow *store_row,
                                                  ObDatum &doc_id_datum,
                                                  ObString &ft,
                                                  common::ObObjMeta &ft_meta)
{
  int ret = OB_SUCCESS;
  const uint64_t rowkey_col_cnt = das_ctdef_->table_param_.get_data_table().get_rowkey_column_num();
  const uint64_t old_proj_cnt = das_ctdef_->old_row_projector_.count();
  const uint64_t new_proj_cnt = das_ctdef_->new_row_projector_.count();

  const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
  const bool need_pos_list = (ObFTSIndexType::OB_FTS_INDEX_TYPE_PHRASE_MATCH
      == das_ctdef_->table_param_.get_data_table().get_fts_index_type());

  const int64_t extra_ft_col_cnt = need_pos_list ? 3 : 2;

  if (OB_UNLIKELY(rowkey_col_cnt + extra_ft_col_cnt != old_proj_cnt || rowkey_col_cnt + extra_ft_col_cnt != new_proj_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid project count", K(ret), K(rowkey_col_cnt), K(old_proj_cnt), K(new_proj_cnt));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;
    const int64_t ft_idx = !is_fts_index_aux ? 1 : 0;

    doc_id_datum = store_row->cells()[row_projector_->at(doc_id_idx)];

    ft = store_row->cells()[row_projector_->at(ft_idx)].get_string();
    ft_meta = das_ctdef_->column_types_.at(ft_idx);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                                 ft_meta.get_type(),
                                                                 ft_meta.get_collation_type(),
                                                                 true /* has lob header */,
                                                                 ft))) {
      LOG_WARN("fail to get real text data", K(ret));
    } else {
      STORAGE_FTS_LOG(DEBUG, "succeed to get fulltext and doc id", K(doc_id_datum), K(ft_meta), K(ft));
    }
  }
  return ret;
}

int ObMultivalueDMLIterator::generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store_row));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_multivalue_index_aux())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, it isn't multivalue index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
  } else {
    int64_t multivalue_idx = OB_INVALID_ID;
    int64_t multivalue_arr_idx = OB_INVALID_ID;
    ObString multivalue_data;
    if (!is_update_ && OB_FAIL(get_multivlaue_json_data(
      store_row, multivalue_idx, multivalue_arr_idx, multivalue_data))) {
      LOG_WARN("fail to get json data.", K(ret), KPC(store_row));
    } else if (is_update_ && OB_FAIL(get_multivlaue_json_data_for_update(
      store_row, multivalue_idx, multivalue_arr_idx, multivalue_data))) {
      LOG_WARN("fail to get json data for update.", K(ret), KPC(store_row));
    } else if (OB_FAIL(ObDASDomainUtils::generate_multivalue_index_rows(
      allocator_, *das_ctdef_, multivalue_idx, multivalue_arr_idx,
      multivalue_data, *row_projector_, *store_row, rows_))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("generate multi value index_rows failed", K(ret), K(multivalue_idx),
          KPC(store_row), K(multivalue_data));
      }
    }
  }
  LOG_DEBUG("generate domain rows", K(ret), K(rows_), KPC(store_row));
  return ret;
}

int ObMultivalueDMLIterator::get_multivlaue_json_data(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t& multivalue_idx,
    int64_t& multivalue_arr_idx,
    ObString &multivalue_data)
{
  int ret = OB_SUCCESS;
  multivalue_idx = OB_INVALID_ID;

  const uint64_t multivalue_col_id = das_ctdef_->table_param_.get_data_table().get_multivalue_col_id();
  bool found = false;

  if (OB_INVALID_ID == multivalue_col_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id or multivalue column id", K(ret),
      K(multivalue_col_id), K(das_ctdef_->table_param_.get_data_table()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < das_ctdef_->column_ids_.count() && !found; ++i) {
      const int64_t column_id = das_ctdef_->column_ids_.at(i);
      const int64_t projector_idx = row_projector_->at(i);
      if (OB_UNLIKELY(projector_idx >= store_row->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index for sr", K(ret), KPC(store_row), K(i), K(main_ctdef_->column_ids_));
      } else if (multivalue_col_id == column_id) {
        found = true;
        multivalue_idx = projector_idx;
        multivalue_arr_idx = multivalue_idx + 1;
        multivalue_data = store_row->cells()[multivalue_arr_idx].get_string();

        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                            ObJsonType,
                                                            CS_TYPE_UTF8MB4_BIN,
                                                            true, multivalue_data))) {
          LOG_WARN("fail to get real json data", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't get multivalue col idx, or get doc id column", K(ret), K(multivalue_idx));
    }
  }

  return ret;
}

int ObMultivalueDMLIterator::get_multivlaue_json_data_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    int64_t& multivalue_idx,
    int64_t& multivalue_arr_idx,
    ObString &multivalue_data)
{
  int ret = OB_SUCCESS;
  bool found = false;

  multivalue_idx = OB_INVALID_ID;
  multivalue_arr_idx = OB_INVALID_ID;

  const uint64_t multivalue_col_id = das_ctdef_->table_param_.get_data_table().get_multivalue_col_id();

  for (int64_t i = 0; OB_SUCC(ret) && i < das_ctdef_->column_ids_.count() && !found; ++i) {
    const int64_t projector_idx = row_projector_->at(i);
    if (multivalue_col_id == das_ctdef_->column_ids_.at(i)) {
      if (projector_idx >= store_row->cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index for sr", K(ret), KPC(store_row), K(i), K(projector_idx));
      } else {
        found = true;
        multivalue_idx = projector_idx;

        if (projector_idx >= store_row->cnt_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index for sr", K(ret), KPC(store_row), K(i), K(projector_idx));
        } else {
          multivalue_arr_idx = projector_idx + 1;
          multivalue_data = store_row->cells()[multivalue_arr_idx].get_string();

          if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                              ObJsonType,
                                                              CS_TYPE_UTF8MB4_BIN,
                                                              true, multivalue_data))) {
            LOG_WARN("fail to get real json data", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't get multivalue col idx, or get doc id column", K(ret), K(multivalue_idx));
  }

  return ret;
}

} // end namespace storage
} // end namespace oceanbase
