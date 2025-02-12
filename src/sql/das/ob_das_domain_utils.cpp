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

#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_domain_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{


ObObjDatumMapType ObFTIndexRowCache::FTS_INDEX_TYPES[] = {OBJ_DATUM_STRING, OBJ_DATUM_STRING, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_8BYTE_DATA};
ObObjDatumMapType ObFTIndexRowCache::FTS_DOC_WORD_TYPES[] = {OBJ_DATUM_STRING, OBJ_DATUM_STRING, OBJ_DATUM_8BYTE_DATA, OBJ_DATUM_8BYTE_DATA};

ObExprOperatorType ObFTIndexRowCache::FTS_INDEX_EXPR_TYPE[] = {T_FUN_SYS_WORD_SEGMENT, T_FUN_SYS_DOC_ID, T_FUN_SYS_WORD_COUNT, T_FUN_SYS_DOC_LENGTH};
ObExprOperatorType ObFTIndexRowCache::FTS_DOC_WORD_EXPR_TYPE[] = {T_FUN_SYS_DOC_ID, T_FUN_SYS_WORD_SEGMENT, T_FUN_SYS_WORD_COUNT, T_FUN_SYS_DOC_LENGTH};

ObFTIndexRowCache::ObFTIndexRowCache()
  : rows_(),
    row_idx_(0),
    is_fts_index_aux_(true),
    helper_(),
    is_inited_(false)
{
}

ObFTIndexRowCache::~ObFTIndexRowCache()
{
  reset();
}

int ObFTIndexRowCache::init(
    const bool is_fts_index_aux,
    const common::ObString &parser_name,
    const common::ObString &parser_properties)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "DocIdMerge", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fulltext dml iterator twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_memctx_, param))) {
    LOG_WARN("failed to create merge memctx", K(ret));
  } else if (OB_FAIL(helper_.init(&(merge_memctx_->get_arena_allocator()), parser_name, parser_properties))) {
    LOG_WARN("fail to init full-text parser helper", K(ret));
  } else {
    row_idx_ = 0;
    is_fts_index_aux_ = is_fts_index_aux;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObFTIndexRowCache::segment(
    const common::ObObjMeta &ft_obj_meta,
    const ObString &doc_id,
    const ObString &fulltext)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTIndexRowCache hasn't be initialized", K(ret), K(is_inited_));
  } else if (FALSE_IT(reuse())) {
  } else if (OB_FAIL(ObDASDomainUtils::generate_fulltext_word_rows(merge_memctx_->get_arena_allocator(), &helper_,
          ft_obj_meta, doc_id, fulltext, is_fts_index_aux_, rows_))) {
    LOG_WARN("fail to generate fulltext word rows", K(ret), K(helper_), K(is_fts_index_aux_));
  } else {
    row_idx_ = 0;
  }
  LOG_TRACE("word segment", K(ret), K(row_idx_), K(rows_.count()), K(doc_id), K(fulltext));
  return ret;
}

int ObFTIndexRowCache::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTIndexRowCache hasn't be initialized", K(ret), K(is_inited_));
  } else if (row_idx_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else {
    row = (rows_[row_idx_]);
    ++row_idx_;
  }
  LOG_TRACE("get next row", K(ret), KPC(row), K(row_idx_), K(rows_.count()));
  return ret;
}

void ObFTIndexRowCache::reset()
{
  rows_.reset();
  row_idx_ = 0;
  is_fts_index_aux_ = true;
  helper_.reset();
  if (OB_NOT_NULL(merge_memctx_)) {
    DESTROY_CONTEXT(merge_memctx_);
    merge_memctx_ = nullptr;
  }
  is_inited_ = false;
}

void ObFTIndexRowCache::reuse()
{
  rows_.reuse();
  row_idx_ = 0;
  if (OB_NOT_NULL(merge_memctx_)) {
    merge_memctx_->reset_remain_one_page();
  }
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
            } else if (OB_FAIL(ObDASUtils::reshape_datum_value(col_type, col_accuracy, true, allocator, rows[i].storage_datums_[j]))) {
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
      doc_word_info.snapshot_ = snapshot;
    } else if (related_ctdef->table_param_.get_data_table().is_fts_index_aux()) {
      doc_word_info.table_id_ = related_ctdef->table_param_.get_data_table().get_table_id();
      doc_word_info.doc_word_found_ = false;
      doc_word_info.snapshot_ = snapshot;
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
    if (FAILEDx(doc_word_infos.push_back(doc_word_info))) {
      LOG_WARN("fail to push back doc word info", K(ret), K(i), KPC(related_ctdef), K(doc_word_info));
    }
  }
  LOG_TRACE("build_ft_doc_word_infos", K(ret), K(ls_id), K(snapshot), K(doc_word_infos), K(related_ctdefs),
      K(related_tablet_ids));
  return ret;
}

/*static*/ int ObDASDomainUtils::generate_fulltext_word_rows(
    common::ObIAllocator &allocator,
    storage::ObFTParseHelper *helper,
    const common::ObObjMeta &ft_obj_meta,
    const ObString &doc_id,
    const ObString &fulltext,
    const bool is_fts_index_aux,
    ObDomainIndexRow &word_rows)
{
  int ret = OB_SUCCESS;
  static int64_t FT_WORD_DOC_COL_CNT = 4;
  const int64_t ft_word_bkt_cnt = MAX(fulltext.length() / 10, 2);
  int64_t doc_length = 0;
  ObFTWordMap ft_word_map;
  void *rows_buf = nullptr;
  blocksstable::ObDatumRow *rows = nullptr;
  if (OB_ISNULL(helper)
      || OB_UNLIKELY(!ft_obj_meta.is_valid())
      || OB_UNLIKELY(doc_id.length() != sizeof(ObDocId) || doc_id.empty()) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(ft_obj_meta), K(doc_id));
  } else if (0 == fulltext.length()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ft_word_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
    LOG_WARN("fail to create ft word map", K(ret), K(ft_word_bkt_cnt));
  } else if (OB_FAIL(segment_and_calc_word_count(allocator, helper, ft_obj_meta.get_collation_type(),
          fulltext, doc_length, ft_word_map))) {
    LOG_WARN("fail to segment and calculate word count", K(ret), KPC(helper),
        K(ft_obj_meta.get_collation_type()), K(fulltext));
  } else if (0 == ft_word_map.size()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(rows_buf = reinterpret_cast<char *>(allocator.alloc(ft_word_map.size() * sizeof(blocksstable::ObDatumRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for full text index rows buffer", K(ret));
  } else {
    int64_t i = 0;
    rows = new (rows_buf) blocksstable::ObDatumRow[ft_word_map.size()];
    for (ObFTWordMap::const_iterator iter = ft_word_map.begin();
         OB_SUCC(ret) && iter != ft_word_map.end();
         ++iter) {
      if (OB_FAIL(rows[i].init(allocator, FT_WORD_DOC_COL_CNT))) {
        LOG_WARN("init datum row failed", K(ret), K(FT_WORD_DOC_COL_CNT));
      } else {
        const ObFTWord &ft_word = iter->first;
        const int64_t word_cnt = iter->second;
        // index row format
        //  -    FTS_INDEX: [WORD], [DOC_ID], [WORD_COUNT], [DOC_LENGTH]
        //  - FTS_DOC_WORD: [DOC_ID], [WORD], [WORD_COUNT], [DOC_LENGTH]
        const int64_t word_idx = is_fts_index_aux ? 0 : 1;
        const int64_t doc_id_idx = is_fts_index_aux ? 1 : 0;
        const int64_t word_cnt_idx = 2;
        const int64_t doc_len_idx = 3;
        rows[i].storage_datums_[word_idx].set_string(ft_word.get_word());
        rows[i].storage_datums_[doc_id_idx].set_string(doc_id);
        rows[i].storage_datums_[word_cnt_idx].set_uint(word_cnt);
        rows[i].storage_datums_[doc_len_idx].set_uint(doc_length);
        if (OB_FAIL(word_rows.push_back(&rows[i]))) {
          LOG_WARN("fail to push back row", K(ret), K(rows[i]));
        } else {
          ObDocId tmp_doc_id;
          tmp_doc_id.tablet_id_ = ((const uint64_t *)doc_id.ptr())[0];
          tmp_doc_id.seq_id_ = ((const uint64_t *)doc_id.ptr())[1];
          STORAGE_FTS_LOG(DEBUG, "succeed to add word row", K(ret), K(is_fts_index_aux), "doc_id", tmp_doc_id,
              K(ft_word), K(word_cnt), K(i), K(rows[i]));
          ++i;
        }
      }
    }
  }
  return ret;
}

/*static*/ int ObDASDomainUtils::segment_and_calc_word_count(
    common::ObIAllocator &allocator,
    storage::ObFTParseHelper *helper,
    const common::ObCollationType &type,
    const ObString &fulltext,
    int64_t &doc_length,
    ObFTWordMap &words_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(helper)
      || OB_UNLIKELY(ObCollationType::CS_TYPE_INVALID == type
                  || ObCollationType::CS_TYPE_PINYIN_BEGIN_MARK <= type)
      || OB_UNLIKELY(!words_count.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(type), K(words_count.created()));
  } else if (OB_FAIL(helper->segment(type, fulltext.ptr(), fulltext.length(), doc_length, words_count))) {
    LOG_WARN("fail to segment", K(ret), KPC(helper), K(type), K(fulltext));
  }

  STORAGE_FTS_LOG(TRACE, "segment and calc word count", K(ret), K(words_count.size()), K(type));
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
  bool is_save_rowkey = true;
  const int64_t index_rowkey_cnt = das_ctdef.table_param_.get_data_table().get_rowkey_column_num();
  bool is_unique_index = das_ctdef.table_param_.get_data_table().get_index_type() == ObIndexType::INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL;
  const int64_t data_table_rowkey_cnt = das_ctdef.table_param_.get_data_table().get_data_table_rowkey_column_num();
  const char* data = nullptr;
  int64_t data_len = 0;
  uint32_t record_num = 0;

  bool is_none_unique_done = false;
  uint64_t column_num = das_ctdef.column_ids_.count();
  // -1 : doc id column
  uint64_t rowkey_column_start = column_num - 1 - data_table_rowkey_cnt;
  uint64_t rowkey_column_end = column_num - 1;

  void *rows_buf = nullptr;
  if (OB_FAIL(get_pure_mutivalue_data(json_str, data, data_len, record_num))) {
    LOG_WARN("failed to parse binary.", K(ret), K(json_str));
  } else if (record_num == 0 && is_unique_index) {
  } else if (OB_FAIL(calc_save_rowkey_policy(allocator, das_ctdef, row_projector, dml_row, record_num, is_save_rowkey))) {
    LOG_WARN("failed to calc store policy.", K(ret), K(data_table_rowkey_cnt));
  } else {
    uint32_t real_record_num = record_num;
    if (record_num == 0 && !is_unique_index) {
      real_record_num = 1;
    }

    if (OB_ISNULL(rows_buf = reinterpret_cast<char *>(allocator.alloc(real_record_num * sizeof(blocksstable::ObDatumRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for multi value index rows buffer", K(ret));
    } else {
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
              // TODO: change obj to datum when do deserialize@xuanxi
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
            } else if (OB_SUCC(ret) && OB_FAIL(ObDASUtils::reshape_datum_value(col_type, col_accuracy, true, allocator, rows[i].storage_datums_[j]))) {
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
      LOG_WARN("fail to allocate fulltext dml iterator memory", K(ret), KP(buf));
    } else {
      ObMultivalueDMLIterator *iter = new (buf) ObMultivalueDMLIterator(param.allocator_, param.row_projector_,
                                                                        param.write_iter_, param.das_ctdef_,
                                                                        param.main_ctdef_);
      domain_iter = static_cast<ObDomainDMLIterator *>(iter);
    }
  } else if (param.das_ctdef_->table_param_.get_data_table().is_vector_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = param.allocator_.alloc(sizeof(ObVecIndexDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate fulltext dml iterator memory", K(ret), KP(buf));
    } else {
      ObVecIndexDMLIterator *iter = new (buf) ObVecIndexDMLIterator(param.allocator_, param.row_projector_,
                                                                    param.write_iter_, param.das_ctdef_,
                                                                    param.main_ctdef_);
      domain_iter = static_cast<ObDomainDMLIterator *>(iter);
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
  row_idx_ = 0;
  if (das_ctdef_->table_param_.get_data_table().is_fts_index()) {
    if (OB_FAIL(ObDomainDMLIterator::rewind())) {
      LOG_WARN("fail to ObDomainDMLIterator::rewind", K(ret));
    } else if (OB_ISNULL(das_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, das_ctdef is nullptr", K(ret), KP(das_ctdef_));
    } else if (ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT == mode_) {
      const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
      const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
      bool is_same = false;
      if (OB_FAIL(ft_parse_helper_.check_is_the_same(parser_str, parser_property_str, is_same))) {
        LOG_WARN("fail to check is the same", K(ret), K(parser_str), K(parser_property_str));
      } else if (is_same) {
        // This is the same as the parser name of the previous index.
        // nothing to do, just skip.
      } else if (FALSE_IT(ft_parse_helper_.reset())) {
      } else if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_str, parser_property_str))) {
        LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
      }
    } else if (ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN == mode_) {
      if (OB_ISNULL(doc_word_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, doc word info is nullptr", K(ret), KPC(doc_word_info_));
      } else if (FALSE_IT(ft_doc_word_iter_.reset())) {
      } else if (OB_FAIL(ft_doc_word_iter_.init(doc_word_info_->doc_word_table_id_,
                                                doc_word_info_->doc_word_ls_id_,
                                                doc_word_info_->doc_word_tablet_id_,
                                                doc_word_info_->snapshot_,
                                                doc_word_info_->doc_word_schema_version_))) {
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
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fulltext dml iterator twice", K(ret), K(is_inited_));
  } else {
    switch (mode_) {
      case ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT: {
        if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_name, parser_properties))) {
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
                                                  doc_word_info_->snapshot_,
                                                  doc_word_info_->doc_word_schema_version_))) {
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
  if (OB_UNLIKELY(mode < ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT || mode >= ObDomainDMLMode::DOMAIN_DML_MODE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid domain dml mode", K(ret), K(mode));
  } else if (OB_ISNULL(das_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, das ctdef is nullptr", K(ret), KPC(das_ctdef_));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_fts_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type", K(ret), K(das_ctdef_->table_param_));
  } else if (OB_FAIL(ObDomainDMLIterator::rewind())) {
    LOG_WARN("fail to ObDomainDMLIterator::rewind", K(ret));
  } else if (mode == mode_) {
    // nothing to do
  } else {
    switch (mode) {
      case ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT: {
        const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
        const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
        bool is_same = false;
        if (OB_FAIL(ft_parse_helper_.check_is_the_same(parser_str, parser_property_str, is_same))) {
          LOG_WARN("fail to check is the same", K(ret), K(parser_str), K(parser_property_str));
        } else if (is_same) {
          // This is the same as the parser name of the previous index.
          // nothing to do, just skip.
        } else if (FALSE_IT(ft_parse_helper_.reset())) {
        } else if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_str, parser_property_str))) {
          LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
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
                                                  doc_word_info_->snapshot_,
                                                  doc_word_info_->doc_word_schema_version_))) {
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
        if (OB_FAIL(generate_ft_word_rows(store_row))) {
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
  ObString doc_id;
  ObString ft;
  common::ObObjMeta ft_meta;
  const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
  if (!is_update_ && OB_FAIL(get_ft_and_doc_id(store_row, doc_id, ft, ft_meta))) {
    LOG_WARN("fail to get fulltext and doc id", K(ret), KPC(store_row));
  } else if (is_update_ && OB_FAIL(get_ft_and_doc_id_for_update(store_row, doc_id, ft, ft_meta))) {
    LOG_WARN("fail to get fulltext and doc id for update", K(ret), KPC(store_row));
  } else if (OB_FAIL(ObDASDomainUtils::generate_fulltext_word_rows(allocator_, &ft_parse_helper_, ft_meta,
          doc_id, ft, is_fts_index_aux, rows_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to generate fulltext word rows", K(ret), K(doc_id), K(ft_parse_helper_),
          K(ft_meta), K(ft), KPC(store_row), K(is_fts_index_aux), K(rows_), KPC(main_ctdef_));
    }
  }
  return ret;
}

int ObFTDMLIterator::scan_ft_word_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  const uint64_t doc_id_col_id = das_ctdef_->table_param_.get_data_table().get_doc_id_col_id();
  if (OB_UNLIKELY(OB_INVALID_ID == doc_id_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id column id", K(ret), K(doc_id_col_id));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;
    ObString doc_id_str = store_row->cells()[row_projector_->at(doc_id_idx)].get_string();
    common::ObDocId doc_id;
    if (OB_FAIL(doc_id.from_string(doc_id_str))) {
      LOG_WARN("fail to parse doc id from string", K(ret), K(doc_id_str));
    } else if (OB_FAIL(ft_doc_word_iter_.do_scan(doc_word_info_->doc_word_table_id_, doc_id))) {
      LOG_WARN("fail to do scan", K(ret), KPC(doc_word_info_), K(doc_id));
    } else {
      do {
        blocksstable::ObDatumRow *row = nullptr;
        blocksstable::ObDatumRow *ft_word_row = nullptr;
        if (OB_FAIL(ft_doc_word_iter_.get_next_row(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (OB_FAIL(build_ft_word_row(row, ft_word_row))) {
          LOG_WARN("fail to build ft word row", K(ret), KPC(row));
        } else if (OB_FAIL(rows_.push_back(ft_word_row))) {
          LOG_WARN("fail push back ft word row", K(ret), KPC(ft_word_row));
        } else {
          LOG_TRACE("succeed to get one ft word from fts doc word", KPC(ft_word_row));
        }
      } while (OB_SUCC(ret));
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("succeed to scan ft word rows", K(doc_id), K(rows_.count()), K(doc_id_str));
      }
    }
  }
  if (OB_SUCC(ret) && GCONF.enable_strict_defensive_check()) {
    common::ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "FTIterDEF"));
    const common::ObString &parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
    const common::ObString &parser_property_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_property();
    storage::ObFTParseHelper ft_parse_helper;
    ObString doc_id;
    ObString ft;
    common::ObObjMeta ft_meta;
    ObDomainIndexRow rows;
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    if (OB_FAIL(ft_parse_helper.init(&allocator, parser_str, parser_property_str))) {
      LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str), K(parser_property_str));
    } else if (!is_update_ && OB_FAIL(get_ft_and_doc_id(store_row, doc_id, ft, ft_meta))) {
      LOG_WARN("fail to get fulltext and doc id", K(ret), KPC(store_row));
    } else if (is_update_ && OB_FAIL(get_ft_and_doc_id_for_update(store_row, doc_id, ft, ft_meta))) {
      LOG_WARN("fail to get fulltext and doc id for update", K(ret), KPC(store_row));
    } else if (OB_FAIL(ObDASDomainUtils::generate_fulltext_word_rows(allocator, &ft_parse_helper, ft_meta,
            doc_id, ft, is_fts_index_aux, rows))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to generate fulltext word rows", K(ret), K(doc_id), K(ft_parse_helper),
            K(ft_meta), K(ft), KPC(store_row), K(is_fts_index_aux), K(rows), KPC(main_ctdef_));
      }
    }
    if (OB_ITER_END || OB_SUCC(ret)) {
      ret = OB_SUCCESS;
      if (OB_UNLIKELY(rows_.count() != rows.count())) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        common::ObDocId docid;
        docid.from_string(doc_id);
        LOG_ERROR("row count isn't equal between scan ft words and generate ft words", K(ret), K(rows_), K(rows),
            K(is_fts_index_aux), K(doc_id), K(docid), K(ft_meta), K(ft), K(ft_parse_helper), KPC(store_row),
            KPC(main_ctdef_));
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
  const int64_t DOC_ID_IDX = das_ctdef_->table_param_.get_data_table().is_fts_index_aux() ? 1 : 0;
  const int64_t WORD_SEGMENT_IDX = das_ctdef_->table_param_.get_data_table().is_fts_index_aux() ? 0 : 1;
  static int64_t WORD_COUNT_IDX = 2;
  static int64_t DOC_LENGTH_IDX = 3;
  void *buf = nullptr;
  blocksstable::ObDatumRow *tmp_row = nullptr;
  if (OB_ISNULL(src_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(src_row));
  } else if (OB_UNLIKELY(4 != src_row->count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, count of src row isn't 4", K(ret), K(src_row->count_), KPC(src_row));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObDatumRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate datum row", K(ret));
  } else if (FALSE_IT(tmp_row = new (buf) blocksstable::ObDatumRow())) {
  } else if (OB_FAIL(tmp_row->init(src_row->count_))) {
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
  } else {
    dest_row = tmp_row;
  }
  return ret;
}

int ObFTDMLIterator::get_ft_and_doc_id(
    const ObChunkDatumStore::StoredRow *store_row,
    ObString &doc_id,
    ObString &ft,
    common::ObObjMeta &ft_meta)
{
  int ret = OB_SUCCESS;
  const uint64_t doc_id_col_id = das_ctdef_->table_param_.get_data_table().get_doc_id_col_id();
  const uint64_t fts_col_id = das_ctdef_->table_param_.get_data_table().get_fulltext_col_id();
  if (OB_UNLIKELY(OB_INVALID_ID == doc_id_col_id || OB_INVALID_ID == fts_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id or fulltext column id", K(ret), K(doc_id_col_id), K(fts_col_id));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;
    const int64_t ft_idx = !is_fts_index_aux ? 1 : 0;
    doc_id = store_row->cells()[row_projector_->at(doc_id_idx)].get_string();
    ft = store_row->cells()[row_projector_->at(ft_idx)].get_string();
    ft_meta = das_ctdef_->column_types_.at(ft_idx);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(doc_id.length() != sizeof(ObDocId)) || OB_ISNULL(doc_id.ptr())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid binary document id", K(ret), K(doc_id));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                          ft_meta.get_type(),
                                                          ft_meta.get_collation_type(),
                                                          ft_meta.has_lob_header(),
                                                          ft))) {
      LOG_WARN("fail to get real text data", K(ret));
    } else {
      STORAGE_FTS_LOG(DEBUG, "succeed to get fulltext and doc id", K(doc_id), K(ft_meta), K(ft));
    }
  }
  return ret;
}

int ObFTDMLIterator::get_ft_and_doc_id_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    ObString &doc_id,
    ObString &ft,
    common::ObObjMeta &ft_meta)
{
  int ret = OB_SUCCESS;
  const uint64_t rowkey_col_cnt = das_ctdef_->table_param_.get_data_table().get_rowkey_column_num();
  const uint64_t old_proj_cnt = das_ctdef_->old_row_projector_.count();
  const uint64_t new_proj_cnt = das_ctdef_->new_row_projector_.count();
  if (OB_UNLIKELY(rowkey_col_cnt + 2 != old_proj_cnt || rowkey_col_cnt + 2 != new_proj_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid project count", K(ret), K(rowkey_col_cnt), K(old_proj_cnt), K(new_proj_cnt));
  } else {
    const bool is_fts_index_aux = das_ctdef_->table_param_.get_data_table().is_fts_index_aux();
    const int64_t doc_id_idx = !is_fts_index_aux ? 0 : 1;
    const int64_t ft_idx = !is_fts_index_aux ? 1 : 0;
    doc_id = store_row->cells()[row_projector_->at(doc_id_idx)].get_string();
    ft = store_row->cells()[row_projector_->at(ft_idx)].get_string();
    ft_meta = das_ctdef_->column_types_.at(ft_idx);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(doc_id.length() != sizeof(ObDocId)) || OB_ISNULL(doc_id.ptr())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid binary document id", K(ret), K(doc_id));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                                 ft_meta.get_type(),
                                                                 ft_meta.get_collation_type(),
                                                                 true /* has lob header */,
                                                                 ft))) {
      LOG_WARN("fail to get real text data", K(ret));
    } else {
      STORAGE_FTS_LOG(DEBUG, "succeed to get fulltext and doc id", K(doc_id), K(ft_meta), K(ft));
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
