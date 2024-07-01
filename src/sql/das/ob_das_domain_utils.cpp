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

#include "lib/geo/ob_s2adapter.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/json_type/ob_json_bin.h"
#include "sql/das/ob_das_domain_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

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
    ObObj *obj_arr = NULL;
    ObS2Cellids cellids;
    char *mbr = NULL;
    int64_t mbr_len = 0;
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
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
        if (OB_ISNULL(obj_arr = reinterpret_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * rowkey_num)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for spatial index row cells", K(ret));
        } else {
          // 索引行[cellid_obj][mbr_obj][rowkey_obj]
          for(uint64_t j = 0; OB_SUCC(ret) && j < rowkey_num; j++) {
            obj_arr[j].set_nop_value();
            const ObObjMeta &col_type = das_ctdef.column_types_.at(j);
            const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
            int64_t projector_idx = row_projector.at(j);
            if (OB_FAIL(dml_row.cells()[projector_idx].to_obj(obj_arr[j], col_type))) {
              LOG_WARN("stored row to new row obj failed", K(ret),
                  K(dml_row.cells()[projector_idx]), K(col_type), K(projector_idx), K(j));
            } else if (OB_FAIL(ObDASUtils::reshape_storage_value(col_type, col_accuracy, allocator, obj_arr[j]))) {
              LOG_WARN("reshape storage value failed", K(ret), K(col_type), K(projector_idx), K(j));
            }
          }
          if (OB_SUCC(ret)) {
            int64_t cellid_col_idx = 0;
            int64_t mbr_col_idx = 1;
            obj_arr[cellid_col_idx].set_uint64(cellids.at(i));
            ObString mbr_val(mbr_len, mbr);
            obj_arr[mbr_col_idx].set_varchar(mbr_val);
            obj_arr[mbr_col_idx].set_collation_type(CS_TYPE_BINARY);
            obj_arr[mbr_col_idx].set_collation_level(CS_LEVEL_IMPLICIT);
            ObNewRow row;
            row.cells_ = obj_arr;
            row.count_ = rowkey_num;
            if (OB_FAIL(spat_rows.push_back(row))) {
              LOG_WARN("failed to push back spatial index row", K(ret), K(row));
            }
          }
        }
      }
    }
  }

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
  ObObj *obj_arr = nullptr;
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
  } else {
    const int64_t obj_cnt = ft_word_map.size() * FT_WORD_DOC_COL_CNT;
    const int64_t obj_arr_size = sizeof(ObObj) * obj_cnt;
    if (OB_ISNULL(obj_arr = reinterpret_cast<ObObj *>(allocator.alloc(obj_arr_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate obj array", K(ret), K(obj_arr_size));
    } else {
      for (int64_t i = 0; i < obj_cnt; ++i) {
        new (obj_arr + i) ObObj();
      }
    }
    int64_t i = 0;
    for (ObFTWordMap::const_iterator iter = ft_word_map.begin();
         OB_SUCC(ret) && iter != ft_word_map.end();
         ++iter) {
      const ObFTWord &ft_word = iter->first;
      const int64_t word_cnt = iter->second;
      // index row format
      //  -    FTS_INDEX: [WORD], [DOC_ID], [WORD_COUNT]
      //  - FTS_DOC_WORD: [DOC_ID], [WORD], [WORD_COUNT]
      const int64_t word_idx = is_fts_index_aux ? 0 : 1;
      const int64_t doc_id_idx = is_fts_index_aux ? 1 : 0;
      const int64_t word_cnt_idx = 2;
      const int64_t doc_len_idx = 3;
      obj_arr[i * FT_WORD_DOC_COL_CNT + word_idx].set_varchar(ft_word.get_word());
      obj_arr[i * FT_WORD_DOC_COL_CNT + word_idx].set_meta_type(ft_obj_meta);
      obj_arr[i * FT_WORD_DOC_COL_CNT + doc_id_idx].set_varbinary(doc_id);
      obj_arr[i * FT_WORD_DOC_COL_CNT + word_cnt_idx].set_uint64(word_cnt);
      obj_arr[i * FT_WORD_DOC_COL_CNT + doc_len_idx].set_uint64(doc_length);
      ObNewRow row;
      row.cells_ = &(obj_arr[i * FT_WORD_DOC_COL_CNT]);
      row.count_ = FT_WORD_DOC_COL_CNT;
      if (OB_FAIL(word_rows.push_back(row))) {
        LOG_WARN("fail to push back row", K(ret), K(row));
      } else {
        ObDocId tmp_doc_id;
        tmp_doc_id.tablet_id_ = ((const uint64_t *)doc_id.ptr())[0];
        tmp_doc_id.seq_id_ = ((const uint64_t *)doc_id.ptr())[1];
        STORAGE_FTS_LOG(DEBUG, "succeed to add word row", K(ret), K(is_fts_index_aux), "doc_id", tmp_doc_id,
            K(ft_word), K(word_cnt), K(i), K(row));
        ++i;
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
                  || ObCollationType::CS_TYPE_EXTENDED_MARK < type)
      || OB_UNLIKELY(!words_count.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(type), K(words_count.created()));
  } else if (OB_FAIL(helper->segment(type, fulltext.ptr(), fulltext.length(), doc_length, words_count))) {
    LOG_WARN("fail to segment", K(ret), KPC(helper), K(type), K(fulltext));
  }
  STORAGE_FTS_LOG(DEBUG, "segment and calc word count", K(ret), K(words_count.size()), K(type));
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

  if (OB_FAIL(get_pure_mutivalue_data(json_str, data, data_len, record_num))) {
    LOG_WARN("failed to parse binary.", K(ret), K(json_str));
  } else if (record_num == 0 && (is_unique_index && rowkey_column_start == 1)) {
  } else if (OB_FAIL(calc_save_rowkey_policy(allocator, das_ctdef, row_projector,
    dml_row, record_num, is_save_rowkey))) {
    LOG_WARN("failed to calc store policy.", K(ret), K(data_table_rowkey_cnt));
  } else {

    ObObj *obj_arr = nullptr;
    int64_t pos = sizeof(uint32_t);

    for (int i = 0; OB_SUCC(ret) && (i < record_num || !is_none_unique_done) ; ++i) {
      if (OB_ISNULL(obj_arr = reinterpret_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * column_num)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for multivalue index row cells", K(ret));
      }
      for(uint64_t j = 0; OB_SUCC(ret) && j < column_num; j++) {
        obj_arr[j].set_nop_value();
        ObObjMeta col_type = das_ctdef.column_types_.at(j);
        const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
        int64_t projector_idx = row_projector.at(j);

        if (multivalue_idx == projector_idx) {
          if (OB_FAIL(obj_arr[j].deserialize(data, data_len, pos))) {
            LOG_WARN("failed to deserialize datum.", K(ret), K(json_str));
          } else {
            if (ob_is_number_or_decimal_int_tc(col_type.get_type()) || ob_is_temporal_type(col_type.get_type())) {
              col_type.set_collation_level(CS_LEVEL_NUMERIC);
            } else {
              col_type.set_collation_level(CS_LEVEL_IMPLICIT);
            }

            obj_arr[j].set_collation_level(col_type.get_collation_level());
            obj_arr[j].set_collation_type(col_type.get_collation_type());
            obj_arr[j].set_type(col_type.get_type());
            is_none_unique_done = true;
          }
        } else if (!is_save_rowkey && (rowkey_column_start >= j && j < rowkey_column_end)) {
          obj_arr[j].set_null();
        } else if (multivalue_arr_idx == projector_idx) {
          obj_arr[j].set_null();
        } else if (OB_FAIL(dml_row.cells()[projector_idx].to_obj(obj_arr[j], col_type))) {
          LOG_WARN("stored row to new row obj failed", K(ret),
              K(dml_row.cells()[projector_idx]), K(col_type), K(projector_idx), K(j));
        }

        if (OB_SUCC(ret) && OB_FAIL(ObDASUtils::reshape_storage_value(col_type, col_accuracy, allocator, obj_arr[j]))) {
          LOG_WARN("reshape storage value failed", K(ret), K(col_type), K(projector_idx), K(j));
        }
      }

      if (OB_SUCC(ret)) {
        ObNewRow row;
        row.cells_ = obj_arr;
        row.count_ = column_num;
        if (OB_FAIL(mvi_rows.push_back(row))) {
          LOG_WARN("failed to push back spatial index row", K(ret), K(row));
        }
      } // end if (OB_SUCC(ret))
    }
  }

  return ret;
}

/*static*/ int ObDomainDMLIterator::create_domain_dml_iterator(
    common::ObIAllocator &allocator,
    const IntFixedArray *row_projector,
    ObDASWriteBuffer::Iterator &write_iter,
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef,
    ObDomainDMLIterator *&domain_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_projector) || OB_ISNULL(das_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(row_projector), KP(das_ctdef));
  } else if (das_ctdef->table_param_.get_data_table().is_spatial_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSpatialDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate spatial dml iterator memory", K(ret), KP(buf));
    } else {
      domain_iter = new (buf) ObSpatialDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef);
    }
  } else if (das_ctdef->table_param_.get_data_table().is_fts_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObFTDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate fulltext dml iterator memory", K(ret), KP(buf));
    } else {
      ObFTDMLIterator *iter = new (buf) ObFTDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef);
      if (OB_FAIL(iter->init(das_ctdef->table_param_.get_data_table().get_fts_parser_name()))) {
        LOG_WARN("fail to init fulltext dml iterator", K(ret), KPC(iter));
      } else {
        domain_iter = static_cast<ObDomainDMLIterator *>(iter);
      }
    }
  } else if (das_ctdef->table_param_.get_data_table().is_multivalue_index()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMultivalueDMLIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate fulltext dml iterator memory", K(ret), KP(buf));
    } else {
      ObMultivalueDMLIterator *iter = new (buf) ObMultivalueDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef);
      domain_iter = static_cast<ObDomainDMLIterator *>(iter);
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported domain index type", K(ret), K(das_ctdef->table_param_.get_data_table()));
  }
  return ret;
}

ObDomainDMLIterator::ObDomainDMLIterator(
    common::ObIAllocator &allocator,
    const IntFixedArray *row_projector,
    ObDASWriteBuffer::Iterator &write_iter,
    const ObDASDMLBaseCtDef *das_ctdef,
    const ObDASDMLBaseCtDef *main_ctdef)
  : row_idx_(0),
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
  row_idx_ = 0;
  rows_.reset();
  row_projector_ = nullptr;
  das_ctdef_ = nullptr;
  main_ctdef_ = nullptr;
  allocator_.reset();
}

void ObDomainDMLIterator::set_ctdef(
    const ObDASDMLBaseCtDef *das_ctdef,
    const IntFixedArray *row_projector)
{
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

int ObDomainDMLIterator::get_next_domain_row(ObNewRow *&row)
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
        }
      }
    }
    if (OB_SUCC(ret) && row_idx_ < rows_.count()) {
      row = &(rows_[row_idx_]);
      ++row_idx_;
      got_row = true;
    }
  }
  LOG_DEBUG("get next domain row", K(ret), K(got_row), K(row_idx_), K(rows_), KPC(row), KPC(sr));
  return ret;
}

int ObDomainDMLIterator::get_next_domain_rows(ObNewRow *&row, int64_t &row_count)
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
          }
        }
      }
      if (OB_SUCC(ret) && row_idx_ < rows_.count()) {
        row = &(rows_[row_idx_]);
        row_count = rows_.count() - row_idx_;
        row_idx_ = rows_.count();
        got_row = true;
      }
    }
    LOG_DEBUG("get next domain rows", K(ret), K(got_row), K(row_idx_), K(row_count), K(rows_), KPC(row), KPC(sr));
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
    } else {
      storage::ObFTParser parser_name;
      const common::ObString parser_str = das_ctdef_->table_param_.get_data_table().get_fts_parser_name();
      if (OB_FAIL(parser_name.parse_from_str(parser_str.ptr(), parser_str.length()))) {
        LOG_WARN("fail to parse name from cstring", K(ret), K(parser_str));
      } else if (parser_name == ft_parse_helper_.get_parser_name()) {
        // This is the same as the parser name of the previous index.
        // nothing to do, just skip.
      } else if (FALSE_IT(ft_parse_helper_.reset())) {
      } else if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_str))) {
        LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_str));
      }
    }
  }
  return ret;
}

int ObFTDMLIterator::init(const common::ObString &parser_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fulltext dml iterator twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(ft_parse_helper_.init(&allocator_, parser_name))) {
    LOG_WARN("fail to init fulltext parse helper", K(ret), K(parser_name));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
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
  }
  STORAGE_FTS_LOG(DEBUG, "generate domain rows", K(ret), K(rows_), KPC(store_row));
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
