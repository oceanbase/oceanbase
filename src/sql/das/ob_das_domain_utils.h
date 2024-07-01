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
#include "lib/hash/ob_hashset.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace sql
{

class ObDASDomainUtils final
{
public:

  static const uint64_t MVI_FULL_ROWKEY_THRESHOLD = 6;
  static const uint64_t MVI_ROWKEY_SIZE_THRESHOLD = 48;

  ObDASDomainUtils() = default;
  ~ObDASDomainUtils() = default;

  static int generate_spatial_index_rows(
      ObIAllocator &allocator,
      const ObDASDMLBaseCtDef &das_ctdef,
      const ObString &wkb_str,
      const IntFixedArray &row_projector,
      const ObDASWriteBuffer::DmlRow &dml_row,
      ObDomainIndexRow &spat_rows);
  static int generate_fulltext_word_rows(
      common::ObIAllocator &allocator,
      storage::ObFTParseHelper *helper,
      const common::ObObjMeta &ft_obj_meta,
      const ObString &doc_id,
      const ObString &fulltext,
      const bool is_fts_index_aux,
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
  static int segment_and_calc_word_count(
      common::ObIAllocator &allocator,
      storage::ObFTParseHelper *helper,
      const common::ObCollationType &type,
      const ObString &fulltext,
      int64_t &doc_length,
      ObFTWordMap &words_count);
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
  typedef common::ObSEArray<common::ObNewRow, DEFAULT_DOMAIN_ROW_COUNT> ObDomainIndexRow;

  static int create_domain_dml_iterator(
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef,
      ObDomainDMLIterator *&domain_iter);

public:
  virtual ~ObDomainDMLIterator();
  virtual void reset();
  virtual int rewind()
  {
    row_idx_ = 0;
    return common::OB_SUCCESS;
  }
  void set_ctdef(const ObDASDMLBaseCtDef *das_ctdef, const IntFixedArray *row_projector);
  void set_row_projector(const IntFixedArray *row_projector) { row_projector_ = row_projector; }
  int get_next_domain_row(ObNewRow *&row);
  int get_next_domain_rows(ObNewRow *&row, int64_t &row_count);
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

protected:
  uint32_t row_idx_;
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
      common::ObIAllocator &allocator,
      const IntFixedArray *row_projector,
      ObDASWriteBuffer::Iterator &write_iter,
      const ObDASDMLBaseCtDef *das_ctdef,
      const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter, das_ctdef, main_ctdef),
      ft_parse_helper_(),
      is_inited_(false)
  {}
  virtual ~ObFTDMLIterator() = default;

  virtual void reset() override;
  virtual int rewind() override;
  int init(const common::ObString &parser_name);

  INHERIT_TO_STRING_KV("ObDomainDMLIterator", ObDomainDMLIterator, K_(ft_parse_helper), K_(is_inited));
protected:
  virtual int generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row) override;
  int get_ft_and_doc_id(
      const ObChunkDatumStore::StoredRow *store_row,
      ObString &doc_id,
      ObString &ft,
      common::ObObjMeta &ft_meta);
  int get_ft_and_doc_id_for_update(
      const ObChunkDatumStore::StoredRow *store_row,
      ObString &doc_id,
      ObString &ft,
      common::ObObjMeta &ft_meta);

private:
  storage::ObFTParseHelper ft_parse_helper_;
  bool is_inited_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_DAS_DOMAIN_UTILS_H
