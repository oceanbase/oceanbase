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
#include "storage/fts/ob_fts_doc_word_iterator.h"

namespace oceanbase
{
namespace sql
{

class ObFTIndexRowCache final
{
public:
  static ObObjDatumMapType FTS_INDEX_TYPES[4];
  static ObObjDatumMapType FTS_DOC_WORD_TYPES[4];
  static ObExprOperatorType FTS_INDEX_EXPR_TYPE[4];
  static ObExprOperatorType FTS_DOC_WORD_EXPR_TYPE[4];

  ObFTIndexRowCache();
  ~ObFTIndexRowCache();
  int init(
      const bool is_fts_index_aux,
      const common::ObString &parser_name,
      const common::ObString &parser_properties);
  int segment(
      const common::ObObjMeta &ft_obj_meta,
      const common::ObString &doc_id,
      const common::ObString &fulltext);
  int get_next_row(blocksstable::ObDatumRow *&row);
  void reset();
  void reuse();
  TO_STRING_KV(K_(row_idx), K_(is_fts_index_aux), K_(helper), K_(is_inited), K_(rows));
private:
  lib::MemoryContext merge_memctx_;
  ObDomainIndexRow rows_;
  uint64_t row_idx_;
  bool is_fts_index_aux_;
  storage::ObFTParseHelper helper_;
  bool is_inited_;

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
      snapshot_(nullptr),
      doc_word_schema_version_(),
      doc_word_found_(false)
  {}
  ~ObFTDocWordInfo() = default;

  TO_STRING_KV(K_(table_id),
               K_(doc_word_table_id),
               K_(doc_word_ls_id),
               K_(doc_word_tablet_id),
               KPC_(snapshot),
               K_(doc_word_schema_version),
               K_(doc_word_found));
public:
  uint64_t table_id_;
  uint64_t doc_word_table_id_;
  share::ObLSID doc_word_ls_id_;
  common::ObTabletID doc_word_tablet_id_;
  const transaction::ObTxReadSnapshot *snapshot_;
  int64_t doc_word_schema_version_;
  bool doc_word_found_;
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

protected:
  ObDomainDMLMode mode_;
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
