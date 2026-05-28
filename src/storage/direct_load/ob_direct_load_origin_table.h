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
#pragma once

#include "share/schema/ob_table_dml_param.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_single_merge.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadOriginTableScanner;
class ObDirectLoadOriginTableGetter;

struct ObDirectLoadOriginTableCreateParam
{
public:
  ObDirectLoadOriginTableCreateParam();
  ~ObDirectLoadOriginTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id),
               K_(tablet_id),
               K_(ls_id),
               K_(tx_id),
               K_(tx_seq));
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ tx_seq_;
};

struct ObDirectLoadOriginTableMeta
{
public:
  ObDirectLoadOriginTableMeta();
  ~ObDirectLoadOriginTableMeta();
  void reset();
  TO_STRING_KV(K_(table_id),
               K_(tablet_id),
               K_(ls_id),
               K_(tx_id),
               K_(tx_seq));
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ tx_seq_;
};

class ObDirectLoadOriginTable
{
public:
  ObDirectLoadOriginTable();
  virtual ~ObDirectLoadOriginTable();
  void reset();
  int init(const ObDirectLoadOriginTableCreateParam &param);
  int scan(const blocksstable::ObDatumRange &key_range,
           common::ObIAllocator &allocator,
           ObDirectLoadOriginTableScanner *&row_iter,
           bool skip_read_lob);
  int get(const blocksstable::ObDatumRowkey &key,
          common::ObIAllocator &allocator,
          ObDirectLoadOriginTableGetter *&row_iter,
          bool skip_read_lob = true);
  bool is_valid() const { return is_inited_; }
  const ObDirectLoadOriginTableMeta &get_meta() const {return meta_; }
  const ObTabletHandle &get_tablet_handle() const { return tablet_handle_; }
  const ObTableStoreIterator &get_table_iter() const { return *(table_iter_.table_iter()); }
  blocksstable::ObSSTable *get_major_sstable() const { return major_sstable_; }
  const common::ObIArray<blocksstable::ObSSTable *> &get_ddl_sstables() const { return ddl_sstables_; }
  TO_STRING_KV(K_(meta), K_(tablet_handle), K_(table_iter), KP_(major_sstable), K_(ddl_sstables));
private:
  int prepare_tables();
private:
  ObDirectLoadOriginTableMeta meta_;
  ObTabletHandle tablet_handle_;
  ObTabletTableIterator table_iter_;
  // ddl sstables may not merge to major sstable
  blocksstable::ObSSTable *major_sstable_;
  common::ObArray<blocksstable::ObSSTable *> ddl_sstables_;
  bool is_inited_;
};

class ObDirectLoadOriginTableAccessor : public ObDirectLoadIStoreRowIterator
{
public:
  ObDirectLoadOriginTableAccessor();
  virtual ~ObDirectLoadOriginTableAccessor();
protected:
  int inner_init(ObDirectLoadOriginTable *table, bool skip_read_lob);
  int init_table_access_param();
  int init_table_access_ctx(bool skip_read_lob);
  int init_get_table_param();
protected:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator stmt_allocator_;
  ObDirectLoadOriginTable *origin_table_;
  ObArray<int32_t> col_ids_;
  share::schema::ObTableSchemaParam schema_param_;
  ObTableAccessParam table_access_param_;
  ObStoreCtx store_ctx_;
  ObTableAccessContext table_access_ctx_;
  ObGetTableParam get_table_param_;
  bool is_inited_;
};

class ObDirectLoadOriginTableScanner final : public ObDirectLoadOriginTableAccessor
{
public:
  ObDirectLoadOriginTableScanner() = default;
  virtual ~ObDirectLoadOriginTableScanner() = default;
  int init(ObDirectLoadOriginTable *table, bool skip_read_lob);
  int open(const blocksstable::ObDatumRange &query_range);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
private:
  ObMultipleScanMerge scan_merge_;
  ObDirectLoadDatumRow datum_row_;
};

class ObDirectLoadOriginTableGetter final : public ObDirectLoadOriginTableAccessor
{
public:
  ObDirectLoadOriginTableGetter() = default;
  virtual ~ObDirectLoadOriginTableGetter() = default;
  int init(ObDirectLoadOriginTable *table, bool skip_read_lob);
  int open(const blocksstable::ObDatumRowkey &key);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
private:
  ObSingleMerge single_merge_;
  ObDirectLoadDatumRow datum_row_;
};

} // namespace storage
} // namespace oceanbase
