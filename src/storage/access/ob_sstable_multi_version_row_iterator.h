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

#ifndef OB_STORAGE_OB_SSTABLE_MULTI_VERSION_ROW_ITERATOR_H_
#define OB_STORAGE_OB_SSTABLE_MULTI_VERSION_ROW_ITERATOR_H_

#include "ob_sstable_row_getter.h"
#include "ob_sstable_row_scanner.h"
#include "ob_sstable_row_multi_scanner.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {

class ObSSTableMultiVersionRowGetter : public ObSSTableRowScanner<>
{
public:
  ObSSTableMultiVersionRowGetter()
      : multi_version_range_(),
      range_idx_(0),
      base_rowkey_(nullptr)
  {}
  virtual ~ObSSTableMultiVersionRowGetter() {}
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) override;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row) override;
protected:
  blocksstable::ObDatumRange multi_version_range_;
  int64_t range_idx_;
private:
  const blocksstable::ObDatumRowkey *base_rowkey_;
  blocksstable::ObDatumRow not_exist_row_;
};

class ObSSTableMultiVersionRowScanner : public ObSSTableMultiVersionRowGetter
{
public:
  ObSSTableMultiVersionRowScanner()
      : base_range_(nullptr)
  {}
  virtual ~ObSSTableMultiVersionRowScanner() {}
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) override;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row) override
  { return ObSSTableRowScanner::inner_get_next_row(row); }
private:
  const blocksstable::ObDatumRange *base_range_;
};


class ObSSTableMultiVersionRowMultiGetter : public ObSSTableRowMultiScanner<>
{
public:
  ObSSTableMultiVersionRowMultiGetter()
      : multi_version_ranges_(),
      range_idx_(0),
      pending_row_(nullptr),
      base_rowkeys_(nullptr)
  {
    multi_version_ranges_.set_attr(ObMemAttr(MTL_ID(), "MVersionRanges"));
  }
  virtual ~ObSSTableMultiVersionRowMultiGetter() {}
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) override;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row) override;
private:
  common::ObSEArray<blocksstable::ObDatumRange, 4> multi_version_ranges_;
  int64_t range_idx_;
  blocksstable::ObDatumRow not_exist_row_;
  const blocksstable::ObDatumRow *pending_row_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *base_rowkeys_;
};

class ObSSTableMultiVersionRowMultiScanner : public ObSSTableRowMultiScanner<>
{
public:
  ObSSTableMultiVersionRowMultiScanner()
      : multi_version_ranges_()
  {
    multi_version_ranges_.set_attr(ObMemAttr(MTL_ID(), "MVersionRanges"));
  }
  virtual ~ObSSTableMultiVersionRowMultiScanner() {}
  virtual void reset() override;
  virtual void reuse() override;
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) override;
private:
  common::ObSEArray<blocksstable::ObDatumRange, 4> multi_version_ranges_;
};

}
}
#endif //OB_STORAGE_OB_SSTABLE_MULTI_VERSION_ROW_ITERATOR_H_
