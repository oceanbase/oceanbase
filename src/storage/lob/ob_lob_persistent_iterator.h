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

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_ITERATOR_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_ITERATOR_

#include "deps/oblib/src/common/row/ob_row_iterator.h"
#include "src/storage/lob/ob_lob_util.h"
#include "src/storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{

class ObPersistentLobApator;
class ObLobMetaInfo;

class ObLobMetaBaseIterator
{
protected:
  ObLobMetaBaseIterator():
    main_tablet_id_(),
    lob_meta_tablet_id_(),
    lob_piece_tablet_id_(),
    rowkey_objs_(),
    seq_id_local_buf_(0),
    scan_param_(),
    row_iter_(nullptr),
    adaptor_(nullptr)
  {}

  virtual ~ObLobMetaBaseIterator() {}


protected:
  int build_rowkey_range(ObLobAccessParam &param, ObRowkey &min_row_key, ObRowkey &max_row_key, ObNewRange &range);
  int build_rowkey_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range);
  int build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObString &seq_id, ObNewRange &range);
  int build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range);
  int build_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range);


  int scan(ObLobAccessParam &param, const bool is_get, ObIAllocator *scan_allocator);
  int rescan(ObLobAccessParam &param);
  int revert_scan_iter();

public:
  VIRTUAL_TO_STRING_KV(
      K_(main_tablet_id),
      K_(lob_meta_tablet_id),
      K_(lob_piece_tablet_id),
      K(rowkey_objs_[0]), K(rowkey_objs_[1]), K(rowkey_objs_[3]), K(rowkey_objs_[3]),
      K_(seq_id_local_buf),
      K_(scan_param),
      KP_(row_iter),
      KPC_(row_iter),
      KP_(adaptor));

protected:
  // tablet id of main table
  ObTabletID main_tablet_id_;
  ObTabletID lob_meta_tablet_id_;

  // not used, just for check
  ObTabletID lob_piece_tablet_id_;

  // rowkey for scan range
  ObObj rowkey_objs_[4];

  // used for single get
  uint32_t seq_id_local_buf_;
  ObTableScanParam scan_param_;
  // lob meta tablet scan iter
  // must be released by calling access service revert_scan_iter
  ObNewRowIterator *row_iter_;
  ObPersistentLobApator *adaptor_;

};

class ObLobMetaIterator : public ObLobMetaBaseIterator
{
public:
  ObLobMetaIterator(const ObLobAccessCtx *access_ctx):
    ObLobMetaBaseIterator(),
    access_ctx_(access_ctx)
  {}

  virtual ~ObLobMetaIterator() { reset(); }

  int reset();
  int open(ObLobAccessParam &param, ObPersistentLobApator* adaptor, ObIAllocator *scan_allocator);
  int rescan(ObLobAccessParam &param);
  int get_next_row(ObLobMetaInfo &row);

  const ObLobAccessCtx* get_access_ctx() const { return access_ctx_; }

public:
  INHERIT_TO_STRING_KV("ObLobMetaBaseIterator", ObLobMetaBaseIterator,
      KP(this), KP_(access_ctx));

private:
  const ObLobAccessCtx *access_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObLobMetaIterator);

};

class ObLobMetaSingleGetter : ObLobMetaBaseIterator
{
public:
  ObLobMetaSingleGetter():
     ObLobMetaBaseIterator(),
     param_(nullptr)
  {}

  ~ObLobMetaSingleGetter() { reset(); }

  int reset();

  int open(ObLobAccessParam &param, ObPersistentLobApator* lob_adatper);

  /**
   * currently only used by json partial update
   *
   * DONOT use other situation
   *
   * get idx lob meta info
  */
  int get_next_row(int idx, ObLobMetaInfo &info);
  int get_next_row(ObString &seq_id, ObLobMetaInfo &info);

public:
  INHERIT_TO_STRING_KV("ObLobMetaBaseIterator", ObLobMetaBaseIterator,
      KP(this), KPC_(param));

private:
  ObLobAccessParam *param_;

  DISALLOW_COPY_AND_ASSIGN(ObLobMetaSingleGetter);

};

class ObLobPersistWriteIter : public ObNewRowIterator
{
public:
    ObLobPersistWriteIter(): param_(nullptr) {}
    virtual ~ObLobPersistWriteIter() {}

    virtual int get_next_row() override { return OB_NOT_IMPLEMENT; }

protected:
  int update_seq_no();

protected:
  ObLobAccessParam *param_;

};


class ObLobPersistInsertSingleRowIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistInsertSingleRowIter():
    row_(nullptr),
    iter_end_(false)
  {}
  int init(ObLobAccessParam *param, ObNewRow *row);

  virtual ~ObLobPersistInsertSingleRowIter() {}
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { iter_end_ = false; }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistInsertSingleRowIter);
private:
  // data members
  ObNewRow *row_;
  bool iter_end_;
};


class ObLobPersistDeleteSingleRowIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistDeleteSingleRowIter():
    row_(nullptr),
    iter_end_(false)
  {}
  int init(ObLobAccessParam *param, ObNewRow *row);

  virtual ~ObLobPersistDeleteSingleRowIter() {}
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { iter_end_ = false; }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistDeleteSingleRowIter);
private:
  // data members
  ObNewRow *row_;
  bool iter_end_;
};

class ObLobPersistUpdateSingleRowIter : public ObLobPersistWriteIter
{
public:
  ObLobPersistUpdateSingleRowIter()
    : old_row_(nullptr),
      new_row_(nullptr),
      got_old_row_(false),
      is_iter_end_(false)
  {}

  virtual ~ObLobPersistUpdateSingleRowIter() {}

  int init(ObLobAccessParam *param, ObNewRow *old_row, ObNewRow *new_row);

  virtual int get_next_row(ObNewRow *&row) override;
  virtual void reset() override {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistUpdateSingleRowIter);

private:
  ObNewRow *old_row_;
  ObNewRow *new_row_;
  bool got_old_row_;
  bool is_iter_end_;
};


class ObLobPersistInsertIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistInsertIter() : meta_iter_(nullptr), new_row_(), row_cell_(), result_() {}
  int init(ObLobAccessParam *param, ObLobMetaWriteIter *meta_iter);
  virtual ~ObLobPersistInsertIter() {}
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { new_row_.reset(); }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistInsertIter);
private:
  // data members
  ObLobMetaWriteIter *meta_iter_;
  ObNewRow new_row_;
  ObObj row_cell_[ObLobMetaUtil::LOB_META_COLUMN_CNT];
  ObLobMetaWriteResult result_;
};


class ObLobPersistDeleteIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistDeleteIter() : meta_iter_(nullptr), new_row_(), row_cell_(), result_() {}
  int init(ObLobAccessParam *param, ObLobMetaScanIter *meta_iter);
  virtual ~ObLobPersistDeleteIter() {}
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { new_row_.reset(); }


private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistDeleteIter);
private:
  // data members
  ObLobMetaScanIter *meta_iter_;
  ObNewRow new_row_;
  ObObj row_cell_[ObLobMetaUtil::LOB_META_COLUMN_CNT];
  ObLobMetaScanResult result_;
};


} // storage
} // oceanbase

#endif