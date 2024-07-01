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

namespace oceanbase
{
namespace storage
{

class ObLobPersistWriteIter : public ObNewRowIterator
{
public:
    ObLobPersistWriteIter(): param_(nullptr) {}
    virtual ~ObLobPersistWriteIter() {}

    virtual int get_next_row() override { return OB_NOT_IMPLEMENT; }

protected:
  int update_seq_no();
  int dec_lob_size(ObLobMetaInfo &info);
  int inc_lob_size(ObLobMetaInfo &info);

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