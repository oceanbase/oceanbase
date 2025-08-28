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

#ifndef OCEANBASE_STORAGE_OB_LOB_HANDLER_H_
#define OCEANBASE_STORAGE_OB_LOB_HANDLER_H_

#include "storage/lob/ob_lob_access_param.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/lob/ob_lob_meta_manager.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaManager;
class ObLobOutRowQueryIter;

class ObLobQueryBaseHandler
{
public:
  ObLobQueryBaseHandler(ObLobAccessParam &param):
    is_inited_(false),
    param_(param),
    lob_meta_mngr_(nullptr)
  {}

  int execute();

protected:
  virtual int do_execute() = 0;
  int init_base(ObLobMetaManager *lob_meta_mngr);

protected:
  bool is_inited_;
  ObLobAccessParam &param_;
  ObLobMetaManager *lob_meta_mngr_;

};

// one query handler corresponds to one interface in lob manager

class ObLobQueryIterHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryIterHandler(ObLobAccessParam &param) :
    ObLobQueryBaseHandler(param),
    result_(nullptr)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

public:
  ObLobOutRowQueryIter *result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryIterHandler);
};

class ObLobQueryDataHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryDataHandler(ObLobAccessParam &param, ObString &data_buffer) :
    ObLobQueryBaseHandler(param),
    result_(data_buffer)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

private:
  int write_data_to_buffer(ObString &buffer, ObString &data);

public:
   ObString &result_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryDataHandler);
};

class ObLobQueryLengthHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryLengthHandler(ObLobAccessParam &param) :
    ObLobQueryBaseHandler(param),
    result_(0)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

public:
  uint64_t result_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryLengthHandler);

};

class ObLobWriteBaseHandler
{
public:
  ObLobWriteBaseHandler(ObLobAccessParam &param):
    is_inited_(false),
    param_(param),
    lob_meta_mngr_(nullptr),
    store_chunk_size_(0)
  {}

protected:

  int init_base(ObLobMetaManager *lob_meta_mngr);

  int write_one_piece(ObLobAccessParam& param, ObLobMetaInfo& meta_row);
  int update_one_piece(ObLobAccessParam& param, ObLobMetaInfo& old_meta_info, ObLobMetaInfo& new_meta_info);
  int erase_one_piece(ObLobAccessParam& param, ObLobMetaInfo& meta_info);
  int write_outrow_result(ObLobAccessParam& param, ObLobMetaWriteIter &write_iter);
  int prepare_data_buffer(ObLobAccessParam& param, ObString &buffer, int64_t buffer_size);

protected:
  bool is_inited_;
  ObLobAccessParam &param_;
  ObLobMetaManager *lob_meta_mngr_;
  int64_t store_chunk_size_;
};


class ObLobFullInsertHandler : public ObLobWriteBaseHandler
{
public:
  ObLobFullInsertHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute(ObString &data);
  int execute(ObLobQueryIter *iter, int64_t append_lob_len, ObString& ori_inrow_data);

private:
  int do_insert(ObLobMetaWriteIter &iter);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobFullInsertHandler);
};

class ObLobAppendHandler : public ObLobWriteBaseHandler
{
public:
  ObLobAppendHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute(ObString &data, bool ori_is_inrow);
  int execute(ObLobQueryIter *iter, int64_t append_lob_len, ObString& ori_inrow_data);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobAppendHandler);
};

class ObLobFullDeleteHandler : public ObLobWriteBaseHandler
{
public:
  ObLobFullDeleteHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute();

private:
  int do_delete(ObLobMetaScanIter &iter);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobFullDeleteHandler);
};

class ObLobEraseHandler : public ObLobWriteBaseHandler
{
public:
  ObLobEraseHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute();

private:
  int erase_process_meta_info(
      ObLobMetaScanIter &meta_iter,
      ObLobMetaScanResult &result,
      ObString &write_buf);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobEraseHandler);
};

class ObLobFillZeroHandler : public ObLobWriteBaseHandler
{
public:
  ObLobFillZeroHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);
  int execute();

private:
  int do_fill_zero_outrow(
      ObLobMetaScanIter &meta_iter,
      ObLobMetaScanResult &result,
      ObString &write_buf);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobFillZeroHandler);
};


class ObLobWriteHandler : public ObLobWriteBaseHandler
{
public:
  ObLobWriteHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute(ObLobQueryIter *iter, ObString& read_buf, ObString& old_data);

private:
  int replace_process_meta_info(
      ObLobMetaScanIter &meta_iter,
      ObLobMetaScanResult &result,
      ObLobQueryIter *iter,
      ObString& read_buf,
      ObString &remain_data,
      ObString &write_buf);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobWriteHandler);
};

class ObLobDiffUpdateHandler : public ObLobWriteBaseHandler
{
public:
  ObLobDiffUpdateHandler(ObLobAccessParam &param):
    ObLobWriteBaseHandler(param)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

  int execute(ObLobLocatorV2& delta_locator, ObLobDiffHeader *diff_header);

private:
  int get_extra_diff_data(ObLobLocatorV2 &lob_locator, ObLobDiffHeader *diff_header, ObString &extra_diff_data);

public:
  TO_STRING_KV(K_(param), K_(store_chunk_size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobDiffUpdateHandler);
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_HANDLER_H_