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

#include "ob_redo_log_generator.h"
#include "ob_memtable_key.h"
#include "ob_memtable.h"
#include "ob_memtable_data.h"
#include "ob_memtable_context.h"
#include "storage/transaction/ob_trans_part_ctx.h"

namespace oceanbase {
using namespace common;
namespace memtable {

void ObRedoLogGenerator::reset()
{
  is_inited_ = false;
  consume_cursor_ = NULL;
  generate_cursor_ = NULL;
  end_guard_ = NULL;
  mem_ctx_ = NULL;
  big_row_size_ = 0;
  generate_big_row_pos_ = INT64_MAX;
  consume_big_row_pos_ = INT64_MAX;
  generate_data_size_ = 0;
  if (NULL != big_row_buf_) {
    ob_free(big_row_buf_);
    big_row_buf_ = NULL;
  }
}

int ObRedoLogGenerator::set(ObMvccRowCallback* start, ObMvccRowCallback* end, ObIMemtableCtx* mem_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(start) || OB_ISNULL(end) || OB_ISNULL(mem_ctx)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != end_guard_) {
    // already set, reset first
    reset();
  }

  if (OB_SUCC(ret)) {
    consume_cursor_ = start;
    generate_cursor_ = start;
    end_guard_ = end;
    mem_ctx_ = mem_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObRedoLogGenerator::set_if_necessary(ObMvccRowCallback* head, ObMemtable* mem)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(head) || OB_ISNULL(mem)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (consume_cursor_ == generate_cursor_) {
    consume_cursor_ = head;
    generate_cursor_ = head;
  } else {
    if (head == generate_cursor_) {
      // the cursor is nelegible since it has not synchronized log or it was set as head before
    } else if (generate_cursor_->on_memtable(mem)) {
      // generate_cursor_ need to redirect in order to clean up the callback that the cursor points to
      generate_cursor_ = head;
    } else {
      // generate_cursor can only points to callback of the following memtable, don't need to modify
    }

    if (head == consume_cursor_) {
      // do nothing
    } else if (consume_cursor_->on_memtable(mem)) {
      consume_cursor_ = head;
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObRedoLogGenerator::generate_and_fill_big_row_redo(const int64_t big_row_size, RedoDataNode& redo,
    ObMvccRowCallback* callback, char* buf, const int64_t buf_len, int64_t& buf_pos)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  ObMemtableMutatorWriter mmw;
  ObMemAttr memattr(mem_ctx_->get_tenant_id(), "LOBMutatorBuf");
  uint64_t table_id = 0;
  ObStoreRowkey rowkey;

#ifdef ERRSIM
  ret = E(EventTable::EN_ALLOCATE_LOB_BUF_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "ERRSIM, memory alloc fail", K(ret));
    return ret;
  }
#endif
  if (big_row_size <= 0 || OB_ISNULL(callback) || OB_ISNULL(buf) || buf_len < 0 || buf_pos < 0 || buf_pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(big_row_size), KP(callback), KP(buf), K(buf_len), K(buf_pos));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    // there is data in big_row_buf, deserialize data from big_row_buf first
  } else if (NULL == (ptr = ob_malloc(big_row_size, memattr))) {
    TRANS_LOG(ERROR, "memory alloc fail", KP(ptr), K(big_row_size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(redo.key_.decode(table_id, rowkey))) {
    TRANS_LOG(WARN, "decode for table_id failed", K(ret));
  } else {
    big_row_buf_ = (char*)ptr;
    big_row_size_ = 0;
    generate_big_row_pos_ = 0;
    consume_big_row_pos_ = generate_big_row_pos_;
    int64_t res_len = 0;
    ObMemtableMutatorMeta meta;
    int64_t tmp_pos = 0;
    bool need_encrypt = false;
    mmw.set_buffer(big_row_buf_, big_row_size);
    if (OB_FAIL(fill_row_redo(mmw, redo))) {
      TRANS_LOG(ERROR, "fill_row_redo", K(ret));
    } else if (OB_FAIL(mmw.serialize(ObTransRowFlag::BIG_ROW_START, res_len))) {
      TRANS_LOG(ERROR, "mmw.serialize fail", K(ret));
    } else if (OB_FAIL(meta.deserialize(big_row_buf_, meta.get_serialize_size(), tmp_pos))) {
      TRANS_LOG(WARN, "meta serialize error", K(ret), K(buf_len), K(buf_pos));
    } else {
      big_row_size_ += res_len;
      // copy meta data when generating the first redo log of big row
      generate_big_row_pos_ = meta.get_serialize_size();
      int64_t orig_buf_pos = buf_pos;
      buf_pos += meta.get_serialize_size();
      if (OB_FAIL(ObMemtableMutatorWriter::handle_big_row_data(
              big_row_buf_, big_row_size_, generate_big_row_pos_, buf, buf_len, buf_pos))) {
        TRANS_LOG(WARN, "failed to encrypt big row data", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(meta.serialize(buf, buf_len, orig_buf_pos))) {
          TRANS_LOG(WARN, "failed to serialize meta", K(ret), K(buf_len), K(orig_buf_pos));
        } else {
          generate_cursor_ = callback;
          // go on serializing following rows
          // optimization: return success when there is now row for serialization
          ret = OB_EAGAIN;
        }
      }
    }
  }

  return ret;
}

// when generating second and afterward redo log for big row,
// take dump logic of version 1.4.70 into account,
// need to generate a fake meta to mark whether current redo log
// is the last log of a big row, the specific steps are as follows:
//
// 1. first judge whether the redo log is the last one, if it is,
//    set the flag in the meta information to BIG_ROW_END;
// 2. serialize the new meta information into buf;
// 3. copy the redo log data to buf until the data is full
int ObRedoLogGenerator::fill_big_row_redo_(char* buf, const int64_t buf_len, int64_t& buf_pos, int64_t& data_size)
{
  int ret = OB_SUCCESS;
  ObMemtableMutatorMeta meta;
  data_size = 0;

  if (OB_ISNULL(buf) || buf_len < 0 || buf_pos < 0 || buf_pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", KP(buf), K(buf_len), K(buf_pos));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    // the size is greater than 40B when filling redo log
  } else if (buf_len - buf_pos <= meta.get_serialize_size()) {
    TRANS_LOG(WARN, "buf len is too small, not enough", K(buf_len), K(buf_pos));
    ret = OB_BUF_NOT_ENOUGH;
  } else if (big_row_size_ - generate_big_row_pos_ <= 0) {
    TRANS_LOG(ERROR, "unexpected big row pos", K_(big_row_size), K_(generate_big_row_pos));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t tmp_pos = 0;
    // deserialize the meta info
    if (OB_FAIL(meta.deserialize(big_row_buf_, meta.get_serialize_size(), tmp_pos))) {
      TRANS_LOG(WARN, "meta serialize error", K(ret), K(buf_len), K(buf_pos));
    } else {
      TRANS_LOG(DEBUG, "desrialize meta succ", K(meta));
      consume_big_row_pos_ = generate_big_row_pos_;
      int64_t orig_buf_pos = buf_pos;
      buf_pos += meta.get_serialize_size();
      if (OB_FAIL(ObMemtableMutatorWriter::handle_big_row_data(
              big_row_buf_, big_row_size_, generate_big_row_pos_, buf, buf_len, buf_pos))) {
        TRANS_LOG(WARN, "failed to encrypt big row data", K(ret));
      } else {
        const bool is_big_row_end = (generate_big_row_pos_ == big_row_size_);
        if (is_big_row_end) {
          data_size = generate_cursor_->get_data_size();
          meta.set_flags(ObTransRowFlag::BIG_ROW_END);
          DEBUG_SYNC(REPLAY_REDO_LOG);
        } else {
          meta.set_flags(ObTransRowFlag::BIG_ROW_MID);
        }
        meta.generate_new_header();
        if (meta.serialize(buf, buf_len, orig_buf_pos)) {
          TRANS_LOG(WARN, "failed to serialize meta", K(ret), K(buf_len), K(orig_buf_pos));
        } else if (!is_big_row_end) {
          ret = OB_EAGAIN;
        }
      }
    }
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(DEBUG,
          "fill big row redo",
          K(ret),
          K(meta),
          K_(consume_big_row_pos),
          K_(generate_big_row_pos),
          "mem_ctx",
          *(static_cast<ObMemtableCtx*>(mem_ctx_)));
    }
  }
  return ret;
}

bool ObRedoLogGenerator::big_row_log_fully_filled() const
{
  return big_row_size_ - generate_big_row_pos_ <= 0;
}

int ObRedoLogGenerator::fill_redo_log(char* buf, const int64_t buf_len, int64_t& buf_pos)
{
  int ret = OB_SUCCESS;
  // records the data amount of all serialized trans node of this fill process
  generate_data_size_ = 0;

  if (OB_ISNULL(buf) || buf_len < 0 || buf_pos < 0 || buf_pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", KP(buf), K(buf_len), K(buf_pos));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (big_row_log_fully_filled()) {
    generate_big_row_pos_ = INT64_MAX;
    consume_big_row_pos_ = INT64_MAX;
    big_row_size_ = 0;
    if (NULL != big_row_buf_) {
      ob_free(big_row_buf_);
      big_row_buf_ = NULL;
    }
    // there is data in big_row_buf, fill_redo_log should first copy data from it
  } else if (OB_FAIL(fill_big_row_redo_(buf, buf_len, buf_pos, generate_data_size_))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "fill big row redo error", K(ret), KP(buf), K(buf_len), K(buf_pos));
    }
  } else {
    // after filling the last redo log of big row, now matter
    // how much memory left, we don't serialize the following rows.
    // in doing this, the release of big row buffer and
    // modification of consumer_cursor_ would be more convenient.
    ret = OB_EAGAIN;
  }
  if (OB_SUCC(ret)) {
    ObMemtableMutatorWriter mmw;
    consume_cursor_ = generate_cursor_;
    mmw.set_buffer(buf, buf_len - buf_pos);
    RedoDataNode redo;
    // record the number of serialized trans node in the filling process
    int64_t data_node_count = 0;
    bool need_serialize = true;
    for (ObMvccRowCallback* iter = (ObMvccRowCallback*)generate_cursor_->get_next();
         OB_SUCCESS == ret && NULL != iter && end_guard_ != iter;
         iter = (ObMvccRowCallback*)iter->get_next()) {
      if (!iter->need_fill_redo()) {
        continue;
      } else if (OB_FAIL(iter->get_redo(redo)) && OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(ERROR, "get_redo", K(ret));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(fill_row_redo(mmw, redo)) && OB_BUF_NOT_ENOUGH != ret) {
        TRANS_LOG(ERROR, "fill_row_redo", K(ret));
      } else if (OB_BUF_NOT_ENOUGH == ret) {
        // buf is not enough: if some rows have been serialized before, that means
        // more redo data is demanding more buf, returns OB_EAGAIN;
        // if the buf is not enough for the first trans node, that means a big row
        // is comming, handle it according to the big row logic
        if (0 != data_node_count) {
          ret = OB_EAGAIN;
        } else {
          // deal with big row logic
          if (OB_FAIL(generate_and_fill_big_row_redo(mmw.get_serialize_size(), redo, iter, buf, buf_len, buf_pos)) &&
              OB_EAGAIN != ret) {
            TRANS_LOG(WARN,
                "fill big row redo error",
                K(ret),
                "iter",
                *iter,
                K_(generate_cursor),
                "mem_ctx",
                *(static_cast<ObMemtableCtx*>(mem_ctx_)));
          } else {
            TRANS_LOG(DEBUG,
                "generate big row redo data success",
                "iter",
                *iter,
                K_(generate_cursor),
                K_(consume_cursor),
                "mem_ctx",
                *(static_cast<ObMemtableCtx*>(mem_ctx_)));
          }
          need_serialize = false;
          // it is unnecessary to traverse the next trans node before big row data has been written
          break;
        }
      } else if (OB_SUCCESS == ret) {
        data_node_count++;
        if (!iter->is_savepoint()) {
          generate_data_size_ += iter->get_data_size();
        }
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        generate_cursor_ = iter;
      }
    }
    if (need_serialize && (OB_EAGAIN == ret || OB_SUCCESS == ret)) {
      int tmp_ret = OB_SUCCESS;
      int64_t res_len = 0;
      if (OB_SUCCESS != (tmp_ret = mmw.serialize(ObTransRowFlag::NORMAL_ROW, res_len))) {
        ret = tmp_ret;
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          TRANS_LOG(ERROR, "mmw.serialize fail", K(ret));
        }
      } else {
        buf_pos += res_len;
      }
    }
  }
  return ret;
}

int ObRedoLogGenerator::undo_fill_redo_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    generate_data_size_ = 0;
    // only when the current row iss not a big row, can generator_cursor_ be rollbacked
    if (0 == big_row_size_) {
      generate_cursor_ = consume_cursor_;
    } else if (consume_big_row_pos_ > 0) {
      // retry happens in the middile of big row, can't rollback generate_cursor_
      generate_big_row_pos_ = consume_big_row_pos_;
    } else {
      // for the first undo of big row, need to release memory,
      // refilling log from scratch, or the flag in meta would be wrong(1->2)
      TRANS_LOG(INFO,
          "undo fill redo log for the first redo log",
          K_(generate_big_row_pos),
          K_(consume_big_row_pos),
          KP_(big_row_buf),
          K_(big_row_size));
      // rollback generate_cursor_ to consumer_cursor_
      generate_cursor_ = consume_cursor_;
      generate_big_row_pos_ = INT64_MAX;
      consume_big_row_pos_ = INT64_MAX;
      big_row_size_ = 0;
      if (NULL != big_row_buf_) {
        ob_free(big_row_buf_);
        big_row_buf_ = NULL;
      }
    }
  }
  return ret;
}

void ObRedoLogGenerator::sync_log_succ(const int64_t log_id, const int64_t log_ts, bool& has_pending_cb)
{
  UNUSED(log_id);

  int ret = OB_SUCCESS;
  has_pending_cb = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (consume_cursor_ != generate_cursor_) {
    bool big_row_generating = false;

    for (ObMvccRowCallback* iter = (ObMvccRowCallback*)consume_cursor_->get_next();
         OB_SUCCESS == ret && NULL != iter && end_guard_ != iter;
         iter = (ObMvccRowCallback*)iter->get_next()) {
      if (iter->need_fill_redo()) {
        iter->set_log_ts(log_ts);
        if (generate_cursor_ == iter && !big_row_log_fully_filled()) {
          big_row_generating = true;
        } else {
          ret = iter->log_sync(log_ts);
        }
      }

      if (generate_cursor_ == iter) {
        ObMvccRowCallback* next_iter = (ObMvccRowCallback*)iter->get_next();
        ObMemtable* memtable = get_first_not_null_memtable_if_exit_(next_iter, end_guard_);
        if (NULL == memtable) {
          has_pending_cb = false;
        } else {
          has_pending_cb = memtable->is_frozen_memtable();
        }
        break;
      }
    }

    consume_cursor_ = big_row_generating ? (ObMvccRowCallback*)generate_cursor_->get_prev() : generate_cursor_;

    if (OB_FAIL(ret)) {
      TRANS_LOG(ERROR, "log sync callback failed", K(ret));
    }
  }
}

ObMemtable* ObRedoLogGenerator::get_first_not_null_memtable_if_exit_(ObMvccRowCallback* start, ObMvccRowCallback* end)
{
  ObMemtable* memtable = NULL;
  for (ObMvccRowCallback* iter = start; NULL == memtable && iter != end; iter = (ObMvccRowCallback*)iter->get_next()) {
    memtable = iter->get_memtable();
  }

  return memtable;
}

int ObRedoLogGenerator::fill_row_redo(ObMemtableMutatorWriter& mmw, const RedoDataNode& redo)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  ObStoreRowkey rowkey;
  const ObMemtableKey* mtk = &redo.key_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mtk)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(ret), K(mtk));
  } else {
    if (OB_FAIL(mtk->decode(table_id, rowkey))) {
      TRANS_LOG(WARN, "mtk decode fail", "ret", ret);
    } else {
      if (OB_FAIL(mmw.append_kv(table_id, rowkey, mem_ctx_->get_max_table_version(), redo))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          TRANS_LOG(WARN, "mutator writer append_kv fail", "ret", ret);
        }
      }
    }
  }
  return ret;
}

};  // end namespace memtable
};  // end namespace oceanbase
