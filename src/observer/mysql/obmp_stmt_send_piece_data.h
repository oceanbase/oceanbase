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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_PIECE_DATA_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_PIECE_DATA_H_

#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace sql
{
class ObMultiStmtItem;
}
namespace observer
{

struct ObGlobalContext;

class ObMPStmtSendPieceData : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_SEND_PIECE_DATA;

  explicit ObMPStmtSendPieceData(const ObGlobalContext &gctx);
  virtual ~ObMPStmtSendPieceData() {}
  int64_t get_single_process_timestamp() const { return single_process_timestamp_; }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  int64_t get_send_timestamp() const { return get_receive_timestamp(); }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
  }
protected:
  virtual int deserialize() { return common::OB_SUCCESS; }
  virtual int before_process() override;
  virtual int process();
  virtual int send_error_packet(int err,
                                const char* errmsg,
                                bool is_partition_hit = true,
                                void *extra_err_info = NULL)
  { return ObMPBase::send_error_packet(err, errmsg, is_partition_hit, extra_err_info); }
  virtual int send_ok_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param)
  { return ObMPBase::send_ok_packet(session, ok_param); }
  virtual int send_eof_packet(const sql::ObSQLSessionInfo &session, const ObMySQLResultSet &result)
  { return ObMPBase::send_eof_packet(session, result); }
  virtual int response_packet(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo* session)
  { return ObMPBase::response_packet(pkt, session); }
  virtual bool need_send_extra_ok_packet()
  { return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet(); }
private:
  int do_process(sql::ObSQLSessionInfo &session);
  int response_result(sql::ObSQLSessionInfo &session);

  int process_send_long_data_stmt(sql::ObSQLSessionInfo &session);
  int store_piece(sql::ObSQLSessionInfo &session);

private:
  sql::ObSqlCtx ctx_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  int32_t stmt_id_;
  uint16_t param_id_;
  int64_t buffer_len_;
  common::ObString buffer_;
  int8_t  piece_mode_;
  bool    is_null_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtSendPieceData);


}; // end of class ObMPStmtSendPieceData

enum ObPieceMode
{
  ObInvalidPiece,
  ObFirstPiece,
  ObNextPiece,
  ObLastPiece
};

class ObPieceBuffer
{
public:
  ObPieceBuffer()
    : mode_(ObInvalidPiece), is_null_(false), buffer_(), pos_(NULL), allocator_(NULL) {}
  ObPieceBuffer(ObIAllocator *allocator, ObPieceMode mode) 
    : mode_(mode), is_null_(false), buffer_(), pos_(NULL), allocator_(allocator) {}
  ~ObPieceBuffer() { reset(); }

  void reset()
  {
    mode_ = ObInvalidPiece;
    if (NULL != allocator_) {
      allocator_->free(&buffer_);
    }
    // free allocator by ObPiece
    allocator_ = NULL;
  }
  void set_piece_mode(ObPieceMode mode) { mode_ = mode; }
  ObPieceMode get_piece_mode() { return mode_; }
  void set_null() { is_null_ = true; }
  bool is_null() { return is_null_; }
  bool is_last_piece() { return ObLastPiece == mode_; }
  int set_piece_buffer(ObString *buf) 
  {
    int ret = OB_SUCCESS;
    if (NULL != allocator_ && NULL != buf && NULL != buf->ptr()) {
      if (OB_FAIL(ob_write_string(*allocator_, *buf, buffer_))) {
        SQL_ENG_LOG(WARN, "failed to write piece buffer", K(ret), K(mode_));
      } else {
        pos_ = buffer_.ptr();
      }
    } else if (NULL == allocator_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "piece allocator is NULL", K(ret));
    } else {
      buffer_.assign(NULL, 0);
      pos_ = NULL;
      is_null_ = true;
    }
    SQL_ENG_LOG(DEBUG, "set_piece_buffer", K(ret), K(buffer_), K(NULL != buf ? *buf : NULL));
    return ret;
  }
  ObString *get_piece_buffer() { return &buffer_; }
  char *&get_position() { return pos_; }
  int64_t to_string(char *buffer, int64_t length) const;
private:
  ObPieceMode mode_;
  bool is_null_;
  ObString buffer_;
  char     *pos_;
  ObIAllocator *allocator_;
};

#define OB_MAX_PIECE_COUNT 1024
typedef common::ObFixedArray<ObPieceBuffer, common::ObIAllocator> ObPieceBufferArray;

class ObPiece
{
public:
  ObPiece() 
    : stmt_id_(0), 
      param_id_(-1), 
      pos_(0),
      buffer_array_(NULL),
      allocator_(NULL),
      is_null_map_(),
      err_ret_(OB_SUCCESS) {}
  ~ObPiece() { reset(); }
  void reset()
  {
    if (NULL != buffer_array_) {
      reset_buffer_array();
    }
    if (NULL != allocator_) {
      allocator_->reset();
    }
    stmt_id_ = 0;
    param_id_ = -1;
    pos_ = 0;
    err_ret_ = OB_SUCCESS;
  }
  void reset_buffer_array()
  {
    if (NULL != buffer_array_) {
      for (uint64_t i = 0; i < buffer_array_->count(); i++) {
        ObPieceBuffer piece_buffer = buffer_array_->at(i);
        piece_buffer.~ObPieceBuffer();
        allocator_->free(&piece_buffer);
      }
    }
  }
  void set_stmt_id(int32_t stmt_id) { stmt_id_ = stmt_id; }
  int32_t get_stmt_id() { return stmt_id_; }
  void set_param_id(uint16_t param_id) { param_id_ = param_id; }
  uint16_t get_param_id() { return param_id_; }
  void set_position(uint64_t pos) { pos_ = pos; }
  uint64_t get_position() { return pos_; }
  void add_position() { pos_++; }
  void set_allocator(ObIAllocator *alloc) { allocator_ = alloc; }
  ObIAllocator *get_allocator() { return allocator_; }
  common::ObBitSet<> &get_is_null_map() { return is_null_map_; }
  void get_is_null_map(char *map, int64_t count) {
    for (int64_t i = 0; i<count; i++) {
      if (is_null_map_.has_member(i)) {
        obmysql::ObMySQLUtil::update_null_bitmap(map, i);
      }
    }
  }
  ObPieceBufferArray *get_buffer_array() { return buffer_array_; }
  void set_buffer_array(ObPieceBufferArray *array) { buffer_array_ = array; }
  int piece_init(sql::ObSQLSessionInfo &session, int32_t stmt_id, uint16_t param_id);
  void set_error_ret(int err_ret) { err_ret_ = err_ret; }
  int get_error_ret() { return err_ret_; }
private:
  int32_t stmt_id_;
  uint16_t param_id_;
  uint64_t pos_;
  ObPieceBufferArray *buffer_array_;
  ObIAllocator *allocator_;
  common::ObBitSet<> is_null_map_;
  int err_ret_;
};  // end of class ObPiece

class ObPieceCache {
  public:
    ObPieceCache() : mem_context_(nullptr), piece_map_() {}
    virtual ~ObPieceCache() { NULL != mem_context_ ? DESTROY_CONTEXT(mem_context_) : (void)(NULL); }
    int init(uint64_t tenant_id)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_,
          lib::ContextParam().set_mem_attr(tenant_id, ObModIds::OB_PL_TEMP)))) {
        SQL_ENG_LOG(WARN, "create memory entity failed");
      } else if (OB_ISNULL(mem_context_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "null memory entity returned");
      } else if (!piece_map_.created() &&
                  OB_FAIL(piece_map_.create(common::hash::cal_next_prime(32),
                                            ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
        SQL_ENG_LOG(WARN, "create sequence current value map failed", K(ret));
      } else { /*do nothing*/ }
      return ret;
    }
    int close_all(sql::ObSQLSessionInfo &session);
    inline bool is_inited() const { return NULL != mem_context_; }
    void reset()
    {
      piece_map_.reuse();
      if (NULL != mem_context_) {
        DESTROY_CONTEXT(mem_context_);
        mem_context_ = NULL;
      }
    }
    // piece
    int make_piece(int32_t stmt_id, 
                   uint16_t param_id,
                   ObPiece *&piece,
                   sql::ObSQLSessionInfo &session);
    int remove_piece(int64_t key, sql::ObSQLSessionInfo &session);
    int add_piece(ObPiece *piece);
    int get_piece(int32_t stmt_id, uint16_t param_id, ObPiece *&piece);
    int get_piece_buffer(int32_t stmt_id, 
                          uint16_t param_id,
                          int32_t offset, 
                          uint64_t piece_size, 
                          ObPieceBuffer &piece_buf,
                          sql::ObSQLSessionInfo &session);
    //merge
    int get_buffer(int32_t stmt_id, 
                    uint16_t param_id,
                    uint64_t count,
                    uint64_t &length, 
                    common::ObFixedArray<ObSqlString, ObIAllocator> &str_buf,
                    char *is_null_map);
    int get_oracle_buffer(int32_t stmt_id, 
                    uint16_t param_id,
                    uint64_t count,
                    uint64_t &length, 
                    common::ObFixedArray<ObSqlString, ObIAllocator> &str_buf,
                    char *is_null_map);
    int get_mysql_buffer(int32_t stmt_id, 
                    uint16_t param_id,
                    uint64_t &length, 
                    ObSqlString &str_buf);
    inline int64_t get_piece_key(int32_t stmt_id, uint16_t param_id)
    {
      return (((static_cast<int64_t>(stmt_id)) << 32) | param_id);
    }
    int add_piece_buffer(ObPiece *piece, ObPieceMode piece_mode, ObString *buf);
    /* merge ObPieceBuffer.buffer_ into buf , and move & free this ObPieceBuffer from buffer_array_
    * when ObPieceBuffer.is_last_piece() 
    * merge this ObPieceBuffer and finish merge
    */
    int merge_piece_buffer(ObPiece *piece, ObSqlString &buf);
    int make_piece_buffer(ObIAllocator *allocator,
                          ObPieceBuffer *&piece_buffer, 
                          ObPieceMode mode, 
                          ObString *buf);
    int init_piece_cache(sql::ObSQLSessionInfo &session);
    void close_piece(ObPiece *&piece, sql::ObSQLSessionInfo &session);
    ObPieceMode get_piece_mode(int8_t mode);
    inline uint64_t get_length_length(uint64_t length) {
      // store_length
      uint64_t len = 0;
      if (length < (uint64_t) 251) {
        len = 1;
      } else if (length < (uint64_t) 0X10000) {
        len = 3;
      } else if (length < (uint64_t) 0X1000000) {
        len = 4;
      } else if (length < UINT64_MAX) {
        len = 9;
      } else if (length == UINT64_MAX) {
        len = 1;
      }
      return len;
    }
  public:
    lib::MemoryContext mem_context_;
    typedef common::hash::ObHashMap<int64_t, ObPiece*,
                                    common::hash::NoPthreadDefendMode> PieceMap;
    PieceMap piece_map_;
};

} // end of namespace observer
} // end of namespace oceanbase

#endif //OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_PIECE_DATA_H_
