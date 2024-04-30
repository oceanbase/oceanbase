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

#ifndef OCEANBASE_COMMON_OB_SCANNER_
#define OCEANBASE_COMMON_OB_SCANNER_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/session/ob_session_val_map.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/engine/ob_exec_feedback_info.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
struct ObExpr;
struct ObEvalCtx;
}
namespace common
{
class ObIAllocator;
// the result of remote sql query
class ObScanner
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t DEFAULT_MAX_SERIALIZE_SIZE
    = (OB_MAX_PACKET_LENGTH - OB_MAX_ROW_KEY_LENGTH * 2 - 1024);
  typedef ObRowStore::Iterator Iterator;
public:
  ObScanner(const char *label = ObModIds::OB_NEW_SCANNER,
            ObIAllocator *allocator = NULL,
            int64_t mem_size_limit = DEFAULT_MAX_SERIALIZE_SIZE,
            uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
            bool use_row_compact = true);
  ObScanner(ObIAllocator &allocator,
            const char *label = ObModIds::OB_NEW_SCANNER,
            int64_t mem_size_limit = DEFAULT_MAX_SERIALIZE_SIZE,
            uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
            bool use_row_compact = true);
  virtual ~ObScanner();
  int init(int64_t mem_size_limit = DEFAULT_MAX_SERIALIZE_SIZE);

  void reuse();
  void reset();

  bool is_inited() const {return is_inited_;}
  int add_row(const ObNewRow &row);

  // Try add row to row store if the memory not exceeded after row added.
  // return OB_SUCCESS too when row not added (%row_added flag set to false).
  //
  // Unlike the add_row() interface we the use %row_added to indicate scanner is full or not,
  // instead of return OB_SIZE_OVERFLOW, because the expr evaluation may got the same error.
  int try_add_row(const common::ObIArray<sql::ObExpr*> &exprs,
                  sql::ObEvalCtx *ctx,
                  bool &row_added);

  Iterator begin() const { return row_store_.begin(); }

  bool is_empty() const { return row_store_.is_empty() && datum_store_.is_empty(); }

  /// Get the total size of the space occupied by the data (including the unused buffer)
  int64_t get_used_mem_size() const { return row_store_.get_used_mem_size(); }
  int64_t get_data_size() const { return row_store_.get_data_size(); }

  int64_t get_row_count() const { return row_store_.get_row_count(); }
  int64_t get_col_count() const { return row_store_.get_col_count(); }
  ObRowStore &get_row_store() { return row_store_; }
  sql::ObChunkDatumStore &get_datum_store() { return datum_store_; }
  const sql::ObChunkDatumStore &get_datum_store() const { return datum_store_; }
  int set_extend_info(const ObString &extend_info);
  const ObString &get_extend_info() const { return extend_info_; }

  void set_label(const char *label) { row_store_.set_label(label); }
  const char *get_label() const { return row_store_.get_label(); }

  void set_mem_block_size(int64_t block_size) { row_store_.set_block_size(block_size); }
  int64_t get_mem_block_size() const { return row_store_.get_block_size(); }

  //Do not modify the mem_size_limit during the usage of the scanner
  int64_t get_mem_size_limit() const { return mem_size_limit_; }

  void set_affected_rows(int64_t affacted_rows) { affected_rows_ = affacted_rows; }
  int64_t get_affected_rows() const { return affected_rows_; }

  int set_row_matched_count(int64_t row_count);
  int64_t get_row_matched_count() const { return row_matched_count_; }
  int set_row_duplicated_count(int64_t row_count);
  int64_t get_row_duplicated_count() const { return row_duplicated_count_; }

  void set_last_insert_id_to_client(const int64_t last_insert_id) { last_insert_id_to_client_ = last_insert_id; }
  int64_t get_last_insert_id_to_client() const { return last_insert_id_to_client_; }
  void set_last_insert_id_session(const int64_t last_insert_id) { last_insert_id_session_ = last_insert_id; }
  int64_t get_last_insert_id_session() const { return last_insert_id_session_; }
  inline void set_last_insert_id_changed(const bool changed) { last_insert_id_changed_ = changed; }
  inline bool get_last_insert_id_changed() const { return last_insert_id_changed_; }
  inline void set_is_result_accurate(bool is_accurate) { is_result_accurate_ = is_accurate; }
  inline bool is_result_accurate() const {return is_result_accurate_;}

  void set_found_rows(int64_t found_rows) { found_rows_ = found_rows; }
  int64_t get_found_rows() const { return found_rows_; }

  void set_err_code(int err_code) { rcode_.rcode_ = err_code; }
  int get_err_code() const { return rcode_.rcode_; }

  void set_memstore_read_row_count(int64_t memstore_read_row_count) { memstore_read_row_count_ = memstore_read_row_count; }
  int64_t get_memstore_read_row_count() const { return memstore_read_row_count_; }

  void set_ssstore_read_row_count(int64_t ssstore_read_row_count) { ssstore_read_row_count_ = ssstore_read_row_count; }
  int64_t get_ssstore_read_row_count() const { return ssstore_read_row_count_; }

  void log_user_error_and_warn() const;
  const transaction::ObTxExecResult &get_trans_result() const { return trans_result_; }
  transaction::ObTxExecResult &get_trans_result() { return trans_result_; }
  /**
   * set error message
   * @param msg [in]
   */
  void store_err_msg(const common::ObString &msg);
  const char *get_err_msg() const { return rcode_.msg_; }
  int store_warning_msg(const ObWarningBuffer &wb);
  const sql::ObSessionValMap& get_session_var_map() const {return user_var_map_;}
  int set_session_var_map(const sql::ObSQLSessionInfo *p_session_info);

  /// copy ObScanner
  int assign(const ObScanner &other_scanner);

  /// dump all data for debug purpose
  void dump() const;
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    row_store_.set_tenant_id(tenant_id);
  }
  ObIArray<sql::ObTableRowCount> &get_table_row_counts() { return table_row_counts_; }
  int assign_implicit_cursor(const common::ObIArray<sql::ObImplicitCursorInfo> &implicit_cursors)
  { return implicit_cursors_.assign(implicit_cursors); }
  const common::ObIArray<sql::ObImplicitCursorInfo> &get_implicit_cursors() const
  { return implicit_cursors_; }
  sql::ObExecFeedbackInfo &get_feedback_info() { return fb_info_; }
  //NEED_SERIALIZE_AND_DESERIALIZE;
  //TODO(yaoying.yyy): set "is_result_accurate" as macro after library is merged
  TO_STRING_KV(N_ROWSTORE, row_store_,
               N_MEM_LIMIT, mem_size_limit_,
               N_AFFECTED_ROWS, affected_rows_,
               K_(row_matched_count),
               K_(row_duplicated_count),
               K_(found_rows),
               N_LAST_INSERT_ID_TO_CLIENT, last_insert_id_to_client_,
               N_LAST_INSERT_ID_SESSION, last_insert_id_session_,
               K_(is_result_accurate),
               K_(trans_result),
               K_(implicit_cursors),
               K_(rcode),
               K_(memstore_read_row_count),
               K_(ssstore_read_row_count));
protected:
  ObRowStore row_store_;
  int64_t mem_size_limit_;  /**< memory size of row store */
  uint64_t tenant_id_;
  const char *label_;
  int64_t affected_rows_;   /**< affected rows of modify operation, e.g. UPDATE, DELETE etc.*/
  int64_t last_insert_id_to_client_;  /**< last auto-increment column value */
  int64_t last_insert_id_session_;    /**< last auto-increment column value in session */
  bool    last_insert_id_changed_;  /**< last_insert_id(#) called*/
  int64_t found_rows_;
  sql::ObSessionValMap user_var_map_;
  bool is_inited_;
  //use for mysql_info
  int64_t row_matched_count_;
  int64_t row_duplicated_count_;
  // Any additional statistical information that needs to be sent through the Scanner, it is recommended to use the extend_info_ frame
  ObString extend_info_;
  ObArenaAllocator inner_allocator_;
  //used for approximate calculation
  bool is_result_accurate_;
  transaction::ObTxExecResult trans_result_;
  common::ObSEArray<sql::ObTableRowCount, 4> table_row_counts_;
  common::ObFixedArray<sql::ObImplicitCursorInfo, common::ObIAllocator> implicit_cursors_;
  sql::ObChunkDatumStore datum_store_;
  obrpc::ObRpcResultCode rcode_;
  sql::ObExecFeedbackInfo fb_info_;
  int64_t memstore_read_row_count_;
  int64_t ssstore_read_row_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObScanner);
};

class ObScannerable
{
public:
  ObScannerable() {};
  virtual ~ObScannerable() {};
  virtual int get_scanner(ObIAllocator &allocator, ObScanner &scanner) = 0;
};

/*class ObSeekScannerable : public ObScannerable
{
public:
  ObSeekScannerable() {};
  virtual ~ObSeekScannerable() {};
  virtual int get_scanner(common::ObScanner &scanner)
  {
    return get_scanner(scanner, 0);
  }

  virtual int get_scanner(common::ObScanner &scanner, int64_t offser) = 0;
};*/

inline void ObScanner::store_err_msg(const common::ObString &msg)
{
  int ret = OB_SUCCESS;
  if (!msg.empty()) {
    //The reason for using databuff_printf here is that when databuff_printf encounters a buffer overflow, it will ensure that the buffer ends with'\0' to ensure the safety of print
    if (OB_FAIL(databuff_printf(rcode_.msg_, common::OB_MAX_ERROR_MSG_LEN, "%.*s", msg.length(), msg.ptr()))) {
      SHARE_LOG(WARN, "store err msg failed", K(ret), K(msg));
      if (common::OB_SIZE_OVERFLOW == ret) {
        rcode_.msg_[common::OB_MAX_ERROR_MSG_LEN - 1] = '\0';
      }
    }
  } else {
    rcode_.msg_[0] = '\0';
  }
}

} /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_SCANNER_ */
