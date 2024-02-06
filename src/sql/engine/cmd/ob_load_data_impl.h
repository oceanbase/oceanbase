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

#ifndef OCEANBASE_SQL_LOAD_DATA_IMPL_H_
#define OCEANBASE_SQL_LOAD_DATA_IMPL_H_

#ifndef TIME_STAT_ON
#define TIME_STAT_ON
#endif

#include "share/ob_define.h"
#include "lib/file/ob_file.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_array_hash_map.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/container/ob_bit_set.h"
#include "lib/utility/ob_utility.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace sql
{

class ObLoadDataStmt;
class ObSqlExpressionFactory;
struct ObPartBufMgrHashInfo;
enum class ObLoadTaskResultFlag;
class ObLoadFileBuffer;
class ObCSVParser;
class ObLoadEscapeSM;
class ObCSVGeneralParser;

typedef common::hash::ObHashMap<common::ObString, int64_t> FileFieldIdxHashMap;
typedef common::hash::ObHashMap<common::ObAddr, int64_t> ServerTimestampHashMap;
typedef common::hash::ObBuildInHashMap<ObPartBufMgrHashInfo, 100> PartitionBufferHashMap;

struct ObLoadDataReplacedExprInfo
{
  ObLoadDataReplacedExprInfo() :
    replaced_expr(NULL),
    correspond_file_field_idx(OB_INVALID_INDEX_INT64)
  {}
  ObConstRawExpr *replaced_expr;//column refs and user variables will be replaced into an const expr
  int64_t correspond_file_field_idx;//the index of column in the load file
  TO_STRING_KV(KPC(replaced_expr), K(correspond_file_field_idx));
};

struct ObLoadTableColumnDesc
{
  ObLoadTableColumnDesc(): column_id_(common::OB_INVALID_ID),
                      is_set_values_(false),
                      expr_value_(NULL),
                      column_type_(common::ObMaxType),
                      array_ref_idx_(common::OB_INVALID_INDEX_INT64) {}
  common::ObString column_name_;
  uint64_t column_id_; //for compare
  bool is_set_values_; //values will get from set assignments
  /* is_set_values_ indicates this value is finally from set assignments
   * e.g. LOAD DATA INFILE (c1, c2) SET c1 = xxx, c3 = xxx;
   * --> c1 : is_set_values_ = TRUE
   * --> c2 : is_set_values_ = FALSE
   * --> c3 : is_set_values_ = TRUE
   */
  ObRawExpr *expr_value_; //the expr of set value
  common::ColumnType column_type_;
  int64_t array_ref_idx_; //index of source data array (field_str_ or set_expr_str_)
  TO_STRING_KV(K_(column_name), K_(column_id), K_(is_set_values), K_(array_ref_idx));
};

class ObInsertValueGenerator
{
public:
  ObInsertValueGenerator() : cs_type_(common::CS_TYPE_INVALID), data_buffer_(NULL), sql_mode_(0) {}
  int init(ObSQLSessionInfo &session, ObLoadFileBuffer* data_buffer, ObSchemaGetterGuard *schema_guard);
  int set_params(common::ObString &insert_header, common::ObCollationType cs_type, int64_t sql_mode);
  int fill_field_expr(common::ObIArray<ObCSVGeneralParser::FieldValue> &field_values,
                      const common::ObBitSet<> &string_values);
  int gen_insert_values(common::ObIArray<common::ObString> &insert_values,
                        common::ObStringBuf &str_buf);
  int gen_insert_sql(ObSqlString &insert_sql);

  common::ObIArray<ObRawExpr *> &get_insert_exprs() { return insert_exprs_; }
  common::ObIArray<ObRawExpr *> &get_field_exprs() { return field_exprs_; }
  common::ObCollationType get_cs_type() { return cs_type_; }
private:
  common::ObCollationType cs_type_;
  ObString insert_header_;
  ObLoadFileBuffer *data_buffer_;
  ObRawExprPrinter expr_printer_;
  common::ObSEArray<ObRawExpr *, 16> insert_exprs_;
  common::ObSEArray<ObRawExpr *, 16> field_exprs_;
  int64_t sql_mode_;
};

//存储row_cnt行序列化的数据，每行的序列化数据是一个SEArray
struct ObDataFrag : common::ObLink
{
  ObDataFrag() = delete;
  ObDataFrag(int64_t struct_size) :
    shuffle_task_id(OB_INVALID_INDEX_INT64),
    frag_size(struct_size - sizeof(ObDataFrag)),
    frag_pos(0),
    row_cnt(0),
    orig_data_size(0) {}

  static const int64_t DEFAULT_STRUCT_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t MAX_ROW_COUNT = 1024;
  int64_t get_remain() { return frag_size - frag_pos; }
  char *get_current() { return data + frag_pos; }
  void add_pos(int64_t size) { frag_pos += size; }
  void add_row_cnt(int64_t cnt) { row_cnt += cnt; }
  void add_orig_data_size(int64_t size) { orig_data_size += size; }

  int64_t shuffle_task_id;
  int64_t frag_size;
  int64_t frag_pos;
  int64_t row_cnt;
  int64_t orig_data_size; //original data size actually read to the data frag
  char data[];
  TO_STRING_KV(K(shuffle_task_id), K(frag_size), K(frag_pos), K(row_cnt));
};

class ObPartDataFragMgr
{
public:
  ObPartDataFragMgr(ObDataFragMgr &data_frag_mgr, uint64_t tenant_id, ObTabletID tablet_id)
    : data_frag_mgr_(data_frag_mgr),
      tenant_id_(tenant_id),
      tablet_id_(tablet_id),
      total_row_consumed_(0),
      total_row_proceduced_(0) {}
  ~ObPartDataFragMgr() {}
  LINK(ObPartDataFragMgr, part_datafrag_hash_link_);

  //common::ObTabletID &get_part_key() { return tablet_id_; }
  common::ObAddr &get_leader_addr() { return leader_addr_; }

  int add_datafrag(ObDataFrag *frag) { return queue_.push(frag); }
  inline bool has_data(int64_t batch_row_count) {
    return  remain_row_count() >= batch_row_count;
  }
  inline int64_t remain_row_count() {
    return ATOMIC_LOAD(&total_row_proceduced_) - total_row_consumed_;
  }
  int reuse() {
    //todo
    return common::OB_SUCCESS;
  }
  int clear();

  int update_part_location(ObExecContext &ctx);

  ObDataFragMgr &data_frag_mgr_;

  uint64_t tenant_id_;
  ObTabletID tablet_id_;

  int64_t total_row_consumed_;

  common::ObAddr leader_addr_;

  //multi-thread accessed:
  volatile int64_t total_row_proceduced_;
  common::ObSpLinkQueue queue_;

public:
  //for batch task
  struct InsertTaskSplitPoint {
    InsertTaskSplitPoint() { reset(); }
    TO_STRING_KV(K(frag_row_pos_), K(frag_data_pos_));
    inline void reset() { frag_row_pos_ = 0; frag_data_pos_ = 0; }
    int64_t frag_row_pos_;
    int64_t frag_data_pos_;
  };

  //int64_t get_batch_count() { return insert_task_split_points_.count(); }
  int prepare_insert_task(int64_t batch_row_count = 1000);
  int next_insert_task(int64_t batch_row_count, ObInsertTask &task);
  int free_frags();
  TO_STRING_KV(K(frag_free_list_), K(queue_top_begin_point_));

private:
  /**
   * @brief 返回frag中第row_num行的起始位置
   */
  int rowoffset2pos(ObDataFrag *frag, int64_t row_num, int64_t &pos);

  InsertTaskSplitPoint queue_top_begin_point_; //begin cursor of queue_.top()
  //common::ObSEArray<InsertTaskSplitPoint, 8> insert_task_split_points_;
  common::ObSEArray<ObDataFrag *, 32> frag_free_list_;

};

struct ObPartDataFragHash
{
  typedef const ObTabletID &Key;
  typedef ObPartDataFragMgr Value;
  typedef ObDLList(ObPartDataFragMgr, part_datafrag_hash_link_) ListHead;
  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(int64_t), 0); }
  static Key key(Value const *value) { return value->tablet_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

class ObDataFragMgr {
public:
  int init(ObExecContext &ctx, uint64_t table_id);
  int get_part_datafrag(ObTabletID tablet_id, ObPartDataFragMgr *&part_datafrag_mgr);
  int64_t get_total_part_cnt() { return total_part_cnt_; }
  int create_datafrag(ObDataFrag *&frag, int64_t min_len);
  void distory_datafrag(ObDataFrag *frag);
  int free_unused_datafrag();
  int clear_all_datafrag();
  common::ObIArray<ObTabletID> &get_tablet_ids() { return tablet_ids_; }
  const common::ObBitSet<> &get_part_bitset() { return part_bitset_; }
  int64_t get_total_allocated_frag_count() const { return ATOMIC_LOAD(&total_alloc_cnt_); }
  int64_t get_total_freed_frag_count() const { return total_free_cnt_; }
  TO_STRING_KV(K_(total_part_cnt),
               "total_alloc_cnt", get_total_allocated_frag_count(),
               K_(total_free_cnt));
private:

  common::ObMemAttr attr_;
  int64_t total_part_cnt_;
  volatile int64_t total_alloc_cnt_;
  int64_t total_free_cnt_;
  common::ObSEArray<ObTabletID, 64> tablet_ids_;
  common::ObBitSet<> part_bitset_;

  //ObTabletID-> ObPartDataFragMgr
  common::hash::ObBuildInHashMap<ObPartDataFragHash, 100> part_datafrag_map_;
};

//one buffer info for one partition
class ObPartitionBufferCtrl
{
public:
  explicit ObPartitionBufferCtrl(ObTabletID tablet_id): tablet_id_(tablet_id), buffer_(NULL) {}
  ~ObPartitionBufferCtrl() {}

  LINK(ObPartitionBufferCtrl, buffer_hash_link_); //hash link list

  template <typename T>
  OB_INLINE void set_buffer(T *buffer) { buffer_ = reinterpret_cast<void *>(buffer); }
  template<typename T>
  OB_INLINE T* get_buffer() { return reinterpret_cast<T *>(buffer_); }

  TO_STRING_KV(K_(tablet_id), KP_(buffer));
public:
  ObTabletID tablet_id_; //hash key
private:
  void* buffer_;
} CACHE_ALIGNED;

struct ObPartBufMgrHashInfo
{
  typedef const ObTabletID &Key;
  typedef ObPartitionBufferCtrl Value;
  typedef ObDLList(ObPartitionBufferCtrl, buffer_hash_link_) ListHead;
  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(ObTabletID), 0); }
  static Key key(Value const *value) { return value->tablet_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

//========================
/*解决table location计算使用的内存无法释放问题*/
class ObAllocatorSwitch : public common::ObIAllocator
{
public:
  ObAllocatorSwitch() : cur_alloc_(&permanent_alloc_) {}
  void switch_permanent() { cur_alloc_ = &permanent_alloc_; }
  void switch_temporary() { cur_alloc_ = &temporary_alloc_; }
  inline
  virtual void *alloc(const int64_t size)
  { return cur_alloc_->alloc(size); }

  inline
  virtual void* alloc(const int64_t size, const ObMemAttr &attr)
  { return cur_alloc_->alloc(size, attr); }

  inline
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
  { return cur_alloc_->realloc(ptr, size, attr); }

  inline
  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
  { return cur_alloc_->realloc(ptr, oldsz, newsz); }

  inline
  virtual void free(void *ptr)
  { return cur_alloc_->free(ptr); }

  inline
  virtual int64_t total() const
  { return cur_alloc_->total(); }

  inline
  virtual int64_t used() const
  { return cur_alloc_->used(); }

  inline
  virtual void reset() { return cur_alloc_->reset(); }

  inline
  virtual void reuse() { return cur_alloc_->reuse(); }

  inline
  virtual void set_attr(const ObMemAttr &attr) { cur_alloc_->set_attr(attr); }

  TO_STRING_KV("cur_alloc_", (cur_alloc_ == &permanent_alloc_) ? "permanent" : "temporary");

private:
  common::ObIAllocator *cur_alloc_;
  ObArenaAllocator permanent_alloc_;
  ObArenaAllocator temporary_alloc_;
};

class ObLoadFileBuffer
{
public:
  static const int64_t MAX_BUFFER_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
public:
  ObLoadFileBuffer() = delete;
  ObLoadFileBuffer(int64_t buffer_size) : pos_(0), buffer_size_(buffer_size) {}
  bool is_valid() const { return pos_ > 0; }
  char *current_ptr() { return buffer_ + pos_; }
  char *begin_ptr() { return buffer_; }
  void update_pos(int64_t len) { pos_ += len; }
  int64_t get_remain_len() { return buffer_size_ - pos_; }
  int64_t get_data_len() { return pos_; }
  int64_t get_buffer_size() { return buffer_size_; }
  int64_t *get_pos() { return &pos_; }
  void reset() { pos_ = 0; }
  TO_STRING_KV(K_(pos), K_(buffer_size));
private:
  int64_t pos_;
  int64_t buffer_size_;
  char buffer_[];
};

struct ObCSVFormats {
  ObCSVFormats() :
    field_term_char_(0),
    line_term_char_(0),
    enclose_char_(0),
    escape_char_(0),
    null_column_fill_zero_string_(false),
    is_simple_format_(false),
    is_line_term_by_counting_field_(false)
  {}
  void init(const ObDataInFileStruct &file_formats);
  int64_t field_term_char_;
  int64_t line_term_char_;
  int64_t enclose_char_;
  int64_t escape_char_;
  /* for empty column, oracle mode do insert '';
   * mysql mode do insert '0' for nonstring-type column, '' for string-type column
   * */
  bool null_column_fill_zero_string_;
  bool is_simple_format_;
  bool is_line_term_by_counting_field_;
};

class ObLoadFileDataTrimer
{
public:
  ObLoadFileDataTrimer() : incomplate_data_(NULL), incomplate_data_len_(0), lines_cnt_(0) {}
  int init(common::ObIAllocator &allocator, const ObCSVFormats &formats);
  //for debug
  ObString get_incomplate_data_string() {
    return ObString(static_cast<int32_t>(incomplate_data_len_), incomplate_data_);
  }

  int backup_incomplate_data(ObLoadFileBuffer &buffer, int64_t valid_data_len);
  int recover_incomplate_data(ObLoadFileBuffer &buffer);
  bool has_incomplate_data() const { return incomplate_data_len_ > 0; }
  int64_t get_lines_count() const { return lines_cnt_; }
  void commit_line_cnt(int64_t line_cnt) { lines_cnt_ += line_cnt; }
private:
  ObCSVFormats formats_;//TODO [load data] change to ObInverseParser(formats)
  char *incomplate_data_;
  int64_t incomplate_data_len_;
  int64_t lines_cnt_;
};

class ObCSVParser {
public:
  static const char *ZERO_STRING;

public:
  ObCSVParser() :
    is_fast_parse_(false),
    total_field_nums_(0)
  {
    reuse();
  }
  void reuse() {
    is_last_buf_ = false;
    cur_pos_ = NULL;
    cur_field_begin_pos_ = NULL;
    cur_field_end_pos_ = NULL;
    cur_line_begin_pos_ = NULL;
    buf_begin_pos_ = NULL;
    buf_end_pos_ = NULL;
    last_end_enclosed_ = NULL;
    field_id_ = 0;
    in_enclose_flag_ = false;
    is_escaped_flag_ = false;
  }
  int init(int64_t file_column_nums,
           const ObCSVFormats &formats,
           const common::ObBitSet<> &string_type_column);
  void next_buf(char *buf_start, const int64_t buf_len, bool is_last_buf = false);
  int next_line(bool &yield_line);
  static int fast_parse_lines(ObLoadFileBuffer &buffer,
                              ObCSVParser &parser,
                              bool is_last_buf,
                              int64_t &valid_len,
                              int64_t &line_count);
  int64_t get_complete_lines_len() { return cur_line_begin_pos_ - buf_begin_pos_; }


  common::ObIArray<ObString> &get_line_store()
  {
    return values_in_line_;
  }

  ObCSVFormats &get_format() { return formats_; }
  bool is_fast_parse() { return is_fast_parse_; }
  void set_fast_parse() { is_fast_parse_ = true; }

private:
  bool is_terminate_char(char cur_char, char *&cur_pos, bool &is_line_term);
  bool is_enclosed_field_start(char *cur_pos, char &cur_char);
  void handle_one_field(char *field_end_pos, bool has_escaped);
  void deal_with_empty_field(ObString &field_str, int64_t index);
  //void deal_with_field_with_escaped_chars(ObString &field_str);
  int deal_with_irregular_line();
  void remove_enclosed_char(char *&cur_field_end_pos);
private:
  // parsing style
  bool is_fast_parse_;
  ObCSVFormats formats_;
  common::ObBitSet<> string_type_column_;
  // parsing state variables
  bool is_last_buf_;
  char *cur_pos_;
  char *cur_field_begin_pos_;
  char *cur_field_end_pos_;
  char *cur_line_begin_pos_;
  char *buf_begin_pos_;
  char *buf_end_pos_;
  char *last_end_enclosed_;
  int64_t field_id_;
  bool in_enclose_flag_;
  bool is_escaped_flag_;
  int64_t total_field_nums_;
  //ObLoadEscapeSM escape_sm_;
  //parsing result: the pointers of each value in one line
  common::ObSEArray<ObString, 1> values_in_line_;
};

struct ObParserErrRec {
  int64_t row_offset_in_task;
  int ret;
  TO_STRING_KV(K(row_offset_in_task), K(ret));
};

struct ObShuffleTaskHandle {
  ObShuffleTaskHandle(ObDataFragMgr &main_datafrag_mgr,
                      common::ObBitSet<> &main_string_values,
                      uint64_t tenant_id);
  ~ObShuffleTaskHandle();
  ObArenaAllocator allocator;
  ObDesExecContext exec_ctx;
  ObLoadFileBuffer *data_buffer;
  ObLoadFileBuffer *escape_buffer;
  ObCSVGeneralParser parser;
  common::ObSEArray<ObRawExpr *, 16> field_exprs;
  common::ObSEArray<ObRawExpr *, 16> insert_exprs;
  ObInsertValueGenerator generator;
  ObTempExpr *calc_tablet_id_expr;
  ObNewRow row_in_file;
  ObDataFragMgr &datafrag_mgr;
  common::ObBitSet<> &string_values;
  ObShuffleResult result;
  ObSEArray<ObParserErrRec, 16> err_records;
  TO_STRING_KV("task_id", result.task_id_);
};


OB_INLINE void ObCSVParser::remove_enclosed_char(char *&cur_field_end_pos)
{
  cur_field_end_pos--;
  cur_field_begin_pos_++;
}

OB_INLINE bool ObCSVParser::is_terminate_char(char cur_char, char *&cur_pos, bool &is_line_term)
{
  bool ret_bool = false;
  bool is_field_term = (cur_char == formats_.field_term_char_);
  is_line_term = (cur_char == formats_.line_term_char_);

  if ((is_field_term || is_line_term) && !is_escaped_flag_) {
    if (!in_enclose_flag_) {
      ret_bool = true; //return true
    } else {
      //with in_enclose_flag_ = true, a term char is valid only if an enclosed char before it
      if (last_end_enclosed_ == cur_pos - 1) {
        remove_enclosed_char(cur_pos);
        ret_bool = true;  //return true
      } else {
        //return false
      }
    }
  } else {
    //return false
  }
  return ret_bool;
}

OB_INLINE bool ObCSVParser::is_enclosed_field_start(char *cur_pos, char &cur_char)
{
  return static_cast<int64_t>(cur_char) == formats_.enclose_char_
          //anything between a pair of enclosed chars will be regarded as string data of one field
          && !in_enclose_flag_
          //enclosed field start must be the first char at the beginning of one field,
          //so cur_char is impossible to be escaped
          && cur_pos == cur_field_begin_pos_;
}

OB_INLINE void ObCSVParser::handle_one_field(char *field_end_pos, bool has_escaped)
{
  if (OB_LIKELY(field_id_ < total_field_nums_)) {
    int32_t str_len = static_cast<int32_t>(field_end_pos - cur_field_begin_pos_);
    if (OB_UNLIKELY(str_len <= 0)) {
      deal_with_empty_field(values_in_line_.at(field_id_), field_id_);
    } else {
      if (!in_enclose_flag_
          && ((str_len == 1 && *cur_field_begin_pos_ == 'N' && has_escaped && cur_pos_ - cur_field_begin_pos_ == 2)
              || (formats_.enclose_char_ != INT64_MAX && !has_escaped
                  && str_len == 4 && 0 == MEMCMP(cur_field_begin_pos_, "NULL", 4)))) { 
        //用一个特殊的flag表示;
        values_in_line_.at(field_id_).assign_ptr(&ObLoadDataUtils::NULL_VALUE_FLAG, 1);
      } else {
        values_in_line_.at(field_id_).assign_ptr(cur_field_begin_pos_, str_len);
      }
    }
  } else {
    //ignored
  }
}

class ObLoadDataBase
{
public:
  ObLoadDataBase() {}
  virtual ~ObLoadDataBase() {}
  //utils
  static constexpr double MEMORY_LIMIT_THRESHOLD = 1.02;
  static const char *SERVER_TENANT_MEMORY_EXAMINE_SQL;

  static int generate_fake_field_strs(common::ObIArray<common::ObString> &file_col_values,
                                      common::ObIAllocator &allocator, const char id_char);
  static int calc_param_offset(const common::ObObj &param_a,
                               const common::ObObj &param_b, int64_t &idx);
  static int construct_insert_sql(common::ObSqlString &insert_sql,
                                  const ObString &q_name,
                                  common::ObIArray<ObLoadTableColumnDesc> &desc,
                                  common::ObIArray<common::ObString> &insert_values,
                                  int64_t num_rows);
  static void set_one_field_objparam(common::ObObjParam &dest_param,
                                     const common::ObString &field_str);

  static int make_parameterize_stmt(ObExecContext &ctx,
                                    common::ObSqlString &insertsql,
                                    ParamStore &param_store,
                                    ObInsertStmt *&insert_stmt);

  static int memory_check_remote(uint64_t tenant_id, bool &need_wait_minor_freeze);
  static int memory_wait_local(ObExecContext &ctx,
                               const ObTabletID &tablet_id,
                               ObAddr &server_addr,
                               int64_t &total_wait_secs,
                               bool &is_leader_changed);

  static int pre_parse_lines(ObLoadFileBuffer &buffer,
                             ObCSVGeneralParser &parser,
                             bool is_last_buf,
                             int64_t &valid_len,
                             int64_t &line_count);

  static void field_to_obj(common::ObObj &obj,
                           const ObCSVGeneralParser::FieldValue &field,
                           const common::ObCollationType cs_type,
                           bool is_string_type_column) {
    if (field.is_null_
        || (0 == field.len_ && lib::is_oracle_mode())) {
      obj.set_null();
    } else {
      obj.set_varchar(field.ptr_, field.len_);
      obj.set_collation_type(cs_type);
    }
  }

  virtual int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) = 0;
};

struct ObFileReadCursor {
  ObFileReadCursor () { reset(); }
  void reset() {
    read_counter_ = 0;
    file_offset_ = 0;
    read_size_ = 0;
    is_end_file_ = false;
  }
  bool inline is_end_file() const { return is_end_file_; }
  int64_t inline get_total_read_MBs() { return file_offset_ >> 20; }
  int64_t inline get_total_read_GBs() { return file_offset_ >> 30; }
  void commit_read() {
    file_offset_ += read_size_;
    read_size_ = 0;
    read_counter_++;
  }
  TO_STRING_KV(K(read_counter_), K(file_offset_), K(read_size_), K(is_end_file_));
  int64_t read_counter_;
  int64_t file_offset_;
  int64_t read_size_;
  bool is_end_file_;
};

/**
 * @brief The ObLoadServerMgr struct
 *        manage server status
 */
struct ObLoadServerInfo
{
  ObLoadServerInfo()
    : last_memory_limit_response(0)
  {}

  ObAddr addr;
  ObSEArray<ObPartDataFragMgr*, 64> part_datafrag_group;
  int64_t last_memory_limit_response;
  TO_STRING_KV(K(addr));
};

/**
 * @brief load data shuffle parallel implementation
 */
class ObLoadDataSPImpl : public ObLoadDataBase
{
public:
  enum class TaskType {
    InvalidTask = -1,
    ShuffleTask = 0,
    InsertTask,
  };
  struct ToolBox {
    ToolBox() : device_handle_(NULL), fd_(), expr_buffer(nullptr) {}
    int init(ObExecContext &ctx, ObLoadDataStmt &load_stmt);
    int build_calc_partid_expr(ObExecContext &ctx,
                               ObLoadDataStmt &load_stmt,
                               ObTempExpr *&calc_tablet_id_expr);
    int release_resources();

    //modules
    ObFileReader file_reader;
    ObIODevice* device_handle_;
    ObIOFd fd_;
    ObFileAppender file_appender;
    ObFileReadCursor read_cursor;
    ObLoadFileDataTrimer data_trimer;
    ObInsertValueGenerator generator;
    ObDataFragMgr data_frag_mgr;

    //running control
    ObParallelTaskController shuffle_task_controller;
    ObParallelTaskController insert_task_controller;
    ObConcurrentFixedCircularArray<ObShuffleTaskHandle *> shuffle_task_reserve_queue;
    ObConcurrentFixedCircularArray<ObInsertTask *> insert_task_reserve_queue;
    common::ObArrayHashMap<ObAddr, int64_t> server_last_available_ts;
    common::ObArrayHashMap<ObAddr, ObLoadServerInfo*> server_info_map;
    common::ObSEArray<ObLoadServerInfo*, 16> server_infos;

    //exec params
    bool is_oracle_mode;
    int64_t tenant_id;
    int64_t max_cpus; //real cpu num of a tenant
    int64_t num_of_file_column;
    int64_t num_of_table_column;
    int64_t parallel;
    int64_t batch_row_count;
    int64_t data_frag_mem_usage_limit; //limit = data_frag_mem_usage_limit * MAX_BUFFER_SIZE
    int64_t file_size;
    int64_t ignore_rows;
    ObCSVFormats formats;
    ObCSVGeneralParser parser;
    common::ObBitSet<> string_type_column_bitset;
    common::ObAddr self_addr;
    ObLoadDupActionType insert_mode;
    ObLoadFileLocation load_file_storage;
    ObLoadDataGID gid;
    int64_t txn_timeout;
    ObLoadDataStat *job_status;


    //temp data
    ObLoadFileBuffer *expr_buffer;
    common::ObSEArray<ObString, 1> insert_values;
    common::ObSEArray<ObString, 1> field_values_in_file;
    ObPhysicalPlan plan;
    int64_t wait_secs_for_mem_release;
    int64_t affected_rows;
    int64_t insert_rt_sum;
    int64_t suffle_rt_sum;
    common::ObSEArray<int64_t, 1> file_buf_row_num;
    int64_t last_session_check_ts;
    common::ObSEArray<ObLoadTableColumnDesc, 16> insert_infos;

    //debug values
    int64_t insert_dispatch_rows;
    int64_t insert_task_count;
    int64_t handle_returned_insert_task_count;

    //prepared data
    common::ObString insert_stmt_head_buff;
    common::ObString exec_ctx_serialized_data;
    common::ObString log_file_name;
    common::ObString load_info;
    common::ObSEArray<ObShuffleTaskHandle *, 64> shuffle_resource;
    common::ObSEArray<ObInsertTask *, 64> insert_resource;
    common::ObSEArray<ObAllocatorSwitch *, 64> ctx_allocators;

  };
public:
  ObLoadDataSPImpl() {}
  ~ObLoadDataSPImpl() {}
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt);

  int shuffle_task_gen_and_dispatch(ObExecContext &ctx, ToolBox &box);
  int next_file_buffer(ToolBox &box, ObLoadFileBuffer &data_buffer, int64_t limit = INT64_MAX);
  int handle_returned_shuffle_task(ToolBox &box, ObShuffleTaskHandle &handle);
  int wait_shuffle_task_return(ToolBox &box);

  int insert_task_gen_and_dispatch(ObExecContext &ctx, ToolBox &box);
  int insert_task_send(ObInsertTask *insert_task, ToolBox &box);
  int handle_returned_insert_task(ObExecContext &ctx,
                                  ToolBox &box,
                                  ObInsertTask &insert_task,
                                  bool &need_retry);
  int log_failed_insert_task(ToolBox &box, ObInsertTask &task);
  int wait_insert_task_return(ObExecContext &ctx, ToolBox &box);

  int create_log_file(ToolBox &box);
  int log_failed_line(ToolBox &box,
                      TaskType task_type,
                      int64_t task_id,
                      int64_t line_num,
                      int err_code,
                      ObString err_msg);

  static int exec_shuffle(int64_t task_id, ObShuffleTaskHandle *handle);
  static int exec_insert(ObInsertTask &task, ObInsertResult &result);

private:
  static int gen_load_table_column_desc(ObExecContext &ctx,
                                        ObLoadDataStmt &load_stmt,
                                        common::ObIArray<ObLoadTableColumnDesc> &insert_infos);
  static int copy_exprs_for_shuffle_task(ObExecContext &ctx,
                                         ObLoadDataStmt &load_stmt,
                                         common::ObIArray<ObLoadTableColumnDesc> &insert_infos,
                                         common::ObIArray<ObRawExpr *> &field_exprs,
                                         common::ObIArray<ObRawExpr *> &insert_exprs);

  static int gen_insert_columns_names_buff(ObExecContext &ctx,
                                           const ObLoadArgument &load_args,
                                           common::ObIArray<ObLoadTableColumnDesc> &insert_infos,
                                           common::ObString &data_buff,
                                           bool need_online_osg = false);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataSPImpl);
  // function members
};

/*
class ObLoadDataImpl : public ObLoadDataBase
{
public:

  enum ParseStep {FIND_LINE_START = 0, FIND_LINE_TERM};

  static const int64_t FILE_READ_BUFFER_SIZE = 2 * 1024 * 1024;
  static const int64_t RESERVED_BYTES_SIZE = CACHE_ALIGN_SIZE;
  static const int64_t FILE_BUFFER_NUM = 2; //can not changed
  static const int64_t EXPECTED_EXPR_VARABLES_TOTAL_NUM = COMMON_PARAM_NUM;
  static const char *ZERO_FIELD_STRING;
  static const int64_t INVALID_CHAR = INT64_MAX;

  typedef common::ObBitSet<EXPECTED_INSERT_COLUMN_NUM> FieldFlagBitSet;

  ObLoadDataImpl(): //arguments
                    schema_guard_(share::schema::ObSchemaMgrItem::MOD_LOAD_DATA_IMPL),
                    part_level_(share::schema::PARTITION_LEVEL_ZERO),
                    part_num_(0),
                    initial_step_(FIND_LINE_START),
                    dtc_params_(),
                    flag_line_term_by_counting_field_(false),

                    //array sizes
                    file_column_number_(0),
                    set_assigned_column_number_(0),
                    insert_column_number_(0),
                    params_count_(0),
                    allocated_buffer_cnt_(0),
                    non_string_type_direct_insert_column_count_(0),

                    //statistics
                    total_err_lines_(0),
                    total_buf_read_(0),
                    total_wait_secs_(0),

                    //allocators
                    expr_calc_allocator_("LoadData"),
                    array_allocator_("LoadData"),

                    //tools
                    table_location_(expr_calc_allocator_),
                    task_controller_(),

                    //runtime variables
                    cur_step_(FIND_LINE_START),
                    parsed_line_count_(0),
                    cur_line_begin_pos_(NULL),
                    field_id_(0),
                    cur_field_begin_pos_(NULL),
                    in_enclose_flag_(false),

                    //some fast access
                    field_term_char_(INVALID_CHAR),
                    line_term_char_(INVALID_CHAR),
                    enclose_char_(INVALID_CHAR),
                    escape_char_(INVALID_CHAR),

                    //arrays
                    set_assigns_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    file_field_def_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    replaced_expr_value_index_store_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    valid_insert_column_info_store_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    params_value_index_store_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    insert_column_names_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    allocated_buffer_store_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    expr_value_bitset_(array_allocator_),
                    field_type_bit_set_(array_allocator_),

                    //runtime arrays
                    insert_values_per_line_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    parsed_field_strs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    expr_strs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                    tablet_ids_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_) {}
  virtual ~ObLoadDataImpl() {}
  int init_everything_first(ObExecContext &ctx,
                            const ObLoadDataStmt &load_stmt,
                            ObSQLSessionInfo &session);
  int init_from_load_stmt(const ObLoadDataStmt &load_stmt);
  int init_table_location_via_fake_insert_stmt(ObExecContext &ctx,
                                               ObPhysicalPlanCtx &plan_ctx,
                                               ObSQLSessionInfo &session_info);
  int init_separator_detectors(common::ObIAllocator &allocator);
  int init_data_buf(common::ObIAllocator &allocator);
  bool is_terminate_char(char &cur_char, int64_t &term_char, char *&cur_pos);
  bool is_terminate(char &cur_char, ObKMPStateMachine &term_state_machine, char *&cur_pos);
  bool is_enclosed_field_start(char *cur_pos, char &cur_char);
  void remove_enclosed_char(char *&cur_field_end_pos);
  int collect_insert_row_strings();
  int handle_one_file_buf(ObExecContext &ctx,
                          ObPhysicalPlanCtx &plan_ctx,
                          char *parsing_begin_pos,
                          const int64_t data_len,
                          bool is_eof);
  int handle_one_file_buf_fast(ObExecContext &ctx,
                               ObPhysicalPlanCtx &plan_ctx,
                               char *parsing_begin_pos,
                               const int64_t data_len,
                               bool is_eof);
  int handle_one_line(ObExecContext &ctx,
                      ObPhysicalPlanCtx &plan_ctx);
  int handle_one_line_local(ObPhysicalPlanCtx &plan_ctx);
  void handle_one_field(char *field_end_pos);
  void deal_with_irregular_line();
  void deal_with_empty_field(common::ObString &field_str, int64_t index);

  bool scan_for_line_start(char *&cur_pos, const char *buf_end);
  bool scan_for_line_end(char *&cur_pos, const char *buf_end);

  int summarize_insert_columns();
  int build_file_field_var_hashmap();
  int do_local_sync_insert(ObPhysicalPlanCtx &plan_ctx);
  ObPartitionBufferCtrl *get_part_buf_ctrl(ObTabletID tablet_id) { return part_buffer_map_.get(tablet_id); }
  int create_part_buffer_ctrl(ObTabletID tablet_id, common::ObIAllocator *allocator, ObPartitionBufferCtrl *&buf_mgr);
  int create_buffer(common::ObIAllocator &allocator, ObLoadbuffer *&buffer);
  void destroy_all_buffer();
  int send_and_switch_buffer(ObExecContext &ctx,
                             ObPhysicalPlanCtx &plan_ctx,
                             ObPartitionBufferCtrl *part_buf_ctrl);
  int send_all_buffer_finally(ObExecContext &ctx, ObPhysicalPlanCtx &plan_ctx);
  int take_record_for_failed_rows(ObPhysicalPlanCtx &plan_ctx, ObLoadbuffer *complete_task);
  int handle_complete_task(ObPhysicalPlanCtx &plan_ctx, ObLoadbuffer *complete_task);
  int wait_server_memory_dump(ObExecContext &ctx, ObTabletID tablet_id);
  int wait_all_task_finished(ObPhysicalPlanCtx &plan_ctx);
  int generate_set_expr_strs(const ObSQLSessionInfo *session_info);
  bool find_insert_column_info(const uint64_t target_column_id, int64_t &found_idx);
  int recursively_replace_varables(ObRawExpr *&raw_expr,
                                   common::ObIAllocator &allocator,
                                   const ObSQLSessionInfo &session_info);
  int analyze_exprs_and_replace_variables(common::ObIAllocator &allocator, const ObSQLSessionInfo &session_info);
  int get_server_last_freeze_ts(const common::ObAddr &server_addr, int64_t &last_freeze_ts);

  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataImpl);
  // function members
private:

  //arguments
  ObLoadArgument load_args_;
  ObDataInFileStruct data_struct_in_file_;
  common::ObString back_quoted_db_table_name_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  share::schema::ObPartitionLevel part_level_;
  int64_t part_num_;
  ParseStep initial_step_;
  common::ObDataTypeCastParams dtc_params_;
  bool flag_line_term_by_counting_field_;

  //array sizes
  int64_t file_column_number_;
  int64_t set_assigned_column_number_;
  int64_t insert_column_number_;
  int64_t params_count_;
  int64_t allocated_buffer_cnt_;
  int64_t non_string_type_direct_insert_column_count_;

  //statistics
  int64_t total_err_lines_;
  int64_t total_buf_read_;
  int64_t total_wait_secs_;

  //allocators
  common::ObArenaAllocator expr_calc_allocator_; // for table locatition calc exprs
  common::ModulePageAllocator array_allocator_;

  //buffer
  common::ObDataBuffer expr_to_string_buf_;
  common::ObDataBuffer escape_data_buffer_;

  //tools/modules
  ObFileReader reader_;
  ObRawExprPrinter expr_printer_;
  ObTableLocation table_location_;
  ObParallelTaskController task_controller_;
  CompleteTaskArray complete_task_array_;

  //runtime variables
  ParseStep cur_step_;
  int64_t parsed_line_count_;
  char *cur_line_begin_pos_;
  int64_t field_id_;
  char *cur_field_begin_pos_;
  bool in_enclose_flag_;
  ObLoadEscapeSM escape_sm_;

  //some fast access
  int64_t field_term_char_;
  int64_t line_term_char_;
  int64_t enclose_char_;
  int64_t escape_char_;

  //KMP detectors
  ObKMPStateMachine line_start_detector_;
  ObKMPStateMachine line_term_detector_;
  ObKMPStateMachine field_term_detector_;

  //timers
  ObLoadDataTimer total_timer_;
  ObLoadDataTimer file_read_timer_;
  ObLoadDataTimer parsing_execute_timer_;
  ObLoadDataTimer rpc_timer_;
  ObLoadDataTimer calc_timer_;
  ObLoadDataTimer buffer_copy_timer_;
  ObLoadDataTimer serialize_timer_;
  ObLoadDataTimer wait_task_timer_;
  ObLoadDataTimer handle_result_timer_;

  //hashmaps: use tenant 500 memory
  PartitionBufferHashMap part_buffer_map_; //hash map(partition id -> BufferInfo)
  FileFieldIdxHashMap varname_field_idx_hashmap_;  //map the variable name to field id in file
  ServerTimestampHashMap server_last_freeze_ts_hashmap_;

  //arrays
  ObAssignments set_assigns_;
  ObSEArray<ObLoadDataStmt::FieldOrVarStruct, EXPECTED_INSERT_COLUMN_NUM> file_field_def_;
  ObSEArray<ObLoadDataReplacedExprInfo, EXPECTED_EXPR_VARABLES_TOTAL_NUM> replaced_expr_value_index_store_;
  ObSEArray<ObLoadTableColumnDesc, EXPECTED_INSERT_COLUMN_NUM> valid_insert_column_info_store_;  //PRC data define
  ObSEArray<int64_t, EXPECTED_EXPR_VARABLES_TOTAL_NUM> params_value_index_store_;
  ObSEArray<common::ObString, EXPECTED_INSERT_COLUMN_NUM> insert_column_names_;
  ObSEArray<ObLoadbuffer*, 64> allocated_buffer_store_;   //just for easy release
  ObExprValueBitSet expr_value_bitset_;
  //TODO wjh: change this to string_type_bit_set
  FieldFlagBitSet field_type_bit_set_;  //a bit is set when field target is NOT a char/varchar/hexstring/text type column

  //runtime arrays
  ObSEArray<common::ObString, EXPECTED_INSERT_COLUMN_NUM> insert_values_per_line_; //only used in stop_on_dup mode
  ObSEArray<common::ObString, EXPECTED_INSERT_COLUMN_NUM> parsed_field_strs_; //store field strs from data file
  ObSEArray<common::ObString, EXPECTED_INSERT_COLUMN_NUM> expr_strs_;
  ObSEArray<ObTabletID, 1> tablet_ids_;
};

OB_INLINE bool ObLoadDataImpl::scan_for_line_start(char *&cur_pos, const char *buf_end)
{
  return line_start_detector_.scan_buf(cur_pos, buf_end);
}

OB_INLINE void ObLoadDataImpl::remove_enclosed_char(char *&cur_field_end_pos)
{
  cur_field_end_pos--;
  cur_field_begin_pos_++;
}

OB_INLINE void ObLoadDataImpl::deal_with_empty_field(ObString &field_str, int64_t index)
{
  if (field_type_bit_set_.has_member(index)
      && !lib::is_oracle_mode()) {
    //a non-string value will be set to "0", "0" will be cast to zero value of target types
    field_str.assign_ptr(ZERO_FIELD_STRING, 1);
  } else {
    //a string value will be set to ''
    field_str.reset();
  }
}
*/

OB_INLINE void ObLoadDataBase::set_one_field_objparam(ObObjParam &dest_param,
                                                      const ObString &field_str) {
  if (ObLoadDataUtils::is_null_field(field_str)
      || (lib::is_oracle_mode() && field_str.empty())) {
    dest_param.set_null();
  } else {
    dest_param.set_varchar(field_str);
  }
  dest_param.set_param_meta();
}

/*
OB_INLINE void ObLoadDataImpl::handle_one_field(char *field_end_pos)
{
  if (OB_LIKELY(field_id_ < file_column_number_)) {
    ObString &field_str = parsed_field_strs_.at(field_id_);
    int32_t str_len = static_cast<int32_t>(field_end_pos - cur_field_begin_pos_);
    if (OB_UNLIKELY(str_len <= 0)) {
      deal_with_empty_field(field_str, field_id_);
    } else {
      field_str.assign_ptr(cur_field_begin_pos_, str_len);
    }
  }
  field_id_++;
}

OB_INLINE bool ObLoadDataImpl::is_terminate_char(char &cur_char, int64_t &term_char, char *&cur_pos)
{
  bool ret_bool = false;
  if (static_cast<int64_t>(cur_char) == term_char && !escape_sm_.is_escaping()) {
    if (!in_enclose_flag_) {
      ret_bool = true; //return true
    } else {
      char *pre_pos = cur_pos - 1;
      if (static_cast<int64_t>(*pre_pos) == enclose_char_ //with in_enclose_flag_ = true, a term char is valid only if an enclosed char before it
          && cur_field_begin_pos_ != pre_pos) {  // 123---->'---->123
        in_enclose_flag_ = false;
        remove_enclosed_char(cur_pos);
        ret_bool = true;  //return true
      } else {
        //return false
      }
    }
  } else {
    //return false
  }
  return ret_bool;
}

OB_INLINE bool ObLoadDataImpl::is_terminate(char &cur_char, ObKMPStateMachine &term_state_machine, char *&cur_pos)
{
  bool ret_bool = false;
  if (term_state_machine.accept_char(cur_char)) {
    char *field_end_pos = cur_pos - term_state_machine.get_pattern_length() + 1;
    if (!in_enclose_flag_) {
      cur_pos = field_end_pos;
      ret_bool = true; //return true
    } else {
      char *pre_pos = field_end_pos - 1;
      if (static_cast<int64_t>(*pre_pos) == enclose_char_
          && !escape_sm_.is_escaping()
          && cur_field_begin_pos_ != field_end_pos) {  // 123---->'---->123
        in_enclose_flag_ = false;
        cur_pos = field_end_pos;
        remove_enclosed_char(cur_pos);
        ret_bool = true; //return true
      } else {
        //return false
      }
    }
  } else {
    //return false
  }
  return ret_bool;
}

OB_INLINE bool ObLoadDataImpl::is_enclosed_field_start(char *cur_pos, char &cur_char)
{
  return static_cast<int64_t>(cur_char) == enclose_char_
          //anything between a pair of enclosed chars will be regarded as string data of one field
          && !in_enclose_flag_
          //enclosed field start must be the first char at the beginning of one field,
          //so cur_char is impossible to be escaped
          && cur_pos == cur_field_begin_pos_;
}
*/

}//sql namespace

}//oceanbase namesapce


#endif // OCEANBASE_SQL_LOAD_DATA_IMPL_H_
