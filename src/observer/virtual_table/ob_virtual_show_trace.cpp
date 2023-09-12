/*
 * Copyright (c) 2022 OceanBase Technology Co.,Ltd.
 * OceanBase is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *          http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 * ---------------------------------------------------------------------------------------
 * Authors:
 *   Juehui <>
 * ---------------------------------------------------------------------------------------
 */
#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_virtual_span_info.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/trace/ob_trace.h"
#include "observer/virtual_table/ob_virtual_show_trace.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::omt;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {

ObVirtualShowTrace::ObVirtualShowTrace() :
    ObVirtualTableScannerIterator(),
    ref_(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    tenant_id_(common::OB_INVALID_ID),
    is_first_get_(true),
    is_use_index_(false),
    tenant_id_array_(),
    show_trace_rec_idx_(-1),
    tag_buf_(NULL),
    is_row_format_(true),
    with_tenant_ctx_(nullptr)
{
}

ObVirtualShowTrace::~ObVirtualShowTrace() {
  reset();
}

void ObVirtualShowTrace::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_first_get_ = true;
  is_use_index_ = false;
  tenant_id_ = common::OB_INVALID_ID;
  tenant_id_array_.reset();
  addr_ = nullptr;
  port_ = 0;
  ipstr_.reset();
  alloc_.reset();
}

int ObVirtualShowTrace::inner_open()
{
  int ret = OB_SUCCESS;

  // retrive span info from virtual span
  SERVER_LOG(DEBUG, "tenant ids", K(effective_tenant_id_), K(tenant_id_array_));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_ip(addr_))) {
      SERVER_LOG(WARN, "failed to set server ip addr", K(ret));
    } else {
    }
  }
  return ret;
}

int ObVirtualShowTrace::retrive_all_span_info()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool with_snap_shot = true;
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  ObString trace_id;
  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "mysql proxy is null", K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "session is null", K(ret));
  } else if (OB_FAIL(trans.start(mysql_proxy, effective_tenant_id_, with_snap_shot))) {
    SERVER_LOG(WARN, "failed to start transaction", K(ret), K(effective_tenant_id_));
  } else {
    int sql_len = 0;
    is_row_format_ = session_->is_row_traceformat();
    SMART_VAR(char[OB_MAX_SQL_LENGTH], sql) {
      const uint64_t exec_tenant_id = effective_tenant_id_;
      //const char *table_name = lib::is_oracle_mode() ? OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TNAME:
      //                                                  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TNAME;
      const char *table_name = OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TNAME;
      trace_id = session_->get_last_flt_trace_id();
      sql_len = snprintf(sql, OB_MAX_SQL_LENGTH,
                           "SELECT svr_ip, svr_port, tenant_id, trace_id, request_id, span_id, "
                           "parent_span_id, span_name, ref_type, start_ts, end_ts, tags, logs "
                           "FROM %s WHERE tenant_id = %lu AND trace_id = '%s'",
                           table_name,
                           effective_tenant_id_,
                           trace_id.ptr());
      LOG_TRACE("send inner sql to retrive records", KP(session_), K(session_->get_proxy_sessid()),
                                                     K(session_->get_sessid()), K(table_name),
                                                     K(tenant_id_), K(trace_id_), K(trace_id),
                                                     K(effective_tenant_id_), K(ObString(sql_len, sql)));
      if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
        ret = OB_SIZE_OVERFLOW;
        SERVER_LOG(WARN, "failed to format sql. size not enough");
      } else {

        { // make sure %res destructed before execute other sql in the same transaction
          SMART_VAR(ObMySQLProxy::MySQLResult, res) {
            ObMySQLResult *result = NULL;
            ObISQLClient *sql_client = &trans;
            uint64_t table_id = OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID;
            ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                     exec_tenant_id,
                                                     table_id);
            // retrive data from client
            if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
              SERVER_LOG(WARN, "failed to read data", K(ret));
            } else if (NULL == (result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "failed to get result", K(ret));
            } else {
              while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                sql::ObFLTShowTraceRec* rec;
                if (OB_FAIL(alloc_trace_rec(rec))) {
                  SERVER_LOG(WARN, "failed to alloc record", K(ret));
                } else if (OB_ISNULL(rec)) {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "record ptr is null");
                } else {
                  OZ(read_show_trace_rec_from_result(*result, *rec));
                  OZ(show_trace_arr_.push_back(rec));
                }
              }
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }
            }
          }
          // deep copy data


        }
      }
    }
    LOG_TRACE("after read dia log from span info", K(show_trace_arr_.count()), K(ret));
  }

  return ret;
}

int ObVirtualShowTrace::read_show_trace_rec_from_result(sqlclient::ObMySQLResult &mysql_result, sql::ObFLTShowTraceRec &rec)
{
  int ret = OB_SUCCESS;
  char trace_id_buf[OB_MAX_SPAN_LENGTH];
  char span_id_buf[OB_MAX_SPAN_LENGTH];
  char parent_id_buf[OB_MAX_SPAN_LENGTH];
  char span_name_buf[OB_MAX_SPAN_LENGTH];
  char ipstr_buf[common::MAX_IP_ADDR_LENGTH + 2];

  int64_t trace_id_len = 0;
  int64_t span_id_len = 0;
  int64_t parent_id_len = 0;
  int64_t span_name_len = 0;
  int64_t tag_len = 0;
  int64_t log_len = 0;
  int64_t ipstr_len = 0;
  char *tag_buf = NULL;
  if (OB_FAIL(get_tag_buf(tag_buf))) {
    SERVER_LOG(WARN, "failed to get tag buf", K(ret));
  } else {
    // trace_id
    EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "trace_id", trace_id_buf, OB_MAX_SPAN_LENGTH, trace_id_len);
    // span_id
    EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "span_id", span_id_buf, OB_MAX_SPAN_LENGTH, span_id_len);
    // parent_span_id
    EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "parent_span_id", parent_id_buf, OB_MAX_SPAN_LENGTH, parent_id_len);
    // span_name
    EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "span_name", span_name_buf, OB_MAX_SPAN_LENGTH, span_name_len);
    // tenant_id
    EXTRACT_INT_FIELD_MYSQL(mysql_result, "tenant_id", rec.data_.tenant_id_, int64_t);
    // request_id
    EXTRACT_INT_FIELD_MYSQL(mysql_result, "request_id", rec.data_.req_id_, int64_t);
    // start_ts
    EXTRACT_INT_FIELD_MYSQL(mysql_result, "start_ts", rec.data_.start_ts_, int64_t);
    // end_ts
    EXTRACT_INT_FIELD_MYSQL(mysql_result, "end_ts", rec.data_.end_ts_, int64_t);
    // trace_id
    EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "svr_ip", ipstr_buf, common::MAX_IP_ADDR_LENGTH + 2, ipstr_len);
    // prot
    EXTRACT_INT_FIELD_MYSQL(mysql_result, "svr_port", rec.port_, int64_t);

    // tags
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      MEMSET(tag_buf, 0x00, OB_MAX_SPAN_TAG_LENGTH);
      EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "tags", tag_buf, OB_MAX_SPAN_TAG_LENGTH, tag_len);
      if (OB_FAIL(ob_write_string(alloc_, ObString(tag_len, tag_buf), rec.data_.tags_))) {
        SERVER_LOG(WARN, "failed to deep copy tag", K(ret));
      }
    }

    // logs
    if (OB_FAIL(ret)) {
     // do nothing
    } else {
      MEMSET(tag_buf, 0x00, OB_MAX_SPAN_TAG_LENGTH);
      EXTRACT_STRBUF_FIELD_MYSQL(mysql_result, "logs", tag_buf, OB_MAX_SPAN_TAG_LENGTH, log_len);
      if (OB_FAIL(ob_write_string(alloc_, ObString(log_len, tag_buf), rec.data_.logs_))) {
        SERVER_LOG(WARN, "failed to deep copy log", K(ret));
      }
    }

    // deep copy string
    OZ (ob_write_string(alloc_, ObString(trace_id_len, trace_id_buf), rec.data_.trace_id_));
    OZ (ob_write_string(alloc_, ObString(span_id_len, span_id_buf), rec.data_.span_id_));
    OZ (ob_write_string(alloc_, ObString(parent_id_len, parent_id_buf), rec.data_.parent_span_id_));
    OZ (ob_write_string(alloc_, ObString(span_name_len, span_name_buf), rec.data_.span_name_));
    OZ (ob_write_string(alloc_, ObString(ipstr_len, ipstr_buf), rec.ipstr_));
  }

  return ret;
}

int ObVirtualShowTrace::get_tag_buf(char *&tag_buf)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (NULL != tag_buf_) {
    // not alloc, do nothing
  } else if (NULL == (buf = (char*)alloc_.alloc(OB_MAX_SPAN_TAG_LENGTH))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      SERVER_LOG(WARN, "alloc mem failed", K(OB_MAX_SPAN_TAG_LENGTH), K(ret));
    }
  } else {
    tag_buf_ = buf;
  }
  tag_buf = tag_buf_;
  return ret;
}

int ObVirtualShowTrace::generate_span_info_tree()
{
  int ret = OB_SUCCESS;
  if (show_trace_arr_.empty()) {
    // do nothing
  } else {
    ObSEArray<sql::ObFLTShowTraceRec*, 16> tmp_arr;
    ObSEArray<sql::ObFLTShowTraceRec*, 16> root_arr;
    // find root span
    bool found_root = false;
    int64_t depth = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < show_trace_arr_.count(); ++i) {
      if (OB_ISNULL(show_trace_arr_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "record ptr is null", K(i));
      } else if (session_->get_last_flt_span_id().empty() &&
            show_trace_arr_.at(i)->data_.parent_span_id_ == "00000000-0000-0000-0000-000000000000") {
        found_root = true;
        show_trace_arr_.at(i)->formatter_.level_ = depth;
        OZ(root_arr.push_back(show_trace_arr_.at(i)));
      } else if (!session_->get_last_flt_span_id().empty() &&
          show_trace_arr_.at(i)->data_.parent_span_id_.compare(session_->get_last_flt_span_id()) == 0) {
        found_root = true;
        show_trace_arr_.at(i)->formatter_.level_ = depth;
        OZ(root_arr.push_back(show_trace_arr_.at(i)));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!found_root) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found root span reset show trace", K(session_->get_last_flt_span_id()), K(session_->get_last_flt_trace_id()));
      show_trace_arr_.reset();
    } else {
      // recursively generate span tree
      for (int64_t i = 0; OB_SUCC(ret) && i < root_arr.count(); ++i) {
        if (OB_ISNULL(root_arr.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "record ptr is null", K(i));
        } else if (OB_FAIL(tmp_arr.push_back(root_arr.at(i)))) {
          LOG_WARN("failed to add root span to array", K(ret), K(i));
        } else if (OB_FAIL(find_child_span_info(NULL, root_arr.at(i)->data_.span_id_, tmp_arr, depth+1))) {
          LOG_WARN("failed to find child span info", K(ret));
        } else {
          // do nothing
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        show_trace_arr_.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.count(); ++i) {
          if (OB_ISNULL(tmp_arr.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "record ptr is null", K(i));
          } else {
            if (is_row_format_) {
              OZ(format_flt_show_trace_record(*tmp_arr.at(i)));
            } else {
              // do nothing
            }
            OZ(show_trace_arr_.push_back(tmp_arr.at(i)));
          }
        }
      }
    }
  }
  LOG_TRACE("after push back show_trace record", K(show_trace_arr_.count()), K(ret));
  return ret;
}

// before generate tree, we should merge span info.
// for spans, when buffer is full, it will flush.
int ObVirtualShowTrace::merge_span_info() {
  int ret = OB_SUCCESS;
  if (show_trace_arr_.count() < 1) {
    // do nothing
  } else {
    ObSEArray<sql::ObFLTShowTraceRec*, 16> tmp_arr;
    std::sort(&show_trace_arr_.at(0), &show_trace_arr_.at(0) + show_trace_arr_.count(),
        [](const sql::ObFLTShowTraceRec* rec1, const sql::ObFLTShowTraceRec* rec2) {
          if (NULL == rec1) {
            return true;
          } else if (NULL == rec2) {
            return false;
          } else {
            return rec1->data_.span_id_ < rec2->data_.span_id_;
          }
        });
    for (int64_t l=0; OB_SUCC(ret) && l < show_trace_arr_.count(); l++) {
      int64_t r = l;
      // if span id is same, just merge it.
      if (OB_ISNULL(show_trace_arr_.at(l))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "record ptr is null", K(l));
      } else {
        ObString l_span_id = show_trace_arr_.at(l)->data_.span_id_;
        while (OB_SUCC(ret) &&
                r+1 < show_trace_arr_.count() &&
                OB_NOT_NULL(show_trace_arr_.at(r+1)) &&
                l_span_id == show_trace_arr_.at(r+1)->data_.span_id_) {
          r++;
        }

        if (r+1 < show_trace_arr_.count()
              && OB_ISNULL(show_trace_arr_.at(r+1))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "record ptr is null", K(r+1));
        } else {
          sql::ObFLTShowTraceRec* rec;
          if (OB_FAIL(alloc_trace_rec(rec))) {
            SERVER_LOG(WARN, "failed to alloc record", K(ret));
          } else if (OB_ISNULL(rec)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "record ptr is null");
          } else {
            OZ(merge_range_span_info(l, r, *rec));
            OZ(tmp_arr.push_back(rec));
            l = r;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothong
    } else {
      show_trace_arr_.reset();
      for (int i = 0; i < tmp_arr.count(); i++) {
        OZ(show_trace_arr_.push_back(tmp_arr.at(i)));
      }
    }
  }
  return ret;
}

int ObVirtualShowTrace::merge_range_span_info(int64_t l, int64_t r, sql::ObFLTShowTraceRec &rec)
{
  int ret = OB_SUCCESS;
  char *tag_buf = NULL;
  if (OB_ISNULL(show_trace_arr_.at(l))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "record ptr is null");
  } else if (FALSE_IT(rec = *show_trace_arr_.at(l))) {
    // do nothing
  } else if (OB_FAIL(get_tag_buf(tag_buf))) {
    SERVER_LOG(WARN, "failed to get tag buf", K(ret));
  } else {
    // merge tags
    int64_t pos = 0;
    MEMSET(tag_buf, 0x00, OB_MAX_SPAN_TAG_LENGTH);
    tag_buf[0] = '[';
    pos++;
    for (int64_t i = l; OB_SUCC(ret) && i < r+1; i++) {
      if (OB_ISNULL(show_trace_arr_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "record ptr is null");
      } else {
        rec.data_.end_ts_ = max(rec.data_.end_ts_, show_trace_arr_.at(i)->data_.end_ts_);
        ObString str = show_trace_arr_.at(i)->data_.tags_.trim();
        if (str.length() == 0) {
         // skip
        } else if (pos + str.length() + 1 > OB_MAX_SPAN_TAG_LENGTH) {
          // skip
        } else {
          MEMCPY(tag_buf+pos, str.ptr(), str.length());
          pos += str.length();
          tag_buf[pos] = ',';
          pos++;
        }
      }
    }

    if (pos == 1) {
      rec.data_.tags_.reset();
    } else {
      tag_buf[pos-1] = ']';
      if (OB_FAIL(ob_write_string(alloc_, ObString(pos, tag_buf), rec.data_.tags_))) {
        SERVER_LOG(WARN, "failed to deep copy log", K(ret));
      }
    }

    // merge logs
    pos = 0;
    MEMSET(tag_buf, 0x00, OB_MAX_SPAN_TAG_LENGTH);
    tag_buf[0] = '[';
    pos++;
    for (int64_t i = l; OB_SUCC(ret) && i < r+1; i++) {
      if (OB_ISNULL(show_trace_arr_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "record ptr is null");
      } else {
        ObString str = show_trace_arr_.at(i)->data_.logs_.trim();
        if (str.length() == 0) {
         // skip
        } else if (pos + str.length() + 1 > OB_MAX_SPAN_TAG_LENGTH) {
          // skip
        } else {
          MEMCPY(tag_buf+pos, str.ptr(), str.length());
          pos += str.length();
          tag_buf[pos] = ',';
          pos++;
        }
      }
    }
    if (pos == 1) {
      rec.data_.logs_.reset();
    } else {
      tag_buf[pos-1] = ']';
      if (OB_FAIL(ob_write_string(alloc_, ObString(pos, tag_buf), rec.data_.logs_))) {
        SERVER_LOG(WARN, "failed to deep copy log", K(ret));
      }
    }
  }
  return ret;
}

int ObVirtualShowTrace::format_flt_show_trace_record(sql::ObFLTShowTraceRec &rec)
{
  int ret = OB_SUCCESS;
  static const char *colors[] = {
    "\033[32m", // GREEN
    "\033[33m", // ORANGE
    "\033[35m", // PURPLE
    "\033[91m", // LIGHTRED
    "\033[92m", // LIGHTGREEN
    "\033[93m", // YELLOW
    "\033[94m", // LIGHTBLUE
    "\033[95m", // PINK
    "\033[96m", // LIGHTCYAN
    "\033[1;31m", // RED
    "\033[1;34m" // BLUE
  };
  const char *color_end = "\033[0m";
  char* name_buf = NULL;
  const sql::ObFLTShowTraceRec::trace_formatter::NameLeftPadding &pad = rec.formatter_;
  int pad_len = max(sizeof("└── "), max(sizeof("├── "), max(sizeof("│   "), sizeof("    "))));
  int64_t len = pad.level_*pad_len + rec.data_.span_name_.length();
  name_buf = static_cast<char *>(alloc_.alloc(len));
  if (NULL == name_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(len));
  } else {
    char *start_buf = name_buf;
    for (int64_t i = 0; OB_SUCC(ret) && i < pad.level_; i++) {
      const sql::ObFLTShowTraceRec::trace_formatter::TreeLine &tl = pad.tree_line_[i];
      const char *txt = " ";
      switch (tl.line_type_) {
        case sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_SPACE: {
          txt = "    ";
          break;
        }
        case sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LINE: {
          txt = "│   ";
          break;
        }
        case sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_NODE: {
          txt = "├── ";
          break;
        }
        case sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LAST_NODE: {
          txt = "└── ";
          break;
        };
      }
      MEMCPY(start_buf, txt, strlen(txt));
      start_buf += strlen(txt);
    }
    int64_t former_len = start_buf - name_buf;
    MEMCPY(start_buf, rec.data_.span_name_.ptr(), rec.data_.span_name_.length());
    rec.data_.span_name_.assign(name_buf, former_len+rec.data_.span_name_.length());
  }
  return ret;
}

int ObVirtualShowTrace::find_child_span_info(sql::ObFLTShowTraceRec::trace_formatter::TreeLine *parent_type,
                                              ObString parent_span_id,
                                              ObIArray<sql::ObFLTShowTraceRec*> &arr,
                                              int64_t depth) {
  int ret = OB_SUCCESS;

  ObSEArray<sql::ObFLTShowTraceRec*, 4> tmp_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < show_trace_arr_.count(); ++i) {
    if (OB_ISNULL(show_trace_arr_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null", K(i));
    } else if (show_trace_arr_.at(i)->data_.parent_span_id_ == parent_span_id) {
       show_trace_arr_.at(i)->formatter_.level_ = depth;
       show_trace_arr_.at(i)->formatter_.tree_line_ =
         static_cast<sql::ObFLTShowTraceRec::trace_formatter::TreeLine *>(alloc_.alloc(sizeof(sql::ObFLTShowTraceRec::trace_formatter::TreeLine)*depth));
       if (NULL == show_trace_arr_.at(i)->formatter_.tree_line_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
       } else {
         MEMSET(show_trace_arr_.at(i)->formatter_.tree_line_, 0, depth);
         /*
         for (int j=0; OB_SUCC(ret) && j < depth; j++) {
            show_trace_arr_.at(i).formatter_.tree_line_[j].color_idx_ = 0;
            show_trace_arr_.at(i).formatter_.tree_line_[j].line_type_
                                    = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_SPACE;
         }
         */
       }

       if (OB_FAIL(tmp_arr.push_back(show_trace_arr_.at(i)))) {
         LOG_WARN("failed to push back show trace value", K(ret), K(i));
       }
     } else {
       // do nothing
     }
  }

  // do tmp_arr sort
  if (tmp_arr.count() == 0) {
    // skipp sort
  } else {
    std::sort(&tmp_arr.at(0), &tmp_arr.at(0) + tmp_arr.count(),
        [](const sql::ObFLTShowTraceRec *rec1, const sql::ObFLTShowTraceRec *rec2) {
          if (NULL == rec1) {
            return true;
          } else if (NULL == rec2) {
            return false;
          } else {
            return rec1->data_.start_ts_ < rec2->data_.start_ts_;
          }
        });
  }

  // copy parent's node format
  for (int64_t i = 0; OB_NOT_NULL(parent_type) && OB_SUCC(ret) && i < tmp_arr.count(); ++i) {
    if (OB_ISNULL(tmp_arr.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null", K(i));
    } else {
      sql::ObFLTShowTraceRec& rec = *tmp_arr.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < depth-1; j++) {
        rec.formatter_.tree_line_[j].line_type_ = parent_type[j].line_type_;
      }
      sql::ObFLTShowTraceRec::trace_formatter::LineType line_type
                                    = rec.formatter_.tree_line_[depth-2].line_type_;
      if (line_type == sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_NODE) {
        rec.formatter_.tree_line_[depth-2].line_type_
                                      = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LINE;
      } else if (line_type == sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LAST_NODE) {
        rec.formatter_.tree_line_[depth-2].line_type_
                                      = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_SPACE;
      }
      rec.formatter_.tree_line_[depth-1].line_type_
                                      = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_SPACE;
    }
  }

  // process current
  if (tmp_arr.count() < 1) {
    // do nothing, remain space
  } else if (tmp_arr.count() == 0) {
    if (OB_ISNULL(tmp_arr.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null");
    } else {
      tmp_arr.at(0)->formatter_.tree_line_[depth-1].line_type_
                                    = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LAST_NODE;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.count()-1; ++i) {
      if (OB_ISNULL(tmp_arr.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "record ptr is null", K(i));
      } else {
        tmp_arr.at(i)->formatter_.tree_line_[depth-1].line_type_
                                    = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_NODE;
      }
    }
    if (OB_ISNULL(tmp_arr.at(tmp_arr.count()-1))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null");
    } else {
      tmp_arr.at(tmp_arr.count()-1)->formatter_.tree_line_[depth-1].line_type_
                                    = sql::ObFLTShowTraceRec::trace_formatter::LineType::LT_LAST_NODE;
    }
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.count(); ++i) {
    if (arr.count() > 0) {
      if (OB_ISNULL(arr.at(arr.count() - 1))) {
        // do nothing
      } else if (OB_ISNULL(tmp_arr.at(i))) {
         // do nothing
      } else {
        if (arr.at(arr.count() - 1)->data_.start_ts_ > tmp_arr.at(i)->data_.start_ts_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid trace span", K(arr.at(arr.count() - 1)->data_), K(tmp_arr.at(i)->data_));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tmp_arr.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null", K(i));
    } else if (OB_FAIL(arr.push_back(tmp_arr.at(i)))) {
      LOG_WARN("failed to push back show trace value", K(ret), K(i));
    } else if (OB_FAIL(find_child_span_info(tmp_arr.at(i)->formatter_.tree_line_,
                                            tmp_arr.at(i)->data_.span_id_,
                                            arr,
                                            depth+1))) {
      LOG_WARN("failed to push back show trace value", K(ret), K(i));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObVirtualShowTrace::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  // first get row
  if (is_first_get_) {
    bool is_valid = true;
    // init inner iterator varaibales
    show_trace_arr_.reset();
    if (OB_FAIL(retrive_all_span_info())) {
      SERVER_LOG(WARN, "failed to retrive all span info", K(ret));
    } else if (OB_FAIL(merge_span_info())) {
      SERVER_LOG(WARN, "failed to merge span info", K(ret));
    } else if (OB_FAIL(generate_span_info_tree())) {
      SERVER_LOG(WARN, "failed to generate span info tree", K(ret));
    } else {
      is_first_get_ = false;
      show_trace_rec_idx_ = 0;
    }
    LOG_TRACE("after pre processed", K(show_trace_arr_.count()), K(ret));
  }

  // display
  if (show_trace_arr_.empty()) {
    // do nothing
    ret = OB_ITER_END;
  } else if (OB_SUCC(ret)) {
    if (show_trace_rec_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid operator_stat array index", K(show_trace_rec_idx_));
    } else if (show_trace_rec_idx_ >= show_trace_arr_.count()) {
      ret = OB_ITER_END;
      show_trace_rec_idx_ = OB_INVALID_ID;
      show_trace_arr_.reset();
    } else if (OB_ISNULL(show_trace_arr_.at(show_trace_rec_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "record ptr is null", K(show_trace_rec_idx_));
    } else {
      sql::ObFLTShowTraceRec rec = *show_trace_arr_.at(show_trace_rec_idx_);
      ++show_trace_rec_idx_;
      if (OB_FAIL(fill_cells(rec))) {
        SERVER_LOG(WARN, "fail to fill cells", K(rec), K(effective_tenant_id_));
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}

int ObVirtualShowTrace::fill_cells(sql::ObFLTShowTraceRec &record)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;

  if (OB_ISNULL(cells)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  } else {
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch(col_id) {
        //server ip
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_); //ipstr_ and port_ were set in set_ip func call
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                             ObCharset::get_default_charset()));
      } break;
        //server port
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
      } break;
      case TENANT_ID: {
        cells[cell_idx].set_int(record.data_.tenant_id_);
      } break;
      case TRACE_ID: {
        cells[cell_idx].set_varchar(record.data_.trace_id_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                             ObCharset::get_default_charset()));
      } break;
      case REQUEST_ID: {
        cells[cell_idx].set_int(record.data_.req_id_);
      } break;
        //rec server ip
      case REC_SVR_IP: {
        cells[cell_idx].set_varchar(record.ipstr_); //ipstr_ and port_ were set in set_ip func call
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                             ObCharset::get_default_charset()));
      } break;
        //rec server port
      case REC_SVR_PORT: {
        cells[cell_idx].set_int(record.port_);
      } break;
      case SPAN_ID: {
        cells[cell_idx].set_varchar(record.data_.span_id_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case PARENT_SPAN_ID: {
        cells[cell_idx].set_varchar(record.data_.parent_span_id_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case SPAN_NAME: {
        cells[cell_idx].set_varchar(record.data_.span_name_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case REF_TYPE: {
        if (record.data_.ref_type_ == 0) {
          cells[cell_idx].set_varchar("CHILD");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else if (record.data_.ref_type_ == 1) {
          cells[cell_idx].set_varchar("FOLLOW");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else {
          // do nothing
        }
      } break;
      case START_TS: {
        cells[cell_idx].set_timestamp(record.data_.start_ts_);
      } break;
      case END_TS: {
        cells[cell_idx].set_timestamp(record.data_.end_ts_);
      } break;
      case ELAPSE: {
        cells[cell_idx].set_int(record.data_.end_ts_ - record.data_.start_ts_);
      } break;
      case TAGS: {
        cells[cell_idx].set_lob_value(ObLongTextType,
                                      record.data_.tags_.ptr(),
                                      record.data_.tags_.length());
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case LOGS: {
        cells[cell_idx].set_lob_value(ObLongTextType,
                                      record.data_.logs_.ptr(),
                                      record.data_.logs_.length());
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
      } break;
      }
    }
  }
  return ret;
}

int ObVirtualShowTrace::set_ip(common::ObAddr *addr)
{
  int ret = OB_SUCCESS;
  MEMSET(server_ip_, 0, sizeof(server_ip_));
  if (NULL == addr){
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(server_ip_, sizeof(server_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(server_ip_);
    port_ = addr_->get_port();
  }
  return ret;
}

int ObVirtualShowTrace::alloc_trace_rec(sql::ObFLTShowTraceRec *&rec)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (NULL == (buf = (char*)alloc_.alloc(sizeof(sql::ObFLTShowTraceRec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      SERVER_LOG(WARN, "alloc mem failed", K(OB_MAX_SPAN_TAG_LENGTH), K(ret));
    }
  } else {
   rec = new(buf)sql::ObFLTShowTraceRec();
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
