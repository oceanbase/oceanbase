/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_FLT_SPAN_MGR_H_
#define OCEANBASE_SQL_OB_FLT_SPAN_MGR_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_define.h"
#include "observer/mysql/ob_ra_queue.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

  static const char TRACE_ID[] = "trace_id";
  static const char SPAN_ID[] = "id";
  static const char SPAN_NAME[] = "name";
  static const char REF_TYPE[] = "is_follow";
  static const char START_TS[] = "start_ts";
  static const char END_TS[] = "end_ts";
  static const char PARENT_SPAN_ID[] = "parent_id";
  class ObFLTSpanMgr;
  class ObFLTSpanData {
  public:
    int64_t tenant_id_;
    ObString trace_id_;
    int64_t req_id_;
    ObString span_id_;
    ObString parent_span_id_;
    ObString span_name_;
    int64_t ref_type_;
    int64_t start_ts_;
    int64_t end_ts_;
    ObString tags_;
    ObString logs_;
    ObFLTSpanData()
    {
      reset();
    }
    ~ObFLTSpanData()
    {
    }

    void reset()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      req_id_ = OB_INVALID_ID;
      ref_type_ = -1;
      start_ts_ = -1;
      end_ts_ = -1;

      trace_id_.reset();
      span_id_.reset();
      parent_span_id_.reset();
      span_name_.reset();
      tags_.reset();
      logs_.reset();
    }

    int64_t get_extra_size() const
    {
      return trace_id_.length() + span_id_.length() + parent_span_id_.length()
              + span_name_.length() + tags_.length() + logs_.length();
    }
    TO_STRING_KV(K_(tenant_id), K_(trace_id),
                K_(req_id), K_(span_id),
                K_(parent_span_id), K_(span_name),
                K_(start_ts), K_(end_ts),
                K_(tags), K_(logs));
  };

  class ObFLTShowTraceRec {
    public:
    /* format trace name and display */
    struct trace_formatter
    {
      enum class LineType
      {
        LT_SPACE, // " "
        LT_LINE,  // "│"
        LT_NODE,  // "├"
        LT_LAST_NODE, // "└"
      };
      struct TreeLine
      {
        TreeLine() : color_idx_(0), line_type_(LineType::LT_SPACE) {}
        int32_t color_idx_;
        LineType line_type_;
      };
      struct NameLeftPadding
      {
        NameLeftPadding() : level_(0), tree_line_(NULL)
        {
        }
        TO_STRING_KV(K_(level), KP_(tree_line));
        int64_t level_;
        TreeLine *tree_line_;
      };
    };
    public:
      trace_formatter::NameLeftPadding formatter_;
      sql::ObFLTSpanData data_;
      ObString ipstr_;
      int64_t port_;

    public:
      ObFLTShowTraceRec() : port_(-1) {}
      ~ObFLTShowTraceRec() {}
    TO_STRING_KV(K_(formatter), K_(data), K_(port), K_(ipstr));
  };

  class ObFLTSpanRec {

  public:
    common::ObConcurrentFIFOAllocator *allocator_;
    sql::ObFLTSpanData data_;

  public:
    ObFLTSpanRec()
      : allocator_(nullptr) {}
    ~ObFLTSpanRec() {}

  public:
    virtual void destroy()
    {
      if (NULL != allocator_) {
        allocator_->free(this);
      }
    }

  public:
    int64_t get_self_size() const
    {
      return sizeof(ObFLTSpanData) + data_.get_extra_size();
    }

  };

  class ObFLTSpanMgr {
  public:
    static const int64_t FLT_SPAN_PAGE_SIZE = (1LL << 21) - ACHUNK_PRESERVE_SIZE; // 2M - 17k
    static const int32_t BATCH_RELEASE_COUNT = 1000;
    //初始化queue大小为10w
    static const int64_t MAX_QUEUE_SIZE = 100000; //10w
    static const int64_t RELEASE_QUEUE_SIZE = 50000; //1w
    static const int64_t EVICT_INTERVAL = 1000000; //1s
    ObFLTSpanMgr()
      : inited_(false), destroyed_(false), request_id_(0), mem_limit_(0),
        allocator_(), queue_(),
        tenant_id_(OB_INVALID_TENANT_ID), tg_id_(-1)
    {
    }

    ~ObFLTSpanMgr()
    {
      if (inited_) {
        destroy();
      }
    }

    int64_t get_start_idx() const { return (int64_t)queue_.get_pop_idx(); }
    int64_t get_end_idx() const { return (int64_t)queue_.get_push_idx(); }
    uint64_t get_tenant_id() { return tenant_id_; }
    int init(uint64_t tenant_id, const int64_t max_mem_size, const int64_t queue_size);
    int record_span(ObFLTSpanData &span_data, bool is_formmated_json);
    int release_old(int64_t limit = BATCH_RELEASE_COUNT); // evict old span and release memory
    uint64_t get_size() { return queue_.get_size(); }
    void destroy();
    static int mtl_init(ObFLTSpanMgr* &span_mgr);
    static void mtl_destroy(ObFLTSpanMgr* &span_mgr);

    void clear_queue()
    {
      (void)release_old(INT64_MAX);
    }

    int get(const int64_t idx, void *&record, ObRaQueue::Ref* ref)
    {
      int ret = OB_SUCCESS;
      if (NULL == (record = queue_.get(idx, ref))) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    int revert(ObRaQueue::Ref* ref)
    {
      queue_.revert(ref);
      return common::OB_SUCCESS;
    }
    void free(void *ptr) { allocator_.free(ptr); ptr = NULL;}

  private:
    DISALLOW_COPY_AND_ASSIGN(ObFLTSpanMgr);

  private:
    bool inited_;
    bool destroyed_;
    uint64_t request_id_;
    int64_t mem_limit_;
    common::ObConcurrentFIFOAllocator allocator_; //alloc mem for  span info
    common::ObRaQueue queue_; // store span node

    // tenant id of this request manager
    uint64_t tenant_id_;
    int tg_id_;
  };
} // namespace sql
} // namespace oceanbase
#endif
