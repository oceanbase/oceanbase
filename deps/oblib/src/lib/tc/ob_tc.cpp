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

#include "ob_tc.h"
#include "deps/deps.h"

static void* imap_fetch(int id);
struct QStat
{
  QStat(): count_(0), bytes_(0), delay_(0), canceled_(0) {}
  void inc(QStat& that) {
    count_ += that.count_;
    bytes_ += that.bytes_;
    delay_ += that.delay_;
    canceled_ += that.canceled_;
  }
  int64_t count_;
  int64_t bytes_;
  int64_t delay_;
  int64_t canceled_;
};

class IQD
{
public:
  IQD(int id, int type, const char* name): id_(id), type_(type) {
    snprintf(name_, sizeof(name_), "%s", name);
  }
  virtual ~IQD() {}
  int get_type() { return type_; }
  int get_id() { return id_; }
  const char* get_name() const { return name_; }
protected:
  int id_;
  int type_;
  char name_[16]; // for debug
};

class ITCLimiter: public IQD
{
public:
  enum { LIMIT_BALANCE_BOUND_MS = 10 };
  ITCLimiter(int id, int type, const char* name): IQD(id, type, name), limit_per_sec_(INT64_MAX), due_ts_(0), storage_key_(0) {}
  ITCLimiter(int id, int type, const char* name, uint64_t storage_key): IQD(id, type, name), limit_per_sec_(INT64_MAX), due_ts_(0), storage_key_(storage_key) {}
  virtual ~ITCLimiter() {}
  virtual int64_t get_cost(TCRequest* req) = 0;
  void set_limit_per_sec(int64_t limit) { limit_per_sec_ = limit; }
  int64_t get_limit_per_sec() { return limit_per_sec_; }
  uint64_t get_storage_key() { return storage_key_; }
  int64_t inc_due_ns(TCRequest* req)
  {
    if (INT64_MAX == limit_per_sec_) {
      if (due_ts_ != 0) {
        due_ts_ = 0;
      }
    } else if (0 == limit_per_sec_) {
      if (due_ts_ != INT64_MAX) {
        due_ts_ = INT64_MAX;
      }
    } else {
      int64_t cur_ns = tc_get_ns();
      int64_t ts_lower_limit = cur_ns - LIMIT_BALANCE_BOUND_MS * 1000 * 1000;
      if (due_ts_ < ts_lower_limit) {
        due_ts_ = ts_lower_limit;
      }
      int64_t inc_ts = get_cost(req) * 1000000000LL/limit_per_sec_;
      ATOMIC_FAA(&due_ts_, inc_ts);
    }
    return due_ts_;
  }
  int64_t get_due_ns() { return due_ts_; }
protected:
  int64_t limit_per_sec_;
  int64_t due_ts_ CACHE_ALIGNED;
  uint64_t storage_key_;
};

#include "ob_tc_limit.cpp"
typedef class ITCLimiter Limiter;
class TCLimiterList
{
public:
  enum { MAX_LIMITER_COUNT = 100 };
  TCLimiterList(): limiter0_(-1, "builtin") { memset(limiter_, 0, sizeof(limiter_)); }

  void set_limit_per_sec(int64_t limit) { limiter0_.set_limit_per_sec(limit); }
  int64_t get_limit_per_sec() { return limiter0_.get_limit_per_sec(); }
  void print_limiters_per_sec(StrFormat& f) {
    char b[256];
    for (int i = 0; i < MAX_LIMITER_COUNT && limiter_[i]; i++) {
      f.append(" %s:%s", limiter_[i]->get_name(), format_bytes(b, sizeof(b), limiter_[i]->get_limit_per_sec()));
    }
  }
  void add_limit(Limiter* limiter) {
    for(int i = 0; i < MAX_LIMITER_COUNT; i++) {
      if (NULL == limiter_[i]) {
        limiter_[i] = limiter;
        break;
      }
    }
  }
  void del_limit(Limiter* limiter) {
    for(int i = 0; i < MAX_LIMITER_COUNT; i++) {
      if (limiter == limiter_[i]) {
        memmove(limiter_ + i, limiter_ + i + 1, (MAX_LIMITER_COUNT - i - 1) * sizeof(limiter_[0]));
        break;
      }
    }
  }
  int64_t recalc_next_active_ns(TCRequest *req)
  {
    int64_t max_ts = limiter0_.get_due_ns();
    if (req == nullptr) {
      for(int i = 0; i < MAX_LIMITER_COUNT && limiter_[i]; i++) {
        int64_t due_ns = limiter_[i]->get_due_ns();
        if (max_ts < due_ns) {
          max_ts = due_ns;
        }
      }
    } else {
      for (int i = 0; i < MAX_LIMITER_COUNT && limiter_[i]; i++) {
        if (req->storage_key_ == limiter_[i]->get_storage_key()) {
          int64_t due_ns = limiter_[i]->get_due_ns();
          if (max_ts < due_ns) {
            max_ts = due_ns;
          }
        }
      }
    }
    return max_ts;
  }
  void inc_due_ns(TCRequest *req)
  {
    limiter0_.inc_due_ns(req);
    for (int i = 0; i < MAX_LIMITER_COUNT && limiter_[i]; i++) {
      if (req->storage_key_ == limiter_[i]->get_storage_key()) {
        limiter_[i]->inc_due_ns(req);
      }
    }
  }

private:
  BytesLimiter limiter0_;
  Limiter* limiter_[MAX_LIMITER_COUNT];
};

TCRequest* link2req(TCLink* p) { return structof(p, TCRequest,link_); }

typedef class TCLimiterList LimiterList;
class OverQuotaList;
class QDesc;
class IQDisc;

class QDesc: public IQD
{
public:
  enum { SHARE_BALANCE_BOUND_MB = 100 };
  friend class OverQuotaList;
  friend class OverReserveList;
  friend class IQDisc;
protected:
  int parent_;
  int root_;
  int64_t weight_;
  LimiterList reserver_;
  LimiterList limiter_;
public:
  QDesc(int id, int type, int parent, int root, const char* name): IQD(id, type, name), parent_(parent), root_(root),
                                                         weight_(1), reserver_(), limiter_()
  {
    reserver_.set_limit_per_sec(0);
  }
  ~QDesc() {}
  int get_root() { return root_; }
  int get_parent() { return parent_; }
  QStat& get_stat(QStat& stat);
  void set_weight(int64_t w) { weight_ = w; }
  int64_t get_weight() { return weight_; }
  void set_limit(int64_t limit) {  limiter_.set_limit_per_sec(limit); }
  int64_t get_limit() { return limiter_.get_limit_per_sec(); }
  void set_reserve(int64_t limit) {  reserver_.set_limit_per_sec(limit); }
  int64_t get_reserve() { return reserver_.get_limit_per_sec(); }
  void add_limit(Limiter* limiter) { limiter_.add_limit(limiter); }
  void del_limit(Limiter* limiter) { limiter_.del_limit(limiter); }
  void add_reserve(Limiter* limiter) { reserver_.add_limit(limiter); }
  void del_reserve(Limiter* limiter) { reserver_.del_limit(limiter); }
  void print_limiters_per_sec(StrFormat& f) { limiter_.print_limiters_per_sec(f); }
};

static IQDisc* fetch_qdisc(int qid, int chan_id);

class IQDisc
{
public:
  enum { SHARE_BALANCE_BOUND_MB = 100 };
  friend class OverQuotaList;
  friend class OverReserveList;
protected:
  QDesc* desc_;
  int chan_id_;
  IQDisc* parent_;
  IQDisc* root_;
  QStat stat_;
  TCDLink ready_dlink_;
  TCLink dirty_link_;
  TCLink over_quota_link_;
  TCLink over_reserve_link_;
  int64_t cur_vast_;
  int64_t max_poped_child_vast_;
  int64_t refresh_ns_;
  TCRequest* candinate_req_;
  bool candinate_is_reserved_;
  int64_t next_active_ts_;
  TCDLink ready_list_;
  TCLink dirty_list_;
public:
  QStat& get_stat() { return stat_; }
  void inc_stat(TCRequest* req) {
    stat_.count_ += 1;
    stat_.bytes_ += req->bytes_;
    stat_.delay_ += refresh_ns_ - req->start_ns_;
    if (req->is_canceled()) {
      stat_.canceled_ += req->bytes_;
    }
  }
public:
  static IQDisc* ready2qdisc(TCDLink* p) { return structof(p, IQDisc, ready_dlink_); }
  static IQDisc* dirty2qdisc(TCLink* p) { return structof(p, IQDisc, dirty_link_); }
  static IQDisc* OverQuota2Qdisc(TCLink* p) { return structof(p, IQDisc, over_quota_link_); }
  static IQDisc* OverReserve2Qdisc(TCLink* p) { return structof(p, IQDisc, over_reserve_link_); }
  static bool next_quota_active_ts_less_than(TCLink* p1, TCLink* p2)
  {
    IQDisc* q1 = OverQuota2Qdisc(p1);
    IQDisc* q2 = OverQuota2Qdisc(p2);
    return q1->next_active_ts_ < q2->next_active_ts_;
  }
  static bool next_reserve_active_ts_less_than(TCLink* p1, TCLink* p2)
  {
    IQDisc* q1 = OverReserve2Qdisc(p1);
    IQDisc* q2 = OverReserve2Qdisc(p2);
    return q1->next_active_ts_ < q2->next_active_ts_;
  }

  IQDisc(QDesc* desc, int chan_id): desc_(desc), chan_id_(chan_id),
                                    parent_(fetch_qdisc(desc->parent_, chan_id)), root_(fetch_qdisc(desc->root_, chan_id)),
                       ready_dlink_(NULL), dirty_link_(NULL), over_quota_link_(NULL), over_reserve_link_(NULL),
                       cur_vast_(0), max_poped_child_vast_(0),
                       refresh_ns_(0), candinate_req_(NULL), candinate_is_reserved_(false),
                       next_active_ts_(INT64_MAX), ready_list_(&ready_list_), dirty_list_(&dirty_list_) {
  }
  virtual ~IQDisc() {}
protected:
  IQDisc* get_parent() { return parent_; }
  IQDisc* get_root() { return root_; }
public:
  virtual TCRequest* quick_top() {
    return nullptr;
  }
  TCRequest* refresh_candinate(int64_t cur_ns) {
      if (refresh_ns_ != cur_ns) {
        candinate_req_ = NULL;
        candinate_is_reserved_ = false;
        int64_t next_active_ts = 0;
        if (NULL == (candinate_req_ = do_refresh_candinate(cur_ns))) {
        } else if ((next_active_ts = desc_->reserver_.recalc_next_active_ns(nullptr)) > cur_ns) {
          if (next_active_ts != INT64_MAX) {
            on_over_reserve(next_active_ts);
          }
        } else {
          candinate_is_reserved_ = true;
        }
        if (candinate_is_reserved_) {
        } else if ((next_active_ts =  desc_->limiter_.recalc_next_active_ns(quick_top())) > cur_ns) {
          candinate_req_ = nullptr;
          if (next_active_ts != INT64_MAX) {
            on_over_quota(next_active_ts); 
          }
        }
        IQDisc* parent = get_parent();
        if (NULL != parent) {
          int64_t lower_bound = parent->max_poped_child_vast_ - (1000LL * 1000LL * SHARE_BALANCE_BOUND_MB)/desc_->weight_;
          if (cur_vast_ < lower_bound) {
            cur_vast_ = lower_bound;
          }
        }
        refresh_ns_ = cur_ns;
      } else {
        assert(false);
      }
      return candinate_req_;
  }
  virtual TCRequest* do_refresh_candinate(int64_t cur_ns) {
    TCRequest* candinate = NULL;
    refresh_dirty_list(cur_ns);
    IQDisc* child = min_vast_child();
    if (child) {
      candinate = child->candinate_req_;
    }
    return candinate;
  }

  TCRequest* pop() {
    TCRequest* req = do_pop();
    if (req) {
      cur_vast_ += calc_req_st(req);
      inc_stat(req);
      if (!req->is_canceled()) {
        desc_->limiter_.inc_due_ns(req);
        desc_->reserver_.inc_due_ns(req);
      }
      IQDisc* parent = get_parent();
      if (parent) {
        parent->add_to_dirty_list(this);
        if (cur_vast_ > parent->max_poped_child_vast_) {
          parent->max_poped_child_vast_ = cur_vast_;
        }
      }
    }
    return req;
  }
  virtual TCRequest* do_pop() {
    TCRequest* req = NULL;
    IQDisc* child = pop_min_vast_child();
    if (child) {
      req = child->pop();
    }
    return req;
  }

  void mark_dirty(IQDisc* child) {
    IQDisc* parent = get_parent();
    if (NULL == child) {
      if (parent) {
        parent->mark_dirty(this);
      }
    } else if (NULL == child->dirty_link_.next_ && NULL == child->over_quota_link_.next_) {
      add_to_dirty_list(child);
      if (parent) {
        parent->mark_dirty(this);
      }
    } else {
      // do nothing
    }
  }

protected:
  void refresh_dirty_list(int64_t cur_ns) {
    TCLink* p = NULL;
    while(&dirty_list_ != (p = dirty_list_.next_)) {
      link_del(&dirty_list_, p);
      p->next_ = NULL;
      inspect_child(dirty2qdisc(p), cur_ns);
    }
  }

  void inspect_child(IQDisc* q, int64_t cur_ns) {
    if (q->ready_dlink_.next_) {
      dlink_del(&q->ready_dlink_);
      q->ready_dlink_.next_ = NULL;
    }
    if (q->refresh_candinate(cur_ns)) {
      if (q->candinate_is_reserved_ == true) {
        candinate_is_reserved_ = true;
      }
      sort_ready_child(q);
    }
  }

  bool has_reserved_req() { return false; }
  void sort_ready_child(IQDisc* child) {
    TCDLink* prev = &ready_list_;
    TCDLink* cur = NULL;
    while(&ready_list_ != (cur = prev->next_) && child->get_predict_vast() > ready2qdisc(cur)->get_predict_vast()) {
      prev = cur;
    }
    dlink_insert(prev, &child->ready_dlink_);
    //char buf[128] = "\0";
    //dump_ready_list(buf, sizeof(buf));
    //info("sort ready child, %s: after sort: %s", child->name_, buf);
  }

  const char* dump_ready_list(char* buf, int64_t limit) {
    int64_t pos = 0;
    TCDLink* prev = &ready_list_;
    TCDLink* cur = NULL;
    while(&ready_list_ != (cur = prev->next_)) {
      prev = cur;
      pos += snprintf(buf + pos, limit - pos, "%s,", ready2qdisc(cur)->desc_->name_);
    }
    return buf;
  }

  void on_over_quota(int64_t next_active_ts);
  void on_over_reserve(int64_t next_active_ts);
  uint64_t calc_req_st(TCRequest* req) { return req->bytes_/desc_->weight_; }
  uint64_t get_predict_vast() {
    int64_t base = (candinate_is_reserved_? 0: INT64_MAX/2);
    return base + cur_vast_ + calc_req_st(candinate_req_);
  }
  IQDisc* min_vast_child() {
    TCDLink* p = ready_list_.next_;
    return (p == &ready_list_)? NULL: ready2qdisc(p);
  }
  IQDisc* pop_min_vast_child() {
    IQDisc* ret = NULL;
    TCDLink* p = ready_list_.next_;
    if (p != &ready_list_) {
      dlink_del(p);
      p->next_ = NULL;
      ret = ready2qdisc(p);
    }
    return ret;
  }
  void add_to_dirty_list(IQDisc* child) {
    link_insert(&dirty_list_, &child->dirty_link_);
  }
};

class BufferQueue: public IQDisc
{
public:
  BufferQueue(QDesc* desc, int chan_id): IQDisc(desc, chan_id) {}
  virtual ~BufferQueue() {}
  int64_t cnt() { return queue_.cnt(); }

  virtual TCRequest* do_refresh_candinate(int64_t cur_ns) {
    return top();
  }
  void push(TCRequest* req) {
    queue_.push(&req->link_);
    mark_dirty(NULL);
  }
  TCRequest* top() {
    TCLink* p = queue_.top();
    return p? link2req(p): NULL;
  }
  TCRequest* do_pop() {
    TCLink* p = queue_.pop();
    return p? link2req(p): NULL;
  }
  TCRequest* quick_top() {
    return top();
  }
private:
  SimpleTCLinkQueue queue_;
};

class WeightedQueue: public IQDisc
{
public:
  WeightedQueue(QDesc* desc, int chan_id): IQDisc(desc, chan_id) {}
  virtual ~WeightedQueue() {}
};

class OverQuotaList
{
public:
  TCLink over_quota_list_;
public:
  OverQuotaList(): over_quota_list_(&over_quota_list_) {}
  ~OverQuotaList() {}
  void sort_over_quota_qdisc(IQDisc* child) {
    assert(NULL == child->dirty_link_.next_);
    assert(NULL == child->over_quota_link_.next_);
    order_link_insert(&over_quota_list_, &child->over_quota_link_, IQDisc::next_quota_active_ts_less_than);
  }
  int64_t get_first_active_ns() {
    TCLink* first = over_quota_list_.next_;
    return (first != &over_quota_list_)? IQDisc::OverQuota2Qdisc(first)->next_active_ts_: INT64_MAX;
  }
  void wake_overquota_req(TCLink* wakel) {
    TCLink* over_h = &over_quota_list_;
    TCLink* pre = over_h;
    TCLink* over_cur = over_h->next_;
    while (over_h != over_cur) {
      if (over_cur == wakel) {
        IQDisc* over_q = IQDisc::OverQuota2Qdisc(over_cur);
        link_del(pre, over_cur);
        over_cur->next_ = NULL;
        over_q->mark_dirty(NULL);
        break;
      } else {
        pre = over_cur;
        over_cur = over_cur->next_;
      }
    }
  }
  void inspect_over_quota_qdisc(int64_t cur_ns) {
    TCLink* h = &over_quota_list_;
    TCLink* cur = NULL;
    while(h != (cur = h->next_)) {
      IQDisc* q = IQDisc::OverQuota2Qdisc(cur);
      if (q->next_active_ts_ > cur_ns) {
        break;
      } else {
        link_del(h, cur);
        cur->next_ = NULL;
        if ((q->next_active_ts_ =  q->desc_->limiter_.recalc_next_active_ns(q->quick_top())) > cur_ns) {
          sort_over_quota_qdisc(q);
        } else {
          q->mark_dirty(NULL);
        }
      }
    }
  }
};

class OverReserveList
{
public:
  TCLink over_reserve_list_;
public:
  OverReserveList(): over_reserve_list_(&over_reserve_list_) {}
  ~OverReserveList() {}
  void sort_over_reserve_qdisc(IQDisc* child) {
    if (NULL == child->over_reserve_link_.next_) {
      order_link_insert(&over_reserve_list_, &child->over_reserve_link_, IQDisc::next_reserve_active_ts_less_than);
    }
  }
  int64_t get_first_active_ns() {
    TCLink* first = over_reserve_list_.next_;
    return (first != &over_reserve_list_)? IQDisc::OverReserve2Qdisc(first)->next_active_ts_: INT64_MAX;
  }
  void inspect_over_reserve_qdisc(int64_t cur_ns, OverQuotaList& over_quota_list) {
    TCLink* h = &over_reserve_list_;
    TCLink* cur = NULL;
    while(h != (cur = h->next_)) {
      IQDisc* q = IQDisc::OverReserve2Qdisc(cur);
      if (q->next_active_ts_ > cur_ns) {
        break;
      } else {
        TCLink* over_h = &(over_quota_list.over_quota_list_);
        TCLink* pre = over_h;
        TCLink* over_cur = over_h->next_;
        while (over_h != over_cur) {
          if (over_cur == cur) {
            IQDisc* over_q = IQDisc::OverQuota2Qdisc(over_cur);
            link_del(pre, over_cur);
            over_cur->next_ = NULL;
            over_q->mark_dirty(NULL);
            break;
          } else {
            pre = over_cur;
            over_cur = over_cur->next_;
          }
        }
        link_del(h, cur);
        cur->next_ = NULL;
        if ((q->next_active_ts_ =  q->desc_->reserver_.recalc_next_active_ns(nullptr)) > cur_ns
            && q->next_active_ts_ != INT64_MAX) {
          sort_over_reserve_qdisc(q);
        } else {
          q->mark_dirty(NULL);
        }
      }
    }
  }
};

class QDescRoot: public QDesc
{
public:
  enum { MAX_TC_CHAN = 32 };
  QDescRoot(int id, const char* name): QDesc(id, QDISC_ROOT, -1, id, name), handler_(NULL), n_chan_(0)  {}
  ~QDescRoot() {}
  int get_n_chan() { return n_chan_; }
  void set_handler(ITCHandler* handler){ handler_ = handler; }
  ITCHandler* get_handler() { return handler_; }
  int qsched_start(int n_chan);
  int qsched_stop();
  int qsched_wait();
  int qsched_submit(TCRequest* req, uint32_t chan_id);
private:
  ITCHandler* handler_;
  int n_chan_;
};

static IQDisc* create_path_and_fetch_leaf(int qid, int chan_id);
static void qsched_stat_report(int chan_id, int64_t cur_us, bool leaf_only);
class QDiscRoot: public IQDisc
{
private:
  enum { QUOTA_BATCH_MS = 1, RESERVE_BATCH_MS = 1 };
  enum { MAX_QTABLES = 32 };
  SimpleTCLinkQueue fallback_queue_;
  OverQuotaList over_quota_list_;
  OverReserveList over_reserve_list_;
  BatchPopQueue req_queue_;
  SingleWaiterCond cond_ CACHE_ALIGNED;
  bool is_stop_ CACHE_ALIGNED;
  pthread_t pd_;
public:
  QDiscRoot(QDesc* desc, int chan_id): IQDisc(desc, chan_id), is_stop_(false) {
    root_ = this;
  }
  virtual ~QDiscRoot() {}
  void set_chan_id(int chan_id) { chan_id_ = chan_id; }
  void sort_over_quota_qdisc(IQDisc* child) {
    over_quota_list_.sort_over_quota_qdisc(child);
  }
  void sort_over_reserve_qdisc(IQDisc* child) {
    over_reserve_list_.sort_over_reserve_qdisc(child);
  }
  void wake_overquota_req(TCLink* wake_q) {
    over_quota_list_.wake_overquota_req(wake_q);
  }
  int submit_req(TCRequest* req) {
    req->start_ns_ = tc_get_ns();
    req_queue_.push(&req->link_);
    cond_.signal();
    return 0;
  }
  TCRequest* refresh_and_pop(int64_t cur_ns, int64_t& next_active_ns) {
    TCRequest* req = NULL;
    QRGuardUnsafe("refresh_and_pop", chan_id_);
    if (handle_req_queue() > 0) {
      next_active_ns = 0;
    }

    over_quota_list_.inspect_over_quota_qdisc(cur_ns - QUOTA_BATCH_MS * 1000 * 1000);
    over_reserve_list_.inspect_over_reserve_qdisc(cur_ns - RESERVE_BATCH_MS * 1000 * 1000, over_quota_list_);
    if (!is_root_over_quota() && refresh_candinate(cur_ns)) {
      req = pop();
    }
    if (next_active_ns > 0) {
      int64_t next_ns = 0;
      if ((next_ns = over_quota_list_.get_first_active_ns()) < next_active_ns) {
        next_active_ns = next_ns;
      }
    }
    return req;
  }
public:
  TCRequest* pop_fallback_queue() {
    TCRequest* req =  NULL;
    TCLink* p = fallback_queue_.pop();
    if (NULL != p) {
      req = link2req(p);
      inc_stat(req);
    }
    return req;
  }
  int do_thread_work() {
    tc_format_thread_name("qsched%d", chan_id_);
    while(!ATOMIC_LOAD(&is_stop_)) {
      int64_t cur_ns = tc_get_ns();
      int64_t next_active_ns = cur_ns + 100 * 1000 * 1000;
      TCRequest* req = pop_fallback_queue()?: refresh_and_pop(cur_ns, next_active_ns);
      QDescRoot* desc = (typeof(desc))desc_;
      if (req) {
        desc->get_handler()->handle(req);
      } else {
        int64_t wait_us = (next_active_ns - cur_ns)/1000;
        if (wait_us > 0) {
          cond_.wait(wait_us);
        }
      }
      if (0 == chan_id_) {
        bool leaf_only = false;
        qsched_stat_report(desc->get_n_chan(), cur_ns/1000, leaf_only);
      }
    }
    return 0;
  }
  static int thread_work(QDiscRoot* root) {
    return root->do_thread_work();
  }
  int start() {
    return pthread_create(&pd_, NULL, (void* (*)(void*))thread_work, this);
  }
  void set_stop() { is_stop_ = true; }
  int wait() { return pthread_join(pd_, NULL); }
private:
  bool is_root_over_quota() { return over_quota_link_.next_; }
  int64_t handle_req_queue() {
    int64_t handle_cnt = 0;
    TCLink* h = NULL;
    h = req_queue_.pop();
    while(h) {
      handle_cnt++;
      TCLink* n = h->next_;
      TCRequest* req = link2req(h);
      BufferQueue* qdisc = (typeof(qdisc))create_path_and_fetch_leaf(req->qid_, chan_id_);
      if (qdisc) {
        qdisc->push(req);
        wake_overquota_req(h);
      } else {
        fallback_queue_.push(&req->link_);
      }
      h = n;
    }
    return handle_cnt;
  }
};

void IQDisc::on_over_quota(int64_t next_active_ts)
{
  next_active_ts_ = next_active_ts;
  ((QDiscRoot*)get_root())->sort_over_quota_qdisc(this);
}

void IQDisc::on_over_reserve(int64_t next_active_ts)
{
  next_active_ts_ = next_active_ts;
  ((QDiscRoot*)get_root())->sort_over_reserve_qdisc(this);
}

static QDiscRoot* create_and_fetch_root(int qid, int chan_id);
static QDiscRoot* fetch_root(int qid, int chan_id);
int QDescRoot::qsched_start(int n_chan)
{
  int err  = 0;
  for(int chan_id = 0; 0 == err && chan_id < n_chan; chan_id++) {
    QDiscRoot* q = create_and_fetch_root(id_, chan_id);
    if (NULL == q) {
      err = -ENOMEM;
    } else {
      err = q->start();
    }
  }
  n_chan_ = n_chan;
  return err;
}

int QDescRoot::qsched_stop()
{
  int err  = 0;
  for(int chan_id = 0; 0 == err && chan_id < n_chan_; chan_id++) {
    QDiscRoot* q = fetch_root(id_, chan_id);
    if (NULL == q) {
      err = -ENOENT;
    } else {
      q->set_stop();
    }
  }
  return err;
}

int QDescRoot::qsched_wait()
{
  int err  = 0;
  for(int chan_id = 0; 0 == err && chan_id < n_chan_; chan_id++) {
    QDiscRoot* q = fetch_root(id_, chan_id);
    if (NULL == q) {
      err = -ENOENT;
    } else {
      q->wait();
    }
  }
  n_chan_ = 0;
  return err;
}

int QDescRoot::qsched_submit(TCRequest* req, uint32_t chan_id)
{
  int err = 0;
  QDiscRoot * q = NULL;
  int n_chan = n_chan_;
  if (n_chan <= 0) {
    err = -EINVAL;
  } else if (NULL == (q = fetch_root(id_, chan_id % n_chan))) {
    err = -ENOENT;
  } else {
    err = q->submit_req(req);
  }
  return err;
}

QStat& QDesc::get_stat(QStat& stat) {
  QDescRoot* root = (typeof(root))imap_fetch(root_);
  int n_chan = root->get_n_chan();
  for(int i = 0; i < n_chan; i++) {
    IQDisc* q = fetch_qdisc(id_, i);
    if (NULL != q) {
      stat.inc(q->get_stat());
    }
  }
  return stat;
}

#include "ob_tc_interface.cpp"
#include "ob_tc_stat.cpp"
