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
class QSchedStat
{
public:
  enum { N = 1024 };
  QSchedStat(): last_report_us_(0) {
    memset(last_stat_, 0, sizeof(last_stat_));
  }
  ~QSchedStat() {}
  void try_report(int n_chan, int64_t cur_us, bool leaf_only) {
    static char buf[1<<15];
    int idx[128];
    StrFormat f(buf, sizeof(buf));
    int active_cnt = 0;
    if (cur_us - last_report_us_ > 1000 * 1000) {
      f.append("|cfg:");
      collect_cfg(f);
      active_cnt = collect_active_grp_idx(idx, arrlen(idx), leaf_only);
      f.append("|stat:");
      collect_stat(f, idx, active_cnt);
      for(int i = 0; i < n_chan; i++) {
        f.append(" |q%d:", i);
        collect_qcount(f, i, idx, active_cnt);
      }
      TC_INFO("QSched: %s", buf);
      last_report_us_ = cur_us;
    }
  }
private:
  bool is_queue(int type) { return type >= QDISC_ROOT && type < QDISC_QUEUE_END; }
  void collect_cfg(StrFormat& f) {
    for(int i = 0; i < N; i++) {
      IQD* qd = (typeof(qd))imap_fetch(i);
      if (NULL == qd) continue;
      char b[256];
      if (is_queue(qd->get_type())) {
        QDesc* desc = (typeof(desc))qd;
        f.append(" %s:%ld", desc->get_name(), desc->get_weight());
        f.append(",%s", format_bytes(b, sizeof(b), desc->get_limit()));
        f.append(",%s", format_bytes(b, sizeof(b), desc->get_reserve()));
        desc->print_limiters_per_sec(f);
      }
    }
  }
  int collect_active_grp_idx(int* idx, int limit, bool leaf_only) {
    int active_cnt = 0;
    for(int i = 0; active_cnt < limit && i < N; i++) {
      IQD* qd = (typeof(qd))imap_fetch(i);
      if (NULL == qd || !is_queue(qd->get_type())) continue;
      QDesc* desc = (typeof(desc))qd;
      if (NULL == desc || (leaf_only && QDISC_BUFFER_QUEUE != desc->get_type())) continue;
      QStat cur_stat;
      desc->get_stat(cur_stat);
      if (cur_stat.count_ - last_stat_[i].count_ > 0) {
        idx[active_cnt++] = i;
      }
    }
    return active_cnt;
  }
  void collect_stat(StrFormat& f, int* idx, int cnt) {
    char b[16];
    for(int j = 0; j < cnt; j++) {
      int i = idx[j];
      QDesc* desc = (typeof(desc))imap_fetch(i);
      QStat cur_stat;
      desc->get_stat(cur_stat);
      int64_t total_count = cur_stat.count_ - last_stat_[i].count_;
      f.append(" %s:%s/%ld:%ld", desc->get_name(), format_bytes(b, sizeof(b), cur_stat.bytes_ - last_stat_[i].bytes_), total_count,  total_count > 0? (cur_stat.delay_ - last_stat_[i].delay_)/total_count: 0);
      last_stat_[i] = cur_stat;
    }
  }
  void collect_qcount(StrFormat& f, int chan_id, int* idx, int cnt) {
    for(int j = 0; j < cnt; j++) {
      int i = idx[j];
      QDesc* desc = (typeof(desc))imap_fetch(i);
      if (QDISC_BUFFER_QUEUE != desc->get_type()) continue;
      BufferQueue* q = (typeof(q))fetch_qdisc(i, chan_id);
      if (NULL != q) {
        f.append(" %ld", q->cnt());
      } else {
        f.append(" X");
      }
    }
  }
private:
  QStat last_stat_[N];
  int64_t last_report_us_;
} qsched_stat_;

static void qsched_stat_report(int chan_id, int64_t cur_us, bool leaf_only)
{
  qsched_stat_.try_report(chan_id, cur_us, leaf_only);
}
