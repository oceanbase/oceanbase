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

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/utility/ob_print_utils.h"
#include <unistd.h>
using namespace oceanbase::common;

void print_usage()
{
  fprintf(stdout, "\nUsage: obperf [OPTION] <datafile.pfda>\n");
  fprintf(stdout, "        -r report performance statistics [default, support observer.pfda only]\n");
  fprintf(stdout, "        -p report pair events statistics [support observer.pfda only]\n");
  fprintf(stdout, "        -D dump raw trace in ASCII [support both observer.pfda and atomic_op.pfda]\n");
  fprintf(stdout, "        -s report counter statistics [support atomic_op.pfda only]\n");
}

void print_version() {
  printf("obperf 1.0.0\n");
  printf("Copyright (C) 2016 Alibaba Group Holding Limited.\n\n");
}

template <typename RECORD>
class Processor
{
public:
  Processor()
      :rec_(NULL)
  {}
  virtual ~Processor() {}
  virtual int init() = 0;
  virtual int handle_record() = 0;
  virtual int report() = 0;
  void set_record(RECORD *arg_rec) { rec_ = arg_rec; }
protected:
  RECORD *rec_;
};

class Dumper: public Processor<ObPerfEventRecorder>
{
public:
  Dumper() {}
  virtual ~Dumper() {}
  virtual int init() { return OB_SUCCESS; }
  virtual int handle_record()
  {
    fprintf(stdout, "%s\n", S(*rec_));
    return OB_SUCCESS;
  }
  virtual int report() { return OB_SUCCESS; }
};

class PerfReporter: public Processor<ObPerfEventRecorder>
{
public:
  PerfReporter() {}
  virtual ~PerfReporter() {}

  virtual int init();
  virtual int handle_record();
  virtual int report();
private:
  struct EventStat
  {
    uint64_t count_;
    uint64_t total_;
    uint64_t min_;
    uint64_t max_;
  };
  typedef ObConcurrentHashMap<uint64_t, EventStat*> EventStatMap;
  typedef ObConcurrentHashMap<uint64_t, EventStatMap*> CatStatMap;
  static bool for_each_cat_id(uint64_t cat_id, EventStatMap* event_stats);
  static bool for_each_event(uint64_t event, EventStat* event_stat);
private:
  CatStatMap map_;
};

int PerfReporter::init()
{
  return map_.init();
}

int PerfReporter::handle_record()
{
  int ret = OB_SUCCESS;
  ObPerfEventRecorder &rec = *rec_;
  if (OB_ENTRY_NOT_EXIST == map_.contains_key(rec.get_cat_id())) {
    EventStatMap *event_stats = new(std::nothrow) EventStatMap();
    if (NULL == event_stats) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(event_stats->init())) {
      OB_LOG(WARN, "failed to init event_stats", K(ret));
    } else if (OB_FAIL(map_.put_refactored(rec.get_cat_id(), event_stats))) {
      OB_LOG(WARN, "failed to push into map", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    EventStatMap *event_stats = NULL;
    if (OB_FAIL(map_.get_refactored(rec.get_cat_id(), event_stats))) {
      OB_LOG(WARN, "failed to get event_stats", K(ret), "cat_id", rec.get_cat_id());
    } else {
      int64_t prev_ts = 0;
      int64_t N = rec.count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
        const ObTimestampEvent &event = rec.get_event(i);
        if (0 == prev_ts) {
          prev_ts = event.timestamp_;
        } else {
          uint64_t elapsed = event.timestamp_ - prev_ts;
          prev_ts = event.timestamp_;
          if (OB_ENTRY_NOT_EXIST == event_stats->contains_key(event.id_)) {
            EventStat *ev_stat = new(std::nothrow) EventStat();
            if (NULL == ev_stat) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              ev_stat->count_ = 1;
              ev_stat->min_ = elapsed;
              ev_stat->max_ = elapsed;
              ev_stat->total_ = elapsed;
              if (OB_FAIL(event_stats->put_refactored(event.id_, ev_stat))) {
                OB_LOG(WARN, "failed to put into map", K(ret));
              }
            }
          } else {
            EventStat *ev_stat = NULL;
            if (OB_FAIL(event_stats->get_refactored(event.id_, ev_stat))) {
              OB_LOG(WARN, "failed to get from map", K(ret));
            } else {
              ev_stat->count_++;
              ev_stat->total_ += elapsed;
              if (elapsed > ev_stat->max_) {
                ev_stat->max_ = elapsed;
              }
              if (elapsed < ev_stat->min_) {
                ev_stat->min_ = elapsed;
              }
            }
          }
        }
      } // end for
    }
  }
  return ret;
}

bool PerfReporter::for_each_cat_id(uint64_t cat_id, EventStatMap* event_stats)
{
  UNUSED(event_stats);
  fprintf(stdout, "cat_id: %lu\n", cat_id);
  fprintf(stdout, "#   %-20s\tavg\tcount\tmin\tmax\n", "event");
  event_stats->for_each(for_each_event);
  return true;
}

bool PerfReporter::for_each_event(uint64_t event_id, EventStat* event_stat)
{
  const char * event_name = NAME(static_cast<ObEventID>(event_id));
  if (NULL == event_name) {
    fprintf(stdout, "    %lu", event_id);
  } else {
    fprintf(stdout, "    %-20s", event_name);
  }
  if (event_stat->count_ > 0) {
    fprintf(stdout, "\t%10.2f\t%lu\t%lu\t%lu\n",
            ((double)event_stat->total_)/((double)event_stat->count_),
            event_stat->count_,
            event_stat->min_, event_stat->max_);
  } else {
    fprintf(stdout, "\t0\tNULL\tNULL\tNULL\n");
  }
  return true;
}

int PerfReporter::report()
{
  map_.for_each(for_each_cat_id);
  return OB_SUCCESS;
}

class PairEventReporter: public Processor<ObPerfEventRecorder>
{
public:
  PairEventReporter() {}
  virtual ~PairEventReporter() {}

  virtual int init();
  virtual int handle_record();
  virtual int report();
private:
  struct EventStat
  {
    uint64_t count_;
    uint64_t total_;
    int64_t cur_begin_ts_;
  };
  typedef ObConcurrentHashMap<uint64_t, EventStat*> EventStatMap;
  typedef ObConcurrentHashMap<uint64_t, EventStatMap*> CatStatMap;
  static inline bool is_pair_begin(uint64_t event_id);
  static bool for_each_cat_id(uint64_t cat_id, EventStatMap* event_stats);
  static bool for_each_event(uint64_t event, EventStat* event_stat);
private:
  CatStatMap map_;
};

int PairEventReporter::init()
{
  return map_.init();
}

bool PairEventReporter::is_pair_begin(uint64_t event_id)
{
  return (event_id - ::oceanbase::name::__PAIR_NAME_BEGIN__) % 2 == 1;
}

int PairEventReporter::handle_record()
{
  int ret = OB_SUCCESS;
  ObPerfEventRecorder &rec = *rec_;
  if (OB_ENTRY_NOT_EXIST == map_.contains_key(rec.get_cat_id())) {
    EventStatMap *event_stats = new(std::nothrow) EventStatMap();
    if (NULL == event_stats) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(event_stats->init())) {
      OB_LOG(WARN, "failed to init event_stats", K(ret));
    } else if (OB_FAIL(map_.put_refactored(rec.get_cat_id(), event_stats))) {
      OB_LOG(WARN, "failed to push into map", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    EventStatMap *event_stats = NULL;
    if (OB_FAIL(map_.get_refactored(rec.get_cat_id(), event_stats))) {
      OB_LOG(WARN, "failed to get event_stats", K(ret), "cat_id", rec.get_cat_id());
    } else {
      int64_t N = rec.count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
        const ObTimestampEvent &event = rec.get_event(i);
        if (event.id_ > ::oceanbase::name::__PAIR_NAME_BEGIN__) {
          // is pair event
          if (is_pair_begin(event.id_)) {
            if (OB_ENTRY_NOT_EXIST == event_stats->contains_key(event.id_)) {
              EventStat *ev_stat = new(std::nothrow) EventStat();
              if (NULL == ev_stat) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                ev_stat->count_ = 0;
                ev_stat->total_ = 0;
                ev_stat->cur_begin_ts_ = event.timestamp_;
                if (OB_FAIL(event_stats->put_refactored(event.id_, ev_stat))) {
                  OB_LOG(WARN, "failed to put into map", K(ret));
                }
              }
            } else {
              EventStat *ev_stat = NULL;
              if (OB_FAIL(event_stats->get_refactored(event.id_, ev_stat))) {
                OB_LOG(WARN, "failed to get from map", K(ret));
              } else {
                ev_stat->cur_begin_ts_ = event.timestamp_;
              }
            }
          } else {  // pair end
            uint64_t begin_id = event.id_ - 1;
            if (OB_ENTRY_NOT_EXIST == event_stats->contains_key(begin_id)) {
              OB_LOG(WARN, "found end event before begin event", K(begin_id));
            } else {
              EventStat *ev_stat = NULL;
              if (OB_FAIL(event_stats->get_refactored(begin_id, ev_stat))) {
                OB_LOG(WARN, "failed to get from map", K(ret));
              } else if (ev_stat->cur_begin_ts_ <= 0
                         || ev_stat->cur_begin_ts_ > event.timestamp_) {
                OB_LOG(WARN, "found end event before begin event", K(begin_id));
              } else {
                uint64_t elapsed = event.timestamp_ - ev_stat->cur_begin_ts_;
                ev_stat->cur_begin_ts_ = 0;  // reset
                ev_stat->count_++;
                ev_stat->total_ += elapsed;
              }
            }
          }
        }
      } // end for
    }
  }
  return ret;
}

bool PairEventReporter::for_each_cat_id(uint64_t cat_id, EventStatMap* event_stats)
{
  UNUSED(event_stats);
  fprintf(stdout, "cat_id: %lu\n", cat_id);
  fprintf(stdout, "#   %-20s\tavg\tcount\n", "pair event");
  event_stats->for_each(for_each_event);
  return true;
}

bool PairEventReporter::for_each_event(uint64_t event_id, EventStat* event_stat)
{
  const char * event_name = NAME(static_cast<ObEventID>(event_id));
  if (NULL == event_name) {
    fprintf(stdout, "    %lu", event_id);
  } else {
    fprintf(stdout, "    %-20.*s", static_cast<int>(strlen(event_name)-strlen("_begin")), event_name);  // remove `_begin'
  }
  if (event_stat->count_ > 0) {
    fprintf(stdout, "\t%10.2f\t%lu\n",
            ((double)event_stat->total_)/((double)event_stat->count_),
            event_stat->count_);
  } else {
    fprintf(stdout, "\t0\tNULL\n");
  }
  return true;
}

int PairEventReporter::report()
{
  map_.for_each(for_each_cat_id);
  return OB_SUCCESS;
}

class AtomicOpDumper: public Processor<ObAtomicOpEventRecorder>
{
public:
  AtomicOpDumper() {}
  virtual ~AtomicOpDumper() {}
  virtual int init() { return OB_SUCCESS; }
  virtual int handle_record()
  {
    fprintf(stdout, "%s\n", S(*rec_));
    return OB_SUCCESS;
  }
  virtual int report() { return OB_SUCCESS; }
};

// report counter statistics
class StatReporter: public Processor<ObAtomicOpEventRecorder>
{
public:
  StatReporter()
      :begin_ts_(0),
       end_ts_(0)
  {}
  virtual ~StatReporter() {}

  virtual int init();
  virtual int handle_record();
  virtual int report();
private:
  struct EventStat
  {
    uint64_t count_;
  };
  typedef ObConcurrentHashMap<uint64_t, EventStat*> EventStatMap;
  static bool for_each_event(uint64_t event, EventStat* event_stat);
  static int64_t elapsed_time_;
private:
  EventStatMap map_;
  int64_t begin_ts_;
  int64_t end_ts_;
};

int64_t StatReporter::elapsed_time_ = 0;
int StatReporter::init()
{
  begin_ts_ = 0;
  end_ts_ = 0;
  return map_.init();
}
int StatReporter::handle_record()
{
  int ret = OB_SUCCESS;
  ObAtomicOpEventRecorder &rec = *rec_;
  int64_t N = rec.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    const ObAtomicOpEvent &event = rec.get_event(i);
    if (0 == begin_ts_) {
      begin_ts_ = event.timestamp_;
    }
    end_ts_ = event.timestamp_;

    if (OB_ENTRY_NOT_EXIST == map_.contains_key(event.id_)) {
      EventStat *ev_stat = new(std::nothrow) EventStat();
      if (NULL == ev_stat) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ev_stat->count_ = 1;
        if (OB_FAIL(map_.put_refactored(event.id_, ev_stat))) {
          OB_LOG(WARN, "failed to put into map", K(ret));
        }
      }
    } else {
      EventStat *ev_stat = NULL;
      if (OB_FAIL(map_.get_refactored(event.id_, ev_stat))) {
        OB_LOG(WARN, "failed to get from map", K(ret));
      } else {
        ev_stat->count_++;
      }
    }
  }  // end for
  return ret;
}
bool StatReporter::for_each_event(uint64_t event_id, EventStat* event_stat)
{
  const char * event_name = NAME(static_cast<ObEventID>(event_id));
  if (NULL == event_name) {
    fprintf(stderr, "unknown event %lu\n", event_id);
  } else {
    fprintf(stdout, "    %20lu %-20s # %10.2f per sec\n",
            event_stat->count_,
            event_name,
            (((double)event_stat->count_) / ((double)elapsed_time_/1000000)));
  }
  return true;
}
int StatReporter::report()
{
  elapsed_time_ = end_ts_ - begin_ts_;
  map_.for_each(for_each_event);
  fprintf(stdout, "\n%10.2f seconds time elapsed.\n", (((double)elapsed_time_)/10000000));
  return OB_SUCCESS;
}

// @param type: 1 normal perf file
//              2 atomic op perf file
int check_file_format(const char* filename, int &type)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char buf[8];
  if (-1 == (fd = ::open(filename, O_RDONLY))) {
    ret = OB_ERR_SYS;
    OB_LOG(WARN, "failed to open perf data file", K(ret), K(filename), K(errno));
  } else {
    ssize_t len = ::read(fd, buf, 8);
    if (len != 8) {
      ret = OB_ERR_SYS;
      OB_LOG(WARN, "failed to read header", K(ret), K(len));
    } else {
      if (0 == memcmp(buf, ObPerfWriter::PERF_MAGIC, 8)) {
        fprintf(stderr, "This is a normal obperf data file.\n");
        type = 1;
      } else if (0 == memcmp(buf, ObAtomicOpEventWriter::ATOMIC_MAGIC, 8)) {
        fprintf(stderr, "This is an atomic_op obperf data file.\n");
        type = 2;
      } else {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "Invalid data file");
      }
    }
  }
  if (fd != -1) {
    (void)::close(fd);
    fd = -1;
  }
  return ret;
}

enum ReportMode
{
  RPT_PERF_MODE = 0,
  RPT_DUMP_MODE = 1,
  RPT_PAIR_MODE = 2,
  RPT_STAT_MODE = 3
};

template <typename RECORD, typename READER, typename PROCESSOR>
int perf_report(PROCESSOR *processor, const char* filename)
{
  READER reader;
  int ret = OB_SUCCESS;
  if (NULL == processor) {
    fprintf(stderr, "failed to allocate processor\n");
  } else if (OB_FAIL(processor->init())) {
    fprintf(stderr, "failed to init processor\n");
  } else if (OB_FAIL(reader.open(filename))) {
    fprintf(stderr, "failed to open data file `%s'\n", filename);
  } else {
    RECORD rec;
    int64_t count = 0;
    while(OB_SUCCESS == ret && OB_SUCCESS == (ret = reader.read(rec))) {
      processor->set_record(&rec);
      if (OB_FAIL(processor->handle_record())) {
        fprintf(stderr, "failed to handle record. (err=%d)\n", ret);
      } else {
        count++;
        rec.reset();
      }
    }
    if (OB_SUCCESS != ret) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        fprintf(stderr, "failed to read data record. (err=%d)\n", ret);
      }
    }
    if (OB_SUCCESS == ret) {
      fprintf(stderr, "Processed %ld records. \n", count);
      (void)processor->report();
    }
  }
  if (NULL != processor) {
    delete processor;
  }
  return ret;
}

int main(int argc, char *argv[])
{
  int c = 0;
  ReportMode report_mode = RPT_PERF_MODE;

  while(-1 != (c = getopt(argc, argv, "hvDpsr"))) {
    switch(c) {
      case 'h':
        print_usage();
        exit(0);
        break;
      case 'v':
        print_version();
        exit(0);
        break;
      case 'D':
        report_mode = RPT_DUMP_MODE;
        break;
      case 'p':
        report_mode = RPT_PAIR_MODE;
        break;
      case 's':
        report_mode = RPT_STAT_MODE;
        break;
      default:
        break;
    }
  }
  if (optind >= argc) {
    print_usage();
    exit(EXIT_FAILURE);
  }
  ////////////////
  OB_LOGGER.set_log_level("WARN");
  const char* filename = argv[optind];
  int filetype = 0;
  int ret = 0;
  if (OB_FAIL(check_file_format(filename, filetype))) {
    ret = -1;
  } else {
    if (filetype == 1) {
      Processor<ObPerfEventRecorder> *processor = NULL;
      switch(report_mode) {
        case RPT_PERF_MODE:
          processor = new PerfReporter();
          break;
        case RPT_DUMP_MODE:
          processor = new Dumper();
          break;
        case RPT_PAIR_MODE:
          processor = new PairEventReporter();
          break;
        default:
          fprintf(stderr, "invalid report mode for perf data\n");
          print_usage();
          exit(EXIT_FAILURE);
          break;
      }
      ret = perf_report<ObPerfEventRecorder, ObPerfReader, Processor<ObPerfEventRecorder> >(processor, filename);
    } else {
      Processor<ObAtomicOpEventRecorder> *processor = NULL;
      switch(report_mode) {
        case RPT_DUMP_MODE:
          processor = new AtomicOpDumper();
          break;
        case RPT_STAT_MODE:
          processor = new StatReporter();
          break;
        default:
          fprintf(stderr, "invalid report mode for atomic op perf data\n");
          print_usage();
          exit(EXIT_FAILURE);
          break;
      }
      ret = perf_report<ObAtomicOpEventRecorder, ObAtomicOpEventReader, Processor<ObAtomicOpEventRecorder> >(processor, filename);
    }
  }
  return ret;
}
