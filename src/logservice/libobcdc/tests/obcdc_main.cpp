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
 *
 * obcdc_tailf module
 */

#define USING_LOG_PREFIX OBLOG_TAILF

#include "obcdc_main.h"
#include "ob_log_instance.h"    // ObLogInstance
#include "share/ob_time_zone_info_manager.h"              // FETCH_TZ_INFO_SQL
#include "ob_log_trans_ctx.h"

#include <stdio.h>        // fprintf
#include <getopt.h>       // getopt_long
#include <stdlib.h>       // strtoull

#define LOG_STD(str, ...) \
  do { \
    fprintf(stderr, str, ##__VA_ARGS__); \
  } while (0)

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObLogMain &ObLogMain::get_instance()
{
  static ObLogMain oblog_main;
  return oblog_main;
}

ObLogMain::ObLogMain() : inited_(false),
                         obcdc_instance_(NULL),
                         obcdc_factory_(),
                         br_printer_(),
                         only_print_hex_(false),
                         only_print_dml_tx_checksum_(false),
                         print_hex_(false),
                         print_lob_md5_(false),
                         use_daemon_(false),
                         data_file_(NULL),
                         heartbeat_file_(NULL),
                         run_time_us_(-1),
                         config_file_(NULL),
                         print_console_(false),
                         verify_mode_(false),
                         enable_reentrant_(false),
                         output_br_detail_(false),
                         output_br_special_detail_(false),
                         start_timestamp_usec_(0),
                         tenant_id_(OB_INVALID_TENANT_ID),
                         tg_match_pattern_(NULL),
                         last_heartbeat_timestamp_usec_(OB_INVALID_VERSION),
                         stop_flag_(true)
{
}

ObLogMain::~ObLogMain()
{
  destroy();
}

int ObLogMain::init(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(parse_args_(argc, argv))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("parse arguments fail", K(ret), K(argc));
    }
  } else if (! check_args_()) {
    LOG_ERROR("check arguments fail");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(br_printer_.init(data_file_, heartbeat_file_, print_console_, only_print_hex_, only_print_dml_tx_checksum_,
      print_hex_, print_lob_md5_, verify_mode_, output_br_detail_, output_br_special_detail_))) {
    LOG_ERROR("init binlog record printer fail", K(ret), K(data_file_), K(heartbeat_file_),
        K(print_console_), K(only_print_hex_), K_(only_print_dml_tx_checksum), K(print_hex_), K(print_lob_md5_),
        K(verify_mode_), K_(output_br_detail), K_(output_br_special_detail));
  } else {
    stop_flag_ = true;
    inited_ = true;
    last_heartbeat_timestamp_usec_ = OB_INVALID_VERSION;
  }

  return ret;
}

void ObLogMain::destroy()
{
  stop();

  inited_ = false;
  obcdc_instance_ = NULL;
  only_print_hex_ = false;
  only_print_dml_tx_checksum_ = false;
  print_hex_ = false;
  print_lob_md5_ = false;
  use_daemon_ = false;
  data_file_ = NULL;
  heartbeat_file_ = NULL;
  run_time_us_ = 0;
  config_file_ = NULL;
  print_console_ = false;
  verify_mode_ = false;
  enable_reentrant_ = false;
  start_timestamp_usec_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tg_match_pattern_ = NULL;
  last_heartbeat_timestamp_usec_ = OB_INVALID_VERSION;
  stop_flag_ = true;
  output_br_detail_ = false;
  output_br_special_detail_ = false;
  br_printer_.destroy();
}

int ObLogMain::parse_args_(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  // option variables
  int opt = -1;
  const char *opt_string = "iIvcdD:f:hH:oVt:rR:OxmT:Pp:s";
  struct option long_opts[] =
  {
    {"print_dml_checksum", 0, NULL, 'c'},
    {"use_daemon", 0, NULL, 'd'},
    {"data_file", 1, NULL, 'D'},
    {"config_file",1,NULL,'f'},
    {"help", 0, NULL, 'h'},
    {"heartbeat_file", 1, NULL, 'H'},
    {"print_console",0,NULL, 'o'},
    {"verify_mode", 0, NULL, 'V'},
    {"start_timestamp", 1, NULL, 't'},                // t: Represents a start-up timestamp in seconds
    {"start_timestamp_usec", 1, NULL, 'T'},           // T: Represents a start-up timestamp in microsecond units
    {"data_start_schema_version", 1, NULL, 's'},
    {"enable_reentrant", 0, NULL, 'r'},
    {"run_time_sec", 1, NULL, 'R'},
    {"only_print_hex", 0, NULL, 'O'},
    {"print_hex", 0, NULL, 'x'},
    {"print_lob_md5", 0, NULL, 'm'},
    {"version", 0, NULL, 'v'},
    {"verify_begin_trans_id", 0, NULL, 'P'},
    {"output_br_detail", 0, NULL, 'i'},
    {"output_br_special_detail", 0, NULL, 'I'},
    {"parse_timezone_info", 0, NULL, 'p'},
    {"delay_release", 0, NULL, 's'},
    {0, 0, 0, 0}
  };

  if (argc <= 1) {
    print_usage(argv[0]);
    ret = OB_IN_STOP_STATE;
  }

  // Parse command line
  while (OB_SUCCESS == ret && (opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
    switch (opt) {
      case 'f': {
        config_file_ = optarg;
        break;
      }

      case 'c': {
        only_print_dml_tx_checksum_ = true;
        break;
      }

      case 'd': {
        use_daemon_ = true;
        break;
      }

      case 'D': {
        data_file_ = optarg;
        break;
      }

      case 'h': {
        print_usage(argv[0]);
        ret = OB_IN_STOP_STATE;
        break;
      }

      case 'H': {
        heartbeat_file_ = optarg;
        break;
      }

      case 'o': {
        print_console_ = true;
        break;
      }

      case 'V': {
        verify_mode_ = true;
        break;
      }

      case 't': {
        start_timestamp_usec_ = strtoll(optarg, NULL, 10) * 1000000L;
        break;
      }

      case 'T': {
        start_timestamp_usec_ = strtoll(optarg, NULL, 10);
        break;
      }

      case 'r': {
        enable_reentrant_ = true;
        break;
      }

      case 'R': {
        run_time_us_ = strtoll(optarg, NULL, 10) * 1000000;
        LOG_STD("RUN_TIME: %ld seconds\n", run_time_us_ / 1000000);
        break;
      }

      case 'p': {
        parse_timezone_info_(optarg);
        ret = OB_IN_STOP_STATE;
        break;
      }

      case 'O': {
        only_print_hex_ = true;
        break;
      }

      case 'x': {
        print_hex_ = true;
        break;
      }

      case 'm': {
        print_lob_md5_ = true;
        break;
      }

      case 'i': {
        // output detail info of binlog record, default off
        output_br_detail_ = true;
        break;
      }

      case 's': {
        delay_release_ = true;
        break;
      }

      case 'I': {
        // output special detail info of binlog record, default off
        // Such as, ObTraceInfo
        output_br_special_detail_ = true;
        break;
      }

      case 'v': {
        ObLogInstance::print_version();
        ret = OB_IN_STOP_STATE;
        break;
      }
      case 'P': {
        // Verify that the begin trans_id function does not fall back and is turned on by default
        LOG_STD("verify_begin_trans_id\n");
        break;
      }

      default:
        ret = OB_ERROR;
        LOG_ERROR("unknown parameters", K(opt), K(opt_string));
        break;
    } // end switch
  } // end while

  return ret;
}

void ObLogMain::print_usage(const char *prog_name)
{
  LOG_STD("USAGE: %s -f config_file_path\n\n"
      "   -v, --version                       print version\n"
      "   -d, --use_daemon                    start as daemon, default no daemon\n"
      "   -D, --data_file                     data file used to store data\n"
      "   -f, --config_file                   configuration file\n"
      "   -h, --help                          display this help\n"
      "   -H, --heartbeat_file                heartbeat file used to store heartbeat data\n"
      "   -o, --print_console                 output result to stderr or stdout, default not output\n"
      "   -V, --verify_mode                   start verify mode\n"
      "   -t, --start_timestamp               start timestamp in second, default current timestamp\n"
      "   -T, --start_timestamp_usec          start timestamp in micro second, default current timestamp\n"
      "   -r, --enable_reentrant              enable reentrant after stop, default disable\n"
      "   -R, --run_time_sec                  run time in seconds, default -1, means to run forever\n"
      "   -x, --print_hex                     print hex for newcolumn, to check implicit char\n"
      "   -c, --print_dml_checksum            only print checksum of dml_trans\n"
      "   -m, --print_lob_md5                 print md5 info for LOB data\n"
      "   -i, --output_br_detail              output immutable detail info of binlog record, default not output\n"

      "\neg: %s -f libobcdc.conf\n",
      prog_name, prog_name);
}

bool ObLogMain::check_args_()
{
  int ret = OB_SUCCESS;
  int nochdir = 1;
  int noclose = 0;

  if (NULL == config_file_) {
    LOG_ERROR("config file is missing");
    ret = OB_INVALID_ARGUMENT;
  } else if (use_daemon_ && daemon(nochdir, noclose) < 0) {
    LOG_ERROR("create daemon process error", K(errno), KERRMSG);
    ret = OB_ERR_UNEXPECTED;
  }

  return OB_SUCCESS == ret;
}

int ObLogMain::start()
{
  int ret = OB_SUCCESS;
  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL != obcdc_instance_) {
    LOG_ERROR("oblog has started");
    ret = OB_NOT_SUPPORTED;
  } else if (stop_flag_) {
    stop_flag_ = false;

    if (NULL == (obcdc_instance_ = obcdc_factory_.construct_obcdc())) {
      LOG_ERROR("construct obcdc fail");
      ret = OB_INIT_FAIL;
    } else {
      ObLogInstance *instance = (ObLogInstance *)obcdc_instance_;
      // Disable redirected output
      instance->set_disable_redirect_log(true);

      if (OB_FAIL(instance->init_with_start_tstamp_usec(config_file_, start_timestamp_usec_, handle_error))) {
        LOG_ERROR("init oblog fail", K(ret), K_(config_file), K_(start_timestamp_usec), KP(handle_error));
      } else {
        _LOG_INFO("sizeof TransCtx: %lu", sizeof(TransCtx));
        _LOG_INFO("sizeof PartTransTask: %lu", sizeof(PartTransTask));
        _LOG_INFO("sizeof LogEntryTask: %lu", sizeof(ObLogEntryTask));
        // do nothing
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(obcdc_instance_->launch())) {
          LOG_ERROR("launch oblog fail", K(ret));
        } else {
          OB_LOGGER.set_log_level(instance->get_log_level());
          OB_LOGGER.set_file_name(instance->get_log_file(), true, false);
          LOG_INFO("start oblog success");
        }
      }
    }
  }

  return ret;
}

void ObLogMain::stop()
{
  stop_flag_ = true;

  if (NULL != obcdc_instance_) {
    obcdc_instance_->stop();
    obcdc_instance_->destroy();
    obcdc_factory_.deconstruct(obcdc_instance_);
    obcdc_instance_ = NULL;
  }
}

void ObLogMain::run()
{
  if (inited_ && NULL != obcdc_instance_) {
    int ret = OB_SUCCESS;
    int64_t end_time = get_timestamp() + run_time_us_;

    while (OB_SUCCESS == ret && ! stop_flag_) {
      IBinlogRecord *br = NULL;
      ret = obcdc_instance_->next_record(&br, NEXT_RECORD_TIMEOUT);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(verify_record_info_(br))) {
          LOG_ERROR("verify_record_info_ fail", KR(ret), K(br));
        } else if (br_printer_.need_print_binlog_record()) {
          // output binlog record
          if (OB_FAIL(br_printer_.print_binlog_record(br))) {
            LOG_ERROR("print_binlog_record fail", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(delay_release_)) {
            static const int64_t delay_release_time = 200; // 200 us, output 5K RPS at most
            usleep(delay_release_time);
          }
          obcdc_instance_->release_record(br);
          br = NULL;
        }
      } else if (OB_TIMEOUT == ret) {
        int64_t left_time = end_time - get_timestamp();
        if (run_time_us_ > 0 && left_time <= 0) {
          ret = OB_TIMEOUT;
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ITER_END == ret) {
        LOG_INFO("iterate BinlogRecord to the END");
        stop_flag_ = true;
        ret = OB_SUCCESS;
      } else if (OB_IN_STOP_STATE == ret) {
        stop_flag_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("next_record fail", KR(ret));
      }
    }
  }
}

void ObLogMain::handle_error(const ObCDCError &err)
{
  LOG_INFO("stop oblog on error", "level", err.level_, "errno", err.errno_, "errmsg", err.errmsg_);
  ObLogMain::get_instance().mark_stop_flag(true);
}

int ObLogMain::verify_record_info_(IBinlogRecord *br)
{
  int ret = OB_SUCCESS;
  static bool is_first_br = true;
  ObLogBR *oblog_br = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMain has not inited", KR(ret));
  } else if (OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("br is null", KR(ret));
  } else if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>(br->getUserData()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get user data fail", KR(ret), K(br), K(oblog_br));
  } else {
    // heartbeat, updtae last_heartbeat_timestamp_usec_
    int64_t checkpoint_timestamp_usec = OB_INVALID_TIMESTAMP;

    if (is_first_br) {
      // oblog_tailf -f $CONFIG -t 0 means start at current time
      // The libobcdc start timestamp is not available
      // So the first BinlogRecord is obtained based on the checkpoint
      checkpoint_timestamp_usec = br->getCheckpoint1() * 1000000 + br->getCheckpoint2();
      is_first_br = false;
    } else if (HEARTBEAT == br->recordType()) {
      checkpoint_timestamp_usec = br->getTimestamp() * 1000000 + br->getRecordUsec();
      if (checkpoint_timestamp_usec < last_heartbeat_timestamp_usec_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("checkpoint rollbacked", KR(ret), K(checkpoint_timestamp_usec), K_(last_heartbeat_timestamp_usec));
      }
    }

    if (OB_SUCC(ret)) {
      last_heartbeat_timestamp_usec_ = std::max(checkpoint_timestamp_usec, last_heartbeat_timestamp_usec_);

      // Calibration timestamp and checkpoint
      int64_t precise_timestamp = ObBinlogRecordPrinter::get_precise_timestamp(*br);
      int64_t timestamp_sec = precise_timestamp / 1000000;
      int64_t timestamp_usec = precise_timestamp % 1000000;
      int64_t expect_checkpoint1 = last_heartbeat_timestamp_usec_ / 1000000;
      int64_t expect_checkpoint2 = last_heartbeat_timestamp_usec_ % 1000000;

      if (OB_UNLIKELY(timestamp_sec != br->getTimestamp())
          || OB_UNLIKELY(timestamp_usec != br->getRecordUsec())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("timestamp is not right", KR(ret), K(precise_timestamp), "br_sec", br->getTimestamp(),
            "br_usec", br->getRecordUsec());
      } else if (OB_UNLIKELY(expect_checkpoint1 != br->getCheckpoint1())
          || OB_UNLIKELY(expect_checkpoint2 != br->getCheckpoint2())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("checkpoint is not right", KR(ret), K(br), K(last_heartbeat_timestamp_usec_),
            K(expect_checkpoint1), "br_checkpoint1", br->getCheckpoint1(),
            K(expect_checkpoint2), "br_checkpoint2", br->getCheckpoint2(),
            "getTimestamp", br->getTimestamp(), "getRecordUsec", br->getRecordUsec(),
            K(is_first_br));
      } else {
        // succ
      }
    }
  }

  return ret;
}

// use tools/import_time_zone_info.py to import tools/timezone_V1.log into server and will dump
// timezone_info.conf in obcdc online schema mode
int ObLogMain::parse_timezone_info_(const char *tzinfo_fpath)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  char *print_buf = nullptr;
  const int64_t buf_len = 128 * _M_;
  int64_t pos = 0;
  int64_t str_len = 0;
  common::ObRequestTZInfoResult tz_info_res;
  const char *parsed_timezone_info_file_name = "timezone_info.parsed";

  if (OB_ISNULL(buf = static_cast<char*>(ob_cdc_malloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory to load tz_info from local file failed", KR(ret), K(buf_len));
  } else if (OB_ISNULL(print_buf = static_cast<char*>(ob_cdc_malloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory to write parsed tz_info file failed", KR(ret), K(buf_len));
  } else if (OB_FAIL(read_from_file(tzinfo_fpath, buf, buf_len))) {
    LOG_ERROR("read_from_file failed", KR(ret), KCSTRING(tzinfo_fpath));
  } else if (OB_FAIL(tz_info_res.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize tz_info_res failed", KR(ret), K(buf_len), K(pos), KP(buf));
  } else {
    LOG_STD("prepare parse timezone_info\n");
    const int64_t tz_info_cnt = tz_info_res.tz_array_.count();
    common::databuff_printf(print_buf, buf_len, str_len, "package: %s\r\n", PACKAGE_STRING);
    common::databuff_printf(print_buf, buf_len, str_len, "timezone_info_version: %ld\r\n", tz_info_res.last_version_);

    for(int64_t i = 0; OB_SUCC(ret) && i < tz_info_cnt; i++) {
      const common::ObTimeZoneInfoPos &pos = tz_info_res.tz_array_[i];
      int64_t tmp_buf_len = 2 * _M_;
      char tmp_buf[tmp_buf_len];
      int64_t tmp_pos = pos.to_string(tmp_buf, tmp_buf_len);
      tmp_buf[tmp_pos] = '\0';

      if (OB_FAIL(common::databuff_printf(print_buf, buf_len, str_len,
          "timezone_info_pos[%ld/%ld]: %s\r\n",
          i + 1, tz_info_cnt, tmp_buf))) {
        LOG_STD("print timezone_info_pos failed, buf_len:%ld, str_len:%ld, tz_info_cnt:%ld/%ld, tmp_buf_len:%ld\n",
              buf_len, str_len, i, tz_info_cnt, tmp_buf_len);
        LOG_ERROR("print timezone_info_pos failed", KR(ret), K(buf_len), K(str_len), K(i), K(tz_info_cnt));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(buf_len < str_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("read len is larger than prepared buf", KR(ret), K(buf_len), K(str_len));
    } else if (OB_FAIL(write_to_file(parsed_timezone_info_file_name, print_buf, str_len))) {
      LOG_ERROR("write parsed timezone_info to file failed", KR(ret));
    } else {
      LOG_STD("parse_timezone_info success. parse from:%s(size: %s) to %s(size: %s)",
          tzinfo_fpath,
          SIZE_TO_STR(pos),
          parsed_timezone_info_file_name,
          SIZE_TO_STR(str_len));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_STD("parse_timezone_info done, ret:%d", ret);
  }

  if (OB_NOT_NULL(print_buf)) {
    ob_cdc_free(print_buf);
    print_buf = nullptr;
  }

  if (OB_NOT_NULL(buf)) {
    ob_cdc_free(buf);
    buf = nullptr;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
