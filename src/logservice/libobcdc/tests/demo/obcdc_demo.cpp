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
 * obcdc_demo
 */

#include <signal.h>
#include <iostream>
#include "include/libobcdc/libobcdc.h"
#include "include/libobcdc/ob_errno.h"

using namespace std;
using namespace oceanbase::libobcdc;
using namespace oceanbase::common;

typedef IBinlogRecord Record;

#define LOG(msg) \
    do { \
      std::cout << "[OBCDC][DEMO] " << msg << std::endl; \
    } while (0)

volatile bool stop_flag = false;

int create_obcdc_instance(ObCDCFactory &cdc_factory, IObCDCInstance *&obcdc_instance)
{
  int ret = OB_SUCCESS;

  if (NULL == (obcdc_instance = cdc_factory.construct_obcdc())) {
    ret = OB_NOT_INIT;
    LOG("[ERROR] construct_obcdc failed");
  }

  return ret;
}

void destroy_obcdc_instance(ObCDCFactory &cdc_factory, IObCDCInstance *obcdc_instance)
{
  obcdc_instance->stop();
  obcdc_instance->destroy();
  cdc_factory.deconstruct(obcdc_instance);
}

int init_obcdc_instance(IObCDCInstance &obcdc_instance)
{
  int ret = OB_SUCCESS;
  const char *config_path = "conf/libobcdc.conf";

  if (OB_SUCCESS != (ret = obcdc_instance.init(config_path, 0))) {
    LOG("obcdc_instance init failed");
  } else if (OB_SUCCESS != (ret = obcdc_instance.launch())) {
    LOG("obcdc_instance launch failed");
  }

  return ret;
}

int fetch_next_cdc_record(IObCDCInstance &obcdc_instance, Record *record)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 10000; // usec

  if (OB_SUCCESS != (ret = obcdc_instance.next_record(&record, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG("[WARN] next_record failed");
    }
  } else if (NULL == record) {
    ret = OB_ERR_UNEXPECTED;
    LOG("invalid record");
  } else {
    LOG("FETCH_RECORD SUCC");
  }

  return ret;
}

int release_cdc_record(IObCDCInstance &obcdc_instance, Record *record)
{
  int ret = OB_SUCCESS;

  obcdc_instance.release_record(record);

  return ret;
}

int handle_cdc_record(Record *record)
{
  int ret = OB_SUCCESS;
  return ret;
}

void handle_signal(int signo)
{
  switch (signo)
  {
    case SIGTERM:
    case SIGHUP:
      LOG("[SIGNAL] obcdc recv SIG TERM, will exit progress");
      stop_flag = true;
      break;
    default:
      LOG("[SIGNAL] obcdc recv unknown signal, skip");
      break;
  }
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  signal(SIGTERM, handle_signal);
  ObCDCFactory cdc_factory;
  IObCDCInstance *obcdc_instance = NULL;

  if (OB_SUCCESS != create_obcdc_instance(cdc_factory, obcdc_instance)) {
    LOG("[ERROR] construct_obcdc_instance failed");
  } else if (NULL == obcdc_instance) {
    ret = OB_ERR_UNEXPECTED;
    LOG("[ERROR] obcdc_instance should not be null!");
  } else {
    if (OB_SUCCESS != init_obcdc_instance(*obcdc_instance)) {
      LOG("[ERROR] obcdc_instance init failed");
    } else {
      while(OB_SUCCESS == ret && ! stop_flag) {
        Record *record = NULL;
        if (OB_SUCCESS != (ret = fetch_next_cdc_record(*obcdc_instance, record))) {
          if (OB_TIMEOUT == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG("[ERROR] fetch_next_cdc_record failed");
          }
        } else if (OB_SUCCESS != (ret = handle_cdc_record(record))) {
          LOG("[ERROR] handle_cdc_record failed");
        } else if (OB_SUCCESS != (ret = release_cdc_record(*obcdc_instance, record))) {
          LOG("[ERROR] release_cdc_record failed");
        }
      }
    }

    destroy_obcdc_instance(cdc_factory, obcdc_instance);
  }

  return 0;
}
