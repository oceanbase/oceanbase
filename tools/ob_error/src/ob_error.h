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

#ifndef OBERROR_ERROR_H
#define OBERROR_ERROR_H

#include "share/ob_errno.h"
#include "os_errno.h"
#include <iostream>
#include <cstdio>
using namespace oceanbase::common;

// code error print define
#define ERROR_PRINT(fmt, ...) printf("%s[%d]-<%s>: " #fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

enum Fac {
  ORA,  // oracle mode // ora
  PLS,  // oracle mode // pls
  MY,   // mysql mode
  NONE
};

struct OSErrorInfo {
public:
  OSErrorInfo() : error_name_(nullptr), error_msg_(nullptr), error_code_(-1)
  {}
  virtual ~OSErrorInfo()
  {}
  void free_space()
  {
    if (nullptr != error_name_) {
      free(error_name_);
      error_name_ = nullptr;
    }
    if (nullptr != error_msg_) {
      free(error_msg_);
      error_msg_ = nullptr;
    }
  }

  char* error_name_;
  char* error_msg_;
  int error_code_;
};

struct MySQLErrorInfo {
public:
  MySQLErrorInfo()
      : error_name_(nullptr),
        error_msg_(nullptr),
        mysql_errno_(-1),
        sqlstate_(nullptr),
        cause_(nullptr),
        solution_(nullptr),
        ob_error_(-1)
  {}
  virtual ~MySQLErrorInfo()
  {}
  void free_space()
  {
    if (nullptr != error_name_) {
      free(error_name_);
      error_name_ = nullptr;
    }
    if (nullptr != error_msg_) {
      free(error_msg_);
      error_msg_ = nullptr;
    }
    if (nullptr != sqlstate_) {
      free(sqlstate_);
      sqlstate_ = nullptr;
    }
    if (nullptr != cause_) {
      free(cause_);
      cause_ = nullptr;
    }
    if (nullptr != solution_) {
      free(solution_);
      solution_ = nullptr;
    }
  }

  char* error_name_;
  char* error_msg_;
  int mysql_errno_;
  char* sqlstate_;
  char* cause_;
  char* solution_;
  int ob_error_;
};

struct OracleErrorInfo {
public:
  OracleErrorInfo()
      : error_name_(nullptr),
        error_msg_(nullptr),
        cause_(nullptr),
        solution_(nullptr),
        facility_(NONE),
        error_code_(-1),
        ob_error_(-1)
  {}
  virtual ~OracleErrorInfo()
  {}
  void free_space()
  {
    if (nullptr != error_name_) {
      free(error_name_);
      error_name_ = nullptr;
    }
    if (nullptr != error_msg_) {
      free(error_msg_);
      error_msg_ = nullptr;
    }
    if (nullptr != cause_) {
      free(cause_);
      cause_ = nullptr;
    }
    if (nullptr != solution_) {
      free(solution_);
      solution_ = nullptr;
    }
  }

  char* error_name_;
  char* error_msg_;
  char* cause_;
  char* solution_;
  Fac facility_;
  int error_code_;
  int ob_error_;
};

struct OBErrorInfo {
public:
  OBErrorInfo() : error_name_(nullptr), error_msg_(nullptr), cause_(nullptr), solution_(nullptr), error_code_(-1)
  {}
  virtual ~OBErrorInfo()
  {}
  void free_space()
  {
    if (nullptr != error_name_) {
      free(error_name_);
      error_name_ = nullptr;
    }
    if (nullptr != error_msg_) {
      free(error_msg_);
      error_msg_ = nullptr;
    }
    if (nullptr != cause_) {
      free(cause_);
      cause_ = nullptr;
    }
    if (nullptr != solution_) {
      free(solution_);
      solution_ = nullptr;
    }
  }

  char* error_name_;
  char* error_msg_;
  char* cause_;
  char* solution_;
  int error_code_;
};

// The length of the second dimension, in order to solve the conflict of multiple identical error codes
constexpr int OB_MAX_SAME_ERROR_COUNT = 10;
constexpr int ORACLE_SPECIAL_ERROR_CODE = 600;
constexpr int ORACLE_MAX_ERROR_CODE = 65535;
constexpr int ORACLE_MSG_PREFIX = 11;  // strlen("ORA-00000: ")
constexpr float OB_ERROR_VERSION = 1.0;

static const char* facility_str[NONE] = {"ORA", "PLS", "MY"};

class ObErrorInfoMgr {
public:
  ObErrorInfoMgr();
  virtual ~ObErrorInfoMgr();

  bool insert_os_error(const char* name, const char* msg, int error_code);
  void print_os_error();
  bool is_os_error_exist()
  {
    return 0 < os_error_count_;
  }

  bool insert_mysql_error(const char* name, const char* msg, int mysql_errno, const char* sqlstate, const char* cause,
      const char* solution, int ob_error);
  void print_mysql_error();
  bool is_mysql_error_exist()
  {
    return 0 < mysql_error_count_;
  }

  bool insert_oracle_error(const char* name, const char* msg, const char* cause, const char* solution, Fac facility,
      int error_code, int ob_error);
  void print_oracle_error();
  bool is_oracle_error_exist()
  {
    return 0 < oracle_error_count_;
  }

  bool insert_ob_error(const char* name, const char* msg, const char* cause, const char* solution, int error_code);
  void print_ob_error();
  bool is_ob_error_exist()
  {
    return 0 < ob_error_count_;
  }

private:
  int os_error_count_;
  OSErrorInfo os_error_[OS_MAX_SAME_ERROR_COUNT];
  int mysql_error_count_;
  MySQLErrorInfo mysql_error_[OB_MAX_SAME_ERROR_COUNT];
  int oracle_error_count_;
  OracleErrorInfo oracle_error_[OB_MAX_SAME_ERROR_COUNT];
  int ob_error_count_;
  OBErrorInfo ob_error_[OB_MAX_SAME_ERROR_COUNT];
};

// oracle error code -> ob error map // Maximum number of identical error codes is OB_MAX_SAME_ERROR_COUNT
static int g_oracle_ora[ORACLE_MAX_ERROR_CODE][OB_MAX_SAME_ERROR_COUNT];
static int g_oracle_pls[ORACLE_MAX_ERROR_CODE][OB_MAX_SAME_ERROR_COUNT];
// mysql error code -> ob error map // Maximum number of identical error codes is OB_MAX_SAME_ERROR_COUNT
static int g_mysql_error[OB_MAX_ERROR_CODE][OB_MAX_SAME_ERROR_COUNT];
// os error code -> ob error map // Maximum number of identical error codes is OS_MAX_SAME_ERROR_COUNT
static int g_os_error[OS_MAX_ERROR_CODE][OS_MAX_SAME_ERROR_COUNT];

// adder
bool add_os_info(int error_code, ObErrorInfoMgr* mgr);
bool add_ob_info(int error_code, ObErrorInfoMgr* mgr);
bool add_oracle_info(Fac oracle_facility, int error_code, int argument, ObErrorInfoMgr* mgr);
bool add_mysql_info(int error_code, ObErrorInfoMgr* mgr);

// parser
void parse_error_code(const char* argv, int& error_code);
void parse_facility(const char* argv, Fac& facility);
bool parse_param(int args, char* argv[]);

// printer
bool print_error_info(Fac facility, int error_code, int argument);

// init
bool init_global_info();

#endif /* OBERROR_ERROR_H */
