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

#include "ob_error.h"
#include <string.h>
#include <getopt.h>

ObErrorInfoMgr::ObErrorInfoMgr() 
    : os_error_count_(0), 
      mysql_error_count_(0), 
      oracle_error_count_(0), 
      ob_error_count_(0)
{}

ObErrorInfoMgr::~ObErrorInfoMgr()
{
  for (int i = 0; i < os_error_count_; i++) {
    os_error_[i].free_space();
  }
  for (int i = 0; i < mysql_error_count_; i++) {
    mysql_error_[i].free_space();
  }
  for (int i = 0; i < oracle_error_count_; i++) {
    oracle_error_[i].free_space();
  }
  for (int i = 0; i < ob_error_count_; i++) {
    ob_error_[i].free_space();
  }
}

bool ObErrorInfoMgr::insert_os_error(const char* name, const char* msg, int error_code)
{
  bool bret = false;
  if (OS_MAX_SAME_ERROR_COUNT <= os_error_count_) {
    ERROR_PRINT("error: ObErrorInfoMgr os_error_[] is full.\n");
  } else if (nullptr == name) {
    ERROR_PRINT("error: name is nullptr.\n");
  } else if (nullptr == msg) {
    ERROR_PRINT("error: msg is nullptr.\n");
  } else {
    os_error_[os_error_count_].error_name_ = strdup(name);
    os_error_[os_error_count_].error_msg_ = strdup(msg);
    os_error_[os_error_count_].error_code_ = error_code;
    os_error_count_++;
    bret = true;
  }
  return bret;
}
void ObErrorInfoMgr::print_os_error()
{
  for (int i = 0; i < os_error_count_; i++) {
    printf("\n\tLinux Error Code: %s(%d)\n\tMessage: %s\n",
        os_error_[i].error_name_,
        os_error_[i].error_code_,
        os_error_[i].error_msg_);
  }
}

bool ObErrorInfoMgr::insert_mysql_error(const char* name, const char* msg, int mysql_errno, const char* sqlstate,
    const char* cause, const char* solution, int ob_error)
{
  bool bret = false;
  if (OB_MAX_SAME_ERROR_COUNT <= mysql_error_count_) {
    ERROR_PRINT("error: ObErrorInfoMgr mysql_error_[] is full.\n");
  } else if (nullptr == name) {
    ERROR_PRINT("error: name is nullptr.\n");
  } else if (nullptr == msg) {
    ERROR_PRINT("error: msg is nullptr.\n");
  } else if (nullptr == sqlstate) {
    ERROR_PRINT("error: sqlstate is nullptr.\n");
  } else if (nullptr == cause) {
    ERROR_PRINT("error: cause is nullptr.\n");
  } else if (nullptr == solution) {
    ERROR_PRINT("error: solution is nullptr.\n");
  } else {
    mysql_error_[mysql_error_count_].error_name_ = strdup(name);
    mysql_error_[mysql_error_count_].error_msg_ = strdup(msg);
    mysql_error_[mysql_error_count_].mysql_errno_ = mysql_errno;
    mysql_error_[mysql_error_count_].sqlstate_ = strdup(sqlstate);
    mysql_error_[mysql_error_count_].cause_ = strdup(cause);
    mysql_error_[mysql_error_count_].solution_ = strdup(solution);
    mysql_error_[mysql_error_count_].ob_error_ = ob_error;
    mysql_error_count_++;
    bret = true;
  }
  return bret;
}
void ObErrorInfoMgr::print_mysql_error()
{
  for (int i = 0; i < mysql_error_count_; i++) {
    if (0 == i) {
      printf("\n\tMySQL Error Code: %d (%s)\n", mysql_error_[i].mysql_errno_, mysql_error_[i].sqlstate_);
      printf("\tMessage: %s\n", mysql_error_[i].error_msg_);
    } else if (0 != strcmp(mysql_error_[i].error_msg_, mysql_error_[i - 1].error_msg_)) {
      printf("\tMessage: %s\n", mysql_error_[i].error_msg_);
    }
  }
  printf("\tRelated OceanBase Error Code:\n");
  for (int i = 0; i < mysql_error_count_; i++) {
    printf("\t\t%s(%d)\n", mysql_error_[i].error_name_, -mysql_error_[i].ob_error_);
  }
}

bool ObErrorInfoMgr::insert_oracle_error(const char* name, const char* msg, const char* cause, const char* solution,
    Fac facility, int error_code, int ob_error)
{
  bool bret = false;
  if (OB_MAX_SAME_ERROR_COUNT <= oracle_error_count_) {
    ERROR_PRINT("error: ObErrorInfoMgr oracle_error_[] is full.\n");
  } else if (nullptr == name) {
    ERROR_PRINT("error: name is nullptr.\n");
  } else if (nullptr == msg) {
    ERROR_PRINT("error: msg is nullptr.\n");
  } else if (nullptr == cause) {
    ERROR_PRINT("error: cause is nullptr.\n");
  } else if (nullptr == solution) {
    ERROR_PRINT("error: solution is nullptr.\n");
  } else if (MY <= facility) {
    ERROR_PRINT("error: facility is not a Oracle facility.\n");
  } else {
    oracle_error_[oracle_error_count_].error_name_ = strdup(name);
    oracle_error_[oracle_error_count_].error_msg_ = strdup(msg);
    oracle_error_[oracle_error_count_].cause_ = strdup(cause);
    oracle_error_[oracle_error_count_].solution_ = strdup(solution);
    oracle_error_[oracle_error_count_].facility_ = facility;
    oracle_error_[oracle_error_count_].error_code_ = error_code;
    oracle_error_[oracle_error_count_].ob_error_ = ob_error;
    oracle_error_count_++;
    bret = true;
  }
  return bret;
}
void ObErrorInfoMgr::print_oracle_error()
{
  for (int i = 0; i < oracle_error_count_; i++) {
    if (0 == i) {
      printf(
          "\n\tOracle Error Code: %s-%05d\n", facility_str[oracle_error_[i].facility_], oracle_error_[i].error_code_);
      printf("\tMessage: %s\n", oracle_error_[i].error_msg_);
    } else if (0 != strcmp(oracle_error_[i].error_msg_, oracle_error_[i - 1].error_msg_)) {
      printf("\tMessage: %s\n", oracle_error_[i].error_msg_);
    }
  }
  printf("\tRelated OceanBase Error Code:\n");
  for (int i = 0; i < oracle_error_count_; i++) {
    printf("\t\t%s(%d)\n", oracle_error_[i].error_name_, -oracle_error_[i].ob_error_);
  }
}

static bool is_special_oracle_error_compatible(int ob_error_code) {
  // These three errors have no argument in Oracle error msg
  return OB_AUTOINC_SERVICE_BUSY == ob_error_code || OB_ROWID_TYPE_MISMATCH == ob_error_code ||
            OB_ROWID_NUM_MISMATCH == ob_error_code;
}

bool ObErrorInfoMgr::insert_ob_error(
    const char* name, const char* msg, const char* cause, const char* solution, int error_code)
{
  bool bret = false;
  if (OB_MAX_SAME_ERROR_COUNT <= ob_error_count_) {
    ERROR_PRINT("error: ObErrorInfoMgr ob_error_[] is full.\n");
  } else if (nullptr == name) {
    ERROR_PRINT("error: name is nullptr.\n");
  } else if (nullptr == msg) {
    ERROR_PRINT("error: msg is nullptr.\n");
  } else if (nullptr == cause) {
    ERROR_PRINT("error: cause is nullptr.\n");
  } else if (nullptr == solution) {
    ERROR_PRINT("error: solution is nullptr.\n");
  } else {
    ob_error_[ob_error_count_].error_name_ = strdup(name);
    ob_error_[ob_error_count_].error_msg_ = strdup(msg);
    ob_error_[ob_error_count_].cause_ = strdup(cause);
    ob_error_[ob_error_count_].solution_ = strdup(solution);
    ob_error_[ob_error_count_].error_code_ = error_code;
    ob_error_count_++;
    bret = true;
  }
  return bret;
}
void ObErrorInfoMgr::print_ob_error()
{
  for (int i = 0; i < ob_error_count_; i++) {
    printf("\n\tOceanBase Error Code: %s(%d)\n\tMessage: %s\n\tCause: %s\n\tSolution: %s\n",
        ob_error_[i].error_name_,
        -ob_error_[i].error_code_,
        ob_error_[i].error_msg_,
        ob_error_[i].cause_,
        ob_error_[i].solution_);
    static const char* compatiable_header = "Compatible Error Code:";
    bool is_compat_header_printed = false;
    int ob_error_code = -ob_error_[i].error_code_;
    int mysql_errno = ob_mysql_errno(ob_error_code);
    if (-1 != mysql_errno) {
      const char *sqlstate = ob_sqlstate(ob_error_code);
      printf("\t%s\n", compatiable_header);
      is_compat_header_printed = true;
      printf("\t\tMySQL: %d(%s)\n", mysql_errno, sqlstate);
    }
    bool need_oracle_print = false;
    int oracle_errno = ob_errpkt_errno(ob_error_code, true);
    if (oracle_errno != -ob_error_code) {
      if (ORACLE_SPECIAL_ERROR_CODE == oracle_errno) {
        // Compatible error for ORA-00600
        if (is_special_oracle_error_compatible(ob_error_code)) {
          need_oracle_print = true;
        }
      } else {
        need_oracle_print = true;
      }
      if (need_oracle_print) {
        const char *oracle_err_msg = ob_errpkt_strerror(ob_error_code, true);
        if (nullptr != oracle_err_msg) {
          if (false == is_compat_header_printed) {
            printf("\t%s\n", compatiable_header);
            is_compat_header_printed = true;
          }
          char oracle_error_code[ORACLE_MSG_PREFIX] = {0};
          strncpy(oracle_error_code, oracle_err_msg, ORACLE_MSG_PREFIX-2);
          printf("\t\tOracle: %s\n", oracle_error_code);
        }
      }
    }
  }
}
//////////////////////////////////////////////////////////////
static void print_help()
{
  printf("This is the ob_error tool. Usage:\n\n"
         "    ob_error [option]\n"
         "    ob_error [facility] error_code [-a ARGUMENT]\n"
         "    ob_error [facility] error_code [--argument ARGUMENT]\n"
         "Get the error information, reasons and possible solutions.\n\n"
         "Query an error:\n\n"
         "    ob_error error_code\n\n"
         "Query an error in MySQL mode:\n\n"
         "    ob_error MY error_code\n\n"
         "Query an error in ORACLE mode:\n\n"
         "    ob_error facility error_code\n"
         "    ob_error facility error_code -a ARGUMENT\n"
         "    ob_error facility error_code --argument ARGUMENT\n\n"
         "ARGUMENT:         \n\n"
         "  Positive number   OceanBase error_code in ORA-00600 error output.\n\n"
         "facility:\n\n"
         "  MY                MySQL mode.\n"
         "  ORA               ORACLE mode. Error from database.\n"
         "  PLS               ORACLE mode. Error from the stored procedure.\n\n"
         "Normal options:\n\n"
         "  --help, -h        Print this message and then exit.\n"
         "  --version, -V     Print version information and then exit.\n\n");
}

static void print_version()
{
  printf("OceanBase ob_error %.1f\n", OB_ERROR_VERSION);
}

static void print_not_found(Fac facility, int error_code, int argument)
{
  if (MY > facility) {  // oracle mode
    if (-1 == argument) {
      printf("\n\tError Code %s-%05d not found.\n", facility_str[facility], error_code);
    } else {
      printf("\n\tError Code %s-%05d arguments: -%d not found.\n", facility_str[facility], error_code, argument);
    }
  } else {
    printf("\n\tError Code %d not found.\n", error_code);
  }
}

bool print_error_info(Fac facility, int error_code, int argument)
{
  bool bret = false;
  ObErrorInfoMgr mgr;
  if (MY > facility) {
    if (-1 != argument) {
      bret = add_ob_info(argument, &mgr);
      if (mgr.is_ob_error_exist()) {
        printf("\nOceanBase:");
        mgr.print_ob_error();
      } else if (!bret) {
        printf("\nOceanBase:");
        print_not_found(NONE, argument, -1);
      }
    }
    bool oracle_bret = add_oracle_info(facility, error_code, argument, &mgr);
    if (mgr.is_oracle_error_exist()) {
      printf("\nOracle:");
      mgr.print_oracle_error();
    } else if (!oracle_bret) {
      printf("\nOracle:");
      print_not_found(facility, error_code, argument);
    }
    bret = true;
  } else if (MY == facility) {
    bret = add_mysql_info(error_code, &mgr);
    if (mgr.is_mysql_error_exist()) {
      printf("\nMySQL:");
      mgr.print_mysql_error();
    } else if (!bret) {
      bret = add_ob_info(error_code, &mgr);
      if (mgr.is_ob_error_exist()) {
        printf("\nOceanBase:");
        mgr.print_ob_error();
      } else if (!bret) {
        printf("\nMySQL:");
        print_not_found(NONE, error_code, argument);
        bret = true;
      }
    }
  } else {
    bret |= add_os_info(error_code, &mgr);
    if (mgr.is_os_error_exist()) {
      printf("\nOperating System:");
      mgr.print_os_error();
    }
    bret |= add_ob_info(error_code, &mgr);
    if (mgr.is_ob_error_exist()) {
      printf("\nOceanBase:");
      mgr.print_ob_error();
    }
    bret |= add_mysql_info(error_code, &mgr);
    if (mgr.is_mysql_error_exist()) {
      printf("\nMySQL:");
      mgr.print_mysql_error();
    }
    bret |= add_oracle_info(facility, error_code, argument, &mgr);
    if (mgr.is_oracle_error_exist()) {
      printf("\nOracle:");
      mgr.print_oracle_error();
    }
    if (!bret) {
      printf("\nOceanBase:");
      print_not_found(NONE, error_code, -1);
      bret = true;
    }
  }
  return bret;
}
bool add_os_info(int error_code, ObErrorInfoMgr* mgr)
{
  bool bret = false;
  if (nullptr == mgr) {
    ERROR_PRINT("ObErrorInfoMgr *mgr is null.\n");
    bret = true;
  } else if (0 == error_code) {
    bret = true;
  } else if (0 < error_code && OS_MAX_ERROR_CODE > error_code) {
    int ob_error = 0;
    int info_count = 0;
    for (int i = 0; i < OS_MAX_SAME_ERROR_COUNT; i++) {
      if (-1 == g_os_error[error_code][i]) {
        break;
      } else {
        ob_error = g_os_error[error_code][i];
        const char* error_msg = str_os_error_msg(-ob_error);
        if (nullptr != error_msg) {
          const char* error_name = str_os_error_name(-ob_error);
          if (mgr->insert_os_error(error_name + strlen("OS_"), error_msg, error_code)) {
            info_count++;
          }
        }
      }
    }
    bret = (0 != info_count);
  }
  return bret;
}

bool add_ob_info(int error_code, ObErrorInfoMgr* mgr)
{
  bool bret = false;
  if (nullptr == mgr) {
    ERROR_PRINT("ObErrorInfoMgr *mgr is null.\n");
    bret = true;
  } else if (0 <= error_code && OB_MAX_ERROR_CODE > error_code) {
    if (0 == error_code) {
      const char* error_msg = "It is not an error.";
      const char* error_name = ob_error_name(-error_code);
      const char* error_cause = ob_error_cause(-error_code);
      const char* error_solution = ob_error_solution(-error_code);
      if (mgr->insert_ob_error(error_name, error_msg, error_cause, error_solution, error_code)) {
        bret = true;
      }
    } else {
      const char* error_usr_msg = ob_errpkt_str_user_error(-error_code, false);
      if (nullptr != error_usr_msg) {
        const char* error_msg = ob_errpkt_strerror(-error_code, false);
        const char* error_name = ob_error_name(-error_code);
        const char* error_cause = ob_error_cause(-error_code);
        const char* error_solution = ob_error_solution(-error_code);
        if (mgr->insert_ob_error(error_name, error_msg, error_cause, error_solution, error_code)) {
          bret = true;
        }
      }
    }
  }
  return bret;
}

static bool add_error_info(int error_code, Fac facility, int g_error[][OB_MAX_SAME_ERROR_COUNT], ObErrorInfoMgr* mgr)
{
  bool bret = false;
  int ob_error = 0;
  int info_count = 0;
  if (nullptr == mgr) {
    ERROR_PRINT("ObErrorInfoMgr *mgr is null.\n");
    bret = true;
  } else if (0 <= error_code) {
    for (int i = 0; i < OB_MAX_SAME_ERROR_COUNT; i++) {
      if (-1 == g_error[error_code][i]) {
        break;
      } else {
        ob_error = g_error[error_code][i];
        const char* error_usr_msg = ob_errpkt_str_user_error(-ob_error, MY > facility);
        if (nullptr != error_usr_msg) {
          const char* error_msg = ob_errpkt_strerror(-ob_error, MY > facility);
          const char* error_name = ob_error_name(-ob_error);
          const char* error_cause = ob_error_cause(-ob_error);
          const char* error_solution = ob_error_solution(-ob_error);
          if (MY > facility) {
            if (mgr->insert_oracle_error(error_name,
                    error_msg + ORACLE_MSG_PREFIX,
                    error_cause,
                    error_solution,
                    facility,
                    error_code,
                    ob_error)) {
              info_count++;
            }
          } else {
            const char* sqlstate = ob_sqlstate(-ob_error);
            if (mgr->insert_mysql_error(
                    error_name, error_msg, error_code, sqlstate, error_cause, error_solution, ob_error)) {
              info_count++;
            }
          }
        }
      }
    }
    bret = (0 != info_count);
  }
  return bret;
}

bool add_mysql_info(int error_code, ObErrorInfoMgr* mgr)
{
  bool bret = false;
  if (nullptr == mgr) {
    ERROR_PRINT("ObErrorInfoMgr *mgr is null.\n");
    bret = true;
  } else if (0 <= error_code && OB_MAX_ERROR_CODE > error_code) {
    if (-1 != g_mysql_error[error_code][0]) {
      // map is not emtpy which means MySQL error exists
      bret = add_error_info(error_code, MY, g_mysql_error, mgr);
    }
  }
  return bret;
}

bool add_oracle_info(Fac oracle_facility, int error_code, int argument, ObErrorInfoMgr* mgr)
{
  bool bret = false;
  if (nullptr == mgr) {
    ERROR_PRINT("ObErrorInfoMgr *mgr is null.\n");
    bret = true;
  } else if (0 <= error_code && ORACLE_MAX_ERROR_CODE > error_code) {
    int info_count = 0;
    int ob_error = -1;
    // Before being called, ensure that argument cannot be set when nullptr == oracle_facility
    if (NONE == oracle_facility) {
      // Handle the case where error is ORA-error_code
      bret |= add_error_info(error_code, ORA, g_oracle_ora, mgr);
      // Handle the case where error is PLS-error_code
      bret |= add_error_info(error_code, PLS, g_oracle_pls, mgr);
    } else {
      if (ORA == oracle_facility) {
        if (ORACLE_SPECIAL_ERROR_CODE == error_code) {
          // 600 is a special error code.
          // If there is no '-a ARG' parameter, the original possible error of ora-00600 will be output
          if (-1 == argument) {
            bret = add_error_info(error_code, ORA, g_oracle_ora, mgr);
          } else {
            ob_error = argument;
            const char* error_usr_msg = ob_errpkt_str_user_error(-ob_error, true);
            if (nullptr != error_usr_msg) {
              // verify that the error is ora-00600
              if (-OB_ERR_PROXY_REROUTE == ob_errpkt_errno(-ob_error, true) ||
                  ORACLE_SPECIAL_ERROR_CODE == ob_errpkt_errno(-ob_error, true)) {
                const char* error_msg = ob_errpkt_strerror(-ob_error, true);
                const char* error_name = ob_error_name(-ob_error);
                const char* error_cause = ob_error_cause(-ob_error);
                const char* error_solution = ob_error_solution(-ob_error);
                if (mgr->insert_oracle_error(error_name,
                        error_msg + ORACLE_MSG_PREFIX,
                        error_cause,
                        error_solution,
                        ORA,
                        error_code,
                        ob_error)) {
                  bret = true;
                }
              }
            }
          }
        } else {
          // '-a ARG' parameter only supports ora-00600 error
          if (-1 != argument) {
            printf("error: '-a ARG' is unsupport in this scene\n"
                   "Use 'ob_error ora 600 -a=ARG'.\n"
                   "Use 'ob_error --help' for help.\n");
            bret = true;
          } else {
            bret = add_error_info(error_code, ORA, g_oracle_ora, mgr);
          }
        }
      } else if (PLS == oracle_facility) {
        // '-a ARG' parameter only supports ora-00600 error
        if (-1 != argument) {
          printf("error: '-a ARG' is unsupport in this scene\n"
                 "Use 'ob_error ora 600 -a ARG'.\n"
                 "Use 'ob_error --help' for help.\n");
          bret = true;
        } else {
          bret = add_error_info(error_code, PLS, g_oracle_pls, mgr);
        }
      }
    }
  }
  return bret;
}

// prevent the atoi parse "123abc"
void parse_error_code(const char* argv, int& error_code)
{
  int len = 0;
  int code = 0;
  if (nullptr != argv) {
    len = strlen(argv);
    for (int i = 0; i < len; i++) {
      if (argv[i] <= '9' && argv[i] >= '0') {
        int tmp_code = code * 10 + (argv[i] - '0');
        if (code > tmp_code) {  // overflow
          code = -1;
          break;
        } else {
          code = tmp_code;
        }
      } else {
        code = -1;
        break;
      }
    }
    error_code = code;
  }
}

void parse_facility(const char* argv, Fac& facility)
{
  if (nullptr != argv) {
    if (0 == strcasecmp(argv, facility_str[ORA])) {
      facility = ORA;
    } else if (0 == strcasecmp(argv, facility_str[PLS])) {
      facility = PLS;
    } else if (0 == strcasecmp(argv, facility_str[MY])) {
      facility = MY;
    }
  }
}

bool parse_param(int args, char* argv[])
{
  bool bret = false;
  Fac facility = NONE;
  int error_code = -1;
  int argument = -1;

  if (1 < args && 5 >= args) {
    parse_facility(argv[1], facility);
    if (NONE == facility) {
      parse_error_code(argv[1], error_code);
    } else if (2 < args) {
      parse_error_code(argv[2], error_code);
    }

    extern char* optarg;
    extern int opterr;

    opterr = 0;  // getopt_long will not print error messages
    int option_index = 0;
    static struct option long_options[] = {
        {"help", 0, 0, 'h'}, {"version", 0, 0, 'V'}, {"argument", 1, 0, 'a'}, {0, 0, 0, 0}};

    int c = getopt_long(args, argv, ":hVa:", long_options, &option_index);
    if (-1 != c) {
      switch (c) {
        case 'h': {
          print_help();
          bret = true;
          break;
        }
        case 'V': {
          print_version();
          bret = true;
          break;
        }
        case 'a': {
          if (ORA == facility && ORACLE_SPECIAL_ERROR_CODE == error_code) {
            parse_error_code(optarg, argument);
            if (-1 == argument) {
              printf("error: '-a ARG': ARG should be a number\n"
                     "Use 'ob_error --help' for help.\n");
              bret = true;
            }
          } else {
            printf("error: '-a ARG' is unsupport in this scene\n"
                   "Use 'ob_error ora 600 -a ARG'.\n"
                   "Use 'ob_error --help' for help.\n");
            bret = true;
          }
          break;
        }
        case ':': {
          printf("error: '-a' missing parameter\n"
                 "Use 'ob_error --help' for help.\n");
          bret = true;
          break;
        }
        default: {
          printf("error: parameters invalid\n"
                 "Use 'ob_error --help' for help.\n");
          bret = true;
          break;
        }
      }
    }

    if (!bret) {
      if (-1 == error_code) {
        printf("error: 'facility' invalid or 'error_code' may overflow or not be a positive number\n"
               "Use 'ob_error --help' for help.\n");
        bret = true;
      } else {
        bret = print_error_info(facility, error_code, argument);
      }
    }
  }

  return bret;
}

static bool insert_oracle_error_slot_ora(int err_map[][OB_MAX_SAME_ERROR_COUNT], int error_code, int ob_error)
{
  bool bret = true;
  int k = 0;
  if (0 > error_code || ORACLE_MAX_ERROR_CODE < error_code) {
    ERROR_PRINT("error: error_code invalid.\n");
    bret = false;
  } else {
    for (k = 0; k < OB_MAX_SAME_ERROR_COUNT; k++) {
      if (-1 == err_map[error_code][k]) {
        if (ORACLE_SPECIAL_ERROR_CODE == error_code) {
          // Compatible error for ORA-00600
          if (is_special_oracle_error_compatible(-ob_error)) {
            err_map[error_code][k] = ob_error;
          }
        } else {
          err_map[error_code][k] = ob_error;
        }
        break;
      }
    }
    if (OB_MAX_SAME_ERROR_COUNT <= k) {
      bret = false;
    }
  }
  return bret;
}
static bool insert_oracle_error_slot_pls(int err_map[][OB_MAX_SAME_ERROR_COUNT], int error_code, int ob_error)
{
  bool bret = true;
  int k = 0;
  if (0 > error_code || ORACLE_MAX_ERROR_CODE < error_code) {
    ERROR_PRINT("error: error_code invalid.\n");
    bret = false;
  } else {
    for (k = 0; k < OB_MAX_SAME_ERROR_COUNT; k++) {
      if (-1 == err_map[error_code][k]) {
        err_map[error_code][k] = ob_error;
        break;
      }
    }
    if (OB_MAX_SAME_ERROR_COUNT <= k) {
      bret = false;
    }
  }
  return bret;
}
static bool insert_mysql_error_slot(int err_map[][OB_MAX_SAME_ERROR_COUNT], int error_code, int ob_error)
{
  bool bret = true;
  int k = 0;
  if (0 > error_code || OB_MAX_ERROR_CODE < error_code) {
    ERROR_PRINT("error: error_code invalid.\n");
    bret = false;
  } else {
    for (k = 0; k < OB_MAX_SAME_ERROR_COUNT; k++) {
      if (-1 == err_map[error_code][k]) {
        err_map[error_code][k] = ob_error;
        break;
      }
    }
    if (OB_MAX_SAME_ERROR_COUNT <= k) {
      bret = false;
    }
  }
  return bret;
}
static bool insert_os_error_slot(int err_map[][OS_MAX_SAME_ERROR_COUNT], int error_code, int ob_error)
{
  bool bret = true;
  int k = 0;
  if (0 > error_code || OS_MAX_ERROR_CODE < error_code) {
    ERROR_PRINT("error: error_code invalid.\n");
    bret = false;
  } else {
    for (k = 0; k < OS_MAX_SAME_ERROR_COUNT; k++) {
      if (-1 == err_map[error_code][k]) {
        err_map[error_code][k] = ob_error;
        break;
      }
    }
    if (OS_MAX_SAME_ERROR_COUNT <= k) {
      bret = false;
    }
  }
  return bret;
}
static bool ob_init_error_to_oberror(int ora_err[][OB_MAX_SAME_ERROR_COUNT], int pls_err[][OB_MAX_SAME_ERROR_COUNT],
    int mysql_err[][OB_MAX_SAME_ERROR_COUNT], int os_err[][OS_MAX_SAME_ERROR_COUNT])
{
  bool bret = true;
  int error_code = -1;
  int k = 0;
  // init os_err map
  for (int i = 0; i < OS_MAX_ERROR_CODE; i++) {
    error_code = os_errno(-i);
    if (-1 != error_code && 0 != error_code) {
      if (!insert_os_error_slot(os_err, error_code, i)) {
        bret = false;
        ERROR_PRINT("error: OS_MAX_SAME_ERROR_COUNT is not enough for OS Error %d(OB Error %d)\n", error_code, i);
      }
    }
  }
  // init mysql_err/ora_err/pls_err map
  for (int i = 0; i < OB_MAX_ERROR_CODE; i++) {
    // init mysql_err map
    error_code = ob_mysql_errno(-i);
    if (-1 != error_code && 0 != error_code) {
      if (0 > error_code)
        error_code = -error_code;
      if (!insert_mysql_error_slot(mysql_err, error_code, i)) {
        ERROR_PRINT("error: OB_MAX_SAME_ERROR_COUNT is not enough for Error %d(OB Error %d)\n", error_code, i);
        bret = false;
      }
    }
    // init ora_err/pls_err map
    const char* error_usr_msg = ob_oracle_str_user_error(-i);
    error_code = ob_oracle_errno(-i);
    if (-1 != error_code && NULL != error_usr_msg) {
      if (0 > error_code)
        error_code = -error_code;
      if (0 == strncmp(error_usr_msg, facility_str[ORA], strlen(facility_str[ORA]))) {
        if (!insert_oracle_error_slot_ora(ora_err, error_code, i)) {
          ERROR_PRINT("error: OB_MAX_SAME_ERROR_COUNT is not enough for ORA-%05d(OB Error %d)\n", error_code, i);
          bret = false;
        }
      } else if (0 == strncmp(error_usr_msg, facility_str[PLS], strlen(facility_str[PLS]))) {
        if (!insert_oracle_error_slot_pls(pls_err, error_code, i)) {
          ERROR_PRINT("error: OB_MAX_SAME_ERROR_COUNT is not enough for PLS-%05d(OB Error %d)\n", error_code, i);
          bret = false;
        }
      }
    }
  }
  return bret;
}

bool init_global_info()
{
  memset(g_oracle_ora, -1, sizeof(g_oracle_ora));
  memset(g_oracle_pls, -1, sizeof(g_oracle_pls));
  memset(g_mysql_error, -1, sizeof(g_mysql_error));
  memset(g_os_error, -1, sizeof(g_os_error));

  return ob_init_error_to_oberror(g_oracle_ora, g_oracle_pls, g_mysql_error, g_os_error);
}

int main(int args, char* argv[])
{
  if (!init_global_info()) {
    printf("\nerror: ob_error init failed.\n");
  } else if (1 < args) {
    if (!parse_param(args, argv)) {
      printf("error: parameters invalid\n"
             "Use 'ob_error --help' for help.\n");
    }
  } else {
    printf("error: missing parameter\n"
           "Use 'ob_error --help' for help.\n");
  }

  return 0;
}