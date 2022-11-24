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

#ifndef OCEANBASE_COMMON_OB_PROFILE_TYPE_H
#define OCEANBASE_COMMON_OB_PROFILE_TYPE_H
namespace oceanbase
{
namespace common
{
/* common */
//#define PCODE " pcode=[%d]"
// Comment SELF since it conflicts with other macro utils.
// Maybe should change these names for more identical.
// #define SELF " self=[%s]"
#define PEER " peer=[%s]"
#define RET " ret=[%d]"
#define TRACE_ID " trace_id=[%ld]"
#define SOURCE_CHANNEL_ID " source_chid=[%u]"
#define CHANNEL_ID " chid=[%u]"
/* Asynchronous rpc launch time*/
#define ASYNC_RPC_START_TIME " async_rpc_start_time=[%ld]"
/*Asynchronous rpc end time, the corresponding relationship is corresponding to chid*/
#define ASYNC_RPC_END_TIME " async_rpc_end_time=[%ld]"
/*Synchronize rpc initiation time*/
#define SYNC_RPC_START_TIME " sync_rpc_start_time=[%ld]"
/*Synchronization rpc end time*/
#define SYNC_RPC_END_TIME " sync_rpc_end_time=[%ld]"
/*The time when the network thread received the packet*/
#define PACKET_RECEIVED_TIME " packet_received_time=[%ld]"
/*The time the request waits in the queue unit: microseconds*/
#define WAIT_TIME_US_IN_QUEUE " wait_time_us_in_queue=[%ld]"
/*The actual start time of processing a packet, excluding queue time, etc.*/
#define HANDLE_PACKET_START_TIME " handle_packet_start_time=[%ld]"
/*The end time of the actual processing of a packet*/
#define HANDLE_PACKET_END_TIME " handle_packet_end_time=[%ld]"

/*The waiting time of the write request in the write queue, unit: microsecond*/
#define WAIT_TIME_US_IN_WRITE_QUEUE " wait_time_us_in_write_queue=[%ld]"
/*Read transaction, write transaction Waiting time in the read-write mixed queue, unit: microsecond*/
#define WAIT_TIME_US_IN_RW_QUEUE " wait_time_us_in_rw_queue=[%ld]"
/*Write transaction waiting time in the commit queue, unit: microsecond*/
#define WAIT_TIME_US_IN_COMMIT_QUEUE " wait_time_us_in_commit_queue=[%ld]"
/*The start time of UPS processing a request, that is, the entire process from the network thread receiving the request packet to replying to the result*/
#define UPS_REQ_START_TIME " req_start_time=[%ld]"
/*The end time of UPS processing a request, this time minus the above time is the total time it takes UPS to process a request*/
#define UPS_REQ_END_TIME " req_end_time=[%ld]"
/*The size of the ObScanner response from UPS, in bytes*/
#define SCANNER_SIZE_BYTES " scanner_size_bytes=[%ld]"

/*The time it takes for the parser to parse the sql statement to generate the logical execution plan, unit: microsecond*/
#define SQL_TO_LOGICALPLAN_TIME_US " sql_to_logicalplan_time_us=[%ld]"
/*The time for the logical execution plan to generate the physical execution plan, unit: microsecond*/
#define LOGICALPLAN_TO_PHYSICALPLAN_TIME_US " logicalplan_to_physicalplan_time_us=[%ld]"
/*The time that the mysql package waits in the ObMySQL queue. Unit: microseconds*/
#define WAIT_TIME_US_IN_SQL_QUEUE " wait_time_us_in_sql_queue=[%ld]"
/*The sql request sent by the client*/
//#define SQL " sql=[%.*s]"
/*The time spent processing a SQL request, not including the time waiting in the ObMySQL queue*/
#define HANDLE_SQL_TIME_MS " handle_sql_time_ms=[%ld]"
/*The time spent by obmysql worker thread interacting with IO thread is only related to SELECT*/
#define IO_CONSUMED_TIME_US " io_consumed_time_us=[%d]"
#define SEND_RESPONSE_MEM_TIME_US " send_response_mem_time_us=[%d]"
#define STMT_EXECUTE_TIME_US " stmt_execute_us=[%ld]"
#define PARSE_EXECUTE_PARAMS_TIME_US " parse_execute_params=[%ld]"
#define CLOSE_PREPARE_RESULT_SET_TIME_US " close_prepare_result_set=[%ld]"
#define CLEAN_SQL_ENV_TIME_US " clean_sql_env_time_us=[%ld]"
#define OPEN_RESULT_SET " open_result_set_time_us=[%ld]"
#define OPEN_SQL_GET_REQUEST " open_sql_get_request_time_us=[%ld]"
#define OPEN_RPC_SCAN " open_rpc_scan_time_us=[%ld]"
#define CONS_SQL_GET_REQUEST " cons_sql_get_request=[%ld]"
#define CONS_SQL_SCAN_REQUEST " cons_sql_scan_request=[%ld]"
#define CREATE_SCAN_PARAM " create_scan_param_time_us=[%ld]"
#define SET_PARAM " set_param_time_us=[%ld]"
#define NEW_OB_SQL_SCAN_PARAM " new_ob_sql_scan_param_time_us=[%ld]"
#define NEW_OB_MS_SQL_SCAN_REQUEST " new_ob_ms_sql_scan_request=[%ld]"
#define RESET_SUB_REQUESTS " reset_sub_requests_time_us=[%ld]"
/*Start time and end time of scan root table*/
#define SCAN_ROOT_TABLE_START_TIME " scan_root_table_start_time=[%ld]"
#define SCAN_ROOT_TABLE_END_TIME " scan_root_table_end_time=[%ld]"
}// common
}// oceanbase

#endif
